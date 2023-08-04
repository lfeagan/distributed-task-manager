package net.vectorcomputing.dtm.postgresql;

import lombok.SneakyThrows;
import net.vectorcomputing.dtm.*;
import org.postgresql.ds.PGSimpleDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.shaded.com.google.common.collect.ImmutableSet;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import org.threeten.extra.PeriodDuration;

import javax.sql.DataSource;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicReference;

/**
 * A long-running simulation both demonstrating how code should be written for each worker and ensuring live-lock and dead-locks are avoided.
 * The basic scenario is that we are processing five-second blocks of data and producing a report that should be sent exactly once.
 * This report is sent to a Kafka topic for subsequent processing. We want to ensure that every window of time within the last 60 minutes has either been completed or is marked to be skipped.
 * Each time a task manager runs, it first checks if there are any buckets in the backlog that need to be processed (status created or failed).
 * If the backlog is empty, it attempts to create a new task. This may fail due to a race condition with another worker that might also decide to insert--that's ok!
 * We just continue and try to acquire the lock on the current time window--who knows, we might win this time!
 * If we acquire the lock on this bucket, we process the data (sleep), send our report to kafka, and exit.
 * If we don't, we quietly exit.
 *
 * A new task worker is created every second. The simulated work to be done by each worker takes 100ms to "process" (we sleep), and represents processing five seconds of data.
 * The task being managed needs a report to be produced for every five seconds of data.
 */
public class PostgresqlTaskManagerSimulation extends TimescaleTestContainer {

    private static final Logger LOGGER = LoggerFactory.getLogger(PostgresqlTaskManagerSimulation.class);

    @BeforeMethod
    public void before() {
        start();
    }

    @AfterMethod
    public void after() {
        stop();
    }

    static AtomicReference<ScheduledFuture> goodWorker = new AtomicReference<>();

    @Test
    public void simulate() throws TaskManagerException {
        final int numThreads = 8;
        final String taskName = "simulation";
        PostgresqlTaskManager ptm = new PostgresqlTaskManager(createNonPoolingDataSource());
        ptm.initialize();
        Duration bucket_interval = Duration.ofSeconds(5);
        SimulationTaskWorker worker = new SimulationTaskWorker(ptm, taskName, bucket_interval);
        ScheduledExecutorService ses = new ScheduledThreadPoolExecutor(numThreads);
        goodWorker.set(ses.scheduleAtFixedRate(worker, 0, 1, TimeUnit.SECONDS));
        ScheduledFuture chaosWorker = ses.scheduleWithFixedDelay(new ChaosTaskWorker(ptm, taskName, bucket_interval), 5, 5, TimeUnit.SECONDS);

        try {
            Thread.sleep(10000);
            disableToxiproxy();
            LOGGER.info("disabled toxiproxy");
            Thread.sleep(10000);
            enableToxiproxy();
            LOGGER.info("enabled toxiproxy");
        } catch (InterruptedException e) {
            // do nothing
        }

        try {
            goodWorker.get().get();
        } catch (CancellationException e) {
            List<Task> allTasks = ptm.getTasks(TaskQuery.builder().build());
            for (Task t : allTasks) {
                Assert.assertEquals(t.getStatus(), TaskStatus.COMPLETE);
            }
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } catch (ExecutionException e) {
            throw new RuntimeException(e);
        }
    }

    public DataSource createNonPoolingDataSource() {
        PGSimpleDataSource ds = new PGSimpleDataSource();
        ds.setDatabaseName("test");
        ds.setUser(getUser());
        ds.setPassword(getPassword());
        ds.setServerNames(new String[] {getHostname()});
        ds.setPortNumbers(new int[] { getPort()});
        return ds;
    }

    private static class SimulationTaskWorker implements Runnable {
        private final TaskManager taskManager;
        private final String taskName;
        private final Duration bucket_interval;

        private int acquiredCount = 0;

        public SimulationTaskWorker(TaskManager taskManager, String taskName, Duration bucket_interval) {
            this.taskManager = taskManager;
            this.taskName = taskName;
            this.bucket_interval = bucket_interval;
        }

        @Override
        @SneakyThrows
        public void run() {
            Instant bucket_time = TimeUtils.alignWithInterval(Instant.now(), Instant.EPOCH, bucket_interval);
            final TaskQuery availableWorkQuery = TaskQuery.builder()
                    .name(taskName)
                    .bucketStartTime(bucket_time.minus(72, ChronoUnit.HOURS)) // 3 day search window
                    .statuses(ImmutableSet.of(TaskStatus.AVAILABLE))
                    .build();
            try {
                Task acquiredTask = taskManager.getAndAcquireFirstTask(availableWorkQuery);
                if (acquiredTask == null) { // backlog query returned nothing
                    // try to create for current bucket
                    try {
                        Task createdTask = taskManager.createTask(taskName, bucket_time, PeriodDuration.of(bucket_interval), "SimulationTaskWorker");
                        if (createdTask != null) {
                            LOGGER.info("created task {}", createdTask);
                            createdTask.acquire("SimulationTaskWorker");
                            acquiredTask = createdTask;
                            LOGGER.info("acquired task {}", acquiredTask);
                        }
                    } catch (Exception e) {
                        // do nothing
                    }
                } else {
                    LOGGER.info("acquired task {} via backlog query", acquiredTask);
                }

                if (acquiredTask != null) {
                    // successfully acquired via backlog query or create-acquire sequence
                    Assert.assertTrue(acquiredTask.isAcquired());
                    try {
                        LOGGER.info("processing acquired task: {}", acquiredTask);
                        Thread.sleep(ThreadLocalRandom.current().nextInt(100));
                        LOGGER.info("processed data");
                        // send to kafka
                        acquiredTask.completed("finished");
                        LOGGER.info("finished task w/bucket time {} at {} ", acquiredTask.getBucketTime(), acquiredTask.getCompletedAt());
                        ++acquiredCount;
                    } catch (Exception e) {
                        // do nothing
                        LOGGER.error("something bad happened", e);
                        acquiredTask.failed(e.getMessage());
                    }
                } else {
                    LOGGER.info("unable to acquire task");
                }
                if (acquiredCount > 10) {
                    goodWorker.get().cancel(false);
                }
            } catch (Exception e) {
                LOGGER.info("unable to complete task", e);
            }
        }
    }

    /**
     * The chaos task worker goes and randomly marks old items as failed to force reprocessing.
     */
    private static class ChaosTaskWorker implements Runnable {
        private final TaskManager taskManager;
        private final String taskName;
        private final Duration bucket_interval;

        private int acquiredCount = 0;

        public ChaosTaskWorker(TaskManager taskManager, String taskName, Duration bucket_interval) {
            this.taskManager = taskManager;
            this.taskName = taskName;
            this.bucket_interval = bucket_interval;
        }

        @Override
        @SneakyThrows
        public void run() {
            Instant bucket_time = TimeUtils.alignWithInterval(Instant.now(), Instant.EPOCH, bucket_interval);
            final TaskQuery completedWorkQuery = TaskQuery.builder()
                    .name(taskName)
                    .bucketStartTime(bucket_time.minusSeconds(60)) // 1 minute search window
                    .statuses(ImmutableSet.of(TaskStatus.COMPLETE))
                    .build();
            try {
                Task acquiredTask = taskManager.getAndAcquireFirstTask(completedWorkQuery);
                if (acquiredTask != null) {
                    Assert.assertTrue(acquiredTask.isAcquired());
                    LOGGER.info("chaos acquired task {} via backlog query", acquiredTask);
                    try {
                        Thread.sleep(ThreadLocalRandom.current().nextInt(10));
                    } catch (InterruptedException e) {
                        // do nothing
                    }
                    acquiredTask.failed("chaos failed this task");
                    LOGGER.info("chaos failed the acquired task w/bucket time {}", acquiredTask.getBucketTime());
                } else {
                    LOGGER.info("chaos unable to acquire task to induce chaos");
                }
            } catch (Exception e) {
                LOGGER.info("Unable to inject chaos", e);
            }
        }
    }
}
