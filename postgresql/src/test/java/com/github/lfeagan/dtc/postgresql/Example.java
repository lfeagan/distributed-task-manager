package com.github.lfeagan.dtc.postgresql;

import com.github.lfeagan.dtc.Task;
import com.github.lfeagan.dtc.TaskManagerException;
import com.github.lfeagan.dtc.TaskSpecification;
import org.postgresql.ds.PGSimpleDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.time.Duration;
import java.util.concurrent.ThreadLocalRandom;

public class Example extends TimescaleTestContainer {

    private static final Logger LOGGER = LoggerFactory.getLogger(PostgresqlTaskManagerSimulation.class);

    public static void main(String[] args) throws TaskManagerException {
        Example example = new Example();
        try {
            example.start();
            example.run();
        } finally {
            example.stop();
        }
    }

    public void run() throws TaskManagerException {
        PostgresqlTaskManager taskManager = new PostgresqlTaskManager(createNonPoolingDataSource());
        taskManager.initialize();
        Duration bucket_interval = Duration.ofSeconds(5);

        TaskSpecification taskSpec = TaskSpecification.builder()
                .taskManager(taskManager)
                .taskName("example_task")
                .workerName("example_worker")
                .bucketInterval(bucket_interval)
                .backlogWindowSize(Duration.ofHours(72))
                .build();

        try {
            final Task acquiredTask = taskSpec.findOrCreateAndAcquire();
            if (acquiredTask != null) {
                taskSpec.process(acquiredTask,
                        (t) -> {
                            try {
                                Thread.sleep(ThreadLocalRandom.current().nextInt(100));
                            } catch (InterruptedException e) {
                                throw new RuntimeException(e);
                            }
                            return "finished";
                        },
                        (t, e) -> e.getMessage());
            } else {
                LOGGER.info("unable to acquire task");
            }
        } catch (Exception e) {
            LOGGER.info("unable to complete task", e);
        }
    }

    private DataSource createNonPoolingDataSource() {
        PGSimpleDataSource ds = new PGSimpleDataSource();
        ds.setDatabaseName("test");
        ds.setUser(getUser());
        ds.setPassword(getPassword());
        ds.setServerNames(new String[]{getHostname()});
        ds.setPortNumbers(new int[]{getPort()});
        return ds;
    }
}
