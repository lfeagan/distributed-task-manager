package net.vectorcomputing.dtm.postgresql;

import net.vectorcomputing.dtm.*;
import org.postgresql.ds.PGSimpleDataSource;
import org.testcontainers.shaded.com.google.common.collect.ImmutableSet;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import org.threeten.extra.PeriodDuration;

import javax.sql.DataSource;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;

public class PostgresqlTaskManagerTest extends TimescaleTestContainer {

   @BeforeMethod
   public void before() {
      start();
   }

   @AfterMethod
   public void after() {
      stop();
   }

   @Test
   public void createAndGet() throws TaskManagerException {
      PostgresqlTaskManager ptm = new PostgresqlTaskManager(createNonPoolingDataSource());
      ptm.initialize();
      Duration bucket_interval = Duration.ofMinutes(5);
      Instant bucket_time = TimeUtils.alignWithInterval(Instant.now(), Instant.EPOCH, bucket_interval);
      String taskName = "createAndGet";
      Task task1 = ptm.createTask(taskName, bucket_time, PeriodDuration.of(bucket_interval), taskName);
      System.out.println("wrote: " + task1);
      task1.acquire("createAndGetTest");
      Assert.assertTrue(task1.isAcquired());
      Assert.assertNotNull(task1.getAcquiredBy(), "acquired by");
      Assert.assertNotNull(task1.getAcquiredAt(), "acquired at");
      task1.completed(null);

      Task task1_fetched = ptm.getTask("createAndGet", bucket_time);
      System.out.println("read:  " + task1_fetched);
   }

   @Test
   public void createAndFail() throws TaskManagerException {
      PostgresqlTaskManager ptm = new PostgresqlTaskManager(createNonPoolingDataSource());
      ptm.initialize();
      Duration bucket_interval = Duration.ofMinutes(5);
      Instant bucket_time = TimeUtils.alignWithInterval(Instant.now(), Instant.EPOCH, bucket_interval);
      String taskName = "createAndFail";
      Task task1 = ptm.createTask(taskName, bucket_time, PeriodDuration.of(bucket_interval), taskName);
      task1.acquire("createAndFailTest");
      Assert.assertEquals(task1.getFailCount(), 0);
      task1.failed("failed manually by test");
      Assert.assertEquals(task1.getFailCount(), 1);
      try {
         task1.failed("again");
         Assert.fail("was able to fail twice");
      } catch (IllegalStateException e) {
         // do nothing, we should be in this state as the lock is not acquired
      }

      task1.acquire("second try");
      task1.failed("second fail");
      Assert.assertEquals(task1.getFailCount(), 2);
   }

   @Test
   public void acquireViaQuery() throws DuplicateTaskException, TaskManagerException {
      final int numTasks = 1;
      final String taskName = "acquiredViaQuery";
      PostgresqlTaskManager ptm = new PostgresqlTaskManager(createNonPoolingDataSource());
      ptm.initialize();
      Duration bucket_interval = Duration.ofMinutes(5);
      Instant bucket_time = TimeUtils.alignWithInterval(Instant.now(), Instant.EPOCH, bucket_interval);
      List<Task> createdTasks = new ArrayList<>(numTasks);
      for (int i=0; i < numTasks; ++i) {
         createdTasks.add(ptm.createTask(taskName, bucket_time.plus(bucket_interval.multipliedBy(i)), PeriodDuration.of(bucket_interval), taskName));
      }
      Assert.assertEquals(createdTasks.size(), numTasks, "created task count");

      Task acquiredViaQuery = null;
      int acquiredCount = 0;
      final TaskQuery query = TaskQuery.builder()
              .name(taskName)
              .bucketStartTime(bucket_time.minusSeconds(1))
              .statuses(ImmutableSet.of(TaskStatus.AVAILABLE))
              .build();
      while ((acquiredViaQuery = ptm.getAndAcquireFirstTask(query)) != null) {
         Assert.assertTrue(acquiredViaQuery.isAcquired());
         acquiredViaQuery.completed("finished");
         ++acquiredCount;
      }
      Assert.assertEquals(acquiredCount, numTasks, "acquired task count");
   }

   @Test
   public void createAndSkip() throws DuplicateTaskException, TaskManagerException {
      PostgresqlTaskManager ptm = new PostgresqlTaskManager(createNonPoolingDataSource());
      ptm.initialize();
      Duration bucket_interval = Duration.ofMinutes(5);
      Instant bucket_time = TimeUtils.alignWithInterval(Instant.now(), Instant.EPOCH, bucket_interval);
      final String taskName = "skipMe";
      Task skipTask = ptm.createTask(taskName, bucket_time, PeriodDuration.of(bucket_interval), "createAndSkip");
      try {
         skipTask.skip("skipping for test");
         Assert.fail("didn't get exception");
      } catch (IllegalStateException e) {
         // do nothing
      }
      skipTask.acquire("createAndSkipTest");
      try {
         skipTask.acquire("again");
         Assert.fail("didn't fail on double acquire");
      } catch (IllegalStateException e) {
         // do nothing
      }
      skipTask.skip("skipping for test");
   }

   @Test
   public void doubleCreate() throws DuplicateTaskException, TaskManagerException {
      PostgresqlTaskManager ptm = new PostgresqlTaskManager(createNonPoolingDataSource());
      ptm.initialize();
      Duration bucket_interval = Duration.ofMinutes(5);
      Instant bucket_time = TimeUtils.alignWithInterval(Instant.now(), Instant.EPOCH, bucket_interval);

      Task first = ptm.createTask("doubleCreate", bucket_time, PeriodDuration.of(bucket_interval), "doubleCreate");
      try {
         Task second = ptm.createTask("doubleCreate", bucket_time, PeriodDuration.of(bucket_interval), "doubleCreate");
         Assert.fail("made two");
      } catch (TaskManagerException e) {
         // do nothing
      }
      Task second = ptm.getTask("doubleCreate", bucket_time);
      second.acquire("second");
      try {
         first.acquire("first");
         Assert.fail("acquired twice");
      } catch (Exception e) {
         e.printStackTrace();
      }
   }

   @Test
   public void multiAssignStatus() throws TaskManagerException {
      PostgresqlTaskManager ptm = new PostgresqlTaskManager(createNonPoolingDataSource());
      ptm.initialize();
      Duration bucket_interval = Duration.ofMinutes(5);
      Instant bucket_time = TimeUtils.alignWithInterval(Instant.now(), Instant.EPOCH, bucket_interval);
      int bucket_count = 10;
      // create 10 tasks
      final String taskName = "multiAssignStatus";
      final List<Task> createdTasks = TaskManagerUtils.createTasksInTimeRange(ptm, taskName,  bucket_time, PeriodDuration.of(bucket_interval), bucket_count, taskName+"_create");
      Assert.assertEquals(createdTasks.size(), bucket_count);
      // verify all have status created
      for (Task task : createdTasks) {
         Assert.assertEquals(task.getStatus(), TaskStatus.AVAILABLE);
      }

      // change all to status running
      ptm.setTaskStatus(ImmutableSet.copyOf(createdTasks), TaskStatus.ACQUIRED, taskName + "_test");
      final List<Task> runningTasks = ptm.getTasks(TaskQuery.builder().name(taskName).build());
      // verify all have status running
      for (Task task : runningTasks) {
         Assert.assertEquals(task.getStatus(), TaskStatus.ACQUIRED);
      }
      Task randomTask = runningTasks.get(ThreadLocalRandom.current().nextInt(bucket_count));
      Assert.assertFalse(randomTask.isAcquired());
      randomTask.acquire(taskName + "_randomTask");

      // attempt to mark everything failed
      try {
         ptm.setTaskStatus(ImmutableSet.copyOf(createdTasks), TaskStatus.ACQUIRED, taskName + "_acqAll");
         Assert.fail("we shouldn't have been able to acquire the lock");
      } catch (TaskManagerException e) {
         // do nothing
      }

      randomTask.completed("done");
      ptm.setTaskStatus(ImmutableSet.copyOf(createdTasks), TaskStatus.SKIP, taskName + "_skipAll");

      final List<Task> skippedTasks = ptm.getTasks(TaskQuery.builder().name(taskName).build());
      for (Task task : skippedTasks) {
         Assert.assertEquals(task.getStatus(), TaskStatus.SKIP);
      }
   }

   @Test
   public void parallelCreate() throws TaskManagerException {
      // create N threads
      // each thread will attempt to M items

      final int numThreads = 8;
      final int numTasks = 100;
      final String taskName = "parallelCreate";
      PostgresqlTaskManager ptm = new PostgresqlTaskManager(createNonPoolingDataSource());
      ptm.initialize();
      Duration bucket_interval = Duration.ofMinutes(5);
      Instant bucket_time = TimeUtils.alignWithInterval(Instant.now(), Instant.EPOCH, bucket_interval);
      final ExecutorService executorService = Executors.newFixedThreadPool(numThreads);
      {
         final List<Future<List<Task>>> creationFutures = new ArrayList<>();
         for (int i = 0; i < numThreads; ++i) {
            TaskCreationWorker worker = new TaskCreationWorker(ptm, numTasks, taskName, bucket_time, PeriodDuration.of(bucket_interval));
            creationFutures.add(executorService.submit(worker));
         }

         final List<Task> createdTasks = new ArrayList<>();
         for (int i = 0; i < numThreads; ++i) {
            try {
               createdTasks.addAll(creationFutures.get(i).get());
            } catch (InterruptedException | ExecutionException e) {
               e.printStackTrace();
            }
         }

         Assert.assertEquals(createdTasks.size(), numTasks, "created task count via future");
         Assert.assertEquals(ptm.getTasks(TaskQuery.builder().name(taskName).statuses(ImmutableSet.of(TaskStatus.AVAILABLE)).build()).size(), numTasks, "created task count via query");
         Assert.assertEquals(ptm.getTasks(TaskQuery.builder().name(taskName).statuses(ImmutableSet.of(TaskStatus.COMPLETE)).build()).size(), 0, "no created tasks are completed");
         Assert.assertEquals(ptm.getTasks(TaskQuery.builder().name(taskName).statuses(ImmutableSet.of(TaskStatus.SKIP)).build()).size(), 0, "no created tasks are skipped");
         Assert.assertEquals(ptm.getTasks(TaskQuery.builder().name(taskName).statuses(ImmutableSet.of(TaskStatus.ACQUIRED)).build()).size(), 0, "no created tasks are running");
      }

      {
         final List<Future<List<Task>>> completedFutures = new ArrayList<>();
         for (int i = 0; i < numThreads; ++i) {
            TaskCompletionWorker worker = new TaskCompletionWorker(ptm, numTasks, taskName, bucket_time, bucket_interval);
            completedFutures.add(executorService.submit(worker));
         }

         final List<Task> completedTasks = new ArrayList<>();
         for (int i = 0; i < numThreads; ++i) {
            try {
               completedTasks.addAll(completedFutures.get(i).get());
            } catch (InterruptedException | ExecutionException e) {
               e.printStackTrace();
            }
         }

         Assert.assertEquals(completedTasks.size(), numTasks, "completed task count via future");
         Assert.assertEquals(ptm.getTasks(TaskQuery.builder().name(taskName).statuses(ImmutableSet.of(TaskStatus.AVAILABLE)).build()).size(), 0, "created task count via query");
         Assert.assertEquals(ptm.getTasks(TaskQuery.builder().name(taskName).statuses(ImmutableSet.of(TaskStatus.COMPLETE)).build()).size(), numTasks, "no created tasks are completed");
         Assert.assertEquals(ptm.getTasks(TaskQuery.builder().name(taskName).statuses(ImmutableSet.of(TaskStatus.SKIP)).build()).size(), 0, "no created tasks are skipped");
         Assert.assertEquals(ptm.getTasks(TaskQuery.builder().name(taskName).statuses(ImmutableSet.of(TaskStatus.ACQUIRED)).build()).size(), 0, "no created tasks are running");
      }
   }

   private static class TaskCreationWorker implements Callable<List<Task>> {
      private final TaskManager taskManager;
      private final int numTasks;
      private final String taskName;
      private final Instant bucket_time;
      private final PeriodDuration bucket_interval;

      public TaskCreationWorker(TaskManager taskManager, int numTasks, String taskName, Instant bucket_time, PeriodDuration bucket_interval) {
         this.taskManager = taskManager;
         this.numTasks = numTasks;
         this.taskName = taskName;
         this.bucket_time = bucket_time;
         this.bucket_interval = bucket_interval;
      }

      @Override
      public List<Task> call() throws Exception {
         List<Task> createdTasks = new ArrayList<>(numTasks);
         for (int i=0; i < numTasks; ++i) {
            try {
               createdTasks.add(taskManager.createTask(taskName, bucket_time.plus(bucket_interval.multipliedBy(i)), bucket_interval, "taskCreationWorker"));
            } catch (Exception e) {
               // do nothing
            }
         }
         return createdTasks;
      }
   }

   private static class TaskCompletionWorker implements Callable<List<Task>> {
      private final TaskManager taskManager;
      private final int numTasks;
      private final String taskName;
      private final Instant bucket_time;
      private final Duration bucket_interval;

      public TaskCompletionWorker(TaskManager taskManager, int numTasks, String taskName, Instant bucket_time, Duration bucket_interval) {
         this.taskManager = taskManager;
         this.numTasks = numTasks;
         this.taskName = taskName;
         this.bucket_time = bucket_time;
         this.bucket_interval = bucket_interval;
      }

      @Override
      public List<Task> call() throws Exception {
         List<Task> completedTasks = new ArrayList<>(numTasks);
         for (int i=0; i < numTasks; ++i) {
            try {
               Task acquiredTask = taskManager.getAndAcquireFirstTask(TaskQuery.builder()
                       .name(taskName)
                       .statuses(ImmutableSet.of(TaskStatus.AVAILABLE)).build());
               if (ThreadLocalRandom.current().nextBoolean()) {
                  acquiredTask.failed("failed");
               } else {
                  acquiredTask.completed("completed");
                  completedTasks.add(acquiredTask);
               }
            } catch (Exception e) {
               // do nothing
            }
         }
         return completedTasks;
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

}
