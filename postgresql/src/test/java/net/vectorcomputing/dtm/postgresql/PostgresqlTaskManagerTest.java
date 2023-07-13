package net.vectorcomputing.dtm.postgresql;

import net.vectorcomputing.dtm.Task;
import net.vectorcomputing.dtm.TaskQuery;
import net.vectorcomputing.dtm.TaskStatus;
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
   public void createAndGet() {
      PostgresqlTaskManager ptm = new PostgresqlTaskManager(createNonPoolingDataSource());
      ptm.initialize();
      PeriodDuration bucket_interval = PeriodDuration.of(Duration.ofMinutes(5));
      Instant bucket_time = TimeUtils.alignWithDuration(Instant.now(), bucket_interval);
      Task task1 = ptm.createTask("createAndGet", bucket_time, bucket_interval);
      System.out.println("wrote: " + task1);
      task1.acquire("createAndGetTest");
      Assert.assertTrue(task1.isAcquired());
      Assert.assertNotNull(task1.getAcquiredBy(), "acquired by");
      Assert.assertNotNull(task1.getAcquiredAt(), "acquired at");
      task1.complete(null);

      Task task1_fetched = ptm.getTask("createAndGet", bucket_time);
      System.out.println("read:  " + task1_fetched);
   }

   @Test
   public void acquireViaQuery() {
      final int numTasks = 1;
      final String taskName = "acquiredViaQuery";
      PostgresqlTaskManager ptm = new PostgresqlTaskManager(createNonPoolingDataSource());
      ptm.initialize();
      PeriodDuration bucket_interval = PeriodDuration.of(Duration.ofMinutes(5));
      Instant bucket_time = TimeUtils.alignWithDuration(Instant.now(), bucket_interval);
      List<Task> createdTasks = new ArrayList<>(numTasks);
      for (int i=0; i < numTasks; ++i) {
         createdTasks.add(ptm.createTask(taskName, bucket_time.plus(bucket_interval.multipliedBy(i)), bucket_interval));
      }
      Assert.assertEquals(createdTasks.size(), numTasks, "created task count");

      Task acquiredViaQuery = null;
      int acquiredCount = 0;
      final TaskQuery query = TaskQuery.builder()
              .name(taskName)
              .bucketStartTime(bucket_time.minusSeconds(1))
              .statuses(ImmutableSet.of(TaskStatus.CREATED, TaskStatus.FAILURE))
              .build();
      while ((acquiredViaQuery = ptm.getAndAcquireFirstTask(query)) != null) {
         Assert.assertTrue(acquiredViaQuery.isAcquired());
         acquiredViaQuery.complete("finished");
         ++acquiredCount;
      }
      Assert.assertEquals(acquiredCount, numTasks, "acquired task count");
   }

   @Test
   public void createAndSkip() {
      PostgresqlTaskManager ptm = new PostgresqlTaskManager(createNonPoolingDataSource());
      ptm.initialize();
      PeriodDuration bucket_interval = PeriodDuration.of(Duration.ofMinutes(5));
      Instant bucket_time = TimeUtils.alignWithDuration(Instant.now(), bucket_interval);
      Task skipTask = ptm.createTask("skipMe", bucket_time, bucket_interval);
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
   public void doubleCreate() {
      PostgresqlTaskManager ptm = new PostgresqlTaskManager(createNonPoolingDataSource());
      ptm.initialize();
      PeriodDuration bucket_interval = PeriodDuration.of(Duration.ofMinutes(5));
      Instant bucket_time = TimeUtils.alignWithDuration(Instant.now(), bucket_interval);

      Task first = ptm.createTask("doubleCreate", bucket_time, bucket_interval);
      try {
         Task second = ptm.createTask("doubleCreate", bucket_time, bucket_interval);
         Assert.fail("made two");
      } catch (RuntimeException e) {
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
