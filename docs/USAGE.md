# Simplified Usage


            TaskSpecification taskSpec = TaskSpecification.builder()
                    .taskManager(taskManager)
                    .taskName("simulation_task")
                    .workerName("simulation_worker")
                    .bucketInterval(bucket_interval)
                    .backlogWindowSize(Duration.ofHours(72))
                    .build();

            try {
                Task acquiredTask = taskSpec.findOrCreateAndAcquire();

                if (acquiredTask != null) {
                    taskSpec.process(acquiredTask,
                            (t) -> {
                                acquiredCount.getAndIncrement();
                                try {
                                    Thread.sleep(ThreadLocalRandom.current().nextInt(100));
                                } catch (InterruptedException e) {
                                    throw new RuntimeException(e);
                                }
                                return "finished"; },
                            (t,e) -> { return e.getMessage();});
                } else {
                    LOGGER.info("unable to acquire task");
                }


# Advanced Usage
The high-level steps to use DTC are:
1. Decide on the name of the task. This name should be unique and help to identify the type of work being performed. For example, if the task is generating a report describing the health of sensors, a good name might be "sensor_health". If there were two sensor health jobs, one on a daily basis and another on an hourly basis, you may want to name them "sensor_health_daily" and "sensor_health_hourly", respectively.
2. Decide on the size of the time window you want to contiguously fill the timeline with.
3. Create an appropriate find-acquire, create-acquire, execute, commit function, as described below.

As mentioned previously, DTC does not run your task. DTC helps your code determine what has been processed and what remains. Throughout the remained of this document, I will describe the combination of a start time and a time interval as a bucket. For example, if we are processing hourly chunks and the current time is 02:34:56Z, I would describe this as the 2 AM bucket. Buckets can be aligned to an interval other than 00:00:00, but for discussion purposes, hour-aligned buckets are easier to reason about. 

## Determine Current Bucket Time
The first step is to determine the current bucket time:
```java
Duration bucket_interval = Duration.ofHours(1);
Instant bucket_time = TimeUtils.alignWithInterval(Instant.now(), Instant.EPOCH, bucket_interval);
```

The TimeUtils method `alignWithInterval` acts like a floor function in a finite field of timestamps modulo the bucket interval, shifting the timestamp backwards in time to the nearest interval offset by an integral multiple from the origin, which is specified as `Instant.EPOCH` in the above example.

Often times you will want to avoid processing the current bucket, as the data to analyze is still being actively inserted. To offset the bucket to refer to the previous bucket, just subtract the bucket_interval from the bucket_time, as shown below:

```java
Duration bucket_interval = Duration.ofHours(1);
Instant bucket_time = TimeUtils.alignWithInterval(Instant.now(), Instant.EPOCH, bucket_interval).minus(bucket_interval);
```

If you want to go back multiple intervals, you can multiply the interval:
```java
Duration bucket_interval = Duration.ofHours(1);
Instant bucket_time = TimeUtils.alignWithInterval(Instant.now(), Instant.EPOCH, bucket_interval).minus(bucket_interval.multipliedBy(3));
```

## Find Available Tasks
The next step is to search the backlog to determine if there are any available tasks. These could be tasks that have been run in the past and failed or that simply haven't been run yet. The first step here is specifying a query to find tasks with a matching task name, that have status available, and that have a bucket time within the last 72 hours (3 days). There is no ideal time threshold to search for, and in fact this parameter need not be specified. But generally speaking, as with any soft real-time system, there comes a point in time at which the diminishing value of processing old data makes it no longer worthwhile.
```java
final String taskName = "usage_example";
final TaskQuery availableWorkQuery = TaskQuery.builder()
    .name(taskName)
    .bucketStartTime(bucket_time.minus(72, ChronoUnit.HOURS)) // 3 day search window
    .statuses(ImmutableSet.of(TaskStatus.AVAILABLE))
    .build();
```

Now we search for the first available task using the available work query. If nothing is returned, then we should see about creating and acquiring a task for the current bucket. The key thing to observe here is that creating and acquiring are independent operations. We might succeed in creating a task, but another worker might run a query and acquire that sees this newly created task and acquires it. It doesn't really matter if this worker creates it and another acquires it, the work will get done.
```java
Task acquiredTask = taskManager.getAndAcquireFirstTask(availableWorkQuery);
if (acquiredTask == null) { // backlog query returned nothing
    // try to create for current bucket
    Task createdTask = taskManager.createTask(taskName, bucket_time, PeriodDuration.of(bucket_interval), "example_worker");
    if (createdTask != null) {
        LOGGER.info("created task {}", createdTask);
        createdTask.acquire("example_worker");
        acquiredTask = createdTask;
        LOGGER.info("acquired task {}", acquiredTask);
    }
}
```

## Execute and Commit Task
At this point in the code, one of three things has happened:
1. An available task was acquired through a backlog query, or
2. A new task was created and acquired, or
3. No task has been acquired.
In scenario 3, we simply move on. Depending on your worker, this might mean you sleep for a minute and check again, or it might mean the worker should exit and will be run again at a later time. For scenarios 1 and 2, it is time to get to work. After completing whatever work is required, the acquired task should be marked as complete, along with an optional message. If the task fails, we should catch the exception and fail the acquired task.
```java
    if (acquiredTask != null && acquiredTask.isAcquired()) {
        // successfully acquired via backlog query or create-acquire sequence
        try {
            LOGGER.info("processing acquired task: {}", acquiredTask);
            // do real work here based on the time specified
            acquiredTask.completed("finished");
            LOGGER.info("finished task w/bucket time {} at {} ", acquiredTask.getBucketTime(), acquiredTask.getCompletedAt());
        } catch (Exception e) {
            LOGGER.error("something bad happened", e);
            acquiredTask.failed(e.getMessage());
            throw e;
        }
    } else {
        LOGGER.info("unable to acquire task");
    }
}
```
