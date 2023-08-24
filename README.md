# DTC – A library for coordinating distributed task execution

## Motivation
The inspiration for DTC was the need to produce a report on the information present in a time series database for a given time range. In a traditional cron type system, we would schedule this job to run at a fixed interval, likely using cron. Most times this task would succeed, but sometimes it might fail due to database or network maintenance events. To deal with this, we could script additional retry logic, but there are other challenges we need to deal with, such as scaling out. So, we might decide to purchase a more industrial solution, such as [JobRunr](https://www.jobrunr.io/en/) to address these issues. However, as we develop more advanced feedback systems, we sometimes need more than one axis of control. For example, a big batch of data may come in at a later time that needs to re-run an already completed analysis. Much like a task queue, we want any service to be able to request the re-processing of a time interval.

## Introduction
DTC helps distributed, asynchronous task execution workers to coordinate their activities to ensure that all time ranges are processed and each time range is processed exactly once. While most tools, such as [cron](https://kubernetes.io/docs/concepts/workloads/controllers/cron-jobs/), [Quartz](http://www.quartz-scheduler.org/), and [DB-Scheduler](https://github.com/kagkarlsson/db-scheduler) are oriented around executing tasks on a fixed schedule, DTC is a hybrid between a work queue and an engine trying to satisfy the constraint of fully covering a time range. In DTC, a task is uniquely identified by a name and a time slot (timestamp+interval). Users query the table for tasks satisfying a set of constraints, such as a status and a time range, and acquire a lock on the first satisfactory task. If no task is available, then the worker can simply exit or go back to sleep. After acquiring a task, a worker will inspect the time range specified and process the appropriate data. If successful, the task will be marked as complete and the transaction will be committed.

DTC operates purely as a library helping developers to find available work needing processing. DTC does not run a task nor does it define a schedule for task execution, you still need to do that externally. DTC's only external dependency is PostgreSQL >= 9.5 (for commands that require `SELECT ... FOR UPDATE SKIP LOCKED`).

## Documentation
- [Usage](docs/USAGE.md)
- [Design](docs/DESIGN.md)

## Download
Latest version: 0.1.1

You can add this library into your Maven/Gradle/SBT/Leiningen project thanks to JitPack.io. Follow the instructions [here](https://jitpack.io/#lfeagan/distributed-task-manager).

### Example Gradle instructions

Add this into your build.gradle file:

```groovy
allprojects {
  repositories {
    maven { url 'https://jitpack.io' }
  }
}

dependencies {
  implementation 'com.github.lfeagan:distributed-task-coordinator:0.1.1'
}
```





