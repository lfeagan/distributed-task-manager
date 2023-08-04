# DTC – A library for coordinating distributed task execution

DTC helps distributed, asynchronous task execution workers to coordinate their activities to ensure that the processing of a given time range occurs exactly once. While most tools, such as [cron](https://kubernetes.io/docs/concepts/workloads/controllers/cron-jobs/), [Quartz](http://www.quartz-scheduler.org/), and [DB-Scheduler](https://github.com/kagkarlsson/db-scheduler) are oriented around executing tasks on a fixed schedule, DTC is a hybrid between a work queue and an engine trying to satisfy the constraint of fully covering a time range. In DTC, a task is uniquely identified by a name and a time slot (timestamp+interval). Users query the table for tasks satisfying a set of constraints, such as a status and a time range, and acquire a lock on the first satisfactory task. If no task is available, then the worker can simply exit or go back to sleep. After acquiring a task, a worker will inspect the time range specified and process the appropriate data. If successful, the task will be marked as complete and the transaction will be committed.

DTC operates purely as a library helping developers to find available work needing processing. DTC does not run a task nor does it define a schedule for task execution, you still need to do that externally. DTC's only external dependency is PostgreSQL >= 9.5 (for commands that require `SELECT ... FOR UPDATE SKIP LOCKED`).

## Overly Simplified Example
To understand how DTC works, think of a calendar with numbered days. Every day, we need to call our mother. Sometimes we might succeed the first time we try, other times we may need to try again later. Using DTC, we might identify this task as `call_mom` with an interval of `P1D` and a time of `00:00:00Z`. Every time we attempt to run this task, we will attempt to acquire a lock on the row uniquely identifying today's entry, if it is still marked as `AVAILABLE`. If we succeed, we will commit the transaction with the status set to `COMPLETE` and if we don't, we will commit with an incremented `fail_count`. If the task encounters an exception, the transaction will simply be rolled back to the status `AVAILABLE` that it was created with and can immediately be discovered by another task worker for execution.

### Call Mom
| Day | Status    |
|-----|-----------|
| 1   | COMPLETE  |
| 2   | COMPLETE  |
| 3   | ACQUIRED  |
| 4   | SKIP      |
| 4   | AVAILABLE |

## Documentation
- [Usage](docs/USAGE.md)
- [Design](docs/DESIGN.md)

## Download
Latest version: 0.1.0

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
  implementation 'com.github.lfeagan:distributed-task-manager:0.1.0'
}
```





