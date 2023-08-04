# DTM – A Distributed Task Manager

DTM helps distributed, asynchronous task execution workers to coordinate their activities to ensure that the processing of a given time range occurs exactly once. While most tools, such as [cron](https://kubernetes.io/docs/concepts/workloads/controllers/cron-jobs/), [Quartz](http://www.quartz-scheduler.org/), and [DB-Scheduler](https://github.com/kagkarlsson/db-scheduler) are oriented around executing tasks on a fixed schedule, DTM is a hybrid between a work queue and an engine trying to satisfy the constraint of fully covering a time range. In DTM, a task is uniquely identified by a name and a time slot (timestamp+interval). Users query the table for tasks satisfying a set of constraints, such as a status and a time range, and acquire a lock on the first satisfactory task. If no task is available, then the worker can simply exit or go back to sleep. After acquiring a task, a worker will inspect the time range specified and process the appropriate data. If successful, the task will be marked as complete and the transaction will be committed.

DTM operates purely as a library helping developers to find available work needing processing. DTM does not run a task nor does it define a schedule for task execution, you still need to do that externally. DTM's only external dependency is PostgreSQL >= 9.5 (for commands that require `SELECT ... FOR UPDATE SKIP LOCKED`).

To understand how DTM works, think of a calendar with numbered days. Every day, we need to call our mother. Sometimes we might succeed the first time we try, other times we may need to try again later. Using DTM, we might identify this task as `call_mom` with an interval of `P1D` and a time of `00:00:00Z`. Every time we attempt to run this task, we will attempt to acquire a lock on the row uniquely identifying today's entry, if it is still marked as `AVAILABLE`. If we succeed, we will commit the transaction with the status set to `COMPLETE` and if we don't, we will commit with an incremented `fail_count`. If the task encounters an exception, the transaction will simply be rolled back to the status `AVAILABLE` that it was created with and can immediately be discovered by another task worker for execution.

#### Call Mom
| Day | Status    |
|-----|-----------|
| 1   | COMPLETE  |
| 2   | COMPLETE  |
| 3   | ACQUIRED  |
| 4   | SKIP      |
| 4   | AVAILABLE |

## Example
For example, we might have a task to produce a report every five minutes that described the number of sensor records present in the five-minute interval that began six minutes ago. In a traditional cron type system, we would schedule this job to run at a fixed interval. Most times it would succeed, but sometimes it would fail. To deal with this, we could script some retry logic, but there are other challenges we need to deal with, such as scaling out. So, we might decide to purchase a more industrial solution, such as [JobRunr](https://www.jobrunr.io/en/) to address these issues. However, as we develop more advanced feedback systems, we sometimes need more than one axis of control. For example, a big batch of data may come in at a later time that needs to re-run of an already completed analysis. Any service can acquire a task at any time and update the status to `AVAILABLE` so that it will be re-run. In this regard, DTM is operating more like a task queue that any service can append to.

The scenario that inspired this work was the need to query a potentially unreliable database and produce a report counting the number of values present for a time range with various groupings of the data to be consumed by other systems to trigger alerts and initiate reprocessing from archives to fill the gaps. In addition to this one time job, it may be desirable to allow other services to adjust the status of a completed task and mark it as available so that it will be processed again, perhaps due to the arrival of a significant volume of new data, bringing the fidelity of the previous result into doubt.


```mermaid
---
title: Task Engine Menagerie
---
flowchart LR
    dtm[(Distributed Task Manager)]
    TaskWorker_1 --> |create|dtm
    TaskWorker_2 --> |find|dtm
    TaskWorker_3 --> |acquire|dtm
    TaskWorker_4 --> |complete|dtm
```

## High-Level Requirements
### Functional
1. Tasks are uniquely identified by a name and time. In practice, each application performing a task, such as computing the record count, will use the same task name every time it executes.
2. Tasks 

### Non-Functional
1. Tasks are completed exactly once. Ideally the same DB will also be used as the queue for the next stage of processing.
2. Designed for minimal maintenance and overhead.
3. Operates purely as a library–no server required.
4. No specialized distributed locking library required.
5. Task lock(s) automatically release on unexpected task failure.

1. Tasks are in one of four states:
    1. AVAILABLE
    2. ACQUIRED
    3. COMPLETE
    4. SKIP

When tasks are initially created, they are in the state available. After being acquired, a task that has failed will return to the available state, but may include a message indicating the reason for the failure. 

### State Transition Diagram
```mermaid
stateDiagram-v2
    [*] --> AVAILABLE
    AVAILABLE --> ACQUIRED : ACQUIRE
    ACQUIRED --> COMPLETE : FINISH
    ACQUIRED --> AVAILABLE : FAIL
    ACQUIRED --> SKIP : SKIP
    COMPLETE --> [*] : COMMIT
    SKIP --> [*] : COMMIT
```

1. Use transactions
2. Use `SELECT FOR UPDATE NOWAIT` to acquire a lock on a row or have an error raised, avoiding race conditions. https://www.2ndquadrant.com/en/blog/what-is-select-skip-locked-for-in-postgresql-9-5/
3. Hold open transaction/connection for duration of work to ensure failures release the lock as a result of the TCP disconnect. https://martin.kleppmann.com/2016/02/08/how-to-do-distributed-locking.html


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





