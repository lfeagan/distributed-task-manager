# DTM – A Distributed Task Manager, Done Right

DTM enables distributed, asynchronous task execution engines to ensure that a task is executed exactly once for a given time slot. The scenario that inspired this work was the need to query a potentially unreliable database and produce a report counting the number of values present for a time range with various groupings of the data to be consumed by other systems to trigger alerts and initiate reprocessing from archives to fill the gaps.

## High-Level Requirements
### Functional
1. Tasks are uniquely identified by a name and time. In practice, each application performing a task, such as computing the record count, will use the same task name every time it executes.
2. Tasks 

### Non-Functional
1. Tasks are completed exactly once. Ideally the same DB will also be used as the queue for the next stage of processing.
2. Designed for minimal maintenance and overhead.
3. Operates purely as a library–no server required.
4. No specialized distributed locking library required.
5. No defects related to 

1. Tasks are in one of five states:
    1. CREATED
    2. RUNNING
    3. COMPLETED
    4. FAILED
    5. SKIP

1. Use transactions
2. Use `SELECT FOR UPDATE NOWAIT` to acquire a lock on a row or have an error raised, avoiding race conditions. https://www.2ndquadrant.com/en/blog/what-is-select-skip-locked-for-in-postgresql-9-5/
3. Hold open transaction/connection for duration of work to ensure failures release the lock as a result of the TCP disconnect. https://martin.kleppmann.com/2016/02/08/how-to-do-distributed-locking.html



