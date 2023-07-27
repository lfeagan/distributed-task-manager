package net.vectorcomputing.dtm;

import org.threeten.extra.PeriodDuration;

import java.time.Instant;
import java.util.List;
import java.util.Set;

public interface TaskManager {

    /**
     * Creates a task, but does not acquire a lock.
     *
     * If concurrent writers attempt to create a task with the same key (name+bucketTime),
     * only one will succeed and subsequent writers will receive a unique constraint violation (PostgreSQL error 23505).
     * @param name
     * @param bucketTime
     * @param bucketInterval
     * @return
     */
    Task createTask(String name, Instant bucketTime, PeriodDuration bucketInterval, String createdBy);

    /**
     * Searches the table of tasks and returns the first task that satisfies the query conditions
     * while also placing an exclusive lock on the row.
     * @param taskQuery
     * @return
     */
    Task getAndAcquireFirstTask(TaskQuery taskQuery);

    Task getTask(String name, Instant bucketTime);

    List<Task> getTasks(TaskQuery taskQuery);

    /**
     * Atomically assign the specified task status to a set of tasks.
     * @param tasks
     * @param updatedStatus
     * @param acquiredBy the name to use when identifying the lock acquirer for the update
     */
    void setTaskStatus(Set<Task> tasks, TaskStatus updatedStatus, String acquiredBy);


//    /**
//     * Marks the specified task status as RUNNING.
//     * @param task
//     * @return
//     */
//    Task acquire(Task task);
//
//    /**
//     * Marks the specified task status as COMPLETE.
//     * @param task
//     * @return
//     */
//    Task complete(Task task);
//
//    /**
//     * Marks the specified task as status FAILED and, if present, sets the message.
//     * @param task
//     */
//    void exception(Task task);


}
