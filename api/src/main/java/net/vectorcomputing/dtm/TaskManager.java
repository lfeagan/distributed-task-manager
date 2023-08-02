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
     * @throws DuplicateTaskException
     */
    Task createTask(String name, Instant bucketTime, PeriodDuration bucketInterval, String createdBy) throws DuplicateTaskException, TaskManagerException;

    /**
     * Searches the table of tasks and returns the first task that satisfies the query conditions
     * while also placing an exclusive lock on the row.
     * @param taskQuery
     * @return
     */
    Task getAndAcquireFirstTask(TaskQuery taskQuery) throws TaskManagerException;

    /**
     * Fetches the task (without acquiring it) with the specified name and bucket time.
     * In SQL terms, this method searches committed rows.
     * @param name
     * @param bucketTime
     * @return
     */
    Task getTask(String name, Instant bucketTime) throws TaskManagerException;

    /**
     * Fetches the task(s) that satisfy the specified task query (without acquiring any of them).
     * In SQL terms, this method searches committed rows.
     * @param taskQuery
     * @return
     */
    List<Task> getTasks(TaskQuery taskQuery) throws TaskManagerException;

    /**
     * Atomically assign the specified task status to a set of tasks.
     * @param tasks
     * @param updatedStatus
     * @param acquiredBy the name to use when identifying the lock acquirer for the update
     */
    void setTaskStatus(Set<Task> tasks, TaskStatus updatedStatus, String acquiredBy) throws TaskManagerException;


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
