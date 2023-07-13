package net.vectorcomputing.dtm;

import org.threeten.extra.PeriodDuration;

import java.time.Instant;
import java.util.List;

public interface TaskManager {

    Task createTask(String name, Instant bucketTime, PeriodDuration bucketInterval);

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
//     * Marks the specified task as status EXCEPTION and, if present, sets the message.
//     * @param task
//     */
//    void exception(Task task);

    Task getAndAcquireFirstTask(TaskQuery taskQuery);

    Task getTask(String name, Instant bucketTime);

    public List<Task> getTasks(TaskQuery taskQuery);

}
