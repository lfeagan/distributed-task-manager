package com.github.lfeagan.dtc;

import org.threeten.extra.PeriodDuration;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

public final class TaskManagerUtils {

    /**
     * Attempts to create tasks with the specified name over the time range from bucket_time to bucket_time + bucket_count * bucket_interval.
     * Returns a list of the tasks that were actually created.
     *
     * @param taskManager the task manager to use when creating the tasks
     * @param taskName the name of the task
     * @param bucket_time the bucket time of the first task
     * @param bucket_interval the time interval between buckets
     * @param bucket_count the number of tasks to create
     * @param createdBy the value to place in the tasks's created by column
     * @return a list of the tasks that were actually created
     */
    public static List<Task> createTasksInTimeRange(TaskManager taskManager, String taskName, Instant bucket_time, PeriodDuration bucket_interval, int bucket_count, String createdBy) throws TaskManagerException {
        List<Task> createdTasks = new ArrayList<>(bucket_count);
        for (int i=0; i < bucket_count; ++i) {
            try {
                Task createdTask = taskManager.createTask(taskName, bucket_time.plus(bucket_interval.multipliedBy(i)), bucket_interval, createdBy);
                createdTasks.add(createdTask);
            } catch (DuplicateTaskException e) {
                // do nothing
            }
        }
        return createdTasks;
    }
}
