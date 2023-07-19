package net.vectorcomputing.dtm;

import org.threeten.extra.PeriodDuration;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

public final class TaskManagerUtils {

    public static List<Task> createTasksInTimeRange(TaskManager taskManager, String taskName, Instant bucket_time, PeriodDuration bucket_interval, int bucket_count) {
        List<Task> createdTasks = new ArrayList<>(bucket_count);
        for (int i=0; i < bucket_count; ++i) {
            try {
                createdTasks.add(taskManager.createTask(taskName, bucket_time.plus(bucket_interval.multipliedBy(i)), bucket_interval));
            } catch (Exception e) {
                // do nothing
            }
        }
        return createdTasks;
    }
}
