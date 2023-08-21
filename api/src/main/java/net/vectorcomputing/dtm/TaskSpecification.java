package net.vectorcomputing.dtm;

import com.github.lfeagan.wheat.time.TimeUtils;
import com.google.common.collect.ImmutableSet;
import lombok.Builder;
import lombok.Data;
import lombok.NonNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.threeten.extra.PeriodDuration;

import java.time.Duration;
import java.time.Instant;
import java.util.Objects;
import java.util.function.BiFunction;
import java.util.function.Function;

@Data
@Builder
public class TaskSpecification {

    private static final Logger LOGGER = LoggerFactory.getLogger(TaskSpecification.class);

    @NonNull
    private final String workerName;
    @NonNull
    private final TaskManager taskManager;
    @NonNull
    private final String taskName;
    @NonNull
    private final Duration bucketInterval;
    @NonNull
    private final Duration backlogWindowSize;

    public Task findOrCreateAndAcquire() throws TaskManagerException {
        Instant bucket_time = TimeUtils.alignWithInterval(Instant.now(), Instant.EPOCH, bucketInterval);
        final TaskQuery availableWorkQuery = TaskQuery.builder()
                .name(taskName)
                .bucketStartTime(bucket_time.minus(backlogWindowSize))
                .statuses(ImmutableSet.of(TaskStatus.AVAILABLE))
                .build();

        Task acquiredTask = taskManager.getAndAcquireFirstTask(availableWorkQuery);
        if (acquiredTask == null) { // backlog query returned nothing
            // try to create for current bucket
            try {
                Task createdTask = taskManager.createTask(taskName, bucket_time, PeriodDuration.of(bucketInterval), workerName);
                if (createdTask != null) {
                    LOGGER.info("Worker {} created task {}", workerName, createdTask);
                    createdTask.acquire(workerName);
                    acquiredTask = createdTask;
                    LOGGER.info("Worker {} acquired task {}", workerName, acquiredTask);
                }
            } catch (Exception e) {
                // do nothing
            }
        } else {
            LOGGER.info("Worker {} acquired task {} via query", workerName, acquiredTask);
        }
        return acquiredTask;
    }

    public void process(final Task acquiredTask, Function<Task,String> successFunction, BiFunction<Task,Exception,String> failureFunction) {
        Objects.requireNonNull(acquiredTask, "must specify acquired task");
        // successfully acquired via backlog query or create-acquire sequence
        if (!acquiredTask.isAcquired()) {
            throw new IllegalArgumentException("Task is not acquired");
        }

        try {
            LOGGER.info("Worker {} processing acquired task: {}", workerName, acquiredTask);
            String message = successFunction.apply(acquiredTask);
            acquiredTask.completed(message);
            LOGGER.info("Worker {} finished task {}", workerName, acquiredTask);
        } catch (Exception e) {
            String message = failureFunction.apply(acquiredTask, e);
            LOGGER.error(message, e);
            acquiredTask.failed(message);
        }
    }

}
