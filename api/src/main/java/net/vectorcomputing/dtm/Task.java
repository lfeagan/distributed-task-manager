package net.vectorcomputing.dtm;

import org.threeten.extra.PeriodDuration;

import java.io.Closeable;
import java.time.Instant;

public interface Task extends Closeable {

    /**
     * The name of this task.
     * @return
     */
    String getName();

    /**
     * The time bucket this task is responsible for processing.
     * @return
     */
    Instant getBucketTime();

    /**
     * The time interval this task is responsible for processing.
     * @return
     */
    PeriodDuration getBucketInterval();

    /**
     * An identifier used to help track who acquired this task.
     * @return
     */
    String getAcquiredBy();

    /**
     * The time this task was acquired.
     * @return
     */
    Instant getAcquiredAt();

    /**
     * This task's status.
     * @return
     */
    TaskStatus getStatus();

    void acquire(String acquiredBy);

    boolean isAcquired();

    /**
     * Sets the task status to complete.
     * Can include an optional message.
     * @param message
     */
    void complete(String message);

    /**
     * Sets the task status to failure.
     * Can include an optional message.
     * @param message
     */
    void failure(String message);

    /**
     * Sets the task status to SKIP.
     * Can include an optional message.
     * @param message
     */
    void skip(String message);

}
