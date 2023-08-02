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
     * An identifier used to help track who created this task.
     * @return
     */
    String getCreatedBy();

    /**
     * The time this task was created.
     * @return
     */
    Instant getCreatedAt();

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

    /**
     * Attempts to acquire this task, and sets the acquried_by column.
     * @param acquiredBy
     */
    void acquire(String acquiredBy);

    /**
     * Returns <code>true</code> if this task has acquired the database lock on the associated row.
     * @return
     */
    boolean isAcquired();

    /**
     * Sets the task status to COMPLETED.
     * Can include an optional message.
     * @param message
     */
    void completed(String message);

    Instant getCompletedAt();

    /**
     * Sets the task status to AVAILABLE and increment the fail count.
     * Can include an optional message.
     * @param message
     */
    void failed(String message);

    int getFailCount();

    /**
     * Sets the task status to SKIP.
     * Can include an optional message.
     * @param message
     */
    void skip(String message);

}
