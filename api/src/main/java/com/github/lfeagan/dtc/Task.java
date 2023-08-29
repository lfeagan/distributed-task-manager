package com.github.lfeagan.dtc;

import org.threeten.extra.PeriodDuration;

import java.io.Closeable;
import java.time.Instant;

public interface Task extends Closeable {

    /**
     * The name of this task.
     * @return this tasks's name
     */
    String getName();

    /**
     * The time bucket this task is responsible for processing.
     * @return this task's time bucket
     */
    Instant getBucketTime();

    /**
     * The time interval this task is responsible for processing.
     * @return this task's time bucket interval
     */
    PeriodDuration getBucketInterval();

    /**
     * An identifier used to help track who created this task.
     * @return the creator of this task
     */
    String getCreatedBy();

    /**
     * The time this task was created.
     * @return the time this task was created
     */
    Instant getCreatedAt();

    /**
     * An identifier used to help track who acquired this task.
     * @return the acquirer of this task
     */
    String getAcquiredBy();

    /**
     * The time this task was acquired.
     * @return the acquisition time
     */
    Instant getAcquiredAt();

    /**
     * This task's status.
     * @return the status
     */
    TaskStatus getStatus();

    /**
     * Attempts to acquire this task, and sets the acquired_by column.
     * @param acquiredBy who is doing the acquiring
     */
    void acquire(String acquiredBy);

    /**
     * Returns <code>true</code> if this task has acquired the database lock on the associated row.
     * @return has the caller acquired this task
     */
    boolean isAcquired();

    /**
     * Sets the task status to COMPLETED.
     * Can include an optional message.
     * @param message optional info about the completion of this task
     */
    void completed(String message);

    Instant getCompletedAt();

    /**
     * Sets the task status to AVAILABLE and increment the fail count.
     * Can include an optional message.
     * @param message optional info about the failure of this task
     */
    void failed(String message);

    int getFailCount();

    /**
     * Sets the task status to SKIP.
     * Can include an optional message.
     * @param message optional info about skipping this task
     */
    void skip(String message);

}
