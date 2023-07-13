package net.vectorcomputing.dtm.postgresql;

import lombok.Builder;
import lombok.Data;
import lombok.NonNull;
import net.vectorcomputing.dtm.Task;
import net.vectorcomputing.dtm.TaskStatus;
import org.threeten.extra.PeriodDuration;

import java.io.IOException;
import java.sql.*;
import java.text.MessageFormat;
import java.time.Instant;
import java.util.Objects;

import static net.vectorcomputing.dtm.postgresql.JdbcUtils.closeWithoutException;

@Data
@Builder
public class PostgresqlTask implements Task {

    @NonNull
    private transient final PostgresqlTaskManager ptm;
    private transient Connection conn;

    @NonNull
    private final String name;

    @NonNull
    private final Instant bucketTime;

    @NonNull
    private final PeriodDuration bucketInterval;

    private String acquiredBy;
    private Instant acquiredAt;
    private Instant completedAt;
    private String message;

    @Builder.Default
    private TaskStatus status = TaskStatus.CREATED;

    private String exception;

    @Builder.Default
    private int exceptionCount = 0;

    @Override
    public synchronized void acquire(String acquiredBy) {
        if (isAcquired()) {
            throw new IllegalStateException("Attempt to re-acquire a lock that has already been acquired");
        }
        Objects.requireNonNull(acquiredBy, "must specify acquired_by");
        PreparedStatement pstmt = null;
        ResultSet resultSet = null;
        try {
            this.conn = ptm.getConnection();
            this.conn.setAutoCommit(false);
            pstmt = this.conn.prepareStatement(ptm.sqlBuilder.selectForUpdateNoWait());
            pstmt.setString(1, this.name);
            pstmt.setTimestamp(2, Timestamp.from(bucketTime));
            resultSet = pstmt.executeQuery();
            if (resultSet.next()) {
                // acquired
                closeWithoutException(pstmt);
                this.acquiredBy = acquiredBy;
                this.acquiredAt = Instant.now();
                pstmt = conn.prepareStatement(ptm.sqlBuilder.updateAcquired());
                pstmt.setString(1, TaskStatus.RUNNING.name());
                pstmt.setString(2, acquiredBy);
                pstmt.setTimestamp(3, Timestamp.from(acquiredAt));
                pstmt.setString(4, name);
                pstmt.setTimestamp(5, Timestamp.from(bucketTime));
                pstmt.executeUpdate();
            } else {
                closeWithoutException(conn);
                this.conn = null;
                throw new RuntimeException("No rows returned from acquire");
            }
        } catch (SQLException e) {
            // if we failed to acquire, close the connection
            closeWithoutException(this.conn);
            String message = MessageFormat.format("Unable to acquire lock on task name {0} bucket time {1}", name, bucketTime);
            throw new RuntimeException(message, e);
        } finally {
            // always close statement but leave connection open in transaction when no exception
            closeWithoutException(resultSet);
            closeWithoutException(pstmt);
        }
    }

    @Override
    public synchronized boolean isAcquired() {
        return this.conn != null;
    }

    @Override
    public synchronized void complete(String message) {
        if (!isAcquired()) {
            throw new IllegalStateException("Lock must be acquired before trying to complete");
        }
        PreparedStatement pstmt = null;
        try {
            // this.conn is already has auto-commit set to false
            // and the lock on the task has already been acquired
            final Instant now = Instant.now();
            pstmt = this.conn.prepareStatement(ptm.sqlBuilder.updateStatusMessageCompletedAt());
            pstmt.setString(1, TaskStatus.COMPLETE.name());
            pstmt.setTimestamp(2,Timestamp.from(now));
            if (message == null) {
                pstmt.setNull(3, Types.CLOB);
            } else {
                pstmt.setString(3, message);
            }
            pstmt.setString(4, this.name);
            pstmt.setTimestamp(5, Timestamp.from(bucketTime));
            pstmt.executeUpdate();
            this.conn.commit();
            // only update the local state after the transaction succeeds
            this.completedAt = now;
            this.message = message;
        } catch (SQLException e) {
            String errorMessage = MessageFormat.format("Unable to complete task name {0} bucket time {1}", name, bucketTime);
            throw new RuntimeException(message, e);
        } finally {
            closeWithoutException(pstmt);
            closeWithoutException(conn);
            this.conn = null;
        }
    }

    @Override
    public synchronized void failure(String message) {
        if (!isAcquired()) {
            throw new IllegalStateException("Lock must be acquired before trying to fail");
        }
        PreparedStatement pstmt = null;
        try {
            // this.conn is already has auto-commit set to false
            // and the lock on the task has already been acquired
            pstmt = this.conn.prepareStatement(ptm.sqlBuilder.updateStatusAndMessage());
            pstmt.setString(1, TaskStatus.FAILURE.name());
            if (message == null) {
                pstmt.setNull(2, Types.CLOB);
            } else {
                pstmt.setString(2, message);
            }
            pstmt.setString(3, this.name);
            pstmt.setTimestamp(4, Timestamp.from(bucketTime));
            pstmt.executeUpdate();
            this.conn.commit();
            // only update the local state after the transaction succeeds
            this.message = message;
        } catch (SQLException e) {
            String errorMessage = MessageFormat.format("Unable to fail task name {0} bucket time {1}", name, bucketTime);
            throw new RuntimeException(message, e);
        } finally {
            closeWithoutException(pstmt);
            closeWithoutException(conn);
            this.conn = null;
        }
    }

    @Override
    public synchronized void skip(String message) {
        if (!isAcquired()) {
            throw new IllegalStateException("Lock must be acquired before trying to skip");
        }
        PreparedStatement pstmt = null;
        try {
            // this.conn is already has auto-commit set to false
            // and the lock on the task has already been acquired
            pstmt = this.conn.prepareStatement(ptm.sqlBuilder.updateStatusAndMessage());
            pstmt.setString(1, TaskStatus.SKIP.name());
            if (message == null) {
                pstmt.setNull(2, Types.CLOB);
            } else {
                pstmt.setString(2, message);
            }
            pstmt.setString(3, this.name);
            pstmt.setTimestamp(4, Timestamp.from(bucketTime));
            pstmt.executeUpdate();
            this.conn.commit();
            // only update the local state after the transaction succeeds
            this.message = message;
        } catch (SQLException e) {
            String errorMessage = MessageFormat.format("Unable to fail task name {0} bucket time {1}", name, bucketTime);
            throw new RuntimeException(message, e);
        } finally {
            closeWithoutException(pstmt);
            closeWithoutException(conn);
            this.conn = null;
        }
    }

    @Override
    public void close() throws IOException {
        closeWithoutException(conn);
        this.conn = null;
    }
}
