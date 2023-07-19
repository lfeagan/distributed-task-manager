package net.vectorcomputing.dtm.postgresql;

import net.vectorcomputing.dtm.TaskManager;
import net.vectorcomputing.dtm.Task;
import net.vectorcomputing.dtm.TaskQuery;
import net.vectorcomputing.dtm.TaskStatus;
import org.postgresql.util.PGInterval;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.threeten.extra.PeriodDuration;

import javax.sql.DataSource;
import java.sql.*;
import java.text.MessageFormat;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

import static net.vectorcomputing.dtm.postgresql.JdbcUtils.closeWithoutException;

public class PostgresqlTaskManager implements TaskManager {

    private static final Logger LOGGER = LoggerFactory.getLogger(PostgresqlTaskManager.class);

    protected final DataSource dataSource;
    protected final SqlBuilder sqlBuilder;

    public PostgresqlTaskManager(final DataSource dataSource) {
        this.dataSource = dataSource;
        this.sqlBuilder = new SqlBuilder("tasks", 32);
    }

    public void initialize() {
        Connection conn = null;
        Statement stmt = null;
        try {
            conn = getConnection();
            stmt = conn.createStatement();
            stmt.execute(sqlBuilder.createTaskTable());
        } catch (SQLException e) {
            throw new RuntimeException("Unable to initialize", e);
        } finally {
            closeWithoutException(stmt);
            closeWithoutException(conn);
        }
    }

    protected Connection getConnection() throws SQLException {
        return dataSource.getConnection();
    }

    @Override
    public Task createTask(String name, Instant bucketTime, PeriodDuration bucketInterval) {
        Connection conn = null;
        PreparedStatement pstmt = null;
        try {
            conn = getConnection();
            conn.setAutoCommit(false);
            pstmt = conn.prepareStatement(sqlBuilder.insertMinimalTask());
            pstmt.setString(1, name);
            pstmt.setTimestamp(2, Timestamp.from(bucketTime));
            org.postgresql.util.PGInterval interval = new PGInterval(bucketInterval.toString());
            pstmt.setObject(3, interval);
            pstmt.setString(4, TaskStatus.CREATED.name());
            pstmt.executeUpdate();
            conn.commit();
            return PostgresqlTask.builder()
                    .name(name)
                    .bucketTime(bucketTime)
                    .bucketInterval(bucketInterval)
                    .status(TaskStatus.CREATED)
                    .ptm(this).build();
        } catch (SQLException e) {
            // unique constraint violation
            if (e.getSQLState().equals("23505")) {
//                throw new DuplicateKeyException()
                String message = MessageFormat.format("Task with name {0} and bucket_time {1} already exists", name, bucketTime);
                throw new RuntimeException(message, e);
            }
            throw new RuntimeException("Unable to create task", e);
        } finally {
            closeWithoutException(pstmt);
            closeWithoutException(conn);
        }
    }

    @Override
    public Task getAndAcquireFirstTask(TaskQuery taskQuery) {
        Connection conn = null;
        PreparedStatement pstmt = null;
        ResultSet resultSet = null;
        final String sql = sqlBuilder.taskQueryToSql(taskQuery) + " FOR UPDATE SKIP LOCKED LIMIT 1";
        try {
            conn = getConnection();
            conn.setAutoCommit(false);
            pstmt = conn.prepareStatement(sql);
            resultSet = pstmt.executeQuery();
            if (resultSet.next()) {
                return currentRowToTask(resultSet, conn);
            } else {
                closeWithoutException(conn);
                return null;
            }
        } catch (Exception e) {
            // on exception, close the connection
            closeWithoutException(conn);
            String message = MessageFormat.format("Unable to get tasks for query {0}", sql);
            throw new RuntimeException(message, e);
        } finally {
            closeWithoutException(resultSet);
            closeWithoutException(pstmt);
        }
    }

    @Override
    public Task getTask(String name, Instant bucketTime) {
        Connection conn = null;
        PreparedStatement pstmt = null;
        ResultSet resultSet = null;
        try {
            conn = getConnection();
            conn.setAutoCommit(false);
            pstmt = conn.prepareStatement(sqlBuilder.selectTask());
            pstmt.setString(1, name);
            pstmt.setTimestamp(2, Timestamp.from(bucketTime));
            resultSet = pstmt.executeQuery();
            if (resultSet.next()) {
                return currentRowToTask(resultSet);
            }
            return null;
        } catch (SQLException e) {
            String message = MessageFormat.format("Unable to get task with name {0} and bucket time {1}", name, bucketTime);
            throw new RuntimeException(message, e);
        } finally {
            closeWithoutException(resultSet);
            closeWithoutException(pstmt);
            closeWithoutException(conn);
        }
    }

    protected PostgresqlTask currentRowToTask(ResultSet resultSet) throws SQLException {
        return currentRowToTask(resultSet, null);
    }

        /**
         * Converts the current row into a task.
         * Only supply the optional connection if you are running SQL that will result in immediately having a live object that has acquired the lock.
         * @param resultSet
         * @param conn
         * @return
         * @throws SQLException
         */
    protected PostgresqlTask currentRowToTask(ResultSet resultSet, Connection conn) throws SQLException {
        PostgresqlTask.PostgresqlTaskBuilder taskBuilder = PostgresqlTask.builder();
        taskBuilder.name(resultSet.getString(1));
        taskBuilder.bucketTime(resultSet.getTimestamp(2).toInstant());
        Object o = resultSet.getObject(3);
        if (o instanceof PGInterval) {
            taskBuilder.bucketInterval( PostgresqlTimeUtils.periodDurationFromPGInterval((PGInterval)o));
        }
        taskBuilder.status(TaskStatus.valueOf(resultSet.getString(4)));
        taskBuilder.acquiredBy(resultSet.getString(5));
        taskBuilder.acquiredAt(resultSet.getTimestamp(6) == null ? null : resultSet.getTimestamp(6).toInstant());
        taskBuilder.ptm(this);
        if (conn != null) {
            taskBuilder.conn(conn);
        }
        return taskBuilder.build();
    }

    @Override
    public List<Task> getTasks(TaskQuery taskQuery) {
        Connection conn = null;
        PreparedStatement pstmt = null;
        ResultSet resultSet = null;
        final String sql = sqlBuilder.taskQueryToSql(taskQuery);
        try {
            conn = getConnection();
            conn.setAutoCommit(false);
            pstmt = conn.prepareStatement(sql);
            resultSet = pstmt.executeQuery();
            List<Task> tasks = new ArrayList<>();
            while (resultSet.next()) {
                tasks.add(currentRowToTask(resultSet));
            }
            return tasks;
        } catch (SQLException e) {
            String message = MessageFormat.format("Unable to get tasks for query {0}", sql);
            throw new RuntimeException(message, e);
        } finally {
            closeWithoutException(resultSet);
            closeWithoutException(pstmt);
            closeWithoutException(conn);
        }
    }

    protected static void rollbackWithoutException(Statement stmt, Logger logger) {
        if (stmt != null) {
            try {
                stmt.execute("ROLLBACK");
            } catch (SQLException e) {
                if (logger != null) {
                    try {
                        logger.error("Unable to rollback transaction", e);
                    } catch (Exception inner_e) {
                        // do nothing
                    }
                }
            }
        }
    }
}
