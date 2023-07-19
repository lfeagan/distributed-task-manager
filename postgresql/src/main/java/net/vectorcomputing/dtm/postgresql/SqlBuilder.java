package net.vectorcomputing.dtm.postgresql;

import com.google.common.base.Joiner;
import lombok.AllArgsConstructor;
import lombok.Builder;
import net.vectorcomputing.dtm.TaskQuery;

import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.List;

@Builder
@AllArgsConstructor
public class SqlBuilder {

    private static final String ALL_COLUMNS = "name, bucket_time, bucket_interval, status, acquired_by, acquired_at, completed_at, message";
    private static final String MINIMAL_COLUMNS = "name, bucket_time, bucket_interval, status";

    /**
     * The table name. Can be a one-, two-, or three-part name.
     */
    private final String tableName;

    /**
     * The length of identifiers in the table.
     */
    @Builder.Default
    private final int idLength = 32;

    private static void validateTableName(final String tableName) {
//        expectNonEmpty(tableName);
        if (!tableName.matches("[a-zA-Z0-9_\\.]+")) {
            String message = MessageFormat.format("Table name must consist exclusively of characters from the set [a-zA-Z0-9_\\.] but was {0}", tableName);
            throw new IllegalArgumentException(message);
        }
    }

    private void validateIdLength() {
        if (idLength < 1) {
            String message = MessageFormat.format("Identifier length must be greater than 0 (zero) but was {0}", idLength);
            throw new IllegalArgumentException(message);
        }
        if (idLength > 255) {
            String message = MessageFormat.format("Identifier length must be less than 256 but was {0}", idLength);
            throw new IllegalArgumentException(message);
        }
    }

    String createTaskTable() {
        validateIdLength();
        return "CREATE TABLE " + tableName
                + "("
                + "name VARCHAR("+ idLength +") NOT NULL, "
                + "bucket_time TIMESTAMPTZ NOT NULL, "
                + "bucket_interval INTERVAL NOT NULL, "
                + "status VARCHAR(16) NOT NULL, "
                + "acquired_by VARCHAR("+ idLength +"), "
                + "acquired_at TIMESTAMPTZ, "
                + "completed_at TIMESTAMPTZ, "
                + "message TEXT, "
                + "PRIMARY KEY (name,bucket_time) "
                + ")";
    }

    String checkTableExists() {
        return "SELECT 1 FROM " + tableName + " WHERE 1=2";
    }

    /**
     * Insert a task that defines all non-nullable fields.
     * Typically used to create a task without acquiring it immediately.
     * @return
     */
    String insertMinimalTask() {
        StringBuilder sb = new StringBuilder("INSERT INTO ");
        sb.append(tableName);
        sb.append(" (");
        sb.append(MINIMAL_COLUMNS);
        sb.append(") VALUES (?,?,?,?)");
        return sb.toString();
    }

    /**
     * Inserts a task with all fields defined.
     * Typically used to create a task and immediately acquire it.
     * @return
     */
    String insertFullTask() {
        StringBuilder sb = new StringBuilder("INSERT INTO ");
        sb.append(tableName);
        sb.append(" (");
        sb.append(ALL_COLUMNS);
        sb.append(") VALUES (?,?,?,?,?,?,?,?)");
        return sb.toString();
    }

    String selectTask() {
        StringBuilder sb = new StringBuilder();
        sb.append("SELECT ");
        sb.append(ALL_COLUMNS);
        sb.append(" FROM ");
        sb.append(tableName);
        sb.append(" WHERE name=? and bucket_time=?");
        return sb.toString();
    }

    String selectForUpdateNoWait() {
        StringBuilder sb = new StringBuilder();
        sb.append("SELECT ");
        sb.append(ALL_COLUMNS);
        sb.append(" FROM ");
        sb.append(tableName);
        sb.append(" WHERE name=? and bucket_time=? FOR UPDATE NOWAIT");
        return sb.toString();
    }

    String selectForUpdateSkipLocked() {
        StringBuilder sb = new StringBuilder();
        sb.append("SELECT ");
        sb.append(ALL_COLUMNS);
        sb.append(" FROM ");
        sb.append(tableName);
        sb.append(" WHERE name=? and bucket_time=? FOR UPDATE SKIP LOCKED");
        return sb.toString();
    }

    String updateAcquired() {
        StringBuilder sb = new StringBuilder();
        sb.append("UPDATE ");
        sb.append(tableName);
        sb.append(" SET status=?, acquired_by=?, acquired_at=?");
        sb.append(" WHERE name=? and bucket_time=?");
        return sb.toString();
    }

    String updateStatus() {
        StringBuilder sb = new StringBuilder();
        sb.append("UPDATE ");
        sb.append(tableName);
        sb.append(" SET status=?");
        sb.append(" WHERE name=? and bucket_time=?");
        return sb.toString();
    }

    String updateStatusAndMessage() {
        StringBuilder sb = new StringBuilder();
        sb.append("UPDATE ");
        sb.append(tableName);
        sb.append(" SET status=?, message=?");
        sb.append(" WHERE name=? and bucket_time=?");
        return sb.toString();
    }

    String updateStatusMessageCompletedAt() {
        StringBuilder sb = new StringBuilder();
        sb.append("UPDATE ");
        sb.append(tableName);
        sb.append(" SET status=?, completed_at=?, message=?");
        sb.append(" WHERE name=? and bucket_time=?");
        return sb.toString();
    }

    String taskQueryToSql(TaskQuery taskQuery) {
        StringBuilder sb = new StringBuilder();
        sb.append("SELECT ");
        sb.append(ALL_COLUMNS);
        sb.append(" FROM ");
        sb.append(tableName);
        sb.append(" WHERE ");
        List<String> predicates = new ArrayList<>();

        if (taskQuery.getName() != null) {
            predicates.add("name='" + taskQuery.getName() + "'");
        }
        if (taskQuery.getBucketStartTime() != null) {
            predicates.add("bucket_time >= '" + PostgresqlTimeUtils.toPostgresqlTimestampWithTz(taskQuery.getBucketStartTime()) + "'::TIMESTAMPTZ");
        }
        if (taskQuery.getBucketEndTime() != null) {
            predicates.add("bucket_time < '" + PostgresqlTimeUtils.toPostgresqlTimestampWithTz(taskQuery.getBucketEndTime()) + "'::TIMESTAMPTZ");
        }
        if (taskQuery.getAcquiredAtStartTime() != null) {
            predicates.add("acquired_at >= '" + PostgresqlTimeUtils.toPostgresqlTimestampWithTz(taskQuery.getAcquiredAtStartTime()) + "'::TIMESTAMPTZ");
        }
        if (taskQuery.getAcquiredAtEndTime() != null) {
            predicates.add("acquired_at < '" + PostgresqlTimeUtils.toPostgresqlTimestampWithTz(taskQuery.getAcquiredAtEndTime()) + "'::TIMESTAMPTZ");
        }
        if (taskQuery.getStatuses() != null && !taskQuery.getStatuses().isEmpty()) {
            List<String> status_predicates = new ArrayList<String>();
            taskQuery.getStatuses().stream().forEach(s -> status_predicates.add(" status = '" + s.name() + "'"));
            predicates.add("(" + Joiner.on(" OR ").join(status_predicates) + ")");
        }
        sb.append(Joiner.on(" AND ").join(predicates));

        return sb.toString();
    }

}
