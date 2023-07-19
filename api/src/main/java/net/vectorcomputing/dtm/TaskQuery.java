package net.vectorcomputing.dtm;

import lombok.Builder;
import lombok.Data;

import java.time.Instant;
import java.util.Set;

@Data
@Builder
public class TaskQuery {
    private String name;
    private Instant bucketStartTime;
    private Instant bucketEndTime;
    private Instant acquiredAtStartTime;
    private Instant acquiredAtEndTime;
    private Set<TaskStatus> statuses;
}
