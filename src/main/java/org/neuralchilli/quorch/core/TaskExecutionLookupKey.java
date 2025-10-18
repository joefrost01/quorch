package org.neuralchilli.quorch.core;

import java.io.Serial;
import java.io.Serializable;
import java.util.Objects;
import java.util.UUID;

/**
 * Lookup key for indexed task execution retrieval.
 * Enables O(1) lookups instead of O(n) map scans.
 *
 * Must be a plain class (not record) for Hazelcast serialization.
 */
public final class TaskExecutionLookupKey implements Serializable {

    @Serial
    private static final long serialVersionUID = 1L;

    private final UUID graphExecutionId;
    private final String taskName;

    public TaskExecutionLookupKey(UUID graphExecutionId, String taskName) {
        if (graphExecutionId == null) {
            throw new IllegalArgumentException("Graph execution ID cannot be null");
        }
        if (taskName == null || taskName.isBlank()) {
            throw new IllegalArgumentException("Task name cannot be null or empty");
        }

        this.graphExecutionId = graphExecutionId;
        this.taskName = taskName;
    }

    public UUID graphExecutionId() {
        return graphExecutionId;
    }

    public String taskName() {
        return taskName;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;

        TaskExecutionLookupKey that = (TaskExecutionLookupKey) obj;
        return Objects.equals(graphExecutionId, that.graphExecutionId) &&
                Objects.equals(taskName, that.taskName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(graphExecutionId, taskName);
    }

    @Override
    public String toString() {
        return "TaskExecutionLookupKey[graphExecutionId=" + graphExecutionId +
                ", taskName=" + taskName + "]";
    }
}