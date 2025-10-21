package org.neuralchilli.quorch.domain;

import javax.annotation.Nonnull;
import java.io.Serializable;
import java.util.Objects;

/**
 * Represents a node in the task execution DAG.
 * Wraps task information for use in JGraphT.
 */
public record TaskNode(
        String taskName,
        boolean isGlobal,
        String globalTaskName
) implements Serializable {

    public TaskNode {
        if (taskName == null || taskName.isBlank()) {
            throw new IllegalArgumentException("Task name cannot be null or empty");
        }
    }

    /**
     * Create a node for a regular (inline) task
     */
    public static TaskNode regular(String taskName) {
        return new TaskNode(taskName, false, null);
    }

    /**
     * Create a node for a global task reference
     */
    public static TaskNode global(String taskName, String globalTaskName) {
        return new TaskNode(taskName, true, globalTaskName);
    }

    /**
     * Get the effective name to use for this task
     */
    public String getEffectiveName() {
        return taskName;
    }

    /**
     * Get the global task name if this is a global reference, otherwise null
     */
    public String getGlobalTaskName() {
        return isGlobal ? globalTaskName : null;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) return true;
        if (obj == null || obj.getClass() != this.getClass()) return false;
        TaskNode that = (TaskNode) obj;
        // Equality based on task name only (for graph operations)
        return Objects.equals(this.taskName, that.taskName);
    }

    @Override
    public int hashCode() {
        // Hash based on task name only (for graph operations)
        return Objects.hash(taskName);
    }

    @Nonnull
    @Override
    public String toString() {
        if (isGlobal) {
            return String.format("TaskNode[%s -> global:%s]", taskName, globalTaskName);
        }
        return String.format("TaskNode[%s]", taskName);
    }
}