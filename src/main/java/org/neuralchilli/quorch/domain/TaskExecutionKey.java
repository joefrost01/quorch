package org.neuralchilli.quorch.domain;

import java.io.Serializable;

/**
 * Unique key for global task deduplication.
 * Combines task name with resolved key expression.
 */
public record TaskExecutionKey(
        String taskName,
        String resolvedKey
) implements Serializable {

    public TaskExecutionKey {
        if (taskName == null || taskName.isBlank()) {
            throw new IllegalArgumentException("Task name cannot be null or empty");
        }
        if (resolvedKey == null || resolvedKey.isBlank()) {
            throw new IllegalArgumentException("Resolved key cannot be null or empty");
        }
    }

    /**
     * Create a key from task name and resolved key expression
     */
    public static TaskExecutionKey of(String taskName, String resolvedKey) {
        return new TaskExecutionKey(taskName, resolvedKey);
    }

    @Override
    public String toString() {
        return taskName + "[" + resolvedKey + "]";
    }
}