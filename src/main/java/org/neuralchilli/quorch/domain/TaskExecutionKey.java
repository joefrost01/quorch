package org.neuralchilli.quorch.domain;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import java.io.Serializable;
import java.util.Objects;

/**
 * Unique key for global task deduplication.
 * Combines task name with resolved key expression.
 */
public record TaskExecutionKey(
        String taskName,
        String resolvedKey
) implements Serializable {

    private static final Logger log = LoggerFactory.getLogger(TaskExecutionKey.class);

    public TaskExecutionKey {
        // Log for debugging key construction
        if (taskName != null && resolvedKey != null) {
            log.trace("Creating TaskExecutionKey: taskName='{}', resolvedKey='{}'", taskName, resolvedKey);
        }
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

    @Nonnull
    @Override
    public String toString() {
        return taskName + "[" + resolvedKey + "]";
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;

        TaskExecutionKey that = (TaskExecutionKey) obj;
        boolean result = Objects.equals(taskName, that.taskName) &&
                Objects.equals(resolvedKey, that.resolvedKey);
        log.trace("TaskExecutionKey.equals() - this: {}, other: {}, result: {}", this, that, result);
        return result;
    }
}