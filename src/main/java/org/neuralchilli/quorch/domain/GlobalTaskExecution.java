package org.neuralchilli.quorch.domain;

import java.io.Serializable;
import java.time.Instant;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

/**
 * Represents execution of a global task shared across multiple graphs.
 * Stored in Hazelcast for deduplication and state sharing.
 */
public record GlobalTaskExecution(
        UUID id,
        String taskName,
        String resolvedKey,
        Map<String, Object> params,
        TaskStatus status,
        Set<UUID> linkedGraphExecutions,
        String workerId,
        String threadName,
        Instant startedAt,
        Instant completedAt,
        Map<String, Object> result,
        String error
) implements Serializable {

    public GlobalTaskExecution {
        if (id == null) {
            throw new IllegalArgumentException("Global task execution ID cannot be null");
        }
        if (taskName == null || taskName.isBlank()) {
            throw new IllegalArgumentException("Task name cannot be null or empty");
        }
        if (resolvedKey == null || resolvedKey.isBlank()) {
            throw new IllegalArgumentException("Resolved key cannot be null or empty");
        }
        if (status == null) {
            throw new IllegalArgumentException("Status cannot be null");
        }

        // Defaults
        if (params == null) {
            params = Map.of();
        }
        if (linkedGraphExecutions == null) {
            linkedGraphExecutions = Set.of();
        }
        if (result == null) {
            result = Map.of();
        }
    }

    /**
     * Create a new global task execution
     */
    public static GlobalTaskExecution create(
            String taskName,
            String resolvedKey,
            Map<String, Object> params,
            UUID initialGraphExecutionId
    ) {
        return new GlobalTaskExecution(
                UUID.randomUUID(),
                taskName,
                resolvedKey,
                params,
                TaskStatus.PENDING,
                Set.of(initialGraphExecutionId),
                null,
                null,
                null,
                null,
                Map.of(),
                null
        );
    }

    /**
     * Link another graph execution to this task
     */
    public GlobalTaskExecution linkGraph(UUID graphExecutionId) {
        Set<UUID> updated = new HashSet<>(linkedGraphExecutions);
        updated.add(graphExecutionId);

        return new GlobalTaskExecution(
                id, taskName, resolvedKey, params, status, updated,
                workerId, threadName, startedAt, completedAt, result, error
        );
    }

    /**
     * Update status
     */
    public GlobalTaskExecution withStatus(TaskStatus newStatus) {
        return new GlobalTaskExecution(
                id, taskName, resolvedKey, params, newStatus, linkedGraphExecutions,
                workerId, threadName, startedAt, completedAt, result, error
        );
    }

    /**
     * Mark as queued
     */
    public GlobalTaskExecution queue() {
        return withStatus(TaskStatus.QUEUED);
    }

    /**
     * Mark as running
     */
    public GlobalTaskExecution start(String workerId, String threadName) {
        return new GlobalTaskExecution(
                id, taskName, resolvedKey, params, TaskStatus.RUNNING, linkedGraphExecutions,
                workerId, threadName, Instant.now(), null, result, error
        );
    }

    /**
     * Mark as completed
     */
    public GlobalTaskExecution complete(Map<String, Object> taskResult) {
        return new GlobalTaskExecution(
                id, taskName, resolvedKey, params, TaskStatus.COMPLETED, linkedGraphExecutions,
                workerId, threadName, startedAt, Instant.now(), taskResult, null
        );
    }

    /**
     * Mark as failed
     */
    public GlobalTaskExecution fail(String errorMessage) {
        return new GlobalTaskExecution(
                id, taskName, resolvedKey, params, TaskStatus.FAILED, linkedGraphExecutions,
                workerId, threadName, startedAt, Instant.now(), result, errorMessage
        );
    }

    /**
     * Get execution key
     */
    public TaskExecutionKey getKey() {
        return TaskExecutionKey.of(taskName, resolvedKey);
    }

    /**
     * Check if task is finished
     */
    public boolean isFinished() {
        return status.isTerminal();
    }

    /**
     * Get number of linked graphs
     */
    public int linkedGraphCount() {
        return linkedGraphExecutions.size();
    }
}