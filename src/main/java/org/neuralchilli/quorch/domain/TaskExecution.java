package org.neuralchilli.quorch.domain;

import java.io.Serializable;
import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.UUID;

/**
 * Represents a single execution of a task within a graph execution.
 * Stored in Hazelcast for distributed state management.
 */
public record TaskExecution(
        UUID id,
        UUID graphExecutionId,
        String taskName,
        TaskStatus status,
        String workerId,
        String threadName,
        Instant startedAt,
        Instant completedAt,
        Long durationMillis,
        Map<String, Object> result,
        String error,
        int attempt,
        boolean isGlobal,
        TaskExecutionKey globalKey
) implements Serializable {

    public TaskExecution {
        if (id == null) {
            throw new IllegalArgumentException("Task execution ID cannot be null");
        }
        if (graphExecutionId == null) {
            throw new IllegalArgumentException("Graph execution ID cannot be null");
        }
        if (taskName == null || taskName.isBlank()) {
            throw new IllegalArgumentException("Task name cannot be null or empty");
        }
        if (status == null) {
            throw new IllegalArgumentException("Status cannot be null");
        }
        if (attempt < 1) {
            throw new IllegalArgumentException("Attempt must be >= 1");
        }

        // Defaults
        if (result == null) {
            result = Map.of();
        }
    }

    /**
     * Create a new task execution
     */
    public static TaskExecution create(
            UUID graphExecutionId,
            String taskName,
            boolean isGlobal,
            TaskExecutionKey globalKey
    ) {
        return new TaskExecution(
                UUID.randomUUID(),
                graphExecutionId,
                taskName,
                TaskStatus.PENDING,
                null,
                null,
                null,
                null,
                null,
                Map.of(),
                null,
                1,
                isGlobal,
                globalKey
        );
    }

    /**
     * Update status
     */
    public TaskExecution withStatus(TaskStatus newStatus) {
        return new TaskExecution(
                id, graphExecutionId, taskName, newStatus, workerId, threadName,
                startedAt, completedAt, durationMillis, result, error, attempt,
                isGlobal, globalKey
        );
    }

    /**
     * Mark as queued
     */
    public TaskExecution queue() {
        return withStatus(TaskStatus.QUEUED);
    }

    /**
     * Mark as running
     */
    public TaskExecution start(String workerId, String threadName) {
        return new TaskExecution(
                id, graphExecutionId, taskName, TaskStatus.RUNNING, workerId, threadName,
                Instant.now(), null, null, result, error, attempt,
                isGlobal, globalKey
        );
    }

    /**
     * Mark as completed
     */
    public TaskExecution complete(Map<String, Object> taskResult) {
        Instant now = Instant.now();
        Long duration = startedAt != null ? Duration.between(startedAt, now).toMillis() : null;

        return new TaskExecution(
                id, graphExecutionId, taskName, TaskStatus.COMPLETED, workerId, threadName,
                startedAt, now, duration, taskResult, null, attempt,
                isGlobal, globalKey
        );
    }

    /**
     * Mark as failed
     */
    public TaskExecution fail(String errorMessage) {
        Instant now = Instant.now();
        Long duration = startedAt != null ? Duration.between(startedAt, now).toMillis() : null;

        return new TaskExecution(
                id, graphExecutionId, taskName, TaskStatus.FAILED, workerId, threadName,
                startedAt, now, duration, result, errorMessage, attempt,
                isGlobal, globalKey
        );
    }

    /**
     * Mark as skipped
     */
    public TaskExecution skip(String reason) {
        return new TaskExecution(
                id, graphExecutionId, taskName, TaskStatus.SKIPPED, workerId, threadName,
                startedAt, Instant.now(), null, result, reason, attempt,
                isGlobal, globalKey
        );
    }

    /**
     * Increment attempt for retry
     */
    public TaskExecution retry() {
        if (!status.canRetry()) {
            throw new IllegalStateException("Cannot retry task in status: " + status);
        }

        return new TaskExecution(
                UUID.randomUUID(), // New ID for retry
                graphExecutionId,
                taskName,
                TaskStatus.PENDING,
                null,
                null,
                null,
                null,
                null,
                Map.of(),
                null,
                attempt + 1,
                isGlobal,
                globalKey
        );
    }

    /**
     * Check if task is finished
     */
    public boolean isFinished() {
        return status.isTerminal();
    }

    /**
     * Get duration as Duration object
     */
    public Duration getDuration() {
        return durationMillis != null ? Duration.ofMillis(durationMillis) : Duration.ZERO;
    }
}