package org.neuralchilli.quorch.domain;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.time.Instant;
import java.util.Map;
import java.util.UUID;

/**
 * Represents execution of a global task shared across multiple graphs.
 * Stored in Hazelcast for deduplication and state sharing.
 * <p>
 * NOTE: Linked graph executions are tracked separately in globalTaskLinks map
 * to avoid race conditions between linking and task state updates.
 */
public record GlobalTaskExecution(
        UUID id,
        String taskName,
        String resolvedKey,
        Map<String, Object> params,
        TaskStatus status,
        String workerId,
        String threadName,
        Instant startedAt,
        Instant completedAt,
        Map<String, Object> result,
        String error
) implements Serializable {

    private static final Logger log = LoggerFactory.getLogger(GlobalTaskExecution.class);

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
            Map<String, Object> params
    ) {
        return new GlobalTaskExecution(
                UUID.randomUUID(),
                taskName,
                resolvedKey,
                params,
                TaskStatus.PENDING,
                null,
                null,
                null,
                null,
                Map.of(),
                null
        );
    }

    /**
     * Update status
     */
    public GlobalTaskExecution withStatus(TaskStatus newStatus) {
        return new GlobalTaskExecution(
                id, taskName, resolvedKey, params, newStatus,
                workerId, threadName, startedAt, completedAt, result, error
        );
    }

    /**
     * Mark as queued
     */
    public GlobalTaskExecution queue() {
        log.debug("Queueing global task: {} (key: {})", id, resolvedKey);
        return withStatus(TaskStatus.QUEUED);
    }

    /**
     * Mark as running
     */
    public GlobalTaskExecution start(String workerId, String threadName) {
        return new GlobalTaskExecution(
                id, taskName, resolvedKey, params, TaskStatus.RUNNING,
                workerId, threadName, Instant.now(), null, result, error
        );
    }

    /**
     * Mark as completed
     */
    public GlobalTaskExecution complete(Map<String, Object> taskResult) {
        log.debug("Completing global task: {} (key: {}) with result: {}", id, resolvedKey, taskResult);
        return new GlobalTaskExecution(
                id, taskName, resolvedKey, params, TaskStatus.COMPLETED,
                workerId, threadName, startedAt, Instant.now(), taskResult, null
        );
    }

    /**
     * Mark as failed
     */
    public GlobalTaskExecution fail(String errorMessage) {
        return new GlobalTaskExecution(
                id, taskName, resolvedKey, params, TaskStatus.FAILED,
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
}