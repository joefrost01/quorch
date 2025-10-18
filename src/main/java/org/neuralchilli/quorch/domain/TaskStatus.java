package org.neuralchilli.quorch.domain;

/**
 * Lifecycle status of a task execution.
 */
public enum TaskStatus {
    /**
     * Task created, waiting for dependencies to complete
     */
    PENDING,

    /**
     * Task published to worker queue
     */
    QUEUED,

    /**
     * Worker is currently executing the task
     */
    RUNNING,

    /**
     * Task completed successfully
     */
    COMPLETED,

    /**
     * Task failed (may retry)
     */
    FAILED,

    /**
     * Task skipped due to upstream failure
     */
    SKIPPED;

    /**
     * Check if this is a terminal state (task finished)
     */
    public boolean isTerminal() {
        return this == COMPLETED || this == FAILED || this == SKIPPED;
    }

    /**
     * Check if task can be retried
     */
    public boolean canRetry() {
        return this == FAILED;
    }

    /**
     * Check if task is in progress
     */
    public boolean isInProgress() {
        return this == QUEUED || this == RUNNING;
    }
}