package org.neuralchilli.quorch.domain;

/**
 * Health status of a worker.
 */
public enum WorkerStatus {
    /**
     * Worker is healthy and processing tasks
     */
    ACTIVE,

    /**
     * Worker hasn't sent heartbeat within threshold
     */
    DEAD,

    /**
     * Worker is shutting down gracefully
     */
    DRAINING;

    /**
     * Check if worker can accept new tasks
     */
    public boolean canAcceptWork() {
        return this == ACTIVE;
    }
}