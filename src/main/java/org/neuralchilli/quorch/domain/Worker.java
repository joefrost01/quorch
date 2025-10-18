package org.neuralchilli.quorch.domain;

import java.io.Serializable;
import java.time.Duration;
import java.time.Instant;

/**
 * Represents a worker process that executes tasks.
 * Stored in Hazelcast for monitoring and health checks.
 */
public record Worker(
        String id,
        int totalThreads,
        int activeThreads,
        WorkerStatus status,
        Instant lastHeartbeat,
        Instant startedAt
) implements Serializable {

    public Worker {
        if (id == null || id.isBlank()) {
            throw new IllegalArgumentException("Worker ID cannot be null or empty");
        }
        if (totalThreads <= 0) {
            throw new IllegalArgumentException("Total threads must be > 0");
        }
        if (activeThreads < 0) {
            throw new IllegalArgumentException("Active threads cannot be negative");
        }
        if (activeThreads > totalThreads) {
            throw new IllegalArgumentException("Active threads cannot exceed total threads");
        }
        if (status == null) {
            throw new IllegalArgumentException("Status cannot be null");
        }
        if (lastHeartbeat == null) {
            throw new IllegalArgumentException("Last heartbeat cannot be null");
        }
        if (startedAt == null) {
            throw new IllegalArgumentException("Started at cannot be null");
        }
    }

    /**
     * Create a new worker registration
     */
    public static Worker create(String id, int totalThreads) {
        Instant now = Instant.now();
        return new Worker(
                id,
                totalThreads,
                0,
                WorkerStatus.ACTIVE,
                now,
                now
        );
    }

    /**
     * Update heartbeat
     */
    public Worker heartbeat() {
        return new Worker(
                id, totalThreads, activeThreads, status, Instant.now(), startedAt
        );
    }

    /**
     * Update active thread count
     */
    public Worker withActiveThreads(int newActiveThreads) {
        return new Worker(
                id, totalThreads, newActiveThreads, status, lastHeartbeat, startedAt
        );
    }

    /**
     * Update status
     */
    public Worker withStatus(WorkerStatus newStatus) {
        return new Worker(
                id, totalThreads, activeThreads, newStatus, lastHeartbeat, startedAt
        );
    }

    /**
     * Mark as draining (shutting down)
     */
    public Worker drain() {
        return withStatus(WorkerStatus.DRAINING);
    }

    /**
     * Check if worker is healthy based on heartbeat threshold
     */
    public boolean isHealthy(Duration threshold) {
        if (status == WorkerStatus.DEAD) {
            return false;
        }

        Duration timeSinceHeartbeat = Duration.between(lastHeartbeat, Instant.now());
        return timeSinceHeartbeat.compareTo(threshold) <= 0;
    }

    /**
     * Get available threads
     */
    public int availableThreads() {
        return totalThreads - activeThreads;
    }

    /**
     * Check if worker has capacity
     */
    public boolean hasCapacity() {
        return availableThreads() > 0 && status.canAcceptWork();
    }

    /**
     * Get uptime
     */
    public Duration getUptime() {
        return Duration.between(startedAt, Instant.now());
    }
}