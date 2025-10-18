package org.neuralchilli.quorch.domain;

import java.io.Serializable;
import java.time.Instant;
import java.util.Map;
import java.util.UUID;

/**
 * Represents a single execution of a graph with specific parameters.
 * Stored in Hazelcast for distributed state management.
 */
public record GraphExecution(
        UUID id,
        String graphName,
        Map<String, Object> params,
        GraphStatus status,
        String triggeredBy,
        Instant startedAt,
        Instant completedAt,
        String error
) implements Serializable {

    public GraphExecution {
        if (id == null) {
            throw new IllegalArgumentException("Graph execution ID cannot be null");
        }
        if (graphName == null || graphName.isBlank()) {
            throw new IllegalArgumentException("Graph name cannot be null or empty");
        }
        if (status == null) {
            throw new IllegalArgumentException("Status cannot be null");
        }
        if (startedAt == null) {
            throw new IllegalArgumentException("Started at cannot be null");
        }

        // Defaults
        if (params == null) {
            params = Map.of();
        }
    }

    /**
     * Create a new graph execution
     */
    public static GraphExecution create(String graphName, Map<String, Object> params, String triggeredBy) {
        return new GraphExecution(
                UUID.randomUUID(),
                graphName,
                params,
                GraphStatus.RUNNING,
                triggeredBy,
                Instant.now(),
                null,
                null
        );
    }

    /**
     * Update status
     */
    public GraphExecution withStatus(GraphStatus newStatus) {
        return new GraphExecution(
                id, graphName, params, newStatus, triggeredBy, startedAt, completedAt, error
        );
    }

    /**
     * Mark as completed
     */
    public GraphExecution complete() {
        return new GraphExecution(
                id, graphName, params, GraphStatus.COMPLETED, triggeredBy, startedAt, Instant.now(), null
        );
    }

    /**
     * Mark as failed
     */
    public GraphExecution fail(String errorMessage) {
        return new GraphExecution(
                id, graphName, params, GraphStatus.FAILED, triggeredBy, startedAt, Instant.now(), errorMessage
        );
    }

    /**
     * Mark as stalled
     */
    public GraphExecution stall() {
        return new GraphExecution(
                id, graphName, params, GraphStatus.STALLED, triggeredBy, startedAt, Instant.now(), null
        );
    }

    /**
     * Pause execution
     */
    public GraphExecution pause() {
        return new GraphExecution(
                id, graphName, params, GraphStatus.PAUSED, triggeredBy, startedAt, completedAt, error
        );
    }

    /**
     * Resume execution
     */
    public GraphExecution resume() {
        if (!status.canResume()) {
            throw new IllegalStateException("Cannot resume execution in status: " + status);
        }
        return new GraphExecution(
                id, graphName, params, GraphStatus.RUNNING, triggeredBy, startedAt, null, null
        );
    }

    /**
     * Check if execution is finished
     */
    public boolean isFinished() {
        return status.isTerminal();
    }
}