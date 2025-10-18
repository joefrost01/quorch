package org.neuralchilli.quorch.domain;

/**
 * Lifecycle status of a graph execution.
 */
public enum GraphStatus {
    /**
     * Graph is being evaluated and tasks are executing
     */
    RUNNING,

    /**
     * All tasks completed successfully
     */
    COMPLETED,

    /**
     * One or more tasks failed permanently
     */
    FAILED,

    /**
     * No progress possible - waiting on failed dependencies
     */
    STALLED,

    /**
     * Manually paused by user
     */
    PAUSED;

    /**
     * Check if this is a terminal state (execution finished)
     */
    public boolean isTerminal() {
        return this == COMPLETED || this == FAILED || this == STALLED;
    }

    /**
     * Check if execution can be resumed
     */
    public boolean canResume() {
        return this == PAUSED;
    }
}