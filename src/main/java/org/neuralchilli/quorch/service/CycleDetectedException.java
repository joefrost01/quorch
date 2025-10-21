package org.neuralchilli.quorch.service;

/**
 * Thrown when a cycle is detected in a workflow graph.
 * Extends RuntimeException as this is a validation error that should
 * be caught during graph loading, not during normal execution.
 */
public class CycleDetectedException extends RuntimeException {

    public CycleDetectedException(String message) {
        super(message);
    }

    public CycleDetectedException(String message, Throwable cause) {
        super(message, cause);
    }
}