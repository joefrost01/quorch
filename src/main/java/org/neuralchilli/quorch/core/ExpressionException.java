package org.neuralchilli.quorch.core;

/**
 * Thrown when expression evaluation fails.
 * Provides clear error messages with context.
 */
public class ExpressionException extends RuntimeException {

    public ExpressionException(String message) {
        super(message);
    }

    public ExpressionException(String message, Throwable cause) {
        super(message, cause);
    }
}