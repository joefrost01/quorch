package org.neuralchilli.quorch.domain;

/**
 * Supported parameter types for task and graph parameters.
 */
public enum ParameterType {
    STRING,
    INTEGER,
    BOOLEAN,
    DATE,
    ARRAY;

    /**
     * Parse type from string (case-insensitive)
     */
    public static ParameterType fromString(String value) {
        if (value == null) {
            throw new IllegalArgumentException("Parameter type cannot be null");
        }
        try {
            return valueOf(value.toUpperCase());
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException(
                    "Invalid parameter type: " + value +
                            ". Valid types: string, integer, boolean, date, array"
            );
        }
    }
}