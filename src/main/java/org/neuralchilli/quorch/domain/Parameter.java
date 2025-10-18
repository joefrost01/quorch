package org.neuralchilli.quorch.domain;

/**
 * Represents a parameter definition for tasks and graphs.
 * Parameters can have types, defaults, and validation rules.
 */
public record Parameter(
        ParameterType type,
        Object defaultValue,  // Can be a JEXL expression string
        boolean required,
        String description
) {
    public Parameter {
        if (type == null) {
            throw new IllegalArgumentException("Parameter type cannot be null");
        }
    }

    /**
     * Create a parameter with no default (optional)
     */
    public static Parameter optional(ParameterType type, String description) {
        return new Parameter(type, null, false, description);
    }

    /**
     * Create a required parameter with no default
     */
    public static Parameter required(ParameterType type, String description) {
        return new Parameter(type, null, true, description);
    }

    /**
     * Create a parameter with a default value
     */
    public static Parameter withDefault(ParameterType type, Object defaultValue, String description) {
        return new Parameter(type, defaultValue, false, description);
    }
}