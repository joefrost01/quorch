package org.neuralchilli.quorch.domain;

import java.io.Serial;
import java.io.Serializable;
import java.util.Objects;

/**
 * Represents a parameter definition for tasks and graphs.
 * Parameters can have types, defaults, and validation rules.
 */
public final class Parameter implements Serializable {

    @Serial
    private static final long serialVersionUID = 1L;

    private final ParameterType type;
    private final Object defaultValue;
    private final boolean required;
    private final String description;

    public Parameter(
            ParameterType type,
            Object defaultValue,
            boolean required,
            String description
    ) {
        if (type == null) {
            throw new IllegalArgumentException("Parameter type cannot be null");
        }

        this.type = type;
        this.defaultValue = defaultValue;
        this.required = required;
        this.description = description;
    }

    // Getters
    public ParameterType type() {
        return type;
    }

    public Object defaultValue() {
        return defaultValue;
    }

    public boolean required() {
        return required;
    }

    public String description() {
        return description;
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

    @Override
    public boolean equals(Object obj) {
        if (obj == this) return true;
        if (obj == null || obj.getClass() != this.getClass()) return false;
        Parameter that = (Parameter) obj;
        return Objects.equals(this.type, that.type) &&
                Objects.equals(this.defaultValue, that.defaultValue) &&
                this.required == that.required &&
                Objects.equals(this.description, that.description);
    }

    @Override
    public int hashCode() {
        return Objects.hash(type, defaultValue, required, description);
    }

    @Override
    public String toString() {
        return "Parameter[" +
                "type=" + type + ", " +
                "defaultValue=" + defaultValue + ", " +
                "required=" + required + ", " +
                "description=" + description + ']';
    }
}