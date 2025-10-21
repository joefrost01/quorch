package org.neuralchilli.quorch.domain;

import java.io.Serial;
import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Represents a workflow graph definition.
 * A graph is a DAG (directed acyclic graph) of tasks with dependencies.
 */
public final class Graph implements Serializable {

    @Serial
    private static final long serialVersionUID = 1L;

    private final String name;
    private final String description;
    private final Map<String, Parameter> params;
    private final Map<String, String> env;
    private final String schedule;
    private final List<TaskReference> tasks;

    public Graph(
            String name,
            String description,
            Map<String, Parameter> params,
            Map<String, String> env,
            String schedule,
            List<TaskReference> tasks
    ) {
        // Validation
        if (name == null || name.isBlank()) {
            throw new IllegalArgumentException("Graph name cannot be null or empty");
        }

        if (!name.matches("^[a-z0-9-]+$")) {
            throw new IllegalArgumentException(
                    "Graph name must match pattern ^[a-z0-9-]+$, got: " + name
            );
        }

        if (tasks == null || tasks.isEmpty()) {
            throw new IllegalArgumentException(
                    "Graph must have at least one task"
            );
        }

        // Assign with defaults
        this.name = name;
        this.description = description;
        this.params = params != null ? Map.copyOf(params) : Map.of();
        this.env = env != null ? Map.copyOf(env) : Map.of();
        this.schedule = schedule;
        this.tasks = List.copyOf(tasks);
    }

    // Getters
    public String name() {
        return name;
    }

    public String description() {
        return description;
    }

    public Map<String, Parameter> params() {
        return params;
    }

    public Map<String, String> env() {
        return env;
    }

    public String schedule() {
        return schedule;
    }

    public List<TaskReference> tasks() {
        return tasks;
    }

    /**
     * Get all task names in this graph (for dependency validation)
     */
    public List<String> getTaskNames() {
        return tasks.stream()
                .map(TaskReference::getEffectiveName)
                .toList();
    }

    /**
     * Check if this graph has a schedule
     */
    public boolean isScheduled() {
        return schedule != null && !schedule.isBlank();
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) return true;
        if (obj == null || obj.getClass() != this.getClass()) return false;
        Graph that = (Graph) obj;
        return Objects.equals(this.name, that.name) &&
                Objects.equals(this.description, that.description) &&
                Objects.equals(this.params, that.params) &&
                Objects.equals(this.env, that.env) &&
                Objects.equals(this.schedule, that.schedule) &&
                Objects.equals(this.tasks, that.tasks);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, description, params, env, schedule, tasks);
    }

    @Override
    public String toString() {
        return "Graph[" +
                "name=" + name + ", " +
                "description=" + description + ", " +
                "params=" + params + ", " +
                "env=" + env + ", " +
                "schedule=" + schedule + ", " +
                "tasks=" + tasks + ']';
    }

    /**
     * Builder for creating graphs fluently
     */
    public static Builder builder(String name) {
        return new Builder(name);
    }

    public static class Builder {
        private final String name;
        private String description;
        private Map<String, Parameter> params = Map.of();
        private Map<String, String> env = Map.of();
        private String schedule;
        private List<TaskReference> tasks;

        public Builder(String name) {
            this.name = name;
        }

        public Builder description(String description) {
            this.description = description;
            return this;
        }

        public Builder params(Map<String, Parameter> params) {
            this.params = params;
            return this;
        }

        public Builder env(Map<String, String> env) {
            this.env = env;
            return this;
        }

        public Builder schedule(String schedule) {
            this.schedule = schedule;
            return this;
        }

        public Builder tasks(List<TaskReference> tasks) {
            this.tasks = tasks;
            return this;
        }

        public Graph build() {
            return new Graph(name, description, params, env, schedule, tasks);
        }
    }
}