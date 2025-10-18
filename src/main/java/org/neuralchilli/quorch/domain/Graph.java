package org.neuralchilli.quorch.domain;

import java.util.List;
import java.util.Map;

/**
 * Represents a workflow graph definition.
 * A graph is a DAG (directed acyclic graph) of tasks with dependencies.
 */
public record Graph(
        String name,
        String description,
        Map<String, Parameter> params,
        Map<String, String> env,
        String schedule,  // Cron expression, optional
        List<TaskReference> tasks
) {
    public Graph {
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

        // Defaults
        if (params == null) {
            params = Map.of();
        }
        if (env == null) {
            env = Map.of();
        }
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