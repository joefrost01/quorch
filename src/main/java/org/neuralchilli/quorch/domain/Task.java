package org.neuralchilli.quorch.domain;

import java.util.List;
import java.util.Map;

/**
 * Represents a task definition.
 * Tasks can be inline (defined within a graph) or global (shared across graphs).
 */
public record Task(
        String name,
        boolean global,
        String key,  // JEXL expression for global task deduplication
        Map<String, Parameter> params,
        String command,
        List<String> args,
        Map<String, String> env,
        int timeout,  // seconds
        int retry,
        List<String> dependsOn
) {
    public Task {
        // Validation
        if (name == null || name.isBlank()) {
            throw new IllegalArgumentException("Task name cannot be null or empty");
        }

        if (!name.matches("^[a-z0-9-]+$")) {
            throw new IllegalArgumentException(
                    "Task name must match pattern ^[a-z0-9-]+$, got: " + name
            );
        }

        if (command == null || command.isBlank()) {
            throw new IllegalArgumentException("Task command cannot be null or empty");
        }

        if (global && (key == null || key.isBlank())) {
            throw new IllegalArgumentException(
                    "Global tasks must have a key expression for deduplication"
            );
        }

        if (global && (params == null || params.isEmpty())) {
            throw new IllegalArgumentException(
                    "Global tasks must define parameters"
            );
        }

        // Defaults
        if (args == null) {
            args = List.of();
        }
        if (env == null) {
            env = Map.of();
        }
        if (dependsOn == null) {
            dependsOn = List.of();
        }
        if (params == null) {
            params = Map.of();
        }
        if (timeout <= 0) {
            timeout = 3600; // 1 hour default
        }
        if (retry < 0) {
            retry = 3; // default 3 retries
        }
    }

    /**
     * Builder for creating tasks fluently
     */
    public static Builder builder(String name) {
        return new Builder(name);
    }

    public static class Builder {
        private final String name;
        private boolean global = false;
        private String key;
        private Map<String, Parameter> params = Map.of();
        private String command;
        private List<String> args = List.of();
        private Map<String, String> env = Map.of();
        private int timeout = 3600;
        private int retry = 3;
        private List<String> dependsOn = List.of();

        public Builder(String name) {
            this.name = name;
        }

        public Builder global(boolean global) {
            this.global = global;
            return this;
        }

        public Builder key(String key) {
            this.key = key;
            return this;
        }

        public Builder params(Map<String, Parameter> params) {
            this.params = params;
            return this;
        }

        public Builder command(String command) {
            this.command = command;
            return this;
        }

        public Builder args(List<String> args) {
            this.args = args;
            return this;
        }

        public Builder env(Map<String, String> env) {
            this.env = env;
            return this;
        }

        public Builder timeout(int timeout) {
            this.timeout = timeout;
            return this;
        }

        public Builder retry(int retry) {
            this.retry = retry;
            return this;
        }

        public Builder dependsOn(List<String> dependsOn) {
            this.dependsOn = dependsOn;
            return this;
        }

        public Task build() {
            return new Task(name, global, key, params, command, args, env, timeout, retry, dependsOn);
        }
    }
}