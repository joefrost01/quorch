package org.neuralchilli.quorch.domain;

import java.io.Serial;
import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Represents a task definition.
 * Tasks can be inline (defined within a graph) or global (shared across graphs).
 */
public final class Task implements Serializable {

    @Serial
    private static final long serialVersionUID = 1L;

    private final String name;
    private final boolean global;
    private final String key;
    private final Map<String, Parameter> params;
    private final String command;
    private final List<String> args;
    private final Map<String, String> env;
    private final int timeout;
    private final int retry;
    private final List<String> dependsOn;

    public Task(
            String name,
            boolean global,
            String key,
            Map<String, Parameter> params,
            String command,
            List<String> args,
            Map<String, String> env,
            int timeout,
            int retry,
            List<String> dependsOn
    ) {
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

        // Assign with defaults
        this.name = name;
        this.global = global;
        this.key = key;
        this.params = params != null ? Map.copyOf(params) : Map.of();
        this.command = command;
        this.args = args != null ? List.copyOf(args) : List.of();
        this.env = env != null ? Map.copyOf(env) : Map.of();
        this.timeout = timeout > 0 ? timeout : 3600;
        this.retry = retry >= 0 ? retry : 3;
        this.dependsOn = dependsOn != null ? List.copyOf(dependsOn) : List.of();
    }

    // Getters
    public String name() { return name; }
    public boolean global() { return global; }
    public String key() { return key; }
    public Map<String, Parameter> params() { return params; }
    public String command() { return command; }
    public List<String> args() { return args; }
    public Map<String, String> env() { return env; }
    public int timeout() { return timeout; }
    public int retry() { return retry; }
    public List<String> dependsOn() { return dependsOn; }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) return true;
        if (obj == null || obj.getClass() != this.getClass()) return false;
        Task that = (Task) obj;
        return Objects.equals(this.name, that.name) &&
                this.global == that.global &&
                Objects.equals(this.key, that.key) &&
                Objects.equals(this.params, that.params) &&
                Objects.equals(this.command, that.command) &&
                Objects.equals(this.args, that.args) &&
                Objects.equals(this.env, that.env) &&
                this.timeout == that.timeout &&
                this.retry == that.retry &&
                Objects.equals(this.dependsOn, that.dependsOn);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, global, key, params, command, args, env, timeout, retry, dependsOn);
    }

    @Override
    public String toString() {
        return "Task[" +
                "name=" + name + ", " +
                "global=" + global + ", " +
                "key=" + key + ", " +
                "params=" + params + ", " +
                "command=" + command + ", " +
                "args=" + args + ", " +
                "env=" + env + ", " +
                "timeout=" + timeout + ", " +
                "retry=" + retry + ", " +
                "dependsOn=" + dependsOn + ']';
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