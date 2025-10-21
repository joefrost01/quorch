package org.neuralchilli.quorch.domain;

import java.io.Serial;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

/**
 * Context for JEXL expression evaluation.
 * Provides access to parameters, environment variables, and task results.
 */
public final class ExpressionContext implements Serializable {

    @Serial
    private static final long serialVersionUID = 1L;

    private final Map<String, Object> params;
    private final Map<String, String> env;
    private final Map<String, Map<String, Object>> taskResults;

    public ExpressionContext(
            Map<String, Object> params,
            Map<String, String> env,
            Map<String, Map<String, Object>> taskResults
    ) {
        this.params = params != null ? new HashMap<>(params) : Map.of();
        this.env = env != null ? new HashMap<>(env) : Map.of();
        this.taskResults = taskResults != null ? new HashMap<>(taskResults) : Map.of();
    }

    public Map<String, Object> params() {
        return params;
    }

    public Map<String, String> env() {
        return env;
    }

    public Map<String, Map<String, Object>> taskResults() {
        return taskResults;
    }

    /**
     * Create a context with only parameters
     */
    public static ExpressionContext withParams(Map<String, Object> params) {
        return new ExpressionContext(params, Map.of(), Map.of());
    }

    /**
     * Create a context with parameters and environment
     */
    public static ExpressionContext withParamsAndEnv(
            Map<String, Object> params,
            Map<String, String> env
    ) {
        return new ExpressionContext(params, env, Map.of());
    }

    /**
     * Create an empty context
     */
    public static ExpressionContext empty() {
        return new ExpressionContext(Map.of(), Map.of(), Map.of());
    }

    /**
     * Add task results to context
     */
    public ExpressionContext withTaskResults(Map<String, Map<String, Object>> taskResults) {
        return new ExpressionContext(this.params, this.env, taskResults);
    }

    /**
     * Get a task result by task name
     */
    public Map<String, Object> getTaskResult(String taskName) {
        return taskResults.getOrDefault(taskName, Map.of());
    }

    @Override
    public String toString() {
        return "ExpressionContext[params=" + params.size() +
                ", env=" + env.size() +
                ", taskResults=" + taskResults.size() + "]";
    }
}