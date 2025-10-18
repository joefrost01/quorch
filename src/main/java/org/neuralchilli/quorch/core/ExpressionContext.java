package org.neuralchilli.quorch.core;

import java.util.Map;

/**
 * Context for JEXL expression evaluation.
 * Provides access to parameters, environment variables, and task results.
 */
public record ExpressionContext(
        Map<String, Object> params,
        Map<String, String> env,
        Map<String, Map<String, Object>> taskResults
) {
    public ExpressionContext {
        if (params == null) {
            params = Map.of();
        }
        if (env == null) {
            env = Map.of();
        }
        if (taskResults == null) {
            taskResults = Map.of();
        }
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
}