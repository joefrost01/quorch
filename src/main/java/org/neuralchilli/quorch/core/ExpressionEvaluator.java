package org.neuralchilli.quorch.core;

import jakarta.enterprise.context.ApplicationScoped;
import org.apache.commons.jexl3.*;
import org.apache.commons.jexl3.introspection.JexlPermissions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

/**
 * Evaluates JEXL expressions with built-in functions and context.
 * Provides safe expression evaluation with proper error handling.
 */
@ApplicationScoped
public class ExpressionEvaluator {

    private static final Logger log = LoggerFactory.getLogger(ExpressionEvaluator.class);

    private final JexlEngine jexl;
    private final DateFunctions dateFunctions;
    private final StringFunctions stringFunctions;

    public ExpressionEvaluator() {
        this.dateFunctions = new DateFunctions();
        this.stringFunctions = new StringFunctions();

        this.jexl = new JexlBuilder()
                .cache(512)  // Cache up to 512 expressions
                .strict(false)  // Allow null-safe navigation
                .silent(false)  // Log errors
                .permissions(JexlPermissions.UNRESTRICTED)  // Allow method calls
                .create();
    }

    /**
     * Evaluate an expression to a string.
     * If the expression is not wrapped in ${}, it's returned as-is.
     */
    public String evaluate(String expression, ExpressionContext context) {
        if (expression == null) {
            return null;
        }

        // If not an expression, return as-is
        if (!isExpression(expression)) {
            return expression;
        }

        try {
            Object result = evaluateExpression(expression, context);
            return result != null ? result.toString() : null;
        } catch (Exception e) {
            String msg = String.format(
                    "Failed to evaluate expression: %s - %s",
                    expression,
                    e.getMessage()
            );
            log.error(msg, e);
            throw new ExpressionException(msg, e);
        }
    }

    /**
     * Evaluate an expression to its raw object type.
     */
    public Object evaluateToObject(String expression, ExpressionContext context) {
        if (expression == null) {
            return null;
        }

        if (!isExpression(expression)) {
            return expression;
        }

        try {
            return evaluateExpression(expression, context);
        } catch (Exception e) {
            String msg = String.format(
                    "Failed to evaluate expression: %s - %s",
                    expression,
                    e.getMessage()
            );
            log.error(msg, e);
            throw new ExpressionException(msg, e);
        }
    }

    /**
     * Evaluate a map of expressions (typically environment variables)
     */
    public Map<String, String> evaluateMap(
            Map<String, String> map,
            ExpressionContext context
    ) {
        if (map == null || map.isEmpty()) {
            return Map.of();
        }

        Map<String, String> result = new HashMap<>();
        for (Map.Entry<String, String> entry : map.entrySet()) {
            String evaluated = evaluate(entry.getValue(), context);
            result.put(entry.getKey(), evaluated);
        }
        return result;
    }

    /**
     * Check if a string contains an expression
     */
    public boolean isExpression(String value) {
        return value != null && value.contains("${") && value.contains("}");
    }

    private Object evaluateExpression(String expression, ExpressionContext context) {
        // Handle embedded expressions in strings (interpolation)
        if (expression.contains("${") && !isSingleExpression(expression)) {
            return interpolateString(expression, context);
        }

        // Extract expression from ${...}
        String extracted = extractExpression(expression);

        // Create JEXL context
        JexlContext jexlContext = createJexlContext(context);

        // Compile and evaluate
        JexlExpression compiled = jexl.createExpression(extracted);
        return compiled.evaluate(jexlContext);
    }

    private boolean isSingleExpression(String expression) {
        // Check if the entire string is a single ${...} expression
        if (!expression.startsWith("${") || !expression.endsWith("}")) {
            return false;
        }

        // Make sure there's only one ${ and one } and they match
        int openCount = 0;
        int firstOpen = -1;

        for (int i = 0; i < expression.length() - 1; i++) {
            if (expression.charAt(i) == '$' && expression.charAt(i + 1) == '{') {
                openCount++;
                if (firstOpen == -1) {
                    firstOpen = i;
                }
            }
        }

        return openCount == 1 && firstOpen == 0;
    }

    private String extractExpression(String expression) {
        if (expression.startsWith("${") && expression.endsWith("}")) {
            return expression.substring(2, expression.length() - 1);
        }
        return expression;
    }

    private String interpolateString(String template, ExpressionContext context) {
        StringBuilder result = new StringBuilder();
        int pos = 0;

        while (pos < template.length()) {
            int start = template.indexOf("${", pos);
            if (start == -1) {
                result.append(template.substring(pos));
                break;
            }

            // Append text before expression
            result.append(template.substring(pos, start));

            // Find matching closing brace
            int end = findClosingBrace(template, start + 2);
            if (end == -1) {
                throw new ExpressionException(
                        "Unclosed expression in: " + template);
            }

            // Evaluate expression
            String expr = template.substring(start, end + 1);
            Object evaluated = evaluateToObject(expr, context);
            result.append(evaluated != null ? evaluated.toString() : "");

            pos = end + 1;
        }

        return result.toString();
    }

    private int findClosingBrace(String str, int start) {
        int depth = 1;
        for (int i = start; i < str.length(); i++) {
            if (str.charAt(i) == '{') {
                depth++;
            } else if (str.charAt(i) == '}') {
                depth--;
                if (depth == 0) {
                    return i;
                }
            }
        }
        return -1;
    }

    private JexlContext createJexlContext(ExpressionContext context) {
        MapContext jexlContext = new MapContext();

        // Add context variables
        jexlContext.set("params", context.params());
        jexlContext.set("env", context.env());
        jexlContext.set("task", context.taskResults());

        // Create namespace map for functions
        Map<String, Object> namespaces = new HashMap<>();
        namespaces.put("date", dateFunctions);
        namespaces.put("string", stringFunctions);

        // Add as namespaces
        jexlContext.set("date", dateFunctions);
        jexlContext.set("string", stringFunctions);

        // Add Math class for math functions
        jexlContext.set("Math", Math.class);

        return jexlContext;
    }

    /**
     * Test if an expression is valid (can be compiled)
     */
    public boolean isValid(String expression) {
        if (expression == null || !isExpression(expression)) {
            return true;
        }

        try {
            String extracted = extractExpression(expression);
            jexl.createExpression(extracted);
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    /**
     * Get a detailed error message for an invalid expression
     */
    public String getValidationError(String expression) {
        if (!isExpression(expression)) {
            return null;
        }

        try {
            String extracted = extractExpression(expression);
            jexl.createExpression(extracted);
            return null;
        } catch (Exception e) {
            return e.getMessage();
        }
    }
}