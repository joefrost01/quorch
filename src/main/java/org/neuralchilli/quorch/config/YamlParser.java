package org.neuralchilli.quorch.config;

import org.neuralchilli.quorch.domain.*;
import org.yaml.snakeyaml.Yaml;
import jakarta.enterprise.context.ApplicationScoped;

import java.io.InputStream;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Parses YAML files into domain objects (Task and Graph).
 */
@ApplicationScoped
public class YamlParser {

    private final Yaml yaml = new Yaml();

    /**
     * Parse a Task definition from YAML string
     */
    public Task parseTask(String yamlContent) {
        Map<String, Object> data = yaml.load(yamlContent);
        return parseTaskFromMap(data);
    }

    /**
     * Parse a Task definition from InputStream
     */
    public Task parseTask(InputStream inputStream) {
        Map<String, Object> data = yaml.load(inputStream);
        return parseTaskFromMap(data);
    }

    /**
     * Parse a Graph definition from YAML string
     */
    public Graph parseGraph(String yamlContent) {
        Map<String, Object> data = yaml.load(yamlContent);
        return parseGraphFromMap(data);
    }

    /**
     * Parse a Graph definition from InputStream
     */
    public Graph parseGraph(InputStream inputStream) {
        Map<String, Object> data = yaml.load(inputStream);
        return parseGraphFromMap(data);
    }

    @SuppressWarnings("unchecked")
    private Task parseTaskFromMap(Map<String, Object> data) {
        String name = getString(data, "name", true);
        boolean global = getBoolean(data, "global", false);
        String key = getString(data, "key", false);
        String command = getString(data, "command", true);
        int timeout = getInt(data, "timeout", 3600);
        int retry = getInt(data, "retry", 3);

        List<String> args = getStringList(data, "args", List.of());
        Map<String, String> env = getStringMap(data, "env", Map.of());
        List<String> dependsOn = getStringList(data, "depends_on", List.of());

        Map<String, Parameter> params = Map.of();
        if (data.containsKey("params")) {
            params = parseParameters((Map<String, Object>) data.get("params"));
        }

        return new Task(name, global, key, params, command, args, env, timeout, retry, dependsOn);
    }

    @SuppressWarnings("unchecked")
    private Graph parseGraphFromMap(Map<String, Object> data) {
        String name = getString(data, "name", true);
        String description = getString(data, "description", false);
        String schedule = getString(data, "schedule", false);

        Map<String, String> env = getStringMap(data, "env", Map.of());

        Map<String, Parameter> params = Map.of();
        if (data.containsKey("params")) {
            params = parseParameters((Map<String, Object>) data.get("params"));
        }

        List<TaskReference> tasks = parseTasks((List<Map<String, Object>>) data.get("tasks"));

        return new Graph(name, description, params, env, schedule, tasks);
    }

    @SuppressWarnings("unchecked")
    private Map<String, Parameter> parseParameters(Map<String, Object> paramsMap) {
        Map<String, Parameter> result = new HashMap<>();

        for (Map.Entry<String, Object> entry : paramsMap.entrySet()) {
            String paramName = entry.getKey();
            Map<String, Object> paramDef = (Map<String, Object>) entry.getValue();

            String typeStr = getString(paramDef, "type", true);
            ParameterType type = ParameterType.fromString(typeStr);

            Object defaultValue = paramDef.get("default");
            boolean required = getBoolean(paramDef, "required", false);
            String description = getString(paramDef, "description", false);

            result.put(paramName, new Parameter(type, defaultValue, required, description));
        }

        return result;
    }

    @SuppressWarnings("unchecked")
    private List<TaskReference> parseTasks(List<Map<String, Object>> tasksList) {
        if (tasksList == null || tasksList.isEmpty()) {
            throw new IllegalArgumentException("Graph must have at least one task");
        }

        List<TaskReference> result = new ArrayList<>();

        for (Map<String, Object> taskData : tasksList) {
            // Check if this is a reference to a global task or an inline task
            if (taskData.containsKey("task")) {
                // Global task reference
                String taskName = getString(taskData, "task", true);
                Map<String, Object> params = (Map<String, Object>) taskData.getOrDefault("params", Map.of());
                List<String> dependsOn = getStringList(taskData, "depends_on", List.of());

                result.add(TaskReference.toGlobal(taskName, params, dependsOn));
            } else {
                // Inline task definition
                Task inlineTask = parseTaskFromMap(taskData);
                List<String> dependsOn = getStringList(taskData, "depends_on", List.of());

                result.add(TaskReference.inline(inlineTask, dependsOn));
            }
        }

        return result;
    }

    // Helper methods for type-safe extraction

    private String getString(Map<String, Object> map, String key, boolean required) {
        Object value = map.get(key);
        if (value == null) {
            if (required) {
                throw new IllegalArgumentException("Missing required field: " + key);
            }
            return null;
        }
        return value.toString();
    }

    private boolean getBoolean(Map<String, Object> map, String key, boolean defaultValue) {
        Object value = map.get(key);
        if (value == null) {
            return defaultValue;
        }
        if (value instanceof Boolean) {
            return (Boolean) value;
        }
        return Boolean.parseBoolean(value.toString());
    }

    private int getInt(Map<String, Object> map, String key, int defaultValue) {
        Object value = map.get(key);
        if (value == null) {
            return defaultValue;
        }
        if (value instanceof Number) {
            return ((Number) value).intValue();
        }
        return Integer.parseInt(value.toString());
    }

    @SuppressWarnings("unchecked")
    private List<String> getStringList(Map<String, Object> map, String key, List<String> defaultValue) {
        Object value = map.get(key);
        if (value == null) {
            return defaultValue;
        }
        if (value instanceof List) {
            return ((List<?>) value).stream()
                    .map(Object::toString)
                    .collect(Collectors.toList());
        }
        return defaultValue;
    }

    @SuppressWarnings("unchecked")
    private Map<String, String> getStringMap(Map<String, Object> map, String key, Map<String, String> defaultValue) {
        Object value = map.get(key);
        if (value == null) {
            return defaultValue;
        }
        if (value instanceof Map) {
            Map<String, String> result = new HashMap<>();
            ((Map<?, ?>) value).forEach((k, v) ->
                    result.put(k.toString(), v != null ? v.toString() : null)
            );
            return result;
        }
        return defaultValue;
    }
}