package org.neuralchilli.quorch.domain;

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.*;

/**
 * Tests for Graph domain object.
 * Verifies construction, validation, and behavior.
 */
class GraphTest {

    @Test
    void shouldCreateValidGraph() {
        // Given: Valid graph data
        Task task = Task.builder("task1")
                .command("echo")
                .args(List.of("hello"))
                .build();

        TaskReference taskRef = TaskReference.inline(task, List.of());

        Parameter param = Parameter.withDefault(ParameterType.STRING, "default", "A parameter");

        // When: Create graph
        Graph graph = new Graph(
                "test-graph",
                "A test graph",
                Map.of("param1", param),
                Map.of("ENV_VAR", "value"),
                "0 2 * * *",
                List.of(taskRef)
        );

        // Then: All fields should be set
        assertThat(graph.name()).isEqualTo("test-graph");
        assertThat(graph.description()).isEqualTo("A test graph");
        assertThat(graph.params()).hasSize(1);
        assertThat(graph.env()).hasSize(1);
        assertThat(graph.schedule()).isEqualTo("0 2 * * *");
        assertThat(graph.tasks()).hasSize(1);
        assertThat(graph.isScheduled()).isTrue();
    }

    @Test
    void shouldCreateGraphWithMinimalFields() {
        // Given: Minimal graph data (only required fields)
        Task task = Task.builder("task1")
                .command("echo")
                .args(List.of("hello"))
                .build();

        TaskReference taskRef = TaskReference.inline(task, List.of());

        // When: Create graph with nulls for optional fields
        Graph graph = new Graph("minimal-graph", null, null, null, null, List.of(taskRef));

        // Then: Optional fields should have defaults
        assertThat(graph.name()).isEqualTo("minimal-graph");
        assertThat(graph.description()).isNull();
        assertThat(graph.params()).isEmpty();
        assertThat(graph.env()).isEmpty();
        assertThat(graph.schedule()).isNull();
        assertThat(graph.tasks()).hasSize(1);
        assertThat(graph.isScheduled()).isFalse();
    }

    @Test
    void shouldRejectNullName() {
        // Given: Task reference
        Task task = Task.builder("task1").command("echo").args(List.of("hello")).build();
        TaskReference taskRef = TaskReference.inline(task, List.of());

        // When/Then: Null name should fail
        assertThatThrownBy(() -> new Graph(null, null, null, null, null, List.of(taskRef)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("name cannot be null");
    }

    @Test
    void shouldRejectBlankName() {
        // Given: Task reference
        Task task = Task.builder("task1").command("echo").args(List.of("hello")).build();
        TaskReference taskRef = TaskReference.inline(task, List.of());

        // When/Then: Blank name should fail
        assertThatThrownBy(() -> new Graph("", null, null, null, null, List.of(taskRef)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("name cannot be null");

        assertThatThrownBy(() -> new Graph("   ", null, null, null, null, List.of(taskRef)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("name cannot be null");
    }

    @Test
    void shouldRejectInvalidNamePattern() {
        // Given: Task reference
        Task task = Task.builder("task1").command("echo").args(List.of("hello")).build();
        TaskReference taskRef = TaskReference.inline(task, List.of());

        // When/Then: Invalid patterns should fail
        assertThatThrownBy(() -> new Graph("Invalid_Name", null, null, null, null, List.of(taskRef)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("must match pattern");

        assertThatThrownBy(() -> new Graph("Invalid Name", null, null, null, null, List.of(taskRef)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("must match pattern");

        assertThatThrownBy(() -> new Graph("Invalid.Name", null, null, null, null, List.of(taskRef)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("must match pattern");
    }

    @Test
    void shouldAcceptValidNamePatterns() {
        // Given: Task reference
        Task task = Task.builder("task1").command("echo").args(List.of("hello")).build();
        TaskReference taskRef = TaskReference.inline(task, List.of());

        // When/Then: Valid patterns should succeed
        assertThatCode(() -> new Graph("valid-name", null, null, null, null, List.of(taskRef)))
                .doesNotThrowAnyException();

        assertThatCode(() -> new Graph("valid123", null, null, null, null, List.of(taskRef)))
                .doesNotThrowAnyException();

        assertThatCode(() -> new Graph("valid-name-123", null, null, null, null, List.of(taskRef)))
                .doesNotThrowAnyException();
    }

    @Test
    void shouldRejectNullTasks() {
        // When/Then: Null tasks should fail
        assertThatThrownBy(() -> new Graph("test", null, null, null, null, null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("at least one task");
    }

    @Test
    void shouldRejectEmptyTasks() {
        // When/Then: Empty tasks should fail
        assertThatThrownBy(() -> new Graph("test", null, null, null, null, List.of()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("at least one task");
    }

    @Test
    void shouldGetTaskNames() {
        // Given: Graph with multiple tasks
        Task task1 = Task.builder("task-a").command("echo").args(List.of("a")).build();
        Task task3 = Task.builder("task-c").command("echo").args(List.of("c")).build();

        TaskReference taskRef1 = TaskReference.inline(task1, List.of());
        TaskReference taskRef2 = TaskReference.toGlobal("global-task", Map.of(), List.of());
        TaskReference taskRef3 = TaskReference.inline(task3, List.of());

        Graph graph = new Graph("test", null, null, null, null, List.of(taskRef1, taskRef2, taskRef3));

        // When: Get task names
        List<String> names = graph.getTaskNames();

        // Then: Should return all task names in order
        assertThat(names).containsExactly("task-a", "global-task", "task-c");
    }

    @Test
    void shouldDetectScheduledGraph() {
        // Given: Graphs with various schedule values
        Task task = Task.builder("task1").command("echo").args(List.of("hello")).build();
        TaskReference taskRef = TaskReference.inline(task, List.of());

        Graph scheduled = new Graph("scheduled", null, null, null, "0 2 * * *", List.of(taskRef));
        Graph notScheduled = new Graph("not-scheduled", null, null, null, null, List.of(taskRef));
        Graph blankSchedule = new Graph("blank-schedule", null, null, null, "  ", List.of(taskRef));

        // Then: isScheduled should work correctly
        assertThat(scheduled.isScheduled()).isTrue();
        assertThat(notScheduled.isScheduled()).isFalse();
        assertThat(blankSchedule.isScheduled()).isFalse();
    }

    @Test
    void shouldCreateImmutableCollections() {
        // Given: Mutable collections
        Map<String, Parameter> mutableParams = new java.util.HashMap<>();
        mutableParams.put("param", Parameter.withDefault(ParameterType.STRING, "value", null));

        Map<String, String> mutableEnv = new java.util.HashMap<>();
        mutableEnv.put("KEY", "value");

        Task task = Task.builder("task1").command("echo").args(List.of("hello")).build();
        List<TaskReference> mutableTasks = new java.util.ArrayList<>();
        mutableTasks.add(TaskReference.inline(task, List.of()));

        // When: Create graph
        Graph graph = new Graph("test", null, mutableParams, mutableEnv, null, mutableTasks);

        // Then: Modifying originals should not affect graph
        mutableParams.put("param2", Parameter.withDefault(ParameterType.STRING, "value2", null));
        mutableEnv.put("KEY2", "value2");

        Task task2 = Task.builder("task2").command("echo").args(List.of("hello")).build();
        mutableTasks.add(TaskReference.inline(task2, List.of()));

        assertThat(graph.params()).hasSize(1);
        assertThat(graph.env()).hasSize(1);
        assertThat(graph.tasks()).hasSize(1);

        // And: Returned collections should be unmodifiable
        assertThatThrownBy(() -> graph.params().put("key", null))
                .isInstanceOf(UnsupportedOperationException.class);
        assertThatThrownBy(() -> graph.env().put("key", "value"))
                .isInstanceOf(UnsupportedOperationException.class);
        assertThatThrownBy(() -> graph.tasks().add(null))
                .isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    void shouldImplementEqualsCorrectly() {
        // Given: Two identical graphs
        Task task = Task.builder("task1").command("echo").args(List.of("hello")).build();
        TaskReference taskRef = TaskReference.inline(task, List.of());

        Graph graph1 = new Graph("test", "desc", Map.of(), Map.of(), null, List.of(taskRef));
        Graph graph2 = new Graph("test", "desc", Map.of(), Map.of(), null, List.of(taskRef));

        // Then: Should be equal
        assertThat(graph1).isEqualTo(graph2);
        assertThat(graph1.hashCode()).isEqualTo(graph2.hashCode());
    }

    @Test
    void shouldNotEqualDifferentGraphs() {
        // Given: Different graphs
        Task task = Task.builder("task1").command("echo").args(List.of("hello")).build();
        TaskReference taskRef = TaskReference.inline(task, List.of());

        Graph graph1 = new Graph("test1", null, null, null, null, List.of(taskRef));
        Graph graph2 = new Graph("test2", null, null, null, null, List.of(taskRef));

        // Then: Should not be equal
        assertThat(graph1).isNotEqualTo(graph2);
    }

    @Test
    void shouldWorkWithBuilder() {
        // Given: Builder
        Task task = Task.builder("task1").command("echo").args(List.of("hello")).build();
        TaskReference taskRef = TaskReference.inline(task, List.of());
        Parameter param = Parameter.required(ParameterType.STRING, "A param");

        // When: Build graph
        Graph graph = Graph.builder("builder-test")
                .description("Built with builder")
                .params(Map.of("p", param))
                .env(Map.of("E", "V"))
                .schedule("0 * * * *")
                .tasks(List.of(taskRef))
                .build();

        // Then: All fields should be set
        assertThat(graph.name()).isEqualTo("builder-test");
        assertThat(graph.description()).isEqualTo("Built with builder");
        assertThat(graph.params()).hasSize(1);
        assertThat(graph.env()).hasSize(1);
        assertThat(graph.schedule()).isEqualTo("0 * * * *");
        assertThat(graph.tasks()).hasSize(1);
    }

    @Test
    void shouldHaveReadableToString() {
        // Given: A graph
        Task task = Task.builder("task1").command("echo").args(List.of("hello")).build();
        TaskReference taskRef = TaskReference.inline(task, List.of());
        Graph graph = new Graph("test-graph", "Test", Map.of(), Map.of(), null, List.of(taskRef));

        // When: Get string representation
        String str = graph.toString();

        // Then: Should contain key information
        assertThat(str).contains("Graph");
        assertThat(str).contains("name=test-graph");
        assertThat(str).contains("description=Test");
    }

    @Test
    void shouldBeSerializable() {
        // Given: A graph
        Task task = Task.builder("task1").command("echo").args(List.of("hello")).build();
        TaskReference taskRef = TaskReference.inline(task, List.of());
        Graph graph = new Graph("test", null, null, null, null, List.of(taskRef));

        // Then: Should be serializable
        assertThat(graph).isInstanceOf(java.io.Serializable.class);
    }
}