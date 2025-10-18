package org.neuralchilli.quorch.config;

import io.quarkus.test.junit.QuarkusTest;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;
import org.neuralchilli.quorch.domain.*;

import java.util.List;

import static org.assertj.core.api.Assertions.*;

@QuarkusTest
class GraphValidatorTest {

    @Inject
    GraphValidator validator;

    @Test
    void shouldAcceptValidGraph() {
        Graph graph = Graph.builder("valid-graph")
                .tasks(List.of(
                        TaskReference.inline(
                                Task.builder("task1").command("echo").build(),
                                List.of()
                        ),
                        TaskReference.inline(
                                Task.builder("task2").command("echo").build(),
                                List.of("task1")
                        )
                ))
                .build();

        // Should not throw
        assertThatCode(() -> validator.validateGraph(graph))
                .doesNotThrowAnyException();
    }

    @Test
    void shouldRejectGraphWithMissingDependency() {
        Graph graph = Graph.builder("invalid-graph")
                .tasks(List.of(
                        TaskReference.inline(
                                Task.builder("task1").command("echo").build(),
                                List.of("non-existent-task")
                        )
                ))
                .build();

        assertThatThrownBy(() -> validator.validateGraph(graph))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining("depends on 'non-existent-task'")
                .hasMessageContaining("not defined in this graph");
    }

    @Test
    void shouldRejectGraphWithCircularDependency() {
        Graph graph = Graph.builder("circular-graph")
                .tasks(List.of(
                        TaskReference.inline(
                                Task.builder("task1").command("echo").dependsOn(List.of("task2")).build(),
                                List.of("task2")
                        ),
                        TaskReference.inline(
                                Task.builder("task2").command("echo").dependsOn(List.of("task1")).build(),
                                List.of("task1")
                        )
                ))
                .build();

        assertThatThrownBy(() -> validator.validateGraph(graph))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining("Circular dependency detected");
    }

    @Test
    void shouldRejectSelfReferentialTask() {
        Graph graph = Graph.builder("self-ref-graph")
                .tasks(List.of(
                        TaskReference.inline(
                                Task.builder("task1").command("echo").dependsOn(List.of("task1")).build(),
                                List.of("task1")
                        )
                ))
                .build();

        assertThatThrownBy(() -> validator.validateGraph(graph))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining("Circular dependency detected");
    }

    @Test
    void shouldAcceptComplexDAG() {
        // Diamond pattern: task1 -> task2, task3 -> task4
        Graph graph = Graph.builder("diamond-graph")
                .tasks(List.of(
                        TaskReference.inline(
                                Task.builder("task1").command("echo").build(),
                                List.of()
                        ),
                        TaskReference.inline(
                                Task.builder("task2").command("echo").build(),
                                List.of("task1")
                        ),
                        TaskReference.inline(
                                Task.builder("task3").command("echo").build(),
                                List.of("task1")
                        ),
                        TaskReference.inline(
                                Task.builder("task4").command("echo").build(),
                                List.of("task2", "task3")
                        )
                ))
                .build();

        // Should not throw
        assertThatCode(() -> validator.validateGraph(graph))
                .doesNotThrowAnyException();
    }
}