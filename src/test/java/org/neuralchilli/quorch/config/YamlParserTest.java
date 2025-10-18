package org.neuralchilli.quorch.config;

import io.quarkus.test.junit.QuarkusTest;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;
import org.neuralchilli.quorch.domain.*;

import static org.assertj.core.api.Assertions.*;

@QuarkusTest
class YamlParserTest {

    @Inject
    YamlParser parser;

    @Test
    void shouldParseSimpleTask() {
        String yaml = """
            name: my-task
            command: echo
            args:
              - hello
              - world
            timeout: 300
            """;

        Task task = parser.parseTask(yaml);

        assertThat(task.name()).isEqualTo("my-task");
        assertThat(task.command()).isEqualTo("echo");
        assertThat(task.args()).containsExactly("hello", "world");
        assertThat(task.timeout()).isEqualTo(300);
        assertThat(task.global()).isFalse();
    }

    @Test
    void shouldParseGlobalTask() {
        String yaml = """
            name: load-data
            global: true
            key: "load_${params.date}"
            params:
              date:
                type: string
                required: true
                description: "Date to load"
            command: python
            args:
              - load.py
              - --date
              - "${params.date}"
            """;

        Task task = parser.parseTask(yaml);

        assertThat(task.name()).isEqualTo("load-data");
        assertThat(task.global()).isTrue();
        assertThat(task.key()).isEqualTo("load_${params.date}");
        assertThat(task.params()).hasSize(1);
        assertThat(task.params().get("date").type()).isEqualTo(ParameterType.STRING);
        assertThat(task.params().get("date").required()).isTrue();
    }

    @Test
    void shouldParseGraphWithInlineTask() {
        String yaml = """
            name: simple-graph
            description: "A simple test graph"
            tasks:
              - name: task1
                command: echo
                args:
                  - hello
            """;

        Graph graph = parser.parseGraph(yaml);

        assertThat(graph.name()).isEqualTo("simple-graph");
        assertThat(graph.description()).isEqualTo("A simple test graph");
        assertThat(graph.tasks()).hasSize(1);
        assertThat(graph.tasks().get(0).isInline()).isTrue();
        assertThat(graph.tasks().get(0).getEffectiveName()).isEqualTo("task1");
    }

    @Test
    void shouldParseGraphWithGlobalTaskReference() {
        String yaml = """
            name: graph-with-global
            tasks:
              - task: load-data
                params:
                  date: "2025-10-17"
            """;

        Graph graph = parser.parseGraph(yaml);

        assertThat(graph.tasks()).hasSize(1);
        assertThat(graph.tasks().get(0).isGlobalReference()).isTrue();
        assertThat(graph.tasks().get(0).taskName()).isEqualTo("load-data");
        assertThat(graph.tasks().get(0).params()).containsEntry("date", "2025-10-17");
    }

    @Test
    void shouldParseGraphWithDependencies() {
        String yaml = """
            name: dag-graph
            tasks:
              - name: task1
                command: echo
                args:
                  - first
              - name: task2
                command: echo
                args:
                  - second
                depends_on:
                  - task1
            """;

        Graph graph = parser.parseGraph(yaml);

        assertThat(graph.tasks()).hasSize(2);
        assertThat(graph.tasks().get(1).dependsOn()).containsExactly("task1");
    }
}