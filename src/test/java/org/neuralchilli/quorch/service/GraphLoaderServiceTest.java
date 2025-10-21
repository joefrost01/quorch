package org.neuralchilli.quorch.service;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;
import io.quarkus.test.junit.QuarkusTest;
import jakarta.inject.Inject;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neuralchilli.quorch.domain.Graph;
import org.neuralchilli.quorch.domain.Task;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;

@QuarkusTest
class GraphLoaderServiceTest {

    @Inject
    GraphLoaderService loader;

    @Inject
    HazelcastInstance hazelcast;

    private Path tempDir;
    private IMap<String, Task> taskDefinitions;
    private IMap<String, Graph> graphDefinitions;

    @BeforeEach
    void setup() throws IOException {
        // Create a real temp directory
        tempDir = Files.createTempDirectory("quorch-test-");

        taskDefinitions = hazelcast.getMap("task-definitions-test");
        graphDefinitions = hazelcast.getMap("graph-definitions-test");

        // Clear any existing data
        taskDefinitions.clear();
        graphDefinitions.clear();
    }

    @AfterEach
    void cleanup() throws IOException {
        // Clean up temp directory
        if (tempDir != null && Files.exists(tempDir)) {
            Files.walk(tempDir)
                    .sorted((a, b) -> -a.compareTo(b)) // Delete files before directories
                    .forEach(path -> {
                        try {
                            Files.delete(path);
                        } catch (IOException e) {
                            // Ignore cleanup errors
                        }
                    });
        }
    }

    @Test
    void shouldLoadSimpleTask() throws IOException {
        // Given: A valid task YAML file
        String taskYaml = """
                name: test-task
                command: echo
                args:
                  - hello
                timeout: 300
                """;

        Path taskFile = tempDir.resolve("test-task.yaml");
        Files.writeString(taskFile, taskYaml);

        // When: Loading the task
        LoadResult result = loader.loadTask(taskFile);

        // Then: Task loaded successfully
        assertThat(result.isSuccess()).isTrue();
        assertThat(result.name()).isEqualTo("test-task");
    }

    @Test
    void shouldLoadGlobalTask() throws IOException {
        // Given: A valid global task
        String taskYaml = """
                name: load-data
                global: true
                key: "load_${params.date}"
                params:
                  date:
                    type: string
                    required: true
                command: python
                args:
                  - load.py
                """;

        Path taskFile = tempDir.resolve("load-data.yaml");
        Files.writeString(taskFile, taskYaml);

        // When: Loading the task
        LoadResult result = loader.loadTask(taskFile);

        // Then: Task loaded successfully
        assertThat(result.isSuccess()).isTrue();
        assertThat(result.name()).isEqualTo("load-data");
    }

    @Test
    void shouldLoadSimpleGraph() throws IOException {
        // Given: A valid graph YAML file
        String graphYaml = """
                name: test-graph
                description: "A test graph"
                tasks:
                  - name: task1
                    command: echo
                    args:
                      - hello
                """;

        Path graphFile = tempDir.resolve("test-graph.yaml");
        Files.writeString(graphFile, graphYaml);

        // When: Loading the graph
        LoadResult result = loader.loadGraph(graphFile);

        // Then: Graph loaded successfully
        assertThat(result.isSuccess()).isTrue();
        assertThat(result.name()).isEqualTo("test-graph");
    }

    @Test
    void shouldFailOnInvalidYaml() throws IOException {
        // Given: Invalid YAML
        String invalidYaml = """
                name: bad-task
                command: echo
                this is not valid yaml: [
                """;

        Path taskFile = tempDir.resolve("bad-task.yaml");
        Files.writeString(taskFile, invalidYaml);

        // When: Loading the task
        LoadResult result = loader.loadTask(taskFile);

        // Then: Load fails with error message
        assertThat(result.isSuccess()).isFalse();
        assertThat(result.error()).isPresent();
    }

    @Test
    void shouldFailOnInvalidTaskName() throws IOException {
        // Given: Task with invalid name
        String taskYaml = """
                name: Invalid_Name_With_Underscores
                command: echo
                """;

        Path taskFile = tempDir.resolve("invalid.yaml");
        Files.writeString(taskFile, taskYaml);

        // When: Loading the task
        LoadResult result = loader.loadTask(taskFile);

        // Then: Load fails with validation error
        assertThat(result.isSuccess()).isFalse();
        assertThat(result.error()).isPresent();
        assertThat(result.error().get()).contains("must match pattern");
    }

    @Test
    void shouldFailOnCircularDependency() throws IOException {
        // Given: Graph with circular dependency
        String graphYaml = """
                name: circular-graph
                tasks:
                  - name: task1
                    command: echo
                    depends_on:
                      - task2
                  - name: task2
                    command: echo
                    depends_on:
                      - task1
                """;

        Path graphFile = tempDir.resolve("circular.yaml");
        Files.writeString(graphFile, graphYaml);

        // When: Loading the graph
        LoadResult result = loader.loadGraph(graphFile);

        // Then: Load fails with circular dependency error
        assertThat(result.isSuccess()).isFalse();
        assertThat(result.error()).isPresent();
        assertThat(result.error().get()).contains("Circular dependency");
    }

    @Test
    void shouldReloadTask() throws IOException {
        // Given: A task file
        Path taskFile = tempDir.resolve("reload-task.yaml");
        String originalYaml = """
                name: reload-task
                command: echo
                args:
                  - original
                """;
        Files.writeString(taskFile, originalYaml);

        LoadResult firstLoad = loader.loadTask(taskFile);
        assertThat(firstLoad.isSuccess()).isTrue();

        // When: Updating and reloading the file
        String updatedYaml = """
                name: reload-task
                command: echo
                args:
                  - updated
                """;
        Files.writeString(taskFile, updatedYaml);

        LoadResult reload = loader.reloadTask(taskFile);

        // Then: Reload succeeds
        assertThat(reload.isSuccess()).isTrue();
        assertThat(reload.name()).isEqualTo("reload-task");
    }

    @Test
    void shouldHandleMissingFile() {
        // Given: A path that doesn't exist
        Path missingFile = tempDir.resolve("does-not-exist.yaml");

        // When: Attempting to load
        LoadResult result = loader.loadTask(missingFile);

        // Then: Load fails gracefully
        assertThat(result.isSuccess()).isFalse();
        assertThat(result.error()).isPresent();
    }

    @Test
    void shouldLoadGraphWithDependencies() throws IOException {
        // Given: A graph with task dependencies
        String graphYaml = """
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
                  - name: task3
                    command: echo
                    args:
                      - third
                    depends_on:
                      - task1
                      - task2
                """;

        Path graphFile = tempDir.resolve("dag-graph.yaml");
        Files.writeString(graphFile, graphYaml);

        // When: Loading the graph
        LoadResult result = loader.loadGraph(graphFile);

        // Then: Graph loaded successfully
        assertThat(result.isSuccess()).isTrue();
        assertThat(result.name()).isEqualTo("dag-graph");
    }
}