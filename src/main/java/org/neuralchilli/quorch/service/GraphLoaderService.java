package org.neuralchilli.quorch.service;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;
import io.quarkus.runtime.StartupEvent;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import jakarta.inject.Inject;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.neuralchilli.quorch.config.YamlParser;
import org.neuralchilli.quorch.domain.Graph;
import org.neuralchilli.quorch.domain.Task;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

/**
 * Loads graph and task definitions from YAML files into Hazelcast.
 * Handles initial load on startup and provides reload capability.
 */
@ApplicationScoped
public class GraphLoaderService {

    private static final Logger log = LoggerFactory.getLogger(GraphLoaderService.class);

    @Inject
    YamlParser yamlParser;

    @Inject
    GraphValidatorService graphValidatorService;

    @Inject
    HazelcastInstance hazelcast;

    @ConfigProperty(name = "orchestrator.config.graphs")
    String graphsPath;

    @ConfigProperty(name = "orchestrator.config.tasks")
    String tasksPath;

    private IMap<String, Graph> graphDefinitions;
    private IMap<String, Task> taskDefinitions;

    /**
     * Initialize and load all configurations on startup
     */
    void onStart(@Observes StartupEvent event) {
        log.info("Initializing GraphLoader");

        // Get Hazelcast maps
        graphDefinitions = hazelcast.getMap("graph-definitions");
        taskDefinitions = hazelcast.getMap("task-definitions");

        // Load all configurations
        log.info("Loading tasks from: {}", tasksPath);
        List<LoadResult> taskResults = loadAllTasks();
        logResults("tasks", taskResults);

        log.info("Loading graphs from: {}", graphsPath);
        List<LoadResult> graphResults = loadAllGraphs();
        logResults("graphs", graphResults);

        log.info("GraphLoader initialization complete");
    }

    /**
     * Load all task definitions from the tasks directory
     */
    public List<LoadResult> loadAllTasks() {
        List<LoadResult> results = new ArrayList<>();

        Path tasksDir = Path.of(tasksPath);
        if (!Files.exists(tasksDir)) {
            log.warn("Tasks directory does not exist: {}", tasksPath);
            return results;
        }

        try (Stream<Path> paths = Files.walk(tasksDir)) {
            paths.filter(p -> p.toString().endsWith(".yaml") || p.toString().endsWith(".yml"))
                    .forEach(path -> results.add(loadTask(path)));
        } catch (IOException e) {
            log.error("Error scanning tasks directory: {}", tasksPath, e);
        }

        return results;
    }

    /**
     * Load all graph definitions from the graphs directory
     */
    public List<LoadResult> loadAllGraphs() {
        List<LoadResult> results = new ArrayList<>();

        Path graphsDir = Path.of(graphsPath);
        if (!Files.exists(graphsDir)) {
            log.warn("Graphs directory does not exist: {}", graphsPath);
            return results;
        }

        try (Stream<Path> paths = Files.walk(graphsDir)) {
            paths.filter(p -> p.toString().endsWith(".yaml") || p.toString().endsWith(".yml"))
                    .forEach(path -> results.add(loadGraph(path)));
        } catch (IOException e) {
            log.error("Error scanning graphs directory: {}", graphsPath, e);
        }

        return results;
    }

    /**
     * Load a single task from file
     */
    public LoadResult loadTask(Path path) {
        try {
            log.debug("Loading task from: {}", path);

            String yaml = Files.readString(path);
            Task task = yamlParser.parseTask(yaml);

            // Validate
            graphValidatorService.validateTask(task);

            // Store in Hazelcast
            taskDefinitions.put(task.name(), task);

            log.info("✓ Loaded task: {}", task.name());
            return LoadResult.success(task.name());

        } catch (IOException e) {
            log.error("✗ Failed to read task file: {}", path, e);
            return LoadResult.failure(path.getFileName().toString(), e);
        } catch (Exception e) {
            log.error("✗ Failed to load task from: {}", path, e);
            return LoadResult.failure(path.getFileName().toString(), e);
        }
    }

    /**
     * Load a single graph from file
     */
    public LoadResult loadGraph(Path path) {
        try {
            log.debug("Loading graph from: {}", path);

            String yaml = Files.readString(path);
            Graph graph = yamlParser.parseGraph(yaml);

            // Validate
            graphValidatorService.validateGraph(graph);

            // Store in Hazelcast
            graphDefinitions.put(graph.name(), graph);

            log.info("✓ Loaded graph: {} ({} tasks)", graph.name(), graph.tasks().size());
            return LoadResult.success(graph.name());

        } catch (IOException e) {
            log.error("✗ Failed to read graph file: {}", path, e);
            return LoadResult.failure(path.getFileName().toString(), e);
        } catch (Exception e) {
            log.error("✗ Failed to load graph from: {}", path, e);
            return LoadResult.failure(path.getFileName().toString(), e);
        }
    }

    /**
     * Reload a task (for hot reload)
     */
    public LoadResult reloadTask(Path path) {
        log.info("Reloading task: {}", path);
        return loadTask(path);
    }

    /**
     * Reload a graph (for hot reload)
     */
    public LoadResult reloadGraph(Path path) {
        log.info("Reloading graph: {}", path);
        return loadGraph(path);
    }

    /**
     * Get all loaded graphs
     */
    public IMap<String, Graph> getGraphs() {
        return graphDefinitions;
    }

    /**
     * Get all loaded tasks
     */
    public IMap<String, Task> getTasks() {
        return taskDefinitions;
    }

    private void logResults(String type, List<LoadResult> results) {
        long successful = results.stream().filter(LoadResult::isSuccess).count();
        long failed = results.size() - successful;

        if (failed > 0) {
            log.warn("Loaded {} {}: {} successful, {} failed",
                    results.size(), type, successful, failed);

            results.stream()
                    .filter(r -> !r.isSuccess())
                    .forEach(r -> log.error("  ✗ {}: {}", r.name(), r.error().orElse("unknown error")));
        } else {
            log.info("Loaded {} {}: all successful", results.size(), type);
        }
    }
}