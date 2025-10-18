package org.neuralchilli.quorch.config;

import io.quarkus.arc.profile.IfBuildProfile;
import io.quarkus.runtime.ShutdownEvent;
import io.quarkus.runtime.StartupEvent;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import jakarta.inject.Inject;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static java.nio.file.StandardWatchEventKinds.*;

/**
 * Watches configuration directories for changes and triggers hot reload.
 * Only active in dev mode.
 */
@ApplicationScoped
@IfBuildProfile("dev")
public class ConfigWatcher {

    private static final Logger log = LoggerFactory.getLogger(ConfigWatcher.class);

    @Inject
    GraphLoader graphLoader;

    @ConfigProperty(name = "orchestrator.config.graphs")
    String graphsPath;

    @ConfigProperty(name = "orchestrator.config.tasks")
    String tasksPath;

    @ConfigProperty(name = "orchestrator.config.watch", defaultValue = "true")
    boolean watchEnabled;

    private WatchService watchService;
    private ExecutorService executor;
    private volatile boolean running = false;

    /**
     * Start watching for file changes
     */
    void onStart(@Observes StartupEvent event) {
        if (!watchEnabled) {
            log.info("Config watching is disabled");
            return;
        }

        try {
            log.info("Starting config file watcher");
            startWatching();
        } catch (IOException e) {
            log.error("Failed to start config watcher", e);
        }
    }

    /**
     * Stop watching on shutdown
     */
    void onStop(@Observes ShutdownEvent event) {
        stopWatching();
    }

    private void startWatching() throws IOException {
        watchService = FileSystems.getDefault().newWatchService();

        Path graphsDir = Path.of(graphsPath);
        Path tasksDir = Path.of(tasksPath);

        // Register directories for watching
        if (Files.exists(graphsDir)) {
            graphsDir.register(watchService, ENTRY_CREATE, ENTRY_MODIFY, ENTRY_DELETE);
            log.info("Watching graphs directory: {}", graphsDir);
        } else {
            log.warn("Graphs directory does not exist: {}", graphsDir);
        }

        if (Files.exists(tasksDir)) {
            tasksDir.register(watchService, ENTRY_CREATE, ENTRY_MODIFY, ENTRY_DELETE);
            log.info("Watching tasks directory: {}", tasksDir);
        } else {
            log.warn("Tasks directory does not exist: {}", tasksDir);
        }

        // Start watching in background thread
        running = true;
        executor = Executors.newSingleThreadExecutor(r -> {
            Thread t = new Thread(r, "config-watcher");
            t.setDaemon(true);
            return t;
        });

        executor.submit(this::watchLoop);

        log.info("Config watcher started successfully");
    }

    private void watchLoop() {
        log.debug("Watch loop started");

        while (running) {
            try {
                WatchKey key = watchService.poll(1, TimeUnit.SECONDS);

                if (key == null) {
                    continue; // No events, check if still running
                }

                for (WatchEvent<?> event : key.pollEvents()) {
                    WatchEvent.Kind<?> kind = event.kind();

                    if (kind == OVERFLOW) {
                        log.warn("Watch event overflow - some changes may have been missed");
                        continue;
                    }

                    @SuppressWarnings("unchecked")
                    WatchEvent<Path> pathEvent = (WatchEvent<Path>) event;
                    Path changed = pathEvent.context();

                    // Only process YAML files
                    if (!isYamlFile(changed)) {
                        continue;
                    }

                    handleFileChange(kind, changed, (Path) key.watchable());
                }

                // Reset the key - important!
                boolean valid = key.reset();
                if (!valid) {
                    log.warn("Watch key no longer valid, stopping watcher");
                    break;
                }

            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                log.info("Config watcher interrupted");
                break;
            } catch (Exception e) {
                log.error("Error in watch loop", e);
            }
        }

        log.debug("Watch loop stopped");
    }

    private void handleFileChange(WatchEvent.Kind<?> kind, Path fileName, Path directory) {
        Path fullPath = directory.resolve(fileName);

        log.info("File {} detected: {}", kind.name(), fileName);

        if (kind == ENTRY_DELETE) {
            log.info("File deleted: {} (no action taken)", fileName);
            return;
        }

        // Determine if it's a graph or task based on directory
        String dirName = directory.getFileName().toString();

        try {
            LoadResult result;

            if (dirName.equals("graphs")) {
                result = graphLoader.reloadGraph(fullPath);
            } else if (dirName.equals("tasks")) {
                result = graphLoader.reloadTask(fullPath);
            } else {
                log.warn("Unknown directory: {}", dirName);
                return;
            }

            if (result.isSuccess()) {
                log.info("✓ Successfully reloaded: {}", result.name());
            } else {
                log.error("✗ Failed to reload {}: {}",
                        result.name(), result.error().orElse("unknown error"));
            }

        } catch (Exception e) {
            log.error("Error reloading file: {}", fileName, e);
        }
    }

    private boolean isYamlFile(Path path) {
        String fileName = path.toString().toLowerCase();
        return fileName.endsWith(".yaml") || fileName.endsWith(".yml");
    }

    private void stopWatching() {
        if (!watchEnabled || watchService == null) {
            return;
        }

        log.info("Stopping config watcher");
        running = false;

        if (executor != null) {
            executor.shutdown();
            try {
                if (!executor.awaitTermination(5, TimeUnit.SECONDS)) {
                    executor.shutdownNow();
                }
            } catch (InterruptedException e) {
                executor.shutdownNow();
                Thread.currentThread().interrupt();
            }
        }

        if (watchService != null) {
            try {
                watchService.close();
            } catch (IOException e) {
                log.error("Error closing watch service", e);
            }
        }

        log.info("Config watcher stopped");
    }
}