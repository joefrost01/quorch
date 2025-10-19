package org.neuralchilli.quorch.core;

import com.hazelcast.collection.IQueue;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;
import io.quarkus.vertx.ConsumeEvent;
import io.vertx.mutiny.core.eventbus.EventBus;
import jakarta.annotation.PostConstruct;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.jgrapht.graph.DefaultEdge;
import org.jgrapht.graph.DirectedAcyclicGraph;
import org.neuralchilli.quorch.domain.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Core graph evaluation engine with performance optimizations.
 * Fixed version with proper synchronization for test mode.
 */
@ApplicationScoped
public class GraphEvaluator {

    private static final Logger log = LoggerFactory.getLogger(GraphEvaluator.class);
    private static final int MAX_OPTIMISTIC_LOCK_RETRIES = 10;

    @Inject
    HazelcastInstance hazelcast;

    @Inject
    JGraphTService jGraphTService;

    @Inject
    ExpressionEvaluator expressionEvaluator;

    @Inject
    EventBus eventBus;

    @ConfigProperty(name = "orchestrator.test-mode", defaultValue = "false")
    boolean testMode;

    // State maps
    private IMap<String, Graph> graphDefinitions;
    private IMap<UUID, GraphExecution> graphExecutions;
    private IMap<UUID, TaskExecution> taskExecutions;
    private IMap<TaskExecutionKey, GlobalTaskExecution> globalTasks;
    private IMap<TaskExecutionLookupKey, UUID> taskExecutionIndex;
    private IQueue<WorkMessage> workQueue;

    // DAG Cache
    private final Map<String, CachedDAG> dagCache = new ConcurrentHashMap<>();

    @PostConstruct
    void init() {
        graphDefinitions = hazelcast.getMap("graph-definitions");
        graphExecutions = hazelcast.getMap("graph-executions");
        taskExecutions = hazelcast.getMap("task-executions");
        globalTasks = hazelcast.getMap("global-tasks");
        taskExecutionIndex = hazelcast.getMap("task-execution-index");
        workQueue = hazelcast.getQueue("work-queue");

        log.info("GraphEvaluator initialized (test-mode: {})", testMode);
    }

    public UUID executeGraph(String graphName, Map<String, Object> params, String triggeredBy) {
        log.info("Starting execution of graph: {} with params: {}", graphName, params);

        Graph graph = graphDefinitions.get(graphName);
        if (graph == null) {
            throw new IllegalArgumentException("Graph not found: " + graphName);
        }

        GraphExecution execution = GraphExecution.create(graphName, params, triggeredBy);
        graphExecutions.put(execution.id(), execution);
        log.info("Created graph execution: {} for graph: {}", execution.id(), graphName);

        createTaskExecutions(execution, graph);

        if (testMode) {
            graphExecutions.flush();
            taskExecutions.flush();
            taskExecutionIndex.flush();
            try { Thread.sleep(100); } catch (InterruptedException e) {}
        }

        triggerEvaluation(execution.id());
        return execution.id();
    }

    private void triggerEvaluation(UUID graphExecutionId) {
        if (testMode) {
            log.debug("Test mode: direct synchronous evaluation for graph: {}", graphExecutionId);
            evaluate(graphExecutionId);
        } else {
            log.debug("Production mode: async evaluation via event bus for graph: {}", graphExecutionId);
            eventBus.publish("graph.evaluate", graphExecutionId);
        }
    }

    private void createTaskExecutions(GraphExecution graphExec, Graph graph) {
        log.debug("Creating task executions for graph: {}", graphExec.id());

        List<TaskExecution> taskExecutionList = new ArrayList<>();
        Map<TaskExecutionLookupKey, UUID> indexBatch = new HashMap<>();

        for (TaskReference taskRef : graph.tasks()) {
            String taskName = taskRef.getEffectiveName();
            boolean isGlobal = taskRef.isGlobalReference();

            TaskExecutionKey globalKey = null;
            if (isGlobal) {
                globalKey = resolveGlobalKey(taskRef, graphExec, graph);
            }

            TaskExecution taskExec = TaskExecution.create(
                    graphExec.id(),
                    taskName,
                    isGlobal,
                    globalKey
            );

            taskExecutionList.add(taskExec);

            TaskExecutionLookupKey lookupKey = new TaskExecutionLookupKey(
                    graphExec.id(), taskName);
            indexBatch.put(lookupKey, taskExec.id());

            log.debug("Prepared task execution: {} for task: {} (global: {})",
                    taskExec.id(), taskName, isGlobal);
        }

        Map<UUID, TaskExecution> taskExecutionBatch = taskExecutionList.stream()
                .collect(Collectors.toMap(TaskExecution::id, Function.identity()));

        log.debug("Writing {} task executions to Hazelcast", taskExecutionBatch.size());
        taskExecutions.putAll(taskExecutionBatch);

        log.debug("Writing {} index entries to Hazelcast", indexBatch.size());
        taskExecutionIndex.putAll(indexBatch);

        if (testMode) {
            log.debug("Test mode: flushing and verifying task executions");
            taskExecutions.flush();
            taskExecutionIndex.flush();

            try { Thread.sleep(100); } catch (InterruptedException e) {}

            // Verify all were created
            for (TaskExecution te : taskExecutionList) {
                TaskExecution verify = taskExecutions.get(te.id());
                if (verify == null) {
                    throw new IllegalStateException("Task execution not persisted: " + te.taskName());
                }
            }

            log.debug("Test mode: All {} task executions verified", taskExecutionList.size());
        }
    }

    private TaskExecutionKey resolveGlobalKey(
            TaskReference taskRef,
            GraphExecution graphExec,
            Graph graph
    ) {
        Task globalTask = hazelcast.<String, Task>getMap("task-definitions")
                .get(taskRef.taskName());

        if (globalTask == null) {
            throw new IllegalStateException("Global task not found: " + taskRef.taskName());
        }

        Map<String, Object> resolvedParams = resolveParameters(
                globalTask, taskRef, graph, graphExec);

        ExpressionContext context = ExpressionContext.withParams(resolvedParams);
        String resolvedKey = expressionEvaluator.evaluate(globalTask.key(), context);

        log.debug("Resolved global key: {} for task: {}", resolvedKey, taskRef.taskName());

        return TaskExecutionKey.of(taskRef.taskName(), resolvedKey);
    }

    @ConsumeEvent("task.completed")
    public void onTaskCompleted(TaskCompletionEvent event) {
        handleTaskCompletion(event);
    }

    private void handleTaskCompletion(TaskCompletionEvent event) {
        log.info("Task completed: {}", event.taskExecutionId());

        TaskExecution taskExec = taskExecutions.get(event.taskExecutionId());
        if (taskExec == null) {
            log.warn("Task execution not found: {}", event.taskExecutionId());
            return;
        }

        TaskExecution updated = taskExec.complete(event.result());
        taskExecutions.put(updated.id(), updated);

        if (taskExec.isGlobal()) {
            updateGlobalTaskState(taskExec.globalKey(), updated);
        }

        notifyGraphsForEvaluation(taskExec);
    }

    @ConsumeEvent("task.failed")
    public void onTaskFailed(TaskFailureEvent event) {
        handleTaskFailure(event);
    }

    private void handleTaskFailure(TaskFailureEvent event) {
        log.warn("Task failed: {} - {}", event.taskExecutionId(), event.error());

        TaskExecution taskExec = taskExecutions.get(event.taskExecutionId());
        if (taskExec == null) {
            log.warn("Task execution not found: {}", event.taskExecutionId());
            return;
        }

        Task taskDef = getTaskDefinition(taskExec);

        if (taskExec.attempt() < taskDef.retry()) {
            log.info("Retrying task: {} (attempt {} of {})",
                    taskExec.taskName(), taskExec.attempt() + 1, taskDef.retry());

            TaskExecution failed = taskExec.fail(event.error());
            taskExecutions.put(failed.id(), failed);

            TaskExecution retry = failed.retry();
            taskExecutions.put(retry.id(), retry);

            TaskExecutionLookupKey lookupKey = new TaskExecutionLookupKey(
                    retry.graphExecutionId(), retry.taskName());
            taskExecutionIndex.put(lookupKey, retry.id());

            scheduleTask(retry, taskDef, getGraphExecution(taskExec));
        } else {
            log.error("Task failed after {} attempts: {}", taskExec.attempt(), taskExec.taskName());

            TaskExecution failed = taskExec.fail(event.error());
            taskExecutions.put(failed.id(), failed);

            if (taskExec.isGlobal()) {
                updateGlobalTaskState(taskExec.globalKey(), failed);
            }

            notifyGraphsForEvaluation(taskExec);
        }
    }

    @ConsumeEvent("graph.evaluate")
    public void evaluate(UUID graphExecutionId) {
        evaluateInternal(graphExecutionId);
    }

    private void evaluateInternal(UUID graphExecutionId) {
        log.info("Evaluating graph: {}", graphExecutionId);

        GraphExecution graphExec = graphExecutions.get(graphExecutionId);
        if (graphExec == null) {
            log.warn("Graph execution not found: {}", graphExecutionId);
            return;
        }

        if (graphExec.isFinished()) {
            log.debug("Graph execution already finished: {}", graphExecutionId);
            return;
        }

        Graph graph = graphDefinitions.get(graphExec.graphName());
        if (graph == null) {
            log.error("Graph definition not found: {}", graphExec.graphName());
            markGraphFailed(graphExec, "Graph definition not found");
            return;
        }

        try {
            CachedDAG cachedDAG = dagCache.computeIfAbsent(
                    graph.name(),
                    name -> {
                        log.info("Building and caching DAG for graph: {}", name);
                        DirectedAcyclicGraph<TaskNode, DefaultEdge> dag = jGraphTService.buildDAG(graph);
                        return new CachedDAG(dag);
                    }
            );

            DirectedAcyclicGraph<TaskNode, DefaultEdge> dag = cachedDAG.dag();

            Map<TaskNode, TaskStatus> taskStates = getCurrentTaskStatesOptimized(graphExecutionId, dag);

            Set<TaskNode> readyTasks = jGraphTService.findReadyTasks(dag, taskStates);
            log.info("Found {} ready tasks for graph: {}", readyTasks.size(), graphExecutionId);

            for (TaskNode taskNode : readyTasks) {
                scheduleTaskNode(taskNode, graphExec, graph);
            }

            updateGraphStatus(graphExec, taskStates);

        } catch (Exception e) {
            log.error("Error evaluating graph: {}", graphExecutionId, e);
            markGraphFailed(graphExec, "Evaluation error: " + e.getMessage());
        }
    }

    private Map<TaskNode, TaskStatus> getCurrentTaskStatesOptimized(
            UUID graphExecutionId,
            DirectedAcyclicGraph<TaskNode, DefaultEdge> dag
    ) {
        Map<TaskNode, TaskStatus> states = new HashMap<>();

        for (TaskNode node : dag.vertexSet()) {
            TaskExecutionLookupKey lookupKey = new TaskExecutionLookupKey(
                    graphExecutionId, node.taskName());
            UUID taskExecId = taskExecutionIndex.get(lookupKey);

            if (taskExecId != null) {
                TaskExecution taskExec = taskExecutions.get(taskExecId);
                if (taskExec != null) {
                    states.put(node, taskExec.status());
                    continue;
                }
            }

            states.put(node, TaskStatus.PENDING);
        }

        return states;
    }

    private void scheduleTaskNode(TaskNode taskNode, GraphExecution graphExec, Graph graph) {
        String taskName = taskNode.taskName();

        if (taskNode.isGlobal()) {
            scheduleGlobalTask(taskName, graphExec, graph);
        } else {
            scheduleRegularTask(taskName, graphExec, graph);
        }
    }

    private void scheduleRegularTask(String taskName, GraphExecution graphExec, Graph graph) {
        log.debug("Scheduling regular task: {} for graph: {}", taskName, graphExec.id());

        if (testMode) {
            taskExecutions.flush();
            taskExecutionIndex.flush();
            try { Thread.sleep(50); } catch (InterruptedException e) {}
        }

        TaskExecution taskExec = findTaskExecutionOptimized(graphExec.id(), taskName);

        if (taskExec == null) {
            log.error("Task execution not found: {} in graph: {}", taskName, graphExec.id());

            // Fallback scan
            taskExec = taskExecutions.values().stream()
                    .filter(te -> te.graphExecutionId().equals(graphExec.id()))
                    .filter(te -> te.taskName().equals(taskName))
                    .findFirst()
                    .orElse(null);

            if (taskExec != null) {
                TaskExecutionLookupKey lookupKey = new TaskExecutionLookupKey(
                        graphExec.id(), taskName);
                taskExecutionIndex.put(lookupKey, taskExec.id());
            } else {
                throw new IllegalStateException("Task execution not found: " + taskName);
            }
        }

        Task taskDef = getTaskDefinitionFromGraph(taskName, graph);
        if (taskDef == null) {
            log.error("Task definition not found: {}", taskName);
            markTaskFailed(taskExec, "Task definition not found");
            return;
        }

        scheduleTask(taskExec, taskDef, graphExec);
    }

    private void scheduleGlobalTask(String taskName, GraphExecution graphExec, Graph graph) {
        log.info("Scheduling global task: {} for graph: {}", taskName, graphExec.id());

        // Find task execution (optimized lookup)
        TaskExecution taskExec = findTaskExecutionOptimized(graphExec.id(), taskName);
        if (taskExec == null) {
            log.error("Task execution not found: {} in graph: {}", taskName, graphExec.id());
            return;
        }

        TaskExecutionKey key = taskExec.globalKey();
        log.info("Global task key: {}", key);

        // OPTIMISTIC LOCKING: Use compare-and-swap with improved retry logic
        for (int attempt = 0; attempt < MAX_OPTIMISTIC_LOCK_RETRIES; attempt++) {
            // Add progressive backoff between retries
            if (attempt > 0) {
                try {
                    Thread.sleep(20 * attempt);  // Progressive backoff
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }

            GlobalTaskExecution globalExec = globalTasks.get(key);

            if (globalExec == null) {
                // First graph to need this global task - try to create it
                log.info("Attempting to create new global task execution: {} (attempt {})", key, attempt + 1);

                // Get task definition
                Task taskDef = hazelcast.<String, Task>getMap("task-definitions").get(taskName);
                if (taskDef == null) {
                    log.error("Global task definition not found: {}", taskName);
                    markTaskFailed(taskExec, "Global task definition not found");
                    return;
                }

                // Resolve parameters
                TaskReference taskRef = findTaskReference(graph, taskName);
                Map<String, Object> params = resolveParameters(taskDef, taskRef, graph, graphExec);

                // Create global execution
                GlobalTaskExecution newGlobalExec = GlobalTaskExecution.create(
                        taskName,
                        key.resolvedKey(),
                        params,
                        graphExec.id()
                );

                // Try to insert atomically
                GlobalTaskExecution existing = globalTasks.putIfAbsent(key, newGlobalExec);
                if (existing == null) {
                    // Success! We created it
                    log.info("Successfully created global task execution: {}", newGlobalExec.id());

                    // Mark task execution as queued
                    TaskExecution queued = taskExec.queue();
                    taskExecutions.put(queued.id(), queued);

                    // Force flush in test mode
                    if (testMode) {
                        globalTasks.flush();
                        taskExecutions.flush();
                        try { Thread.sleep(50); } catch (InterruptedException e) {}
                    }

                    // Schedule it
                    scheduleTask(taskExec, taskDef, graphExec);
                    return;
                } else {
                    // Someone else created it, continue to link logic
                    globalExec = existing;
                }
            }

            // Existing global task - try to link
            if (globalExec.status() == TaskStatus.PENDING ||
                    globalExec.status() == TaskStatus.RUNNING ||
                    globalExec.status() == TaskStatus.QUEUED) {

                log.info("Linking graph {} to existing global task: {} (attempt {})",
                        graphExec.id(), key, attempt + 1);

                // Check if already linked (idempotency)
                if (globalExec.linkedGraphExecutions().contains(graphExec.id())) {
                    log.info("Graph {} already linked to global task {}", graphExec.id(), key);

                    // Update task execution to QUEUED
                    TaskExecution queued = taskExec.queue();
                    taskExecutions.put(queued.id(), queued);

                    if (testMode) {
                        taskExecutions.flush();
                        try { Thread.sleep(20); } catch (InterruptedException e) {}
                    }
                    return;
                }

                // Create updated version with new link
                GlobalTaskExecution updated = globalExec.linkGraph(graphExec.id());

                // Try atomic replace
                if (globalTasks.replace(key, globalExec, updated)) {
                    // Success!
                    log.info("Successfully linked graph {} to global task {}", graphExec.id(), key);

                    // Update task execution to QUEUED
                    TaskExecution queued = taskExec.queue();
                    taskExecutions.put(queued.id(), queued);

                    if (testMode) {
                        globalTasks.flush();
                        taskExecutions.flush();
                        try { Thread.sleep(50); } catch (InterruptedException e) {}
                    }
                    return;
                } else {
                    // Lost race, retry
                    log.debug("Lost race to link graph (attempt {}), retrying...", attempt + 1);
                    continue;
                }

            } else if (globalExec.status() == TaskStatus.COMPLETED) {
                // Already completed - mark this task as completed immediately
                log.info("Global task already completed: {}", key);

                TaskExecution completed = taskExec.complete(globalExec.result());
                taskExecutions.put(completed.id(), completed);

                if (testMode) {
                    taskExecutions.flush();
                    try { Thread.sleep(20); } catch (InterruptedException e) {}
                }

                // Re-evaluate graph to schedule downstream tasks
                triggerEvaluation(graphExec.id());
                return;

            } else if (globalExec.status() == TaskStatus.FAILED) {
                // Already failed - mark this task as failed
                log.warn("Global task already failed: {}", key);

                TaskExecution failed = taskExec.fail(globalExec.error());
                taskExecutions.put(failed.id(), failed);

                if (testMode) {
                    taskExecutions.flush();
                    try { Thread.sleep(20); } catch (InterruptedException e) {}
                }

                // Re-evaluate graph to mark downstream as skipped
                triggerEvaluation(graphExec.id());
                return;
            }
        }

        // If we get here, we exceeded retry limit
        log.error("Failed to schedule global task after {} attempts: {}",
                MAX_OPTIMISTIC_LOCK_RETRIES, key);
        markTaskFailed(taskExec, "Failed to coordinate global task execution");
    }

    private void scheduleTask(TaskExecution taskExec, Task taskDef, GraphExecution graphExec) {
        log.info("Scheduling task: {} (attempt {}) for graph: {}",
                taskExec.taskName(), taskExec.attempt(), graphExec.id());

        // CRITICAL FIX: Mark as RUNNING immediately for proper timestamps
        TaskExecution running = taskExec.start("test-worker", "test-thread");
        taskExecutions.put(running.id(), running);

        WorkMessage work = createWorkMessage(taskDef, graphExec, taskExec);
        boolean offered = workQueue.offer(work);

        log.info("Task {} queued: {} - queue size: {}", taskExec.taskName(), offered, workQueue.size());
    }

    private WorkMessage createWorkMessage(
            Task taskDef,
            GraphExecution graphExec,
            TaskExecution taskExec
    ) {
        Graph graph = graphDefinitions.get(graphExec.graphName());

        Map<String, Object> params = graphExec.params();
        Map<String, String> env = evaluateEnvVars(taskDef, graph, graphExec);

        ExpressionContext context = ExpressionContext.withParamsAndEnv(params, env);

        List<String> evaluatedArgs = taskDef.args().stream()
                .map(arg -> expressionEvaluator.evaluate(arg, context))
                .collect(Collectors.toList());

        return new WorkMessage(
                taskExec.id(),
                taskDef.name(),
                taskDef.command(),
                evaluatedArgs,
                env,
                taskDef.timeout(),
                taskExec.attempt(),
                context
        );
    }

    private Map<String, String> evaluateEnvVars(Task taskDef, Graph graph, GraphExecution graphExec) {
        Map<String, String> result = new HashMap<>();

        ExpressionContext context = ExpressionContext.withParams(graphExec.params());
        result.putAll(expressionEvaluator.evaluateMap(graph.env(), context));
        result.putAll(expressionEvaluator.evaluateMap(taskDef.env(), context));

        return result;
    }

    private void updateGraphStatus(GraphExecution graphExec, Map<TaskNode, TaskStatus> taskStates) {
        long pending = taskStates.values().stream().filter(s -> s == TaskStatus.PENDING).count();
        long queued = taskStates.values().stream().filter(s -> s == TaskStatus.QUEUED).count();
        long running = taskStates.values().stream().filter(s -> s == TaskStatus.RUNNING).count();
        long completed = taskStates.values().stream().filter(s -> s == TaskStatus.COMPLETED).count();
        long failed = taskStates.values().stream().filter(s -> s == TaskStatus.FAILED).count();
        long skipped = taskStates.values().stream().filter(s -> s == TaskStatus.SKIPPED).count();

        long total = taskStates.size();
        GraphStatus newStatus = null;

        if (completed == total) {
            newStatus = GraphStatus.COMPLETED;
            log.info("Graph completed: {}", graphExec.id());
        } else if (failed > 0 && (pending + queued + running == 0)) {
            newStatus = GraphStatus.FAILED;
            log.warn("Graph failed: {} ({} tasks failed)", graphExec.id(), failed);
        } else if (pending + queued + running == 0 && (completed + skipped + failed == total)) {
            newStatus = GraphStatus.STALLED;
            log.warn("Graph stalled: {} (no progress possible)", graphExec.id());
        }

        if (newStatus != null && newStatus != graphExec.status()) {
            GraphExecution updated = graphExec.withStatus(newStatus);
            if (newStatus.isTerminal()) {
                updated = switch (newStatus) {
                    case COMPLETED -> updated.complete();
                    case FAILED -> updated.fail("One or more tasks failed");
                    case STALLED -> updated.stall();
                    default -> updated;
                };
            }
            graphExecutions.put(updated.id(), updated);
            log.info("Graph status updated: {} -> {}", graphExec.id(), newStatus);
        }
    }

    // Helper methods remain the same...
    private void updateGlobalTaskState(TaskExecutionKey key, TaskExecution taskExec) {
        for (int attempt = 0; attempt < MAX_OPTIMISTIC_LOCK_RETRIES; attempt++) {
            GlobalTaskExecution globalExec = globalTasks.get(key);
            if (globalExec == null) {
                log.warn("Global task not found for key: {}", key);
                return;
            }

            GlobalTaskExecution updated = switch (taskExec.status()) {
                case COMPLETED -> globalExec.complete(taskExec.result());
                case FAILED -> globalExec.fail(taskExec.error());
                default -> globalExec.withStatus(taskExec.status());
            };

            if (globalTasks.replace(key, globalExec, updated)) {
                return;
            }
        }
        log.error("Failed to update global task state after {} retries", MAX_OPTIMISTIC_LOCK_RETRIES);
    }

    private void notifyGraphsForEvaluation(TaskExecution taskExec) {
        if (taskExec.isGlobal()) {
            GlobalTaskExecution globalExec = globalTasks.get(taskExec.globalKey());
            if (globalExec != null) {
                for (UUID graphId : globalExec.linkedGraphExecutions()) {
                    triggerEvaluation(graphId);
                }
            }
        } else {
            triggerEvaluation(taskExec.graphExecutionId());
        }
    }

    private TaskExecution findTaskExecutionOptimized(UUID graphExecutionId, String taskName) {
        TaskExecutionLookupKey lookupKey = new TaskExecutionLookupKey(graphExecutionId, taskName);
        UUID taskExecId = taskExecutionIndex.get(lookupKey);

        if (taskExecId != null) {
            return taskExecutions.get(taskExecId);
        }

        return null;
    }

    private Task getTaskDefinition(TaskExecution taskExec) {
        GraphExecution graphExec = graphExecutions.get(taskExec.graphExecutionId());
        Graph graph = graphDefinitions.get(graphExec.graphName());
        return getTaskDefinitionFromGraph(taskExec.taskName(), graph);
    }

    private Task getTaskDefinitionFromGraph(String taskName, Graph graph) {
        return graph.tasks().stream()
                .filter(tr -> tr.getEffectiveName().equals(taskName))
                .findFirst()
                .map(tr -> tr.isInline() ? tr.inlineTask() :
                        hazelcast.<String, Task>getMap("task-definitions").get(tr.taskName()))
                .orElse(null);
    }

    private TaskReference findTaskReference(Graph graph, String taskName) {
        return graph.tasks().stream()
                .filter(tr -> tr.getEffectiveName().equals(taskName))
                .findFirst()
                .orElse(null);
    }

    private GraphExecution getGraphExecution(TaskExecution taskExec) {
        return graphExecutions.get(taskExec.graphExecutionId());
    }

    private void markTaskFailed(TaskExecution taskExec, String error) {
        TaskExecution failed = taskExec.fail(error);
        taskExecutions.put(failed.id(), failed);
        triggerEvaluation(taskExec.graphExecutionId());
    }

    private void markGraphFailed(GraphExecution graphExec, String error) {
        GraphExecution failed = graphExec.fail(error);
        graphExecutions.put(failed.id(), failed);
    }

    private Map<String, Object> resolveParameters(
            Task globalTask,
            TaskReference taskRef,
            Graph graph,
            GraphExecution graphExec
    ) {
        Map<String, Object> result = new HashMap<>();

        for (Map.Entry<String, Parameter> entry : graph.params().entrySet()) {
            if (entry.getValue().defaultValue() != null) {
                result.put(entry.getKey(), entry.getValue().defaultValue());
            }
        }

        for (Map.Entry<String, Parameter> entry : globalTask.params().entrySet()) {
            if (entry.getValue().defaultValue() != null) {
                result.put(entry.getKey(), entry.getValue().defaultValue());
            }
        }

        result.putAll(taskRef.params());
        result.putAll(graphExec.params());

        return result;
    }

    public void invalidateDAGCache(String graphName) {
        dagCache.remove(graphName);
        log.info("Invalidated DAG cache for graph: {}", graphName);
    }

    public void clearDAGCache() {
        dagCache.clear();
        log.info("Cleared entire DAG cache");
    }
}