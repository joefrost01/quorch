package org.neuralchilli.quorch.service;

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
public class GraphEvaluatorService {

    private static final Logger log = LoggerFactory.getLogger(GraphEvaluatorService.class);
    private static final int MAX_OPTIMISTIC_LOCK_RETRIES = 10;

    @Inject
    HazelcastInstance hazelcast;

    @Inject
    JGraphTService jGraphTService;

    @Inject
    ExpressionEvaluatorService expressionEvaluatorService;

    @Inject
    EventBus eventBus;

    @ConfigProperty(name = "orchestrator.test-mode", defaultValue = "false")
    boolean testMode;

    // State maps
    private IMap<String, Graph> graphDefinitions;
    private IMap<UUID, GraphExecution> graphExecutions;
    private IMap<UUID, TaskExecution> taskExecutions;
    IMap<TaskExecutionKey, GlobalTaskExecution> globalTasks;  // Core task state
    IMap<TaskExecutionKey, Set<UUID>> globalTaskLinks;        // Graph linkages (separate!)
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
        globalTaskLinks = hazelcast.getMap("global-task-links");
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
        log.debug("Graph execution {} params: {}", execution.id(), params);

        createTaskExecutions(execution, graph);

        if (testMode) {
            graphExecutions.flush();
            taskExecutions.flush();
            taskExecutionIndex.flush();
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
            }
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
                log.debug("Creating task execution for graph {}: taskName={}, isGlobal={}, globalKey={}",
                        graphExec.id(), taskName, isGlobal, globalKey);
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

            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
            }

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
        String resolvedKey = expressionEvaluatorService.evaluate(globalTask.key(), context);

        log.debug("Resolved global key: {} for task: {}", resolvedKey, taskRef.taskName());
        log.debug("Global key resolution details - taskName: {}, keyExpression: {}, resolvedParams: {}, resolvedKey: {}",
                taskRef.taskName(), globalTask.key(), resolvedParams, resolvedKey);
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
            // CRITICAL: Update the global task state when it completes
            TaskExecutionKey key = taskExec.globalKey();
            log.info("Updating global task state for key: {}", key);

            // Use a simpler approach - just update directly without optimistic locking
            // since we're the only one who should be completing this task
            GlobalTaskExecution globalExec = globalTasks.get(key);
            if (globalExec == null) {
                log.warn("Global task not found for key: {}", key);
            } else {
                // Update to completed status with result
                GlobalTaskExecution completedGlobal = globalExec.complete(event.result());
                globalTasks.put(key, completedGlobal);
                log.info("Successfully updated global task {} to COMPLETED", key);
            }
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
                // CRITICAL: Update the global task state when it fails
                TaskExecutionKey key = taskExec.globalKey();
                log.info("Updating global task state to FAILED for key: {}", key);

                // Use simpler approach without optimistic locking
                GlobalTaskExecution globalExec = globalTasks.get(key);
                if (globalExec == null) {
                    log.warn("Global task not found for key: {}", key);
                } else {
                    // Update to failed status with error
                    GlobalTaskExecution failedGlobal = globalExec.fail(event.error());
                    globalTasks.put(key, failedGlobal);
                    log.info("Successfully updated global task {} to FAILED", key);
                }
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
            try {
                Thread.sleep(50);
            } catch (InterruptedException e) {
            }
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

        TaskExecution taskExec = findTaskExecutionOptimized(graphExec.id(), taskName);
        if (taskExec == null) {
            log.error("Task execution not found: {} in graph: {}", taskName, graphExec.id());
            return;
        }

        TaskExecutionKey key = taskExec.globalKey();
        log.info("Global task key: {}", key);

        // ALWAYS ensure this graph is linked, regardless of task state
        synchronized (key.toString().intern()) {
            Set<UUID> currentLinks = globalTaskLinks.get(key);
            if (currentLinks == null) {
                currentLinks = new HashSet<>();
            }

            if (!currentLinks.contains(graphExec.id())) {
                Set<UUID> updatedLinks = new HashSet<>(currentLinks);
                updatedLinks.add(graphExec.id());
                globalTaskLinks.put(key, updatedLinks);

                log.info("Linked graph {} to global task {} (total linked: {})",
                        graphExec.id(), key, updatedLinks.size());
            } else {
                log.info("Graph {} already linked to global task {}", graphExec.id(), key);
            }
        }

        // Now handle the global task execution
        GlobalTaskExecution globalExec = globalTasks.get(key);

        if (globalExec == null) {
            // Try to create new global task
            log.info("Attempting to create new global task execution: {}", key);

            Task taskDef = hazelcast.<String, Task>getMap("task-definitions").get(taskName);
            if (taskDef == null) {
                log.error("Global task definition not found: {}", taskName);
                markTaskFailed(taskExec, "Global task definition not found");
                return;
            }

            TaskReference taskRef = findTaskReference(graph, taskName);
            Map<String, Object> params = resolveParameters(taskDef, taskRef, graph, graphExec);

            GlobalTaskExecution newGlobalExec = GlobalTaskExecution.create(
                    taskName,
                    key.resolvedKey(),
                    params
            ).queue();

            // Use putIfAbsent for atomic creation
            GlobalTaskExecution existing = globalTasks.putIfAbsent(key, newGlobalExec);

            if (existing == null) {
                // We created it!
                globalExec = newGlobalExec;
                log.info("Successfully created global task execution: {}", newGlobalExec.id());

                // Queue the actual work
                TaskExecution queued = taskExec.queue();
                taskExecutions.put(queued.id(), queued);

                scheduleTask(taskExec, taskDef, graphExec);
            } else {
                // Someone else created it
                globalExec = existing;
                log.info("Global task already exists with status: {}", globalExec.status());
            }
        } else {
            log.info("Global task already exists for key: {} with status: {}", key, globalExec.status());
        }

        // CRITICAL: Update our task execution based on global task status
        // This ensures that graphs linking to already-completed tasks get the right status
        if (globalExec != null && globalExec.status().isTerminal()) {
            TaskExecution updated = taskExec.withStatus(globalExec.status());
            if (globalExec.status() == TaskStatus.COMPLETED) {
                updated = taskExec.complete(globalExec.result());
                log.info("Global task already completed, marking task {} as completed", taskExec.id());
            } else if (globalExec.status() == TaskStatus.FAILED) {
                updated = taskExec.fail(globalExec.error());
                log.info("Global task already failed, marking task {} as failed", taskExec.id());
            }
            taskExecutions.put(updated.id(), updated);

            // If the global task is already done, trigger immediate evaluation
            log.info("Global task is terminal ({}), triggering immediate evaluation for graph {}",
                    globalExec.status(), graphExec.id());

            if (testMode) {
                taskExecutions.flush();
                try {
                    Thread.sleep(50);
                } catch (InterruptedException e) {
                }
            }

            triggerEvaluation(graphExec.id());
        }

        if (testMode) {
            globalTasks.flush();
            globalTaskLinks.flush();
            taskExecutions.flush();
            try {
                Thread.sleep(50);
            } catch (InterruptedException e) {
            }
        }
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
                .map(arg -> expressionEvaluatorService.evaluate(arg, context))
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
        result.putAll(expressionEvaluatorService.evaluateMap(graph.env(), context));
        result.putAll(expressionEvaluatorService.evaluateMap(taskDef.env(), context));

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
            // Get linked graphs from separate map
            Set<UUID> linkedGraphs = globalTaskLinks.get(taskExec.globalKey());
            if (linkedGraphs != null) {
                log.info("Notifying {} linked graphs for global task {} completion",
                        linkedGraphs.size(), taskExec.globalKey());
                for (UUID graphId : linkedGraphs) {
                    triggerEvaluation(graphId);
                }
            } else {
                log.warn("No linked graphs found for global task: {}", taskExec.globalKey());
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