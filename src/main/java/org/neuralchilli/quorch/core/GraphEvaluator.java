package org.neuralchilli.quorch.core;

import com.hazelcast.collection.IQueue;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;
import io.quarkus.vertx.ConsumeEvent;
import io.vertx.mutiny.core.eventbus.EventBus;
import jakarta.annotation.PostConstruct;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.jgrapht.graph.DefaultEdge;
import org.jgrapht.graph.DirectedAcyclicGraph;
import org.neuralchilli.quorch.domain.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Core graph evaluation engine.
 * Evaluates graph state, schedules ready tasks, and handles task completion events.
 *
 * This is the orchestration brain that:
 * - Builds DAGs from graph definitions
 * - Finds tasks ready to execute based on dependencies
 * - Schedules regular and global tasks
 * - Updates graph execution status
 * - Handles task completion/failure notifications
 */
@ApplicationScoped
public class GraphEvaluator {

    private static final Logger log = LoggerFactory.getLogger(GraphEvaluator.class);

    @Inject
    HazelcastInstance hazelcast;

    @Inject
    JGraphTService jGraphTService;

    @Inject
    ExpressionEvaluator expressionEvaluator;

    @Inject
    EventBus eventBus;

    // State maps
    private IMap<String, Graph> graphDefinitions;
    private IMap<UUID, GraphExecution> graphExecutions;
    private IMap<UUID, TaskExecution> taskExecutions;
    private IMap<TaskExecutionKey, GlobalTaskExecution> globalTasks;

    // Performance optimization: indexed task execution lookup
    private IMap<TaskExecutionLookupKey, UUID> taskExecutionIndex;

    // Performance optimization: cached DAGs
    private IMap<String, CachedDAG> dagCache;

    // Work queue
    private IQueue<WorkMessage> workQueue;

    @PostConstruct
    void init() {
        graphDefinitions = hazelcast.getMap("graph-definitions");
        graphExecutions = hazelcast.getMap("graph-executions");
        taskExecutions = hazelcast.getMap("task-executions");
        globalTasks = hazelcast.getMap("global-tasks");
        taskExecutionIndex = hazelcast.getMap("task-execution-index");
        dagCache = hazelcast.getMap("dag-cache");
        workQueue = hazelcast.getQueue("work-queue");

        log.info("GraphEvaluator initialized");
    }

    /**
     * Start a new graph execution.
     * Creates execution record and triggers initial evaluation.
     */
    public UUID executeGraph(String graphName, Map<String, Object> params, String triggeredBy) {
        log.info("Starting execution of graph: {} with params: {}", graphName, params);

        // Get graph definition
        Graph graph = graphDefinitions.get(graphName);
        if (graph == null) {
            throw new IllegalArgumentException("Graph not found: " + graphName);
        }

        // Create graph execution
        GraphExecution execution = GraphExecution.create(graphName, params, triggeredBy);
        graphExecutions.put(execution.id(), execution);

        log.info("Created graph execution: {} for graph: {}", execution.id(), graphName);

        // Create task executions for all tasks in graph
        createTaskExecutions(execution, graph);

        // Trigger initial evaluation
        eventBus.send("graph.evaluate", execution.id());

        return execution.id();
    }

    /**
     * Create task execution records for all tasks in the graph.
     * Initializes all tasks as PENDING.
     */
    private void createTaskExecutions(GraphExecution graphExec, Graph graph) {
        for (TaskReference taskRef : graph.tasks()) {
            String taskName = taskRef.getEffectiveName();
            boolean isGlobal = taskRef.isGlobalReference();

            TaskExecutionKey globalKey = null;
            if (isGlobal) {
                // Resolve global key for this task
                globalKey = resolveGlobalKey(taskRef, graphExec, graph);
            }

            TaskExecution taskExec = TaskExecution.create(
                    graphExec.id(),
                    taskName,
                    isGlobal,
                    globalKey
            );

            taskExecutions.put(taskExec.id(), taskExec);

            // Index for fast lookup
            TaskExecutionLookupKey lookupKey = new TaskExecutionLookupKey(
                    graphExec.id(), taskName);
            taskExecutionIndex.put(lookupKey, taskExec.id());

            log.debug("Created task execution: {} for task: {} (global: {})",
                    taskExec.id(), taskName, isGlobal);
        }
    }

    /**
     * Resolve the global key for a global task reference.
     * Evaluates the key expression with current parameters.
     */
    private TaskExecutionKey resolveGlobalKey(
            TaskReference taskRef,
            GraphExecution graphExec,
            Graph graph
    ) {
        // Get the global task definition
        Task globalTask = hazelcast.<String, Task>getMap("task-definitions")
                .get(taskRef.taskName());

        if (globalTask == null) {
            throw new IllegalStateException(
                    "Global task not found: " + taskRef.taskName());
        }

        // Merge parameters: graph defaults -> task reference params -> runtime params
        Map<String, Object> resolvedParams = resolveParameters(
                globalTask, taskRef, graph, graphExec);

        // Create expression context
        ExpressionContext context = ExpressionContext.withParams(resolvedParams);

        // Evaluate key expression
        String resolvedKey = expressionEvaluator.evaluate(globalTask.key(), context);

        log.debug("Resolved global key: {} for task: {}", resolvedKey, taskRef.taskName());

        return TaskExecutionKey.of(taskRef.taskName(), resolvedKey);
    }

    /**
     * Handle task completion event.
     * Updates task state and triggers re-evaluation of affected graphs.
     */
    @ConsumeEvent("task.completed")
    public void onTaskCompleted(TaskCompletionEvent event) {
        log.info("Task completed: {}", event.taskExecutionId());

        TaskExecution taskExec = taskExecutions.get(event.taskExecutionId());
        if (taskExec == null) {
            log.warn("Task execution not found: {}", event.taskExecutionId());
            return;
        }

        // Update task execution
        TaskExecution updated = taskExec.complete(event.result());
        taskExecutions.put(updated.id(), updated);

        // If this is a global task, update global task state
        if (taskExec.isGlobal()) {
            updateGlobalTaskState(taskExec.globalKey(), updated);
        }

        // Trigger re-evaluation of graph
        notifyGraphsForEvaluation(taskExec);
    }

    /**
     * Handle task failure event.
     * Updates task state and triggers re-evaluation or retry logic.
     */
    @ConsumeEvent("task.failed")
    public void onTaskFailed(TaskFailureEvent event) {
        log.warn("Task failed: {} - {}", event.taskExecutionId(), event.error());

        TaskExecution taskExec = taskExecutions.get(event.taskExecutionId());
        if (taskExec == null) {
            log.warn("Task execution not found: {}", event.taskExecutionId());
            return;
        }

        // Get task definition to check retry policy
        Task taskDef = getTaskDefinition(taskExec);

        // Check if we should retry
        if (taskExec.attempt() < taskDef.retry()) {
            log.info("Retrying task: {} (attempt {} of {})",
                    taskExec.taskName(), taskExec.attempt() + 1, taskDef.retry());

            // Mark current as failed
            TaskExecution failed = taskExec.fail(event.error());
            taskExecutions.put(failed.id(), failed);

            // Create retry
            TaskExecution retry = failed.retry();
            taskExecutions.put(retry.id(), retry);

            // Update index to point to retry
            TaskExecutionLookupKey lookupKey = new TaskExecutionLookupKey(
                    retry.graphExecutionId(), retry.taskName());
            taskExecutionIndex.put(lookupKey, retry.id());

            // Schedule retry immediately
            scheduleTask(retry, taskDef, getGraphExecution(taskExec));
        } else {
            log.error("Task failed after {} attempts: {}", taskExec.attempt(), taskExec.taskName());

            // Mark as permanently failed
            TaskExecution failed = taskExec.fail(event.error());
            taskExecutions.put(failed.id(), failed);

            // If global task, update global state
            if (taskExec.isGlobal()) {
                updateGlobalTaskState(taskExec.globalKey(), failed);
            }

            // Trigger re-evaluation (will mark downstream as skipped)
            notifyGraphsForEvaluation(taskExec);
        }
    }

    /**
     * Evaluate a graph execution.
     * Finds ready tasks and schedules them for execution.
     */
    @ConsumeEvent("graph.evaluate")
    public void evaluate(UUID graphExecutionId) {
        log.debug("Evaluating graph execution: {}", graphExecutionId);

        GraphExecution graphExec = graphExecutions.get(graphExecutionId);
        if (graphExec == null) {
            log.warn("Graph execution not found: {}", graphExecutionId);
            return;
        }

        // Skip if already finished
        if (graphExec.isFinished()) {
            log.debug("Graph execution already finished: {}", graphExecutionId);
            return;
        }

        // Get graph definition
        Graph graph = graphDefinitions.get(graphExec.graphName());
        if (graph == null) {
            log.error("Graph definition not found: {}", graphExec.graphName());
            markGraphFailed(graphExec, "Graph definition not found");
            return;
        }

        try {
            // Get or build DAG (cached for performance)
            DirectedAcyclicGraph<TaskNode, DefaultEdge> dag = getOrBuildDAG(graph);

            // Get current task states (optimized with index)
            Map<TaskNode, TaskStatus> taskStates = getCurrentTaskStatesOptimized(graphExecutionId, dag);

            // Find ready tasks
            Set<TaskNode> readyTasks = jGraphTService.findReadyTasks(dag, taskStates);

            log.debug("Found {} ready tasks for graph: {}", readyTasks.size(), graphExecutionId);

            // Schedule each ready task
            for (TaskNode taskNode : readyTasks) {
                scheduleTaskNode(taskNode, graphExec, graph);
            }

            // Update graph status based on current state
            updateGraphStatus(graphExec, taskStates);

        } catch (Exception e) {
            log.error("Error evaluating graph: {}", graphExecutionId, e);
            markGraphFailed(graphExec, "Evaluation error: " + e.getMessage());
        }
    }

    /**
     * Get or build cached DAG for a graph.
     * DAGs are immutable for a given graph definition, so we can cache them.
     */
    private DirectedAcyclicGraph<TaskNode, DefaultEdge> getOrBuildDAG(Graph graph) {
        CachedDAG cached = dagCache.get(graph.name());

        if (cached != null) {
            log.debug("Using cached DAG for graph: {}", graph.name());
            return cached.dag();
        }

        log.debug("Building new DAG for graph: {}", graph.name());
        DirectedAcyclicGraph<TaskNode, DefaultEdge> dag = jGraphTService.buildDAG(graph);

        // Cache it
        dagCache.put(graph.name(), new CachedDAG(dag));

        return dag;
    }

    /**
     * Get current status of all tasks in the graph (optimized version).
     * Uses index for O(1) lookups instead of O(n) scans.
     */
    private Map<TaskNode, TaskStatus> getCurrentTaskStatesOptimized(
            UUID graphExecutionId,
            DirectedAcyclicGraph<TaskNode, DefaultEdge> dag
    ) {
        Map<TaskNode, TaskStatus> states = new HashMap<>();

        for (TaskNode node : dag.vertexSet()) {
            // O(1) indexed lookup instead of O(n) scan
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

            // Fallback to PENDING if not found
            states.put(node, TaskStatus.PENDING);
        }

        return states;
    }

    /**
     * Get current status of all tasks in the graph.
     */
    private Map<TaskNode, TaskStatus> getCurrentTaskStates(
            UUID graphExecutionId,
            DirectedAcyclicGraph<TaskNode, DefaultEdge> dag
    ) {
        Map<TaskNode, TaskStatus> states = new HashMap<>();

        for (TaskNode node : dag.vertexSet()) {
            TaskStatus status = getTaskStatus(graphExecutionId, node.taskName());
            states.put(node, status);
        }

        return states;
    }

    /**
     * Get status of a specific task in a graph execution.
     */
    private TaskStatus getTaskStatus(UUID graphExecutionId, String taskName) {
        // Find task execution for this graph and task
        return taskExecutions.values().stream()
                .filter(te -> te.graphExecutionId().equals(graphExecutionId))
                .filter(te -> te.taskName().equals(taskName))
                .map(TaskExecution::status)
                .findFirst()
                .orElse(TaskStatus.PENDING);
    }

    /**
     * Schedule a task node for execution.
     */
    private void scheduleTaskNode(TaskNode taskNode, GraphExecution graphExec, Graph graph) {
        String taskName = taskNode.taskName();

        if (taskNode.isGlobal()) {
            scheduleGlobalTask(taskName, graphExec, graph);
        } else {
            scheduleRegularTask(taskName, graphExec, graph);
        }
    }

    /**
     * Schedule a regular (non-global) task.
     */
    private void scheduleRegularTask(String taskName, GraphExecution graphExec, Graph graph) {
        log.debug("Scheduling regular task: {} for graph: {}", taskName, graphExec.id());

        // Find task execution (optimized lookup)
        TaskExecution taskExec = findTaskExecutionOptimized(graphExec.id(), taskName);
        if (taskExec == null) {
            log.error("Task execution not found: {} in graph: {}", taskName, graphExec.id());
            return;
        }

        // Get task definition (inline or global)
        Task taskDef = getTaskDefinitionFromGraph(taskName, graph);
        if (taskDef == null) {
            log.error("Task definition not found: {}", taskName);
            markTaskFailed(taskExec, "Task definition not found");
            return;
        }

        scheduleTask(taskExec, taskDef, graphExec);
    }

    /**
     * Schedule a global task (with deduplication).
     */
    private void scheduleGlobalTask(String taskName, GraphExecution graphExec, Graph graph) {
        log.debug("Scheduling global task: {} for graph: {}", taskName, graphExec.id());

        // Find task execution (optimized lookup)
        TaskExecution taskExec = findTaskExecutionOptimized(graphExec.id(), taskName);
        if (taskExec == null) {
            log.error("Task execution not found: {} in graph: {}", taskName, graphExec.id());
            return;
        }

        TaskExecutionKey key = taskExec.globalKey();

        // Lock the global task key to ensure atomic operations
        globalTasks.lock(key);

        try {
            // Check if global task already exists
            GlobalTaskExecution globalExec = globalTasks.get(key);

            if (globalExec == null) {
                // First graph to need this global task - start it
                log.info("Starting new global task execution: {}", key);

                // Get task definition
                Task taskDef = hazelcast.<String, Task>getMap("task-definitions")
                        .get(taskName);

                if (taskDef == null) {
                    log.error("Global task definition not found: {}", taskName);
                    markTaskFailed(taskExec, "Global task definition not found");
                    return;
                }

                // Resolve parameters
                TaskReference taskRef = findTaskReference(graph, taskName);
                Map<String, Object> params = resolveParameters(taskDef, taskRef, graph, graphExec);

                // Create global execution
                globalExec = GlobalTaskExecution.create(
                        taskName,
                        key.resolvedKey(),
                        params,
                        graphExec.id()
                );
                globalTasks.put(key, globalExec);

                // Schedule it
                scheduleTask(taskExec, taskDef, graphExec);

            } else if (globalExec.status() == TaskStatus.PENDING ||
                    globalExec.status() == TaskStatus.RUNNING ||
                    globalExec.status() == TaskStatus.QUEUED) {
                // Already running - just link this graph
                log.info("Linking graph {} to existing global task: {}", graphExec.id(), key);

                GlobalTaskExecution linked = globalExec.linkGraph(graphExec.id());
                globalTasks.put(key, linked);

                // Update task execution to QUEUED (it's waiting on the global task)
                TaskExecution queued = taskExec.queue();
                taskExecutions.put(queued.id(), queued);

            } else if (globalExec.status() == TaskStatus.COMPLETED) {
                // Already completed - mark this task as completed immediately
                log.info("Global task already completed: {}", key);

                TaskExecution completed = taskExec.complete(globalExec.result());
                taskExecutions.put(completed.id(), completed);

                // Re-evaluate graph to schedule downstream tasks
                eventBus.send("graph.evaluate", graphExec.id());

            } else if (globalExec.status() == TaskStatus.FAILED) {
                // Already failed - mark this task as failed
                log.warn("Global task already failed: {}", key);

                TaskExecution failed = taskExec.fail(globalExec.error());
                taskExecutions.put(failed.id(), failed);

                // Re-evaluate graph to mark downstream as skipped
                eventBus.send("graph.evaluate", graphExec.id());
            }
        } finally {
            globalTasks.unlock(key);
        }
    }

    /**
     * Actually schedule a task to the work queue.
     */
    private void scheduleTask(TaskExecution taskExec, Task taskDef, GraphExecution graphExec) {
        log.info("Scheduling task: {} (attempt {}) for graph: {}",
                taskExec.taskName(), taskExec.attempt(), graphExec.id());

        // Mark as queued
        TaskExecution queued = taskExec.queue();
        taskExecutions.put(queued.id(), queued);

        // Create work message
        WorkMessage work = createWorkMessage(taskDef, graphExec, taskExec);

        // Publish to work queue
        workQueue.offer(work);

        log.debug("Task queued: {} - queue size: {}", taskExec.taskName(), workQueue.size());
    }

    /**
     * Create a work message for the worker pool.
     */
    private WorkMessage createWorkMessage(
            Task taskDef,
            GraphExecution graphExec,
            TaskExecution taskExec
    ) {
        // Get graph definition for context
        Graph graph = graphDefinitions.get(graphExec.graphName());

        // Create expression context
        Map<String, Object> params = graphExec.params();
        Map<String, String> env = evaluateEnvVars(taskDef, graph, graphExec);

        ExpressionContext context = ExpressionContext.withParamsAndEnv(params, env);

        // Evaluate args
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

    /**
     * Evaluate environment variables (graph + task).
     */
    private Map<String, String> evaluateEnvVars(Task taskDef, Graph graph, GraphExecution graphExec) {
        Map<String, String> result = new HashMap<>();

        // Start with graph env
        ExpressionContext context = ExpressionContext.withParams(graphExec.params());
        result.putAll(expressionEvaluator.evaluateMap(graph.env(), context));

        // Override with task env
        result.putAll(expressionEvaluator.evaluateMap(taskDef.env(), context));

        return result;
    }

    /**
     * Update graph execution status based on task states.
     */
    private void updateGraphStatus(GraphExecution graphExec, Map<TaskNode, TaskStatus> taskStates) {
        // Count task states
        long pending = taskStates.values().stream().filter(s -> s == TaskStatus.PENDING).count();
        long queued = taskStates.values().stream().filter(s -> s == TaskStatus.QUEUED).count();
        long running = taskStates.values().stream().filter(s -> s == TaskStatus.RUNNING).count();
        long completed = taskStates.values().stream().filter(s -> s == TaskStatus.COMPLETED).count();
        long failed = taskStates.values().stream().filter(s -> s == TaskStatus.FAILED).count();
        long skipped = taskStates.values().stream().filter(s -> s == TaskStatus.SKIPPED).count();

        long total = taskStates.size();

        GraphStatus newStatus = null;

        if (completed == total) {
            // All tasks completed
            newStatus = GraphStatus.COMPLETED;
            log.info("Graph completed: {}", graphExec.id());
        } else if (failed > 0 && (pending + queued + running == 0)) {
            // Some tasks failed and nothing left to run
            newStatus = GraphStatus.FAILED;
            log.warn("Graph failed: {} ({} tasks failed)", graphExec.id(), failed);
        } else if (pending + queued + running == 0 && (completed + skipped + failed == total)) {
            // Nothing running but not all completed - stalled
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

    // Helper methods

    private void updateGlobalTaskState(TaskExecutionKey key, TaskExecution taskExec) {
        GlobalTaskExecution globalExec = globalTasks.get(key);
        if (globalExec != null) {
            GlobalTaskExecution updated = switch (taskExec.status()) {
                case COMPLETED -> globalExec.complete(taskExec.result());
                case FAILED -> globalExec.fail(taskExec.error());
                default -> globalExec.withStatus(taskExec.status());
            };
            globalTasks.put(key, updated);
        }
    }

    private void notifyGraphsForEvaluation(TaskExecution taskExec) {
        if (taskExec.isGlobal()) {
            // Notify all linked graphs
            GlobalTaskExecution globalExec = globalTasks.get(taskExec.globalKey());
            if (globalExec != null) {
                for (UUID graphId : globalExec.linkedGraphExecutions()) {
                    eventBus.send("graph.evaluate", graphId);
                }
            }
        } else {
            // Notify this graph only
            eventBus.send("graph.evaluate", taskExec.graphExecutionId());
        }
    }

    /**
     * Find task execution using optimized index lookup.
     */
    private TaskExecution findTaskExecutionOptimized(UUID graphExecutionId, String taskName) {
        TaskExecutionLookupKey lookupKey = new TaskExecutionLookupKey(graphExecutionId, taskName);
        UUID taskExecId = taskExecutionIndex.get(lookupKey);

        if (taskExecId != null) {
            return taskExecutions.get(taskExecId);
        }

        return null;
    }

    private TaskExecution findTaskExecution(UUID graphExecutionId, String taskName) {
        return taskExecutions.values().stream()
                .filter(te -> te.graphExecutionId().equals(graphExecutionId))
                .filter(te -> te.taskName().equals(taskName))
                .findFirst()
                .orElse(null);
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
        eventBus.send("graph.evaluate", taskExec.graphExecutionId());
    }

    private void markGraphFailed(GraphExecution graphExec, String error) {
        GraphExecution failed = graphExec.fail(error);
        graphExecutions.put(failed.id(), failed);
    }

    /**
     * Resolve parameters with proper scoping hierarchy:
     * 1. Runtime invocation
     * 2. Graph task reference
     * 3. Global task defaults
     * 4. Graph defaults
     */
    private Map<String, Object> resolveParameters(
            Task globalTask,
            TaskReference taskRef,
            Graph graph,
            GraphExecution graphExec
    ) {
        Map<String, Object> result = new HashMap<>();

        // Level 4: Graph defaults
        for (Map.Entry<String, Parameter> entry : graph.params().entrySet()) {
            if (entry.getValue().defaultValue() != null) {
                result.put(entry.getKey(), entry.getValue().defaultValue());
            }
        }

        // Level 3: Global task defaults
        for (Map.Entry<String, Parameter> entry : globalTask.params().entrySet()) {
            if (entry.getValue().defaultValue() != null) {
                result.put(entry.getKey(), entry.getValue().defaultValue());
            }
        }

        // Level 2: Task reference params
        result.putAll(taskRef.params());

        // Level 1: Runtime params (highest priority)
        result.putAll(graphExec.params());

        return result;
    }

    /**
     * Lookup key for task execution index.
     */
    public record TaskExecutionLookupKey(
            UUID graphExecutionId,
            String taskName
    ) implements Serializable {}

    /**
     * Cached DAG wrapper for serialization.
     */
    public record CachedDAG(
            DirectedAcyclicGraph<TaskNode, DefaultEdge> dag
    ) implements Serializable {}
}