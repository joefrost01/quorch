## Deployment

### Database Migrations

**Flyway Configuration**:

```properties
# application.properties
quarkus.flyway.migrate-at-start=true
quarkus.flyway.locations=classpath:db/migration
```

**Migration Files**:

```sql
-- V1__initial_schema.sql
CREATE TABLE graphs (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name VARCHAR(255) UNIQUE NOT NULL,
    description TEXT,
    yaml_content TEXT NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP NOT NULL DEFAULT NOW(),
    CONSTRAINT graphs_name_format CHECK (name ~ '^[a-z0-9-]+# GKE Event-Driven Orchestrator Design Document

## Table of Contents
1. [Overview](#overview)
2. [Architecture](#architecture)
3. [Data Model](#data-model)
4. [Configuration Schema](#configuration-schema)
5. [Core Components](#core-components)
6. [Interfaces and Abstractions](#interfaces-and-abstractions)
7. [Environment Profiles](#environment-profiles)
8. [Deployment](#deployment)
9. [Development Workflow](#development-workflow)
10. [Testing Strategy](#testing-strategy)
11. [Implementation Phases](#implementation-phases)

## Overview

### Purpose
Build a Quarkus-based workflow orchestrator running on Google Kubernetes Engine (GKE) that enables event-driven execution of data pipeline tasks. The system provides visualization of workflow state similar to Apache Airflow, with a focus on simplicity and developer experience.

### Key Requirements
- Event-driven architecture using Pub/Sub for task coordination
- SSR-based UI for workflow visualization (no React)
- Worker pods that pull tasks from queues
- Support for DBT, BigQuery Extract, Dataplex, and Dataflow tasks
- Management topic for worker lifecycle commands
- Worker heartbeat mechanism with automatic work recovery
- State persistence in Cloud SQL (Postgres)
- Graph evaluation only on event arrival (not polling)
- JGraphT for DAG management
- Support for global tasks (shared across multiple graphs)
- Repository pattern for database abstraction
- Queue abstraction for local development

### Technology Stack
- **Framework**: Quarkus with Vert.x event bus
- **Graph Library**: JGraphT
- **Database**: Cloud SQL Postgres (production), H2 (local dev)
- **Messaging**: Google Cloud Pub/Sub (production), Hazelcast (local dev)
- **Container Orchestration**: GKE with HPA
- **UI**: Qute templates (SSR)
- **Language**: Java 21+

## Architecture

### System Context Diagram

```
┌─────────────────────────────────────────────────────────────────┐
│                         GKE Cluster                             │
│                                                                 │
│  ┌────────────────────┐           ┌─────────────────────────┐   │
│  │   Orchestrator     │           │   Worker Pods (HPA)     │   │
│  │    (Quarkus)       │           │                         │   │
│  │                    │           │  ┌────────┐ ┌────────┐  │   │
│  │  - Graph Loader    │           │  │Worker 1│ │Worker 2│  │   │
│  │  - Event Consumer  │           │  │        │ │        │  │   │
│  │  - Graph Evaluator │           │  └────────┘ └────────┘  │   │
│  │  - Worker Monitor  │           │         ...             │   │
│  │  - UI (SSR)        │           │                         │   │
│  │  - REST API        │           │  Each worker:           │   │
│  │                    │           │  - Pulls from queue     │   │
│  └────────────────────┘           │  - Executes tasks       │   │
│           │                       │  - Sends heartbeats     │   │
│           │                       │  - Publishes results    │   │
│           │                       └─────────────────────────┘   │
└───────────┼─────────────────────────────────────────────────────┘
            │
    ┌───────┴────────┐
    ▼                ▼
┌─────────┐    ┌──────────────┐      ┌──────────────────┐
│Cloud SQL│    │  Pub/Sub     │      │  Config Files    │
│Postgres │    │              │      │  (ConfigMap)     │
│         │    │ ┌──────────┐ │      │                  │
│State &  │    │ │Task Queue│ │      │  graphs/*.yaml   │
│Metadata │    │ └──────────┘ │      │  tasks/*.yaml    │
└─────────┘    │ ┌──────────┐ │      └──────────────────┘
               │ │Management│ │
               │ │  Topic   │ │
               │ └──────────┘ │
               │ ┌──────────┐ │
               │ │  Events  │ │
               │ │  Topic   │ │
               │ └──────────┘ │
               └──────────────┘
```

### Component Interaction Flow

#### Graph Evaluation Flow
```
1. External Event arrives (task completed/failed)
   ↓
2. ExternalEventConsumer bridges to internal event bus
   ↓
3. GraphEvaluator receives internal event
   ↓
4. Update task status in repository
   ↓
5. If global task → find all affected graphs
   ↓
6. Build DAG using JGraphT
   ↓
7. Determine ready tasks (all dependencies met)
   ↓
8. For each ready task:
   - Create TaskExecution record
   - Publish "task.ready" event
     ↓
9. TaskQueuePublisher receives "task.ready"
   ↓
10. Publish WorkMessage to queue (Pub/Sub or Hazelcast)
    ↓
11. Worker pulls message
    ↓
12. Worker executes task
    ↓
13. Worker publishes result to events topic
    ↓
14. Loop back to step 1
```

#### Worker Lifecycle Flow
```
1. Worker pod starts
   ↓
2. Worker sends registration message to management topic
   ↓
3. WorkerMonitor creates worker record
   ↓
4. Worker starts heartbeat timer (every 10s)
   ↓
5. WorkerMonitor checks heartbeats (every 30s)
   ↓
6. If no heartbeat for 60s:
   - Mark worker as dead
   - Publish "worker.died" event
     ↓
7. WorkRecoveryService receives "worker.died"
   ↓
8. Find all assigned work for dead worker
   ↓
9. For each orphaned task:
   - If retries remaining → publish "task.retry"
   - Else → mark as failed
     ↓
10. GraphEvaluator re-evaluates affected graphs
```

## Data Model

### Database Schema

All timestamps are stored in UTC. The schema uses UUID for primary keys to avoid conflicts in distributed systems.

#### Tables

##### `graphs`
Stores graph definitions loaded from YAML configuration files.

```sql
CREATE TABLE graphs (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name VARCHAR(255) UNIQUE NOT NULL,
    description TEXT,
    yaml_content TEXT NOT NULL,  -- Original YAML for reference/debugging
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP NOT NULL DEFAULT NOW(),
    CONSTRAINT graphs_name_format CHECK (name ~ '^[a-z0-9-]+$')
);

CREATE INDEX idx_graphs_name ON graphs(name);
```

##### `tasks`
Task definitions, both global (shared across graphs) and graph-specific.

```sql
CREATE TABLE tasks (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name VARCHAR(255) UNIQUE NOT NULL,
    type VARCHAR(50) NOT NULL,  -- 'dbt', 'bq_extract', 'dataplex', 'dataflow'
    config JSONB NOT NULL,
    retry_policy JSONB NOT NULL DEFAULT '{"max_retries": 3, "backoff": "exponential", "initial_delay_seconds": 30}'::jsonb,
    timeout_seconds INT NOT NULL DEFAULT 3600,
    is_global BOOLEAN NOT NULL DEFAULT FALSE,
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP NOT NULL DEFAULT NOW(),
    CONSTRAINT tasks_name_format CHECK (name ~ '^[a-z0-9-]+$'),
    CONSTRAINT tasks_type_valid CHECK (type IN ('dbt', 'bq_extract', 'dataplex', 'dataflow')),
    CONSTRAINT tasks_timeout_positive CHECK (timeout_seconds > 0)
);

CREATE INDEX idx_tasks_name ON tasks(name);
CREATE INDEX idx_tasks_global ON tasks(name) WHERE is_global = TRUE;
CREATE INDEX idx_tasks_type ON tasks(type);
```

##### `graph_edges`
Defines the DAG structure within each graph (task dependencies).

```sql
CREATE TABLE graph_edges (
    graph_id UUID NOT NULL REFERENCES graphs(id) ON DELETE CASCADE,
    from_task_id UUID NOT NULL REFERENCES tasks(id) ON DELETE CASCADE,
    to_task_id UUID NOT NULL REFERENCES tasks(id) ON DELETE CASCADE,
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    PRIMARY KEY (graph_id, from_task_id, to_task_id),
    CONSTRAINT graph_edges_no_self_reference CHECK (from_task_id != to_task_id)
);

CREATE INDEX idx_graph_edges_graph ON graph_edges(graph_id);
CREATE INDEX idx_graph_edges_from ON graph_edges(from_task_id);
CREATE INDEX idx_graph_edges_to ON graph_edges(to_task_id);
```

##### `graph_executions`
Tracks execution instances of graphs.

```sql
CREATE TABLE graph_executions (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    graph_id UUID NOT NULL REFERENCES graphs(id) ON DELETE CASCADE,
    status VARCHAR(20) NOT NULL DEFAULT 'running',
    started_at TIMESTAMP NOT NULL DEFAULT NOW(),
    completed_at TIMESTAMP,
    triggered_by VARCHAR(255) NOT NULL,  -- Event ID or system trigger
    error_message TEXT,
    CONSTRAINT graph_executions_status_valid CHECK (
        status IN ('running', 'completed', 'failed', 'stalled')
    ),
    CONSTRAINT graph_executions_completed_when_done CHECK (
        (status IN ('completed', 'failed') AND completed_at IS NOT NULL) OR
        (status IN ('running', 'stalled') AND completed_at IS NULL)
    )
);

CREATE INDEX idx_graph_executions_graph ON graph_executions(graph_id);
CREATE INDEX idx_graph_executions_status ON graph_executions(status);
CREATE INDEX idx_graph_executions_started ON graph_executions(started_at DESC);
```

##### `task_executions`
Tracks individual task executions within graph executions.

```sql
CREATE TABLE task_executions (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    graph_execution_id UUID NOT NULL REFERENCES graph_executions(id) ON DELETE CASCADE,
    task_id UUID NOT NULL REFERENCES tasks(id) ON DELETE CASCADE,
    status VARCHAR(20) NOT NULL DEFAULT 'pending',
    attempt INT NOT NULL DEFAULT 0,
    assigned_worker_id VARCHAR(255),
    queued_at TIMESTAMP,
    started_at TIMESTAMP,
    completed_at TIMESTAMP,
    error_message TEXT,
    result JSONB,
    -- For global tasks: link to the canonical execution
    global_execution_id UUID REFERENCES task_executions(id),
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    CONSTRAINT task_executions_unique_per_graph UNIQUE(graph_execution_id, task_id),
    CONSTRAINT task_executions_status_valid CHECK (
        status IN ('pending', 'queued', 'running', 'completed', 'failed', 'skipped')
    ),
    CONSTRAINT task_executions_attempt_positive CHECK (attempt >= 0),
    CONSTRAINT task_executions_timestamps_ordered CHECK (
        (queued_at IS NULL OR started_at IS NULL OR queued_at <= started_at) AND
        (started_at IS NULL OR completed_at IS NULL OR started_at <= completed_at)
    )
);

CREATE INDEX idx_task_executions_graph ON task_executions(graph_execution_id);
CREATE INDEX idx_task_executions_task ON task_executions(task_id);
CREATE INDEX idx_task_executions_status ON task_executions(status);
CREATE INDEX idx_task_executions_worker ON task_executions(assigned_worker_id) 
    WHERE assigned_worker_id IS NOT NULL;
CREATE INDEX idx_task_executions_global ON task_executions(global_execution_id) 
    WHERE global_execution_id IS NOT NULL;
CREATE INDEX idx_task_executions_running_global ON task_executions(task_id) 
    WHERE status = 'running' AND global_execution_id IS NULL;
```

##### `workers`
Registry of active and dead workers.

```sql
CREATE TABLE workers (
    id VARCHAR(255) PRIMARY KEY,  -- Kubernetes pod name
    status VARCHAR(20) NOT NULL DEFAULT 'active',
    last_heartbeat TIMESTAMP NOT NULL DEFAULT NOW(),
    registered_at TIMESTAMP NOT NULL DEFAULT NOW(),
    capabilities JSONB NOT NULL DEFAULT '{}'::jsonb,  -- For future heterogeneous workers
    metadata JSONB NOT NULL DEFAULT '{}'::jsonb,  -- Pod IP, node name, etc.
    CONSTRAINT workers_status_valid CHECK (status IN ('active', 'dead'))
);

CREATE INDEX idx_workers_status ON workers(status);
CREATE INDEX idx_workers_heartbeat ON workers(last_heartbeat) WHERE status = 'active';
```

##### `work_assignments`
Tracks which worker is assigned to which task (for failure recovery).

```sql
CREATE TABLE work_assignments (
    task_execution_id UUID PRIMARY KEY REFERENCES task_executions(id) ON DELETE CASCADE,
    worker_id VARCHAR(255) NOT NULL REFERENCES workers(id) ON DELETE CASCADE,
    assigned_at TIMESTAMP NOT NULL DEFAULT NOW(),
    heartbeat_deadline TIMESTAMP NOT NULL,
    CONSTRAINT work_assignments_deadline_future CHECK (heartbeat_deadline > assigned_at)
);

CREATE INDEX idx_work_assignments_worker ON work_assignments(worker_id);
CREATE INDEX idx_work_assignments_deadline ON work_assignments(heartbeat_deadline);
```

### Entity Relationships

```
graphs 1──────* graph_edges *──────1 tasks
  │                                    │
  │                                    │
  │ 1                                  │ 1
  │                                    │
  * graph_executions                   * task_executions
        │                                    │
        │ 1                                  │ *
        │                                    │
        *────────────────────────────────────*
                                             │
                                             │ 1
                                             │
                                             * work_assignments * ────1 workers
```

### Enums and Constants

```java
public enum TaskType {
    DBT("dbt"),
    BQ_EXTRACT("bq_extract"),
    DATAPLEX("dataplex"),
    DATAFLOW("dataflow");
    
    private final String value;
}

public enum TaskStatus {
    PENDING,    // Created but not yet queued
    QUEUED,     // Published to queue
    RUNNING,    // Worker is executing
    COMPLETED,  // Successfully finished
    FAILED,     // Failed (may retry)
    SKIPPED     // Skipped due to upstream failure
}

public enum GraphStatus {
    RUNNING,    // In progress
    COMPLETED,  // All tasks completed
    FAILED,     // One or more tasks failed permanently
    STALLED     // No tasks can make progress
}

public enum WorkerStatus {
    ACTIVE,     // Receiving heartbeats
    DEAD        // No heartbeat for >60s
}

public class Constants {
    public static final int WORKER_HEARTBEAT_INTERVAL_SECONDS = 10;
    public static final int WORKER_DEAD_THRESHOLD_SECONDS = 60;
    public static final int WORKER_MONITOR_CHECK_INTERVAL_SECONDS = 30;
    public static final String CONFIG_PATH = "/config";
    public static final String GRAPHS_PATTERN = "graphs/*.yaml";
    public static final String TASKS_PATTERN = "tasks/*.yaml";
}
```

## Configuration Schema

### Task Definition YAML

Task definitions are reusable components that can be referenced by multiple graphs.

**Location**: `tasks/{task-name}.yaml`

**Schema**:
```yaml
# Unique task name (must match filename without .yaml extension)
name: string                # Required. Pattern: ^[a-z0-9-]+$

# Task type determines which handler executes it
type: enum                  # Required. One of: dbt, bq_extract, dataplex, dataflow

# Is this task shared across multiple graphs?
global: boolean             # Optional. Default: false

# Retry configuration
retry_policy:               # Optional
  max_retries: integer      # Default: 3
  backoff: enum             # Default: exponential. One of: exponential, linear, constant
  initial_delay_seconds: integer  # Default: 30
  max_delay_seconds: integer      # Optional. Default: 3600

# Maximum execution time
timeout_seconds: integer    # Optional. Default: 3600

# Task-specific configuration (passed to task handler)
config: object              # Required. Structure depends on task type
```

**Example - DBT Task**:
```yaml
# tasks/load-market-data.yaml
name: load-market-data
type: dbt
global: true
retry_policy:
  max_retries: 3
  backoff: exponential
  initial_delay_seconds: 30
timeout_seconds: 600
config:
  project: analytics
  models: +market_data
  vars:
    region: us
    refresh_mode: full
  target: prod
```

**Example - BigQuery Extract Task**:
```yaml
# tasks/extract-prices.yaml
name: extract-prices
type: bq_extract
timeout_seconds: 300
config:
  project: my-project
  dataset: raw_data
  table: prices
  destination: gs://my-bucket/prices/*.parquet
  format: parquet
  compression: snappy
```

**Example - Dataplex Task**:
```yaml
# tasks/run-quality-checks.yaml
name: run-quality-checks
type: dataplex
timeout_seconds: 1800
config:
  project: my-project
  location: us-central1
  lake: analytics-lake
  zone: raw-zone
  scan: market-data-quality
  wait_for_completion: true
```

**Example - Dataflow Task**:
```yaml
# tasks/aggregate-metrics.yaml
name: aggregate-metrics
type: dataflow
timeout_seconds: 3600
retry_policy:
  max_retries: 2
  backoff: exponential
config:
  project: my-project
  region: us-central1
  template: gs://my-bucket/templates/aggregate-market-metrics
  temp_location: gs://my-bucket/temp
  parameters:
    input: gs://my-bucket/prices/*.parquet
    output: my-project:analytics.market_metrics
    window_size: 1h
```

### Graph Definition YAML

Graphs define workflows as DAGs of tasks.

**Location**: `graphs/{graph-name}.yaml`

**Schema**:
```yaml
# Unique graph name (must match filename without .yaml extension)
name: string                # Required. Pattern: ^[a-z0-9-]+$

# Human-readable description (supports multi-line)
description: string         # Optional

# List of tasks in this graph
tasks: array                # Required. At least one task
  - # Option 1: Reference to existing task definition
    task: string            # Task name from tasks/*.yaml
    
  - # Option 2: Inline task definition
    name: string            # Required
    type: enum              # Required
    timeout_seconds: integer
    retry_policy: object
    config: object
    depends_on: array       # Optional. List of task names this depends on
      - string
```

**Example - Market Pipeline**:
```yaml
# graphs/market-pipeline.yaml
name: market-pipeline
description: |
  Daily market data processing pipeline.
  Runs after market close to process and aggregate trading data.
  
  Steps:
  1. Load raw market data (DBT)
  2. Extract prices to GCS (BigQuery)
  3. Run data quality checks (Dataplex)
  4. Aggregate metrics (Dataflow)

tasks:
  # Reference to global task
  - task: load-market-data
  
  # Inline task with dependencies
  - name: extract-prices
    type: bq_extract
    timeout_seconds: 300
    config:
      project: my-project
      dataset: raw_data
      table: prices
      destination: gs://my-bucket/prices/*.parquet
      format: parquet
    depends_on:
      - load-market-data
  
  - name: run-dataplex-quality
    type: dataplex
    timeout_seconds: 1800
    config:
      project: my-project
      lake: analytics-lake
      scan: market-data-quality
    depends_on:
      - extract-prices
  
  - name: aggregate-metrics
    type: dataflow
    timeout_seconds: 3600
    config:
      project: my-project
      template: gs://my-bucket/templates/aggregate-market-metrics
      parameters:
        input: gs://my-bucket/prices/*.parquet
        output: my-project:analytics.market_metrics
    depends_on:
      - run-dataplex-quality
```

**Example - Multi-Source Graph**:
```yaml
# graphs/data-fusion.yaml
name: data-fusion
description: |
  Combines multiple data sources for reporting.

tasks:
  # Two independent tasks can run in parallel
  - task: load-market-data  # Global task
  
  - name: load-customer-data
    type: dbt
    config:
      project: analytics
      models: +customer_data
  
  # This task waits for both
  - name: join-datasets
    type: bq_extract
    config:
      query: |
        SELECT * FROM market_data m
        JOIN customer_data c ON m.customer_id = c.id
      destination: gs://my-bucket/joined/*.parquet
    depends_on:
      - load-market-data
      - load-customer-data
  
  - name: generate-report
    type: dataflow
    config:
      template: gs://my-bucket/templates/report-generator
      parameters:
        input: gs://my-bucket/joined/*.parquet
    depends_on:
      - join-datasets
```

### Validation Rules

The system must validate YAML configurations on load:

1. **Task Validation**:
   - Name matches `^[a-z0-9-]+$` pattern
   - Type is one of the supported types
   - Timeout is positive integer
   - Config structure matches task type requirements
   - If global=true, task can be referenced by multiple graphs

2. **Graph Validation**:
   - Name matches `^[a-z0-9-]+$` pattern
   - At least one task defined
   - All task references resolve to existing tasks
   - All `depends_on` references exist in the graph
   - No circular dependencies (DAG check using JGraphT)
   - Each task appears at most once in the graph

3. **Global Task Rules**:
   - Global tasks can only be defined in `tasks/*.yaml` (not inline in graphs)
   - Multiple graphs can reference the same global task
   - When a global task completes, it marks completed for ALL graphs containing it

## Core Components

### Orchestrator Service Components

**Purpose**: Load graph and task definitions from YAML files on startup.

**Lifecycle**: Executes once on application startup.

**Responsibilities**:
- Read all files matching `tasks/*.yaml` and `graphs/*.yaml`
- Parse YAML into domain objects
- Validate configurations
- Detect circular dependencies using JGraphT
- Upsert into database via repositories
- Log warnings for any invalid configurations

**Interface**:
```java
@ApplicationScoped
public class GraphLoader {
    @Inject TaskRepository taskRepository;
    @Inject GraphRepository graphRepository;
    @Inject GraphValidator graphValidator;
    @Inject Logger logger;
    
    @ConfigProperty(name = "orchestrator.config.path")
    String configPath;
    
    void onStart(@Observes StartupEvent event) {
        logger.info("Loading task and graph definitions from {}", configPath);
        loadTasks();
        loadGraphs();
        logger.info("Configuration loading complete");
    }
    
    private void loadTasks() {
        Path tasksDir = Path.of(configPath, "tasks");
        // Load all .yaml files
        // Parse with SnakeYAML
        // Validate each task
        // Upsert via taskRepository
    }
    
    private void loadGraphs() {
        Path graphsDir = Path.of(configPath, "graphs");
        // Load all .yaml files
        // Parse with SnakeYAML
        // Resolve task references
        // Validate DAG structure
        // Upsert via graphRepository
    }
}
```

**Error Handling**:
- Invalid YAML: Log error, skip file, continue loading others
- Circular dependency: Log error, reject graph, fail startup
- Missing task reference: Log error, reject graph, fail startup
- Duplicate names: Log error, reject duplicate, keep first loaded

**Configuration**:
```properties
# application.properties
orchestrator.config.path=/config
orchestrator.config.reload-on-change=false  # Future: watch for changes
```

#### 2. ExternalEventConsumer

**Purpose**: Bridge external messaging system (Pub/Sub or Hazelcast) to internal Vert.x event bus.

**Lifecycle**: Runs continuously, consuming from external topics.

**Responsibilities**:
- Consume messages from `events-topic` (task results)
- Consume messages from `management-topic` (worker heartbeats, commands)
- Transform external message formats to internal events
- Publish to Vert.x event bus
- Handle deserialization errors gracefully

**Interface**:
```java
@ApplicationScoped
public class ExternalEventConsumer {
    @Inject EventBus eventBus;
    @Inject Logger logger;
    
    @Incoming("external-events")  // Configured per environment
    public CompletionStage<Void> onTaskEvent(Message<TaskEventMessage> message) {
        TaskEventMessage external = message.getPayload();
        logger.debug("Received task event: {}", external);
        
        try {
            OrchestratorEvent internalEvent = switch(external.status()) {
                case "STARTED" -> new TaskStartedEvent(
                    external.executionId(), 
                    external.workerId()
                );
                case "COMPLETED" -> new TaskCompletedEvent(
                    external.executionId(), 
                    external.result()
                );
                case "FAILED" -> new TaskFailedEvent(
                    external.executionId(), 
                    external.errorMessage(),
                    external.attempt()
                );
                default -> {
                    logger.warn("Unknown task status: {}", external.status());
                    yield null;
                }
            };
            
            if (internalEvent != null) {
                String address = "task." + external.status().toLowerCase();
                eventBus.publish(address, internalEvent);
            }
            
            return message.ack();
        } catch (Exception e) {
            logger.error("Error processing task event", e);
            return message.nack(e);
        }
    }
    
    @Incoming("external-management")
    public CompletionStage<Void> onManagementEvent(Message<ManagementEventMessage> message) {
        ManagementEventMessage external = message.getPayload();
        
        try {
            if ("HEARTBEAT".equals(external.type())) {
                eventBus.publish("worker.heartbeat", new WorkerHeartbeatEvent(
                    external.workerId(),
                    external.timestamp()
                ));
            } else if ("REGISTERED".equals(external.type())) {
                eventBus.publish("worker.registered", new WorkerRegisteredEvent(
                    external.workerId(),
                    external.metadata()
                ));
            }
            
            return message.ack();
        } catch (Exception e) {
            logger.error("Error processing management event", e);
            return message.nack(e);
        }
    }
}
```

**Message Formats**:

*TaskEventMessage (from workers)*:
```json
{
  "executionId": "uuid",
  "taskName": "load-market-data",
  "status": "COMPLETED|FAILED|STARTED",
  "workerId": "worker-abc123",
  "timestamp": "2025-10-17T10:30:00Z",
  "attempt": 1,
  "result": {},
  "errorMessage": "optional error"
}
```

*ManagementEventMessage (from workers)*:
```json
{
  "type": "HEARTBEAT|REGISTERED",
  "workerId": "worker-abc123",
  "timestamp": "2025-10-17T10:30:00Z",
  "metadata": {
    "podIp": "10.0.1.5",
    "nodeName": "gke-node-1",
    "capabilities": ["dbt", "bq_extract"]
  }
}
```

#### 3. GraphEvaluator

**Purpose**: Core orchestration logic - evaluate graph state and schedule ready tasks.

**Lifecycle**: Event-driven, triggered by internal events.

**Responsibilities**:
- Build DAG from graph definition using JGraphT
- Determine which tasks are ready to execute (dependencies met)
- Handle global task coordination
- Detect graph completion or stalling
- Publish task scheduling events

**Event Listeners**:
- `task.completed` - Task finished successfully
- `task.failed` - Task failed
- `graph.evaluate` - Explicit request to evaluate
- `task.retry` - Retry a failed task

**Interface**:
```java
@ApplicationScoped
public class GraphEvaluator {
    @Inject EventBus eventBus;
    @Inject JGraphTService graphService;
    @Inject TaskExecutionRepository taskExecutionRepository;
    @Inject GraphExecutionRepository graphExecutionRepository;
    @Inject Logger logger;
    
    @ConsumeEvent("task.completed")
    @ConsumeEvent("task.failed")
    public void onTaskStatusChange(TaskEvent event) {
        logger.info("Task {} changed to {}", event.executionId(), event.status());
        
        // Update database
        taskExecutionRepository.updateStatus(
            event.executionId(), 
            TaskStatus.valueOf(event.status()),
            event.errorMessage(),
            event.result()
        );
        
        // Find graph execution(s) to re-evaluate
        TaskExecution te = taskExecutionRepository.findById(event.executionId());
        
        if (te.task().isGlobal()) {
            // Global task affects multiple graphs
            List<UUID> affectedGraphs = 
                taskExecutionRepository.findGraphsContainingTask(te.task().name());
            
            logger.info("Global task {} affects {} graphs", 
                te.task().name(), affectedGraphs.size());
            
            affectedGraphs.forEach(graphExecId -> 
                eventBus.publish("graph.evaluate", graphExecId)
            );
        } else {
            // Single graph affected
            eventBus.publish("graph.evaluate", te.graphExecutionId());
        }
    }
    
    @ConsumeEvent("graph.evaluate")
    @Transactional
    public void evaluate(UUID graphExecutionId) {
        logger.debug("Evaluating graph execution {}", graphExecutionId);
        
        GraphExecution execution = graphExecutionRepository.findById(graphExecutionId);
        
        // Build DAG
        DirectedAcyclicGraph<Task, DefaultEdge> dag = 
            graphService.buildDAG(execution.graph());
        
        // Get current task states
        Map<Task, TaskStatus> taskStates = 
            taskExecutionRepository.getTaskStates(graphExecutionId);
        
        // Find tasks ready to execute
        Set<Task> readyTasks = graphService.findReadyTasks(dag, taskStates);
        
        logger.info("Found {} ready tasks for graph {}", 
            readyTasks.size(), execution.graph().name());
        
        // Schedule each ready task
        for (Task task : readyTasks) {
            scheduleTask(graphExecutionId, task);
        }
        
        // Check if graph is done
        updateGraphStatus(graphExecutionId, taskStates, readyTasks);
        
        // Publish evaluation complete event
        eventBus.publish("graph.evaluated", new GraphEvaluatedEvent(
            graphExecutionId,
            readyTasks.size()
        ));
    }
    
    private void scheduleTask(UUID graphExecutionId, Task task) {
        if (task.isGlobal()) {
            scheduleGlobalTask(graphExecutionId, task);
        } else {
            scheduleRegularTask(graphExecutionId, task);
        }
    }
    
    private void scheduleGlobalTask(UUID graphExecutionId, Task task) {
        // Check if global task already running
        Optional<TaskExecution> running = 
            taskExecutionRepository.findRunningGlobalExecution(task.name());
        
        if (running.isPresent()) {
            // Link this graph's execution to the running one
            logger.info("Linking to existing global task execution: {}", 
                running.get().id());
            
            taskExecutionRepository.linkToGlobalExecution(
                graphExecutionId, 
                task.id(), 
                running.get().id()
            );
        } else {
            // Start new global execution
            scheduleRegularTask(graphExecutionId, task);
        }
    }
    
    private void scheduleRegularTask(UUID graphExecutionId, Task task) {
        TaskExecution execution = taskExecutionRepository.create(
            graphExecutionId,
            task,
            TaskStatus.PENDING
        );
        
        logger.info("Scheduling task {} for graph execution {}", 
            task.name(), graphExecutionId);
        
        eventBus.publish("task.ready", new TaskReadyEvent(
            execution.id(),
            task
        ));
    }
    
    private void updateGraphStatus(
        UUID graphExecutionId, 
        Map<Task, TaskStatus> taskStates,
        Set<Task> readyTasks
    ) {
        boolean allCompleted = taskStates.values().stream()
            .allMatch(s -> s == TaskStatus.COMPLETED || s == TaskStatus.SKIPPED);
        
        boolean anyFailed = taskStates.values().stream()
            .anyMatch(s -> s == TaskStatus.FAILED);
        
        boolean stalled = readyTasks.isEmpty() && !allCompleted;
        
        GraphStatus newStatus;
        if (allCompleted) {
            newStatus = GraphStatus.COMPLETED;
            eventBus.publish("graph.completed", 
                new GraphCompletedEvent(graphExecutionId));
        } else if (stalled) {
            newStatus = GraphStatus.STALLED;
            eventBus.publish("graph.stalled", 
                new GraphStalledEvent(graphExecutionId));
        } else if (anyFailed) {
            newStatus = GraphStatus.FAILED;
            eventBus.publish("graph.failed", 
                new GraphFailedEvent(graphExecutionId));
        } else {
            newStatus = GraphStatus.RUNNING;
        }
        
        graphExecutionRepository.updateStatus(graphExecutionId, newStatus);
    }
    
    @ConsumeEvent("task.retry")
    public void onTaskRetry(TaskReadyEvent event) {
        logger.info("Retrying task execution {}", event.executionId());
        
        TaskExecution te = taskExecutionRepository.findById(event.executionId());
        
        if (te.attempt() < te.task().retryPolicy().maxRetries()) {
            taskExecutionRepository.incrementAttempt(event.executionId());
            eventBus.publish("task.ready", event);
        } else {
            logger.warn("Max retries exceeded for task execution {}", 
                event.executionId());
            
            taskExecutionRepository.updateStatus(
                event.executionId(),
                TaskStatus.FAILED,
                "Max retries exceeded",
                null
            );
            
            eventBus.publish("task.failed", new TaskFailedEvent(
                event.executionId(),
                "Max retries exceeded",
                te.attempt()
            ));
        }
    }
}
```

#### 4. TaskQueuePublisher

**Purpose**: Publish work to the queue for workers to consume.

**Lifecycle**: Event-driven, triggered by "task.ready" events.

**Responsibilities**:
- Transform internal task events to external work messages
- Publish to appropriate queue (via QueueService interface)
- Update task status to QUEUED
- Handle publishing failures

**Interface**:
```java
@ApplicationScoped
public class TaskQueuePublisher {
    @Inject EventBus eventBus;
    @Inject QueueService queueService;
    @Inject TaskExecutionRepository taskExecutionRepository;
    @Inject Logger logger;
    
    @ConsumeEvent("task.ready")
    public Uni<Void> onTaskReady(TaskReadyEvent event) {
        logger.info("Publishing task {} to queue", event.task().name());
        
        WorkMessage workMessage = new WorkMessage(
            event.executionId(),
            event.task().name(),
            event.task().type(),
            event.task().config(),
            event.task().retryPolicy(),
            event.task().timeoutSeconds()
        );
        
        return queueService.publishWork(workMessage)
            .invoke(() -> {
                taskExecutionRepository.updateStatus(
                    event.executionId(),
                    TaskStatus.QUEUED,
                    null,
                    null
                );
                
                eventBus.publish("task.queued", new TaskQueuedEvent(
                    event.executionId(),
                    event.task()
                ));
            })
            .onFailure().invoke(error -> {
                logger.error("Failed to publish task to queue", error);
                
                // Retry by re-publishing task.ready event after delay
                Uni.createFrom().voidItem()
                    .onItem().delayIt().by(Duration.ofSeconds(5))
                    .subscribe().with(
                        v -> eventBus.publish("task.ready", event)
                    );
            })
            .replaceWithVoid();
    }
}
```

**WorkMessage Format**:
```json
{
  "executionId": "uuid",
  "taskName": "load-market-data",
  "taskType": "dbt",
  "config": {
    "project": "analytics",
    "models": "+market_data"
  },
  "retryPolicy": {
    "maxRetries": 3,
    "backoff": "exponential",
    "initialDelaySeconds": 30
  },
  "timeoutSeconds": 600
}
```

#### 5. WorkerMonitor

**Purpose**: Monitor worker health and recover work from dead workers.

**Lifecycle**: Scheduled task running every 30 seconds.

**Responsibilities**:
- Check worker heartbeats
- Mark workers as dead if no heartbeat for 60 seconds
- Publish "worker.died" events for dead workers
- Update worker heartbeat timestamps

**Interface**:
```java
@ApplicationScoped
public class WorkerMonitor {
    @Inject EventBus eventBus;
    @Inject WorkerRepository workerRepository;
    @Inject Logger logger;
    
    @ConfigProperty(name = "orchestrator.worker.dead-threshold-seconds")
    int deadThresholdSeconds;
    
    @ConsumeEvent("worker.heartbeat")
    @Transactional
    public void onHeartbeat(WorkerHeartbeatEvent event) {
        logger.trace("Heartbeat from worker {}", event.workerId());
        workerRepository.updateHeartbeat(event.workerId(), event.timestamp());
    }
    
    @ConsumeEvent("worker.registered")
    @Transactional
    public void onWorkerRegistered(WorkerRegisteredEvent event) {
        logger.info("Worker registered: {}", event.workerId());
        
        workerRepository.upsert(new Worker(
            event.workerId(),
            WorkerStatus.ACTIVE,
            Instant.now(),
            Instant.now(),
            event.metadata()
        ));
    }
    
    @Scheduled(every = "${orchestrator.worker.monitor-interval}")
    @Transactional
    public void checkDeadWorkers() {
        Instant deadline = Instant.now()
            .minusSeconds(deadThresholdSeconds);
        
        List<Worker> deadWorkers = workerRepository.findDeadWorkers(deadline);
        
        if (!deadWorkers.isEmpty()) {
            logger.warn("Found {} dead workers", deadWorkers.size());
            
            for (Worker worker : deadWorkers) {
                workerRepository.markDead(worker.id());
                
                eventBus.publish("worker.died", new WorkerDiedEvent(
                    worker.id()
                ));
            }
        }
    }
}
```

**Configuration**:
```properties
# application.properties
orchestrator.worker.dead-threshold-seconds=60
orchestrator.worker.monitor-interval=30s
```

#### 6. WorkRecoveryService

**Purpose**: Recover work from dead workers.

**Lifecycle**: Event-driven, triggered by "worker.died" events.

**Responsibilities**:
- Find all tasks assigned to dead worker
- Retry tasks that haven't exceeded max retries
- Fail tasks that have exceeded max retries
- Clean up work assignments

**Interface**:
```java
@ApplicationScoped
public class WorkRecoveryService {
    @Inject EventBus eventBus;
    @Inject TaskExecutionRepository taskExecutionRepository;
    @Inject WorkAssignmentRepository workAssignmentRepository;
    @Inject Logger logger;
    
    @ConsumeEvent("worker.died")
    @Transactional
    public void recoverWork(WorkerDiedEvent event) {
        logger.warn("Recovering work from dead worker: {}", event.workerId());
        
        List<TaskExecution> orphanedTasks = 
            taskExecutionRepository.findByAssignedWorker(event.workerId());
        
        logger.info("Found {} orphaned tasks", orphanedTasks.size());
        
        for (TaskExecution te : orphanedTasks) {
            recoverTask(te);
            workAssignmentRepository.deleteByTaskExecution(te.id());
        }
    }
    
    private void recoverTask(TaskExecution te) {
        if (te.attempt() < te.task().retryPolicy().maxRetries()) {
            logger.info("Retrying task execution {} (attempt {}/{})",
                te.id(), te.attempt(), te.task().retryPolicy().maxRetries());
            
            taskExecutionRepository.resetToPending(te.id());
            
            eventBus.publish("task.retry", new TaskReadyEvent(
                te.id(),
                te.task()
            ));
        } else {
            logger.warn("Task execution {} exceeded max retries", te.id());
            
            taskExecutionRepository.updateStatus(
                te.id(),
                TaskStatus.FAILED,
                "Worker died, max retries exceeded",
                null
            );
            
            eventBus.publish("task.failed", new TaskFailedEvent(
                te.id(),
                "Worker died, max retries exceeded",
                te.attempt()
            ));
        }
    }
}
```

#### 7. JGraphTService

**Purpose**: Build and query DAGs using JGraphT library.

**Lifecycle**: Stateless service, used by GraphEvaluator.

**Responsibilities**:
- Build DirectedAcyclicGraph from graph definition
- Detect cycles (throws exception)
- Find topologically sorted execution order
- Determine which tasks are ready (all dependencies completed)

**Interface**:
```java
@ApplicationScoped
public class JGraphTService {
    @Inject Logger logger;
    
    /**
     * Build a DAG from a graph definition.
     * @throws CycleFoundException if graph contains cycles
     */
    public DirectedAcyclicGraph<Task, DefaultEdge> buildDAG(Graph graph) {
        DirectedAcyclicGraph<Task, DefaultEdge> dag = 
            new DirectedAcyclicGraph<>(DefaultEdge.class);
        
        // Add all tasks as vertices
        for (Task task : graph.tasks()) {
            dag.addVertex(task);
        }
        
        // Add edges from dependencies
        for (GraphEdge edge : graph.edges()) {
            try {
                dag.addEdge(edge.fromTask(), edge.toTask());
            } catch (IllegalArgumentException e) {
                throw new CycleFoundException(
                    "Cycle detected in graph: " + graph.name(), e);
            }
        }
        
        logger.debug("Built DAG for graph {} with {} vertices and {} edges",
            graph.name(), dag.vertexSet().size(), dag.edgeSet().size());
        
        return dag;
    }
    
    /**
     * Find tasks that are ready to execute.
     * A task is ready if all its dependencies are COMPLETED or SKIPPED.
     */
    public Set<Task> findReadyTasks(
        DirectedAcyclicGraph<Task, DefaultEdge> dag,
        Map<Task, TaskStatus> currentStates
    ) {
        Set<Task> ready = new HashSet<>();
        
        for (Task task : dag.vertexSet()) {
            TaskStatus status = currentStates.getOrDefault(task, TaskStatus.PENDING);
            
            // Skip if already queued, running, completed, or failed
            if (status != TaskStatus.PENDING) {
                continue;
            }
            
            // Check if all dependencies are satisfied
            Set<DefaultEdge> incomingEdges = dag.incomingEdgesOf(task);
            boolean allDependenciesMet = true;
            
            for (DefaultEdge edge : incomingEdges) {
                Task dependency = dag.getEdgeSource(edge);
                TaskStatus depStatus = currentStates.get(dependency);
                
                if (depStatus != TaskStatus.COMPLETED && depStatus != TaskStatus.SKIPPED) {
                    allDependenciesMet = false;
                    break;
                }
            }
            
            if (allDependenciesMet) {
                ready.add(task);
            }
        }
        
        return ready;
    }
    
    /**
     * Get topological sort of tasks (useful for UI visualization).
     */
    public List<Task> getTopologicalOrder(DirectedAcyclicGraph<Task, DefaultEdge> dag) {
        TopologicalOrderIterator<Task, DefaultEdge> iterator = 
            new TopologicalOrderIterator<>(dag);
        
        List<Task> order = new ArrayList<>();
        iterator.forEachRemaining(order::add);
        
        return order;
    }
}
```

### Worker Pod Components

#### 1. WorkerMain

**Purpose**: Main entry point for worker pod.

**Lifecycle**: Runs continuously until killed.

**Responsibilities**:
- Register with orchestrator on startup
- Start heartbeat timer
- Listen for work from queue
- Listen for management commands
- Graceful shutdown

**Interface**:
```java
@ApplicationScoped
public class WorkerMain {
    @Inject TaskExecutor taskExecutor;
    @Inject QueueService queueService;
    @Inject ManagementService managementService;
    @Inject Logger logger;
    
    @ConfigProperty(name = "worker.id")
    String workerId;  // Defaults to $HOSTNAME
    
    @ConfigProperty(name = "worker.heartbeat-interval-seconds")
    int heartbeatIntervalSeconds;
    
    private volatile boolean running = true;
    
    void onStart(@Observes StartupEvent event) {
        logger.info("Worker {} starting", workerId);
        
        // Register with orchestrator
        managementService.register(workerId, getMetadata());
        
        // Start heartbeat
        startHeartbeat();
        
        // Start listening for management commands
        managementService.listenForCommands(this::onManagementCommand);
        
        logger.info("Worker {} ready", workerId);
    }
    
    void onStop(@Observes ShutdownEvent event) {
        logger.info("Worker {} shutting down", workerId);
        running = false;
    }
    
    @Incoming("work-queue")
    public CompletionStage<Void> processWork(Message<WorkMessage> message) {
        if (!running) {
            logger.warn("Worker shutting down, rejecting work");
            return message.nack(new IllegalStateException("Worker shutting down"));
        }
        
        WorkMessage work = message.getPayload();
        logger.info("Received work: task={}, execution={}", 
            work.taskName(), work.executionId());
        
        try {
            // Register assignment
            managementService.registerAssignment(work.executionId(), workerId);
            
            // Notify started
            managementService.publishTaskStarted(work.executionId(), workerId);
            
            // Execute task
            TaskResult result = taskExecutor.execute(work);
            
            // Publish result
            if (result.isSuccess()) {
                managementService.publishTaskCompleted(
                    work.executionId(), 
                    workerId,
                    result.data()
                );
            } else {
                managementService.publishTaskFailed(
                    work.executionId(),
                    workerId,
                    result.error(),
                    work.attempt()
                );
            }
            
            return message.ack();
            
        } catch (Exception e) {
            logger.error("Error processing work", e);
            
            managementService.publishTaskFailed(
                work.executionId(),
                workerId,
                e.getMessage(),
                work.attempt()
            );
            
            return message.nack(e);
        }
    }
    
    private void startHeartbeat() {
        Executors.newSingleThreadScheduledExecutor().scheduleAtFixedRate(
            () -> {
                if (running) {
                    try {
                        managementService.sendHeartbeat(workerId);
                    } catch (Exception e) {
                        logger.error("Error sending heartbeat", e);
                    }
                }
            },
            0,
            heartbeatIntervalSeconds,
            TimeUnit.SECONDS
        );
    }
    
    private void onManagementCommand(ManagementCommand cmd) {
        if ("KILL".equals(cmd.type()) && workerId.equals(cmd.targetWorker())) {
            logger.warn("Received KILL command, shutting down");
            running = false;
            System.exit(0);
        }
    }
    
    private Map<String, Object> getMetadata() {
        return Map.of(
            "podIp", System.getenv().getOrDefault("POD_IP", "unknown"),
            "nodeName", System.getenv().getOrDefault("NODE_NAME", "unknown"),
            "capabilities", List.of("dbt", "bq_extract", "dataplex", "dataflow")
        );
    }
}
```

**Configuration**:
```properties
# application.properties
worker.id=${HOSTNAME}
worker.heartbeat-interval-seconds=10
```

#### 2. TaskExecutor

**Purpose**: Dispatch work to appropriate task handlers.

**Lifecycle**: Stateless service.

**Responsibilities**:
- Route work based on task type
- Apply timeout enforcement
- Catch and wrap exceptions
- Return standardized results

**Interface**:
```java
@ApplicationScoped
public class TaskExecutor {
    @Inject DbtHandler dbtHandler;
    @Inject BqExtractHandler bqExtractHandler;
    @Inject DataplexHandler dataplexHandler;
    @Inject DataflowHandler dataflowHandler;
    @Inject Logger logger;
    
    public TaskResult execute(WorkMessage work) {
        logger.info("Executing task: type={}, name={}", 
            work.taskType(), work.taskName());
        
        try {
            TaskResult result = executeWithTimeout(work);
            
            logger.info("Task completed successfully: {}", work.taskName());
            return result;
            
        } catch (TimeoutException e) {
            logger.error("Task timed out: {}", work.taskName());
            return TaskResult.failure("Task execution timed out after " + 
                work.timeoutSeconds() + " seconds");
                
        } catch (Exception e) {
            logger.error("Task failed: " + work.taskName(), e);
            return TaskResult.failure(e.getMessage());
        }
    }
    
    private TaskResult executeWithTimeout(WorkMessage work) 
            throws TimeoutException, InterruptedException, ExecutionException {
        
        ExecutorService executor = Executors.newSingleThreadExecutor();
        
        Future<TaskResult> future = executor.submit(() -> {
            return switch(work.taskType()) {
                case DBT -> dbtHandler.execute(work.config());
                case BQ_EXTRACT -> bqExtractHandler.execute(work.config());
                case DATAPLEX -> dataplexHandler.execute(work.config());
                case DATAFLOW -> dataflowHandler.execute(work.config());
            };
        });
        
        try {
            return future.get(work.timeoutSeconds(), TimeUnit.SECONDS);
        } finally {
            executor.shutdownNow();
        }
    }
}
```

#### 3. Task Handlers

Each task type has a dedicated handler. All handlers implement a common interface:

```java
public interface TaskHandler {
    TaskResult execute(Map<String, Object> config);
}
```

**DbtHandler**:
```java
@ApplicationScoped
public class DbtHandler implements TaskHandler {
    @Inject Logger logger;
    
    @Override
    public TaskResult execute(Map<String, Object> config) {
        String project = (String) config.get("project");
        String models = (String) config.get("models");
        String target = (String) config.getOrDefault("target", "prod");
        
        logger.info("Running DBT: project={}, models={}, target={}", 
            project, models, target);
        
        try {
            // Execute DBT via CLI
            ProcessBuilder pb = new ProcessBuilder(
                "dbt", "run",
                "--project-dir", "/dbt/" + project,
                "--models", models,
                "--target", target
            );
            
            Process process = pb.start();
            int exitCode = process.waitFor();
            
            if (exitCode == 0) {
                return TaskResult.success(Map.of(
                    "models_run", extractModelsFromOutput(process)
                ));
            } else {
                String error = new String(process.getErrorStream().readAllBytes());
                return TaskResult.failure("DBT failed: " + error);
            }
            
        } catch (Exception e) {
            logger.error("Error executing DBT", e);
            return TaskResult.failure(e.getMessage());
        }
    }
    
    private List<String> extractModelsFromOutput(Process process) {
        // Parse DBT output to get list of models that ran
        // Implementation details omitted
        return List.of();
    }
}
```

**BqExtractHandler**:
```java
@ApplicationScoped
public class BqExtractHandler implements TaskHandler {
    @Inject Logger logger;
    
    @Override
    public TaskResult execute(Map<String, Object> config) {
        String project = (String) config.get("project");
        String dataset = (String) config.get("dataset");
        String table = (String) config.get("table");
        String destination = (String) config.get("destination");
        
        logger.info("Extracting BQ table: {}.{}.{} to {}", 
            project, dataset, table, destination);
        
        try {
            BigQuery bigquery = BigQueryOptions.newBuilder()
                .setProjectId(project)
                .build()
                .getService();
            
            TableId tableId = TableId.of(project, dataset, table);
            String format = (String) config.getOrDefault("format", "PARQUET");
            
            ExtractJobConfiguration configuration = ExtractJobConfiguration.newBuilder(
                    tableId,
                    destination
                )
                .setFormat(format)
                .build();
            
            Job job = bigquery.create(JobInfo.of(configuration));
            job = job.waitFor();
            
            if (job.getStatus().getError() == null) {
                return TaskResult.success(Map.of(
                    "destination", destination,
                    "bytes_exported", job.getStatistics().toString()
                ));
            } else {
                return TaskResult.failure("BQ Extract failed: " + 
                    job.getStatus().getError());
            }
            
        } catch (Exception e) {
            logger.error("Error extracting from BigQuery", e);
            return TaskResult.failure(e.getMessage());
        }
    }
}
```

**DataplexHandler** and **DataflowHandler** follow similar patterns, using respective GCP client libraries.

## Interfaces and Abstractions

### Repository Pattern

All data access goes through repository interfaces, allowing easy swapping between implementations.

#### Base Repository Interface

```java
public interface Repository<T, ID> {
    T findById(ID id);
    Optional<T> findByIdOptional(ID id);
    List<T> findAll();
    T save(T entity);
    void delete(ID id);
}
```

#### GraphRepository

```java
public interface GraphRepository extends Repository<Graph, UUID> {
    Optional<Graph> findByName(String name);
    Graph upsertFromYaml(String name, String yamlContent);
    List<Graph> findAll();
}

@ApplicationScoped
@RequiresDialect(Database.POSTGRESQL)
public class PostgresGraphRepository implements GraphRepository {
    @Inject DataSource dataSource;
    
    @Override
    public Graph findById(UUID id) {
        // JDBC query
    }
    
    @Override
    public Graph upsertFromYaml(String name, String yamlContent) {
        // INSERT ... ON CONFLICT UPDATE
    }
}

@ApplicationScoped
@RequiresDialect(Database.H2)
public class H2GraphRepository implements GraphRepository {
    @Inject DataSource dataSource;
    
    // H2-specific implementation (MERGE syntax)
}
```

#### TaskRepository

```java
public interface TaskRepository extends Repository<Task, UUID> {
    Optional<Task> findByName(String name);
    Task upsertFromYaml(String name, TaskDefinition definition);
    List<Task> findGlobalTasks();
}
```

#### TaskExecutionRepository

```java
public interface TaskExecutionRepository extends Repository<TaskExecution, UUID> {
    TaskExecution create(UUID graphExecutionId, Task task, TaskStatus initialStatus);
    void updateStatus(UUID id, TaskStatus status, String error, Map<String, Object> result);
    Map<Task, TaskStatus> getTaskStates(UUID graphExecutionId);
    List<TaskExecution> findByAssignedWorker(String workerId);
    Optional<TaskExecution> findRunningGlobalExecution(String taskName);
    void linkToGlobalExecution(UUID graphExecId, UUID taskId, UUID globalExecId);
    void resetToPending(UUID id);
    void incrementAttempt(UUID id);
    List<UUID> findGraphsContainingTask(String taskName);
}
```

#### GraphExecutionRepository

```java
public interface GraphExecutionRepository extends Repository<GraphExecution, UUID> {
    GraphExecution create(UUID graphId, String triggeredBy);
    void updateStatus(UUID id, GraphStatus status);
    GraphExecution getCurrentExecution(UUID graphId);
    List<GraphExecution> getHistory(UUID graphId, int limit);
}
```

#### WorkerRepository

```java
public interface WorkerRepository extends Repository<Worker, String> {
    void upsert(Worker worker);
    void updateHeartbeat(String workerId, Instant timestamp);
    void markDead(String workerId);
    List<Worker> findDeadWorkers(Instant deadlineBefore);
    List<Worker> findActive();
}
```

#### WorkAssignmentRepository

```java
public interface WorkAssignmentRepository {
    void create(UUID taskExecutionId, String workerId);
    void deleteByTaskExecution(UUID taskExecutionId);
    Optional<WorkAssignment> findByTaskExecution(UUID taskExecutionId);
}
```

### Queue Abstraction

Queue operations are abstracted behind an interface to support multiple implementations.

#### QueueService Interface

```java
public interface QueueService {
    /**
     * Publish work to the task queue.
     * Returns a Uni for async/reactive handling.
     */
    Uni<Void> publishWork(WorkMessage message);
    
    /**
     * Publish an event (task result).
     */
    Uni<Void> publishEvent(TaskEventMessage message);
    
    /**
     * Publish to management topic.
     */
    Uni<Void> publishManagement(ManagementEventMessage message);
}
```

#### PubSubQueueService (Production)

```java
@ApplicationScoped
@RequiresProfile("prod")
public class PubSubQueueService implements QueueService {
    @Inject Logger logger;
    
    @ConfigProperty(name = "pubsub.project")
    String projectId;
    
    @ConfigProperty(name = "pubsub.topic.work")
    String workTopicName;
    
    @ConfigProperty(name = "pubsub.topic.events")
    String eventsTopicName;
    
    @ConfigProperty(name = "pubsub.topic.management")
    String managementTopicName;
    
    private Publisher workPublisher;
    private Publisher eventsPublisher;
    private Publisher managementPublisher;
    
    @PostConstruct
    void init() throws IOException {
        TopicName workTopic = TopicName.of(projectId, workTopicName);
        TopicName eventsTopic = TopicName.of(projectId, eventsTopicName);
        TopicName mgmtTopic = TopicName.of(projectId, managementTopicName);
        
        workPublisher = Publisher.newBuilder(workTopic).build();
        eventsPublisher = Publisher.newBuilder(eventsTopic).build();
        managementPublisher = Publisher.newBuilder(mgmtTopic).build();
    }
    
    @Override
    public Uni<Void> publishWork(WorkMessage message) {
        String json = toJson(message);
        PubsubMessage pubsubMessage = PubsubMessage.newBuilder()
            .setData(ByteString.copyFromUtf8(json))
            .build();
        
        return Uni.createFrom().completionStage(
            workPublisher.publish(pubsubMessage)
        ).replaceWithVoid();
    }
    
    @Override
    public Uni<Void> publishEvent(TaskEventMessage message) {
        String json = toJson(message);
        PubsubMessage pubsubMessage = PubsubMessage.newBuilder()
            .setData(ByteString.copyFromUtf8(json))
            .build();
        
        return Uni.createFrom().completionStage(
            eventsPublisher.publish(pubsubMessage)
        ).replaceWithVoid();
    }
    
    @Override
    public Uni<Void> publishManagement(ManagementEventMessage message) {
        String json = toJson(message);
        PubsubMessage pubsubMessage = PubsubMessage.newBuilder()
            .setData(ByteString.copyFromUtf8(json))
            .build();
        
        return Uni.createFrom().completionStage(
            managementPublisher.publish(pubsubMessage)
        ).replaceWithVoid();
    }
    
    @PreDestroy
    void cleanup() {
        if (workPublisher != null) workPublisher.shutdown();
        if (eventsPublisher != null) eventsPublisher.shutdown();
        if (managementPublisher != null) managementPublisher.shutdown();
    }
    
    private String toJson(Object obj) {
        // Use Jackson or similar
        return "{}";
    }
}
```

#### HazelcastQueueService (Local Dev)

```java
@ApplicationScoped
@RequiresProfile("dev")
public class HazelcastQueueService implements QueueService {
    @Inject Logger)
);

-- ... (rest of schema from Data Model section)
```

```sql
-- V2__add_version_columns.sql
ALTER TABLE task_executions ADD COLUMN version INT NOT NULL DEFAULT 0;
ALTER TABLE task_executions ADD COLUMN last_event_sequence BIGINT NOT NULL DEFAULT 0;
ALTER TABLE graph_executions ADD COLUMN version INT NOT NULL DEFAULT 0;
```

```sql
-- V3__add_pause_support.sql
ALTER TABLE graph_executions 
    DROP CONSTRAINT graph_executions_status_valid;

ALTER TABLE graph_executions
    ADD CONSTRAINT graph_executions_status_valid 
    CHECK (status IN ('running', 'completed', 'failed', 'stalled', 'paused'));
```

### Kubernetes Manifests

#### Namespace

```yaml
# namespace.yaml
apiVersion: v1
kind: Namespace
metadata:
  name: orchestrator
```

#### ConfigMap

```yaml
# configmap.yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: workflow-definitions
  namespace: orchestrator
data:
  # Tasks
  load-market-data.yaml: |
    name: load-market-data
    type: dbt
    global: true
    retry_policy:
      max_retries: 3
      backoff: exponential
      initial_delay_seconds: 30
    timeout_seconds: 600
    config:
      project: analytics
      models: +market_data
      target: prod
  
  # Graphs
  market-pipeline.yaml: |
    name: market-pipeline
    description: Daily market data processing pipeline
    tasks:
      - task: load-market-data
      - name: extract-prices
        type: bq_extract
        timeout_seconds: 300
        config:
          project: my-project
          dataset: raw_data
          table: prices
          destination: gs://my-bucket/prices/*.parquet
        depends_on:
          - load-market-data
```

#### Secrets

```yaml
# secrets.yaml
apiVersion: v1
kind: Secret
metadata:
  name: orchestrator-secrets
  namespace: orchestrator
type: Opaque
stringData:
  cloudsql-username: orchestrator
  cloudsql-password: <password>
  oauth-client-secret: <oauth-secret>
```

#### Cloud SQL Proxy

```yaml
# cloudsql-proxy.yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: cloudsql-proxy
  namespace: orchestrator
spec:
  replicas: 1
  selector:
    matchLabels:
      app: cloudsql-proxy
  template:
    metadata:
      labels:
        app: cloudsql-proxy
    spec:
      serviceAccountName: orchestrator-sa
      containers:
      - name: cloud-sql-proxy
        image: gcr.io/cloudsql-docker/gce-proxy:latest
        command:
          - "/cloud_sql_proxy"
          - "-instances=<PROJECT_ID>:<REGION>:<INSTANCE>=tcp:0.0.0.0:5432"
        ports:
        - containerPort: 5432
---
apiVersion: v1
kind: Service
metadata:
  name: cloudsql-proxy
  namespace: orchestrator
spec:
  selector:
    app: cloudsql-proxy
  ports:
  - port: 5432
    targetPort: 5432
```

#### Orchestrator Deployment

```yaml
# orchestrator-deployment.yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: orchestrator
  namespace: orchestrator
spec:
  replicas: 1  # Single instance, no HA
  strategy:
    type: Recreate  # Kill old pod before starting new one
  selector:
    matchLabels:
      app: orchestrator
  template:
    metadata:
      labels:
        app: orchestrator
    spec:
      serviceAccountName: orchestrator-sa
      containers:
      - name: orchestrator
        image: gcr.io/<PROJECT_ID>/orchestrator:latest
        ports:
        - containerPort: 8080
          name: http
        env:
        - name: QUARKUS_PROFILE
          value: "prod"
        - name: GCP_PROJECT_ID
          value: "<PROJECT_ID>"
        - name: CLOUDSQL_HOST
          value: "cloudsql-proxy"
        - name: CLOUDSQL_DATABASE
          value: "orchestrator"
        - name: CLOUDSQL_USERNAME
          valueFrom:
            secretKeyRef:
              name: orchestrator-secrets
              key: cloudsql-username
        - name: CLOUDSQL_PASSWORD
          valueFrom:
            secretKeyRef:
              name: orchestrator-secrets
              key: cloudsql-password
        - name: JAVA_OPTS
          value: "-Xmx2g -Xms1g"
        resources:
          requests:
            memory: "2Gi"
            cpu: "1000m"
          limits:
            memory: "4Gi"
            cpu: "2000m"
        livenessProbe:
          httpGet:
            path: /q/health/live
            port: 8080
          initialDelaySeconds: 60
          periodSeconds: 10
        readinessProbe:
          httpGet:
            path: /q/health/ready
            port: 8080
          initialDelaySeconds: 30
          periodSeconds: 5
        volumeMounts:
        - name: config
          mountPath: /config
          readOnly: true
      volumes:
      - name: config
        configMap:
          name: workflow-definitions
---
apiVersion: v1
kind: Service
metadata:
  name: orchestrator
  namespace: orchestrator
  annotations:
    prometheus.io/scrape: "true"
    prometheus.io/port: "8080"
    prometheus.io/path: "/q/metrics"
spec:
  selector:
    app: orchestrator
  ports:
  - port: 8080
    targetPort: 8080
    name: http
  type: LoadBalancer  # Or use Ingress
```

#### Worker Deployment

```yaml
# worker-deployment.yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: workers
  namespace: orchestrator
spec:
  replicas: 2
  strategy:
    type: RollingUpdate
    rollingUpdate:
      maxSurge: 1
      maxUnavailable: 0  # Ensure no work is lost
  selector:
    matchLabels:
      app: worker
  template:
    metadata:
      labels:
        app: worker
    spec:
      serviceAccountName: worker-sa
      containers:
      - name: worker
        image: gcr.io/<PROJECT_ID>/worker:latest
        env:
        - name: QUARKUS_PROFILE
          value: "prod"
        - name: GCP_PROJECT_ID
          value: "<PROJECT_ID>"
        - name: WORKER_ID
          valueFrom:
            fieldRef:
              fieldPath: metadata.name
        - name: POD_IP
          valueFrom:
            fieldRef:
              fieldPath: status.podIP
        - name: NODE_NAME
          valueFrom:
            fieldRef:
              fieldPath: spec.nodeName
        - name: JAVA_OPTS
          value: "-Xmx1g -Xms512m"
        resources:
          requests:
            memory: "1Gi"
            cpu: "500m"
          limits:
            memory: "2Gi"
            cpu: "1000m"
        livenessProbe:
          httpGet:
            path: /q/health/live
            port: 8080
          initialDelaySeconds: 30
          periodSeconds: 10
        volumeMounts:
        - name: config
          mountPath: /config
          readOnly: true
      volumes:
      - name: config
        configMap:
          name: workflow-definitions
      terminationGracePeriodSeconds: 300  # Allow tasks to finish
---
apiVersion: autoscaling/v2
kind: HorizontalPodAutoscaler
metadata:
  name: workers-hpa
  namespace: orchestrator
spec:
  scaleTargetRef:
    apiVersion: apps/v1
    kind: Deployment
    name: workers
  minReplicas: 2
  maxReplicas: 20
  metrics:
  - type: External
    external:
      metric:
        name: pubsub.googleapis.com|subscription|num_undelivered_messages
        selector:
          matchLabels:
            resource.labels.subscription_id: orchestrator-work-sub
      target:
        type: AverageValue
        averageValue: "30"  # Scale when >30 messages per worker
  behavior:
    scaleDown:
      stabilizationWindowSeconds: 300  # Wait 5min before scaling down
      policies:
      - type: Percent
        value: 50
        periodSeconds: 60
    scaleUp:
      stabilizationWindowSeconds: 0
      policies:
      - type: Percent
        value: 100
        periodSeconds: 15
      - type: Pods
        value: 4
        periodSeconds: 15
      selectPolicy: Max
```

#### Service Accounts

```yaml
# service-accounts.yaml
apiVersion: v1
kind: ServiceAccount
metadata:
  name: orchestrator-sa
  namespace: orchestrator
  annotations:
    iam.gke.io/gcp-service-account: orchestrator@<PROJECT_ID>.iam.gserviceaccount.com
---
apiVersion: v1
kind: ServiceAccount
metadata:
  name: worker-sa
  namespace: orchestrator
  annotations:
    iam.gke.io/gcp-service-account: orchestrator-worker@<PROJECT_ID>.iam.gserviceaccount.com
```

#### Ingress (Optional)

```yaml
# ingress.yaml
apiVersion: networking.k8s.io/v1
kind: Ingress
metadata:
  name: orchestrator-ingress
  namespace: orchestrator
  annotations:
    kubernetes.io/ingress.class: "gce"
    cert-manager.io/cluster-issuer: "letsencrypt-prod"
spec:
  tls:
  - hosts:
    - orchestrator.example.com
    secretName: orchestrator-tls
  rules:
  - host: orchestrator.example.com
    http:
      paths:
      - path: /*
        pathType: ImplementationSpecific
        backend:
          service:
            name: orchestrator
            port:
              number: 8080
```

### GCP IAM Setup

**Orchestrator Service Account** (`orchestrator@<PROJECT_ID>.iam.gserviceaccount.com`):
- `cloudsql.client` - Connect to Cloud SQL
- `pubsub.subscriber` - Subscribe to events and management topics
- `logging.logWriter` - Write to Cloud Logging
- `monitoring.metricWriter` - Write metrics

**Worker Service Account** (`orchestrator-worker@<PROJECT_ID>.iam.gserviceaccount.com`):
- `pubsub.subscriber` - Subscribe to work queue and management topic
- `pubsub.publisher` - Publish task events
- `bigquery.dataEditor` - Run BigQuery extracts
- `dataflow.developer` - Launch Dataflow jobs
- `dataplex.dataScannerRunner` - Run Dataplex scans
- `storage.objectAdmin` - Read/write to GCS for task outputs
- `logging.logWriter` - Write to Cloud Logging

**Workload Identity Binding**:
```bash
gcloud iam service-accounts add-iam-policy-binding \
    orchestrator@<PROJECT_ID>.iam.gserviceaccount.com \
    --role roles/iam.workloadIdentityUser \
    --member "serviceAccount:<PROJECT_ID>.svc.id.goog[orchestrator/orchestrator-sa]"

gcloud iam service-accounts add-iam-policy-binding \
    orchestrator-worker@<PROJECT_ID>.iam.gserviceaccount.com \
    --role roles/iam.workloadIdentityUser \
    --member "serviceAccount:<PROJECT_ID>.svc.id.goog[orchestrator/worker-sa]"
```

### Pub/Sub Topics and Subscriptions

```bash
# Create topics
gcloud pubsub topics create orchestrator-work
gcloud pubsub topics create orchestrator-events
gcloud pubsub topics create orchestrator-management

# Create subscriptions
gcloud pubsub subscriptions create orchestrator-work-sub \
    --topic=orchestrator-work \
    --ack-deadline=600 \
    --message-retention-duration=1h \
    --max-delivery-attempts=3

gcloud pubsub subscriptions create orchestrator-events-sub \
    --topic=orchestrator-events \
    --ack-deadline=60 \
    --message-retention-duration=1h

gcloud pubsub subscriptions create orchestrator-management-sub \
    --topic=orchestrator-management \
    --ack-deadline=60 \
    --message-retention-duration=10m
```

### Deployment Process

**Step 1: Build Images**
```bash
# Orchestrator
cd orchestrator
./mvnw clean package -Pnative
docker build -f src/main/docker/Dockerfile.jvm -t gcr.io/<PROJECT_ID>/orchestrator:latest .
docker push gcr.io/<PROJECT_ID>/orchestrator:latest

# Worker
cd ../worker
./mvnw clean package -Pnative
docker build -f src/main/docker/Dockerfile.jvm -t gcr.io/<PROJECT_ID>/worker:latest .
docker push gcr.io/<PROJECT_ID>/worker:latest
```

**Step 2: Apply Kubernetes Manifests**
```bash
kubectl apply -f k8s/namespace.yaml
kubectl apply -f k8s/secrets.yaml
kubectl apply -f k8s/service-accounts.yaml
kubectl apply -f k8s/configmap.yaml
kubectl apply -f k8s/cloudsql-proxy.yaml
kubectl apply -f k8s/orchestrator-deployment.yaml
kubectl apply -f k8s/worker-deployment.yaml
kubectl apply -f k8s/ingress.yaml
```

**Step 3: Verify Deployment**
```bash
kubectl get pods -n orchestrator
kubectl logs -n orchestrator deployment/orchestrator
kubectl logs -n orchestrator deployment/workers
```

**Step 4: Run Migrations**
Migrations run automatically on orchestrator startup via Flyway.

### Updating Graph Definitions

To update graphs without redeploying:

1. Update ConfigMap:
```bash
kubectl edit configmap workflow-definitions -n orchestrator
```

2. Recreate pods to pick up changes:
```bash
kubectl rollout restart deployment/orchestrator -n orchestrator
kubectl rollout restart deployment/workers -n orchestrator
```

Note: This causes brief downtime. In-flight executions will resume when pods restart.

## Testing Strategy

### Unit Tests

**Repository Tests** (using Testcontainers):
```java
@QuarkusTest
@TestProfile(PostgresTestProfile.class)
class TaskRepositoryTest {
    @Inject TaskRepository taskRepo;
    
    @Test
    void shouldCreateAndFindTask() {
        Task task = new Task(
            UUID.randomUUID(),
            "test-task",
            TaskType.DBT,
            Map.of("project", "test"),
            new RetryPolicy(3, "exponential", 30, null),
            600,
            false
        );
        
        Task saved = taskRepo.save(task);
        Optional<Task> found = taskRepo.findByName("test-task");
        
        assertThat(found).isPresent();
        assertThat(found.get().name()).isEqualTo("test-task");
    }
}
```

**JGraphT Service Tests**:
```java
@QuarkusTest
class JGraphTServiceTest {
    @Inject JGraphTService jGraphTService;
    
    @Test
    void shouldDetectCycle() {
        Graph graph = createGraphWithCycle();
        
        assertThatThrownBy(() -> jGraphTService.buildDAG(graph))
            .isInstanceOf(CycleFoundException.class);
    }
    
    @Test
    void shouldFindReadyTasks() {
        Graph graph = createLinearGraph();  // A -> B -> C
        DirectedAcyclicGraph<Task, DefaultEdge> dag = jGraphTService.buildDAG(graph);
        
        Map<Task, TaskStatus> states = Map.of(
            taskA, TaskStatus.COMPLETED,
            taskB, TaskStatus.PENDING,
            taskC, TaskStatus.PENDING
        );
        
        Set<Task> ready = jGraphTService.findReadyTasks(dag, states);
        
        assertThat(ready).containsOnly(taskB);
    }
}
```

**Graph Evaluator Tests**:
```java
@QuarkusTest
class GraphEvaluatorTest {
    @Inject GraphEvaluator evaluator;
    @Inject EventBus eventBus;
    
    @Test
    void shouldScheduleReadyTasks() {
        List<TaskReadyEvent> published = new ArrayList<>();
        eventBus.consumer("task.ready", msg -> {
            published.add(msg.body());
        });
        
        UUID graphExecId = setupGraphExecution();  // A (completed) -> B, C
        
        evaluator.evaluate(graphExecId);
        
        assertThat(published).hasSize(2);  // B and C are ready
    }
}
```

### Integration Tests

**Full Flow Test** (using Testcontainers + Hazelcast):
```java
@QuarkusTest
@TestProfile(IntegrationTestProfile.class)
class WorkflowIntegrationTest {
    @Inject GraphRepository graphRepo;
    @Inject GraphEvaluator evaluator;
    @Inject QueueService queueService;
    
    @Test
    void shouldExecuteSimpleWorkflow() throws Exception {
        // Load graph definition
        Graph graph = loadGraphFromYaml("test-graphs/simple.yaml");
        graphRepo.save(graph);
        
        // Create execution
        GraphExecution execution = graphExecRepo.create(
            graph.id(),
            "integration-test"
        );
        
        // Trigger evaluation
        evaluator.evaluate(execution.id());
        
        // Wait for completion (with timeout)
        await().atMost(30, SECONDS).until(() -> {
            GraphExecution updated = graphExecRepo.findById(execution.id());
            return updated.status() == GraphStatus.COMPLETED;
        });
        
        // Verify all tasks completed
        List<TaskExecution> tasks = taskExecRepo.findByGraphExecution(execution.id());
        assertThat(tasks).allMatch(te -> te.status() == TaskStatus.COMPLETED);
    }
}
```

**Global Task Test**:
```java
@Test
void shouldShareGlobalTaskAcrossGraphs() {
    // Create two graphs that both use global task
    Graph graph1 = loadGraphFromYaml("test-graphs/with-global-1.yaml");
    Graph graph2 = loadGraphFromYaml("test-graphs/with-global-2.yaml");
    
    graphRepo.save(graph1);
    graphRepo.save(graph2);
    
    // Start both executions
    GraphExecution exec1 = graphExecRepo.create(graph1.id(), "test");
    GraphExecution exec2 = graphExecRepo.create(graph2.id(), "test");
    
    evaluator.evaluate(exec1.id());
    evaluator.evaluate(exec2.id());
    
    // Wait for global task to complete
    await().atMost(10, SECONDS).until(() -> {
        List<TaskExecution> globalExecs = 
            taskExecRepo.findGlobalExecutions("load-market-data");
        return globalExecs.stream()
            .anyMatch(te -> te.status() == TaskStatus.COMPLETED);
    });
    
    // Verify only ONE global task execution occurred
    List<TaskExecution> globalExecs = 
        taskExecRepo.findGlobalExecutions("load-market-data");
    assertThat(globalExecs).hasSize(1);
    
    // Verify both graph executions progressed
    assertThat(taskExecRepo.findByGraphExecution(exec1.id()))
        .anyMatch(te -> te.status() != TaskStatus.PENDING);
    assertThat(taskExecRepo.findByGraphExecution(exec2.id()))
        .anyMatch(te -> te.status() != TaskStatus.PENDING);
}
```

### Chaos Engineering Tests

**Worker Death Test**:
```java
@QuarkusTest
@TestProfile(ChaosTestProfile.class)
class WorkerFailureTest {
    @Inject WorkerMonitor workerMonitor;
    @Inject WorkRecoveryService workRecovery;
    @Inject TaskExecutionRepository taskExecRepo;
    
    @Test
    void shouldRecoverWorkFromDeadWorker() {
        // Setup: worker starts executing task
        String workerId = "test-worker-1";
        UUID taskExecId = createTaskExecution();
        
        taskExecRepo.updateStatus(taskExecId, TaskStatus.RUNNING, null, null);
        taskExecRepo.assignWorker(taskExecId, workerId);
        
        // Worker sends heartbeat
        workerRepo.upsert(new Worker(
            workerId, 
            WorkerStatus.ACTIVE,
            Instant.now(),
            Instant.now(),
            Map.of()
        ));
        
        // Simulate worker death (no more heartbeats)
        // ... wait for dead threshold ...
        Thread.sleep(65_000);
        
        // Run monitor check
        workerMonitor.checkDeadWorkers();
        
        // Verify worker marked dead
        Worker worker = workerRepo.findById(workerId);
        assertThat(worker.status()).isEqualTo(WorkerStatus.DEAD);
        
        // Verify task was recovered
        await().atMost(5, SECONDS).until(() -> {
            TaskExecution te = taskExecRepo.findById(taskExecId);
            return te.status() == TaskStatus.PENDING;
        });
    }
}
```

**Database Connection Loss Test**:
```java
@Test
void shouldHandleDatabaseUnavailability() {
    // Stop database container
    postgresContainer.stop();
    
    // Verify orchestrator stops accepting work
    assertThatThrownBy(() -> evaluator.evaluate(UUID.randomUUID()))
        .isInstanceOf(PersistenceException.class);
    
    // Restart database
    postgresContainer.start();
    
    // Verify orchestrator recovers
    UUID graphExecId = setupGraphExecution();
    assertThatCode(() -> evaluator.evaluate(graphExecId))
        .doesNotThrowAnyException();
}
```

**Queue Unavailability Test**:
```java
@Test
void shouldHandleQueueUnavailability() {
    // Stop Hazelcast/Pub/Sub
    queueService.shutdown();
    
    // Attempt to queue task
    Uni<Void> result = queueService.publishWork(createWorkMessage());
    
    // Verify failure is handled
    assertThatThrownBy(() -> result.await().indefinitely())
        .isInstanceOf(Exception.class);
    
    // Restart queue
    queueService.start();
    
    // Verify recovery
    result = queueService.publishWork(createWorkMessage());
    assertThatCode(() -> result.await().indefinitely())
        .doesNotThrowAnyException();
}
```

**Concurrent Evaluation Test**:
```java
@Test
void shouldHandleConcurrentGraphEvaluations() throws Exception {
    UUID graphExecId = setupGraphExecution();
    
    // Trigger multiple concurrent evaluations
    ExecutorService executor = Executors.newFixedThreadPool(10);
    List<Future<?>> futures = new ArrayList<>();
    
    for (int i = 0; i < 10; i++) {
        futures.add(executor.submit(() -> {
            evaluator.evaluate(graphExecId);
        }));
    }
    
    // Wait for all to complete
    for (Future<?> future : futures) {
        future.get(10, TimeUnit.SECONDS);
    }
    
    // Verify no duplicate task executions
    List<TaskExecution> tasks = taskExecRepo.findByGraphExecution(graphExecId);
    Map<UUID, Long> taskCounts = tasks.stream()
        .collect(Collectors.groupingBy(
            te -> te.task().id(),
            Collectors.counting()
        ));
    
    assertThat(taskCounts.values()).allMatch(count -> count == 1);
}
```

### Load Tests

**JMeter Test Plan**:
```xml
<!-- load-test.jmx -->
<jmeterTestPlan version="1.2">
  <hashTree>
    <TestPlan>
      <stringProp name="TestPlan.comments">Orchestrator Load Test</stringProp>
      <boolProp name="TestPlan.functional_mode">false</boolProp>
    </TestPlan>
    <hashTree>
      <ThreadGroup>
        <stringProp name="ThreadGroup.num_threads">100</stringProp>
        <stringProp name="ThreadGroup.ramp_time">60</stringProp>
        <stringProp name="ThreadGroup.duration">600</stringProp>
      </ThreadGroup>
      <hashTree>
        <HTTPSamplerProxy>
          <stringProp name="HTTPSampler.domain">orchestrator.example.com</stringProp>
          <stringProp name="HTTPSampler.path">/api/graphs/${graphId}/execute</stringProp>
          <stringProp name="HTTPSampler.method">POST</stringProp>
        </HTTPSamplerProxy>
      </hashTree>
    </hashTree>
  </hashTree>
</jmeterTestPlan>
```

**Load Test Scenarios**:
1. **Steady State**: 100 concurrent graph executions, each with 10 tasks
2. **Burst**: Spike from 10 to 1000 executions over 1 minute
3. **Long Duration**: 24-hour test with 50 constant executions
4. **Worker Scaling**: Verify HPA scales workers appropriately

**Performance Targets**:
- Graph evaluation latency: p50 < 100ms, p99 < 500ms
- Task queuing latency: p50 < 50ms, p99 < 200ms
- Worker task pickup: < 1s from queue to execution
- Database queries: all < 100ms
- End-to-end graph execution: proportional to actual task duration (minimal overhead)

### Contract Tests

**Message Format Tests**:
```java
@QuarkusTest
class MessageContractTest {
    @Test
    void workMessageShouldMatchContract() {
        WorkMessage msg = new WorkMessage(
            UUID.randomUUID(),
            "test-task",
            TaskType.DBT,
            Map.of("project", "test"),
            new RetryPolicy(3, "exponential", 30, null),
            600
        );
        
        String json = toJson(msg);
        
        // Verify structure
        JsonAssert.assertThat(json)
            .node("executionId").isPresent()
            .node("taskName").isEqualTo("test-task")
            .node("taskType").isEqualTo("dbt")
            .node("config.project").isEqualTo("test")
            .node("retryPolicy.maxRetries").isEqualTo(3)
            .node("timeoutSeconds").isEqualTo(600);
    }
    
    @Test
    void taskEventMessageShouldMatchContract() {
        TaskEventMessage msg = new TaskEventMessage(
            UUID.randomUUID(),
            "test-task",
            "COMPLETED",
            "worker-1",
            Instant.now(),
            1L,
            Map.of("result", "success"),
            null
        );
        
        String json = toJson(msg);
        
        JsonAssert.assertThat(json)
            .node("executionId").isPresent()
            .node("taskName").isEqualTo("test-task")
            .node("status").isEqualTo("COMPLETED")
            .node("workerId").isEqualTo("worker-1")
            .node("sequenceNumber").isEqualTo(1);
    }
}
```

## Implementation Phases

### Phase 1: Core Infrastructure (Sprint 1-2)

**Goals**: Basic orchestration working end-to-end in local dev.

**Stories**:

1. **Setup Project Structure**
   - Create Maven/Gradle multi-module project
   - Configure Quarkus dependencies
   - Setup H2 and Hazelcast for dev profile
   - Create Docker Compose for local dev
   - **Acceptance**: `mvn clean install` succeeds

2. **Database Schema & Migrations**
   - Implement Flyway migrations
   - Create all tables from data model
   - Add indexes
   - **Acceptance**: Migrations run successfully on H2 and Postgres

3. **Repository Pattern Implementation**
   - Define repository interfaces
   - Implement Postgres repositories
   - Implement H2 repositories (where different)
   - Add profile-based selection
   - **Acceptance**: All repository tests pass

4. **Queue Abstraction**
   - Define QueueService interface
   - Implement HazelcastQueueService
   - Implement PubSubQueueService (stub for now)
   - **Acceptance**: Can publish and consume messages in dev

5. **YAML Configuration Loading**
   - Implement GraphLoader
   - Add YAML parsing with SnakeYAML
   - Implement GraphValidator
   - Add cycle detection with JGraphT
   - **Acceptance**: Sample graphs load successfully on startup

6. **Vert.x Event Bus Setup**
   - Configure event bus
   - Define all event types (sealed interfaces/records)
   - Create ExternalEventConsumer bridge
   - **Acceptance**: Events flow from external queue to internal bus

### Phase 2: Core Orchestration (Sprint 3-4)

**Goals**: Graph evaluation and task scheduling working.

**Stories**:

1. **JGraphT Service**
   - Implement DAG building
   - Implement cycle detection
   - Implement ready task detection
   - Implement topological sorting
   - **Acceptance**: All JGraphT service tests pass

2. **Graph Evaluator**
   - Implement graph evaluation logic
   - Handle task completion events
   - Handle task failure events
   - Implement global task coordination
   - **Acceptance**: Simple graph executes successfully

3. **Task Queue Publisher**
   - Implement work message publishing
   - Add retry logic for publish failures
   - Update task status to QUEUED
   - **Acceptance**: Tasks appear in queue after evaluation

4. **Worker Main Loop**
   - Implement worker registration
   - Implement heartbeat mechanism
   - Implement work consumption
   - Implement task result publishing
   - **Acceptance**: Worker picks up work and publishes results

5. **Stubbed Task Handlers**
   - Implement stub for DbtHandler
   - Implement stub for BqExtractHandler
   - Implement stub for DataplexHandler
   - Implement stub for DataflowHandler
   - **Acceptance**: All task types can be stubbed in dev

6. **End-to-End Integration Test**
   - Create simple test graph (3 tasks, linear)
   - Test full execution flow
   - Verify all tasks complete
   - **Acceptance**: Integration test passes reliably

### Phase 3: Resilience & Monitoring (Sprint 5-6)

**Goals**: Worker failure recovery, observability, resource limits.

**Stories**:

1. **Worker Monitor**
   - Implement heartbeat checking
   - Implement dead worker detection
   - Publish worker.died events
   - **Acceptance**: Dead workers detected within 60s

2. **Work Recovery Service**
   - Implement orphaned task detection
   - Implement retry logic
   - Implement max retry handling
   - **Acceptance**: Tasks recover after worker death

3. **Race Condition Prevention**
   - Add optimistic locking with version columns
   - Add global task execution lock (unique index)
   - Add event sequence numbers
   - **Acceptance**: Concurrent evaluations don't create duplicates

4. **Metrics Implementation**
   - Add Micrometer metrics
   - Implement MetricsService
   - Add Prometheus endpoint
   - Create Grafana dashboard
   - **Acceptance**: Metrics visible in Prometheus

5. **Structured Logging**
   - Add execution IDs to all log statements
   - Implement structured logging format
   - Add log sanitization for secrets
   - **Acceptance**: Logs parseable in Cloud Logging

6. **OpenTelemetry Tracing**
   - Add tracing instrumentation
   - Implement TracingService
   - Propagate trace context through events
   - **Acceptance**: End-to-end traces visible

7. **Resource Limits**
   - Implement GraphValidator with limits
   - Implement ExecutionThrottler
   - Implement TaskCircuitBreaker
   - **Acceptance**: Limits enforced, violations logged

8. **Data Retention**
   - Implement RetentionService
   - Add cleanup scheduled job
   - Add archive to BigQuery (optional)
   - **Acceptance**: Old executions cleaned up daily

### Phase 4: UI & API (Sprint 7-8)

**Goals**: Web UI for visualization and management.

**Stories**:

1. **Graph List Page**
   - Create GraphController
   - Create graphs-list.html template
   - Integrate Tabulator.js
   - Add execute button
   - **Acceptance**: Can view all graphs and trigger executions

2. **Graph Detail Page**
   - Create graph-detail.html template
   - Implement DAG visualization with SVG
   - Add auto-refresh for running executions
   - Show task table
   - **Acceptance**: Can view real-time execution status

3. **Graph History Page**
   - Create graph-history.html template
   - Add date range filters
   - Integrate Tabulator.js for history table
   - **Acceptance**: Can view past executions with filtering

4. **Gantt View**
   - Create gantt-view.html template
   - Integrate vis-timeline.js
   - Show task execution timeline
   - **Acceptance**: Can view execution timeline

5. **REST API**
   - Create GraphApiController
   - Implement trigger endpoint
   - Implement pause/resume endpoints
   - Implement status query endpoint
   - **Acceptance**: API contract tests pass

6. **CSS Styling**
   - Create app.css
   - Style status badges
   - Style DAG visualization
   - Make responsive
   - **Acceptance**: UI looks professional

7. **OAuth Integration**
   - Implement SecurityService
   - Add JWT token validation
   - Add role-based permissions
   - Protect all endpoints
   - **Acceptance**: Unauthorized requests rejected

8. **Cloud Logging Links**
   - Generate Cloud Logging URLs
   - Add log links to task table
   - Include execution ID in log queries
   - **Acceptance**: Log links work correctly

### Phase 5: Production Deployment (Sprint 9-10)

**Goals**: Deploy to GKE, implement real task handlers.

**Stories**:

1. **Real Task Handlers**
   - Implement production DbtHandler
   - Implement production BqExtractHandler
   - Implement production DataplexHandler
   - Implement production DataflowHandler
   - **Acceptance**: All handlers work in prod profile

2. **Pub/Sub Integration**
   - Finish PubSubQueueService implementation
   - Create topics and subscriptions
   - Test message flow
   - **Acceptance**: Messages flow through Pub/Sub

3. **GCP IAM Setup**
   - Create service accounts
   - Assign IAM roles
   - Configure Workload Identity
   - **Acceptance**: Pods can authenticate to GCP services

4. **Kubernetes Manifests**
   - Create all K8s YAML files
   - Configure Cloud SQL proxy
   - Configure HPA
   - Configure Ingress
   - **Acceptance**: All manifests apply successfully

5. **CI/CD Pipeline**
   - Setup Cloud Build
   - Create build triggers
   - Implement automated testing
   - Implement automated deployment
   - **Acceptance**: Commits trigger builds and deployments

6. **Monitoring & Alerting**
   - Setup Prometheus scraping
   - Create Grafana dashboards
   - Configure alerting rules
   - Integrate with Dynatrace
   - **Acceptance**: Alerts fire for errors

7. **Documentation**
   - Write deployment runbook
   - Document configuration options
   - Create troubleshooting guide
   - Document API endpoints
   - **Acceptance**: Team can deploy and operate

8. **Load Testing**
   - Create JMeter test plans
   - Run load tests
   - Tune performance
   - Verify HPA behavior
   - **Acceptance**: Meets performance targets

9. **Production Readiness Review**
   - Security audit
   - Performance review
   - Disaster recovery plan
   - Incident response procedures
   - **Acceptance**: Sign-off from stakeholders

10. **Go-Live**
   - Deploy to production
   - Migrate first workflow
   - Monitor closely
   - **Acceptance**: First production workflow executes successfully

### Phase 6: Iteration & Enhancement (Ongoing)

**Future Enhancements** (not in initial scope):

1. **Configuration Hot Reload**
   - Watch ConfigMap for changes
   - Reload without restart
   - Version graph definitions

2. **High Availability**
   - Multiple orchestrator instances
   - Leader election
   - Distributed coordination

3. **Conditional Execution**
   - Task predicates
   - Branch tasks
   - Dynamic task generation

4. **Cross-Graph Dependencies**
   - Graph-level dependencies
   - Complex trigger rules

5. **Advanced Scheduling**
   - Cron-based triggers
   - Event-based triggers (beyond Pub/Sub)
   - Backfill capabilities

6. **Cost Tracking**
   - Parse task results for billing info
   - Aggregate costs per graph
   - Cost alerts

7. **Audit & Compliance**
   - Detailed audit logs
   - Compliance reporting
   - Data lineage tracking

8. **Enhanced UI**
   - Graph editor (visual DAG builder)
   - Execution comparison
   - Advanced filtering and search
   - Email/Slack notifications

## Operational Considerations

### Failure Scenarios & Recovery

**Cloud SQL Unavailable**:
- **Impact**: Orchestrator cannot read/write state
- **Detection**: Health check fails, database connection errors
- **Recovery**:
   - GKE restarts orchestrator pod
   - Pod waits for Cloud SQL to become available
   - No data loss (Postgres handles failures)
- **Mitigation**: Cloud SQL HA configuration

**Pub/Sub Unavailable**:
- **Impact**: Tasks cannot be queued, events cannot be published
- **Detection**: Publish operations fail
- **Recovery**:
   - Orchestrator retries with exponential backoff
   - Messages are not lost (stored in DB state)
   - When Pub/Sub recovers, pending tasks are re-queued
- **Mitigation**: Pub/Sub is highly available, regional outages rare

**Orchestrator Pod Dies**:
- **Impact**: In-progress evaluations lost (but not graph state)
- **Detection**: Kubernetes liveness probe fails
- **Recovery**:
   - GKE immediately starts new pod
   - New pod loads graph definitions
   - Running executions continue from DB state
   - Tasks in queue continue to be processed by workers
- **Mitigation**: Keep orchestrator stateless, rely on DB for state

**Worker Pod Dies**:
- **Impact**: Task execution interrupted
- **Detection**: No heartbeat for 60s
- **Recovery**:
   - WorkerMonitor marks worker dead
   - WorkRecoveryService re-queues task
   - Task retries up to max_retries
- **Mitigation**: Set appropriate retry policies per task

**Database Corruption**:
- **Impact**: Potential data loss
- **Detection**: Query errors, constraint violations
- **Recovery**:
   - Restore from Cloud SQL automated backup
   - Replay recent events if available
- **Mitigation**:
   - Cloud SQL automated backups (daily + point-in-time)
   - Regular backup testing

**Full Cluster Outage**:
- **Impact**: All orchestration stops
- **Detection**: All pods unreachable
- **Recovery**:
   - Failover to backup GKE cluster (manual)
   - Restore database from backup
   - Redeploy all components
- **Mitigation**: Multi-region setup (future enhancement)

### Monitoring & Alerting

**Critical Alerts**:
1. **Orchestrator Pod Down**
   - Trigger: No healthy pods for 2 minutes
   - Action: Page on-call engineer

2. **Database Connection Failed**
   - Trigger: Connection errors > 10/minute
   - Action: Page on-call engineer

3. **Worker Pods Scaling Issues**
   - Trigger: Queue depth > 1000 but workers not scaling
   - Action: Alert team, check HPA

4. **Task Failure Rate High**
   - Trigger: Task failure rate > 20% over 10 minutes
   - Action: Alert team, investigate tasks

5. **Stalled Graph Executions**
   - Trigger: Graph in STALLED status for > 30 minutes
   - Action: Alert team, review dependencies

**Warning Alerts**:
1. **Queue Depth Growing**
   - Trigger: Queue depth > 500
   - Action: Monitor, may need more workers

2. **Slow Task Execution**
   - Trigger: p99 task duration > 2x normal
   - Action: Investigate specific tasks

3. **Circuit Breaker Open**
   - Trigger: Circuit breaker open for any task
   - Action: Investigate failing task

**Dashboards**:
1. **Overview Dashboard**
   - Active graph executions
   - Task completion rate
   - Worker count
   - Queue depth

2. **Performance Dashboard**
   - Task duration histograms by type
   - Graph evaluation latency
   - Database query performance
   - Queue publish/consume rates

3. **Reliability Dashboard**
   - Error rates by component
   - Worker death rate
   - Task retry rate
   - Circuit breaker status

### Troubleshooting Guide

**Problem: Graph execution stuck in RUNNING**
```
1. Check graph detail page - which tasks are pending?
2. Look at task dependencies - are upstream tasks completed?
3. Check task_executions table:
   SELECT * FROM task_executions WHERE graph_execution_id = '<id>';
4. If tasks are QUEUED but not RUNNING:
   - Check worker count: kubectl get pods -n orchestrator
   - Check queue depth in Pub/Sub console
   - Check worker logs for errors
5. If tasks are PENDING but should be QUEUED:
   - Check orchestrator logs for evaluation errors
   - Manually trigger evaluation via API
```

**Problem: Tasks failing repeatedly**
```
1. Check task logs via Cloud Logging link
2. Look at error message in task_executions table
3. Common issues:
   - Invalid config (typo in YAML)
   - Missing GCP permissions
   - Resource not found (table, bucket, etc.)
   - Timeout too short
4. Fix and retry:
   - Update ConfigMap
   - Restart pods to reload config
   - Trigger new execution
```

**Problem: Workers not scaling**
```
1. Check HPA status:
   kubectl get hpa -n orchestrator
2. Check metric server:
   kubectl top pods -n orchestrator
3. Check Pub/Sub metric:
   gcloud pubsub subscriptions describe orchestrator-work-sub
4. If metric not available:
   - Check Workload Identity binding
   - Check IAM permissions
5. Manual scale if needed:
   kubectl scale deployment/workers -n orchestrator --replicas=10
```

**Problem: High database CPU**
```
1. Check slow query log in Cloud SQL
2. Look for missing indexes
3. Check for lock contention
4. Consider:
   - Adding indexes
   - Optimizing queries
   - Scaling up Cloud SQL instance
```

### Backup & Restore

**Backup Strategy**:
- Cloud SQL automated backups: Daily at 2 AM UTC
- Point-in-time recovery: Enabled (7 days)
- ConfigMap backups: Stored in Git
- Docker images: Tagged and stored in GCR

**Restore Procedure**:
```bash
# 1. Restore database from backup
gcloud sql backups restore <BACKUP_ID> \
    --backup-instance=<INSTANCE> \
    --backup-project=<PROJECT>

# 2. Verify data
kubectl exec -it deployment/orchestrator -n orchestrator -- \
    psql -h cloudsql-proxy -U orchestrator -d orchestrator \
    -c "SELECT COUNT(*) FROM graphs;"

# 3. Restart pods to reconnect
kubectl rollout restart deployment/orchestrator -n orchestrator
kubectl rollout restart deployment/workers -n orchestrator

# 4. Verify system health
kubectl get pods -n orchestrator
curl https://orchestrator.example.com/q/health
```

**Disaster Recovery RTO/RPO**:
- **RTO** (Recovery Time Objective): 1 hour
- **RPO** (Recovery Point Objective): 5 minutes (point-in-time recovery)

### Security Considerations

**Secrets Management**:
- Database passwords: Kubernetes Secrets
- OAuth client secrets: Kubernetes Secrets
- GCP service account keys: Workload Identity (no keys stored)

**Network Security**:
- Ingress: TLS termination with cert-manager
- Internal communication: Within VPC, no public endpoints
- Cloud SQL: Private IP only

**Access Control**:
- API: OAuth token required
- UI: OAuth login required
- Kubernetes: RBAC configured

**Audit Logging**:
- All API calls logged with user identity
- All graph executions logged with trigger source
- All configuration changes logged

**Compliance**:
- Data residency: Configurable by region
- Data retention: Configurable, default 90 days
- PII handling: No PII stored in system

## Conclusion

This design provides a complete, production-ready event-driven orchestrator for GKE with the following key characteristics:

**Strengths**:
- Simple, understandable architecture
- Event-driven for loose coupling
- Resilient to worker and orchestrator failures
- Scalable via HPA
- Observable via metrics, logs, and traces
- Easy local development with H2 and Hazelcast
- Clean separation of concerns via repository pattern and queue abstraction

**Limitations** (acceptable for v1):
- Single orchestrator instance (no HA)
- No configuration hot reload
- No conditional task execution
- No cross-graph dependencies
- Tasks must be idempotent (system doesn't enforce)

**Future Enhancements**:
- High availability orchestrator with leader election
- Configuration hot reload
- Advanced scheduling (cron, complex triggers)
- Cost tracking and budget alerts
- Visual graph editor
- Enhanced monitoring and alerting

The phased implementation approach ensures early value delivery while building toward a robust, production-grade system.# GKE Event-Driven Orchestrator Design Document

## Table of Contents
1. [Overview](#overview)
2. [Architecture](#architecture)
3. [Data Model](#data-model)
4. [Configuration Schema](#configuration-schema)
5. [Core Components](#core-components)
6. [Interfaces and Abstractions](#interfaces-and-abstractions)
7. [Environment Profiles](#environment-profiles)
8. [Deployment](#deployment)
9. [Development Workflow](#development-workflow)
10. [Testing Strategy](#testing-strategy)
11. [Implementation Phases](#implementation-phases)

## Overview

### Purpose
Build a Quarkus-based workflow orchestrator running on Google Kubernetes Engine (GKE) that enables event-driven execution of data pipeline tasks. The system provides visualization of workflow state similar to Apache Airflow, with a focus on simplicity and developer experience.

### Key Requirements
- Event-driven architecture using Pub/Sub for task coordination
- SSR-based UI for workflow visualization (no React)
- Worker pods that pull tasks from queues
- Support for DBT, BigQuery Extract, Dataplex, and Dataflow tasks
- Management topic for worker lifecycle commands
- Worker heartbeat mechanism with automatic work recovery
- State persistence in Cloud SQL (Postgres)
- Graph evaluation only on event arrival (not polling)
- JGraphT for DAG management
- Support for global tasks (shared across multiple graphs)
- Repository pattern for database abstraction
- Queue abstraction for local development

### Technology Stack
- **Framework**: Quarkus with Vert.x event bus
- **Graph Library**: JGraphT
- **Database**: Cloud SQL Postgres (production), H2 (local dev)
- **Messaging**: Google Cloud Pub/Sub (production), Hazelcast (local dev)
- **Container Orchestration**: GKE with HPA
- **UI**: Qute templates (SSR)
- **Language**: Java 21+

## Architecture

### System Context Diagram

```
┌─────────────────────────────────────────────────────────────────┐
│                         GKE Cluster                              │
│                                                                  │
│  ┌────────────────────┐           ┌─────────────────────────┐  │
│  │   Orchestrator     │           │   Worker Pods (HPA)     │  │
│  │    (Quarkus)       │           │                         │  │
│  │                    │           │  ┌────────┐ ┌────────┐  │  │
│  │  - Graph Loader    │           │  │Worker 1│ │Worker 2│  │  │
│  │  - Event Consumer  │           │  │        │ │        │  │  │
│  │  - Graph Evaluator │           │  └────────┘ └────────┘  │  │
│  │  - Worker Monitor  │           │         ...              │  │
│  │  - UI (SSR)        │           │                         │  │
│  │  - REST API        │           │  Each worker:           │  │
│  │                    │           │  - Pulls from queue     │  │
│  └────────────────────┘           │  - Executes tasks       │  │
│           │                       │  - Sends heartbeats     │  │
│           │                       │  - Publishes results    │  │
│           │                       └─────────────────────────┘  │
└───────────┼──────────────────────────────────────────────────────┘
            │
    ┌───────┴────────┐
    ▼                ▼
┌─────────┐    ┌──────────────┐      ┌──────────────────┐
│Cloud SQL│    │  Pub/Sub     │      │  Config Files    │
│Postgres │    │              │      │  (ConfigMap)     │
│         │    │ ┌──────────┐ │      │                  │
│State &  │    │ │Task Queue│ │      │  graphs/*.yaml   │
│Metadata │    │ └──────────┘ │      │  tasks/*.yaml    │
└─────────┘    │ ┌──────────┐ │      └──────────────────┘
               │ │Management│ │
               │ │  Topic   │ │
               │ └──────────┘ │
               │ ┌──────────┐ │
               │ │  Events  │ │
               │ │  Topic   │ │
               │ └──────────┘ │
               └──────────────┘
```

### Component Interaction Flow

#### Graph Evaluation Flow
```
1. External Event arrives (task completed/failed)
   ↓
2. ExternalEventConsumer bridges to internal event bus
   ↓
3. GraphEvaluator receives internal event
   ↓
4. Update task status in repository
   ↓
5. If global task → find all affected graphs
   ↓
6. Build DAG using JGraphT
   ↓
7. Determine ready tasks (all dependencies met)
   ↓
8. For each ready task:
   - Create TaskExecution record
   - Publish "task.ready" event
   ↓
9. TaskQueuePublisher receives "task.ready"
   ↓
10. Publish WorkMessage to queue (Pub/Sub or Hazelcast)
    ↓
11. Worker pulls message
    ↓
12. Worker executes task
    ↓
13. Worker publishes result to events topic
    ↓
14. Loop back to step 1
```

#### Worker Lifecycle Flow
```
1. Worker pod starts
   ↓
2. Worker sends registration message to management topic
   ↓
3. WorkerMonitor creates worker record
   ↓
4. Worker starts heartbeat timer (every 10s)
   ↓
5. WorkerMonitor checks heartbeats (every 30s)
   ↓
6. If no heartbeat for 60s:
   - Mark worker as dead
   - Publish "worker.died" event
   ↓
7. WorkRecoveryService receives "worker.died"
   ↓
8. Find all assigned work for dead worker
   ↓
9. For each orphaned task:
   - If retries remaining → publish "task.retry"
   - Else → mark as failed
   ↓
10. GraphEvaluator re-evaluates affected graphs
```

## Data Model

### Database Schema

All timestamps are stored in UTC. The schema uses UUID for primary keys to avoid conflicts in distributed systems.

#### Tables

##### `graphs`
Stores graph definitions loaded from YAML configuration files.

```sql
CREATE TABLE graphs (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name VARCHAR(255) UNIQUE NOT NULL,
    description TEXT,
    yaml_content TEXT NOT NULL,  -- Original YAML for reference/debugging
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP NOT NULL DEFAULT NOW(),
    CONSTRAINT graphs_name_format CHECK (name ~ '^[a-z0-9-]+$')
);

CREATE INDEX idx_graphs_name ON graphs(name);
```

##### `tasks`
Task definitions, both global (shared across graphs) and graph-specific.

```sql
CREATE TABLE tasks (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name VARCHAR(255) UNIQUE NOT NULL,
    type VARCHAR(50) NOT NULL,  -- 'dbt', 'bq_extract', 'dataplex', 'dataflow'
    config JSONB NOT NULL,
    retry_policy JSONB NOT NULL DEFAULT '{"max_retries": 3, "backoff": "exponential", "initial_delay_seconds": 30}'::jsonb,
    timeout_seconds INT NOT NULL DEFAULT 3600,
    is_global BOOLEAN NOT NULL DEFAULT FALSE,
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP NOT NULL DEFAULT NOW(),
    CONSTRAINT tasks_name_format CHECK (name ~ '^[a-z0-9-]+$'),
    CONSTRAINT tasks_type_valid CHECK (type IN ('dbt', 'bq_extract', 'dataplex', 'dataflow')),
    CONSTRAINT tasks_timeout_positive CHECK (timeout_seconds > 0)
);

CREATE INDEX idx_tasks_name ON tasks(name);
CREATE INDEX idx_tasks_global ON tasks(name) WHERE is_global = TRUE;
CREATE INDEX idx_tasks_type ON tasks(type);
```

##### `graph_edges`
Defines the DAG structure within each graph (task dependencies).

```sql
CREATE TABLE graph_edges (
    graph_id UUID NOT NULL REFERENCES graphs(id) ON DELETE CASCADE,
    from_task_id UUID NOT NULL REFERENCES tasks(id) ON DELETE CASCADE,
    to_task_id UUID NOT NULL REFERENCES tasks(id) ON DELETE CASCADE,
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    PRIMARY KEY (graph_id, from_task_id, to_task_id),
    CONSTRAINT graph_edges_no_self_reference CHECK (from_task_id != to_task_id)
);

CREATE INDEX idx_graph_edges_graph ON graph_edges(graph_id);
CREATE INDEX idx_graph_edges_from ON graph_edges(from_task_id);
CREATE INDEX idx_graph_edges_to ON graph_edges(to_task_id);
```

##### `graph_executions`
Tracks execution instances of graphs.

```sql
CREATE TABLE graph_executions (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    graph_id UUID NOT NULL REFERENCES graphs(id) ON DELETE CASCADE,
    status VARCHAR(20) NOT NULL DEFAULT 'running',
    started_at TIMESTAMP NOT NULL DEFAULT NOW(),
    completed_at TIMESTAMP,
    triggered_by VARCHAR(255) NOT NULL,  -- Event ID or system trigger
    error_message TEXT,
    CONSTRAINT graph_executions_status_valid CHECK (
        status IN ('running', 'completed', 'failed', 'stalled')
    ),
    CONSTRAINT graph_executions_completed_when_done CHECK (
        (status IN ('completed', 'failed') AND completed_at IS NOT NULL) OR
        (status IN ('running', 'stalled') AND completed_at IS NULL)
    )
);

CREATE INDEX idx_graph_executions_graph ON graph_executions(graph_id);
CREATE INDEX idx_graph_executions_status ON graph_executions(status);
CREATE INDEX idx_graph_executions_started ON graph_executions(started_at DESC);
```

##### `task_executions`
Tracks individual task executions within graph executions.

```sql
CREATE TABLE task_executions (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    graph_execution_id UUID NOT NULL REFERENCES graph_executions(id) ON DELETE CASCADE,
    task_id UUID NOT NULL REFERENCES tasks(id) ON DELETE CASCADE,
    status VARCHAR(20) NOT NULL DEFAULT 'pending',
    attempt INT NOT NULL DEFAULT 0,
    assigned_worker_id VARCHAR(255),
    queued_at TIMESTAMP,
    started_at TIMESTAMP,
    completed_at TIMESTAMP,
    error_message TEXT,
    result JSONB,
    -- For global tasks: link to the canonical execution
    global_execution_id UUID REFERENCES task_executions(id),
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    CONSTRAINT task_executions_unique_per_graph UNIQUE(graph_execution_id, task_id),
    CONSTRAINT task_executions_status_valid CHECK (
        status IN ('pending', 'queued', 'running', 'completed', 'failed', 'skipped')
    ),
    CONSTRAINT task_executions_attempt_positive CHECK (attempt >= 0),
    CONSTRAINT task_executions_timestamps_ordered CHECK (
        (queued_at IS NULL OR started_at IS NULL OR queued_at <= started_at) AND
        (started_at IS NULL OR completed_at IS NULL OR started_at <= completed_at)
    )
);

CREATE INDEX idx_task_executions_graph ON task_executions(graph_execution_id);
CREATE INDEX idx_task_executions_task ON task_executions(task_id);
CREATE INDEX idx_task_executions_status ON task_executions(status);
CREATE INDEX idx_task_executions_worker ON task_executions(assigned_worker_id) 
    WHERE assigned_worker_id IS NOT NULL;
CREATE INDEX idx_task_executions_global ON task_executions(global_execution_id) 
    WHERE global_execution_id IS NOT NULL;
CREATE INDEX idx_task_executions_running_global ON task_executions(task_id) 
    WHERE status = 'running' AND global_execution_id IS NULL;
```

##### `workers`
Registry of active and dead workers.

```sql
CREATE TABLE workers (
    id VARCHAR(255) PRIMARY KEY,  -- Kubernetes pod name
    status VARCHAR(20) NOT NULL DEFAULT 'active',
    last_heartbeat TIMESTAMP NOT NULL DEFAULT NOW(),
    registered_at TIMESTAMP NOT NULL DEFAULT NOW(),
    capabilities JSONB NOT NULL DEFAULT '{}'::jsonb,  -- For future heterogeneous workers
    metadata JSONB NOT NULL DEFAULT '{}'::jsonb,  -- Pod IP, node name, etc.
    CONSTRAINT workers_status_valid CHECK (status IN ('active', 'dead'))
);

CREATE INDEX idx_workers_status ON workers(status);
CREATE INDEX idx_workers_heartbeat ON workers(last_heartbeat) WHERE status = 'active';
```

##### `work_assignments`
Tracks which worker is assigned to which task (for failure recovery).

```sql
CREATE TABLE work_assignments (
    task_execution_id UUID PRIMARY KEY REFERENCES task_executions(id) ON DELETE CASCADE,
    worker_id VARCHAR(255) NOT NULL REFERENCES workers(id) ON DELETE CASCADE,
    assigned_at TIMESTAMP NOT NULL DEFAULT NOW(),
    heartbeat_deadline TIMESTAMP NOT NULL,
    CONSTRAINT work_assignments_deadline_future CHECK (heartbeat_deadline > assigned_at)
);

CREATE INDEX idx_work_assignments_worker ON work_assignments(worker_id);
CREATE INDEX idx_work_assignments_deadline ON work_assignments(heartbeat_deadline);
```

### Entity Relationships

```
graphs 1──────* graph_edges *──────1 tasks
  │                                    │
  │                                    │
  │ 1                                  │ 1
  │                                    │
  * graph_executions                   * task_executions
        │                                    │
        │ 1                                  │ *
        │                                    │
        *────────────────────────────────────*
                                             │
                                             │ 1
                                             │
                                             * work_assignments * ────1 workers
```

### Enums and Constants

```java
public enum TaskType {
    DBT("dbt"),
    BQ_EXTRACT("bq_extract"),
    DATAPLEX("dataplex"),
    DATAFLOW("dataflow");
    
    private final String value;
}

public enum TaskStatus {
    PENDING,    // Created but not yet queued
    QUEUED,     // Published to queue
    RUNNING,    // Worker is executing
    COMPLETED,  // Successfully finished
    FAILED,     // Failed (may retry)
    SKIPPED     // Skipped due to upstream failure
}

public enum GraphStatus {
    RUNNING,    // In progress
    COMPLETED,  // All tasks completed
    FAILED,     // One or more tasks failed permanently
    STALLED     // No tasks can make progress
}

public enum WorkerStatus {
    ACTIVE,     // Receiving heartbeats
    DEAD        // No heartbeat for >60s
}

public class Constants {
    public static final int WORKER_HEARTBEAT_INTERVAL_SECONDS = 10;
    public static final int WORKER_DEAD_THRESHOLD_SECONDS = 60;
    public static final int WORKER_MONITOR_CHECK_INTERVAL_SECONDS = 30;
    public static final String CONFIG_PATH = "/config";
    public static final String GRAPHS_PATTERN = "graphs/*.yaml";
    public static final String TASKS_PATTERN = "tasks/*.yaml";
}
```

## Configuration Schema

### Task Definition YAML

Task definitions are reusable components that can be referenced by multiple graphs.

**Location**: `tasks/{task-name}.yaml`

**Schema**:
```yaml
# Unique task name (must match filename without .yaml extension)
name: string                # Required. Pattern: ^[a-z0-9-]+$

# Task type determines which handler executes it
type: enum                  # Required. One of: dbt, bq_extract, dataplex, dataflow

# Is this task shared across multiple graphs?
global: boolean             # Optional. Default: false

# Retry configuration
retry_policy:               # Optional
  max_retries: integer      # Default: 3
  backoff: enum             # Default: exponential. One of: exponential, linear, constant
  initial_delay_seconds: integer  # Default: 30
  max_delay_seconds: integer      # Optional. Default: 3600

# Maximum execution time
timeout_seconds: integer    # Optional. Default: 3600

# Task-specific configuration (passed to task handler)
config: object              # Required. Structure depends on task type
```

**Example - DBT Task**:
```yaml
# tasks/load-market-data.yaml
name: load-market-data
type: dbt
global: true
retry_policy:
  max_retries: 3
  backoff: exponential
  initial_delay_seconds: 30
timeout_seconds: 600
config:
  project: analytics
  models: +market_data
  vars:
    region: us
    refresh_mode: full
  target: prod
```

**Example - BigQuery Extract Task**:
```yaml
# tasks/extract-prices.yaml
name: extract-prices
type: bq_extract
timeout_seconds: 300
config:
  project: my-project
  dataset: raw_data
  table: prices
  destination: gs://my-bucket/prices/*.parquet
  format: parquet
  compression: snappy
```

**Example - Dataplex Task**:
```yaml
# tasks/run-quality-checks.yaml
name: run-quality-checks
type: dataplex
timeout_seconds: 1800
config:
  project: my-project
  location: us-central1
  lake: analytics-lake
  zone: raw-zone
  scan: market-data-quality
  wait_for_completion: true
```

**Example - Dataflow Task**:
```yaml
# tasks/aggregate-metrics.yaml
name: aggregate-metrics
type: dataflow
timeout_seconds: 3600
retry_policy:
  max_retries: 2
  backoff: exponential
config:
  project: my-project
  region: us-central1
  template: gs://my-bucket/templates/aggregate-market-metrics
  temp_location: gs://my-bucket/temp
  parameters:
    input: gs://my-bucket/prices/*.parquet
    output: my-project:analytics.market_metrics
    window_size: 1h
```

### Graph Definition YAML

Graphs define workflows as DAGs of tasks.

**Location**: `graphs/{graph-name}.yaml`

**Schema**:
```yaml
# Unique graph name (must match filename without .yaml extension)
name: string                # Required. Pattern: ^[a-z0-9-]+$

# Human-readable description (supports multi-line)
description: string         # Optional

# List of tasks in this graph
tasks: array                # Required. At least one task
  - # Option 1: Reference to existing task definition
    task: string            # Task name from tasks/*.yaml
    
  - # Option 2: Inline task definition
    name: string            # Required
    type: enum              # Required
    timeout_seconds: integer
    retry_policy: object
    config: object
    depends_on: array       # Optional. List of task names this depends on
      - string
```

**Example - Market Pipeline**:
```yaml
# graphs/market-pipeline.yaml
name: market-pipeline
description: |
  Daily market data processing pipeline.
  Runs after market close to process and aggregate trading data.
  
  Steps:
  1. Load raw market data (DBT)
  2. Extract prices to GCS (BigQuery)
  3. Run data quality checks (Dataplex)
  4. Aggregate metrics (Dataflow)

tasks:
  # Reference to global task
  - task: load-market-data
  
  # Inline task with dependencies
  - name: extract-prices
    type: bq_extract
    timeout_seconds: 300
    config:
      project: my-project
      dataset: raw_data
      table: prices
      destination: gs://my-bucket/prices/*.parquet
      format: parquet
    depends_on:
      - load-market-data
  
  - name: run-dataplex-quality
    type: dataplex
    timeout_seconds: 1800
    config:
      project: my-project
      lake: analytics-lake
      scan: market-data-quality
    depends_on:
      - extract-prices
  
  - name: aggregate-metrics
    type: dataflow
    timeout_seconds: 3600
    config:
      project: my-project
      template: gs://my-bucket/templates/aggregate-market-metrics
      parameters:
        input: gs://my-bucket/prices/*.parquet
        output: my-project:analytics.market_metrics
    depends_on:
      - run-dataplex-quality
```

**Example - Multi-Source Graph**:
```yaml
# graphs/data-fusion.yaml
name: data-fusion
description: |
  Combines multiple data sources for reporting.

tasks:
  # Two independent tasks can run in parallel
  - task: load-market-data  # Global task
  
  - name: load-customer-data
    type: dbt
    config:
      project: analytics
      models: +customer_data
  
  # This task waits for both
  - name: join-datasets
    type: bq_extract
    config:
      query: |
        SELECT * FROM market_data m
        JOIN customer_data c ON m.customer_id = c.id
      destination: gs://my-bucket/joined/*.parquet
    depends_on:
      - load-market-data
      - load-customer-data
  
  - name: generate-report
    type: dataflow
    config:
      template: gs://my-bucket/templates/report-generator
      parameters:
        input: gs://my-bucket/joined/*.parquet
    depends_on:
      - join-datasets
```

### Validation Rules

The system must validate YAML configurations on load:

1. **Task Validation**:
   - Name matches `^[a-z0-9-]+$` pattern
   - Type is one of the supported types
   - Timeout is positive integer
   - Config structure matches task type requirements
   - If global=true, task can be referenced by multiple graphs

2. **Graph Validation**:
   - Name matches `^[a-z0-9-]+$` pattern
   - At least one task defined
   - All task references resolve to existing tasks
   - All `depends_on` references exist in the graph
   - No circular dependencies (DAG check using JGraphT)
   - Each task appears at most once in the graph

3. **Global Task Rules**:
   - Global tasks can only be defined in `tasks/*.yaml` (not inline in graphs)
   - Multiple graphs can reference the same global task
   - When a global task completes, it marks completed for ALL graphs containing it

## Core Components

### Orchestrator Service Components

**Purpose**: Load graph and task definitions from YAML files on startup.

**Lifecycle**: Executes once on application startup.

**Responsibilities**:
- Read all files matching `tasks/*.yaml` and `graphs/*.yaml`
- Parse YAML into domain objects
- Validate configurations
- Detect circular dependencies using JGraphT
- Upsert into database via repositories
- Log warnings for any invalid configurations

**Interface**:
```java
@ApplicationScoped
public class GraphLoader {
    @Inject TaskRepository taskRepository;
    @Inject GraphRepository graphRepository;
    @Inject GraphValidator graphValidator;
    @Inject Logger logger;
    
    @ConfigProperty(name = "orchestrator.config.path")
    String configPath;
    
    void onStart(@Observes StartupEvent event) {
        logger.info("Loading task and graph definitions from {}", configPath);
        loadTasks();
        loadGraphs();
        logger.info("Configuration loading complete");
    }
    
    private void loadTasks() {
        Path tasksDir = Path.of(configPath, "tasks");
        // Load all .yaml files
        // Parse with SnakeYAML
        // Validate each task
        // Upsert via taskRepository
    }
    
    private void loadGraphs() {
        Path graphsDir = Path.of(configPath, "graphs");
        // Load all .yaml files
        // Parse with SnakeYAML
        // Resolve task references
        // Validate DAG structure
        // Upsert via graphRepository
    }
}
```

**Error Handling**:
- Invalid YAML: Log error, skip file, continue loading others
- Circular dependency: Log error, reject graph, fail startup
- Missing task reference: Log error, reject graph, fail startup
- Duplicate names: Log error, reject duplicate, keep first loaded

**Configuration**:
```properties
# application.properties
orchestrator.config.path=/config
orchestrator.config.reload-on-change=false  # Future: watch for changes
```

#### 2. ExternalEventConsumer

**Purpose**: Bridge external messaging system (Pub/Sub or Hazelcast) to internal Vert.x event bus.

**Lifecycle**: Runs continuously, consuming from external topics.

**Responsibilities**:
- Consume messages from `events-topic` (task results)
- Consume messages from `management-topic` (worker heartbeats, commands)
- Transform external message formats to internal events
- Publish to Vert.x event bus
- Handle deserialization errors gracefully

**Interface**:
```java
@ApplicationScoped
public class ExternalEventConsumer {
    @Inject EventBus eventBus;
    @Inject Logger logger;
    
    @Incoming("external-events")  // Configured per environment
    public CompletionStage<Void> onTaskEvent(Message<TaskEventMessage> message) {
        TaskEventMessage external = message.getPayload();
        logger.debug("Received task event: {}", external);
        
        try {
            OrchestratorEvent internalEvent = switch(external.status()) {
                case "STARTED" -> new TaskStartedEvent(
                    external.executionId(), 
                    external.workerId()
                );
                case "COMPLETED" -> new TaskCompletedEvent(
                    external.executionId(), 
                    external.result()
                );
                case "FAILED" -> new TaskFailedEvent(
                    external.executionId(), 
                    external.errorMessage(),
                    external.attempt()
                );
                default -> {
                    logger.warn("Unknown task status: {}", external.status());
                    yield null;
                }
            };
            
            if (internalEvent != null) {
                String address = "task." + external.status().toLowerCase();
                eventBus.publish(address, internalEvent);
            }
            
            return message.ack();
        } catch (Exception e) {
            logger.error("Error processing task event", e);
            return message.nack(e);
        }
    }
    
    @Incoming("external-management")
    public CompletionStage<Void> onManagementEvent(Message<ManagementEventMessage> message) {
        ManagementEventMessage external = message.getPayload();
        
        try {
            if ("HEARTBEAT".equals(external.type())) {
                eventBus.publish("worker.heartbeat", new WorkerHeartbeatEvent(
                    external.workerId(),
                    external.timestamp()
                ));
            } else if ("REGISTERED".equals(external.type())) {
                eventBus.publish("worker.registered", new WorkerRegisteredEvent(
                    external.workerId(),
                    external.metadata()
                ));
            }
            
            return message.ack();
        } catch (Exception e) {
            logger.error("Error processing management event", e);
            return message.nack(e);
        }
    }
}
```

**Message Formats**:

*TaskEventMessage (from workers)*:
```json
{
  "executionId": "uuid",
  "taskName": "load-market-data",
  "status": "COMPLETED|FAILED|STARTED",
  "workerId": "worker-abc123",
  "timestamp": "2025-10-17T10:30:00Z",
  "attempt": 1,
  "result": {},
  "errorMessage": "optional error"
}
```

*ManagementEventMessage (from workers)*:
```json
{
  "type": "HEARTBEAT|REGISTERED",
  "workerId": "worker-abc123",
  "timestamp": "2025-10-17T10:30:00Z",
  "metadata": {
    "podIp": "10.0.1.5",
    "nodeName": "gke-node-1",
    "capabilities": ["dbt", "bq_extract"]
  }
}
```

#### 3. GraphEvaluator

**Purpose**: Core orchestration logic - evaluate graph state and schedule ready tasks.

**Lifecycle**: Event-driven, triggered by internal events.

**Responsibilities**:
- Build DAG from graph definition using JGraphT
- Determine which tasks are ready to execute (dependencies met)
- Handle global task coordination
- Detect graph completion or stalling
- Publish task scheduling events

**Event Listeners**:
- `task.completed` - Task finished successfully
- `task.failed` - Task failed
- `graph.evaluate` - Explicit request to evaluate
- `task.retry` - Retry a failed task

**Interface**:
```java
@ApplicationScoped
public class GraphEvaluator {
    @Inject EventBus eventBus;
    @Inject JGraphTService graphService;
    @Inject TaskExecutionRepository taskExecutionRepository;
    @Inject GraphExecutionRepository graphExecutionRepository;
    @Inject Logger logger;
    
    @ConsumeEvent("task.completed")
    @ConsumeEvent("task.failed")
    public void onTaskStatusChange(TaskEvent event) {
        logger.info("Task {} changed to {}", event.executionId(), event.status());
        
        // Update database
        taskExecutionRepository.updateStatus(
            event.executionId(), 
            TaskStatus.valueOf(event.status()),
            event.errorMessage(),
            event.result()
        );
        
        // Find graph execution(s) to re-evaluate
        TaskExecution te = taskExecutionRepository.findById(event.executionId());
        
        if (te.task().isGlobal()) {
            // Global task affects multiple graphs
            List<UUID> affectedGraphs = 
                taskExecutionRepository.findGraphsContainingTask(te.task().name());
            
            logger.info("Global task {} affects {} graphs", 
                te.task().name(), affectedGraphs.size());
            
            affectedGraphs.forEach(graphExecId -> 
                eventBus.publish("graph.evaluate", graphExecId)
            );
        } else {
            // Single graph affected
            eventBus.publish("graph.evaluate", te.graphExecutionId());
        }
    }
    
    @ConsumeEvent("graph.evaluate")
    @Transactional
    public void evaluate(UUID graphExecutionId) {
        logger.debug("Evaluating graph execution {}", graphExecutionId);
        
        GraphExecution execution = graphExecutionRepository.findById(graphExecutionId);
        
        // Build DAG
        DirectedAcyclicGraph<Task, DefaultEdge> dag = 
            graphService.buildDAG(execution.graph());
        
        // Get current task states
        Map<Task, TaskStatus> taskStates = 
            taskExecutionRepository.getTaskStates(graphExecutionId);
        
        // Find tasks ready to execute
        Set<Task> readyTasks = graphService.findReadyTasks(dag, taskStates);
        
        logger.info("Found {} ready tasks for graph {}", 
            readyTasks.size(), execution.graph().name());
        
        // Schedule each ready task
        for (Task task : readyTasks) {
            scheduleTask(graphExecutionId, task);
        }
        
        // Check if graph is done
        updateGraphStatus(graphExecutionId, taskStates, readyTasks);
        
        // Publish evaluation complete event
        eventBus.publish("graph.evaluated", new GraphEvaluatedEvent(
            graphExecutionId,
            readyTasks.size()
        ));
    }
    
    private void scheduleTask(UUID graphExecutionId, Task task) {
        if (task.isGlobal()) {
            scheduleGlobalTask(graphExecutionId, task);
        } else {
            scheduleRegularTask(graphExecutionId, task);
        }
    }
    
    private void scheduleGlobalTask(UUID graphExecutionId, Task task) {
        // Check if global task already running
        Optional<TaskExecution> running = 
            taskExecutionRepository.findRunningGlobalExecution(task.name());
        
        if (running.isPresent()) {
            // Link this graph's execution to the running one
            logger.info("Linking to existing global task execution: {}", 
                running.get().id());
            
            taskExecutionRepository.linkToGlobalExecution(
                graphExecutionId, 
                task.id(), 
                running.get().id()
            );
        } else {
            // Start new global execution
            scheduleRegularTask(graphExecutionId, task);
        }
    }
    
    private void scheduleRegularTask(UUID graphExecutionId, Task task) {
        TaskExecution execution = taskExecutionRepository.create(
            graphExecutionId,
            task,
            TaskStatus.PENDING
        );
        
        logger.info("Scheduling task {} for graph execution {}", 
            task.name(), graphExecutionId);
        
        eventBus.publish("task.ready", new TaskReadyEvent(
            execution.id(),
            task
        ));
    }
    
    private void updateGraphStatus(
        UUID graphExecutionId, 
        Map<Task, TaskStatus> taskStates,
        Set<Task> readyTasks
    ) {
        boolean allCompleted = taskStates.values().stream()
            .allMatch(s -> s == TaskStatus.COMPLETED || s == TaskStatus.SKIPPED);
        
        boolean anyFailed = taskStates.values().stream()
            .anyMatch(s -> s == TaskStatus.FAILED);
        
        boolean stalled = readyTasks.isEmpty() && !allCompleted;
        
        GraphStatus newStatus;
        if (allCompleted) {
            newStatus = GraphStatus.COMPLETED;
            eventBus.publish("graph.completed", 
                new GraphCompletedEvent(graphExecutionId));
        } else if (stalled) {
            newStatus = GraphStatus.STALLED;
            eventBus.publish("graph.stalled", 
                new GraphStalledEvent(graphExecutionId));
        } else if (anyFailed) {
            newStatus = GraphStatus.FAILED;
            eventBus.publish("graph.failed", 
                new GraphFailedEvent(graphExecutionId));
        } else {
            newStatus = GraphStatus.RUNNING;
        }
        
        graphExecutionRepository.updateStatus(graphExecutionId, newStatus);
    }
    
    @ConsumeEvent("task.retry")
    public void onTaskRetry(TaskReadyEvent event) {
        logger.info("Retrying task execution {}", event.executionId());
        
        TaskExecution te = taskExecutionRepository.findById(event.executionId());
        
        if (te.attempt() < te.task().retryPolicy().maxRetries()) {
            taskExecutionRepository.incrementAttempt(event.executionId());
            eventBus.publish("task.ready", event);
        } else {
            logger.warn("Max retries exceeded for task execution {}", 
                event.executionId());
            
            taskExecutionRepository.updateStatus(
                event.executionId(),
                TaskStatus.FAILED,
                "Max retries exceeded",
                null
            );
            
            eventBus.publish("task.failed", new TaskFailedEvent(
                event.executionId(),
                "Max retries exceeded",
                te.attempt()
            ));
        }
    }
}
```

#### 4. TaskQueuePublisher

**Purpose**: Publish work to the queue for workers to consume.

**Lifecycle**: Event-driven, triggered by "task.ready" events.

**Responsibilities**:
- Transform internal task events to external work messages
- Publish to appropriate queue (via QueueService interface)
- Update task status to QUEUED
- Handle publishing failures

**Interface**:
```java
@ApplicationScoped
public class TaskQueuePublisher {
    @Inject EventBus eventBus;
    @Inject QueueService queueService;
    @Inject TaskExecutionRepository taskExecutionRepository;
    @Inject Logger logger;
    
    @ConsumeEvent("task.ready")
    public Uni<Void> onTaskReady(TaskReadyEvent event) {
        logger.info("Publishing task {} to queue", event.task().name());
        
        WorkMessage workMessage = new WorkMessage(
            event.executionId(),
            event.task().name(),
            event.task().type(),
            event.task().config(),
            event.task().retryPolicy(),
            event.task().timeoutSeconds()
        );
        
        return queueService.publishWork(workMessage)
            .invoke(() -> {
                taskExecutionRepository.updateStatus(
                    event.executionId(),
                    TaskStatus.QUEUED,
                    null,
                    null
                );
                
                eventBus.publish("task.queued", new TaskQueuedEvent(
                    event.executionId(),
                    event.task()
                ));
            })
            .onFailure().invoke(error -> {
                logger.error("Failed to publish task to queue", error);
                
                // Retry by re-publishing task.ready event after delay
                Uni.createFrom().voidItem()
                    .onItem().delayIt().by(Duration.ofSeconds(5))
                    .subscribe().with(
                        v -> eventBus.publish("task.ready", event)
                    );
            })
            .replaceWithVoid();
    }
}
```

**WorkMessage Format**:
```json
{
  "executionId": "uuid",
  "taskName": "load-market-data",
  "taskType": "dbt",
  "config": {
    "project": "analytics",
    "models": "+market_data"
  },
  "retryPolicy": {
    "maxRetries": 3,
    "backoff": "exponential",
    "initialDelaySeconds": 30
  },
  "timeoutSeconds": 600
}
```

#### 5. WorkerMonitor

**Purpose**: Monitor worker health and recover work from dead workers.

**Lifecycle**: Scheduled task running every 30 seconds.

**Responsibilities**:
- Check worker heartbeats
- Mark workers as dead if no heartbeat for 60 seconds
- Publish "worker.died" events for dead workers
- Update worker heartbeat timestamps

**Interface**:
```java
@ApplicationScoped
public class WorkerMonitor {
    @Inject EventBus eventBus;
    @Inject WorkerRepository workerRepository;
    @Inject Logger logger;
    
    @ConfigProperty(name = "orchestrator.worker.dead-threshold-seconds")
    int deadThresholdSeconds;
    
    @ConsumeEvent("worker.heartbeat")
    @Transactional
    public void onHeartbeat(WorkerHeartbeatEvent event) {
        logger.trace("Heartbeat from worker {}", event.workerId());
        workerRepository.updateHeartbeat(event.workerId(), event.timestamp());
    }
    
    @ConsumeEvent("worker.registered")
    @Transactional
    public void onWorkerRegistered(WorkerRegisteredEvent event) {
        logger.info("Worker registered: {}", event.workerId());
        
        workerRepository.upsert(new Worker(
            event.workerId(),
            WorkerStatus.ACTIVE,
            Instant.now(),
            Instant.now(),
            event.metadata()
        ));
    }
    
    @Scheduled(every = "${orchestrator.worker.monitor-interval}")
    @Transactional
    public void checkDeadWorkers() {
        Instant deadline = Instant.now()
            .minusSeconds(deadThresholdSeconds);
        
        List<Worker> deadWorkers = workerRepository.findDeadWorkers(deadline);
        
        if (!deadWorkers.isEmpty()) {
            logger.warn("Found {} dead workers", deadWorkers.size());
            
            for (Worker worker : deadWorkers) {
                workerRepository.markDead(worker.id());
                
                eventBus.publish("worker.died", new WorkerDiedEvent(
                    worker.id()
                ));
            }
        }
    }
}
```

**Configuration**:
```properties
# application.properties
orchestrator.worker.dead-threshold-seconds=60
orchestrator.worker.monitor-interval=30s
```

#### 6. WorkRecoveryService

**Purpose**: Recover work from dead workers.

**Lifecycle**: Event-driven, triggered by "worker.died" events.

**Responsibilities**:
- Find all tasks assigned to dead worker
- Retry tasks that haven't exceeded max retries
- Fail tasks that have exceeded max retries
- Clean up work assignments

**Interface**:
```java
@ApplicationScoped
public class WorkRecoveryService {
    @Inject EventBus eventBus;
    @Inject TaskExecutionRepository taskExecutionRepository;
    @Inject WorkAssignmentRepository workAssignmentRepository;
    @Inject Logger logger;
    
    @ConsumeEvent("worker.died")
    @Transactional
    public void recoverWork(WorkerDiedEvent event) {
        logger.warn("Recovering work from dead worker: {}", event.workerId());
        
        List<TaskExecution> orphanedTasks = 
            taskExecutionRepository.findByAssignedWorker(event.workerId());
        
        logger.info("Found {} orphaned tasks", orphanedTasks.size());
        
        for (TaskExecution te : orphanedTasks) {
            recoverTask(te);
            workAssignmentRepository.deleteByTaskExecution(te.id());
        }
    }
    
    private void recoverTask(TaskExecution te) {
        if (te.attempt() < te.task().retryPolicy().maxRetries()) {
            logger.info("Retrying task execution {} (attempt {}/{})",
                te.id(), te.attempt(), te.task().retryPolicy().maxRetries());
            
            taskExecutionRepository.resetToPending(te.id());
            
            eventBus.publish("task.retry", new TaskReadyEvent(
                te.id(),
                te.task()
            ));
        } else {
            logger.warn("Task execution {} exceeded max retries", te.id());
            
            taskExecutionRepository.updateStatus(
                te.id(),
                TaskStatus.FAILED,
                "Worker died, max retries exceeded",
                null
            );
            
            eventBus.publish("task.failed", new TaskFailedEvent(
                te.id(),
                "Worker died, max retries exceeded",
                te.attempt()
            ));
        }
    }
}
```

#### 7. JGraphTService

**Purpose**: Build and query DAGs using JGraphT library.

**Lifecycle**: Stateless service, used by GraphEvaluator.

**Responsibilities**:
- Build DirectedAcyclicGraph from graph definition
- Detect cycles (throws exception)
- Find topologically sorted execution order
- Determine which tasks are ready (all dependencies completed)

**Interface**:
```java
@ApplicationScoped
public class JGraphTService {
    @Inject Logger logger;
    
    /**
     * Build a DAG from a graph definition.
     * @throws CycleFoundException if graph contains cycles
     */
    public DirectedAcyclicGraph<Task, DefaultEdge> buildDAG(Graph graph) {
        DirectedAcyclicGraph<Task, DefaultEdge> dag = 
            new DirectedAcyclicGraph<>(DefaultEdge.class);
        
        // Add all tasks as vertices
        for (Task task : graph.tasks()) {
            dag.addVertex(task);
        }
        
        // Add edges from dependencies
        for (GraphEdge edge : graph.edges()) {
            try {
                dag.addEdge(edge.fromTask(), edge.toTask());
            } catch (IllegalArgumentException e) {
                throw new CycleFoundException(
                    "Cycle detected in graph: " + graph.name(), e);
            }
        }
        
        logger.debug("Built DAG for graph {} with {} vertices and {} edges",
            graph.name(), dag.vertexSet().size(), dag.edgeSet().size());
        
        return dag;
    }
    
    /**
     * Find tasks that are ready to execute.
     * A task is ready if all its dependencies are COMPLETED or SKIPPED.
     */
    public Set<Task> findReadyTasks(
        DirectedAcyclicGraph<Task, DefaultEdge> dag,
        Map<Task, TaskStatus> currentStates
    ) {
        Set<Task> ready = new HashSet<>();
        
        for (Task task : dag.vertexSet()) {
            TaskStatus status = currentStates.getOrDefault(task, TaskStatus.PENDING);
            
            // Skip if already queued, running, completed, or failed
            if (status != TaskStatus.PENDING) {
                continue;
            }
            
            // Check if all dependencies are satisfied
            Set<DefaultEdge> incomingEdges = dag.incomingEdgesOf(task);
            boolean allDependenciesMet = true;
            
            for (DefaultEdge edge : incomingEdges) {
                Task dependency = dag.getEdgeSource(edge);
                TaskStatus depStatus = currentStates.get(dependency);
                
                if (depStatus != TaskStatus.COMPLETED && depStatus != TaskStatus.SKIPPED) {
                    allDependenciesMet = false;
                    break;
                }
            }
            
            if (allDependenciesMet) {
                ready.add(task);
            }
        }
        
        return ready;
    }
    
    /**
     * Get topological sort of tasks (useful for UI visualization).
     */
    public List<Task> getTopologicalOrder(DirectedAcyclicGraph<Task, DefaultEdge> dag) {
        TopologicalOrderIterator<Task, DefaultEdge> iterator = 
            new TopologicalOrderIterator<>(dag);
        
        List<Task> order = new ArrayList<>();
        iterator.forEachRemaining(order::add);
        
        return order;
    }
}
```

### Worker Pod Components

#### 1. WorkerMain

**Purpose**: Main entry point for worker pod.

**Lifecycle**: Runs continuously until killed.

**Responsibilities**:
- Register with orchestrator on startup
- Start heartbeat timer
- Listen for work from queue
- Listen for management commands
- Graceful shutdown

**Interface**:
```java
@ApplicationScoped
public class WorkerMain {
    @Inject TaskExecutor taskExecutor;
    @Inject QueueService queueService;
    @Inject ManagementService managementService;
    @Inject Logger logger;
    
    @ConfigProperty(name = "worker.id")
    String workerId;  // Defaults to $HOSTNAME
    
    @ConfigProperty(name = "worker.heartbeat-interval-seconds")
    int heartbeatIntervalSeconds;
    
    private volatile boolean running = true;
    
    void onStart(@Observes StartupEvent event) {
        logger.info("Worker {} starting", workerId);
        
        // Register with orchestrator
        managementService.register(workerId, getMetadata());
        
        // Start heartbeat
        startHeartbeat();
        
        // Start listening for management commands
        managementService.listenForCommands(this::onManagementCommand);
        
        logger.info("Worker {} ready", workerId);
    }
    
    void onStop(@Observes ShutdownEvent event) {
        logger.info("Worker {} shutting down", workerId);
        running = false;
    }
    
    @Incoming("work-queue")
    public CompletionStage<Void> processWork(Message<WorkMessage> message) {
        if (!running) {
            logger.warn("Worker shutting down, rejecting work");
            return message.nack(new IllegalStateException("Worker shutting down"));
        }
        
        WorkMessage work = message.getPayload();
        logger.info("Received work: task={}, execution={}", 
            work.taskName(), work.executionId());
        
        try {
            // Register assignment
            managementService.registerAssignment(work.executionId(), workerId);
            
            // Notify started
            managementService.publishTaskStarted(work.executionId(), workerId);
            
            // Execute task
            TaskResult result = taskExecutor.execute(work);
            
            // Publish result
            if (result.isSuccess()) {
                managementService.publishTaskCompleted(
                    work.executionId(), 
                    workerId,
                    result.data()
                );
            } else {
                managementService.publishTaskFailed(
                    work.executionId(),
                    workerId,
                    result.error(),
                    work.attempt()
                );
            }
            
            return message.ack();
            
        } catch (Exception e) {
            logger.error("Error processing work", e);
            
            managementService.publishTaskFailed(
                work.executionId(),
                workerId,
                e.getMessage(),
                work.attempt()
            );
            
            return message.nack(e);
        }
    }
    
    private void startHeartbeat() {
        Executors.newSingleThreadScheduledExecutor().scheduleAtFixedRate(
            () -> {
                if (running) {
                    try {
                        managementService.sendHeartbeat(workerId);
                    } catch (Exception e) {
                        logger.error("Error sending heartbeat", e);
                    }
                }
            },
            0,
            heartbeatIntervalSeconds,
            TimeUnit.SECONDS
        );
    }
    
    private void onManagementCommand(ManagementCommand cmd) {
        if ("KILL".equals(cmd.type()) && workerId.equals(cmd.targetWorker())) {
            logger.warn("Received KILL command, shutting down");
            running = false;
            System.exit(0);
        }
    }
    
    private Map<String, Object> getMetadata() {
        return Map.of(
            "podIp", System.getenv().getOrDefault("POD_IP", "unknown"),
            "nodeName", System.getenv().getOrDefault("NODE_NAME", "unknown"),
            "capabilities", List.of("dbt", "bq_extract", "dataplex", "dataflow")
        );
    }
}
```

**Configuration**:
```properties
# application.properties
worker.id=${HOSTNAME}
worker.heartbeat-interval-seconds=10
```

#### 2. TaskExecutor

**Purpose**: Dispatch work to appropriate task handlers.

**Lifecycle**: Stateless service.

**Responsibilities**:
- Route work based on task type
- Apply timeout enforcement
- Catch and wrap exceptions
- Return standardized results

**Interface**:
```java
@ApplicationScoped
public class TaskExecutor {
    @Inject DbtHandler dbtHandler;
    @Inject BqExtractHandler bqExtractHandler;
    @Inject DataplexHandler dataplexHandler;
    @Inject DataflowHandler dataflowHandler;
    @Inject Logger logger;
    
    public TaskResult execute(WorkMessage work) {
        logger.info("Executing task: type={}, name={}", 
            work.taskType(), work.taskName());
        
        try {
            TaskResult result = executeWithTimeout(work);
            
            logger.info("Task completed successfully: {}", work.taskName());
            return result;
            
        } catch (TimeoutException e) {
            logger.error("Task timed out: {}", work.taskName());
            return TaskResult.failure("Task execution timed out after " + 
                work.timeoutSeconds() + " seconds");
                
        } catch (Exception e) {
            logger.error("Task failed: " + work.taskName(), e);
            return TaskResult.failure(e.getMessage());
        }
    }
    
    private TaskResult executeWithTimeout(WorkMessage work) 
            throws TimeoutException, InterruptedException, ExecutionException {
        
        ExecutorService executor = Executors.newSingleThreadExecutor();
        
        Future<TaskResult> future = executor.submit(() -> {
            return switch(work.taskType()) {
                case DBT -> dbtHandler.execute(work.config());
                case BQ_EXTRACT -> bqExtractHandler.execute(work.config());
                case DATAPLEX -> dataplexHandler.execute(work.config());
                case DATAFLOW -> dataflowHandler.execute(work.config());
            };
        });
        
        try {
            return future.get(work.timeoutSeconds(), TimeUnit.SECONDS);
        } finally {
            executor.shutdownNow();
        }
    }
}
```

#### 3. Task Handlers

Each task type has a dedicated handler. All handlers implement a common interface:

```java
public interface TaskHandler {
    TaskResult execute(Map<String, Object> config);
}
```

**DbtHandler**:
```java
@ApplicationScoped
public class DbtHandler implements TaskHandler {
    @Inject Logger logger;
    
    @Override
    public TaskResult execute(Map<String, Object> config) {
        String project = (String) config.get("project");
        String models = (String) config.get("models");
        String target = (String) config.getOrDefault("target", "prod");
        
        logger.info("Running DBT: project={}, models={}, target={}", 
            project, models, target);
        
        try {
            // Execute DBT via CLI
            ProcessBuilder pb = new ProcessBuilder(
                "dbt", "run",
                "--project-dir", "/dbt/" + project,
                "--models", models,
                "--target", target
            );
            
            Process process = pb.start();
            int exitCode = process.waitFor();
            
            if (exitCode == 0) {
                return TaskResult.success(Map.of(
                    "models_run", extractModelsFromOutput(process)
                ));
            } else {
                String error = new String(process.getErrorStream().readAllBytes());
                return TaskResult.failure("DBT failed: " + error);
            }
            
        } catch (Exception e) {
            logger.error("Error executing DBT", e);
            return TaskResult.failure(e.getMessage());
        }
    }
    
    private List<String> extractModelsFromOutput(Process process) {
        // Parse DBT output to get list of models that ran
        // Implementation details omitted
        return List.of();
    }
}
```

**BqExtractHandler**:
```java
@ApplicationScoped
public class BqExtractHandler implements TaskHandler {
    @Inject Logger logger;
    
    @Override
    public TaskResult execute(Map<String, Object> config) {
        String project = (String) config.get("project");
        String dataset = (String) config.get("dataset");
        String table = (String) config.get("table");
        String destination = (String) config.get("destination");
        
        logger.info("Extracting BQ table: {}.{}.{} to {}", 
            project, dataset, table, destination);
        
        try {
            BigQuery bigquery = BigQueryOptions.newBuilder()
                .setProjectId(project)
                .build()
                .getService();
            
            TableId tableId = TableId.of(project, dataset, table);
            String format = (String) config.getOrDefault("format", "PARQUET");
            
            ExtractJobConfiguration configuration = ExtractJobConfiguration.newBuilder(
                    tableId,
                    destination
                )
                .setFormat(format)
                .build();
            
            Job job = bigquery.create(JobInfo.of(configuration));
            job = job.waitFor();
            
            if (job.getStatus().getError() == null) {
                return TaskResult.success(Map.of(
                    "destination", destination,
                    "bytes_exported", job.getStatistics().toString()
                ));
            } else {
                return TaskResult.failure("BQ Extract failed: " + 
                    job.getStatus().getError());
            }
            
        } catch (Exception e) {
            logger.error("Error extracting from BigQuery", e);
            return TaskResult.failure(e.getMessage());
        }
    }
}
```

**DataplexHandler** and **DataflowHandler** follow similar patterns, using respective GCP client libraries.

## Interfaces and Abstractions

### Repository Pattern

All data access goes through repository interfaces, allowing easy swapping between implementations.

#### Base Repository Interface

```java
public interface Repository<T, ID> {
    T findById(ID id);
    Optional<T> findByIdOptional(ID id);
    List<T> findAll();
    T save(T entity);
    void delete(ID id);
}
```

#### GraphRepository

```java
public interface GraphRepository extends Repository<Graph, UUID> {
    Optional<Graph> findByName(String name);
    Graph upsertFromYaml(String name, String yamlContent);
    List<Graph> findAll();
}

@ApplicationScoped
@RequiresDialect(Database.POSTGRESQL)
public class PostgresGraphRepository implements GraphRepository {
    @Inject DataSource dataSource;
    
    @Override
    public Graph findById(UUID id) {
        // JDBC query
    }
    
    @Override
    public Graph upsertFromYaml(String name, String yamlContent) {
        // INSERT ... ON CONFLICT UPDATE
    }
}

@ApplicationScoped
@RequiresDialect(Database.H2)
public class H2GraphRepository implements GraphRepository {
    @Inject DataSource dataSource;
    
    // H2-specific implementation (MERGE syntax)
}
```

#### TaskRepository

```java
public interface TaskRepository extends Repository<Task, UUID> {
    Optional<Task> findByName(String name);
    Task upsertFromYaml(String name, TaskDefinition definition);
    List<Task> findGlobalTasks();
}
```

#### TaskExecutionRepository

```java
public interface TaskExecutionRepository extends Repository<TaskExecution, UUID> {
    TaskExecution create(UUID graphExecutionId, Task task, TaskStatus initialStatus);
    void updateStatus(UUID id, TaskStatus status, String error, Map<String, Object> result);
    Map<Task, TaskStatus> getTaskStates(UUID graphExecutionId);
    List<TaskExecution> findByAssignedWorker(String workerId);
    Optional<TaskExecution> findRunningGlobalExecution(String taskName);
    void linkToGlobalExecution(UUID graphExecId, UUID taskId, UUID globalExecId);
    void resetToPending(UUID id);
    void incrementAttempt(UUID id);
    List<UUID> findGraphsContainingTask(String taskName);
}
```

#### GraphExecutionRepository

```java
public interface GraphExecutionRepository extends Repository<GraphExecution, UUID> {
    GraphExecution create(UUID graphId, String triggeredBy);
    void updateStatus(UUID id, GraphStatus status);
    GraphExecution getCurrentExecution(UUID graphId);
    List<GraphExecution> getHistory(UUID graphId, int limit);
}
```

#### WorkerRepository

```java
public interface WorkerRepository extends Repository<Worker, String> {
    void upsert(Worker worker);
    void updateHeartbeat(String workerId, Instant timestamp);
    void markDead(String workerId);
    List<Worker> findDeadWorkers(Instant deadlineBefore);
    List<Worker> findActive();
}
```

#### WorkAssignmentRepository

```java
public interface WorkAssignmentRepository {
    void create(UUID taskExecutionId, String workerId);
    void deleteByTaskExecution(UUID taskExecutionId);
    Optional<WorkAssignment> findByTaskExecution(UUID taskExecutionId);
}
```

### Queue Abstraction

Queue operations are abstracted behind an interface to support multiple implementations.

#### QueueService Interface

```java
public interface QueueService {
    /**
     * Publish work to the task queue.
     * Returns a Uni for async/reactive handling.
     */
    Uni<Void> publishWork(WorkMessage message);
    
    /**
     * Publish an event (task result).
     */
    Uni<Void> publishEvent(TaskEventMessage message);
    
    /**
     * Publish to management topic.
     */
    Uni<Void> publishManagement(ManagementEventMessage message);
}
```

#### PubSubQueueService (Production)

```java
@ApplicationScoped
@RequiresProfile("prod")
public class PubSubQueueService implements QueueService {
    @Inject Logger logger;
    
    @ConfigProperty(name = "pubsub.project")
    String projectId;
    
    @ConfigProperty(name = "pubsub.topic.work")
    String workTopicName;
    
    @ConfigProperty(name = "pubsub.topic.events")
    String eventsTopicName;
    
    @ConfigProperty(name = "pubsub.topic.management")
    String managementTopicName;
    
    private Publisher workPublisher;
    private Publisher eventsPublisher;
    private Publisher managementPublisher;
    
    @PostConstruct
    void init() throws IOException {
        TopicName workTopic = TopicName.of(projectId, workTopicName);
        TopicName eventsTopic = TopicName.of(projectId, eventsTopicName);
        TopicName mgmtTopic = TopicName.of(projectId, managementTopicName);
        
        workPublisher = Publisher.newBuilder(workTopic).build();
        eventsPublisher = Publisher.newBuilder(eventsTopic).build();
        managementPublisher = Publisher.newBuilder(mgmtTopic).build();
    }
    
    @Override
    public Uni<Void> publishWork(WorkMessage message) {
        String json = toJson(message);
        PubsubMessage pubsubMessage = PubsubMessage.newBuilder()
            .setData(ByteString.copyFromUtf8(json))
            .build();
        
        return Uni.createFrom().completionStage(
            workPublisher.publish(pubsubMessage)
        ).replaceWithVoid();
    }
    
    @Override
    public Uni<Void> publishEvent(TaskEventMessage message) {
        String json = toJson(message);
        PubsubMessage pubsubMessage = PubsubMessage.newBuilder()
            .setData(ByteString.copyFromUtf8(json))
            .build();
        
        return Uni.createFrom().completionStage(
            eventsPublisher.publish(pubsubMessage)
        ).replaceWithVoid();
    }
    
    @Override
    public Uni<Void> publishManagement(ManagementEventMessage message) {
        String json = toJson(message);
        PubsubMessage pubsubMessage = PubsubMessage.newBuilder()
            .setData(ByteString.copyFromUtf8(json))
            .build();
        
        return Uni.createFrom().completionStage(
            managementPublisher.publish(pubsubMessage)
        ).replaceWithVoid();
    }
    
    @PreDestroy
    void cleanup() {
        if (workPublisher != null) workPublisher.shutdown();
        if (eventsPublisher != null) eventsPublisher.shutdown();
        if (managementPublisher != null) managementPublisher.shutdown();
    }
    
    private String toJson(Object obj) {
        // Use Jackson or similar
        return "{}";
    }
}
```

#### HazelcastQueueService (Local Dev)

```java
@ApplicationScoped
@RequiresProfile("dev")
public class HazelcastQueueService implements QueueService {
    @Inject Logger