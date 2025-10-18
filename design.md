# Event-Driven Workflow Orchestrator - Design Document

## Table of Contents
1. [Overview](#overview)
2. [Architecture](#architecture)
3. [Core Concepts](#core-concepts)
4. [Configuration Schema](#configuration-schema)
5. [Expression Language](#expression-language)
6. [Global Tasks](#global-tasks)
7. [Parameter Scoping](#parameter-scoping)
8. [Development Mode](#development-mode)
9. [Production Deployment](#production-deployment)
10. [UI Design](#ui-design)
11. [Data Model](#data-model)
12. [Implementation Details](#implementation-details)
13. [Testing Strategy](#testing-strategy)
14. [Deployment Guide](#deployment-guide)

---

## Overview

### Project Vision

A lightweight, portable, event-driven workflow orchestrator that **gets out of your way**. Designed for data teams who want to ship fast without operational overhead.

**Core Philosophy**:
- Simple over complex
- Portable over locked-in
- YAML over code
- Files over databases (for events)
- Commands over plugins

### Key Differentiators

1. **Global Task Deduplication** - Run once, notify many graphs
2. **Single Binary** - No dependencies, runs anywhere
3. **File-Based Events** - JSONL files, not databases
4. **Multi-Threaded Workers** - Efficient for I/O-bound tasks
5. **Dev Mode** - Single process with embedded worker
6. **Trial Run** - See what would execute without executing

### Target Users

- Data engineering teams (5-50 people)
- Organizations with multi-cloud or hybrid deployments
- Teams that value simplicity and portability
- Cost-conscious organizations
- Edge computing scenarios

### Not For

- FAANG-scale (millions of tasks/day)
- Teams that need complex multi-tenancy
- Microservice orchestration (use Temporal)
- Teams deeply invested in Airflow ecosystem

---

## Architecture

### High-Level Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                    Orchestrator Process                     │
│                                                             │
│  ┌────────────────────────────────────────────────────────┐ │
│  │              Core Orchestration                        │ │
│  │  ┌──────────────┐  ┌──────────────┐  ┌─────────────┐   │ │
│  │  │Graph Loader  │  │Event Bus     │  │Graph        │   │ │
│  │  │(YAML→Memory) │  │(Vert.x)      │  │Evaluator    │   │ │
│  │  └──────────────┘  └──────────────┘  └─────────────┘   │ │
│  │  ┌──────────────┐  ┌──────────────┐  ┌─────────────┐   │ │
│  │  │State Manager │  │Worker Monitor│  │Task Queue   │   │ │
│  │  │(Hazelcast)   │  │              │  │Publisher    │   │ │
│  │  └──────────────┘  └──────────────┘  └─────────────┘   │ │
│  └────────────────────────────────────────────────────────┘ │
│                                                             │
│  ┌────────────────────────────────────────────────────────┐ │
│  │              Worker Pool (Embedded in Dev)             │ │
│  │  ┌─────────┐  ┌─────────┐  ┌─────────┐  ┌─────────┐    │ │
│  │  │Thread 1 │  │Thread 2 │  │Thread 3 │  │Thread 4 │    │ │
│  │  └─────────┘  └─────────┘  └─────────┘  └─────────┘    │ │
│  └────────────────────────────────────────────────────────┘ │
│                                                             │
│  ┌────────────────────────────────────────────────────────┐ │
│  │                    Web UI (SSR)                        │ │
│  │  ┌──────────────────────────────────────────────────┐  │ │
│  │  │  Qute Templates + Tabler CSS + Monaco Editor     │  │ │
│  │  └──────────────────────────────────────────────────┘  │ │
│  └────────────────────────────────────────────────────────┘ │
└─────────────────────────────────────────────────────────────┘
                              │
                ┌─────────────┴─────────────┐
                ↓                           ↓
        ┌──────────────┐          ┌──────────────────┐
        │ Event Store  │          │  Config Files    │
        │ (JSONL)      │          │  (YAML)          │
        │              │          │                  │
        │ Local: files │          │  graphs/*.yaml   │
        │ Prod: GCS/S3 │          │  tasks/*.yaml    │
        └──────────────┘          └──────────────────┘
```

### Production Architecture (GKE)

```
┌─────────────────────────────────────────────────────────────┐
│                         GKE Cluster                         │
│                                                             │
│  ┌──────────────────┐         ┌─────────────────────────┐   │
│  │   Orchestrator   │         │   Worker Pods (HPA)     │   │
│  │    (1 replica)   │         │                         │   │
│  │                  │         │  ┌────────┐ ┌────────┐  │   │
│  │  - Graph Eval    │         │  │Worker 1│ │Worker 2│  │   │
│  │  - State Mgmt    │         │  │16 thrd │ │16 thrd │  │   │
│  │  - UI (SSR)      │         │  └────────┘ └────────┘  │   │
│  │  - API           │         │       ...               │   │
│  └──────────────────┘         └─────────────────────────┘   │
│         │                                                   │
└─────────┼───────────────────────────────────────────────────┘
          │
    ┌─────┴─────┐
    ▼           ▼
┌────────┐  ┌────────────┐      ┌──────────────┐
│Hazel-  │  │  GCS/S3    │      │ ConfigMap    │
│cast    │  │            │      │              │
│Cluster │  │ Events:    │      │ graphs/*.yaml│
│(3 AZs) │  │ *.jsonl    │      │ tasks/*.yaml │
└────────┘  └────────────┘      └──────────────┘
```

---

## Core Concepts

### 1. Tasks

**Definition**: A single unit of work. Just a command to execute.

**Key Properties**:
- `name` - Unique identifier
- `command` - Program to execute
- `args` - Command arguments (with JEXL expressions)
- `env` - Environment variables
- `timeout` - Maximum execution time
- `retry` - Retry policy

**Example**:
```yaml
name: extract-data
command: python
args:
  - /scripts/extract.py
  - --date
  - "${params.batch_date}"
env:
  PYTHONUNBUFFERED: "1"
  API_KEY: "${env.API_KEY}"
timeout: 600  # 10 minutes
retry: 3
```

### 2. Global Tasks

**Definition**: Tasks that can be shared across multiple graphs. Run once per unique parameter set.

**Key Feature**: Deduplication via parameterized keys.

**Key Properties**:
- `global: true` - Marks as global
- `key` - Expression that uniquely identifies this task instance
- `params` - Parameters with defaults

**Example**:
```yaml
name: load-market-data
global: true
key: "load_market_${params.batch_date}_${params.region}"

params:
  batch_date:
    type: string
    required: true
  region:
    type: string
    default: us

command: dbt
args:
  - run
  - --models
  - +market_data
  - --vars
  - "batch_date:${params.batch_date},region:${params.region}"
```

**Behavior**:
- Graph A needs `load-market-data[2025-10-17, us]`
- Graph B needs `load-market-data[2025-10-17, us]`
- Task runs **once**, both graphs notified on completion
- Graph C needs `load-market-data[2025-10-16, us]`
- Different key → runs separately

### 3. Graphs

**Definition**: A directed acyclic graph (DAG) of tasks with dependencies.

**Key Properties**:
- `name` - Unique identifier
- `params` - Parameters with defaults (can be overridden at runtime)
- `env` - Graph-level environment variables
- `tasks` - List of tasks (inline or references to global tasks)
- `schedule` (optional) - Cron expression for scheduled execution

**Example**:
```yaml
name: daily-etl
description: Daily ETL pipeline

params:
  batch_date:
    type: string
    default: "${date.today()}"
  region:
    type: string
    default: us

env:
  GCS_BUCKET: "gs://data-${params.region}"
  PROCESSING_DATE: "${params.batch_date}"

schedule: "0 2 * * *"  # 2 AM daily

tasks:
  # Reference global task
  - task: load-market-data
    params:
      batch_date: "${params.batch_date}"
      region: "${params.region}"
  
  # Inline task
  - name: transform-data
    command: python
    args:
      - /scripts/transform.py
      - --input
      - "${env.GCS_BUCKET}/raw/${params.batch_date}"
    depends_on:
      - load-market-data
```

### 4. Graph Executions

**Definition**: A single run of a graph with specific parameters.

**Lifecycle**:
1. `RUNNING` - Graph is being evaluated/executed
2. `COMPLETED` - All tasks completed successfully
3. `FAILED` - One or more tasks failed permanently
4. `STALLED` - No progress possible (waiting on failed dependencies)
5. `PAUSED` - Manually paused by user

### 5. Task Executions

**Definition**: A single execution of a task within a graph execution.

**Lifecycle**:
1. `PENDING` - Task created, waiting for dependencies
2. `QUEUED` - Published to worker queue
3. `RUNNING` - Worker is executing
4. `COMPLETED` - Successfully finished
5. `FAILED` - Failed (may retry)
6. `SKIPPED` - Skipped due to upstream failure

### 6. Workers

**Definition**: Processes that pull tasks from queue and execute them.

**Types**:
- **Embedded Worker** (dev mode) - Runs in same process as orchestrator
- **Dedicated Workers** (prod) - Separate pods with multi-threaded execution

**Key Properties**:
- `worker_id` - Unique identifier (hostname/pod name)
- `threads` - Number of concurrent task executions
- `heartbeat` - Periodic health signal (every 10s)
- `capabilities` - What tasks can run (future: heterogeneous workers)

---

## Configuration Schema

### Task Definition Schema

**Location**: `tasks/{task-name}.yaml`

```yaml
# Required: Task name (must match filename without .yaml)
name: string  # Pattern: ^[a-z0-9-]+$

# Optional: Mark as global task
global: boolean  # Default: false

# Required for global tasks: Unique key expression
key: string  # JEXL expression with params

# Optional: Parameters (required for global tasks with key)
params:
  param_name:
    type: string|integer|boolean|date|array  # Required
    default: any  # Optional (JEXL expression allowed)
    required: boolean  # Default: false
    description: string  # Optional

# Required: Command to execute
command: string

# Required: Command arguments
args:
  - string  # JEXL expressions allowed

# Optional: Environment variables
env:
  KEY: string  # JEXL expressions allowed

# Optional: Timeout in seconds
timeout: integer  # Default: 3600 (1 hour)

# Optional: Retry policy
retry: integer  # Default: 3 (max retry attempts)
```

### Graph Definition Schema

**Location**: `graphs/{graph-name}.yaml`

```yaml
# Required: Graph name (must match filename without .yaml)
name: string  # Pattern: ^[a-z0-9-]+$

# Optional: Human-readable description
description: string

# Optional: Parameters
params:
  param_name:
    type: string|integer|boolean|date|array
    default: any  # JEXL expression allowed
    required: boolean  # Default: false
    description: string

# Optional: Graph-level environment variables
env:
  KEY: string  # JEXL expressions allowed

# Optional: Schedule (cron expression)
schedule: string  # e.g., "0 2 * * *"

# Optional: Triggers
triggers:
  - type: webhook|pubsub|schedule
    # Type-specific config

# Required: Tasks
tasks:
  # Option 1: Reference global task
  - task: string  # Global task name
    params:  # Optional param overrides
      param_name: any  # JEXL expression
    depends_on:  # Optional
      - string  # Task names
  
  # Option 2: Inline task definition
  - name: string
    command: string
    args:
      - string
    env:
      KEY: string
    timeout: integer
    retry: integer
    depends_on:
      - string
```

### Orchestrator Configuration

**Location**: `application.yaml` (or `application-{profile}.yaml`)

```yaml
orchestrator:
  # Mode: dev or prod
  mode: dev
  
  # Config paths
  config:
    graphs: ./graphs  # Directory containing graph YAML files
    tasks: ./tasks    # Directory containing task YAML files
    watch: true       # Watch for file changes (dev only)
  
  # Development mode settings
  dev:
    worker-threads: 4       # Embedded worker thread count
    trial-run: false        # If true, only log commands instead of executing
    enable-editor: true     # Enable web-based YAML editor
  
  # Storage
  storage:
    events: file://./data/events  # or gs://bucket/events or s3://bucket/events
  
  # Hazelcast
  hazelcast:
    embedded: true  # true for dev, false for prod
    cluster-name: orchestrator
    members:        # Prod only
      - orchestrator-0.orchestrator
      - orchestrator-1.orchestrator
      - orchestrator-2.orchestrator
  
  # Worker settings (for dedicated workers)
  worker:
    threads: 16             # Threads per worker pod
    heartbeat-interval: 10  # Seconds
    dead-threshold: 60      # Seconds

# Web server
server:
  port: 8080

# Logging
logging:
  level: info
  format: json  # or text
```

---

## Expression Language

### JEXL Overview

We use Apache Commons JEXL 3 for all expressions. Expressions are wrapped in `${ }`.

**Key Features**:
- Arithmetic: `+`, `-`, `*`, `/`, `%`
- Comparison: `==`, `!=`, `<`, `>`, `<=`, `>=`
- Logical: `&&`, `||`, `!`
- Ternary: `condition ? true_val : false_val`
- Null-safe: `object?.property`
- Elvis: `value ?: default`

### Built-in Variables

```
${params.name}          - Access parameter
${env.NAME}             - Access environment variable
${task.taskname.result} - Access upstream task result (JSON)
```

### Built-in Functions

#### Date Functions

```
${date.today()}                              → "2025-10-17"
${date.now('yyyy-MM-dd HH:mm:ss')}          → "2025-10-17 14:30:00"
${date.add(date.today(), 1, 'days')}        → "2025-10-18"
${date.sub(date.today(), 7, 'days')}        → "2025-10-10"
${date.format(params.date, 'yyyy/MM/dd')}   → "2025/10/17"
```

#### String Functions

```
${string.uuid()}                             → "550e8400-e29b-41d4-a716-446655440000"
${string.slugify('My Workflow Name')}       → "my-workflow-name"
```

#### String Methods

```
${'hello'.toUpperCase()}                     → "HELLO"
${'HELLO'.toLowerCase()}                     → "hello"
${'2025-10-17'.replace('-', '_')}           → "2025_10_17"
${'  text  '.trim()}                         → "text"
```

#### Math Functions

```
${Math.round(3.7)}                           → 4
${Math.floor(3.7)}                           → 3
${Math.ceil(3.2)}                            → 4
${Math.max(10, 20)}                          → 20
${Math.min(10, 20)}                          → 10
```

### Examples

```yaml
# Simple variable substitution
command: echo
args:
  - "Processing ${params.region}"

# Arithmetic
env:
  BATCH_SIZE: "${params.scale * 100}"
  WORKER_COUNT: "${params.scale * 4}"

# Conditionals
args:
  - "${params.full_refresh ? '--full-refresh' : '--incremental'}"

# Complex logic
env:
  PARALLELISM: "${params.scale > 10 ? 32 : params.scale > 5 ? 16 : 4}"

# String manipulation
env:
  TABLE_NAME: "${'data_' + params.region + '_' + params.date.replace('-', '_')}"

# Null-safe access
env:
  API_KEY: "${params.config?.api?.key ?: env.DEFAULT_API_KEY}"

# Date operations
params:
  yesterday:
    default: "${date.add(date.today(), -1, 'days')}"

# Reference upstream task output
args:
  - --row-count
  - "${task.extract.result.row_count}"
```

---

## Global Tasks

### Concept

Global tasks solve the problem of multiple graphs needing the same data/computation for the same parameters.

**Without global tasks**:
```
Graph A: load-data[2025-10-17] → runs
Graph B: load-data[2025-10-17] → runs (duplicate!)
Graph C: load-data[2025-10-17] → runs (duplicate!)
```

**With global tasks**:
```
Graph A: load-data[2025-10-17] → starts execution
Graph B: load-data[2025-10-17] → links to same execution
Graph C: load-data[2025-10-17] → links to same execution
→ Runs once, all three graphs proceed together
```

### Key Expression

The `key` field uniquely identifies a task instance. It should include all parameters that affect the task's output.

**Good Keys**:
```yaml
# Includes date and region - different dates/regions = different data
key: "load_data_${params.batch_date}_${params.region}"

# Includes all meaningful params
key: "bootstrap_curves_${params.date}_${params.region}_${params.model_version}"
```

**Bad Keys**:
```yaml
# Too generic - all graphs share same execution even with different params
key: "load_data"

# Includes volatile data - every execution is "unique"
key: "load_data_${params.batch_date}_${date.now()}"
```

### State Management

Global tasks are tracked in Hazelcast:

```java
// Key structure
record TaskExecutionKey(
    String taskName,
    String resolvedKey  // Evaluated expression
) {}

// State
record GlobalTaskExecution(
    UUID id,
    String taskName,
    String resolvedKey,
    Map<String, Object> params,
    TaskStatus status,
    Set<UUID> linkedGraphExecutions,  // Which graphs are waiting
    Instant startedAt,
    Instant completedAt,
    Map<String, Object> result
) {}
```

### Execution Flow

1. Graph A evaluates, needs global task with key `load_data_2025-10-17_us`
2. Check Hazelcast for existing execution with that key
3. **Not found** → Start new execution, add Graph A to linked graphs
4. Graph B evaluates, needs same key
5. **Found, status=RUNNING** → Add Graph B to linked graphs, don't start new execution
6. Task completes → Notify both Graph A and Graph B
7. Both graphs re-evaluate and schedule downstream tasks

---

## Parameter Scoping

### Hierarchy (Highest to Lowest Priority)

1. **Runtime Invocation** - Params passed when triggering graph
2. **Graph Task Reference** - Params in graph's task definition
3. **Global Task Defaults** - Defaults in global task definition
4. **Graph Defaults** - Defaults in graph params

### Example

**Global Task**:
```yaml
# tasks/process-data.yaml
name: process-data
global: true
key: "process_${params.date}_${params.region}"

params:
  date:
    type: string
    required: true
  region:
    type: string
    default: us      # Level 3: Global task default
  threads:
    type: integer
    default: 4       # Level 3: Global task default
```

**Graph**:
```yaml
# graphs/etl-pipeline.yaml
name: etl-pipeline

params:
  date:
    type: string
    default: "${date.today()}"  # Level 4: Graph default
  region:
    type: string
    default: eu                 # Level 4: Graph default (overrides global)

tasks:
  - task: process-data
    params:
      date: "${params.date}"
      region: "${params.region}"
      threads: 8                # Level 2: Task reference override
```

**Runtime Invocation**:
```bash
curl -X POST /api/graphs/etl-pipeline/execute \
  -d '{"params": {"date": "2025-10-15", "region": "asia"}}'
# Level 1: Runtime override (highest priority)
```

**Final Resolution**:
```
date: "2025-10-15"    (Level 1: runtime)
region: "asia"        (Level 1: runtime)
threads: 8            (Level 2: task reference)
```

---

## Development Mode

### Single Process Architecture

In dev mode, everything runs in one process:

```
┌─────────────────────────────────────────┐
│      Single JVM Process                 │
│                                         │
│  Orchestrator Core                      │
│  ↓                                      │
│  Embedded Hazelcast                     │
│  ↓                                      │
│  Embedded Worker Pool (4 threads)       │
│  ↓                                      │
│  Local Event Store (./data/events)      │
│  ↓                                      │
│  Web UI (http://localhost:8080)         │
└─────────────────────────────────────────┘
```

**Start command**:
```bash
./orchestrator --profile=dev
# or
java -jar orchestrator.jar --profile=dev
```

**Output**:
```
[INFO] Orchestrator starting in DEV mode
[INFO] Loading graphs from ./graphs
[INFO]   - daily-etl.yaml
[INFO]   - market-pipeline.yaml
[INFO] Loading tasks from ./tasks
[INFO]   - load-market-data.yaml
[INFO]   - bootstrap-curves.yaml
[INFO] Starting embedded Hazelcast cluster
[INFO] Hazelcast cluster formed (1 member)
[INFO] Starting embedded worker pool with 4 threads
[INFO]   - worker-thread-1 ready
[INFO]   - worker-thread-2 ready
[INFO]   - worker-thread-3 ready
[INFO]   - worker-thread-4 ready
[INFO] Starting web server on port 8080
[INFO] 
[INFO] ============================================
[INFO] Orchestrator ready!
[INFO] UI: http://localhost:8080
[INFO] Mode: DEVELOPMENT
[INFO] Trial Run: DISABLED
[INFO] ============================================
```

### Hot Reload

In dev mode, watch for file changes:

```java
@ApplicationScoped
@RequiresProfile("dev")
public class ConfigWatcher {
    @ConfigProperty(name = "orchestrator.config.watch")
    boolean watchEnabled;
    
    void onStart(@Observes StartupEvent event) {
        if (watchEnabled) {
            startWatching();
        }
    }
    
    private void startWatching() {
        WatchService watchService = FileSystems.getDefault().newWatchService();
        
        Path graphsDir = Path.of("./graphs");
        Path tasksDir = Path.of("./tasks");
        
        graphsDir.register(watchService, ENTRY_MODIFY, ENTRY_CREATE);
        tasksDir.register(watchService, ENTRY_MODIFY, ENTRY_CREATE);
        
        new Thread(() -> {
            while (true) {
                WatchKey key = watchService.take();
                
                for (WatchEvent<?> event : key.pollEvents()) {
                    Path changed = (Path) event.context();
                    logger.info("Config file changed: {}", changed);
                    
                    // Reload
                    if (changed.toString().endsWith(".yaml")) {
                        reloadConfig(changed);
                    }
                }
                
                key.reset();
            }
        }).start();
    }
    
    private void reloadConfig(Path file) {
        try {
            if (file.getParent().endsWith("graphs")) {
                graphLoader.reloadGraph(file);
            } else if (file.getParent().endsWith("tasks")) {
                graphLoader.reloadTask(file);
            }
            logger.info("✓ Config reloaded: {}", file);
        } catch (Exception e) {
            logger.error("✗ Failed to reload config: {}", file, e);
        }
    }
}
```

### Trial Run Mode

Execute graphs without actually running commands. Perfect for testing configuration.

**Enable**:
```yaml
# application-dev.yaml
orchestrator:
  dev:
    trial-run: true
```

**Behavior**:
```java
@ApplicationScoped
public class TaskExecutor {
    @ConfigProperty(name = "orchestrator.dev.trial-run")
    boolean trialRun;
    
    public TaskResult execute(WorkMessage work) {
        String fullCommand = buildCommandString(work);
        
        if (trialRun) {
            // Log what would execute, but don't execute
            logger.info("TRIAL RUN - Would execute: {}", fullCommand);
            logger.info("  Working dir: {}", work.workingDir());
            logger.info("  Environment: {}", work.env());
            logger.info("  Timeout: {}s", work.timeoutSeconds());
            
            // Return fake success
            return TaskResult.success(Map.of(
                "trial_run", true,
                "command", fullCommand
            ));
        }
        
        // Real execution
        return executeCommand(work);
    }
}
```

**Output**:
```
[INFO] Graph execution started: daily-etl (trial-run)
[INFO] Task: load-market-data
[INFO]   TRIAL RUN - Would execute: dbt run --models +market_data --vars batch_date:2025-10-17
[INFO]   Working dir: /opt/dbt
[INFO]   Environment: {DBT_PROFILES_DIR=/dbt/profiles, DBT_TARGET=dev}
[INFO]   Timeout: 600s
[INFO] Task: transform-data
[INFO]   TRIAL RUN - Would execute: python /scripts/transform.py --date 2025-10-17
[INFO]   Working dir: /opt/scripts
[INFO]   Environment: {PYTHONUNBUFFERED=1}
[INFO]   Timeout: 300s
[INFO] Graph execution completed: daily-etl (trial-run) - 0.5s
```

### Web-Based YAML Editor

In dev mode, enable in-browser editing of graphs and tasks.

**Enable**:
```yaml
orchestrator:
  dev:
    enable-editor: true
```

**UI Route**: `/editor`

**Features**:
- Monaco Editor (VS Code's editor)
- YAML syntax highlighting
- Real-time validation
- Save to file
- Auto-reload on save

**Implementation**:
```html
<!-- editor.html -->
<div class="page-body">
  <div class="container-xl">
    <div class="row">
      <div class="col-3">
        <div class="card">
          <div class="card-header">
            <h3 class="card-title">Files</h3>
          </div>
          <div class="list-group list-group-flush">
            <div class="list-group-item">
              <div class="text-muted">Graphs</div>
              {#for graph in graphs}
              <a href="#" onclick="loadFile('graphs/{graph}.yaml')" 
                 class="list-group-item list-group-item-action">
                {graph}.yaml
              </a>
              {/for}
            </div>
            <div class="list-group-item">
              <div class="text-muted">Tasks</div>
              {#for task in tasks}
              <a href="#" onclick="loadFile('tasks/{task}.yaml')" 
                 class="list-group-item list-group-item-action">
                {task}.yaml
              </a>
              {/for}
            </div>
          </div>
        </div>
      </div>
      
      <div class="col-9">
        <div class="card">
          <div class="card-header">
            <h3 class="card-title" id="filename"></h3>
            <div class="card-actions">
              <button class="btn btn-primary" onclick="saveFile()">
                <i class="ti ti-device-floppy"></i> Save
              </button>
              <button class="btn btn-secondary" onclick="validateFile()">
                <i class="ti ti-check"></i> Validate
              </button>
            </div>
          </div>
          <div class="card-body p-0">
            <div id="editor" style="height: 600px;"></div>
          </div>
          <div class="card-footer" id="validation-result"></div>
        </div>
      </div>
    </div>
  </div>
</div>

<script src="https://cdn.jsdelivr.net/npm/monaco-editor@latest/min/vs/loader.js"></script>
<script>
let editor;
let currentFile;

require.config({ 
  paths: { 
    vs: 'https://cdn.jsdelivr.net/npm/monaco-editor@latest/min/vs' 
  }
});

require(['vs/editor/editor.main'], function() {
  editor = monaco.editor.create(document.getElementById('editor'), {
    language: 'yaml',
    theme: 'vs-dark',
    automaticLayout: true,
    minimap: { enabled: false }
  });
});

async function loadFile(path) {
  currentFile = path;
  document.getElementById('filename').textContent = path;
  
  const response = await fetch('/api/editor/load?path=' + path);
  const content = await response.text();
  
  editor.setValue(content);
}

async function saveFile() {
  const content = editor.getValue();
  
  const response = await fetch('/api/editor/save', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({
      path: currentFile,
      content: content
    })
  });
  
  if (response.ok) {
    showValidation('✓ File saved successfully', 'success');
  } else {
    const error = await response.text();
    showValidation('✗ Save failed: ' + error, 'danger');
  }
}

async function validateFile() {
  const content = editor.getValue();
  
  const response = await fetch('/api/editor/validate', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({
      path: currentFile,
      content: content
    })
  });
  
  const result = await response.json();
  
  if (result.valid) {
    showValidation('✓ Valid YAML', 'success');
  } else {
    showValidation('✗ ' + result.error, 'danger');
  }
}

function showValidation(message, type) {
  const resultDiv = document.getElementById('validation-result');
  resultDiv.innerHTML = `<div class="alert alert-${type}">${message}</div>`;
  setTimeout(() => {
    resultDiv.innerHTML = '';
  }, 3000);
}
</script>
```

**API Endpoints**:
```java
@Path("/api/editor")
@RequiresProfile("dev")
public class EditorController {
    @ConfigProperty(name = "orchestrator.config.graphs")
    String graphsPath;
    
    @ConfigProperty(name = "orchestrator.config.tasks")
    String tasksPath;
    
    @GET
    @Path("/load")
    public String loadFile(@QueryParam("path") String path) {
        Path filePath = resolveSecurePath(path);
        return Files.readString(filePath);
    }
    
    @POST
    @Path("/save")
    public Response saveFile(SaveRequest request) {
        Path filePath = resolveSecurePath(request.path());
        
        // Validate first
        ValidationResult validation = validator.validate(request.content());
        if (!validation.isValid()) {
            return Response.status(400).entity(validation.error()).build();
        }
        
        // Write to file
        Files.writeString(filePath, request.content());
        
        // Trigger reload
        if (request.path().startsWith("graphs/")) {
            graphLoader.reloadGraph(filePath);
        } else {
            taskLoader.reloadTask(filePath);
        }
        
        return Response.ok().build();
    }
    
    @POST
    @Path("/validate")
    public ValidationResult validateFile(ValidateRequest request) {
        return validator.validate(request.content());
    }
    
    private Path resolveSecurePath(String path) {
        // Prevent directory traversal
        Path base = Path.of(graphsPath).getParent();
        Path resolved = base.resolve(path).normalize();
        
        if (!resolved.startsWith(base)) {
            throw new SecurityException("Invalid path");
        }
        
        return resolved;
    }
}
```

---

## Production Deployment

### GKE Architecture

**Components**:
1. Orchestrator (1 replica) - Stateless, can be restarted
2. Worker Pods (2-20 replicas, HPA) - Pull work from Hazelcast queue
3. Hazelcast Cluster (3 nodes, 1 per AZ) - State storage
4. ConfigMap - Graph and task YAML files
5. GCS - Event log storage

### Hazelcast Cluster

**StatefulSet**:
```yaml
apiVersion: apps/v1
kind: StatefulSet
metadata:
  name: hazelcast
  namespace: orchestrator
spec:
  serviceName: hazelcast
  replicas: 3
  selector:
    matchLabels:
      app: hazelcast
  template:
    metadata:
      labels:
        app: hazelcast
    spec:
      affinity:
        podAntiAffinity:
          requiredDuringSchedulingIgnoredDuringExecution:
          - labelSelector:
              matchExpressions:
              - key: app
                operator: In
                values:
                - hazelcast
            topologyKey: topology.kubernetes.io/zone
      containers:
      - name: hazelcast
        image: hazelcast/hazelcast:5.3
        env:
        - name: JAVA_OPTS
          value: "-Xms2g -Xmx2g"
        ports:
        - containerPort: 5701
        resources:
          requests:
            memory: 2Gi
            cpu: 500m
          limits:
            memory: 4Gi
            cpu: 2000m
---
apiVersion: v1
kind: Service
metadata:
  name: hazelcast
  namespace: orchestrator
spec:
  clusterIP: None
  selector:
    app: hazelcast
  ports:
  - port: 5701
```

### Orchestrator Deployment

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: orchestrator
  namespace: orchestrator
spec:
  replicas: 1
  strategy:
    type: Recreate  # Kill old before starting new
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
        image: gcr.io/project/orchestrator:latest
        env:
        - name: QUARKUS_PROFILE
          value: prod
        - name: ORCHESTRATOR_MODE
          value: prod
        - name: HAZELCAST_MEMBERS
          value: "hazelcast-0.hazelcast,hazelcast-1.hazelcast,hazelcast-2.hazelcast"
        - name: GCS_EVENTS_PATH
          value: "gs://my-bucket/orchestrator/events"
        ports:
        - containerPort: 8080
        resources:
          requests:
            memory: 1Gi
            cpu: 500m
          limits:
            memory: 2Gi
            cpu: 1000m
        livenessProbe:
          httpGet:
            path: /q/health/live
            port: 8080
          initialDelaySeconds: 30
        readinessProbe:
          httpGet:
            path: /q/health/ready
            port: 8080
          initialDelaySeconds: 10
        volumeMounts:
        - name: config
          mountPath: /config
          readOnly: true
      volumes:
      - name: config
        configMap:
          name: workflow-config
---
apiVersion: v1
kind: Service
metadata:
  name: orchestrator
  namespace: orchestrator
spec:
  type: LoadBalancer
  selector:
    app: orchestrator
  ports:
  - port: 80
    targetPort: 8080
```

### Worker Deployment

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: workers
  namespace: orchestrator
spec:
  replicas: 2
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
        image: gcr.io/project/orchestrator-worker:latest
        env:
        - name: QUARKUS_PROFILE
          value: prod
        - name: WORKER_THREADS
          value: "16"
        - name: WORKER_ID
          valueFrom:
            fieldRef:
              fieldPath: metadata.name
        - name: HAZELCAST_MEMBERS
          value: "hazelcast-0.hazelcast,hazelcast-1.hazelcast,hazelcast-2.hazelcast"
        resources:
          requests:
            memory: 2Gi
            cpu: 1000m
          limits:
            memory: 4Gi
            cpu: 2000m
        volumeMounts:
        - name: config
          mountPath: /config
          readOnly: true
      volumes:
      - name: config
        configMap:
          name: workflow-config
      terminationGracePeriodSeconds: 300  # Let tasks finish
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
  - type: Resource
    resource:
      name: cpu
      target:
        type: Utilization
        averageUtilization: 70
  behavior:
    scaleDown:
      stabilizationWindowSeconds: 300
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
```

### ConfigMap

```yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: workflow-config
  namespace: orchestrator
data:
  # Tasks
  load-market-data.yaml: |
    name: load-market-data
    global: true
    key: "load_market_${params.batch_date}_${params.region}"
    params:
      batch_date:
        type: string
        required: true
      region:
        type: string
        default: us
    command: dbt
    args:
      - run
      - --models
      - +market_data
    timeout: 600
  
  # Graphs
  daily-etl.yaml: |
    name: daily-etl
    params:
      batch_date:
        type: string
        default: "${date.today()}"
    schedule: "0 2 * * *"
    tasks:
      - task: load-market-data
        params:
          batch_date: "${params.batch_date}"
```

### Updating Graphs in Production

**Process**:
1. Edit YAML files locally or in Git
2. Update ConfigMap
3. Restart orchestrator pod to reload config

```bash
# Update ConfigMap from directory
kubectl create configmap workflow-config \
  --from-file=graphs/ \
  --from-file=tasks/ \
  --dry-run=client -o yaml | kubectl apply -f -

# Restart orchestrator to reload
kubectl rollout restart deployment/orchestrator -n orchestrator

# Wait for restart
kubectl rollout status deployment/orchestrator -n orchestrator
```

**CI/CD Integration**:
```yaml
# .github/workflows/deploy-workflows.yml
name: Deploy Workflows

on:
  push:
    branches: [main]
    paths:
      - 'graphs/**'
      - 'tasks/**'

jobs:
  deploy:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v3
      
      - name: Authenticate to GCP
        uses: google-github-actions/auth@v1
        with:
          credentials_json: ${{ secrets.GCP_SA_KEY }}
      
      - name: Setup gcloud
        uses: google-github-actions/setup-gcloud@v1
      
      - name: Get GKE credentials
        run: |
          gcloud container clusters get-credentials orchestrator \
            --region us-central1 --project my-project
      
      - name: Update ConfigMap
        run: |
          kubectl create configmap workflow-config \
            --from-file=graphs/ \
            --from-file=tasks/ \
            --dry-run=client -o yaml | kubectl apply -f -
      
      - name: Restart Orchestrator
        run: |
          kubectl rollout restart deployment/orchestrator -n orchestrator
          kubectl rollout status deployment/orchestrator -n orchestrator
```

---

## UI Design

### Technology Stack

- **Framework**: Qute (Quarkus templating)
- **CSS**: Tabler.io (MIT license, comprehensive UI kit)
- **Icons**: Tabler Icons
- **Charts**: Chart.js
- **DAG Visualization**: dagre-d3
- **Gantt Charts**: vis-timeline
- **Editor**: Monaco Editor (VS Code)
- **Interactivity**: Vanilla JavaScript + Server-Sent Events (SSE)

### Page Structure

```
┌─────────────────────────────────────────────────────────────┐
│ Orchestrator                              [User] [Settings] │
├─────┬───────────────────────────────────────────────────────┤
│     │                                                       │
│ Nav │  Page Content                                         │
│     │                                                       │
│     │                                                       │
│Dash │                                                       │
│     │                                                       │
│     │                                                       │
│Grph │                                                       │
│     │                                                       │
│     │                                                       │
│Task │                                                       │
│     │                                                       │
│     │                                                       │
│Wrkr │                                                       │
│     │                                                       │
│     │                                                       │
│Evnt │                                                       │
│     │                                                       │
│     │                                                       │
│Sett │                                                       │
│     │                                                       │
└─────┴───────────────────────────────────────────────────────┘
```

### Key Pages

#### 1. Dashboard (`/`)

**Purpose**: Overview of system health and recent activity

**Content**:
- Stats cards: Active graphs, queued tasks, active workers, success rate
- Recent executions table
- Active graphs list with status
- System health indicators

#### 2. Graphs List (`/graphs`)

**Purpose**: Browse all available graphs

**Content**:
- Searchable/filterable table
- Columns: Name, Description, Last Run, Status, Actions
- Actions: Execute, View, History

#### 3. Graph Detail (`/graphs/{id}`)

**Purpose**: View graph execution in real-time

**Tabs**:
1. **Topology** - DAG visualization with color-coded nodes
2. **Tasks** - Table of all tasks with status, duration, logs link
3. **Gantt** - Timeline view of execution
4. **Logs** - Aggregated logs from all tasks

**Actions**:
- Execute (with param form)
- Pause/Resume
- View History

#### 4. Graph History (`/graphs/{id}/history`)

**Purpose**: View past executions

**Content**:
- Table of executions with filters (date range, status)
- Click to view specific execution

#### 5. Global Tasks (`/tasks`)

**Purpose**: View all global tasks and their current state

**Content**:
- Table of global tasks
- Active executions (which graphs are using them)
- Recent completions

#### 6. Workers (`/workers`)

**Purpose**: Monitor worker health and utilization

**Content**:
- Table of workers with status, threads, active tasks
- Worker metrics (CPU, memory if available)
- Heartbeat indicators

#### 7. Events (`/events`)

**Purpose**: View event log

**Content**:
- Searchable/filterable event stream
- Real-time updates
- Download events as JSONL

#### 8. Editor (`/editor`) [Dev Only]

**Purpose**: Edit graphs and tasks in browser

**Content**:
- File tree (graphs, tasks)
- Monaco editor with YAML syntax highlighting
- Save to file
- Validation

#### 9. Settings (`/settings`)

**Purpose**: System configuration

**Content**:
- Nuclear options (restart, hard reset)
- System info
- Configuration display

### Real-Time Updates

Use Server-Sent Events (SSE) for live updates:

```java
@Path("/api/stream")
public class StreamController {
    @Inject
    EventBus eventBus;
    
    @GET
    @Path("/graphs/{id}")
    @Produces(MediaType.SERVER_SENT_EVENTS)
    public Multi<GraphEvent> streamGraphEvents(@PathParam("id") UUID graphId) {
        return Multi.createFrom().emitter(emitter -> {
            Consumer<Message<Object>> handler = msg -> {
                GraphEvent event = convertToGraphEvent(msg.body());
                emitter.emit(event);
            };
            
            MessageConsumer<Object> consumer = eventBus.consumer("graph." + graphId);
            consumer.handler(handler);
            
            emitter.onTermination(() -> consumer.unregister());
        });
    }
}
```

```javascript
// Client-side
const eventSource = new EventSource('/api/stream/graphs/123');

eventSource.addEventListener('task-status', (event) => {
  const data = JSON.parse(event.data);
  updateTaskRow(data.taskId, data.status);
});

eventSource.addEventListener('graph-status', (event) => {
  const data = JSON.parse(event.data);
  updateGraphStatus(data.status);
});
```

---

## Data Model

### In-Memory State (Hazelcast)

#### Maps

```java
// Graph definitions (loaded from YAML)
IMap<String, Graph> graphDefinitions;

// Task definitions (loaded from YAML)
IMap<String, Task> taskDefinitions;

// Graph executions (current state)
IMap<UUID, GraphExecution> graphExecutions;

// Task executions (current state)
IMap<UUID, TaskExecution> taskExecutions;

// Global task executions (deduplicated)
IMap<TaskExecutionKey, GlobalTaskExecution> globalTasks;

// Worker registry
IMap<String, Worker> workers;
```

#### Queues

```java
// Work queue (tasks waiting for workers)
IQueue<WorkMessage> workQueue;
```

### Event Log (JSONL Files)

**File Structure**:
```
events/
├── 2025-10-15.jsonl
├── 2025-10-16.jsonl
└── 2025-10-17.jsonl
```

**Event Types**:
```json
{"event_type":"GRAPH_STARTED","timestamp":"2025-10-17T10:00:00Z","graph_execution_id":"uuid","graph_name":"daily-etl","triggered_by":"schedule","params":{"batch_date":"2025-10-17"}}

{"event_type":"TASK_QUEUED","timestamp":"2025-10-17T10:00:01Z","task_execution_id":"uuid","task_name":"load-data","graph_execution_id":"uuid"}

{"event_type":"TASK_STARTED","timestamp":"2025-10-17T10:00:05Z","task_execution_id":"uuid","worker_id":"worker-1","thread":"worker-1-thread-3"}

{"event_type":"TASK_COMPLETED","timestamp":"2025-10-17T10:05:00Z","task_execution_id":"uuid","duration_seconds":295,"result":{"rows_loaded":150000}}

{"event_type":"TASK_FAILED","timestamp":"2025-10-17T10:05:00Z","task_execution_id":"uuid","error":"Connection timeout","attempt":1}

{"event_type":"GRAPH_COMPLETED","timestamp":"2025-10-17T10:30:00Z","graph_execution_id":"uuid","status":"COMPLETED","duration_seconds":1800}

{"event_type":"GLOBAL_TASK_STARTED","timestamp":"2025-10-17T10:00:00Z","task_name":"load-market-data","resolved_key":"load_market_2025-10-17_us","params":{"batch_date":"2025-10-17","region":"us"},"initiated_by_graph":"uuid"}

{"event_type":"GLOBAL_TASK_LINKED","timestamp":"2025-10-17T10:05:00Z","resolved_key":"load_market_2025-10-17_us","linked_graph":"uuid"}

{"event_type":"GLOBAL_TASK_COMPLETED","timestamp":"2025-10-17T10:15:00Z","resolved_key":"load_market_2025-10-17_us","result":{"rows":1500000}}

{"event_type":"WORKER_HEARTBEAT","timestamp":"2025-10-17T10:00:00Z","worker_id":"worker-1","active_threads":3,"total_threads":16}

{"event_type":"WORKER_DIED","timestamp":"2025-10-17T10:00:00Z","worker_id":"worker-2"}

{"event_type":"CLUSTER_RESTART","timestamp":"2025-10-17T10:00:00Z","initiated_by":"admin@company.com"}

{"event_type":"CLUSTER_RESET","timestamp":"2025-10-17T10:00:00Z","initiated_by":"admin@company.com"}
```

### Recovery on Startup

```java
@ApplicationScoped
public class StateRecoveryService {
    void onStart(@Observes StartupEvent event) {
        // Find last checkpoint or reset event
        Instant recoverySince = findRecoveryPoint();
        
        // Read events since recovery point
        Stream<Event> events = eventStore.readSince(recoverySince);
        
        // Replay events to rebuild state
        events.forEach(this::replayEvent);
        
        // Clean up stale state
        cleanupStaleExecutions();
    }
    
    private Instant findRecoveryPoint() {
        // Look for CLUSTER_RESET events
        Optional<Event> reset = eventStore.findLastEvent("CLUSTER_RESET");
        if (reset.isPresent()) {
            return reset.get().timestamp();
        }
        
        // Default: recover last 24 hours
        return Instant.now().minus(24, ChronoUnit.HOURS);
    }
}
```

---

## Implementation Details

### Core Components

#### 1. Graph Loader

```java
@ApplicationScoped
public class GraphLoader {
    @Inject
    ExpressionEvaluator expressionEvaluator;
    @Inject
    GraphValidator graphValidator;
    @Inject
    HazelcastInstance hazelcast;
    
    @ConfigProperty(name = "orchestrator.config.graphs")
    String graphsPath;
    
    @ConfigProperty(name = "orchestrator.config.tasks")
    String tasksPath;
    
    private IMap<String, Graph> graphDefinitions;
    private IMap<String, Task> taskDefinitions;
    
    void onStart(@Observes StartupEvent event) {
        graphDefinitions = hazelcast.getMap("graph-definitions");
        taskDefinitions = hazelcast.getMap("task-definitions");
        
        loadTasks();
        loadGraphs();
    }
    
    private void loadTasks() {
        try (Stream<Path> paths = Files.walk(Path.of(tasksPath))) {
            paths.filter(p -> p.toString().endsWith(".yaml"))
                 .forEach(this::loadTask);
        }
    }
    
    private void loadTask(Path path) {
        String yaml = Files.readString(path);
        Task task = parseTask(yaml);
        
        // Validate
        validator.validateTask(task);
        
        // Store
        taskDefinitions.put(task.name(), task);
        
        logger.info("Loaded task: {}", task.name());
    }
    
    private void loadGraphs() {
        try (Stream<Path> paths = Files.walk(Path.of(graphsPath))) {
            paths.filter(p -> p.toString().endsWith(".yaml"))
                 .forEach(this::loadGraph);
        }
    }
    
    private void loadGraph(Path path) {
        String yaml = Files.readString(path);
        Graph graph = parseGraph(yaml);
        
        // Validate
        graphValidator.validateGraph(graph);
        
        // Store
        graphDefinitions.put(graph.name(), graph);
        
        logger.info("Loaded graph: {} ({} tasks)", 
            graph.name(), graph.tasks().size());
    }
}
```

#### 2. Graph Evaluator

```java
@ApplicationScoped
public class GraphEvaluator {
    @Inject
    EventBus eventBus;
    @Inject
    JGraphTService jGraphTService;
    @Inject
    HazelcastInstance hazelcast;
    
    private IMap<UUID, GraphExecution> graphExecutions;
    private IMap<UUID, TaskExecution> taskExecutions;
    private IMap<TaskExecutionKey, GlobalTaskExecution> globalTasks;
    
    @PostConstruct
    void init() {
        graphExecutions = hazelcast.getMap("graph-executions");
        taskExecutions = hazelcast.getMap("task-executions");
        globalTasks = hazelcast.getMap("global-tasks");
    }
    
    @ConsumeEvent("task.completed")
    @ConsumeEvent("task.failed")
    public void onTaskStatusChange(TaskEvent event) {
        // Update task execution
        TaskExecution te = taskExecutions.get(event.executionId());
        te = te.withStatus(event.status());
        taskExecutions.put(event.executionId(), te);
        
        // Find graph execution(s) to re-evaluate
        if (te.isGlobal()) {
            // Global task - notify all linked graphs
            GlobalTaskExecution gte = globalTasks.get(te.globalKey());
            gte.linkedGraphExecutions().forEach(graphExecId -> 
                eventBus.publish("graph.evaluate", graphExecId)
            );
        } else {
            // Regular task - notify its graph
            eventBus.publish("graph.evaluate", te.graphExecutionId());
        }
    }
    
    @ConsumeEvent("graph.evaluate")
    public void evaluate(UUID graphExecutionId) {
        GraphExecution exec = graphExecutions.get(graphExecutionId);
        
        // Build DAG
        DirectedAcyclicGraph<TaskNode, DefaultEdge> dag = 
            jGraphTService.buildDAG(exec.graph());
        
        // Get current state
        Map<TaskNode, TaskStatus> state = getCurrentState(graphExecutionId);
        
        // Find ready tasks
        Set<TaskNode> ready = jGraphTService.findReadyTasks(dag, state);
        
        // Schedule each ready task
        ready.forEach(task -> scheduleTask(graphExecutionId, task));
        
        // Update graph status
        updateGraphStatus(graphExecutionId, state);
    }
    
    private void scheduleTask(UUID graphExecutionId, TaskNode node) {
        if (node.isGlobal()) {
            scheduleGlobalTask(graphExecutionId, node);
        } else {
            scheduleRegularTask(graphExecutionId, node);
        }
    }
}
```

#### 3. Worker Pool

```java
@ApplicationScoped
public class WorkerPool {
    @Inject
    HazelcastInstance hazelcast;
    @Inject
    TaskExecutor taskExecutor;
    @Inject
    EventLogger eventLogger;
    
    @ConfigProperty(name = "worker.threads")
    int workerThreads;
    
    @ConfigProperty(name = "worker.id")
    String workerId;
    
    private IQueue<WorkMessage> workQueue;
    private ExecutorService executorService;
    private volatile boolean running = false;
    
    @PostConstruct
    void init() {
        workQueue = hazelcast.getQueue("work-queue");
    }
    
    public void start(int threads) {
        this.workerThreads = threads;
        this.running = true;
        
        // Create thread pool
        this.executorService = Executors.newFixedThreadPool(
            workerThreads,
            new ThreadFactory() {
                private final AtomicInteger counter = new AtomicInteger(0);
                
                @Override
                public Thread newThread(Runnable r) {
                    Thread t = new Thread(r);
                    t.setName(workerId + "-thread-" + counter.incrementAndGet());
                    return t;
                }
            }
        );
        
        // Start worker threads
        for (int i = 0; i < workerThreads; i++) {
            executorService.submit(this::workerLoop);
        }
        
        // Start heartbeat
        startHeartbeat();
        
        logger.info("Worker pool started: {} threads", workerThreads);
    }
    
    private void workerLoop() {
        String threadName = Thread.currentThread().getName();
        
        while (running) {
            try {
                // Pull work (blocking with timeout)
                WorkMessage work = workQueue.poll(5, TimeUnit.SECONDS);
                
                if (work == null) {
                    continue;  // Timeout, try again
                }
                
                executeWork(work, threadName);
                
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            } catch (Exception e) {
                logger.error("[{}] Error in worker loop", threadName, e);
            }
        }
    }
    
    private void executeWork(WorkMessage work, String threadName) {
        logger.info("[{}] Executing: {}", threadName, work.taskName());
        
        eventLogger.logTaskStarted(work.executionId(), workerId, threadName);
        
        Instant start = Instant.now();
        
        try {
            TaskResult result = taskExecutor.execute(work);
            Duration duration = Duration.between(start, Instant.now());
            
            if (result.isSuccess()) {
                logger.info("[{}] Completed: {} ({})", 
                    threadName, work.taskName(), duration);
                
                eventLogger.logTaskCompleted(
                    work.executionId(), result.data(), duration);
            } else {
                logger.error("[{}] Failed: {}", threadName, work.taskName());
                
                eventLogger.logTaskFailed(
                    work.executionId(), result.error(), work.attempt());
            }
            
        } catch (Exception e) {
            logger.error("[{}] Exception: {}", threadName, work.taskName(), e);
            
            eventLogger.logTaskFailed(
                work.executionId(), e.getMessage(), work.attempt());
        }
    }
}
```

#### 4. Task Executor

```java
@ApplicationScoped
public class TaskExecutor {
    @ConfigProperty(name = "orchestrator.dev.trial-run")
    boolean trialRun;
    
    @Inject
    ExpressionEvaluator expressionEvaluator;
    
    public TaskResult execute(WorkMessage work) {
        // Evaluate all expressions
        String command = expressionEvaluator.evaluate(
            work.command(), work.context());
        
        List<String> args = work.args().stream()
            .map(arg -> expressionEvaluator.evaluate(arg, work.context()))
            .toList();
        
        Map<String, String> env = work.env().entrySet().stream()
            .collect(Collectors.toMap(
                Map.Entry::getKey,
                e -> expressionEvaluator.evaluate(e.getValue(), work.context())
            ));
        
        // Build full command
        List<String> fullCommand = new ArrayList<>();
        fullCommand.add(command);
        fullCommand.addAll(args);
        
        // Trial run mode
        if (trialRun) {
            return executeTrialRun(fullCommand, env, work);
        }
        
        // Real execution
        return executeCommand(fullCommand, env, work);
    }
    
    private TaskResult executeTrialRun(
        List<String> command, 
        Map<String, String> env,
        WorkMessage work
    ) {
        String commandStr = String.join(" ", command);
        
        logger.info("═══════════════════════════════════════");
        logger.info("TRIAL RUN - Would execute:");
        logger.info("  Command: {}", commandStr);
        logger.info("  Timeout: {}s", work.timeoutSeconds());
        logger.info("  Environment:");
        env.forEach((k, v) -> logger.info("    {}={}", k, v));
        logger.info("═══════════════════════════════════════");
        
        // Return fake success
        return TaskResult.success(Map.of(
            "trial_run", true,
            "command", commandStr,
            "would_timeout", work.timeoutSeconds()
        ));
    }
    
    private TaskResult executeCommand(
        List<String> command,
        Map<String, String> env,
        WorkMessage work
    ) {
        try {
            ProcessBuilder pb = new ProcessBuilder(command);
            
            // Set environment
            pb.environment().putAll(env);
            
            // Redirect stderr to stdout
            pb.redirectErrorStream(true);
            
            // Start process
            Process process = pb.start();
            
            // Capture output
            StringBuilder output = new StringBuilder();
            try (BufferedReader reader = new BufferedReader(
                    new InputStreamReader(process.getInputStream()))) {
                String line;
                while ((line = reader.readLine()) != null) {
                    output.append(line).append("\n");
                    logger.info("[TASK OUTPUT] {}", line);
                }
            }
            
            // Wait for completion with timeout
            boolean completed = process.waitFor(
                work.timeoutSeconds(), 
                TimeUnit.SECONDS
            );
            
            if (!completed) {
                process.destroyForcibly();
                return TaskResult.failure(
                    "Task timed out after " + work.timeoutSeconds() + "s");
            }
            
            int exitCode = process.exitValue();
            
            if (exitCode == 0) {
                // Try to parse last line as JSON for downstream tasks
                Map<String, Object> result = tryParseJsonOutput(
                    output.toString());
                
                return TaskResult.success(result);
            } else {
                return TaskResult.failure(
                    "Task exited with code " + exitCode + "\n" + 
                    output.toString());
            }
            
        } catch (IOException e) {
            return TaskResult.failure(
                "Failed to start process: " + e.getMessage());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return TaskResult.failure("Task interrupted");
        }
    }
    
    private Map<String, Object> tryParseJsonOutput(String output) {
        try {
            String[] lines = output.split("\n");
            if (lines.length == 0) {
                return Map.of("output", output);
            }
            
            String lastLine = lines[lines.length - 1].trim();
            
            if (lastLine.startsWith("{") && lastLine.endsWith("}")) {
                ObjectMapper mapper = new ObjectMapper();
                return mapper.readValue(lastLine, 
                    new TypeReference<Map<String, Object>>() {});
            }
        } catch (Exception e) {
            // Not JSON, that's fine
        }
        
        return Map.of("output", output);
    }
}
```

#### 5. Event Store

```java
@ApplicationScoped
public class EventStore {
    @ConfigProperty(name = "orchestrator.storage.events")
    String eventsPath;
    
    @Inject
    StorageAdapter storageAdapter;
    
    private final DateTimeFormatter dateFormat = 
        DateTimeFormatter.ofPattern("yyyy-MM-dd");
    
    public void append(Event event) {
        String date = dateFormat.format(
            LocalDate.ofInstant(event.timestamp(), ZoneOffset.UTC));
        
        String filename = date + ".jsonl";
        String path = eventsPath + "/" + filename;
        
        String json = toJson(event) + "\n";
        
        storageAdapter.append(path, json);
    }
    
    public Stream<Event> readSince(Instant since) {
        LocalDate sinceDate = LocalDate.ofInstant(since, ZoneOffset.UTC);
        LocalDate today = LocalDate.now();
        
        return sinceDate.datesUntil(today.plusDays(1))
            .flatMap(date -> {
                String filename = dateFormat.format(date) + ".jsonl";
                String path = eventsPath + "/" + filename;
                
                try {
                    return storageAdapter.readLines(path)
                        .map(this::parseEvent)
                        .filter(e -> e.timestamp().isAfter(since));
                } catch (IOException e) {
                    logger.warn("Could not read events from {}", path);
                    return Stream.empty();
                }
            });
    }
    
    public Optional<Event> findLastEvent(String eventType) {
        // Search backwards from today
        LocalDate date = LocalDate.now();
        
        for (int i = 0; i < 30; i++) {  // Search last 30 days
            String filename = dateFormat.format(date) + ".jsonl";
            String path = eventsPath + "/" + filename;
            
            try {
                List<Event> events = storageAdapter.readLines(path)
                    .map(this::parseEvent)
                    .filter(e -> e.eventType().equals(eventType))
                    .toList();
                
                if (!events.isEmpty()) {
                    return Optional.of(events.get(events.size() - 1));
                }
            } catch (IOException e) {
                // File doesn't exist, continue
            }
            
            date = date.minusDays(1);
        }
        
        return Optional.empty();
    }
    
    private String toJson(Event event) {
        try {
            ObjectMapper mapper = new ObjectMapper();
            return mapper.writeValueAsString(event);
        } catch (JsonProcessingException e) {
            throw new RuntimeException("Failed to serialize event", e);
        }
    }
    
    private Event parseEvent(String json) {
        try {
            ObjectMapper mapper = new ObjectMapper();
            return mapper.readValue(json, Event.class);
        } catch (JsonProcessingException e) {
            throw new RuntimeException("Failed to parse event", e);
        }
    }
}
```

#### 6. Storage Adapter

```java
public interface StorageAdapter {
    void append(String path, String content);
    Stream<String> readLines(String path) throws IOException;
}

@ApplicationScoped
public class StorageAdapterFactory {
    public StorageAdapter create(String path) {
        if (path.startsWith("gs://")) {
            return new GcsStorageAdapter();
        } else if (path.startsWith("s3://")) {
            return new S3StorageAdapter();
        } else if (path.startsWith("file://") || path.startsWith("./")) {
            return new LocalFilesystemAdapter();
        } else {
            throw new IllegalArgumentException(
                "Unsupported storage path: " + path);
        }
    }
}

@ApplicationScoped
public class LocalFilesystemAdapter implements StorageAdapter {
    @Override
    public void append(String path, String content) {
        Path filePath = Path.of(path.replace("file://", ""));
        
        // Create directory if needed
        Files.createDirectories(filePath.getParent());
        
        // Append to file
        Files.writeString(filePath, content, 
            StandardOpenOption.CREATE, 
            StandardOpenOption.APPEND);
    }
    
    @Override
    public Stream<String> readLines(String path) throws IOException {
        Path filePath = Path.of(path.replace("file://", ""));
        return Files.lines(filePath);
    }
}

@ApplicationScoped
public class GcsStorageAdapter implements StorageAdapter {
    @Inject
    Storage storage;
    
    @Override
    public void append(String path, String content) {
        // Parse gs://bucket/path
        String pathWithoutScheme = path.substring("gs://".length());
        String[] parts = pathWithoutScheme.split("/", 2);
        String bucket = parts[0];
        String objectPath = parts[1];
        
        BlobId blobId = BlobId.of(bucket, objectPath);
        BlobInfo blobInfo = BlobInfo.newBuilder(blobId).build();
        
        // Check if exists
        Blob blob = storage.get(blobId);
        
        if (blob != null) {
            // Append to existing
            byte[] existing = blob.getContent();
            byte[] combined = concat(existing, content.getBytes());
            storage.create(blobInfo, combined);
        } else {
            // Create new
            storage.create(blobInfo, content.getBytes());
        }
    }
    
    @Override
    public Stream<String> readLines(String path) throws IOException {
        String pathWithoutScheme = path.substring("gs://".length());
        String[] parts = pathWithoutScheme.split("/", 2);
        String bucket = parts[0];
        String objectPath = parts[1];
        
        BlobId blobId = BlobId.of(bucket, objectPath);
        Blob blob = storage.get(blobId);
        
        if (blob == null) {
            throw new FileNotFoundException(path);
        }
        
        String content = new String(blob.getContent());
        return content.lines();
    }
}
```

---

## Testing Strategy

### Unit Tests

**Repository Tests**:
```java
@QuarkusTest
class GraphRepositoryTest {
    @Inject
    GraphRepository graphRepo;
    
    @Test
    void shouldLoadGraphFromYaml() {
        String yaml = """
            name: test-graph
            tasks:
              - name: task1
                command: echo
                args:
                  - hello
            """;
        
        Graph graph = graphRepo.parseYaml(yaml);
        
        assertThat(graph.name()).isEqualTo("test-graph");
        assertThat(graph.tasks()).hasSize(1);
    }
}
```

**Expression Evaluator Tests**:
```java
@QuarkusTest
class ExpressionEvaluatorTest {
    @Inject
    ExpressionEvaluator evaluator;
    
    @Test
    void shouldEvaluateSimpleExpression() {
        Context ctx = new Context(
            Map.of("region", "us"),
            Map.of(),
            Map.of()
        );
        
        String result = evaluator.evaluate("${params.region}", ctx);
        
        assertThat(result).isEqualTo("us");
    }
    
    @Test
    void shouldEvaluateConditional() {
        Context ctx = new Context(
            Map.of("env", "prod"),
            Map.of(),
            Map.of()
        );
        
        String result = evaluator.evaluate(
            "${params.env == 'prod' ? 'production' : 'development'}", 
            ctx
        );
        
        assertThat(result).isEqualTo("production");
    }
}
```

**JGraphT Service Tests**:
```java
@QuarkusTest
class JGraphTServiceTest {
    @Inject
    JGraphTService jGraphTService;
    
    @Test
    void shouldDetectCycle() {
        Graph graph = createGraphWithCycle();
        
        assertThatThrownBy(() -> jGraphTService.buildDAG(graph))
            .isInstanceOf(CycleFoundException.class);
    }
    
    @Test
    void shouldFindReadyTasks() {
        // A -> B -> C
        Graph graph = createLinearGraph();
        DirectedAcyclicGraph<TaskNode, DefaultEdge> dag = 
            jGraphTService.buildDAG(graph);
        
        Map<TaskNode, TaskStatus> state = Map.of(
            taskA, TaskStatus.COMPLETED,
            taskB, TaskStatus.PENDING,
            taskC, TaskStatus.PENDING
        );
        
        Set<TaskNode> ready = jGraphTService.findReadyTasks(dag, state);
        
        assertThat(ready).containsOnly(taskB);
    }
}
```

### Integration Tests

**Full Graph Execution**:
```java
@QuarkusTest
@TestProfile(IntegrationTestProfile.class)
class GraphExecutionIntegrationTest {
    @Inject
    GraphEvaluator evaluator;
    @Inject
    GraphRepository graphRepo;
    @Inject
    WorkerPool workerPool;
    
    @BeforeEach
    void setup() {
        // Start embedded worker
        workerPool.start(2);
    }
    
    @Test
    void shouldExecuteSimpleGraph() {
        // Load test graph
        Graph graph = graphRepo.loadFromFile("test-graphs/simple.yaml");
        
        // Create execution
        GraphExecution exec = graphExecRepo.create(
            graph.id(),
            Map.of("date", "2025-10-17"),
            "test"
        );
        
        // Trigger evaluation
        evaluator.evaluate(exec.id());
        
        // Wait for completion
        await().atMost(30, SECONDS).until(() -> {
            GraphExecution updated = graphExecRepo.findById(exec.id());
            return updated.status() == GraphStatus.COMPLETED;
        });
        
        // Verify all tasks completed
        List<TaskExecution> tasks = 
            taskExecRepo.findByGraphExecution(exec.id());
        
        assertThat(tasks).allMatch(te -> 
            te.status() == TaskStatus.COMPLETED);
    }
    
    @Test
    void shouldHandleGlobalTaskDeduplication() {
        // Create two graphs that use same global task
        Graph graph1 = graphRepo.loadFromFile("test-graphs/with-global-1.yaml");
        Graph graph2 = graphRepo.loadFromFile("test-graphs/with-global-2.yaml");
        
        Map<String, Object> params = Map.of("date", "2025-10-17");
        
        // Start both executions
        GraphExecution exec1 = graphExecRepo.create(
            graph1.id(), params, "test");
        GraphExecution exec2 = graphExecRepo.create(
            graph2.id(), params, "test");
        
        evaluator.evaluate(exec1.id());
        evaluator.evaluate(exec2.id());
        
        // Wait for completion
        await().atMost(30, SECONDS).until(() -> {
            GraphExecution e1 = graphExecRepo.findById(exec1.id());
            GraphExecution e2 = graphExecRepo.findById(exec2.id());
            return e1.status() == GraphStatus.COMPLETED &&
                   e2.status() == GraphStatus.COMPLETED;
        });
        
        // Verify only ONE global task execution occurred
        List<GlobalTaskExecution> globalExecs = 
            globalTaskRepo.findByResolvedKey("load_data_2025-10-17");
        
        assertThat(globalExecs).hasSize(1);
        assertThat(globalExecs.get(0).linkedGraphExecutions())
            .containsExactlyInAnyOrder(exec1.id(), exec2.id());
    }
}
```

**Trial Run Test**:
```java
@QuarkusTest
@TestProfile(TrialRunTestProfile.class)
class TrialRunTest {
    @Inject
    TaskExecutor taskExecutor;
    
    @Test
    void shouldLogCommandWithoutExecuting() {
        WorkMessage work = new WorkMessage(
            UUID.randomUUID(),
            "test-task",
            "python",
            List.of("script.py", "--dangerous"),
            Map.of("API_KEY", "secret"),
            600
        );
        
        TaskResult result = taskExecutor.execute(work);
        
        // Should succeed without actually running
        assertThat(result.isSuccess()).isTrue();
        assertThat(result.data()).containsEntry("trial_run", true);
        assertThat(result.data()).containsEntry("command", 
            "python script.py --dangerous");
        
        // Verify nothing was actually executed (check logs)
    }
}
```

### End-to-End Tests

Use Testcontainers for full stack:

```java
@QuarkusTest
@TestProfile(E2ETestProfile.class)
class EndToEndTest {
    @Container
    static HazelcastContainer hazelcast = 
        new HazelcastContainer("hazelcast/hazelcast:5.3");
    
    @Test
    void shouldExecuteGraphEndToEnd() {
        // Deploy graph via API
        given()
            .contentType("application/yaml")
            .body(loadTestGraph())
        .when()
            .post("/api/graphs")
        .then()
            .statusCode(201);
        
        // Trigger execution
        String executionId = given()
            .contentType("application/json")
            .body(Map.of("params", Map.of("date", "2025-10-17")))
        .when()
            .post("/api/graphs/test-graph/execute")
        .then()
            .statusCode(200)
            .extract()
            .path("executionId");
        
        // Poll for completion
        await().atMost(60, SECONDS).until(() -> {
            String status = given()
                .when()
                    .get("/api/graphs/executions/" + executionId)
                .then()
                    .statusCode(200)
                    .extract()
                    .path("status");
            
            return "COMPLETED".equals(status);
        });
        
        // Verify events logged
        String events = given()
            .when()
                .get("/api/events?execution=" + executionId)
            .then()
                .statusCode(200)
                .extract()
                .asString();
        
        assertThat(events).contains("GRAPH_STARTED");
        assertThat(events).contains("TASK_COMPLETED");
        assertThat(events).contains("GRAPH_COMPLETED");
    }
}
```

---

## Deployment Guide

### Local Development

**Prerequisites**:
- Java 21+
- Maven or Gradle

**Steps**:

1. Clone repository
```bash
git clone https://github.com/yourorg/orchestrator.git
cd orchestrator
```

2. Build
```bash
./mvnw clean package
```

3. Create config directories
```bash
mkdir -p graphs tasks data/events
```

4. Create sample graph
```bash
cat > graphs/hello-world.yaml <<EOF
name: hello-world
tasks:
  - name: say-hello
    command: echo
    args:
      - "Hello, World!"
EOF
```

5. Run
```bash
java -jar target/quarkus-app/quarkus-run.jar
```

6. Open browser
```bash
open http://localhost:8080
```

### Docker

**Build**:
```bash
docker build -t orchestrator:latest .
```

**Run**:
```bash
docker run -p 8080:8080 \
  -v $(pwd)/graphs:/config/graphs \
  -v $(pwd)/tasks:/config/tasks \
  -v $(pwd)/data:/data \
  orchestrator:latest
```

### Docker Compose (Full Stack)

```yaml
# docker-compose.yml
version: '3.8'

services:
  hazelcast:
    image: hazelcast/hazelcast:5.3
    environment:
      JAVA_OPTS: "-Xms512m -Xmx512m"
    ports:
      - "5701:5701"
  
  orchestrator:
    build: .
    environment:
      ORCHESTRATOR_MODE: prod
      HAZELCAST_MEMBERS: hazelcast:5701
    ports:
      - "8080:8080"
    volumes:
      - ./graphs:/config/graphs:ro
      - ./tasks:/config/tasks:ro
      - ./data:/data
    depends_on:
      - hazelcast
  
  worker:
    build: .
    command: ["worker"]
    environment:
      WORKER_THREADS: 8
      HAZELCAST_MEMBERS: hazelcast:5701
    volumes:
      - ./graphs:/config/graphs:ro
      - ./tasks:/config/tasks:ro
    depends_on:
      - hazelcast
    deploy:
      replicas: 2
```

### GKE Deployment

**Prerequisites**:
- GKE cluster
- kubectl configured
- Container images pushed to GCR

**Deploy Hazelcast**:
```bash
kubectl apply -f k8s/hazelcast-statefulset.yaml
```

**Create ConfigMap**:
```bash
kubectl create configmap workflow-config \
  --from-file=graphs/ \
  --from-file=tasks/ \
  -n orchestrator
```

**Deploy Orchestrator**:
```bash
kubectl apply -f k8s/orchestrator-deployment.yaml
```

**Deploy Workers**:
```bash
kubectl apply -f k8s/worker-deployment.yaml
```

**Verify**:
```bash
kubectl get pods -n orchestrator
kubectl logs -n orchestrator deployment/orchestrator
```

**Access UI**:
```bash
kubectl port-forward -n orchestrator svc/orchestrator 8080:80
open http://localhost:8080
```

---

## Summary

### What We're Building

A **lightweight, portable, event-driven workflow orchestrator** that:

1. **Runs anywhere** - Single binary, no dependencies
2. **Uses simple YAML** - No Python code required
3. **Deduplicates work** - Global tasks run once, notify many
4. **Scales efficiently** - Multi-threaded workers for I/O workloads
5. **Has great DX** - Dev mode, trial run, hot reload, web editor
6. **Is observable** - Events in files, metrics, real-time UI
7. **Gets out of your way** - Simple, fast, focused

### Key Features

✅ **Global Tasks** - Run once per parameter set, notify all graphs
✅ **Parameter Scoping** - Clean override hierarchy
✅ **JEXL Expressions** - Powerful, safe templating
✅ **Single Process Dev Mode** - Everything in one process
✅ **Trial Run** - Test without executing
✅ **Web Editor** - Edit YAML in browser (dev only)
✅ **File-Based Events** - JSONL files, not databases
✅ **Multi-Threaded Workers** - Efficient for GCP workloads
✅ **Tabler UI** - Professional, polished interface
✅ **Hazelcast State** - Fast, distributed state management

### Technology Stack

- **Language**: Java 21
- **Framework**: Quarkus
- **State**: Hazelcast (embedded in dev, clustered in prod)
- **Events**: JSONL files (local/GCS/S3)
- **UI**: Qute + Tabler CSS + Monaco Editor
- **Expressions**: Apache Commons JEXL
- **DAG**: JGraphT
- **Deployment**: Single binary or GKE

### Next Steps

1. **Create GitHub repository**
2. **Generate feature issues** from this design
3. **Setup CI/CD pipeline** (GitHub Actions)
4. **Implement core features** in phases
5. **Test extensively** (unit, integration, E2E)
6. **Deploy to GKE**
7. **Iterate based on feedback**

---

## Appendix: Example YAML Files

### Example: Global Task

```yaml
# tasks/load-market-data.yaml
name: load-market-data
global: true
key: "load_market_${params.batch_date}_${params.region}"

params:
  batch_date:
    type: string
    required: true
    description: Business date to process (YYYY-MM-DD)
  
  region:
    type: string
    default: us
    description: Market region (us, eu, asia)

command: dbt
args:
  - run
  - --models
  - +market_data
  - --vars
  - "batch_date:${params.batch_date},region:${params.region}"
  - --target
  - prod

env:
  DBT_PROFILES_DIR: /dbt/profiles
  DBT_TARGET: prod

timeout: 600
retry: 3
```

### Example: Graph with Multiple Tasks

```yaml
# graphs/daily-risk-calculation.yaml
name: daily-risk-calculation
description: |
  Daily risk calculation pipeline.
  Runs after market close to calculate VaR and stress scenarios.

params:
  batch_date:
    type: string
    default: "${date.today()}"
    description: Processing date
  
  region:
    type: string
    default: us
    description: Market region
  
  confidence_level:
    type: number
    default: 0.99
    description: VaR confidence level

env:
  GCS_BUCKET: "gs://risk-data-${params.region}"
  PROCESSING_DATE: "${params.batch_date}"

schedule: "0 18 * * 1-5"  # 6 PM, weekdays only

tasks:
  # Global task - shared with other graphs
  - task: load-market-data
    params:
      batch_date: "${params.batch_date}"
      region: "${params.region}"
  
  # Inline task - specific to this graph
  - name: calculate-var
    command: python
    args:
      - /opt/risk/var.py
      - --date
      - "${params.batch_date}"
      - --region
      - "${params.region}"
      - --confidence
      - "${params.confidence_level}"
      - --output
      - "${env.GCS_BUCKET}/var/${params.batch_date}.parquet"
    env:
      PYTHONUNBUFFERED: "1"
    timeout: 1800
    retry: 2
    depends_on:
      - load-market-data
  
  - name: stress-test
    command: python
    args:
      - /opt/risk/stress.py
      - --date
      - "${params.batch_date}"
      - --scenarios
      - "recession,spike,crash"
      - --input
      - "${env.GCS_BUCKET}/var/${params.batch_date}.parquet"
    timeout: 3600
    depends_on:
      - calculate-var
  
  - name: generate-report
    command: python
    args:
      - /opt/risk/report.py
      - --date
      - "${params.batch_date}"
      - --var-results
      - "${task.calculate-var.result.output_file}"
      - --stress-results
      - "${task.stress-test.result.output_file}"
    depends_on:
      - calculate-var
      - stress-test
```

### Example: Configuration

```yaml
# application.yaml
orchestrator:
  mode: dev
  
  config:
    graphs: ./graphs
    tasks: ./tasks
    watch: true
  
  dev:
    worker-threads: 4
    trial-run: false
    enable-editor: true
  
  storage:
    events: file://./data/events
  
  hazelcast:
    embedded: true
    cluster-name: orchestrator-dev
  
  worker:
    threads: 4
    heartbeat-interval: 10
    dead-threshold: 60

server:
  port: 8080

logging:
  level: INFO
  format: text

---

# application-prod.yaml
orchestrator:
  mode: prod
  
  config:
    graphs: /config/graphs
    tasks: /config/tasks
    watch: false
  
  storage:
    events: gs://my-bucket/orchestrator/events
  
  hazelcast:
    embedded: false
    cluster-name: orchestrator-prod
    members:
      - hazelcast-0.hazelcast.orchestrator.svc.cluster.local
      - hazelcast-1.hazelcast.orchestrator.svc.cluster.local
      - hazelcast-2.hazelcast.orchestrator.svc.cluster.local
  
  worker:
    threads: 16
    heartbeat-interval: 10
    dead-threshold: 60

server:
  port: 8080

logging:
  level: INFO
  format: json
```

---

**END OF DESIGN DOCUMENT**