[![CI Pipeline](https://github.com/joefrost01/quorch/actions/workflows/build.yml/badge.svg)](https://github.com/joefrost01/quorch/actions/workflows/build.yml)
[![Quarto Docs](https://img.shields.io/badge/docs-online-blue.svg)](https://joefrost01.github.io/quorch/)

# Quorch

**A lightweight, portable, event-driven workflow orchestrator that gets out of your way.**

Designed for data teams who want to ship fast without operational overhead. Define workflows in simple YAML, run anywhere from your laptop to GKE, and let Quorch handle the rest.

## Features

- **🎯 Simple YAML Configuration** - No code required, just declare your workflows
- **🌍 Global Task Deduplication** - Run expensive tasks once, notify many graphs
- **📦 Single Binary** - No dependencies, runs anywhere
- **📁 File-Based Events** - JSONL files instead of databases for simplicity
- **⚡ Multi-Threaded Workers** - Efficient parallel execution for I/O-bound tasks
- **🔄 Dev Mode** - Single process with embedded worker and hot reload
- **🧪 Trial Run** - Test workflows without executing commands
- **🎨 Modern UI** - Real-time monitoring with clean, professional interface
- **🔌 Portable** - Local development, Docker, or Kubernetes

## Quick Start

### Local Development

```bash
# Clone and build
git clone https://github.com/yourorg/quorch.git
cd quorch
./mvnw clean package

# Create config directories
mkdir -p graphs tasks data/events

# Create your first workflow
cat > graphs/hello-world.yaml <<EOF
name: hello-world
tasks:
  - name: greet
    command: echo
    args:
      - "Hello from Quorch!"
EOF

# Run in dev mode
./mvnw quarkus:dev

# Open browser
open http://localhost:8080
```

### Docker

```bash
docker build -f src/main/docker/Dockerfile.jvm -t quorch:latest .

docker run -p 8080:8080 \
  -v $(pwd)/graphs:/config/graphs \
  -v $(pwd)/tasks:/config/tasks \
  -v $(pwd)/data:/data \
  quorch:latest
```

## Core Concepts

### Tasks

A task is a single unit of work - just a command to execute:

```yaml
name: extract-data
command: python
args:
  - /scripts/extract.py
  - --date
  - "${params.batch_date}"
env:
  API_KEY: "${env.API_KEY}"
timeout: 600
retry: 3
```

### Global Tasks

Global tasks run once per unique parameter set and can be shared across multiple graphs:

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
```

**Key Feature**: If Graph A and Graph B both need `load-market-data[2025-10-17, us]`, the task runs once and both graphs are notified on completion.

### Graphs

A graph is a DAG of tasks with dependencies:

```yaml
name: daily-etl
description: Daily ETL pipeline

params:
  batch_date:
    type: string
    default: "${date.today()}"

schedule: "0 2 * * *"  # 2 AM daily

tasks:
  # Reference global task
  - task: load-market-data
    params:
      batch_date: "${params.batch_date}"
  
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

## Expression Language

Quorch uses JEXL for dynamic expressions in `${ }`:

```yaml
# Variables
args:
  - "${params.region}"
  - "${env.API_KEY}"

# Date functions
params:
  yesterday:
    default: "${date.add(date.today(), -1, 'days')}"

# Conditionals
args:
  - "${params.full_refresh ? '--full-refresh' : '--incremental'}"

# Upstream task results
args:
  - --row-count
  - "${task.extract.result.row_count}"

# String manipulation
env:
  TABLE_NAME: "${'data_' + params.region + '_' + params.date.replace('-', '_')}"
```

### Built-in Functions

**Date:**
- `date.today()` → `"2025-10-17"`
- `date.now('yyyy-MM-dd HH:mm:ss')` → `"2025-10-17 14:30:00"`
- `date.add(date, n, 'days')` / `date.sub(date, n, 'days')`
- `date.format(date, 'yyyy/MM/dd')`

**String:**
- `string.uuid()` → UUID
- `string.slugify('My Name')` → `"my-name"`

**Math:** `Math.round()`, `Math.floor()`, `Math.ceil()`, `Math.max()`, `Math.min()`

## Configuration

### Development Mode

```yaml
# application.yml
orchestrator:
  mode: dev
  
  config:
    graphs: ./graphs
    tasks: ./tasks
    watch: true  # Hot reload on file changes
  
  dev:
    worker-threads: 4
    trial-run: false      # Set true to simulate execution
    enable-editor: true   # Web-based YAML editor
  
  storage:
    events: file://./data/events

server:
  port: 8080
```

### Production Mode

```yaml
# application-prod.yml
orchestrator:
  mode: prod
  
  config:
    graphs: /config/graphs
    tasks: /config/tasks
  
  storage:
    events: gs://my-bucket/orchestrator/events  # or s3://...
  
  hazelcast:
    embedded: false
    cluster-name: orchestrator-prod
    members:
      - hazelcast-0.hazelcast.svc.cluster.local
      - hazelcast-1.hazelcast.svc.cluster.local
      - hazelcast-2.hazelcast.svc.cluster.local
  
  worker:
    threads: 16
    heartbeat-interval: 10
```

## Architecture

### Development Mode
```
┌─────────────────────────────────────┐
│      Single JVM Process             │
│  - Orchestrator Core                │
│  - Embedded Hazelcast               │
│  - Embedded Worker (4 threads)      │
│  - Web UI                           │
│  - Local Event Store                │
└─────────────────────────────────────┘
```

### Production Mode (GKE)
```
┌────────────────┐     ┌─────────────────────┐
│  Orchestrator  │────▶│  Worker Pods (HPA)  │
│  (1 replica)   │     │  (2-20 replicas)    │
└────────┬───────┘     └──────────┬──────────┘
         │                        │
         ├────────────────────────┤
         │                        │
    ┌────▼────┐              ┌────▼───┐
    │Hazelcast│              │  GCS   │
    │ Cluster │              │ Events │
    │(3 nodes)│              └────────┘
    └─────────┘
```

## Triggers

### Cron Schedule

```yaml
name: nightly-report
schedule: "0 2 * * *"  # 2 AM daily
tasks:
  - name: generate-report
    command: python
    args: [report.py]
```

### Webhook

```bash
curl -X POST http://localhost:8080/api/triggers/webhook/my-graph \
  -H "Content-Type: application/json" \
  -d '{"params": {"date": "2025-10-17"}}'
```

### Google Pub/Sub

```yaml
name: event-driven-pipeline
triggers:
  - type: pubsub
    subscription: projects/my-project/subscriptions/data-events
tasks:
  - name: process-event
    command: python
    args: [process.py]
```

## Web UI

Access the web UI at `http://localhost:8080`:

- **Dashboard** - System overview and recent activity
- **Graphs** - Browse and execute workflows
- **Graph Detail** - Real-time execution monitoring with DAG visualization
- **Tasks** - View global tasks and executions
- **Workers** - Monitor worker health and utilization
- **Events** - Searchable event log
- **Editor** *(dev mode)* - Edit YAML files in-browser
- **Settings** - System configuration and admin actions

### Real-Time Updates

The UI uses Server-Sent Events (SSE) for real-time updates without polling.

## API

### Execute a Graph

```bash
POST /api/graphs/{name}/execute
{
  "params": {
    "batch_date": "2025-10-17",
    "region": "us"
  }
}
```

### Get Execution Status

```bash
GET /api/graphs/executions/{id}
```

### List Workers

```bash
GET /api/workers
```

### Query Events

```bash
GET /api/events?type=GRAPH_STARTED&since=2025-10-17T00:00:00Z
```

Full API documentation available at `/q/swagger-ui` when running.

## Deployment

### Kubernetes (GKE)

```bash
# Create namespace
kubectl create namespace orchestrator

# Deploy Hazelcast cluster
kubectl apply -f k8s/hazelcast-statefulset.yaml

# Create ConfigMap from local files
kubectl create configmap workflow-config \
  --from-file=graphs/ \
  --from-file=tasks/ \
  -n orchestrator

# Deploy orchestrator
kubectl apply -f k8s/orchestrator-deployment.yaml

# Deploy workers with HPA
kubectl apply -f k8s/worker-deployment.yaml

# Access UI
kubectl port-forward -n orchestrator svc/orchestrator 8080:80
```

### Update Workflows in Production

```bash
# Update ConfigMap
kubectl create configmap workflow-config \
  --from-file=graphs/ \
  --from-file=tasks/ \
  --dry-run=client -o yaml | kubectl apply -f -

# Restart orchestrator to reload
kubectl rollout restart deployment/orchestrator -n orchestrator
```

## Monitoring

### Prometheus Metrics

Metrics exposed at `/q/metrics`:

- `graph_executions_total{status}` - Executions by status
- `graph_execution_duration_seconds` - Execution duration histogram
- `tasks_queued` - Tasks waiting for workers
- `tasks_executing` - Tasks currently running
- `workers_active` - Active workers
- `global_tasks_active` - Active global task executions

### Health Checks

- `/q/health/live` - Liveness probe
- `/q/health/ready` - Readiness probe (includes Hazelcast check)

### Structured Logging

Production uses JSON logging with structured fields:

```json
{
  "timestamp": "2025-10-17T10:00:00Z",
  "level": "INFO",
  "message": "Task completed",
  "graphExecutionId": "uuid",
  "taskExecutionId": "uuid",
  "taskName": "load-data",
  "duration": 295,
  "workerId": "worker-1"
}
```

## Development Features

### Hot Reload

In dev mode, file changes trigger automatic reload:

```bash
# Edit a graph
vim graphs/my-graph.yaml

# Quorch detects and reloads automatically
# [INFO] Config file changed: my-graph.yaml
# [INFO] ✓ Graph reloaded: my-graph
```

### Trial Run Mode

Test workflows without executing commands:

```yaml
# application.yml
orchestrator:
  dev:
    trial-run: true
```

Output:
```
[INFO] TRIAL RUN - Would execute: python script.py --date 2025-10-17
[INFO]   Working dir: /opt/scripts
[INFO]   Environment: {API_KEY=secret}
[INFO]   Timeout: 600s
```

### Web-Based Editor

Enable in-browser YAML editing (dev mode only):

```yaml
orchestrator:
  dev:
    enable-editor: true
```

Access at `/editor` - features Monaco editor with syntax highlighting and validation.

## Parameter Scoping

Parameters resolve with clear priority:

1. **Runtime** - Params passed when triggering
2. **Graph Task Reference** - Params in graph's task definition
3. **Global Task Defaults** - Defaults in global task
4. **Graph Defaults** - Defaults in graph params

Example:
```yaml
# Global task default: region=us
# Graph default: region=eu
# Task reference: region=asia
# Runtime: region=africa

# Final value: africa (runtime wins)
```

## Event Log

All events stored in JSONL files (local or GCS/S3):

```
events/
├── 2025-10-15.jsonl
├── 2025-10-16.jsonl
└── 2025-10-17.jsonl
```

Events include:
- Graph lifecycle (started, completed, failed)
- Task lifecycle (queued, started, completed, failed)
- Global task operations (started, linked, completed)
- Worker heartbeats
- Admin actions

## Use Cases

Quorch is ideal for:

- **Data Engineering Pipelines** - ETL, ELT, data transformations
- **Scheduled Jobs** - Reports, exports, cleanups
- **Event-Driven Workflows** - Process events from Pub/Sub, webhooks
- **Multi-Cloud Deployments** - Works anywhere, no vendor lock-in
- **Edge Computing** - Lightweight enough for edge locations
- **Development Workflows** - CI/CD, testing, deployments

## Not For

- FAANG-scale (millions of tasks/day) - Use Airflow or Temporal
- Complex multi-tenancy - Single orchestrator per environment
- Microservice orchestration - Use Temporal or Conductor
- Teams deeply invested in Airflow ecosystem

## Technology Stack

- **Language**: Java 21
- **Framework**: Quarkus 3.x
- **State**: Hazelcast (embedded dev, clustered prod)
- **Events**: JSONL files (local/GCS/S3)
- **UI**: Qute templates + Tabler CSS
- **Expressions**: Apache Commons JEXL
- **DAG**: JGraphT
- **Scheduler**: Quarkus Quartz

## Contributing

Contributions welcome! Please see [CONTRIBUTING.md](CONTRIBUTING.md) for guidelines.

## License

[Your License Here - e.g., Apache 2.0]

## Roadmap

- [ ] Multiple execution backends (ECS, Cloud Run)
- [ ] Advanced retry policies (exponential backoff, jitter)
- [ ] Task templates and libraries
- [ ] Workflow versioning
- [ ] Advanced monitoring dashboards
- [ ] Plugin system for custom functions
- [ ] RBAC and multi-tenancy
- [ ] Workflow composition and subgraphs

## Support

- **Documentation**: [docs/](docs/)
- **Issues**: [GitHub Issues](https://github.com/yourorg/quorch/issues)
- **Discussions**: [GitHub Discussions](https://github.com/yourorg/quorch/discussions)

---

**Built with ❤️ for data teams who value simplicity.**