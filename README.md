# JobRail

JobRail is a **Redis-backed background job queue built with Rust**.

It provides the infrastructure needed to reliably create, schedule,
execute, retry, and monitor background jobs through a Rust API and
worker system.

> **Status:** JobRail is currently under active development and has
> **not been deployed yet**. It currently runs locally / self-hosted.

---

## Architecture

```text
                         ┌──────────────────┐
                         │   Application    │
                         └────────┬─────────┘
                                  │
                                  │ HTTP
                                  ▼
                         ┌──────────────────┐
                         │    Axum API      │
                         └────────┬─────────┘
                                  │
                                  ▼
                         ┌──────────────────┐
                         │      Redis       │
                         │                  │
                         │  Waiting         │
                         │  Prioritized     │
                         │  Delayed         │
                         │  Scheduled       │
                         │  Active          │
                         │  Processing      │
                         └────────┬─────────┘
                                  │
                                  ▼
                         ┌──────────────────┐
                         │     Worker       │
                         └────────┬─────────┘
                                  │
                                  ▼
                         ┌──────────────────┐
                         │   Job Handler    │
                         └──────────────────┘
```

JobRail separates the **API that accepts jobs** from the **workers that
execute them**.

This allows job producers and job consumers to scale independently.

---

## Features

### Job Processing

- Job creation
- Job states
- State transitions
- Redis-backed queues
- Worker concurrency
- Atomic job claiming
- Job execution
- Job completion
- Job failure handling

### Reliability

- At-least-once delivery
- Lease-based job ownership
- Lease renewal
- Expired-job recovery
- Atomic Redis operations
- Lua scripts for state transitions

### Retries

- Configurable maximum attempts
- Retry scheduling
- Exponential backoff
- Delayed retries
- Manual retry of failed jobs

### Scheduling

- Delayed jobs
- Scheduled jobs
- Scheduled-job promotion
- Repeatable jobs
- Repeatable-job scheduler

### Job History

- Attempt tracking
- Attempt status
- Attempt start timestamps
- Attempt finish timestamps
- Attempt errors

### Job Controls

- Cancel jobs
- Retry failed jobs
- Inspect attempts
- List jobs
- Filter jobs by state
- Cursor-based pagination

---

## Idempotency

JobRail supports **idempotency keys** to prevent duplicate logical job
creation.

```text
Application
     │
     │ idempotencyKey
     ▼
  JobRail
     │
     ├── Existing job ──► Return existing job
     │
     └── New job ───────► Create job
```

An idempotency key allows a client to safely retry a request without
accidentally creating the same logical job multiple times.

---

## Project Structure

```text
jobrail/
├── Cargo.toml
│
├── crates/
│   ├── core/
│   │   └── Job models and domain logic
│   │
│   ├── jobrail-redis/
│   │   └── Redis storage and queue operations
│   │
│   ├── worker/
│   │   └── Background workers and schedulers
│   │
│   ├── axum-api/
│   │   └── HTTP API
│   │
│   └── cli/
│       └── CLI utilities
│
├── examples/
│   └── API examples
│
└── README.md
```

---

# Getting Started

## Requirements

Before running JobRail locally, make sure you have:

- [Rust](https://www.rust-lang.org/)
- Cargo
- Redis
- Docker (optional)

---

## Start Redis

### Run Redis locally

```bash
redis-server
```

### Or run Redis with Docker

```bash
docker run --name jobrail-redis \
  -p 6379:6379 \
  -d redis
```

Verify that Redis is running:

```bash
redis-cli ping
```

Expected output:

```text
PONG
```

---

# Run JobRail

## Run the API

From the JobRail repository:

```bash
cargo run -p axum-api
```

The API runs at:

```text
http://localhost:3001
```

---

## Run the Worker

In another terminal:

```bash
cargo run -p jobrail-worker
```

The worker connects to Redis and waits for jobs to execute.

---

## Run the Scheduled Job Scheduler

For scheduled jobs:

```bash
cargo run -p jobrail-worker --bin scheduled_scheduler
```

---

## Run the Repeatable Job Scheduler

For repeatable jobs:

```bash
cargo run -p jobrail-worker --bin repeatable_scheduler
```

---

## Local Development Setup

A typical local development setup uses five terminals:

```text
Terminal 1
──────────
Redis

Terminal 2
──────────
Axum API

Terminal 3
──────────
Worker

Terminal 4
──────────
Scheduled Scheduler

Terminal 5
──────────
Repeatable Scheduler
```

---

# API

The current API is available under:

```text
/v1
```

## Jobs

Method Endpoint Description

---

`POST` `/v1/jobs` Create a job
`GET` `/v1/jobs` List jobs
`GET` `/v1/jobs/{id}` Get a job
`POST` `/v1/jobs/{id}/cancel` Cancel a job
`POST` `/v1/jobs/{id}/retry` Retry a failed job
`GET` `/v1/jobs/{id}/attempts` Get job attempts

## Repeatable Jobs

---

Method Endpoint Description

---

`POST` `/v1/repeatable-jobs` Create a repeatable job

`GET` `/v1/repeatable-jobs` List repeatable jobs

`GET` `/v1/repeatable-jobs/{id}` Get a repeatable job

`DELETE` `/v1/repeatable-jobs/{id}` Delete a repeatable job

`POST` `/v1/repeatable-jobs/{id}/disable` Disable a repeatable
job

---

---

# Create a Job

```bash
curl -X POST http://localhost:3001/v1/jobs \
  -H "Content-Type: application/json" \
  -d '{
    "name": "send-email",
    "payload": {
      "to": "user@example.com"
    }
  }'
```

### Example Response

```json
{
  "id": "job-id",
  "name": "send-email",
  "state": "Waiting",
  "attemptsMade": 0,
  "attemptsStarted": 0,
  "runAt": null
}
```

---

# Delayed Job

A delayed job starts after a specified delay.

```bash
curl -X POST http://localhost:3001/v1/jobs \
  -H "Content-Type: application/json" \
  -d '{
    "name": "send-reminder",
    "payload": {
      "userId": "123"
    },
    "delayMs": 10000
  }'
```

The job will become available for execution after `10,000` milliseconds.

---

# Scheduled Job

A scheduled job runs at a specific timestamp.

```bash
curl -X POST http://localhost:3001/v1/jobs \
  -H "Content-Type: application/json" \
  -d '{
    "name": "generate-report",
    "payload": {
      "reportId": "123"
    },
    "runAt": 1800000000000
  }'
```

`runAt` is a Unix timestamp in milliseconds and must be in the future.

---

# Repeatable Job

Repeatable jobs execute according to a configured schedule.

```bash
curl -X POST http://localhost:3001/v1/repeatable-jobs \
  -H "Content-Type: application/json" \
  -d '{
    "name": "cleanup",
    "payload": {
      "type": "cleanup"
    },
    "schedule": {
      "EveryMillis": 60000
    }
  }'
```

The example above runs the job every `60,000` milliseconds.

---

# TypeScript SDK

JobRail also provides a separate TypeScript SDK:

```text
jobrail-js
```

Install it with:

```bash
npm install jobrail
```

## Create a Client

```typescript
import { JobRail } from "jobrail";

const client = new JobRail({
  baseUrl: "http://localhost:3001",
});
```

## Create a Job

```typescript
const job = await client.jobs.create({
  name: "send-email",
  payload: {
    to: "user@example.com",
  },
});
```

The SDK communicates with the JobRail HTTP API.

```text
Application
     │
     │ jobrail-js
     ▼
┌─────────────────┐
│    Axum API     │
└────────┬────────┘
         │
         ▼
       Redis
         │
         ▼
      Worker
```

---

# Reliability Model

JobRail is designed around **at-least-once job delivery**.

A worker claims a job using a lease and ownership token.

```text
Waiting
   │
   │ claim
   ▼
 Active
   │
   ├──── success ────► Completed
   │
   └──── failure ────► Retry / Failed
```

If a worker crashes while processing a job, its lease can expire.

The recovery process can then make the job available for another worker.

This means job handlers should be designed to safely handle repeated
execution when necessary.

---

# Leases and Worker Ownership

A lease gives a worker temporary ownership of a job.

```text
Worker A
   │
   │ claims job
   ▼
Job
   │
   │ lease expires
   ▼
Recovery
   │
   ▼
Worker B
```

JobRail also uses an ownership token to protect against stale workers.

For example:

```text
Worker A
token = ABC

Worker B
token = XYZ
```

If Worker A's lease expires and Worker B claims the job, the current
token becomes `XYZ`.

Worker A can no longer modify the job using the old token:

```text
ABC != XYZ

→ Worker A is stale
→ Operation is rejected
```

This provides **stale-worker protection**.

---

# Redis and Atomic Operations

Redis stores queue membership and job metadata.

JobRail uses **Lua scripts** for operations that require multiple Redis
commands to behave atomically.

For example, claiming a job may involve:

```text
Claim Job
   │
   ├── Verify job is available
   │
   ├── Move job to active
   │
   ├── Create ownership lease
   │
   └── Update job state
```

These operations need to happen as a single atomic operation so multiple
workers cannot incorrectly claim the same job.

Conceptually:

```text
                Redis
                  │
                  ▼
            Lua Script
                  │
       ┌──────────┼──────────┐
       │          │          │
    Verify      Move       Update
    Job         Job        State
       │          │          │
       └──────────┼──────────┘
                  │
                  ▼
               Atomic
```

---

# Development

## Format the Workspace

```bash
cargo fmt --all
```

## Check the Workspace

```bash
cargo check --workspace
```

## Run All Tests

```bash
cargo test --workspace
```

## Check Formatting

```bash
cargo fmt --all -- --check
```

---

# Testing

JobRail contains tests covering:

- Queue behavior
- Scheduling
- Retries
- Attempts
- Repeatable jobs
- API behavior

Run all tests:

```bash
cargo test --workspace
```

Run the API integration tests:

```bash
cargo test -p axum-api --test api
```

---

# Current Status

JobRail is currently an **open-source project under active
development**.

## Currently Implemented

- Redis-backed queues
- Workers
- Worker concurrency
- Job lifecycle
- Retries
- Exponential backoff
- Delayed jobs
- Scheduled jobs
- Repeatable jobs
- Leases
- Lease renewal
- Expired-job recovery
- Attempt history
- Idempotency
- Job cancellation
- Job retry
- Job listing
- Cursor pagination
- State filtering
- Axum API
- TypeScript SDK
- API integration tests

> **Deployment:** JobRail has not been deployed yet and currently runs
> locally / self-hosted.

---

# Planned

The following features are planned for later:

- Production deployment
- Authentication
- API keys
- Organizations
- Projects
- Usage metering
- Billing
- Cloud dashboard
- Managed infrastructure
- Autoscaling
- High availability
- JobRail Cloud control plane

---

# Roadmap

## Public OSS

Feature Status

---

- Core queue engine ✅
- Redis storage ✅
- Workers ✅
- Retries ✅
- Scheduling ✅
- Repeatable jobs ✅
- Leases & recovery ✅
- Attempts ✅
- Axum API ✅
- API integration tests ✅
- TypeScript SDK ✅
- Documentation 🚧
- Examples 🚧
- CI ⏳
- Production deployment ⏳

## JobRail Cloud

Feature Status

---

- Authentication ⏳
- Organizations ⏳
- Projects ⏳
- API keys ⏳
- Usage metering ⏳
- Billing ⏳
- Cloud dashboard ⏳
- Managed infrastructure ⏳
- Autoscaling ⏳
- High availability ⏳

---

# License

JobRail is released under the [MIT License](LICENSE).

---

# Quick Checks

From the `jobrail` repository:

```bash
cargo fmt --all
cargo check --workspace
cargo test --workspace
cargo fmt --all -- --check
```
