# Pipelines

A lightweight, single-JVM Java 21 data-processing pipeline framework focused on:
- Deterministic ordering end-to-end (seq/subSeq) even with parallel transforms
- Backpressure propagation across stages and across pipelines
- Resource budgeting (CPU/memory/IO/external QPS)
- Robust error handling with dead-letter sinks
- First-class metrics and a simple admin UI + gRPC API

What’s built in this repo
- Core model and runtime
  - `Record<T>` with deterministic `seq` and `subSeq` ordering
  - Interfaces: `Source<T>`, `Transform<I,O>`, `Sink<T>` and optional `BatchSink<T>`
  - Runtime `Pipeline<I,O>` with:
    - Source thread, transform worker pool, single ordered sink thread
    - Bounded in-flight submissions (`maxInFlight`) for backpressure
    - Batching to sinks with size- and time-based flush (`sinkBatchSize`, `sinkFlushEveryMillis`)
    - Per-stage timers/meters, percent-time breakdown, error metrics
    - DLQ hooks for transform/sink failures
  - Builder `PipelineBuilder` for configuration

- Sources
  - `FileBytesSource`: emits one record per file
  - `FileChunkSource`: streams files in fixed-size chunks to avoid full-file reads
  - `QueueSource<T>`: bounded, blocking queue source for multi-pipeline fan-out; exposes depth gauge and timed offer helpers

- Transforms
  - `TransformChain`: sequential composition of multiple transforms
  - `RouterTransform`: route per record to a chain or publish to a `QueueSource`
  - `JsonFieldSelector`: light JSON string-field based routing (e.g., by `type`)
  - `HttpExternalTransform`: calls external HTTP services with QPS budgeting
  - `LengthPrefixedReframerTransform`: reframes length-prefixed byte streams, carrying partial frames across invocations

- Sinks
  - `FileBytesSink`: file-based sink; implements `BatchSink<byte[]>`
  - `JdbcBytesBatchSink`: simple JDBC batch insert example (seq, subseq, payload)

- Budgeting
  - `SimpleBudgetManager`: CPU slots, memory permits, IO bytes/sec token bucket, external QPS token bucket

- Error handling
  - `DeadLetterSink<T>` and `FileDeadLetterSink<T>` (JSONL) for transform/sink failures

- Metrics and Admin
  - Codahale Metrics registry across the runtime
  - gRPC admin service (`PipelineAdminServer`) exposes:
    - `GetStatus`: running/paused/queue size/timed flushes
    - `GetMetrics`: stage timers/meters, sink batch metrics (flush count, avg size, p50/p95/p99), route stats with 1m rates, queue depths
    - `Control`: pause/resume/stop
    - `GetErrors`: recent DLQ entries
  - HTTP admin bridge + UI (`AdminServer` and `/public/index.html`):
    - Status, metrics summary, route stats table, queue depth table, error panel, OpenAPI link

- Multi-pipeline topology
  - Route to `QueueSource` for a second pipeline
  - Backpressure propagates from Pipeline 2 → Pipeline 1 via bounded queue and `maxInFlight`

- Demo
  - `DemoIngestorMain`: File → Router by JSON field `type` → transform chain (uppercase + suffix + optional HTTP) OR publish to queue; second pipeline consumes queue; admin HTTP + gRPC running

- Testing
  - JUnit5 tests covering: ordering, retries, batching, backpressure propagation, chunked file source, reframing transform, router, JSON selector, JDBC batch, and end-to-end flows

Design notes
- Deterministic ordering maintained by seq/subSeq and ordered sink emission per input batch
- Backpressure controls include bounded queues, `maxInFlight`, and blocking publish to queues; time-based flush bounds latency for low traffic
- Admin/gRPC-first makes observability a core feature; the HTTP bridge powers a simple diagnostics UI

## Java Context

When you are asked to work on Java project, you will:
- Use gradlew to bootstrap
- Use lateset GA java jdk features
- Prefer to use as less third party framework as possible
- Use Guice for dependency injection
- Use junit5 for unit testing without assert4j
- Prefer blackbox testing with least amuont of mocking
- Always lint and checkstyle for codes
- Target unit test coverage at 85% and no lower than 80%

When designing java projects:
- Design interface use GRPC API
- Adding grpc to rest bridge and setup Openapi with swagger
- Always build simple web page for admin, diagnostic and debugging

Always consider the following observability requirments:
- Have comprehensive logging and metrics on user requests/responses
- A single failure is not toleratable and needs to be trackable for debugging purpose
- Serivce shall be expected to be used by bad behaving end users and shall be easy to identify bad user and be protected against them

When connecting to an external resource like database or service:
- Always emit metrics on usage and able to throttle and have proper retry
- Always have mocks for testing. Mocks shall be as close to transport layer as possible so as much code can be tested
- Always have comprehensive logging when failure happens so when contacting external team you can provide enough information
