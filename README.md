Pipelines (MVP)

Overview
- Lightweight Java 21 data processing pipeline with Sources, Transforms, and Sinks.
- Focus on deterministic ordering, backpressure, budgeting, metrics, and admin controls.
- Includes gRPC admin API and HTTP bridge with a simple dashboard.

Key Concepts
- Source: produces Records with seq/subSeq for deterministic order. Examples: FileBytesSource, FileChunkSource, QueueSource.
- Transform: converts input Record to zero or more outputs while preserving seq. Examples: Identity, TransformChain, RouterTransform, HttpExternalTransform, LengthPrefixedReframerTransform.
- Sink: consumes records (supports batching via BatchSink). Examples: FileBytesSink, JdbcBytesBatchSink (example).
- Budget: SimpleBudgetManager controls CPU, memory, IO, external QPS.
- Admin: gRPC service for status/metrics/control, HTTP bridge for web UI.

Backpressure Tuning
- Queue capacities: Use bounded queues (e.g., QueueSource(capacity)) to exert backpressure downstream.
- Max in-flight: PipelineBuilder.maxInFlight(N) caps concurrent submitted items not yet drained to the sink. This allows downstream pressure (e.g., Router publishing to a QueueSource) to throttle upstream reads.
- Workers: Tune transform workers for CPU/I/O mix. Too many can cause contention and queue growth; too few limits throughput.
- Sink batching: PipelineBuilder.sinkBatchSize(B) controls how many records to flush per batch; larger batches reduce IO overhead.
- Time-based flush: PipelineBuilder.sinkFlushEveryMillis(T) ensures partial batches flush after T ms to bound latency for low-traffic scenarios.
- External QPS: SimpleBudgetManager.acquireExternalOp() enforces QPS for external calls; tune externalQps accordingly.

Metrics
- Core timers/meters: source/transform/sink timers, input/output rates, error rates; per-stage time %.
- Sink batch metrics: counters (flushes, records), timers (flush duration), histogram (batch size), timed flushes count.
- Router metrics: per-route counters (transform/queue/drop), meters (1m rates), and transform timers.
- Queue depths: QueueSource.registerMetrics(registry, name) registers a gauge queuesource.<name>.depth.

Admin
- HTTP UI at / shows status (running/paused/queueSize/timedFlushes), metrics summary, route stats, queue depths, and recent errors.
- gRPC service exposes GetStatus, GetMetrics, Control, GetErrors.

Demo
- DemoIngestorMain wires FileBytesSource → RouterTransform(JsonFieldSelector("type")) → chain (uppercase + suffix + optional HTTP) OR publish to QueueSource; second pipeline consumes QueueSource.

Testing
- JUnit5 tests cover flow, ordering, retry, batching, backpressure propagation, chunked file source, reframing transform, and JDBC batch sink.

