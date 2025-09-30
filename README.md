Pipelines (MVP)

Overview
- Lightweight Java 21 data processing pipeline with Sources, Transforms, and Sinks.
- Focus on deterministic ordering, backpressure, budgeting, metrics, and admin controls.
- Includes gRPC admin API and HTTP bridge with a simple dashboard.

Quick Start
- Build and run the demo with resources on the classpath:
  - `./gradlew run`
  - Default port: `8080` (override with `PIPELINES_PORT` or `-Dpipelines.port`)
- Open admin pages:
  - Admin UI: `http://localhost:8080/`
  - Swagger UI: `http://localhost:8080/swagger`
  - OpenAPI YAML (generated via gRPC reflection): `http://localhost:8080/openapi.yaml`
  - Status JSON: `http://localhost:8080/status`
  - Metrics JSON: `http://localhost:8080/metrics`
  - REST→gRPC (POST JSON): `http://localhost:8080/grpc/{Service}/{Method}`

Run from IntelliJ
- Reimport as Gradle project so `src/main/resources` is on the run classpath.
- Use the Gradle task “run” or run `DemoIngestorMain` with the module classpath that includes resources.
- If `/` shows “No admin page”, run via `./gradlew run` to include resources.

Admin HTML pages
- Dashboard (`/`): Status, metrics summary, sink batch metrics, route stats, queue depths, recent errors.
- Swagger (`/swagger`): Interact with gRPC methods via the REST bridge using `/openapi.yaml`.
- REST→gRPC bridge (unary): POST JSON to `/grpc/{Service}/{Method}` and receive JSON response.

Examples
- Get status: `curl -s -X POST http://localhost:8080/grpc/PipelineAdmin/GetStatus -H 'Content-Type: application/json' -d '{}'`
- Get metrics: `curl -s -X POST http://localhost:8080/grpc/PipelineAdmin/GetMetrics -H 'Content-Type: application/json' -d '{}'`
- Pause: `curl -s -X POST http://localhost:8080/grpc/PipelineAdmin/Control -H 'Content-Type: application/json' -d '{"action":"pause"}'`

Key Concepts
- Source: produces Records with seq/subSeq for deterministic order. Examples: FileBytesSource, FileChunkSource, QueueSource.
- Transform: converts input Record to zero or more outputs while preserving seq. Examples: Identity, TransformChain, RouterTransform, HttpExternalTransform, LengthPrefixedReframerTransform.
- Sink: consumes records (supports batching via BatchSink). Examples: FileBytesSink, JdbcBytesBatchSink (example).
- Budget: SimpleBudgetManager controls CPU, memory, IO, external QPS.
  - External QPS supports non-blocking acquire via `Budget.acquireExternalOpAsync()` used by async transforms (no thread blocking).
  - Per-route QPS: For demo route `alpha`, set `PIPELINES_ROUTE_QPS_ALPHA` (or `-Dpipelines.route.qps.alpha`) to override route-specific QPS; grants are counted in metric `pipeline.route.alpha.qps.grants`.
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
- If `PIPELINES_EXT_URL` is set, the chain uses an ASYNC HTTP transform (non-blocking) and leverages `Budget.acquireExternalOpAsync()` for QPS limiting (no blocked threads).
- Queue depth gauge registered as `queuesource.demo.queue.depth` and rendered in the UI.
 - Outbound HTTP uses an isolated async IO pool; tune threads via `-Dpipelines.iohttp=<n>` (default 8).

Testing
- JUnit5 tests cover flow, ordering, retry, batching, backpressure propagation, chunked file source, reframing transform, and JDBC batch sink.

Troubleshooting
- “No admin page” on `/`: Run via `./gradlew run`, or ensure `src/main/resources` is on the run classpath in your IDE.
- Swagger “duplicated mapping key”: Hard refresh; generator now uses fully-qualified schema names to avoid collisions.
