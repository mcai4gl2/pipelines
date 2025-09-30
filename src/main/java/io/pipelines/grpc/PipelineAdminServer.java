package io.pipelines.grpc;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;
import io.grpc.protobuf.services.ProtoReflectionService;
import io.pipelines.runtime.Pipeline;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

public class PipelineAdminServer implements AutoCloseable {
    private final Server server;
    private final boolean ready = true;

    public PipelineAdminServer(int port, Pipeline<?, ?> pipeline, MetricRegistry registry, Path dlqTransform, Path dlqSink) {
        this.server = ServerBuilder.forPort(port)
                .addService(new ServiceImpl(pipeline, registry, dlqTransform, dlqSink))
                .addService(ProtoReflectionService.newInstance())
                .build();
    }

    public void start() throws IOException { server.start(); }

    @Override
    public void close() { server.shutdownNow(); }

    private static class ServiceImpl extends PipelineAdminGrpc.PipelineAdminImplBase {
        private final Pipeline<?, ?> pipeline;
        private final MetricRegistry registry;
        private final Path dlqTransform;
        private final Path dlqSink;
        ServiceImpl(Pipeline<?, ?> pipeline, MetricRegistry registry, Path dlqTransform, Path dlqSink) {
            this.pipeline = pipeline; this.registry = registry; this.dlqTransform = dlqTransform; this.dlqSink = dlqSink;
        }

        @Override
        public void getStatus(Empty request, StreamObserver<Status> responseObserver) {
            long timed = registry.counter("pipeline.sink.batch.flushes.timed").getCount();
            Status resp = Status.newBuilder()
                    .setRunning(pipeline.isRunning())
                    .setPaused(pipeline.isPaused())
                    .setQueueSize(pipeline.getQueueSize())
                    .setTimedFlushes(timed)
                    .build();
            responseObserver.onNext(resp);
            responseObserver.onCompleted();
        }

        @Override
        public void getMetrics(Empty request, StreamObserver<MetricsSummary> responseObserver) {
            Timer src = registry.timer("pipeline.source.time");
            Timer xform = registry.timer("pipeline.transform.time");
            Timer sink = registry.timer("pipeline.sink.time");
            long flushes = registry.counter("pipeline.sink.batch.flushes").getCount();
            long recs = registry.counter("pipeline.sink.batch.records").getCount();
            Timer flushTimer = registry.timer("pipeline.sink.batch.flush.timer");
            com.codahale.metrics.Snapshot ts = flushTimer.getSnapshot();
            com.codahale.metrics.Histogram sizeHist = registry.histogram("pipeline.sink.batch.size");
            com.codahale.metrics.Snapshot ss = sizeHist.getSnapshot();
            java.util.List<RouteStat> routeStats = routeStats();
            java.util.List<QueueDepth> queueDepths = queueDepths();
            MetricsSummary resp = MetricsSummary.newBuilder()
                    .setSourceCount(src.getCount())
                    .setTransformCount(xform.getCount())
                    .setSinkCount(sink.getCount())
                    .setSourceMeanMs(toMillis(src))
                    .setTransformMeanMs(toMillis(xform))
                    .setSinkMeanMs(toMillis(sink))
                    .setSourcePct(pipeline.sourcePct())
                    .setTransformPct(pipeline.transformPct())
                    .setSinkPct(pipeline.sinkPct())
                    .setSinkBatchFlushes(flushes)
                    .setSinkBatchAvgSize(flushes == 0 ? 0.0 : ((double) recs) / flushes)
                    .setSinkBatchFlushP50Ms(nsToMs(ts.getMedian()))
                    .setSinkBatchFlushP95Ms(nsToMs(ts.get95thPercentile()))
                    .setSinkBatchFlushP99Ms(nsToMs(ts.get99thPercentile()))
                    .setSinkBatchSizeP50(ss.getMedian())
                    .setSinkBatchSizeP95(ss.get95thPercentile())
                    .setSinkBatchSizeP99(ss.get99thPercentile())
                    .addAllRouteStats(routeStats)
                    .addAllQueueDepths(queueDepths)
                    .build();
            responseObserver.onNext(resp);
            responseObserver.onCompleted();
        }

        private double nsToMs(double ns) { return ns / 1_000_000.0; }

        private java.util.List<RouteStat> routeStats() {
            java.util.Map<String, RouteStat.Builder> map = new java.util.HashMap<>();
            for (var e : registry.getCounters().entrySet()) {
                String name = e.getKey();
                if (!name.startsWith("pipeline.route.")) continue;
                String rest = name.substring("pipeline.route.".length());
                int dot = rest.lastIndexOf('.');
                if (dot <= 0) continue;
                String key = rest.substring(0, dot);
                String kind = rest.substring(dot + 1);
                RouteStat.Builder b = map.computeIfAbsent(key, k -> RouteStat.newBuilder().setKey(k));
                long val = e.getValue().getCount();
                switch (kind) {
                    case "transform" -> b.setRoutedToTransform(val);
                    case "queue" -> b.setRoutedToQueue(val);
                    case "drop" -> b.setDropped(val);
                }
            }
            // fill rates from meters and timers
            for (var entry : map.entrySet()) {
                String key = entry.getKey();
                RouteStat.Builder b = entry.getValue();
                var t = registry.getTimers().get("pipeline.route." + key + ".transform.timer");
                if (t != null) b.setTransformRate1M(t.getOneMinuteRate());
                var q = registry.getMeters().get("pipeline.route." + key + ".queue.rate");
                if (q != null) b.setQueueRate1M(q.getOneMinuteRate());
                var d = registry.getMeters().get("pipeline.route." + key + ".drop.rate");
                if (d != null) b.setDropRate1M(d.getOneMinuteRate());
            }
            java.util.ArrayList<RouteStat> list = new java.util.ArrayList<>();
            for (var b : map.values()) list.add(b.build());
            return list;
        }

        private java.util.List<QueueDepth> queueDepths() {
            java.util.ArrayList<QueueDepth> list = new java.util.ArrayList<>();
            for (var e : registry.getGauges().entrySet()) {
                String name = e.getKey();
                if (!name.startsWith("queuesource.") || !name.endsWith(".depth")) continue;
                Object val = e.getValue().getValue();
                int depth = 0;
                if (val instanceof Number n) depth = n.intValue();
                String key = name.substring("queuesource.".length(), name.length() - ".depth".length());
                list.add(QueueDepth.newBuilder().setName(key).setDepth(depth).build());
            }
            return list;
        }

        @Override
        public void control(ControlRequest request, StreamObserver<ControlResponse> responseObserver) {
            String action = request.getAction();
            if ("pause".equalsIgnoreCase(action)) pipeline.pause();
            else if ("resume".equalsIgnoreCase(action)) pipeline.resume();
            else if ("stop".equalsIgnoreCase(action)) pipeline.stop();
            responseObserver.onNext(ControlResponse.newBuilder().build());
            responseObserver.onCompleted();
        }

        @Override
        public void getErrors(ErrorsRequest request, StreamObserver<Errors> responseObserver) {
            int limit = request.getLimit();
            if (limit <= 0) limit = 10;
            Errors.Builder b = Errors.newBuilder();
            try {
                tail(dlqTransform, limit).forEach(b::addTransform);
                tail(dlqSink, limit).forEach(b::addSink);
            } catch (IOException ignored) {}
            responseObserver.onNext(b.build());
            responseObserver.onCompleted();
        }

        @Override
        public void health(Empty request, StreamObserver<HealthStatus> responseObserver) {
            HealthStatus hs = HealthStatus.newBuilder()
                    .setReady(true)
                    .setRunning(pipeline.isRunning())
                    .build();
            responseObserver.onNext(hs);
            responseObserver.onCompleted();
        }

        private static List<String> tail(Path file, int n) throws IOException {
            if (file == null || !Files.exists(file)) return java.util.List.of();
            List<String> lines = Files.readAllLines(file, StandardCharsets.UTF_8);
            int from = Math.max(0, lines.size() - n);
            return lines.subList(from, lines.size());
        }

        private static long toMillis(Timer t) {
            double rate = t.getMeanRate();
            if (t.getCount() == 0 || rate <= 0) return 0;
            return (long) (1000.0 / rate);
        }
    }
}
