package io.pipelines.ingestor;

import com.codahale.metrics.MetricRegistry;
import io.pipelines.admin.AdminServer;
import io.pipelines.budget.Budget;
import io.pipelines.budget.SimpleBudgetManager;
import io.pipelines.config.PipelineConfig;
import io.pipelines.core.Sink;
import io.pipelines.core.Source;
import io.pipelines.core.Transform;
import io.pipelines.grpc.PipelineAdminServer;
import io.pipelines.retry.ExponentialBackoffRetryPolicy;
import io.pipelines.runtime.Pipeline;
import io.pipelines.runtime.PipelineBuilder;
import io.pipelines.sink.FileBytesSink;
import io.pipelines.source.FileBytesSource;
import io.pipelines.source.QueueSource;
import io.pipelines.transform.HttpExternalTransform;
import io.pipelines.transform.RouterTransform;
import io.pipelines.transform.Selector;
import io.pipelines.transform.TransformChain;

import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;

public class DemoIngestorMain {
    public static void main(String[] args) throws Exception {
        PipelineConfig cfg = PipelineConfig.fromEnv();
        Path in = cfg.inputDir();
        Path out = cfg.outputDir();
        Files.createDirectories(out);

        MetricRegistry registry = new MetricRegistry();
        Budget budget = new SimpleBudgetManager(cfg.cpuThreads(), cfg.memoryBytes(), cfg.ioBytesPerSec(), cfg.externalQps(), null);

        Source<byte[]> source = new FileBytesSource(in);
        Sink<byte[]> sink = new FileBytesSink(out);

        // Transform chain: uppercase then append suffix; optional external call if PIPELINES_EXT_URL set
        Transform<byte[], byte[]> toUpper = r -> java.util.List.of(new io.pipelines.core.Record<>(r.seq(), 0, new String(r.payload()).toUpperCase().getBytes()));
        Transform<byte[], byte[]> addSuffix = r -> java.util.List.of(new io.pipelines.core.Record<>(r.seq(), 0, (new String(r.payload()) + "_CHAIN").getBytes()));
        String ext = System.getenv("PIPELINES_EXT_URL");
        TransformChain<byte[], byte[]> chain;
        if (ext != null && !ext.isBlank()) {
            var http = new HttpExternalTransform(URI.create(ext), budget, Duration.ofSeconds(5));
            chain = new TransformChain<>(toUpper, addSuffix, http);
        } else {
            chain = new TransformChain<>(toUpper, addSuffix);
        }

        // Router: route by JSON field `type`: if equals 'alpha' -> chain, else publish to queue
        QueueSource<byte[]> queue = new QueueSource<>(1024);
        queue.registerMetrics(registry, "demo.queue");
        var jsonSel = new io.pipelines.transform.JsonFieldSelector("type");
        Selector<byte[]> selector = r -> {
            String t = jsonSel.route(r);
            return (t != null && t.equalsIgnoreCase("alpha")) ? "chain" : "queue";
        };
        RouterTransform<byte[], byte[]> router = new RouterTransform<byte[], byte[]>(selector)
                .route("chain", chain)
                .publish("queue", queue)
                .withMetrics(registry);

        var retry = new ExponentialBackoffRetryPolicy(3, 10, 100);

        Pipeline<byte[], byte[]> p1 = new PipelineBuilder<byte[], byte[]>()
                .source(source)
                .transform(router)
                .sink(sink)
                .budget(budget)
                .retry(retry)
                .workers(cfg.workers())
                .queueCapacity(cfg.queueCapacity())
                .sinkBatchSize(8)
                .sinkFlushEveryMillis(100)
                .metrics(registry)
                .build();

        // Second pipeline consumes the queue and writes to out/queue
        Path out2 = out.resolve("queue");
        Files.createDirectories(out2);
        Pipeline<byte[], byte[]> p2 = new PipelineBuilder<byte[], byte[]>()
                .source(queue)
                .transform(r -> java.util.List.of(new io.pipelines.core.Record<>(r.seq(), 0, r.payload())))
                .sink(new FileBytesSink(out2))
                .budget(budget)
                .retry(retry)
                .workers(Math.max(1, cfg.workers() / 2))
                .queueCapacity(Math.max(16, cfg.queueCapacity() / 2))
                .sinkBatchSize(8)
                .sinkFlushEveryMillis(100)
                .metrics(new MetricRegistry())
                .build();

        try (AdminServer admin = new AdminServer(cfg.adminPort(), cfg.adminPort() + 1,
                out.resolve("dlq_transform.jsonl"), out.resolve("dlq_sink.jsonl"));
             PipelineAdminServer grpc = new PipelineAdminServer(cfg.adminPort() + 1, p1, registry,
                     out.resolve("dlq_transform.jsonl"), out.resolve("dlq_sink.jsonl"))) {
            p1.start();
            p2.start();
            admin.start();
            grpc.start();
            Runtime.getRuntime().addShutdownHook(new Thread(() -> { p1.close(); p2.close(); }));
            Thread.currentThread().join();
        }
    }
}
