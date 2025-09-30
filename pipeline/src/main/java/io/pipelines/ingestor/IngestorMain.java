package io.pipelines.ingestor;

import com.codahale.metrics.MetricRegistry;
import com.google.inject.Guice;
import com.google.inject.Injector;
import io.pipelines.admin.AdminServer;
import io.pipelines.config.PipelineConfig;
import io.pipelines.runtime.Pipeline;
import io.pipelines.grpc.PipelineAdminServer;

public class IngestorMain {
    public static void main(String[] args) throws Exception {
        PipelineConfig cfg = PipelineConfig.fromEnv();
        Injector injector = Guice.createInjector(new PipelineModule(cfg));
        Pipeline<byte[], byte[]> pipeline = injector.getInstance(Pipeline.class);
        MetricRegistry registry = injector.getInstance(MetricRegistry.class);
        try (AdminServer admin = new AdminServer(cfg.adminPort(), cfg.adminPort() + 1,
                cfg.outputDir().resolve("dlq_transform.jsonl"), cfg.outputDir().resolve("dlq_sink.jsonl"));
             PipelineAdminServer grpc = new PipelineAdminServer(cfg.adminPort() + 1, pipeline, registry,
                     cfg.outputDir().resolve("dlq_transform.jsonl"), cfg.outputDir().resolve("dlq_sink.jsonl"))) {
            pipeline.start();
            admin.start();
            grpc.start();
            // Keep running until stopped
            Runtime.getRuntime().addShutdownHook(new Thread(pipeline::close));
            Thread.currentThread().join();
        }
    }
}
