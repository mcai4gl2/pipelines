package io.pipelines.ingestor;

import com.codahale.metrics.MetricRegistry;
import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import io.pipelines.budget.Budget;
import io.pipelines.budget.SimpleBudgetManager;
import io.pipelines.config.PipelineConfig;
import io.pipelines.core.Sink;
import io.pipelines.core.Source;
import io.pipelines.core.Transform;
import io.pipelines.runtime.Pipeline;
import io.pipelines.runtime.PipelineBuilder;
import io.pipelines.sink.FileBytesSink;
import io.pipelines.source.FileBytesSource;
import io.pipelines.transform.IdentityBytesTransform;
import io.pipelines.retry.ExponentialBackoffRetryPolicy;
import io.pipelines.error.FileDeadLetterSink;
import io.pipelines.error.DeadLetterSink;

import java.io.IOException;

public class PipelineModule extends AbstractModule {
    private final PipelineConfig config;

    public PipelineModule(PipelineConfig config) { this.config = config; }

    @Override
    protected void configure() {
        bind(PipelineConfig.class).toInstance(config);
    }

    @Provides @Singleton MetricRegistry metricRegistry() { return new MetricRegistry(); }

    @Provides @Singleton Budget budget() { return new SimpleBudgetManager(config.cpuThreads(), config.memoryBytes(), config.ioBytesPerSec(), config.externalQps(), null); }

    @Provides Source<byte[]> source() throws IOException { return new FileBytesSource(config.inputDir()); }

    @Provides Transform<byte[], byte[]> transform() { return new IdentityBytesTransform(); }

    @Provides Sink<byte[]> sink() throws IOException { return new FileBytesSink(config.outputDir()); }

    @Provides @Singleton DeadLetterSink<byte[]> dlqIn() throws java.io.IOException { return new FileDeadLetterSink<>(config.outputDir().resolve("dlq_transform.jsonl")); }
    @Provides @Singleton DeadLetterSink<byte[]> dlqOut() throws java.io.IOException { return new FileDeadLetterSink<>(config.outputDir().resolve("dlq_sink.jsonl")); }

    @Provides @Singleton Pipeline<byte[], byte[]> pipeline(Source<byte[]> src, Transform<byte[], byte[]> tf, Sink<byte[]> sk, Budget budget, MetricRegistry registry, DeadLetterSink<byte[]> dlqIn, DeadLetterSink<byte[]> dlqOut) {
        return new PipelineBuilder<byte[], byte[]>()
                .source(src)
                .transform(tf)
                .sink(sk)
                .budget(budget)
                .retry(new ExponentialBackoffRetryPolicy(3, 10, 100))
                .workers(config.workers())
                .queueCapacity(config.queueCapacity())
                .metrics(registry)
                .deadLetterIn(dlqIn)
                .deadLetterOut(dlqOut)
                .build();
    }
}
