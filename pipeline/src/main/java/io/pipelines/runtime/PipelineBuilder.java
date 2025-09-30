package io.pipelines.runtime;

import com.codahale.metrics.MetricRegistry;
import io.pipelines.budget.Budget;
import io.pipelines.core.Sink;
import io.pipelines.core.Source;
import io.pipelines.core.Transform;
import io.pipelines.metrics.Metrics;
import io.pipelines.retry.RetryPolicy;
import io.pipelines.error.DeadLetterSink;

import java.util.Objects;

public class PipelineBuilder<I, O> {
    private Source<I> source;
    private Transform<I, O> transform;
    private Sink<O> sink;
    private Budget budget;
    private RetryPolicy retryPolicy;
    private int workers = 4;
    private int queueCapacity = 1024;
    private MetricRegistry metricRegistry = new MetricRegistry();
    private DeadLetterSink<I> dlqIn;
    private DeadLetterSink<O> dlqOut;
    private int sinkBatchSize = 16;
    private int sinkFlushEveryMillis = 100;
    private int maxInFlight = 1024;

    public PipelineBuilder<I, O> source(Source<I> s) { this.source = s; return this; }
    public PipelineBuilder<I, O> transform(Transform<I, O> t) { this.transform = t; return this; }
    public PipelineBuilder<I, O> sink(Sink<O> s) { this.sink = s; return this; }
    public PipelineBuilder<I, O> budget(Budget b) { this.budget = b; return this; }
    public PipelineBuilder<I, O> retry(RetryPolicy r) { this.retryPolicy = r; return this; }
    public PipelineBuilder<I, O> workers(int w) { this.workers = w; return this; }
    public PipelineBuilder<I, O> queueCapacity(int c) { this.queueCapacity = c; return this; }
    public PipelineBuilder<I, O> metrics(MetricRegistry r) { this.metricRegistry = r; return this; }
    public PipelineBuilder<I, O> deadLetterIn(DeadLetterSink<I> d) { this.dlqIn = d; return this; }
    public PipelineBuilder<I, O> deadLetterOut(DeadLetterSink<O> d) { this.dlqOut = d; return this; }
    public PipelineBuilder<I, O> sinkBatchSize(int n) { this.sinkBatchSize = Math.max(1, n); return this; }
    public PipelineBuilder<I, O> sinkFlushEveryMillis(int ms) { this.sinkFlushEveryMillis = Math.max(0, ms); return this; }
    public PipelineBuilder<I, O> maxInFlight(int n) { this.maxInFlight = Math.max(1, n); return this; }

    public Pipeline<I, O> build() {
        Objects.requireNonNull(source, "source");
        Objects.requireNonNull(transform, "transform");
        Objects.requireNonNull(sink, "sink");
        Objects.requireNonNull(budget, "budget");
        Objects.requireNonNull(retryPolicy, "retryPolicy");
        return new Pipeline<>(source, transform, sink, budget, retryPolicy, workers, queueCapacity, new Metrics(metricRegistry), dlqIn, dlqOut, sinkBatchSize, sinkFlushEveryMillis, maxInFlight);
    }
}
