package io.pipelines.runtime;

import com.codahale.metrics.MetricRegistry;
import io.pipelines.budget.SimpleBudgetManager;
import io.pipelines.core.BatchSink;
import io.pipelines.core.Record;
import io.pipelines.core.Sink;
import io.pipelines.core.Source;
import io.pipelines.retry.ExponentialBackoffRetryPolicy;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class SinkBatchingTest {
    static class ListSource implements Source<Integer> {
        private final List<Integer> data;
        private int idx = 0;
        ListSource(List<Integer> data) { this.data = data; }
        @Override public Optional<Record<Integer>> poll() {
            if (idx >= data.size()) return Optional.empty();
            return Optional.of(new Record<>(idx, 0, data.get(idx++)));
        }
        @Override public boolean isFinished() { return idx >= data.size(); }
    }

    static class BatchCountingSink implements Sink<Integer>, BatchSink<Integer> {
        final List<Integer> batchSizes = new ArrayList<>();
        @Override public void accept(Record<Integer> record) {}
        @Override public void acceptBatch(List<Record<Integer>> records) { batchSizes.add(records.size()); }
    }

    @Test
    void batches_are_grouped_by_configured_size() throws Exception {
        var src = new ListSource(java.util.List.of(1,2,3,4,5));
        var sink = new BatchCountingSink();
        var budget = new SimpleBudgetManager(2, 1024*1024, 0, 0, null);
        var retry = new ExponentialBackoffRetryPolicy(2, 1, 5);
        var pipeline = new PipelineBuilder<Integer, Integer>()
                .source(src)
                .transform(r -> java.util.List.of(new Record<>(r.seq(), 0, r.payload())))
                .sink(sink)
                .budget(budget)
                .retry(retry)
                .workers(2)
                .queueCapacity(8)
                .sinkBatchSize(3)
                .maxInFlight(4)
                .metrics(new MetricRegistry())
                .build();
        pipeline.start();
        int tries=0; while(!src.isFinished() && tries++<200){ Thread.sleep(10);} Thread.sleep(200);
        pipeline.close();
        // Expect [3,2]
        assertEquals(java.util.List.of(3,2), sink.batchSizes);
    }
}
