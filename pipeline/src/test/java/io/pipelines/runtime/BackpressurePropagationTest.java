package io.pipelines.runtime;

import com.codahale.metrics.MetricRegistry;
import io.pipelines.budget.SimpleBudgetManager;
import io.pipelines.core.Record;
import io.pipelines.core.Source;
import io.pipelines.retry.ExponentialBackoffRetryPolicy;
import io.pipelines.source.QueueSource;
import io.pipelines.transform.RouterTransform;
import io.pipelines.transform.Selector;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertTrue;

public class BackpressurePropagationTest {
    static class FastSource implements Source<byte[]> {
        int i=0; int max;
        FastSource(int max) { this.max = max; }
        @Override public Optional<Record<byte[]>> poll() {
            if (i>=max) return Optional.empty();
            return Optional.of(new Record<>(i++, 0, ("x"+i).getBytes()));
        }
        @Override public boolean isFinished() { return i>=max; }
    }

    @Test
    void queue_backpressure_limits_inflight() throws Exception {
        var src = new FastSource(1000);
        var q = new QueueSource<byte[]>(2); // tiny capacity to force blocking
        Selector<byte[]> sel = r -> "q";
        var router = new RouterTransform<byte[], byte[]>(sel).publish("q", q);
        var sink = (io.pipelines.core.Sink<byte[]>) r -> {};

        var p1 = new PipelineBuilder<byte[], byte[]>()
                .source(src)
                .transform(router)
                .sink(sink)
                .budget(new SimpleBudgetManager(2, 1024*1024, 0, 0, null))
                .retry(new ExponentialBackoffRetryPolicy(2, 1, 10))
                .workers(2)
                .queueCapacity(8)
                .maxInFlight(4)
                .metrics(new MetricRegistry())
                .build();
        p1.start();
        Thread.sleep(200); // allow some processing
        assertTrue(p1.getInflight() <= 4, "inflight should be bounded by maxInFlight");
        p1.close();
    }
}

