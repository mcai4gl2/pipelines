package io.pipelines.transform;

import com.codahale.metrics.MetricRegistry;
import io.pipelines.core.Record;
import io.pipelines.core.Transform;
import io.pipelines.source.QueueSource;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Routes records to a transform chain or publishes to a queue for another pipeline.
 */
public class RouterTransform<I, O> implements Transform<I, O> {
    private final Selector<I> selector;
    private final Map<String, Transform<I, O>> routes = new HashMap<>();
    private final Map<String, QueueSource<O>> queues = new HashMap<>();
    private Transform<I, O> defaultRoute;
    private MetricRegistry metrics;

    public RouterTransform(Selector<I> selector) {
        this.selector = selector;
    }

    public RouterTransform<I, O> route(String key, Transform<I, O> transform) {
        routes.put(key, transform); return this;
    }
    public RouterTransform<I, O> publish(String key, QueueSource<O> queue) {
        queues.put(key, queue); return this;
    }
    public RouterTransform<I, O> defaultRoute(Transform<I, O> transform) { this.defaultRoute = transform; return this; }
    public RouterTransform<I, O> withMetrics(MetricRegistry registry) { this.metrics = registry; return this; }

    @Override
    public List<Record<O>> apply(Record<I> input) throws Exception {
        String key = selector.route(input);
        Transform<I, O> t = routes.getOrDefault(key, defaultRoute);
        if (t != null) {
            if (metrics != null) metrics.counter("pipeline.route." + key + ".transform").inc();
            if (metrics != null) {
                com.codahale.metrics.Timer.Context ctx = metrics.timer("pipeline.route." + key + ".transform.timer").time();
                try {
                    return t.apply(input);
                } finally {
                    ctx.stop();
                }
            }
            return t.apply(input);
        }
        QueueSource<O> q = queues.get(key);
        if (q != null) {
            q.offerPayload((O) input.payload());
            if (metrics != null) {
                metrics.counter("pipeline.route." + key + ".queue").inc();
                metrics.meter("pipeline.route." + key + ".queue.rate").mark();
            }
            return List.of();
        }
        if (metrics != null) {
            metrics.counter("pipeline.route." + key + ".drop").inc();
            metrics.meter("pipeline.route." + key + ".drop.rate").mark();
        }
        // no route; drop
        return List.of();
    }
}
