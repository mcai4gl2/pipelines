package io.pipelines.metrics;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;

public class Metrics {
    private final MetricRegistry registry;

    public Metrics(MetricRegistry registry) {
        this.registry = registry;
    }

    public MetricRegistry registry() { return registry; }

    public Counter counter(String name) { return registry.counter(name); }
    public Meter meter(String name) { return registry.meter(name); }
    public Timer timer(String name) { return registry.timer(name); }
}

