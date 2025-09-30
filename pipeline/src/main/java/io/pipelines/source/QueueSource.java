package io.pipelines.source;

import io.pipelines.core.Record;
import io.pipelines.core.Source;

import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

public class QueueSource<T> implements Source<T> {
    private final BlockingQueue<Record<T>> queue;
    private volatile boolean finished = false;
    private final java.util.concurrent.atomic.AtomicLong seq = new java.util.concurrent.atomic.AtomicLong(0);
    private String metricsName;

    public QueueSource(int capacity) {
        this.queue = new LinkedBlockingQueue<>(capacity);
    }

    public void offer(Record<T> r) throws InterruptedException { queue.put(r); }
    public void offerPayload(T payload) throws InterruptedException { queue.put(new Record<>(seq.getAndIncrement(), 0, payload)); }
    public boolean tryOfferPayload(T payload, long timeout, TimeUnit unit) throws InterruptedException {
        return queue.offer(new Record<>(seq.getAndIncrement(), 0, payload), timeout, unit);
    }
    public boolean tryOfferPayloadMillis(T payload, long timeoutMillis) throws InterruptedException {
        return tryOfferPayload(payload, timeoutMillis, TimeUnit.MILLISECONDS);
    }

    public int size() { return queue.size(); }
    public int remainingCapacity() { return queue.remainingCapacity(); }

    public void finish() { this.finished = true; }

    @Override
    public Optional<Record<T>> poll() {
        Record<T> r = queue.poll();
        return Optional.ofNullable(r);
    }

    @Override
    public boolean isFinished() {
        return finished && queue.isEmpty();
    }

    public void registerMetrics(com.codahale.metrics.MetricRegistry registry, String name) {
        this.metricsName = name;
        registry.register("queuesource." + name + ".depth", (com.codahale.metrics.Gauge<Integer>) this::size);
    }
}
