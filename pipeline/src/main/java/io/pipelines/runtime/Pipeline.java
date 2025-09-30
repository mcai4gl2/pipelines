package io.pipelines.runtime;

import com.codahale.metrics.Meter;
import com.codahale.metrics.Timer;
import io.pipelines.budget.Budget;
import io.pipelines.core.Record;
import io.pipelines.core.Sink;
import io.pipelines.core.Source;
import io.pipelines.core.Transform;
import io.pipelines.core.AsyncTransform;
import io.pipelines.metrics.Metrics;
import io.pipelines.error.DeadLetterSink;
import io.pipelines.retry.RetryPolicy;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Single-source -> single-transform -> single-sink pipeline with ordering, backpressure, budgeting and retries.
 */
public class Pipeline<I, O> implements AutoCloseable {
    private final Source<I> source;
    private final Transform<I, O> transform;
    private final AsyncTransform<I, O> asyncTransform; // optional
    private final Sink<O> sink;
    private final Budget budget;
    private final RetryPolicy retryPolicy;
    private final int queueCapacity;
    private final int workers;
    private final Metrics metrics;
    private final DeadLetterSink<I> dlqIn;
    private final DeadLetterSink<O> dlqOut;

    private final ExecutorService workerPool;
    private final ArrayBlockingQueue<Batch<O>> queue;
    private final AtomicBoolean running = new AtomicBoolean(false);
    private final AtomicBoolean paused = new AtomicBoolean(false);
    private final java.util.concurrent.atomic.AtomicInteger inflight = new java.util.concurrent.atomic.AtomicInteger(0);
    private volatile Thread srcThread;
    private volatile Thread sinkThread;

    private final Timer sourceTimer;
    private final Timer transformTimer;
    private final Timer sinkTimer;
    private final Meter inMeter;
    private final Meter outMeter;
    private final Meter errorMeter;

    private volatile long nextSeq = 0;
    private final int sinkBatchSize;
    private final int sinkFlushEveryMillis;
    private final com.codahale.metrics.Counter sinkBatchFlushesCounter;
    private final com.codahale.metrics.Counter sinkBatchRecordsCounter;
    private final java.util.concurrent.atomic.LongAdder sourceNanos = new java.util.concurrent.atomic.LongAdder();
    private final java.util.concurrent.atomic.LongAdder transformNanos = new java.util.concurrent.atomic.LongAdder();
    private final java.util.concurrent.atomic.LongAdder sinkNanos = new java.util.concurrent.atomic.LongAdder();
    private final int maxInFlight;

    public Pipeline(Source<I> source,
                    Transform<I, O> transform,
                    Sink<O> sink,
                    Budget budget,
                    RetryPolicy retryPolicy,
                    int workers,
                    int queueCapacity,
                    Metrics metrics,
                    DeadLetterSink<I> dlqIn,
                    DeadLetterSink<O> dlqOut,
                    int sinkBatchSize,
                    int sinkFlushEveryMillis,
                    int maxInFlight) {
        this.source = Objects.requireNonNull(source);
        this.transform = Objects.requireNonNull(transform);
        this.asyncTransform = (transform instanceof AsyncTransform) ? (AsyncTransform<I,O>) transform : null;
        this.sink = Objects.requireNonNull(sink);
        this.budget = Objects.requireNonNull(budget);
        this.retryPolicy = Objects.requireNonNull(retryPolicy);
        this.queueCapacity = Math.max(1, queueCapacity);
        this.workers = Math.max(1, workers);
        this.metrics = Objects.requireNonNull(metrics);
        this.workerPool = Executors.newFixedThreadPool(this.workers);
        this.queue = new ArrayBlockingQueue<>(this.queueCapacity);
        this.sourceTimer = metrics.timer("pipeline.source.time");
        this.transformTimer = metrics.timer("pipeline.transform.time");
        this.sinkTimer = metrics.timer("pipeline.sink.time");
        this.inMeter = metrics.meter("pipeline.input.rate");
        this.outMeter = metrics.meter("pipeline.output.rate");
        this.errorMeter = metrics.meter("pipeline.error.rate");
        this.dlqIn = dlqIn;
        this.dlqOut = dlqOut;
        this.sinkBatchSize = Math.max(1, sinkBatchSize);
        this.sinkFlushEveryMillis = Math.max(0, sinkFlushEveryMillis);
        this.sinkBatchFlushesCounter = metrics.registry().counter("pipeline.sink.batch.flushes");
        this.sinkBatchRecordsCounter = metrics.registry().counter("pipeline.sink.batch.records");
        this.maxInFlight = Math.max(1, maxInFlight);
    }

    public void start() {
        if (!running.compareAndSet(false, true)) return;
        // source pump
        srcThread = new Thread(this::runSource, "pipeline-source");
        srcThread.start();

        // single sink thread to enforce ordering
        sinkThread = new Thread(this::runSink, "pipeline-sink");
        sinkThread.start();
    }

    public void pause() { paused.set(true); }
    public void resume() { paused.set(false); }
    public void stop() {
        running.set(false);
        // allow source to exit cleanly and signal poison to sink
        Thread st = srcThread; Thread kt = sinkThread;
        if (st != null) { try { st.join(5000); } catch (InterruptedException ie) { Thread.currentThread().interrupt(); } }
        if (kt != null) { try { kt.join(5000); } catch (InterruptedException ie) { Thread.currentThread().interrupt(); } }
        workerPool.shutdown();
    }

    public boolean isRunning() { return running.get(); }
    public boolean isPaused() { return paused.get(); }
    public int getQueueSize() { return queue.size(); }
    public int getInflight() { return inflight.get(); }

    public double sourcePct() { long a = sourceNanos.sum(); long b = transformNanos.sum(); long c = sinkNanos.sum(); long t=a+b+c; return t==0?0.0:(100.0*a)/t; }
    public double transformPct() { long a = sourceNanos.sum(); long b = transformNanos.sum(); long c = sinkNanos.sum(); long t=a+b+c; return t==0?0.0:(100.0*b)/t; }
    public double sinkPct() { long a = sourceNanos.sum(); long b = transformNanos.sum(); long c = sinkNanos.sum(); long t=a+b+c; return t==0?0.0:(100.0*c)/t; }

    private void runSource() {
        while (running.get()) {
            if (paused.get()) { sleepQuiet(5); continue; }
            // Backpressure: limit pending/in-flight tasks
            if (inflight.get() >= this.maxInFlight) { sleepQuiet(1); continue; }
            Optional<Record<I>> opt;
            long t0 = System.nanoTime();
            try (Timer.Context ignored = sourceTimer.time()) {
                opt = source.poll();
            } finally {
                sourceNanos.add(System.nanoTime() - t0);
            }
            if (opt.isEmpty()) {
                if (source.isFinished()) break;
                sleepQuiet(1);
                continue;
            }
            inMeter.mark();
            Record<I> in = opt.get();
            inflight.incrementAndGet();
            // apply transform with retries on worker threads that will also respect budgets
            if (asyncTransform != null) {
                inflight.incrementAndGet(); // already incremented above; keep symmetry for async path
                processAsync(in, 1);
            } else {
                workerPool.submit(() -> process(in));
            }
        }
        // wait for all submitted work to complete, then signal end
        while (inflight.get() > 0) { sleepQuiet(5); }
        queue.offer(Batch.poison());
    }

    private void process(Record<I> in) {
        if (!budget.tryAcquireCpu()) { // backoff when no CPU slot
            sleepQuiet(1);
            process(in);
            return;
        }
        try {
            int attempt = 0;
            while (true) {
                attempt++;
                long t0 = System.nanoTime();
                try (Timer.Context ignored = transformTimer.time()) {
                    List<Record<O>> outputs = transform.apply(in);
                    if (outputs == null) outputs = List.of();
                    // ensure deterministic subSeq order
                    outputs = new ArrayList<>(outputs);
                    outputs.sort(java.util.Comparator.comparingInt(Record::subSeq));
                    queue.put(Batch.of(in.seq(), outputs));
                    return;
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    return;
                } catch (Exception e) {
                    if (!retryPolicy.shouldRetry(attempt, e) && dlqIn != null) {
                        dlqIn.acceptFailure("transform", in, e);
                    }
                    if (retryPolicy.shouldRetry(attempt, e)) {
                        errorMeter.mark();
                        sleepQuiet(retryPolicy.backoffMillis(attempt));
                    } else {
                        errorMeter.mark();
                        return;
                    }
                } finally {
                    transformNanos.add(System.nanoTime() - t0);
                }
            }
        } finally {
            budget.releaseCpu();
            inflight.decrementAndGet();
        }
    }

    private void processAsync(Record<I> in, int attempt) {
        long t0 = System.nanoTime();
        try (Timer.Context ignored = transformTimer.time()) {
            asyncTransform.applyAsync(in).whenComplete((outputs, ex) -> {
                try {
                    if (ex != null) {
                        if (retryPolicy.shouldRetry(attempt, ex instanceof Exception ? (Exception) ex : new Exception(ex))) {
                            errorMeter.mark();
                            long backoff = retryPolicy.backoffMillis(attempt);
                            workerPool.submit(() -> {
                                sleepQuiet(backoff);
                                processAsync(in, attempt + 1);
                            });
                        } else {
                            errorMeter.mark();
                            if (dlqIn != null && ex instanceof Exception) dlqIn.acceptFailure("transform", in, (Exception) ex);
                        }
                        return;
                    }
                    List<Record<O>> outs = (outputs == null) ? List.of() : outputs;
                    outs = new ArrayList<>(outs);
                    outs.sort(java.util.Comparator.comparingInt(Record::subSeq));
                    try { queue.put(Batch.of(in.seq(), outs)); } catch (InterruptedException ie) { Thread.currentThread().interrupt(); }
                } catch (Exception e2) {
                    errorMeter.mark();
                } finally {
                    transformNanos.add(System.nanoTime() - t0);
                    inflight.decrementAndGet();
                }
            });
        }
    }

    private void runSink() {
        try {
            java.util.ArrayList<Record<O>> emitBuffer = new java.util.ArrayList<>();
            long batchStartNanos = 0L;
            long expectedSeq = 0;
            java.util.TreeMap<Long, Batch<O>> buffer = new java.util.TreeMap<>();
            long lastFlush = System.currentTimeMillis();
            while (true) {
                Batch<O> batch = sinkFlushEveryMillis > 0 ? queue.poll(sinkFlushEveryMillis, java.util.concurrent.TimeUnit.MILLISECONDS) : queue.take();
                if (batch == null) {
                    if (!emitBuffer.isEmpty()) {
                        try {
                            long now = System.nanoTime();
                            flushToSink(emitBuffer);
                            metrics.registry().timer("pipeline.sink.batch.flush.timer").update(now - batchStartNanos, java.util.concurrent.TimeUnit.NANOSECONDS);
                            metrics.registry().histogram("pipeline.sink.batch.size").update(emitBuffer.size());
                        } catch (Exception e) { errorMeter.mark(); } finally { emitBuffer.clear(); batchStartNanos = 0L; }
                        metrics.registry().counter("pipeline.sink.batch.flushes.timed").inc();
                    }
                    lastFlush = System.currentTimeMillis();
                    continue;
                }
                if (batch.isPoison()) { // flush remaining before exit
                    try {
                        if (!emitBuffer.isEmpty()) {
                            long now = System.nanoTime();
                            flushToSink(emitBuffer);
                            metrics.registry().timer("pipeline.sink.batch.flush.timer").update(now - batchStartNanos, java.util.concurrent.TimeUnit.NANOSECONDS);
                            metrics.registry().histogram("pipeline.sink.batch.size").update(emitBuffer.size());
                        }
                    } catch (Exception ignored) {} finally { emitBuffer.clear(); batchStartNanos = 0L; }
                    return;
                }
                buffer.put(batch.seq, batch);
                Batch<O> ready;
                while ((ready = buffer.remove(expectedSeq)) != null) {
                    for (Record<O> next : ready.items) {
                        long t0 = System.nanoTime();
                        try (Timer.Context ignored = sinkTimer.time()) {
                            emitBuffer.add(next);
                            if (emitBuffer.size() == 1) batchStartNanos = System.nanoTime();
                            // Flush in batches
                            if (emitBuffer.size() >= sinkBatchSize) {
                                long now = System.nanoTime();
                                flushToSink(emitBuffer);
                                metrics.registry().timer("pipeline.sink.batch.flush.timer").update(now - batchStartNanos, java.util.concurrent.TimeUnit.NANOSECONDS);
                                metrics.registry().histogram("pipeline.sink.batch.size").update(emitBuffer.size());
                                emitBuffer.clear();
                                batchStartNanos = 0L;
                            }
                        } catch (Exception e) {
                            errorMeter.mark();
                            if (dlqOut != null) dlqOut.acceptFailure("sink", next, e);
                        } finally {
                            sinkNanos.add(System.nanoTime() - t0);
                        }
                    }
                    expectedSeq++;
                }
                // keep partial batch to accumulate across iterations; time-based flush handled by poll timeout
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private void flushToSink(java.util.List<Record<O>> records) throws Exception {
        if (records.isEmpty()) return;
        if (sink instanceof io.pipelines.core.BatchSink<O> bs) {
            bs.acceptBatch(records);
            outMeter.mark(records.size());
            sinkBatchFlushesCounter.inc();
            sinkBatchRecordsCounter.inc(records.size());
        } else {
            for (Record<O> r : records) {
                sink.accept(r);
                outMeter.mark();
            }
            sinkBatchFlushesCounter.inc();
            sinkBatchRecordsCounter.inc(records.size());
        }
    }

    private static void sleepQuiet(long millis) {
        try { Thread.sleep(millis); } catch (InterruptedException ie) { Thread.currentThread().interrupt(); }
    }

    @Override
    public void close() {
        stop();
        workerPool.shutdownNow();
    }

    static final class Batch<T> {
        final long seq;
        final java.util.List<Record<T>> items;
        private final boolean poison;

        private Batch(long seq, java.util.List<Record<T>> items, boolean poison) {
            this.seq = seq; this.items = items; this.poison = poison;
        }
        static <T> Batch<T> of(long seq, java.util.List<Record<T>> items) { return new Batch<>(seq, items, false); }
        static <T> Batch<T> poison() { return new Batch<>(Long.MAX_VALUE, java.util.List.of(), true); }
        boolean isPoison() { return poison; }
    }
}
