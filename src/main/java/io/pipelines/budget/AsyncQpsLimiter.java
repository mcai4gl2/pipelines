package io.pipelines.budget;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Simple non-blocking QPS limiter that returns a CompletionStage completing when a permit is available.
 * Uses a shared daemon scheduler to delay completion without blocking threads.
 */
public class AsyncQpsLimiter implements AutoCloseable {
    private static final ScheduledExecutorService SCHEDULER = Executors.newSingleThreadScheduledExecutor(new ThreadFactory() {
        @Override public Thread newThread(Runnable r) { Thread t = new Thread(r, "async-qps-limiter"); t.setDaemon(true); return t; }
    });

    private final long intervalNanos;
    private final AtomicLong nextAvailableNanos = new AtomicLong(0);

    public AsyncQpsLimiter(long permitsPerSecond) {
        if (permitsPerSecond <= 0) {
            this.intervalNanos = 0;
        } else {
            this.intervalNanos = TimeUnit.SECONDS.toNanos(1) / Math.max(1, permitsPerSecond);
        }
    }

    public CompletionStage<Void> acquire() {
        if (intervalNanos == 0) return CompletableFuture.completedFuture(null);
        long now = System.nanoTime();
        while (true) {
            long current = nextAvailableNanos.get();
            long earliest = Math.max(current, now);
            long next = earliest + intervalNanos;
            if (nextAvailableNanos.compareAndSet(current, next)) {
                long delay = Math.max(0, earliest - now);
                if (delay == 0) return CompletableFuture.completedFuture(null);
                CompletableFuture<Void> fut = new CompletableFuture<>();
                SCHEDULER.schedule(() -> fut.complete(null), delay, TimeUnit.NANOSECONDS);
                return fut;
            }
        }
    }

    @Override
    public void close() {
        // shared scheduler; no-op
    }
}

