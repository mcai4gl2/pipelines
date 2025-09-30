package io.pipelines.budget;

import org.junit.jupiter.api.Test;

import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

public class SimpleBudgetManagerAsyncTest {
    @Test
    void async_acquire_honors_qps_without_blocking_callers() throws Exception {
        SimpleBudgetManager b = new SimpleBudgetManager(1, 1024, 0, 10, null); // 10 qps ~ 100ms spacing
        long t0 = System.nanoTime();
        b.acquireExternalOpAsync().toCompletableFuture().get(1, java.util.concurrent.TimeUnit.SECONDS);
        long t1 = System.nanoTime();
        b.acquireExternalOpAsync().toCompletableFuture().get(1, java.util.concurrent.TimeUnit.SECONDS);
        long t2 = System.nanoTime();
        long dt = TimeUnit.NANOSECONDS.toMillis(t2 - t1);
        assertTrue(dt >= 60, "expected spacing >= 60ms but was " + dt + "ms");
    }
}

