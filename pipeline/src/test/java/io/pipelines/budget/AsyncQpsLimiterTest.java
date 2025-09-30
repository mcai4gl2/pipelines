package io.pipelines.budget;

import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.junit.jupiter.api.Assertions.*;

public class AsyncQpsLimiterTest {
    @Test
    void acquires_with_spacing() throws Exception {
        AsyncQpsLimiter limiter = new AsyncQpsLimiter(10); // 100ms per permit
        long t0 = System.nanoTime();
        limiter.acquire().toCompletableFuture().get();
        long t1 = System.nanoTime();
        limiter.acquire().toCompletableFuture().get();
        long t2 = System.nanoTime();
        long dt = (t2 - t1) / 1_000_000; // ms
        // Allow some slack in CI; expect at least ~60ms spacing
        assertTrue(dt >= 60, "expected spacing >= 60ms but was " + dt + "ms");
    }
}

