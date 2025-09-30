package io.pipelines.budget;

import java.time.Clock;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

/**
 * Simple budgeting using semaphores and a naive token bucket for rate limiting.
 */
public class SimpleBudgetManager implements Budget {
    private final Semaphore cpu;
    private final Semaphore mem;
    private final long ioBytesPerSec;
    private final long externalQps;
    private final Clock clock;

    private long ioTokens;
    private long lastIoRefillNanos;
    private long qpsTokens;
    private long lastQpsRefillNanos;

    public SimpleBudgetManager(int cpuThreads, long memoryBytes, long ioBytesPerSec, long externalQps, Clock clock) {
        this.cpu = new Semaphore(Math.max(0, cpuThreads));
        // memory tracked as units of 1 byte; cap at int max for semaphore fairness
        long memPermits = Math.min(memoryBytes, Integer.MAX_VALUE);
        this.mem = new Semaphore((int) memPermits);
        this.ioBytesPerSec = Math.max(0, ioBytesPerSec);
        this.externalQps = Math.max(0, externalQps);
        this.clock = clock == null ? Clock.systemUTC() : clock;
        long now = System.nanoTime();
        this.lastIoRefillNanos = now;
        this.lastQpsRefillNanos = now;
        this.ioTokens = this.ioBytesPerSec; // initial burst of 1s
        this.qpsTokens = this.externalQps; // initial burst of 1s
    }

    @Override
    public boolean tryAcquireCpu() {
        return cpu.tryAcquire();
    }

    @Override
    public void releaseCpu() {
        cpu.release();
    }

    @Override
    public long tryAcquireMemory(long bytes) {
        int want = (int) Math.min(bytes, Integer.MAX_VALUE);
        int granted = mem.drainPermits();
        int toGrant = Math.min(granted, want);
        if (granted > toGrant) {
            mem.release(granted - toGrant);
        }
        return toGrant;
    }

    @Override
    public void releaseMemory(long bytes) {
        int release = (int) Math.min(bytes, Integer.MAX_VALUE);
        mem.release(release);
    }

    @Override
    public synchronized void consumeIoBytes(long bytes) throws InterruptedException {
        if (ioBytesPerSec <= 0) return; // no limit
        long need = bytes;
        while (need > 0) {
            refillIo();
            if (ioTokens <= 0) {
                Thread.sleep(1);
                continue;
            }
            long take = Math.min(ioTokens, need);
            ioTokens -= take;
            need -= take;
        }
    }

    @Override
    public synchronized void acquireExternalOp() throws InterruptedException {
        if (externalQps <= 0) return; // no limit
        while (true) {
            refillQps();
            if (qpsTokens > 0) {
                qpsTokens--;
                return;
            }
            Thread.sleep(1);
        }
    }

    private void refillIo() {
        long now = System.nanoTime();
        long elapsed = now - lastIoRefillNanos;
        if (elapsed <= 0) return;
        long add = (ioBytesPerSec * elapsed) / TimeUnit.SECONDS.toNanos(1);
        if (add > 0) {
            ioTokens = Math.min(ioBytesPerSec, ioTokens + add);
            lastIoRefillNanos = now;
        }
    }

    private void refillQps() {
        long now = System.nanoTime();
        long elapsed = now - lastQpsRefillNanos;
        if (elapsed <= 0) return;
        long add = (externalQps * elapsed) / TimeUnit.SECONDS.toNanos(1);
        if (add > 0) {
            qpsTokens = Math.min(externalQps, qpsTokens + add);
            lastQpsRefillNanos = now;
        }
    }
}

