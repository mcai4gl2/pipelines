package io.pipelines.budget;

/**
 * Budget governs resource consumption across the pipeline.
 */
public interface Budget extends AutoCloseable {
    /** Acquire a CPU thread slot (non-blocking preferred). Return true if acquired. */
    boolean tryAcquireCpu();
    void releaseCpu();

    /** Acquire memory bytes. Return number actually granted (may be 0). */
    long tryAcquireMemory(long bytes);
    void releaseMemory(long bytes);

    /** Block as needed to respect IO throughput budget for this many bytes. */
    void consumeIoBytes(long bytes) throws InterruptedException;

    /** Block as needed to respect external QPS budget (one op). */
    void acquireExternalOp() throws InterruptedException;

    @Override
    default void close() {}
}

