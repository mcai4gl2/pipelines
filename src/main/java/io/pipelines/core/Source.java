package io.pipelines.core;

import java.io.Closeable;
import java.util.Optional;

/**
 * A Source produces records. Implementations should be non-blocking where possible and
 * support deterministic ordering via seq.
 */
public interface Source<T> extends Closeable {
    /**
     * Fetch next available record if any. Return empty when temporarily no data; return empty consistently
     * and rely on {@link #isFinished()} to indicate completion for finite sources.
     */
    Optional<Record<T>> poll();

    /**
     * Whether the source has reached a terminal state and will produce no more records.
     */
    boolean isFinished();

    @Override
    default void close() {}
}

