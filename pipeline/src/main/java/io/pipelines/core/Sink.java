package io.pipelines.core;

import java.io.Closeable;

/**
 * Sink consumes records, ideally in deterministic order of seq then subSeq.
 */
public interface Sink<T> extends Closeable {
    void accept(Record<T> record) throws Exception;

    @Override
    default void close() {}
}

