package io.pipelines.error;

import io.pipelines.core.Record;

public interface DeadLetterSink<T> extends AutoCloseable {
    void acceptFailure(String stage, Record<T> record, Exception e);
    @Override default void close() {}
}

