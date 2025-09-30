package io.pipelines.core;

import java.util.List;

/** Optional sink capability to consume records in batches for IO efficiency. */
public interface BatchSink<T> extends Sink<T> {
    void acceptBatch(List<Record<T>> records) throws Exception;
}

