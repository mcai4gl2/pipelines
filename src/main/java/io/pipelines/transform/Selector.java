package io.pipelines.transform;

import io.pipelines.core.Record;

@FunctionalInterface
public interface Selector<I> {
    String route(Record<I> record);
}

