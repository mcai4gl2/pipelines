package io.pipelines.core;

import java.util.List;

/**
 * Transform converts an input record into zero or more output records.
 * Implementations must preserve the input's seq in outputs while assigning subSeq deterministically.
 */
public interface Transform<I, O> {
    List<Record<O>> apply(Record<I> input) throws Exception;
}

