package io.pipelines.core;

import java.util.List;
import java.util.concurrent.CompletionStage;

/**
 * Async transform that returns its outputs via CompletionStage to avoid blocking worker threads.
 */
public interface AsyncTransform<I, O> {
    CompletionStage<List<Record<O>>> applyAsync(Record<I> input);
}

