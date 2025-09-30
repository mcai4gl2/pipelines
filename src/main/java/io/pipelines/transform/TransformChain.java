package io.pipelines.transform;

import io.pipelines.core.Record;
import io.pipelines.core.Transform;

import java.util.ArrayList;
import java.util.List;

/**
 * Sequentially applies multiple transforms, flattening outputs. The final outputs are reindexed with deterministic subSeq.
 */
public class TransformChain<I, O> implements Transform<I, O> {
    private final List<Transform<?, ?>> stages;

    @SafeVarargs
    public TransformChain(Transform<?, ?>... stages) {
        this.stages = List.of(stages);
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    @Override
    public List<Record<O>> apply(Record<I> input) throws Exception {
        List<Record<?>> current = List.of(input);
        for (Transform stage : stages) {
            List<Record<?>> next = new ArrayList<>();
            for (Record<?> r : current) {
                List out = stage.apply((Record) r);
                if (out != null) next.addAll(out);
            }
            current = next;
        }
        // reindex subseq deterministically
        List<Record<O>> result = new ArrayList<>(current.size());
        int i = 0;
        for (Record<?> r : current) {
            result.add(new Record<>(((Record<?>) r).seq(), i++, (O) ((Record<?>) r).payload()));
        }
        return result;
    }
}

