package io.pipelines.transform;

import io.pipelines.core.Record;
import io.pipelines.core.Transform;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

public class TransformChainTest {
    @Test
    void applies_stages_in_order_and_reindexes() throws Exception {
        Transform<String, String> stage1 = r -> List.of(new Record<>(r.seq(), 3, r.payload() + "A"));
        Transform<String, String> stage2 = r -> List.of(new Record<>(r.seq(), 7, r.payload() + "B"));
        TransformChain<String, String> chain = new TransformChain<>(stage1, stage2);
        List<Record<String>> out = chain.apply(new Record<>(42, 0, ""));
        assertEquals(1, out.size());
        assertEquals(42, out.get(0).seq());
        assertEquals(0, out.get(0).subSeq());
        assertEquals("AB", out.get(0).payload());
    }
}

