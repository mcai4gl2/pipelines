package io.pipelines.transform;

import io.pipelines.core.Record;
import io.pipelines.core.Transform;
import io.pipelines.source.QueueSource;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

public class RouterTransformTest {
    @Test
    void routes_to_transform() throws Exception {
        Selector<String> sel = r -> "t";
        Transform<String, String> t = r -> List.of(new Record<>(r.seq(), 0, r.payload().toUpperCase()));
        RouterTransform<String, String> router = new RouterTransform<String, String>(sel).route("t", t);
        List<Record<String>> out = router.apply(new Record<>(1, 0, "x"));
        assertEquals(1, out.size());
        assertEquals("X", out.get(0).payload());
    }

    @Test
    void routes_to_queue_and_returns_empty() throws Exception {
        Selector<String> sel = r -> "q";
        QueueSource<String> q = new QueueSource<>(8);
        RouterTransform<String, String> router = new RouterTransform<String, String>(sel).publish("q", q);
        List<Record<String>> out = router.apply(new Record<>(2, 0, "y"));
        assertTrue(out.isEmpty());
        assertTrue(q.poll().isPresent());
    }
}
