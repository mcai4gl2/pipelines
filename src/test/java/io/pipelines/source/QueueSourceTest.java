package io.pipelines.source;

import io.pipelines.core.Record;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

public class QueueSourceTest {
    @Test
    void assigns_seq_on_payload_offer_and_finishes() throws Exception {
        QueueSource<String> q = new QueueSource<>(4);
        q.offerPayload("a");
        q.offerPayload("b");
        Record<String> r1 = q.poll().orElseThrow();
        Record<String> r2 = q.poll().orElseThrow();
        assertEquals(0, r1.seq());
        assertEquals(1, r2.seq());
        assertFalse(q.isFinished());
        q.finish();
        assertTrue(q.isFinished());
    }
}

