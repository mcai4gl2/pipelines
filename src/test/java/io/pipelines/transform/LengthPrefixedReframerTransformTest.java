package io.pipelines.transform;

import io.pipelines.core.Record;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

public class LengthPrefixedReframerTransformTest {
    private static byte[] frame(String s) {
        byte[] data = s.getBytes(java.nio.charset.StandardCharsets.UTF_8);
        ByteBuffer bb = ByteBuffer.allocate(4 + data.length);
        bb.putInt(data.length);
        bb.put(data);
        return bb.array();
    }

    @Test
    void carries_incomplete_and_emits_complete_frames() throws Exception {
        var t = new LengthPrefixedReframerTransform(1024);
        byte[] f1 = frame("hello");
        byte[] f2 = frame("world");
        // Feed in 3 chunks: first half of f1 header, rest of f1+part of f2, then rest of f2
        byte[] c1 = java.util.Arrays.copyOfRange(f1, 0, 2);
        byte[] c2 = concat(java.util.Arrays.copyOfRange(f1, 2, f1.length), java.util.Arrays.copyOfRange(f2, 0, 3));
        byte[] c3 = java.util.Arrays.copyOfRange(f2, 3, f2.length);
        List<Record<byte[]>> out1 = t.apply(new Record<>(1, 0, c1));
        assertTrue(out1.isEmpty());
        List<Record<byte[]>> out2 = t.apply(new Record<>(2, 0, c2));
        assertEquals(1, out2.size());
        assertEquals("hello", new String(out2.get(0).payload()));
        List<Record<byte[]>> out3 = t.apply(new Record<>(3, 0, c3));
        assertEquals(1, out3.size());
        assertEquals("world", new String(out3.get(0).payload()));
    }

    private static byte[] concat(byte[] a, byte[] b) {
        byte[] c = new byte[a.length + b.length];
        System.arraycopy(a, 0, c, 0, a.length);
        System.arraycopy(b, 0, c, a.length, b.length);
        return c;
    }
}

