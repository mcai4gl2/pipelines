package io.pipelines.source;

import io.pipelines.core.Record;
import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;

public class FileChunkSourceTest {
    @Test
    void emits_fixed_size_chunks_in_order() throws Exception {
        Path dir = Files.createTempDirectory("chunks");
        Files.writeString(dir.resolve("a.txt"), "abcdefghij"); // 10 bytes
        var src = new FileChunkSource(dir, 3);
        Record<byte[]> r1 = src.poll().orElseThrow();
        Record<byte[]> r2 = src.poll().orElseThrow();
        Record<byte[]> r3 = src.poll().orElseThrow();
        Record<byte[]> r4 = src.poll().orElseThrow();
        assertEquals("abc", new String(r1.payload()));
        assertEquals("def", new String(r2.payload()));
        assertEquals("ghi", new String(r3.payload()));
        assertEquals("j", new String(r4.payload()));
        assertTrue(src.isFinished() || src.poll().isEmpty());
    }
}

