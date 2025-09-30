package io.pipelines.financial;

import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDate;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class MissingRangeSourceTest {
    @Test
    void computesMissingAndChunks() throws Exception {
        Path tmp = Files.createTempDirectory("mrs-test");
        try {
            // Seed existing for ABC: 2024-01-01..2024-01-05
            Path out = tmp.resolve("ABC.csv");
            StringBuilder sb = new StringBuilder("date,open,high,low,close,volume\n");
            for (int i = 1; i <= 5; i++) sb.append(LocalDate.of(2024,1,i)).append(",1,1,1,1,100\n");
            Files.writeString(out, sb.toString(), StandardCharsets.UTF_8);

            LocalDate start = LocalDate.of(2024,1,1);
            LocalDate end = LocalDate.of(2024,3,31);
            // Missing ranges: 2024-01-06..2024-03-31
            MissingRangeSource src = new MissingRangeSource(List.of("ABC"), tmp, start, end, 1);
            assertEquals(5, src.initialCounts().get("ABC"));
            // Weekends are skipped; chunking by 1 month still yields 3 windows of weekdays
            assertEquals(3, src.windowCounts().get("ABC"));

            int emitted = 0;
            java.util.Optional<io.pipelines.core.Record<RangeRequest>> r;
            while ((r = src.poll()).isPresent()) {
                emitted++;
                RangeRequest rr = r.get().payload();
                assertEquals("ABC", rr.ticker());
                assertFalse(rr.start().isAfter(rr.end()));
            }
            assertEquals(3, emitted);
            assertTrue(src.isFinished());
        } finally {
            try (var s = Files.walk(tmp)) { s.sorted(java.util.Comparator.reverseOrder()).forEach(p -> { try { Files.deleteIfExists(p); } catch (Exception ignore) {} }); }
        }
    }

    @Test
    void skipsWeekendsOnlyRange() throws Exception {
        Path tmp = Files.createTempDirectory("mrs-test2");
        try {
            // A weekend-only range (Sat-Sun): 2024-01-06..2024-01-07
            LocalDate start = LocalDate.of(2024,1,6);
            LocalDate end = LocalDate.of(2024,1,7);
            MissingRangeSource src = new MissingRangeSource(List.of("ABC"), tmp, start, end, 1);
            assertEquals(0, src.windowCounts().get("ABC"));
            assertTrue(src.poll().isEmpty());
            assertTrue(src.isFinished());
        } finally {
            try (var s = Files.walk(tmp)) { s.sorted(java.util.Comparator.reverseOrder()).forEach(p -> { try { Files.deleteIfExists(p); } catch (Exception ignore) {} }); }
        }
    }
}
