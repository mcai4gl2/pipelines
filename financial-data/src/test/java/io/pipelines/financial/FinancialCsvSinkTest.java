package io.pipelines.financial;

import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;

class FinancialCsvSinkTest {
    @Test
    void writesHeaderOnceAndAppends() throws Exception {
        Path tmp = Files.createTempDirectory("sink-test");
        try {
            FinancialCsvSink sink = new FinancialCsvSink(tmp);
            io.pipelines.core.Record<FinancialCsv> r1 = new io.pipelines.core.Record<>(0,0,
                    new FinancialCsv("ABC.csv", "2024-01-01,1,1,1,1,100\n".getBytes(StandardCharsets.UTF_8), "ABC", 1));
            sink.accept(r1);
            io.pipelines.core.Record<FinancialCsv> r2 = new io.pipelines.core.Record<>(1,0,
                    new FinancialCsv("ABC.csv", "2024-01-02,1,1,1,1,100\n".getBytes(StandardCharsets.UTF_8), "ABC", 1));
            sink.accept(r2);

            Path out = tmp.resolve("ABC.csv");
            String content = Files.readString(out, StandardCharsets.UTF_8);
            String[] lines = content.split("\n");
            assertEquals("date,open,high,low,close,volume", lines[0]);
            assertEquals("2024-01-01,1,1,1,1,100", lines[1]);
            assertEquals("2024-01-02,1,1,1,1,100", lines[2]);
        } finally {
            try (var s = Files.walk(tmp)) { s.sorted(java.util.Comparator.reverseOrder()).forEach(p -> { try { Files.deleteIfExists(p); } catch (Exception ignore) {} }); }
        }
    }
}

