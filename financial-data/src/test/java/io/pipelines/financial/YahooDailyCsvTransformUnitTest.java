package io.pipelines.financial;

import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;

class YahooDailyCsvTransformUnitTest {
    @Test
    void transformsWindowAndSkipsExisting() throws Exception {
        // Mock client to return 5 sequential days
        YahooClient mock = (ticker, p1, p2, interval) -> {
            LocalDate s = LocalDate.of(2024,1,1);
            java.util.List<Long> ts = java.util.stream.LongStream.range(0,5)
                .mapToObj(i -> s.plusDays(i))
                .map(d -> d.atStartOfDay().toEpochSecond(ZoneOffset.UTC))
                .toList();
            java.util.List<Double> ones = java.util.Collections.nCopies(5, 1.0);
            java.util.List<Long> vols = java.util.Collections.nCopies(5, 100L);
            return "{\"chart\":{\"result\":[{\"timestamp\":" + ts + ",\"indicators\":{\"quote\":[{\"open\":" + ones + ",\"high\":" + ones + ",\"low\":" + ones + ",\"close\":" + ones + ",\"volume\":" + vols + "}]}}]}}";
        };

        YahooDailyCsvTransform t = new YahooDailyCsvTransform(mock, "1d");
        java.util.Set<LocalDate> existing = new java.util.HashSet<>();
        existing.add(LocalDate.of(2024,1,2)); // skip this date
        RangeRequest req = new RangeRequest("XYZ", LocalDate.of(2024,1,1), LocalDate.of(2024,1,5), existing);
        io.pipelines.core.Record<RangeRequest> in = new io.pipelines.core.Record<>(0,0, req);

        java.util.List<io.pipelines.core.Record<FinancialCsv>> out = t.apply(in);
        assertEquals(1, out.size());
        FinancialCsv csv = out.get(0).payload();
        String chunk = new String(csv.data(), StandardCharsets.UTF_8);
        // 4 rows expected (5 total minus 1 existing)
        assertEquals(4, chunk.lines().count());
        assertTrue(chunk.startsWith("2024-01-01,"));
        assertTrue(chunk.contains("2024-01-05,"));
        assertFalse(chunk.contains("2024-01-02,"));
    }
}

