package io.pipelines.financial;

import io.pipelines.retry.ExponentialBackoffRetryPolicy;
import io.pipelines.runtime.Pipeline;
import io.pipelines.runtime.PipelineBuilder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static org.junit.jupiter.api.Assertions.*;

class YahooPipelineTest {
    private Path tmp;

    @BeforeEach
    void setup() throws IOException {
        tmp = Files.createTempDirectory("fin-data-test");
    }

    @AfterEach
    void cleanup() throws IOException {
        try (var s = Files.walk(tmp)) {
            s.sorted(java.util.Comparator.reverseOrder()).forEach(p -> { try { Files.deleteIfExists(p); } catch (IOException ignore) {} });
        }
    }

    @Test
    void initialDownloadChunkedAndOrdered() throws Exception {
        String ticker = "TEST";
        // Build fake dataset spanning 40 days across two months to force chunking when monthsPerChunk=1
        List<LocalDate> days = new ArrayList<>();
        LocalDate start = LocalDate.of(2024, 1, 10);
        LocalDate cur = start;
        while (days.size() < 40) {
            if (io.pipelines.financial.TradingCalendars.isUsEquityTradingDay(cur)) days.add(cur);
            cur = cur.plusDays(1);
        }

        MissingRangeSource source = new MissingRangeSource(List.of(ticker), tmp, days.get(0), days.get(days.size()-1), 1);
        MockYahooTransform transform = new MockYahooTransform(Map.of(ticker, days));
        FinancialCsvSink sink = new FinancialCsvSink(tmp);

        Pipeline<RangeRequest, FinancialCsv> p = new PipelineBuilder<RangeRequest, FinancialCsv>()
                .source(source)
                .transform(transform)
                .sink(sink)
                .budget(new io.pipelines.budget.SimpleBudgetManager(2, 1024*1024, 0, 0, null))
                .retry(new ExponentialBackoffRetryPolicy(2, 5, 20))
                .workers(2)
                .queueCapacity(16)
                .sinkBatchSize(4)
                .sinkFlushEveryMillis(50)
                .build();

        p.start();
        waitDone(p, source);
        p.stop();

        Path out = tmp.resolve(ticker + ".csv");
        assertTrue(Files.exists(out));
        List<String> lines = Files.readAllLines(out, StandardCharsets.UTF_8);
        assertFalse(lines.isEmpty());
        assertEquals(1 + days.size(), lines.size(), "header + rows must match");
        // Check first data line date equals start
        assertTrue(lines.get(1).startsWith(days.get(0).toString()));
        // Check last data line date equals last
        assertTrue(lines.get(lines.size()-1).startsWith(days.get(days.size()-1).toString()));
        // Ensure client was called for multiple windows
        int plannedWindows = source.windowCounts().getOrDefault(ticker, 0);
        assertEquals(plannedWindows, transform.windowCalls.getOrDefault(ticker, 0), "transform calls must equal planned windows");
    }

    @Test
    void resumeSkipsExisting() throws Exception {
        String ticker = "TEST2";
        List<LocalDate> days = new ArrayList<>();
        LocalDate start = LocalDate.of(2024, 3, 1);
        LocalDate cur = start;
        while (days.size() < 20) {
            if (io.pipelines.financial.TradingCalendars.isUsEquityTradingDay(cur)) days.add(cur);
            cur = cur.plusDays(1);
        }

        // Pre-create file with first 5 rows
        Path out = tmp.resolve(ticker + ".csv");
        Files.createDirectories(tmp);
        StringBuilder sb = new StringBuilder("date,open,high,low,close,volume\n");
        for (int i = 0; i < 5; i++) sb.append(days.get(i)).append(",1,1,1,1,100\n");
        Files.writeString(out, sb.toString(), StandardCharsets.UTF_8);

        MissingRangeSource source = new MissingRangeSource(List.of(ticker), tmp, days.get(0), days.get(days.size()-1), 2);
        MockYahooTransform transform = new MockYahooTransform(Map.of(ticker, days));
        FinancialCsvSink sink = new FinancialCsvSink(tmp);

        Pipeline<RangeRequest, FinancialCsv> p = new PipelineBuilder<RangeRequest, FinancialCsv>()
                .source(source)
                .transform(transform)
                .sink(sink)
                .budget(new io.pipelines.budget.SimpleBudgetManager(2, 1024*1024, 0, 0, null))
                .retry(new ExponentialBackoffRetryPolicy(2, 5, 20))
                .workers(2)
                .queueCapacity(16)
                .sinkBatchSize(4)
                .sinkFlushEveryMillis(50)
                .build();

        p.start();
        waitDone(p, source);
        p.stop();

        List<String> lines = Files.readAllLines(out, StandardCharsets.UTF_8);
        assertEquals(1 + days.size(), lines.size());
        // Ensure first 5 remained, and no duplicates
        assertEquals(days.get(0).toString(), lines.get(1).substring(0, 10));
        assertEquals(days.get(4).toString(), lines.get(5).substring(0, 10));
        assertEquals(days.get(19).toString(), lines.get(lines.size()-1).substring(0, 10));
    }

    private static void waitDone(Pipeline<?,?> p, MissingRangeSource source) throws InterruptedException {
        while (true) {
            if (source.isFinished() && p.getInflight() == 0 && p.getQueueSize() == 0) break;
            Thread.sleep(10);
        }
    }

    static class MockYahooTransform implements io.pipelines.core.Transform<RangeRequest, FinancialCsv> {
        private final Map<String, List<LocalDate>> data;
        final Map<String, Integer> windowCalls = new ConcurrentHashMap<>();

        MockYahooTransform(Map<String, List<LocalDate>> data) { this.data = data; }

        @Override
        public List<io.pipelines.core.Record<FinancialCsv>> apply(io.pipelines.core.Record<RangeRequest> input) {
            RangeRequest req = input.payload();
            windowCalls.merge(req.ticker(), 1, Integer::sum);
            List<LocalDate> all = data.getOrDefault(req.ticker(), List.of());
            StringBuilder chunk = new StringBuilder();
            int rows = 0;
            for (LocalDate d : all) {
                if (d.isBefore(req.start()) || d.isAfter(req.end())) continue;
                if (req.existingDates().contains(d)) continue;
                chunk.append(d).append(",1,1,1,1,100\n");
                rows++;
            }
            if (rows == 0) return List.of();
            FinancialCsv out = new FinancialCsv(req.ticker()+".csv", chunk.toString().getBytes(StandardCharsets.UTF_8), req.ticker(), rows);
            return List.of(new io.pipelines.core.Record<>(input.seq(), 0, out));
        }
    }
}
