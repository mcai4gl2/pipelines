package io.pipelines.financial;

import io.pipelines.retry.ExponentialBackoffRetryPolicy;
import io.pipelines.runtime.Pipeline;
import io.pipelines.runtime.PipelineBuilder;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class PerTickerSummaryTest {
    @Test
    void computesBeforeAfterAddedPerTicker() throws Exception {
        Path tmp = Files.createTempDirectory("summary-test");
        try {
            String t1 = "AAA";
            String t2 = "BBB";
            // Seed AAA with 2 rows
            String header = "date,open,high,low,close,volume\n";
            String aSeed = header + "2024-01-01,1,1,1,1,100\n" + "2024-01-02,1,1,1,1,100\n";
            Files.writeString(tmp.resolve(t1 + ".csv"), aSeed, StandardCharsets.UTF_8);

            // Prepare dataset: AAA has 5 days, BBB has 3 days
            List<LocalDate> aDays = new ArrayList<>();
            for (int i = 1; i <= 5; i++) aDays.add(LocalDate.of(2024,1,i));
            List<LocalDate> bDays = List.of(LocalDate.of(2024,2,1), LocalDate.of(2024,2,2), LocalDate.of(2024,2,3));

            MissingRangeSource source = new MissingRangeSource(List.of(t1, t2), tmp,
                    LocalDate.of(2024,1,1), LocalDate.of(2024,3,1), 2);
            YahooPipelineTest.MockYahooTransform transform = new YahooPipelineTest.MockYahooTransform(Map.of(
                    t1, aDays,
                    t2, bDays
            ));
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
            // wait for finish
            while (true) {
                if (source.isFinished() && p.getInflight() == 0 && p.getQueueSize() == 0) break;
                Thread.sleep(10);
            }
            p.stop();

            // Compute before/after like main
            var before = source.initialCounts();
            var after = new java.util.HashMap<String,Integer>();
            for (String t : List.of(t1, t2)) {
                Path f = tmp.resolve(t + ".csv");
                int lines = 0;
                if (Files.exists(f)) {
                    try (var br = Files.newBufferedReader(f)) { while (br.readLine() != null) lines++; }
                    if (lines > 0) lines -= 1;
                }
                after.put(t, lines);
            }

            assertEquals(2, before.get(t1));
            assertEquals(0, before.get(t2));
            assertEquals(5, after.get(t1));
            assertEquals(3, after.get(t2));
            int addedA = Math.max(0, after.get(t1) - before.get(t1));
            int addedB = Math.max(0, after.get(t2) - before.get(t2));
            assertEquals(3, addedA);
            assertEquals(3, addedB);
        } finally {
            try (var s = Files.walk(tmp)) { s.sorted(java.util.Comparator.reverseOrder()).forEach(p -> { try { Files.deleteIfExists(p); } catch (Exception ignore) {} }); }
        }
    }
}

