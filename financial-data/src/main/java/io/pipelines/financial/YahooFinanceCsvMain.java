package io.pipelines.financial;

import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import picocli.CommandLine;
import io.pipelines.budget.SimpleBudgetManager;
import io.pipelines.core.Sink;
import io.pipelines.retry.ExponentialBackoffRetryPolicy;
import io.pipelines.runtime.Pipeline;
import io.pipelines.runtime.PipelineBuilder;

import java.nio.file.Path;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;

/**
 * CLI to download and backfill Yahoo Finance prices into CSVs.
 */
@CommandLine.Command(name = "yahoo-csv", mixinStandardHelpOptions = true, description = "Download Yahoo Finance prices to CSVs")
public final class YahooFinanceCsvMain implements Callable<Integer> {
    @CommandLine.Option(names = {"-t", "--ticker"}, split = ",", description = "Tickers (comma-separated or repeat option)", defaultValue = "SPY,AAPL,MSFT,USDCNY=X,USDHKD=X,GBPUSD=X,HKDCNY=X")
    List<String> tickers = new ArrayList<>();

    @CommandLine.Option(names = {"-o", "--out"}, description = "Output directory", defaultValue = "financial-output")
    String outDir;

    @CommandLine.Option(names = {"-c", "--chunk-months"}, description = "Months per backfill chunk", defaultValue = "6")
    int chunkMonths;

    @CommandLine.Option(names = {"-s", "--start"}, description = "Start date (yyyy-MM-dd)", defaultValue = "2020-01-01")
    LocalDate startDate;

    @CommandLine.Option(names = {"-e", "--end"}, description = "End date (yyyy-MM-dd); default today")
    LocalDate endDate;

    public static void main(String[] args) {
        int code = new CommandLine(new YahooFinanceCsvMain()).execute(args);
        System.exit(code);
    }

    @Override
    public Integer call() throws Exception {
        if (endDate == null) endDate = LocalDate.now(ZoneOffset.UTC);
        if (startDate.isAfter(endDate)) {
            System.err.println("Start date must be on/before end date");
            return 2;
        }

        MissingRangeSource source = new MissingRangeSource(tickers, Path.of(outDir), startDate, endDate, Math.max(1, chunkMonths));
        MetricRegistry registry = new MetricRegistry();
        YahooDailyCsvTransform transform = new YahooDailyCsvTransform(new HttpYahooClient(), "1d", registry);
        Sink<FinancialCsv> sink = new FinancialCsvSink(Path.of(outDir));
        var budget = new SimpleBudgetManager(
                Math.max(2, Runtime.getRuntime().availableProcessors() / 2),
                64L * 1024 * 1024, // 64MB logical mem permits
                0,                 // no IO limit
                5,                 // external QPS budget to be polite
                null);
        var retry = new ExponentialBackoffRetryPolicy(3, 50, 1_000);

        Pipeline<RangeRequest, FinancialCsv> pipeline = new PipelineBuilder<RangeRequest, FinancialCsv>()
                .source(source)
                .transform(transform)
                .sink(sink)
                .budget(budget)
                .retry(retry)
                .workers(4)
                .queueCapacity(64)
                .maxInFlight(32)
                .sinkBatchSize(1)
                .sinkFlushEveryMillis(500)
                .metrics(registry)
                .build();

        pipeline.start();

        Thread printer = new Thread(() -> printMetricsEvery(registry, pipeline, 15_000), "metrics-printer");
        printer.setDaemon(true);
        printer.start();

        // Wait for completion: finite source -> when finished and no inflight and empty queue
        while (true) {
            if (source.isFinished() && pipeline.getInflight() == 0 && pipeline.getQueueSize() == 0) break;
            try { Thread.sleep(200); } catch (InterruptedException ie) { Thread.currentThread().interrupt(); break; }
        }

        // One last metrics print and per-ticker summary
        printOnce(registry, pipeline);

        java.util.Map<String,Integer> before = source.initialCounts();
        java.util.Map<String,Integer> after = new java.util.HashMap<>();
        for (String t : tickers) {
            java.nio.file.Path p = Path.of(outDir).resolve(t + ".csv");
            int lines = 0;
            if (java.nio.file.Files.exists(p)) {
                try (java.io.BufferedReader br = java.nio.file.Files.newBufferedReader(p)) {
                    while (br.readLine() != null) lines++;
                }
                // subtract header
                if (lines > 0) lines -= 1;
            }
            after.put(t, lines);
        }
        System.out.println("Per-ticker summary:");
        for (String t : tickers) {
            int b = before.getOrDefault(t, 0);
            int a = after.getOrDefault(t, 0);
            int added = Math.max(0, a - b);
            int windows = source.windowCounts().getOrDefault(t, 0);
            System.out.println("  " + t + ": before=" + b + " after=" + a + " added=" + added + " plannedWindows=" + windows);
        }
        pipeline.stop();
        System.out.println("Saved CSVs for tickers: " + tickers + " to " + outDir);
        return 0;
    }

    private static void printMetricsEvery(MetricRegistry r, Pipeline<?, ?> p, long millis) {
        while (p.isRunning()) {
            printOnce(r, p);
            try { Thread.sleep(millis); } catch (InterruptedException ie) { Thread.currentThread().interrupt(); return; }
        }
        // final
        printOnce(r, p);
    }

    private static void printOnce(MetricRegistry r, Pipeline<?, ?> p) {
        Meter in = r.meter("pipeline.input.rate");
        Meter out = r.meter("pipeline.output.rate");
        Meter err = r.meter("pipeline.error.rate");
        Timer src = r.timer("pipeline.source.time");
        Timer xfm = r.timer("pipeline.transform.time");
        Timer snk = r.timer("pipeline.sink.time");
        long flushes = r.counter("pipeline.sink.batch.flushes").getCount();
        long timedFlushes = r.counter("pipeline.sink.batch.flushes.timed").getCount();

        String now = Instant.now().toString();
        System.out.println("[" + now + "] metrics:" +
                " inCnt=" + in.getCount() + " 1m=" + fmt(in.getOneMinuteRate()) +
                " | outCnt=" + out.getCount() + " 1m=" + fmt(out.getOneMinuteRate()) +
                " | errCnt=" + err.getCount() + " 1m=" + fmt(err.getOneMinuteRate()) +
                " | qSize=" + p.getQueueSize() + " inflight=" + p.getInflight() +
                " | %src=" + fmt(p.sourcePct()) + " %xfm=" + fmt(p.transformPct()) + " %snk=" + fmt(p.sinkPct()) +
                " | flushes=" + flushes + " timedFlushes=" + timedFlushes +
                " | t.p50(ms)=" + nsToMs(p50(src)) + "/" + nsToMs(p50(xfm)) + "/" + nsToMs(p50(snk))
        );
    }

    private static double p50(Timer t) {
        if (t == null) return 0;
        var s = t.getSnapshot();
        return s.getMedian();
    }

    private static String fmt(double v) { return String.format("%.3f", v); }
    private static String nsToMs(double nanos) { return String.format("%.3f", nanos / 1_000_000.0); }
}
