package io.pipelines.financial;

import com.codahale.metrics.MetricRegistry;
import java.nio.charset.StandardCharsets;
import io.pipelines.core.Record;
import io.pipelines.core.Transform;

import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Fetches daily price data from Yahoo Finance v8 chart API for a specific range and outputs CSV chunk bytes
 * (no header). It filters out any dates already present in the ticker file using the provided existingDates set.
 */
public class YahooDailyCsvTransform implements Transform<RangeRequest, FinancialCsv> {
    private final YahooClient client;
    private final String interval;   // e.g., 1d
    private final MetricRegistry registry; // optional
    private final java.util.Map<String,Integer> consecutiveFailures = new java.util.concurrent.ConcurrentHashMap<>();

    public YahooDailyCsvTransform() { this(new HttpYahooClient(), "1d", null); }
    public YahooDailyCsvTransform(YahooClient client, String interval) { this(client, interval, null); }
    public YahooDailyCsvTransform(YahooClient client, String interval, MetricRegistry registry) {
        this.client = client;
        this.interval = interval;
        this.registry = registry;
    }

    @Override
    public List<Record<FinancialCsv>> apply(Record<RangeRequest> input) throws Exception {
        RangeRequest req = input.payload();
        String ticker = req.ticker();
        LocalDate start = req.start();
        LocalDate end = req.end();
        Set<LocalDate> existing = req.existingDates();

        long p1 = start.atStartOfDay().toEpochSecond(ZoneOffset.UTC);
        long p2 = end.plusDays(1).atStartOfDay().toEpochSecond(ZoneOffset.UTC) - 1; // inclusive end
        if (registry != null) registry.counter("yahoo.fetch.windows").inc();
        String body;
        try {
            body = client.fetch(ticker, p1, p2, interval);
        } catch (Exception e) {
            if (registry != null) registry.counter("yahoo.fetch.failures").inc();
            int streak = consecutiveFailures.merge(ticker, 1, Integer::sum);
            if (streak >= 5 && streak % 5 == 0) {
                System.err.println("[yahoo] failures streak=" + streak + " ticker=" + ticker + " lastWindow=" + start + ".." + end + " err=" + e.getMessage());
            }
            throw new RuntimeException("yahoo fetch failed for " + ticker + " window " + start + ".." + end + ", attempts=" + consecutiveFailures, e);
        }
        // reset failure streak on success
        consecutiveFailures.remove(ticker);
        long[] timestamps = extractLongArray(body, "\"timestamp\"\\s*:");
        double[] open = extractDoubleArray(body, "\"open\"\\s*:");
        double[] high = extractDoubleArray(body, "\"high\"\\s*:");
        double[] low = extractDoubleArray(body, "\"low\"\\s*:");
        double[] close = extractDoubleArray(body, "\"close\"\\s*:");
        long[] volume = extractLongArray(body, "\"volume\"\\s*:");

        int n = Math.min(timestamps.length,
                Math.min(open.length, Math.min(high.length, Math.min(low.length, Math.min(close.length, volume.length)))));

        StringBuilder chunk = new StringBuilder();
        for (int i = 0; i < n; i++) {
            LocalDate d = Instant.ofEpochSecond(timestamps[i]).atZone(ZoneOffset.UTC).toLocalDate();
            if (d.isBefore(start) || d.isAfter(end)) continue;
            if (existing.contains(d)) continue; // skip already present
            chunk.append(d).append(',')
                 .append(num(open[i])).append(',')
                 .append(num(high[i])).append(',')
                 .append(num(low[i])).append(',')
                 .append(num(close[i])).append(',')
                 .append(volume[i]).append('\n');
        }

        int rows = (int) chunk.chars().filter(ch -> ch == '\n').count();
        if (rows == 0) {
            if (registry != null) registry.counter("yahoo.fetch.zeroRows").inc();
            return List.of();
        }
        FinancialCsv out = new FinancialCsv(ticker + ".csv", chunk.toString().getBytes(StandardCharsets.UTF_8), ticker, rows);
        if (registry != null) {
            registry.counter("yahoo.rows.appended").inc(rows);
            registry.counter("yahoo.chunks.appended").inc();
        }
        List<Record<FinancialCsv>> list = new ArrayList<>(1);
        list.add(new Record<>(input.seq(), 0, out));
        return list;
    }

    // Very narrow JSON array extraction for numeric arrays like: "timestamp":[1696118400, ...]
    private static long[] extractLongArray(String json, String keyRegex) {
        Optional<String> arr = extractArray(json, keyRegex);
        if (arr.isEmpty()) return new long[0];
        String[] parts = arr.get().split(",");
        long[] out = new long[parts.length];
        for (int i = 0; i < parts.length; i++) {
            String p = parts[i].trim();
            if (p.equals("null") || p.isEmpty()) out[i] = 0L; else out[i] = (long) Double.parseDouble(p);
        }
        return out;
    }

    private static double[] extractDoubleArray(String json, String keyRegex) {
        Optional<String> arr = extractArray(json, keyRegex);
        if (arr.isEmpty()) return new double[0];
        String[] parts = arr.get().split(",");
        double[] out = new double[parts.length];
        for (int i = 0; i < parts.length; i++) {
            String p = parts[i].trim();
            if (p.equals("null") || p.isEmpty()) out[i] = Double.NaN; else out[i] = Double.parseDouble(p);
        }
        return out;
    }

    private static Optional<String> extractArray(String json, String keyRegex) {
        Pattern p = Pattern.compile(keyRegex + "\\s*\\[(.*?)\\]", Pattern.DOTALL);
        Matcher m = p.matcher(json);
        if (m.find()) {
            return Optional.ofNullable(m.group(1));
        }
        return Optional.empty();
    }

    private static String num(double d) {
        if (Double.isNaN(d)) return "";
        return Double.toString(d);
    }
}
