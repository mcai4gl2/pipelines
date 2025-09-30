package io.pipelines.financial;

import io.pipelines.core.Record;
import io.pipelines.core.Source;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.util.*;
import java.util.function.Predicate;

/**
 * Builds a backfill plan per ticker by inspecting existing CSV files and emitting only missing ranges,
 * split into 6-month windows, ordered from oldest to newest to keep file chronological.
 */
public class MissingRangeSource implements Source<RangeRequest> {
    private final List<RangeRequest> plan;
    private int idx = 0;
    private final Map<String, Integer> initialCounts = new HashMap<>();
    private final Map<String, Integer> windowCounts = new HashMap<>();

    public MissingRangeSource(List<String> tickers, Path outDir) {
        this(tickers, outDir, LocalDate.of(1970,1,1), LocalDate.now(ZoneOffset.UTC), 6);
    }

    public MissingRangeSource(List<String> tickers, Path outDir, LocalDate startBound, LocalDate endBound, int monthsPerChunk) {
        this.plan = new ArrayList<>();
        for (String t : tickers) {
            Set<LocalDate> existing = readExisting(outDir.resolve(t + ".csv"));
            initialCounts.put(t, existing.size());
            boolean isFx = t != null && t.contains("=X");
            LocalDate today = LocalDate.now(ZoneOffset.UTC);
            LocalDate effectiveEnd = endBound.isAfter(today.minusDays(1)) ? today.minusDays(1) : endBound;
            java.util.function.Predicate<LocalDate> isTradingDay = isFx ? TradingCalendars::isWeekday : TradingCalendars::isUsEquityTradingDay;
            List<Range> missing = computeMissingRanges(existing, startBound, effectiveEnd, isTradingDay);
            List<Range> chunks = splitIntoChunks(rangesSorted(missing), monthsPerChunk, isTradingDay);
            windowCounts.put(t, chunks.size());
            for (Range r : chunks) {
                plan.add(new RangeRequest(t, r.start, r.end, existing));
            }
        }
    }

    private static List<Range> rangesSorted(List<Range> in) {
        in.sort(Comparator.comparing(a -> a.start));
        return in;
    }

    private static Set<LocalDate> readExisting(Path path) {
        Set<LocalDate> dates = new HashSet<>();
        if (!Files.exists(path)) return dates;
        try (BufferedReader br = Files.newBufferedReader(path, StandardCharsets.UTF_8)) {
            String line; boolean first = true;
            while ((line = br.readLine()) != null) {
                if (first) { first = false; continue; }
                if (line.isBlank()) continue;
                int comma = line.indexOf(',');
                if (comma <= 0) continue;
                try { dates.add(LocalDate.parse(line.substring(0, comma))); } catch (Exception ignore) {}
            }
        } catch (IOException ignore) {}
        return dates;
    }

    private static List<Range> computeMissingRanges(Set<LocalDate> existing, LocalDate start, LocalDate endInclusive, Predicate<LocalDate> isTradingDay) {
        List<Range> out = new ArrayList<>();
        LocalDate curRangeStart = null;
        LocalDate cur = start;
        while (!cur.isAfter(endInclusive)) {
            boolean nonTrading = !isTradingDay.test(cur);
            boolean hasData = existing.contains(cur);

            if (hasData) {
                if (curRangeStart != null) {
                    out.add(new Range(curRangeStart, cur.minusDays(1)));
                    curRangeStart = null;
                }
            } else {
                if (curRangeStart == null) {
                    // start a new range only on a trading day; skip starting on non-trading
                    if (!nonTrading) curRangeStart = cur;
                } // else in an open range: keep it open across non-trading days by not closing
            }
            cur = cur.plusDays(1);
        }
        if (curRangeStart != null) out.add(new Range(curRangeStart, endInclusive));
        return out;
    }

    private static List<Range> splitIntoChunks(List<Range> ranges, int monthsPerChunk, java.util.function.Predicate<LocalDate> isTradingDay) {
        List<Range> out = new ArrayList<>();
        for (Range r : ranges) {
            LocalDate s = r.start;
            while (!s.isAfter(r.end)) {
                // Advance s to next trading day
                while (!isTradingDay.test(s) && !s.isAfter(r.end)) s = s.plusDays(1);
                if (s.isAfter(r.end)) break;
                LocalDate eCandidate = s.plusMonths(monthsPerChunk).minusDays(1);
                if (eCandidate.isAfter(r.end)) eCandidate = r.end;
                // Retreat end to previous trading day
                LocalDate e = eCandidate;
                while (!isTradingDay.test(e) && e.isAfter(s)) e = e.minusDays(1);
                if (e.isBefore(s)) { s = eCandidate.plusDays(1); continue; }
                out.add(new Range(s, e));
                s = e.plusDays(1);
            }
        }
        return out;
    }

    private record Range(LocalDate start, LocalDate end) {}

    @Override
    public Optional<Record<RangeRequest>> poll() {
        if (idx >= plan.size()) return Optional.empty();
        Record<RangeRequest> r = new Record<>(idx, 0, plan.get(idx));
        idx++;
        return Optional.of(r);
    }

    @Override
    public boolean isFinished() {
        return idx >= plan.size();
    }

    public Map<String, Integer> initialCounts() { return new HashMap<>(initialCounts); }
    public Map<String, Integer> windowCounts() { return new HashMap<>(windowCounts); }
}
