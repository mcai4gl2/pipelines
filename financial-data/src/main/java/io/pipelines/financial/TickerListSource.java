package io.pipelines.financial;

import io.pipelines.core.Record;
import io.pipelines.core.Source;

import java.util.List;
import java.util.Optional;

/**
 * Emits a fixed list of tickers as records, then completes.
 */
public class TickerListSource implements Source<String> {
    private final List<String> tickers;
    private int idx = 0;

    public TickerListSource(List<String> tickers) {
        this.tickers = List.copyOf(tickers);
    }

    @Override
    public Optional<Record<String>> poll() {
        if (idx >= tickers.size()) return Optional.empty();
        Record<String> r = new Record<>(idx, 0, tickers.get(idx));
        idx++;
        return Optional.of(r);
    }

    @Override
    public boolean isFinished() {
        return idx >= tickers.size();
    }
}
