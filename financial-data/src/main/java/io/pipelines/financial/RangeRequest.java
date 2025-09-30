package io.pipelines.financial;

import java.time.LocalDate;
import java.util.Set;

/**
 * Request to fetch a date range [start, end] for a ticker.
 * existingDates contains dates already present in the file to allow filtering.
 */
public record RangeRequest(String ticker, LocalDate start, LocalDate end, Set<LocalDate> existingDates) {}

