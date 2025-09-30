package io.pipelines.financial;

/**
 * CSV payload chunk for a ticker file. rowsAdded helps build per-ticker summaries.
 */
public record FinancialCsv(String fileName, byte[] data, String ticker, int rowsAdded) {}
