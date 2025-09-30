package io.pipelines.financial;

public interface YahooClient {
    String fetch(String ticker, long period1, long period2, String interval) throws Exception;
}

