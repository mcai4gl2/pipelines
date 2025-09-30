package io.pipelines.financial;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;

final class HttpYahooClient implements YahooClient {
    private final HttpClient http = HttpClient.newHttpClient();

    @Override
    public String fetch(String ticker, long period1, long period2, String interval) throws Exception {
        String url = String.format(
                "https://query1.finance.yahoo.com/v8/finance/chart/%s?interval=%s&period1=%d&period2=%d",
                java.net.URLEncoder.encode(ticker, java.nio.charset.StandardCharsets.UTF_8),
                java.net.URLEncoder.encode(interval, java.nio.charset.StandardCharsets.UTF_8),
                period1, period2);
        HttpRequest req = HttpRequest.newBuilder(URI.create(url))
                .header("User-Agent", "Mozilla/5.0")
                .GET()
                .build();
        HttpResponse<String> resp = null;
        int attempts = 0;
        while (attempts < 3) {
            attempts++;
            resp = http.send(req, HttpResponse.BodyHandlers.ofString());
            if (resp.statusCode() == 200) break;
            if (attempts < 3) Thread.sleep(250L * attempts);
        }
        if (resp == null || resp.statusCode() != 200) throw new RuntimeException("Yahoo fetch failed: " + (resp==null?"none":resp.statusCode()));
        return resp.body();
    }
}

