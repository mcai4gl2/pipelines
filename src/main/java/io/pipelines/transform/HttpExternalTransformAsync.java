package io.pipelines.transform;

import io.pipelines.core.AsyncTransform;
import io.pipelines.core.Record;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

/**
 * Non-blocking HTTP transform using HttpClient.sendAsync. Avoids blocking pipeline worker threads.
 */
public class HttpExternalTransformAsync implements AsyncTransform<byte[], byte[]> {
    private final HttpClient client;
    private final URI uri;
    private final Duration timeout;
    private final io.pipelines.budget.AsyncQpsLimiter qpsLimiter;
    private final io.pipelines.budget.Budget budget;
    private final com.codahale.metrics.MetricRegistry metrics;
    private final String qpsMetricName;

    public HttpExternalTransformAsync(URI uri, Duration timeout) {
        this.client = HttpClient.newBuilder().executor(Outbound.executor()).build();
        this.uri = uri;
        this.timeout = timeout == null ? Duration.ofSeconds(5) : timeout;
        this.qpsLimiter = null;
        this.budget = null;
        this.metrics = null;
        this.qpsMetricName = null;
    }

    public HttpExternalTransformAsync(URI uri, Duration timeout, io.pipelines.budget.AsyncQpsLimiter limiter) {
        this.client = HttpClient.newBuilder().executor(Outbound.executor()).build();
        this.uri = uri;
        this.timeout = timeout == null ? Duration.ofSeconds(5) : timeout;
        this.qpsLimiter = limiter;
        this.budget = null;
        this.metrics = null;
        this.qpsMetricName = null;
    }

    public HttpExternalTransformAsync(URI uri, Duration timeout, io.pipelines.budget.Budget budget) {
        this.client = HttpClient.newBuilder().executor(Outbound.executor()).build();
        this.uri = uri;
        this.timeout = timeout == null ? Duration.ofSeconds(5) : timeout;
        this.qpsLimiter = null;
        this.budget = budget;
        this.metrics = null;
        this.qpsMetricName = null;
    }

    public HttpExternalTransformAsync(URI uri, Duration timeout, io.pipelines.budget.AsyncQpsLimiter limiter, com.codahale.metrics.MetricRegistry metrics, String metricName) {
        this.client = HttpClient.newBuilder().executor(Outbound.executor()).build();
        this.uri = uri;
        this.timeout = timeout == null ? Duration.ofSeconds(5) : timeout;
        this.qpsLimiter = limiter;
        this.budget = null;
        this.metrics = metrics;
        this.qpsMetricName = metricName;
    }

    @Override
    public CompletionStage<List<Record<byte[]>>> applyAsync(Record<byte[]> input) {
        HttpRequest req = HttpRequest.newBuilder(uri)
                .timeout(timeout)
                .header("Content-Type", "application/octet-stream")
                .POST(HttpRequest.BodyPublishers.ofByteArray(input.payload()))
                .build();
        java.util.concurrent.CompletionStage<Void> gate;
        if (budget != null) gate = budget.acquireExternalOpAsync();
        else if (qpsLimiter != null) gate = qpsLimiter.acquire();
        else gate = java.util.concurrent.CompletableFuture.completedFuture(null);
        return gate.thenCompose(v -> {
                    if (metrics != null && qpsMetricName != null) metrics.counter(qpsMetricName).inc();
                    return client.sendAsync(req, HttpResponse.BodyHandlers.ofByteArray());
                })
                .thenApply(resp -> List.of(new Record<>(input.seq(), 0, resp.body())));
    }

    // Outbound HTTP executor isolation
    static final class Outbound {
        private static final java.util.concurrent.ExecutorService EXEC =
                java.util.concurrent.Executors.newFixedThreadPool(Integer.getInteger("pipelines.iohttp", 8), r -> {
                    Thread t = new Thread(r, "http-async"); t.setDaemon(true); return t;
                });
        static java.util.concurrent.ExecutorService executor() { return EXEC; }
    }
}
