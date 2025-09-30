package io.pipelines.transform;

import io.pipelines.budget.Budget;
import io.pipelines.core.Record;
import io.pipelines.core.Transform;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.List;

/**
 * Simple HTTP transform that posts payload bytes and returns response bytes.
 */
public class HttpExternalTransform implements Transform<byte[], byte[]> {
    private final HttpClient client;
    private final URI uri;
    private final Budget budget;
    private final Duration timeout;

    public HttpExternalTransform(URI uri, Budget budget, Duration timeout) {
        this.client = HttpClient.newHttpClient();
        this.uri = uri;
        this.budget = budget;
        this.timeout = timeout == null ? Duration.ofSeconds(5) : timeout;
    }

    @Override
    public List<Record<byte[]>> apply(Record<byte[]> input) throws Exception {
        budget.acquireExternalOp();
        HttpRequest req = HttpRequest.newBuilder(uri)
                .timeout(timeout)
                .header("Content-Type", "application/octet-stream")
                .POST(HttpRequest.BodyPublishers.ofByteArray(input.payload()))
                .build();
        HttpResponse<byte[]> resp = client.send(req, HttpResponse.BodyHandlers.ofByteArray());
        return List.of(new Record<>(input.seq(), 0, resp.body()));
    }
}

