package io.pipelines.admin;

import com.codahale.metrics.MetricRegistry;
import io.pipelines.budget.SimpleBudgetManager;
import io.pipelines.core.Record;
import io.pipelines.core.Source;
import io.pipelines.grpc.PipelineAdminServer;
import io.pipelines.retry.ExponentialBackoffRetryPolicy;
import io.pipelines.runtime.Pipeline;
import io.pipelines.runtime.PipelineBuilder;
import io.pipelines.sink.FileBytesSink;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;

public class AdminServerTest {
    AdminServer http;
    PipelineAdminServer grpc;
    Pipeline<byte[], byte[]> pipeline;

    @AfterEach
    void tearDown() {
        if (http != null) http.close();
        if (grpc != null) grpc.close();
        if (pipeline != null) pipeline.close();
    }

    static class FixedSource implements Source<byte[]> {
        private int i = 0;
        private final int max;
        FixedSource(int max) { this.max = max; }
        @Override public Optional<Record<byte[]>> poll() { if (i>=max) return Optional.empty(); return Optional.of(new Record<>(i, 0, ("r"+i++).getBytes())); }
        @Override public boolean isFinished() { return i>=max; }
    }

    private static int freePort() throws Exception {
        try (java.net.ServerSocket s = new java.net.ServerSocket(0)) { return s.getLocalPort(); }
    }

    @Test
    void serves_status_metrics_and_openapi() throws Exception {
        int adminPort = freePort();
        int grpcPort = adminPort + 1;
        Path out = Files.createTempDirectory("admin-test");

        var src = new FixedSource(5);
        var sink = new FileBytesSink(out);
        var registry = new MetricRegistry();
        pipeline = new PipelineBuilder<byte[], byte[]>()
                .source(src)
                .transform(r -> java.util.List.of(new Record<>(r.seq(), 0, r.payload())))
                .sink(sink)
                .budget(new SimpleBudgetManager(2, 1024*1024, 0, 0, null))
                .retry(new ExponentialBackoffRetryPolicy(2, 1, 10))
                .workers(2)
                .queueCapacity(8)
                .metrics(registry)
                .build();
        grpc = new PipelineAdminServer(grpcPort, pipeline, registry, out.resolve("dlq_transform.jsonl"), out.resolve("dlq_sink.jsonl"));
        http = new AdminServer(adminPort, grpcPort, out.resolve("dlq_transform.jsonl"), out.resolve("dlq_sink.jsonl"));
        pipeline.start();
        grpc.start();
        http.start();

        HttpClient client = HttpClient.newHttpClient();
        HttpResponse<String> status = client.send(HttpRequest.newBuilder(URI.create("http://127.0.0.1:"+adminPort+"/status")).GET().build(), HttpResponse.BodyHandlers.ofString());
        assertEquals(200, status.statusCode());
        assertTrue(status.body().contains("running"));

        HttpResponse<String> metrics = client.send(HttpRequest.newBuilder(URI.create("http://127.0.0.1:"+adminPort+"/metrics")).GET().build(), HttpResponse.BodyHandlers.ofString());
        assertEquals(200, metrics.statusCode());
        assertTrue(metrics.body().contains("sourceCount"));
        assertTrue(metrics.body().contains("routeStats"));

        HttpResponse<String> openapi = client.send(HttpRequest.newBuilder(URI.create("http://127.0.0.1:"+adminPort+"/openapi.yaml")).GET().build(), HttpResponse.BodyHandlers.ofString());
        assertEquals(200, openapi.statusCode());
        assertTrue(openapi.body().startsWith("openapi:"));
        assertTrue(openapi.body().contains("/grpc/PipelineAdmin/GetStatus"));
    }

    @Test
    void rest_to_grpc_bridge_invokes_unary() throws Exception {
        int adminPort = freePort();
        int grpcPort = adminPort + 1;
        Path out = Files.createTempDirectory("admin-test2");

        var src = new FixedSource(1);
        var sink = new FileBytesSink(out);
        var registry = new MetricRegistry();
        pipeline = new PipelineBuilder<byte[], byte[]>()
                .source(src)
                .transform(r -> java.util.List.of(new Record<>(r.seq(), 0, r.payload())))
                .sink(sink)
                .budget(new SimpleBudgetManager(2, 1024*1024, 0, 0, null))
                .retry(new ExponentialBackoffRetryPolicy(2, 1, 10))
                .workers(1)
                .queueCapacity(4)
                .metrics(registry)
                .build();
        grpc = new PipelineAdminServer(grpcPort, pipeline, registry, out.resolve("dlq_transform.jsonl"), out.resolve("dlq_sink.jsonl"));
        http = new AdminServer(adminPort, grpcPort, out.resolve("dlq_transform.jsonl"), out.resolve("dlq_sink.jsonl"));
        pipeline.start();
        grpc.start();
        http.start();

        HttpClient client = HttpClient.newHttpClient();
        HttpRequest req = HttpRequest.newBuilder(URI.create("http://127.0.0.1:"+adminPort+"/grpc/PipelineAdmin/GetStatus"))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString("{}"))
                .build();
        HttpResponse<String> resp = client.send(req, HttpResponse.BodyHandlers.ofString());
        assertEquals(200, resp.statusCode());
        assertTrue(resp.body().contains("running"));
    }
}
