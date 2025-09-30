package io.pipelines.transform;

import com.sun.net.httpserver.Headers;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import io.pipelines.budget.SimpleBudgetManager;
import io.pipelines.core.Record;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

public class HttpExternalTransformTest {
    HttpServer server;

    @BeforeEach
    void startServer() throws IOException {
        server = HttpServer.create(new InetSocketAddress(0), 0);
        server.createContext("/echo", new EchoHandler());
        server.start();
    }

    @AfterEach
    void stopServer() { server.stop(0); }

    @Test
    void posts_and_returns_response() throws Exception {
        int port = server.getAddress().getPort();
        var budget = new SimpleBudgetManager(4, 1024 * 1024, 0, 10_000, null);
        var t = new HttpExternalTransform(new URI("http://127.0.0.1:" + port + "/echo"), budget, Duration.ofSeconds(2));
        List<Record<byte[]>> out = t.apply(new Record<>(1, 0, "hello".getBytes(StandardCharsets.UTF_8)));
        assertEquals(1, out.size());
        assertEquals("hello", new String(out.get(0).payload(), StandardCharsets.UTF_8));
    }

    @Test
    void async_posts_and_returns_response_without_blocking() throws Exception {
        int port = server.getAddress().getPort();
        var limiter = new io.pipelines.budget.AsyncQpsLimiter(1000);
        var t = new HttpExternalTransformAsync(new URI("http://127.0.0.1:" + port + "/echo"), Duration.ofSeconds(2), limiter);
        var fut = t.applyAsync(new Record<>(2, 0, "hi".getBytes(StandardCharsets.UTF_8)));
        List<Record<byte[]>> out = fut.toCompletableFuture().get();
        assertEquals(1, out.size());
        assertEquals("hi", new String(out.get(0).payload(), StandardCharsets.UTF_8));
    }

    static class EchoHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            byte[] body = exchange.getRequestBody().readAllBytes();
            Headers h = exchange.getResponseHeaders();
            h.add("Content-Type", "application/octet-stream");
            exchange.sendResponseHeaders(200, body.length);
            try (OutputStream os = exchange.getResponseBody()) {
                os.write(body);
            }
        }
    }
}
