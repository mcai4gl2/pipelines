package io.pipelines.admin;

import com.sun.net.httpserver.Headers;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.pipelines.grpc.MetricsSummary;
import io.pipelines.grpc.Empty;
import io.pipelines.grpc.PipelineAdminGrpc;
import io.pipelines.grpc.ControlRequest;
import io.pipelines.grpc.Status;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.Executors;
import java.nio.file.Path;
import com.google.protobuf.Descriptors;
import io.pipelines.grpc.PipelinesProto;
import io.grpc.reflection.v1alpha.ServerReflectionGrpc;
import io.grpc.reflection.v1alpha.ServerReflectionRequest;
import io.grpc.reflection.v1alpha.ServerReflectionResponse;
import com.google.protobuf.DescriptorProtos;

/**
 * Minimal admin server providing a status page and JSON metrics.
 */
public class AdminServer implements AutoCloseable {
    private final HttpServer server;
    private final ManagedChannel channel;
    private final PipelineAdminGrpc.PipelineAdminBlockingStub stub;
    private final Path dlqTransform;
    private final Path dlqSink;

    public AdminServer(int port, int grpcPort, Path dlqTransform, Path dlqSink) throws IOException {
        this.server = HttpServer.create(new InetSocketAddress(port), 0);
        this.channel = ManagedChannelBuilder.forAddress("127.0.0.1", grpcPort).usePlaintext().build();
        this.stub = PipelineAdminGrpc.newBlockingStub(channel);
        this.dlqTransform = dlqTransform;
        this.dlqSink = dlqSink;
        server.createContext("/", new StaticHandler("/public/index.html"));
        server.createContext("/metrics", new MetricsHandler());
        server.createContext("/status", new StatusHandler());
        server.createContext("/control", new ControlHandler());
        server.createContext("/openapi.yaml", new OpenApiHandler());
        server.createContext("/grpc", new GrpcBridgeHandler());
        server.createContext("/errors", new ErrorsHandler());
        server.createContext("/swagger", new StaticHandler("/public/swagger.html"));
        server.setExecutor(Executors.newCachedThreadPool());
    }

    public void start() { server.start(); }

    @Override
    public void close() { server.stop(0); channel.shutdownNow(); }

    private class StaticHandler implements HttpHandler {
        private final String resourcePath;
        StaticHandler(String resourcePath) { this.resourcePath = resourcePath; }
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            Headers h = exchange.getResponseHeaders();
            h.add("Content-Type", "text/html; charset=utf-8");
            try (InputStream in = AdminServer.class.getResourceAsStream(resourcePath)) {
                if (in == null) {
                    String notFound = "No admin page";
                    exchange.sendResponseHeaders(404, notFound.length());
                    try (OutputStream os = exchange.getResponseBody()) { os.write(notFound.getBytes(StandardCharsets.UTF_8)); }
                    return;
                }
                byte[] bytes = in.readAllBytes();
                exchange.sendResponseHeaders(200, bytes.length);
                try (OutputStream os = exchange.getResponseBody()) { os.write(bytes); }
            }
        }
    }

    private class GrpcBridgeHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            if (!"POST".equalsIgnoreCase(exchange.getRequestMethod())) {
                exchange.sendResponseHeaders(405, -1); return;
            }
            String[] parts = exchange.getRequestURI().getPath().split("/");
            if (parts.length < 4) { exchange.sendResponseHeaders(404, -1); return; }
            String svc = parts[2];
            String mth = parts[3];
            String body = new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8);
            try {
                // Resolve method via reflection descriptors
                var files = new OpenApiHandler().fetchDescriptorsViaReflection();
                Descriptors.MethodDescriptor method = null;
                String fullServiceName = null;
                for (var fd : files) {
                    for (var sd : fd.getServices()) {
                        if (sd.getName().equals(svc)) {
                            method = sd.findMethodByName(mth);
                            if (method != null) fullServiceName = sd.getFullName();
                            if (method != null) break;
                        }
                    }
                    if (method != null) break;
                }
                if (method == null) {
                    // Fallback to generated descriptor (known services)
                    Descriptors.FileDescriptor gen = PipelinesProto.getDescriptor();
                    for (var sd : gen.getServices()) {
                        if (sd.getName().equals(svc)) { method = sd.findMethodByName(mth); fullServiceName = sd.getFullName(); break; }
                    }
                    if (method == null) { exchange.sendResponseHeaders(404, -1); return; }
                }
                // Build DynamicMessage from JSON
                com.google.protobuf.DynamicMessage.Builder inBuilder = com.google.protobuf.DynamicMessage.newBuilder(method.getInputType());
                com.google.protobuf.util.JsonFormat.parser().ignoringUnknownFields().merge(body, inBuilder);
                com.google.protobuf.DynamicMessage inMsg = inBuilder.build();
                // Invoke unary via generic stub
                io.grpc.MethodDescriptor<com.google.protobuf.DynamicMessage, com.google.protobuf.DynamicMessage> desc =
                        io.grpc.MethodDescriptor.<com.google.protobuf.DynamicMessage, com.google.protobuf.DynamicMessage>newBuilder()
                                .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
                                .setFullMethodName(io.grpc.MethodDescriptor.generateFullMethodName(fullServiceName != null ? fullServiceName : svc, mth))
                                .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(inMsg))
                                .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(com.google.protobuf.DynamicMessage.newBuilder(method.getOutputType()).build()))
                                .build();
                com.google.protobuf.DynamicMessage out = io.grpc.stub.ClientCalls.blockingUnaryCall(channel, desc, io.grpc.CallOptions.DEFAULT, inMsg);
                String json = com.google.protobuf.util.JsonFormat.printer().includingDefaultValueFields().print(out);
                byte[] bytes = json.getBytes(StandardCharsets.UTF_8);
                exchange.getResponseHeaders().add("Content-Type", "application/json");
                exchange.sendResponseHeaders(200, bytes.length);
                try (OutputStream os = exchange.getResponseBody()) { os.write(bytes); }
            } catch (Exception e) {
                String err = "{\"error\":\"" + e.getMessage().replace("\"", "'") + "\"}";
                byte[] bytes = err.getBytes(StandardCharsets.UTF_8);
                exchange.getResponseHeaders().add("Content-Type", "application/json");
                exchange.sendResponseHeaders(500, bytes.length);
                try (OutputStream os = exchange.getResponseBody()) { os.write(bytes); }
            }
        }
    }

    private class MetricsHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            String json = toJson();
            byte[] bytes = json.getBytes(StandardCharsets.UTF_8);
            exchange.getResponseHeaders().add("Content-Type", "application/json");
            exchange.sendResponseHeaders(200, bytes.length);
            try (OutputStream os = exchange.getResponseBody()) { os.write(bytes); }
        }

        private String toJson() {
            MetricsSummary m = stub.getMetrics(Empty.getDefaultInstance());
            StringBuilder sb = new StringBuilder();
            sb.append('{');
            sb.append("\"sourceCount\":").append(m.getSourceCount()).append(',');
            sb.append("\"transformCount\":").append(m.getTransformCount()).append(',');
            sb.append("\"sinkCount\":").append(m.getSinkCount()).append(',');
            sb.append("\"sourceMeanMs\":").append(m.getSourceMeanMs()).append(',');
            sb.append("\"transformMeanMs\":").append(m.getTransformMeanMs()).append(',');
            sb.append("\"sinkMeanMs\":").append(m.getSinkMeanMs()).append(',');
            sb.append(String.format("\"sourcePct\":%.2f,\"transformPct\":%.2f,\"sinkPct\":%.2f,\"sinkBatchFlushes\":%d,\"sinkBatchAvgSize\":%.2f,\"sinkBatchFlushP50Ms\":%.2f,\"sinkBatchFlushP95Ms\":%.2f,\"sinkBatchFlushP99Ms\":%.2f,\"sinkBatchSizeP50\":%.2f,\"sinkBatchSizeP95\":%.2f,\"sinkBatchSizeP99\":%.2f", m.getSourcePct(), m.getTransformPct(), m.getSinkPct(), m.getSinkBatchFlushes(), m.getSinkBatchAvgSize(), m.getSinkBatchFlushP50Ms(), m.getSinkBatchFlushP95Ms(), m.getSinkBatchFlushP99Ms(), m.getSinkBatchSizeP50(), m.getSinkBatchSizeP95(), m.getSinkBatchSizeP99()));
            // routeStats
            sb.append(',').append("\"routeStats\":[");
            for (int i = 0; i < m.getRouteStatsCount(); i++) {
                var rs = m.getRouteStats(i);
                sb.append('{')
                        .append("\"key\":\"").append(rs.getKey()).append("\",")
                        .append("\"transform\":").append(rs.getRoutedToTransform()).append(',')
                        .append("\"queue\":").append(rs.getRoutedToQueue()).append(',')
                        .append("\"drop\":").append(rs.getDropped()).append(',')
                        .append("\"transformRate1m\":").append(rs.getTransformRate1M()).append(',')
                        .append("\"queueRate1m\":").append(rs.getQueueRate1M()).append(',')
                        .append("\"dropRate1m\":").append(rs.getDropRate1M())
                        .append('}');
                if (i < m.getRouteStatsCount() - 1) sb.append(',');
            }
            sb.append(']');
            // queue depths
            sb.append(',').append("\"queueDepths\":[");
            for (int i = 0; i < m.getQueueDepthsCount(); i++) {
                var qd = m.getQueueDepths(i);
                sb.append('{')
                        .append("\"name\":\"").append(qd.getName()).append("\",")
                        .append("\"depth\":").append(qd.getDepth())
                        .append('}');
                if (i < m.getQueueDepthsCount() - 1) sb.append(',');
            }
            sb.append(']');
            sb.append('}');
            return sb.toString();
        }
    }

    private class StatusHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            Status s = stub.getStatus(Empty.getDefaultInstance());
            String json = "{" +
                    "\"running\":" + s.getRunning() + "," +
                    "\"paused\":" + s.getPaused() + "," +
                    "\"queueSize\":" + s.getQueueSize() + "," +
                    "\"timedFlushes\":" + s.getTimedFlushes() +
                    "}";
            byte[] bytes = json.getBytes(StandardCharsets.UTF_8);
            exchange.getResponseHeaders().add("Content-Type", "application/json");
            exchange.sendResponseHeaders(200, bytes.length);
            try (OutputStream os = exchange.getResponseBody()) { os.write(bytes); }
        }
    }

    private class ControlHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            String query = exchange.getRequestURI().getQuery();
            String action = null;
            if (query != null) {
                for (String part : query.split("&")) {
                    String[] kv = part.split("=", 2);
                    if (kv.length == 2 && kv[0].equals("action")) { action = kv[1]; break; }
                }
            }
            if (action != null) {
                stub.control(ControlRequest.newBuilder().setAction(action).build());
            }
            byte[] bytes = "{}".getBytes(StandardCharsets.UTF_8);
            exchange.getResponseHeaders().add("Content-Type", "application/json");
            exchange.sendResponseHeaders(200, bytes.length);
            try (OutputStream os = exchange.getResponseBody()) { os.write(bytes); }
        }
    }

    private class OpenApiHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            String yaml = buildOpenApiYaml();
            byte[] bytes = yaml.getBytes(StandardCharsets.UTF_8);
            exchange.getResponseHeaders().add("Content-Type", "application/yaml");
            exchange.sendResponseHeaders(200, bytes.length);
            try (OutputStream os = exchange.getResponseBody()) { os.write(bytes); }
        }

        private String buildOpenApiYaml() {
            StringBuilder sb = new StringBuilder();
            sb.append("openapi: 3.0.3\n");
            sb.append("info:\n  title: Pipeline Admin API (generated)\n  version: 0.1.0\n");
            sb.append("paths:\n");
            // Query reflection service for services and descriptors
            java.util.List<Descriptors.FileDescriptor> files = fetchDescriptorsViaReflection();
            java.util.Set<String> seenMsgs = new java.util.HashSet<>();
            for (Descriptors.FileDescriptor fd : files) {
                for (Descriptors.ServiceDescriptor svc : fd.getServices()) {
                    for (Descriptors.MethodDescriptor m : svc.getMethods()) {
                        String path = String.format("  /grpc/%s/%s:\n", svc.getName(), m.getName());
                        sb.append(path);
                        sb.append("    post:\n");
                        sb.append("      summary: ").append("Invoke ").append(svc.getName()).append('.').append(m.getName()).append("\n");
                        sb.append("      requestBody:\n");
                        sb.append("        required: true\n");
                        sb.append("        content:\n");
                        sb.append("          application/json:\n");
                        sb.append("            schema:\n");
                        sb.append("              $ref: '#/components/schemas/").append(m.getInputType().getName()).append("'\n");
                        sb.append("      responses:\n");
                        sb.append("        '200':\n");
                        sb.append("          description: OK\n");
                        sb.append("          content:\n");
                        sb.append("            application/json:\n");
                        sb.append("              schema:\n");
                        sb.append("                $ref: '#/components/schemas/").append(m.getOutputType().getName()).append("'\n");
                        // mark messages to emit schemas later
                        markMessageTypes(m, seenMsgs);
                    }
                }
            }
            sb.append("components:\n  schemas:\n");
            for (Descriptors.FileDescriptor fd : files) {
                for (Descriptors.Descriptor d : fd.getMessageTypes()) {
                    if (seenMsgs.contains(d.getName())) emitSchema(sb, d, new java.util.HashSet<>());
                }
            }
            return sb.toString();
        }

        private void markMessageTypes(Descriptors.MethodDescriptor m, java.util.Set<String> seen) {
            seen.add(m.getInputType().getName());
            seen.add(m.getOutputType().getName());
        }

        private java.util.List<Descriptors.FileDescriptor> fetchDescriptorsViaReflection() {
            java.util.List<Descriptors.FileDescriptor> result = new java.util.ArrayList<>();
            try {
                var stub = ServerReflectionGrpc.newStub(channel);
                java.util.concurrent.CountDownLatch latch = new java.util.concurrent.CountDownLatch(1);
                java.util.List<ServerReflectionResponse> responses = new java.util.ArrayList<>();
                io.grpc.stub.StreamObserver<ServerReflectionRequest> reqObs = stub.serverReflectionInfo(new io.grpc.stub.StreamObserver<>() {
                    @Override public void onNext(ServerReflectionResponse value) { responses.add(value); }
                    @Override public void onError(Throwable t) { latch.countDown(); }
                    @Override public void onCompleted() { latch.countDown(); }
                });
                // Ask for list of services
                reqObs.onNext(ServerReflectionRequest.newBuilder().setListServices("*").build());
                // Give reflection a moment to respond
                reqObs.onCompleted();
                latch.await(500, java.util.concurrent.TimeUnit.MILLISECONDS);
                java.util.Set<String> services = new java.util.HashSet<>();
                for (ServerReflectionResponse r : responses) {
                    if (r.hasListServicesResponse()) {
                        for (var s : r.getListServicesResponse().getServiceList()) services.add(s.getName());
                    }
                }
                // Request file descriptors for each service symbol
                java.util.Map<String, DescriptorProtos.FileDescriptorProto> protoByName = new java.util.HashMap<>();
                responses.clear();
                java.util.concurrent.CountDownLatch latch2 = new java.util.concurrent.CountDownLatch(1);
                io.grpc.stub.StreamObserver<ServerReflectionRequest> reqObs2 = stub.serverReflectionInfo(new io.grpc.stub.StreamObserver<>() {
                    @Override public void onNext(ServerReflectionResponse value) { responses.add(value); }
                    @Override public void onError(Throwable t) { latch2.countDown(); }
                    @Override public void onCompleted() { latch2.countDown(); }
                });
                for (String s : services) {
                    reqObs2.onNext(ServerReflectionRequest.newBuilder().setFileContainingSymbol(s).build());
                }
                reqObs2.onCompleted();
                latch2.await(1000, java.util.concurrent.TimeUnit.MILLISECONDS);
                for (ServerReflectionResponse r : responses) {
                    if (r.hasFileDescriptorResponse()) {
                        for (com.google.protobuf.ByteString b : r.getFileDescriptorResponse().getFileDescriptorProtoList()) {
                            DescriptorProtos.FileDescriptorProto p = DescriptorProtos.FileDescriptorProto.parseFrom(b);
                            protoByName.put(p.getName(), p);
                        }
                    }
                }
                // Build FileDescriptor with dependencies
                java.util.Map<String, Descriptors.FileDescriptor> fdByName = new java.util.HashMap<>();
                for (var p : protoByName.values()) {
                    buildFileDescriptor(p.getName(), protoByName, fdByName);
                }
                result.addAll(fdByName.values());
            } catch (Exception e) {
                // Fallback: return empty to avoid breaking endpoint
            }
            return result;
        }

        private Descriptors.FileDescriptor buildFileDescriptor(String name,
                java.util.Map<String, DescriptorProtos.FileDescriptorProto> protoByName,
                java.util.Map<String, Descriptors.FileDescriptor> fdByName) throws Descriptors.DescriptorValidationException {
            if (fdByName.containsKey(name)) return fdByName.get(name);
            DescriptorProtos.FileDescriptorProto p = protoByName.get(name);
            if (p == null) return null;
            Descriptors.FileDescriptor[] deps = new Descriptors.FileDescriptor[p.getDependencyCount()];
            for (int i = 0; i < p.getDependencyCount(); i++) {
                String depName = p.getDependency(i);
                deps[i] = buildFileDescriptor(depName, protoByName, fdByName);
            }
            Descriptors.FileDescriptor fd = Descriptors.FileDescriptor.buildFrom(p, deps);
            fdByName.put(name, fd);
            return fd;
        }

        private void emitSchema(StringBuilder sb, Descriptors.Descriptor d, java.util.Set<String> emitted) {
            if (!emitted.add(d.getName())) return;
            sb.append("    ").append(d.getName()).append(":\n");
            sb.append("      type: object\n");
            if (!d.getFields().isEmpty()) sb.append("      properties:\n");
            for (Descriptors.FieldDescriptor f : d.getFields()) {
                sb.append("        ").append(f.getName()).append(":\n");
                String typeLine = fieldTypeLine(f);
                sb.append(typeLine);
            }
            // emit dependencies
            for (Descriptors.FieldDescriptor f : d.getFields()) {
                if (f.getJavaType() == Descriptors.FieldDescriptor.JavaType.MESSAGE) {
                    emitSchema(sb, f.getMessageType(), emitted);
                }
            }
        }

        private String fieldTypeLine(Descriptors.FieldDescriptor f) {
            String indent = "          ";
            boolean repeated = f.isRepeated();
            StringBuilder sb = new StringBuilder();
            if (repeated) {
                sb.append(indent).append("type: array\n");
                sb.append(indent).append("items:\n");
                sb.append(indent).append("  ");
                if (f.getJavaType() == Descriptors.FieldDescriptor.JavaType.MESSAGE) {
                    sb.append("$ref: '#/components/schemas/").append(f.getMessageType().getName()).append("'\n");
                } else {
                    appendScalar(sb, f);
                }
                return sb.toString();
            }
            if (f.getJavaType() == Descriptors.FieldDescriptor.JavaType.MESSAGE) {
                sb.append(indent).append("$ref: '#/components/schemas/").append(f.getMessageType().getName()).append("'\n");
            } else {
                appendScalar(sb, f);
            }
            return sb.toString();
        }

        private void appendScalar(StringBuilder sb, Descriptors.FieldDescriptor f) {
            String indent = "          ";
            switch (f.getJavaType()) {
                case STRING -> sb.append(indent).append("type: string\n");
                case INT -> { sb.append(indent).append("type: integer\n"); sb.append(indent).append("format: int32\n"); }
                case LONG -> { sb.append(indent).append("type: integer\n"); sb.append(indent).append("format: int64\n"); }
                case FLOAT -> { sb.append(indent).append("type: number\n"); sb.append(indent).append("format: float\n"); }
                case DOUBLE -> { sb.append(indent).append("type: number\n"); sb.append(indent).append("format: double\n"); }
                case BOOLEAN -> sb.append(indent).append("type: boolean\n");
                case BYTE_STRING -> { sb.append(indent).append("type: string\n"); sb.append(indent).append("format: byte\n"); }
                case ENUM -> { sb.append(indent).append("type: string\n"); }
                case MESSAGE -> sb.append(indent).append("type: object\n"); // handled above normally
            }
        }
    }

    private class ErrorsHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            io.pipelines.grpc.Errors errs = stub.getErrors(io.pipelines.grpc.ErrorsRequest.newBuilder().setLimit(10).build());
            String json = "{" +
                    toJsonArray("transform", errs.getTransformList()) + "," +
                    toJsonArray("sink", errs.getSinkList()) +
                    "}";
            byte[] bytes = json.getBytes(StandardCharsets.UTF_8);
            exchange.getResponseHeaders().add("Content-Type", "application/json");
            exchange.sendResponseHeaders(200, bytes.length);
            try (OutputStream os = exchange.getResponseBody()) { os.write(bytes); }
        }

        private String toJsonArray(String key, java.util.List<String> lines) {
            StringBuilder sb = new StringBuilder();
            sb.append('\"').append(key).append("\":[");
            for (int i = 0; i < lines.size(); i++) {
                sb.append(lines.get(i));
                if (i < lines.size() - 1) sb.append(',');
            }
            sb.append(']');
            return sb.toString();
        }
    }
}
