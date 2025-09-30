package io.pipelines.cli;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.pipelines.grpc.*;

public class AdminCli {
    public static void main(String[] args) {
        if (args.length == 0) { usage(); return; }
        String host = System.getProperty("host", System.getenv().getOrDefault("PIPELINES_HOST", "127.0.0.1"));
        int port = Integer.parseInt(System.getProperty("port", System.getenv().getOrDefault("PIPELINES_GRPC_PORT", "8081")));
        ManagedChannel ch = ManagedChannelBuilder.forAddress(host, port).usePlaintext().build();
        PipelineAdminGrpc.PipelineAdminBlockingStub stub = PipelineAdminGrpc.newBlockingStub(ch);
        try {
            switch (args[0]) {
                case "status" -> {
                    Status s = stub.getStatus(Empty.getDefaultInstance());
                    System.out.printf("{\"running\":%s,\"paused\":%s,\"queueSize\":%d}%n", s.getRunning(), s.getPaused(), s.getQueueSize());
                }
                case "metrics" -> {
                    MetricsSummary m = stub.getMetrics(Empty.getDefaultInstance());
                    System.out.printf("{\"sourceCount\":%d,\"transformCount\":%d,\"sinkCount\":%d,\"sourceMeanMs\":%.2f,\"transformMeanMs\":%.2f,\"sinkMeanMs\":%.2f,\"sourcePct\":%.2f,\"transformPct\":%.2f,\"sinkPct\":%.2f}%n",
                            m.getSourceCount(), m.getTransformCount(), m.getSinkCount(), m.getSourceMeanMs(), m.getTransformMeanMs(), m.getSinkMeanMs(), m.getSourcePct(), m.getTransformPct(), m.getSinkPct());
                }
                case "control" -> {
                    if (args.length < 2) { System.err.println("control requires action: pause|resume|stop"); System.exit(2); }
                    stub.control(ControlRequest.newBuilder().setAction(args[1]).build());
                    System.out.println("{}");
                }
                case "errors" -> {
                    int limit = args.length >= 2 ? Integer.parseInt(args[1]) : 10;
                    Errors e = stub.getErrors(ErrorsRequest.newBuilder().setLimit(limit).build());
                    System.out.print("{\"transform\":[");
                    for (int i = 0; i < e.getTransformCount(); i++) {
                        System.out.print(e.getTransform(i));
                        if (i < e.getTransformCount() - 1) System.out.print(',');
                    }
                    System.out.print("],\"sink\":[");
                    for (int i = 0; i < e.getSinkCount(); i++) {
                        System.out.print(e.getSink(i));
                        if (i < e.getSinkCount() - 1) System.out.print(',');
                    }
                    System.out.println("]}");
                }
                case "health" -> {
                    HealthStatus h = stub.health(Empty.getDefaultInstance());
                    System.out.printf("{\"ready\":%s,\"running\":%s}%n", h.getReady(), h.getRunning());
                }
                default -> usage();
            }
        } finally {
            ch.shutdownNow();
        }
    }

    private static void usage() {
        System.out.println("Usage: AdminCli <status|metrics|control <action>|errors [limit]|health>");
    }
}

