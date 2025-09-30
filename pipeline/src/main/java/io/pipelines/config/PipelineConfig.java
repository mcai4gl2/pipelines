package io.pipelines.config;

import java.nio.file.Path;

public record PipelineConfig(
        Path inputDir,
        Path outputDir,
        int workers,
        int queueCapacity,
        int cpuThreads,
        long memoryBytes,
        long ioBytesPerSec,
        long externalQps,
        int adminPort
) {
    public static PipelineConfig fromEnv() {
        Path in = Path.of(System.getProperty("pipelines.in", System.getenv().getOrDefault("PIPELINES_IN", ".")));
        Path out = Path.of(System.getProperty("pipelines.out", System.getenv().getOrDefault("PIPELINES_OUT", "./out")));
        int workers = Integer.parseInt(System.getProperty("pipelines.workers", System.getenv().getOrDefault("PIPELINES_WORKERS", "4")));
        int queue = Integer.parseInt(System.getProperty("pipelines.queue", System.getenv().getOrDefault("PIPELINES_QUEUE", "1024")));
        int cpu = Integer.parseInt(System.getProperty("pipelines.cpu", System.getenv().getOrDefault("PIPELINES_CPU", "4")));
        long mem = Long.parseLong(System.getProperty("pipelines.mem", System.getenv().getOrDefault("PIPELINES_MEM", "104857600")));
        long io = Long.parseLong(System.getProperty("pipelines.io", System.getenv().getOrDefault("PIPELINES_IO", "10485760")));
        long qps = Long.parseLong(System.getProperty("pipelines.qps", System.getenv().getOrDefault("PIPELINES_QPS", "1000")));
        int port = Integer.parseInt(System.getProperty("pipelines.port", System.getenv().getOrDefault("PIPELINES_PORT", "8080")));
        return new PipelineConfig(in, out, workers, queue, cpu, mem, io, qps, port);
    }
}

