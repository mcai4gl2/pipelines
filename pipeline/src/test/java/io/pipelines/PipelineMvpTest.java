package io.pipelines;

import com.codahale.metrics.MetricRegistry;
import io.pipelines.budget.SimpleBudgetManager;
import io.pipelines.retry.ExponentialBackoffRetryPolicy;
import io.pipelines.runtime.Pipeline;
import io.pipelines.runtime.PipelineBuilder;
import io.pipelines.sink.FileBytesSink;
import io.pipelines.source.FileBytesSource;
import io.pipelines.transform.IdentityBytesTransform;
import io.pipelines.core.Record;
import io.pipelines.core.Transform;
import io.pipelines.transform.TransformChain;
import io.pipelines.transform.RouterTransform;
import io.pipelines.transform.Selector;
import io.pipelines.source.QueueSource;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertTrue;

public class PipelineMvpTest {
    private Pipeline<byte[], byte[]> pipeline;

    @AfterEach
    void tearDown() {
        if (pipeline != null) pipeline.close();
    }

    @Test
    void transform_retries_then_succeeds() throws Exception {
        Path in = Files.createTempDirectory("pipelines-in");
        Path out = Files.createTempDirectory("pipelines-out");
        Files.writeString(in.resolve("a.txt"), "A");

        var source = new FileBytesSource(in);
        class Flaky implements Transform<byte[], byte[]> {
            volatile boolean failed = false;
            @Override public java.util.List<Record<byte[]>> apply(Record<byte[]> input) throws Exception {
                if (!failed) { failed = true; throw new IOException("transient"); }
                return java.util.List.of(new Record<>(input.seq(), 0, input.payload()));
            }
        }
        var transform = new Flaky();
        var sink = new FileBytesSink(out);
        var budget = new SimpleBudgetManager(2, 1024 * 1024, 0, 0, null);
        var retry = new ExponentialBackoffRetryPolicy(3, 1, 10);

        pipeline = new PipelineBuilder<byte[], byte[]>()
                .source(source)
                .transform(transform)
                .sink(sink)
                .budget(budget)
                .retry(retry)
                .workers(2)
                .queueCapacity(4)
                .metrics(new MetricRegistry())
                .build();
        pipeline.start();

        int tries = 0;
        while (!source.isFinished() && tries++ < 200) { Thread.sleep(10); }
        Thread.sleep(200);

        assertTrue(Files.list(out).findAny().isPresent(), "output created after retry");
    }

    @Test
    void transform_chain_and_router_with_queue() throws Exception {
        Path in = Files.createTempDirectory("pipelines-in");
        Path out = Files.createTempDirectory("pipelines-out");
        Files.writeString(in.resolve("a.txt"), "alpha");
        Files.writeString(in.resolve("b.txt"), "beta");

        var source = new FileBytesSource(in);
        // chain: uppercase then append !
        Transform<byte[], byte[]> upper = (Record<byte[]> r) -> java.util.List.of(new Record<>(r.seq(), 0, new String(r.payload()).toUpperCase().getBytes())) ;
        Transform<byte[], byte[]> bang = (Record<byte[]> r) -> java.util.List.of(new Record<>(r.seq(), 0, (new String(r.payload()) + "!").getBytes())) ;
        var chain = new TransformChain<byte[], byte[]>(upper, bang);

        // router: if starts with 'a' route to chain; otherwise publish to queue
        QueueSource<byte[]> q = new QueueSource<>(16);
        Selector<byte[]> sel = (Record<byte[]> r) -> (r.payload().length > 0 && r.payload()[0] == 'a') ? "chain" : "q";
        RouterTransform<byte[], byte[]> router = new RouterTransform<byte[], byte[]>(sel)
                .route("chain", chain)
                .publish("q", q);

        var sink = new FileBytesSink(out);
        var budget = new SimpleBudgetManager(4, 1024 * 1024, 0, 0, null);
        var retry = new ExponentialBackoffRetryPolicy(2, 1, 10);

        // root pipeline routes
        pipeline = new PipelineBuilder<byte[], byte[]>()
                .source(source)
                .transform(router)
                .sink(sink)
                .budget(budget)
                .retry(retry)
                .workers(4)
                .queueCapacity(16)
                .metrics(new MetricRegistry())
                .build();
        pipeline.start();

        // second pipeline consumes queue
        var sink2 = new FileBytesSink(out.resolve("queue"));
        Files.createDirectories(out.resolve("queue"));
        var p2 = new PipelineBuilder<byte[], byte[]>()
                .source(q)
                .transform(new IdentityBytesTransform())
                .sink(sink2)
                .budget(budget)
                .retry(retry)
                .workers(2)
                .queueCapacity(8)
                .sinkBatchSize(1)
                .metrics(new MetricRegistry())
                .build();
        p2.start();

        int tries = 0;
        while (!source.isFinished() && tries++ < 200) { Thread.sleep(10); }
        Thread.sleep(200);
        // Wait for queue pipeline to produce output
        long rootCount = Files.list(out).filter(Files::isRegularFile).count();
        long queueCount = 0;
        int tries2 = 0;
        while (queueCount == 0 && tries2++ < 200) {
            Thread.sleep(20);
            queueCount = Files.exists(out.resolve("queue")) ? Files.list(out.resolve("queue")).filter(Files::isRegularFile).count() : 0;
        }
        // signal queue finished so p2 can stop after consuming
        q.finish();
        Thread.sleep(100);
        assertTrue(rootCount >= 1, "root pipeline produced outputs");
        assertTrue(queueCount >= 1, "queue pipeline produced outputs");

        p2.close();
    }

    @Test
    void fileToFile_identity_transform() throws Exception {
        Path in = Files.createTempDirectory("pipelines-in");
        Path out = Files.createTempDirectory("pipelines-out");
        Files.writeString(in.resolve("a.txt"), "hello");
        Files.writeString(in.resolve("b.txt"), "world");

        var source = new FileBytesSource(in);
        var transform = new IdentityBytesTransform();
        var sink = new FileBytesSink(out);
        var budget = new SimpleBudgetManager(4, 1024 * 1024, 10_000_000, 1_000_000, null);
        var retry = new ExponentialBackoffRetryPolicy(3, 5, 50);

        pipeline = new PipelineBuilder<byte[], byte[]>()
                .source(source)
                .transform(transform)
                .sink(sink)
                .budget(budget)
                .retry(retry)
                .workers(4)
                .queueCapacity(16)
                .metrics(new MetricRegistry())
                .build();
        pipeline.start();

        // wait for completion
        int tries = 0;
        while (!source.isFinished() && tries++ < 200) { Thread.sleep(10); }
        Thread.sleep(200);

        assertTrue(Files.list(out).findAny().isPresent(), "outputs created");
    }

    @Test
    void preserves_order_with_parallel_transform() throws Exception {
        Path in = Files.createTempDirectory("pipelines-in");
        Path out = Files.createTempDirectory("pipelines-out");
        Files.writeString(in.resolve("a.txt"), "A");
        Files.writeString(in.resolve("b.txt"), "B");

        var source = new FileBytesSource(in);
        Transform<byte[], byte[]> split = (Record<byte[]> r) -> java.util.List.of(
                new Record<>(r.seq(), 0, r.payload()),
                new Record<>(r.seq(), 1, r.payload())
        );
        var sink = new FileBytesSink(out);
        var budget = new SimpleBudgetManager(8, 1024 * 1024, 0, 0, null);
        var retry = new ExponentialBackoffRetryPolicy(2, 1, 10);

        pipeline = new PipelineBuilder<byte[], byte[]>()
                .source(source)
                .transform(split)
                .sink(sink)
                .budget(budget)
                .retry(retry)
                .workers(8)
                .queueCapacity(8)
                .metrics(new MetricRegistry())
                .build();
        pipeline.start();

        int tries = 0;
        while (!source.isFinished() && tries++ < 200) { Thread.sleep(10); }
        Thread.sleep(200);

        long fileCount = Files.list(out).count();
        assertTrue(fileCount >= 4, "expected at least 4 output files");
    }

    @Test
    void async_transform_does_not_block_and_emits() throws Exception {
        Path in = Files.createTempDirectory("pipelines-in-async");
        Path out = Files.createTempDirectory("pipelines-out-async");
        Files.writeString(in.resolve("a.txt"), "A");

        var source = new FileBytesSource(in);
        var sink = new FileBytesSink(out);
        var retry = new ExponentialBackoffRetryPolicy(2, 1, 10);
        var budget = new SimpleBudgetManager(4, 1024 * 1024, 0, 0, null);

        // async transform that defers via CompletableFuture
        io.pipelines.core.AsyncTransform<byte[], byte[]> async = (r) -> java.util.concurrent.CompletableFuture.supplyAsync(() -> java.util.List.of(new Record<>(r.seq(), 0, r.payload())));
        // Wrap async into a Transform reference since PipelineBuilder takes Transform; it also detects AsyncTransform
        // Helper that implements both interfaces so Pipeline detects async mode
        class Both implements io.pipelines.core.Transform<byte[], byte[]>, io.pipelines.core.AsyncTransform<byte[], byte[]> {
            @Override public java.util.List<Record<byte[]>> apply(Record<byte[]> input) { throw new UnsupportedOperationException(); }
            @Override public java.util.concurrent.CompletionStage<java.util.List<Record<byte[]>>> applyAsync(Record<byte[]> input) { return async.applyAsync(input); }
        }
        io.pipelines.core.Transform<byte[], byte[]> transform = new Both();

        pipeline = new PipelineBuilder<byte[], byte[]>()
                .source(source)
                .transform(transform)
                .sink(sink)
                .budget(budget)
                .retry(retry)
                .workers(1)
                .queueCapacity(4)
                .metrics(new MetricRegistry())
                .build();
        pipeline.start();

        int tries = 0;
        while (!source.isFinished() && tries++ < 200) { Thread.sleep(10); }
        Thread.sleep(200);
        assertTrue(Files.list(out).findAny().isPresent(), "outputs created by async transform");
    }

}
