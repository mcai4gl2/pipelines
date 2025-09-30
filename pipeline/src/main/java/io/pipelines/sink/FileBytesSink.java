package io.pipelines.sink;

import io.pipelines.core.BatchSink;
import io.pipelines.core.Record;
import io.pipelines.core.Sink;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

/**
 * Writes each record into a file under the output directory, named by sequence.
 */
public class FileBytesSink implements Sink<byte[]>, BatchSink<byte[]> {
    private final Path outDir;

    public FileBytesSink(Path outDir) throws IOException {
        this.outDir = outDir;
        Files.createDirectories(outDir);
    }

    @Override
    public void accept(Record<byte[]> record) throws IOException {
        String name = String.format("%020d_%06d.bin", record.seq(), record.subSeq());
        Path out = outDir.resolve(name);
        Files.write(out, record.payload(), StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.WRITE);
    }

    @Override
    public void acceptBatch(java.util.List<Record<byte[]>> records) throws IOException {
        for (Record<byte[]> r : records) {
            accept(r);
        }
    }
}
