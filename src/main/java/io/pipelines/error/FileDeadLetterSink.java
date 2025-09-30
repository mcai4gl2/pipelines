package io.pipelines.error;

import io.pipelines.core.Record;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.Instant;

public class FileDeadLetterSink<T> implements DeadLetterSink<T> {
    private final Path file;

    public FileDeadLetterSink(Path file) throws IOException {
        this.file = file;
        Files.createDirectories(file.getParent());
        if (!Files.exists(file)) {
            Files.writeString(file, "", StandardCharsets.UTF_8, StandardOpenOption.CREATE);
        }
    }

    @Override
    public synchronized void acceptFailure(String stage, Record<T> record, Exception e) {
        String json = String.format(
                "{\"ts\":\"%s\",\"stage\":\"%s\",\"seq\":%d,\"subSeq\":%d,\"error\":\"%s\"}%n",
                Instant.now(), stage, record == null ? -1 : record.seq(), record == null ? -1 : record.subSeq(),
                safe(e.toString())
        );
        try {
            Files.writeString(file, json, StandardCharsets.UTF_8, StandardOpenOption.APPEND);
        } catch (IOException ignored) {
        }
    }

    private static String safe(String s) { return s.replace("\"", "'"); }
}

