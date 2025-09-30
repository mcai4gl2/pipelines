package io.pipelines.source;

import io.pipelines.core.Record;
import io.pipelines.core.Source;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;

/**
 * Streams files from a directory as fixed-size chunks to avoid loading entire files.
 * Deterministic order: path-sorted files, then increasing chunk index per file.
 */
public class FileChunkSource implements Source<byte[]> {
    private final List<Path> files;
    private final int chunkSize;
    private int fileIdx = 0;
    private InputStream current;
    private int chunkIndexInFile = 0;
    private long globalSeq = 0;

    public FileChunkSource(Path dir, int chunkSize) throws IOException {
        this.chunkSize = Math.max(1, chunkSize);
        try (var stream = Files.list(dir)) {
            this.files = stream.filter(Files::isRegularFile)
                    .sorted(Comparator.comparing(Path::toString))
                    .toList();
        }
    }

    @Override
    public Optional<Record<byte[]>> poll() {
        try {
            while (true) {
                if (current == null) {
                    if (fileIdx >= files.size()) return Optional.empty();
                    current = Files.newInputStream(files.get(fileIdx));
                    chunkIndexInFile = 0;
                }
                byte[] buf = current.readNBytes(chunkSize);
                if (buf.length == 0) {
                    current.close();
                    current = null;
                    fileIdx++;
                    continue;
                }
                Record<byte[]> r = new Record<>(globalSeq++, chunkIndexInFile++, buf);
                return Optional.of(r);
            }
        } catch (IOException e) {
            // skip unreadable file
            try { if (current != null) current.close(); } catch (IOException ignored) {}
            current = null;
            fileIdx++;
            return Optional.empty();
        }
    }

    @Override
    public boolean isFinished() {
        return fileIdx >= files.size() && current == null;
    }
}

