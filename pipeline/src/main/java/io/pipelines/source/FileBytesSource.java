package io.pipelines.source;

import io.pipelines.core.Record;
import io.pipelines.core.Source;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;

/**
 * Emits one record per file with the entire file bytes as payload, ordered by file path.
 */
public class FileBytesSource implements Source<byte[]> {
    private final List<Path> files;
    private int idx = 0;

    public FileBytesSource(Path dir) throws IOException {
        try (var stream = Files.list(dir)) {
            this.files = stream
                    .filter(Files::isRegularFile)
                    .sorted(Comparator.comparing(Path::toString))
                    .toList();
        }
    }

    @Override
    public Optional<Record<byte[]>> poll() {
        if (idx >= files.size()) return Optional.empty();
        Path p = files.get(idx);
        try {
            byte[] data = Files.readAllBytes(p);
            Record<byte[]> r = new Record<>(idx, 0, data);
            idx++;
            return Optional.of(r);
        } catch (IOException e) {
            // skip unreadable file but advance to avoid infinite loop
            idx++;
            return Optional.empty();
        }
    }

    @Override
    public boolean isFinished() {
        return idx >= files.size();
    }
}

