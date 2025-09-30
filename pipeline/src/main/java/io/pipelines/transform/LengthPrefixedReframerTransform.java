package io.pipelines.transform;

import io.pipelines.core.Record;
import io.pipelines.core.Transform;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

/**
 * Reframes a stream of byte[] chunks into length-prefixed messages (4-byte big-endian length) and carries
 * incomplete data across calls. Thread-safe via synchronization.
 */
public class LengthPrefixedReframerTransform implements Transform<byte[], byte[]> {
    private final ByteBuffer buffer;
    private long currentFrameStartSeq = -1;
    private int outSubSeq = 0;

    public LengthPrefixedReframerTransform(int maxCarryBytes) {
        this.buffer = ByteBuffer.allocate(Math.max(1024, maxCarryBytes));
        buffer.clear();
    }

    @Override
    public synchronized List<Record<byte[]>> apply(Record<byte[]> input) {
        // Append input to buffer
        // Use combined local array to avoid resizing the internal buffer
        // Work on a local dynamic buffer to avoid reflection hacks
        byte[] combined = new byte[buffer.position() + input.payload().length];
        buffer.flip();
        buffer.get(combined, 0, buffer.limit());
        System.arraycopy(input.payload(), 0, combined, buffer.limit(), input.payload().length);
        buffer.clear();

        int pos = 0;
        List<Record<byte[]>> out = new ArrayList<>();
        if (currentFrameStartSeq < 0) currentFrameStartSeq = input.seq();
        while (pos + 4 <= combined.length) {
            int len = ((combined[pos] & 0xFF) << 24) | ((combined[pos+1] & 0xFF) << 16) | ((combined[pos+2] & 0xFF) << 8) | (combined[pos+3] & 0xFF);
            if (pos + 4 + len > combined.length) break; // incomplete
            byte[] msg = java.util.Arrays.copyOfRange(combined, pos + 4, pos + 4 + len);
            out.add(new Record<>(currentFrameStartSeq, outSubSeq++, msg));
            pos += 4 + len;
            currentFrameStartSeq = input.seq(); // frames that start after this point will be attributed to the latest seq
        }
        // carry remainder back into buffer
        int remaining = combined.length - pos;
        if (remaining > 0) {
            buffer.put(combined, pos, remaining);
        }
        return out;
    }
}
