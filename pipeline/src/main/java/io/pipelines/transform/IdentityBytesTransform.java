package io.pipelines.transform;

import io.pipelines.core.Record;
import io.pipelines.core.Transform;

import java.util.List;

/**
 * Identity transform for byte[] payloads.
 */
public class IdentityBytesTransform implements Transform<byte[], byte[]> {
    @Override
    public List<Record<byte[]>> apply(Record<byte[]> input) {
        return List.of(new Record<>(input.seq(), 0, input.payload()));
    }
}

