package io.pipelines.runtime;

import io.pipelines.core.Record;

import java.util.Map;
import java.util.TreeMap;

/**
 * Buffers out-of-order records and emits in increasing seq/subSeq order.
 */
public class OrderedBuffer<T> {
    private long nextSeq;
    private int nextSubSeq;
    private final TreeMap<Record<T>, Record<T>> buffer = new TreeMap<>();

    public OrderedBuffer(long startingSeq) {
        this.nextSeq = startingSeq;
        this.nextSubSeq = 0;
    }

    public void add(Record<T> r) {
        buffer.put(r, r);
    }

    /**
     * Try to pop the next record in order, or null if not ready.
     */
    public Record<T> pollNext() {
        Map.Entry<Record<T>, Record<T>> first = buffer.firstEntry();
        if (first == null) return null;
        Record<T> r = first.getKey();
        if (r.seq() == nextSeq && r.subSeq() == nextSubSeq) {
            buffer.pollFirstEntry();
            nextSubSeq++;
            return r;
        }
        if (r.seq() > nextSeq) return null;
        // if seq equal but subseq skipped, cannot emit yet
        return null;
    }
}

