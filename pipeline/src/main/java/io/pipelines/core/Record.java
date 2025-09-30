package io.pipelines.core;

import java.util.Objects;

/**
 * A generic record wrapper that carries a payload and deterministic ordering info.
 */
public final class Record<T> implements Comparable<Record<?>> {
    private final long seq; // monotonically increasing across the source
    private final int subSeq; // for fan-out within a transform
    private final T payload;

    public Record(long seq, int subSeq, T payload) {
        this.seq = seq;
        this.subSeq = subSeq;
        this.payload = payload;
    }

    public long seq() { return seq; }
    public int subSeq() { return subSeq; }
    public T payload() { return payload; }

    @Override
    public int compareTo(Record<?> o) {
        int c = Long.compare(this.seq, o.seq);
        if (c != 0) return c;
        return Integer.compare(this.subSeq, o.subSeq);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof Record<?> that)) return false;
        return seq == that.seq && subSeq == that.subSeq && Objects.equals(payload, that.payload);
    }

    @Override
    public int hashCode() {
        return Objects.hash(seq, subSeq, payload);
    }

    @Override
    public String toString() {
        return "Record{" +
                "seq=" + seq +
                ", subSeq=" + subSeq +
                ", payload=" + payload +
                '}';
    }
}

