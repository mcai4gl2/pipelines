package io.pipelines.transform;

import io.pipelines.core.Record;

import java.nio.charset.StandardCharsets;

/**
 * Simple selector that extracts a JSON string field value by name from a byte[] payload.
 * Not a full JSON parser; expects well-formed JSON and string field values.
 */
public class JsonFieldSelector implements Selector<byte[]> {
    private final String field;

    public JsonFieldSelector(String field) { this.field = field; }

    @Override
    public String route(Record<byte[]> record) {
        String s = new String(record.payload(), StandardCharsets.UTF_8);
        String needle = "\"" + field + "\""; // "field"
        int i = s.indexOf(needle);
        if (i < 0) return "";
        int colon = s.indexOf(':', i + needle.length());
        if (colon < 0) return "";
        int q1 = s.indexOf('"', colon + 1);
        if (q1 < 0) return "";
        int q2 = s.indexOf('"', q1 + 1);
        if (q2 < 0) return "";
        return s.substring(q1 + 1, q2);
    }
}

