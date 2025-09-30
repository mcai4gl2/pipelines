package io.pipelines.transform;

import io.pipelines.core.Record;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

public class JsonFieldSelectorTest {
    @Test
    void extracts_string_field() {
        var sel = new JsonFieldSelector("type");
        String json = "{\"type\":\"alpha\",\"x\":1}";
        String key = sel.route(new Record<>(1, 0, json.getBytes(java.nio.charset.StandardCharsets.UTF_8)));
        assertEquals("alpha", key);
    }
}

