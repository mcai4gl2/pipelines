package io.pipelines.sink;

import io.pipelines.core.Record;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class JdbcBytesBatchSinkTest {
    @Test
    void writes_rows_in_batch() throws Exception {
        String url = "jdbc:h2:mem:test;DB_CLOSE_DELAY=-1";
        try (Connection c = DriverManager.getConnection(url)) {
            try (Statement s = c.createStatement()) {
                s.execute("CREATE TABLE t(seq BIGINT, subseq INT, payload BLOB)");
            }
        }
        JdbcBytesBatchSink sink = new JdbcBytesBatchSink(url, null, null, "t");
        sink.acceptBatch(List.of(
                new Record<>(0,0, "a".getBytes()),
                new Record<>(1,0, "b".getBytes()),
                new Record<>(2,0, "c".getBytes())
        ));
        try (Connection c = DriverManager.getConnection(url)) {
            try (Statement s = c.createStatement()) {
                ResultSet rs = s.executeQuery("SELECT COUNT(*) FROM t");
                rs.next();
                assertEquals(3, rs.getInt(1));
            }
        }
    }
}

