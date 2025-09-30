package io.pipelines.sink;

import io.pipelines.core.BatchSink;
import io.pipelines.core.Record;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;

/**
 * Simple JDBC batch sink for byte[] payload with schema (seq BIGINT, subseq INT, payload BLOB).
 * Intended as an example; tune for your DB and use DataSource pooling in production.
 */
public class JdbcBytesBatchSink implements BatchSink<byte[]> {
    private final String jdbcUrl;
    private final String user;
    private final String password;
    private final String table;

    public JdbcBytesBatchSink(String jdbcUrl, String user, String password, String table) {
        this.jdbcUrl = jdbcUrl;
        this.user = user;
        this.password = password;
        this.table = table;
    }

    @Override
    public void acceptBatch(List<Record<byte[]>> records) throws Exception {
        if (records == null || records.isEmpty()) return;
        try (Connection c = getConnection()) {
            c.setAutoCommit(false);
            String sql = "INSERT INTO " + table + " (seq, subseq, payload) VALUES (?, ?, ?)";
            try (PreparedStatement ps = c.prepareStatement(sql)) {
                for (Record<byte[]> r : records) {
                    ps.setLong(1, r.seq());
                    ps.setInt(2, r.subSeq());
                    ps.setBytes(3, r.payload());
                    ps.addBatch();
                }
                ps.executeBatch();
                c.commit();
            } catch (SQLException e) {
                c.rollback();
                throw e;
            }
        }
    }

    @Override
    public void accept(Record<byte[]> record) throws Exception {
        acceptBatch(java.util.List.of(record));
    }

    private Connection getConnection() throws SQLException {
        return (user == null) ? DriverManager.getConnection(jdbcUrl) : DriverManager.getConnection(jdbcUrl, user, password);
    }
}
