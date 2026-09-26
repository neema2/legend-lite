package com.legend.warehouse.server;

import com.legend.Nullable;
import com.legend.warehouse.sqlapi.SqlApi.ApiError;
import com.legend.warehouse.sqlapi.SqlApi.ResultMeta;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.time.Instant;

/**
 * Every statement, as it finishes: who ran it, what, when, and how it
 * ended -- a table in the warehouse's own {@code system} database, which
 * W2 exposes to users through grants.
 */
public final class History implements AutoCloseable {

    private final Connection db;
    private final PreparedStatement insert;

    public History(Path dataDir) throws SQLException {
        db = DriverManager.getConnection("jdbc:duckdb:" + dataDir.resolve("system.duckdb"));
        try (Statement s = db.createStatement()) {
            s.execute("""
                    CREATE TABLE IF NOT EXISTS query_history (
                      statement_id VARCHAR PRIMARY KEY,
                      principal VARCHAR NOT NULL,
                      catalog VARCHAR NOT NULL,
                      sql_text VARCHAR NOT NULL,
                      state VARCHAR NOT NULL,
                      submitted_at TIMESTAMP NOT NULL,
                      started_at TIMESTAMP,
                      finished_at TIMESTAMP,
                      row_count BIGINT,
                      error_code VARCHAR,
                      error_message VARCHAR)""");
        }
        insert = db.prepareStatement("INSERT INTO query_history VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)");
    }

    synchronized void record(Statements.Run run) {
        try {
            ResultMeta r = run.result;
            ApiError e = run.error;
            insert.setString(1, run.id);
            insert.setString(2, run.principal);
            insert.setString(3, run.request.catalog());
            insert.setString(4, run.request.sql());
            insert.setString(5, run.state.wire());
            insert.setTimestamp(6, ts(run.submitted));
            insert.setTimestamp(7, ts(run.started));
            insert.setTimestamp(8, ts(run.finished));
            if (r == null) insert.setNull(9, java.sql.Types.BIGINT);
            else insert.setLong(9, r.rowCount());
            insert.setString(10, e == null ? null : e.code().name());
            insert.setString(11, e == null ? null : e.message());
            insert.executeUpdate();
        } catch (SQLException failed) {
            // History must never fail the statement it describes.
            System.err.println("warehouse: could not record statement " + run.id + ": " + failed.getMessage());
        }
    }

    /** How many statements are recorded for a principal (tests and admin). */
    public synchronized long count(String principal) throws SQLException {
        try (PreparedStatement p = db.prepareStatement("SELECT count(*) FROM query_history WHERE principal = ?")) {
            p.setString(1, principal);
            try (var rs = p.executeQuery()) {
                rs.next();
                return rs.getLong(1);
            }
        }
    }

    private static @Nullable Timestamp ts(@Nullable Instant i) {
        return i == null ? null : Timestamp.from(i);
    }

    @Override
    public synchronized void close() throws SQLException {
        insert.close();
        db.close();
    }
}
