package com.legend.warehouse.server;

import com.legend.Nullable;
import com.legend.warehouse.server.duck.Collect;
import com.legend.warehouse.server.duck.Conn;
import com.legend.warehouse.server.duck.Database;
import com.legend.warehouse.server.duck.DuckException;
import com.legend.warehouse.server.duck.Result;
import com.legend.warehouse.sqlapi.SqlApi.ApiError;
import com.legend.warehouse.sqlapi.SqlApi.ResultMeta;
import com.legend.server.Json;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;

/**
 * Every statement, as it finishes: who ran it, what, when, and how it
 * ended -- a table in the warehouse's own {@code system} database, which
 * W2 exposes to users through grants.
 */
public final class History implements AutoCloseable {

    private static final String WAREHOUSE = "warehouse";

    private final Conn conn;

    /** The history in the warehouse's system database (opened, and closed, by the server). */
    public History(Database system) throws DuckException {
        conn = system.connect(WAREHOUSE);
        conn.execute("""
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
                  error_message VARCHAR)""").close();
    }

    synchronized void record(Statements.Run run) {
        ResultMeta r = run.result;
        ApiError e = run.error;
        try (Result ignored = conn.execute("""
                INSERT INTO query_history VALUES (?, ?, ?, ?, ?, CAST(? AS TIMESTAMP), CAST(? AS TIMESTAMP),
                  CAST(? AS TIMESTAMP), ?, ?, ?)""",
                run.id, run.principal, run.request.catalog(), run.request.sql(), run.state.wire(),
                ts(run.submitted), ts(run.started), ts(run.finished), r == null ? null : (Long) r.rowCount(),
                e == null ? null : e.code().name(), e == null ? null : e.message())) {
            // written
        } catch (DuckException failed) {
            // History must never fail the statement it describes.
            System.err.println("warehouse: could not record statement " + run.id + ": " + failed.getMessage());
        }
    }

    /**
     * The principal's own most recent statements, newest first, as the API reports them
     * ({@code GET /sql/v1/history}): each statement's id, catalog, SQL, state, times, rows and error.
     */
    public synchronized List<Json.Node> recent(String principal, int limit) throws Exception {
        try (Result r = conn.execute("""
                SELECT statement_id, catalog, sql_text, state, strftime(submitted_at, '%Y-%m-%dT%H:%M:%S.%fZ'),
                       strftime(finished_at, '%Y-%m-%dT%H:%M:%S.%fZ'), row_count, error_code
                FROM query_history WHERE principal = ? ORDER BY submitted_at DESC, statement_id LIMIT ?""",
                principal, (long) limit)) {
            List<Json.Node> out = new ArrayList<>();
            String[] names = {"statementId", "catalog", "sql", "state", "submittedAt", "finishedAt", "rowCount", "errorCode"};
            for (List<Json.Node> row : Collect.json(r, limit)) {
                LinkedHashMap<String, Json.Node> f = new LinkedHashMap<>();
                for (int i = 0; i < names.length; i++) {
                    if (!(row.get(i) instanceof Json.Null)) f.put(names[i], row.get(i));
                }
                out.add(new Json.Obj(f));
            }
            return out;
        }
    }

    /** How many statements are recorded for a principal (tests and admin). */
    public synchronized long count(String principal) throws Exception {
        try (Result r = conn.execute("SELECT count(*) FROM query_history WHERE principal = ?", principal)) {
            return Long.parseLong(((com.legend.server.Json.Str) Collect.json(r, 1).get(0).get(0)).value());
        }
    }

    /** An instant as a TIMESTAMP literal in UTC (DuckDB's TIMESTAMP carries no zone). */
    private static @Nullable String ts(@Nullable Instant i) {
        return i == null ? null : java.time.LocalDateTime.ofInstant(i, java.time.ZoneOffset.UTC).toString();
    }

    @Override
    public synchronized void close() {
        conn.close();
    }
}
