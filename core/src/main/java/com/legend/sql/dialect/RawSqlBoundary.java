// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.sql.dialect;

import java.util.ArrayList;
import java.util.List;
import java.util.function.UnaryOperator;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * THE raw-SQL boundary (audit 23 / R0 relocation): the engine's H2 text, translated for the database a session runs
 * on. The engine's test corpus hands {@code executeInDb} LITERAL H2-flavored DDL/DML strings (unquoted keyword column
 * names, {@code CURRENT_TIMESTAMP()}, H2 type kinds, H2 statement orderings), written for the H2 its connections
 * declare.
 *
 * <p>Which text crosses is decided by the database its connection DECLARES (decision D2, the dialect program's Phase
 * 1c; {@link SqlDialect#raw}): only text written for {@link #ENGINE_TEXT}, and only through the dialect that runs it
 * ({@link DuckDb#fromEngineText}, {@link Postgres#fromEngineText}; H2 runs it as written, the others refuse it). Text
 * written for any other database runs as written, and SQL a dialect rendered never passes through here. Java-generated
 * engine text (Ddl's H2 spelling) still crosses when a body sends it through {@code executeInDb} on an H2 connection.
 * RawSqlLedgerTest pins the caller set shrink-only.
 * This is the one sanctioned home for pattern-based SQL rewriting; adding a
 * recognizer anywhere else is an architecture violation (RUNNABILITY_PLAN
 * R0 rule). The designed replacement is the parser leg — raw statements
 * parsed into the SQL IR and rendered per dialect (LEGEND_SQL_VISION) —
 * at which point this class shrinks to the parser call.
 */
public final class RawSqlBoundary {

    private RawSqlBoundary() {
    }

    /** One EXECUTED raw statement: the corpus's own H2 text and whether
     * it produced a result set (a query) — the executor's fact, never a
     * reading of the text. */
    public record Raw(String sql, boolean query) {
    }

    /** THE RAW-SQL LEDGER (Phase 2b, batch 137): an OBJECT the caller
     * creates per execution and hands through {@code ExecuteOptions}; the
     * executor appends every raw statement that EXECUTED (recorded after
     * the fact, with its kind — a failed statement is never recorded, so
     * the ledger mirrors executed reality by construction); the referee
     * reads it from the caller that owns it. Until batch 137 this was a
     * thread-local the harness installed and the referee read back — the
     * last of the ledger slots on the thread. (The metadata-only side
     * channel that rode beside it had no reader and is gone.) */
    public static final class Recorder {
        private final List<Raw> entries = new java.util.ArrayList<>();

        public Recorder() {
        }

        /** A ledger that starts with a PREFIX (the session's seeds so far). */
        public Recorder(List<Raw> prefix) {
            entries.addAll(prefix);
        }

        public void recordExecuted(String sql, boolean query) {
            entries.add(new Raw(sql, query));
        }

        public List<Raw> entries() {
            return java.util.Collections.unmodifiableList(entries);
        }

        /** The NON-QUERY statements only — the referee's seeds: a query
         * never seeds, and this subsequence is append-only across a
         * session's tests, so the mirror's cursor indexes it stably. */
        public List<String> seeds() {
            return entries.stream().filter(r -> !r.query()).map(Raw::sql).toList();
        }

        /** Ledger position for a transactional attempt: entries and seeds. */
        public record Mark(int entries, int seeds) {
        }

        public Mark mark() {
            return new Mark(entries.size(), seeds().size());
        }

        /** Statements recorded during a rolled-back attempt leave with it. */
        public void truncateTo(Mark m) {
            while (entries.size() > m.entries()) {
                entries.remove(entries.size() - 1);
            }
        }
    }


    private static final Pattern CREATE_HEAD = Pattern.compile(
            "(?i)^\\s*create\\s+table\\s+[\\w.\"]+\\s*\\(");

    private static final Pattern INSERT_COLS = Pattern.compile(
            "(?i)^(\\s*insert\\s+into\\s+[\\w.\"]+\\s*\\()([^)]*)(\\))");

    // Hoisted out of String.replaceAll/matches call sites, which recompile
    // the pattern on EVERY invocation. Measured hot: a JFR profile of the
    // corpus sweep put Pattern.compile at 149 execution samples, 93 of them
    // under translated and 75 under quoteCreateColumns — this boundary runs
    // per seeded statement, per test, across 2,019 corpus tests. Semantics
    // are unchanged: String.replaceAll(re, r) IS
    // Pattern.compile(re).matcher(s).replaceAll(r).
    private static final Pattern CURRENT_TS = Pattern.compile(
            "(?i)\\bCURRENT_TIMESTAMP\\(\\)");

    private static final Pattern DROP_SCHEMA = Pattern.compile(
            "(?i)\\bdrop\\s+schema\\s+(\\w+)\\s+if\\s+exists\\b");

    private static final Pattern CREATE_SCHEMA = Pattern.compile(
            "(?i)\\bcreate\\s+schema\\s+(?!if\\b)(\\w+)");

    private static final Pattern NON_COLUMN_HEAD = Pattern.compile(
            "(?i)primary|constraint|foreign|unique|check");

    private static final Pattern FLOAT_KIND = Pattern.compile("(?i)\\bFLOAT\\b");

    /** An UNALIASED {@code count(*)} projection item (followed by a comma
     * or FROM — never inside a larger expression, where an alias would be
     * a syntax error). H2's JDBC names it {@code COUNT(*)} and the engine
     * tests read {@code .value('COUNT(*)')} (F6.6 witness:
     * ddl::dropAndCreateTable); DuckDB names it {@code count_star()} —
     * the alias preserves H2's OBSERVABLE naming on the DuckDB target. */
    private static final Pattern COUNT_STAR_ITEM = Pattern.compile(
            "(?i)\\bcount\\(\\s*\\*\\s*\\)(?=\\s*(,|from\\b))");

    private static final Pattern BIT_KIND = Pattern.compile("(?i)\\bBIT\\b");

    private static final Pattern CLOB_KIND = Pattern.compile("(?i)\\bCLOB\\b");

    /**
     * One corpus-authored H2 statement, translated for DuckDB execution:
     * quote keyword column names in CREATE/INSERT column lists (legal
     * unquoted on H2, syntax errors on DuckDB), drop
     * {@code CURRENT_TIMESTAMP()} parens, and map H2 type KINDS on the
     * type part only (FLOAT is an 8-byte double on H2; BIT is a boolean).
     * Callers split multi-statement blobs first — recognizers anchor at
     * statement start.
     */
    /** The H2 dialect LEVEL this boundary translates from: the engine's
     * v2_1_214 extension (its goldens' "upgraded" branch), legend-engine's own
     * H2 ({@code EngineH2VersionPinTest} in spec ties this literal to the pinned
     * engine POM's {@code h2.version}; lite's own jar is 2.5.252 since
     * 2026-11-05). {@code SELECT H2VERSION()} on an H2-typed connection — the
     * engine's assertEqualsH2Compatible / createDbExtensionForH2 version probe —
     * answers with it. */
    public static final String H2_DIALECT_VERSION = "2.1.214";

    private static final Pattern H2_VERSION_FN =
            Pattern.compile("(?i)\\bH2VERSION\\s*\\(\\s*\\)");

    /** The declared database whose text is legend-engine's H2 (the engine's test database): the one language raw
     *  text is translated from (decision D2, {@link SqlDialect#raw}). */
    public static final String ENGINE_TEXT = "H2";

    /** One statement of the engine's H2 text in the flavor DuckDB and Postgres share (the dialect program's owner guard,
     *  2026-10-15: what differs between them is theirs, passed in): {@code columnTypes} spells a column definition's
     *  type part, after FLOAT, BIT and CLOB are read as H2 means them ({@link DuckDb#fromEngineText} as they are,
     *  {@link Postgres#fromEngineText} its own); {@code bareName} is a bare column name as the target folds it. */
    public static String translated(String sql, UnaryOperator<String> columnTypes, UnaryOperator<String> bareName) {
        String out = CURRENT_TS.matcher(sql).replaceAll("CURRENT_TIMESTAMP");
        out = H2_VERSION_FN.matcher(out).replaceAll("'" + H2_DIALECT_VERSION + "'");
        out = COUNT_STAR_ITEM.matcher(out)
                .replaceAll("count(*) AS \"COUNT(*)\"");
        // H2 accepts name-first `Drop schema <name> if exists cascade`
        // (corpus testTDSJoin.pure:1047); DuckDB only parses IF EXISTS
        // before the name
        out = DROP_SCHEMA.matcher(out).replaceAll("Drop schema if exists $1");
        // schema creation is idempotent at this boundary: H2 test dbs are
        // per-connection ephemeral, the DuckDB catalog persists across a
        // family's seeds — a re-run `create schema X` must not abort the
        // seed chain
        out = CREATE_SCHEMA.matcher(out).replaceAll("Create schema if not exists $1");
        Matcher cm = CREATE_HEAD.matcher(out);
        if (cm.find()) {
            return quoteCreateColumns(out, cm.end(), columnTypes, bareName);
        }
        Matcher m = INSERT_COLS.matcher(out);
        if (!m.find()) {
            return out;
        }
        StringBuilder cols = new StringBuilder();
        for (String c : m.group(2).split(",")) {
            if (cols.length() > 0) {
                cols.append(", ");
            }
            String name = c.strip();
            cols.append(name.startsWith("\"") ? name : "\"" + bareName.apply(name) + "\"");
        }
        return m.group(1) + cols + m.group(3) + out.substring(m.end(3));
    }

    /**
     * Quote the column NAME of each top-level column definition in a
     * CREATE TABLE literal (constraint entries — PRIMARY KEY(...) etc —
     * pass through). The type part maps H2's FLOAT (an 8-byte double) to
     * DOUBLE (DuckDB's FLOAT is REAL) — on the TYPE PART only: a
     * whole-statement replace once renamed a column literally named
     * "float" (relationalSetUp testTable).
     */
    private static String quoteCreateColumns(String sql, int bodyStart, UnaryOperator<String> columnTypes,
            UnaryOperator<String> bareName) {
        int depth = 1;
        int end = bodyStart;
        while (end < sql.length() && depth > 0) {
            char c = sql.charAt(end);
            if (c == '(') {
                depth++;
            } else if (c == ')') {
                depth--;
            }
            end++;
        }
        String body = sql.substring(bodyStart, end - 1);
        List<String> parts = new ArrayList<>();
        int d = 0;
        int start = 0;
        for (int i = 0; i < body.length(); i++) {
            char c = body.charAt(i);
            if (c == '(') {
                d++;
            } else if (c == ')') {
                d--;
            } else if (c == ',' && d == 0) {
                parts.add(body.substring(start, i));
                start = i + 1;
            }
        }
        parts.add(body.substring(start));
        StringBuilder out = new StringBuilder();
        for (String part : parts) {
            String col = part.strip();
            if (out.length() > 0) {
                out.append(", ");
            }
            int sp = 0;
            while (sp < col.length() && !Character.isWhitespace(col.charAt(sp))
                    && col.charAt(sp) != '(') {
                sp++;
            }
            String head = col.substring(0, sp);
            if (col.startsWith("\"")) {
                // pre-quoted name (model-derived DDL quotes fully): the
                // TYPE PART still needs the H2->DuckDB kind mapping
                int endq = col.indexOf('"', 1);
                out.append(col, 0, endq + 1)
                        .append(mapColumnTypes(col.substring(endq + 1), columnTypes));
            } else if (NON_COLUMN_HEAD.matcher(head).matches()) {
                out.append(col);
            } else {
                // H2 semantics on the TYPE PART only: FLOAT is an 8-byte
                // double; BIT is a boolean (DuckDB's BIT is a bitstring)
                out.append('\"').append(bareName.apply(head)).append('\"')
                        .append(mapColumnTypes(col.substring(sp), columnTypes));
            }
        }
        return sql.substring(0, bodyStart) + out + sql.substring(end - 1);
    }

    /**
     * H2 semantics on the TYPE PART only: FLOAT is an 8-byte double; BIT is a
     * boolean (DuckDB's BIT is a bitstring). Extracted so the three patterns
     * are compiled once rather than per column per statement per test — this
     * ran on the corpus seed path 2,019 times over.
     */
    private static String mapColumnTypes(String typePart, UnaryOperator<String> columnTypes) {
        String t = FLOAT_KIND.matcher(typePart).replaceAll("DOUBLE");
        t = BIT_KIND.matcher(t).replaceAll("BOOLEAN");
        t = CLOB_KIND.matcher(t).replaceAll("TEXT");
        return columnTypes.apply(t);
    }
}
