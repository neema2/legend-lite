// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.exec;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * THE corpus raw-SQL boundary (audit 23 / R0 relocation): the engine's test
 * corpus hands {@code executeInDb} LITERAL H2-flavored DDL/DML strings
 * (unquoted keyword column names, {@code CURRENT_TIMESTAMP()}, H2 type
 * kinds, H2 statement orderings). Something must translate them to the
 * executing backend, and it happens HERE — never in the dialect renderers,
 * which speak only the SQL IR, and never against platform-GENERATED SQL,
 * which is IR-rendered and must not pass through this class.
 *
 * <p>CONTRACT: text-level translation of corpus-authored statements only.
 * This is the one sanctioned home for pattern-based SQL rewriting; adding a
 * recognizer anywhere else is an architecture violation (RUNNABILITY_PLAN
 * R0 rule). The designed replacement is the parser leg — raw statements
 * parsed into the SQL IR and rendered per dialect (LEGEND_SQL_VISION) —
 * at which point this class shrinks to the parser call.
 */
public final class RawSqlBoundary {

    private RawSqlBoundary() {
    }

    private static final Pattern CREATE_HEAD = Pattern.compile(
            "(?i)^\\s*create\\s+table\\s+[\\w.\"]+\\s*\\(");

    private static final Pattern INSERT_COLS = Pattern.compile(
            "(?i)^(\\s*insert\\s+into\\s+[\\w.\"]+\\s*\\()([^)]*)(\\))");

    /**
     * One corpus-authored H2 statement, translated for DuckDB execution:
     * quote keyword column names in CREATE/INSERT column lists (legal
     * unquoted on H2, syntax errors on DuckDB), drop
     * {@code CURRENT_TIMESTAMP()} parens, and map H2 type KINDS on the
     * type part only (FLOAT is an 8-byte double on H2; BIT is a boolean).
     * Callers split multi-statement blobs first — recognizers anchor at
     * statement start.
     */
    public static String h2ToDuckDb(String sql) {
        String out = sql.replaceAll("(?i)\\bCURRENT_TIMESTAMP\\(\\)", "CURRENT_TIMESTAMP");
        // H2 accepts name-first `Drop schema <name> if exists cascade`
        // (corpus testTDSJoin.pure:1047); DuckDB only parses IF EXISTS
        // before the name
        out = out.replaceAll(
                "(?i)\\bdrop\\s+schema\\s+(\\w+)\\s+if\\s+exists\\b",
                "Drop schema if exists $1");
        // schema creation is idempotent at this boundary: H2 test dbs are
        // per-connection ephemeral, the DuckDB catalog persists across a
        // family's seeds — a re-run `create schema X` must not abort the
        // seed chain
        out = out.replaceAll(
                "(?i)\\bcreate\\s+schema\\s+(?!if\\b)(\\w+)",
                "Create schema if not exists $1");
        Matcher cm = CREATE_HEAD.matcher(out);
        if (cm.find()) {
            return quoteCreateColumns(out, cm.end());
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
            cols.append(name.startsWith("\"") ? name : "\"" + name + "\"");
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
    private static String quoteCreateColumns(String sql, int bodyStart) {
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
                        .append(col.substring(endq + 1)
                                .replaceAll("(?i)\\bFLOAT\\b", "DOUBLE")
                                .replaceAll("(?i)\\bBIT\\b", "BOOLEAN"));
            } else if (head.matches("(?i)primary|constraint|foreign|unique|check")) {
                out.append(col);
            } else {
                // H2 semantics on the TYPE PART only: FLOAT is an 8-byte
                // double; BIT is a boolean (DuckDB's BIT is a bitstring)
                out.append('\"').append(head).append('\"').append(
                        col.substring(sp).replaceAll("(?i)\\bFLOAT\\b", "DOUBLE")
                                .replaceAll("(?i)\\bBIT\\b", "BOOLEAN"));
            }
        }
        return sql.substring(0, bodyStart) + out + sql.substring(end - 1);
    }
}
