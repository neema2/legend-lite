// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.sql.dialect;

import com.legend.sql.SqlExpr;
import com.legend.sql.SqlFn;
import com.legend.sql.SqlSelect;
import com.legend.sql.SqlSource;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * {@link SqlFn#MD5}, {@link SqlFn#SHA1} and {@link SqlFn#SHA256} on the in-process databases (2026-10-16, the dialect
 * program's Phase 3): a string's digest in lower-case hex, the same on H2 (its {@code HASH}, which has no
 * {@code md5}/{@code sha1}/{@code sha256}) as on DuckDB, and as Java computes it.
 */
class DigestTest {

    private static SqlSelect digest(SqlFn fn, String s) {
        return SqlSelect.starOf(new SqlSource.Dual()).withProjections(List.of(new SqlSelect.Projection(
                SqlExpr.Call.of(fn, new SqlExpr.StringLit(s)), "v", null)));
    }

    @Test
    void theInProcessDatabasesDigestAsJavaDoes() throws Exception {
        Map<SqlFn, String> algorithms = Map.of(SqlFn.MD5, "MD5", SqlFn.SHA1, "SHA-1", SqlFn.SHA256, "SHA-256");
        for (var db : List.<Map.Entry<SqlDialect, String>>of(Map.entry(new DuckDb(), "jdbc:duckdb:"),
                Map.entry(new H2(), "jdbc:h2:mem:digest"))) {
            String name = db.getKey().getClass().getSimpleName();
            try (Connection c = DriverManager.getConnection(db.getValue()); Statement st = c.createStatement()) {
                for (var a : algorithms.entrySet()) {
                    for (String s : List.of("Scott", "Firm X", "é ü")) {
                        assertEquals(java(a.getValue(), s), one(st, db.getKey().render(digest(a.getKey(), s))),
                                name + " " + a.getKey() + " " + s);
                    }
                }
            }
        }
    }

    /** A NULL's digest is NULL on each (the PR audit of 2026-10-16). */
    @Test
    void aNullsDigestIsNull() throws Exception {
        for (var db : List.<Map.Entry<SqlDialect, String>>of(Map.entry(new DuckDb(), "jdbc:duckdb:"),
                Map.entry(new H2(), "jdbc:h2:mem:digest_null"))) {
            try (Connection c = DriverManager.getConnection(db.getValue()); Statement st = c.createStatement()) {
                for (SqlFn fn : List.of(SqlFn.MD5, SqlFn.SHA1, SqlFn.SHA256)) {
                    SqlSelect q = SqlSelect.starOf(new SqlSource.Dual()).withProjections(List.of(new SqlSelect.Projection(
                            SqlExpr.Call.of(fn, new SqlExpr.NullLit()), "v", null)));
                    assertEquals(null, one(st, db.getKey().render(q)), db.getKey().getClass().getSimpleName() + " " + fn);
                }
            }
        }
    }

    private static String java(String algorithm, String s) throws Exception {
        byte[] d = java.security.MessageDigest.getInstance(algorithm).digest(
                s.getBytes(java.nio.charset.StandardCharsets.UTF_8));
        return java.util.HexFormat.of().formatHex(d);
    }

    private static Object one(Statement st, String sql) throws Exception {
        try (ResultSet rs = st.executeQuery(sql)) {
            assertTrue(rs.next(), sql);
            return rs.getObject(1);
        }
    }
}
