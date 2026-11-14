// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.sql.dialect;

import com.legend.sql.NameQuoting;
import com.legend.sql.SqlDdl;
import com.legend.sql.SqlDml;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * A store's DECLARED-QUOTED column name is one identifier wherever DDL and DML write it -- a CREATE TABLE's column, its
 * PRIMARY KEY list, an INSERT's column list -- on every relational dialect, whatever its characters (stream S, injection
 * safety, 2026-11-14). DuckDB's {@code ddlIdentifier} wrote such a name as {@code '"' + name + '"'}, its quote not
 * doubled, the one dialect that did not: the audit's breakout through a project's {@code ###Data} load
 * ({@code com.legend.server.DeclaredNameBreakoutTest}, the same hole end to end). Read by {@link InjectionFuzzTest}'s
 * lexer oracle: nothing left open, the marker inside a quoted identifier.
 */
class DeclaredDdlNamesTest {

    /** The statements that write a declared-quoted {@code name}, each with its place's name. */
    private static Map<String, Object> places(String name) {
        Map<String, Object> out = new LinkedHashMap<>();
        SqlDdl.ColumnType integer = new SqlDdl.ColumnType.Plain(SqlDdl.ColumnType.Kind.INTEGER);
        out.put("CREATE TABLE's column", new SqlDdl.CreateTable(null, "T", NameQuoting.BARE,
                List.of(new SqlDdl.Column("ID", false, integer, true, false),
                        new SqlDdl.Column(name, true, integer, false, false))));
        out.put("PRIMARY KEY list", new SqlDdl.CreateTable(null, "T", NameQuoting.BARE,
                List.of(new SqlDdl.Column(name, true, integer, true, true))));
        out.put("INSERT's columns", new SqlDml.InsertFromTable(null, "T", NameQuoting.BARE, List.of("ID", name),
                Set.of(name), "S"));
        return out;
    }

    @Test
    void aDeclaredQuotedColumnIsOneIdentifierInEveryDdlAndInsertOnEveryDialect() {
        List<String> wrong = new ArrayList<>();
        int written = 0;
        for (Map.Entry<SqlDialect, InjectionFuzzTest.Rules> d : PreQuotedNamesTest.dialects().entrySet()) {
            for (String payload : InjectionFuzzTest.PAYLOADS) {
                for (Map.Entry<String, Object> place : places(payload).entrySet()) {
                    String sql = place.getValue() instanceof SqlDdl ddl ? d.getKey().render(ddl)
                            : d.getKey().render((SqlDml) place.getValue());
                    written++;
                    String finding = InjectionFuzzTest.finding(sql, d.getValue());
                    if (finding != null) {
                        wrong.add(d.getKey().getClass().getSimpleName() + ", " + place.getKey() + ", " + payload
                                + ": " + finding + ": " + sql.replace("\n", " "));
                    }
                }
            }
        }
        // every dialect, every payload, every place: written (none refused), none left open or walked out of
        assertEquals(PreQuotedNamesTest.dialects().size() * InjectionFuzzTest.PAYLOADS.size() * 3, written);
        assertEquals(List.of(), wrong);
    }

    /** The audit's DuckDB breakout, its DDL as the project's load writes it: one CREATE TABLE, the name one column. */
    @Test
    void duckDbWritesTheAuditsNameAsOneColumn() {
        String name = "x\" INTEGER); COPY (SELECT 42 AS pwned) TO '/no/such/dir/p.csv'; CREATE TABLE z(\"ID";
        String sql = new DuckDb().render(new SqlDdl.CreateTable(null, "PARTY", NameQuoting.BARE,
                List.of(new SqlDdl.Column(name, true, new SqlDdl.ColumnType.Plain(SqlDdl.ColumnType.Kind.INTEGER),
                        false, false))));
        assertEquals("Create Table PARTY(\"x\"\" INTEGER); COPY (SELECT 42 AS pwned) TO '/no/such/dir/p.csv'; "
                + "CREATE TABLE z(\"\"ID\" INTEGER NULL);", sql);
    }
}
