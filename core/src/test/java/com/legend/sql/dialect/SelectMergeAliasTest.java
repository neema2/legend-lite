// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.sql.dialect;

import com.legend.sql.NameQuoting;
import com.legend.sql.SqlExpr;
import com.legend.sql.SqlRewriter;
import com.legend.sql.SqlSelect;
import com.legend.sql.SqlSource;
import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;

/**
 * A merged select drops a table column's alias only where the database labels the column, as the dialect writes it,
 * with the alias's own name (SelectMerge.namedAs; PR #81, 2026-11-13, the correctness audit's finding that only
 * PostgresArmTest held the rule): DuckDB, SQLite, ClickHouse and Snowflake (which quotes every name) label it as
 * written, Postgres in lower case, H2 in upper case. Each dialect's own merge pass, over a select of a table's
 * {@code CODE} and {@code code} read through a subselect.
 */
class SelectMergeAliasTest {

    private static final SqlSource.Table T = new SqlSource.Table("T", "t0", List.of(), false, Map.of(),
            NameQuoting.BARE);

    /** {@code SELECT _d.CODE AS CODE, _d.code AS code FROM (SELECT t0.CODE AS CODE, t0.code AS code FROM T t0) _d}. */
    private static SqlSelect twoLevels() {
        SqlSelect in = SqlSelect.starOf(T).withProjections(List.of(
                new SqlSelect.Projection(SqlExpr.Column.physical("t0", "CODE"), "CODE", null),
                new SqlSelect.Projection(SqlExpr.Column.physical("t0", "code"), "code", null)));
        return SqlSelect.starOf(new SqlSource.Subselect(in, "_d", null)).withProjections(List.of(
                new SqlSelect.Projection(SqlExpr.Column.derived("_d", "CODE"), "CODE", null),
                new SqlSelect.Projection(SqlExpr.Column.derived("_d", "code"), "code", null)));
    }

    /** The one select {@code dialect}'s merge makes of {@link #twoLevels}: each column's alias by its name, the column's
     *  own name where the alias was dropped. */
    private static Map<String, String> aliases(AnsiSqlRenderer dialect) {
        SqlRewriter merge = dialect.passes().stream().filter(SelectMerge.class::isInstance).findFirst()
                .orElseThrow(() -> new AssertionError(dialect.getClass().getSimpleName() + " has no SelectMerge"));
        SqlSelect out = assertInstanceOf(SqlSelect.class, merge.rewrite(twoLevels()));
        assertInstanceOf(SqlSource.Table.class, out.from(), "the two selects merged: " + out);
        Map<String, String> byName = new LinkedHashMap<>();
        for (SqlSelect.Projection p : out.projections()) {
            String name = ((SqlExpr.Column) p.expr()).name();
            byName.put(name, p.alias() == null ? "(none)" : p.alias());
        }
        return byName;
    }

    @Test
    void aDatabaseThatLabelsAColumnAsWrittenDropsBoth() {
        Map<String, String> none = Map.of("CODE", "(none)", "code", "(none)");
        for (AnsiSqlRenderer d : List.of(new DuckDb(), new SQLite(), new ClickHouse(), new Snowflake())) {
            assertEquals(none, aliases(d), d.getClass().getSimpleName());
        }
    }

    @Test
    void postgresKeepsTheAliasOfANameItFoldsToLowerCase() {
        assertEquals(Map.of("CODE", "CODE", "code", "(none)"), aliases(new Postgres()));
    }

    @Test
    void h2KeepsTheAliasOfANameItFoldsToUpperCase() {
        assertEquals(Map.of("CODE", "(none)", "code", "code"), aliases(new H2()));
    }
}
