// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.sql.dialect;

import com.legend.sql.SqlSelect;
import com.legend.sql.SqlSource;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** E-1 (docs/EXECUTION_PLAN_BOUNDARY_2026_10_05.md §10): the one writer, and the statement entry point beside render. */
class SqlWriterTest {

    private static final SqlSelect QUERY = SqlSelect.starOf(
            new SqlSource.Table("T_PERSON", "t0", List.of(), false, java.util.Map.of()));

    @Test
    void parametersAreListedInTheOrderTheirPlaceholdersAreWritten() {
        SqlWriter w = new SqlWriter();
        w.append("a = ").bind(new RenderedStatement.Bind("x", null)).append(" AND b = ANY(")
                .bind(new RenderedStatement.Bind("ids", "INTEGER")).append(") AND c = ")
                .bind(new RenderedStatement.Bind("x", null));
        assertEquals(new RenderedStatement("a = ? AND b = ANY(?) AND c = ?", List.of(
                new RenderedStatement.Bind("x", null), new RenderedStatement.Bind("ids", "INTEGER"),
                new RenderedStatement.Bind("x", null))), w.statement());
    }

    @Test
    void aStatementWithBoundParametersIsRefusedAsText() {
        SqlWriter w = new SqlWriter().append("a = ").bind(new RenderedStatement.Bind("x", null));
        var refused = assertThrows(IllegalStateException.class, w::text);
        assertTrue(refused.getMessage().contains("render it as a statement"), refused.getMessage());
    }

    @Test
    void aQueryWithNoParametersRendersTheSameTextEitherWay() {
        for (SqlDialect d : List.of(new DuckDb(), new H2(), new Postgres())) {
            RenderedStatement s = d.renderStatement(QUERY);
            assertEquals(d.render(QUERY), s.sql(), d.getClass().getSimpleName());
            assertEquals(List.of(), s.binds(), d.getClass().getSimpleName());
        }
    }

    @Test
    void theLegacyEngineTextPlanBindsNothing() {
        var refused = assertThrows(DialectCapability.class, () -> new EngineStyleH2().renderStatement(QUERY));
        assertTrue(refused.getMessage().contains("template variables"), refused.getMessage());
    }
}
