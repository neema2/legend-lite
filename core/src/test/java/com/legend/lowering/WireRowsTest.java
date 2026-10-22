// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.lowering;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;

import com.legend.compiler.element.type.Multiplicity;
import com.legend.compiler.element.type.Type;
import com.legend.sql.NameQuoting;
import com.legend.sql.OutputCol;
import com.legend.sql.SqlExpr;
import com.legend.sql.SqlQuery;
import com.legend.sql.SqlSelect;
import com.legend.sql.SqlSource;
import com.legend.sql.SqlType;
import com.legend.sql.SqlUnion;
import com.legend.sql.SqlWith;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

/**
 * The typed-rows wire's shapes (the plan path program's step 6, 2026-10-18): a select is wrapped once, its row the
 * wrapper's one projection, its own sort keys lifted onto the wrapper; a statement's CTEs stay at its head, the
 * wire taken over the body (H2 parses a derived table's text again for each plan it weighs, and a verdict's WITH
 * wrapped inside two of them exhausted its heap on the corpus's cross-database bodies); a UNION ALL of unordered
 * selects is wrapped branch by branch.
 */
final class WireRowsTest {

    private static final OutputCol A = new OutputCol("a", SqlType.Scalar.BIGINT, false);
    private static final OutputCol B = new OutputCol("b", SqlType.Scalar.VARCHAR, true);
    private static final Type.RelationType SCHEMA = new Type.RelationType(List.of(
            new Type.Column("a", Type.Primitive.INTEGER, Multiplicity.Bounded.ONE),
            new Type.Column("b", Type.Primitive.STRING, Multiplicity.Bounded.ZERO_ONE)));

    private static SqlSource table() {
        return new SqlSource.Table("p", "p", List.of(A, B), false, Map.of(), NameQuoting.BARE);
    }

    private static SqlSelect projected(SqlSource from, List<SqlSelect.SortKey> orderBy) {
        return new SqlSelect(List.of(
                new SqlSelect.Projection(SqlExpr.Column.of("p", A), "a", A),
                new SqlSelect.Projection(SqlExpr.Column.of("p", B), "b", B)),
                false, from, null, List.of(), null, null, orderBy, null, null, List.of(A, B));
    }

    private static SqlSelect wrapped(SqlQuery rows) {
        SqlSelect s = assertInstanceOf(SqlSelect.class, rows);
        assertEquals(1, s.projections().size());
        assertEquals("_wire_row", s.outputs().get(0).name());
        assertEquals("_wire1", assertInstanceOf(SqlSource.Subselect.class, s.from()).alias());
        return s;
    }

    @Test
    void aSelectIsWrappedOnce_itsProjectionsKept() {
        SqlSelect s = wrapped(WireRender.rows(projected(table(), List.of()), SCHEMA));
        SqlSelect inner = assertInstanceOf(SqlSelect.class,
                assertInstanceOf(SqlSource.Subselect.class, s.from()).inner());
        assertEquals(2, inner.projections().size());
    }

    @Test
    void aStatementsCtesStayAtItsHead_theWireOverTheBody() {
        SqlWith plan = new SqlWith(List.of(new SqlWith.Cte("f", projected(table(), List.of()))),
                projected(new SqlSource.Cte("f", "p", List.of(A, B)), List.of()), false);
        SqlWith w = assertInstanceOf(SqlWith.class, WireRender.rows(plan, SCHEMA));
        assertEquals(1, w.ctes().size());
        assertEquals("f", w.ctes().get(0).name());
        wrapped(w.body());
    }

    @Test
    void anOrderedSelectsKeysAreLifted() {
        SqlSelect ordered = projected(table(), List.of(new SqlSelect.SortKey(
                SqlExpr.Column.of("p", A), true, null, "a")));
        SqlSelect s = wrapped(WireRender.rows(ordered, SCHEMA));
        assertEquals(1, s.orderBy().size());
    }

    @Test
    void aUnionAllIsWrappedBranchByBranch() {
        SqlUnion u = SqlUnion.ofBranches(List.of(projected(table(), List.of()), projected(table(), List.of())), true);
        SqlUnion rows = assertInstanceOf(SqlUnion.class, WireRender.rows(u, SCHEMA));
        assertEquals(2, rows.branches().size());
        for (SqlQuery b : rows.branches()) {
            wrapped(b);
        }
    }
}
