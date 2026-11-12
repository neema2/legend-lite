// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0
package com.legend.sql.dialect;

import com.legend.database.Databases;
import com.legend.model.ConnectionDefinition.DatabaseType;
import com.legend.sql.OutputCol;
import com.legend.sql.RaiseKind;
import com.legend.sql.SqlExpr;
import com.legend.sql.SqlFn;
import com.legend.sql.SqlSelect;
import com.legend.sql.SqlSource;
import com.legend.sql.SqlType;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * A RAISE'S MESSAGE IS ITS KIND (phase D's D1.1, 2026-11-12; the orchestrator's addition 3): the product's dialects
 * write a {@link SqlExpr.RaiseText}'s kind's name and never its operands, so a raise's SQL is byte-identical with its
 * operands and without them -- no value's text built, no parameter bound for one -- on every database; the user's own
 * {@code fail} message is the one operand written. The compatibility edge's dialects write legend-pure's sentence
 * ({@code edge.PureMessages}).
 */
class RaiseTextTest {

    private static SqlSelect select(SqlExpr message) {
        OutputCol v = new OutputCol("v", SqlType.Scalar.VARCHAR, true, OutputCol.Origin.DERIVED);
        SqlExpr raise = SqlExpr.Call.of(SqlFn.ERROR, message, new SqlExpr.StringLit("3:7"));
        return new SqlSelect(List.of(new SqlSelect.Projection(raise, "v", v)), false, new SqlSource.Dual(), null,
                List.of(), null, null, List.of(), null, null, List.of(v));
    }

    /** {@code dialect}'s text of {@code q}, or its refusal's message: the two renders compared either way. */
    private static String text(SqlDialect dialect, SqlSelect q) {
        try {
            return dialect.render(q);
        } catch (RuntimeException refused) {
            return "refused: " + refused.getMessage();
        }
    }

    private static final SqlExpr OPERAND = new SqlExpr.Cast(new SqlExpr.Column("t", "operand_col"), SqlType.Scalar.VARCHAR);

    @Test
    void aProductRaiseWritesItsKindAndNoOperand() {
        for (DatabaseType type : Databases.implemented()) {
            SqlDialect dialect = Databases.dialect(type);
            String with = text(dialect, select(new SqlExpr.RaiseText(RaiseKind.DIVIDE_BY_ZERO, List.of(OPERAND,
                    new SqlExpr.StringLit("by")))));
            String without = text(dialect, select(new SqlExpr.RaiseText(RaiseKind.DIVIDE_BY_ZERO, List.of())));
            assertEquals(without, with, type.name());
            assertFalse(with.contains("operand_col"), type + ": " + with);
            if (!with.startsWith("refused: ")) {
                assertTrue(with.contains("DIVIDE_BY_ZERO"), type + ": " + with);
            }
        }
    }

    @Test
    void aUsersFailMessageIsWritten() {
        for (DatabaseType type : Databases.implemented()) {
            String fail = text(Databases.dialect(type), select(new SqlExpr.RaiseText(RaiseKind.USER_FAIL,
                    List.of(new SqlExpr.StringLit("my own words")))));
            assertTrue(fail.startsWith("refused: ") || fail.contains("my own words"), type + ": " + fail);
            assertEquals(fail, text(com.legend.edge.PureText.dialect(type), select(new SqlExpr.RaiseText(
                    RaiseKind.USER_FAIL, List.of(new SqlExpr.StringLit("my own words"))))), type.name());
        }
    }

    @Test
    void aRaisesMessageIsOnlyItsKindAndOperands() {
        assertEquals(List.of("kind", "operands", "type"), Arrays.stream(SqlExpr.RaiseText.class.getRecordComponents())
                .map(java.lang.reflect.RecordComponent::getName).toList());
    }

    /** The edge writes TODAY's message of each moved family, byte for byte, on every database: its SQL is what the
     *  product wrote before the move (PCT's assertErrors compare legend-pure's text). */
    @Test
    void theEdgeWritesLegendPuresSentence() {
        SqlExpr a = new SqlExpr.Cast(new SqlExpr.Column("t", "operand_col"), SqlType.Scalar.BIGINT);
        SqlExpr before = SqlExpr.Call.of(SqlFn.CONCAT, SqlExpr.Call.of(SqlFn.CONCAT, new SqlExpr.StringLit(
                "Cannot divide "), new SqlExpr.Cast(a, SqlType.Scalar.VARCHAR)), new SqlExpr.StringLit(" by zero"));
        for (DatabaseType type : Databases.implemented()) {
            assertEquals(text(Databases.dialect(type), select(before)), text(com.legend.edge.PureText.dialect(type),
                    select(new SqlExpr.RaiseText(RaiseKind.DIVIDE_BY_ZERO, List.of(a)))), type + ": rem's");
            assertEquals(text(Databases.dialect(type), select(new SqlExpr.StringLit("Division by zero"))),
                    text(com.legend.edge.PureText.dialect(type), select(new SqlExpr.RaiseText(
                            RaiseKind.DIVIDE_BY_ZERO, List.of()))), type + ": Integer / Integer's");
        }
    }

    @Test
    void aKindNoSiteRaisesYetIsRefusedAtTheEdge() {
        var refused = assertThrows(UnsupportedOperationException.class, () -> com.legend.edge.PureMessages.sentence(
                new SqlExpr.RaiseText(RaiseKind.CSV_TOO_WIDE, List.of())));
        assertTrue(refused.getMessage().contains("CSV_TOO_WIDE"), refused.getMessage());
    }
}
