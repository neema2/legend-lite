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
            // a fresh dialect per statement: ClickHouse numbers each raise in its statement (rand(n), main's #81)
            String with = text(Databases.dialect(type), select(new SqlExpr.RaiseText(RaiseKind.DIVIDE_BY_ZERO,
                    List.of(OPERAND, new SqlExpr.StringLit("by")))));
            String without = text(Databases.dialect(type), select(new SqlExpr.RaiseText(RaiseKind.DIVIDE_BY_ZERO,
                    List.of())));
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

    private static SqlExpr cat(SqlExpr... parts) {
        SqlExpr out = parts[0];
        for (int i = 1; i < parts.length; i++) {
            out = SqlExpr.Call.of(SqlFn.CONCAT, out, parts[i]);
        }
        return out;
    }

    private static SqlExpr str(SqlExpr x) {
        return new SqlExpr.Cast(x, SqlType.Scalar.VARCHAR);
    }

    private static SqlExpr lit(String s) {
        return new SqlExpr.StringLit(s);
    }

    /** Each moved family's raise against the message the lowering wrote before the move (D1.1, families 2-12). */
    @Test
    void theEdgeWritesEachMovedFamilysSentence() {
        SqlExpr a = new SqlExpr.Cast(new SqlExpr.Column("t", "operand_col"), SqlType.Scalar.BIGINT);
        SqlExpr b = new SqlExpr.Cast(new SqlExpr.Column("t", "other_col"), SqlType.Scalar.BIGINT);
        java.util.Map<SqlExpr, SqlExpr> moved = new java.util.LinkedHashMap<>();
        moved.put(new SqlExpr.RaiseText(RaiseKind.SHIFT_OUT_OF_RANGE, List.of()),
                lit("Unsupported number of bits to shift - max bits allowed is 62"));
        moved.put(new SqlExpr.RaiseText(RaiseKind.DATE_PART_ABSENT, List.of(lit("hour"), a)),
                cat(lit("Cannot get hour for "), str(a)));
        moved.put(new SqlExpr.RaiseText(RaiseKind.INDEX_OUT_OF_BOUNDS, List.of(a, b)),
                cat(lit("The system is trying to get an element at offset "), str(a),
                        lit(" where the collection is of size "), str(b)));
        moved.put(new SqlExpr.RaiseText(RaiseKind.INSERT_OUT_OF_BOUNDS, List.of()),
                lit("add(set, index, value): index out of bounds"));
        moved.put(new SqlExpr.RaiseText(RaiseKind.SQRT_OF_NEGATIVE, List.of(a)),
                cat(lit("Unable to compute sqrt of "), SqlExpr.Call.of(SqlFn.FLOAT_PRINT, a)));
        moved.put(new SqlExpr.RaiseText(RaiseKind.SIZE_NOT_MULTIPLICITY, List.of(lit("[1..*]"))),
                lit("Cannot cast a collection of size 0 to multiplicity [1..*]"));
        moved.put(new SqlExpr.RaiseText(RaiseKind.SIZE_NOT_MULTIPLICITY, List.of(lit("[1]"), b)),
                new SqlExpr.Call(SqlFn.CONCAT, List.of(lit("Cannot cast a collection of size "), str(b),
                        lit(" to multiplicity [1]"))));
        moved.put(new SqlExpr.RaiseText(RaiseKind.SLICE_BOUNDS_INVERTED, List.of(a, b)),
                SqlExpr.Call.of(SqlFn.CONCAT, SqlExpr.Call.of(SqlFn.CONCAT, SqlExpr.Call.of(SqlFn.CONCAT,
                        lit("The low bound ("), str(a)), lit(") can't be higher than the high bound (")),
                        SqlExpr.Call.of(SqlFn.CONCAT, str(b), lit(") in a slice operation"))));
        moved.put(new SqlExpr.RaiseText(RaiseKind.DATE_COMPONENT_INVALID, List.of(lit("day"),
                new SqlExpr.IntLit(2016), new SqlExpr.IntLit(12), new SqlExpr.IntLit(32))), lit("Invalid day: 2016-12-32"));
        moved.put(new SqlExpr.RaiseText(RaiseKind.DATE_COMPONENT_INVALID, List.of(lit("month"), new SqlExpr.IntLit(13))),
                lit("Invalid month: 13"));
        moved.put(new SqlExpr.RaiseText(RaiseKind.DATE_COMPONENT_INVALID, List.of(lit("second"),
                new SqlExpr.FloatLit(61.5))), lit("Invalid second: 61.5"));
        moved.put(new SqlExpr.RaiseText(RaiseKind.DATE_COMPONENT_INVALID, List.of(lit("hour"), a)),
                cat(lit("Invalid hour: "), str(a)));
        moved.put(new SqlExpr.RaiseText(RaiseKind.CAST_IMPOSSIBLE, List.of(lit("Integer"), lit("Date"))),
                lit("Cast exception: Integer cannot be cast to Date"));
        moved.put(new SqlExpr.RaiseText(RaiseKind.RANGE_STEP_ZERO, List.of()), lit("range step must not be 0"));
        moved.put(new SqlExpr.RaiseText(RaiseKind.CONSTRAINT_UNEVALUABLE, List.of(lit("posInt"))),
                lit("Unable to evaluate constraint [posInt]: data not available - check your mappings"));
        moved.put(new SqlExpr.RaiseText(RaiseKind.USER_FAIL, List.of(lit("my own words"))), lit("my own words"));
        moved.put(new SqlExpr.RaiseText(RaiseKind.MAP_KEYS_MISALIGNED, List.of()),
                lit("The map key list does not align with the map value list."));
        moved.put(new SqlExpr.RaiseText(RaiseKind.MAP_KEY_NULL, List.of()), lit("Map keys can not be NULL."));
        moved.put(new SqlExpr.RaiseText(RaiseKind.MAP_KEYS_NOT_UNIQUE, List.of()), lit("Map keys must be unique."));
        moved.put(new SqlExpr.RaiseText(RaiseKind.TEXT_NOT_INTEGER, List.of(a)), SqlExpr.Call.of(SqlFn.CONCAT,
                lit("Could not convert string '"), a, lit("' to INT64")));
        moved.put(new SqlExpr.RaiseText(RaiseKind.TEXT_NOT_DATE, List.of(a)), SqlExpr.Call.of(SqlFn.CONCAT,
                lit("Could not parse '"), a, lit("' as a date")));
        moved.put(new SqlExpr.RaiseText(RaiseKind.VALUE_NOT_HELD, List.of(lit("an integer past 64 bits has no SQLite"
                + " value"))), lit("an integer past 64 bits has no SQLite value"));
        moved.put(new SqlExpr.RaiseText(RaiseKind.FRACTION_NOT_KEPT, List.of(lit("the second "), a, lit(" has more"))),
                cat(lit("the second "), str(a), lit(" has more")));
        moved.put(new SqlExpr.RaiseText(RaiseKind.CSV_TOO_WIDE, List.of(lit("a record of the CSV file is wide"))),
                lit("a record of the CSV file is wide"));
        moved.put(new SqlExpr.RaiseText(RaiseKind.LIST_LENGTHS_DIFFER, List.of(lit("zip"))),
                lit("zip: the two value lists differ in length"));
        for (DatabaseType type : Databases.implemented()) {
            for (var e : moved.entrySet()) {
                // both through the edge: a raise nested in the old message's own SQL (SQLite's Float print) is the
                // edge's sentence on both sides
                assertEquals(text(com.legend.edge.PureText.dialect(type), select(e.getValue())),
                        text(com.legend.edge.PureText.dialect(type), select(e.getKey())), type + ": " + e.getKey());
            }
        }
    }
}
