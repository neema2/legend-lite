// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.lowering;

import com.legend.base.Nullable;
import com.legend.compiler.element.type.Type;
import com.legend.error.AssertKind;
import com.legend.sql.SqlExpr;
import com.legend.sql.SqlFn;

import java.util.Map;
import java.util.function.UnaryOperator;

import static com.legend.lowering.VerdictSql.sentence;

/**
 * THE PRODUCT'S FORM OF {@link VerdictWords} (census J29's planning half, 2026-11-18): lite's own sentences, the
 * {@link AssertKind} table both judges share, each side WRITTEN by lite's rules ({@link LiteText}).
 */
public final class LiteVerdictWords implements VerdictWords {

    public static final LiteVerdictWords INSTANCE = new LiteVerdictWords();

    private LiteVerdictWords() {
    }

    private static SqlExpr lit(String s) {
        return new SqlExpr.StringLit(s);
    }

    @Override
    public SqlExpr sizeDiffers(SqlExpr want, SqlExpr count) {
        return sentence(AssertKind.SIZE, Map.of("expected", want, "actual", count));
    }

    @Override
    public SqlExpr sizeEquals(SqlExpr size) {
        return sentence(AssertKind.NOT_SIZE, Map.of("size", size));
    }

    @Override
    public SqlExpr notEmpty() {
        return lit(sentence(AssertKind.NOT_EMPTY));
    }

    @Override
    public SqlExpr notEmptyWritten(SqlExpr writtenList) {
        return sentence(AssertKind.EMPTY, Map.of("collection", writtenList));
    }

    @Override
    public SqlExpr notContained(SqlExpr writtenList, SqlExpr writtenValue) {
        return sentence(AssertKind.CONTAINS, Map.of("collection", writtenList, "value", writtenValue));
    }

    @Override
    public SqlExpr contained(SqlExpr writtenList, SqlExpr writtenValue) {
        return sentence(AssertKind.NOT_CONTAINS, Map.of("collection", writtenList, "value", writtenValue));
    }

    @Override
    public SqlExpr notContainedCanons(SqlExpr list, SqlExpr value) {
        return sentence(AssertKind.CONTAINS, Map.of("collection", list, "value", value));
    }

    @Override
    public SqlExpr notInstanceOf(SqlExpr text, boolean quote, String type, String actual) {
        SqlExpr value = SqlExpr.Call.of(SqlFn.COALESCE, quote ? LiteText.of(Type.Primitive.STRING, text) : text,
                lit("[]"));
        return sentence(AssertKind.INSTANCE_OF, Map.of("type", lit(type), "actual", value));
    }

    @Override
    public SqlExpr elementText(Type kind, SqlExpr value, Styled styled) {
        return LiteText.of(kind, value);
    }

    @Override
    public @Nullable SqlExpr elementFailure(SqlExpr expected, SqlExpr actual, String expectedKind, String actualKind,
            SqlExpr index) {
        // lite's sentence names the first failing element by its zero-based index (the AT_INDEX frame around EQUALS)
        return atIndex(VerdictSql.equalsOrKinds(expected, actual, lit(expectedKind), lit(actualKind),
                sentence(AssertKind.EQUALS, Map.of("expected", expected, "actual", actual))), index);
    }

    @Override
    public @Nullable SqlExpr notWithin(SqlExpr expected, SqlExpr delta, SqlExpr actual, Type kind) {
        return sentence(AssertKind.WITHIN_TOLERANCE, Map.of("expected", LiteText.of(kind, expected), "delta",
                LiteText.of(kind, delta), "actual", LiteText.of(kind, actual)));
    }

    @Override
    public SqlExpr notEquivalent(String columns, SqlExpr expectedRows, SqlExpr actualRows, SqlExpr sameCount,
            SqlExpr cellIndex, SqlExpr expectedCell, SqlExpr actualCell, SqlExpr delta,
            @Nullable SqlExpr expectedWritten, @Nullable SqlExpr actualWritten) {
        return sentence(AssertKind.TDS_EQUIVALENT, Map.of(
                "expected", java.util.Objects.requireNonNull(expectedWritten, "the expected grid, written"),
                "delta", LiteText.of(Type.Primitive.FLOAT, delta),
                "actual", java.util.Objects.requireNonNull(actualWritten, "the actual grid, written")));
    }

    @Override
    public SqlExpr noError(String expected) {
        return sentence(AssertKind.NO_ERROR, Map.of("expected", LiteText.of(Type.Primitive.STRING,
                lit(expected))));
    }

    @Override
    public SqlExpr errorDiffers(String expected, SqlExpr actual) {
        return sentence(AssertKind.ERROR, Map.of("expected", LiteText.of(Type.Primitive.STRING, lit(expected)),
                "actual", LiteText.of(Type.Primitive.STRING, actual)));
    }

    @Override
    public SqlExpr lineDiffers(long expected, SqlExpr actual) {
        return sentence(AssertKind.ERROR_LINE, Map.of("expected", lit(Long.toString(expected)), "actual", actual));
    }

    @Override
    public SqlExpr columnDiffers(long expected, SqlExpr actual) {
        return sentence(AssertKind.ERROR_COLUMN, Map.of("expected", lit(Long.toString(expected)), "actual",
                actual));
    }

    @Override
    public SqlExpr frameSide(SqlExpr text, UnaryOperator<SqlExpr> list) {
        return SqlExpr.Call.of(SqlFn.COALESCE, list.apply(text), lit("[]"));
    }

    @Override
    public SqlExpr identical(SqlExpr actual) {
        return sentence(AssertKind.IS_NOT, Map.of("actual", actual));
    }

    @Override
    public SqlExpr frame(boolean json, boolean multiset, boolean wantEqual, SqlExpr expected, SqlExpr actual,
            @Nullable SqlExpr expectedKind, @Nullable SqlExpr actualKind) {
        if (!wantEqual) {
            return sentence(AssertKind.NOT_EQUALS, Map.of("actual", actual));
        }
        AssertKind row = json ? AssertKind.JSON_EQUALS
                : multiset ? AssertKind.SAME_ELEMENTS : AssertKind.EQUALS;
        SqlExpr plain = sentence(row, Map.of("expected", expected, "actual", actual));
        if (row != AssertKind.EQUALS || expectedKind == null || actualKind == null) {
            return plain;
        }
        // two texts that tie: each side's kind after it (the orchestrator's ruling, 2026-11-17)
        return VerdictSql.equalsOrKinds(expected, actual, expectedKind, actualKind, plain);
    }

    @Override
    public SqlExpr atIndex(SqlExpr message, int index) {
        return atIndex(message, lit(Integer.toString(index)));
    }

    private static SqlExpr atIndex(SqlExpr message, SqlExpr index) {
        return VerdictSql.atIndexFrame(message, index);
    }
}
