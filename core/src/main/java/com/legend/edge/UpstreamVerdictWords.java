// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.edge;

import com.legend.base.Nullable;
import com.legend.compiler.element.type.Type;
import com.legend.lowering.Styled;
import com.legend.lowering.VerdictReads;
import com.legend.lowering.VerdictSql;
import com.legend.sql.SqlExpr;
import com.legend.sql.SqlFn;

import java.util.List;
import java.util.function.UnaryOperator;

/**
 * LEGEND-PURE'S WORDS OF A FAILED ASSERT, THE DATABASE JUDGE'S (census J29's planning half, 2026-11-18): the edge's form
 * of the two-form {@link com.legend.lowering.VerdictWords} kind (census C1's FailureWords, which named whose words they
 * were by a flag) -- the assert files' own messages, moved from the product's VerdictSql: assertEquals.pure's
 * {@code '\nexpected: %r\nactual:   %r'}, assertSize.pure's, assertError.pure's, ... Each side read as its CANON, a
 * value as the representation facet writes it ({@code VerdictSql.represented}).
 */
public final class UpstreamVerdictWords implements com.legend.lowering.VerdictWords {

    public static final UpstreamVerdictWords INSTANCE = new UpstreamVerdictWords();

    private UpstreamVerdictWords() {
    }

    private static SqlExpr lit(String s) {
        return new SqlExpr.StringLit(s);
    }

    private static SqlExpr concat(SqlExpr... parts) {
        return new SqlExpr.Call(SqlFn.CONCAT, List.of(parts));
    }

    @Override
    public String name() {
        return EdgeHostWords.NAME;
    }

    @Override
    public VerdictReads reads() {
        return VerdictReads.CANONS;
    }

    @Override
    public SqlExpr sizeDiffers(SqlExpr want, SqlExpr count) {
        // assertSize.pure: 'expected size: %s, actual size: %s'
        return concat(lit("expected size: "), want, lit(", actual size: "), count);
    }

    @Override
    public SqlExpr sizeEquals(SqlExpr size) {
        // the prelude's assertNotSize: 'size should not equal: %s'
        return concat(lit("size should not equal: "), size);
    }

    @Override
    public SqlExpr notEmpty() {
        // assertNotEmpty.pure
        return lit("Expected non-empty collection");
    }

    @Override
    public SqlExpr notEmptyWritten(SqlExpr writtenList) {
        // assertEmpty.pure: '%s is not empty' over the collection as a List
        return concat(writtenList, lit(" is not empty"));
    }

    @Override
    public SqlExpr notContained(SqlExpr writtenList, SqlExpr writtenValue) {
        // assertContains.pure: '%s does not contain %r'
        return concat(writtenList, lit(" does not contain "), writtenValue);
    }

    @Override
    public SqlExpr contained(SqlExpr writtenList, SqlExpr writtenValue) {
        // the prelude's assertNotContains: '%s should not contain %r'
        return concat(writtenList, lit(" should not contain "), writtenValue);
    }

    @Override
    public SqlExpr notContainedCanons(SqlExpr list, SqlExpr value) {
        return concat(list, lit(" does not contain "), value);
    }

    @Override
    public SqlExpr notInstanceOf(SqlExpr text, boolean quote, String type, String actual) {
        // assertInstanceOf.pure: 'expected %r to be an instance of %s, actual: %s'
        SqlExpr repr = SqlExpr.Call.of(SqlFn.COALESCE, reprNewlines(quote ? VerdictSql.quoted(text) : text),
                lit("[]"));
        return concat(lit("expected "), repr, lit(" to be an instance of " + type + ", actual: " + actual));
    }

    @Override
    public SqlExpr elementText(Type kind, SqlExpr value, Styled styled) {
        return VerdictSql.represented(styled, kind, value);
    }

    @Override
    public @Nullable SqlExpr elementFailure(SqlExpr expected, SqlExpr actual, String expectedKind, String actualKind,
            SqlExpr index) {
        // the frame's words, as the host's row read words the first failing element
        return null;
    }

    @Override
    public @Nullable SqlExpr notWithin(SqlExpr expected, SqlExpr delta, SqlExpr actual, Type kind) {
        // the frame's words over the two values
        return null;
    }

    @Override
    public SqlExpr notEquivalent(String columns, SqlExpr expectedRows, SqlExpr actualRows, SqlExpr sameCount,
            SqlExpr cellIndex, SqlExpr expectedCell, SqlExpr actualCell, SqlExpr delta,
            @Nullable SqlExpr expectedWritten, @Nullable SqlExpr actualWritten) {
        // the host's words (the two grids' columns and rows where their row counts differ, upstream's '... is not
        // equivalent to: ...' shape; else the first cell not within, by its row-major index)
        SqlExpr rows = concat(lit("\n" + columns + " ("), expectedRows,
                lit(" rows)\n is not equivalent to:\n" + columns + " ("), actualRows, lit(" rows)"));
        SqlExpr cell = concat(lit("assertTdsEquivalent: cell "), cellIndex, lit(" expected "),
                SqlExpr.Call.of(SqlFn.COALESCE, expectedCell, lit("null")), lit(", got "),
                SqlExpr.Call.of(SqlFn.COALESCE, actualCell, lit("null")));
        return new SqlExpr.Case(List.of(new SqlExpr.Case.When(SqlExpr.Call.of(SqlFn.NOT, sameCount), rows)), cell,
                SqlExpr.Reason.JUDGE);
    }

    @Override
    public SqlExpr noError(String expected) {
        // interpreted AssertError.java: PureAssertFail("No error was thrown")
        return lit("No error was thrown");
    }

    @Override
    public SqlExpr errorDiffers(String expected, SqlExpr actual) {
        // assertError.pure:24, the /4 body's format
        return concat(lit("Execution error message mismatch.\nThe actual message was \""), actual,
                lit("\"\nwhere the expected message was:\"" + expected + "\""));
    }

    @Override
    public SqlExpr lineDiffers(long expected, SqlExpr actual) {
        // assertError.pure:25
        return concat(lit("Execution error line mismatch. Actual: "), actual, lit(" where expected: " + expected));
    }

    @Override
    public SqlExpr columnDiffers(long expected, SqlExpr actual) {
        // assertError.pure:26
        return concat(lit("Execution error column mismatch. Actual: "), actual, lit(" where expected: " + expected));
    }

    @Override
    public SqlExpr frameSide(SqlExpr text, UnaryOperator<SqlExpr> list) {
        // a newline inside a spelled value written as %r writes it, \n
        return SqlExpr.Call.of(SqlFn.COALESCE, list.apply(reprNewlines(text)), lit("null"));
    }

    @Override
    public SqlExpr identical(SqlExpr actual) {
        // the prelude's assertIsNot: 'Should not be identical to: %r'
        return concat(lit("Should not be identical to: "), actual);
    }

    @Override
    public SqlExpr frame(boolean json, boolean multiset, boolean wantEqual, SqlExpr expected, SqlExpr actual,
            @Nullable SqlExpr expectedKind, @Nullable SqlExpr actualKind) {
        // essential/tests/assertEquals.pure: '\nexpected: %r\nactual:   %r'; assertNotEquals.pure: '%r should not equal
        // %r' -- no family name before it
        return wantEqual ? concat(lit("\nexpected: "), expected, lit("\nactual:   "), actual)
                : concat(expected, lit(" should not equal "), actual);
    }

    @Override
    public SqlExpr atIndex(SqlExpr message, int index) {
        // legend-pure names no element: the element's own failure
        return message;
    }

    @Override
    public String conditionFailed(boolean wantTrue) {
        return "Assert failed";
    }

    @Override
    public String valuesDiffer(String expected, String actual) {
        return "\nexpected: " + expected + "\nactual:   " + actual;
    }

    /** {@code text} with each newline written {@code \\n}, as Pure's {@code %r} writes one inside a String (J40,
     *  2026-11-18: moved from the product's VerdictSql, which no product code read). */
    private static SqlExpr reprNewlines(SqlExpr text) {
        return SqlExpr.Call.of(SqlFn.REPLACE, text, new SqlExpr.StringLit("\n"), new SqlExpr.StringLit("\\n"));
    }
}
