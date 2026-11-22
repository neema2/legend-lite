// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.lowering;

import com.legend.base.Nullable;
import com.legend.compiler.element.type.Type;
import com.legend.sql.SqlExpr;

import java.util.function.UnaryOperator;

/**
 * THE DATABASE JUDGE'S WORDS OF A FAILED ASSERT, PLANNED (census J29's planning half, 2026-11-18, the orchestrator's
 * order: "every legend-pure sentence comes from an edge text hook; the product holds only lite's sentences"). One method
 * per failure kind, each building the verdict row's message from the SQL pieces the verdict already computed -- a method
 * words a failure, it never decides one. Its one form is {@link LiteVerdictWords} (lite's {@code AssertKind} table):
 * until item 3's S3 (2026-11-21) a two-form kind with legend-pure's form at the edge (UpstreamVerdictWords, gone); the
 * database judge plans for lite's flow alone. Goes at S4, when the database judge carries its values to the record.
 * Signatures only.
 */
public interface VerdictWords {

    /** An assertSize that failed: the size wanted and the count, each as text. */
    SqlExpr sizeDiffers(SqlExpr want, SqlExpr count);

    /** An assertNotSize that failed: the size, as text. */
    SqlExpr sizeEquals(SqlExpr size);

    /** An assertNotEmpty that failed. */
    SqlExpr notEmpty();

    /** An assertEmpty that failed over a collection written as one bracketed list. */
    SqlExpr notEmptyWritten(SqlExpr writtenList);

    /** An assertContains that failed, the collection and the value written. */
    SqlExpr notContained(SqlExpr writtenList, SqlExpr writtenValue);

    /** An assertNotContains that failed, the collection and the value written. */
    SqlExpr contained(SqlExpr writtenList, SqlExpr writtenValue);

    /** An assertContains that failed, the collection's canons as a list and the value's canon. */
    SqlExpr notContainedCanons(SqlExpr list, SqlExpr value);

    /** An assertInstanceOf decided false at planning: the value's text ({@code quote}: a String, quoted as written),
     *  the type's name, and the value's own type's. */
    SqlExpr notInstanceOf(SqlExpr text, boolean quote, String type, String actual);

    /** A quantified assert's element written: {@code value} of {@code kind}. */
    SqlExpr elementText(Type kind, SqlExpr value, Styled styled);

    /** A quantified assert's first failing element, at the zero-based {@code index}; null: the frame's words. */
    @Nullable SqlExpr elementFailure(SqlExpr expected, SqlExpr actual, String expectedKind, String actualKind,
            SqlExpr index);

    /** An assertEqWithinTolerance that failed, each number of {@code kind}; null: the frame's words. */
    @Nullable SqlExpr notWithin(SqlExpr expected, SqlExpr delta, SqlExpr actual, Type kind);

    /** An assertError whose body raised nothing. */
    SqlExpr noError(String expected);

    /** An assertError whose body raised the message {@code actual}. */
    SqlExpr errorDiffers(String expected, SqlExpr actual);

    /** An assertError whose error was raised at another line, {@code actual} its text. */
    SqlExpr lineDiffers(long expected, SqlExpr actual);

    /** An assertError whose error was raised at another column, {@code actual} its text. */
    SqlExpr columnDiffers(long expected, SqlExpr actual);

    /** A side's text in the frame where it is not written: {@code text}, a grid's cells listed by {@code list}. */
    SqlExpr frameSide(SqlExpr text, UnaryOperator<SqlExpr> list);

    /** An assertIsNot that failed. */
    SqlExpr identical(SqlExpr actual);

    /** The frame's failure over two sides' texts: a JSON pair, a multiset, or two values ({@code wantEqual} false: a
     *  negated family); each side's kind where the two texts may tie, or null. */
    SqlExpr frame(boolean json, boolean multiset, boolean wantEqual, SqlExpr expected, SqlExpr actual,
            @Nullable SqlExpr expectedKind, @Nullable SqlExpr actualKind);

    /** An unrolled quantifier's element's failure {@code message}, at the zero-based {@code index}. */
    SqlExpr atIndex(SqlExpr message, int index);
}
