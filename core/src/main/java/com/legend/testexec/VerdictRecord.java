// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.testexec;

import com.legend.base.Nullable;
import com.legend.testexecutionplan.TypedSide;

import java.util.List;

/**
 * H10 SPIKE (claude/compat-judges-spike, never for merge; CP-14 (iii), ruled 2026-11-19): A FAILED VERDICT AS DATA --
 * its kind and its operands, the facts the host judge decided, and no words. One record per kind the deleted
 * {@code HostWords} named (25), its fields that method's arguments. The run's presenter
 * ({@link JudgeRun.Hearing#present}) words it in its audience's words: lite's ({@code LiteHostWords.present}) or
 * legend-pure's ({@code edge.EdgeHostWords.present}). The judge never knows whose.
 */
public sealed interface VerdictRecord {

    /** A TDSRow.values pair that differs: the raw cells, their typed grids, and whether the order is incidental. */
    record RowValuesDiffer(List<Object> e, List<Object> a, List<Equality.Typed> te, List<Equality.Typed> ta,
            boolean incidental) implements VerdictRecord {
    }

    /** An assertNotEquals that found the sides equal ({@code aClass}: the actual side's class, if an instance). */
    record NotEqual(List<Object> e, List<Object> a, @Nullable String aClass) implements VerdictRecord {
    }

    /** An assertSameElements over two sides a verdict has failed, each side's values in its own order. */
    record SameElements(List<Object> e, List<Object> a, @Nullable String eClass, @Nullable String aClass)
            implements VerdictRecord {
    }

    /** An assertSameElements over two TDSRow.values cell pools that differ as multisets. */
    record CellMultisetDiffers(String family, List<Object> e, List<Object> a) implements VerdictRecord {
    }

    /** An assertTdsEquivalent that failed: the columns or the row counts ({@code cellDiff} null), or a cell. */
    record NotEquivalent(TypedSide.Tabular one, TypedSide.Tabular two, double delta, @Nullable String cellDiff)
            implements VerdictRecord {
    }

    /** Two grids that are equal where {@code wantEqual} is false, or differ where it is true. */
    record Grids(TypedSide.Tabular te, TypedSide.Tabular ta, boolean wantEqual) implements VerdictRecord {
    }

    /** An assertSize that failed. */
    record Size(@Nullable Object expected, long actual) implements VerdictRecord {
    }

    /** An assertContains whose collection holds no such value. */
    record NotContained(List<Object> collection, @Nullable Object value, @Nullable String collectionClass,
            @Nullable String valueClass) implements VerdictRecord {
    }

    /** An assertEqWithinTolerance that failed. */
    record NotWithin(Number expected, Number actual, Number delta) implements VerdictRecord {
    }

    /** A default-message assert's failure ({@code kind}: NOT_EQ, IS_NOT, NOT_CONTAINS, NOT_SIZE), over its sides. */
    record Negated(String kind, List<Object> first, List<Object> second) implements VerdictRecord {
    }

    /** An assert or assertFalse over a condition that failed. */
    record Condition(boolean wantTrue) implements VerdictRecord {
    }

    /** An assertEmpty ({@code wantEmpty}) or assertNotEmpty that failed over {@code side}, its values decoded. */
    record Emptiness(boolean wantEmpty, TypedSide side, List<Object> values) implements VerdictRecord {
    }

    /** An assertEq whose values are not eq, each side's declared type name. */
    record NotEq(@Nullable Object e, @Nullable Object a, @Nullable String eClass, @Nullable String aClass,
            @Nullable String eDeclared, @Nullable String aDeclared) implements VerdictRecord {
    }

    /** An assertInstanceOf over an instance of {@code wireType} that is no {@code type}. */
    record NotInstanceOfClass(@Nullable Object v, String wireType, String type) implements VerdictRecord {
    }

    /** An assertInstanceOf over a value whose carrier, a {@code carrier}, is no {@code type}. */
    record NotInstanceOf(@Nullable Object v, String type, String carrier) implements VerdictRecord {
    }

    /** An assertIs whose element references differ. */
    record NotSameInstance(String name) implements VerdictRecord {
    }

    /** A quantified assert's element's failure at {@code index}. SPIKE STUB: the inner failure arrives already worded
     *  ({@code sentence}), because an element's failure is raised as an {@code AssertFailed} carrying text; a build
     *  carries the inner record instead. */
    record AtIndex(String sentence, int index) implements VerdictRecord {
    }

    /** A row of a quantified assert whose values differ. */
    record RowNotEqual(@Nullable Object e, @Nullable Object a, @Nullable String eClass, @Nullable String aClass)
            implements VerdictRecord {
    }

    /** An assertEquals whose sides differ: their values, as carried, and their classes. */
    record SidesNotEqual(List<Object> e, List<Object> a, List<Object> eCarried, List<Object> aCarried,
            @Nullable String eClass, @Nullable String aClass) implements VerdictRecord {
    }

    /** An assertJsonStringsEqual whose texts differ, at {@code diff}. */
    record JsonDiffers(String expected, String actual, boolean incidentalRoot, String diff) implements VerdictRecord {
    }

    /** An assertError whose body raised nothing. */
    record NoError(String expected) implements VerdictRecord {
    }

    /** An assertError whose body raised {@code actual}. */
    record ErrorDiffers(String expected, String actual) implements VerdictRecord {
    }

    /** An assertError whose error was raised at another line. */
    record LineDiffers(long expected, long actual) implements VerdictRecord {
    }

    /** An assertError whose error was raised at another column. */
    record ColumnDiffers(long expected, long actual) implements VerdictRecord {
    }

    /** An UNJUDGED verdict row's evidence: its two canons' excerpts. */
    record Unjudged(String expected, String actual) implements VerdictRecord {
    }
}
