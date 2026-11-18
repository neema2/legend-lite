// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.testexec;

import com.legend.base.Nullable;
import com.legend.testexecutionplan.TypedSide;

import java.util.List;

/**
 * THE HOST JUDGE'S WORDS OF A FAILED ASSERT (census J29, 2026-11-18, the orchestrator's order: "every legend-pure
 * sentence comes from an edge text hook; the product holds only lite's sentences"): one method per failure kind, over
 * the facts the verdict already decided -- a method words a failure, it never decides one. The run hook names them
 * ({@link HostForms#words}): lite's runs {@link LiteHostWords} (the product's {@code AssertSentence} table), the
 * compatibility edge's legend-pure's words ({@code edge.EdgeHostWords}). The judge asks; it never branches on whose
 * words they are. Signatures only (HookRulingTest).
 */
public interface HostWords {

    /** The words' name, the plan's data ({@code TestPlan.words}): a run refuses a plan worded otherwise. */
    String name();

    /** A TDSRow.values pair that differs: the raw cells, their typed grids, and whether the order is incidental. */
    String rowValuesDiffer(List<Object> e, List<Object> a, List<Equality.Typed> te, List<Equality.Typed> ta,
            boolean incidental);

    /** An assertNotEquals that found the sides equal ({@code aClass}: the actual side's class, if an instance). */
    String notEqual(List<Object> e, List<Object> a, @Nullable String aClass);

    /** An assertSameElements over two sides a verdict has failed, each side's values in its own order. */
    String sameElements(List<Object> e, List<Object> a, @Nullable String eClass, @Nullable String aClass);

    /** An assertSameElements over two TDSRow.values cell pools that differ as multisets. */
    String cellMultisetDiffers(String family, List<Object> e, List<Object> a, HostRules rules);

    /** An assertTdsEquivalent that failed: the columns or the row counts ({@code cellDiff} null), or a cell. */
    String notEquivalent(TypedSide.Tabular one, TypedSide.Tabular two, double delta, @Nullable String cellDiff);

    /** Two grids that are equal where {@code wantEqual} is false, or differ where it is true. */
    String grids(TypedSide.Tabular te, TypedSide.Tabular ta, boolean wantEqual);

    /** An assertSize that failed. */
    String size(@Nullable Object expected, long actual);

    /** An assertContains whose collection holds no such value. */
    String notContained(List<Object> collection, @Nullable Object value, @Nullable String collectionClass,
            @Nullable String valueClass);

    /** An assertEqWithinTolerance that failed. */
    String notWithin(Number expected, Number actual, Number delta);

    /** A default-message assert's failure ({@code kind}: NOT_EQ, IS_NOT, NOT_CONTAINS, NOT_SIZE), over its sides. */
    String negated(String kind, List<Object> first, List<Object> second);

    /** An assert or assertFalse over a condition that failed. */
    String condition(boolean wantTrue);

    /** An assertEmpty ({@code wantEmpty}) or assertNotEmpty that failed over {@code side}, its values decoded. */
    String emptiness(boolean wantEmpty, TypedSide side, List<Object> values);

    /** An assertEq whose values are not eq. */
    String notEq(@Nullable Object e, @Nullable Object a, @Nullable String eClass, @Nullable String aClass);

    /** An assertInstanceOf over an instance of {@code wireType} that is no {@code type}. */
    String notInstanceOfClass(@Nullable Object v, String wireType, String type);

    /** An assertInstanceOf over a value whose carrier, a {@code carrier}, is no {@code type}. */
    String notInstanceOf(@Nullable Object v, String type, String carrier);

    /** An assertIs whose element references differ. */
    String notSameInstance(String name);

    /** A quantified assert's element's failure, {@code sentence}, at {@code index}: the words as they name it. */
    String atIndex(String sentence, int index);

    /** A row of a quantified assert whose values differ. */
    String rowNotEqual(@Nullable Object e, @Nullable Object a, @Nullable String eClass, @Nullable String aClass);

    /** An assertEquals whose sides differ: their values, as carried ({@code eCarried}, {@code aCarried}), and their
     *  classes. */
    String sidesNotEqual(List<Object> e, List<Object> a, List<Object> eCarried, List<Object> aCarried,
            @Nullable String eClass, @Nullable String aClass);

    /** An assertJsonStringsEqual whose texts differ, at {@code diff}. */
    String jsonDiffers(String expected, String actual, boolean incidentalRoot, String diff);

    /** An assertError whose body raised nothing. */
    String noError(String expected);

    /** An assertError whose body raised {@code actual}. */
    String errorDiffers(String expected, String actual);

    /** An assertError whose error was raised at another line. */
    String lineDiffers(long expected, long actual);

    /** An assertError whose error was raised at another column. */
    String columnDiffers(long expected, long actual);

    /** An UNJUDGED verdict row's evidence: its two canons' excerpts, framed for a diagnosis. */
    String unjudged(String expected, String actual);

    /** The message an assertError matches a caught {@code error} by (K10 of the edge audit, the orchestrator's ruling
     *  (A) of 2026-11-17, re-ported onto J29's hook, 2026-11-18): lite's words, the error's own message; legend-pure's,
     *  for a raise of a planned site, the words the plan's edge records hold for it ({@code hearing}'s
     *  {@link EdgeHearing#raisedWords}). */
    String caught(RuntimeException error, EdgeHearing hearing);
}
