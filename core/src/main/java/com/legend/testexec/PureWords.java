// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.testexec;

import com.legend.base.Nullable;

import java.util.List;

/**
 * LEGEND-PURE'S WORDS OF AN ASSERT'S FAILURE, AT RUN (C5's step B, the orchestrator's ruling (1) of 2026-11-17): the
 * host judge's messages under the compatibility rules -- legend-pure's {@code toRepresentation} of a value and the
 * assert family's sentences around it (assertEquals.pure, assertSameElements.pure, ...) -- over values that exist only
 * after the run, so worded at run, never at planning. A ruled hook (HookRulingTest): signatures only; the compatibility
 * edge implements it ({@code edge.EdgePureWords}), reached through the run's {@link EdgeHearing#pureWords}; the
 * product's {@link NoEdgeRun} refuses it by name. Under lite's own rules the host judge writes lite's words
 * ({@code LiteWords}) and never asks. Each decision stays the product's ({@link PureAsserts}): these only word it.
 */
public interface PureWords {

    /** {@code v}'s {@code toRepresentation} ({@code instanceClass}: an instance's {@code instanceOf} half). */
    String repr(@Nullable Object v, @Nullable String instanceClass);

    /** A side as a list of representations ({@code joinStrings('[', ', ', ']')}). */
    String reprList(List<Object> side, @Nullable String instanceClass);

    /** assertEquals' failure: each side's representation where both hold one value, else both as lists. */
    String equals(List<Object> expected, List<Object> actual, @Nullable String expectedClass,
            @Nullable String actualClass);

    /** assertSameElements' failure over two sides already sorted (both as lists of representations). */
    String sameElements(List<Object> sortedExpected, List<Object> sortedActual, @Nullable String expectedClass,
            @Nullable String actualClass);

    /** {@code '\nexpected: %r\nactual:   %r'} of two values. */
    String pair(@Nullable Object expected, @Nullable String expectedClass, @Nullable Object actual,
            @Nullable String actualClass);

    /** assertNotEquals' failure. */
    String notEquals(List<Object> notExpected, List<Object> actual);

    /** assertContains' failure. */
    String notContains(List<Object> collection, @Nullable Object value, @Nullable String collectionClass,
            @Nullable String valueClass);

    /** assertEmpty's failure. */
    String notEmpty(List<Object> collection);

    /** assertSize's failure. */
    String size(@Nullable Object expected, long actual);

    /** assertIsNot's failure: {@code 'Should not be identical to: %r'}. */
    String identical(@Nullable Object actual);

    /** assertNotContains' failure: {@code '%s should not contain %r'}. */
    String contains(List<Object> collection, @Nullable Object value);

    /** A grid's rows as the database judge frames a side's canons (none {@code []}, one its cells, several in
     *  brackets; a row's cells by {@code %r}), the pair as assertEquals' ({@code wantEqual}) or assertNotEquals'
     *  failure. */
    String grids(List<List<Object>> expected, List<List<Object>> actual, boolean wantEqual);

    /** assertInstanceOf's failure: {@code v} (an instance of {@code valueClass}, if one) is not a {@code type}, its
     *  carrier an {@code actual}. */
    String notInstanceOf(@Nullable Object v, @Nullable String valueClass, String type, String actual);
}
