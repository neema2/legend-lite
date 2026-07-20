// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.resolver;

import com.legend.compiler.spec.typed.TypedSpec;
import java.util.List;
/**
 * THE milestoning context &mdash; engine parity: ONE context object per
 * cursor ({@code MilestoningDatesPropagationFunctions} /
 * {@code getMilestoningContextForQualifiedProperty}). It answers, for any
 * table or class, <em>"what date filter applies HERE, in which
 * dimension?"</em> &mdash; the question the resolver previously
 * reconstructed per site from {@code (rootMilestoning, rootStrategy,
 * rootSweep, date-list)} tuples, the recurring audit bug seam (audit 13:
 * dimension + provenance; audit 14: mid-table governance).
 *
 * <p>Forms:
 * <ul>
 *   <li>{@link #NONE} &mdash; no context: {@code allVersions()} sweeps and
 *       non-temporal fetches. Nothing filters.</li>
 *   <li>POINT &mdash; a date per DIMENSION the context carries: a
 *       single-dimension fetch fills one, a bi-temporal fetch fills both
 *       ({@code .all(processingDate, businessDate)} &mdash; real pure's
 *       argument order).</li>
 *   <li>RANGE &mdash; a validity-overlap window
 *       ({@code allVersionsInRange} and its {@code getAll(C, start, end)}
 *       spelling): every supporting table range-filters; there is NO point
 *       date to propagate or serialize (audit: the old size-2 list made
 *       range vs bi-temporal a per-site guess).</li>
 * </ul>
 *
 * <p>Dimension logic lives HERE and nowhere else: {@link #dateFor} returns
 * the date matching a table's own milestoning dimension &mdash;
 * cross-dimension asks answer {@code null} (engine capability rule: a
 * business date never filters processing columns).
 */
record TemporalContext(TypedSpec processing, TypedSpec business,
                       TypedSpec rangeStart, TypedSpec rangeEnd,
                       String rangeDim) {

    static final TemporalContext NONE =
            new TemporalContext(null, null, null, null, null);

    static TemporalContext single(String strategy, TypedSpec date) {
        // audit 23: EXHAUSTIVE — the old else built a BUSINESS context
        // for ANY other strategy (a bitemporal class with one date, or a
        // null strategy, silently stamped business columns only)
        return switch (strategy) {
            case "processingtemporal" ->
                    new TemporalContext(date, null, null, null, null);
            // 'bitemporal' with ONE date: the propagation convention fills
            // the BUSINESS slot — pinned row-correct by the three
            // *ToBiTemporalDatePropagation corpus tests (probed loud and
            // reverted, audit 23 batch 2). A dimension-TAGGED propagation
            // is the honest future shape; this API loses the source
            // dimension.
            case "businesstemporal", "bitemporal" ->
                    new TemporalContext(null, date, null, null, null);
            case null, default ->
                    throw new com.legend.error.NotImplementedException(
                            "single-date temporal context for strategy '"
                            + strategy + "' is not defined");
        };
    }

    /** Real pure {@code .all(processingDate, businessDate)} order. */
    static TemporalContext bitemporal(TypedSpec processingDate,
            TypedSpec businessDate) {
        return new TemporalContext(processingDate, businessDate, null, null, null);
    }

    /** A validity-overlap window IN ONE DIMENSION ({@code rangeDim} = the
     * swept class's strategy) — the old size-2 date list left range vs
     * bi-temporal vs dimension a per-site guess. */
    static TemporalContext range(String strategy, TypedSpec start,
            TypedSpec end) {
        return new TemporalContext(null, null, start, end, strategy);
    }

    boolean rangeAppliesTo(String strategy) {
        return rangeStart != null && strategy != null
                && strategy.equals(rangeDim);
    }

    boolean isEmpty() {
        return processing == null && business == null && rangeStart == null;
    }

    boolean isRange() {
        return rangeStart != null;
    }

    /** The point date for a table/class of {@code strategy}'s dimension;
     * {@code null} when the context has none for it (cross-dimension = no
     * filter), when the context is a RANGE, or when {@code strategy} is
     * null/bitemporal (a bi-temporal consumer takes both via the
     * dimension-specific accessors). */
    TypedSpec dateFor(String strategy) {
        if ("processingtemporal".equals(strategy)) {
            return processing;
        }
        if ("businesstemporal".equals(strategy)) {
            return business;
        }
        return null;
    }

    /** The legacy {@code rootMilestoning} list shape (the Substitution
     * boundary keeps it this slice): bi-temporal = [processing, business],
     * single = [date], range/none = [] (a range has no point to read
     * back). */
    List<TypedSpec> legacyDates() {
        if (processing != null && business != null) {
            return List.of(processing, business);
        }
        if (processing != null) {
            return List.of(processing);
        }
        if (business != null) {
            return List.of(business);
        }
        return List.of();
    }
}
