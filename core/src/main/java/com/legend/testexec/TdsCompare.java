// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.testexec;

import com.legend.testexecutionplan.TypedSide;

import java.util.ArrayList;
import java.util.List;

/**
 * THE TDS-GRID AND RENDERED-TEXT COMPARE POLICIES (One-Platform Plan
 * Phase 2b — moved from the harness at {@code wireEquals}' deletion;
 * the policies survive, the private copies die). Cell equality is
 * {@link PureAsserts#equalScalar} — the ONE owner; this class holds the
 * GRID-SHAPED adjudications:
 *
 * <ul>
 * <li><b>Header pins</b> — column names compare ordered and exactly
 * (the platform emits them; a divergence is a finding).</li>
 * <li><b>The order policy</b> — pure equality is ordered, but an
 * actual side with no {@code sort} in its chain has no defined SQL row
 * order (corpus expectations encode H2's incidental order, ours is
 * DuckDB's): rows then compare as a TUPLE multiset — row cohesion,
 * never loose cells (cross-row shuffles must fail).</li>
 * <li><b>Rendered text</b> (F4.3) — CSV/TDS frame and header lines pin
 * exactly; data lines ordered-or-multiset per the chain's sortedness;
 * cells string-equal or bounded-float-tolerant ({@code LL_TOL_COUNT}
 * counted). The cross-kind numeric collapse stays deleted.</li>
 * <li><b>Instrumentation</b> — {@code LL_ORD_COUNT} emits an
 * {@code [ord]} line when a pass depended on order leniency
 * (C0.4/F2.4); measurement only, never the verdict.</li>
 * </ul>
 */
public final class TdsCompare {

    private TdsCompare() {
    }

    /** TDS grids compare STRUCTURALLY: column names ordered+exact, rows
     * under the order policy. */
    public static boolean grids(TypedSide.Tabular expected,
            TypedSide.Tabular actual, boolean ordered, HostRules rules) {
        if (expected.columns().size() != actual.columns().size()) {
            return false;
        }
        for (int i = 0; i < expected.columns().size(); i++) {
            if (!expected.columns().get(i).name()
                    .equals(actual.columns().get(i).name())) {
                return false;
            }
        }
        List<List<Object>> e = new ArrayList<>();
        expected.rows().forEach(r -> e.add(r.values()));
        List<List<Object>> a = new ArrayList<>();
        actual.rows().forEach(r -> a.add(r.values()));
        if (e.size() != a.size()) {
            return false;
        }
        // the grid's DECLARED column types decide each cell (step 2b);
        // the rows are the judge's, ordered or as a multiset
        // each grid's cells typed by its own declared columns (the user's ruling, 2026-11-03: never the peer's)
        List<String> eKinds = new ArrayList<>();
        expected.columns().forEach(c -> eKinds.add(c.pureType()));
        List<String> aKinds = new ArrayList<>();
        actual.columns().forEach(c -> aKinds.add(c.pureType()));
        List<Object> ef = new ArrayList<>();
        e.forEach(ef::addAll);
        List<Object> af = new ArrayList<>();
        a.forEach(af::addAll);
        List<Equality.Typed> te = Equality.grid(ef, eKinds);
        List<Equality.Typed> ta = Equality.grid(af, aKinds);
        if (ordered) {
            return Equality.ordered(te, ta, rules);
        }
        if (!Equality.rowMultiset(te, ta, Math.max(1, eKinds.size()), rules)) {
            return false;
        }
        // H6 PROTOTYPE (MEASUREMENT ONLY): an unordered grid compared as a row multiset where the ordered compare fails
        if (!Equality.ordered(te, ta, rules)) {
            com.legend.diagnostics.LeniencyProbe.changed("S131", "grid rows as a multiset");
            if ((com.legend.diagnostics.LeniencyProbe.off("S131") || com.legend.diagnostics.LeniencyProbe.off("S131T"))) {
                return false;
            }
        }
        // C0.4: a multiset pass the POSITIONAL compare would reject is a
        // pass that depends on order leniency — countable per sweep
        ordLeniency("row-multiset", () -> Equality.ordered(te, ta, rules));
        return true;
    }

    /** ROW COHESION (audit 9): an ordered compare's multiset fallback
     * matches ROW TUPLES of width {@code w}, never loose cells. */

    // ── V7 §8 leg 1: the GRID-CANON byte-channel policies — the
    // flat-cells comparison rules live HERE with the other grid
    // policies (renderedText/grids/rowTupleMultiset), the one owner;
    // the verdict arm only dispatches. Every refusal counts a named
    // decline — census fuel, never a silent skip.

    /** First differing entry of two (sorted) canon lists — the alarm's
     * diagnosis payload (a bare host/sql flag cannot be diagnosed). */
    public static String firstCanonDiff(List<String> e, List<String> a) {
        int n = Math.min(e.size(), a.size());
        for (int i = 0; i < n; i++) {
            if (!e.get(i).equals(a.get(i))) {
                return " diff@" + i + " e<" + truncCanon(e.get(i))
                        + "> a<" + truncCanon(a.get(i)) + ">";
            }
        }
        return e.size() != a.size()
                ? " sizes " + e.size() + "/" + a.size() : "";
    }

    private static String truncCanon(String s) {
        return s.length() > 120 ? s.substring(0, 120) + "…" : s;
    }




    /** {@code assertTdsEquivalent} cell comparison (engine
     * tdsEquivalent.pure — moved from the harness's TdsEquivalence at
     * Phase 2b): numeric cells within {@code |delta|}, temporal cells
     * within {@code |timeDeltaInSeconds|} seconds, everything else by
     * same-kind equality (the cross-kind String.valueOf collapse stays
     * deleted — F6.5); rows zip IN ORDER. Null = equivalent. */
    public static @com.legend.base.Nullable String tdsEquivalent(
            List<Object> expected, List<Object> got, double delta,
            double timeDeltaSeconds) {
        if (expected.size() != got.size()) {
            return "assertTdsEquivalent: expected " + expected.size()
                    + " cells, got " + got.size();
        }
        for (int i = 0; i < expected.size(); i++) {
            Object x = expected.get(i);
            Object y = got.get(i);
            if (x == null && y == null) {
                continue;
            }
            if (x instanceof Number nx && y instanceof Number ny) {
                if (Math.abs(nx.doubleValue() - ny.doubleValue())
                        <= Math.abs(delta)) {
                    continue;
                }
            } else {
                Double ex = epochSeconds(x);
                Double ey = epochSeconds(y);
                if (ex != null && ey != null) {
                    if (Math.abs(ex - ey) <= Math.abs(timeDeltaSeconds)) {
                        continue;
                    }
                } else if (java.util.Objects.equals(x, y)) {
                    continue;
                }
            }
            return "assertTdsEquivalent: cell " + i + " expected " + x
                    + ", got " + y;
        }
        return null;
    }

    private static @com.legend.base.Nullable Double epochSeconds(
            @com.legend.base.Nullable Object v) {
        // THE wire temporal type only (D-arc: sql/java.time temporals
        // never escape the fetch seam) — instant floor, UTC
        return v instanceof com.legend.values.PureDateLiteral d
                ? (double) d.toInstantFloor()
                        .toEpochSecond(java.time.ZoneOffset.UTC)
                : null;
    }

    /** C0.4: under {@code LL_ORD_COUNT}, emit an {@code [ord]} line when
     * a comparison passed ONLY because of multiset row leniency —
     * {@code strictHolds} is the order-strict re-check. Measurement
     * only; never changes the verdict. {@code tag} (audit-of-audits
     * #10) names the SITE so the census separates loose-cell matching
     * from row-tuple matching in the counted population. */
    public static void ordLeniency(String tag,
            java.util.function.BooleanSupplier strictHolds) {
        if (System.getenv("LL_ORD_COUNT") != null
                && !strictHolds.getAsBoolean()) {
            System.err.println("[ord] " + tag + " order-leniency-dependent pass");
        }
    }
}
