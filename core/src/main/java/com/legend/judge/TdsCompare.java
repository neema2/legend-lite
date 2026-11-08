// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.judge;

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
            TypedSide.Tabular actual, boolean ordered, com.legend.executionplan.ExecutionPlan.JudgingRules rules) {
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
            return Equality.ordered(te, ta, rules) == null;
        }
        if (!Equality.rowMultiset(te, ta, Math.max(1, eKinds.size()), rules)) {
            return false;
        }
        // C0.4: a multiset pass the POSITIONAL compare would reject is a
        // pass that depends on order leniency — countable per sweep
        ordLeniency("row-multiset", () -> Equality.ordered(te, ta, rules) == null);
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

    /** F4.3: RENDERED-TEXT comparison — the KEPT policies over the
     * platform's own text. {@code form}: CSVTEXT (header line + data
     * lines + trailing newline), TDSTEXT ('#TDS' frame), or
     * CSVJOIN:&lt;sep&gt; (the calendar family's one-line spelling).
     * Frame and header lines pin EXACTLY; data lines ordered or multiset
     * per the chain's sortedness; cells string-equal or bounded-float-
     * tolerant. */
    public static boolean renderedText(String expected, String actual,
            String form, boolean sorted) {
        boolean held = renderedTextVerdict(expected, actual, form, sorted);
        // R1b divergence instrument — measurement only, cannot affect
        // the verdict (CANONICAL_FORM_SPEC §0); the chain's sortedness
        // rides along so the byte channel judges unordered grids under
        // the same declared row-multiset policy as the verdict
        CanonicalDivergence.probeGridText(expected, actual, held, sorted,
                form);
        return held;
    }

    private static boolean renderedTextVerdict(String expected, String actual,
            String form, boolean sorted) {
        if (expected.equals(actual)) {
            return true;
        }
        if (form.startsWith("CSVJOIN:")) {
            String sep = form.substring("CSVJOIN:".length());
            String[] et = expected.split(java.util.regex.Pattern.quote(sep), -1);
            String[] at = actual.split(java.util.regex.Pattern.quote(sep), -1);
            if (et.length != at.length) {
                return false;
            }
            if (sorted) {
                for (int i = 0; i < et.length; i++) {
                    if (!cellEquals(et[i], at[i])) {
                        return false;
                    }
                }
                return true;
            }
            // token multiset (the sep-join destroyed row boundaries;
            // bounded by equal counts + per-cell policy)
            List<String> pool = new ArrayList<>(List.of(at));
            for (String e : et) {
                int hit = -1;
                for (int i = 0; i < pool.size(); i++) {
                    if (cellEquals(e, pool.get(i))) {
                        hit = i;
                        break;
                    }
                }
                if (hit < 0) {
                    return false;
                }
                pool.remove(hit);
            }
            return true;
        }
        String[] el = expected.split("\n", -1);
        String[] al = actual.split("\n", -1);
        if (el.length != al.length) {
            return false;
        }
        // pinned frame lines: CSVTEXT {first, trailing ''}; TDSTEXT
        // {'#TDS', header, trailing '#'}
        int dataFrom = form.equals("TDSTEXT") ? 2 : 1;
        int dataTo = el.length - 1;   // last line: '' (CSV) or '#' (TDS)
        for (int i = 0; i < el.length; i++) {
            boolean pinned = i < dataFrom || i >= dataTo;
            if (pinned && !el[i].equals(al[i])) {
                return false;
            }
        }
        if (sorted) {
            for (int i = dataFrom; i < dataTo; i++) {
                if (!lineEquals(el[i], al[i])) {
                    return false;
                }
            }
            return true;
        }
        List<String> pool = new ArrayList<>();
        for (int i = dataFrom; i < dataTo; i++) {
            pool.add(al[i]);
        }
        for (int i = dataFrom; i < dataTo; i++) {
            String e = el[i];
            int hit = -1;
            for (int j = 0; j < pool.size(); j++) {
                if (lineEquals(e, pool.get(j))) {
                    hit = j;
                    break;
                }
            }
            if (hit < 0) {
                return false;
            }
            pool.remove(hit);
        }
        // F2.4 ord-leniency instrument carries over to the text policy
        final String[] elf = el;
        final String[] alf = al;
        final int from = dataFrom;
        final int to = dataTo;
        ordLeniency("text-line-multiset", () -> {
            for (int i = from; i < to; i++) {
                if (!lineEquals(elf[i], alf[i])) {
                    return false;
                }
            }
            return true;
        });
        return true;
    }

    /** One text line: exact, else per-cell (comma-split both sides
     * IDENTICALLY — symmetric ambiguity for embedded commas). */
    private static boolean lineEquals(String e, String a) {
        if (e.equals(a)) {
            return true;
        }
        String[] ec = e.split(",", -1);
        String[] ac = a.split(",", -1);
        if (ec.length != ac.length) {
            return false;
        }
        for (int i = 0; i < ec.length; i++) {
            if (!cellEquals(ec[i], ac[i])) {
                return false;
            }
        }
        return true;
    }

    /** One cell — THE ONE float leniency (2026-09-20): two decimal-point
     * float prints within {@code 2 * ulp(max(|x|, |y|))} (the judges' own
     * rule — Equality, VerdictSql); the "golden's printed precision"
     * tolerance is DELETED (it hid H2's DECFLOAT arithmetic from the
     * host judge: the 21 calendar rows). Integers and scientific forms
     * compare as strings. Counted under {@code LL_TOL_COUNT}. */
    private static boolean cellEquals(String e, String a) {
        if (e.equals(a)) {
            return true;
        }
        if (e.indexOf('.') < 0 || a.indexOf('.') < 0) {
            return false;
        }
        try {
            boolean within = Equality.withinTwoUlp(Double.parseDouble(e), Double.parseDouble(a));
            if (within) {
                // the exact-comparison report (S15/S132 narrowed, 2026-11-05): the tolerance, not equality, decided
                com.legend.diagnostics.Diagnostics.judgeSend("ulp-tolerance", "host-cell " + e + " ~ " + a);
            }
            return within;
        } catch (NumberFormatException nfe) {
            return false;
        }
    }
}
