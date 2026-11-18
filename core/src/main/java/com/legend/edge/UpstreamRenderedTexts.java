// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.edge;

import java.util.ArrayList;
import java.util.List;

/**
 * THE COMPATIBILITY RULES' RENDERED-TEXT COMPARISON (registers S131, S132; census C1, the orchestrator's ruling of
 * 2026-11-17: moved from {@code testexec.TdsCompare}): where a rendered text and its golden differ, an unsorted
 * query's lines compared as a multiset and float tokens within 2 ULP -- the edge's form of the run hook's
 * {@link com.legend.testexec.RenderedTexts}; lite's own runs hold the two strings as the verdict.
 */
public enum UpstreamRenderedTexts implements com.legend.testexec.RenderedTexts {
    INSTANCE;

    /** F4.3: RENDERED-TEXT comparison — the KEPT policies over the
     * platform's own text. {@code form}: CSVTEXT (header line + data
     * lines + trailing newline), TDSTEXT ('#TDS' frame), or
     * CSVJOIN:&lt;sep&gt; (the calendar family's one-line spelling).
     * Frame and header lines pin EXACTLY; data lines ordered or multiset
     * per the chain's sortedness; cells string-equal or bounded-float-
     * tolerant. */
    @Override
    public boolean held(String expected, String actual, String form, boolean sorted) {
        boolean held = renderedTextVerdict(expected, actual, form, sorted);
        // R1b divergence instrument — measurement only, cannot affect
        // the verdict (CANONICAL_FORM_SPEC §0); the chain's sortedness
        // rides along so the byte channel judges unordered grids under
        // the same declared row-multiset policy as the verdict
        probeGridText(expected, actual, held, sorted, form);
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
        com.legend.testexec.TdsCompare.ordLeniency("text-line-multiset", () -> {
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
            boolean within = com.legend.edge.CompiledPureHostForms.withinUlps(Double.parseDouble(e), Double.parseDouble(a));
            if (within) {
                // the exact-comparison report (S15/S132 narrowed, 2026-11-05): the tolerance, not equality, decided
                com.legend.diagnostics.Diagnostics.judgeSend("ulp-tolerance", "host-cell " + e + " ~ " + a);
            }
            return within;
        } catch (NumberFormatException nfe) {
            return false;
        }
    }

    /** R1b — the GRID channel (toCSV text verdicts): the actual side is
     * ALREADY the platform's SQL-side render (Render = engine toCSV,
     * H3 headline), so the byte answer is plain string equality; the
     * census measures how often edge.UpstreamRenderedTexts's KEPT
     * leniencies (row multiset, bounded float tolerance) do work the
     * byte channel would refuse. */
    static void probeGridText(String expected, String actual,
            boolean held, boolean sorted, String form) {
        if (expected.equals(actual)) {
            com.legend.testexec.CanonicalDivergence.record("gridText", held, "EQUAL");
            return;
        }
        // The CSVJOIN family spells the whole grid on ONE line (rows
        // joined by the form's separator), which hid row-order drift
        // from the line-based classifier as cell-diff@line0. Normalize
        // rows to lines FIRST so one classifier judges every form —
        // applied to both sides identically, so the multiset check
        // below stays two-sided.
        if (form.startsWith("CSVJOIN:")) {
            String sep = form.substring("CSVJOIN:".length());
            expected = expected.replace(sep, "\n");
            actual = actual.replace(sep, "\n");
        }
        // name the first difference so the census classifies WHICH
        // leniency did the work (row order vs cell spelling)
        String[] el = expected.split("\\n", -1);
        String[] al = actual.split("\\n", -1);
        String why;
        if (el.length != al.length) {
            why = "line-count " + el.length + "!=" + al.length;
        } else {
            int i = 0;
            while (i < el.length && el[i].equals(al[i])) {
                i++;
            }
            java.util.List<String> es = new java.util.ArrayList<>(java.util.List.of(el));
            java.util.List<String> as = new java.util.ArrayList<>(java.util.List.of(al));
            java.util.Collections.sort(es);
            java.util.Collections.sort(as);
            why = es.equals(as)
                    ? "row-order-only@line" + i
                    : "cell-diff@line" + i + " e<" + trunc(el[i]) + "> a<"
                            + trunc(al[i]) + ">";
        }
        // Row order on an UNORDERED chain is undefined on BOTH backends
        // (no ORDER BY — the SQL spec fixes no arrival order, and the
        // engine golden's order is its own accident): the byte answer
        // is content equality under the same two-sided multiset policy
        // the host verdict already applies, gated on the SAME
        // compile-time sortedness fact the caller passes. A sorted
        // chain in the wrong order stays a REAL disagreement. Counted
        // never silent: the rescue is its own printed census (it is
        // arrival-order-dependent BY DEFINITION, so it is a diagnostic
        // count, never a pinnable one — pin disagree instead, which
        // this policy makes exact).
        if (!sorted && why.startsWith("row-order-only")) {
            com.legend.diagnostics.Census.inc(com.legend.diagnostics.Census.Key.ROW_ORDER_CANON);
            com.legend.testexec.CanonicalDivergence.record("gridText", held, "EQUAL");
            return;
        }
        com.legend.testexec.CanonicalDivergence.record("gridText", held, "DIFFER:" + why);
    }

    private static String trunc(String s) {
        return s.length() > 60 ? s.substring(0, 60) + "…" : s;
    }
}
