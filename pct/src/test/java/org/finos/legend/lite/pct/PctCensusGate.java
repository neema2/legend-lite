// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package org.finos.legend.lite.pct;

import com.legend.judge.SqlTypeCensus;
import junit.extensions.TestSetup;
import junit.framework.Test;

/**
 * M4 §3.4 — THE PCT CENSUS GATE: the G6/G7 JVMs run the same
 * {@link SqlTypeCensus} instruments as every other lane, but until
 * this hook nothing ASSERTED them there — the suites measured and
 * discarded. Each PCT suite's teardown now pins the lane.
 *
 * <p>Each suite is judged on ITS OWN traffic (Bazel workplan P3-09): the counters are process-wide and
 * cumulative, so the gate snapshots them at the suite's start and checks the difference at its end, against the
 * suite's own ceiling, measured on its own target. A suite's verdict no longer depends on which suites shared its JVM
 * (until 2026-10-05 the ceilings were whole-JVM, measured on the five-suite composite lane). Never-happens invariants
 * ({@code mismatch == 0} — a label lie escaped reconciliation) hold everywhere; the ceilings ratchet DOWN as families
 * burn. The wire census's five pins (adopt-pending, divergence and int-or-null-empty per lane and suite, wire-unknown,
 * null-breach) RETIRED 2026-10-29 (the doors' audit, D64): the arm that fed them lost its last caller when the plan
 * runner took over every read and was deleted; every one read zero by construction, a dead gate
 * ({@code DanglingStateGuardTest}). Their history to that day is in git at 619e0c12.
 */
public final class PctCensusGate {

    private PctCensusGate() {
    }

    // (The per-lane H2 split retired 2026-08-26: every pin below is
    // EQUALITY-0 on BOTH lanes — the lanes converged as the carriers
    // and stamps landed.)

    // MEASURED 2026-08-25 at the last teardown of each lane's JVM:
    // G6 full pct JVM on DuckDB (after Grammar, incl. ChannelB traffic)
    //   mismatch=0 untyped=813 adopt-pending=101 diverge=78
    // G7 h2 Relation JVM: mismatch=0 untyped=17 adopt-pending=0 diverge=0
    // untyped 813 -> 808 banked at the M4 re-land (typed claim roots);
    // the lane's one NEW class is wire-delivered LITERAL <- VARCHAR —
    // the registered carrier pair, adjudicated by design (F10).
    // 808 -> 728 (2026-08-25 rules burn: label-less ScalarSubquery
    // projection read + probed date_trunc/decimal-arithmetic/window
    // rules + empty-ArrayLit-is-Nil). MEASURED on the gate's OWN
    // composition (unfiltered `mvn clean test`, ONE cumulative JVM,
    // final teardown after Grammar = 728). A first ratchet to 336 was
    // a WRONG-DENOMINATOR measurement — a -Dtest-filtered run has a
    // different JVM composition; counters are cumulative per JVM, so
    // this pin only ever moves on the unfiltered gate command's
    // numbers (the G6 chain trip that caught it: Standard's teardown
    // alone reads 611).
    // 728 -> 273 (pct-tail burn: list_max/min element identity,
    // ADD_INTERVAL -> TIMESTAMP, bit-op widest-int, greatest/least
    // branch promotion) -> 120 (2026-08-25 FULL burn: XOR/REPEAT_STR/
    // TIMEZONE groups, list_sum/avg/median/mode via the reducer
    // promotions, list_product/append/reduce, the map family
    // (concat/from_entries/extract/keys/values), SPLIT/
    // REGEXP_EXTRACT_ALL -> VARCHAR[], FoldCall body-and-init rule,
    // decimal-mix union promotion in branchPromote — every rule
    // probed on the 1.5.0 reference jar). 120 -> 108: HASH -> BIGINT
    // (our OWN renderer reinterprets to signed BIGINT — the CEILING
    // rule-vs-emission mistake repeated; probe the emission, not the
    // bare builtin). 108 -> 20 (§4bZ-U EXECUTION, 2026-08-25 — the
    // five legs, each G4-witnessed): demand-driven pivot stamps (36
    // Column + 2 Reducer; the stamp speaks the Reducer's EMISSION
    // fact, not the pure contract — the first cut re-ran the CEILING
    // mistake and the wire census caught it); the RAISES fact (9
    // error() rows now counted raises=, never type debt); fetchDb
    // DECLARED JDBC-spec schemas; the binding-door sweep (fold
    // element+accumulator, Comparators element, minus-fold
    // LIST_REDUCE params, collection-map element door) + the
    // typedList conform-by-emission door (empty/NULL list positions
    // cast to their pure element's array: zip/joinStrings/fold init)
    // + declared struct-field slots (an absent optional property's
    // NULL contributes its layout type) + the REM decimal rule
    // (probed union shape). 20 -> 1 (§4bZ-U tree-receipt burn,
    // 2026-08-25 — every one of the 20 construction trees captured
    // and mechanism-fixed): property reads over stamped params
    // (foldResolver/mapElemResolver struct-field arms), the accIsList
    // fold rule (probed: the list-boxed lane delivers the acc's own
    // array), mixed-class concatenate to the VARIANT carrier (probed:
    // raw struct concat FIELD-UNIONS and smears class identity — one
    // value one carrier, the hetero-literal doctrine), the dedup
    // typedList door (empty removeDuplicates), and the
    // InstanceProjection elem stamp (the hardcoded-VARCHAR lateral).
    // 1 -> 0 (2026-08-26): the last row was testSimpleProject's EMPTY
    // `values` collection — an empty literal types Nil, so the
    // instance-projection lateral VARCHAR-guessed its element under a
    // StructGet('val'); the element type now comes from the colspec
    // BODY's own declared segment types
    // (InstanceProjection.pathTypesOf). ZERO on both lanes — HARDENED
    // TO EQUALITY: a new untyped root is a regression, witness in the
    // failure message. (ChannelB-context tags on pct witnesses are
    // unreliable — channel A never sets CONTEXT; capture TREES.)
    // (h2's 17 burned with the B3 temporal-text stamps — measured 0 on
    // the post-B3 G7; EQUALITY both lanes.)
    private static final long MAX_UNTYPED = 0;
    // THE NULLABILITY LEDGER (§4bZ-V E, 2026-08-26 — §4Z ledger #4):
    // this lane carried 6 literal-NullLit DOUBLE value-frames (the
    // corr/covarPopulation/covarSample PCT family); N1 made a
    // projected literal NULL declare its slot nullable at construction
    // (reconcileLabels), burning them with the corpus lane's 6,472
    // union pads. Residue adjudicated EMPTY — EQUALITY at zero on
    // both pct lanes: a row here is a COMPUTED bottom (a
    // NULL-propagating expression) under a required label.
    private static final long MAX_BOTTOM_MULT = 0;
    /** The counters a suite is judged on, in {@link #check}'s order: a snapshot, so a suite's own traffic is the
     *  difference between its start and its end (Bazel workplan P3-09: measured per suite, whatever shares the JVM). */
    private static long[] counts() {
        return new long[] {
            SqlTypeCensus.mismatchCount(),
            SqlTypeCensus.untypedCount(),
            SqlTypeCensus.bottomMultCount(),
        };
    }

    public static Test wrap(String suite, Test t) {
        return new TestSetup(t) {
            private long[] start = new long[3];

            @Override
            protected void setUp() {
                start = counts();
            }

            @Override
            protected void tearDown() {
                long[] end = counts();
                long[] d = new long[end.length];
                for (int i = 0; i < d.length; i++) {
                    d[i] = end[i] - start[i];
                }
                System.out.println("[pct-census] " + suite + " (its own traffic): mismatch=" + d[0]
                        + " untyped=" + d[1] + " bottom-mult=" + d[2]);
                System.out.println("[pct-census] after " + suite + ": "
                        + SqlTypeCensus.summary());
                SqlTypeCensus.classes(100).forEach(c -> System.out
                        .println("[pct-census] class: " + c));
                SqlTypeCensus.allSamples().forEach((cls, ws) ->
                        ws.forEach(w -> System.out.println(
                                "[pct-census] witness: " + cls + " :: "
                                        + w)));
                check(suite, "label lie escaped reconciliation (mismatch)", d[0], 0);
                check(suite, "untyped projection roots grew — a missing"
                                + " rule or an unstamped leaf", d[1], MAX_UNTYPED);
                check(suite, "computed NULL under a required-multiplicity"
                                + " label (bottom-mult)", d[2], MAX_BOTTOM_MULT);
            }
        };
    }

    private static void check(String suite, String what, long actual,
            long ceiling) {
        if (actual > ceiling) {
            throw new AssertionError("[pct-census] " + suite + ": " + what
                    + ": " + actual + " > " + ceiling + " — "
                    + SqlTypeCensus.summary());
        }
    }
}
