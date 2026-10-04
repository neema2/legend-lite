// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package org.finos.legend.lite.pct;

import com.legend.exec.SqlTypeCensus;
import junit.extensions.TestSetup;
import junit.framework.Test;

/**
 * M4 §3.4 — THE PCT CENSUS GATE: the G6/G7 JVMs run the same
 * {@link SqlTypeCensus} instruments as every other lane, but until
 * this hook nothing ASSERTED them there — the suites measured and
 * discarded. Each PCT suite's teardown now pins the lane.
 *
 * <p>Counters are CUMULATIVE PER JVM (the trap roster: measure lanes
 * whole, never per-suite), and one JVM (the lane's junit_test) runs every pct test
 * class in file order — so per-suite deltas are meaningless and only
 * ORDER-SAFE facts are asserted at each teardown: never-happens
 * invariants ({@code mismatch == 0} — a label lie escaped
 * reconciliation) and whole-JVM ceilings (a teardown observes a prefix
 * of the JVM's traffic, so a prefix exceeding the JVM ceiling is
 * already a regression). Ceilings are MEASURED per lane (2026-08-25)
 * and ratchet DOWN as families burn; the h2 lane (G7,
 * {@code LEGENDLITE_PCT_BACKEND=h2}) is its own JVM with its own
 * numbers.
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
    // §4bZ-V C ADJUDICATED (2026-08-26, the wire-tree capture method):
    // diverge 78 -> 45 -> 0 and adopt-pending 101 -> 64 -> 0, BOTH
    // HARDENED TO EQUALITY. The kills, each probed on 1.5.0:
    // star-tail label reconciliation (EVERY remaining row lived in a
    // star-bearing frame the old size gate skipped wholesale — labels
    // now adopt computed types through one leading star + k computed
    // tail); Float-declared TDS cells seed DOUBLE literals (the
    // DOUBLE<>DECIMAL(p,s) head-column family — DecimalLit seeds made
    // DuckDB type whole Values columns DECIMAL); scale-0 DecimalLits
    // beyond long are big pure INTEGERS and type HUGEINT, within long
    // they are d-suffixed pure DECIMALS and the renderer CASTS so the
    // wire reads DECIMAL (bare digits read INTEGER; typing the FACT by
    // magnitude instead flipped percentile's carrier dispatch — facts
    // follow the contract, emissions follow the fact); >38-digit
    // fractional literals read DOUBLE (DECIMAL's precision cap);
    // GUID() casts to VARCHAR (pure String contract; bare uuid() wires
    // UUID); repeatString VARCHAR-casts an untyped arg (DuckDB's
    // binder picked the BLOB overload for bare NULL); descending
    // continuous percentile is the NEGATION identity -(qc(-v, p)) —
    // the (1-p) transform diverged in float ULPs from the engine's
    // WITHIN GROUP DESC path (testPercentile_Relation_Window's
    // byte-compare refereed).
    // THE POSTGRES LANE (gate 7P, leg P2) is its own JVM with its own numbers, as G7's were: MEASURED
    // 2026-10-02 on Postgres 16.15, the five suites in one JVM, final teardown: diverge=53, all a
    // computed Postgres numeric the label names otherwise -- DECIMAL(p,s) where Postgres keeps no
    // precision on a computed numeric (34), DOUBLE where avg or a division delivers numeric (11), HUGEINT
    // where sum(bigint) is numeric (8) -- and the Variant carrier delivered as text (STRUCT <- VARCHAR, 3:
    // leg P4). Its driver's own spellings (int8, text, bool ...) are names, not divergence
    // (SqlTypeCensus.normalizeMeta). Every other pin holds at its DuckDB/H2 value on this lane too.
    private static final boolean POSTGRES = "postgres".equals(System.getenv("LEGENDLITE_PCT_BACKEND"));
    // 53 -> 57 (2026-10-02, tier 1: multi-column pivots, whole-partition medians, half-even rounding to a
    // scale and calendar buckets now RUN on Postgres): the same classes, more of their plans -- a decimal
    // rounded exactly is a computed numeric with no declared precision
    // 57 -> 63 (2026-10-02, lists of scalars as native arrays: 83 more tests' plans now RUN on Postgres --
    // the same classes)
    // 63 -> 76 (2026-10-02, structs and non-scalar lists as jsonb): one new class -- a struct delivered as
    // jsonb (label STRUCT, wire JSON), Postgres's representation by design -- and more plans of the others
    // 76 -> 16 (2026-10-02, Postgres DELIVERS the platform's numeric types, as H2 does): the moments and
    // sqrt/exp/ln/log10 compute in double precision; the root casts its DECIMAL(p,s) and HUGEINT columns
    // (RootNumericTypes), whose precision the census now reads where the driver's type name has none; a
    // literal sum's fold keeps its HUGEINT. Left: a struct, jsonb on Postgres by design (STRUCT <- JSON,
    // 13), or text (STRUCT <- VARCHAR, 3)
    private static final long MAX_WIRE_DIVERGE = POSTGRES ? 16 : 0;
    private static final long MAX_ADOPT_PENDING = 0;
    // THE NULLABILITY LEDGER (§4bZ-V E, 2026-08-26 — §4Z ledger #4):
    // this lane carried 6 literal-NullLit DOUBLE value-frames (the
    // corr/covarPopulation/covarSample PCT family); N1 made a
    // projected literal NULL declare its slot nullable at construction
    // (reconcileLabels), burning them with the corpus lane's 6,472
    // union pads. Residue adjudicated EMPTY — EQUALITY at zero on
    // both pct lanes: a row here is a COMPUTED bottom (a
    // NULL-propagating expression) under a required label.
    private static final long MAX_BOTTOM_MULT = 0;
    // §4bZ-V D (2026-08-26): the wire probe now ADJUDICATES every
    // column. Unknown = a probe that could not judge (zero-output and
    // pivot frames are no-claim by doctrine; the old 8 were all pivot
    // tests) — EQUALITY at zero. Int-or-null settles on VALUE
    // evidence: all 219 lane rows PROVED all-NULL (empty-result PCT
    // fixtures — greatest/least_Empty et al.); a valued column lands
    // in diverge (EQUALITY-0) instead. Ceiling, shape-driven.
    private static final long MAX_WIRE_UNKNOWN = 0;
    // 219→226 (2026-08-27 Channel B leg-4 batch): mangled function
    // ids now RESOLVE as value references, so the two previously
    // ERRORING tests (testContainsWithFunction,
    // testRemoveDuplicates...Explicit — ResolutionException, NO plan)
    // compile real plans whose empty-fixture columns join this bucket
    // — compile-COVERAGE growth, not a typed-column degradation
    // (cumulative per-JVM counter: 223 Essential / 226 by
    // Unclassified).
    // 226→231 (2026-09-17, NUMERIC CHARTER Rule 2a): a Number-declared
    // native over Float operands is now Float-typed by the typer
    // (NumberKinds.refine — max([1, 2.5, …]) et al.), so its EMPTY-fixture
    // columns (all-NULL, value-proven) carry the DOUBLE label and join this
    // bucket instead of the unrefined Number's — a label move on empty
    // results, not a typed-column degradation (cumulative per JVM:
    // 230 Unclassified / 231 Grammar).
    private static final long MAX_INT_NULL_EMPTY = 231;
    // E2E-audit converse census (TYPE_E2E_AUDIT §3): wire NULL under
    // an always-present label — 49 on this lane (46 HUGEINT
    // empty-group sums + 3 DOUBLE float aggregates). Ceiling;
    // burns at the nullability-inference leg.
    // §E3 M-N3 (2026-08-27): labels adopt slot-truth nullability at
    // construction — a wire NULL under a never-null label is a
    // compiler bug. 49 (46 HUGEINT empty-group sums + 3 DOUBLE,
    // E2E-audit measure) -> 0 at the flip.
    private static final long MAX_NULL_BREACH = 0;
    // §4bZ-V B3+B4 (2026-08-26): the admissible bucket is DELETED with
    // the relation itself — the temporal-text traffic is the
    // TEMPORAL_TEXT carrier, the JSON egress conforms by emission, and
    // a pair matching no named relation now lands in MISMATCH (pinned
    // 0 below) — strictly louder than any ceiling here could be.

    public static Test wrap(String suite, Test t) {
        return new TestSetup(t) {
            @Override
            protected void tearDown() {
                System.out.println("[pct-census] after " + suite + ": "
                        + SqlTypeCensus.summary());
                // the untyped DECOMPOSITION (the corpus lane's census
                // display, brought to the pct lane 2026-08-25 — no
                // silent caps: the ceiling is only adjudicable when
                // every class is visible with witnesses; 40 -> 100 at
                // N0's bottom-mult SHAPE split, §4bZ-V E)
                SqlTypeCensus.classes(100).forEach(c -> System.out
                        .println("[pct-census] class: " + c));
                SqlTypeCensus.allSamples().forEach((cls, ws) ->
                        ws.forEach(w -> System.out.println(
                                "[pct-census] witness: " + cls + " :: "
                                        + w)));
                check(suite, "label lie escaped reconciliation (mismatch)",
                        SqlTypeCensus.mismatchCount(), 0);
                check(suite, "wire adopt-pending grew",
                        SqlTypeCensus.wireAdoptPendingCount(),
                        MAX_ADOPT_PENDING);
                check(suite, "wire divergence grew",
                        SqlTypeCensus.wireDivergeCount(), MAX_WIRE_DIVERGE);
                check(suite, "untyped projection roots grew — a missing"
                                + " rule or an unstamped leaf",
                        SqlTypeCensus.untypedCount(), MAX_UNTYPED);
                check(suite, "computed NULL under a required-multiplicity"
                                + " label (bottom-mult)",
                        SqlTypeCensus.bottomMultCount(), MAX_BOTTOM_MULT);
                check(suite, "unadjudicated wire probes appeared",
                        SqlTypeCensus.wireUnknownCount(), MAX_WIRE_UNKNOWN);
                check(suite, "proven-empty int-or-null columns grew",
                        SqlTypeCensus.wireIntOrNullEmptyCount(),
                        MAX_INT_NULL_EMPTY);
                check(suite, "null-under-required-label breaches grew",
                        SqlTypeCensus.nullBreachCount(), MAX_NULL_BREACH);
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
