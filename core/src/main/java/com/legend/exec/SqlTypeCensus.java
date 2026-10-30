// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.exec;

import com.legend.sql.OutputCol;
import com.legend.sql.SqlExpr;
import com.legend.sql.SqlQuery;
import com.legend.sql.SqlSelect;
import com.legend.sql.SqlSource;
import com.legend.sql.SqlType;
import com.legend.sql.SqlTyping;
import com.legend.sql.SqlUnion;
import com.legend.sql.TypeFact;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.LongAdder;

/**
 * TYPED-IR Slice 1 — THE LABEL-LIE CENSUS (instrument &rarr; census
 * &rarr; flip): for every EXECUTED plan, compare each output column's
 * DECLARED label (stamp-derived today) against the type the
 * {@link SqlTyping} judgment computes from the expression ACTUALLY
 * built. Pure measurement beside the comparison layer (the
 * CanonicalDivergence pattern): probes consume a finished plan and
 * count; nothing here can produce a result or affect execution.
 *
 * <p>Buckets per column: AGREE (label == computed), SUBSUMED (the
 * named lossless subtyping relation), MISMATCH (both known,
 * different, matching NO named relation — the lie census, classified
 * by the declared/computed pair; §4bZ-V B4 deleted the old
 * admissible-carrier forgiveness, so this bucket is the only place a
 * differing pair can land), UNTYPED (the judgment has no rule yet —
 * the coverage census, classified by expression shape).
 *
 * <p>The WIRE census -- each result's own metadata against the labels ({@code probeWire}, the D1 watch, the slack
 * settle), its counters, their read surface and the pins on them -- was deleted 2026-10-29 (the doors' audit, D64):
 * callerless since the plan runner reads every result, its counters were dead gates ({@code DanglingStateGuardTest}:
 * readers, no writer). Its history is in git at 619e0c12.
 */
public final class SqlTypeCensus {

    private SqlTypeCensus() {
    }

    private static final LongAdder PLANS = new LongAdder();
    private static final LongAdder AGREE = new LongAdder();
    /** Lossless subtype-in-supertype slots ({@link SqlTyping#subsumes}
     * — §4bZ-V B2). (The admissible bucket is DELETED with the
     * relation, §4bZ-V B4: a pair matching no named relation lands in
     * {@link #MISMATCH}, which every lane pins at zero.) */
    private static final LongAdder SUBSUMED = new LongAdder();
    /** Engine-compat carry-through slots (§4bZ), SPLIT by provenance
     * (§4Z ledger #1 repin, 2026-08-26; refined same day — the pair
     * alone cannot tell a seam read from an aggregate over one):
     * ORIGIN = a bare COLUMN READ with a differing pair — the mapping
     * seam's own mismatch, one row per declared property/column kind
     * crossing (a NEW one is a model fact to justify); DERIVED = a
     * non-column expression with a differing pair — an operation
     * (SUM/MAX...) over a tagged read whose result keeps the pure
     * contract label (the wire-7 SUM-transport family, 111 -> 153's
     * +33); TRANSPORTED = the pair is EQUAL — an upper read merely
     * carrying the tag (plumbing, grows with query shape only). */
    private static final LongAdder BOTTOM_OK = new LongAdder();
    /** RAISING roots (§4bZ-U leg 3): a projection whose expression
     * {@code error()}s yields no value and conforms to its declared
     * slot — counted visibly, never type debt. */
    private static final LongAdder RAISES_OK = new LongAdder();
    private static final LongAdder BOTTOM_MULT = new LongAdder();
    private static final LongAdder MISMATCH = new LongAdder();
    private static final LongAdder UNTYPED = new LongAdder();


    /** declared-&gt;computed pair (mismatch) / expr shape (untyped)
     * &rarr; occurrence count. */
    private static final Map<String, LongAdder> CLASSES =
            new ConcurrentHashMap<>();
    /** First few WITNESSES per class — enough detail to locate the
     * emission seam (column name + expression sketch). Bounded. */
    private static final Map<String, List<String>> SAMPLES =
            new ConcurrentHashMap<>();
    private static final int SAMPLES_PER_CLASS = 3;

    /** The runners' per-test marker (set beside StampCensus.CONTEXT
     * by ChannelB and the corpus Runner — test-side wiring): witness
     * attribution without exec reaching into the middle-end
     * (Invariant 6d). */
    public static final ThreadLocal<String> CONTEXT = new ThreadLocal<>();

    // ------------------------------------------------------------------
    // §E3 M-N1 — THE NULLABILITY DIFFERENTIAL (fact vs label): for
    // every labeled projection with a Typed fact, compare the fact's
    // §E3 nullable (construction-computed, echo-derived leaves) against
    // the label's OutputCol.nullable (the pure-multiplicity echo).
    // Census-first: this is the M-N3 flip's payload — UNDER-DECLARED
    // rows (fact may-null, label promises present) are the adoption
    // set the 925-breach ceiling burns by; OVER-DECLARED rows (label
    // nullable, fact PROVES never-null) tighten at the flip and must
    // be adjudicated against the breach ledger first. Bottom facts are
    // owned by the existing bottom-ok/bottom-mult ledgers; separate
    // maps keep every pre-§E3 census print byte-stable.
    // ------------------------------------------------------------------
    private static final LongAdder NUL_AGREE_REQUIRED = new LongAdder();
    private static final LongAdder NUL_AGREE_NULLABLE = new LongAdder();
    private static final LongAdder NUL_UNDER_DECLARED = new LongAdder();
    private static final LongAdder NUL_OVER_DECLARED = new LongAdder();
    private static final Map<String, LongAdder> NUL_CLASSES =
            new ConcurrentHashMap<>();
    private static final Map<String, List<String>> NUL_SAMPLES =
            new ConcurrentHashMap<>();

    private static void nulDifferential(OutputCol declared, SqlExpr e,
            boolean grouped) {
        // §E3 M-N2: the compared side is the SLOT truth — the node
        // fact refined by the select's non-empty-group proof
        // (SqlTyping.slotNullable, the M-N3 adoption's one owner)
        boolean fact = SqlTyping.slotNullable(e, grouped);
        boolean label = declared.nullable();
        if (fact == label) {
            (fact ? NUL_AGREE_NULLABLE : NUL_AGREE_REQUIRED).increment();
            return;
        }
        String cls = (fact ? "nul-under-declared[" : "nul-over-declared[")
                + shapeOf(e) + "] " + declared.type();
        (fact ? NUL_UNDER_DECLARED : NUL_OVER_DECLARED).increment();
        NUL_CLASSES.computeIfAbsent(cls, k -> new LongAdder()).increment();
        List<String> ws = NUL_SAMPLES.computeIfAbsent(cls,
                k -> java.util.Collections.synchronizedList(
                        new ArrayList<>()));
        if (ws.size() < SAMPLES_PER_CLASS) {
            String ctx = CONTEXT.get();
            ws.add(declared.name() + " := " + sketch(e)
                    + (ctx == null ? "" : " [" + ctx + "]"));
        }
    }

    /** §E3 M-N3 pins: post-flip both counts are construction
     * invariants (reconciled labels ARE the slot truth). */
    public static long nullableUnderDeclaredCount() {
        return NUL_UNDER_DECLARED.sum();
    }

    public static long nullableOverDeclaredCount() {
        return NUL_OVER_DECLARED.sum();
    }

    public static String nullableDifferentialSummary() {
        return "agree-required=" + NUL_AGREE_REQUIRED.sum()
                + " agree-nullable=" + NUL_AGREE_NULLABLE.sum()
                + " under-declared=" + NUL_UNDER_DECLARED.sum()
                + " over-declared=" + NUL_OVER_DECLARED.sum();
    }

    /** The full differential report (summary + every class with its
     * witnesses) — the runner's target/ dump payload. */
    public static List<String> nullableDifferentialReport() {
        List<String> out = new ArrayList<>();
        out.add(nullableDifferentialSummary());
        NUL_CLASSES.entrySet().stream()
                .sorted((a, b) -> Long.compare(b.getValue().sum(),
                        a.getValue().sum()))
                .forEach(en -> {
                    out.add(en.getValue().sum() + "x " + en.getKey());
                    NUL_SAMPLES.getOrDefault(en.getKey(), List.of())
                            .forEach(w -> out.add("  " + w));
                });
        return out;
    }

    /** V7 probe isolation (V7_ASSERT_VERDICT_CHARTER §4.1): the corpus
     * dual channel RE-EXECUTES assert statements through the production
     * path as a referee probe; those duplicate executions must not
     * double-feed this census — its sweep ceilings pin the PRIMARY
     * lane's counts, and a probe-inflated count reads as compiler
     * drift. Toggled by the single-threaded harness around the probe
     * only; every other lane sees the flag permanently false. */
    private static final java.util.concurrent.atomic.AtomicBoolean
            PROBE_SUSPENDED = new java.util.concurrent.atomic.AtomicBoolean();

    public static void probeSuspend(boolean on) {
        PROBE_SUSPENDED.set(on);
    }

    /** Current suspension state — nested referee executions (the
     * sql-text arm's OUR-ROWS derivation under a dual-channel probe)
     * save-and-restore instead of blind-clearing an outer probe's
     * suspension. */
    public static boolean probeSuspended() {
        return PROBE_SUSPENDED.get();
    }

    public static void probe(SqlQuery plan) {
        if (PROBE_SUSPENDED.get()) {
            return;
        }
        PLANS.increment();
        walk(plan);
    }

    private static void walk(SqlQuery q) {
        if (q instanceof SqlUnion u) {
            u.branches().forEach(SqlTypeCensus::walk);
            return;
        }
        if (q instanceof com.legend.sql.SqlWith w) {
            w.ctes().forEach(c -> walk(c.query()));
            walk(w.body());
            return;
        }
        SqlSelect s = (SqlSelect) q;
        List<SqlSource> sources = new ArrayList<>();
        collect(s.from(), sources);
        for (SqlSource src : sources) {
            if (src instanceof SqlSource.Subselect sub) {
                walk(sub.inner());
            }
        }
        if (s.projections().size() != s.outputs().size()) {
            return;   // star selects and shape-mismatched frames: no
                      // per-column claim to check
        }
        for (int i = 0; i < s.projections().size(); i++) {
            SqlExpr e = s.projections().get(i).expr();
            if (e instanceof SqlExpr.Star
                    || e instanceof SqlExpr.StarExcept) {
                continue;
            }
            OutputCol declared = s.outputs().get(i);
            // the TREE is the one type channel (the judge deleted
            // 2026-08-24 — its parity with the stored types was pinned
            // at zero divergence on every lane before deletion)
            TypeFact v = e.type();
            switch (v) {
                case TypeFact.Bottom b -> {
                    // §E3 M-N3/M-N4 (supersedes the N1/N2 framing):
                    // labels adopt slot truth at construction and a
                    // Bottom slot IS nullable, so EVERY Bottom row —
                    // literal pad or computed — lands in bottom-ok BY
                    // CONSTRUCTION; the bottom-mult bucket is
                    // structurally empty (its EQUALITY-0 pin is now a
                    // reconciliation-bypass tripwire, same class as
                    // the differential pin).
                    if (declared.nullable()) {
                        BOTTOM_OK.increment();
                    } else {
                        String cls = "null-under-required-multiplicity["
                                + bottomShape(e) + "]: " + declared.type();
                        BOTTOM_MULT.increment();
                        classify(cls);
                        sample(cls, declared.name() + " := " + sketch(e));
                    }
                }
                case TypeFact.Raises r -> RAISES_OK.increment();
                case TypeFact.Unknown u -> {
                    UNTYPED.increment();
                    String cls = "untyped: " + shapeOf(e);
                    classify(cls);
                    sample(cls, declared.name() + " := " + sketch(e));
                }
                case TypeFact.Typed t -> {
                    nulDifferential(declared, e, !s.groupBy().isEmpty());
                    if (t.type().equals(declared.type())) {
                        AGREE.increment();
                    } else if (SqlTyping.subsumes(declared.type(),
                            t.type())) {
                        String cls = "subsumed " + declared.type()
                                + " <- " + t.type();
                        SUBSUMED.increment();
                        classify(cls);
                    } else {
                        String cls = "declared " + declared.type()
                                + " <> computed " + t.type();
                        MISMATCH.increment();
                        classify(cls);
                        sample(cls, declared.name() + " := " + sketch(e));
                    }
                }
            }
        }
    }

    private static void collect(SqlSource src, List<SqlSource> out) {
        if (src instanceof SqlSource.Join j) {
            collect(j.left(), out);
            collect(j.right(), out);
        } else {
            out.add(src);
        }
    }

    private static String shapeOf(SqlExpr e) {
        return e instanceof SqlExpr.Call c ? "Call:" + c.fn()
                : e.getClass().getSimpleName();
    }

    /** N0 (§4bZ-V E): the bottom-mult class key carries the expression
     * SHAPE so the backlog decomposes BY CAUSE machine-counted —
     * literal NullLit pads apart from NULL-propagating computations
     * (the pad conclusion rested on 12 sampled witnesses before this
     * key). Cast is transparent: a pad cast to its declared kind is
     * still a pad. */
    private static String bottomShape(SqlExpr e) {
        return e instanceof SqlExpr.Cast c
                ? "Cast(" + bottomShape(c.value()) + ")"
                : shapeOf(e);
    }

    /** Same-package instruments (Executor's carrier-migration census)
     * count through the one classifier — a named class per probe. */
    static void classifyExternal(String cls) {
        classify(cls);
    }

    private static void classify(String cls) {
        CLASSES.computeIfAbsent(cls, k -> new LongAdder()).increment();
    }

    private static void sample(String cls, String witness) {
        List<String> ws = SAMPLES.computeIfAbsent(cls,
                k -> java.util.Collections.synchronizedList(
                        new ArrayList<>()));
        if (ws.size() < SAMPLES_PER_CLASS) {
            String ctx = CONTEXT.get();
            ws.add(witness + (ctx == null ? "" : " [" + ctx + "]"));
        }
    }

    /** A one-line expression sketch — enough to grep the emission seam,
     * never a full plan dump. */
    private static String sketch(SqlExpr e) {
        return switch (e) {
            case SqlExpr.Call c -> c.fn() + "(" + c.args().stream()
                    .map(SqlTypeCensus::sketchLeaf)
                    .collect(java.util.stream.Collectors.joining(","))
                    // the blind ARGUMENT, down to its leaf (the locator)
                    + (c.args().stream().anyMatch(x -> x.type() instanceof TypeFact.Unknown)
                            ? " " + memberAnatomy(c.args()) : "")
                    + ")";
            case SqlExpr.Cast c -> "CAST(" + sketchLeaf(c.value()) + " AS "
                    + c.target() + ")";
            default -> sketchLeaf(e);
        };
    }

    private static String sketchLeaf(SqlExpr e) {
        return switch (e) {
            case SqlExpr.Column c ->
                    (c.table() == null ? "" : c.table() + ".") + c.name();
            case SqlExpr.StringLit sl -> "'…'";
            case SqlExpr.IntLit il -> String.valueOf(il.value());
            case SqlExpr.Call c -> c.fn() + "(…)";
            // family locators: WHY the rule declined — the first
            // UNTYPED member's shape, so witnesses name the blind leaf
            case SqlExpr.Case c -> {
                List<SqlExpr> ms = new ArrayList<>();
                c.whens().forEach(w -> ms.add(w.then()));
                if (c.otherwise() != null) {
                    ms.add(c.otherwise());
                }
                yield "Case(" + memberAnatomy(ms) + ")";
            }
            case SqlExpr.ArrayLit al ->
                    "ArrayLit(" + memberAnatomy(al.elements()) + ")";
            // a struct names its first BLIND field, down to the leaf
            case SqlExpr.StructLit sl -> {
                for (SqlExpr.StructLit.Field f : sl.fields()) {
                    if (f.value().type() instanceof TypeFact.Unknown) {
                        yield "StructLit(" + f.name() + " := " + sketchLeaf(f.value()) + ")";
                    }
                }
                yield "StructLit(all fields typed)";
            }
            case SqlExpr.CompactList cl ->
                    "CompactList(" + sketch(cl.list()) + ")";
            case com.legend.sql.SqlAgg.Reducer r ->
                    r.fn() + "[" + (r.args().isEmpty() ? ""
                            : sketchLeaf(r.args().get(0))) + "]";
            // the untyped-ScalarSubquery family locator: WHY the rule
            // declined (outputs count + first label + inner root shape)
            case SqlExpr.ScalarSubquery s -> {
                List<OutputCol> os = s.subquery().outputs();
                String inner = s.subquery() instanceof SqlSelect is
                        && !is.projections().isEmpty()
                        ? sketchLeaf(is.projections().get(0).expr()) : "?";
                yield "ScalarSubquery(outs="
                        + (os == null ? "null" : os.size())
                        + (os == null || os.isEmpty() ? ""
                                : " " + os.get(0).type())
                        + " inner=" + inner + ")";
            }
            default -> e.getClass().getSimpleName();
        };
    }

    /** Member anatomy for branch/element families: the first untyped
     * member's shape (the blind leaf; RAISING members carry their own
     * fact and are never blind), or — every member typed — the
     * distinct member types (a promote failure). */
    private static String memberAnatomy(List<SqlExpr> ms) {
        for (SqlExpr m : ms) {
            if (m.type() instanceof TypeFact.Unknown) {
                return "blind=" + sketch(m);
            }
        }
        java.util.Set<String> kinds = new java.util.LinkedHashSet<>();
        for (SqlExpr m : ms) {
            kinds.add(m.type() instanceof TypeFact.Typed t
                    ? String.valueOf(t.type())
                    : m.type().getClass().getSimpleName());
        }
        return "types=" + kinds;
    }

    /** Bounded witnesses for a class (attribution — the emission-seam
     * locator). */
    public static List<String> samplesOf(String cls) {
        return List.copyOf(SAMPLES.getOrDefault(cls, List.of()));
    }

    /** Every sampled class with its witnesses. */
    public static Map<String, List<String>> allSamples() {
        Map<String, List<String>> out = new java.util.TreeMap<>();
        SAMPLES.forEach((k, v) -> out.put(k, List.copyOf(v)));
        return out;
    }

    public static long mismatchCount() {
        return MISMATCH.sum();
    }

    /** Computed-NULL values under a required-multiplicity label — the
     * nullability program's ledger (§4bZ-V E). Pinned per lane once
     * the pad slots declare slot truth (N1/N2). */
    public static long bottomMultCount() {
        return BOTTOM_MULT.sum();
    }

    /** Projection roots the tree cannot type — RULE coverage debt
     * (and, post-judge, the leaf-regression tripwire: a new unstamped
     * construction site GROWS this). Ceiling-pinned in the corpus
     * runner; ratchets down as rules land. */
    public static long untypedCount() {
        return UNTYPED.sum();
    }

    public static String summary() {
        return "plans=" + PLANS.sum() + " cols: agree=" + AGREE.sum()
                + " subsumed=" + SUBSUMED.sum()
                + " bottom-ok=" + BOTTOM_OK.sum()
                + " raises=" + RAISES_OK.sum()
                + " bottom-mult-backlog=" + BOTTOM_MULT.sum()
                + " mismatch=" + MISMATCH.sum() + " untyped=" + UNTYPED.sum();
    }

    /** The classified census, largest classes first. */
    public static List<String> classes(int top) {
        return CLASSES.entrySet().stream()
                .sorted((a, b) -> Long.compare(b.getValue().sum(),
                        a.getValue().sum()))
                .limit(top)
                .map(en -> en.getValue().sum() + "x " + en.getKey())
                .toList();
    }
}
