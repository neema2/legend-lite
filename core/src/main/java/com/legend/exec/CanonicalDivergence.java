// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.exec;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicLong;

/**
 * R1's divergence instrument (docs/CANONICAL_FORM_SPEC.md §0): for
 * every assert-family verdict the K-arm computes, ALSO decide the
 * byte-channel answer — {@code render(e) == render(a)} over the R0
 * canonical form — and census the agreement. The harness publishes the
 * table at end of run. PURE MEASUREMENT: nothing here can affect a
 * verdict (the probe returns void), and the classes stay out of every
 * production path by construction — the TimingLedger idiom.
 *
 * <p>Row classes: AGREE (byte answer == lattice answer — the ⟺ claim
 * holds on this operand pair), DISAGREE (the claim fails — an R0 spec
 * gap, a render bug, or a policy row like 2-ULP tolerance doing work),
 * RESIDUE (an operand outside the byte channel's claimed domain — §4).
 * DISAGREE rows are the R2 blockers; RESIDUE rows size the walls.
 */
public final class CanonicalDivergence {

    private CanonicalDivergence() {
    }

    /** One disagreement/residue witness (bounded sample). */
    public record Row(String family, boolean held, String detail) {
    }

    // R2a — the DUAL-VERDICT census (the ratified permanent referee):
    // SQL byte verdict vs host lattice, plus counted declines
    private static final int SAMPLE_CAP = 200;
    private static final ConcurrentLinkedQueue<Row> SAMPLES =
            new ConcurrentLinkedQueue<>();

    /** Census an equal-family verdict ({@code assertEquals}/{@code
     * assertEq}): {@code held} is the lattice answer already computed by
     * the K-arm; the byte answer is the KIND-QUALIFIED canonical-render
     * compare — (kindClass, text) pairs, because the render is not
     * injective across kinds (String "8" and Integer 8 both spell "8";
     * CANONICAL_FORM_SPEC §3 amendment). The numeric tower shares one
     * kind class (pure's cross-kind numeric equality). */
    public static void probeEqual(String family, List<Object> e,
            List<Object> a, boolean held) {
        probeEqual(family, e, a, held, false);
    }

    /** {@code unordered}: the caller's COMPILE-TIME order view of the
     * fetch (OrderView.INCIDENTAL — no ORDER BY, arrival order
     * undefined on both backends). The byte channel then compares
     * sorted renders on BOTH sides — the same declared multiset policy
     * the host verdict applies under the same fact (§8.3c: five
     * value-list rows read as VALUE divergences until the payload
     * showed positional drift the host had already lawfully
     * disregarded). */
    public static void probeEqual(String family, List<Object> e,
            List<Object> a, boolean held, boolean unordered) {
        if (MUTED.get()) {
            return;
        }
        record(family, held, byteEqual(e, a, unordered));
    }

    /** Census an {@code assertSameElements} verdict: the byte channel's
     * multiset rule is canonical-render each element, SORT the rendered
     * strings, compare — the census-side stand-in for R2's canonical
     * ORDER BY. */
    public static void probeSameElements(List<Object> e, List<Object> a,
            boolean held) {
        if (MUTED.get()) {
            return;
        }
        record("assertSameElements", held, byteEqual(e, a, true));
    }

    /** R1b — the GRID channel (toCSV text verdicts): the actual side is
     * ALREADY the platform's SQL-side render (Render = engine toCSV,
     * H3 headline), so the byte answer is plain string equality; the
     * census measures how often TdsCompare.renderedText's KEPT
     * leniencies (row multiset, bounded float tolerance) do work the
     * byte channel would refuse. */
    public static void probeGridText(String expected, String actual,
            boolean held, boolean sorted, String form) {
        if (MUTED.get()) {
            return;
        }
        if (expected.equals(actual)) {
            record("gridText", held, "EQUAL");
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
            java.util.List<String> es = new java.util.ArrayList<>(List.of(el));
            java.util.List<String> as = new java.util.ArrayList<>(List.of(al));
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
            Census.inc(Census.Key.ROW_ORDER_CANON);
            record("gridText", held, "EQUAL");
            return;
        }
        record("gridText", held, "DIFFER:" + why);
    }

    /** Unordered-chain grid compares whose content matched only under
     * the declared row-multiset policy (see probeGridText). */
    private static String trunc(String s) {
        return s.length() > 60 ? s.substring(0, 60) + "…" : s;
    }

    /** Byte-channel answer encoded as a STRING — {@code "EQUAL"},
     * {@code "DIFFER"}, or {@code "residue:<reason>"} naming what fell
     * out of the claimed domain (the census's wall-sizing detail). */
    private static String byteEqual(List<Object> e, List<Object> a,
            boolean sorted) {
        if (e.size() != a.size()) {
            return "DIFFER";
        }
        List<String> er = new ArrayList<>(e.size());
        List<String> ar = new ArrayList<>(a.size());
        for (int i = 0; i < e.size(); i++) {
            String left = keyOf(e.get(i));
            String right = keyOf(a.get(i));
            if (left.startsWith("residue:")) {
                return left;
            }
            if (right.startsWith("residue:")) {
                return right;
            }
            er.add(left);
            ar.add(right);
        }
        if (sorted) {
            er.sort(String::compareTo);
            ar.sort(String::compareTo);
        }
        // the census diagnoses itself: a bare DIFFER cost a full
        // filtered-rerun per row during the §8.3b wobble hunt
        return er.equals(ar) ? "EQUAL"
                : "DIFFER" + TdsCompare.firstCanonDiff(er, ar);
    }

    /** The byte-channel comparison key: kindClass + canonical text
     * (spec §3 amendment — the render alone is not injective across
     * kinds), or a residue marker. */
    private static String keyOf(@com.legend.Nullable Object v) {
        return switch (CanonicalForm.render(v)) {
            case CanonicalForm.Result.Text t -> kindClass(v) + "\u0000" + t.value();
            case CanonicalForm.Result.Residue r -> "residue:" + r.reason();
        };
    }

    /** Pure's equality kind classes: the numeric tower is ONE class
     * (cross-kind numeric equality); everything else compares only
     * within its own kind. */
    private static String kindClass(@com.legend.Nullable Object v) {
        return switch (v) {
            case null -> "null";
            case Number n -> "numeric";
            case Boolean b -> "boolean";
            case String s -> "string";
            case com.legend.values.PureDateLiteral d -> "temporal";
            // unreachable: keyOf calls this only for values render()
            // accepted, and render's default is Residue — throwing keeps
            // the no-plausible-bucket rule (Charter C2.4)
            default -> throw new IllegalStateException(
                    "kindClass over unrendered kind: " + v.getClass());
        };
    }

    /** V7 probe isolation for the R1 instrument family (probeEqual/
     * probeSameElements/probeGridText → AGREE/DISAGREE/RESIDUE): the
     * corpus dual channel re-runs the same asserts through the
     * production path, and un-gated probes would double-count the
     * HOST lane's pinned leniency census (disagree ≤ 27). The
     * sql-verdict channel and the declared-policy counters stay LIVE
     * during the probe — they are the probe's own instruments. */
    private static final java.util.concurrent.atomic.AtomicBoolean
            R1_SUSPENDED = new java.util.concurrent.atomic.AtomicBoolean();

    /** FULL mute (harness-deletion flip probe): a DIAGNOSTIC duplicate
     * execution is not a census fact for ANY channel here — unlike the
     * V7 dual-channel probe (whose sql-verdict rows are its own
     * instrument, see r1Suspend), the whole-test flip probe re-runs
     * entire bodies and must leave every pinned counter untouched. */
    private static final java.util.concurrent.atomic.AtomicBoolean
            MUTED = new java.util.concurrent.atomic.AtomicBoolean();

    public static void muteAll(boolean on) {
        MUTED.set(on);
    }


    public static void r1Suspend(boolean on) {
        R1_SUSPENDED.set(on);
    }

    private static void record(String family, boolean held, String byteAns) {
        if (R1_SUSPENDED.get()) {
            return;
        }
        if (byteAns.startsWith("residue:")) {
            Census.inc(Census.Key.DIVERGENCE_RESIDUE);
            sample(new Row(family, held, byteAns));
        } else if (byteAns.equals("EQUAL") == held) {
            Census.inc(Census.Key.DIVERGENCE_AGREE);
        } else {
            Census.inc(Census.Key.DIVERGENCE_DISAGREE);
            DISAGREE_SAMPLES.add(new Row(family, held, "lattice=" + held
                    + " byte=" + byteAns.replaceFirst("^DIFFER", "false")
                    + " [" + CONTEXT_SOURCE.get() + "]"));
        }
    }

    private static void sample(Row r) {
        if (SAMPLES.size() < SAMPLE_CAP) {
            SAMPLES.add(r);
        }
    }

    /** R2a: one dual-verdict row — the DB byte verdict against the
     * host lattice. Disagreement is the permanent referee's alarm. */
    public static void probeSqlVerdict(String family, boolean hostHeld,
            boolean sqlHeld) {
        if (MUTED.get()) {
            return;
        }
        probeSqlVerdict(family, hostHeld, sqlHeld, "");
    }

    /** RESERVED witness buffer for the DUAL-VERDICT ALARM: a
     * disagreement is the gated-to-zero signal and must never lose its
     * witness to shared-buffer crowding (a 200-cap buffer full of
     * decline rows swallowed the one alarm row, 2026-08-28). */
    private static final ConcurrentLinkedQueue<Row> SQL_DISAGREE_SAMPLES =
            new ConcurrentLinkedQueue<>();

    public static List<Row> sqlDisagreeSamples() {
        return List.copyOf(SQL_DISAGREE_SAMPLES);
    }

    /** The BYTE channel's disagree rows get the same reserved buffer:
     * the pinned-census witnesses must never lose attribution to
     * decline-row crowding of the shared 200-cap (the 2026-08-28
     * lesson, re-learned 2026-09-01 hunting a ±1 count wobble whose
     * rows were exactly the ones past the cap). */
    private static final ConcurrentLinkedQueue<Row> DISAGREE_SAMPLES =
            new ConcurrentLinkedQueue<>();

    public static List<Row> disagreeSamples() {
        return List.copyOf(DISAGREE_SAMPLES);
    }

    /** {@code detail} carries the two canon texts + fine kinds so a
     * disagreement names its own diagnosis in the census. */
    public static void probeSqlVerdict(String family, boolean hostHeld,
            boolean sqlHeld, String detail) {
        if (MUTED.get()) {
            return;
        }
        SQL_CENSUS.merge("claimed " + family, 1L, Long::sum);
        CHANNEL_SEEN.set(true);
        if (hostHeld == sqlHeld) {
            Census.inc(Census.Key.SQL_AGREE);
        } else {
            Census.inc(Census.Key.SQL_DISAGREE);
            // ATTRIBUTION (charter §8.3b adjudication need): an alarm
            // witness without its test name was unactionable — the
            // running test rides CONTEXT_SOURCE
            Row r = new Row(family, hostHeld,
                    "sql-verdict host=" + hostHeld + " sql=" + sqlHeld
                            + " " + detail + " [" + CONTEXT_SOURCE.get()
                            + "]");
            if (SQL_DISAGREE_SAMPLES.size() < 50) {
                SQL_DISAGREE_SAMPLES.add(r);
            }
            sample(r);
        }
    }

    /** R2a: a side the SQL channel declined (non-scalar kind, unclaimed
     * render, lowering refusal) — the host lattice judged instead. The
     * REASON rides the sample buffer so the V6 burn targets families,
     * not a bare count. */
    public static void sqlDeclined() {
        if (MUTED.get()) {
            return;
        }
        sqlDeclined("unclassified");
    }

    public static void sqlDeclined(String reason) {
        if (MUTED.get()) {
            return;
        }
        Census.inc(Census.Key.SQL_DECLINED);
        sample(new Row("sqlDecline", false, reason));
        // leg 3.0 census: the decline attributed to the assert family
        // being adjudicated (the reason's head, before its first ':') —
        // one row per decline EVENT (a grid pair may record one per side)
        int c = reason.indexOf(':');
        String head = c < 0 ? reason : reason.substring(0, c);
        SQL_CENSUS.merge("declined " + CURRENT_FAMILY.get() + " " + head,
                1L, Long::sum);
        if (!CHANNEL_SEEN.get()) {
            // one row per declined ASSERT — what the family's rows sum to
            SQL_CENSUS.merge("declined-asserts " + CURRENT_FAMILY.get(), 1L, Long::sum);
        }
        CHANNEL_SEEN.set(true);
    }

    // ── leg 3.0 (docs/DATABASE_MODE_HOMEWORK_2026_09_18.md §4): the
    // CLAIM / DECLINE census per assert family — what the SQL canon
    // already judges and what it declines, by reason. Attribution
    // only: no verdict reads any of this. CURRENT_FAMILY is the assert
    // family AssertVerdicts is adjudicating (set at its one entry), so
    // a decline recorded deeper (TdsCompare, the wrap) lands on it.
    private static final java.util.concurrent.atomic.AtomicReference<String>
            CURRENT_FAMILY = new java.util.concurrent.atomic.AtomicReference<>("?");
    private static final java.util.concurrent.ConcurrentHashMap<String, Long>
            SQL_CENSUS = new java.util.concurrent.ConcurrentHashMap<>();

    /** A Decimal pair equal in value but not in scale (3.0D vs 3.00D):
     * unequal under the engine's assert seam (host mode), equal under a
     * scale-normalized canon — the D5 amendment's witness count. */

    public static void decimalScaleOnly() {
        if (MUTED.get()) {
            return;
        }
        Census.inc(Census.Key.DECIMAL_SCALE_ONLY);
        sample(new Row("decimalScaleOnly", false,
                "[" + CONTEXT_SOURCE.get() + "]"));
    }

    public static long decimalScaleOnlyCount() {
        return Census.count(Census.Key.DECIMAL_SCALE_ONLY);
    }

    /** An assert of {@code family} was judged by a route that has NO
     * byte channel (the SQL-text rows arm, the rendered-text arm, the
     * grid-pair arm): counted so "never attempted" has a reason. */
    public static void sqlRoute(String family, String route) {
        if (MUTED.get()) {
            return;
        }
        SQL_CENSUS.merge("not-attempted " + family + " " + route, 1L, Long::sum);
        CHANNEL_SEEN.set(true);
    }

    /** The assert being adjudicated RAISED before any channel judged
     * (a side that errors, a failed verdict thrown as AssertFailed):
     * counted on the current family so "never attempted" has a reason. */
    public static void sqlRaised() {
        if (MUTED.get() || CHANNEL_SEEN.get()) {
            return;   // a judged assert that then failed is not "never attempted"
        }
        SQL_CENSUS.merge("not-attempted " + CURRENT_FAMILY.get() + " raised",
                1L, Long::sum);
        CHANNEL_SEEN.set(true);   // a nested entry's raise is one raise
    }

    /** The verdict finished with NO byte verdict and no channel spoke
     * (no claim, no decline, no route): the residue named by
     * construction, so the census always sums. */
    public static void sqlNoChannel() {
        if (MUTED.get() || CHANNEL_SEEN.get()) {
            return;
        }
        SQL_CENSUS.merge("not-attempted " + CURRENT_FAMILY.get() + " no-channel",
                1L, Long::sum);
        CHANNEL_SEEN.set(true);
    }

    /** The current assert's switch arm yielded null (not this arm's
     * shape; the generic path continues): counted, so "adjudicated"
     * minus the channels is explained. */
    public static void sqlFellThrough() {
        if (MUTED.get()) {
            return;
        }
        SQL_CENSUS.merge("not-attempted " + CURRENT_FAMILY.get() + " fell-through",
                1L, Long::sum);
    }

    /** Whether any channel (claim, decline, route) spoke for the current
     * assert — set by them, reset at {@link #sqlFamily}. */
    private static final java.util.concurrent.atomic.AtomicBoolean CHANNEL_SEEN =
            new java.util.concurrent.atomic.AtomicBoolean();

    /** A statement-root assert enters the adjudicator, BEFORE its family
     * is known (the lineage, quantified, if-branch and SQL-text root
     * arms run first): a raise from there lands on its own row. */
    public static void sqlEnter() {
        if (MUTED.get()) {
            return;
        }
        CURRENT_FAMILY.set("(pre-arm)");
        CHANNEL_SEEN.set(false);
    }

    /** Leg 3.1: database mode decided the assert (the verdict row). */
    public static void sqlJudgedInDatabase(String family) {
        if (MUTED.get()) {
            return;
        }
        SQL_CENSUS.merge("judged-in-database " + family, 1L, Long::sum);
        CHANNEL_SEEN.set(true);
    }

    /** Leg 3.1: database mode could NOT decide the assert — the assert
     * failed with this reason; the per-mode ceiling the register pins. */
    public static void sqlUnjudged(String family, String reason) {
        if (MUTED.get()) {
            return;
        }
        int c = reason.indexOf(':');
        String head = c < 0 ? reason : reason.substring(0, c);
        SQL_CENSUS.merge("unjudged " + family + " " + head, 1L, Long::sum);
        CHANNEL_SEEN.set(true);
    }

    /** An assert of {@code family} enters adjudication. */
    public static void sqlFamily(String family) {
        if (MUTED.get()) {
            return;
        }
        CURRENT_FAMILY.set(family);
        CHANNEL_SEEN.set(false);
        SQL_CENSUS.merge("adjudicated " + family, 1L, Long::sum);
    }

    /** The census rows, sorted: {@code adjudicated <family>},
     * {@code claimed <family>} (a byte verdict was produced),
     * {@code declined <family> <reason-head>}. */
    public static java.util.SortedMap<String, Long> sqlCensus() {
        return new java.util.TreeMap<>(SQL_CENSUS);
    }

    public static long sqlDisagreeCount() {
        return Census.count(Census.Key.SQL_DISAGREE);
    }

    public static long sqlDeclinedCount() {
        return Census.count(Census.Key.SQL_DECLINED);
    }

    /** OPEN_REGISTER §5 / X6: a byte-differing Double pair the DECLARED
     * 2-ULP dialect-arithmetic policy adjudicated equal (cross-dialect
     * libm last-ULP drift — H2-derived corpus goldens vs DuckDB
     * transcendentals). Counted so the R3 census can retire or ratify
     * the policy from its real witness set. */
    public static void sqlUlpPolicy(String detail) {
        if (MUTED.get()) {
            return;
        }
        Census.inc(Census.Key.SQL_ULP_POLICY);
        sample(new Row("sqlUlpPolicy", true, detail));
    }

    public static long sqlUlpPolicyCount() {
        return Census.count(Census.Key.SQL_ULP_POLICY);
    }

    /** V7 batch 2: a byte-differing pair the DECLARED TDSNull-sentinel
     * policy adjudicated equal (PureAsserts equalScalar: an EXPECTED
     * 'TDSNull' equals an actual NULL cell — the engine golden's null
     * spelling). Counted like the 2-ULP policy: rides ON TOP of the
     * byte channel, never a disagreement rescue. */

    public static void sqlTdsNullPolicy(String detail) {
        if (MUTED.get()) {
            return;
        }
        Census.inc(Census.Key.SQL_TDSNULL_POLICY);
        sample(new Row("sqlTdsNullPolicy", true, detail));
    }

    public static long sqlTdsNullPolicyCount() {
        return Census.count(Census.Key.SQL_TDSNULL_POLICY);
    }

    // ── V7 (docs/V7_ASSERT_VERDICT_CHARTER.md §4.1): the corpus DUAL
    // CHANNEL census — the harness's host verdict vs the production
    // AssertVerdicts route, per assert FORM (name/arity). DISTINCT from
    // the sql-verdict counters above (those are the production path's
    // INNER referee: PureAsserts vs the DB byte canon); this table
    // compares two whole adjudicators, and its disagree rows ARE batch
    // 2's work list. Measurement only — no entry can affect a verdict.
    private static final java.util.concurrent.ConcurrentHashMap<String, long[]>
            V7_FORMS = new java.util.concurrent.ConcurrentHashMap<>();
    private static final java.util.concurrent.ConcurrentHashMap<String, AtomicLong>
            V7_DECLINES = new java.util.concurrent.ConcurrentHashMap<>();
    private static final int V7_DECLINE_KEY_CAP = 400;
    private static final ConcurrentLinkedQueue<Row> V7_SAMPLES =
            new ConcurrentLinkedQueue<>();
    /** Side-size histogram, log2 buckets (0, 1, 2-3, 4-7, …): the
     * golden-size fact the census's §5-1 homework asked for — V12's
     * VALUES-literal cost bracket rides these counts. */
    private static final java.util.concurrent.atomic.AtomicLongArray
            V7_SIDE_ROWS = new java.util.concurrent.atomic.AtomicLongArray(16);
    /** Per-ROW decline attribution (step-0 census, 2026-08-30): every
     * non-exec-passing decline records {@code test :: form :: reason}.
     * UNCAPPED by doctrine — a census surface with a silent cap reads
     * as "covered everything" when it didn't (the sqltypes top-20
     * lesson); the population is bounded by the sweep's own decline
     * count (~420 at the last pin), never unbounded growth. The
     * exec-passing bucket is EXCLUDED (1,495 verified-comfort rows —
     * not census targets). */
    private static final ConcurrentLinkedQueue<String>
            V7_DECLINE_WITNESSES = new ConcurrentLinkedQueue<>();

    /** Attribution source for disagreement samples — the HARNESS wires
     * its per-test context holder here (invariant 6d: exec never
     * depends on the middle-end, so the supplier is injected). */
    public static volatile java.util.function.Supplier<String>
            CONTEXT_SOURCE = () -> "<unattributed>";

    /** One dual-channel verdict pair: both adjudicators judged. */
    public static void v7Verdict(String form, boolean hostPass,
            boolean prodPass, String detail) {
        if (MUTED.get()) {
            return;
        }
        long[] c = V7_FORMS.computeIfAbsent(form, k -> new long[2]);
        synchronized (c) {
            c[hostPass == prodPass ? 0 : 1]++;
        }
        if (hostPass != prodPass && V7_SAMPLES.size() < SAMPLE_CAP) {
            V7_SAMPLES.add(new Row(form, hostPass,
                    "host=" + (hostPass ? "pass" : "fail")
                            + " prod=" + (prodPass ? "pass" : "fail")
                            + " [" + CONTEXT_SOURCE.get() + "] " + detail));
        }
    }

    /** A NAMED per-form decline (D2: never a silent skip) — the §2 host
     * partition, host-unsupported forms, and production walls. The
     * reason is a bounded classification key, not free prose. */
    public static void v7Declined(String form, String reason) {
        if (MUTED.get()) {
            return;
        }
        String r = reason.length() > 200 ? reason.substring(0, 200) + "…"
                : reason;
        String key = form + " :: " + r;
        if (V7_DECLINES.size() >= V7_DECLINE_KEY_CAP
                && !V7_DECLINES.containsKey(key)) {
            key = form + " :: …overflow";
        }
        V7_DECLINES.computeIfAbsent(key, k -> new AtomicLong())
                .incrementAndGet();
        if (!r.startsWith("assert-sql-text-with-exec-passing")) {
            // the witness carries a WIDER reason cut than the aggregate
            // key (500 vs 200): the key bounds the aggregation space,
            // the witness is diagnosis — the getAll walk contexts and
            // stamp callees live past the key's cut
            String rw = reason.length() > 500
                    ? reason.substring(0, 500) + "…" : reason;
            V7_DECLINE_WITNESSES.add(
                    CONTEXT_SOURCE.get() + " :: " + form + " :: " + rw);
        }
    }

    /** One assert side's fetched element count (histogram feed). */
    public static void v7SideRows(int n) {
        if (MUTED.get()) {
            return;
        }
        int b = n <= 0 ? 0 : Math.min(64 - Long.numberOfLeadingZeros(n), 15);
        V7_SIDE_ROWS.incrementAndGet(b);
    }

    public static long v7DisagreeCount() {
        long d = 0;
        for (long[] c : V7_FORMS.values()) {
            synchronized (c) {
                d += c[1];
            }
        }
        return d;
    }

    public static long v7DeclinedCount() {
        return V7_DECLINES.values().stream().mapToLong(AtomicLong::get).sum();
    }

    /** THE METAMODEL QUARANTINE VOCABULARY (user ruling 2026-08-30,
     * charter §4AF Slice Q; row-level receipts in
     * FULL_RESIDUE_CENSUS_2026_08_30.md §4a): decline reasons that are
     * METAMODEL-ONLY — reflection over the authored artifact, function
     * bodies as data, protocol-node conversion, extension-lambda eval.
     * A PARTITION of the census, never a test exclusion: the entries
     * are the PRODUCTION system's own exact refusal spellings (the
     * CanonDeclines register discipline — never test names), the
     * per-row decline witnesses keep every quarantined row attributed,
     * and the count is EXACT-pinned so any movement is loud. Two
     * receipt-scoped entries (the stamp invariant and the bare-lambda
     * wall are generic spellings): ALL their measured witnesses live
     * inside quarantine-family tests (toPostgresModel / tesIsToOne);
     * an outside witness appears in the census immediately and
     * repromotes the row to the active burn. */
    private static final List<String> METAMODEL_QUARANTINE = List.of(
            // mapping/store reflection — the no-scalar overload refusals
            // (classMappingById RETIRED 2026-09-02 — batch 5;
            // rootClassMappingByClass / _classMappingByClass / view /
            // inferRelationalType RETIRED 2026-09-02 — group F burn: Pure
            // bodies over the metamodel store, natives deleted)
            // toPostgresModel conversion family (newState + the stamp
            // invariant's 17 in-family witnesses + the SQLNull layout)
            "resolved overload 'meta::relational::functions::toPostgresModel::newState'",
            "MULTIPLICITY-STAMP INVARIANT VIOLATED",
            "^meta::relational::metamodel::SQLNull(…) has no canonical layout",
            // function bodies as data (pkOfFunc + the tesIsToOne
            // deactivate/InstanceValue reflection tests)
            "has no property 'expressionSequence'",
            "unknown type 'InstanceValue'",
            "a non-let intermediate statement in a bare lambda literal",
            // PLAN-NODE MODEL WALKS (adjudicated 2026-08-31, §4AE growth
            // rule): allNodes/executionNodes/cast/supportsStream asserts
            // evaluate the tests' filter lambdas over plan-node objects
            // — pure code with no store demand, the same class as
            // pkOfFunc's function-bodies-as-data (a burn attempt via the
            // planWalk side door was REVERTED: it grew the parallel
            // evaluator the one-router ruling forbids). The family waits
            // for the metamodel-as-data program with the rest.
            "class query under TypedMap is not resolvable yet");

    private static boolean quarantined(String key) {
        for (String q : METAMODEL_QUARANTINE) {
            if (key.contains(q)) {
                return true;
            }
        }
        return false;
    }

    /** SLICE-1 channel move (charter §4AF): with the harness's try-run
     * lane deleted, quarantine-family failures surface as TEST-LEVEL
     * walls (the same failure texts, thrown before per-assert
     * adjudication). The partition follows its tests: the runner reports
     * each ERROR wall here, vocabulary-matched by the SAME list. */
    private static final java.util.Set<String> QUARANTINED_WALL_TESTS =
            java.util.Collections.newSetFromMap(
                    new java.util.concurrent.ConcurrentHashMap<>());

    public static void noteWall(String test, String reason) {
        if (quarantined(reason)) {
            QUARANTINED_WALL_TESTS.add(test);
        }
    }

    public static long v7QuarantinedWallCount() {
        return QUARANTINED_WALL_TESTS.size();
    }

    /** Slice Q reader: declines in the metamodel quarantine partition. */
    public static long v7QuarantinedCount() {
        return V7_DECLINES.entrySet().stream()
                .filter(e -> quarantined(e.getKey()))
                .mapToLong(e -> e.getValue().get()).sum();
    }

    /** V7 §8.0 leg 0 — the lane-classification guard's reader: total
     * declines carrying one classification reason (the "form ::
     * reason" key tail), summed across forms. */
    public static long v7DeclinedByReason(String reason) {
        String tail = " :: " + reason;
        return V7_DECLINES.entrySet().stream()
                .filter(e -> e.getKey().endsWith(tail))
                .mapToLong(e -> e.getValue().get()).sum();
    }

    /** Bucket reader: declines whose reason STARTS with {@code bucket}
     * (sub-reasons ride behind " :: " — assert-sql-text-unable-to-exec
     * :: diff-noreplay counts under its bucket). */
    public static long v7DeclinedByReasonPrefix(String bucket) {
        String tail = " :: " + bucket;
        return V7_DECLINES.entrySet().stream()
                .filter(e -> {
                    int i = e.getKey().indexOf(" :: ");
                    return i >= 0 && e.getKey().substring(i + 4)
                            .startsWith(bucket);
                })
                .mapToLong(e -> e.getValue().get()).sum();
    }

    public static String v7Summary() {
        long agree = 0;
        long disagree = 0;
        for (long[] c : V7_FORMS.values()) {
            synchronized (c) {
                agree += c[0];
                disagree += c[1];
            }
        }
        StringBuilder hist = new StringBuilder();
        for (int i = 0; i < V7_SIDE_ROWS.length(); i++) {
            long n = V7_SIDE_ROWS.get(i);
            if (n > 0) {
                hist.append(' ').append(i == 0 ? "0" : i == 1 ? "1"
                        : (1L << (i - 1)) + "-" + ((1L << i) - 1))
                        .append(':').append(n);
            }
        }
        // The user-ratified OUTCOME buckets (2026-08-28): sql-text and
        // test-data are BY-DESIGN partitions with honest strength
        // labels — exec-passing = golden EXECUTED on H2, rows EQUAL
        // (the only comfort bucket); text-only = nothing ran, text is
        // the contract; unable-to-exec = transparent residue by named
        // sub-reason; csv = the TDG compares. declined = real backlog.
        long execPass = v7DeclinedByReasonPrefix(
                "assert-sql-text-with-exec-passing");
        long textOnly = v7DeclinedByReasonPrefix("assert-sql-text-only");
        long noExec = v7DeclinedByReasonPrefix(
                "assert-sql-text-unable-to-exec");
        long csv = v7DeclinedByReasonPrefix("assert-test-data-csv");
        // Slice Q (charter §4AF): the metamodel quarantine splits OUT
        // of `declined` — declined = the ACTIVE burn backlog only
        long quarantinedN = v7QuarantinedCount();
        return "dual-channel agree=" + agree + " disagree=" + disagree
                + " | sql-text: exec-passing=" + execPass
                + " text-only=" + textOnly
                + " UNABLE-TO-EXEC=" + noExec
                + " | test-data-csv=" + csv
                + " | declined=" + (v7DeclinedCount()
                        - execPass - textOnly - noExec - csv - quarantinedN)
                + " | metamodel-quarantined=" + quarantinedN
                + "+walls=" + v7QuarantinedWallCount()
                + " | side-rows" + (hist.isEmpty() ? " none" : hist);
    }

    /** Per-form table + classified decline reasons + disagreement
     * samples — sorted for stable console diffs (DISPLAY ordering only,
     * the SqlTypeCensus report precedent). */
    public static List<String> v7Report() {
        List<String> out = new ArrayList<>();
        V7_FORMS.entrySet().stream()
                .sorted(java.util.Map.Entry.comparingByKey())
                .forEach(e -> {
                    long[] c = e.getValue();
                    long a;
                    long d;
                    synchronized (c) {
                        a = c[0];
                        d = c[1];
                    }
                    out.add("form " + e.getKey() + " agree=" + a
                            + " disagree=" + d);
                });
        V7_DECLINES.entrySet().stream()
                .sorted(java.util.Map.Entry.comparingByKey())
                .forEach(e -> out.add("declined " + e.getKey() + " = "
                        + e.getValue().get()));
        // per-row attribution AFTER the aggregate table (sorted for
        // stable console diffs — display ordering only); witness sums
        // reconcile against the non-exec-passing aggregate counts
        V7_DECLINE_WITNESSES.stream().sorted()
                .forEach(w -> out.add("decline-witness " + w));
        V7_SAMPLES.forEach(r -> out.add("disagree-witness " + r.family()
                + " " + r.detail()));
        return out;
    }

    public static String summary() {
        return "agree=" + Census.count(Census.Key.DIVERGENCE_AGREE) + " disagree=" + Census.count(Census.Key.DIVERGENCE_DISAGREE)
                + " residue=" + Census.count(Census.Key.DIVERGENCE_RESIDUE)
                + " | sql-verdict agree=" + Census.count(Census.Key.SQL_AGREE)
                + " disagree=" + Census.count(Census.Key.SQL_DISAGREE)
                + " declined=" + Census.count(Census.Key.SQL_DECLINED)
                + " ulp-policy=" + Census.count(Census.Key.SQL_ULP_POLICY)
                + " row-order-canon=" + Census.count(Census.Key.ROW_ORDER_CANON)
;
    }

    public static long disagreeCount() {
        return Census.count(Census.Key.DIVERGENCE_DISAGREE);
    }

    public static long residueCount() {
        return Census.count(Census.Key.DIVERGENCE_RESIDUE);
    }

    public static List<Row> samples() {
        return List.copyOf(SAMPLES);
    }

    public static void reset() {
        Census.reset(Census.Key.DIVERGENCE_AGREE, Census.Key.DIVERGENCE_DISAGREE, Census.Key.DIVERGENCE_RESIDUE,
                Census.Key.SQL_AGREE, Census.Key.SQL_DISAGREE, Census.Key.SQL_DECLINED, Census.Key.SQL_ULP_POLICY,
                Census.Key.SQL_TDSNULL_POLICY, Census.Key.ROW_ORDER_CANON);
        SAMPLES.clear();
        SQL_DISAGREE_SAMPLES.clear();
        DISAGREE_SAMPLES.clear();
        V7_FORMS.clear();
        V7_DECLINES.clear();
        V7_SAMPLES.clear();
        for (int i = 0; i < V7_SIDE_ROWS.length(); i++) {
            V7_SIDE_ROWS.set(i, 0);
        }
    }
}
