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

    /** Converse-tripwire breaches: a wire NULL arrived under a label
     * promising always-present (per settled statement-column, not per
     * cell). Measure-first (E2E audit) — adjudicated before pinning. */
    private static final LongAdder NULL_BREACH = new LongAdder();

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

    // ------------------------------------------------------------------
    // §E3 SLACK CENSUS (post-flip precision instrument — the breach
    // tripwire's CONVERSE): the breach pin can only falsify a
    // never-null claim; nothing bounds over-loosening. Watch every
    // nullable=TRUE final column; per settled statement, a column that
    // DELIVERED VALUES and never a NULL is a SLACK ROW — a spot where
    // the label may be looser than the data. Deliberately NOT
    // pinnable: absence of NULLs in test data is evidence, never
    // proof — this census RANKS the precision refinements (one-row
    // subquery proofs, per-field struct presence, the
    // INNER-equivalent join shape), it does not adjudicate them.
    // ------------------------------------------------------------------
    private static final LongAdder SLACK_ROWS = new LongAdder();
    private static final LongAdder SLACK_CONFIRMED = new LongAdder();
    private static final LongAdder SLACK_NO_EVIDENCE = new LongAdder();
    private static final Map<String, LongAdder> SLACK_CLASSES =
            new ConcurrentHashMap<>();
    private static final Map<String, List<String>> SLACK_SAMPLES =
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

    private static final LongAdder WIRE_AGREE = new LongAdder();
    private static final LongAdder WIRE_DELIVERED = new LongAdder();
    private static final LongAdder WIRE_ADOPT_PENDING = new LongAdder();
    private static final LongAdder WIRE_NULL_AMBIG = new LongAdder();
    private static final LongAdder WIRE_DIVERGE = new LongAdder();
    private static final LongAdder WIRE_UNKNOWN = new LongAdder();

    /** CONTRACT PROGRAM step 1 — THE WIRE CENSUS: our label (the
     * contract) vs the ResultSet's OWN metadata (the factual wire, no
     * extra round trip — it rides with the data). Ground truth per
     * dialect, total expression coverage, zero inference. Phase 1 it
     * MEASURES the guess-vs-reality gap (each divergence adjudicates
     * adopt / conform / fix-emitter); phase 3 it is the always-green
     * tripwire. FAILURE-ISOLATED like every instrument (the
     * Dual.alias lesson): a metadata hiccup counts as unknown, never
     * throws into execution. */
    public static void probeWire(SqlQuery plan, java.sql.ResultSet rs,
            String dialect) {
        WIRE_WATCH.remove();   // defensive: a statement that errored
                               // before settling must not leak watches
        if (PROBE_SUSPENDED.get()) {
            return;   // V7 probe isolation — see probe()
        }
        try {
            java.sql.ResultSetMetaData md = rs.getMetaData();
            List<OutputCol> outs = plan.outputs();
            if (outs.isEmpty()) {
                // D2 ADJUDICATED (2026-08-26): a ZERO-OUTPUT plan (a
                // late-bound raw grid / star root — the wire kind is
                // the schema authority there) carries no per-column
                // claim to check — the walk's own star-frame doctrine,
                // not an unknown. Both corpus rows were this shape
                // (filter::in::H2Test, ddl::dropAndCreateTable).
                return;
            }
            if (md.getColumnCount() != outs.size()) {
                if (Executor.hasPivot(plan)) {
                    // D2 ADJUDICATED (2026-08-26): a dynamic pivot's
                    // result columns are data-dependent (one per
                    // pivoted VALUE — resolveColumns' own doctrine),
                    // so the static outputs cannot enumerate them —
                    // no per-column claim, not an unknown. All 8 pct
                    // rows were pivot tests.
                    return;
                }
                // D2 (§4bZ-V D2): every unknown carries a CLASS and a
                // witness — "never once examined" ends here
                WIRE_UNKNOWN.increment();
                String cls = "wire-unknown[" + dialect
                        + "] shape-mismatch outs=" + outs.size()
                        + " meta=" + md.getColumnCount();
                classify(cls);
                sample(cls, plan.getClass().getSimpleName());
                return;
            }
            for (int i = 0; i < outs.size(); i++) {
                String label = wireSpelling(outs.get(i).type());
                String meta = normalizeMeta(md.getColumnTypeName(i + 1));
                // E2E-audit CONVERSE tripwire (measure-first): a label
                // promising always-present is a claim the wire can
                // refute — watch every nullable=false column for NULL
                // sightings; settle names the breaches. (The N1 arm
                // made literal pads declare slot truth; this measures
                // whether frames ABOVE them re-derive the pure [1]
                // and under-declare — TypeFact carries no nullability,
                // so expression re-reads cannot transport it.)
                if (!outs.get(i).nullable()) {
                    watch(dialect).required.put(i + 1,
                            new WatchCol(i + 1,
                                    String.valueOf(outs.get(i).type()),
                                    watchWitness(plan, i)));
                } else {
                    // §E3 slack census: the converse watch — a
                    // nullable label that only ever delivers values
                    // is a slack candidate (see the census banner)
                    watch(dialect).loose.put(i + 1, new LooseCol(i + 1,
                            "nul-slack[" + looseShape(plan, i) + "] "
                                    + outs.get(i).type(),
                            watchWitness(plan, i)));
                }
                if (label == null || meta.isEmpty()) {
                    WIRE_UNKNOWN.increment();
                    String cls = "wire-unknown[" + dialect + "] "
                            + (label == null
                                    ? "no-spelling label-type="
                                            + outs.get(i).type()
                                    : "empty-meta label=" + label);
                    classify(cls);
                    sample(cls, outs.get(i).name());
                    continue;
                }
                if (label.equals(meta)) {
                    WIRE_AGREE.increment();
                } else if (delivers(outs.get(i).type(), meta)) {
                    // THE DELIVERY RELATION (adjudicated 2026-08-23):
                    // value-subset narrowing (every INTEGER fits the
                    // BIGINT contract; DECIMAL(p,s) fits DECIMAL(P>=p,s))
                    // and the registered carrier conventions — the
                    // admissible() pairs read at the wire
                    WIRE_DELIVERED.increment();
                    classify("wire-delivered[" + dialect + "] " + label
                            + " <- " + meta);
                } else if (meta.equals("HUGEINT")
                        && label.equals("BIGINT")) {
                    // ADOPT-PENDING (the testLargePlus adjudication):
                    // integer aggregates legitimately exceed 64 bits —
                    // pure semantics sides with the WIRE; the CONTRACT
                    // is what widens (at construction, the builder
                    // slice). Counted visibly until labels widen —
                    // decode is BigInteger-safe today.
                    WIRE_ADOPT_PENDING.increment();
                    classify("wire-adopt-pending[" + dialect
                            + "] BIGINT <- HUGEINT");
                    sample("wire-adopt-pending[" + dialect
                            + "] BIGINT <- HUGEINT", outs.get(i).name());
                } else if (meta.equals("INTEGER")
                        && !integerFamily(outs.get(i).type())) {
                    // AMBIGUOUS metadata (D1, §4bZ-V): DuckDB spells an
                    // all-NULL column INTEGER — indistinguishable from
                    // a real integer wire without VALUE evidence. Not
                    // counted here: the column is WATCHED, the
                    // Executor's fetch funnel marks any non-null driver
                    // object, and settleWire() adjudicates at statement
                    // end — valued = a REAL divergence (rides the
                    // EQUALITY-0 diverge pins, loud), all-NULL = no
                    // factual wire type exists (named, benign).
                    watch(dialect).add(new WatchCol(i + 1, label,
                            watchWitness(plan, i)));
                } else {
                    String cls = "wire[" + dialect + "] label=" + label
                            + " <> meta=" + meta;
                    WIRE_DIVERGE.increment();
                    classify(cls);
                    sample(cls, outs.get(i).name());
                }
            }
        } catch (java.sql.SQLException | RuntimeException e) {
            // instrument isolation: measurement must never throw into
            // execution — an unreadable metadata is a counted unknown
            // (classed + witnessed since D2: the probe error's own
            // shape is the adjudication evidence)
            WIRE_UNKNOWN.increment();
            String cls = "wire-unknown[" + dialect + "] probe-error="
                    + e.getClass().getSimpleName();
            classify(cls);
            String msg = String.valueOf(e.getMessage());
            sample(cls, msg.length() > 120 ? msg.substring(0, 120) : msg);
        }
    }

    /** One D1-watched result column: 1-based JDBC index, our label's
     * wire spelling, and the witness text (output name + the
     * projection's expression sketch and stored fact — the mechanism
     * locator a bare column name cannot give). */
    private record WatchCol(int column, String label, String witness) {
    }

    /** One slack-watched nullable column: its census class (projection
     * shape + label type, built at probe time — the plan is gone by
     * settle) and its witness. */
    private record LooseCol(int column, String cls, String witness) {
    }

    /** The slack class's shape key: the projection's expression shape
     * when the plan is a shape-aligned select, else the plan kind. */
    private static String looseShape(SqlQuery plan, int i) {
        if (plan instanceof SqlSelect s
                && s.projections().size() == s.outputs().size()) {
            return shapeOf(s.projections().get(i).expr());
        }
        return plan.getClass().getSimpleName();
    }

    /** The witness text for a watched column: output name plus, when
     * the plan is a shape-aligned select, the projection's sketch and
     * stored fact. */
    private static String watchWitness(SqlQuery plan, int i) {
        String name = plan.outputs().get(i).name();
        if (plan instanceof SqlSelect s
                && s.projections().size() == s.outputs().size()) {
            SqlExpr e = s.projections().get(i).expr();
            // §E3 M-N3: the full fact prints (the M-N1 legacy-format
            // shim retired with the flip — a breach witness's
            // nullability IS the evidence now)
            return name + " := " + sketch(e) + " fact=" + e.type();
        }
        return name;
    }

    /** Per-statement D1 watch state — set by {@link #probeWire},
     * marked by the Executor's fetch funnel, settled at statement end.
     * Thread-confined like {@link #CONTEXT}. */
    private static final ThreadLocal<WireWatch> WIRE_WATCH =
            new ThreadLocal<>();

    private static final class WireWatch {
        private final String dialect;
        private final List<WatchCol> candidates = new ArrayList<>();
        private final java.util.Set<Integer> valued =
                new java.util.HashSet<>();
        /** E2E-audit converse tripwire: columns whose label PROMISES
         * always-present ({@code nullable=false}), watched for wire
         * NULL sightings. */
        private final Map<Integer, WatchCol> required =
                new java.util.HashMap<>();
        private final java.util.Set<Integer> nulled =
                new java.util.HashSet<>();
        /** §E3 slack census: nullable-labeled columns watched for the
         * CONVERSE — values but never a NULL. */
        private final Map<Integer, LooseCol> loose =
                new java.util.HashMap<>();
        private final java.util.Set<Integer> looseNulled =
                new java.util.HashSet<>();

        private WireWatch(String dialect) {
            this.dialect = dialect;
        }

        private void add(WatchCol c) {
            candidates.add(c);
        }
    }

    private static WireWatch watch(String dialect) {
        WireWatch w = WIRE_WATCH.get();
        if (w == null) {
            w = new WireWatch(dialect);
            WIRE_WATCH.set(w);
        }
        return w;
    }

    /** The Executor's fetch funnel reports a NON-NULL driver object
     * for column {@code column} (1-based) — the value evidence D1
     * needs. No-op unless the statement carries watched columns. */
    public static void wireValueSeen(int column) {
        WireWatch w = WIRE_WATCH.get();
        if (w != null) {
            w.valued.add(column);
        }
    }

    /** The Executor's fetch funnel reports a NULL driver object —
     * the converse tripwire's evidence: a wire NULL under a label
     * that promised always-present. No-op without a watch. */
    public static void wireNullSeen(int column) {
        WireWatch w = WIRE_WATCH.get();
        if (w == null) {
            return;
        }
        if (w.required.containsKey(column)) {
            w.nulled.add(column);
        }
        if (w.loose.containsKey(column)) {
            w.looseNulled.add(column);
        }
    }

    /** Statement end (the Executor's finally): adjudicate every
     * watched int-or-null column on its value evidence. A VALUED
     * column under a non-integer label is a real divergence — counted
     * in the diverge bucket every lane pins at EQUALITY-0, so it goes
     * loud. An all-NULL column has NO factual wire type (DuckDB's
     * INTEGER spelling is a driver placeholder) — counted and named,
     * benign. */
    public static void settleWire() {
        WireWatch w = WIRE_WATCH.get();
        if (w == null) {
            return;
        }
        WIRE_WATCH.remove();
        for (Integer col : w.nulled) {
            WatchCol c = w.required.get(col);
            if (c == null) {
                continue;   // nulled only ever holds required keys;
                            // guard for the checker
            }
            String cls = "wire-null-under-required-label[" + w.dialect
                    + "] " + c.label();
            NULL_BREACH.increment();
            classify(cls);
            sample(cls, c.witness());
        }
        for (WatchCol c : w.candidates) {
            if (w.valued.contains(c.column())) {
                String cls = "wire[" + w.dialect + "] label=" + c.label()
                        + " <> meta=INTEGER(valued)";
                WIRE_DIVERGE.increment();
                classify(cls);
                sample(cls, c.witness());
            } else {
                String cls = "wire-int-or-null-empty[" + w.dialect
                        + "] label=" + c.label();
                WIRE_NULL_AMBIG.increment();
                classify(cls);
                sample(cls, c.witness());
            }
        }
        for (LooseCol c : w.loose.values()) {
            if (w.looseNulled.contains(c.column())) {
                SLACK_CONFIRMED.increment();   // the label earned its keep
            } else if (w.valued.contains(c.column())) {
                // values delivered, never a NULL — a slack candidate
                SLACK_ROWS.increment();
                SLACK_CLASSES.computeIfAbsent(c.cls(),
                        k -> new LongAdder()).increment();
                List<String> ws = SLACK_SAMPLES.computeIfAbsent(c.cls(),
                        k -> java.util.Collections.synchronizedList(
                                new ArrayList<>()));
                if (ws.size() < SAMPLES_PER_CLASS) {
                    ws.add(c.witness());
                }
            } else {
                SLACK_NO_EVIDENCE.increment();   // zero rows — no verdict
            }
        }
    }

    /** §E3 slack census read surface (non-pinnable — evidence, not
     * proof; the runner prints and dumps). */
    public static String slackSummary() {
        return "slack=" + SLACK_ROWS.sum()
                + " confirmed-nullable=" + SLACK_CONFIRMED.sum()
                + " no-evidence=" + SLACK_NO_EVIDENCE.sum();
    }

    /** The slack decomposition (summary + classes + witnesses),
     * largest classes first. */
    public static List<String> slackReport() {
        List<String> out = new ArrayList<>();
        out.add(slackSummary());
        SLACK_CLASSES.entrySet().stream()
                .sorted((a, b) -> Long.compare(b.getValue().sum(),
                        a.getValue().sum()))
                .forEach(en -> {
                    out.add(en.getValue().sum() + "x " + en.getKey());
                    SLACK_SAMPLES.getOrDefault(en.getKey(), List.of())
                            .forEach(wt -> out.add("  " + wt));
                });
        return out;
    }

    /** Does the wire's factual type SATISFY the label's contract
     * without value loss? Exact is handled by the caller; this covers
     * value-subset narrowing and the registered carrier conventions
     * (metaToType + admissible — ONE relation, read from both sides). */
    private static boolean delivers(SqlType label, String meta) {
        // the kind-faithful carriers: spelling TEXT is their physical
        // form on every backend (F10 proper + §4bZ-V B3 + the
        // disagree-9 burn's DECIMAL_TEXT — the registered pairs)
        if ((label == SqlType.Scalar.LITERAL
                || label == SqlType.Scalar.TEMPORAL_TEXT
                || label == SqlType.Scalar.DECIMAL_TEXT)
                && meta.equals("VARCHAR")) {
            return true;
        }
        // integer-width chain: every narrower integer fits
        if (label == SqlType.Scalar.BIGINT
                && (meta.equals("INTEGER") || meta.equals("SMALLINT")
                        || meta.equals("TINYINT"))) {
            return true;
        }
        if (label == SqlType.Scalar.INTEGER
                && (meta.equals("SMALLINT") || meta.equals("TINYINT"))) {
            return true;
        }
        // NUMERIC CHARTER Rule 2 (docs/NUMERIC_CHARTER_2026_09_17.md): a
        // Float-DECLARED column (label DOUBLE) may arrive as the database's
        // DECIMAL — bare literals and DECIMAL columns keep the database's
        // own kind inside the query; the label carries the declared kind
        // and the carrier keeps its digits (the engine's TDS decode, `* 1.0`
        // over a BigDecimal-backed Float). Delivered, never a divergence.
        if (label == SqlType.Scalar.DOUBLE && meta.startsWith("DECIMAL(")) {
            return true;
        }
        // decimal narrowing at the same scale
        if (label instanceof SqlType.Decimal d && meta.startsWith("DECIMAL(")) {
            try {
                String[] ps = meta.substring(8, meta.length() - 1).split(",");
                int p = Integer.parseInt(ps[0].trim());
                int sc = Integer.parseInt(ps[1].trim());
                return sc == d.scale() && p <= d.precision();
            } catch (NumberFormatException | IndexOutOfBoundsException e) {
                return false;
            }
        }
        SqlType mt = metaToType(meta);
        return mt != null && SqlTyping.subsumes(label, mt);
    }

    private static boolean integerFamily(SqlType t) {
        return t == SqlType.Scalar.BIGINT || t == SqlType.Scalar.INTEGER
                || t == SqlType.Scalar.HUGEINT;
    }

    private static @com.legend.base.Nullable SqlType metaToType(String meta) {
        return switch (meta) {
            case "VARCHAR" -> SqlType.Scalar.VARCHAR;
            case "JSON" -> SqlType.Scalar.JSON;
            case "DATE" -> SqlType.Scalar.DATE;
            case "TIMESTAMP" -> SqlType.Scalar.TIMESTAMP;
            case "DOUBLE" -> SqlType.Scalar.DOUBLE;
            case "BOOLEAN" -> SqlType.Scalar.BOOLEAN;
            case "BIGINT" -> SqlType.Scalar.BIGINT;
            default -> null;
        };
    }

    /** Our label in the wire's own vocabulary (DuckDB-family type
     * names), for comparison against getColumnTypeName. Composites
     * compare by HEAD (full struct field lists are driver-fragile).
     * Null = no spelling (compare impossible — counted unknown). */
    private static @com.legend.base.Nullable String wireSpelling(SqlType t) {
        if (t instanceof SqlType.Scalar sc) {
            return sc.name();
        }
        if (t instanceof SqlType.Decimal d) {
            return "DECIMAL(" + d.precision() + "," + d.scale() + ")";
        }
        if (t instanceof SqlType.Array) {
            return "ARRAY";
        }
        if (t instanceof SqlType.Struct) {
            return "STRUCT";
        }
        if (t instanceof SqlType.Map) {
            return "MAP";
        }
        return null;
    }

    private static String normalizeMeta(@com.legend.base.Nullable String name) {
        if (name == null) {
            return "";
        }
        String n = name.toUpperCase(java.util.Locale.ROOT).trim();
        if (n.startsWith("STRUCT")) {
            return "STRUCT";
        }
        if (n.startsWith("MAP")) {
            return "MAP";
        }
        if (n.endsWith("[]") || n.startsWith("ARRAY")) {
            return "ARRAY";
        }
        // driver spelling families -> our scalar names
        if (n.equals("CHARACTER VARYING") || n.startsWith("VARCHAR")) {
            return "VARCHAR";
        }
        if (n.startsWith("DECIMAL") || n.startsWith("NUMERIC")) {
            return n.replace("NUMERIC", "DECIMAL").replace(" ", "");
        }
        if (n.startsWith("TIMESTAMP")) {
            return "TIMESTAMP";
        }
        return n;
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

    public static long wireDivergeCount() {
        return WIRE_DIVERGE.sum();
    }

    /** Converse-tripwire breaches (E2E audit): wire NULL under an
     * always-present label — the nullability the expression channel
     * cannot transport (TypeFact carries no nullable dimension).
     * Values are engine-correct; the label under-declares. Burns at
     * the chartered nullability-inference leg. */
    public static long nullBreachCount() {
        return NULL_BREACH.sum();
    }

    /** Probes that could not adjudicate (shape mismatch on a
     * non-empty-outputs plan, unreadable metadata, probe error) —
     * classed and witnessed since D2; zero-output plans are a no-claim
     * skip, not an unknown. */
    public static long wireUnknownCount() {
        return WIRE_UNKNOWN.sum();
    }

    /** D1-settled all-NULL columns: metadata said INTEGER, value
     * evidence said NO VALUES EXIST — no factual wire type to compare.
     * Grows with all-NULL result columns (query shape), never with
     * label bugs (those land in diverge, pinned EQUALITY-0). */
    public static long wireIntOrNullEmptyCount() {
        return WIRE_NULL_AMBIG.sum();
    }

    public static long wireAdoptPendingCount() {
        return WIRE_ADOPT_PENDING.sum();
    }

    public static String summary() {
        return "plans=" + PLANS.sum() + " cols: agree=" + AGREE.sum()
                + " subsumed=" + SUBSUMED.sum()
                + " bottom-ok=" + BOTTOM_OK.sum()
                + " raises=" + RAISES_OK.sum()
                + " bottom-mult-backlog=" + BOTTOM_MULT.sum()
                + " mismatch=" + MISMATCH.sum() + " untyped=" + UNTYPED.sum()
                + " | wire: agree=" + WIRE_AGREE.sum()
                + " delivered=" + WIRE_DELIVERED.sum()
                + " adopt-pending=" + WIRE_ADOPT_PENDING.sum()
                + " int-null-empty=" + WIRE_NULL_AMBIG.sum()
                + " null-breach=" + NULL_BREACH.sum()
                + " diverge=" + WIRE_DIVERGE.sum()
                + " unknown=" + WIRE_UNKNOWN.sum();
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
