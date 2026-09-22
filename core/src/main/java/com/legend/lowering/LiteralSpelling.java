// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.lowering;

import com.legend.compiler.element.type.Type;
import com.legend.sql.SqlExpr;
import com.legend.sql.SqlFn;
import com.legend.sql.SqlType;

import java.util.List;

/**
 * THE ONE OWNER of pure's value-spelling grammar in SQL (F10 proper
 * slice 1, docs/F10_CARRIER_DESIGN.md §3): every place that writes a
 * pure value AS TEXT inside a query builds its spelling HERE. Before
 * this module the knowledge lived in three divergent copies — the
 * verdict lane's canon leaves ({@code CanonicalRenderSql}), the
 * execution lane's print forms ({@code Scalars.floatRepr} +
 * {@code MixedEncoding} element ids), and the host-side parse.
 *
 * <p>TWO NAMED TABLES, deliberately kept apart (never silently merged):
 *
 * <ul>
 *   <li><b>LITERAL grammar</b> ({@link #literal}, {@link #leaf}) — the
 *       six mutually disjoint source spellings (bare int, pointed
 *       float, D-suffix decimal, quoted string, bare bool, %-prefixed
 *       temporal). The byte-verdict language, and — slice 2 — the
 *       kind-faithful carrier's cell encoding.</li>
 *   <li><b>PRINT forms</b> ({@link #floatPrint},
 *       {@link #decimalPrintD}, {@link #datePrint},
 *       {@link #dateTimePrint}) — pure's toString output as the
 *       execution wire spells it today (dates WITHOUT the % prefix,
 *       DateTime with the +0000 suffix, Decimal with the D the canon
 *       strips back off).</li>
 * </ul>
 *
 * <p>KNOWN DIVERGENCES between the tables (recorded, resolved by later
 * slices — slice 1 is byte-identical by charter):
 * float — {@link #floatCanon} unfolds EVERY exponent textually and
 * unifies zeros to '0.0'; {@link #floatPrint} re-renders through
 * DECIMAL(38,18)/HUGEINT and keeps the exponent beyond that envelope.
 * temporal — the LITERAL grammar prefixes %, print forms do not;
 * {@link #temporalCanon} normalizes both wire spellings through one
 * text pipeline.
 */
public final class LiteralSpelling {

    private LiteralSpelling() {
    }

    // ==================================================================
    // LITERAL grammar (verdict canon; slice-2 carrier)
    // ==================================================================

    /** The canon LEAF print of a scalar kind (no literal framing) —
     * null = unclaimed kind (the caller declines, counted). */
    public static @com.legend.base.Nullable SqlExpr leaf(SqlExpr v, Type t) {
        if (t == Type.Primitive.STRING) {
            return v;
        }
        if (t == Type.Primitive.BOOLEAN || t == Type.Primitive.INTEGER
                || t == Type.Primitive.STRICT_DATE) {
            // DuckDB casts: booleans print true/false, integers bare,
            // dates ISO — already the H1 forms
            return new SqlExpr.Cast(v, SqlType.Scalar.VARCHAR);
        }
        if (t == Type.Primitive.DECIMAL
                || t instanceof Type.PrecisionDecimal) {
            // PrecisionDecimal IS Decimal with a declared shape — same
            // scale-normalized canonical form (V6 burn)
            return decimalCanon(v);
        }
        if (t instanceof Type.EnumType) {
            // enum values ride the wire as their NAMES (the canonical
            // form per H1: bare member name) — the kind gate already
            // scoped equality to the SAME enumeration
            return v;
        }
        if (t == Type.Primitive.FLOAT) {
            // (NUMERIC CHARTER Rule 2 is applied by the CALLER for a
            // Float-DECLARED side — CanonicalRenderSql.wrapWithCanon —
            // never here: this leaf also spells the FLOAT candidate of
            // an unrefined Number side, whose wire kind is the answer)
            return floatCanon(v);
        }
        if (t == Type.Primitive.DATE_TIME || t == Type.Primitive.DATE) {
            return temporalCanon(v);
        }
        return null;
    }

    /** The full PURE-LITERAL spelling: {@link #leaf} plus the framing
     * that makes the six forms mutually disjoint (quotes + escapes for
     * strings, D suffix for decimals, % prefix for temporals). */
    public static @com.legend.base.Nullable SqlExpr literal(SqlExpr v, Type kind) {
        if (kind instanceof Type.EnumType et) {
            if ("meta::pure::metamodel::type::Enum".equals(et.fqn())) {
                // the ABSTRACT Enum declaration (a mapping's toDomainValue,
                // an EnumValueMapping's .enum): the wire holds the NAME and
                // the enumeration is a row fact the metamodel does not
                // carry yet — no spelling (a spelled 'Enum.NAME' would
                // fabricate inequality against a concrete enumeration)
                return null;
            }
            // pure's own enum literal: Enumeration.NAME — the wire carries
            // the NAME (a TypedEnumValue lowers to it; a mapped cell
            // decodes to it); the enumeration is the declaration's,
            // static on every side that is enum-DECLARED. Disjoint from
            // the six primitive spellings (unquoted, carries '::'), so an
            // enum never byte-equals its name string, and two
            // enumerations' same-named values never equal (pure's rule —
            // stricter than the host judge, which compares the names).
            return SqlExpr.Call.of(SqlFn.CONCAT,
                    new SqlExpr.StringLit(et.fqn() + "."), v);
        }
        SqlExpr leaf = leaf(v, kind);
        if (leaf == null) {
            return null;
        }
        if (kind == Type.Primitive.STRING) {
            // pure string literal: backslash then quote escape, quoted
            SqlExpr escaped = SqlExpr.Call.of(SqlFn.REPLACE,
                    SqlExpr.Call.of(SqlFn.REPLACE, leaf,
                            new SqlExpr.StringLit("\\"),
                            new SqlExpr.StringLit("\\\\")),
                    new SqlExpr.StringLit("'"),
                    new SqlExpr.StringLit("\\'"));
            return SqlExpr.Call.of(SqlFn.CONCAT,
                    SqlExpr.Call.of(SqlFn.CONCAT,
                            new SqlExpr.StringLit("'"), escaped),
                    new SqlExpr.StringLit("'"));
        }
        if (kind == Type.Primitive.DECIMAL
                || kind instanceof Type.PrecisionDecimal) {
            // a PrecisionDecimal IS a Decimal with declared shape — its
            // pure literal is D-suffixed the same (grammar hole found
            // by the 2b select carrier: the typed side's candidate
            // spelled '1.0' against the carrier's '1.0D')
            return SqlExpr.Call.of(SqlFn.CONCAT, leaf,
                    new SqlExpr.StringLit("D"));
        }
        if (kind == Type.Primitive.STRICT_DATE
                || kind == Type.Primitive.DATE_TIME
                // the abstract Date stamp (grid schema columns carry
                // it) is a temporal too — pure spells every temporal
                // literal %-prefixed; leaf() already claims DATE, only
                // the prefix was missing (V7 leg-1 alarm, dates family)
                || kind == Type.Primitive.DATE) {
            return SqlExpr.Call.of(SqlFn.CONCAT,
                    new SqlExpr.StringLit("%"), leaf);
        }
        return leaf;   // Integer bare, Float with its point, Boolean bare
    }

    /**
     * VALUE-LANE WIRE-CELL EGRESS CONFORMANCE (disagree-9 burn,
     * user-adjudicated 2026-08-31; receipts VERDICT_DISAGREEMENT_BURN
     * R3/R5/R8): where a DB-computed cell egresses into the pure VALUE
     * domain, the engine's own decode governs the observable —
     * <ul>
     *   <li>TIMESTAMP cells decode at NINE subsecond digits (the
     *       engine's fromSQLTimestamp {@code %09d} — BOTH its
     *       transform paths), spelled here as the precision-faithful
     *       TEMPORAL_TEXT carrier so the byte canon preserves it and
     *       the executor parses it (no new decode arm);</li>
     *   <li>DECIMAL cells decode SCALE-CANONICAL (the store lane
     *       erases wire scale before pure equality ever runs), spelled
     *       as the D-suffixed DECIMAL_TEXT carrier.</li>
     * </ul>
     * LITERAL-rooted expressions are NOT wire cells — written
     * precision/scale is pure-observable and their own carriers
     * (TEMPORAL_TEXT via RootLiterals, DecimalLit scale) already hold
     * it; they pass through untouched. TDS cells never reach this
     * (the raw lane keeps driver spellings — R6). Returns null when no
     * conformance applies (caller keeps the cell as built).
     */
    /** The READ LANE the conformance serves — each pins its own
     * receipts (all measured, 2026-08-31):
     * <ul>
     *   <li>{@code SCALAR_ROOT}: column-rooted cells only — PCT pins
     *       pure-COMPUTED scalars (timeBucket, arithmetic) at
     *       PURE-defined precision, and a bare literal must keep its
     *       TIMESTAMP carrier (the Any-pair literal channel);</li>
     *   <li>{@code MAP_CHANNEL}: as SCALAR_ROOT plus WRITTEN temporal
     *       literals spell their static text (the milestoning
     *       population constants ARE strings in the engine);</li>
     *   <li>{@code GRID_FETCH}: EVERY temporal cell — a grid under a
     *       values-read is one ResultSet read per column, computed
     *       cells included (engine ResultSetValueHandlers keys on the
     *       RESULTSET type; witnesses parseDate/adjustDate re-opened
     *       when the deleted host twin's blanket decode was replaced
     *       by the column-rooted form).</li>
     * </ul> */
    enum ValueLane { SCALAR_ROOT, MAP_CHANNEL, GRID_FETCH }

    static SqlExpr.@com.legend.base.Nullable Cast wireValueEgress(SqlExpr e,
            SqlType declared, ValueLane lane) {
        // the DECLARED egress label (the pure type's SQL mapping) keys
        // the decode — the engine transformer's own key (R8 dispatches
        // by pure property type); a tree-typed expr that KNOWS it is a
        // TEXT CARRIER (literal round-trips, variant/identity text,
        // JSON) overrides and skips — but a MODEL-vs-PHYSICAL temporal
        // skew (store declares DATE, DDL made TIMESTAMP — the
        // jsonDateWrap class) must NOT skip: the runtime typeof
        // dispatch below owns it
        if (e.type() instanceof com.legend.sql.TypeFact.Typed t
                && (t.type() == SqlType.Scalar.VARCHAR
                        || t.type() == SqlType.Scalar.TEMPORAL_TEXT
                        || t.type() == SqlType.Scalar.DECIMAL_TEXT
                        || t.type() == SqlType.Scalar.LITERAL
                        || t.type() == SqlType.Scalar.JSON)) {
            return null;
        }
        if (declared == SqlType.Scalar.TIMESTAMP) {
            // scope per lane (see ValueLane); the decode itself: a
            // physically-DATE cell decodes date-only, everything else
            // at NINE subsecond digits — runtime typeof dispatch (the
            // jsonDateWrap idiom; setup DDL can diverge from the store
            // declaration). NULL propagates by its own arm.
            if (columnRooted(e) || (lane == ValueLane.GRID_FETCH
                    && staticTemporalText(e, true) == null)) {
                SqlExpr nine = SqlExpr.Call.of(SqlFn.STRFTIME, e,
                        new SqlExpr.FormatLit(
                                com.legend.sql.DateFmt.ISO_NANO));
                return new SqlExpr.Cast(new SqlExpr.Case(List.of(
                        new SqlExpr.Case.When(
                                SqlExpr.Call.of(SqlFn.IS_NULL, e),
                                new SqlExpr.NullLit()),
                        new SqlExpr.Case.When(
                                SqlExpr.Call.of(SqlFn.EQUAL,
                                        SqlExpr.Call.of(SqlFn.TYPEOF, e),
                                        new SqlExpr.StringLit("DATE")),
                                SqlExpr.Call.of(SqlFn.STRFTIME, e,
                                        new SqlExpr.FormatLit(
                                                com.legend.sql.DateFmt
                                                        .DATE)))),
                        nine),
                        SqlType.Scalar.TEMPORAL_TEXT);
            }
            // WRITTEN temporal literals spell STATICALLY — the
            // TIMESTAMP round-trip truncates written digits (the same
            // fidelity rule as the scalar RootLiterals swap and the
            // mixed-element static spelling). Collections always; BARE
            // literals only off the scalar root (the population
            // receipt pins the written form on the value lanes; the
            // scalar Any-pair root keeps its TIMESTAMP carrier).
            SqlExpr lit = staticTemporalText(e,
                    lane != ValueLane.SCALAR_ROOT);
            return lit == null ? null
                    : new SqlExpr.Cast(lit, SqlType.Scalar.TEMPORAL_TEXT);
        }
        if (declared instanceof SqlType.Decimal) {
            // COLUMN-ROOTED cells only: the erasure is a STORE-READ
            // decode (R3) — COMPUTED decimals keep their arithmetic
            // scale (PCT testDecimalTimes pins 19.905D*17774D =
            // 353791.470, scale 3) and WRITTEN literals their written
            // scale. (Computed-over-column values erase in the
            // interpreted lane too — propagated erasure — but no
            // witness demands it at this egress yet; widen with one.)
            if (!columnRooted(e)) {
                return null;
            }
            SqlExpr text = new SqlExpr.Cast(e, SqlType.Scalar.VARCHAR);
            // strip FRACTIONAL trailing zeros only (a dotless spelling
            // has no wire scale to erase), then a bare trailing dot
            SqlExpr stripped = new SqlExpr.Case(List.of(
                    new SqlExpr.Case.When(
                            SqlExpr.Call.of(SqlFn.GREATER,
                                    SqlExpr.Call.of(SqlFn.STRPOS, text,
                                            new SqlExpr.StringLit(".")),
                                    new SqlExpr.IntLit(0)),
                            SqlExpr.Call.of(SqlFn.RTRIM,
                                    SqlExpr.Call.of(SqlFn.RTRIM, text,
                                            new SqlExpr.StringLit("0")),
                                    new SqlExpr.StringLit(".")))),
                    text);
            // NULL-preserving by hand: DuckDB concat treats NULL as ''
            // (a NULL cell must egress NULL, not a bare 'D')
            return new SqlExpr.Cast(new SqlExpr.Case(List.of(
                    new SqlExpr.Case.When(
                            SqlExpr.Call.of(SqlFn.IS_NULL, e),
                            new SqlExpr.NullLit())),
                    SqlExpr.Call.of(SqlFn.CONCAT, stripped,
                            new SqlExpr.StringLit("D"))),
                    SqlType.Scalar.DECIMAL_TEXT);
        }
        return null;
    }

    /** A column-rooted egress expression (possibly cast-wrapped): a
     * STORE-READ cell — the class the engine's ResultSet decode
     * applies to. */
    private static boolean columnRooted(SqlExpr e) {
        SqlExpr n = e;
        while (n instanceof SqlExpr.Cast c) {
            n = c.value();
        }
        return n instanceof SqlExpr.Column;
    }

    /** THE written-temporal static spelling (one owner — the typed-spec
     * arm {@code MixedEncoding#staticSubsecondSpelling} delegates its
     * text here): a subsecond-written temporal spells its compile-time
     * text because a TIMESTAMP round-trip truncates written digits past
     * the DB's micro storage. */
    static String writtenTemporalText(
            com.legend.values.PureDateLiteral d) {
        return d.toEngineString().replace(' ', 'T');
    }

    /** WRITTEN temporal literals at value egress — a bare
     * {@code TIMESTAMP '...'} or an
     * {@code UNNEST(list_filter([TIMESTAMP '...', ...], λ))} collection
     * — rebuilt with each literal's OWN TEXT (see
     * {@link #writtenTemporalText}). Null = not that shape. */
    private static @com.legend.base.Nullable SqlExpr staticTemporalText(SqlExpr e,
            boolean bareOk) {
        // NEGATIVE (BC) years stay on the TIMESTAMP path everywhere —
        // the executor's BC-safe fetch owns them; the engine-string
        // parse takes no leading minus (testAdjustByMinutesBigNumber)
        if (bareOk && e instanceof SqlExpr.TimestampLit ts
                && !ts.iso().startsWith("-")) {
            return new SqlExpr.StringLit(ts.iso().replace(' ', 'T'));
        }
        if (e instanceof SqlExpr.Call c
                && (c.fn() == SqlFn.UNNEST || c.fn() == SqlFn.LIST_FILTER)
                && !c.args().isEmpty()) {
            SqlExpr inner = staticTemporalText(c.args().get(0), false);
            if (inner == null) {
                return null;
            }
            List<SqlExpr> args = new java.util.ArrayList<>(c.args());
            args.set(0, inner);
            return new SqlExpr.Call(c.fn(), args);
        }
        if (e instanceof SqlExpr.CompactList cl) {
            SqlExpr inner = staticTemporalText(cl.list(), false);
            return inner == null ? null : new SqlExpr.CompactList(inner);
        }
        if (e instanceof SqlExpr.ArrayLit a && !a.elements().isEmpty()
                && a.elements().stream()
                        .allMatch(x -> x instanceof SqlExpr.TimestampLit t
                                && !t.iso().startsWith("-"))) {
            List<SqlExpr> out = new java.util.ArrayList<>();
            for (SqlExpr x : a.elements()) {
                out.add(new SqlExpr.StringLit(((SqlExpr.TimestampLit) x)
                        .iso().replace(' ', 'T')));
            }
            return new SqlExpr.ArrayLit(out);
        }
        return null;
    }

    /** Decimal: SCALE-PRESERVING (X2, VERDICT_RULE_AUDIT — engine
     * Decimal equality is getValue().equals, scale-sensitive; the old
     * scale-normalized canon followed the deleted compareTo grant).
     * CAST already preserves scale ('8.00'); only the wire's 'D'
     * representation suffix (variant/identity VARCHAR channel: 2D,
     * 1.0D) normalizes away. */
    static SqlExpr decimalCanon(SqlExpr v) {
        return SqlExpr.Call.of(SqlFn.REGEXP_REPLACE,
                new SqlExpr.Cast(v, SqlType.Scalar.VARCHAR),
                new SqlExpr.StringLit("[Dd]$"),
                new SqlExpr.StringLit(""));
    }

    /**
     * Float: fixed-point ALWAYS (H1 — pure never prints exponent
     * notation). DuckDB's CAST is shortest-repr but switches to
     * exponent for small/large magnitudes; those unfold through a
     * COMPLETE textual exponent unfold. Non-finite
     * spellings pass through — out of the claimed domain (§4), they
     * can never equal legitimate canonical text and the parallel host
     * referee names them residue.
     */
    /** Rule 2's conversion for a Float-DECLARED cell (docs/JUDGING_TWO_MODES
     * §1): CAST AS DOUBLE unless the tree KNOWS the value is already a
     * DOUBLE or rides a text carrier (VARCHAR / temporal text / decimal
     * text / literal / JSON — a cast there would error the query). An
     * UNKNOWN fact converts: the declared kind is the authority, never
     * the tree's knowledge of the wire (the census §9 lesson — on H2 no
     * fact is known and nothing converted: `35.50000000000`, JSON `68`). */
    static SqlExpr declaredDouble(SqlExpr v) {
        // a wire the platform's typing already knows is a DOUBLE needs no
        // cast; the facts are the platform's and every dialect DELIVERS
        // them (H2 makes its DECFLOAT avg a DOUBLE itself: H2AvgDelivers)
        if (v.type() instanceof com.legend.sql.TypeFact.Typed t
                && (t.type() == SqlType.Scalar.DOUBLE
                        || t.type() == SqlType.Scalar.VARCHAR
                        || t.type() == SqlType.Scalar.TEMPORAL_TEXT
                        || t.type() == SqlType.Scalar.DECIMAL_TEXT
                        || t.type() == SqlType.Scalar.LITERAL
                        || t.type() == SqlType.Scalar.JSON)) {
            return v;
        }
        return new SqlExpr.Cast(v, SqlType.Scalar.DOUBLE);
    }

    static SqlExpr floatCanon(SqlExpr v) {
        SqlExpr base = new SqlExpr.Cast(v, SqlType.Scalar.VARCHAR);
        SqlExpr unfolded = exponentUnfold(base);
        return new SqlExpr.Case(List.of(
                // ZEROS UNIFY (spec §3, witness parseFloat('-000.000')):
                // pure grants 0.0 == -0.0, so the canonical render of
                // every zero is '0.0'. Detected TEXTUALLY (F10 slice 1):
                // the old v = 0.0 compare forced SQL to cast the COLUMN
                // to DOUBLE, which errored the whole wrapped query on
                // print-form identity carriers ('7.345D') — the canon
                // must be TOTAL over any column it can meet. For genuine
                // DOUBLE columns the zero texts are exactly 0.0/-0.0,
                // so the regex is equivalence, not leniency.
                new SqlExpr.Case.When(
                        SqlExpr.Call.of(SqlFn.REGEXP_FULL_MATCH, base,
                                new SqlExpr.StringLit("-?0+(\\.0+)?")),
                        new SqlExpr.StringLit("0.0")),
                // both wire spellings of the exponent: DuckDB's 1.3421e-08,
                // H2's 1.3421E-8 / 1.0E7 (a DOUBLE prints as Java does)
                new SqlExpr.Case.When(SqlExpr.Call.of(SqlFn.OR, has(base, "e"), has(base, "E")),
                        unfolded),
                new SqlExpr.Case.When(SqlExpr.Call.of(SqlFn.NOT,
                        has(base, ".")),
                        SqlExpr.Call.of(SqlFn.CONCAT, base,
                                new SqlExpr.StringLit(".0")))),
                base);
    }

    /**
     * Temporal (DateTime/Date stamps): the scalar-channel form —
     * {@code T}-separated, trailing subsecond zeros stripped,
     * {@code +0000} on time-bearing values only. Handles BOTH wire
     * spellings (a TIMESTAMP cell's cast and the precision-faithful
     * VARCHAR convention) through one text pipeline.
     */
    static SqlExpr temporalCanon(SqlExpr v) {
        // an ALREADY-SUFFIXED wire text (the variant-identity channel
        // prints pure's +0000 form) normalizes before the pipeline —
        // the suffix re-appends canonically at the end.
        // SUBSECOND PRECISION IS PRESERVED AS WRITTEN (A1, spec §3:
        // .000 != .0 != none are DISTINCT pure values — AbstractPureDate
        // compares the exact subsecond STRING). The old trailing-zero
        // strip was a NO-OP on TIMESTAMP casts (DuckDB already prints
        // minimal subseconds — probed 2026-08-23) and WRONG on the
        // precision-faithful VARCHAR convention, where the text is
        // authoritative.
        SqlExpr bare = SqlExpr.Call.of(SqlFn.REGEXP_REPLACE,
                new SqlExpr.Cast(v, SqlType.Scalar.VARCHAR),
                new SqlExpr.StringLit("(\\+0000|Z)$"),
                new SqlExpr.StringLit(""));
        SqlExpr t = SqlExpr.Call.of(SqlFn.REPLACE, bare,
                new SqlExpr.StringLit(" "), new SqlExpr.StringLit("T"));
        SqlExpr timeBearing = has(t, "T");
        return new SqlExpr.Case(List.of(new SqlExpr.Case.When(timeBearing,
                SqlExpr.Call.of(SqlFn.CONCAT, t,
                        new SqlExpr.StringLit("+0000")))),
                t);
    }

    /**
     * COMPLETE textual exponent unfold (V10c — replaces the bounded
     * DECIMAL(38,18) cast, which silently zeroed values beyond its
     * envelope): the shortest-repr mantissa digits shift by the
     * exponent as TEXT, so any finite double prints fixed-point
     * exactly — {@code 1.3421e-08 → 0.000000013421},
     * {@code 1e+300 → 1000…000.0}. Pure never prints exponent
     * notation (H1); now neither can we, for any magnitude.
     */
    private static SqlExpr exponentUnfold(SqlExpr base) {
        SqlExpr sign = new SqlExpr.Case(List.of(new SqlExpr.Case.When(
                SqlExpr.Call.of(SqlFn.STARTS_WITH, base,
                        new SqlExpr.StringLit("-")),
                new SqlExpr.StringLit("-"))),
                new SqlExpr.StringLit(""));
        // mantissa without sign, e.g. '1.3421'; its digits '13421';
        // intLen = digits before the dot; exp as an integer
        SqlExpr mant = SqlExpr.Call.of(SqlFn.REGEXP_EXTRACT, base,
                new SqlExpr.StringLit("-?([0-9]+(?:\\.[0-9]+)?)[eE]"),
                new SqlExpr.IntLit(1));
        SqlExpr digits = SqlExpr.Call.of(SqlFn.REPLACE, mant,
                new SqlExpr.StringLit("."), new SqlExpr.StringLit(""));
        SqlExpr dotPos = SqlExpr.Call.of(SqlFn.STRPOS, mant,
                new SqlExpr.StringLit("."));
        SqlExpr intLen = new SqlExpr.Case(List.of(new SqlExpr.Case.When(
                SqlExpr.Call.of(SqlFn.EQUAL, dotPos, new SqlExpr.IntLit(0)),
                SqlExpr.Call.of(SqlFn.LENGTH, mant))),
                SqlExpr.Call.of(SqlFn.MINUS, dotPos, new SqlExpr.IntLit(1)));
        // the exponent digits, NULL when there are none: H2 folds a constant
        // operand at prepare time BRANCH-BLIND (a literal golden cell), so
        // CAST('' AS INTEGER) would raise under the untaken CASE arm
        SqlExpr expText = SqlExpr.Call.of(SqlFn.REGEXP_EXTRACT,
                base, new SqlExpr.StringLit("[eE]([+-]?[0-9]+)$"),
                new SqlExpr.IntLit(1));
        SqlExpr exp = new SqlExpr.Cast(new SqlExpr.Case(List.of(new SqlExpr.Case.When(
                SqlExpr.Call.of(SqlFn.EQUAL, expText, new SqlExpr.StringLit("")),
                new SqlExpr.NullLit())), expText), SqlType.Scalar.INTEGER);
        SqlExpr pointPos = SqlExpr.Call.of(SqlFn.PLUS, intLen, exp);
        SqlExpr dLen = SqlExpr.Call.of(SqlFn.LENGTH, digits);
        // three shapes by where the point lands
        SqlExpr tiny = SqlExpr.Call.of(SqlFn.CONCAT,
                SqlExpr.Call.of(SqlFn.CONCAT, new SqlExpr.StringLit("0."),
                        zeros(SqlExpr.Call.of(SqlFn.MINUS,
                                new SqlExpr.IntLit(0), pointPos))),
                digits);
        SqlExpr huge = SqlExpr.Call.of(SqlFn.CONCAT,
                SqlExpr.Call.of(SqlFn.CONCAT, digits,
                        zeros(SqlExpr.Call.of(SqlFn.MINUS, pointPos, dLen))),
                new SqlExpr.StringLit(".0"));
        SqlExpr mid = SqlExpr.Call.of(SqlFn.CONCAT,
                SqlExpr.Call.of(SqlFn.CONCAT,
                        SqlExpr.Call.of(SqlFn.SUBSTRING, digits,
                                new SqlExpr.IntLit(1), pointPos),
                        new SqlExpr.StringLit(".")),
                SqlExpr.Call.of(SqlFn.SUBSTRING, digits,
                        SqlExpr.Call.of(SqlFn.PLUS, pointPos,
                                new SqlExpr.IntLit(1))));
        SqlExpr body = new SqlExpr.Case(List.of(
                new SqlExpr.Case.When(SqlExpr.Call.of(SqlFn.LESS_EQUAL,
                        pointPos, new SqlExpr.IntLit(0)), tiny),
                new SqlExpr.Case.When(SqlExpr.Call.of(SqlFn.GREATER_EQUAL,
                        pointPos, dLen), huge)),
                mid);
        return SqlExpr.Call.of(SqlFn.CONCAT, sign, body);
    }

    /** {@code n} zeros (RPAD over empty; negative n yields ''). */
    private static SqlExpr zeros(SqlExpr n) {
        // rpad's length parameter binds INTEGER, not BIGINT
        return SqlExpr.Call.of(SqlFn.RPAD, new SqlExpr.StringLit(""),
                new SqlExpr.Cast(
                        SqlExpr.Call.of(SqlFn.GREATEST, n,
                                new SqlExpr.IntLit(0)),
                        SqlType.Scalar.INTEGER),
                new SqlExpr.StringLit("0"));
    }

    private static SqlExpr has(SqlExpr text, String needle) {
        return SqlExpr.Call.of(SqlFn.GREATER,
                SqlExpr.Call.of(SqlFn.STRPOS, text,
                        new SqlExpr.StringLit(needle)),
                new SqlExpr.IntLit(0));
    }

    /** F10 slice 2 — the MIXED-NUMERIC carrier encoder: a literal
     * collection whose elements are ≥2 DISTINCT numeric kinds rebuilds
     * as an array of pure-literal spellings (each element carries its
     * own kind; the DOUBLE promotion that erased Integer 1 into 1.0
     * dies here). Null = not this shape (homogeneous, non-numeric,
     * non-literal) — the caller keeps its lane. */
    static @com.legend.base.Nullable SqlExpr mixedNumericArray(
            com.legend.compiler.spec.typed.TypedSpec spec, SqlExpr lowered) {
        if (!(spec instanceof com.legend.compiler.spec.typed.TypedCollection c)
                || !(lowered instanceof SqlExpr.ArrayLit la)
                || c.elements().size() < 2
                || la.elements().size() != c.elements().size()) {
            return null;
        }
        java.util.List<SqlExpr> spelled =
                new java.util.ArrayList<>(la.elements().size());
        java.util.Set<Type> kinds = new java.util.HashSet<>();
        for (int i = 0; i < c.elements().size(); i++) {
            Type t = c.elements().get(i).info().type();
            Type kind = t instanceof Type.PrecisionDecimal
                    ? Type.Primitive.DECIMAL : t;
            if (kind != Type.Primitive.INTEGER && kind != Type.Primitive.FLOAT
                    && kind != Type.Primitive.DECIMAL) {
                return null;
            }
            kinds.add(kind);
            SqlExpr lit = literal(la.elements().get(i), kind);
            if (lit == null) {
                return null;
            }
            spelled.add(lit);
        }
        if (kinds.size() < 2) {
            return null;
        }
        // the construction-site MARK (the sort arm's idiom): the tree
        // CARRIES the spelling contract — Array(LITERAL), physically
        // VARCHAR[], an identity cast; list-less backends strip it at
        // render. Without it the label knew what the tree did not (the
        // 2x LITERAL<>VARCHAR census rows — flip adjudication).
        return new SqlExpr.Cast(new SqlExpr.ArrayLit(spelled),
                new com.legend.sql.SqlType.Array(
                        com.legend.sql.SqlType.Scalar.LITERAL));
    }

    /** The spelling->PRINT projection of a LITERAL-labeled text, in SQL
     * (the burn-down doctrine — a TRANSFORM, never an inversion to
     * raw): quoted strings unescape and unquote, %-temporals strip the
     * mark, every other kind's print IS its spelling (bare ints,
     * pointed floats, D-decimals, bools). First-byte dispatch is
     * grammar-driven on a LABELED wire — the six spellings are
     * first-byte disjoint BY DESIGN (the banned version ran on
     * unlabeled text). ONE recipe for every print consumer:
     * pureToString's Any arm, format's spelled-argument slots, and the
     * makeString join family (M4 §2R — the three residual-row
     * witnesses). */
    static SqlExpr printForm(SqlExpr x) {
        SqlExpr txt = new SqlExpr.Cast(x,
                PureSql.type(Type.Primitive.STRING));
        SqlExpr body = SqlExpr.Call.of(SqlFn.SUBSTRING, txt,
                new SqlExpr.IntLit(2),
                SqlExpr.Call.of(SqlFn.MINUS,
                        SqlExpr.Call.of(SqlFn.LENGTH, txt),
                        new SqlExpr.IntLit(2)));
        SqlExpr unesc = SqlExpr.Call.of(SqlFn.REPLACE,
                SqlExpr.Call.of(SqlFn.REPLACE, body,
                        new SqlExpr.StringLit("\\'"),
                        new SqlExpr.StringLit("'")),
                new SqlExpr.StringLit("\\\\"),
                new SqlExpr.StringLit("\\"));
        return new SqlExpr.Case(List.of(
                new SqlExpr.Case.When(
                        SqlExpr.Call.of(SqlFn.STARTS_WITH, txt,
                                new SqlExpr.StringLit("'")),
                        unesc),
                new SqlExpr.Case.When(
                        SqlExpr.Call.of(SqlFn.STARTS_WITH, txt,
                                new SqlExpr.StringLit("%")),
                        SqlExpr.Call.of(SqlFn.SUBSTRING, txt,
                                new SqlExpr.IntLit(2)))),
                txt);
    }

    // unspell + unspellMarked (the STRUCTURAL INVERSE pair) DELETED
    // (spell-debt burn-down 2026-08-24): their three consumers (the
    // equality unspell-one-side arm, format's decomposition, the
    // Any-conformance re-wrap) were compensation for spellings reaching
    // mid-expression consumers — the probe and the full chain showed no
    // live flow does; when the parked hetero claim lands, consumers
    // conform BY EMISSION (spell the static side / transform spelling to
    // print form / keep the label through casts), never by inverting.

    // ==================================================================
    // PRINT forms (execution wire; pure toString spellings)
    // ==================================================================

    /**
     * Pure prints a Float via its MINIMAL decimal repr: DuckDB's shortest
     * round-trip VARCHAR cast already matches ('1.5', '2.0') EXCEPT where it
     * chooses exponent notation — those re-render plain through a
     * DECIMAL(38,18) cast with trailing zeros trimmed (and a bare trailing
     * dot restored to '.0'). Magnitudes outside DECIMAL(38,18) keep the
     * exponent form.
     */
    public static SqlExpr floatPrint(SqlExpr x) {
        SqlExpr s = new SqlExpr.Cast(x, SqlType.Scalar.VARCHAR);
        // FRACTION-FREE values render through HUGEINT — exact plain digits
        // for the whole [1e16, 1e38) band where the DECIMAL(38,18) cast
        // fabricates garbage (audit: 1e18 printed ...042.42...); every
        // double >= 2^53 is fraction-free, so all large magnitudes take
        // this branch.
        SqlExpr intPlain = SqlExpr.Call.of(SqlFn.CONCAT,
                new SqlExpr.Cast(new SqlExpr.Cast(x, SqlType.Scalar.HUGEINT),
                        SqlType.Scalar.VARCHAR),
                new SqlExpr.StringLit(".0"));
        SqlExpr plain = SqlExpr.Call.of(SqlFn.RTRIM,
                new SqlExpr.Cast(new SqlExpr.Cast(x, new SqlType.Decimal(38, 18)),
                        SqlType.Scalar.VARCHAR),
                new SqlExpr.StringLit("0"));
        SqlExpr fixed = new SqlExpr.Case(List.of(new SqlExpr.Case.When(
                SqlExpr.Call.of(SqlFn.ENDS_WITH, plain, new SqlExpr.StringLit(".")),
                SqlExpr.Call.of(SqlFn.CONCAT, plain, new SqlExpr.StringLit("0")))),
                plain);
        SqlExpr hasExp = SqlExpr.Call.of(SqlFn.GREATER,
                SqlExpr.Call.of(SqlFn.STRPOS, s, new SqlExpr.StringLit("e")),
                new SqlExpr.IntLit(0));
        SqlExpr fractionFree = SqlExpr.Call.of(SqlFn.AND,
                SqlExpr.Call.of(SqlFn.EQUAL, x, SqlExpr.Call.of(SqlFn.FLOOR_RAW, x)),
                SqlExpr.Call.of(SqlFn.LESS,
                        SqlExpr.Call.of(SqlFn.ABS, x), new SqlExpr.FloatLit(1e38)));
        // The DECIMAL path stays only where the scale-18 cast is exact for
        // short-decimal values: fractional magnitudes in [1e-17, 2^53)
        // (below 1e-17 the scale rounds — 1.5e-18 gained a digit; audit).
        SqlExpr inRange = SqlExpr.Call.of(SqlFn.AND,
                SqlExpr.Call.of(SqlFn.GREATER_EQUAL,
                        SqlExpr.Call.of(SqlFn.ABS, x), new SqlExpr.FloatLit(1e-17)),
                SqlExpr.Call.of(SqlFn.LESS,
                        SqlExpr.Call.of(SqlFn.ABS, x), new SqlExpr.FloatLit(9.007199254740992e15)));
        return new SqlExpr.Case(List.of(
                new SqlExpr.Case.When(
                        SqlExpr.Call.of(SqlFn.AND, hasExp, fractionFree), intPlain),
                new SqlExpr.Case.When(
                        SqlExpr.Call.of(SqlFn.AND, hasExp, inRange), fixed)), s);
    }

    /** Decimal PRINT form: the value's cast text with the {@code D}
     * suffix (the identity-channel wire spelling the canon strips). */
    public static SqlExpr decimalPrintD(SqlExpr x) {
        return SqlExpr.Call.of(SqlFn.CONCAT,
                new SqlExpr.Cast(x, SqlType.Scalar.VARCHAR),
                new SqlExpr.StringLit("D"));
    }

    /** StrictDate LITERAL from a typed DATE value: % + ISO date. */
    /** The full ISO timestamp text of a TIME-BEARING pure date literal
     * (hour/minute precisions pad to the SQL timestamp shape); null for
     * date-only and partial literals. */
    public static @com.legend.base.Nullable String isoTimestamp(
            com.legend.values.PureDateLiteral d) {
        return switch (d) {
            case com.legend.values.PureDateLiteral.DateWithHour h -> h.toEngineString() + ":00:00";
            case com.legend.values.PureDateLiteral.DateWithMinute mi -> mi.toEngineString() + ":00";
            case com.legend.values.PureDateLiteral.DateWithSecond se -> se.toEngineString();
            case com.legend.values.PureDateLiteral.DateWithSubsecond su -> su.toEngineString();
            default -> null;
        };
    }

    /** A UTC ISO timestamp text re-spelled at the same INSTANT in
     * {@code zone} (the engine's dbTimeZone literal rule — the shape,
     * with its sub-second digits, is kept; batch 86). */
    public static String inZone(String utcIso, String zone) {
        java.time.LocalDateTime ldt = java.time.LocalDateTime.parse(utcIso);
        // the engine's connection zones include the legacy SHORT ids ('EST',
        // the corpus's own test connection) — the same table the engine-text
        // renderer resolves them through (EngineStyleH2)
        java.time.LocalDateTime shifted = ldt.atZone(java.time.ZoneOffset.UTC)
                .withZoneSameInstant(java.time.ZoneId.of(zone, java.time.ZoneId.SHORT_IDS))
                .toLocalDateTime();
        String out = shifted.toString();
        return out.length() < utcIso.length()
                ? out + utcIso.substring(out.length()) : out;   // keep :00 / .SSS shape
    }

    public static SqlExpr strictDateLiteral(SqlExpr x) {
        return SqlExpr.Call.of(SqlFn.CONCAT,
                new SqlExpr.StringLit("%"), datePrint(x));
    }

    /** DateTime LITERAL at the element's STATIC subsecond precision
     * (the caller resolves the format): % + T-separated print + +0000.
     * Precision-faithful by construction — pairs with the A1 fix
     * (temporalCanon no longer strips written subseconds). */
    public static SqlExpr dateTimeLiteral(SqlExpr x, SqlExpr.FormatLit fmt) {
        return SqlExpr.Call.of(SqlFn.CONCAT,
                new SqlExpr.StringLit("%"), dateTimePrint(x, fmt));
    }

    /** PARTIAL-date LITERAL: the string cell IS the body (master's
     * pinned partial-date carrier); % prefixes it. */
    public static SqlExpr partialDateLiteral(SqlExpr text) {
        return SqlExpr.Call.of(SqlFn.CONCAT,
                new SqlExpr.StringLit("%"), text);
    }

    /** StrictDate PRINT form: bare ISO date (no % — print, not literal). */
    public static SqlExpr datePrint(SqlExpr x) {
        return SqlExpr.Call.of(SqlFn.STRFTIME, x,
                new SqlExpr.FormatLit(com.legend.sql.DateFmt.DATE));
    }

    /** DateTime PRINT form: strftime at the literal's own subsecond
     * precision (a STATIC attribute the caller resolves) with pure's
     * {@code +0000} suffix. */
    public static SqlExpr dateTimePrint(SqlExpr x, SqlExpr.FormatLit fmt) {
        return SqlExpr.Call.of(SqlFn.CONCAT,
                SqlExpr.Call.of(SqlFn.STRFTIME, x, fmt),
                new SqlExpr.StringLit("+0000"));
    }
}
