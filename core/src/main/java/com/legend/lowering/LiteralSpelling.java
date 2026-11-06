// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.lowering;

import com.legend.compiler.element.type.Type;
import com.legend.sql.SqlAgg;
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
                                        new SqlExpr.TypeNameLit(SqlType.Scalar.DATE)),
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
        // them (H2 makes its DECFLOAT avg a DOUBLE itself: AggregatesDeliverDouble)
        if (v.type() instanceof com.legend.sql.TypeFact.Typed t
                && (t.type() == SqlType.Scalar.DOUBLE
                        || t.type() == SqlType.Scalar.VARCHAR
                        || t.type() == SqlType.Scalar.TEMPORAL_TEXT
                        || t.type() == SqlType.Scalar.DECIMAL_TEXT
                        || t.type() == SqlType.Scalar.JSON)) {
            return v;
        }
        return new SqlExpr.Cast(v, SqlType.Scalar.DOUBLE);
    }

    /** Pure's literal spelling of a Float: one call, {@link SqlFn#FLOAT_CANON}, which each dialect spells (2026-10-11:
     *  the portable spelling reads the value's text some fifty times, which multiplied a dialect whose double text is
     *  itself a long expression -- MariaDB's five-element mixed list was 545 KB of SQL). */
    static SqlExpr floatCanon(SqlExpr v) {
        return SqlExpr.Call.of(SqlFn.FLOAT_CANON, v);
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
        if (v.type() instanceof com.legend.sql.TypeFact.Typed tt
                && (tt.type() == SqlType.Scalar.TIMESTAMP || tt.type() == SqlType.Scalar.TIMESTAMPTZ)) {
            // a time-bearing timestamp carries no written precision: its text is the one owner's (dateTimeText: the
            // fraction's trailing zeros trimmed), never the database's own cast, whose digits are the column's
            // (ClickHouse prints a DateTime64(6) with six, a DateTime with none; DuckDB the minimal ones: channel B's
            // date tests on ClickHouse, 2026-10-20). A DATE cell under a timestamp's label (the store declares one
            // where the DDL made the other) is its day, as the cast spells it (the corpus's columnValueDifferenceTest
            // and rowValueDifferenceTest, measured the same day). NULL stays NULL.
            return new SqlExpr.Case(List.of(
                    new SqlExpr.Case.When(SqlExpr.Call.of(SqlFn.IS_NULL, v), new SqlExpr.NullLit()),
                    new SqlExpr.Case.When(timeBearing, dateTimeText(v))), t);
        }
        return new SqlExpr.Case(List.of(new SqlExpr.Case.When(timeBearing,
                SqlExpr.Call.of(SqlFn.CONCAT, t,
                        new SqlExpr.StringLit("+0000")))),
                t);
    }



    private static SqlExpr has(SqlExpr text, String needle) {
        return SqlExpr.Call.of(SqlFn.GREATER,
                SqlExpr.Call.of(SqlFn.STRPOS, text,
                        new SqlExpr.StringLit(needle)),
                new SqlExpr.IntLit(0));
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
     * Pure prints a Float as its {@code toString} does: one call, {@link SqlFn#FLOAT_PRINT}, which each dialect spells
     * (2026-10-22: the portable spelling, dialect/FloatPrintText, reads the value's text some nine times, which
     * multiplied a dialect whose double text is itself a long expression -- Snowflake's PCT statement for
     * {@code 1.5 + 2.25} was 48 KB).
     */
    public static SqlExpr floatPrint(SqlExpr x) {
        // A Float the STORE holds as an integer -- a physical column the store declares integral under a Float
        // property (legend-engine's test tradeTable.quantity INT under the validation showcase's Float) -- prints the
        // integer the database holds, as legend-engine's cast of the column does ('Quantity not in range: 1000000',
        // measured on the corpus 2026-10-18, step 6 of the plan path program). The store's fact, read off the scan's
        // own column (origin PHYSICAL), never inferred; a computed Float, and a projected one read back through a
        // frame, keep Pure's text (the output layer's step 5: a whole Float's .0 on every database).
        if (x instanceof SqlExpr.Column c && c.type() instanceof com.legend.sql.TypeFact.Typed t
                && (c.origin() == com.legend.sql.OutputCol.Origin.PHYSICAL
                        || c.origin() == com.legend.sql.OutputCol.Origin.PHYSICAL_QUOTED)
                && isIntegral(t.type())) {
            return new SqlExpr.Cast(x, SqlType.Scalar.VARCHAR);
        }
        return SqlExpr.Call.of(SqlFn.FLOAT_PRINT, x);
    }

    /** Whether {@code t} is one of the integral SQL kinds. */
    private static boolean isIntegral(SqlType t) {
        return t == SqlType.Scalar.INTEGER || t == SqlType.Scalar.BIGINT || t == SqlType.Scalar.HUGEINT;
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

    /** A DateTime's literal spelling at exactly {@code digits} subsecond digits (a tagged slot's kind
     *  {@code DateTime(n)}, track L): the nine-digit text cut to the written digits, Pure's {@code +0000}; a whole
     *  second's at none; NULL stays NULL. */
    public static SqlExpr dateTimeLiteralAt(SqlExpr x, int digits) {
        SqlExpr body = digits == 0
                ? SqlExpr.Call.of(SqlFn.STRFTIME, x, new SqlExpr.FormatLit(com.legend.sql.DateFmt.ISO_LOCAL))
                : SqlExpr.Call.of(SqlFn.LEFT,
                        SqlExpr.Call.of(SqlFn.STRFTIME, x, new SqlExpr.FormatLit(com.legend.sql.DateFmt.ISO_NANO)),
                        new SqlExpr.IntLit(20 + digits));
        return new SqlExpr.Case(List.of(new SqlExpr.Case.When(SqlExpr.Call.of(SqlFn.IS_NULL, x),
                new SqlExpr.NullLit())),
                SqlExpr.Call.of(SqlFn.CONCAT, new SqlExpr.StringLit("%"), body, new SqlExpr.StringLit("+0000")));
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

    /**
     * THE DateTime's text (docs/OUTPUT_LAYER_DESIGN_2026_10_10.md §4, the one owner, 2026-10-18): Pure's print form
     * of a DateTime read from a database -- {@code T}, the fraction's digits with their trailing zeros trimmed (a
     * timestamp carries no written precision: {@code .123} prints {@code .123}, {@code .123456789} all nine, a whole
     * second none), {@code +0000}. Every output of a timestamp slot writes this (the CSV, the JSON wires, Pure's TDS
     * text, {@code toString}); a literal's own digits are {@link #dateTimePrint}'s, spelled statically where the
     * typer knows them. The fraction is read at nine digits, which every SQL dialect spells.
     */
    public static SqlExpr dateTimeText(SqlExpr x) {
        SqlExpr digits = SqlExpr.Call.of(SqlFn.RTRIM,
                SqlExpr.Call.of(SqlFn.STRFTIME, x,
                        new SqlExpr.FormatLit(List.of(com.legend.sql.DateFmt.Part.SUBSEC_NANO))),
                new SqlExpr.StringLit("0"));
        SqlExpr fraction = new SqlExpr.Case(List.of(new SqlExpr.Case.When(
                SqlExpr.Call.of(SqlFn.EQUAL, digits, new SqlExpr.StringLit("")), new SqlExpr.StringLit(""))),
                SqlExpr.Call.of(SqlFn.CONCAT, new SqlExpr.StringLit("."), digits));
        return SqlExpr.Call.of(SqlFn.CONCAT,
                SqlExpr.Call.of(SqlFn.CONCAT,
                        SqlExpr.Call.of(SqlFn.STRFTIME, x,
                                new SqlExpr.FormatLit(com.legend.sql.DateFmt.ISO_LOCAL)),
                        fraction),
                new SqlExpr.StringLit("+0000"));
    }

    /**
     * A DateTime cell's text in Pure's TDS text (a relation's {@code toString}): as
     * {@link #dateTimeText}, with AT LEAST THREE fraction digits -- {@code 2024-01-02T10:30:00.000+0000} for a whole
     * second, {@code .123} for a millisecond, {@code .123456789} for a nanosecond, finer trailing zeros trimmed. The
     * reference is legend-engine's own test (core_functions_standard/date/extract/dayOfWeek.pure,
     * {@code testDayOfWeek_Relation}): a TDS literal's {@code 1985-01-12T06:30:00Z} prints
     * {@code 1985-01-12T06:30:00.000+0000} in the relation's text, where the value's own text (interpreted Pure's
     * {@code toString} of {@code %1985-01-12T06:30:00}) has no fraction. Measured 2026-10-18 (the plan path
     * program's step 6, channel B's relation and standard suites: 35 goldens spell the three digits), correcting
     * step 2's one trimmed text for every cell.
     */
    public static SqlExpr dateTimeTextTds(SqlExpr x) {
        SqlExpr nine = SqlExpr.Call.of(SqlFn.STRFTIME, x,
                new SqlExpr.FormatLit(List.of(com.legend.sql.DateFmt.Part.SUBSEC_NANO)));
        SqlExpr three = SqlExpr.Call.of(SqlFn.SUBSTRING, nine, new SqlExpr.IntLit(1), new SqlExpr.IntLit(3));
        SqlExpr finer = SqlExpr.Call.of(SqlFn.RTRIM,
                SqlExpr.Call.of(SqlFn.SUBSTRING, nine, new SqlExpr.IntLit(4), new SqlExpr.IntLit(6)),
                new SqlExpr.StringLit("0"));
        return SqlExpr.Call.of(SqlFn.CONCAT,
                SqlExpr.Call.of(SqlFn.CONCAT,
                        SqlExpr.Call.of(SqlFn.STRFTIME, x,
                                new SqlExpr.FormatLit(com.legend.sql.DateFmt.ISO_LOCAL)),
                        SqlExpr.Call.of(SqlFn.CONCAT, SqlExpr.Call.of(SqlFn.CONCAT, new SqlExpr.StringLit("."), three),
                                finer)),
                new SqlExpr.StringLit("+0000"));
    }

    /** A date value carried as its own text (a value root spelled at the precision it has: a year, a month, a day,
     *  an hour, a minute, a second or finer; with or without Pure's {@code %}; a literal's print form with its
     *  {@code +0000} already): one with seconds and no zone takes Pure's {@code +0000}, as Pure prints a DateTime;
     *  every other precision is its text, Pure's DateTime literal taking a zone only after seconds (the MongoDB
     *  lane, 2026-10-18: {@code %1973-11-13T23} and {@code %2025-02-10T20:10:20+0000} are read as written). */
    // the zone a written date-time text gets in SQL (register S121, pending: legend-pure writes +0000 from the minute)
    public static SqlExpr dateTimeTextOfText(SqlExpr text) {
        SqlExpr colons = SqlExpr.Call.of(SqlFn.MINUS, SqlExpr.Call.of(SqlFn.LENGTH, text),
                SqlExpr.Call.of(SqlFn.LENGTH, SqlExpr.Call.of(SqlFn.REPLACE, text, new SqlExpr.StringLit(":"),
                        new SqlExpr.StringLit(""))));
        SqlExpr secondsNoZone = SqlExpr.Call.of(SqlFn.AND,
                SqlExpr.Call.of(SqlFn.GREATER_EQUAL, colons, new SqlExpr.IntLit(2)),
                SqlExpr.Call.of(SqlFn.NOT, SqlExpr.Call.of(SqlFn.ENDS_WITH, text, new SqlExpr.StringLit("+0000"))));
        return new SqlExpr.Case(List.of(new SqlExpr.Case.When(secondsNoZone,
                SqlExpr.Call.of(SqlFn.CONCAT, text, new SqlExpr.StringLit("+0000")))), text);
    }

    /** Whether a timestamp's time of day is not midnight: the engine's {@code formatDateTime} rule for an ABSTRACT
     *  Date, defined over the value (hasHour) -- a timed value is a DateTime, a date-only one its day. */
    public static SqlExpr timed(SqlExpr c) {
        return SqlExpr.Call.of(SqlFn.NOT_EQUAL,
                SqlExpr.Call.of(SqlFn.STRFTIME, c, new SqlExpr.FormatLit(com.legend.sql.DateFmt.TIME_ONLY)),
                new SqlExpr.StringLit("00:00:00"));
    }
}
