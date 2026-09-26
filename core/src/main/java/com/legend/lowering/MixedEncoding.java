package com.legend.lowering;

import com.legend.compiler.element.type.PlatformTypes;
import com.legend.compiler.element.type.Type;
import com.legend.compiler.spec.typed.TypedCDate;
import com.legend.compiler.spec.typed.TypedCollection;
import com.legend.compiler.spec.typed.TypedSpec;
import com.legend.sql.SqlExpr;
import com.legend.sql.SqlFn;
import com.legend.sql.SqlType;
import com.legend.values.PureDateLiteral;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * The MIXED-ELEMENT two-channel encoding — DATABASE-EXECUTED — extracted
 * from {@link Scalars} (CodeShapeGuardrail file split, stamp C1 slice).
 * Elements of DIFFERENT concrete kinds under the Number/Date LUB split
 * into an IDENTITY channel (pure print form, computed BY SQL) and a
 * COMPARABLE channel (CAST AS DOUBLE / strptime-padded TIMESTAMP);
 * selections order by the comparable, return the identity. TENET:
 * encodings chosen by STATIC type; every value computation runs in the
 * database (elements may be arbitrary expressions).
 */
final class MixedEncoding {

    private MixedEncoding() {
    }

    /** Null when not per-element encodable or not mixed. */
    record MixedElems(List<SqlExpr> ids, List<SqlExpr> vals) {

        SqlExpr idList() {
            return new SqlExpr.ArrayLit(ids);
        }

        SqlExpr valList() {
            return new SqlExpr.ArrayLit(vals);
        }

        /** {@code ids[list_position(vals, <winner>)]} — the selection recipe. */
        SqlExpr select(SqlExpr winner) {
            SqlExpr sel = SqlExpr.Call.of(SqlFn.LIST_GET, idList(),
                    SqlExpr.Call.of(SqlFn.LIST_POSITION,
                            valList(), winner));
            // F10 slice 2b: a NUMERIC mix's selected identity IS a
            // pure-literal spelling — the Cast is the construction-site
            // LITERAL label (scalarRoot reads it; physically CAST AS
            // VARCHAR over text, an identity). Date/string mixes keep
            // the unmarked print carrier until slice 3 lands the
            // temporal arm on both grammar halves.
            return markLiteral(sel);
        }

        /** The construction-site LITERAL label for any composition
         * that returns ONE identity text (select, mode's last-run
         * pick). Slice 3: EVERY element arm now spells the pure
         * literal (temporals joined via the %-forms), so the mark is
         * unconditional — the 2b numericOnly gate dissolved. */
        SqlExpr markLiteral(SqlExpr sel) {
            return new SqlExpr.Cast(sel, SqlType.Scalar.LITERAL);
        }
    }

    /** Identity consumers of the NUMBER-LUB literal carrier rebuild
     * element identity from the TYPED elements (encodeMixed), never from
     * the carrier (the numeric UNWRAP is {@link Numerics#numList}).
     * Non-carrier shapes pass through untouched. */
    static @com.legend.base.Nullable MixedElems mixedElems(TypedSpec arg,
                                 SqlExpr lowered) {
        if (!(arg instanceof TypedCollection c)
                || c.elements().size() < 2
                || !(lowered instanceof SqlExpr.ArrayLit la)
                || la.elements().size() != c.elements().size()) {
            return null;
        }
        Type lub = c.info().type();
        if (lub != Type.Primitive.NUMBER && lub != Type.Primitive.DATE) {
            return null;   // uniform-kind collections keep their native carrier
        }
        return encodeAll(c.elements(), la.elements());
    }

    /** The n-ary form: max(2D, 1.23) — each ARG one element. */
    static @com.legend.base.Nullable MixedElems mixedArgs(List<TypedSpec> args,
                                List<SqlExpr> lowered) {
        Set<Type> kinds = new HashSet<>();
        for (var a : args) {
            kinds.add(a.info().type());
        }
        return kinds.size() > 1 ? encodeAll(args, lowered) : null;
    }

    private static @com.legend.base.Nullable MixedElems encodeAll(
            List<TypedSpec> elems,
            List<SqlExpr> lowered) {
        List<SqlExpr> ids = new ArrayList<>();
        List<SqlExpr> vals = new ArrayList<>();
        for (int i = 0; i < elems.size(); i++) {
            if (!encodeMixed(elems.get(i), lowered.get(i), ids, vals)) {
                return null;
            }
        }
        return new MixedElems(ids, vals);
    }

    /**
     * One element's (identity, comparable) SQL pair, dispatched on its
     * STATIC type. All value work happens in SQL.
     */
    private static boolean encodeMixed(TypedSpec e,
                                       SqlExpr x,
                                       List<SqlExpr> ids,
                                       List<SqlExpr> vals) {
        // a carrier-wrapped element unwraps: identity/comparable both
        // build from the RAW value (floatRepr over json cannot type)
        x = unwrapVariant(x);
        Type t = e.info().type();
        if (t == Type.Primitive.INTEGER) {
            ids.add(new SqlExpr.Cast(x, SqlType.Scalar.VARCHAR));
            vals.add(new SqlExpr.Cast(x, SqlType.Scalar.DOUBLE));
            return true;
        }
        if (t == Type.Primitive.FLOAT) {
            // F10 slice 2: the LITERAL table (floatCanon — total
            // fixed-point, pure never prints exponents), so selection
            // ids byte-match the carrier's spellings on both sides
            ids.add(LiteralSpelling.literal(x, Type.Primitive.FLOAT));
            vals.add(x);
            return true;
        }
        if (t == Type.Primitive.DECIMAL || t instanceof Type.PrecisionDecimal) {
            // literal D-form == the old print D-form byte-for-byte (a
            // DECIMAL cast text has no D to strip)
            ids.add(LiteralSpelling.literal(x, Type.Primitive.DECIMAL));
            vals.add(new SqlExpr.Cast(x, SqlType.Scalar.DOUBLE));
            return true;
        }
        if (t == Type.Primitive.STRICT_DATE) {
            // F10 slice 3: temporal ids are LITERAL spellings (%-form)
            // — the decoder's PureDateLiteral arm parses them; typed
            // host values replace the old date-string convention
            ids.add(LiteralSpelling.strictDateLiteral(x));
            vals.add(new SqlExpr.Cast(x, SqlType.Scalar.TIMESTAMP));
            return true;
        }
        if (t == Type.Primitive.DATE_TIME) {
            SqlExpr lit = staticSubsecondSpelling(e);
            ids.add(lit != null ? lit
                    : LiteralSpelling.dateTimeLiteral(x,
                            new SqlExpr.FormatLit(dateTimeFormatOf(e))));
            vals.add(x);
            return true;
        }
        if (t == Type.Primitive.DATE) {
            // PARTIAL dates travel as STRINGS (master's pinned carrier): the
            // string IS the print form; the comparable composes via
            // make_timestamp from split components (strptime %Y rejects
            // 5-digit years; make_timestamp reaches year 294246).
            SqlExpr cmp = partialComparable(e, x);
            if (cmp == null) {
                return false;
            }
            ids.add(LiteralSpelling.partialDateLiteral(x));
            vals.add(cmp);
            return true;
        }
        return false;
    }

    /** F10 slice 3b — ONE element's pure-literal spelling for the
     * Any-position carrier, by its STATIC kind. Null = unspellable
     * (instances; variants; carriers — an enum spells Enumeration.NAME) — the caller keeps the JSON lane for the whole
     * collection. A previously-boxed element unwraps first. */
    static @com.legend.base.Nullable SqlExpr elementLiteral(TypedSpec e,
            SqlExpr x) {
        if (e instanceof com.legend.compiler.spec.typed.TypedTypeRef
                || e instanceof com.legend.compiler.spec.typed.TypedPackageableRef) {
            // a TYPE or ELEMENT value (String, Car, a Mapping): its bare
            // simple NAME is its spelling — the wire convention already
            // (the Lowerer lowers both to the name), unquoted, so it never
            // equals a string; the host half (LiteralText) reads a bare
            // identifier back as the name
            return x;
        }
        if (e.info().type() == Type.Primitive.DATE_TIME) {
            SqlExpr lit = staticSubsecondSpelling(e);
            if (lit != null) {
                return lit;
            }
        }
        return spellByKind(e.info().type(), unwrapVariant(x),
                dateTimeFormatOf(e));
    }

    /** A SUBSECOND-written DateTime literal spells STATICALLY (the
     * one spelling owner is {@link LiteralSpelling#writtenTemporalText}
     * — disagree-9 burn, testDayOfMonth receipt). Null = not that
     * shape. */
    private static @com.legend.base.Nullable SqlExpr staticSubsecondSpelling(
            TypedSpec e) {
        return e instanceof TypedCDate cd && cd.value()
                instanceof PureDateLiteral.DateWithSubsecond d
                ? new SqlExpr.StringLit("%"
                        + LiteralSpelling.writtenTemporalText(d) + "+0000")
                : null;
    }

    /** The per-KIND spelling core — shared by the element encoder above
     * (typed elements) and the relation values-flatten (STATIC column
     * kinds; conform-by-emission: the actual side spells with the SAME
     * grammar owner the claimed expected side used, so grid asserts
     * byte-compare). */
    static @com.legend.base.Nullable SqlExpr spellByKind(Type t, SqlExpr x,
            List<com.legend.sql.DateFmt> dateTimeFmt) {
        if (t == Type.Primitive.INTEGER || t == Type.Primitive.FLOAT
                || t == Type.Primitive.BOOLEAN
                || t == Type.Primitive.STRING) {
            return LiteralSpelling.literal(x, t);
        }
        if (t == Type.Primitive.DECIMAL
                || t instanceof Type.PrecisionDecimal) {
            return LiteralSpelling.literal(x, Type.Primitive.DECIMAL);
        }
        if (t == Type.Primitive.STRICT_DATE) {
            return LiteralSpelling.strictDateLiteral(x);
        }
        if (t == Type.Primitive.DATE_TIME) {
            return LiteralSpelling.dateTimeLiteral(x,
                    new SqlExpr.FormatLit(dateTimeFmt));
        }
        if (t == Type.Primitive.DATE) {
            return LiteralSpelling.partialDateLiteral(x);
        }
        if (t instanceof Type.EnumType) {
            return LiteralSpelling.literal(x, t);   // Enumeration.NAME
        }
        return null;
    }

    /** The ANY-LUB two-channel encoding for SORT (interpreted
     * Compare.java's general case): identity = the pure-literal spelling
     * (the carrier's own lane), comparable = a RANK STRUCT ordering
     * cross-kind by comparison group — numbers &lt; dates &lt; Boolean
     * &lt; String ({@code PRIMITIVE_TYPE_COMPARISON_ORDER} collapsed to
     * its comparable groups: numbers compare numerically ACROSS
     * Integer/Float, dates chronologically ACROSS kinds — the engine's
     * numeric/date arms run before the rank fallback) — and within a
     * group by the group's own channel. Null = an element outside the
     * spellable primitives (the caller keeps its current lane). */
    static @com.legend.base.Nullable MixedElems rankedElems(TypedSpec arg,
            SqlExpr lowered) {
        if (!(arg instanceof TypedCollection c)
                || c.elements().size() < 2
                || !(lowered instanceof SqlExpr.ArrayLit la)
                || la.elements().size() != c.elements().size()
                || !PlatformTypes.isAny(c.info().type())) {
            return null;
        }
        List<SqlExpr> ids = new ArrayList<>();
        List<SqlExpr> vals = new ArrayList<>();
        for (int i = 0; i < c.elements().size(); i++) {
            TypedSpec e = c.elements().get(i);
            // the Any-LUB carrier's elements ARE the pure-literal
            // spellings already (the literal lane built them) — the
            // identity passes through VERBATIM; the comparable derives
            // from the spelling by the element's STATIC kind
            SqlExpr x = la.elements().get(i);
            SqlExpr cmp = rankedComparable(e, x);
            if (cmp == null) {
                return null;
            }
            ids.add(x);
            vals.add(cmp);
        }
        return new MixedElems(ids, vals);
    }

    /** One element's rank-struct comparable, derived FROM ITS SPELLING —
     * field order IS the comparison order (struct ordering is
     * lexicographic); exactly one group channel is non-null, and the
     * NULL slots carry their declared types (the StructLit declared-slot
     * arm). Number spellings cast back numerically; a string's quoted
     * spelling preserves string order under its constant quote prefix;
     * boolean spellings order 'false' &lt; 'true' textually. Decimal
     * (D-suffix spelling) and temporal (%-form) kinds bail — their
     * spellings don't cast back; the caller keeps its lane. */
    private static @com.legend.base.Nullable SqlExpr rankedComparable(
            TypedSpec e, SqlExpr x) {
        Type t = e.info().type();
        int group;
        SqlExpr n = null;
        SqlExpr d = null;
        SqlExpr s = null;
        if (t == Type.Primitive.INTEGER || t == Type.Primitive.FLOAT
                || t == Type.Primitive.NUMBER) {
            group = 0;
            n = new SqlExpr.Cast(x, SqlType.Scalar.DOUBLE);
        } else if (t == Type.Primitive.BOOLEAN) {
            group = 2;
            s = x;
        } else if (t == Type.Primitive.STRING) {
            group = 3;
            s = x;
        } else {
            return null;
        }
        return new SqlExpr.StructLit(List.of(
                new SqlExpr.StructLit.Field("g", new SqlExpr.IntLit(group)),
                new SqlExpr.StructLit.Field("n",
                        n != null ? n : new SqlExpr.NullLit(),
                        SqlType.Scalar.DOUBLE),
                new SqlExpr.StructLit.Field("d",
                        d != null ? d : new SqlExpr.NullLit(),
                        SqlType.Scalar.TIMESTAMP),
                new SqlExpr.StructLit.Field("s",
                        s != null ? s : new SqlExpr.NullLit(),
                        SqlType.Scalar.VARCHAR)));
    }

    /** A date operand's chronological comparable (strptime-padded partials); non-dates pass through. */
    static SqlExpr dateComparableOrSelf(TypedSpec e,
                                        SqlExpr x) {
        Type t = e.info().type();
        if (t == Type.Primitive.DATE) {
            SqlExpr cmp = partialComparable(e, x);
            if (cmp != null) {
                return cmp;
            }
        }
        if (t == Type.Primitive.STRICT_DATE) {
            return new SqlExpr.Cast(x, SqlType.Scalar.TIMESTAMP);
        }
        return x;
    }

    /** DateTime print format — subsecond DIGIT COUNT is a static attribute of the literal. */
    private static List<com.legend.sql.DateFmt> dateTimeFormatOf(TypedSpec e) {
        if (e instanceof TypedCDate cd
                && cd.value() instanceof PureDateLiteral.DateWithSubsecond) {
            return com.legend.sql.DateFmt.ISO_MICRO;
        }
        return com.legend.sql.DateFmt.ISO_LOCAL;
    }

    /**
     * A PARTIAL date string's chronological comparable, composed IN SQL:
     * {@code make_timestamp(split_part(x,'-',i)...)} per the STATIC
     * precision; null when the precision is not a known partial form.
     */
    private static @com.legend.base.Nullable SqlExpr partialComparable(TypedSpec e,
                                             SqlExpr x) {
        PureDateLiteral.Precision prec = Scalars.datePrecision(e);
        if (prec.atLeast(PureDateLiteral.Precision.HOUR)) {
            return null;
        }
        SqlExpr one = new SqlExpr.IntLit(1);
        SqlExpr zero = new SqlExpr.IntLit(0);
        SqlExpr year = new SqlExpr.Cast(
                SqlExpr.Call.of(SqlFn.SPLIT_PART, x, new SqlExpr.StringLit("-"), one),
                SqlType.Scalar.BIGINT);
        SqlExpr month = prec.atLeast(PureDateLiteral.Precision.MONTH) ? new SqlExpr.Cast(
                SqlExpr.Call.of(SqlFn.SPLIT_PART, x, new SqlExpr.StringLit("-"),
                        new SqlExpr.IntLit(2)),
                SqlType.Scalar.BIGINT) : one;
        SqlExpr day = prec.atLeast(PureDateLiteral.Precision.DAY) ? new SqlExpr.Cast(
                SqlExpr.Call.of(SqlFn.SPLIT_PART, x, new SqlExpr.StringLit("-"),
                        new SqlExpr.IntLit(3)),
                SqlType.Scalar.BIGINT) : one;
        return SqlExpr.Call.of(SqlFn.MAKE_TIMESTAMP, year, month, day, zero, zero, zero);
    }

    /** An Any-LUB {@code if} with DIFFERING branch kinds rides the
     * VARIANT carrier (the mixed-list discipline: a raw CASE cannot even
     * type — 'TDSNull' vs INT32, the TDS-getter witness); NULL stays the
     * bare empty carrier. Same-kind or non-Any ifs emit raw branches. */
    static SqlExpr lubCase(Type lub, TypedSpec thenB,
            @com.legend.base.Nullable TypedSpec elseB, SqlExpr cond,
            SqlExpr thenS, SqlExpr elseS) {
        boolean mixed = lub instanceof Type.ClassType ifCt
                && PlatformTypes.isAny(ifCt)
                && elseB != null
                && !thenB.info().type().equals(elseB.info().type());
        return new SqlExpr.Case(
                List.of(new SqlExpr.Case.When(cond,
                        mixed ? variantBranch(thenB, thenS) : thenS)),
                mixed && elseB != null ? variantBranch(elseB, elseS) : elseS);
    }

    /** One branch of a mixed-kind if on the variant carrier: a value is
     * TO_VARIANT-wrapped; a [1]-stamped NULL literal (the ^TDSNull()
     * instance — batch 69a) is the JSON null VALUE by the value law, so
     * the branch survives as an element; an optional NULL stays bare. */
    private static SqlExpr variantBranch(
            com.legend.compiler.spec.typed.TypedSpec b, SqlExpr s) {
        if (!(s instanceof SqlExpr.NullLit)) {
            return SqlExpr.Call.of(SqlFn.TO_VARIANT, s);
        }
        boolean oneStamped = b.info().multiplicity()
                instanceof com.legend.compiler.element.type.Multiplicity.Bounded mb
                && mb.lower() >= 1 && Integer.valueOf(1).equals(mb.upper());
        return oneStamped
                ? new SqlExpr.Cast(new SqlExpr.StringLit("null"),
                        com.legend.sql.SqlType.Scalar.JSON)
                : s;
    }

    /**
     * A primitive needle against class-typed elements (or vice versa) can
     * never be a member — the kinds are disjoint in pure's type system.
     * Any/mixed stays undecided (falls through to the SQL comparison).
     */
    static boolean kindMismatch(Type needle, Type elems) {
        boolean np = needle instanceof Type.Primitive || needle instanceof Type.PrecisionDecimal;
        boolean ep = elems instanceof Type.Primitive || elems instanceof Type.PrecisionDecimal;
        boolean nc = Scalars.isClassish(needle) && !PlatformTypes.isAny(needle);
        boolean ec = Scalars.isClassish(elems) && !PlatformTypes.isAny(elems);
        return (np && ec) || (nc && ep);
    }

    /** The VALUE LAW of the variant lane: a [1]-STAMPED element is a
     * VALUE and never vanishes — TDSNull is DATA on the grid convention
     * (engine tds.pure: {@code TDSRow.values : Any[*]} holds a
     * ^TDSNull() instance per null cell, built at the row read,
     * relationalMappingExecution buildExecutionResultInTDS). Its
     * surviving representation is the JSON null VALUE: statically for
     * the TDSNull literal (its scalar form IS the SQL NULL literal),
     * via COALESCE for a runtime [1] cell whose wire may still carry
     * NULL (declared-required columns under left joins — the
     * nullability lie). A ZERO-lower-bound element keeps the bare wrap:
     * an EMPTY decays by variant-decay semantics (pure collections hold
     * no empties). Static non-null literals keep the bare wrap so their
     * SQL stays byte-identical (COALESCE is a no-op on them). The
     * spelling {@code CAST('null' AS JSON)} is the dialect-probed
     * text-to-JSON idiom both backends navigate. {@code lowered} is the
     * element's already-lowered scalar form (the Lowerer owns scalar()).
     *
     * <p>SCOPE ({@code cellSlots}): the runtime COALESCE applies ONLY to
     * ROW-CELLS collections (TypedCollection.rowCells() — the
     * construction-declared fact off the Typer's rowCells() synthesis),
     * where a declared-[1] cell's WIRE can still carry NULL
     * (left joins under the nullability lie) and the slot is grid DATA. A
     * plain value collection's [1] element is stamp-guaranteed non-null,
     * and the bare TO_VARIANT shape is load-bearing downstream (consumers
     * structurally peel it — struct extraction, variant-column detection;
     * the blanket wrap broke both, gate-caught 2026-08-24). */
    static SqlExpr variantElement(
            com.legend.compiler.spec.typed.TypedSpec e, SqlExpr lowered,
            boolean cellSlots) {
        boolean oneStamped = e.info().multiplicity()
                instanceof com.legend.compiler.element.type.Multiplicity.Bounded b
                && b.lower() >= 1 && Integer.valueOf(1).equals(b.upper());
        SqlExpr jsonNull = new SqlExpr.Cast(new SqlExpr.StringLit("null"),
                com.legend.sql.SqlType.Scalar.JSON);
        if (oneStamped && lowered instanceof SqlExpr.NullLit) {
            return jsonNull;
        }
        SqlExpr wrapped = SqlExpr.Call.of(SqlFn.TO_VARIANT, lowered);
        if (cellSlots && oneStamped && !staticallyNonNull(lowered)) {
            return new SqlExpr.Call(SqlFn.COALESCE,
                    java.util.List.of(wrapped, jsonNull));
        }
        return wrapped;
    }

    private static boolean staticallyNonNull(SqlExpr e) {
        return e instanceof SqlExpr.StringLit || e instanceof SqlExpr.IntLit
                || e instanceof SqlExpr.FloatLit
                || e instanceof SqlExpr.DecimalLit
                || e instanceof SqlExpr.BoolLit || e instanceof SqlExpr.DateLit
                || e instanceof SqlExpr.TimestampLit;
    }

    /** The ONE inverse of the variant wrap — understands every shape
     * {@link #variantElement} can emit: {@code TO_VARIANT(v)} and the
     * cell-slot law's {@code COALESCE(TO_VARIANT(v), CAST('null' AS
     * JSON))}. Consumers that need the raw value back (format arg
     * decomposition, mixed-identity encode, literal spelling, numeric
     * aggregates, membership carrier decisions) ask HERE; each matching
     * the wrap shape locally went stale the first time the wrap changed
     * (gate-caught 2026-08-24 — struct extraction and variant-column
     * detection missed the COALESCE form). Returns {@code x} unchanged
     * when not wrapped. */
    /** A value entering an Any-declared (JSON-carried) slot: a JSON value
     * passes; a typed primitive wraps as the literal carrier does; a value
     * on the LITERAL LANE (an Any cell read: a quoted string, a
     * {@code %}-prefixed date, a bare number / boolean) converts by its own
     * spelling — the one boundary where the literal lane meets a JSON slot. */
    /** The Pair STRUCT carrier {@code (first, second)}: a slot the Pair
     * declares Any rides the JSON carrier ({@link #variantSlot}). */
    static SqlExpr pairStruct(Type pairType, SqlExpr first, Type firstType,
            SqlExpr second, Type secondType) {
        List<Type> pairArgs = pairType instanceof Type.GenericType pg ? pg.arguments() : List.of();
        SqlExpr[] slots = {first, second};
        Type[] types = {firstType, secondType};
        for (int i = 0; i < 2; i++) {
            if (i < pairArgs.size() && pairArgs.get(i) instanceof Type.ClassType ct
                    && PlatformTypes.isAny(ct)) {
                slots[i] = variantSlot(slots[i], types[i]);
            }
        }
        return new SqlExpr.StructLit(List.of(
                new SqlExpr.StructLit.Field("first", slots[0]),
                new SqlExpr.StructLit.Field("second", slots[1])));
    }

    static SqlExpr variantSlot(SqlExpr x, Type staticType) {
        com.legend.sql.SqlType wire = x.type() instanceof com.legend.sql.TypeFact.Typed tf ? tf.type() : null;
        if (wire == com.legend.sql.SqlType.Scalar.JSON) {
            return x;
        }
        // a typed primitive, or a plain wire value (a VARCHAR cell read) —
        // the JSON value of what it holds; only the LITERAL lane's spelled
        // text needs its spelling read back
        if (staticType instanceof Type.Primitive
                || (wire != null && wire != com.legend.sql.SqlType.Scalar.LITERAL)) {
            return SqlExpr.Call.of(SqlFn.TO_VARIANT, x);
        }
        SqlExpr text = new SqlExpr.Cast(x, com.legend.sql.SqlType.Scalar.VARCHAR);
        SqlExpr quoted = SqlExpr.Call.of(SqlFn.STARTS_WITH, text, new SqlExpr.StringLit("'"));
        SqlExpr unquoted = SqlExpr.Call.of(SqlFn.REPLACE,
                SqlExpr.Call.of(SqlFn.REPLACE,
                        SqlExpr.Call.of(SqlFn.SUBSTRING, text, new SqlExpr.IntLit(2),
                                SqlExpr.Call.of(SqlFn.MINUS, SqlExpr.Call.of(SqlFn.LENGTH, text),
                                        new SqlExpr.IntLit(2))),
                        new SqlExpr.StringLit("\\'"), new SqlExpr.StringLit("'")),
                new SqlExpr.StringLit("\\\\"), new SqlExpr.StringLit("\\"));
        SqlExpr dated = SqlExpr.Call.of(SqlFn.STARTS_WITH, text, new SqlExpr.StringLit("%"));
        return new SqlExpr.Case(List.of(
                new SqlExpr.Case.When(SqlExpr.Call.of(SqlFn.IS_NULL, text), new SqlExpr.NullLit()),
                new SqlExpr.Case.When(SqlExpr.Call.of(SqlFn.EQUAL, text,
                        new SqlExpr.StringLit(PlatformTypes.TDS_NULL_CELL)), new SqlExpr.NullLit()),
                new SqlExpr.Case.When(quoted, SqlExpr.Call.of(SqlFn.TO_VARIANT, unquoted)),
                new SqlExpr.Case.When(dated, SqlExpr.Call.of(SqlFn.TO_VARIANT,
                        SqlExpr.Call.of(SqlFn.SUBSTRING, text, new SqlExpr.IntLit(2))))),
                new SqlExpr.Cast(text, com.legend.sql.SqlType.Scalar.JSON));
    }

    static SqlExpr unwrapVariant(SqlExpr x) {
        if (x instanceof SqlExpr.Call c && c.fn() == SqlFn.COALESCE
                && c.args().size() == 2
                && c.args().get(0) instanceof SqlExpr.Call inner
                && inner.fn() == SqlFn.TO_VARIANT
                && c.args().get(1) instanceof SqlExpr.Cast cast
                && cast.target() == com.legend.sql.SqlType.Scalar.JSON
                && cast.value() instanceof SqlExpr.StringLit s
                && s.value().equals("null")) {
            return inner.args().get(0);
        }
        if (x instanceof SqlExpr.Call c && c.fn() == SqlFn.TO_VARIANT) {
            return c.args().get(0);
        }
        return x;
    }

    /** True iff {@link #unwrapVariant} would peel — the carrier-presence
     * question the membership/aggregate arms ask. */
    static boolean variantWrapped(SqlExpr x) {
        return unwrapVariant(x) != x;
    }

    /** EQUALITY BY EMISSION (the claim's eq lane, M4 re-land): when
     * exactly one lowered operand is LITERAL-marked, the OTHER side —
     * if its static kind spells — is re-emitted as its spelling, so
     * both sides byte-compare in the grammar (which IS pure equality:
     * six kinds, disjoint spellings; §0.4 receipts — canonical-string
     * compare is pure's own mechanism). The literal side is never
     * unspelled (the burn-down doctrine). Returns the operands
     * unchanged when neither or both are literal-marked, or the other
     * side cannot spell (dynamic kinds keep their existing lanes).
     * The mark is the STORED type fact — the typed IR's read (the
     * parked branch needed its judge + LambdaWire scope here). */
    static java.util.List<SqlExpr> equalityEmission(TypedSpec a0,
            TypedSpec a1, java.util.List<SqlExpr> cargs) {
        boolean l0 = literalMarked(cargs.get(0));
        boolean l1 = literalMarked(cargs.get(1));
        if (l0 == l1) {
            return cargs;
        }
        int plain = l0 ? 1 : 0;
        SqlExpr spelled = elementLiteral(plain == 0 ? a0 : a1,
                cargs.get(plain));
        if (spelled == null) {
            return cargs;
        }
        return plain == 0 ? java.util.List.of(spelled, cargs.get(1))
                : java.util.List.of(cargs.get(0), spelled);
    }

    /** FORMAT's LITERAL-carried argument list (M4 re-land): each slot's
     * value is the spelling->PRINT projection ({@link
     * LiteralSpelling#printForm} — the burn-down doctrine's transform,
     * never an inversion). Numeric/bool directives want the VALUE kind
     * back — the print re-types by the slot's STATIC type (emission by
     * kind; text kinds stay text). A non-carried list returns
     * unchanged. */
    static SqlExpr printedFormatSlots(SqlExpr argColl,
            List<TypedSpec> typedElems) {
        if (!(argColl instanceof SqlExpr.Cast mc
                && mc.target() instanceof SqlType.Array ma2
                && ma2.element() == com.legend.sql.SqlType.Scalar.LITERAL
                && mc.value() instanceof SqlExpr.ArrayLit sla)) {
            return argColl;
        }
        List<SqlExpr> printed =
                new java.util.ArrayList<>(sla.elements().size());
        for (int i = 0; i < sla.elements().size(); i++) {
            SqlExpr p = LiteralSpelling.printForm(sla.elements().get(i));
            Type st = i < typedElems.size()
                    ? typedElems.get(i).info().type() : null;
            if (st == Type.Primitive.INTEGER
                    || st == Type.Primitive.FLOAT
                    || st == Type.Primitive.BOOLEAN
                    || st == Type.Primitive.DECIMAL
                    || st instanceof Type.PrecisionDecimal) {
                p = new SqlExpr.Cast(p, PureSql.type(
                        st instanceof Type.PrecisionDecimal
                                ? Type.Primitive.DECIMAL : st));
            }
            printed.add(p);
        }
        return new SqlExpr.ArrayLit(printed);
    }

    /** The COMPARATOR-FORM needle wrap (M4 post-landing audit): when
     * the collection is LITERAL-carried, the needle substituted into a
     * contains-comparator body must be SPELLED by its static kind AND
     * MARKED (the raw needle's TEXT collided with spellings —
     * eq('1', 1) answered true; and the comparator's both-element
     * param stamps are honest only because this mark makes the needle
     * a carrier value too). Unspellable needles return unchanged —
     * they can never byte-equal a spelling. */
    static SqlExpr markedNeedle(TypedSpec needleSpec, SqlExpr needle,
            SqlExpr coll) {
        if (coll.type() instanceof com.legend.sql.TypeFact.Typed ct
                && ct.type() instanceof SqlType.Array ca
                && ca.element() == com.legend.sql.SqlType.Scalar.LITERAL) {
            SqlExpr spelled = elementLiteral(needleSpec, needle);
            if (spelled != null) {
                return new SqlExpr.Cast(spelled,
                        com.legend.sql.SqlType.Scalar.LITERAL);
            }
        }
        return needle;
    }

    private static boolean literalMarked(SqlExpr e) {
        return e.type() instanceof com.legend.sql.TypeFact.Typed t
                && (t.type() == com.legend.sql.SqlType.Scalar.LITERAL
                        || (t.type() instanceof SqlType.Array a
                                && a.element()
                                        == com.legend.sql.SqlType.Scalar.LITERAL));
    }

    /** A constructed field's value in its layout SLOT's carrier (WORLD_MAP
     * §4): a struct-shaped value (a class with a layout — a single
     * {@code [$d]} or a many navigation) bound to a JSON slot (the declared
     * class is polymorphic/layoutless: {@code parameters :
     * RelationalOperationElement[*]}) takes the variant carrier per
     * element — the same {@code to_json} the literal-collection arm
     * spells, so every instance of the class shares ONE struct type
     * (DuckDB unifies the shape-CASE branches; the wire decodes per
     * element). Values already on the JSON carrier pass through. */
    static SqlExpr slotCarrier(SqlExpr v, boolean many, SqlType valueType,
            SqlType slot, java.util.function.Supplier<String> fresh) {
        if (slot != SqlType.Scalar.JSON || !(valueType instanceof SqlType.Struct)) {
            return v;
        }
        if (many) {
            String x = fresh.get();
            return SqlExpr.Call.of(SqlFn.LIST_TRANSFORM, v,
                    new SqlExpr.Lambda(List.of(x),
                            SqlExpr.Call.of(SqlFn.TO_VARIANT, SqlExpr.Column.derived(null, x))));
        }
        return SqlExpr.Call.of(SqlFn.TO_VARIANT, v);
    }

    /** The identity layout's SYNTHETIC fields at a construction/copy site:
     * F13 {@code __id} (one deterministic id per site — a copy is a NEW
     * instance) and WORLD_MAP §4 {@code __type} (the constructed class, so
     * a polymorphic slot's value is judged by its own classifier). Null for
     * a model field. */
    static SqlExpr.StructLit.@com.legend.base.Nullable Field syntheticField(
            Type.Column c, @com.legend.base.Nullable String siteId, String classFqn) {
        if (com.legend.compiler.element.ClassLayouts.SYNTHETIC_ID.equals(c.name())
                && siteId != null) {
            return new SqlExpr.StructLit.Field(c.name(), new SqlExpr.StringLit(siteId));
        }
        if (com.legend.compiler.element.ClassLayouts.SYNTHETIC_TYPE.equals(c.name())) {
            return new SqlExpr.StructLit.Field(c.name(), new SqlExpr.StringLit(classFqn));
        }
        return null;
    }

    static String simpleName(String qn) {
        int cut = qn.lastIndexOf("::");
        return cut < 0 ? qn : qn.substring(cut + 2);
    }


    /** Pure LITERAL FLATTENING at construction (audit-of-R1: consumer-site
     * compaction was whack-a-mole): a VALUE-lane literal collection with an
     * element that CAN be empty ([0..1]/[0..*] stamped — an optional, a
     * conditional-membership residual) compacts ONCE so every consumer sees
     * the pure collection. */
    static boolean compacts(TypedCollection c) {
        return CollectionLanes.valueLane(c) && c.elements().stream().anyMatch(e ->
                e.info().multiplicity() instanceof com.legend.compiler.element.type.Multiplicity.Bounded b
                && b.lower() == 0);
    }
}
