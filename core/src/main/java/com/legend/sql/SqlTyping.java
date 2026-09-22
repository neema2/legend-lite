// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.sql;

import java.util.List;

/**
 * The ONE owner of SQL typing knowledge (docs/TYPED_SQL_IR.md): the
 * per-node typing RULES the {@link SqlExpr} constructors call (the type
 * is a property OF the tree, computed once at construction), the
 * ADMISSIBILITY relation (the registered label-carrier pairs), and the
 * label RECONCILIATION {@link SqlSelect}'s constructor applies. The
 * transitional judge (consumption-side re-derivation, then a
 * leaf-binding rebuilder) is DELETED — its parity with the stored
 * types was pinned at zero divergence on every lane first.
 *
 * <p>Typing stays deliberately PARTIAL: {@code UNKNOWN} means "no rule
 * yet" — coverage grows rule by rule, and an untypeable expression is
 * COUNTED (the census untyped ceiling), never guessed. A rule is added
 * only when the backend's behavior is certain; the aggregate
 * numeric-promotion rules ({@link #reducerType}) come from an
 * empirical probe of the reference backend (DuckDB 1.5.0, 2026-08-24 —
 * matrix in the charter's M1 receipts).
 */
public final class SqlTyping {

    private SqlTyping() {
    }

    public static final TypeFact BOTTOM = new TypeFact.Bottom();
    public static final TypeFact RAISES = new TypeFact.Raises();
    public static final TypeFact UNKNOWN = new TypeFact.Unknown();

    public static TypeFact typed(SqlType t) {
        return new TypeFact.Typed(t);
    }

    /** The may-be-null variant of a fact (§E3 M-N1): marks a Typed
     * fact nullable, preserving its type and tolerance. Bottom (IS the
     * NULL value), Raises (never yields) and Unknown (no claim) pass
     * through. */
    static TypeFact nullable(TypeFact f) {
        return f instanceof TypeFact.Typed t && !t.nullable()
                ? new TypeFact.Typed(t.type(), true) : f;
    }

    /** Can this OPERAND deliver SQL NULL at runtime? Typed answers its
     * own dimension; Bottom IS null; Raises never yields; UNKNOWN
     * cannot prove presence — true, the safe side (TypeFact doc: false
     * is a proof claim). Non-value ride-alongs contribute nothing:
     * lambdas (bodies speak through the per-function rules), format
     * literals; an ArrayLit/StructLit is a DEFINITE composite value
     * even when its element type is unknowable (the empty []). */
    static boolean mayBeNull(SqlExpr e) {
        if (e instanceof SqlExpr.Lambda || e instanceof SqlExpr.FormatLit
                || e instanceof SqlExpr.ArrayLit
                || e instanceof SqlExpr.StructLit) {
            return false;
        }
        return switch (e.type()) {
            case TypeFact.Typed t -> t.nullable();
            case TypeFact.Raises r -> false;
            default -> true;
        };
    }

    /** §E3 slack fix 2 — can this collection's ELEMENTS be SQL NULL?
     * Provably pure operands (the slack census's 1,308-row UNNEST
     * family): {@link SqlExpr.CompactList} — stripping SQL NULL
     * elements IS its contract; an ArrayLit whose every element is
     * non-null; the splitter/range families (probed 1.5.0:
     * string_split over 'a,,b' yields empty strings, never NULL).
     * Cast/Group transport the value — elements unchanged (an
     * element-kind cast RAISES on failure, never NULLs — not
     * TRY_CAST). Everything else stays may-null: SqlType carries no
     * element-nullability dimension. */
    private static boolean elementsMayBeNull(SqlExpr coll) {
        return switch (coll) {
            case SqlExpr.CompactList cl -> false;
            case SqlExpr.ArrayLit al -> anyNullable(al.elements());
            case SqlExpr.Cast c -> elementsMayBeNull(c.value());
            case SqlExpr.Group g -> elementsMayBeNull(g.inner());
            case SqlExpr.Call c -> c.fn() != SqlFn.SPLIT
                    && c.fn() != SqlFn.REGEXP_EXTRACT_ALL
                    && c.fn() != SqlFn.RANGE_FN;
            default -> true;
        };
    }

    private static boolean anyNullable(List<SqlExpr> a) {
        for (SqlExpr e : a) {
            if (mayBeNull(e)) {
                return true;
            }
        }
        return false;
    }

    private static boolean allNullable(List<SqlExpr> a) {
        for (SqlExpr e : a) {
            if (!mayBeNull(e)) {
                return false;
            }
        }
        return true;
    }

    // shared scalar verdicts — constructed once, stored on every node of
    // the kind (constants, not a cache: there is no lifecycle)
    static final TypeFact T_BOOLEAN = typed(SqlType.Scalar.BOOLEAN);
    static final TypeFact T_INTEGER = typed(SqlType.Scalar.INTEGER);
    static final TypeFact T_BIGINT = typed(SqlType.Scalar.BIGINT);
    static final TypeFact T_HUGEINT = typed(SqlType.Scalar.HUGEINT);
    static final TypeFact T_DOUBLE = typed(SqlType.Scalar.DOUBLE);
    static final TypeFact T_VARCHAR = typed(SqlType.Scalar.VARCHAR);
    static final TypeFact T_DATE = typed(SqlType.Scalar.DATE);
    static final TypeFact T_TIMESTAMP = typed(SqlType.Scalar.TIMESTAMP);
    static final TypeFact T_JSON = typed(SqlType.Scalar.JSON);

    // ------------------------------------------------------------------
    // THE LABEL FLIP (TYPED_SQL_IR.md §3/§6, executed 2026-08-24): an
    // OutputCol label is the PURE-CONTRACT erasure; the projection's
    // stored type is the WIRE. SqlSelect's canonical constructor
    // reconciles the two: equal or REGISTERED (subsumed/tolerated) keeps the
    // contract; anything else was a label lie and the label adopts the
    // wire. The admissibility relation MOVED here from the census (the
    // flip encodes it; the census reads the same relation).
    // ------------------------------------------------------------------

    /** Per-SLOT label reconciliation — called by {@link SqlSelect}'s
     * constructor for each explicit projection against its DECLARED
     * output (outputs-from-projections: the positional reconcileLabels
     * and its star-tail shift are subsumed — every slot arrives already
     * paired). KIND: equal or ADMITTED keeps the pure-contract erasure;
     * a label lie ADOPTS the wire. (A third CONFORM-by-emitted-cast
     * verdict was tried at this seam and REVERTED by the referee — a
     * type-pair cannot distinguish the concrete-Float conversion from
     * the abstract-Number identity carrier; conformance by emission
     * lives at the STAMP-GUARDED mapping-read seam in the lowering —
     * T4 leg 1.) NULLABILITY (§E3 M-N3 — THE FLIP): the label ADOPTS
     * the slot truth ({@link #slotNullable}) in BOTH directions — the
     * pure-multiplicity echo is no longer an authority here (the [1]
     * contract stays the PURE type; the SQL label speaks the physics).
     * A wire NULL under a nullable=false label is thereafter a compiler
     * bug, pinned EQUALITY-0. Star expansions carry no per-column claim
     * (their inherited outputs are the DDL/join-pad authorities, M-N2). */
    static OutputCol reconcileSlot(SqlExpr pe, OutputCol oc,
            boolean grouped) {
        // THE SLOT IS THE WIRE (docs/WIRE_SLOT_HOMEWORK_2026_09_19.md): the
        // declared label is a Pure fact the schema holds; the slot adopts
        // what the expression computes, unconditionally
        SqlType type = pe.type() instanceof TypeFact.Typed t ? t.type() : oc.type();
        // §E3 M-N3 THE FLIP: nullability adopts the slot truth in
        // both directions (the projected-NullLit N1 arm is
        // subsumed — a Bottom slot IS nullable by definition;
        // union pads restore presence above by member merging).
        boolean nul = SqlTyping.slotNullable(pe, grouped);
        if (type.equals(oc.type()) && nul == oc.nullable()) {
            return oc;
        }
        return new OutputCol(oc.name(), type, nul, oc.origin());
    }

    /** Union-label reconciliation — called by {@link SqlUnion}'s
     * canonical constructor (the SqlSelect compact-ctor idiom brought
     * to the OTHER query node; the D1 tripwire's first catch found the
     * gap: a union's outputs asserted the pure-contract erasure over
     * branches whose own labels had already adopted their wire, so the
     * frame above re-read the stale contract —
     * testSQLQueryMergingForInnerJoins2's String-declared property
     * over dTable.pk INTEGER). Column i adopts the branches' UNIFORM
     * computed type when it differs unsubsumed from the label;
     * branch tolerance and nullability propagate (a union cell is
     * nullable when any branch's is). Mixed branch types keep the
     * contract — loud at the wire, never guessed. ORIGIN (SQL-IR
     * slice 2, audit #7 — the union blanket-DERIVED gap deleted): a
     * union's delivered labels are its FIRST branch's labels (SQL's
     * own rule), so each slot inherits the first branch's origin —
     * branch outputs now derive from branch projections, so the
     * inherited fact is construction truth, never a stamp. */
    static @com.legend.base.Nullable List<OutputCol> reconcileUnionLabels(
            List<? extends SqlQuery> branches,
            @com.legend.base.Nullable List<OutputCol> outputs) {
        if (outputs == null || branches.isEmpty()) {
            return outputs;
        }
        List<OutputCol> os = null;
        for (int i = 0; i < outputs.size(); i++) {
            OutputCol oc = outputs.get(i);
            SqlType t = null;
            boolean nul = false;
            boolean uniform = true;
            OutputCol.Origin first = null;
            for (SqlQuery b : branches) {
                List<OutputCol> bo = b.outputs();
                if (bo == null || bo.size() != outputs.size()) {
                    uniform = false;
                    break;
                }
                OutputCol bc = bo.get(i);
                if (first == null) {
                    first = bc.origin();
                }
                nul |= bc.nullable();
                if (t == null) {
                    t = bc.type();
                } else if (!t.equals(bc.type())) {
                    uniform = false;
                    break;
                }
            }
            if (!uniform || t == null || first == null) {
                continue;
            }
            // the slot is the wire: the branches' uniform computed type
            SqlType type = t;
            // §E3 M-N3: the union cell's nullability ADOPTS the
            // branches' OR (a cell is nullable exactly when some
            // branch's is — the contract echo is no longer a floor)
            boolean nullable = nul;
            if (type.equals(oc.type()) && nullable == oc.nullable() && first == oc.origin()) {
                continue;
            }
            if (os == null) {
                os = new java.util.ArrayList<>(outputs);
            }
            os.set(i, new OutputCol(oc.name(), type, nullable, first));
        }
        return os == null ? outputs : List.copyOf(os);
    }


    // THE ADMISSIBILITY RELATION IS DELETED (§4bZ-V B4, 2026-08-26 —
    // "admissible() EMPTY, nothing forgiven"). Every arm it ever held
    // ended as one of three honest fates, each measured to zero before
    // its delete (full history in charter §4bZ-V B and TYPED_SQL_IR.md):
    //  - RE-HOMED as a named proven relation: subsumption
    //    ({@code subsumes} — TIMESTAMP<-DATE, same-scale Decimal
    //    widening, round-trip witnesses) and tag-gated engine compat
    //    ({@code carryThrough} — mapping-seam provenance only);
    //  - MODELED as a first-class carrier: LITERAL and TEMPORAL_TEXT
    //    (construction-stamped, registered wire pairs in the census's
    //    delivers());
    //  - CONFORMED BY EMISSION: the JSON egress serializes to VARCHAR
    //    at the statement root (Lowerer.conformJsonEgress); GUID and
    //    the decimal spellings cast at their emitters.
    // A (declared, computed) pair that differs and matches none of the
    // named relations ADOPTS the wire and goes loud — there is no
    // forgiveness-by-kind-pair left in the type system.

    /** THE SUBSUMPTION RELATION (§4bZ-V B2, re-homed from
     * the deleted admissibility relation 2026-08-26): a value of the computed kind in
     * a slot of the declared kind LOSES NOTHING — subtype wire in
     * supertype slot, proven by round-trip decode witnesses
     * (SubsumptionWitnessTest: the decode under the supertype label is
     * IDENTICAL to the subtype's own decode — the Executor fetch
     * switch is driver-object-kind-keyed, so the label never coerces
     * the value). Distinct from the carrier types (LITERAL /
     * TEMPORAL_TEXT — deliberate representations) and
     * {@link #carryThrough} (tag-gated engine compat): nothing here is
     * forgiven, the relation is the type system's own subtyping made
     * visible. */
    public static boolean subsumes(SqlType declared, SqlType computed) {
        // the abstract-Date slot (F5.4): TIMESTAMP is where abstract
        // Date erases; a StrictDate value's DATE wire is the subtype
        if (declared == SqlType.Scalar.TIMESTAMP
                && computed == SqlType.Scalar.DATE) {
            return true;
        }
        // same-scale Decimal widening: a narrower computed decimal
        // fits any wider label at the same scale — value-identical
        return declared instanceof SqlType.Decimal d
                && computed instanceof SqlType.Decimal c2
                && d.scale() == c2.scale()
                && d.precision() >= c2.precision();
    }

    // ------------------------------------------------------------------
    // THE RULE TABLE — called by the node constructors (SqlExpr/SqlAgg).
    // Each rule is a function of the children's STORED verdicts; no rule
    // walks a finished tree.
    // ------------------------------------------------------------------

    /** {@link SqlExpr.Call} — the per-function rules: the KIND from
     * {@link #callKind}, the NULLABILITY from the §E3 arm below. */
    static TypeFact callType(SqlFn fn, List<SqlExpr> a) {
        return callNullability(fn, a, callKind(fn, a));
    }

    /** THE PER-FUNCTION NULLABILITY ARM (§E3 M-N1). Default scalar
     * composition = any-operand-nullable (SQL strictness); every
     * exception below is a PROBED EMISSION fact — DuckDB 1.5.0
     * reference jar, 2026-08-26 battery (the date_trunc/CEILING
     * lesson: the rule describes what WE deliver, never the bare
     * builtin). The default arm never LOWERS a rule-computed
     * nullability (identity transports already carry their operand's). */
    private static TypeFact callNullability(SqlFn fn, List<SqlExpr> a,
            TypeFact base) {
        if (!(base instanceof TypeFact.Typed t)) {
            return base;   // Bottom IS null; Raises never yields;
                           // Unknown claims nothing
        }
        boolean nul = switch (fn) {
            // SQL definition: the null tests always yield a boolean
            case IS_NULL, IS_NOT_NULL -> false;
            // probed: concat()/concat_ws SKIP NULL args (engine
            // parity is the emission's own reason); all-NULL -> ''
            case CONCAT, CONCAT_JOIN -> false;
            // probed: hash(NULL) yields a value, and our signed
            // reinterpretation preserves it; typeof(NULL) -> 'NULL'
            case HASH, TYPEOF -> false;
            // probed: list_concat/map_concat treat NULL as the empty
            // collection; list_append(NULL, x) -> [x]
            case LIST_CONCAT, MAP_CONCAT, LIST_APPEND -> false;
            // NULL only when EVERY operand is (probed: greatest/least
            // IGNORE NULL members — the COALESCE composition)
            case COALESCE, GREATEST, LEAST -> allNullable(a);
            // probed: out-of-range list_extract and missing
            // list_position -> NULL
            case LIST_GET, LIST_POSITION, PURE_SPLIT_PART -> true;  // NULL past the end
            // §E3 slack fix 2 — ELEMENT PURITY (probed 1.5.0:
            // unnest(NULL) and unnest([]) yield ZERO rows, so UNNEST's
            // value nullability is exactly its ELEMENTS')
            case UNNEST -> a.isEmpty() || elementsMayBeNull(a.get(0));
            // probed: x // 0 and mod(x, 0) -> NULL (not an error)
            case INT_DIVIDE, REM, MOD -> true;
            // probed: every list reduction over the EMPTY list -> NULL
            // (sum/avg/median/mode/product/min/max/bool_and/bool_or)
            case LIST_SUM, LIST_AVG, LIST_MEDIAN, LIST_MODE,
                    LIST_PRODUCT, LIST_MAX, LIST_MIN, LIST_BOOL_AND,
                    LIST_BOOL_OR -> true;
            // probed: list_reduce(NULL, f) -> NULL, empty RAISES, and
            // each step is the body's value — nullable at the node
            case LIST_REDUCE -> true;
            // probed: a missing key/path extracts NULL
            case VARIANT_GET -> true;
            default -> t.nullable() || anyNullable(a);
        };
        return nul == t.nullable() ? base
                : new TypeFact.Typed(t.type(), nul);
    }

    /** The per-function KIND rules (the Slice-1 switch, verbatim,
     * lifted to verdicts). */
    private static TypeFact callKind(SqlFn fn, List<SqlExpr> a) {
        return switch (fn) {
            case AND, OR, NOT, EQUAL, NOT_EQUAL, LESS, LESS_EQUAL, GREATER,
                    GREATER_EQUAL, IS_NULL, IS_NOT_NULL, IN, STARTS_WITH,
                    ENDS_WITH, MATCHES, REGEXP_FULL_MATCH, LIST_EXISTS,
                    LIST_FOR_ALL, IS_DISTINCT_FROM, ALL_DISTINCT, NULL_SAFE_EQUAL,
                    NULL_SAFE_NOT_EQUAL, LIST_BOOL_AND, LIST_BOOL_OR,
                    XOR -> T_BOOLEAN;
            case CONCAT, CONCAT_JOIN, UPPER, LOWER, TRIM, LTRIM, RTRIM,
                    REPLACE, SUBSTRING, LEFT, RIGHT, LPAD, RPAD,
                    REVERSE_STRING, UC_FIRST, LC_FIRST, SPLIT_PART, PURE_SPLIT_PART,
                    REGEXP_EXTRACT, REGEXP_REPLACE, CHR, ENCODE_BASE64,
                    DECODE_BASE64, MD5, SHA1, SHA256, GUID, DAYNAME,
                    MONTHNAME, STRFTIME, TYPEOF, BOOL_TO_TEXT, FORMAT,
                    JSON_TYPE, JSON_PRETTY, CURRENT_USER_FN, REPEAT_STR -> T_VARCHAR;
            case LENGTH, STRPOS, ASCII_CODE, LEVENSHTEIN, LIST_LENGTH,
                    LIST_POSITION, EXTRACT, DATE_DIFF, EPOCH_SECONDS,
                    EPOCH_MS, PARSE_INT,
                    // HASH types as OUR EMISSION: the DuckDB renderer
                    // reinterprets hash()'s UBIGINT into signed BIGINT
                    // (DuckDb.hashSigned — exact two's-complement, the
                    // pure hashCode contract). A bare-function probe
                    // says UBIGINT — the CEILING lesson again: rules
                    // describe what WE deliver, not the raw builtin.
                    HASH -> T_BIGINT;
            case SQRT, CBRT, EXP, LN, LOG10, POW, PI, SIN, COS, TAN, ASIN,
                    ACOS, ATAN, ATAN2, SINH, COSH, TANH, COT, RADIANS,
                    DEGREES, DIVIDE, JARO_WINKLER -> T_DOUBLE;
            case TODAY, MAKE_DATE -> T_DATE;
            case NOW, MAKE_TIMESTAMP, STRPTIME, PARSE_DATE, FROM_EPOCH_SECONDS,
                    FROM_EPOCH_MS, TIMEZONE -> T_TIMESTAMP;
            // PROBED 1.5.0 (2026-08-25 full-burn): the list aggregates
            // follow the SAME reducer promotions as their grouped
            // twins, read through the element (list_sum int->HUGEINT,
            // list_avg/median->DOUBLE, list_mode identity)
            case LIST_SUM -> a.isEmpty() ? UNKNOWN
                    : reduceCollectionType(SqlAgg.Fn.SUM, a.get(0));
            case LIST_AVG -> a.isEmpty() ? UNKNOWN
                    : reduceCollectionType(SqlAgg.Fn.AVG, a.get(0));
            case LIST_MEDIAN -> a.isEmpty() ? UNKNOWN
                    : reduceCollectionType(SqlAgg.Fn.MEDIAN, a.get(0));
            case LIST_MODE -> a.isEmpty() ? UNKNOWN
                    : reduceCollectionType(SqlAgg.Fn.MODE, a.get(0));
            // list_product -> DOUBLE for every numeric list (probed)
            case LIST_PRODUCT -> listProductType(a);
            // list_append keeps the list's type when the appended
            // element speaks it (probed; a promoting append is a
            // different type — UNKNOWN, never guessed)
            case LIST_APPEND -> listAppendType(a);
            // list_reduce yields the lambda BODY's value (the running
            // accumulator — params bound by the attachment door;
            // probed: list_reduce([ints], +) -> INTEGER)
            case LIST_REDUCE -> a.size() == 2
                    && a.get(1) instanceof SqlExpr.Lambda lam
                    && lam.body().type() instanceof TypeFact.Typed bt
                    ? typed(bt.type()) : UNKNOWN;
            // map family (probed): concat keeps the uniform Map type;
            // entries [{first K, second V}] build MAP(K, V); extract/
            // values yield the VALUE list, keys the KEY list
            case MAP_CONCAT -> uniform(a, BOTTOM);
            case MAP_FROM_ENTRIES -> mapFromEntriesType(a);
            case MAP_EXTRACT, MAP_VALUES -> a.isEmpty() ? UNKNOWN
                    : a.get(0).type() instanceof TypeFact.Typed t
                            && t.type() instanceof SqlType.Map m
                    ? typed(new SqlType.Array(m.value())) : UNKNOWN;
            case MAP_KEYS -> a.isEmpty() ? UNKNOWN
                    : a.get(0).type() instanceof TypeFact.Typed t
                            && t.type() instanceof SqlType.Map m
                    ? typed(new SqlType.Array(m.key())) : UNKNOWN;
            // PROBED on the reference jar (DuckDB 1.5.0, 2026-08-25):
            // date_trunc returns TIMESTAMP at EVERY granularity —
            // day/month/year included. (The 1.4.4 CLI returns DATE for
            // day-and-coarser: a live version-skew trap; the rule is
            // written from the 1.5.0 JDBC probe, the executing backend.)
            // The NULL value propagates; H2's DATE delivery rides the
            // TIMESTAMP<-DATE subsumption arm at the wire.
            case DATE_TRUNC_DAY, DATE_TRUNC -> {
                if (a.isEmpty()) {
                    yield UNKNOWN;
                }
                yield a.stream().anyMatch(
                        x -> x.type() instanceof TypeFact.Bottom)
                        ? BOTTOM : T_TIMESTAMP;
            }
            case TO_VARIANT, JSON_MERGE_PATCH, VARIANT_GET -> T_JSON;
            case VARIANT_ELEMENTS ->
                    typed(new SqlType.Array(SqlType.Scalar.JSON));
            // string splitters yield VARCHAR[] by definition (probed
            // 1.5.0 — the XStore traderKerb CASE chain's blind leaf)
            case SPLIT, REGEXP_EXTRACT_ALL ->
                    typed(new SqlType.Array(SqlType.Scalar.VARCHAR));
            case RANGE_FN -> typed(new SqlType.Array(SqlType.Scalar.BIGINT));
            case REPEAT_VALUE -> !a.isEmpty() && a.get(0).type() instanceof TypeFact.Typed t0
                    ? typed(new SqlType.Array(t0.type())) : UNKNOWN;
            case COALESCE -> uniform(a, BOTTOM);
            case LIST_FILTER, LIST_SORT, LIST_SORT_DESC, LIST_TAIL,
                    LIST_INIT, LIST_SLICE, LIST_DISTINCT, LIST_REVERSE ->
                    a.isEmpty() ? UNKNOWN : arrayPass(a.get(0).type());
            case LIST_CONCAT -> uniform(a, BOTTOM);
            case LIST_GET, UNNEST ->
                    a.isEmpty() ? UNKNOWN : element(a.get(0).type());
            // PROBED DuckDB 1.5.0 (2026-08-25, pct-tail burn):
            // list_max/list_min are ELEMENT-PRESERVING (int->int,
            // varchar->varchar, date->date — the identity family)
            case LIST_MAX, LIST_MIN ->
                    a.isEmpty() ? UNKNOWN : element(a.get(0).type());
            // date/timestamp + INTERVAL -> TIMESTAMP at EVERY unit,
            // DATE input included (probed; the NULL value propagates)
            case ADD_INTERVAL, ADD_INTERVAL_TEMPORAL -> {
                if (a.isEmpty()) {
                    yield UNKNOWN;
                }
                yield a.stream().anyMatch(
                        x -> x.type() instanceof TypeFact.Bottom)
                        ? BOTTOM : T_TIMESTAMP;
            }
            // bit ops are WIDTH-PRESERVING on the integer family
            // (probed: INT&INT->INT, BIGINT|INT->BIGINT — the widest
            // member; non-integer operands have no bit story)
            case BIT_AND, BIT_OR, BIT_XOR, BIT_NOT, BIT_SHIFT_LEFT,
                    BIT_SHIFT_RIGHT -> bitOpType(a);
            // greatest/least follow the BRANCH-FAMILY promotion
            // (probed: equal kinds identity, BIGINT/INT -> BIGINT;
            // decimal mixes follow version formulas — stay UNKNOWN)
            case GREATEST, LEAST -> uniform(a, BOTTOM);
            case LIST_FLATTEN -> {
                if (a.isEmpty()) {
                    yield UNKNOWN;
                }
                yield a.get(0).type() instanceof TypeFact.Typed t
                        && t.type() instanceof SqlType.Array outer
                        && outer.element() instanceof SqlType.Array inner
                        ? typed(inner) : UNKNOWN;
            }
            // list_zip(a, b, truncate): pairs as UNNAMED structs whose
            // fields are addressed positionally ("1", "2" — the
            // StructGet numeric-field spelling); each side's element type
            case LIST_ZIP -> {
                if (a.size() < 2
                        || !(a.get(0).type() instanceof TypeFact.Typed ta)
                        || !(ta.type() instanceof SqlType.Array aa)
                        || !(a.get(1).type() instanceof TypeFact.Typed tb)
                        || !(tb.type() instanceof SqlType.Array ab)) {
                    yield UNKNOWN;
                }
                yield typed(new SqlType.Array(new SqlType.Struct(List.of(
                        new SqlType.Struct.Field("1", aa.element()),
                        new SqlType.Struct.Field("2", ab.element())))));
            }
            // struct_insert(s, 'name', v): s's fields plus the new one,
            // typed by the value (its declared slot when the value's own
            // fact is bottom)
            case STRUCT_INSERT -> {
                if (a.size() != 3 || !(a.get(0).type() instanceof TypeFact.Typed st)
                        || !(st.type() instanceof SqlType.Struct s)
                        || !(a.get(1) instanceof SqlExpr.StringLit name)) {
                    yield UNKNOWN;
                }
                SqlType vt = a.get(2).type() instanceof TypeFact.Typed vtt ? vtt.type()
                        : a.get(2).type() instanceof TypeFact.Bottom ? SqlType.Scalar.JSON : null;
                if (vt == null) {
                    yield UNKNOWN;
                }
                java.util.List<SqlType.Struct.Field> fs = new java.util.ArrayList<>(s.fields());
                fs.add(new SqlType.Struct.Field(name.value(), vt));
                yield typed(new SqlType.Struct(fs));
            }
            case LIST_TRANSFORM -> {
                if (a.size() != 2 || !(a.get(1) instanceof SqlExpr.Lambda lam)
                        || lam.params().size() != 1) {
                    yield UNKNOWN;
                }
                if (!(a.get(0).type() instanceof TypeFact.Typed lt)
                        || !(lt.type() instanceof SqlType.Array)) {
                    yield UNKNOWN;
                }
                // the lambda parameter is bound to the ELEMENT type by
                // the knowledge OWNER — the judge's rebind (M1) or the
                // typed Lambda node (M2); this rule only reads the body
                yield lam.body().type() instanceof TypeFact.Typed bt
                        ? typed(new SqlType.Array(bt.type())) : UNKNOWN;
            }
            // ARITHMETIC family — probed rules split to arithType
            // (method-size guard; receipts on each arm there)
            case PLUS, MINUS, TIMES, REM, MOD, NEGATE, ABS,
                    INT_DIVIDE, ROUND, CEILING, FLOOR, SIGN ->
                    arithType(fn, a);
            // error() RAISES — it yields no value and conforms to every
            // slot (§4bZ-U leg 3: the fourth TypeFact variant, replacing
            // uniform()'s structural ERROR skip)
            case ERROR -> RAISES;
            // everything else: no rule yet — counted, never guessed
            default -> UNKNOWN;
        };
    }

    /** §E3 M-N2 — THE SLOT-TRUTH REFINEMENT: a projection's delivered
     * nullability at ITS SELECT. A top-level {@link SqlAgg.Reducer}
     * under GROUP BY sits over non-empty groups by construction, so
     * the node's empty-group nullability drops and the slot keeps only
     * the operand-derived part (an all-NULL operand group still
     * reduces to NULL — probed 1.5.0). {@code LIST} refines to
     * NON-NULL outright (it collects NULLs into the array — probed
     * {@code [null]}). The SAMP moment family (stddev_samp/var_samp/
     * covar_samp and their aliases) does NOT refine — n&ge;2 required,
     * NULL on a one-row group (probed); the POP family yields 0.0 on
     * any non-empty group and refines. Consumers: the differential
     * census now (measurement), label adoption at M-N3 (one owner —
     * this function). */
    public static boolean slotNullable(SqlExpr pe, boolean grouped) {
        if (grouped && pe instanceof SqlAgg.Reducer r
                && r.type() instanceof TypeFact.Typed t && t.nullable()
                && groupRefines(r.fn())) {
            return r.fn() != SqlAgg.Fn.LIST && anyNullable(r.args());
        }
        return switch (pe.type()) {
            case TypeFact.Typed t -> t.nullable();
            case TypeFact.Raises r -> false;
            default -> true;
        };
    }

    /** Reducers whose empty-group NULL is their ONLY node-level null
     * source — under a non-empty-group proof they deliver a value
     * whenever their operands do (probed 1.5.0, M-N2 battery:
     * stddev_pop/var_pop/covar_pop on one row -> 0.0, corr -> NaN not
     * NULL, quantile_disc -> value; arg_max with an all-NULL key ->
     * NULL, covered by the operand arm). */
    private static boolean groupRefines(SqlAgg.Fn fn) {
        return switch (fn) {
            case SUM, AVG, MIN, MAX, ANY_VALUE, MODE, QUANTILE_DISC,
                    QUANTILE_CONT, MEDIAN, ARG_MAX, ARG_MIN, STRING_AGG,
                    BOOL_AND, BOOL_OR, LIST, STDDEV_POP, VAR_POP,
                    COVAR_POP, CORR -> true;
            default -> false;
        };
    }

    // ------------------------------------------------------------------
    // §E3-S — WHERE≡INNER PAD NEUTRALIZATION (the outer-to-inner join
    // simplification, label-level): a WHERE that NULL-REJECTS any
    // column of a join's padded side drops every padded row, so that
    // side's columns recover their DDL nullability. Applied by
    // SqlSelect's ctor to STAR-FRAMED joins only (no projections —
    // the frame's outputs are recomputable from the tree; projection
    // frames would fight fact adoption). The classifier is
    // CONSERVATIVE: only AND-decomposed conjuncts whose subtree
    // contains NO null-tolerant node count (a false proof would be a
    // breach — the EQUALITY-0 pin adjudicates every tightening).
    // ------------------------------------------------------------------

    /** The star-framed join's outputs with WHERE-neutralized pads:
     * recomputes per-column nullability from the JOIN TREE (each pad
     * edge applies unless the WHERE null-rejects that side), then
     * adopts it by name. Names are 1:1 for star frames (no prefix
     * renames); an unmatched name keeps its current claim. */
    static List<OutputCol> wherePadNeutralized(SqlSource.Join from,
            SqlExpr where, List<OutputCol> outputs) {
        java.util.Set<String> rejected = new java.util.HashSet<>();
        strictRejections(where, rejected);
        if (rejected.isEmpty()) {
            return outputs;
        }
        java.util.Map<String, Boolean> tree =
                new java.util.HashMap<>();
        treeNullability(from, rejected, tree);
        List<OutputCol> os = null;
        for (int i = 0; i < outputs.size(); i++) {
            OutputCol oc = outputs.get(i);
            Boolean nul = tree.get(oc.name());
            if (nul == null || nul == oc.nullable()) {
                continue;
            }
            if (os == null) {
                os = new java.util.ArrayList<>(outputs);
            }
            os.set(i, new OutputCol(oc.name(), oc.type(), nul, oc.origin()));
        }
        return os == null ? outputs : List.copyOf(os);
    }

    /** Per-name nullability of a join tree: leaves speak their own
     * outputs; each Join applies its pad to a side UNLESS the WHERE
     * null-rejects a column OF that side (no padded row survives the
     * filter, so the pad is vacuous — and inner pads neutralize the
     * same way, the WHERE spans the whole tree). */
    private static void treeNullability(SqlSource src,
            java.util.Set<String> rejected,
            java.util.Map<String, Boolean> out) {
        if (src instanceof SqlSource.Join j) {
            java.util.Map<String, Boolean> left = new java.util.HashMap<>();
            java.util.Map<String, Boolean> right = new java.util.HashMap<>();
            treeNullability(j.left(), rejected, left);
            treeNullability(j.right(), rejected, right);
            if (j.kind().padsLeft() && !intersects(rejected, left)) {
                left.replaceAll((k, v) -> true);
            }
            if (j.kind().padsRight() && !intersects(rejected, right)) {
                right.replaceAll((k, v) -> true);
            }
            out.putAll(left);
            out.putAll(right);
            return;
        }
        for (OutputCol c : src.outputs()) {
            out.put(c.name(), c.nullable());
        }
    }

    private static boolean intersects(java.util.Set<String> names,
            java.util.Map<String, Boolean> side) {
        for (String n : names) {
            if (side.containsKey(n)) {
                return true;
            }
        }
        return false;
    }

    /** The read-door surface of the classifier (the lowering's
     * resolution doors thread this into {@code Fold.sourceColumn} so
     * pad flips and frame outputs agree): the column names this WHERE
     * null-rejects; empty when there is no WHERE or no strict
     * conjunct. */
    public static java.util.Set<String> whereNullRejections(
            @com.legend.base.Nullable SqlExpr where) {
        if (where == null) {
            return java.util.Set.of();
        }
        java.util.Set<String> out = new java.util.HashSet<>();
        strictRejections(where, out);
        return out;
    }

    /** Collect the column NAMES null-rejected by this predicate:
     * AND/parenthesis decompose; a conjunct counts only when its whole
     * subtree is NULL-STRICT (a NULL input can never make it TRUE) —
     * any null-tolerant node (IS NULL, COALESCE, OR, CASE, the
     * null-safe comparison family) disqualifies the conjunct. */
    private static void strictRejections(SqlExpr where,
            java.util.Set<String> out) {
        if (where instanceof SqlExpr.Group g) {
            strictRejections(g.inner(), out);
            return;
        }
        if (where instanceof SqlExpr.Call c && c.fn() == SqlFn.AND) {
            for (SqlExpr a : c.args()) {
                strictRejections(a, out);
            }
            return;
        }
        if (nullStrict(where)) {
            columnNames(where, out);
        }
    }

    /** No node in this subtree can turn a NULL operand into TRUE.
     * Blacklist, conservative: the null tests, COALESCE, OR, CASE,
     * the null-safe/distinct comparison family, and boolean list
     * reductions (their empty/NULL edges are their own). Subqueries
     * contribute no outer columns (children() never descends), so
     * their presence is harmless. */
    private static boolean nullStrict(SqlExpr e) {
        if (e instanceof SqlExpr.Case) {
            return false;
        }
        if (e instanceof SqlExpr.Call c) {
            switch (c.fn()) {
                case IS_NULL, COALESCE, OR, NULL_SAFE_EQUAL,
                        NULL_SAFE_NOT_EQUAL, IS_DISTINCT_FROM, ALL_DISTINCT,
                        LIST_BOOL_AND, LIST_BOOL_OR -> {
                    return false;
                }
                default -> {
                }
            }
        }
        for (SqlExpr ch : e.children()) {
            if (!nullStrict(ch)) {
                return false;
            }
        }
        return true;
    }

    private static void columnNames(SqlExpr e, java.util.Set<String> out) {
        if (e instanceof SqlExpr.Column c) {
            out.add(c.name());
        }
        for (SqlExpr ch : e.children()) {
            columnNames(ch, out);
        }
    }

    /** {@link SqlExpr.Membership} — BOOLEAN, nullable when the needle
     * or the collection may be NULL (the node's own probed truth
     * table: NULL needle &rarr; NULL, NULL collection &rarr; NULL;
     * absent-from-non-empty is FALSE, never NULL). */
    static TypeFact membershipType(SqlExpr needle, SqlExpr collection) {
        return mayBeNull(needle) || mayBeNull(collection)
                ? nullable(T_BOOLEAN) : T_BOOLEAN;
    }

    /** {@link SqlExpr.Cast} — the target type; nullability TRANSPORTS
     * (§E3 charter table: a cast never creates or removes presence —
     * CAST(NULL AS T) is NULL). */
    static TypeFact castType(SqlExpr value, SqlType target) {
        TypeFact t = typed(target);
        return mayBeNull(value) ? nullable(t) : t;
    }

    /** The ARITHMETIC rule family (split from callType at the
     * 250-line method guard — same probed receipts, one seam). */
    private static TypeFact arithType(SqlFn fn, List<SqlExpr> a) {
        return switch (fn) {
            // ARITHMETIC PROMOTION (DuckDB 1.5.0 probed matrix,
            // 2026-08-24 — receipts in TYPED_SQL_IR.md): any DOUBLE
            // operand wins; an all-integer family promotes to its
            // widest member; DECIMAL operands follow version-specific
            // precision formulas and stay deliberately UNKNOWN; the
            // NULL value propagates (arithmetic is strict). PLUS/MINUS
            // also carry the probed date arms (DATE ± int -> DATE,
            // DATE - DATE -> BIGINT).
            case PLUS, MINUS -> {
                if (a.size() == 2) {
                    TypeFact l = a.get(0).type();
                    TypeFact r = a.get(1).type();
                    if (l instanceof TypeFact.Typed lt
                            && r instanceof TypeFact.Typed rt) {
                        boolean lDate = lt.type() == SqlType.Scalar.DATE;
                        boolean rDate = rt.type() == SqlType.Scalar.DATE;
                        if (lDate && rDate) {
                            yield fn == SqlFn.MINUS ? T_BIGINT : UNKNOWN;
                        }
                        if (lDate && integerKind(rt.type())
                                || rDate && fn == SqlFn.PLUS
                                        && integerKind(lt.type())) {
                            yield T_DATE;
                        }
                    }
                }
                TypeFact dec = decimalArith(false, a);
                yield dec != null ? dec : numericPromotion(a);
            }
            case TIMES -> {
                TypeFact dec = decimalArith(true, a);
                yield dec != null ? dec : numericPromotion(a);
            }
            // REM renders the bare MOD(a, b) — PROBED 1.5.0 (2026-08-25):
            // decimal-bearing pairs return the no-carry UNION shape
            // (DEC(3,1)%DEC(3,1)->DEC(3,1); DEC(18,6)%DEC(4,2)->DEC(18,6);
            // DEC(3,1)%INT->DEC(11,1); INT%DEC(4,2)->DEC(12,2)):
            // s=max(s1,s2), p=max(i1,i2)+s, cap 38. Pure MOD's emission
            // is the positive-mod COMPOSITE (MOD(MOD+b, b)) — its
            // decimal shape is unprobed and stays UNKNOWN.
            case REM -> {
                TypeFact dec = remDecimalType(a);
                yield dec != null ? dec : numericPromotion(a);
            }
            case MOD -> numericPromotion(a);
            case NEGATE, ABS -> {
                if (a.isEmpty()) {
                    yield UNKNOWN;
                }
                TypeFact f = a.get(0).type();
                if (f instanceof TypeFact.Bottom) {
                    yield BOTTOM;
                }
                yield f instanceof TypeFact.Typed t
                        && (integerKind(t.type())
                                || t.type() == SqlType.Scalar.DOUBLE
                                || t.type() instanceof SqlType.Decimal)
                        ? f : UNKNOWN;   // sign flip keeps the domain
            }
            case INT_DIVIDE -> {
                TypeFact p2 = numericPromotion(a);
                if (p2 instanceof TypeFact.Typed t
                        || p2 instanceof TypeFact.Bottom) {
                    yield p2;   // int//int keeps width (probed)
                }
                // any DOUBLE/DECIMAL operand: probed DOUBLE — but that
                // falls out of numericPromotion for DOUBLE; a decimal
                // operand stays UNKNOWN (unprobed corners)
                yield UNKNOWN;
            }
            case ROUND -> {
                if (a.isEmpty()) {
                    yield UNKNOWN;
                }
                TypeFact f = a.get(0).type();
                if (f instanceof TypeFact.Bottom) {
                    yield BOTTOM;
                }
                yield f instanceof TypeFact.Typed t
                        ? integerKind(t.type()) ? f
                                : t.type() == SqlType.Scalar.DOUBLE
                                        ? T_DOUBLE : UNKNOWN
                        : UNKNOWN;
            }
            // CEILING/FLOOR/SIGN type as OUR OWN EMISSION, not the bare
            // backend function: the renderer spells them CAST(... AS
            // BIGINT) for every input (AnsiSqlRenderer — pure's
            // ceiling/floor/sign : Integer contract), so the delivered
            // wire is BIGINT always. (The old DOUBLE/Decimal arms
            // described bare ceil() — a rule-vs-emission lie the §4bZ
            // guest-list audit exposed: 20 wire rows the deleted
            // blanket arm had been hiding.)
            case CEILING, FLOOR, SIGN -> {
                if (a.isEmpty()) {
                    yield UNKNOWN;
                }
                TypeFact f = a.get(0).type();
                if (f instanceof TypeFact.Bottom) {
                    yield BOTTOM;
                }
                yield f instanceof TypeFact.Typed t
                        && (integerKind(t.type())
                                || t.type() == SqlType.Scalar.DOUBLE
                                || t.type() instanceof SqlType.Decimal)
                        ? T_BIGINT : UNKNOWN;
            }
            default -> throw new IllegalStateException(
                    "non-arithmetic fn routed to arithType: " + fn);
        };
    }

    /** Bit ops (probed): width-preserving on the integer family. */
    private static TypeFact bitOpType(List<SqlExpr> a) {
        boolean bottom = false;
        int width = 0;
        for (SqlExpr e : a) {
            TypeFact f = e.type();
            if (f instanceof TypeFact.Bottom) {
                bottom = true;
            } else if (f instanceof TypeFact.Typed t
                    && integerKind(t.type())) {
                width = Math.max(width, intWidth(t.type()));
            } else {
                width = -1;
                break;
            }
        }
        if (width < 0 || a.isEmpty()) {
            return UNKNOWN;
        }
        if (bottom) {
            return BOTTOM;
        }
        return switch (width) {
            case 1 -> T_INTEGER;
            case 2 -> T_BIGINT;
            case 3 -> T_HUGEINT;
            default -> UNKNOWN;
        };
    }

    /** list_product -> DOUBLE for every numeric list (probed). */
    private static TypeFact listProductType(List<SqlExpr> a) {
        if (a.isEmpty()) {
            return UNKNOWN;
        }
        TypeFact f = a.get(0).type();
        if (f instanceof TypeFact.Bottom) {
            return BOTTOM;
        }
        return f instanceof TypeFact.Typed t
                && t.type() instanceof SqlType.Array ? T_DOUBLE : UNKNOWN;
    }

    /** list_append keeps the list's type when the appended element
     * speaks it (probed); a promoting append stays UNKNOWN. */
    private static TypeFact listAppendType(List<SqlExpr> a) {
        if (a.size() != 2 || !(a.get(0).type()
                instanceof TypeFact.Typed t
                && t.type() instanceof SqlType.Array at)) {
            return UNKNOWN;
        }
        TypeFact e2 = a.get(1).type();
        return e2 instanceof TypeFact.Bottom
                || (e2 instanceof TypeFact.Typed et
                        && et.type().equals(at.element()))
                ? typed(at) : UNKNOWN;
    }

    /** map_from_entries: [{first K, second V}] -> MAP(K, V) (probed). */
    private static TypeFact mapFromEntriesType(List<SqlExpr> a) {
        if (a.size() != 1 || !(a.get(0).type()
                instanceof TypeFact.Typed t
                && t.type() instanceof SqlType.Array at
                && at.element() instanceof SqlType.Struct st
                && st.fields().size() == 2)) {
            return UNKNOWN;
        }
        return typed(new SqlType.Map(st.fields().get(0).type(),
                st.fields().get(1).type()));
    }

    /** {@link SqlExpr.Case} — the branch family's shared type; a CASE
     * whose every branch is the NULL value is itself the NULL value.
     * §E3 nullability (the charter's CASE row): any nullable branch
     * &or; any Bottom branch &or; a MISSING ELSE (SQL: unmatched
     * &rarr; NULL). Conditions never contribute — an unmatched WHEN
     * falls through. */
    static TypeFact caseType(List<SqlExpr.Case.When> whens,
            @com.legend.base.Nullable SqlExpr otherwise) {
        java.util.List<SqlExpr> branches =
                new java.util.ArrayList<>(whens.size() + 1);
        for (SqlExpr.Case.When w : whens) {
            branches.add(w.then());
        }
        if (otherwise != null) {
            branches.add(otherwise);
        }
        TypeFact t = uniform(branches, BOTTOM);
        boolean nul = otherwise == null;
        for (SqlExpr b : branches) {
            nul = nul || mayBeNull(b);
        }
        return nul ? nullable(t) : t;
    }

    /** {@link SqlExpr.ArrayLit} — the uniform element type, wrapped.
     * An all-NULL literal array is a definite ARRAY value with an
     * unknowable element type — UNKNOWN, not bottom. */
    static TypeFact arrayLitType(List<SqlExpr> elements) {
        if (elements.isEmpty()) {
            return UNKNOWN;
        }
        return uniform(elements, UNKNOWN) instanceof TypeFact.Typed t
                ? typed(new SqlType.Array(t.type())) : UNKNOWN;
    }

    /** {@link SqlExpr.StructLit} — every field's type, in declared
     * order. A NULL-valued field (an absent optional property) takes
     * its builder-DECLARED slot type when supplied (§4bZ-U leg 2 —
     * the layout builder holds the class layout); an untypeable field
     * with no declaration leaves the layout partial. */
    static TypeFact structLitType(List<SqlExpr.StructLit.Field> fields) {
        java.util.List<SqlType.Struct.Field> fs =
                new java.util.ArrayList<>(fields.size());
        for (SqlExpr.StructLit.Field f : fields) {
            if (f.value().type() instanceof TypeFact.Typed t) {
                fs.add(new SqlType.Struct.Field(f.name(), t.type()));
            } else if (f.value().type() instanceof TypeFact.Bottom
                    && f.declared() != null) {
                fs.add(new SqlType.Struct.Field(f.name(), f.declared()));
            } else {
                return UNKNOWN;
            }
        }
        return typed(new SqlType.Struct(fs));
    }

    /** {@link SqlExpr.StructGet} — the named field of a typed struct;
     * extraction from the NULL value is the NULL value. */
    static TypeFact structGetType(SqlExpr source, String field) {
        TypeFact sv = source.type();
        if (sv instanceof TypeFact.Bottom) {
            return BOTTOM;
        }
        if (sv instanceof TypeFact.Typed t
                && t.type() instanceof SqlType.Struct s) {
            for (SqlType.Struct.Field f : s.fields()) {
                if (f.name().equals(field)) {
                    // §E3: a field's PRESENCE is not provable —
                    // Struct.Field carries no nullability dimension
                    // and an absent optional property IS a NULL field
                    // (structLitType's declared-slot arm). Safe side;
                    // per-field authority is a queued refinement.
                    return nullable(typed(f.type()));
                }
            }
        }
        return UNKNOWN;
    }

    /** {@link SqlExpr.ScalarSubquery} — the single output's DECLARED
     * label (builder knowledge carried on the subquery, read here).
     * A LABEL-LESS single-projection wrap (the scalar-position value
     * envelopes: outs=0, one Reducer/expr projection — 340 census
     * rows, 2026-08-25) reads the projection's own STORED type — the
     * tree's construction-time knowledge through the select, never a
     * re-derivation. */
    static TypeFact scalarSubqueryType(SqlQuery sub) {
        // §E3: a scalar subquery over ZERO rows is NULL (probed
        // 1.5.0: (select 1 where false) -> NULL) — nullable at the
        // node UNLESS the inner select PROVES exactly one row
        // (slack-census fix 1): an ungrouped single-aggregate select
        // yields its one row over ANY input, empty included (probed:
        // sum() over zero rows -> one NULL row), so the subquery adds
        // no nullability beyond the inner slot's own.
        boolean oneRow = oneRowAggregate(sub);
        if (sub.outputs().size() == 1) {
            TypeFact t = typed(sub.outputs().get(0).type());
            return oneRow && !sub.outputs().get(0).nullable()
                    ? t : nullable(t);
        }
        if (sub instanceof SqlSelect s && s.outputs().isEmpty()
                && s.projections().size() == 1
                && !(s.projections().get(0).expr() instanceof SqlExpr.Star)
                && !(s.projections().get(0).expr()
                        instanceof SqlExpr.StarExcept)) {
            TypeFact inner = s.projections().get(0).expr().type();
            return oneRow ? inner : nullable(inner);
        }
        return UNKNOWN;
    }

    /** The ONE-ROW PROOF: an ungrouped select that AGGREGATES returns
     * exactly one row regardless of input — provided nothing can drop
     * or skip that row (no HAVING, no QUALIFY, no OFFSET, no LIMIT 0).
     * §E3-S extension (slack burn): the aggregate may sit ANYWHERE in
     * a projection expression ({@code coalesce(string_agg(..), '')} —
     * the joinStrings envelope), not only at the root: SQL makes the
     * whole select single-row the moment one aggregate appears outside
     * a window. Everything else stays at the safe zero-rows-is-NULL
     * default. */
    private static boolean oneRowAggregate(SqlQuery sub) {
        if (!(sub instanceof SqlSelect s)
                || !s.groupBy().isEmpty()
                || s.having() != null || s.qualify() != null
                || s.offset() != null
                || (s.limit() != null && s.limit() < 1)) {
            return false;
        }
        for (SqlSelect.Projection p : s.projections()) {
            if (aggregateRooted(p.expr())) {
                return true;
            }
        }
        return false;
    }

    /** Does this expression AGGREGATE its select — a Reducer (or the
     * ordered/JSON aggregation nodes) outside any window or lambda?
     * Windowed aggregates keep per-row cardinality and never count;
     * expression walkers never descend into subqueries by the
     * children() contract. */
    private static boolean aggregateRooted(SqlExpr e) {
        if (e instanceof SqlAgg.Reducer
                || e instanceof SqlExpr.OrderedListAgg
                || e instanceof SqlExpr.JsonArrayAgg) {
            return true;
        }
        if (e instanceof SqlExpr.WindowCall
                || e instanceof SqlExpr.Lambda) {
            return false;
        }
        for (SqlExpr c : e.children()) {
            if (aggregateRooted(c)) {
                return true;
            }
        }
        return false;
    }

    /** {@link SqlExpr.CheckedOne} — the element of a definite list;
     * narrowing the NULL value flows the NULL value. */
    static TypeFact checkedOneType(SqlExpr list) {
        TypeFact lv = list.type();
        if (lv instanceof TypeFact.Bottom) {
            return BOTTOM;
        }
        // §E3: the exactly-one guard FLOWS the engine-noOp empty on a
        // 0/NULL input (the node's own D1 contract) — nullable always
        return lv instanceof TypeFact.Typed t
                && t.type() instanceof SqlType.Array at
                ? nullable(typed(at.element())) : UNKNOWN;
    }

    /** {@link SqlExpr.FoldCall} — the fold's value is the ACCUMULATOR's:
     * typed only when the lambda BODY (the per-step accumulator) and
     * the INIT agree — a type-changing fold (int seed, string result
     * mid-flight) or an unknown body stays UNKNOWN, never guessed.
     * The list-boxed lane ({@code accIsList}) delivers the acc's own
     * ARRAY (probed — see the arm). */
    static TypeFact foldType(SqlExpr source, SqlExpr.Lambda lambda,
            SqlExpr init, boolean accIsList) {
        // ONE agree-check for both lanes; the LIST-boxed lane
        // additionally requires the agreed type to BE an array — it
        // delivers the accumulator's own array (PROBED 1.5.0, §4bZ-U:
        // list_reduce over [e]-wrapped elements with a list acc ->
        // INTEGER[], the fold-collection-accumulator receipt). A
        // type-changing fold stays honestly UNKNOWN either way.
        TypeFact t = lambda.body().type() instanceof TypeFact.Typed bt
                && init.type() instanceof TypeFact.Typed it
                && bt.type().equals(it.type())
                && (!accIsList || bt.type() instanceof SqlType.Array)
                ? typed(bt.type()) : UNKNOWN;
        // §E3: a NULL source folds to NULL (probed 1.5.0:
        // list_reduce(NULL, f) -> NULL), an empty source folds to the
        // INIT, and every step is the BODY's value — the fold may be
        // null when any of the three may
        return mayBeNull(source) || mayBeNull(init)
                || mayBeNull(lambda.body()) ? nullable(t) : t;
    }

    /** {@link SqlExpr.WindowCall} — a windowed {@link SqlAgg.Reducer}
     * keeps its own promotion (probed: sum/avg/count/min OVER () match
     * the grouped results). Ranking kinds PROBED on the reference jar
     * (DuckDB 1.5.0, 2026-08-25): row_number/rank/dense_rank/ntile
     * &rarr; BIGINT; percent_rank/cume_dist &rarr; DOUBLE. Value kinds
     * (LAG/LEAD/FIRST/LAST/NTH) are element-preserving — the first
     * argument's stored type (a LAG default arg widens only within the
     * family; an unknown arg stays unknown, never guessed). */
    static TypeFact windowType(SqlAgg fn) {
        return switch (fn) {
            case SqlAgg.Reducer r -> r.type();
            case SqlAgg.RankingFn rf -> switch (rf.fn()) {
                case ROW_NUMBER, RANK, DENSE_RANK, NTILE -> T_BIGINT;
                case PERCENT_RANK, CUME_DIST -> T_DOUBLE;
                default -> UNKNOWN;
            };
            // §E3: the value kinds read outside the frame at its edges
            // (probed 1.5.0: lag over a one-row frame -> NULL; a
            // default arg narrows this only when provably reached —
            // unmodeled, the safe side stands)
            case SqlAgg.ValueFn vf -> vf.args().isEmpty()
                    ? UNKNOWN : nullable(vf.args().get(0).type());
        };
    }

    /**
     * {@link SqlAgg.Reducer} — THE AGGREGATE PROMOTION RULES (the
     * Slice-1 header's deferred rule, M1 task 2). Written from an
     * empirical probe of the reference backend (DuckDB 1.5.0,
     * 2026-08-24; every arm below is a probed fact, matrix in
     * TYPED_SQL_IR.md M1 receipts):
     * <ul>
     * <li>COUNT counts rows — BIGINT, argument type irrelevant;</li>
     * <li>STRING_AGG concatenates — VARCHAR for every input;</li>
     * <li>BOOL_AND/BOOL_OR — BOOLEAN;</li>
     * <li>the moment family (STDDEV/VAR samp+pop, VARIANCE, CORR,
     *     COVAR) — DOUBLE for every numeric input;</li>
     * <li>SUM widens: integer family (and BOOLEAN) &rarr; HUGEINT
     *     (the wire census's adopt-pending fact, now typed at the
     *     source), DOUBLE &rarr; DOUBLE, Decimal(p,s) &rarr;
     *     Decimal(38,s);</li>
     * <li>AVG &rarr; DOUBLE over numerics (Decimal included);
     *     temporals average to TIMESTAMP;</li>
     * <li>MIN/MAX/ANY_VALUE/MODE/QUANTILE_DISC/ARG_MAX/ARG_MIN are
     *     element-preserving — identity (LITERAL carriers flow, the
     *     F10 3b rule);</li>
     * <li>MEDIAN/QUANTILE_CONT interpolate: integers &rarr; DOUBLE,
     *     Decimal stays, temporals &rarr; TIMESTAMP, and MEDIAN alone
     *     is identity on VARCHAR/BOOLEAN; interpolation over a LITERAL
     *     carrier would break the spelling contract — UNKNOWN;</li>
     * <li>LIST collects — Array(input).</li>
     * </ul>
     * Everything else (the Lowerer-internal markers, unprobed inputs)
     * stays UNKNOWN — certainty only, never a guess.
     *
     * <p>§E3 M-N1: every reducer except COUNT is NULLABLE AT THE NODE
     * — probed 1.5.0 (2026-08-26): sum/min/avg/median/string_agg/
     * bool_and/list over ZERO rows &rarr; NULL, count &rarr; 0;
     * stddev_samp additionally needs n&ge;2 (NULL on a single row).
     * Refinement under a GROUP BY proof (SQL groups are non-empty by
     * construction) is the {@link SqlSelect} ctor's M-N2 arm — and the
     * moment family must KEEP its nullability there.
     */
    static TypeFact reducerType(SqlAgg.Fn fn, TypeFact arg0) {
        TypeFact base = reducerKind(fn, arg0);
        return fn == SqlAgg.Fn.COUNT ? base : nullable(base);
    }

    private static TypeFact reducerKind(SqlAgg.Fn fn, TypeFact arg0) {
        switch (fn) {
            case COUNT -> {
                return T_BIGINT;
            }
            case STRING_AGG -> {
                return T_VARCHAR;
            }
            case BOOL_AND, BOOL_OR -> {
                return T_BOOLEAN;
            }
            case STDDEV_SAMP, STDDEV_POP, VAR_SAMP, VAR_POP, STDDEV,
                    VARIANCE, CORR, COVAR_SAMP, COVAR_POP -> {
                return T_DOUBLE;
            }
            default -> {
            }
        }
        if (!(arg0 instanceof TypeFact.Typed t0)) {
            return UNKNOWN;
        }
        SqlType t = t0.type();
        return switch (fn) {
            case SUM -> {
                if (integerFamily(t) || t == SqlType.Scalar.BOOLEAN) {
                    yield T_HUGEINT;
                }
                if (t == SqlType.Scalar.DOUBLE) {
                    yield T_DOUBLE;
                }
                yield t instanceof SqlType.Decimal d
                        ? typed(new SqlType.Decimal(38, d.scale())) : UNKNOWN;
            }
            case AVG -> {
                if (integerFamily(t) || t == SqlType.Scalar.DOUBLE
                        || t instanceof SqlType.Decimal) {
                    yield T_DOUBLE;
                }
                yield t == SqlType.Scalar.DATE
                        || t == SqlType.Scalar.TIMESTAMP
                        ? T_TIMESTAMP : UNKNOWN;
            }
            // element-preserving: the ARG'S OWN FACT transports — the
            // engine-compat tolerance rides identity reducers (max of
            // raw carried values is a raw carried value, §4bZ)
            case MIN, MAX, ANY_VALUE, MODE, QUANTILE_DISC, ARG_MAX,
                    ARG_MIN -> t0;
            case MEDIAN, QUANTILE_CONT -> {
                if (integerFamily(t) || t == SqlType.Scalar.DOUBLE) {
                    yield T_DOUBLE;
                }
                if (t instanceof SqlType.Decimal) {
                    yield typed(t);
                }
                if (t == SqlType.Scalar.DATE
                        || t == SqlType.Scalar.TIMESTAMP) {
                    yield T_TIMESTAMP;
                }
                yield fn == SqlAgg.Fn.MEDIAN
                        && (t == SqlType.Scalar.VARCHAR
                                || t == SqlType.Scalar.BOOLEAN)
                        ? typed(t) : UNKNOWN;
            }
            case LIST -> typed(new SqlType.Array(t));
            default -> UNKNOWN;
        };
    }

    /** {@link SqlExpr.ReduceCollection} — the SAME aggregate promotion,
     * read through the collection carrier (probed: DuckDB's
     * {@code list_aggregate} matches the grouped aggregate's result
     * types element-wise). */
    static TypeFact reduceCollectionType(SqlAgg.Fn fn, SqlExpr collection) {
        TypeFact r = collection.type() instanceof TypeFact.Typed t
                && t.type() instanceof SqlType.Array at
                ? reducerType(fn, typed(at.element())) : UNKNOWN;
        // §E3: a NULL collection reduces to NULL even for COUNT
        // (probed 1.5.0: list_aggregate(NULL, 'count') -> NULL); the
        // empty-list NULLs already ride the reducer's node nullability
        return mayBeNull(collection) ? nullable(r) : r;
    }

    private static boolean integerFamily(SqlType t) {
        return t == SqlType.Scalar.BIGINT || t == SqlType.Scalar.INTEGER
                || t == SqlType.Scalar.HUGEINT;
    }

    /** An array-in/array-out passthrough (sort/filter/slice family);
     * transporting the NULL value flows the NULL value. */
    private static TypeFact arrayPass(TypeFact v) {
        if (v instanceof TypeFact.Bottom) {
            return BOTTOM;
        }
        return v instanceof TypeFact.Typed t
                && t.type() instanceof SqlType.Array ? v : UNKNOWN;
    }

    /** Element extraction (LIST_GET/UNNEST); extraction from the NULL
     * value is the NULL value. */
    private static TypeFact element(TypeFact v) {
        if (v instanceof TypeFact.Bottom) {
            return BOTTOM;
        }
        return v instanceof TypeFact.Typed t
                && t.type() instanceof SqlType.Array at
                ? typed(at.element()) : UNKNOWN;
    }

    private static boolean integerKind(SqlType t) {
        return t == SqlType.Scalar.INTEGER || t == SqlType.Scalar.BIGINT
                || t == SqlType.Scalar.HUGEINT;
    }

    /** DECIMAL arithmetic promotion — PROBED on the reference jar
     * (DuckDB 1.5.0 JDBC, 2026-08-25; 17 probe points, every one
     * reproduced by this rule; the 1.4.4 CLI differs — version skew is
     * live, always probe the executing jar). Applies only when a
     * DECIMAL operand is present (null = not applicable, the caller
     * falls to {@link #numericPromotion}): any DOUBLE operand wins;
     * the NULL value propagates (strict); integers enter the fold as
     * DECIMAL(10,0)/BIGINT (19,0)/HUGEINT (38,0). Pairwise LEFT fold:
     * PLUS/MINUS s=max(s1,s2), w=max(p1-s1,p2-s2)+s+1; TIMES s=s1+s2,
     * w=p1+p2 — each step capped at the operands' storage-class
     * boundary: 18 when BOTH operand widths &le;18 (the int64 class —
     * the carry digit never promotes across it: probed
     * (18,0)+(18,0)&rarr;(18,0)), else 38. */
    private static @com.legend.base.Nullable TypeFact decimalArith(
            boolean multiply, List<SqlExpr> a) {
        if (a.isEmpty()) {
            return null;
        }
        boolean sawDecimal = false;
        boolean bottom = false;
        boolean dbl = false;
        java.util.List<int[]> ds = new java.util.ArrayList<>(a.size());
        for (SqlExpr e : a) {
            TypeFact f = e.type();
            if (f instanceof TypeFact.Bottom) {
                bottom = true;
                continue;
            }
            if (!(f instanceof TypeFact.Typed t)) {
                return null;
            }
            SqlType ty = t.type();
            if (ty == SqlType.Scalar.DOUBLE) {
                dbl = true;
            } else if (ty instanceof SqlType.Decimal d) {
                sawDecimal = true;
                ds.add(new int[] {d.precision(), d.scale()});
            } else if (ty == SqlType.Scalar.INTEGER) {
                ds.add(new int[] {10, 0});
            } else if (ty == SqlType.Scalar.BIGINT) {
                ds.add(new int[] {19, 0});
            } else if (ty == SqlType.Scalar.HUGEINT) {
                ds.add(new int[] {38, 0});
            } else {
                return null;
            }
        }
        if (!sawDecimal) {
            return null;   // the int/double lattice owns it
        }
        if (bottom) {
            return BOTTOM;
        }
        if (dbl) {
            return T_DOUBLE;
        }
        int p = ds.get(0)[0];
        int s = ds.get(0)[1];
        for (int i = 1; i < ds.size(); i++) {
            int p2 = ds.get(i)[0];
            int s2 = ds.get(i)[1];
            int cap = p <= 18 && p2 <= 18 ? 18 : 38;
            int ns;
            int nw;
            if (multiply) {
                ns = s + s2;
                nw = p + p2;
            } else {
                ns = Math.max(s, s2);
                nw = Math.max(p - s, p2 - s2) + ns + 1;
            }
            p = Math.min(nw, cap);
            s = ns;
        }
        return typed(new SqlType.Decimal(p, s));
    }

    /** Binary/n-ary arithmetic promotion (the probed matrix): any
     * DOUBLE operand &rarr; DOUBLE; all-integer &rarr; the widest
     * member; a DECIMAL/temporal/unknown operand &rarr; UNKNOWN
     * (DuckDB's decimal precision arithmetic is version-specific —
     * decimal operands take the PROBED {@link #decimalArith} rule at
     * the PLUS/MINUS/TIMES call sites; MOD/REM decimals stay unprobed
     * corners); any NULL-value operand &rarr; the NULL value
     * (arithmetic is strict). */
    private static TypeFact numericPromotion(List<SqlExpr> a) {
        if (a.isEmpty()) {
            return UNKNOWN;
        }
        boolean bottom = false;
        boolean dbl = false;
        int width = 0;
        for (SqlExpr e : a) {
            TypeFact f = e.type();
            if (f instanceof TypeFact.Bottom) {
                bottom = true;
                continue;
            }
            if (!(f instanceof TypeFact.Typed t)) {
                return UNKNOWN;
            }
            SqlType ty = t.type();
            if (ty == SqlType.Scalar.DOUBLE) {
                dbl = true;
            } else if (ty == SqlType.Scalar.INTEGER) {
                width = Math.max(width, 1);
            } else if (ty == SqlType.Scalar.BIGINT) {
                width = Math.max(width, 2);
            } else if (ty == SqlType.Scalar.HUGEINT) {
                width = Math.max(width, 3);
            } else {
                return UNKNOWN;
            }
        }
        if (bottom) {
            return BOTTOM;   // NULL propagates through arithmetic
        }
        if (dbl) {
            return T_DOUBLE;
        }
        return switch (width) {
            case 1 -> T_INTEGER;
            case 2 -> T_BIGINT;
            case 3 -> T_HUGEINT;
            default -> UNKNOWN;
        };
    }

    /** The single shared type of a branch/argument family: BOTTOM
     * members are ADMISSIBLE anywhere and skipped; all typeable members
     * must agree exactly; an UNKNOWN member poisons. A family that is
     * entirely the NULL value resolves to {@code allBottom} — the
     * caller names what that means for its shape (the NULL value for
     * CASE/COALESCE, UNKNOWN for a literal array's element type). */
    private static TypeFact uniform(List<SqlExpr> es, TypeFact allBottom) {
        SqlType common = null;
        boolean saw = false;
        boolean emptyArray = false;
        for (SqlExpr e : es) {
            TypeFact v = e.type();
            if (v instanceof TypeFact.Bottom) {
                saw = true;
                continue;
            }
            // a RAISING member never yields a value — bottom-like in a
            // branch family: admissible anywhere, never the family's
            // type (witness: the checked-extract CASE, error-guard +
            // LIST_GET over Array(LITERAL), types LITERAL). The fact
            // rides the tree (TypeFact.Raises), not a structural match.
            if (v instanceof TypeFact.Raises) {
                saw = true;
                continue;
            }
            // the EMPTY array literal is pure's Nil — it conforms to
            // EVERY array type and constrains nothing in a branch
            // family (witness: concatenate's optional-prop lowering,
            // CASE WHEN … THEN [] ELSE [x] — 2026-08-25, the blind
            // leaf under the LIST_CONCAT/UNNEST untyped chains)
            if (e instanceof SqlExpr.ArrayLit al
                    && al.elements().isEmpty()) {
                saw = true;
                emptyArray = true;
                continue;
            }
            if (!(v instanceof TypeFact.Typed t)) {
                return UNKNOWN;
            }
            saw = true;
            if (common == null) {
                common = t.type();
            } else if (!common.equals(t.type())) {
                common = branchPromote(common, t.type());
                if (common == null) {
                    return UNKNOWN;
                }
            }
        }
        if (common != null) {
            // a skipped [] only conforms to an ARRAY family — an empty
            // array beside scalar members is a shape clash, never typed
            return !emptyArray || common instanceof SqlType.Array
                    ? typed(common) : UNKNOWN;
        }
        if (emptyArray) {
            // an all-[]/NULL family is an empty-ARRAY value with an
            // unknowable element — never the NULL value
            return UNKNOWN;
        }
        return saw ? allBottom : UNKNOWN;
    }

    /** BRANCH-FAMILY promotion (DuckDB 1.5.0 probed, 2026-08-24 —
     * CASE/COALESCE mixed members; the same lattice benefits ArrayLit
     * and LIST_CONCAT through this shared rule): widest integer wins;
     * any DOUBLE wins over the integer family AND over decimals;
     * DATE+TIMESTAMP promote to TIMESTAMP. DECIMAL union promotion
     * PROBED 2026-08-25 (mixed literal lists): s=max(s1,s2),
     * w=maxIntDigits+s — NO carry digit (union holds either value,
     * it never adds them: [Dec(2,1), BIGINT] -> Dec(20,1)) — capped
     * 38; ints enter as (10,0)/(19,0)/(38,0). Remaining cross-kind
     * pairs ERROR at execution — null (UNKNOWN), never guessed. */
    private static @com.legend.base.Nullable SqlType branchPromote(
            SqlType a, SqlType b) {
        if (integerKind(a) && integerKind(b)) {
            return intWidth(a) >= intWidth(b) ? a : b;
        }
        boolean aNum = integerKind(a) || a instanceof SqlType.Decimal;
        boolean bNum = integerKind(b) || b instanceof SqlType.Decimal;
        if (a == SqlType.Scalar.DOUBLE && bNum
                || b == SqlType.Scalar.DOUBLE && aNum) {
            return SqlType.Scalar.DOUBLE;
        }
        if (aNum && bNum
                && (a instanceof SqlType.Decimal
                        || b instanceof SqlType.Decimal)) {
            int[] da = unionDec(a);
            int[] db = unionDec(b);
            int s = Math.max(da[1], db[1]);
            int w = Math.min(
                    Math.max(da[0] - da[1], db[0] - db[1]) + s, 38);
            return new SqlType.Decimal(w, s);
        }
        if (a == SqlType.Scalar.DATE && b == SqlType.Scalar.TIMESTAMP
                || a == SqlType.Scalar.TIMESTAMP
                        && b == SqlType.Scalar.DATE) {
            return SqlType.Scalar.TIMESTAMP;
        }
        return null;
    }

    /** {@link SqlExpr.DecimalLit} — typed as THE EMISSION (the literal
     * renders {@code toPlainString()}): a scale-0 plain-digit literal
     * is read by the backend as its MAGNITUDE integer kind (probed
     * 1.5.0: {@code 17774} -> INTEGER, {@code 9999999999999999999} ->
     * HUGEINT; the old always-Decimal(p,0) fact was the wire ledger's
     * (10,3)<>(15,3) times family and the (19/20,0)<>HUGEINT
     * large-arithmetic family). Fractional literals keep their exact
     * {@code Decimal(p,s)}. */
    static TypeFact decimalLitType(java.math.BigDecimal v) {
        if (v.scale() < 0) {
            // 1E+3 has precision 1 but renders "1000" — normalize so
            // precision counts the RENDERED digits (the fact's
            // Decimal(1,0) would cast-overflow at execution)
            v = v.setScale(0);
        }
        if (v.scale() > 0) {
            // the fact counts the RENDERED digits (probed 1.5.0, B8:
            // 0.99 -> DECIMAL(3,2), 0.5 -> DECIMAL(2,1), 12.345 ->
            // DECIMAL(5,3) — a sub-1 literal's leading zero COUNTS, so
            // precision = integer digits + scale; BigDecimal.precision()
            // omits that zero and under-stated sub-1 facts by one, the
            // corr-literal wire divergence's root). Beyond DECIMAL's
            // max precision the backend reads the literal DOUBLE
            // (probed 1.5.0: 39+ rendered digits -> DOUBLE, 38 ->
            // DECIMAL(38,s) — testComplexPow's 41-digit expected
            // literal was the wire ledger's last row).
            int rendered = Math.max(v.precision() - v.scale(), 1) + v.scale();
            if (rendered > 38) {
                return T_DOUBLE;
            }
            return typed(new SqlType.Decimal(rendered, v.scale()));
        }
        // scale-0 SPLITS by provenance, decidable by MAGNITUDE alone:
        // a value beyond long can only be a big PURE INTEGER (integer
        // literals fitting long lower as IntLit, never here) — the
        // backend reads its bare digits HUGEINT (probed 1.5.0), so the
        // fact says so. Within long it is a d-suffixed pure DECIMAL
        // (17774d) — the contract is Decimal(p,0) and the EXECUTION
        // renderer conforms the emission with a decimal cast (bare
        // digits would read INTEGER — the (10,3)<>(15,3) times family;
        // a first cut typed the FACT by magnitude instead and flipped
        // percentile's mixed-kind carrier dispatch: facts drive
        // dispatch, so the fact must follow the CONTRACT and the
        // emission must follow the fact).
        int bits = v.toBigIntegerExact().bitLength();
        if (bits >= 128) {
            // beyond HUGEINT the backend reads the bare digits DOUBLE
            // (probed 1.5.0: 39- and 45-digit integer literals ->
            // DOUBLE), and a Decimal(p,0) fact would emit a cast past
            // DECIMAL's 38-precision cap
            return T_DOUBLE;
        }
        if (bits >= 64) {
            return T_HUGEINT;
        }
        return typed(new SqlType.Decimal(Math.max(v.precision(), 1), 0));
    }

    /** REM over a decimal-bearing numeric pair — the probed no-carry
     * union shape (see the {@code case REM} receipt). Null = not a
     * typed decimal-bearing numeric pair. */
    private static @com.legend.base.Nullable TypeFact remDecimalType(
            List<SqlExpr> a) {
        if (a.size() != 2
                || !(a.get(0).type() instanceof TypeFact.Typed l)
                || !(a.get(1).type() instanceof TypeFact.Typed r)) {
            return null;
        }
        boolean lDec = l.type() instanceof SqlType.Decimal;
        boolean rDec = r.type() instanceof SqlType.Decimal;
        if (!lDec && !rDec
                || !(lDec || integerKind(l.type()))
                || !(rDec || integerKind(r.type()))) {
            return null;
        }
        int[] da = unionDec(l.type());
        int[] db = unionDec(r.type());
        int s = Math.max(da[1], db[1]);
        int w = Math.min(Math.max(da[0] - da[1], db[0] - db[1]) + s, 38);
        return typed(new SqlType.Decimal(w, s));
    }

    private static int[] unionDec(SqlType t) {
        if (t instanceof SqlType.Decimal d) {
            return new int[] {d.precision(), d.scale()};
        }
        return new int[] {t == SqlType.Scalar.INTEGER ? 10
                : t == SqlType.Scalar.BIGINT ? 19 : 38, 0};
    }

    private static int intWidth(SqlType t) {
        return t == SqlType.Scalar.INTEGER ? 1
                : t == SqlType.Scalar.BIGINT ? 2 : 3;
    }
}
