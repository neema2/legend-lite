package com.legend.sql;

import java.math.BigDecimal;
import java.util.List;

/**
 * Scalar SQL expressions &mdash; sealed, immutable, data-only. Function calls
 * ({@link Call}) carry SEMANTIC names; the dialect owns the SQL spelling
 * (an unknown semantic name is a loud rendering error, never a fallback).
 */
public sealed interface SqlExpr
        permits SqlExpr.Column, SqlExpr.Star, SqlExpr.StarExcept, SqlExpr.StringLit, SqlExpr.IntLit,
                SqlExpr.FloatLit, SqlExpr.DecimalLit, SqlExpr.BoolLit, SqlExpr.NullLit,
                SqlExpr.DateLit, SqlExpr.TimestampLit, SqlExpr.FormatLit, SqlExpr.ArrayLit,
                SqlExpr.OrderedListAgg, SqlExpr.JsonArray,
                SqlExpr.StructLit, SqlExpr.StructGet, SqlExpr.Call,
                SqlExpr.Case, SqlExpr.Exists, SqlExpr.ScalarSubquery, SqlExpr.CheckedOne,
                SqlExpr.InSubquery, SqlExpr.Quantified,
                SqlExpr.CompactList,
                SqlExpr.DeferredTdsString, SqlExpr.WindowCall,
                SqlExpr.Lambda, SqlExpr.Cast, SqlExpr.FoldCall, SqlExpr.JsonObject,
                SqlExpr.JsonArrayAgg, SqlExpr.PlanParam, SqlExpr.Group,
                SqlExpr.RowOrder, SqlExpr.ReduceCollection, SqlExpr.Membership,
                SqlExpr.TempTableInSplice,
                SqlAgg.Reducer {

    /**
     * The DIRECT {@link SqlExpr} children, in rebuild order — the ONE
     * structural-recursion contract every expression walker shares. The
     * switch is EXHAUSTIVE with no default arm: a new variant fails
     * compilation HERE (and in {@link #withChildren}) until it declares
     * its children, and every walker inherits correct traversal.
     * Query-carrying nodes ({@link Exists}, {@link ScalarSubquery}) own
     * their inner traversal — expression walkers do not descend into
     * subqueries through this contract.
     */
    default List<SqlExpr> children() {
        return switch (this) {
            case Column ignored -> List.of();
            case RowOrder ignored -> List.of();
            case Membership m -> List.of(m.needle(), m.collection());
            case ReduceCollection rc -> {
                java.util.List<SqlExpr> out = new java.util.ArrayList<>();
                out.add(rc.collection());
                out.addAll(rc.extras());
                yield out;
            }
            case Star ignored -> List.of();
            case StarExcept ignored -> List.of();
            case StringLit ignored -> List.of();
            case IntLit ignored -> List.of();
            case FloatLit ignored -> List.of();
            case DecimalLit ignored -> List.of();
            case BoolLit ignored -> List.of();
            case NullLit ignored -> List.of();
            case DateLit ignored -> List.of();
            case TimestampLit ignored -> List.of();
            case FormatLit ignored -> List.of();
            case PlanParam ignored -> List.of();
            case TempTableInSplice ignored -> List.of();
            case Group g -> List.of(g.inner());
            case ArrayLit a -> a.elements();
            case OrderedListAgg o -> List.of(o.value(), o.orderBy());
            case StructLit s -> s.fields().stream()
                    .map(StructLit.Field::value).toList();
            case StructGet g -> List.of(g.source());
            case Call c -> c.args();
            case Case cs -> {
                java.util.List<SqlExpr> out = new java.util.ArrayList<>();
                for (Case.When w : cs.whens()) {
                    out.add(w.condition());
                    out.add(w.then());
                }
                if (cs.otherwise() != null) {
                    out.add(cs.otherwise());
                }
                yield out;
            }
            case Exists ignored -> List.of();
            case ScalarSubquery ignored -> List.of();
            case InSubquery i -> List.of(i.value());
            case Quantified q -> List.of(q.value());
            case CheckedOne co -> List.of(co.list());   // flags ride
            case CompactList cl -> List.of(cl.list());
            case DeferredTdsString ignored -> List.of();
            case WindowCall w -> {
                java.util.List<SqlExpr> out = new java.util.ArrayList<>();
                if (w.fn() instanceof SqlExpr fe) {
                    out.add(fe);
                }
                out.addAll(w.partitionBy());
                for (SqlSelect.SortKey k : w.orderBy()) {
                    out.add(k.expr());
                }
                yield out;
            }
            case Lambda l -> List.of(l.body());
            case Cast c -> List.of(c.value());
            case FoldCall f -> List.of(f.source(), f.lambda(), f.init());
            case JsonObject j -> j.kv();
            case JsonArray j -> j.elements();
            case JsonArrayAgg j -> {
                java.util.List<SqlExpr> out = new java.util.ArrayList<>();
                out.add(j.value());
                for (JsonArrayAgg.Key k : j.orderKeys()) {
                    out.add(k.expr());
                }
                yield out;
            }
            case SqlAgg.Reducer r -> {
                java.util.List<SqlExpr> out =
                        new java.util.ArrayList<>(r.args());
                for (SqlSelect.SortKey k : r.orderBy()) {
                    out.add(k.expr());
                }
                yield out;
            }
        };
    }

    /**
     * This node with its direct children replaced by {@code cs}
     * ({@code cs.size() == children().size()}, same order). The other
     * half of the recursion contract — see {@link #children()}.
     */
    default SqlExpr withChildren(List<SqlExpr> cs) {
        return switch (this) {
            case Column ignored -> this;
            case RowOrder ignored -> this;
            case ReduceCollection rc -> new ReduceCollection(rc.reducer(),
                    cs.get(0), cs.subList(1, cs.size()));
            case Membership m -> new Membership(cs.get(0), cs.get(1));
            case Star ignored -> this;
            case StarExcept ignored -> this;
            case StringLit ignored -> this;
            case IntLit ignored -> this;
            case FloatLit ignored -> this;
            case DecimalLit ignored -> this;
            case BoolLit ignored -> this;
            case NullLit ignored -> this;
            case DateLit ignored -> this;
            case TimestampLit ignored -> this;
            case FormatLit ignored -> this;
            case PlanParam ignored -> this;
            case TempTableInSplice ignored -> this;
            case Exists ignored -> this;
            case ScalarSubquery ignored -> this;
            case InSubquery i -> new InSubquery(cs.get(0), i.subquery());
            case Quantified q -> new Quantified(cs.get(0), q.comparison(), q.quantifier(), q.subquery());
            case CheckedOne co2 -> new CheckedOne(cs.get(0),
                    co2.scalarCarrier(), co2.atLeastOnly());
            case CompactList ignored -> new CompactList(cs.get(0));
            case DeferredTdsString ignored -> this;
            case Group ignored -> new Group(cs.get(0));
            case ArrayLit ignored -> new ArrayLit(cs);
            case OrderedListAgg ignored ->
                    new OrderedListAgg(cs.get(0), cs.get(1));
            case StructLit s -> {
                java.util.List<StructLit.Field> fs = new java.util.ArrayList<>();
                for (int i = 0; i < s.fields().size(); i++) {
                    fs.add(new StructLit.Field(s.fields().get(i).name(),
                            cs.get(i), s.fields().get(i).declared()));
                }
                yield new StructLit(fs);
            }
            case StructGet g -> new StructGet(cs.get(0), g.field());
            case Call c -> new Call(c.fn(), cs);
            case Case old -> {
                java.util.List<Case.When> ws = new java.util.ArrayList<>();
                int i = 0;
                for (int w = 0; w < old.whens().size(); w++) {
                    ws.add(new Case.When(cs.get(i), cs.get(i + 1)));
                    i += 2;
                }
                yield new Case(ws, old.otherwise() == null ? null : cs.get(i));
            }
            case WindowCall w -> {
                int base = w.fn() instanceof SqlExpr ? 1 : 0;
                SqlAgg fn = base == 1 ? (SqlAgg) cs.get(0) : w.fn();
                int np = w.partitionBy().size();
                java.util.List<SqlSelect.SortKey> ks = new java.util.ArrayList<>();
                for (int i = 0; i < w.orderBy().size(); i++) {
                    SqlSelect.SortKey k = w.orderBy().get(i);
                    ks.add(new SqlSelect.SortKey(cs.get(base + np + i),
                            k.ascending(), k.nullOrder(), k.outputName()));
                }
                yield new WindowCall(fn, cs.subList(base, base + np), ks,
                        w.frame());
            }
            // supplied-leaf knowledge (the builder's, like Column's
            // type): a body swap keeps it — recomputable by no rule
            case Lambda l -> new Lambda(l.params(), cs.get(0), l.type());
            case Cast c -> new Cast(cs.get(0), c.target(), c.conform());
            case FoldCall f -> new FoldCall(cs.get(0), (Lambda) cs.get(1),
                    cs.get(2), f.accIsList(), f.homogeneous());
            case JsonObject ignored -> new JsonObject(cs);
            case JsonArray ignored -> new JsonArray(cs);
            case JsonArrayAgg j -> {
                java.util.List<JsonArrayAgg.Key> ks = new java.util.ArrayList<>();
                for (int i = 0; i < j.orderKeys().size(); i++) {
                    ks.add(new JsonArrayAgg.Key(cs.get(1 + i),
                            j.orderKeys().get(i).desc()));
                }
                yield new JsonArrayAgg(cs.get(0), ks);
            }
            case SqlAgg.Reducer r -> {
                int na = r.args().size();
                java.util.List<SqlSelect.SortKey> ks = new java.util.ArrayList<>();
                for (int i = 0; i < r.orderBy().size(); i++) {
                    SqlSelect.SortKey k = r.orderBy().get(i);
                    ks.add(new SqlSelect.SortKey(cs.get(na + i), k.ascending(),
                            k.nullOrder(), k.outputName()));
                }
                yield new SqlAgg.Reducer(r.fn(), cs.subList(0, na),
                        r.distinct(), ks);
            }
        };
    }

    /**
     * Identity-preserving one-level rewrite: {@code f} over each direct
     * child, reassembled through {@link #withChildren} only when a child
     * actually changed. Walkers recurse by calling this from their own
     * default arm — a variant they do not special-case still traverses
     * correctly by construction.
     */
    default SqlExpr mapChildren(java.util.function.UnaryOperator<SqlExpr> f) {
        List<SqlExpr> cs = children();
        if (cs.isEmpty()) {
            return this;
        }
        java.util.List<SqlExpr> rw = new java.util.ArrayList<>(cs.size());
        boolean same = true;
        for (SqlExpr c : cs) {
            SqlExpr r = f.apply(c);
            same = same && r == c;
            rw.add(r);
        }
        return same ? this : withChildren(rw);
    }

    /**
     * The expression's TYPE FACT (TYPED_SQL_IR.md §2) — {@code Typed},
     * {@code Bottom} (the NULL value) or {@code Unknown} — STORED as a
     * record component on every node, immutable with the tree.
     * Compositions compute it ONCE in the canonical constructor from
     * their children's stored verdicts via the {@link SqlTyping} rule
     * table (the compact constructor recomputes, so the component
     * cannot lie); leaves are SUPPLIED by the builder (M1: Column and
     * Lambda default UNKNOWN — their stamping is M2, and the UNKNOWN
     * count ratchets down). The prior constructor arities remain as
     * compute-and-delegate doors, so construction sites change zero
     * times; their placeholder argument is overwritten by the compact
     * constructor wherever the type is derivable.
     */
    TypeFact type();

    /** A column reference, optionally qualified by a source alias. */
    /** {@code table} null = unqualified reference (lambda params,
     * pivot args, post-unqualify rewrites). */
    /** The backend's PHYSICAL ROW-ORDER pseudo-column — the determinism
     * key for insertion-ordered aggregation (joinStrings parity). Spelled
     * per dialect (DuckDB {@code rowid}, H2 {@code _ROWID_}); a plain
     * Column would bake one backend's spelling into the IR. */
    record RowOrder(@com.legend.Nullable String table,
            TypeFact type) implements SqlExpr {
        public RowOrder {
            type = SqlTyping.T_BIGINT;
        }

        public RowOrder(@com.legend.Nullable String table) {
            this(table, SqlTyping.UNKNOWN);
        }
    }

    /** SEMANTIC collection reduction (CARRIER_REDESIGN.md R1): reduce a
     * collection VALUE with the named ANSI aggregate ({@code string_agg},
     * {@code quantile_cont}, {@code var_samp}, ...); {@code extras} are
     * the aggregate's trailing arguments (separator, quantile). The
     * dialect strategy layer owns the emission — DuckDB's native
     * {@code list_aggregate}, or the portable FUSION into the collecting
     * subselect (the engine's shape). No backend spelling lives here. */
    record ReduceCollection(SqlAgg.Fn reducer, SqlExpr collection,
            java.util.List<SqlExpr> extras,
            TypeFact type) implements SqlExpr {
        public ReduceCollection {
            extras = java.util.List.copyOf(extras);
            type = SqlTyping.reduceCollectionType(reducer, collection);
        }

        public ReduceCollection(SqlAgg.Fn reducer, SqlExpr collection,
                java.util.List<SqlExpr> extras) {
            this(reducer, collection, extras, SqlTyping.UNKNOWN);
        }
    }

    /** SEMANTIC collection membership (CARRIER_REDESIGN.md R2):
     * {@code needle} in the collection VALUE. NULL TRUTH TABLE (probed
     * DuckDB 1.5 list_contains, the reference semantics): NULL needle
     * -> NULL; needle absent (even with NULL elements) -> FALSE; needle
     * present -> TRUE; empty collection -> FALSE. SQL {@code IN}
     * differs ONLY when a NULL element exists and the needle is absent
     * (NULL, not FALSE) — indistinguishable in filter position; the
     * portable rule CASE-wraps in the NULL-element-literal case. The
     * collection may be a PlanParam at bind time (§2). */
    record Membership(SqlExpr needle, SqlExpr collection,
            TypeFact type) implements SqlExpr {
        public Membership {
            type = SqlTyping.membershipType(needle, collection);
        }

        public Membership(SqlExpr needle, SqlExpr collection) {
            this(needle, collection, SqlTyping.UNKNOWN);
        }
    }

    record Column(@com.legend.Nullable String table, String name,
            TypeFact type, OutputCol.@com.legend.Nullable Origin origin)
            implements SqlExpr {
        /** M1 leaf default — the builder supplies the type in M2. */
        public Column(@com.legend.Nullable String table, String name) {
            this(table, name, SqlTyping.UNKNOWN, null);
        }


        /** An UNTYPED reference to a name the query INVENTED (alias,
         * projection label) — origin stamped, type unknown (M1). */
        public static Column derived(@com.legend.Nullable String table,
                String name) {
            return new Column(table, name, SqlTyping.UNKNOWN,
                    OutputCol.Origin.DERIVED);
        }

        /** An UNTYPED reference to a name that exists in DDL — origin
         * stamped, type unknown (M1). */
        public static Column physical(@com.legend.Nullable String table,
                String name) {
            return new Column(table, name, SqlTyping.UNKNOWN,
                    OutputCol.Origin.PHYSICAL);
        }

        /** The STAMPED reference to a source's declared output — the M2
         * leaf-supply door (TYPED_SQL_IR.md §2): the builder always has
         * the {@link OutputCol} in hand, and the reference carries its
         * declared type. A TOLERATED slot's re-read keeps the tag
         * (§4bZ — the engine-compat provenance rides stamped reads up
         * through select layers, so the FINAL plan's outputs still
         * carry it for the wire census). */
        public static Column of(@com.legend.Nullable String table,
                OutputCol col) {
            // §E3 M-N1 leaf input: the frame's OWN nullable label —
            // today the pure-multiplicity echo; the DDL/join-pad
            // authority replaces the echo at M-N2 and this door
            // transports it unchanged
            return new Column(table, col.name(), new TypeFact.Typed(
                    col.type(), col.nullable(), col.tolerated()),
                    col.origin());
        }

        /** Stamped reference by bare name — for the builder's OWN
         * synthetic columns, whose type it just declared. §E3: the
         * caller states the slot's nullability with the same authority
         * it states the type (no default — every site decides). */
        public static Column of(@com.legend.Nullable String table,
                String name, SqlType t, boolean nullable,
                OutputCol.Origin origin) {
            return new Column(table, name,
                    new TypeFact.Typed(t, nullable, false), origin);
        }

        /** Stamped when {@code outs} claims the name, plain (UNKNOWN)
         * otherwise — the lookup door for callers holding a source's
         * declared output list. */
        public static Column of(@com.legend.Nullable String table,
                List<OutputCol> outs, String name) {
            return outs.stream().filter(c -> c.name().equals(name))
                    .findFirst().map(oc -> of(table, oc))
                    .orElseGet(() -> new Column(table, name));
        }

        /** §E3 M-N2 — the JOIN-PAD door: this reference with its fact
         * marked may-be-null (a read resolved from a NULL-padded join
         * side may be NULL regardless of the column's DDL). Identity
         * when the fact already says so or makes no claim. */
        public Column asNullable() {
            if (type instanceof TypeFact.Typed t && !t.nullable()) {
                return new Column(table, name, new TypeFact.Typed(
                        t.type(), true, t.tolerated()), origin);
            }
            return this;
        }

        /** A LAMBDA-PARAMETER reference, stamped MECHANICALLY as the
         * element of the collection the lambda ranges over (M3 slice 0
         * — the LIST_TRANSFORM/FILTER param-binding knowledge, supplied
         * by the builder that holds the collection). UNKNOWN unless the
         * collection's stored type is a definite array — never a
         * hand-reasoned guess. NOT for fold accumulators (their type is
         * not the element's). */
        public static Column param(String name, SqlExpr collection) {
            // §E3: element PRESENCE is not provable from Array(T) —
            // SqlType carries no element-nullability dimension, and a
            // carrier physically holds SQL NULLs until compaction
            // (CompactList's own reason to exist) — so the param
            // stamps may-be-null, the safe side
            return collection.type() instanceof TypeFact.Typed t
                    && t.type() instanceof SqlType.Array at
                    ? new Column(null, name,
                            new TypeFact.Typed(at.element(), true, false),
                            OutputCol.Origin.DERIVED)
                    : derived(null, name);
        }
    }

    /** {@code *} or {@code alias.*}. */
    /** {@code alias.* EXCLUDE (a, b)} — the star minus named columns (pivot key synthesis). */
    record StarExcept(@com.legend.Nullable String table, List<String> except,
            TypeFact type) implements SqlExpr {
        public StarExcept {
            except = List.copyOf(except);
            type = SqlTyping.UNKNOWN;   // not a scalar value
        }

        public StarExcept(@com.legend.Nullable String table,
                List<String> except) {
            this(table, except, SqlTyping.UNKNOWN);
        }
    }

    /** {@code table} null = unqualified {@code *}. */
    record Star(@com.legend.Nullable String table,
            TypeFact type) implements SqlExpr {
        public Star {
            type = SqlTyping.UNKNOWN;   // not a scalar value
        }

        public Star(@com.legend.Nullable String table) {
            this(table, SqlTyping.UNKNOWN);
        }
    }

    record StringLit(String value, TypeFact type)
            implements SqlExpr {
        public StringLit {
            type = SqlTyping.T_VARCHAR;
        }

        public StringLit(String value) {
            this(value, SqlTyping.UNKNOWN);
        }
    }

    record IntLit(long value, TypeFact type) implements SqlExpr {
        public IntLit {
            type = SqlTyping.T_BIGINT;
        }

        public IntLit(long value) {
            this(value, SqlTyping.UNKNOWN);
        }
    }

    record FloatLit(double value, TypeFact type)
            implements SqlExpr {
        public FloatLit {
            type = SqlTyping.T_DOUBLE;
        }

        public FloatLit(double value) {
            this(value, SqlTyping.UNKNOWN);
        }
    }

    record DecimalLit(BigDecimal value, TypeFact type)
            implements SqlExpr {
        public DecimalLit {
            type = SqlTyping.decimalLitType(value);
        }

        public DecimalLit(BigDecimal value) {
            this(value, SqlTyping.UNKNOWN);
        }
    }

    record BoolLit(boolean value, TypeFact type)
            implements SqlExpr {
        public BoolLit {
            type = SqlTyping.T_BOOLEAN;
        }

        public BoolLit(boolean value) {
            this(value, SqlTyping.UNKNOWN);
        }
    }

    record NullLit(TypeFact type) implements SqlExpr {
        public NullLit {
            type = SqlTyping.BOTTOM;   // the NULL value — bottom
        }

        public NullLit() {
            this(SqlTyping.UNKNOWN);
        }
    }

    /** ISO {@code yyyy-MM-dd}; renders as a typed DATE literal. */
    record DateLit(String iso, TypeFact type) implements SqlExpr {
        public DateLit {
            type = SqlTyping.T_DATE;
        }

        public DateLit(String iso) {
            this(iso, SqlTyping.UNKNOWN);
        }
    }

    /** ISO timestamp; renders as a typed TIMESTAMP literal. */
    /** An EXPLICIT parenthesization group — the engine's {@code group}
     * dynafunction (extensionDefaults.pure:224, format '(%s)'). Parens
     * are STRUCTURAL, never derived from operator arity: the engine
     * emits group when an and/or nests under the OPPOSITE operator and
     * when a predicate merges with its null-guards under or/not
     * (pureToSQLQuery newAndOrDynaFunctionRelaxedBrackets:5376,
     * moveExtraFilterToFilter:4610). */
    record Group(SqlExpr inner, TypeFact type) implements SqlExpr {
        public Group {
            type = inner.type();   // parens are structural — transport
        }

        public Group(SqlExpr inner) {
            this(inner, SqlTyping.UNKNOWN);
        }
    }

    /** An execution-plan TEMPLATE parameter ({@code ${name}} — the
     * engine's freemarker placeholder for a function parameter or an
     * Allocation-bound variable). Plan-text vocabulary only: it renders
     * through the engine-style dialect and is a loud error in any
     * executable dialect. */
    record PlanParam(String name, Kind kind, boolean optional,
            @com.legend.Nullable String enumMapFn,
            TypeFact type) implements SqlExpr {
        /** {@code RAW} splices {@code ${name}} bare — the temp-table IN
         * protocol's {@code inFilterClause_X} wrapper variable
         * (processInOperation.pure); plan-text vocabulary only. */
        public enum Kind { STRING, DATE, DATETIME, FLOAT, BOOLEAN, ENUM,
            OTHER, RAW }

        public PlanParam {
            type = SqlTyping.UNKNOWN;   // plan-text vocabulary — no rule
        }

        public PlanParam(String name, Kind kind, boolean optional,
                @com.legend.Nullable String enumMapFn) {
            this(name, kind, optional, enumMapFn, SqlTyping.UNKNOWN);
        }

        public PlanParam(String name, Kind kind, boolean optional) {
            this(name, kind, optional, null);
        }

        public PlanParam(String name, Kind kind) {
            this(name, kind, false);
        }

        public PlanParam(String name, boolean stringTyped) {
            this(name, stringTyped ? Kind.STRING : Kind.OTHER, false);
        }
    }

    /** A TYPED date format — a list of {@link DateFmt} parts, never a
     * C-format string a renderer must re-parse (remediation T3.2). Rides
     * as the format argument of STRFTIME/STRPTIME. */
    record FormatLit(List<DateFmt> parts, TypeFact type)
            implements SqlExpr {
        public FormatLit {
            parts = List.copyOf(parts);
            type = SqlTyping.UNKNOWN;   // a format ride-along, not a value
        }

        public FormatLit(List<DateFmt> parts) {
            this(parts, SqlTyping.UNKNOWN);
        }
    }

    record TimestampLit(String iso, TypeFact type)
            implements SqlExpr {
        public TimestampLit {
            type = SqlTyping.T_TIMESTAMP;
        }

        public TimestampLit(String iso) {
            this(iso, SqlTyping.UNKNOWN);
        }
    }

    /** A list literal, {@code [a, b, c]} in DuckDB. */
    /** {@code list(value ORDER BY key)} — identity-preserving ordered aggregation. */
    record OrderedListAgg(SqlExpr value, SqlExpr orderBy,
            TypeFact type) implements SqlExpr {
        public OrderedListAgg {
            // §E3: an ordered aggregation over ZERO rows is NULL (the
            // reducer node rule — probed string_agg/list empty -> NULL)
            type = SqlTyping.nullable(SqlTyping.T_VARCHAR);
        }

        public OrderedListAgg(SqlExpr value, SqlExpr orderBy) {
            this(value, orderBy, SqlTyping.UNKNOWN);
        }
    }

    record ArrayLit(List<SqlExpr> elements, TypeFact type)
            implements SqlExpr {
        public ArrayLit {
            type = SqlTyping.arrayLitType(elements);
        }

        public ArrayLit(List<SqlExpr> elements) {
            this(elements, SqlTyping.UNKNOWN);
        }
    }

    /**
     * A named-field composite literal ({@code {'f': v, …}} in DuckDB). Field
     * ORDER is the emitting frontend's declared layout — load-bearing, never
     * inferred from the value set.
     */
    record StructLit(List<Field> fields, TypeFact type)
            implements SqlExpr {
        public StructLit {
            fields = List.copyOf(fields);
            type = SqlTyping.structLitType(fields);
        }

        public StructLit(List<Field> fields) {
            this(fields, SqlTyping.UNKNOWN);
        }

        /** {@code declared}: the field's DECLARED SQL type, supplied by
         * a builder that holds the class layout (§4bZ-U leg 2 — the M2
         * leaf-supply pattern applied to struct fields): a NULL-valued
         * field (an absent optional property) then still contributes
         * its slot type to {@link SqlTyping#structLitType}. Null when
         * the builder has no layout in hand (zip's pair synthesis). */
        public record Field(String name, SqlExpr value,
                @com.legend.Nullable SqlType declared) {
            public Field(String name, SqlExpr value) {
                this(name, value, null);
            }
        }
    }

    /** Field extraction from a composite value ({@code struct_extract(x, 'f')} in DuckDB). */
    record StructGet(SqlExpr source, String field,
            TypeFact type) implements SqlExpr {

        /** Construction-time fold: reading a named field off a LITERAL
         * struct is that field's value — extraction scaffolding over a
         * just-built literal erases (and with it the blind-typing hole:
         * a literal pair holding an untyped NULL member made the whole
         * struct — and every extraction from it — UNKNOWN; the folded
         * value keeps its own honest fact). A missing field keeps the
         * StructGet: loud downstream, never a silent null. */
        public static SqlExpr of(SqlExpr source, String field) {
            if (source instanceof StructLit sl) {
                for (StructLit.Field f : sl.fields()) {
                    if (f.name().equals(field)) {
                        return f.value();
                    }
                }
            }
            return new StructGet(source, field);
        }
        public StructGet {
            type = SqlTyping.structGetType(source, field);
        }

        public StructGet(SqlExpr source, String field) {
            this(source, field, SqlTyping.UNKNOWN);
        }
    }

    /** A function application by SEMANTIC vocabulary entry (see {@link SqlFn}). */
    record Call(SqlFn fn, List<SqlExpr> args, TypeFact type)
            implements SqlExpr {
        public Call {
            type = SqlTyping.callType(fn, args);
        }

        public Call(SqlFn fn, List<SqlExpr> args) {
            this(fn, args, SqlTyping.UNKNOWN);
        }

        public static Call of(SqlFn fn, SqlExpr... args) {
            return new Call(fn, List.of(args));
        }
    }

    /** {@code CASE WHEN ... THEN ... [WHEN ...] ELSE ... END}. */
    /** {@code otherwise} null = no ELSE branch (SQL semantics: NULL). */
    record Case(List<When> whens, @com.legend.Nullable SqlExpr otherwise,
            TypeFact type) implements SqlExpr {
        public Case {
            type = SqlTyping.caseType(whens, otherwise);
        }

        public Case(List<When> whens,
                @com.legend.Nullable SqlExpr otherwise) {
            this(whens, otherwise, SqlTyping.UNKNOWN);
        }

        public record When(SqlExpr condition, SqlExpr then) {
        }
    }

    /** {@code EXISTS (subquery)} &mdash; Boolean-composable association predicate. */
    record Exists(SqlQuery subquery, TypeFact type)
            implements SqlExpr {
        public Exists {
            type = SqlTyping.T_BOOLEAN;
        }

        public Exists(SqlQuery subquery) {
            this(subquery, SqlTyping.UNKNOWN);
        }
    }

    /** {@code value IN (subquery)} &mdash; relation::in: the searched column is
     *  a single-column subquery (its nulls dropped inside it by the lowering, so
     *  the predicate is two-valued, the Pure body's answer). */
    record InSubquery(SqlExpr value, SqlQuery subquery, TypeFact type)
            implements SqlExpr {
        public InSubquery {
            type = SqlTyping.T_BOOLEAN;
        }

        public InSubquery(SqlExpr value, SqlQuery subquery) {
            this(value, subquery, SqlTyping.UNKNOWN);
        }
    }

    /** SQL's quantifier over a subquery. */
    enum Quantifier { ANY, ALL }

    /** {@code value <comparison> ANY|ALL (subquery)} &mdash; the ten quantified
     *  comparisons of relation quantification; {@code comparison} is one of the
     *  six infix comparison operators. */
    record Quantified(SqlExpr value, SqlFn comparison, Quantifier quantifier,
            SqlQuery subquery, TypeFact type) implements SqlExpr {
        public Quantified {
            type = SqlTyping.T_BOOLEAN;
        }

        public Quantified(SqlExpr value, SqlFn comparison, Quantifier quantifier, SqlQuery subquery) {
            this(value, comparison, quantifier, subquery, SqlTyping.UNKNOWN);
        }
    }

    /** A single-value subquery in scalar position. */
    record ScalarSubquery(SqlQuery subquery, TypeFact type)
            implements SqlExpr {
        public ScalarSubquery {
            type = SqlTyping.scalarSubqueryType(subquery);
        }

        public ScalarSubquery(SqlQuery subquery) {
            this(subquery, SqlTyping.UNKNOWN);
        }
    }

    /** CHECKED NARROWING over a definite list value — pure's toOne as
     * ONE semantic node with per-dialect spellings (the guard-emission
     * design, STAMP_DISCIPLINE_PROGRAM D1): execution dialects spell
     * the size guard (>1 raises pure's "Cannot cast a collection of
     * size N to multiplicity [1]", 1 yields the element, 0/NULL flows
     * the engine-noOp empty); the engine-TEXT channel renders the
     * INNER value verbatim (the engine's processNoOp view — the
     * NULLS-suppression precedent). */
    record CheckedOne(SqlExpr list, boolean scalarCarrier,
            boolean atLeastOnly, TypeFact type) implements SqlExpr {
        public CheckedOne {
            // toOneMany (atLeastOnly) KEEPS the collection — the value
            // is the guarded list itself, so its fact transports; only
            // the exactly-one narrowing yields the ELEMENT (§4bZ-U
            // leg 2: the egress boxing exposed the element-typed fact
            // over a list-shaped wire as a label lie)
            type = atLeastOnly ? list.type()
                    : SqlTyping.checkedOneType(list);
        }

        public CheckedOne(SqlExpr list, boolean scalarCarrier,
                boolean atLeastOnly) {
            this(list, scalarCarrier, atLeastOnly, SqlTyping.UNKNOWN);
        }

        /** The original exactly-one LIST form. */
        public CheckedOne(SqlExpr list) {
            this(list, false, false);
        }
    }

    /** PURE-COLLECTION carrier compaction (shortcut audit §5, value
     * lane): strips SQL NULL elements from a list carrier — a pure
     * collection holds no empties, and a NULL in a value-lane carrier
     * can only MEAN an empty (values of non-variant types are never
     * carried null; a variant/Any JSON null decays to empty). SEMANTIC
     * node: the dialect renderer owns the list-function spelling
     * (carrier purity ratchet — the CheckedOne/D1 precedent). */
    record CompactList(SqlExpr list, TypeFact type)
            implements SqlExpr {
        public CompactList {
            type = list.type();   // compaction is carrier-preserving
        }

        public CompactList(SqlExpr list) {
            this(list, SqlTyping.UNKNOWN);
        }
    }

    /** A relation-toString whose COLUMN LIST is dynamic (a pivot with
     * runtime-discovered keys): the '#TDS' text cannot compose at
     * lowering — the EXECUTION BOUNDARY resolves it (DynamicPivot's
     * two-phase discipline; the lowering layer records the typed
     * schema by {@code id}). Types-free by design — the sql package
     * never carries compiler types. A node reaching a renderer is a
     * routing bug and walls loudly there. */
    record DeferredTdsString(SqlSelect inner, String alias, int id,
            Form form, boolean renderTdsNull,
            TypeFact type) implements SqlExpr {
        /** The relation TEXT form the boundary composes (batch 82: a
         * LATE-BOUND raw grid — executeInDbToTDS — defers its toCSV the
         * same way a pivot inner defers its '#TDS' toString). */
        public enum Form { TDS_STRING, CSV }

        public DeferredTdsString {
            type = SqlTyping.T_VARCHAR;   // a relation-text cell
        }

        public DeferredTdsString(SqlSelect inner, String alias, int id) {
            this(inner, alias, id, Form.TDS_STRING, false, SqlTyping.UNKNOWN);
        }

        public DeferredTdsString(SqlSelect inner, String alias, int id,
                Form form, boolean renderTdsNull) {
            this(inner, alias, id, form, renderTdsNull, SqlTyping.UNKNOWN);
        }
    }

    /**
     * The temp-table IN splice (the engine's
     * {@code generateTempTableSelectSQLQuery} — processInOperation's
     * over-threshold arm): renders as the fixed temp-select template
     * {@code select "<t>_0".ColumnForStoringInCollection as
     * ColumnForStoringInCollection from <t> as "<t>_0"} inside
     * {@code in (...)}. Plan-text vocabulary only — a loud error in any
     * executable dialect.
     */
    record TempTableInSplice(String tempTableName,
            TypeFact type) implements SqlExpr {
        public TempTableInSplice {
            type = SqlTyping.UNKNOWN;   // plan-text vocabulary — no rule
        }

        public TempTableInSplice(String tempTableName) {
            this(tempTableName, SqlTyping.UNKNOWN);
        }
    }

    /**
     * {@code json_object(k1, v1, k2, v2, ...)} &mdash; the graph-serialize
     * envelope's per-row object. {@code kv} alternates string-literal keys
     * with value expressions.
     */
    record JsonObject(List<SqlExpr> kv, TypeFact type)
            implements SqlExpr {
        public JsonObject {
            type = SqlTyping.T_JSON;
        }

        public JsonObject(List<SqlExpr> kv) {
            this(kv, SqlTyping.UNKNOWN);
        }
    }

    /**
     * {@code json_array(e1, e2, ...)} &mdash; a JSON ARRAY of the given
     * values, each keeping its own JSON kind (strings quoted, numbers
     * bare): the result-envelope's cell rows, column lists and activity
     * lists (a semantic node — the dialect spells the constructor).
     */
    record JsonArray(List<SqlExpr> elements, TypeFact type)
            implements SqlExpr {
        public JsonArray {
            elements = List.copyOf(elements);
            type = SqlTyping.T_JSON;
        }

        public JsonArray(List<SqlExpr> elements) {
            this(elements, SqlTyping.UNKNOWN);
        }
    }

    /**
     * {@code coalesce(json_group_array(x), '[]')} &mdash; the SNAPSHOT
     * aggregation of an envelope: all rows into one JSON-array value; an
     * empty rowset is the EMPTY ARRAY, never SQL NULL.
     */
    record JsonArrayAgg(SqlExpr value, List<Key> orderKeys,
            TypeFact type) implements SqlExpr {
        public JsonArrayAgg {
            orderKeys = orderKeys == null ? List.of() : List.copyOf(orderKeys);
            type = SqlTyping.T_JSON;
        }

        public JsonArrayAgg(SqlExpr value, List<Key> orderKeys) {
            this(value, orderKeys, SqlTyping.UNKNOWN);
        }

        /** Unordered aggregation (scan order — the pre-determinism shape). */
        public JsonArrayAgg(SqlExpr value) {
            this(value, List.of());
        }

        /** One ordered-agg key: union WITNESS keys render DESC (the
         * TRUE-first contract), pk determinism keys ASC. */
        public record Key(SqlExpr expr, boolean desc) {
        }
    }

    /**
     * {@code fn(...) OVER (PARTITION BY ... ORDER BY ... frame)}. Any
     * {@link SqlAgg} kind is legal here &mdash; this is the ONLY position that
     * admits the window-only kinds.
     */
    /** {@code frame} null = no explicit frame clause (dialect default). */
    record WindowCall(SqlAgg fn, List<SqlExpr> partitionBy, List<SqlSelect.SortKey> orderBy,
                      @com.legend.Nullable Frame frame,
                      TypeFact type) implements SqlExpr {
        public WindowCall {
            type = SqlTyping.windowType(fn);
        }

        public WindowCall(SqlAgg fn, List<SqlExpr> partitionBy,
                List<SqlSelect.SortKey> orderBy,
                @com.legend.Nullable Frame frame) {
            this(fn, partitionBy, orderBy, frame, SqlTyping.UNKNOWN);
        }

        /** {@code ROWS|RANGE BETWEEN <from> AND <to>}. */
        public record Frame(Kind kind, Bound from, Bound to) {
            public enum Kind { ROWS, RANGE }

            public sealed interface Bound {
                record UnboundedPreceding() implements Bound {
                }

                record Preceding(Number n) implements Bound {
                }

                record CurrentRow() implements Bound {
                }

                record Following(Number n) implements Bound {
                }

                record UnboundedFollowing() implements Bound {
                }

                /** {@code INTERVAL n UNIT PRECEDING} — the _RangeInterval frame side. */
                record IntervalPreceding(long n, String unit) implements Bound {
                }

                record IntervalFollowing(long n, String unit) implements Bound {
                }
            }
        }
    }

    /** A lambda for DuckDB list functions: {@code x -> body} / {@code (a, x) -> body}. */
    record Lambda(List<String> params, SqlExpr body,
            TypeFact type) implements SqlExpr {
        /** M1 leaf default — parameter typing is M2 (a lambda is not a
         * value; consumers read its BODY through the per-function
         * rules). */
        public Lambda(List<String> params, SqlExpr body) {
            this(params, body, SqlTyping.UNKNOWN);
        }

        /** ATTACHMENT-SITE param binding (M3 slice 0): rebuild a
         * single-param lambda's parameter references STAMPED as the
         * collection's element — the builder that joins a lambda to
         * its collection is the one holder of this knowledge (the
         * judge's rebind did this at consumption; this does it once,
         * at construction). Identity when the collection's element is
         * unknown, or the lambda has other than one parameter. */
        public static SqlExpr bind(SqlExpr lam, SqlExpr collection) {
            if (!(lam instanceof Lambda l) || l.params().size() != 1
                    || !(collection.type() instanceof TypeFact.Typed t
                            && t.type() instanceof SqlType.Array)) {
                return lam;
            }
            String p = l.params().get(0);
            SqlExpr body = rebindParam(l.body(), p,
                    Column.param(p, collection));
            return body == l.body() ? l
                    : new Lambda(l.params(), body, l.type());
        }

        /** By-name substitution of unqualified param reads, shadow-
         * stopped at inner lambdas re-binding the same name; walkers
         * enter through bodies (mapChildren), never subqueries. */
        private static SqlExpr rebindParam(SqlExpr e, String p, Column ref) {
            if (e instanceof Column c && c.table() == null
                    && p.equals(c.name())) {
                return ref;
            }
            if (e instanceof Lambda inner && inner.params().contains(p)) {
                return e;
            }
            return e.mapChildren(ch -> rebindParam(ch, p, ref));
        }
    }

    /**
     * {@code CAST(value AS <type>[])} — the target rides as a PURE type; the
     * SQL type name is the dialect's business. {@code array} casts to a list
     * of the target ({@code toMany}). A dialect may render a variant-access
     * value through its text-extraction idiom (DuckDB {@code ->>}) — that
     * swap is RENDERING knowledge, not IR content.
     */
    /** {@code conform} — SYNTH-CONFORMANCE PROVENANCE (T4 leg 1, the
     * typed-level seam the cast-provenance register called for): a
     * cast EMITTED by the mapping-read conformance, semantically the
     * engine's decode-side coercion. Execution dialects render it;
     * the engine-TEXT channel elides it (the goldens pin the engine's
     * own no-cast spelling — the wire-coercion precedent). User casts
     * are never conform. */
    record Cast(SqlExpr value, SqlType target, boolean conform,
            TypeFact type) implements SqlExpr {
        public Cast {
            type = SqlTyping.castType(value, target);
        }

        public Cast(SqlExpr value, SqlType target, boolean conform) {
            this(value, target, conform, SqlTyping.UNKNOWN);
        }

        public Cast(SqlExpr value, SqlType target) {
            this(value, target, false);
        }
    }

    /**
     * A FOLD over a collection value, in PURE conventions: the lambda's
     * parameters are {@code (element, accumulator)} — Pure's order — and the
     * dialect owns the encoding (DuckDB: {@code list_reduce} with swapped
     * params, single-item-list wrap/unwrap when {@code accIsList}; a
     * lambda-less backend: recursive CTE or a loud error).
     */
    record FoldCall(SqlExpr source, Lambda lambda, SqlExpr init, boolean accIsList,
                    boolean homogeneous, TypeFact type) implements SqlExpr {
        public FoldCall {
            type = SqlTyping.foldType(source, lambda, init, accIsList);
        }

        public FoldCall(SqlExpr source, Lambda lambda, SqlExpr init,
                boolean accIsList, boolean homogeneous) {
            this(source, lambda, init, accIsList, homogeneous,
                    SqlTyping.UNKNOWN);
        }
    }
}
