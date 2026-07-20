package com.legend.lowering;

import com.legend.builtin.Pure;
import com.legend.compiler.element.type.ExprType;
import com.legend.compiler.element.type.Multiplicity;
import com.legend.compiler.element.type.PlatformTypes;
import com.legend.compiler.element.type.Type;
import com.legend.compiler.spec.typed.FoldStrategy;
import com.legend.compiler.spec.typed.TypedAggCol;
import com.legend.compiler.spec.typed.TypedAggregate;
import com.legend.compiler.spec.typed.TypedAsOfJoin;
import com.legend.compiler.spec.typed.TypedCBoolean;
import com.legend.compiler.spec.typed.TypedCDate;
import com.legend.compiler.spec.typed.TypedCDecimal;
import com.legend.compiler.spec.typed.TypedCFloat;
import com.legend.compiler.spec.typed.TypedCInteger;
import com.legend.compiler.spec.typed.TypedCString;
import com.legend.compiler.spec.typed.TypedCast;
import com.legend.compiler.spec.typed.TypedColSpec;
import com.legend.compiler.spec.typed.TypedCollection;
import com.legend.compiler.spec.typed.TypedCollectionRelation;
import com.legend.compiler.spec.typed.TypedConcatenate;
import com.legend.compiler.spec.typed.TypedCopyInstance;
import com.legend.compiler.spec.typed.TypedDistinct;
import com.legend.compiler.spec.typed.TypedDrop;
import com.legend.compiler.spec.typed.TypedEnumValue;
import com.legend.compiler.spec.typed.TypedExtend;
import com.legend.compiler.spec.typed.TypedExtendAgg;
import com.legend.compiler.spec.typed.TypedExtendWindow;
import com.legend.compiler.spec.typed.TypedFilter;
import com.legend.compiler.spec.typed.TypedFlatten;
import com.legend.compiler.spec.typed.TypedFold;
import com.legend.compiler.spec.typed.TypedFrom;
import com.legend.compiler.spec.typed.TypedFuncCol;
import com.legend.compiler.spec.typed.TypedGroupBy;
import com.legend.compiler.spec.typed.TypedIf;
import com.legend.compiler.spec.typed.TypedJoin;
import com.legend.compiler.spec.typed.TypedJoinSlot;
import com.legend.compiler.spec.typed.TypedLambda;
import com.legend.compiler.spec.typed.TypedLet;
import com.legend.compiler.spec.typed.TypedLimit;
import com.legend.compiler.spec.typed.TypedMap;
import com.legend.compiler.spec.typed.TypedNativeCall;
import com.legend.compiler.spec.typed.TypedNavigate;
import com.legend.compiler.spec.typed.TypedNewInstance;
import com.legend.compiler.spec.typed.TypedOver;
import com.legend.compiler.spec.typed.TypedPackageableRef;
import com.legend.compiler.spec.typed.TypedPivot;
import com.legend.compiler.spec.typed.TypedProject;
import com.legend.compiler.spec.typed.TypedPropertyAccess;
import com.legend.compiler.spec.typed.TypedRename;
import com.legend.compiler.spec.typed.TypedSelect;
import com.legend.compiler.spec.typed.TypedSerializeGraph;
import com.legend.compiler.spec.typed.TypedSlice;
import com.legend.compiler.spec.typed.TypedSort;
import com.legend.compiler.spec.typed.TypedSortBy;
import com.legend.compiler.spec.typed.TypedSourceUrl;
import com.legend.compiler.spec.typed.TypedSpec;
import com.legend.compiler.spec.typed.TypedTableReference;
import com.legend.compiler.spec.typed.TypedTds;
import com.legend.compiler.spec.typed.TypedVariable;
import com.legend.compiler.spec.typed.TypedWrite;
import com.legend.error.LegendCompileException;
import com.legend.error.ModelException;
import com.legend.error.NotImplementedException;
import com.legend.sql.OutputCol;
import com.legend.sql.SqlAgg;
import com.legend.sql.SqlExpr;
import com.legend.sql.SqlFn;
import com.legend.sql.SqlQuery;
import com.legend.sql.SqlSelect;
import com.legend.sql.SqlSource;
import com.legend.sql.SqlType;
import com.legend.sql.SqlUnion;
import com.legend.values.PureDateLiteral;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;
/**
 * Phase I &mdash; relation-pipeline lowering (M2 scope: table/TDS sources +
 * filter/select/rename/sort/slicing/distinct; mappings and class sources are
 * Phase H). One exhaustive dispatch per {@code TypedSpec} kind; every fold
 * decision is {@link Fold}'s; scalar natives are {@link Scalars}'.
 */
public final class Lowerer {

    private int aliasCounter = 0;
    private int tdsCounter;

    /**
     * The CANONICAL class-value layout resolver (ClassLayouts, supplied by the
     * driver): declared stored properties in declaration order — a struct's
     * fields come from the MODEL, never from an instance's value set. Empty
     * when no model rides along (unit tests over pure-relational queries);
     * class values then keep hitting the loud walls.
     */
    private final Function<Type,
            Optional<List<Type.Column>>> classLayout;

    /** Whether a class FQN exists in the driving model (layoutless-LUB detection). */
    private final Predicate<String> classExists;

    public Lowerer() {
        this(t -> Optional.empty(), f -> false);
    }

    public Lowerer(Function<Type,
            Optional<List<Type.Column>>> classLayout,
                   Predicate<String> classExists) {
        this.classLayout = classLayout;
        this.classExists = classExists;
    }

    /** SQL type of a value, seeing through class layouts (structs) before {@link PureSql}. */
    private SqlType sqlTypeOf(Type t) {
        // Platform CARRIER types own their SQL shape (List = bare array,
        // Pair = struct, Map = MAP) — List's declared `values` property is
        // its pure-side surface, never a struct layout at the SQL boundary.
        if (PlatformTypes.isListCarrier(t)
                || PlatformTypes.isPairCarrier(t)
                || PlatformTypes.isMapCarrier(t)) {
            return PureSql.type(t);
        }
        return classLayout.apply(t)
                .<SqlType>map(cols -> new SqlType.Struct(
                        cols.stream().map(c -> {
                            SqlType ft = sqlTypeOf(c.type());
                            boolean many = c.multiplicity() instanceof
                                    Multiplicity.Bounded b && b.isMany();
                            return new SqlType.Struct.Field(c.name(),
                                    many ? new SqlType.Array(ft) : ft);
                        }).toList()))
                .orElseGet(() -> {
                    // A MODEL class with no layoutable properties reaching a
                    // VALUE boundary is a heterogeneous LUB (mixed instance
                    // kinds meeting at an abstract ancestor) — it travels as
                    // variant JSON, like Any. Non-model classes keep
                    // PureSql's loud wall.
                    if (t instanceof Type.ClassType ct
                            && !PlatformTypes.isVariant(ct)
                            && !PlatformTypes.isNil(ct)
                            && !PlatformTypes.isAny(ct)
                            && classExists.test(ct.fqn())) {
                        return SqlType.Scalar.JSON;
                    }
                    return PureSql.type(t);
                });
    }

    /**
     * Enclosing lambda scopes for CORRELATED nesting: when a relation query is
     * lowered INSIDE a lambda (a correlated subquery), the outer lambda's
     * resolver is pushed here so the inner predicate can reference outer rows.
     */
    private final ArrayDeque<ColumnResolver>
            enclosing = new ArrayDeque<>();

    /** Query-level let bindings ({@code |let a = ...; ...$a...}), lowered once. */
    private final Map<String, SqlExpr> letBindings = new HashMap<>();

    /**
     * Lower a typed QUERY BODY: leading {@code let} statements bind their
     * lowered values into query scope (substitution — the lean output has no
     * trace of the lets); the final statement is the query.
     */
    public SqlQuery lower(List<TypedSpec> body) {
        for (int i = 0; i < body.size() - 1; i++) {
            if (!(body.get(i) instanceof TypedLet let)) {
                throw new IllegalStateException(
                        "only let statements may precede the query expression");
            }
            letBindings.put(let.name(), scalar(let.value(), (var, name) -> {
                throw new IllegalStateException(
                        "a query-level let has no row scope for $" + var);
            }));
        }
        return lower(body.get(body.size() - 1));
    }

    /** Lower a typed query to SQL: relation pipelines and scalar roots. */
    public SqlQuery lower(TypedSpec spec) {
        // from() is execution-context metadata, fully consumed by Phase H —
        // the SQL is its source's. (Relation-typed from-roots take the
        // relation() arm below; a GRAPH-typed root needs the unwrap here.)
        if (spec instanceof TypedFrom from) {
            return lower(from.source());
        }
        // The resolved GRAPH envelope keeps its CLASS-typed info (the
        // result-shape contract) but lowers as a relation.
        if (spec instanceof TypedSerializeGraph g) {
            return serializeGraph(g);
        }
        // A terminal concatenate is a BARE set operation — no wrapping SELECT *.
        if (spec instanceof TypedConcatenate c) {
            return union(c);
        }
        if (spec.info().type() instanceof Type.RelationType) {
            return relation(spec);
        }
        // relation->map(row|scalar) at the ROOT is the single-column
        // projection (pure: a VALUE collection derived from rows; the
        // Executor's COLLECTION shape reads N rows × 1 column). A
        // COLLECTION-VALUED mapper ($r.values — the row's cells) FLATTENS
        // per pure map semantics: project the cell array, then UNNEST.
        if (spec instanceof TypedMap m
                && m.source().info().type() instanceof Type.RelationType
                && m.mapper() instanceof TypedLambda ml
                && !(ml.info().type() instanceof Type.FunctionType ft
                        && ft.result().type() instanceof Type.RelationType)) {
            boolean collectionMapper = isCollectionMapper(ml);
            Multiplicity colMult =
                    ml.info().type() instanceof Type.FunctionType fnT
                            ? fnT.result().multiplicity()
                            : Multiplicity.Bounded.ZERO_ONE;
            SqlSelect proj = relation(new TypedProject(
                    m.source(),
                    List.of(new TypedFuncCol("value", ml)),
                    new ExprType(
                            new Type.RelationType(List.of(
                                    new Type.RelationType.Column("value",
                                            spec.info().type(), colMult))),
                            Multiplicity.Bounded.ONE)));
            if (!collectionMapper) {
                return proj;
            }
            String sub = nextAlias();
            return SqlSelect.starOf(new SqlSource.Subselect(proj, sub))
                    .withProjections(List.of(new SqlSelect.Projection(
                                    SqlExpr.Call.of(SqlFn.UNNEST,
                                            new SqlExpr.Column(sub, "value")),
                                    "value")),
                            List.of(new OutputCol("value",
                                    sqlTypeOf(spec.info().type()), true)));
        }
        return scalarRoot(spec);
    }

    /**
     * SCALAR result shape: a FROM-less single-value SELECT. Collections and
     * class roots (COLLECTION/GRAPH shapes) are still honestly unbuilt.
     */
    private SqlSelect scalarRoot(TypedSpec spec) {
        SqlExpr e = scalar(spec, (var, name) -> {
            throw new IllegalStateException("a scalar query has no row scope for $"
                    + var + "." + name);
        });
        // COLLECTION roots explode to N rows (the result-shape contract:
        // Executor reads a collection as N rows x 1 column).
        if (isMany(spec)) {
            e = SqlExpr.Call.of(SqlFn.UNNEST, e);
        }
        return new SqlSelect(
                List.of(new SqlSelect.Projection(e, "value")), false, null,
                null, List.of(), null, null, List.of(), null, null,
                List.of(new OutputCol("value", sqlTypeOf(spec.info().type()),
                        PureSql.nullable(spec.info().multiplicity()))));
    }

    /**
     * A map mapper whose per-row value is a COLLECTION (pure map flattens
     * those): declared multiplicity above one, or an ARRAY-producing body
     * (a one-column row's cells is a single-element LIST, not a scalar).
     */
    /** Every element is a property read off the SAME row variable — the
     * Typer's TDSRow cells synthesis, the one shape that prints TDSNull. */
    private static boolean isRowCells(TypedCollection tc) {
        String var = null;
        for (TypedSpec e : tc.elements()) {
            if (!(e instanceof TypedPropertyAccess pa
                    && pa.source()
                            instanceof TypedVariable v)) {
                return false;
            }
            if (var == null) {
                var = v.name();
            } else if (!var.equals(v.name())) {
                return false;
            }
        }
        return !tc.elements().isEmpty();
    }

    private static boolean isCollectionMapper(
            TypedLambda ml) {
        // Collection mapper iff the lowered value is a SQL LIST, which is
        // exactly a TypedCollection body (list_value carrier). A loose
        // declared multiplicity over a non-collection body still lowers to
        // a plain scalar column — wrapping it in UNNEST/flatten is a type
        // error, not a flatten. Casts distribute element-wise over
        // collections (the value stays a LIST) — look through them
        // ($x.values->cast(@StrictDate), calendar DateRange).
        TypedSpec last = ml.body().get(ml.body().size() - 1);
        while (last instanceof TypedCast tc) {
            last = tc.source();
        }
        return last instanceof TypedCollection;
    }

    private String nextAlias() {
        return "t" + aliasCounter++;
    }

    // ==================================================================
    // Relation ops
    // ==================================================================

    private SqlSelect relation(TypedSpec spec) {
        // POSITIONAL reads over a relation: at(n) IS slice(n, n+1);
        // first()/head() IS limit 1 — row selection, not value extraction
        if (spec instanceof TypedNativeCall pc
                && !pc.args().isEmpty()
                && pc.args().get(0).info().type() instanceof Type.RelationType) {
            String fqn = pc.callee().qualifiedName();
            if (fqn.equals("meta::pure::functions::collection::at")
                    && pc.args().size() == 2
                    && pc.args().get(1) instanceof TypedCInteger n) {
                var one = ExprType.one(
                        Type.Primitive.INTEGER);
                return relation(new TypedSlice(
                        pc.args().get(0),
                        new TypedCInteger(
                                n.value().longValue(), one),
                        new TypedCInteger(
                                n.value().longValue() + 1, one),
                        pc.args().get(0).info()));
            }
            if ((fqn.equals("meta::pure::functions::collection::first")
                    || fqn.equals("meta::pure::functions::collection::head"))
                    && pc.args().size() == 1) {
                var one = ExprType.one(
                        Type.Primitive.INTEGER);
                return relation(new TypedLimit(
                        pc.args().get(0),
                        new TypedCInteger(1L, one),
                        pc.args().get(0).info()));
            }
        }
        return switch (spec) {
            case TypedSourceUrl su -> SqlSelect.starOf(
                    new SqlSource.SourceUrl(su.url(), nextAlias(), outputsOf(su.info())));
            case TypedTableReference t -> SqlSelect.starOf(
                    new SqlSource.Table(t.table(), nextAlias(), outputsOf(t.info())));

            case TypedTds tds -> tdsLiteral(tds);

            case TypedFilter f -> filter(f);

            case TypedSelect sel -> narrowTo(relation(sel.source()), sel.columns(), sel.info());

            case TypedDistinct d -> distinct(d);
            // lateral(rel, {row | relationOf(row)}): the lambda's relation
            // body lowers with the row param CORRELATED to the left alias
            // (the enclosing-resolver channel); DuckDB joins it per-row via
            // CROSS JOIN LATERAL. Schema = T+V (checker's schema algebra).
            case TypedNativeCall nc when nc.callee().qualifiedName()
                    .equals("meta::pure::functions::relation::lateral")
                    && nc.args().size() == 2
                    && nc.args().get(1) instanceof TypedLambda lam -> {
                SqlSelect left = relation(nc.args().get(0));
                SqlSource leftSide = asLeftJoinSide(left);
                String param = lam.parameters().get(0);
                ColumnResolver leftCols = (v, name) ->
                        (v == null || v.equals(param))
                                ? resolveOrThrow(SqlSelect.starOf(leftSide), name)
                                : null;
                enclosing.push((v, name) -> {
                    SqlExpr r = leftCols.resolve(v, name);
                    if (r == null) {
                        // NOT this scope's variable: the UnfoldableRef SIGNAL
                        // lets scopedResolver continue outward (a hard throw
                        // severed the whole enclosing chain behind lateral).
                        throw new UnfoldableRef(name);
                    }
                    return r;
                });
                SqlSelect right;
                try {
                    right = relation(lam.body().get(lam.body().size() - 1));
                } finally {
                    enclosing.pop();
                }
                SqlSource.Join join = new SqlSource.Join(leftSide,
                        new SqlSource.Subselect(right, nextAlias()),
                        SqlSource.Join.Kind.CROSS_LATERAL,
                        null);   // CROSS JOIN takes no ON clause
                yield SqlSelect.starOf(join)
                        .withProjections(List.of(), outputsOf(nc.info()));
            }
            // relation::variant::flatten(collection, ~col): the collection
            // UNNESTs as the single column (real flatten.pure semantics).
            case TypedCollectionRelation cr -> {
                // Inside lateral(...) the collection may read the OUTER row
                // (the enclosing-resolver channel); otherwise it must be
                // self-contained.
                var outerScopes = List.copyOf(enclosing);
                SqlExpr value = scalar(cr.value(), (v, name) -> {
                    for (var outer : outerScopes) {
                        if (attempt(() -> outer.resolve(v, name))
                                instanceof Resolution.Resolved o) {
                            return o.expr();
                        }
                    }
                    throw new IllegalStateException("collection-relation value must"
                            + " be self-contained, referenced column: " + name);
                });
                Type elem = ((Type.RelationType) cr.info().type())
                        .columns().get(0).type();
                SqlExpr list = elem instanceof Type.ClassType
                        ? SqlExpr.Call.of(SqlFn.VARIANT_ELEMENTS, value)
                        : value;
                // A VARIANT ELEMENT column keeps JSON elements; the list may
                // itself be a variant (fromJson(...)->toMany(@Variant)).
                if (cr.value().info().type() instanceof Type.ClassType vc
                        && PlatformTypes.isVariant(vc)
                        && !(elem instanceof Type.ClassType)) {
                    list = SqlExpr.Call.of(SqlFn.VARIANT_ELEMENTS, value);
                }
                yield SqlSelect.starOf(new SqlSource.Subselect(
                        new SqlSelect(List.of(new SqlSelect.Projection(
                                        SqlExpr.Call.of(SqlFn.UNNEST, list),
                                        cr.column())),
                                false, null, null, List.of(), null, null, List.of(),
                                null, null, outputsOf(cr.info())),
                        nextAlias()));
            }

            case TypedRename r -> rename(r);

            case TypedSort s -> sort(s);
            case TypedSortBy sb -> sortBy(sb);

            case TypedLimit l -> {
                SqlSelect src = relation(l.source());
                yield (Fold.limitFolds(src) ? src : isolate(src)).withLimit(intOf(l.count()));
            }

            // first()/head() over a RELATION: the first row — LIMIT 1 (the
            // result stays row-typed, one row's TABULAR).
            case TypedNativeCall n when n.args().size() == 1
                    && n.args().get(0).info().type() instanceof Type.RelationType
                    && (isFamily(n, "first") || isFamily(n, "head")) -> {
                SqlSelect src = relation(n.args().get(0));
                yield (Fold.limitFolds(src) ? src : isolate(src)).withLimit(1L);
            }

            case TypedDrop d -> {
                SqlSelect src = relation(d.source());
                yield (Fold.offsetFolds(src) ? src : isolate(src)).withOffset(intOf(d.count()));
            }

            case TypedSlice s -> {
                SqlSelect src = relation(s.source());
                long start = intOf(s.start());
                SqlSelect base = Fold.offsetFolds(src) ? src : isolate(src);
                yield base.withOffset(start).withLimit(intOf(s.stop()) - start);
            }

            case TypedGroupBy g -> groupBy(g);
            case TypedNavigate nav -> navigate(nav);

            case TypedAggregate a -> aggregate(a);

            case TypedExtend e -> extend(relation(e.source()), e.columns(), e.info());

            // project over INSTANCE LITERALS (PCT's ^X(...)->project(~[...])):
            // no store exists — each instance becomes one SELECT over a 1-row
            // anchor, its to-many property paths exploding via LEFT JOIN
            // LATERAL UNNEST (cross-product across columns, real pure; LEFT so
            // an empty array NULLs its column instead of killing the row).
            case TypedProject p when isInstanceLiteral(p.source()) ->
                    projectOverInstances(p);

            case TypedProject p -> project(relation(p.source()), p.columns(), p.info());

            case TypedConcatenate c -> SqlSelect.starOf(
                    new SqlSource.Subselect(union(c), nextAlias()));

            case TypedExtendWindow w -> extendWindow(w);

            case TypedJoin j -> join(j);

            case TypedAsOfJoin aj -> asOfJoin(aj);

            case TypedExtendAgg ea -> extendAgg(ea);

            // from(mapping, runtime): execution-context metadata — a Phase-H
            // concern; the relation flows through unchanged.
            case TypedFrom fr -> relation(fr.source());

            // cast(@Relation<(…)>): when EVERY target column exists in the
            // source, the cast is a real projection — surviving columns only,
            // each SQL-CAST where its type changed (String->Integer). Target
            // names ABSENT from the source are the pivot idiom's dynamic
            // columns — those stay type-only (zero SQL footprint).
            case TypedCast c
                    when c.source().info().type() instanceof Type.RelationType srcRow
                    && c.info().type() instanceof Type.RelationType tgtRow ->
                    relationCast(c, srcRow, tgtRow);

            case TypedFlatten fl -> flatten(fl);

            case TypedPivot pv -> pivot(pv);

            // the Typer's `.rows` MARKER (identity over a relation value —
            // the K result frame's row-index/envelope disambiguator): the
            // resolver erases it on resolved paths; this is the defensive
            // FLOOR for G-direct paths (audit 20c H1 — the marker leaked
            // to this switch's default on the plain compile path).
            case TypedPropertyAccess pa
                    when pa.property().equals(com.legend.compiler
                            .element.type.PlatformTypes.ROWS_MARKER)
                    && pa.source().info().type() instanceof Type.RelationType ->
                    relation(pa.source());

            // STORE-ONLY nodes: reaching the lowerer is not a missing rule —
            // it means the Phase H resolver failed to rewrite them away. Say
            // so, instead of the frontier default's misdiagnosis.
            case TypedJoinSlot js ->
                    throw new NotImplementedException(
                            "TypedJoinSlot (pipeline slot join '" + js.alias()
                          + "') escaped Phase H store resolution — a resolver gap,"
                          + " not a missing lowering rule");

            // Rows-level ->toOne() over a relation ($r.values.rows->toOne(),
            // the corpus's single-ROW claim): row-identical to the relation
            // itself. The exactly-one contract is enforced where the value
            // is CONSUMED (the executor's scalar second-row guard, audit
            // 21b F10) — engine toOne throws at the reader, never in SQL.
            case TypedNativeCall nc when
                    "meta::pure::functions::multiplicity::toOne"
                            .equals(nc.callee().qualifiedName())
                    && !nc.args().isEmpty()
                    && nc.args().get(0).info().type()
                            instanceof Type.RelationType ->
                    relation(nc.args().get(0));

            case TypedNativeCall nc when isBareSingleColumnSort(nc) ->
                    naturalSort(nc);

            // SANCTIONED frontier default (root package-info invariant is
            // scoped to hiding-prone switches): the not-yet-lowered TypedSpec
            // variants churn every milestone; each throws LOUD and NAMED.
            default -> throw new NotImplementedException("lowering not yet implemented for "
                    + spec.getClass().getSimpleName()
                    + (spec instanceof TypedNativeCall nc2
                            ? " ('" + nc2.callee().qualifiedName()
                                    + "' in relation position)"
                            : ""));
        };
    }

    /** Nested concatenates flatten into ONE multi-branch union. */
    private SqlUnion union(TypedConcatenate c) {
        List<SqlQuery> branches = new ArrayList<>();
        collectBranches(c, branches);
        return new SqlUnion(branches, true, outputsOf(c.info()));
    }

    private void collectBranches(TypedSpec spec, List<SqlQuery> out) {
        if (spec instanceof TypedConcatenate c) {
            collectBranches(c.left(), out);
            collectBranches(c.right(), out);
        } else {
            out.add(relation(spec));
        }
    }

    /**
     * groupBy: keys + aggregates REPLACE the projection list; the GROUP BY
     * clause carries the key expressions.
     */
    private SqlSelect groupBy(TypedGroupBy g) {
        SqlSelect src = relation(g.source());
        SqlSelect base = Fold.groupByFolds(src) ? src : isolate(src);
        return foldOrIsolate(base, "groupBy", b -> buildGroupBy(b, g));
    }

    private SqlSelect buildGroupBy(SqlSelect base0, TypedGroupBy g) {
        // CALENDAR AGGREGATIONS (task G1): calendar natives in agg maps
        // LEFT-join the fiscal calendar table (twice per distinct
        // date/end/type) before the aggs lower over it
        java.util.Map<TypedAggCol, CalendarAgg.Ctx> calCtx =
                new java.util.IdentityHashMap<>();
        SqlSelect base = CalendarAgg.joinCalendars(base0, g.aggs(), calCtx,
                spec -> scalar(spec, (v, name) -> resolveOrThrow(base0, name)),
                () -> aliasCounter++);
        List<SqlExpr> keys = new ArrayList<>(g.keys().size());
        List<SqlSelect.Projection> ps = new ArrayList<>();
        for (TypedGroupBy.GroupKey k : g.keys()) {
            SqlExpr e = k.fn().isPresent()
                    ? scalar(last(k.fn().get()), (v, name) -> resolveOrThrow(base, name))
                    : resolveOrThrow(base, k.column());
            keys.add(e);
            ps.add(new SqlSelect.Projection(e,
                    e instanceof SqlExpr.Column c && c.name().equals(k.column()) ? null : k.column()));
        }
        for (TypedAggCol a : g.aggs()) {
            ps.add(new SqlSelect.Projection(
                    aggValue(base, a, calCtx.get(a)), a.name()));
        }
        return base.withGroupBy(keys).withProjections(ps, outputsOf(g.info()));
    }

    /**
     * The GRAPH-serialize envelope (Phase H4a SNAPSHOT): one
     * {@code json_object} per source row keyed by the fetch tree's leaves,
     * nested children as CORRELATED scalar subqueries (the parent scope
     * rides the enclosing-resolver channel — the EXISTS mechanism), and an
     * {@code arrayWrap} node aggregates the objects into one JSON-array
     * {@code result} value.
     */
    private SqlSelect serializeGraph(TypedSerializeGraph g) {
        SqlSelect src = relation(g.source());
        // json_group_array is an AGGREGATE and the envelope REPLACES the
        // projection list — the groupBy folding constraints are exactly right.
        SqlSelect base = Fold.groupByFolds(src) ? src : isolate(src);
        ColumnResolver own = scopedResolver(base, g.rowVar());
        List<SqlExpr> kv = new ArrayList<>(2 * (g.leaves().size() + g.nested().size()));
        for (TypedFuncCol leaf : g.leaves()) {
            kv.add(new SqlExpr.StringLit(leaf.name()));
            // STRICT own-select resolution: leaves read their own row only —
            // an outer-scope fallback could silently supply a SAME-NAMED
            // parent column to a nested child's leaf (audit L2); a miss is
            // loud naming the leaf.
            switch (attempt(() -> scalar(last(leaf.fn()),
                    (v, name) -> resolveOrThrow(base, name)))) {
                case Resolution.Resolved r -> kv.add(r.expr());
                case Resolution.Unfoldable u -> throw new IllegalStateException(
                        "serialize leaf '" + leaf.name() + "' references column '"
                                + u.column() + "', unresolvable in the envelope source");
            }
        }
        for (var child : g.nested()) {
            kv.add(new SqlExpr.StringLit(child.property()));
            enclosing.push(own);
            try {
                kv.add(new SqlExpr.ScalarSubquery(serializeGraph(child.node())));
            } finally {
                enclosing.pop();
            }
        }
        // bareValue: a to-many PRIMITIVE leaf aggregates the raw values —
        // ["abc","def"] — never json_object envelopes
        SqlExpr obj = g.bareValue() ? kv.get(1) : new SqlExpr.JsonObject(kv);
        SqlExpr result = g.arrayWrap() ? new SqlExpr.JsonArrayAgg(obj) : obj;
        return base.withProjections(
                List.of(new SqlSelect.Projection(result, "result")),
                List.of(new OutputCol("result", PureSql.type(Type.Primitive.STRING), false)));
    }

    /** aggregate: whole-relation reduction — aggregates only, no GROUP BY clause. */
    private SqlSelect aggregate(TypedAggregate a) {
        SqlSelect src = relation(a.source());
        SqlSelect base = Fold.groupByFolds(src) ? src : isolate(src);
        return foldOrIsolate(base, "aggregate", b -> {
            List<SqlSelect.Projection> ps = new ArrayList<>(a.aggs().size());
            for (TypedAggCol col : a.aggs()) {
                ps.add(new SqlSelect.Projection(aggValue(b, col), col.name()));
            }
            return b.withProjections(ps, outputsOf(a.info()));
        });
    }

    /**
     * One agg column: the map lambda yields the value expression; the reduce
     * lambda's resolved overload names the SQL reducer. A bare-row map
     * ({@code x|$x}) is COUNT(*)-style — no value argument.
     */
    private SqlAgg.Reducer aggExpr(SqlSelect base, TypedAggCol a) {
        SqlExpr e = aggValue(base, a);
        if (!(e instanceof SqlAgg.Reducer r)) {
            throw new IllegalStateException("aggregate '" + a.name()
                    + "' composes multiple reducers (wavg) — PIVOT USING"
                    + " takes exactly one");
        }
        return r;
    }

    /**
     * Window position accepts COMPOSED aggregates (wavg = SUM(v*w)/SUM(w),
     * hashCode = HASH(LIST(x))): every bare reducer inside the value
     * expression gets the SAME window spec.
     */
    private static SqlExpr windowize(SqlExpr e, List<SqlExpr> partitionBy,
            List<SqlSelect.SortKey> orderBy, SqlExpr.WindowCall.Frame frame) {
        return switch (e) {
            case SqlAgg.Reducer r ->
                    new SqlExpr.WindowCall(r, partitionBy, orderBy, frame);
            case SqlExpr.Call c -> new SqlExpr.Call(c.fn(), c.args().stream()
                    .map(x -> windowize(x, partitionBy, orderBy, frame)).toList());
            case SqlExpr.Cast c ->
                    new SqlExpr.Cast(windowize(c.value(), partitionBy, orderBy, frame),
                            c.target());
            // A reducer under a CASE arm must window too (audit: the first
            // agg recipe that guards with CASE would render bare).
            case SqlExpr.Case cs -> new SqlExpr.Case(
                    cs.whens().stream().map(w -> new SqlExpr.Case.When(
                            windowize(w.condition(), partitionBy, orderBy, frame),
                            windowize(w.then(), partitionBy, orderBy, frame))).toList(),
                    cs.otherwise() == null ? null
                            : windowize(cs.otherwise(), partitionBy, orderBy, frame));
            // Composite carriers: a reducer anywhere inside must window
            // (audit 15: the open default let these render bare aggregates).
            case SqlExpr.ArrayLit a -> new SqlExpr.ArrayLit(a.elements().stream()
                    .map(x -> windowize(x, partitionBy, orderBy, frame)).toList());
            case SqlExpr.StructLit s -> new SqlExpr.StructLit(s.fields().stream()
                    .map(f -> new SqlExpr.StructLit.Field(f.name(),
                            windowize(f.value(), partitionBy, orderBy, frame)))
                    .toList());
            case SqlExpr.StructGet g -> new SqlExpr.StructGet(
                    windowize(g.source(), partitionBy, orderBy, frame), g.field());
            case SqlExpr.JsonObject j -> new SqlExpr.JsonObject(j.kv().stream()
                    .map(x -> windowize(x, partitionBy, orderBy, frame)).toList());
            case SqlExpr.FoldCall f -> new SqlExpr.FoldCall(
                    windowize(f.source(), partitionBy, orderBy, frame), f.lambda(),
                    windowize(f.init(), partitionBy, orderBy, frame),
                    f.accIsList(), f.homogeneous());
            // Ordered-set / array aggregates are not expressible as OVER()
            // window functions in this IR — bare emission would silently
            // change grouping semantics; the shape stays loud until built.
            case SqlExpr.OrderedListAgg ignored ->
                    throw new com.legend.error.NotImplementedException(
                            "ordered list aggregate in window position");
            case SqlExpr.JsonArrayAgg ignored ->
                    throw new com.legend.error.NotImplementedException(
                            "json array aggregate in window position");
            // Subqueries own their scope: a reducer inside aggregates THERE,
            // never over this window. Already-windowed calls keep their spec.
            case SqlExpr.Exists x -> x;
            case SqlExpr.ScalarSubquery s -> s;
            case SqlExpr.WindowCall w -> w;
            case SqlExpr.Lambda l -> l;
            // Leaves: no reducer can hide below.
            case SqlExpr.Column ignored -> e;
            case SqlExpr.Star ignored -> e;
            case SqlExpr.StarExcept ignored -> e;
            case SqlExpr.StringLit ignored -> e;
            case SqlExpr.IntLit ignored -> e;
            case SqlExpr.FloatLit ignored -> e;
            case SqlExpr.DecimalLit ignored -> e;
            case SqlExpr.BoolLit ignored -> e;
            case SqlExpr.NullLit ignored -> e;
            case SqlExpr.DateLit ignored -> e;
            case SqlExpr.TimestampLit ignored -> e;
        };
    }

    private SqlExpr aggValue(SqlSelect base, TypedAggCol a) {
        return aggValue(base, a, null);
    }

    private SqlExpr aggValue(SqlSelect base, TypedAggCol a,
            CalendarAgg.Ctx calendar) {
        TypedSpec reduceBody = last(a.reduce());
        // A cast WRAPPING the reducer (y|$y->plus()->cast(@Integer)) rides
        // AROUND the SQL aggregate: unwrap, lower the inner reducer, re-wrap
        // by the cast policy (widening/same-type is the assertion no-op —
        // the PCT shapes are Integer->Integer).
        if (reduceBody instanceof TypedCast rc
                && rc.source() instanceof TypedNativeCall) {
            SqlExpr inner = aggValue(base, new TypedAggCol(a.name(), a.map(),
                    new TypedLambda(a.reduce().parameters(),
                            List.of(rc.source()), a.reduce().info()),
                    a.orderKey(), a.orderAsc()));
            return castByPolicy(inner, rc.source().info().type(), rc.target());
        }
        if (!(reduceBody instanceof TypedNativeCall call)) {
            throw new IllegalStateException("aggregate reduce must be a native reducer call, got "
                    + reduceBody.getClass().getSimpleName());
        }
        // A SCALAR wrapping the reducer (y|$y->average()->round()): lower the
        // inner aggregate, then apply the scalar rule around it — trailing
        // args must be literal-lowerable (no row scope out here).
        if (Aggregates.reducerOrNull(call.callee()) == null
                && !call.args().isEmpty()
                && call.args().get(0) instanceof TypedNativeCall innerAgg
                && Aggregates.reducerOrNull(innerAgg.callee()) != null) {
            SqlExpr inner = aggValue(base, new TypedAggCol(a.name(), a.map(),
                    new TypedLambda(a.reduce().parameters(),
                            List.of(innerAgg), a.reduce().info()),
                    a.orderKey(), a.orderAsc()));
            List<SqlExpr> wrapped = new ArrayList<>();
            wrapped.add(inner);
            for (int i = 1; i < call.args().size(); i++) {
                wrapped.add(scalar(call.args().get(i), noScope()));
            }
            return Scalars.lower(call, wrapped);
        }
        String fn = Aggregates.reducerFor(call.callee());
        TypedSpec mapBody = last(a.map());
        // ORDER-SENSITIVE aggregation (sortBy before joinStrings): the key
        // lowers in the SAME row scope as the map body and rides inside
        // the SQL aggregate (string_agg(x, sep ORDER BY k))
        List<SqlSelect.SortKey> aggOrder = a.orderKey() == null ? List.of()
                : List.of(new SqlSelect.SortKey(
                        scalar(last(a.orderKey()),
                                (v, name) -> resolveOrThrow(base, name)),
                        a.orderAsc(), null));
        // Reducer EXTRA arguments (joinStrings('_') carries its separator;
        // percentile carries p [+ ascending, continuous]): literal args ride
        // along after the value; variable refs are the reducer's own
        // collection params; anything ELSE is unsupported and must be LOUD.
        List<SqlExpr> extra = new ArrayList<>();
        List<Boolean> flags = new ArrayList<>();
        TypedCast valueCast = null;
        boolean distinctValues = false;
        for (TypedSpec argSpec : call.args()) {
            if (argSpec instanceof TypedNativeCall dn
                    && dn.callee().qualifiedName().equals(
                            "meta::pure::functions::collection::distinct")
                    && dn.args().size() == 1
                    && dn.args().get(0) instanceof TypedVariable) {
                // y|$y->distinct()->count(): DISTINCT inside the SQL
                // aggregate (engine: count(distinct(col)))
                distinctValues = true;
            } else if (argSpec instanceof TypedCBoolean b) {
                flags.add(b.value());
            } else if (argSpec instanceof TypedCString
                    || argSpec instanceof TypedCInteger
                    || argSpec instanceof TypedCFloat
                    || argSpec instanceof TypedCDecimal) {
                extra.add(scalar(argSpec, (v, name) -> resolveOrThrow(base, name)));
            } else if (argSpec instanceof TypedCast vc
                    && vc.source() instanceof TypedVariable) {
                // $x->cast(@T)->plus(): the grouped VALUES cast before
                // reducing — a value-cast on the aggregated expression.
                valueCast = vc;
            } else if (!(argSpec instanceof TypedVariable)) {
                throw new IllegalStateException("aggregate reducer argument of kind "
                        + argSpec.getClass().getSimpleName()
                        + " is not supported (literals only)");
            }
        }
        // percentile(p, ascending, continuous): continuous selects the
        // QUANTILE flavor; descending order is the 1-p quantile.
        // variance(isBiasCorrected): false selects the POPULATION variance.
        if (!flags.isEmpty()) {
            if ("VAR_SAMP".equals(fn) && flags.size() == 1 && extra.isEmpty()) {
                fn = flags.get(0) ? "VAR_SAMP" : "VAR_POP";
            } else if ("QUANTILE_CONT".equals(fn) && flags.size() == 2
                    && extra.size() == 1) {
                fn = flags.get(1) ? "QUANTILE_CONT" : "QUANTILE_DISC";
                if (!flags.get(0)) {
                    if (flags.get(1)) {
                        // CONTINUOUS interpolation is symmetric: the
                        // descending p-quantile IS the ascending (1-p).
                        extra.set(0, SqlExpr.Call.of(SqlFn.MINUS,
                                new SqlExpr.IntLit(1), extra.get(0)));
                    } else {
                        // DISCRETE is NOT symmetric (SQL-standard
                        // PERCENTILE_DISC picks the first value whose
                        // cume_dist >= p in DESC order — the ceil(p*N)-th
                        // largest); index the sorted list exactly.
                        fn = "__QDISC_DESC__";
                    }
                }
            } else {
                throw new IllegalStateException("boolean reducer arguments are"
                        + " only understood on percentile(p, ascending,"
                        + " continuous) and variance(isBiasCorrected)");
            }
        }
        // BI-VARIATE map: rowMapper(value, key) decomposes into the SQL
        // aggregate's two arguments — CORR(a, b), ARG_MAX(v, k), ...
        if (mapBody instanceof TypedNativeCall rm
                && rm.callee().qualifiedName().equals("meta::pure::functions::math::mathUtility::rowMapper")
                && rm.args().size() == 2) {
            SqlExpr first = scalar(rm.args().get(0), (v, name) -> resolveOrThrow(base, name));
            SqlExpr second = scalar(rm.args().get(1), (v, name) -> resolveOrThrow(base, name));
            if ("__WAVG__".equals(fn)) {
                // Weighted average: SUM(v*w)/SUM(w) — no single SQL reducer.
                return SqlExpr.Call.of(SqlFn.DIVIDE,
                        new SqlAgg.Reducer("SUM",
                                List.of(SqlExpr.Call.of(SqlFn.TIMES, first, second)), false),
                        new SqlAgg.Reducer("SUM", List.of(second), false));
            }
            return new SqlAgg.Reducer(fn, List.of(first, second), false);
        }
        if ("__WAVG__".equals(fn)) {
            throw new IllegalStateException(
                    "wavg expects a rowMapper(value, weight) map body");
        }
        if ((distinctValues || "__IS_DISTINCT__".equals(fn))
                && mapBody instanceof TypedVariable) {
            throw new com.legend.error.NotImplementedException(
                    "DISTINCT aggregate over a bare group variable — no value"
                    + " column to deduplicate");
        }
        if (mapBody instanceof TypedVariable && extra.isEmpty()) {
            return new SqlAgg.Reducer(fn, List.of(), false);
        }
        // CALENDAR native in map position: the value is the CASE over the
        // pre-joined calendar aliases; the fn's VALUE argument aggregates
        if (calendar != null
                && CalendarAgg.calendarCallOf(mapBody)
                        instanceof TypedNativeCall calCall) {
            SqlExpr calVal = scalar(calCall.args().get(3),
                    (v, name) -> resolveOrThrow(base, name));
            return new SqlAgg.Reducer(fn,
                    List.of(CalendarAgg.caseValue(calCall, calendar, calVal)),
                    false);
        }
        SqlExpr value = scalar(mapBody, (v, name) -> resolveOrThrow(base, name));
        if (valueCast != null) {
            value = castByPolicy(value, valueCast.source().info().type(), valueCast.target());
        }
        // isDistinct over a group: COUNT(DISTINCT x) = COUNT(x) — no single
        // SQL reducer (engine testGroupByIsDistinct golden).
        if ("__IS_DISTINCT__".equals(fn)) {
            if (!extra.isEmpty()) {
                throw new IllegalStateException("isDistinct aggregate with"
                        + " extra arguments — the group form takes none"
                        + " (audit 22a M5: dropped extras rendered the group"
                        + " SQL for a non-group call)");
            }
            return SqlExpr.Call.of(SqlFn.EQUAL,
                    new SqlAgg.Reducer("COUNT", List.of(value), true),
                    new SqlAgg.Reducer("COUNT", List.of(value), false));
        }
        // uniqueValueOnly over a group (collectionExtension.pure): the
        // single distinct value, else empty — CASE WHEN COUNT(DISTINCT x)
        // = 1 THEN MAX(x) END (max of one value IS the value).
        if ("__UNIQUE_VALUE_ONLY__".equals(fn)) {
            // the 2-arg form's DEFAULT rides as the CASE else
            SqlExpr uvDefault = extra.isEmpty() ? new SqlExpr.NullLit()
                    : extra.get(0);
            if (extra.size() > 1) {
                throw new IllegalStateException(
                        "uniqueValueOnly aggregate with " + extra.size()
                                + " extra arguments");
            }
            return new SqlExpr.Case(List.of(new SqlExpr.Case.When(
                    SqlExpr.Call.of(SqlFn.EQUAL,
                            new SqlAgg.Reducer("COUNT", List.of(value), true),
                            new SqlExpr.IntLit(1)),
                    new SqlAgg.Reducer("MAX", List.of(value), false))),
                    uvDefault);
        }
        // hashCode over a group: HASH(LIST(values)) — no single SQL reducer.
        // DuckDB hash() is UBIGINT (surfaces as a non-integer through JDBC);
        // the result is a pure Integer — shift into signed BIGINT range
        // (deterministic; real pure's hash VALUES are platform-specific,
        // only determinism and type are the contract).
        if ("__HASH_LIST__".equals(fn)) {
            // shift by 2^63 exactly (PLUS Long.MIN_VALUE — a MAX_VALUE
            // shift left hash 2^64-1 mapping to 2^63, one past BIGINT)
            return new SqlExpr.Cast(
                    SqlExpr.Call.of(SqlFn.PLUS,
                            new SqlExpr.Cast(
                                    SqlExpr.Call.of(SqlFn.HASH,
                                            new SqlAgg.Reducer("LIST", List.of(value), false)),
                                    SqlType.Scalar.HUGEINT),
                            new SqlExpr.IntLit(Long.MIN_VALUE)),
                    SqlType.Scalar.BIGINT);
        }
        // 1-arg joinStrings joins with the EMPTY separator
        // (stringExtension.pure:253) — DuckDB's bare STRING_AGG(x)
        // defaults to a COMMA, which would be silently wrong.
        if ("STRING_AGG".equals(fn) && extra.isEmpty()) {
            extra.add(new SqlExpr.StringLit(""));
        }
        // joinStrings(prefix, sep, suffix): STRING_AGG takes only the
        // separator — prefix/suffix concatenate AROUND the aggregate
        // (the audit: three extras produced an invalid 4-arg string_agg).
        if ("STRING_AGG".equals(fn) && extra.size() == 3) {
            return SqlExpr.Call.of(SqlFn.CONCAT,
                    SqlExpr.Call.of(SqlFn.CONCAT, extra.get(0),
                            new SqlAgg.Reducer(fn, List.of(value, extra.get(1)),
                                    false, aggOrder)),
                    extra.get(2));
        }
        // Descending DISCRETE percentile: the ceil(p*N)-th element of the
        // DESC-sorted values (SQL-standard PERCENTILE_DISC ... ORDER BY
        // DESC semantics — no direct DuckDB reducer).
        if ("__QDISC_DESC__".equals(fn)) {
            return SqlExpr.Call.of(SqlFn.LIST_GET,
                    SqlExpr.Call.of(SqlFn.LIST_SORT_DESC,
                            new SqlAgg.Reducer("LIST", List.of(value), false)),
                    new SqlExpr.Cast(
                            SqlExpr.Call.of(SqlFn.CEILING,
                                    SqlExpr.Call.of(SqlFn.TIMES,
                                            extra.get(0),
                                            new SqlAgg.Reducer("COUNT",
                                                    List.of(value), false))),
                            SqlType.Scalar.BIGINT));
        }
        List<SqlExpr> args = new ArrayList<>();
        args.add(value);
        args.addAll(extra);
        return new SqlAgg.Reducer(fn, args, distinctValues, aggOrder);
    }

    /**
     * extend (append=true) / project (append=false) with computed columns.
     * Column lambdas resolve against the CURRENT select via substitution, so
     * a plain-projection or star select stays flat.
     */
    /** extend(~cols): existing projections stay, computed columns APPEND. */
    private SqlSelect extend(SqlSelect src, List<TypedFuncCol> columns,
                             ExprType info) {
        SqlSelect base = Fold.extendFolds(src) ? src : isolate(src);
        return computedColumns(base, columns, info, true);
    }

    /** project(~cols): the computed columns REPLACE the projection list. */
    private SqlSelect project(SqlSelect src, List<TypedFuncCol> columns,
                              ExprType info) {
        SqlSelect base = Fold.projectionFolds(src) ? src : isolate(src);
        return computedColumns(base, columns, info, false);
    }

    /**
     * Lower computed columns over {@code base}: one attempt, isolate ONCE on
     * an unfoldable ref, then loud (isolation is idempotent for resolution).
     */
    private SqlSelect computedColumns(SqlSelect base, List<TypedFuncCol> columns,
                                      ExprType info,
                                      boolean keepExisting) {
        SqlSelect attempt1 = tryComputedColumns(base, columns, info, keepExisting);
        if (attempt1 != null) {
            return attempt1;
        }
        SqlSelect isolated = isolate(base);
        SqlSelect attempt2 = tryComputedColumns(isolated, columns, info, keepExisting);
        if (attempt2 != null) {
            return attempt2;
        }
        throw new IllegalStateException("extend/project columns "
                + columns.stream().map(TypedFuncCol::name).toList()
                + " reference names unresolvable even after isolation");
    }

    /** One pass; null when any column's refs would not fold against {@code base}. */
    private SqlSelect tryComputedColumns(SqlSelect base, List<TypedFuncCol> columns,
                                         ExprType info,
                                         boolean keepExisting) {
        List<SqlSelect.Projection> ps = new ArrayList<>();
        if (keepExisting) {
            // starProjections handles the JOIN-source case (no single alias:
            // bare * over the disjoint-by-invariant sides).
            ps.addAll(starProjections(base));
        }
        for (TypedFuncCol c : columns) {
            switch (attempt(() -> scalar(last(c.fn()), (v, name) -> resolveOrThrow(base, name)))) {
                case Resolution.Resolved r -> ps.add(new SqlSelect.Projection(r.expr(), c.name()));
                case Resolution.Unfoldable u -> {
                    return null;
                }
            }
        }
        return base.withProjections(ps, outputsOf(info));
    }

    private SqlSelect filter(TypedFilter f) {
        SqlSelect src = relation(f.source());
        boolean windowRef = false;
        SqlExpr predicate = null;
        if (tryPredicate(src, f.predicate()) instanceof Resolution.Resolved r) {
            predicate = r.expr();
        } else if (src.groupBy().isEmpty()) {
            // Window-aware path: refs to window-column aliases substitute the
            // WindowCall itself — QUALIFY admits window expressions.
            WindowPredicate viaProjections = tryWindowPredicate(src, f.predicate());
            if (viaProjections != null && viaProjections.sawWindow()) {
                predicate = viaProjections.expr();
                windowRef = true;
            }
        }
        if (predicate == null) {
            src = isolate(src);
            predicate = predicateOrThrow(src, f.predicate(), "filter");
        }
        Fold.FilterSlot slot = Fold.filterSlot(src, windowRef);
        if (slot == Fold.FilterSlot.ISOLATE) {
            src = isolate(src);
            predicate = predicateOrThrow(src, f.predicate(), "filter");
            slot = Fold.filterSlot(src, false);
        }
        return switch (slot) {
            case WHERE -> src.withWhere(src.where() == null ? predicate
                    : SqlExpr.Call.of(SqlFn.AND, src.where(), predicate));
            case HAVING -> src.withHaving(src.having() == null ? predicate
                    : SqlExpr.Call.of(SqlFn.AND, src.having(), predicate));
            case QUALIFY -> src.withQualify(src.qualify() == null ? predicate
                    : SqlExpr.Call.of(SqlFn.AND, src.qualify(), predicate));
            case ISOLATE -> throw new IllegalStateException("unreachable: isolated above");
        };
    }

    /**
     * Lower the predicate against this select's columns; null = a ref would
     * not fold. Over a GROUPED select, refs resolve to the projection
     * EXPRESSIONS themselves (group keys and aggregate calls are exactly what
     * standard SQL admits in HAVING).
     */
    /** The isolate-terminal boundary: the select was JUST isolated, so an
     * unfoldable ref can never become foldable — LOUD, never a dropped
     * predicate (the ONE retry contract, shared by filter and whereLambda). */
    private SqlExpr predicateOrThrow(SqlSelect isolated, TypedLambda lambda, String op) {
        return switch (tryPredicate(isolated, lambda)) {
            case Resolution.Resolved r -> r.expr();
            case Resolution.Unfoldable u -> throw new IllegalStateException(
                    op + " predicate references column '" + u.column()
                            + "', unresolvable even after isolation");
        };
    }

    private Resolution tryPredicate(SqlSelect select, TypedLambda lambda) {
        ColumnResolver columns = select.groupBy().isEmpty()
                ? scopedResolver(select, lambda.parameters().get(0))
                : (v, name) -> projectionExprOrThrow(select, name);
        return attempt(() -> scalar(last(lambda), columns));
    }

    private record WindowPredicate(SqlExpr expr, boolean sawWindow) {
    }

    /** Resolve refs via projections, noting whether any substituted a window call. */
    private WindowPredicate tryWindowPredicate(SqlSelect select, TypedLambda lambda) {
        var saw = new AtomicBoolean();
        return switch (attempt(() -> scalar(last(lambda), (v, name) -> {
            SqlExpr resolved = projectionExprOrThrow(select, name);
            if (resolved instanceof SqlExpr.WindowCall) {
                saw.set(true);
            }
            return resolved;
        }))) {
            case Resolution.Resolved r -> new WindowPredicate(r.expr(), saw.get());
            case Resolution.Unfoldable u -> null;
        };
    }

    /** A post-aggregation ref: the projection's expression, computed or not. */
    private SqlExpr projectionExprOrThrow(SqlSelect select, String column) {
        for (SqlSelect.Projection p : select.projections()) {
            if (column.equals(p.outputName())) {
                return p.expr();
            }
        }
        throw new UnfoldableRef(column);
    }

    private SqlExpr resolveOrThrow(SqlSelect select, String column) {
        SqlExpr resolved = Fold.resolveInto(select, column);
        if (resolved == null) {
            throw new UnfoldableRef(column);
        }
        return resolved;
    }

    /**
     * A lambda-body resolver over {@code select} — the correlation channel
     * for nested relation queries. VAR-AWARE: the lambda's OWN parameter
     * resolves against the own select; any OTHER variable belongs to an
     * ENCLOSING lambda and tries the outer scopes FIRST. Resolving outer
     * vars own-select-first silently SELF-CORRELATES whenever the outer
     * row's column name also exists on the inner row (same-named columns
     * across joined tables, self-joins — the audit's two wrong-answer
     * regressions). The own select remains the last resort for other vars
     * (legacy fallthrough).
     */
    private ColumnResolver scopedResolver(SqlSelect select, String ownVar) {
        // SNAPSHOT the enclosing scopes at creation: iterating the LIVE
        // deque includes this resolver itself once inner scopes run — any
        // correlated miss then recurses forever (audit: StackOverflow where
        // the contract promises a loud UnfoldableRef).
        var outers = List.copyOf(enclosing);
        return (var, name) -> {
            boolean own = var == null || var.equals(ownVar);
            if (own && attempt(() -> resolveOrThrow(select, name))
                    instanceof Resolution.Resolved o) {
                return o.expr();
            }
            for (var outer : outers) {
                if (attempt(() -> outer.resolve(var, name))
                        instanceof Resolution.Resolved found) {
                    return found.expr();
                }
            }
            if (!own && attempt(() -> resolveOrThrow(select, name))
                    instanceof Resolution.Resolved fallback) {
                return fallback.expr();
            }
            throw new UnfoldableRef(name);
        };
    }

    /**
     * Row-scope resolver: variable/property references to SQL expressions.
     * {@code propOrNull == null} means a bare {@code $var} reference. May
     * throw {@link UnfoldableRef}; callers at TRY boundaries convert via
     * {@link #attempt} (the ONE catch site).
     */
    @FunctionalInterface
    private interface ColumnResolver {
        SqlExpr resolve(String var, String propOrNull);
    }

    /** The resolve-or-fold outcome at a try boundary. */
    private sealed interface Resolution {
        record Resolved(SqlExpr expr) implements Resolution { }

        record Unfoldable(String column) implements Resolution { }
    }

    /**
     * The RELATION-LEVEL try boundary: build the op over the folded select;
     * a computed-column reference (extend'ed expression, window column) is
     * unfoldable in place, so ISOLATE and rebuild — references then resolve
     * to plain columns of the subselect. Loud when isolation cannot cure it.
     */
    private SqlSelect foldOrIsolate(SqlSelect base, String op,
            Function<SqlSelect, SqlSelect> build) {
        try {
            return build.apply(base);
        } catch (UnfoldableRef first) {
            try {
                return build.apply(isolate(base));
            } catch (UnfoldableRef second) {
                throw new IllegalStateException(op + " reference '"
                        + second.getMessage() + "' cannot be resolved even after"
                        + " isolation");
            }
        }
    }

    /** The EXPRESSION-LEVEL {@link UnfoldableRef} catch site: run the attempt, name the outcome. */
    private Resolution attempt(Supplier<SqlExpr> attemptFn) {
        try {
            return new Resolution.Resolved(attemptFn.get());
        } catch (UnfoldableRef e) {
            return new Resolution.Unfoldable(e.getMessage());
        }
    }

    /**
     * The resolve-or-fold SIGNAL (not an error): thrown per unresolvable
     * reference and converted to a {@link Resolution} at {@link #attempt}
     * or retried after isolation at {@link #foldOrIsolate} — never caught
     * anywhere else. Stack traces are suppressed — this is
     * control flow in a hot path, pending the sealed-Resolution redesign
     * (docs/DESIGN_DEBT.md).
     */
    private static final class UnfoldableRef extends RuntimeException {
        UnfoldableRef(String column) {
            super(column);
        }

        @Override
        public synchronized Throwable fillInStackTrace() {
            return this;
        }
    }

    /** select(~cols) / distinct(~cols): narrow the projection list. */
    /** select(~cols): narrow the projection list. */
    private SqlSelect narrowTo(SqlSelect src, List<String> columns,
                               ExprType info) {
        SqlSelect base = Fold.projectionFolds(src) ? src : isolate(src);
        return projectColumns(base, columns, info);
    }

    /** distinct(~cols): narrow AND dedup (distinct has its own fold policy). */
    private SqlSelect distinctNarrowTo(SqlSelect src, List<String> columns,
                                       ExprType info) {
        SqlSelect base = Fold.projectionFolds(src) ? src : isolate(src);
        if (!Fold.distinctNarrowFolds(base, columns)) {
            base = isolate(base);
        }
        return projectColumns(base, columns, info).withDistinct();
    }

    /**
     * Project {@code columns} off {@code base}, isolating ONCE if any fails
     * to resolve (then loud — isolation is idempotent for resolution). Two
     * clean attempts, no index-reset restarts.
     */
    private SqlSelect projectColumns(SqlSelect base, List<String> columns,
                                     ExprType info) {
        List<SqlSelect.Projection> ps = tryProjectAll(base, columns);
        if (ps == null) {
            base = isolate(base);
            ps = tryProjectAll(base, columns);
            if (ps == null) {
                throw new IllegalStateException("select/distinct columns " + columns
                        + " cannot all be resolved even after isolation");
            }
        }
        return base.withProjections(ps, outputsOf(info));
    }

    /** All columns resolved against {@code base}, or null if any misses. */
    private static List<SqlSelect.Projection> tryProjectAll(SqlSelect base, List<String> columns) {
        List<SqlSelect.Projection> ps = new ArrayList<>(columns.size());
        for (String c : columns) {
            SqlExpr e = Fold.resolveInto(base, c);
            if (e == null) {
                return null;
            }
            ps.add(new SqlSelect.Projection(e,
                    e instanceof SqlExpr.Column col && col.name().equals(c) ? null : c));
        }
        return ps;
    }

    private SqlSelect distinct(TypedDistinct d) {
        SqlSelect src = relation(d.source());
        if (d.columns() != null && !d.columns().isEmpty()) {
            return distinctNarrowTo(src, d.columns(), d.info());
        }
        return (Fold.distinctFolds(src) ? src : isolate(src)).withDistinct();
    }

    /**
     * rename lowers to a FULL explicit projection from the (always-known)
     * output schema &mdash; flat and self-describing; no EXCLUDE gymnastics.
     * Column ORDER is the checker's {@code T-Z+V} (real pure: renamed
     * columns move to the END) &mdash; iterating {@code r.info()} keeps the
     * SQL aligned with the typed schema the executor reads by index.
     */
    private SqlSelect rename(TypedRename r) {
        SqlSelect src = relation(r.source());
        SqlSelect base = Fold.projectionFolds(src) ? src : isolate(src);
        Type.RelationType outSchema = (Type.RelationType) r.info().type();
        // Each output column reverse-maps to the source column it renames.
        Function<String, String> sourceOf = out -> {
            for (TypedRename.ColRename cr : r.renames()) {
                if (cr.to().equals(out)) {
                    return cr.from();
                }
            }
            return out;
        };
        // Pre-pass: if ANY source column would not resolve to a plain column
        // reference in the folded select, isolate ONCE, then project.
        for (Type.Column c : outSchema.columns()) {
            if (Fold.resolveInto(base, sourceOf.apply(c.name())) == null) {
                base = isolate(base);
                break;
            }
        }
        List<SqlSelect.Projection> ps = new ArrayList<>();
        for (Type.Column c : outSchema.columns()) {
            String from = sourceOf.apply(c.name());
            SqlExpr e = Fold.resolveInto(base, from);
            if (e == null) {
                throw new IllegalStateException("rename source column '" + from
                        + "' cannot be resolved after isolation");
            }
            ps.add(new SqlSelect.Projection(e, c.name().equals(
                    e instanceof SqlExpr.Column col ? col.name() : null) ? null : c.name()));
        }
        return base.withProjections(ps, outputsOf(r.info()));
    }

    private SqlSelect sort(TypedSort s) {
        SqlSelect src = relation(s.source());
        SqlSelect base = Fold.sortFolds(src) ? src : isolate(src);
        List<SqlSelect.SortKey> keys = new ArrayList<>(s.keys().size());
        for (TypedSort.TypedSortKey k : s.keys()) {
            SqlExpr e = Fold.resolveInto(base, k.column());
            if (e == null) {
                base = isolate(base);
                return sortOnto(base, s);
            }
            keys.add(new SqlSelect.SortKey(e, k.ascending(), null));
        }
        return base.withOrderBy(keys);
    }

    /**
     * {@code sortBy(rel, key-lambda)} — ORDER BY over the lowered key
     * EXPRESSION (TypedSort is column-name-keyed; sortBy's key is a
     * per-row lambda). Fold.sortFolds decides extend-vs-isolate, same as
     * sort; the key expression resolves against the base select's row.
     */
    private SqlSelect sortBy(TypedSortBy sb) {
        SqlSelect src = relation(sb.source());
        SqlSelect base = Fold.sortFolds(src) ? src : isolate(src);
        // One isolate retry on an unfoldable key ref (a computed projection
        // column): behind the subselect it is a plain output column.
        SqlSelect fin1 = base;
        if (attempt(() -> scalar(last(sb.key()), (v, name) -> resolveOrThrow(fin1, name)))
                instanceof Resolution.Resolved r) {
            return base.withOrderBy(List.of(
                    new SqlSelect.SortKey(r.expr(), sb.ascending(), null)));
        }
        SqlSelect iso = isolate(base);
        SqlExpr key = scalar(last(sb.key()), (v, name) -> resolveOrThrow(iso, name));
        return iso.withOrderBy(List.of(new SqlSelect.SortKey(key, sb.ascending(), null)));
    }

    private SqlSelect sortOnto(SqlSelect base, TypedSort s) {
        List<SqlSelect.SortKey> keys = new ArrayList<>(s.keys().size());
        for (TypedSort.TypedSortKey k : s.keys()) {
            SqlExpr.Column e = Fold.sourceColumn(base.from(), k.column());
            if (e == null) {
                throw new IllegalStateException("sort key '" + k.column()
                        + "' cannot be resolved after isolation");
            }
            keys.add(new SqlSelect.SortKey(e, k.ascending(), null));
        }
        return base.withOrderBy(keys);
    }

    /** TDS literal → VALUES; empty → one all-NULL row gated by WHERE 1=0 (schema, zero rows). */
    private SqlSelect tdsLiteral(TypedTds tds) {
        Type.RelationType schema = (Type.RelationType) tds.info().type();
        List<String> names = schema.columns().stream().map(Type.Column::name).toList();
        // The _tds alias names the literal's VALUES source (engine parity;
        // the shared counter keeps multiple literals distinct).
        String alias = "_tds" + tdsCounter++;
        if (tds.rows().isEmpty()) {
            List<SqlExpr> nulls = names.stream().map(n -> (SqlExpr) new SqlExpr.NullLit()).toList();
            SqlSource.Values v = new SqlSource.Values(List.of(nulls), names, alias, outputsOf(tds.info()));
            return SqlSelect.starOf(v).withWhere(SqlExpr.Call.of(SqlFn.EQUAL,
                    new SqlExpr.IntLit(1), new SqlExpr.IntLit(0)));
        }
        List<List<SqlExpr>> rows = new ArrayList<>(tds.rows().size());
        for (List<String> row : tds.rows()) {
            List<SqlExpr> cells = new ArrayList<>(row.size());
            for (int i = 0; i < row.size(); i++) {
                cells.add(Scalars.tdsCell(row.get(i), schema.columns().get(i).type()));
            }
            rows.add(cells);
        }
        return SqlSelect.starOf(new SqlSource.Values(rows, names, alias, outputsOf(tds.info())));
    }

    // ==================================================================
    // Joins — a structural SOURCE (JoinTree); sides bind per lambda param
    // ==================================================================

    /**
     * USER-facing {@code navigate(~alias: <relation>, {s,t|cond})} — the
     * clean-sheet dynamic navigation over relations: a PREFIXED LEFT join
     * (alias_COL columns), riding the exact TypedJoin machinery; struct
     * reads ({@code $r.alias.COL}) flatten in the scalar path. Class-extent
     * navigates are STORE material and never reach the lowerer.
     */
    private SqlSelect navigate(TypedNavigate nav) {
        if (nav.form() != TypedNavigate.Form.PRE_MAP
                || !(nav.target().info().type() instanceof Type.RelationType targetRel)
                || nav.alias().isEmpty()) {
            throw new IllegalStateException("store-only navigate (class-extent"
                    + " target) reached the lowerer — resolver bug");
        }
        String alias = nav.alias().get();
        var srcRow = (Type.RelationType) nav.source().info().type();
        Type.RelationType targetRow = targetRel;
        List<Type.Column> flat = new ArrayList<>(srcRow.columns());
        for (Type.Column c : targetRow.columns()) {
            flat.add(new Type.Column(navFlatColumn(alias, c.name()), c.type(),
                    Multiplicity.Bounded.ZERO_ONE));
        }
        var flatInfo = new ExprType(
                new Type.RelationType(flat), nav.info().multiplicity());
        var leftKind = new TypedEnumValue(
                "meta::pure::functions::relation::JoinKind", "LEFT", nav.info());
        return join(new TypedJoin(nav.source(),
                nav.target(), leftKind, nav.predicate(),
                Optional.of(navSlotPrefix(alias)), flatInfo));
    }

    /** THE navigate flat-column convention ({@code slot_COL}): mint and
     * read-side reconstruction share this one owner (audit 15 — they were
     * spelled independently, the same drift class JoinIdentity retired). */
    private static String navSlotPrefix(String slot) {
        return slot + "_";
    }

    private static String navFlatColumn(String slot, String col) {
        return navSlotPrefix(slot) + col;
    }

    private SqlSelect join(TypedJoin j) {
        SqlSelect leftSel = relation(j.left());
        // A RENAME-ONLY select (star + plain column renames, no clauses —
        // what a PREFIXED join produces) can HOST further joins flat: its
        // join tree is the left side and its renames carry into the chain's
        // projections; refs to renamed columns in the ON condition
        // substitute to their underlying columns (the resolver's prefix
        // chains stay one flat SELECT — the real engine's shape).
        List<SqlSelect.Projection> leftCarry = null;
        SqlSource left;
        if (j.prefix().isPresent() && isRenameOnlySelect(leftSel)) {
            // Hosting is only sound when the new join is PREFIXED — the
            // prefixed joined() branch re-emits the carry; the unprefixed
            // branch is SELECT * and would DROP the renames/narrowing
            // (audit blocker: rename->join lost its rename silently).
            leftCarry = leftSel.projections();
            left = leftSel.from();
        } else {
            left = asLeftJoinSide(leftSel);
        }
        SqlSource right = asRightSide(relation(j.right()));
        SqlExpr on = sideCondition(j.condition(), left, right, leftCarry);
        SqlSource.Join.Kind kind = switch (j.kind().value()) {
            case "INNER" -> SqlSource.Join.Kind.INNER;
            case "LEFT" -> SqlSource.Join.Kind.LEFT;
            case "RIGHT" -> SqlSource.Join.Kind.RIGHT;
            case "FULL" -> SqlSource.Join.Kind.FULL;
            default -> throw new IllegalStateException("unknown join kind " + j.kind().value());
        };
        return joined(new SqlSource.Join(left, right, kind, on), j.prefix(),
                j.right(), j.info(), leftCarry);
    }

    /** asOfJoin: DuckDB ASOF LEFT JOIN; ON = optional keys AND the match inequality. */
    private SqlSelect asOfJoin(TypedAsOfJoin aj) {
        SqlSource left = asLeftJoinSide(relation(aj.left()));
        SqlSource right = asRightSide(relation(aj.right()));
        SqlExpr on = sideCondition(aj.match(), left, right);
        if (aj.condition().isPresent()) {
            on = SqlExpr.Call.of(SqlFn.AND, sideCondition(aj.condition().get(), left, right), on);
        }
        Set<String> leftNames = new HashSet<>();
        schemaOf(aj.left()).columns().forEach(c -> leftNames.add(c.name()));
        return joined(new SqlSource.Join(left, right, SqlSource.Join.Kind.ASOF_LEFT, on),
                aj.prefix(), aj.right(), aj.info(), null, leftNames::contains);
    }

    /**
     * The joined select: bare star when column names are disjoint (Phase G
     * guarantees), or left star + explicitly re-aliased right columns when a
     * prefix renames EVERY right column.
     */
    private SqlSelect joined(SqlSource.Join source, Optional<String> prefix,
                             TypedSpec rightNode, ExprType info) {
        return joined(source, prefix, rightNode, info, null, name -> true);
    }

    private SqlSelect joined(SqlSource.Join source, Optional<String> prefix,
                             TypedSpec rightNode, ExprType info,
                             List<SqlSelect.Projection> leftCarry) {
        return joined(source, prefix, rightNode, info, leftCarry, name -> true);
    }

    private SqlSelect joined(SqlSource.Join source, Optional<String> prefix,
                             TypedSpec rightNode, ExprType info,
                             List<SqlSelect.Projection> leftCarry,
                             Predicate<String> renameWhen) {
        SqlSelect out = SqlSelect.starOf(source);
        if (prefix.isEmpty()) {
            return out.withProjections(List.of(), outputsOf(info));
        }
        List<SqlSelect.Projection> ps = new ArrayList<>();
        if (leftCarry != null) {
            ps.addAll(leftCarry);   // the hosted chain's star + prior renames
        } else if (source.left() instanceof SqlSource.Join leftTree) {
            // A bare join tree has no single alias, and Star(null) would
            // expand the WHOLE FROM — leaking the new right side's
            // unprefixed columns (audit blocker). Enumerate the left
            // tree's columns explicitly (names are disjoint by the
            // Phase-G join invariant).
            for (OutputCol c : leftTree.outputs()) {
                ps.add(new SqlSelect.Projection(
                        Fold.sourceColumn(leftTree, c.name()), null));
            }
        } else {
            ps.add(new SqlSelect.Projection(new SqlExpr.Star(source.left().alias()), null));
        }
        for (Type.Column c : schemaOf(rightNode).columns()) {
            ps.add(new SqlSelect.Projection(
                    new SqlExpr.Column(source.right().alias(), c.name()),
                    renameWhen.test(c.name()) ? prefix.get() + c.name() : null));
        }
        return out.withProjections(ps, outputsOf(info));
    }

    /**
     * Star + plain-column renames, nothing else — the shape a prefixed join
     * produces. Such a select adds no row semantics; it can host further
     * joins with its renames carried forward.
     */
    private static boolean isRenameOnlySelect(SqlSelect s) {
        if (s.projections().isEmpty() || s.distinct()
                || s.where() != null || !s.groupBy().isEmpty() || s.having() != null
                || s.qualify() != null || !s.orderBy().isEmpty()
                || s.limit() != null || s.offset() != null) {
            return false;
        }
        if (!(s.from() instanceof SqlSource.Join || s.from() instanceof SqlSource.Table)) {
            return false;
        }
        for (SqlSelect.Projection p : s.projections()) {
            if (!(p.expr() instanceof SqlExpr.Star || p.expr() instanceof SqlExpr.Column)) {
                return false;
            }
        }
        return true;
    }

    /**
     * A join side must be FROM-addressable: a bare scan joins directly;
     * anything with clauses wraps. A bare JOIN-select may stay a bare join
     * tree ONLY on the LEFT — SQL join syntax is left-associative, so
     * {@code (a JOIN b) JOIN c} renders flat, while a join on the RIGHT would
     * be ambiguous and must wrap.
     */
    /**
     * A join's LEFT side: a bare select unwraps to its source — including a
     * bare join TREE (SQL joins are left-associative, so chains stay flat).
     */
    private SqlSource asLeftJoinSide(SqlSelect side) {
        return isBareSelect(side) ? side.from() : new SqlSource.Subselect(side, nextAlias());
    }

    /**
     * A join's RIGHT side (also pivot's source): a bare select unwraps ONLY
     * to a non-join source — a join tree on the right would re-associate.
     */
    private SqlSource asRightSide(SqlSelect side) {
        return isBareSelect(side) && !(side.from() instanceof SqlSource.Join)
                ? side.from()
                : new SqlSource.Subselect(side, nextAlias());
    }

    /** No clause set — the select adds nothing over its source. */
    private static boolean isBareSelect(SqlSelect side) {
        return side.projections().isEmpty() && !side.distinct()
                && side.where() == null && side.groupBy().isEmpty() && side.having() == null
                && side.qualify() == null && side.orderBy().isEmpty()
                && side.limit() == null && side.offset() == null;
    }

    /**
     * The two-parameter condition: each lambda variable binds to its side.
     * A flat-chained left side is a join TREE — its refs resolve by walking
     * side schemas ({@link Fold#sourceColumn}), not by a single alias.
     */
    private SqlExpr sideCondition(TypedLambda lambda, SqlSource left, SqlSource right) {
        return sideCondition(lambda, left, right, null);
    }

    private SqlExpr sideCondition(TypedLambda lambda, SqlSource left, SqlSource right,
                                  List<SqlSelect.Projection> leftCarry) {
        String leftVar = lambda.parameters().get(0);
        return scalar(last(lambda), (var, prop) -> {
            boolean isLeft = var.equals(leftVar);
            if (isLeft && leftCarry != null) {
                // A hosted chain's renamed column substitutes to its
                // underlying plain column (PF_OID -> t1.OID).
                for (SqlSelect.Projection pj : leftCarry) {
                    if (prop.equals(pj.outputName())
                            && pj.expr() instanceof SqlExpr.Column c) {
                        return c;
                    }
                }
            }
            SqlSource side = isLeft ? left : right;
            SqlExpr.Column c = side instanceof SqlSource.Join
                    ? Fold.sourceColumn(side, prop)
                    : new SqlExpr.Column(side.alias(), prop);
            if (c == null) {
                throw new IllegalStateException("join condition references unknown column '"
                        + prop + "' on its " + (isLeft ? "left" : "right") + " side");
            }
            return c;
        });
    }

    // ==================================================================
    // Window lowering — extend(over(...), ...) and whole-relation agg extend
    // ==================================================================

    /** extend(over(~p,[keys],frame), cols/aggs): window columns APPEND like extend. */
    private SqlSelect extendWindow(TypedExtendWindow w) {
        SqlSelect src = relation(w.source());
        SqlSelect base = Fold.windowFolds(src) ? src : isolate(src);
        return foldOrIsolate(base, "extend window", b -> {
            Over over = lowerOver(b, w.window());
            List<SqlSelect.Projection> ps = new ArrayList<>(starProjections(b));
            for (TypedFuncCol c : w.columns()) {
                SqlExpr e = windowScalar(last(c.fn()), b, over);
                ps.add(new SqlSelect.Projection(e, c.name()));
            }
            for (TypedAggCol a : w.aggs()) {
                ps.add(new SqlSelect.Projection(
                        windowize(aggValue(b, a), over.partitionBy(), over.orderBy(),
                                over.frame()),
                        a.name()));
            }
            return b.withProjections(ps, outputsOf(w.info()));
        });
    }

    /** extend(~total : x|$x.AGE : y|$y->sum()) — whole-relation window: SUM(x) OVER (). */
    private SqlSelect extendAgg(TypedExtendAgg ea) {
        SqlSelect src = relation(ea.source());
        SqlSelect base = Fold.windowFolds(src) ? src : isolate(src);
        return foldOrIsolate(base, "extend aggregate", b -> {
            List<SqlSelect.Projection> ps = new ArrayList<>(starProjections(b));
            for (TypedAggCol a : ea.aggs()) {
                ps.add(new SqlSelect.Projection(
                        windowize(aggValue(b, a), List.of(), List.of(), null),
                        a.name()));
            }
            return b.withProjections(ps, outputsOf(ea.info()));
        });
    }

    private List<SqlSelect.Projection> starProjections(SqlSelect base) {
        if (!base.projections().isEmpty()) {
            return base.projections();
        }
        SqlExpr star = base.from() instanceof SqlSource.Join
                ? new SqlExpr.Star(null) : new SqlExpr.Star(base.from().alias());
        return List.of(new SqlSelect.Projection(star, null));
    }

    private record Over(List<SqlExpr> partitionBy, List<SqlSelect.SortKey> orderBy,
                        SqlExpr.WindowCall.Frame frame) {
    }

    /** Partition/order/frame of an over(...) — DESC→NULLS FIRST, ASC→NULLS LAST (master's pin). */
    private Over lowerOver(SqlSelect base, TypedOver over) {
        List<SqlExpr> parts = new ArrayList<>(over.partitions().size());
        for (String p : over.partitions()) {
            parts.add(resolveOrThrow(base, p));
        }
        List<SqlSelect.SortKey> keys = new ArrayList<>(over.sortKeys().size());
        for (TypedSort.TypedSortKey k : over.sortKeys()) {
            keys.add(new SqlSelect.SortKey(resolveOrThrow(base, k.column()), k.ascending(),
                    k.ascending() ? SqlSelect.SortKey.NullOrder.NULLS_LAST
                            : SqlSelect.SortKey.NullOrder.NULLS_FIRST));
        }
        return new Over(parts, keys, over.frame().map(this::frame).orElse(null));
    }

    /** rows(a,b) / range(a,b): negative→PRECEDING, 0→CURRENT ROW, positive→FOLLOWING. */
    private SqlExpr.WindowCall.Frame frame(TypedSpec spec) {
        if (!(spec instanceof TypedNativeCall call)) {
            throw new IllegalStateException("window frame lowering expects rows()/range(), got "
                    + spec.getClass().getSimpleName());
        }
        // INTERVAL ranges (_range(n, DurationUnit, m, DurationUnit) and the
        // unbounded mixes): each bounded side pairs an Integer with its
        // DurationUnit — RANGE BETWEEN INTERVAL n UNIT PRECEDING/FOLLOWING.
        {
            List<TypedSpec> as = call.args();
            boolean interval = as.stream().anyMatch(a ->
                    a instanceof TypedEnumValue ev
                            && ev.enumFqn().equals("meta::pure::functions::date::DurationUnit"));
            if (interval) {
                SqlExpr.WindowCall.Frame.Bound from;
                SqlExpr.WindowCall.Frame.Bound to;
                int i = 0;
                if (isUnboundedCall(as.get(i))) {
                    from = new SqlExpr.WindowCall.Frame.Bound.UnboundedPreceding();
                    i += 1;
                } else {
                    from = intervalBound(as.get(i), as.get(i + 1), true);
                    i += 2;
                }
                if (i < as.size() && isUnboundedCall(as.get(i))) {
                    to = new SqlExpr.WindowCall.Frame.Bound.UnboundedFollowing();
                } else {
                    to = intervalBound(as.get(i), as.get(i + 1), false);
                }
                return new SqlExpr.WindowCall.Frame(
                        SqlExpr.WindowCall.Frame.Kind.RANGE, from, to);
            }
        }
        boolean rows = Pure.nativeNamed("rows", call.callee().signatureKey());
        // LITERAL bounds validate here: a start beyond the end (2 FOLLOWING
        // .. 1 FOLLOWING; 1 FOLLOWING .. 1 PRECEDING) is a COMPILE error,
        // never bad SQL (PCT: invalid window frame boundary).
        Number from = numericBound(call.args().get(0));
        Number to = numericBound(call.args().get(1));
        if (from != null && to != null && from.doubleValue() > to.doubleValue()) {
            // Real rows()/_range() assert text verbatim (PCT message parity).
            throw new ModelException(
                    LegendCompileException.Phase.LOWER,
                    "Invalid window frame boundary - lower bound of window frame"
                            + " cannot be greater than the upper bound!");
        }
        return new SqlExpr.WindowCall.Frame(
                rows ? SqlExpr.WindowCall.Frame.Kind.ROWS : SqlExpr.WindowCall.Frame.Kind.RANGE,
                bound(call.args().get(0), true), bound(call.args().get(1), false));
    }

    private SqlExpr.WindowCall.Frame.Bound bound(TypedSpec arg, boolean fromSide) {
        // A negative literal arrives as unary minus AROUND the number — unwrap.
        if (arg instanceof TypedNativeCall neg
                && Pure.nativeNamed("minus", neg.callee().signatureKey())
                && neg.args().size() == 1 && numericBound(neg.args().get(0)) != null) {
            return new SqlExpr.WindowCall.Frame.Bound.Preceding(numericBound(neg.args().get(0)));
        }
        Number n = numericBound(arg);
        if (n != null) {
            double v = n.doubleValue();
            if (v < 0) {
                return new SqlExpr.WindowCall.Frame.Bound.Preceding(negate(n));
            }
            if (v > 0) {
                return new SqlExpr.WindowCall.Frame.Bound.Following(n);
            }
            return new SqlExpr.WindowCall.Frame.Bound.CurrentRow();
        }
        if (arg instanceof TypedNativeCall call
                && Pure.nativeNamed("unbounded", call.callee().signatureKey())) {
            return fromSide ? new SqlExpr.WindowCall.Frame.Bound.UnboundedPreceding()
                    : new SqlExpr.WindowCall.Frame.Bound.UnboundedFollowing();
        }
        // NO fallback: an unrecognized bound is a loud error, never UNBOUNDED.
        throw new IllegalStateException("window frame bound must be a numeric literal or"
                + " unbounded(), got " + arg.getClass().getSimpleName());
    }

    /**
     * Whether a write destination reaches a PHYSICAL store table. A
     * TDS-accessor destination normalizes to the literal relation itself
     * (no table anywhere) — the write is vacuous and only the count is
     * observable.
     */
    private static boolean containsStoreTable(TypedSpec n) {
        if (n instanceof TypedTableReference
                || n instanceof TypedPackageableRef) {
            return true;
        }
        for (TypedSpec child : n.children()) {
            if (containsStoreTable(child)) {
                return true;
            }
        }
        return false;
    }

    private static boolean isUnboundedCall(TypedSpec arg) {
        return arg instanceof TypedNativeCall c
                && Pure.nativeNamed("unbounded", c.callee().signatureKey());
    }

    /** One INTERVAL frame side: signed Integer + DurationUnit literal. */
    private SqlExpr.WindowCall.Frame.Bound intervalBound(TypedSpec amount, TypedSpec unit,
            boolean fromSide) {
        Number n = numericBound(amount);
        if (n == null || !(unit instanceof TypedEnumValue ev)) {
            throw new IllegalStateException("interval frame bound needs a literal"
                    + " Integer and a DurationUnit literal");
        }
        long v = n.longValue();
        if (v < 0) {
            return new SqlExpr.WindowCall.Frame.Bound.IntervalPreceding(-v, ev.value());
        }
        if (v > 0) {
            return new SqlExpr.WindowCall.Frame.Bound.IntervalFollowing(v, ev.value());
        }
        return new SqlExpr.WindowCall.Frame.Bound.CurrentRow();
    }

    /** The numeric value of a literal frame bound, or null (RANGE takes decimals). */
    private static Number numericBound(TypedSpec arg) {
        // A negative literal arrives as unary minus AROUND the number.
        if (arg instanceof TypedNativeCall neg
                && Pure.nativeNamed("minus", neg.callee().signatureKey())
                && neg.args().size() == 1) {
            Number inner = numericBound(neg.args().get(0));
            return inner == null ? null : -inner.doubleValue();
        }
        return switch (arg) {
            case TypedCInteger c -> c.value().longValue();
            case TypedCFloat c -> c.value();
            case TypedCDecimal c -> c.value();
            default -> null;
        };
    }

    private static Number negate(Number n) {
        return switch (n) {
            case Long l -> -l;
            case Double d -> -d;
            case java.math.BigDecimal b -> b.negate();
            default -> -n.doubleValue();
        };
    }

    /**
     * A window column's body, classified AT LOWERING (the deliberate Phase-G
     * deferral): ranking natives take no column; value natives (lag/lead/...)
     * get their column from the WRAPPING property access
     * ({@code $p->lag($r).SALARY}); anything else lowers as an ordinary scalar
     * whose window-native subterms recurse through this method.
     */
    private SqlExpr windowScalar(TypedSpec body, SqlSelect base, Over over) {
        switch (body) {
            case TypedPropertyAccess p when p.source() instanceof TypedNativeCall call
                    && Windows.lookup(call.callee()) != null -> {
                Windows.WindowFn fn = Windows.lookup(call.callee());
                List<SqlExpr> args = new ArrayList<>();
                // Resolve through the select — a folded project's column is
                // an ALIAS whose defining expression must substitute (a raw
                // FROM-qualified ref binds to nothing).
                args.add(resolveOrThrow(base, p.property()));
                trailingIntArgs(call, args);
                return new SqlExpr.WindowCall(new SqlAgg.ValueFn(fn.sqlName(), args),
                        over.partitionBy(), over.orderBy(), over.frame());
            }
            // reduce(rel, w, row, map, agg): the WINDOWED map+reduce — the
            // agg-col machinery arriving as a native call (real reduce.pure).
            // The synthetic TypedAggCol reuses aggValue's whole reducer
            // dispatch (rowMapper decomposition, composed aggs, casts), and
            // windowize stamps the shared window spec on every reducer.
            case TypedNativeCall call
                    when call.callee().qualifiedName()
                            .equals("meta::pure::functions::relation::reduce")
                    && call.args().size() == 5
                    && call.args().get(3) instanceof TypedLambda mapFn
                    && call.args().get(4) instanceof TypedLambda aggFn -> {
                return windowize(aggValue(base, new TypedAggCol("_reduce", mapFn, aggFn)),
                        over.partitionBy(), over.orderBy(), over.frame());
            }
            // zScore(p,w,r,~col): COMPOSED window expression — real zScore.pure
            // is (col - average(...)) / max(stdDevPopulation(...), 1e-10).
            case TypedNativeCall call
                    when call.callee().qualifiedName()
                            .equals("meta::pure::functions::math::zScore")
                    && call.args().size() == 4
                    && call.args().get(3)
                            instanceof TypedColSpec zcs -> {
                SqlExpr col = resolveOrThrow(base, zcs.name());
                SqlExpr avg = new SqlExpr.WindowCall(
                        new SqlAgg.Reducer("AVG", List.of(col), false),
                        over.partitionBy(), over.orderBy(), over.frame());
                SqlExpr std = new SqlExpr.WindowCall(
                        new SqlAgg.Reducer("STDDEV_POP", List.of(col), false),
                        over.partitionBy(), over.orderBy(), over.frame());
                return SqlExpr.Call.of(SqlFn.DIVIDE,
                        SqlExpr.Call.of(SqlFn.MINUS, col, avg),
                        SqlExpr.Call.of(SqlFn.GREATEST, std,
                                new SqlExpr.DecimalLit(
                                        new java.math.BigDecimal("0.0000000001"))));
            }
            // Real pure's 4-arg colToAgg window aggregates: average(p,w,r,~col).
            case TypedNativeCall call when Windows.aggregate(call.callee()) != null -> {
                TypedSpec colArg = call.args().get(call.args().size() - 1);
                if (!(colArg instanceof TypedColSpec cs)) {
                    throw new IllegalStateException(
                            "window aggregate colToAgg must be a ~column colspec");
                }
                return new SqlExpr.WindowCall(
                        new SqlAgg.Reducer(Windows.aggregate(call.callee()),
                                // resolve through the select — a folded
                                // project's alias substitutes its expression
                                List.of(resolveOrThrow(base, cs.name())),
                                false),
                        over.partitionBy(), over.orderBy(), over.frame());
            }
            case TypedNativeCall call when Windows.lookup(call.callee()) != null -> {
                Windows.WindowFn fn = Windows.lookup(call.callee());
                if (fn.kind() != Windows.Kind.RANKING) {
                    throw new IllegalStateException("window value function '"
                            + call.callee().qualifiedName()
                            + "' needs a property access naming its column");
                }
                List<SqlExpr> args = new ArrayList<>();
                trailingIntArgs(call, args);
                return new SqlExpr.WindowCall(new SqlAgg.RankingFn(fn.sqlName(), args),
                        over.partitionBy(), over.orderBy(), over.frame());
            }
            case TypedNativeCall call -> {
                List<SqlExpr> args = call.args().stream()
                        .map(a -> windowScalar(a, base, over)).toList();
                return Scalars.lower(call, args);
            }
            // Thunk lambdas (if branches) stay on the WINDOW channel — their
            // bodies may hold lag/lead property accesses that plain scalar
            // lowering cannot place.
            case TypedLambda l when l.parameters().isEmpty() -> {
                return new SqlExpr.Lambda(l.parameters(), windowScalar(last(l), base, over));
            }
            // Casts keep the channel too (window bodies end in ->cast(@Date)).
            case TypedCast c -> {
                return cast(c, windowScalar(c.source(), base, over));
            }
            // if(cond, |then, |else) in a window body: CASE WHEN whose arms
            // stay on the window channel (lead/lag accesses inside branches).
            case TypedIf i -> {
                return new SqlExpr.Case(
                        List.of(new SqlExpr.Case.When(
                                windowScalar(i.condition(), base, over),
                                windowScalar(thunkBody(i.thenBranch()), base, over))),
                        i.elseBranch().map(e -> windowScalar(thunkBody(e), base, over))
                                .orElse(new SqlExpr.NullLit()));
            }
            default -> {
                return scalar(body, (v, name) -> resolveOrThrow(base, name));
            }
        }
    }

    /**
     * Literal Integer args (ntile n, lag/lead offset) ride along; variable
     * refs are the window params; anything else is LOUD, never dropped.
     */
    private static void trailingIntArgs(TypedNativeCall call, List<SqlExpr> args) {
        for (TypedSpec a : call.args()) {
            if (a instanceof TypedCInteger c) {
                args.add(new SqlExpr.IntLit(c.value().longValue()));
            } else if (!(a instanceof TypedVariable)) {
                throw new IllegalStateException("window function argument of kind "
                        + a.getClass().getSimpleName() + " is not supported (literals only)");
            }
        }
    }

    // ==================================================================
    // Scalar lowering (lambda bodies)
    // ==================================================================

    /** {@code columns} resolves (lambda variable, property) to a SQL expression in scope. */
    private SqlExpr scalar(TypedSpec spec, ColumnResolver columns) {
        return switch (spec) {
            // A literal BEYOND long (the parser kept it a BigInteger)
            // renders as a plain numeric literal — DuckDB reads HUGEINT.
            case TypedCInteger c -> c.value() instanceof java.math.BigInteger big
                    ? new SqlExpr.DecimalLit(new java.math.BigDecimal(big))
                    : new SqlExpr.IntLit(c.value().longValue());
            case TypedCString c -> new SqlExpr.StringLit(c.value());
            case TypedCBoolean c -> new SqlExpr.BoolLit(c.value());
            case TypedCFloat c -> new SqlExpr.FloatLit(c.value());
            case TypedCDecimal c ->
                    new SqlExpr.DecimalLit(c.value());
            // Date literals: full dates/timestamps render typed; PARTIAL
            // dates (year / year-month) compare as STRINGS in SQL (master's
            // pinned semantics) — represented as string literals here.
            case TypedCDate d -> switch (d.value()) {
                case PureDateLiteral.StrictDate sd ->
                        new SqlExpr.DateLit(sd.toEngineString());
                case PureDateLiteral.Year y ->
                        new SqlExpr.StringLit(y.toEngineString());
                case PureDateLiteral.YearMonth ym ->
                        new SqlExpr.StringLit(ym.toEngineString());
                // Every time-bearing precision is a TIMESTAMP — exhaustive,
                // so a new precision variant demands a decision here.
                // HOUR/MINUTE-precision literals PAD to the full timestamp
                // shape SQL demands (%2015-04-15T17 is 17:00:00); second-level
                // precision is already full.
                case PureDateLiteral.DateWithHour h ->
                        new SqlExpr.TimestampLit(h.toEngineString() + ":00:00");
                case PureDateLiteral.DateWithMinute mi ->
                        new SqlExpr.TimestampLit(mi.toEngineString() + ":00");
                case PureDateLiteral.DateWithSecond se ->
                        new SqlExpr.TimestampLit(se.toEngineString());
                case PureDateLiteral.DateWithSubsecond su ->
                        new SqlExpr.TimestampLit(su.toEngineString());
            };
            // %latest in VALUE position (generated milestoning-date reads:
            // the engine's k_businessDate golden projects the LatestDate
            // constant '9999-12-31T00:00:00.0000+0000') — the FIXED engine
            // sentinel, not the table's INFINITY_DATE (which governs only
            // the milestoning PREDICATE, TemporalFrame's arm)
            case com.legend.compiler.spec.typed.TypedCLatestDate ignored ->
                    new SqlExpr.TimestampLit("9999-12-31 00:00:00.0000");
            // The EMPTY collection [] (Nil[0]) in scalar position IS SQL
            // NULL — a [0] value has no cell representation other than null
            // (the mapping enum decode chain's tail: CASE ... ELSE NULL).
            case TypedCollection c when c.elements().isEmpty() -> new SqlExpr.NullLit();
            // A HETEROGENEOUS literal — element LUB Any ([1, 'a']) or a class
            // LUB with NO canonical layout (mixed instance kinds meeting at
            // an abstract ancestor): each element wraps as variant JSON, the
            // one SQL carrier that keeps per-element runtime kinds (a raw
            // mixed array cannot even type).
            case TypedCollection c when c.info().type() instanceof Type.ClassType ct
                    && !PlatformTypes.isVariant(ct)
                    && !PlatformTypes.isNil(ct)
                    && classLayout.apply(ct).isEmpty() ->
                    new SqlExpr.ArrayLit(c.elements().stream()
                            .map(e -> (SqlExpr) SqlExpr.Call.of(
                                    SqlFn.TO_VARIANT, scalar(e, columns)))
                            .toList());
            case TypedCollection c -> {
                // HETEROGENEOUS Pair elements (Pair<String,String> with
                // Pair<String,Integer>: LUB Pair<String,Any>): every element
                // CASTS to the LUB's struct shape or the array cannot type
                if (c.info().type() instanceof Type.GenericType lubG
                        && PlatformTypes
                                .isPairCarrier(lubG)) {
                    boolean uniform = c.elements().stream()
                            .allMatch(e -> e.info().type().equals(c.info().type()));
                    if (!uniform) {
                        // rebuild each struct with per-field COERCION to the
                        // LUB: Any-typed slots take the variant carrier
                        // (to_json), never a text CAST
                        yield new SqlExpr.ArrayLit(c.elements().stream()
                                .map(e -> (SqlExpr) pairToLub(scalar(e, columns),
                                        e.info().type(), lubG))
                                .toList());
                    }
                }
                yield new SqlExpr.ArrayLit(
                        c.elements().stream().map(e -> scalar(e, columns)).toList());
            }
            // $r.alias.COL — a NAVIGATE slot's struct column flattens to
            // its prefixed physical column (alias_COL).
            case TypedPropertyAccess p when p.source() instanceof TypedPropertyAccess inner
                    && inner.source() instanceof TypedVariable v
                    && inner.info().type() instanceof Type.RelationType
                    -> columns.resolve(v.name(),
                            navFlatColumn(inner.property(), p.property()));
            // A let-bound VALUE's field ($person.firstName after
            // |let person = ^Person(…)): extract from the lowered binding —
            // there is no row scope to resolve against.
            case TypedPropertyAccess p when p.source() instanceof TypedVariable v
                    && letBindings.containsKey(v.name())
                    -> new SqlExpr.StructGet(letBindings.get(v.name()), p.property());
            case TypedPropertyAccess p when p.source() instanceof TypedVariable v
                    -> columns.resolve(v.name(), p.property());
            // Field access on a CLASS-typed VALUE (an instance literal, a
            // native call's struct result, a nested pair): the visible-literal
            // case inlines the field's own expression (no struct round-trip);
            // anything opaque extracts from the struct.
            // A property CHAIN over to(@Class) on a VARIANT (to(@Firm).boss.name):
            // every hop is a JSON field extraction; only the LEAF materializes
            // (real PCT testToClassAndAccessNestedProperty pins multi-hop).
            case TypedPropertyAccess p when variantCastBase(p) != null -> {
                ArrayDeque<String> path = new ArrayDeque<>();
                TypedSpec cur = p;
                while (cur instanceof TypedPropertyAccess pa) {
                    path.addFirst(pa.property());
                    cur = pa.source();
                }
                var vc = (TypedCast) cur;
                SqlExpr e = scalar(vc.source(), columns);
                for (String seg : path) {
                    e = SqlExpr.Call.of(SqlFn.VARIANT_GET, e,
                            new SqlExpr.StringLit(seg));
                }
                Type leaf = p.info().type();
                if (leaf instanceof Type.ClassType lc
                        && !PlatformTypes.isVariant(lc)
                        && !PlatformTypes.isAny(lc)) {
                    // a class-typed LEAF is the unsupported-column case —
                    // real relation runtime's verbatim rejection
                    throw new ModelException(
                            LegendCompileException.Phase.LOWER,
                            "The type " + lc.fqn() + " is not supported yet!");
                }
                yield p.info().multiplicity().isMany()
                        ? new SqlExpr.Cast(e,
                                new SqlType.Array(PureSql.type(leaf)))
                        : new SqlExpr.Cast(e, PureSql.type(leaf));
            }
            case TypedPropertyAccess p when p.source()
                        instanceof TypedNewInstance ni -> {
                TypedSpec v = ni.properties().get(p.property());
                // The MODEL declares the field; an unset optional is NULL.
                yield v == null ? new SqlExpr.NullLit() : scalar(v, columns);
            }
            default -> scalarStructural(spec, columns);
        };
    }

    /** Scalar lowering, arm group (sequential order preserved:
     * guarded patterns depend on it) — the 523-line dispatch split
     * at arm boundaries; each group defaults to the next. */
    private SqlExpr scalarStructural(TypedSpec spec, ColumnResolver columns) {
        return switch (spec) {
            // Field access over a TO-MANY class value (filter(...).legalName)
            // MAPS the extraction; a to-one source extracts directly.
            // List.values over the bare-list carrier is the IDENTITY.
            case TypedPropertyAccess p when "values".equals(p.property())
                    && PlatformTypes
                            .isListCarrier(p.source().info().type()) ->
                    scalar(p.source(), columns);
            case TypedPropertyAccess p when classLayout.apply(p.source().info().type()).isPresent()
                    && isMany(p.source()) -> {
                String elem = "_pa" + aliasCounter++;
                yield SqlExpr.Call.of(SqlFn.LIST_TRANSFORM,
                        scalar(p.source(), columns),
                        new SqlExpr.Lambda(List.of(elem),
                                new SqlExpr.StructGet(
                                        new SqlExpr.Column(null, elem), p.property())));
            }
            case TypedPropertyAccess p when classLayout.apply(p.source().info().type()).isPresent()
                    -> new SqlExpr.StructGet(scalar(p.source(), columns), p.property());
            // ^Class(prop=value, …) as a VALUE: a struct with the MODEL's
            // canonical layout (declared stored properties, declaration
            // order) — never the instance's own field set; an omitted
            // property is a NULL field.
            // ^$existing(prop=value, …): the copy is the source's canonical
            // struct with the overridden fields replaced — pure layout
            // rebuild, no new SQL shapes.
            case TypedCopyInstance cp -> {
                // the List CARRIER is a bare array, not its layout struct —
                // a values override replaces it wholesale; other platform
                // carriers reject loudly rather than emit the wrong shape
                if (PlatformTypes
                        .isListCarrier(cp.info().type())
                        || cp.classFqn().equals(
                                PlatformTypes.LIST)) {
                    TypedSpec ov = cp.overrides().get("values");
                    yield ov == null ? scalar(cp.source(), columns)
                            : asList(scalar(ov, columns), isMany(ov));
                }
                if (PlatformTypes
                        .isMapCarrier(cp.info().type())) {
                    throw new NotImplementedException(
                            "^$var(…) copy of the Map carrier is not lowered");
                }
                var layout = classLayout.apply(cp.info().type()).orElseThrow(() ->
                        new IllegalStateException("^$var(…) copy of " + cp.classFqn()
                                + " has no canonical layout"));
                SqlExpr src = scalar(cp.source(), columns);
                yield new SqlExpr.StructLit(layout.stream().map(c -> {
                    TypedSpec ov = cp.overrides().get(c.name());
                    SqlExpr v = ov != null ? scalar(ov, columns)
                            : new SqlExpr.StructGet(src, c.name());
                    if (ov != null && c.multiplicity() instanceof
                            Multiplicity.Bounded b
                            && b.isMany()) {
                        v = asList(v, isMany(ov));
                    }
                    return new SqlExpr.StructLit.Field(c.name(), v);
                }).toList());
            }
            case TypedNewInstance n -> {
                // ^List(values=[...]): the List CARRIER is the bare SQL list
                // (the same carrier list() produces — one carrier per type).
                if (n.classFqn().equals(
                        PlatformTypes.LIST)) {
                    TypedSpec values = n.properties().get("values");
                    yield values == null
                            ? new SqlExpr.ArrayLit(List.of())
                            : asList(scalar(values, columns), isMany(values));
                }
                // ^Pair(first=..., second=...): the Pair STRUCT carrier —
                // its layout IS first/second (the platform declaration)
                if (n.classFqn().equals(
                        PlatformTypes.PAIR)) {
                    yield new SqlExpr.StructLit(List.of(
                            new SqlExpr.StructLit.Field("first",
                                    scalar(n.properties().get("first"), columns)),
                            new SqlExpr.StructLit.Field("second",
                                    scalar(n.properties().get("second"), columns))));
                }
                var layout = classLayout.apply(n.info().type()).orElseThrow(() ->
                        new IllegalStateException("class value ^" + n.classFqn()
                                + "(…) has no canonical layout — the class declares no"
                                + " stored properties (or no model rides this lowering)"));
                yield new SqlExpr.StructLit(layout.stream().map(c -> {
                    TypedSpec value = n.properties().get(c.name());
                    SqlExpr v = value != null ? scalar(value, columns) : new SqlExpr.NullLit();
                    // A TO-MANY property is LIST-shaped in the canonical
                    // layout even when this instance supplies one value —
                    // every instance of a class shares ONE struct shape. The
                    // wrap decision uses the VALUE's typed multiplicity: an
                    // already-many expression ($p.nicknames) is a list even
                    // when it doesn't lower to a literal array (audit:
                    // structural-only check double-wrapped it).
                    if (c.multiplicity() instanceof
                            Multiplicity.Bounded b
                            && b.isMany()) {
                        v = asList(v, value != null && isMany(value));
                    }
                    return new SqlExpr.StructLit.Field(c.name(), v);
                }).toList());
            }
            // A bare variable: a query-level let binding substitutes; else a
            // lambda variable (a list element inside exists/forAll etc.).
            case TypedVariable v -> letBindings.containsKey(v.name())
                    ? letBindings.get(v.name())
                    : columns.resolve(v.name(), null);
            // An inner lambda: ALL its parameters shadow; everything else
            // resolves outward through the enclosing resolver.
            case TypedLambda l -> new SqlExpr.Lambda(l.parameters(),
                    scalar(last(l), lambdaResolver(l.parameters(), columns)));
            // RELATION-level predicates — the true-SQL-EXISTS family. The
            // collection natives accept a Relation argument (T binds the
            // relation; the lambda is row-shaped via relation column access);
            // in SQL these ARE the EXISTS forms, correlated via the enclosing
            // scope stack:
            //   exists(rel, p)  -> EXISTS (SELECT * FROM rel WHERE p)
            //   forAll(rel, p)  -> NOT EXISTS (... WHERE NOT p)   [vacuously true]
            //   isEmpty(rel)    -> NOT EXISTS (...);  isNotEmpty -> EXISTS (...)
            //   size(rel)       -> (SELECT COUNT(*) FROM ...)
            case TypedNativeCall n when n.args().size() >= 1
                    && n.args().get(0).info().type() instanceof Type.RelationType
                    && relationPredicate(n) != null -> {
                var predicate = Objects.requireNonNull(relationPredicate(n));
                enclosing.push(columns);
                try {
                    yield predicate.lower(this, n);
                } finally {
                    enclosing.pop();
                }
            }
            case TypedFold f -> fold(f, columns);

            // map over a COLLECTION value -> listTransform (relation map is H).
            // pure map FLATTENS collection-valued mappers (audit 22a H3:
            // [1,2,3]->map(x|[$x,10*$x]) is the FLAT 6-element collection,
            // not 3 nested lists) — same TypedCollection-body policy as the
            // relation->map value-collection arm below.
            case TypedMap m
                    when !(m.source().info().type() instanceof Type.RelationType) -> {
                SqlExpr transformed = SqlExpr.Call.of(SqlFn.LIST_TRANSFORM,
                        scalar(m.source(), columns), scalar(m.mapper(), columns));
                yield isCollectionMapper(m.mapper())
                        ? SqlExpr.Call.of(SqlFn.LIST_FLATTEN, transformed)
                        : transformed;
            }

            // Variant navigation: get(v, key) -> JSON access. The MAP
            // overload of the same bare name lowers through its own rule.
            case TypedNativeCall n when isFamily(n, "get")
                    && !PlatformTypes
                            .isMapCarrier(n.args().get(0).info().type()) ->
                    SqlExpr.Call.of(SqlFn.VARIANT_GET,
                            scalar(n.args().get(0), columns), scalar(n.args().get(1), columns));

            case TypedCast c -> cast(c, columns);

            // if(cond, {|then}, {|else}) — scalar position: CASE WHEN.
            // If-chains (the mapping enum decode emission) render as NESTED
            // CASE expressions in the otherwise slot — correct; single-CASE
            // flattening is a cosmetic peephole if ever demanded.
            case TypedIf i -> new SqlExpr.Case(
                    List.of(new SqlExpr.Case.When(
                            scalar(i.condition(), columns),
                            scalar(thunkBody(i.thenBranch()), columns))),
                    i.elseBranch().map(e -> scalar(thunkBody(e), columns))
                            .orElse(new SqlExpr.NullLit()));

            // An enum VALUE in scalar position renders as its name string
            // (plangen :2591 parity; the mapping decode CASE compares against
            // these names, and result cells carry the name). Cross-type
            // equality (enum vs string / different enums) is guarded in the
            // equality arm below — silently-true 'NYC'=='NYC' across types
            // was an audit finding.
            case TypedEnumValue e -> new SqlExpr.StringLit(e.value());

            default -> scalarRelationalArms(spec, columns);
        };
    }

    /** Scalar lowering, arm group (sequential order preserved:
     * guarded patterns depend on it) — the 523-line dispatch split
     * at arm boundaries; each group defaults to the next. */
    private SqlExpr scalarRelationalArms(TypedSpec spec, ColumnResolver columns) {
        return switch (spec) {
            // Real pure equality is TYPE-aware: an enum value equals nothing
            // of a different enum or a non-string kind — a static FALSE
            // (never name-coincidence 'X'=='X' across enums, never a DB
            // conversion error for enum-vs-Integer). Enum-vs-STRING stays
            // the corpus's deliberate name-comparison convention.
            // (Class-instance eq is REFERENCE identity in real pure — NOT
            // recoverable here: value serialization erases identity, and
            // the PCT harness inlines captured instances BY VALUE, so
            // eq($x,$x) and eq($x,$y) arrive as identical text. Instances
            // keep struct comparison; the identity tests are ledgered.)
            case TypedNativeCall n when (isFamily(n, "equal") || isFamily(n, "eq"))
                    && enumTypeMismatch(n.args()) -> new SqlExpr.BoolLit(false);

            // COLLECTION-VALUED relation nodes in scalar position: the list
            // encodings ([1,2,3]->filter/slice/drop/take over a value, not a
            // table). Relation-typed sources take the relation() arms.
            case TypedFilter f when !(f.source().info().type() instanceof Type.RelationType) ->
                    SqlExpr.Call.of(SqlFn.LIST_FILTER,
                            scalar(f.source(), columns), scalar(f.predicate(), columns));
            // slice(start, stop): 0-based exclusive-stop -> 1-based inclusive
            // array_slice; a NEGATIVE start clamps to the list head (PCT).
            case TypedSlice s when !(s.source().info().type() instanceof Type.RelationType) -> {
                SqlExpr lo = clamp0(scalar(s.start(), columns));
                SqlExpr hi = clamp0(scalar(s.stop(), columns));
                SqlExpr sliced = SqlExpr.Call.of(SqlFn.LIST_SLICE,
                        scalar(s.source(), columns), onePlus(lo),
                        // STOP clamps too: DuckDB reads a negative bound
                        // FROM THE END (slice(l,0,-1) returned the whole
                        // list; pure says empty — audit).
                        hi);
                // inverted bounds RAISE real pure's message, in the database
                yield new SqlExpr.Case(List.of(new SqlExpr.Case.When(
                        SqlExpr.Call.of(SqlFn.GREATER, lo, hi),
                        SqlExpr.Call.of(SqlFn.ERROR,
                                SqlExpr.Call.of(SqlFn.CONCAT,
                                        SqlExpr.Call.of(SqlFn.CONCAT,
                                                SqlExpr.Call.of(SqlFn.CONCAT,
                                                        new SqlExpr.StringLit("The low bound ("),
                                                        new SqlExpr.Cast(lo, SqlType.Scalar.VARCHAR)),
                                                new SqlExpr.StringLit(") can't be higher than the high bound (")),
                                        SqlExpr.Call.of(SqlFn.CONCAT,
                                                new SqlExpr.Cast(hi, SqlType.Scalar.VARCHAR),
                                                new SqlExpr.StringLit(") in a slice operation")))))),
                        sliced);
            }
            // drop(n): the suffix from n+1; negative n drops nothing (PCT).
            case TypedDrop d when !(d.source().info().type() instanceof Type.RelationType) -> {
                SqlExpr src = scalar(d.source(), columns);
                yield SqlExpr.Call.of(SqlFn.LIST_SLICE, src,
                        onePlus(clamp0(scalar(d.count(), columns))),
                        SqlExpr.Call.of(SqlFn.LIST_LENGTH, src));
            }
            // take(n): the prefix; negative n takes nothing (PCT) — the clamp
            // matters because DuckDB reads a negative bound FROM THE END.
            case TypedLimit t when !(t.source().info().type() instanceof Type.RelationType) ->
                    SqlExpr.Call.of(SqlFn.LIST_SLICE,
                            scalar(t.source(), columns), new SqlExpr.IntLit(1),
                            clamp0(scalar(t.count(), columns)));
            // A let in EXPRESSION position (a callee shape the statement
            // folder didn't reach): bind and yield the value — the let IS
            // its value.
            case TypedLet l -> {
                SqlExpr v = scalar(l.value(), columns);
                letBindings.put(l.name(), v);
                yield v;
            }
            // makeString/joinStrings over TDS ROW CELLS (the Typer's
            // row-var $r.values synthesis: per-column reads off ONE row
            // variable): stringify each element HERE — the engine's TDSRow
            // print convention, 'TDSNull' for an empty cell — so the list
            // never takes the Any-collection JSON carrier (whose VARCHAR
            // cast quotes strings). Keyed to the CELLS SHAPE (audit 9): an
            // arbitrary user collection must not print 'TDSNull'.
            case TypedNativeCall n
                    when (isFamily(n, "makeString") || isFamily(n, "joinStrings"))
                    && !n.args().isEmpty()
                    && n.args().get(0)
                            instanceof TypedCollection tc
                    && isRowCells(tc) -> {
                List<SqlExpr> elems = new ArrayList<>(tc.elements().size());
                for (TypedSpec e : tc.elements()) {
                    elems.add(SqlExpr.Call.of(SqlFn.COALESCE,
                            new SqlExpr.Cast(scalar(e, columns),
                                    SqlType.Scalar.VARCHAR),
                            new SqlExpr.StringLit("TDSNull")));
                }
                List<SqlExpr> args = new ArrayList<>();
                args.add(new SqlExpr.ArrayLit(elems));
                for (int i = 1; i < n.args().size(); i++) {
                    args.add(scalar(n.args().get(i), columns));
                }
                yield Scalars.lower(n, args);
            }
            case TypedNativeCall n -> Scalars.lower(n,
                    n.args().stream().map(a -> scalar(a, columns)).toList());
            // write(rel, accessor) returns the COUNT of rows written (the
            // PCT contract). A TDS-relation accessor destination has no
            // physical table — the write is vacuous and only the count is
            // observable; a REAL store destination stays loud until the
            // insert path exists.
            case TypedWrite w -> {
                boolean accessor = w.destination().isEmpty()
                        || !containsStoreTable(w.destination().get());
                if (!accessor) {
                    throw new NotImplementedException(
                            "TypedWrite to a store destination is not yet implemented");
                }
                SqlSelect src = relation(w.source());
                SqlSelect count = SqlSelect.starOf(
                                new SqlSource.Subselect(src, nextAlias()))
                        .withProjections(List.of(new SqlSelect.Projection(
                                        new SqlAgg.Reducer("COUNT", List.of(), false), null)),
                                List.of(new OutputCol("count",
                                        SqlType.Scalar.BIGINT, false)));
                yield new SqlExpr.ScalarSubquery(count);
            }
            // A CLASS REFERENCE in scalar position carries its SIMPLE name
            // (PCT: STR_Person->toString() == 'STR_Person').
            case TypedPackageableRef ref -> {
                String fqn = ref.fullPath();
                int idx = fqn.lastIndexOf("::");
                yield new SqlExpr.StringLit(idx < 0 ? fqn : fqn.substring(idx + 2));
            }
            // from() in scalar position: execution-context metadata only —
            // the value is its source's
            case TypedFrom fr2 ->
                    scalar(fr2.source(), columns);
            // relation->map(row|scalar) consumed as a VALUE COLLECTION
            // (makeString/joinStrings tails): aggregate the projected
            // column to a LIST value via a scalar subquery
            case TypedMap m2
                    when m2.source().info().type() instanceof Type.RelationType
                    && m2.mapper() instanceof TypedLambda ml2
                    && !(ml2.info().type() instanceof Type.FunctionType ft2
                            && ft2.result().type() instanceof Type.RelationType) -> {
                Multiplicity colMult2 =
                        ml2.info().type() instanceof Type.FunctionType fnT2
                                ? fnT2.result().multiplicity()
                                : Multiplicity.Bounded.ZERO_ONE;
                SqlSelect proj = relation(new TypedProject(
                        m2.source(),
                        List.of(new TypedFuncCol("value", ml2)),
                        new ExprType(
                                new Type.RelationType(List.of(
                                        new Type.RelationType.Column("value",
                                                m2.info().type(), colMult2))),
                                Multiplicity.Bounded.ONE)));
                String sub = nextAlias();
                SqlSelect agg = SqlSelect.starOf(new SqlSource.Subselect(proj, sub))
                        .withProjections(List.of(new SqlSelect.Projection(
                                        new SqlAgg.Reducer("LIST", List.of(
                                                new SqlExpr.Column(sub, "value")), false),
                                        null)),
                                List.of(new OutputCol("value",
                                        SqlType.Scalar.VARCHAR, true)));
                SqlExpr listed = new SqlExpr.ScalarSubquery(agg);
                // pure map FLATTENS collection-valued mappers ($r.values):
                // the list-of-cell-arrays flattens one level
                boolean collMapper = isCollectionMapper(ml2);
                yield collMapper
                        ? SqlExpr.Call.of(SqlFn.LIST_FLATTEN, listed)
                        : listed;
            }
            // A COLUMN READ over a relation chain in scalar position
            // ($tds.rows.id — the TDS getter desugar): narrow to the one
            // column and take the single-column relation route below.
            case TypedPropertyAccess pa
                    when pa.source().info().type() instanceof Type.RelationType prt -> {
                Type.RelationType.Column c = prt.columns().stream()
                        .filter(x -> x.name().equals(pa.property())).findFirst()
                        .orElseThrow(() -> new com.legend.error
                                .NotImplementedException("relation has no column '"
                                        + pa.property() + "' in scalar read"));
                yield scalar(new TypedSelect(pa.source(),
                        List.of(pa.property()),
                        new ExprType(
                                new Type.RelationType(List.of(c)),
                                pa.info().multiplicity())), columns);
            }
            // A single-column RELATION consumed in SCALAR position. A
            // TO-ONE read is the correlated scalar subquery (value-position
            // filtered navigation): DuckDB raises on more than one row
            // (pure toOne semantics); an empty result is NULL ([0..1]). A
            // TO-MANY read is a VALUE COLLECTION (contains/in/makeString
            // consumers): aggregate the column to a LIST — the bare scalar
            // subquery would raise on the second row. The OUTER row
            // resolver rides the enclosing channel either way.
            case TypedSpec rel when rel.info().type()
                    instanceof Type.RelationType rt
                    && rt.columns().size() == 1 -> {
                enclosing.push((v, name) -> {
                    SqlExpr r = columns.resolve(v, name);
                    if (r == null) {
                        throw new UnfoldableRef(name);
                    }
                    return r;
                });
                try {
                    boolean toMany = !(rel.info().multiplicity()
                            instanceof com.legend.compiler.element.type
                                    .Multiplicity.Bounded mb1
                            && mb1.isToOne());
                    if (!toMany) {
                        yield new SqlExpr.ScalarSubquery(relation(rel));
                    }
                    String sub = nextAlias();
                    String col = rt.columns().get(0).name();
                    SqlSelect agg = SqlSelect.starOf(
                            new SqlSource.Subselect(relation(rel), sub))
                            .withProjections(List.of(new SqlSelect.Projection(
                                            new SqlAgg.Reducer("LIST", List.of(
                                                    new SqlExpr.Column(sub, col)),
                                                    false),
                                            null)),
                                    List.of(new OutputCol(col,
                                            SqlType.Scalar.VARCHAR,
                                            true)));
                    yield new SqlExpr.ScalarSubquery(agg);
                } finally {
                    enclosing.pop();
                }
            }
            // SANCTIONED frontier default — see relation() above.
            default -> throw new NotImplementedException("scalar lowering not yet implemented for "
                    + spec.getClass().getSimpleName());
        };
    }

    /** Clamp a (possibly negative) index to zero — PCT's slice/drop/take edge semantics. */
    private static SqlExpr clamp0(SqlExpr e) {
        return e instanceof SqlExpr.IntLit i
                ? new SqlExpr.IntLit(Math.max(0, i.value()))
                : SqlExpr.Call.of(SqlFn.GREATEST, e, new SqlExpr.IntLit(0));
    }

    /** 0-based → 1-based shift, constant-folded for literals. */
    private static SqlExpr onePlus(SqlExpr e) {
        return e instanceof SqlExpr.IntLit i
                ? new SqlExpr.IntLit(i.value() + 1)
                : SqlExpr.Call.of(SqlFn.PLUS, e, new SqlExpr.IntLit(1));
    }

    /** A value in LIST position: singleton-wrap unless it is already a list (or NULL = empty). */
    private static SqlExpr asList(SqlExpr e, boolean many) {
        return many || e instanceof SqlExpr.ArrayLit || e instanceof SqlExpr.NullLit
                ? e : new SqlExpr.ArrayLit(List.of(e));
    }

    /** Bare ->sort() over a SINGLE-COLUMN relation stream (assert
     * vocabulary: $result.rows.values->sort()) — a multi-column relation
     * has no natural order and stays at the loud frontier default. */
    private static boolean isBareSingleColumnSort(TypedNativeCall nc) {
        return "meta::pure::functions::collection::sort"
                        .equals(nc.callee().qualifiedName())
                && nc.args().size() == 1
                && nc.args().get(0).info().type()
                        instanceof Type.RelationType srt
                && srt.columns().size() == 1;
    }

    /** Natural ascending order on the stream's one column. */
    private SqlSelect naturalSort(TypedNativeCall nc) {
        Type.RelationType srt =
                (Type.RelationType) nc.args().get(0).info().type();
        return relation(nc.args().get(0)).withOrderBy(List.of(
                SqlSelect.SortKey.asc(new SqlExpr.Column(null,
                        srt.columns().get(0).name()))));
    }

    /** An instance literal in relation position: {@code ^X(…)} or a collection of them. */
    private static boolean isInstanceLiteral(TypedSpec source) {
        return source instanceof TypedNewInstance
                || (source instanceof TypedCollection c
                        && !c.elements().isEmpty()
                        && c.elements().stream().allMatch(e ->
                                e instanceof TypedNewInstance));
    }

    /**
     * Lower {@code <instances>->project(~[alias: x|$x.path…])}: one SELECT per
     * instance (UNION ALL for a collection). A colspec whose path crosses a
     * TO-MANY property becomes a {@code LEFT JOIN LATERAL
     * (SELECT unnest(<array literal>) AS elem)} off a one-row anchor — LEFT
     * so an empty array yields NULL, lateral chaining so independent arrays
     * CROSS-multiply (real pure's project semantics over instances).
     */
    private SqlSelect projectOverInstances(TypedProject p) {
        List<TypedNewInstance> instances =
                p.source() instanceof TypedCollection c
                        ? c.elements().stream()
                                .map(e -> (TypedNewInstance) e)
                                .toList()
                        : List.of((TypedNewInstance) p.source());
        List<OutputCol> outputs = outputsOf(p.info());
        List<SqlQuery> branches = new ArrayList<>(instances.size());
        for (var inst : instances) {
            branches.add(instanceSelect(inst, p.columns(), outputs));
        }
        if (branches.size() == 1) {
            return (SqlSelect) branches.get(0);
        }
        return SqlSelect.starOf(new SqlSource.Subselect(
                new SqlUnion(branches, true, outputs), nextAlias()));
    }

    private SqlSelect instanceSelect(TypedNewInstance inst,
                                     List<TypedFuncCol> columns, List<OutputCol> outputs) {
        SqlSource src = null;
        // ONE unnest per to-many PATH PREFIX (the NavPath-registry rule):
        // two colspecs over $x.addresses iterate the SAME collection — real
        // pure yields (city, zip) pairs, never their cross product. Only
        // INDEPENDENT collections cross-multiply.
        Map<String, String> unnestByPrefix = new LinkedHashMap<>();
        List<SqlSelect.Projection> ps = new ArrayList<>(columns.size());
        for (TypedFuncCol col : columns) {
            List<String> path = pathOf(col);
            if (path == null) {
                // COMPUTED column ($v.a + $v.b, coalesce($x.f, ...)): the row
                // param's property accesses resolve to the instance's literal
                // values; the body lowers as an ordinary scalar. A MANY-valued
                // body has no relational cell shape (bare to-many paths
                // explode via unnest; a computed one must be loud, never a
                // silent list-in-a-cell).
                TypedSpec bodyLast = last(col.fn());
                if (bodyLast.info().multiplicity().isMany()) {
                    throw new NotImplementedException(
                            "instance-literal project: computed column '" + col.name()
                                    + "' is collection-valued — only bare to-many"
                                    + " property paths explode");
                }
                String param = col.fn().parameters().get(0);
                SqlExpr computed = scalar(last(col.fn()), (v, name) -> {
                    if (!param.equals(v)) {
                        throw new IllegalStateException("instance-literal project:"
                                + " unresolved variable $" + v);
                    }
                    TypedSpec pv = inst.properties().get(name);
                    return pv == null ? new SqlExpr.NullLit() : scalar(pv, noScope());
                });
                ps.add(new SqlSelect.Projection(computed, col.name()));
                continue;
            }
            // Walk the path over the literal: to-one instance hops recurse;
            // the first TO-MANY value becomes the unnest source.
            TypedSpec cur = inst;
            SqlExpr value = null;
            for (int i = 0; i < path.size(); i++) {
                if (!(cur instanceof TypedNewInstance ni)) {
                    throw new NotImplementedException(
                            "instance-literal project: '" + col.name()
                                    + "' navigates through a non-instance value");
                }
                TypedSpec v = ni.properties().get(path.get(i));
                if (v == null) {
                    value = new SqlExpr.NullLit();   // unset property: NULL column
                    break;
                }
                if (v instanceof TypedCollection many) {
                    // TO-MANY: explode via lateral unnest (shared per path
                    // prefix); the residual path reads fields off the element.
                    String prefix = String.join(".", path.subList(0, i + 1));
                    String alias = unnestByPrefix.get(prefix);
                    if (alias == null) {
                        alias = nextAlias();
                        unnestByPrefix.put(prefix, alias);
                        SqlExpr array = many.elements().isEmpty()
                                ? new SqlExpr.NullLit()
                                : new SqlExpr.ArrayLit(many.elements().stream()
                                        .map(e -> scalar(e, noScope())).toList());
                        SqlSelect unnest = new SqlSelect(
                                List.of(new SqlSelect.Projection(
                                        SqlExpr.Call.of(SqlFn.UNNEST, array),
                                        "elem")),
                                false, null, null, List.of(), null, null, List.of(), null, null,
                                List.of(new OutputCol("elem",
                                        SqlType.Scalar.VARCHAR, true)));
                        SqlSource right = new SqlSource.Subselect(unnest, alias);
                        src = src == null
                                ? anchorJoin(right)
                                : new SqlSource.Join(src, right,
                                        SqlSource.Join.Kind.LEFT_LATERAL,
                                        new SqlExpr.BoolLit(true));
                    }
                    value = new SqlExpr.Column(alias, "elem");
                    for (int r = i + 1; r < path.size(); r++) {
                        value = new SqlExpr.StructGet(value, path.get(r));
                    }
                    break;
                }
                if (i == path.size() - 1) {
                    value = scalar(v, noScope());
                } else {
                    cur = v;
                }
            }
            ps.add(new SqlSelect.Projection(Objects.requireNonNull(value), col.name()));
        }
        return new SqlSelect(ps, false, src, null, List.of(), null, null,
                List.of(), null, null, outputs);
    }

    /** The 1-row anchor a lateral chain hangs off (an empty array must NULL, not kill, the row). */
    private SqlSource anchorJoin(SqlSource right) {
        SqlSource anchor = new SqlSource.Values(
                List.of(List.of(new SqlExpr.IntLit(1))), List.of("_anchor"), nextAlias(),
                List.of(new OutputCol("_anchor", SqlType.Scalar.BIGINT, false)));
        return new SqlSource.Join(anchor, right,
                SqlSource.Join.Kind.LEFT_LATERAL, new SqlExpr.BoolLit(true));
    }

    /** A colspec body as a bare property path rooted at the lambda parameter; null = computed. */
    private static List<String> pathOf(TypedFuncCol col) {
        String param = col.fn().parameters().get(0);
        ArrayDeque<String> path = new ArrayDeque<>();
        TypedSpec cur = col.fn().body().get(col.fn().body().size() - 1);
        while (cur instanceof TypedPropertyAccess pa) {
            path.addFirst(pa.property());
            cur = pa.source();
        }
        if (!(cur instanceof TypedVariable v) || !v.name().equals(param) || path.isEmpty()) {
            return null;   // COMPUTED body — the caller lowers it as a scalar
        }
        return List.copyOf(path);
    }

    /**
     * The variant→class cast at the base of a property-access chain
     * ({@code to(@C).a.b} — every {@code source()} hop a property access),
     * or null when the chain roots elsewhere.
     */
    private static TypedCast variantCastBase(TypedSpec spec) {
        TypedSpec cur = spec;
        while (cur instanceof TypedPropertyAccess pa) {
            cur = pa.source();
        }
        if (cur instanceof TypedCast vc
                && vc.source().info().type() instanceof Type.ClassType vsrc
                && PlatformTypes.isVariant(vsrc)
                && vc.target() instanceof Type.ClassType vtgt
                && !PlatformTypes.isVariant(vtgt)) {
            return vc;
        }
        return null;
    }

    /** Rebuild a pair struct with fields COERCED to the LUB's slots (Any -> variant). */
    private static SqlExpr pairToLub(SqlExpr pair, Type own, Type.GenericType lub) {
        String[] names = {"first", "second"};
        List<SqlExpr.StructLit.Field> fields = new ArrayList<>(2);
        for (int i = 0; i < 2; i++) {
            SqlExpr f = new SqlExpr.StructGet(pair, names[i]);
            Type lubArg = lub.arguments().get(i);
            Type ownArg = own instanceof Type.GenericType og && og.arguments().size() == 2
                    ? og.arguments().get(i) : null;
            if (lubArg instanceof Type.ClassType lc
                    && PlatformTypes.isAny(lc)
                    && (ownArg == null || !(ownArg instanceof Type.ClassType oc
                            && PlatformTypes.isAny(oc)))) {
                f = SqlExpr.Call.of(SqlFn.TO_VARIANT, f);
            }
            fields.add(new SqlExpr.StructLit.Field(names[i], f));
        }
        return new SqlExpr.StructLit(fields);
    }

    /** A resolver for positions where no row scope exists (literal evaluation). */
    private ColumnResolver noScope() {
        return (var, name) -> {
            throw new IllegalStateException(
                    "an instance literal has no row scope for $" + var
                            + (name == null ? "" : "." + name));
        };
    }

    /**
     * pivot(~col, ~agg:...): DuckDB native PIVOT source. Single pivot column
     * (multi-column key synthesis is a later slice); aggregates via the same
     * reduce-overload dispatch as groupBy.
     */
    private SqlSelect pivot(TypedPivot pv) {
        SqlSelect src = relation(pv.source());
        SqlSource inner = asRightSide(src);
        List<SqlExpr> on;
        if (pv.pivotColumns().size() == 1) {
            on = List.of(Fold.sourceColumn(inner, pv.pivotColumns().get(0)));
        } else {
            // MULTI-column pivot: synthesize the COMPOSITE KEY — the pivot
            // columns concatenated with the '__|__' separator (the same
            // separator the dynamic-column templates carry), the originals
            // EXCLUDE'd — then pivot the single synthetic key.
            String keyName = nextAlias();
            SqlExpr key = null;
            for (String c : pv.pivotColumns()) {
                SqlExpr col = new SqlExpr.Cast(Fold.sourceColumn(inner, c),
                        SqlType.Scalar.VARCHAR);
                key = key == null ? col
                        : SqlExpr.Call.of(SqlFn.CONCAT,
                                SqlExpr.Call.of(SqlFn.CONCAT, key,
                                        new SqlExpr.StringLit(com.legend.compiler.element.type
                                                .Type.RelationType.PIVOT_SEPARATOR)),
                                col);
            }
            List<OutputCol> keyedOutputs = new ArrayList<>();
            for (OutputCol oc : inner.outputs()) {
                if (!pv.pivotColumns().contains(oc.name())) {
                    keyedOutputs.add(oc);
                }
            }
            keyedOutputs.add(new OutputCol(keyName,
                    SqlType.Scalar.VARCHAR, false));
            SqlSelect keyed = SqlSelect.starOf(inner).withProjections(
                    List.of(new SqlSelect.Projection(
                                    new SqlExpr.StarExcept(inner.alias(), pv.pivotColumns()), null),
                            new SqlSelect.Projection(key, keyName)),
                    keyedOutputs);
            inner = new SqlSource.Subselect(keyed, nextAlias());
            on = List.of(Fold.sourceColumn(inner, keyName));
        }
        List<SqlSource.Pivot.Using> usings = new ArrayList<>();
        SqlSelect forAgg = SqlSelect.starOf(inner);
        for (TypedAggCol a : pv.aggs()) {
            usings.add(new SqlSource.Pivot.Using(aggExpr(forAgg, a), a.name()));
        }
        // Static pivot values pin the output columns via PIVOT ... IN (v…).
        List<SqlExpr> in = pv.values().stream()
                .map(v -> scalar(v, (var, name) -> {
                    throw new IllegalStateException(
                            "pivot values must be literal, referenced column: " + name);
                }))
                .toList();
        // Fully-qualified refs; a dialect whose PIVOT forbids qualifiers in
        // USING (DuckDB) strips them AT RENDER TIME.
        return SqlSelect.starOf(new SqlSource.Pivot(inner, on, in, usings, nextAlias(),
                outputsOf(pv.info())));
    }

    /**
     * flatten(~col): the column explodes via UNNEST in the select list —
     * schema-driven explicit projections (every other column plain, the
     * flattened one replaced). Downstream refs to the flattened column are
     * COMPUTED projections, so the fold policy isolates them naturally.
     * A Variant column casts to JSON[] first; a typed list unnests directly.
     */
    private SqlSelect flatten(TypedFlatten fl) {
        SqlSelect src = relation(fl.source());
        SqlSelect base = Fold.extendFolds(src) ? src : isolate(src);
        return foldOrIsolate(base, "flatten", b -> buildFlatten(b, fl));
    }

    private SqlSelect buildFlatten(SqlSelect base,
            TypedFlatten fl) {
        Type.RelationType schema = schemaOf(fl.source());
        List<SqlSelect.Projection> ps = new ArrayList<>();
        for (Type.Column c : schema.columns()) {
            SqlExpr col = resolveOrThrow(base, c.name());
            if (c.name().equals(fl.column())) {
                SqlExpr list = c.type() instanceof Type.ClassType
                        ? new SqlExpr.Call(SqlFn.VARIANT_ELEMENTS, List.of(col))
                        : col;
                ps.add(new SqlSelect.Projection(
                        new SqlExpr.Call(SqlFn.UNNEST, List.of(list)), c.name()));
            } else {
                ps.add(new SqlSelect.Projection(col, null));
            }
        }
        return base.withProjections(ps, outputsOf(fl.info()));
    }

    /**
     * to(@T) / toMany(@T) / cast(@T) in scalar position (all arrive as
     * {@code TypedCast}; multiplicity separates them):
     * <ul>
     *   <li>Variant source, scalar target: master's rule — a {@code ->} access
     *       becomes {@code ->>} (text extraction strips JSON quoting) before
     *       {@code CAST(... AS T)}.</li>
     *   <li>Variant source, many target ({@code toMany}): {@code CAST} to an
     *       array of the target; {@code @Variant} keeps JSON elements.</li>
     *   <li>Non-variant source: multiplicity/type erasure — identity.</li>
     * </ul>
     */
    private SqlExpr cast(TypedCast c,
                         ColumnResolver columns) {
        return cast(c, scalar(c.source(), columns));
    }

    /** The cast policy over an ALREADY-LOWERED source (scalar or window channel). */
    private SqlExpr cast(TypedCast c, SqlExpr value) {
        boolean variantSource = c.source().info().type()
                instanceof Type.ClassType ct && PlatformTypes.isVariant(ct);
        if (!variantSource) {
            // A CONVERTING primitive cast (String->@Integer) must reach SQL —
            // returning the value bare left VARCHAR in arithmetic (audit:
            // the cast bucket). A WIDENING cast (Integer->@Number: the target
            // is the source's lattice supertype) is a type ASSERTION — the
            // value is already one; converting would corrupt it (42 -> 42.0).
            // DELIBERATE corpus divergence from real pure: pure's cast never
            // converts (a non-integral Number->@Integer is a runtime error
            // there); the corpus contract (engine-lite lineage) is SQL-style
            // conversion, so a narrowing cast converts here.
            Type src = c.source().info().type();
            if (isSqlPrimitive(c.target()) && isSqlPrimitive(src)
                    && !isWidening(src, c.target())
                    && !PureSql.type(src).equals(PureSql.type(c.target()))) {
                // A converting cast over a COLLECTION is ELEMENT-WISE — the
                // scalar channel carries collections as LISTs and DuckDB has
                // no LIST->scalar cast (calendar DateRange family: row-var
                // .values is an ArrayLit even at bounded-1 multiplicity).
                if (value instanceof SqlExpr.ArrayLit lit) {
                    return new SqlExpr.ArrayLit(lit.elements().stream()
                            .map(e -> (SqlExpr) new SqlExpr.Cast(
                                    e, PureSql.type(c.target())))
                            .toList());
                }
                return isMany(c)
                        ? SqlExpr.Call.of(SqlFn.LIST_TRANSFORM, value,
                                new SqlExpr.Lambda(List.of("x"),
                                        new SqlExpr.Cast(
                                                new SqlExpr.Column(null, "x"),
                                                PureSql.type(c.target()))))
                        : new SqlExpr.Cast(value, PureSql.type(c.target()));
            }
            return value;
        }
        boolean many = isMany(c);
        if (many) {
            boolean variantTarget = c.target() instanceof Type.ClassType t
                    && PlatformTypes.isVariant(t);
            return variantTarget
                    ? SqlExpr.Call.of(SqlFn.VARIANT_ELEMENTS, value)
                    // A to-many cast targets an ARRAY of the element type —
                    // expressed in the TYPE (SqlType.Array). JSON null stays
                    // SQL NULL (real relation-land pins toVariant(NULL) =
                    // 'null' vs toVariant([]) = '[]'); the list CONSUMERS
                    // (contains/isEmpty/joinStrings) are null-safe instead.
                    : new SqlExpr.Cast(value,
                            new SqlType.Array(PureSql.type(c.target())));
        }
        if (c.target() instanceof Type.ClassType tc
                && !PlatformTypes.isVariant(tc)
                && !PlatformTypes.isAny(tc)) {
            // to(@ModelClass) MATERIALIZED as a value: the real relation
            // runtime rejects class-typed columns — message verbatim
            // (property reads through the cast never come here; the
            // extraction arm in scalar() fields them).
            throw new ModelException(
                    LegendCompileException.Phase.LOWER,
                    "The type " + tc.fqn() + " is not supported yet!");
        }
        // The dialect may render this cast through its text-extraction idiom
        // (DuckDB ->>) — that is RENDERING knowledge; the IR keeps the access.
        return new SqlExpr.Cast(value, PureSql.type(c.target()));
    }

    /**
     * The one cast policy, applied to an already-lowered expression: a
     * CONVERTING primitive cast emits SQL; widening/same-type/non-primitive
     * is the assertion no-op.
     */
    private static SqlExpr castByPolicy(SqlExpr e, Type src, Type target) {
        if (isSqlPrimitive(target) && isSqlPrimitive(src)
                && !isWidening(src, target)
                && !PureSql.type(src).equals(PureSql.type(target))) {
            return new SqlExpr.Cast(e, PureSql.type(target));
        }
        return e;
    }

    /** Whether {@code tgt} is {@code src}'s primitive-lattice supertype (cast-as-assertion). */
    private static boolean isWidening(Type src, Type tgt) {
        if (tgt == Type.Primitive.NUMBER) {
            return src == Type.Primitive.INTEGER || src == Type.Primitive.FLOAT
                    || src == Type.Primitive.DECIMAL || src instanceof Type.PrecisionDecimal;
        }
        if (tgt == Type.Primitive.DATE) {
            return src == Type.Primitive.STRICT_DATE || src == Type.Primitive.DATE_TIME;
        }
        return false;
    }

    /** A Pure type with a direct scalar SQL carrier (primitives and sized decimals). */
    private static boolean isSqlPrimitive(Type t) {
        return (t instanceof Type.Primitive p
                        && p != Type.Primitive.BYTE && p != Type.Primitive.LATEST_DATE
                        && p != Type.Primitive.STRICT_TIME)
                || t instanceof Type.PrecisionDecimal;
    }

    /**
     * A relation cast whose target columns ALL exist in the source projects
     * them (SQL-CAST where the type changed); any absent name means the
     * pivot idiom's dynamic columns — type-only pass-through.
     */
    private SqlSelect relationCast(TypedCast c,
                                   Type.RelationType srcRow, Type.RelationType tgtRow) {
        Map<String, Type.Column> src = new LinkedHashMap<>();
        for (Type.Column col : srcRow.columns()) {
            src.put(col.name(), col);
        }
        boolean allKnown = tgtRow.columns().stream()
                .allMatch(tc -> src.containsKey(tc.name()));
        SqlSelect base = relation(c.source());
        if (!allKnown) {
            return base;   // dynamic (pivot) columns: re-type only
        }
        // Identity is POSITIONAL: a cast that merely REORDERS columns must
        // project (the executor matches result columns to the schema by
        // position — returning the source order would silently mislabel
        // cells; audit).
        boolean identity = tgtRow.columns().size() == srcRow.columns().size();
        for (int i = 0; identity && i < tgtRow.columns().size(); i++) {
            Type.Column tc = tgtRow.columns().get(i);
            Type.Column sc = srcRow.columns().get(i);
            identity = tc.name().equals(sc.name()) && tc.type().equals(sc.type());
        }
        if (identity) {
            return base;
        }
        SqlSelect first = Fold.projectionFolds(base) ? base : isolate(base);
        SqlSelect out = tryRelationCast(first, src, tgtRow, c);
        if (out == null) {
            out = tryRelationCast(isolate(first), src, tgtRow, c);
        }
        if (out == null) {
            throw new IllegalStateException("relation cast columns unresolvable"
                    + " even after isolation: " + tgtRow.typeName());
        }
        return out;
    }

    /** One pass; null when any target column would not fold against {@code base}. */
    private SqlSelect tryRelationCast(SqlSelect base, Map<String, Type.Column> src,
                                      Type.RelationType tgtRow,
                                      TypedCast c) {
        List<SqlSelect.Projection> ps = new ArrayList<>(tgtRow.columns().size());
        for (Type.Column tc : tgtRow.columns()) {
            switch (attempt(() -> resolveOrThrow(base, tc.name()))) {
                case Resolution.Resolved r -> {
                    Type from = src.get(tc.name()).type();
                    SqlExpr v = from.equals(tc.type())
                            || !isSqlPrimitive(tc.type()) || !isSqlPrimitive(from)
                            ? r.expr()
                            : new SqlExpr.Cast(r.expr(), PureSql.type(tc.type()));
                    ps.add(new SqlSelect.Projection(v, tc.name()));
                }
                case Resolution.Unfoldable u -> {
                    return null;
                }
            }
        }
        return base.withProjections(ps, outputsOf(c.info()));
    }

    private static ColumnResolver lambdaResolver(
            List<String> params, ColumnResolver outer) {
        return (var, prop) -> params.contains(var)
                ? (prop == null ? new SqlExpr.Column(null, var) : new SqlExpr.Column(var, prop))
                : outer.resolve(var, prop);
    }

    private static boolean isMany(TypedSpec spec) {
        return spec.info().multiplicity().requireBounded("lowering").isMany();
    }

    /**
     * fold: emitted in PURE conventions — {@link SqlExpr.FoldCall} with the
     * {@code (element, accumulator)} lambda exactly as written. The Phase-G
     * strategy collapses to logical facts (Concatenation is a list concat;
     * MapReduce pre-transforms; {@code accIsList} rides for the dialect's
     * encoding decisions). NOTHING here knows how any backend folds.
     */
    private SqlExpr fold(TypedFold f,
                         ColumnResolver columns) {
        SqlExpr source = scalar(f.source(), columns);
        SqlExpr init = scalar(f.init(), columns);
        List<String> ps = f.reducer().parameters();
        return switch (f.strategy()) {
            // A TO-ONE side (1->fold(add, …), scalar init) concatenates as a
            // singleton list. Already-list-shaped values ([9] lowers to an
            // ArrayLit despite mult [1]) and NULL (the []-born empty, which
            // DuckDB list_concat treats as []) pass through.
            case FoldStrategy.Concatenation c ->
                    new SqlExpr.Call(SqlFn.LIST_CONCAT,
                            List.of(asList(init, isMany(f.init())),
                                    asList(source, isMany(f.source()))));
            case FoldStrategy.SameType st ->
                    new SqlExpr.FoldCall(source,
                            new SqlExpr.Lambda(ps,
                                    scalar(last(f.reducer()), lambdaResolver(ps, columns))),
                            init, isMany(f.init()), true);
            case FoldStrategy.MapReduce mr -> {
                String elem = ps.get(0);
                SqlExpr.Lambda transform = new SqlExpr.Lambda(List.of(elem),
                        scalar(mr.transform(), lambdaResolver(List.of(elem), columns)));
                SqlExpr transformed = new SqlExpr.Call(
                        SqlFn.LIST_TRANSFORM, List.of(source, transform));
                // The transform makes source elements accumulator-typed.
                yield new SqlExpr.FoldCall(transformed,
                        new SqlExpr.Lambda(List.of(mr.freshParam(), mr.accParam()),
                                scalar(mr.reducer(), lambdaResolver(
                                        List.of(mr.accParam(), mr.freshParam()), columns))),
                        init, isMany(f.init()), true);
            }
            case FoldStrategy.CollectionBuild cb ->
                    new SqlExpr.FoldCall(source,
                            new SqlExpr.Lambda(ps,
                                    scalar(last(f.reducer()), lambdaResolver(ps, columns))),
                            init, isMany(f.init()), false);
        };
    }

    // ==================================================================
    // Relation-level predicate family (EXISTS forms)
    // ==================================================================

    private interface RelationPredicate {
        SqlExpr lower(Lowerer lowerer, TypedNativeCall call);
    }

    /**
     * Enum equality that cannot mean NAME comparison: two DIFFERENT enums,
     * or an enum against a non-string type. Enum values lower as name
     * strings (plangen parity), so enum-vs-STRING equality IS the corpus's
     * deliberate name-comparison convention and stays allowed; the blocked
     * shapes would be silently wrong ('X' == OtherEnum.X) or DB type
     * errors (enum vs Integer).
     */
    private static boolean enumTypeMismatch(List<TypedSpec> args) {
        if (args.size() != 2) {
            return false;
        }
        var a = args.get(0).info().type();
        var b = args.get(1).info().type();
        boolean ae = a instanceof Type.EnumType;
        boolean be = b instanceof Type.EnumType;
        if (ae && be) {
            return !((Type.EnumType) a).fqn().equals(((Type.EnumType) b).fqn());
        }
        if (ae != be) {
            var other = ae ? b : a;
            // Any/Nil/Variant are UNDECIDED, not disjoint — an Any-typed
            // operand may hold this very enum at run time ([E.X, 1]->first()
            // == E.X is true in real pure); a static FALSE would be silently
            // wrong. Those fall through to the SQL name comparison.
            if (PlatformTypes.isAny(other)
                    || PlatformTypes.isNil(other)
                    || PlatformTypes.isVariant(other)) {
                return false;
            }
            return !(other instanceof Type.Primitive prim
                    && prim == Type.Primitive.STRING);
        }
        return false;
    }

    private static boolean isFamily(TypedNativeCall n, String pureName) {
        // signatureKey membership — the LAST parser-node dispatch the re-audit
        // found dodging the parser-free wall (ArchUnit cannot see a dependency
        // reached through definition()'s return type + contains(Object)).
        return Pure.nativeNamed(pureName, n.callee().signatureKey());
    }

    private static RelationPredicate relationPredicate(TypedNativeCall n) {
        if (isFamily(n, "size")) {
            // NOTE (audit 22b F1 residual): size over a RELATION value
            // counts ROWS regardless of the value's [1] multiplicity (one
            // relation != one row — a value-mult constant fold here broke
            // three correlated-count pins). rows->toOne()->size() therefore
            // still answers the row count when toOne sits mid-expression;
            // the exactly-one contract is reader-enforced at toOne ROOTS
            // only. Documented residual, not silently folded.
            // COUNT(*) is a zero-key aggregation: a grouped/deduped/truncated
            // source must count from OUTSIDE (COUNT(*) per group is a row per
            // group — a multi-row scalar subquery).
            return (lw, call) -> {
                SqlSelect src = lw.relation(call.args().get(0));
                SqlSelect base = Fold.groupByFolds(src) && !Fold.unnestInProjections(src)
                        ? src : lw.isolate(src);
                return new SqlExpr.ScalarSubquery(base
                        .withProjections(List.of(new SqlSelect.Projection(
                                SqlAgg.Reducer.of("COUNT"), null)), List.of()));
            };
        }
        if (isFamily(n, "exists")) {
            return (lw, call) -> new SqlExpr.Exists(select1(
                    lw.whereLambda(call.args().get(0), call.args().get(1), false)));
        }
        if (isFamily(n, "forAll")) {
            return (lw, call) -> SqlExpr.Call.of(SqlFn.NOT, new SqlExpr.Exists(select1(
                    lw.whereLambda(call.args().get(0), call.args().get(1), true))));
        }
        if (isFamily(n, "isEmpty")) {
            return (lw, call) -> SqlExpr.Call.of(SqlFn.NOT,
                    new SqlExpr.Exists(select1(lw.relation(call.args().get(0)))));
        }
        if (isFamily(n, "isNotEmpty")) {
            return (lw, call) -> new SqlExpr.Exists(select1(lw.relation(call.args().get(0))));
        }
        return null;
    }

    /**
     * An EXISTS subquery projects the constant {@code 1} — its columns are
     * never read, and {@code SELECT 1} is the reference engines' lean shape
     * ({@code buildExistsPredicate}).
     */
    private static SqlSelect select1(SqlSelect s) {
        return s.withProjections(List.of(new SqlSelect.Projection(
                new SqlExpr.IntLit(1), null)), List.of());
    }

    /** Lower {@code rel} and fold {@code pred} (negated for forAll) into its WHERE. */
    private SqlSelect whereLambda(TypedSpec rel, TypedSpec predArg, boolean negate) {
        if (!(predArg instanceof TypedLambda lambda)) {
            throw new IllegalStateException("relation exists/forAll expects a predicate lambda");
        }
        SqlSelect sub = relation(rel);
        SqlExpr pred;
        if (tryPredicate(sub, lambda) instanceof Resolution.Resolved r) {
            pred = r.expr();
        } else {
            sub = isolate(sub);
            pred = predicateOrThrow(sub, lambda, "exists/forAll");
        }
        if (negate) {
            pred = SqlExpr.Call.of(SqlFn.NOT, pred);
        }
        return sub.withWhere(sub.where() == null ? pred
                : SqlExpr.Call.of(SqlFn.AND, sub.where(), pred));
    }

    // ==================================================================
    // Plumbing
    // ==================================================================

    /** Close the current select into a subselect and open a fresh star select over it. */
    /** An if-branch is a 0-param SINGLE-expression thunk; its body is the value. */
    private static TypedSpec thunkBody(
            TypedSpec branch) {
        if (branch instanceof TypedLambda l) {
            if (l.body().size() != 1) {
                throw new IllegalStateException("if-branch thunk has "
                        + l.body().size() + " statements; a last-statement pick"
                        + " would silently drop the rest");
            }
            return l.body().get(0);
        }
        return branch;
    }

    private SqlSelect isolate(SqlSelect s) {
        return SqlSelect.starOf(new SqlSource.Subselect(s, nextAlias()));
    }

    private static TypedSpec last(TypedLambda lambda) {
        return lambda.body().get(lambda.body().size() - 1);
    }

    private static long intOf(TypedSpec spec) {
        if (spec instanceof TypedCInteger c) {
            return c.value().longValue();
        }
        throw new NotImplementedException(
                "dynamic slicing bounds are not lowered yet (literal expected), got "
                        + spec.getClass().getSimpleName());
    }

    private static Type.RelationType schemaOf(TypedSpec spec) {
        return (Type.RelationType) spec.info().type();
    }

    private List<OutputCol> outputsOf(ExprType info) {
        if (!(info.type() instanceof Type.RelationType rt)) {
            return List.of();
        }
        // THE Pure→SQL type boundary: plans carry SQL types only. A
        // ROW-STRUCT column (a user navigate's slot) is typed nesting over a
        // FLAT physical reality — its outputs are the prefixed columns the
        // join actually emitted (alias_COL, all nullable: LEFT semantics).
        List<OutputCol> out = new ArrayList<>(rt.columns().size());
        for (Type.Column c : rt.columns()) {
            if (c.type() instanceof Type.RelationType sub) {
                for (Type.Column sc : sub.columns()) {
                    out.add(new OutputCol(c.name() + "_" + sc.name(),
                            PureSql.type(sc.type()), true));
                }
                continue;
            }
            out.add(new OutputCol(c.name(), sqlTypeOf(c.type()),
                    PureSql.nullable(c.multiplicity())));
        }
        return out;
    }
}
