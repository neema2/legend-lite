package com.legend.lowering;

import com.legend.compiler.element.type.Type;
import com.legend.compiler.spec.typed.TypedCBoolean;
import com.legend.compiler.spec.typed.TypedCFloat;
import com.legend.compiler.spec.typed.TypedCInteger;
import com.legend.compiler.spec.typed.TypedCString;
import com.legend.compiler.spec.typed.TypedAggCol;
import com.legend.compiler.spec.typed.TypedAggregate;
import com.legend.compiler.spec.typed.TypedCollection;
import com.legend.compiler.spec.typed.TypedConcatenate;
import com.legend.compiler.spec.typed.TypedDistinct;
import com.legend.compiler.spec.typed.TypedExtend;
import com.legend.compiler.spec.typed.TypedExtendAgg;
import com.legend.compiler.spec.typed.TypedExtendWindow;
import com.legend.compiler.spec.typed.TypedFuncCol;
import com.legend.compiler.spec.typed.TypedGroupBy;
import com.legend.compiler.spec.typed.TypedDrop;
import com.legend.compiler.spec.typed.TypedFilter;
import com.legend.compiler.spec.typed.TypedLambda;
import com.legend.compiler.spec.typed.TypedLimit;
import com.legend.compiler.spec.typed.TypedNativeCall;
import com.legend.compiler.spec.typed.TypedOver;
import com.legend.compiler.spec.typed.TypedProject;
import com.legend.compiler.spec.typed.TypedPropertyAccess;
import com.legend.compiler.spec.typed.TypedRename;
import com.legend.compiler.spec.typed.TypedSelect;
import com.legend.compiler.spec.typed.TypedSlice;
import com.legend.compiler.spec.typed.TypedSort;
import com.legend.compiler.spec.typed.TypedSpec;
import com.legend.compiler.spec.typed.TypedTableReference;
import com.legend.compiler.spec.typed.TypedTds;
import com.legend.compiler.spec.typed.TypedVariable;
import com.legend.sql.OutputCol;
import com.legend.sql.SqlAgg;
import com.legend.sql.SqlExpr;
import com.legend.sql.SqlFn;
import com.legend.sql.SqlQuery;
import com.legend.sql.SqlSelect;
import com.legend.sql.SqlSource;
import com.legend.sql.SqlUnion;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

/**
 * Phase I &mdash; relation-pipeline lowering (M2 scope: table/TDS sources +
 * filter/select/rename/sort/slicing/distinct; mappings and class sources are
 * Phase H). One exhaustive dispatch per {@code TypedSpec} kind; every fold
 * decision is {@link Fold}'s; scalar natives are {@link Scalars}'.
 */
public final class Lowerer {

    private int aliasCounter = 0;

    /**
     * The CANONICAL class-value layout resolver (ClassLayouts, supplied by the
     * driver): declared stored properties in declaration order — a struct's
     * fields come from the MODEL, never from an instance's value set. Empty
     * when no model rides along (unit tests over pure-relational queries);
     * class values then keep hitting the loud walls.
     */
    private final java.util.function.Function<Type,
            java.util.Optional<java.util.List<Type.Column>>> classLayout;

    /** Whether a class FQN exists in the driving model (layoutless-LUB detection). */
    private final java.util.function.Predicate<String> classExists;

    public Lowerer() {
        this(t -> java.util.Optional.empty(), f -> false);
    }

    public Lowerer(java.util.function.Function<Type,
            java.util.Optional<java.util.List<Type.Column>>> classLayout,
                   java.util.function.Predicate<String> classExists) {
        this.classLayout = classLayout;
        this.classExists = classExists;
    }

    /** SQL type of a value, seeing through class layouts (structs) before {@link PureSql}. */
    private com.legend.sql.SqlType sqlTypeOf(Type t) {
        return classLayout.apply(t)
                .<com.legend.sql.SqlType>map(cols -> new com.legend.sql.SqlType.Struct(
                        cols.stream().map(c -> {
                            com.legend.sql.SqlType ft = sqlTypeOf(c.type());
                            boolean many = c.multiplicity() instanceof
                                    com.legend.compiler.element.type.Multiplicity.Bounded b && b.isMany();
                            return new com.legend.sql.SqlType.Struct.Field(c.name(),
                                    many ? new com.legend.sql.SqlType.Array(ft) : ft);
                        }).toList()))
                .orElseGet(() -> {
                    // A MODEL class with no layoutable properties reaching a
                    // VALUE boundary is a heterogeneous LUB (mixed instance
                    // kinds meeting at an abstract ancestor) — it travels as
                    // variant JSON, like Any. Non-model classes keep
                    // PureSql's loud wall.
                    if (t instanceof Type.ClassType ct
                            && !com.legend.compiler.element.type.PlatformTypes.isVariant(ct)
                            && !com.legend.compiler.element.type.PlatformTypes.isNil(ct)
                            && !com.legend.compiler.element.type.PlatformTypes.isAny(ct)
                            && classExists.test(ct.fqn())) {
                        return com.legend.sql.SqlType.Scalar.JSON;
                    }
                    return PureSql.type(t);
                });
    }

    /**
     * Enclosing lambda scopes for CORRELATED nesting: when a relation query is
     * lowered INSIDE a lambda (a correlated subquery), the outer lambda's
     * resolver is pushed here so the inner predicate can reference outer rows.
     */
    private final java.util.ArrayDeque<ColumnResolver>
            enclosing = new java.util.ArrayDeque<>();

    /** Query-level let bindings ({@code |let a = ...; ...$a...}), lowered once. */
    private final java.util.Map<String, SqlExpr> letBindings = new java.util.HashMap<>();

    /**
     * Lower a typed QUERY BODY: leading {@code let} statements bind their
     * lowered values into query scope (substitution — the lean output has no
     * trace of the lets); the final statement is the query.
     */
    public SqlQuery lower(List<com.legend.compiler.spec.typed.TypedSpec> body) {
        for (int i = 0; i < body.size() - 1; i++) {
            if (!(body.get(i) instanceof com.legend.compiler.spec.typed.TypedLet let)) {
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
        if (spec instanceof com.legend.compiler.spec.typed.TypedFrom from) {
            return lower(from.source());
        }
        // The resolved GRAPH envelope keeps its CLASS-typed info (the
        // result-shape contract) but lowers as a relation.
        if (spec instanceof com.legend.compiler.spec.typed.TypedSerializeGraph g) {
            return serializeGraph(g);
        }
        // A terminal concatenate is a BARE set operation — no wrapping SELECT *.
        if (spec instanceof TypedConcatenate c) {
            return union(c);
        }
        if (spec.info().type() instanceof Type.RelationType) {
            return relation(spec);
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
            e = SqlExpr.Call.of(com.legend.sql.SqlFn.UNNEST, e);
        }
        return new SqlSelect(
                List.of(new SqlSelect.Projection(e, "value")), false, null,
                null, List.of(), null, null, List.of(), null, null,
                List.of(new OutputCol("value", sqlTypeOf(spec.info().type()),
                        PureSql.nullable(spec.info().multiplicity()))));
    }

    private String nextAlias() {
        return "t" + aliasCounter++;
    }

    // ==================================================================
    // Relation ops
    // ==================================================================

    private SqlSelect relation(TypedSpec spec) {
        return switch (spec) {
            case com.legend.compiler.spec.typed.TypedSourceUrl su -> SqlSelect.starOf(
                    new SqlSource.SourceUrl(su.url(), nextAlias(), outputsOf(su.info())));
            case TypedTableReference t -> SqlSelect.starOf(
                    new SqlSource.Table(t.table(), nextAlias(), outputsOf(t.info())));

            case TypedTds tds -> tdsLiteral(tds);

            case TypedFilter f -> filter(f);

            case TypedSelect sel -> narrowTo(relation(sel.source()), sel.columns(), sel.info());

            case TypedDistinct d -> distinct(d);

            case TypedRename r -> rename(r);

            case TypedSort s -> sort(s);
            case com.legend.compiler.spec.typed.TypedSortBy sb -> sortBy(sb);

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
            case com.legend.compiler.spec.typed.TypedNavigate nav -> navigate(nav);

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

            case com.legend.compiler.spec.typed.TypedJoin j -> join(j);

            case com.legend.compiler.spec.typed.TypedAsOfJoin aj -> asOfJoin(aj);

            case TypedExtendAgg ea -> extendAgg(ea);

            // from(mapping, runtime): execution-context metadata — a Phase-H
            // concern; the relation flows through unchanged.
            case com.legend.compiler.spec.typed.TypedFrom fr -> relation(fr.source());

            // cast(@Relation<(…)>): when EVERY target column exists in the
            // source, the cast is a real projection — surviving columns only,
            // each SQL-CAST where its type changed (String->Integer). Target
            // names ABSENT from the source are the pivot idiom's dynamic
            // columns — those stay type-only (zero SQL footprint).
            case com.legend.compiler.spec.typed.TypedCast c
                    when c.source().info().type() instanceof Type.RelationType srcRow
                    && c.info().type() instanceof Type.RelationType tgtRow ->
                    relationCast(c, srcRow, tgtRow);

            case com.legend.compiler.spec.typed.TypedFlatten fl -> flatten(fl);

            case com.legend.compiler.spec.typed.TypedPivot pv -> pivot(pv);

            // STORE-ONLY nodes: reaching the lowerer is not a missing rule —
            // it means the Phase H resolver failed to rewrite them away. Say
            // so, instead of the frontier default's misdiagnosis.
            case com.legend.compiler.spec.typed.TypedJoinSlot js ->
                    throw new com.legend.error.NotImplementedException(
                            "TypedJoinSlot (pipeline slot join '" + js.alias()
                          + "') escaped Phase H store resolution — a resolver gap,"
                          + " not a missing lowering rule");

            // SANCTIONED frontier default (root package-info invariant is
            // scoped to hiding-prone switches): the not-yet-lowered TypedSpec
            // variants churn every milestone; each throws LOUD and NAMED.
            default -> throw new com.legend.error.NotImplementedException("lowering not yet implemented for "
                    + spec.getClass().getSimpleName());
        };
    }

    /** Nested concatenates flatten into ONE multi-branch union. */
    private SqlUnion union(TypedConcatenate c) {
        List<com.legend.sql.SqlQuery> branches = new ArrayList<>();
        collectBranches(c, branches);
        return new SqlUnion(branches, true, outputsOf(c.info()));
    }

    private void collectBranches(TypedSpec spec, List<com.legend.sql.SqlQuery> out) {
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

    private SqlSelect buildGroupBy(SqlSelect base, TypedGroupBy g) {
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
            ps.add(new SqlSelect.Projection(aggValue(base, a), a.name()));
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
    private SqlSelect serializeGraph(com.legend.compiler.spec.typed.TypedSerializeGraph g) {
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
        SqlExpr obj = new SqlExpr.JsonObject(kv);
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
            default -> e;
        };
    }

    private SqlExpr aggValue(SqlSelect base, TypedAggCol a) {
        TypedSpec reduceBody = last(a.reduce());
        // A cast WRAPPING the reducer (y|$y->plus()->cast(@Integer)) rides
        // AROUND the SQL aggregate: unwrap, lower the inner reducer, re-wrap
        // by the cast policy (widening/same-type is the assertion no-op —
        // the PCT shapes are Integer->Integer).
        if (reduceBody instanceof com.legend.compiler.spec.typed.TypedCast rc
                && rc.source() instanceof TypedNativeCall) {
            SqlExpr inner = aggValue(base, new TypedAggCol(a.name(), a.map(),
                    new TypedLambda(a.reduce().parameters(),
                            List.of(rc.source()), a.reduce().info())));
            return castByPolicy(inner, rc.source().info().type(), rc.target());
        }
        if (!(reduceBody instanceof TypedNativeCall call)) {
            throw new IllegalStateException("aggregate reduce must be a native reducer call, got "
                    + reduceBody.getClass().getSimpleName());
        }
        String fn = Aggregates.reducerFor(call.callee());
        TypedSpec mapBody = last(a.map());
        // Reducer EXTRA arguments (joinStrings('_') carries its separator;
        // percentile carries p [+ ascending, continuous]): literal args ride
        // along after the value; variable refs are the reducer's own
        // collection params; anything ELSE is unsupported and must be LOUD.
        List<SqlExpr> extra = new ArrayList<>();
        List<Boolean> flags = new ArrayList<>();
        com.legend.compiler.spec.typed.TypedCast valueCast = null;
        for (TypedSpec argSpec : call.args()) {
            if (argSpec instanceof com.legend.compiler.spec.typed.TypedCBoolean b) {
                flags.add(b.value());
            } else if (argSpec instanceof com.legend.compiler.spec.typed.TypedCString
                    || argSpec instanceof TypedCInteger
                    || argSpec instanceof com.legend.compiler.spec.typed.TypedCFloat
                    || argSpec instanceof com.legend.compiler.spec.typed.TypedCDecimal) {
                extra.add(scalar(argSpec, (v, name) -> resolveOrThrow(base, name)));
            } else if (argSpec instanceof com.legend.compiler.spec.typed.TypedCast vc
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
                    extra.set(0, SqlExpr.Call.of(SqlFn.MINUS,
                            new SqlExpr.IntLit(1), extra.get(0)));
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
        if (mapBody instanceof TypedVariable && extra.isEmpty()) {
            return new SqlAgg.Reducer(fn, List.of(), false);
        }
        SqlExpr value = scalar(mapBody, (v, name) -> resolveOrThrow(base, name));
        if (valueCast != null) {
            value = castByPolicy(value, valueCast.source().info().type(), valueCast.target());
        }
        // hashCode over a group: HASH(LIST(values)) — no single SQL reducer.
        if ("__HASH_LIST__".equals(fn)) {
            return SqlExpr.Call.of(com.legend.sql.SqlFn.HASH,
                    new SqlAgg.Reducer("LIST", List.of(value), false));
        }
        // joinStrings(prefix, sep, suffix): STRING_AGG takes only the
        // separator — prefix/suffix concatenate AROUND the aggregate
        // (the audit: three extras produced an invalid 4-arg string_agg).
        if ("STRING_AGG".equals(fn) && extra.size() == 3) {
            return SqlExpr.Call.of(SqlFn.CONCAT,
                    SqlExpr.Call.of(SqlFn.CONCAT, extra.get(0),
                            new SqlAgg.Reducer(fn, List.of(value, extra.get(1)), false)),
                    extra.get(2));
        }
        List<SqlExpr> args = new ArrayList<>();
        args.add(value);
        args.addAll(extra);
        return new SqlAgg.Reducer(fn, args, false);
    }

    /**
     * extend (append=true) / project (append=false) with computed columns.
     * Column lambdas resolve against the CURRENT select via substitution, so
     * a plain-projection or star select stays flat.
     */
    /** extend(~cols): existing projections stay, computed columns APPEND. */
    private SqlSelect extend(SqlSelect src, List<TypedFuncCol> columns,
                             com.legend.compiler.element.type.ExprType info) {
        SqlSelect base = Fold.extendFolds(src) ? src : isolate(src);
        return computedColumns(base, columns, info, true);
    }

    /** project(~cols): the computed columns REPLACE the projection list. */
    private SqlSelect project(SqlSelect src, List<TypedFuncCol> columns,
                              com.legend.compiler.element.type.ExprType info) {
        SqlSelect base = Fold.projectionFolds(src) ? src : isolate(src);
        return computedColumns(base, columns, info, false);
    }

    /**
     * Lower computed columns over {@code base}: one attempt, isolate ONCE on
     * an unfoldable ref, then loud (isolation is idempotent for resolution).
     */
    private SqlSelect computedColumns(SqlSelect base, List<TypedFuncCol> columns,
                                      com.legend.compiler.element.type.ExprType info,
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
                                         com.legend.compiler.element.type.ExprType info,
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
        var saw = new java.util.concurrent.atomic.AtomicBoolean();
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
        var outers = java.util.List.copyOf(enclosing);
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
            java.util.function.Function<SqlSelect, SqlSelect> build) {
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
    private Resolution attempt(java.util.function.Supplier<SqlExpr> attemptFn) {
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
                               com.legend.compiler.element.type.ExprType info) {
        SqlSelect base = Fold.projectionFolds(src) ? src : isolate(src);
        return projectColumns(base, columns, info);
    }

    /** distinct(~cols): narrow AND dedup (distinct has its own fold policy). */
    private SqlSelect distinctNarrowTo(SqlSelect src, List<String> columns,
                                       com.legend.compiler.element.type.ExprType info) {
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
                                     com.legend.compiler.element.type.ExprType info) {
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
        java.util.function.Function<String, String> sourceOf = out -> {
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
    private SqlSelect sortBy(com.legend.compiler.spec.typed.TypedSortBy sb) {
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
        String alias = nextAlias();
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
    private SqlSelect navigate(com.legend.compiler.spec.typed.TypedNavigate nav) {
        if (nav.form() != com.legend.compiler.spec.typed.TypedNavigate.Form.PRE_MAP
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
            flat.add(new Type.Column(alias + "_" + c.name(), c.type(),
                    com.legend.compiler.element.type.Multiplicity.Bounded.ZERO_ONE));
        }
        var flatInfo = new com.legend.compiler.element.type.ExprType(
                new Type.RelationType(flat), nav.info().multiplicity());
        var leftKind = new com.legend.compiler.spec.typed.TypedEnumValue(
                "meta::pure::functions::relation::JoinKind", "LEFT", nav.info());
        return join(new com.legend.compiler.spec.typed.TypedJoin(nav.source(),
                nav.target(), leftKind, nav.predicate(),
                java.util.Optional.of(alias + "_"), flatInfo));
    }

    private SqlSelect join(com.legend.compiler.spec.typed.TypedJoin j) {
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
    private SqlSelect asOfJoin(com.legend.compiler.spec.typed.TypedAsOfJoin aj) {
        SqlSource left = asLeftJoinSide(relation(aj.left()));
        SqlSource right = asRightSide(relation(aj.right()));
        SqlExpr on = sideCondition(aj.match(), left, right);
        if (aj.condition().isPresent()) {
            on = SqlExpr.Call.of(SqlFn.AND, sideCondition(aj.condition().get(), left, right), on);
        }
        java.util.Set<String> leftNames = new java.util.HashSet<>();
        schemaOf(aj.left()).columns().forEach(c -> leftNames.add(c.name()));
        return joined(new SqlSource.Join(left, right, SqlSource.Join.Kind.ASOF_LEFT, on),
                aj.prefix(), aj.right(), aj.info(), null, leftNames::contains);
    }

    /**
     * The joined select: bare star when column names are disjoint (Phase G
     * guarantees), or left star + explicitly re-aliased right columns when a
     * prefix renames EVERY right column.
     */
    private SqlSelect joined(SqlSource.Join source, java.util.Optional<String> prefix,
                             TypedSpec rightNode, com.legend.compiler.element.type.ExprType info) {
        return joined(source, prefix, rightNode, info, null, name -> true);
    }

    private SqlSelect joined(SqlSource.Join source, java.util.Optional<String> prefix,
                             TypedSpec rightNode, com.legend.compiler.element.type.ExprType info,
                             List<SqlSelect.Projection> leftCarry) {
        return joined(source, prefix, rightNode, info, leftCarry, name -> true);
    }

    private SqlSelect joined(SqlSource.Join source, java.util.Optional<String> prefix,
                             TypedSpec rightNode, com.legend.compiler.element.type.ExprType info,
                             List<SqlSelect.Projection> leftCarry,
                             java.util.function.Predicate<String> renameWhen) {
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
            for (com.legend.sql.OutputCol c : leftTree.outputs()) {
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
            for (com.legend.compiler.spec.typed.TypedFuncCol c : w.columns()) {
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
        boolean rows = com.legend.builtin.Pure.nativeNamed("rows", call.callee().signatureKey());
        return new SqlExpr.WindowCall.Frame(
                rows ? SqlExpr.WindowCall.Frame.Kind.ROWS : SqlExpr.WindowCall.Frame.Kind.RANGE,
                bound(call.args().get(0), true), bound(call.args().get(1), false));
    }

    private SqlExpr.WindowCall.Frame.Bound bound(TypedSpec arg, boolean fromSide) {
        // A negative literal arrives as unary minus AROUND the number — unwrap.
        if (arg instanceof TypedNativeCall neg
                && com.legend.builtin.Pure.nativeNamed("minus", neg.callee().signatureKey())
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
                && com.legend.builtin.Pure.nativeNamed("unbounded", call.callee().signatureKey())) {
            return fromSide ? new SqlExpr.WindowCall.Frame.Bound.UnboundedPreceding()
                    : new SqlExpr.WindowCall.Frame.Bound.UnboundedFollowing();
        }
        // NO fallback: an unrecognized bound is a loud error, never UNBOUNDED.
        throw new IllegalStateException("window frame bound must be a numeric literal or"
                + " unbounded(), got " + arg.getClass().getSimpleName());
    }

    /** The numeric value of a literal frame bound, or null (RANGE takes decimals). */
    private static Number numericBound(TypedSpec arg) {
        return switch (arg) {
            case TypedCInteger c -> c.value().longValue();
            case TypedCFloat c -> c.value();
            case com.legend.compiler.spec.typed.TypedCDecimal c -> c.value();
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
                args.add(new SqlExpr.Column(base.from().alias(), p.property()));
                trailingIntArgs(call, args);
                return new SqlExpr.WindowCall(new SqlAgg.ValueFn(fn.sqlName(), args),
                        over.partitionBy(), over.orderBy(), over.frame());
            }
            // Real pure's 4-arg colToAgg window aggregates: average(p,w,r,~col).
            case TypedNativeCall call when Windows.aggregate(call.callee()) != null -> {
                TypedSpec colArg = call.args().get(call.args().size() - 1);
                if (!(colArg instanceof com.legend.compiler.spec.typed.TypedColSpec cs)) {
                    throw new IllegalStateException(
                            "window aggregate colToAgg must be a ~column colspec");
                }
                return new SqlExpr.WindowCall(
                        new SqlAgg.Reducer(Windows.aggregate(call.callee()),
                                List.of(new SqlExpr.Column(base.from().alias(), cs.name())),
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
            case TypedCInteger c -> new SqlExpr.IntLit(c.value().longValue());
            case TypedCString c -> new SqlExpr.StringLit(c.value());
            case TypedCBoolean c -> new SqlExpr.BoolLit(c.value());
            case TypedCFloat c -> new SqlExpr.FloatLit(c.value());
            case com.legend.compiler.spec.typed.TypedCDecimal c ->
                    new SqlExpr.DecimalLit(c.value());
            // Date literals: full dates/timestamps render typed; PARTIAL
            // dates (year / year-month) compare as STRINGS in SQL (master's
            // pinned semantics) — represented as string literals here.
            case com.legend.compiler.spec.typed.TypedCDate d -> switch (d.value()) {
                case com.legend.values.PureDateLiteral.StrictDate sd ->
                        new SqlExpr.DateLit(sd.toEngineString());
                case com.legend.values.PureDateLiteral.Year y ->
                        new SqlExpr.StringLit(y.toEngineString());
                case com.legend.values.PureDateLiteral.YearMonth ym ->
                        new SqlExpr.StringLit(ym.toEngineString());
                // Every time-bearing precision is a TIMESTAMP — exhaustive,
                // so a new precision variant demands a decision here.
                // HOUR/MINUTE-precision literals PAD to the full timestamp
                // shape SQL demands (%2015-04-15T17 is 17:00:00); second-level
                // precision is already full.
                case com.legend.values.PureDateLiteral.DateWithHour h ->
                        new SqlExpr.TimestampLit(h.toEngineString() + ":00:00");
                case com.legend.values.PureDateLiteral.DateWithMinute mi ->
                        new SqlExpr.TimestampLit(mi.toEngineString() + ":00");
                case com.legend.values.PureDateLiteral.DateWithSecond se ->
                        new SqlExpr.TimestampLit(se.toEngineString());
                case com.legend.values.PureDateLiteral.DateWithSubsecond su ->
                        new SqlExpr.TimestampLit(su.toEngineString());
            };
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
                    && !com.legend.compiler.element.type.PlatformTypes.isVariant(ct)
                    && !com.legend.compiler.element.type.PlatformTypes.isNil(ct)
                    && classLayout.apply(ct).isEmpty() ->
                    new SqlExpr.ArrayLit(c.elements().stream()
                            .map(e -> (SqlExpr) SqlExpr.Call.of(
                                    com.legend.sql.SqlFn.TO_VARIANT, scalar(e, columns)))
                            .toList());
            case TypedCollection c -> new SqlExpr.ArrayLit(
                    c.elements().stream().map(e -> scalar(e, columns)).toList());
            // $r.alias.COL — a NAVIGATE slot's struct column flattens to
            // its prefixed physical column (alias_COL).
            case TypedPropertyAccess p when p.source() instanceof TypedPropertyAccess inner
                    && inner.source() instanceof TypedVariable v
                    && inner.info().type() instanceof Type.RelationType
                    -> columns.resolve(v.name(), inner.property() + "_" + p.property());
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
            case TypedPropertyAccess p when p.source()
                        instanceof com.legend.compiler.spec.typed.TypedNewInstance ni -> {
                TypedSpec v = ni.properties().get(p.property());
                // The MODEL declares the field; an unset optional is NULL.
                yield v == null ? new SqlExpr.NullLit() : scalar(v, columns);
            }
            // Field access over a TO-MANY class value (filter(...).legalName)
            // MAPS the extraction; a to-one source extracts directly.
            case TypedPropertyAccess p when classLayout.apply(p.source().info().type()).isPresent()
                    && isMany(p.source()) -> {
                String elem = "_pa" + aliasCounter++;
                yield SqlExpr.Call.of(com.legend.sql.SqlFn.LIST_TRANSFORM,
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
            case com.legend.compiler.spec.typed.TypedNewInstance n -> {
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
                            com.legend.compiler.element.type.Multiplicity.Bounded b
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
                var predicate = java.util.Objects.requireNonNull(relationPredicate(n));
                enclosing.push(columns);
                try {
                    yield predicate.lower(this, n);
                } finally {
                    enclosing.pop();
                }
            }
            case com.legend.compiler.spec.typed.TypedFold f -> fold(f, columns);

            // map over a COLLECTION value -> listTransform (relation map is H).
            case com.legend.compiler.spec.typed.TypedMap m
                    when !(m.source().info().type() instanceof Type.RelationType) ->
                    SqlExpr.Call.of(com.legend.sql.SqlFn.LIST_TRANSFORM,
                            scalar(m.source(), columns), scalar(m.mapper(), columns));

            // Variant navigation: get(v, key) -> JSON access.
            case TypedNativeCall n when isFamily(n, "get") ->
                    SqlExpr.Call.of(com.legend.sql.SqlFn.VARIANT_GET,
                            scalar(n.args().get(0), columns), scalar(n.args().get(1), columns));

            case com.legend.compiler.spec.typed.TypedCast c -> cast(c, columns);

            // if(cond, {|then}, {|else}) — scalar position: CASE WHEN.
            // If-chains (the mapping enum decode emission) render as NESTED
            // CASE expressions in the otherwise slot — correct; single-CASE
            // flattening is a cosmetic peephole if ever demanded.
            case com.legend.compiler.spec.typed.TypedIf i -> new SqlExpr.Case(
                    java.util.List.of(new SqlExpr.Case.When(
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
            case com.legend.compiler.spec.typed.TypedEnumValue e -> new SqlExpr.StringLit(e.value());

            case TypedNativeCall n when isFamily(n, "equal")
                    && enumTypeMismatch(n.args()) ->
                    throw new com.legend.error.NotImplementedException(
                            "equality between an enum value and a non-matching type"
                                    + " is not lowered (enum values render as name"
                                    + " strings; cross-type equality would be"
                                    + " silently wrong)");

            // COLLECTION-VALUED relation nodes in scalar position: the list
            // encodings ([1,2,3]->filter/slice/drop/take over a value, not a
            // table). Relation-typed sources take the relation() arms.
            case TypedFilter f when !(f.source().info().type() instanceof Type.RelationType) ->
                    SqlExpr.Call.of(com.legend.sql.SqlFn.LIST_FILTER,
                            scalar(f.source(), columns), scalar(f.predicate(), columns));
            // slice(start, stop): 0-based exclusive-stop -> 1-based inclusive
            // array_slice; a NEGATIVE start clamps to the list head (PCT).
            case TypedSlice s when !(s.source().info().type() instanceof Type.RelationType) ->
                    SqlExpr.Call.of(com.legend.sql.SqlFn.LIST_SLICE,
                            scalar(s.source(), columns),
                            onePlus(clamp0(scalar(s.start(), columns))),
                            // STOP clamps too: DuckDB reads a negative bound
                            // FROM THE END (slice(l,0,-1) returned the whole
                            // list; pure says empty — audit).
                            clamp0(scalar(s.stop(), columns)));
            // drop(n): the suffix from n+1; negative n drops nothing (PCT).
            case TypedDrop d when !(d.source().info().type() instanceof Type.RelationType) -> {
                SqlExpr src = scalar(d.source(), columns);
                yield SqlExpr.Call.of(com.legend.sql.SqlFn.LIST_SLICE, src,
                        onePlus(clamp0(scalar(d.count(), columns))),
                        SqlExpr.Call.of(com.legend.sql.SqlFn.LIST_LENGTH, src));
            }
            // take(n): the prefix; negative n takes nothing (PCT) — the clamp
            // matters because DuckDB reads a negative bound FROM THE END.
            case TypedLimit t when !(t.source().info().type() instanceof Type.RelationType) ->
                    SqlExpr.Call.of(com.legend.sql.SqlFn.LIST_SLICE,
                            scalar(t.source(), columns), new SqlExpr.IntLit(1),
                            clamp0(scalar(t.count(), columns)));
            // A let in EXPRESSION position (a callee shape the statement
            // folder didn't reach): bind and yield the value — the let IS
            // its value.
            case com.legend.compiler.spec.typed.TypedLet l -> {
                SqlExpr v = scalar(l.value(), columns);
                letBindings.put(l.name(), v);
                yield v;
            }
            case TypedNativeCall n -> Scalars.lower(n,
                    n.args().stream().map(a -> scalar(a, columns)).toList());
            // SANCTIONED frontier default — see relation() above.
            default -> throw new com.legend.error.NotImplementedException("scalar lowering not yet implemented for "
                    + spec.getClass().getSimpleName());
        };
    }

    /** Clamp a (possibly negative) index to zero — PCT's slice/drop/take edge semantics. */
    private static SqlExpr clamp0(SqlExpr e) {
        return e instanceof SqlExpr.IntLit i
                ? new SqlExpr.IntLit(Math.max(0, i.value()))
                : SqlExpr.Call.of(com.legend.sql.SqlFn.GREATEST, e, new SqlExpr.IntLit(0));
    }

    /** 0-based → 1-based shift, constant-folded for literals. */
    private static SqlExpr onePlus(SqlExpr e) {
        return e instanceof SqlExpr.IntLit i
                ? new SqlExpr.IntLit(i.value() + 1)
                : SqlExpr.Call.of(com.legend.sql.SqlFn.PLUS, e, new SqlExpr.IntLit(1));
    }

    /** A value in LIST position: singleton-wrap unless it is already a list (or NULL = empty). */
    private static SqlExpr asList(SqlExpr e, boolean many) {
        return many || e instanceof SqlExpr.ArrayLit || e instanceof SqlExpr.NullLit
                ? e : new SqlExpr.ArrayLit(List.of(e));
    }

    /** An instance literal in relation position: {@code ^X(…)} or a collection of them. */
    private static boolean isInstanceLiteral(TypedSpec source) {
        return source instanceof com.legend.compiler.spec.typed.TypedNewInstance
                || (source instanceof TypedCollection c
                        && !c.elements().isEmpty()
                        && c.elements().stream().allMatch(e ->
                                e instanceof com.legend.compiler.spec.typed.TypedNewInstance));
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
        List<com.legend.compiler.spec.typed.TypedNewInstance> instances =
                p.source() instanceof TypedCollection c
                        ? c.elements().stream()
                                .map(e -> (com.legend.compiler.spec.typed.TypedNewInstance) e)
                                .toList()
                        : List.of((com.legend.compiler.spec.typed.TypedNewInstance) p.source());
        List<OutputCol> outputs = outputsOf(p.info());
        List<SqlQuery> branches = new ArrayList<>(instances.size());
        for (var inst : instances) {
            branches.add(instanceSelect(inst, p.columns(), outputs));
        }
        if (branches.size() == 1) {
            return (SqlSelect) branches.get(0);
        }
        return SqlSelect.starOf(new SqlSource.Subselect(
                new com.legend.sql.SqlUnion(branches, true, outputs), nextAlias()));
    }

    private SqlSelect instanceSelect(com.legend.compiler.spec.typed.TypedNewInstance inst,
                                     List<TypedFuncCol> columns, List<OutputCol> outputs) {
        SqlSource src = null;
        // ONE unnest per to-many PATH PREFIX (the NavPath-registry rule):
        // two colspecs over $x.addresses iterate the SAME collection — real
        // pure yields (city, zip) pairs, never their cross product. Only
        // INDEPENDENT collections cross-multiply.
        java.util.Map<String, String> unnestByPrefix = new java.util.LinkedHashMap<>();
        List<SqlSelect.Projection> ps = new ArrayList<>(columns.size());
        for (TypedFuncCol col : columns) {
            List<String> path = pathOf(col);
            // Walk the path over the literal: to-one instance hops recurse;
            // the first TO-MANY value becomes the unnest source.
            TypedSpec cur = inst;
            SqlExpr value = null;
            for (int i = 0; i < path.size(); i++) {
                if (!(cur instanceof com.legend.compiler.spec.typed.TypedNewInstance ni)) {
                    throw new com.legend.error.NotImplementedException(
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
                                        SqlExpr.Call.of(com.legend.sql.SqlFn.UNNEST, array),
                                        "elem")),
                                false, null, null, List.of(), null, null, List.of(), null, null,
                                List.of(new OutputCol("elem",
                                        com.legend.sql.SqlType.Scalar.VARCHAR, true)));
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
            ps.add(new SqlSelect.Projection(java.util.Objects.requireNonNull(value), col.name()));
        }
        return new SqlSelect(ps, false, src, null, List.of(), null, null,
                List.of(), null, null, outputs);
    }

    /** The 1-row anchor a lateral chain hangs off (an empty array must NULL, not kill, the row). */
    private SqlSource anchorJoin(SqlSource right) {
        SqlSource anchor = new SqlSource.Values(
                List.of(List.of(new SqlExpr.IntLit(1))), List.of("_anchor"), nextAlias(),
                List.of(new OutputCol("_anchor", com.legend.sql.SqlType.Scalar.BIGINT, false)));
        return new SqlSource.Join(anchor, right,
                SqlSource.Join.Kind.LEFT_LATERAL, new SqlExpr.BoolLit(true));
    }

    /** A colspec body as a bare property path rooted at the lambda parameter — loud otherwise. */
    private static List<String> pathOf(TypedFuncCol col) {
        String param = col.fn().parameters().get(0);
        java.util.ArrayDeque<String> path = new java.util.ArrayDeque<>();
        TypedSpec cur = col.fn().body().get(col.fn().body().size() - 1);
        while (cur instanceof TypedPropertyAccess pa) {
            path.addFirst(pa.property());
            cur = pa.source();
        }
        if (!(cur instanceof TypedVariable v) || !v.name().equals(param) || path.isEmpty()) {
            throw new com.legend.error.NotImplementedException(
                    "instance-literal project supports bare property paths ($"
                            + param + ".a.b) only");
        }
        return List.copyOf(path);
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
    private SqlSelect pivot(com.legend.compiler.spec.typed.TypedPivot pv) {
        if (pv.pivotColumns().size() != 1) {
            throw new com.legend.error.NotImplementedException("multi-column pivot is not lowered yet");
        }
        SqlSelect src = relation(pv.source());
        SqlSource inner = asRightSide(src);
        List<SqlExpr> on = List.of(Fold.sourceColumn(inner, pv.pivotColumns().get(0)));
        List<SqlSource.Pivot.Using> usings = new ArrayList<>();
        SqlSelect forAgg = SqlSelect.starOf(inner);
        for (TypedAggCol a : pv.aggs()) {
            usings.add(new SqlSource.Pivot.Using(aggExpr(forAgg, a), a.name()));
        }
        // Fully-qualified refs; a dialect whose PIVOT forbids qualifiers in
        // USING (DuckDB) strips them AT RENDER TIME.
        return SqlSelect.starOf(new SqlSource.Pivot(inner, on, usings, nextAlias(),
                outputsOf(pv.info())));
    }

    /**
     * flatten(~col): the column explodes via UNNEST in the select list —
     * schema-driven explicit projections (every other column plain, the
     * flattened one replaced). Downstream refs to the flattened column are
     * COMPUTED projections, so the fold policy isolates them naturally.
     * A Variant column casts to JSON[] first; a typed list unnests directly.
     */
    private SqlSelect flatten(com.legend.compiler.spec.typed.TypedFlatten fl) {
        SqlSelect src = relation(fl.source());
        SqlSelect base = Fold.extendFolds(src) ? src : isolate(src);
        Type.RelationType schema = schemaOf(fl.source());
        List<SqlSelect.Projection> ps = new ArrayList<>();
        for (Type.Column c : schema.columns()) {
            SqlExpr col = Fold.resolveInto(base, c.name());
            if (col == null) {
                throw new IllegalStateException("flatten source column '" + c.name()
                        + "' cannot be resolved (unresolvable projection)");
            }
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
    private SqlExpr cast(com.legend.compiler.spec.typed.TypedCast c,
                         ColumnResolver columns) {
        SqlExpr value = scalar(c.source(), columns);
        boolean variantSource = c.source().info().type()
                instanceof Type.ClassType ct && com.legend.compiler.element.type.PlatformTypes.isVariant(ct);
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
                return new SqlExpr.Cast(value, PureSql.type(c.target()));
            }
            return value;
        }
        boolean many = isMany(c);
        if (many) {
            boolean variantTarget = c.target() instanceof Type.ClassType t
                    && com.legend.compiler.element.type.PlatformTypes.isVariant(t);
            return variantTarget
                    ? SqlExpr.Call.of(com.legend.sql.SqlFn.VARIANT_ELEMENTS, value)
                    // A to-many cast targets an ARRAY of the element type —
                    // expressed in the TYPE (SqlType.Array), not a side flag.
                    : new SqlExpr.Cast(value,
                            new com.legend.sql.SqlType.Array(PureSql.type(c.target())));
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
    private SqlSelect relationCast(com.legend.compiler.spec.typed.TypedCast c,
                                   Type.RelationType srcRow, Type.RelationType tgtRow) {
        java.util.Map<String, Type.Column> src = new java.util.LinkedHashMap<>();
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
    private SqlSelect tryRelationCast(SqlSelect base, java.util.Map<String, Type.Column> src,
                                      Type.RelationType tgtRow,
                                      com.legend.compiler.spec.typed.TypedCast c) {
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
    private SqlExpr fold(com.legend.compiler.spec.typed.TypedFold f,
                         ColumnResolver columns) {
        SqlExpr source = scalar(f.source(), columns);
        SqlExpr init = scalar(f.init(), columns);
        List<String> ps = f.reducer().parameters();
        return switch (f.strategy()) {
            // A TO-ONE side (1->fold(add, …), scalar init) concatenates as a
            // singleton list. Already-list-shaped values ([9] lowers to an
            // ArrayLit despite mult [1]) and NULL (the []-born empty, which
            // DuckDB list_concat treats as []) pass through.
            case com.legend.compiler.spec.typed.FoldStrategy.Concatenation c ->
                    new SqlExpr.Call(SqlFn.LIST_CONCAT,
                            List.of(asList(init, isMany(f.init())),
                                    asList(source, isMany(f.source()))));
            case com.legend.compiler.spec.typed.FoldStrategy.SameType st ->
                    new SqlExpr.FoldCall(source,
                            new SqlExpr.Lambda(ps,
                                    scalar(last(f.reducer()), lambdaResolver(ps, columns))),
                            init, isMany(f.init()), true);
            case com.legend.compiler.spec.typed.FoldStrategy.MapReduce mr -> {
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
            case com.legend.compiler.spec.typed.FoldStrategy.CollectionBuild cb ->
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
    private static boolean enumTypeMismatch(List<com.legend.compiler.spec.typed.TypedSpec> args) {
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
            return !(other instanceof Type.Primitive prim
                    && prim == Type.Primitive.STRING);
        }
        return false;
    }

    private static boolean isFamily(TypedNativeCall n, String pureName) {
        // signatureKey membership — the LAST parser-node dispatch the re-audit
        // found dodging the parser-free wall (ArchUnit cannot see a dependency
        // reached through definition()'s return type + contains(Object)).
        return com.legend.builtin.Pure.nativeNamed(pureName, n.callee().signatureKey());
    }

    private static RelationPredicate relationPredicate(TypedNativeCall n) {
        if (isFamily(n, "size")) {
            return (lw, call) -> new SqlExpr.ScalarSubquery(lw.relation(call.args().get(0))
                    .withProjections(List.of(new SqlSelect.Projection(
                            SqlAgg.Reducer.of("COUNT"), null)), List.of()));
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
    private static com.legend.compiler.spec.typed.TypedSpec thunkBody(
            com.legend.compiler.spec.typed.TypedSpec branch) {
        if (branch instanceof com.legend.compiler.spec.typed.TypedLambda l) {
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
        throw new com.legend.error.NotImplementedException(
                "dynamic slicing bounds are not lowered yet (literal expected), got "
                        + spec.getClass().getSimpleName());
    }

    private static Type.RelationType schemaOf(TypedSpec spec) {
        return (Type.RelationType) spec.info().type();
    }

    private List<OutputCol> outputsOf(com.legend.compiler.element.type.ExprType info) {
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
