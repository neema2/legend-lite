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
import com.legend.compiler.spec.typed.TypedFuncCol;
import com.legend.compiler.spec.typed.TypedGroupBy;
import com.legend.compiler.spec.typed.TypedDrop;
import com.legend.compiler.spec.typed.TypedFilter;
import com.legend.compiler.spec.typed.TypedLambda;
import com.legend.compiler.spec.typed.TypedLimit;
import com.legend.compiler.spec.typed.TypedNativeCall;
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

    /** Lower a typed relation pipeline to a SQL query. */
    public SqlQuery lower(TypedSpec spec) {
        // A terminal concatenate is a BARE set operation — no wrapping SELECT *.
        return spec instanceof TypedConcatenate c ? union(c) : relation(spec);
    }

    private String nextAlias() {
        return "t" + aliasCounter++;
    }

    // ==================================================================
    // Relation ops
    // ==================================================================

    private SqlSelect relation(TypedSpec spec) {
        return switch (spec) {
            case TypedTableReference t -> SqlSelect.starOf(
                    new SqlSource.Table(t.table(), nextAlias(), outputsOf(t.info())));

            case TypedTds tds -> tdsLiteral(tds);

            case TypedFilter f -> filter(f);

            case TypedSelect sel -> narrowTo(relation(sel.source()), sel.columns(), false, sel.info());

            case TypedDistinct d -> distinct(d);

            case TypedRename r -> rename(r);

            case TypedSort s -> sort(s);

            case TypedLimit l -> {
                SqlSelect src = relation(l.source());
                yield (Fold.limitFolds(src) ? src : isolate(src)).withLimit(intOf(l.count()));
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

            case TypedAggregate a -> aggregate(a);

            case TypedExtend e -> extendWith(relation(e.source()), e.columns(), e.info(), true);

            case TypedProject p -> extendWith(relation(p.source()), p.columns(), p.info(), false);

            case TypedConcatenate c -> SqlSelect.starOf(
                    new SqlSource.Subselect(union(c), nextAlias()));

            default -> throw new IllegalStateException("lowering not yet implemented for "
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
        List<SqlExpr> keys = new ArrayList<>(g.keys().size());
        List<SqlSelect.Projection> ps = new ArrayList<>();
        for (TypedGroupBy.GroupKey k : g.keys()) {
            SqlExpr e = k.fn().isPresent()
                    ? scalar(last(k.fn().get()), name -> resolveOrThrow(base, name))
                    : Fold.resolveInto(base, fromAlias(base), k.column());
            if (e == null) {
                return groupByOnto(isolate(base), g);
            }
            keys.add(e);
            ps.add(new SqlSelect.Projection(e,
                    e instanceof SqlExpr.Column c && c.name().equals(k.column()) ? null : k.column()));
        }
        for (TypedAggCol a : g.aggs()) {
            ps.add(new SqlSelect.Projection(aggExpr(base, a), a.name()));
        }
        return base.withGroupBy(keys).withProjections(ps, outputsOf(g.info()));
    }

    private SqlSelect groupByOnto(SqlSelect base, TypedGroupBy g) {
        List<SqlExpr> keys = new ArrayList<>();
        List<SqlSelect.Projection> ps = new ArrayList<>();
        for (TypedGroupBy.GroupKey k : g.keys()) {
            SqlExpr e = new SqlExpr.Column(fromAlias(base), k.column());
            keys.add(e);
            ps.add(new SqlSelect.Projection(e, null));
        }
        for (TypedAggCol a : g.aggs()) {
            ps.add(new SqlSelect.Projection(aggExpr(base, a), a.name()));
        }
        return base.withGroupBy(keys).withProjections(ps, outputsOf(g.info()));
    }

    /** aggregate: whole-relation reduction — aggregates only, no GROUP BY clause. */
    private SqlSelect aggregate(TypedAggregate a) {
        SqlSelect src = relation(a.source());
        SqlSelect base = Fold.groupByFolds(src) ? src : isolate(src);
        List<SqlSelect.Projection> ps = new ArrayList<>(a.aggs().size());
        for (TypedAggCol col : a.aggs()) {
            ps.add(new SqlSelect.Projection(aggExpr(base, col), col.name()));
        }
        return base.withProjections(ps, outputsOf(a.info()));
    }

    /**
     * One agg column: the map lambda yields the value expression; the reduce
     * lambda's resolved overload names the SQL reducer. A bare-row map
     * ({@code x|$x}) is COUNT(*)-style — no value argument.
     */
    private SqlAgg.Reducer aggExpr(SqlSelect base, TypedAggCol a) {
        TypedSpec reduceBody = last(a.reduce());
        if (!(reduceBody instanceof TypedNativeCall call)) {
            throw new IllegalStateException("aggregate reduce must be a native reducer call, got "
                    + reduceBody.getClass().getSimpleName());
        }
        String fn = Aggregates.reducerFor(call.callee().definition(), call.callee().qualifiedName());
        TypedSpec mapBody = last(a.map());
        if (mapBody instanceof TypedVariable) {
            return new SqlAgg.Reducer(fn, List.of(), false);
        }
        return new SqlAgg.Reducer(fn,
                List.of(scalar(mapBody, name -> resolveOrThrow(base, name))), false);
    }

    /**
     * extend (append=true) / project (append=false) with computed columns.
     * Column lambdas resolve against the CURRENT select via substitution, so
     * a plain-projection or star select stays flat.
     */
    private SqlSelect extendWith(SqlSelect src, List<TypedFuncCol> columns,
                                 com.legend.compiler.spec.ExprType info, boolean append) {
        SqlSelect base = (append ? Fold.extendFolds(src) : Fold.projectionFolds(src))
                ? src : isolate(src);
        List<SqlSelect.Projection> ps = new ArrayList<>();
        if (append) {
            if (base.projections().isEmpty()) {
                ps.add(new SqlSelect.Projection(new SqlExpr.Star(fromAlias(base)), null));
            } else {
                ps.addAll(base.projections());
            }
        }
        for (TypedFuncCol c : columns) {
            SqlExpr e;
            try {
                e = scalar(last(c.fn()), name -> resolveOrThrow(base, name));
            } catch (UnfoldableRef ref) {
                return extendWith(isolate(base), columns, info, append);
            }
            ps.add(new SqlSelect.Projection(e, c.name()));
        }
        return base.withProjections(ps, outputsOf(info));
    }

    private SqlSelect filter(TypedFilter f) {
        SqlSelect src = relation(f.source());
        SqlExpr predicate = tryPredicate(src, f.predicate());
        if (predicate == null) {
            src = isolate(src);
            predicate = tryPredicate(src, f.predicate());
        }
        Fold.FilterSlot slot = Fold.filterSlot(src, false);
        if (slot == Fold.FilterSlot.ISOLATE) {
            src = isolate(src);
            predicate = tryPredicate(src, f.predicate());
            slot = Fold.filterSlot(src, false);
        }
        return switch (slot) {
            case WHERE -> src.withWhere(src.where() == null ? predicate
                    : SqlExpr.Call.of("and", src.where(), predicate));
            case HAVING -> src.withHaving(src.having() == null ? predicate
                    : SqlExpr.Call.of("and", src.having(), predicate));
            case QUALIFY -> src.withQualify(src.qualify() == null ? predicate
                    : SqlExpr.Call.of("and", src.qualify(), predicate));
            case ISOLATE -> throw new IllegalStateException("unreachable: isolated above");
        };
    }

    /**
     * Lower the predicate against this select's columns; null = a ref would
     * not fold. Over a GROUPED select, refs resolve to the projection
     * EXPRESSIONS themselves (group keys and aggregate calls are exactly what
     * standard SQL admits in HAVING).
     */
    private SqlExpr tryPredicate(SqlSelect select, TypedLambda lambda) {
        try {
            java.util.function.Function<String, SqlExpr> columns = select.groupBy().isEmpty()
                    ? name -> resolveOrThrow(select, name)
                    : name -> projectionExprOrThrow(select, name);
            return scalar(last(lambda), columns);
        } catch (UnfoldableRef e) {
            return null;
        }
    }

    /** A post-aggregation ref: the projection's expression, computed or not. */
    private SqlExpr projectionExprOrThrow(SqlSelect select, String column) {
        for (SqlSelect.Projection p : select.projections()) {
            String name = p.alias() != null ? p.alias()
                    : p.expr() instanceof SqlExpr.Column c ? c.name() : null;
            if (column.equals(name)) {
                return p.expr();
            }
        }
        throw new UnfoldableRef(column);
    }

    private SqlExpr resolveOrThrow(SqlSelect select, String column) {
        SqlExpr resolved = Fold.resolveInto(select, fromAlias(select), column);
        if (resolved == null) {
            throw new UnfoldableRef(column);
        }
        return resolved;
    }

    private static final class UnfoldableRef extends RuntimeException {
        UnfoldableRef(String column) {
            super(column);
        }
    }

    /** select(~cols) / distinct(~cols): narrow the projection list. */
    private SqlSelect narrowTo(SqlSelect src, List<String> columns, boolean distinct,
                               com.legend.compiler.spec.ExprType info) {
        SqlSelect base = Fold.projectionFolds(src) ? src : isolate(src);
        if (distinct && !Fold.distinctNarrowFolds(base, columns)) {
            base = isolate(base);
        }
        List<SqlSelect.Projection> ps = new ArrayList<>(columns.size());
        for (String c : columns) {
            SqlExpr e = Fold.resolveInto(base, fromAlias(base), c);
            if (e == null) {
                base = isolate(base);
                return narrowTo(base, columns, distinct, info);
            }
            ps.add(new SqlSelect.Projection(e,
                    e instanceof SqlExpr.Column col && col.name().equals(c) ? null : c));
        }
        SqlSelect out = base.withProjections(ps, outputsOf(info));
        return distinct ? out.withDistinct() : out;
    }

    private SqlSelect distinct(TypedDistinct d) {
        SqlSelect src = relation(d.source());
        if (d.columns() != null && !d.columns().isEmpty()) {
            return narrowTo(src, d.columns(), true, d.info());
        }
        return (Fold.distinctFolds(src) ? src : isolate(src)).withDistinct();
    }

    /**
     * rename lowers to a FULL explicit projection from the (always-known)
     * output schema &mdash; flat and self-describing; no EXCLUDE gymnastics.
     */
    private SqlSelect rename(TypedRename r) {
        SqlSelect src = relation(r.source());
        SqlSelect base = Fold.projectionFolds(src) ? src : isolate(src);
        Type.RelationType sourceSchema = schemaOf(r.source());
        // Pre-pass: if ANY source column would not resolve to a plain column
        // reference in the folded select, isolate ONCE, then project.
        for (Type.Column c : sourceSchema.columns()) {
            if (Fold.resolveInto(base, fromAlias(base), c.name()) == null) {
                base = isolate(base);
                break;
            }
        }
        List<SqlSelect.Projection> ps = new ArrayList<>();
        for (Type.Column c : sourceSchema.columns()) {
            String target = c.name();
            for (TypedRename.ColRename cr : r.renames()) {
                if (cr.from().equals(c.name())) {
                    target = cr.to();
                }
            }
            SqlExpr e = Fold.resolveInto(base, fromAlias(base), c.name());
            ps.add(new SqlSelect.Projection(e, target.equals(
                    e instanceof SqlExpr.Column col ? col.name() : null) ? null : target));
        }
        return base.withProjections(ps, outputsOf(r.info()));
    }

    private SqlSelect sort(TypedSort s) {
        SqlSelect src = relation(s.source());
        SqlSelect base = Fold.sortFolds(src) ? src : isolate(src);
        List<SqlSelect.SortKey> keys = new ArrayList<>(s.keys().size());
        for (TypedSort.TypedSortKey k : s.keys()) {
            SqlExpr e = Fold.resolveInto(base, fromAlias(base), k.column());
            if (e == null) {
                base = isolate(base);
                return sortOnto(base, s);
            }
            keys.add(new SqlSelect.SortKey(e, k.ascending(), null));
        }
        return base.withOrderBy(keys);
    }

    private SqlSelect sortOnto(SqlSelect base, TypedSort s) {
        List<SqlSelect.SortKey> keys = new ArrayList<>(s.keys().size());
        for (TypedSort.TypedSortKey k : s.keys()) {
            keys.add(new SqlSelect.SortKey(
                    new SqlExpr.Column(fromAlias(base), k.column()), k.ascending(), null));
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
            return SqlSelect.starOf(v).withWhere(SqlExpr.Call.of("equal",
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
    // Scalar lowering (lambda bodies)
    // ==================================================================

    /** {@code columns} resolves a row-property name to a SQL expression in scope. */
    private SqlExpr scalar(TypedSpec spec, Function<String, SqlExpr> columns) {
        return switch (spec) {
            case TypedCInteger c -> new SqlExpr.IntLit(c.value().longValue());
            case TypedCString c -> new SqlExpr.StringLit(c.value());
            case TypedCBoolean c -> new SqlExpr.BoolLit(c.value());
            case TypedCFloat c -> new SqlExpr.FloatLit(c.value());
            case TypedCollection c -> new SqlExpr.ArrayLit(
                    c.elements().stream().map(e -> scalar(e, columns)).toList());
            case TypedPropertyAccess p when p.source() instanceof TypedVariable
                    -> columns.apply(p.property());
            case TypedNativeCall n -> Scalars.lower(n,
                    n.args().stream().map(a -> scalar(a, columns)).toList());
            default -> throw new IllegalStateException("scalar lowering not yet implemented for "
                    + spec.getClass().getSimpleName());
        };
    }

    // ==================================================================
    // Plumbing
    // ==================================================================

    /** Close the current select into a subselect and open a fresh star select over it. */
    private SqlSelect isolate(SqlSelect s) {
        return SqlSelect.starOf(new SqlSource.Subselect(s, nextAlias()));
    }

    private static String fromAlias(SqlSelect s) {
        return switch (s.from()) {
            case SqlSource.Table t -> t.alias();
            case SqlSource.Subselect sub -> sub.alias();
            case SqlSource.Values v -> v.alias();
            case SqlSource.Join j -> throw new IllegalStateException(
                    "join sources resolve per-side (M4)");
        };
    }

    private static TypedSpec last(TypedLambda lambda) {
        return lambda.body().get(lambda.body().size() - 1);
    }

    private static long intOf(TypedSpec spec) {
        if (spec instanceof TypedCInteger c) {
            return c.value().longValue();
        }
        throw new IllegalStateException(
                "dynamic slicing bounds are not lowered yet (literal expected), got "
                        + spec.getClass().getSimpleName());
    }

    private static Type.RelationType schemaOf(TypedSpec spec) {
        return (Type.RelationType) spec.info().type();
    }

    private static List<OutputCol> outputsOf(com.legend.compiler.spec.ExprType info) {
        if (!(info.type() instanceof Type.RelationType rt)) {
            return List.of();
        }
        return rt.columns().stream()
                .map(c -> new OutputCol(c.name(), c.type(), c.multiplicity()))
                .toList();
    }
}
