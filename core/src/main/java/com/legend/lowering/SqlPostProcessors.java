// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.lowering;

import com.legend.compiler.spec.typed.TypedCollection;
import com.legend.compiler.spec.typed.TypedLambda;
import com.legend.compiler.spec.typed.TypedNativeCall;
import com.legend.compiler.spec.typed.TypedNewInstance;
import com.legend.compiler.spec.typed.TypedSpec;
import com.legend.error.NotImplementedException;
import com.legend.sql.SqlExpr;
import com.legend.sql.SqlQuery;
import com.legend.sql.SqlSelect;
import com.legend.sql.SqlSource;
import com.legend.sql.SqlUnion;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * The connection's {@code sqlQueryPostProcessorsConnectionAware} hooks
 * (real relationalRuntime.pure:42), applied over legend-lite's OWN SQL
 * IR: the engine hands its SQL metamodel to opaque lambdas; we
 * RECOGNIZE the shapes the corpus builds (replaceTables over literal
 * table pairs) and run the equivalent IR rewrite — Java orchestrates,
 * the database executes. Unknown hook shapes inside the recognized
 * channel are loud, never silently dropped.
 */
public final class SqlPostProcessors {

    private SqlPostProcessors() {
    }

    /** The tableReplace renames of every {@code execute()} call a
     * statement REACHES — inline calls and calls behind ORDINARY lets
     * ({@code let result = execute(...).values} over a class-rooted
     * execute is a plain let, not a let-bound exec frame; the assert
     * side's re-plan of its spliced chain must still rename — batch 80,
     * testGraphFetchWithTableMapperPostProcessor). Conflicting renames
     * are loud, never a silent pick. {@code inline} β-expands a runtime
     * helper call; {@code letPrefix} are the statement's preceding lets. */
    public static Map<String, String> reachableRenames(TypedSpec stmt,
            java.util.function.UnaryOperator<TypedSpec> letBound,
            java.util.function.UnaryOperator<TypedSpec> inline) {
        Map<String, String> out = new LinkedHashMap<>();
        walkExecutes(stmt, letBound, new java.util.HashSet<>(), inline, out);
        return out;
    }

    /** {@code letBound}: the caller's let chase (invariant 6h — the
     * lowering never reaches into the compiler's assembly). */
    private static void walkExecutes(TypedSpec n,
            java.util.function.UnaryOperator<TypedSpec> letBound,
            java.util.Set<String> seen,
            java.util.function.UnaryOperator<TypedSpec> inline,
            Map<String, String> out) {
        if (n instanceof TypedNativeCall ec
                && com.legend.builtin.NativeFn.Handle.isExecute(ec.callee().qualifiedName())
                && ec.args().size() >= 3) {
            TypedSpec rt = inline.apply(letBound.apply(ec.args().get(2)));
            for (var e : com.legend.compiler.spec.typed.ExecutionContext.reader()
                    .bind(letBound).read(java.util.Optional.empty(), rt)
                    .postProcessors().tableReplace().entrySet()) {
                String prev = out.putIfAbsent(e.getKey(), e.getValue());
                if (prev != null && !prev.equals(e.getValue())) {
                    throw new IllegalStateException("conflicting table renames"
                            + " for '" + e.getKey() + "': '" + prev + "' vs '"
                            + e.getValue() + "'");
                }
            }
        }
        if (n instanceof com.legend.compiler.spec.typed.TypedVariable tv
                && seen.add(tv.name())) {
            TypedSpec bound = letBound.apply(n);
            if (bound != n) {
                walkExecutes(bound, letBound, seen, inline, out);
            }
        }
        for (TypedSpec c : n.children()) {
            walkExecutes(c, letBound, seen, inline, out);
        }
    }

    // ===== the IR rewrite =====

    public static SqlQuery apply(SqlQuery q, Map<String, String> map) {
        if (map.isEmpty()) {
            return q;
        }
        return apply(q, (java.util.function.UnaryOperator<String>)
                n2 -> map.getOrDefault(n2, n2));
    }

    /** FUNCTION form — the relationalMapper channel resolves spellings
     * per-name (db identity may need model lookups). Identity output =
     * no rewrite. */
    public static SqlQuery apply(SqlQuery q,
            java.util.function.UnaryOperator<String> map) {
        return switch (q) {
            case SqlSelect s -> applySelect(s, map);
            case SqlUnion u -> new SqlUnion(u.branches().stream()
                    .map(b -> apply(b, map)).toList(), u.all(), u.outputs());
            case com.legend.sql.SqlWith w -> new com.legend.sql.SqlWith(
                    w.ctes().stream().map(c -> new com.legend.sql.SqlWith.Cte(
                            c.name(), apply(c.query(), map))).toList(),
                    apply(w.body(), map));
        };
    }

    /** The frame's recorded post-processing over a lowered query: the
     * table renames, then CTE extraction when the processor is installed
     * (the orchestrator supplies both facts — exec never calls here). */
    public static SqlQuery applyRecorded(SqlQuery q, Map<String, String> tableReplace,
            boolean extractCtes, boolean nonExecutable) {
        SqlQuery out = apply(q, tableReplace);
        out = nonExecutable ? nonExecutable(out) : out;
        return extractCtes ? extractSubqueriesAsCtes(out) : out;
    }

    // ---- nonExecutable (the engine's nonExecutablePostProcessor) ----

    /** {@code nonExecutable}: every SELECT in the tree — the root, each
     * FROM-tree subselect, each union branch, each CTE — takes
     * {@code <filter> and 1 = 2} (a bare {@code 1 = 2} when it had no
     * filter), so the query still parses and plans but returns no rows
     * (engine processRelationalOperationForNonExecutable). Join ON
     * conditions and projections are untouched. */
    public static SqlQuery nonExecutable(SqlQuery q) {
        return switch (q) {
            case SqlSelect s -> nonExecutableSelect(s);
            case com.legend.sql.SqlUnion u -> new com.legend.sql.SqlUnion(
                    u.branches().stream().map(SqlPostProcessors::nonExecutable).toList(),
                    u.all(), u.outputs());
            case com.legend.sql.SqlWith w -> new com.legend.sql.SqlWith(
                    w.ctes().stream().map(c -> new com.legend.sql.SqlWith.Cte(
                            c.name(), nonExecutable(c.query()))).toList(),
                    nonExecutable(w.body()));
            default -> q;
        };
    }

    private static SqlSelect nonExecutableSelect(SqlSelect s) {
        SqlExpr never = SqlExpr.Call.of(com.legend.sql.SqlFn.EQUAL,
                new SqlExpr.IntLit(1), new SqlExpr.IntLit(2));
        SqlExpr where = s.where() == null ? never
                : SqlExpr.Call.of(com.legend.sql.SqlFn.AND, s.where(), never);
        return new SqlSelect(s.projections(), s.distinct(),
                nonExecutableSource(s.from()), where, s.groupBy(), s.having(),
                s.qualify(), s.orderBy(), s.limit(), s.offset(), s.outputs());
    }

    private static SqlSource nonExecutableSource(SqlSource src) {
        return switch (src) {
            case SqlSource.Join j -> new SqlSource.Join(nonExecutableSource(j.left()),
                    nonExecutableSource(j.right()), j.kind(), j.on());
            case SqlSource.Subselect sub -> new SqlSource.Subselect(
                    nonExecutable(sub.inner()), sub.alias(), sub.frameName());
            default -> src;
        };
    }

    // ---- CTE extraction (the engine's cteExtractionPostProcessor) ----

    /** {@code extractSubqueriesAsCTEs}: every SUBSELECT in the FROM tree
     * (the join tree's derived tables) becomes a common table expression
     * {@code subquery_cte_<level>_<index>} — level = nesting depth from
     * the root (1 = the root's own subselects), index = a per-level
     * counter in tree order that carries across siblings; a subselect's
     * OWN subselects extract first (the child's CTEs precede the
     * parent's), and the reference keeps the derived table's alias. A
     * query without subselects is itself. */
    public static SqlQuery extractSubqueriesAsCtes(SqlQuery q) {
        if (!(q instanceof SqlSelect root)) {
            return q;
        }
        List<com.legend.sql.SqlWith.Cte> ctes = new java.util.ArrayList<>();
        java.util.Map<Integer, Integer> levelIndex = new java.util.HashMap<>();
        SqlSelect body = extractLevel(root, 1, levelIndex, ctes);
        return ctes.isEmpty() ? q : new com.legend.sql.SqlWith(ctes, body);
    }

    private static SqlSelect extractLevel(SqlSelect select, int level,
            java.util.Map<Integer, Integer> levelIndex,
            List<com.legend.sql.SqlWith.Cte> out) {
        SqlSource from = extractSource(select.from(), level, levelIndex, out);
        return from == select.from() ? select : new SqlSelect(select.projections(),
                select.distinct(), from, select.where(), select.groupBy(),
                select.having(), select.qualify(), select.orderBy(), select.limit(),
                select.offset(), select.outputs());
    }

    private static SqlSource extractSource(SqlSource src, int level,
            java.util.Map<Integer, Integer> levelIndex,
            List<com.legend.sql.SqlWith.Cte> out) {
        return switch (src) {
            case SqlSource.Join j -> {
                SqlSource l = extractSource(j.left(), level, levelIndex, out);
                SqlSource r = extractSource(j.right(), level, levelIndex, out);
                yield l == j.left() && r == j.right() ? j
                        : new SqlSource.Join(l, r, j.kind(), j.on());
            }
            case SqlSource.Subselect sub when sub.inner() instanceof SqlSelect inner -> {
                // the child's own subselects first (deeper CTEs precede)
                SqlSelect processed = extractLevel(inner, level + 1, levelIndex, out);
                int index = levelIndex.getOrDefault(level, 0) + 1;
                levelIndex.put(level, index);
                String name = "subquery_cte_" + level + "_" + index;
                out.add(new com.legend.sql.SqlWith.Cte(name, processed));
                yield new SqlSource.Table(name, sub.alias(), sub.outputs());
            }
            default -> src;
        };
    }

    private static SqlSelect applySelect(SqlSelect s,
            java.util.function.UnaryOperator<String> m) {
        return new SqlSelect(
                s.projections().stream().map(p -> new SqlSelect.Projection(
                        expr(p.expr(), m), p.outputName(), p.out())).toList(),
                s.distinct(),
                source(s.from(), m),
                s.where() == null ? null : expr(s.where(), m),
                s.groupBy().stream().map(g -> expr(g, m)).toList(),
                s.having() == null ? null : expr(s.having(), m),
                s.qualify() == null ? null : expr(s.qualify(), m),
                s.orderBy().stream().map(k -> new SqlSelect.SortKey(
                        expr(k.expr(), m), k.ascending(), k.nullOrder(),
                        k.outputName()))
                        .toList(),
                s.limit(), s.offset(), s.outputs());
    }

    private static SqlSource source(SqlSource src,
            java.util.function.UnaryOperator<String> m) {
        return switch (src) {
            case SqlSource.Table t -> {
                String nn = m.apply(t.name());
                yield nn.equals(t.name()) ? t
                        : new SqlSource.Table(nn, t.alias(), t.outputs());
            }
            // The name mapper applies to a function name too -- it is
            // the same kind of identifier, and a rename that skipped
            // functions would silently leave one pointing at the old
            // name.
            case SqlSource.TableFunction f -> {
                String nn = m.apply(f.name());
                yield nn.equals(f.name()) ? f
                        : new SqlSource.TableFunction(nn, f.arguments(),
                                f.alias(), f.outputs());
            }
            case SqlSource.Join j -> new SqlSource.Join(source(j.left(), m),
                    source(j.right(), m), j.kind(),
                    j.on() == null ? null : expr(j.on(), m));
            case SqlSource.VarSetPlaceholder vp -> vp;
            case SqlSource.RawSql raw -> raw;   // carried text: opaque to rewrites
            case SqlSource.Subselect sub -> new SqlSource.Subselect(
                    apply(sub.inner(), m), sub.alias(), sub.frameName());
            // TOTAL by construction — a Pivot's INNER source and a Values
            // row expression rename like any other (the old default arm
            // silently skipped both)
            case SqlSource.Pivot p -> new SqlSource.Pivot(source(p.source(), m),
                    p.on().stream().map(x -> expr(x, m)).toList(),
                    p.in().stream().map(x -> expr(x, m)).toList(),
                    p.usings().stream().map(u -> new SqlSource.Pivot.Using(
                            (com.legend.sql.SqlAgg.Reducer) expr(u.agg(), m),
                            u.alias(), u.type())).toList(),
                    p.alias(), p.outputs());
            case SqlSource.Values v -> new SqlSource.Values(
                    v.rows().stream().map(r -> r.stream()
                            .map(x -> expr(x, m)).toList()).toList(),
                    v.columns(), v.alias(), v.outputs());
            case SqlSource.SourceUrl u -> u;
            case SqlSource.Dual d -> d;
        };
    }

    private static SqlExpr expr(SqlExpr e,
            java.util.function.UnaryOperator<String> m) {
        return switch (e) {
            case SqlExpr.Call c -> new SqlExpr.Call(c.fn(),
                    c.args().stream().map(a -> expr(a, m)).toList());
            case SqlExpr.Case cs -> new SqlExpr.Case(
                    cs.whens().stream().map(w -> new SqlExpr.Case.When(
                            expr(w.condition(), m), expr(w.then(), m)))
                            .toList(),
                    cs.otherwise() == null ? null
                            : expr(cs.otherwise(), m));
            case SqlExpr.Cast ct -> new SqlExpr.Cast(expr(ct.value(), m),
                    ct.target());
            case SqlExpr.Group g -> new SqlExpr.Group(expr(g.inner(), m));
            case SqlExpr.Exists ex -> new SqlExpr.Exists(
                    apply(ex.subquery(), m));
            case SqlExpr.ScalarSubquery sq -> new SqlExpr.ScalarSubquery(
                    apply(sq.subquery(), m));
            // an AGGREGATE's arguments (the graph envelope's list(json_object(
            // …, (SELECT … FROM personTable …))) — its correlated child
            // subquery renames like any other; batch 80): a Reducer has no
            // expression children of its own, so the default arm skipped it
            case com.legend.sql.SqlAgg.Reducer r -> new com.legend.sql.SqlAgg.Reducer(
                    r.fn(), r.args().stream().map(a -> expr(a, m)).toList(),
                    r.distinct(),
                    r.orderBy().stream().map(k -> new SqlSelect.SortKey(
                            expr(k.expr(), m), k.ascending(), k.nullOrder(),
                            k.outputName())).toList());
            default -> e.mapChildren(x -> expr(x, m));
        };
    }
}
