package com.legend.sql;

import java.util.ArrayList;
import java.util.List;

/**
 * Structural MIR&rarr;MIR rewriter (remediation T3.2 step 3) &mdash; the SQL
 * layer's analog of {@code TypedSpec.mapChildren}: a bottom-up deep map with
 * identity preservation, so a pass overrides only the node hooks it cares
 * about and can never drop a field by hand-rebuilding. Hooks fire AFTER the
 * node's children were rewritten; the default hook is the identity.
 *
 * <p>This is the ONE traversal dialect passes share (the hand-walked
 * {@code SubselectPrune} predates it). Renderers run passes at
 * {@code render()} entry &mdash; rewrites belong here, never inside render
 * methods.
 */
public abstract class SqlRewriter {

    // ---- hooks (post-children; default identity) ----

    protected SqlQuery select(SqlSelect s) {
        return s;
    }

    protected SqlQuery union(SqlUnion u) {
        return u;
    }

    protected SqlSource source(SqlSource s) {
        return s;
    }

    protected SqlExpr expr(SqlExpr e) {
        return e;
    }

    /** Entry point for the RENDERER'S pass loop: sees the statement
     * ROOT before the deep walk — the one place a root-only rule
     * (clause of the outermost select) can live inside the pass
     * framework. Default: the plain deep rewrite. */
    public SqlQuery rewriteRoot(SqlQuery q) {
        return rewrite(q);
    }

    // ---- the walk ----

    public final SqlQuery rewrite(SqlQuery q) {
        return switch (q) {
            case SqlSelect s -> {
                List<SqlSelect.Projection> ps = mapList(s.projections(), p -> {
                    SqlExpr e2 = rewriteExpr(p.expr());
                    return e2 == p.expr() ? p
                            : new SqlSelect.Projection(e2, p.alias(), p.out());
                });
                SqlSource f = rewriteSource(s.from());
                SqlExpr w = s.where() == null ? null : rewriteExpr(s.where());
                List<SqlExpr> g = mapList(s.groupBy(), this::rewriteExpr);
                SqlExpr h = s.having() == null ? null : rewriteExpr(s.having());
                SqlExpr ql = s.qualify() == null ? null : rewriteExpr(s.qualify());
                List<SqlSelect.SortKey> ob = mapList(s.orderBy(), k -> {
                    SqlExpr e2 = rewriteExpr(k.expr());
                    return e2 == k.expr() ? k : new SqlSelect.SortKey(
                            e2, k.ascending(), k.nullOrder(), k.outputName());
                });
                SqlSelect out = ps == s.projections() && f == s.from()
                        && w == s.where() && g == s.groupBy() && h == s.having()
                        && ql == s.qualify() && ob == s.orderBy()
                        ? s
                        : new SqlSelect(ps, s.distinct(), f, w, g, h, ql, ob,
                                s.limit(), s.offset(), s.outputs());
                yield select(out);
            }
            case SqlUnion u -> {
                List<SqlQuery> bs = mapList(u.branches(), this::rewrite);
                yield union(bs == u.branches() ? u
                        : new SqlUnion(bs, u.all(), u.outputs()));
            }
            case SqlWith w -> {
                List<SqlWith.Cte> cs = mapList(w.ctes(), c -> {
                    SqlQuery q2 = rewrite(c.query());
                    return q2 == c.query() ? c : new SqlWith.Cte(c.name(), q2);
                });
                SqlQuery b2 = rewrite(w.body());
                yield cs == w.ctes() && b2 == w.body() ? w : new SqlWith(cs, b2);
            }
        };
    }

    protected final SqlSource rewriteSource(SqlSource s) {
        SqlSource out = switch (s) {
            case SqlSource.Dual d -> d;
            case SqlSource.Table t -> t;
            case SqlSource.SourceUrl u -> u;
            case SqlSource.VarSetPlaceholder vp -> vp;
            case SqlSource.RawSql r -> r;   // carried text: a leaf
            case SqlSource.Subselect sub -> {
                SqlQuery i = rewrite(sub.inner());
                yield i == sub.inner() ? sub
                        : new SqlSource.Subselect(i, sub.alias(), sub.frameName());
            }
            case SqlSource.Values v -> {
                List<List<SqlExpr>> rows = mapList(v.rows(),
                        r -> mapList(r, this::rewriteExpr));
                yield rows == v.rows() ? v
                        : new SqlSource.Values(rows, v.columns(), v.alias(),
                                v.outputs());
            }
            case SqlSource.Join j -> {
                SqlSource l = rewriteSource(j.left());
                SqlSource r = rewriteSource(j.right());
                SqlExpr on = j.on() == null ? null : rewriteExpr(j.on());
                yield l == j.left() && r == j.right() && on == j.on() ? j
                        : new SqlSource.Join(l, r, j.kind(), on);
            }
            case SqlSource.Pivot p -> {
                SqlSource src = rewriteSource(p.source());
                List<SqlExpr> on = mapList(p.on(), this::rewriteExpr);
                List<SqlExpr> in = mapList(p.in(), this::rewriteExpr);
                List<SqlSource.Pivot.Using> us = mapList(p.usings(), u -> {
                    SqlAgg.Reducer r2 = (SqlAgg.Reducer) rewriteExpr(u.agg());
                    return r2 == u.agg() ? u
                            : new SqlSource.Pivot.Using(r2, u.alias(), u.type());
                });
                yield src == p.source() && on == p.on() && in == p.in()
                        && us == p.usings() ? p
                        : new SqlSource.Pivot(src, on, in, us, p.alias(), p.outputs());
            }
        };
        return source(out);
    }

    protected final SqlExpr rewriteExpr(SqlExpr e) {
        SqlExpr out = switch (e) {
            case SqlExpr.Column c -> c;
            case SqlExpr.RowOrder r -> r;
            case SqlExpr.TempTableInSplice t -> t;
            case SqlExpr.Membership m -> {
                SqlExpr n2 = rewriteExpr(m.needle());
                SqlExpr c2 = rewriteExpr(m.collection());
                yield n2 == m.needle() && c2 == m.collection() ? m
                        : new SqlExpr.Membership(n2, c2);
            }
            case SqlExpr.ReduceCollection rc -> {
                SqlExpr col = rewriteExpr(rc.collection());
                java.util.List<SqlExpr> ex = mapList(rc.extras(),
                        this::rewriteExpr);
                yield col == rc.collection() && ex == rc.extras() ? rc
                        : new SqlExpr.ReduceCollection(rc.reducer(), col, ex);
            }
            case SqlExpr.Star st -> st;
            case SqlExpr.StarExcept se -> se;
            case SqlExpr.StringLit l -> l;
            case SqlExpr.IntLit l -> l;
            case SqlExpr.FloatLit l -> l;
            case SqlExpr.DecimalLit l -> l;
            case SqlExpr.BoolLit l -> l;
            case SqlExpr.NullLit l -> l;
            case SqlExpr.DateLit l -> l;
            case SqlExpr.TimestampLit l -> l;
            case SqlExpr.FormatLit l -> l;
            case SqlExpr.PlanParam p -> p;
            case SqlExpr.Group g -> {
                SqlExpr i = rewriteExpr(g.inner());
                yield i == g.inner() ? g : new SqlExpr.Group(i);
            }
            case SqlExpr.OrderedListAgg o -> {
                SqlExpr v = rewriteExpr(o.value());
                SqlExpr ob = rewriteExpr(o.orderBy());
                yield v == o.value() && ob == o.orderBy() ? o
                        : new SqlExpr.OrderedListAgg(v, ob);
            }
            case SqlExpr.ArrayLit a -> {
                List<SqlExpr> es = mapList(a.elements(), this::rewriteExpr);
                yield es == a.elements() ? a : new SqlExpr.ArrayLit(es);
            }
            case SqlExpr.StructLit sl -> {
                List<SqlExpr.StructLit.Field> fs = mapList(sl.fields(), fl -> {
                    SqlExpr v = rewriteExpr(fl.value());
                    return v == fl.value() ? fl
                            : new SqlExpr.StructLit.Field(fl.name(), v,
                                    fl.declared());
                });
                yield fs == sl.fields() ? sl : new SqlExpr.StructLit(fs);
            }
            case SqlExpr.StructGet sg -> {
                SqlExpr src = rewriteExpr(sg.source());
                yield src == sg.source() ? sg
                        : new SqlExpr.StructGet(src, sg.field());
            }
            case SqlExpr.Call c -> {
                List<SqlExpr> as = mapList(c.args(), this::rewriteExpr);
                yield as == c.args() ? c : new SqlExpr.Call(c.fn(), as);
            }
            case SqlExpr.Case cs -> {
                List<SqlExpr.Case.When> ws = mapList(cs.whens(), wn -> {
                    SqlExpr cd = rewriteExpr(wn.condition());
                    SqlExpr th = rewriteExpr(wn.then());
                    return cd == wn.condition() && th == wn.then() ? wn
                            : new SqlExpr.Case.When(cd, th);
                });
                SqlExpr ot = cs.otherwise() == null ? null
                        : rewriteExpr(cs.otherwise());
                yield ws == cs.whens() && ot == cs.otherwise() ? cs
                        : new SqlExpr.Case(ws, ot);
            }
            case SqlExpr.Exists ex -> {
                SqlQuery sub = rewrite(ex.subquery());
                yield sub == ex.subquery() ? ex : new SqlExpr.Exists(sub);
            }
            case SqlExpr.InSubquery i -> {
                SqlExpr v = rewriteExpr(i.value());
                SqlQuery sub = rewrite(i.subquery());
                yield v == i.value() && sub == i.subquery() ? i : new SqlExpr.InSubquery(v, sub);
            }
            case SqlExpr.Quantified q -> {
                SqlExpr v = rewriteExpr(q.value());
                SqlQuery sub = rewrite(q.subquery());
                yield v == q.value() && sub == q.subquery() ? q
                        : new SqlExpr.Quantified(v, q.comparison(), q.quantifier(), sub);
            }
            // rewriteExpr, NOT the expr() hook: the hook alone is a
            // SHALLOW visit — nothing under the wrapper gets walked, so
            // dialect passes (SubstringClamp) never reach nested calls.
            // The CompactList copy of the old expr() spelling cost a PCT
            // regression (removeDuplicatesBy: the un-clamped substr) —
            // and the CheckedOne original had the same latent hole.
            case SqlExpr.CheckedOne co -> {
                SqlExpr in = rewriteExpr(co.list());
                yield in == co.list() ? co : new SqlExpr.CheckedOne(in,
                        co.scalarCarrier(), co.atLeastOnly());
            }
            case SqlExpr.CompactList cl -> {
                SqlExpr in = rewriteExpr(cl.list());
                yield in == cl.list() ? cl : new SqlExpr.CompactList(in);
            }
            case SqlExpr.ScalarSubquery sq -> {
                SqlQuery sub = rewrite(sq.subquery());
                yield sub == sq.subquery() ? sq
                        : new SqlExpr.ScalarSubquery(sub);
            }
            case SqlExpr.DeferredTdsString d -> {
                SqlQuery sub = rewrite(d.inner());
                yield sub == d.inner() ? d
                        : new SqlExpr.DeferredTdsString((SqlSelect) sub,
                                d.alias(), d.id(), d.form(), d.renderTdsNull());
            }
            case SqlExpr.JsonObject j -> {
                List<SqlExpr> kv = mapList(j.kv(), this::rewriteExpr);
                yield kv == j.kv() ? j : new SqlExpr.JsonObject(kv);
            }
            case SqlExpr.JsonArray j -> {
                List<SqlExpr> es = mapList(j.elements(), this::rewriteExpr);
                yield es == j.elements() ? j : new SqlExpr.JsonArray(es);
            }
            case SqlExpr.JsonArrayAgg ja -> {
                SqlExpr v = rewriteExpr(ja.value());
                List<SqlExpr.JsonArrayAgg.Key> ks = mapList(ja.orderKeys(), k -> {
                    SqlExpr e2 = rewriteExpr(k.expr());
                    return e2 == k.expr() ? k
                            : new SqlExpr.JsonArrayAgg.Key(e2, k.desc());
                });
                yield v == ja.value() && ks == ja.orderKeys() ? ja
                        : new SqlExpr.JsonArrayAgg(v, ks);
            }
            case SqlExpr.WindowCall w -> {
                SqlAgg fn2 = rewriteAgg(w.fn());
                List<SqlExpr> pb = mapList(w.partitionBy(), this::rewriteExpr);
                List<SqlSelect.SortKey> ob = mapList(w.orderBy(), k -> {
                    SqlExpr e2 = rewriteExpr(k.expr());
                    return e2 == k.expr() ? k : new SqlSelect.SortKey(
                            e2, k.ascending(), k.nullOrder(), k.outputName());
                });
                yield fn2 == w.fn() && pb == w.partitionBy() && ob == w.orderBy()
                        ? w : new SqlExpr.WindowCall(fn2, pb, ob, w.frame());
            }
            case SqlExpr.Lambda l -> {
                SqlExpr b = rewriteExpr(l.body());
                yield b == l.body() ? l : new SqlExpr.Lambda(l.params(), b);
            }
            case SqlExpr.Cast c -> {
                SqlExpr v = rewriteExpr(c.value());
                yield v == c.value() ? c
                        : new SqlExpr.Cast(v, c.target(), c.conform());
            }
            case SqlExpr.FoldCall f -> {
                SqlExpr src = rewriteExpr(f.source());
                SqlExpr.Lambda lm = (SqlExpr.Lambda) rewriteExpr(f.lambda());
                SqlExpr in = rewriteExpr(f.init());
                yield src == f.source() && lm == f.lambda() && in == f.init()
                        ? f : new SqlExpr.FoldCall(src, lm, in, f.accIsList(),
                                f.homogeneous());
            }
            case SqlAgg.Reducer r -> (SqlExpr) rewriteAgg(r);
        };
        return expr(out);
    }

    /** SqlAgg variants (Reducer doubles as an SqlExpr). */
    protected final SqlAgg rewriteAgg(SqlAgg a) {
        return switch (a) {
            case SqlAgg.Reducer r -> {
                List<SqlExpr> as = mapList(r.args(), this::rewriteExpr);
                List<SqlSelect.SortKey> ob = mapList(r.orderBy(), k -> {
                    SqlExpr e2 = rewriteExpr(k.expr());
                    return e2 == k.expr() ? k : new SqlSelect.SortKey(
                            e2, k.ascending(), k.nullOrder(), k.outputName());
                });
                yield as == r.args() && ob == r.orderBy() ? r
                        : new SqlAgg.Reducer(r.fn(), as, r.distinct(), ob);
            }
            case SqlAgg.RankingFn rf -> {
                List<SqlExpr> as = mapList(rf.args(), this::rewriteExpr);
                yield as == rf.args() ? rf : new SqlAgg.RankingFn(rf.fn(), as);
            }
            case SqlAgg.ValueFn vf -> {
                List<SqlExpr> as = mapList(vf.args(), this::rewriteExpr);
                yield as == vf.args() ? vf : new SqlAgg.ValueFn(vf.fn(), as);
            }
        };
    }

    /** Element-wise map preserving LIST identity when nothing changed. */
    private static <T> List<T> mapList(List<T> xs,
            java.util.function.UnaryOperator<T> f) {
        List<T> out = null;
        for (int i = 0; i < xs.size(); i++) {
            T y = f.apply(xs.get(i));
            if (out == null && y != xs.get(i)) {
                out = new ArrayList<>(xs.subList(0, i));
            }
            if (out != null) {
                out.add(y);
            }
        }
        return out == null ? xs : out;
    }
}
