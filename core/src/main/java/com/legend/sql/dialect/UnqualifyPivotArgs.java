package com.legend.sql.dialect;

import com.legend.sql.SqlAgg;
import com.legend.sql.SqlExpr;
import com.legend.sql.SqlRewriter;
import com.legend.sql.SqlSelect;
import com.legend.sql.SqlSource;

/**
 * DuckDB PIVOT forbids qualified column refs inside ON/USING (remediation
 * T3.2 step 3): the stripping is MIR&rarr;MIR here, not a render-time
 * rewrite inside {@code pivotSource}. The unqualify walk is the one moved
 * VERBATIM from the renderer &mdash; deliberately hand-walked, because its
 * stop rules differ from the generic traversal: subqueries own their
 * aliases and lambdas bind their own params, so it does NOT descend into
 * them (audit 15: exhaustive, no default).
 */
final class UnqualifyPivotArgs extends SqlRewriter {

    @Override
    protected SqlSource source(SqlSource s) {
        if (!(s instanceof SqlSource.Pivot p)) {
            return s;
        }
        return new SqlSource.Pivot(p.source(),
                p.on().stream().map(UnqualifyPivotArgs::unqualify).toList(),
                p.in(),
                p.usings().stream().map(u -> new SqlSource.Pivot.Using(
                        (SqlAgg.Reducer) unqualify(u.agg()), u.alias(), u.type())).toList(),
                p.alias(), p.outputs());
    }

    private static SqlExpr unqualify(SqlExpr e) {
        return switch (e) {
            case SqlExpr.TempTableInSplice t -> t;
            case SqlExpr.Column c -> new SqlExpr.Column(null, c.name(), c.type(), c.origin());
            case SqlExpr.RowOrder ignored -> new SqlExpr.RowOrder(null);
            case SqlExpr.ReduceCollection rc -> rc;
            case SqlExpr.Membership m2 -> m2;
            case SqlExpr.Call c -> new SqlExpr.Call(c.fn(),
                    c.args().stream().map(UnqualifyPivotArgs::unqualify).toList());
            case SqlExpr.Cast c -> new SqlExpr.Cast(unqualify(c.value()),
                    c.target(), c.conform());
            case SqlExpr.Case c -> new SqlExpr.Case(
                    c.whens().stream().map(w -> new SqlExpr.Case.When(
                            unqualify(w.condition()), unqualify(w.then()))).toList(),
                    c.otherwise() == null ? null : unqualify(c.otherwise()));
            case SqlExpr.ArrayLit a -> new SqlExpr.ArrayLit(
                    a.elements().stream().map(UnqualifyPivotArgs::unqualify).toList());
            case SqlExpr.StructGet g ->
                    new SqlExpr.StructGet(unqualify(g.source()), g.field());
            case SqlExpr.StructLit sl -> new SqlExpr.StructLit(
                    sl.fields().stream().map(f -> new SqlExpr.StructLit.Field(
                            f.name(), unqualify(f.value()),
                            f.declared())).toList());
            case SqlExpr.OrderedListAgg o -> new SqlExpr.OrderedListAgg(
                    unqualify(o.value()), unqualify(o.orderBy()));
            case SqlExpr.JsonObject j -> new SqlExpr.JsonObject(
                    j.kv().stream().map(UnqualifyPivotArgs::unqualify).toList());
            case SqlExpr.JsonArray j -> new SqlExpr.JsonArray(
                    j.elements().stream().map(UnqualifyPivotArgs::unqualify).toList());
            case SqlExpr.JsonArrayAgg ja ->
                    new SqlExpr.JsonArrayAgg(unqualify(ja.value()),
                            ja.orderKeys().stream()
                                    .map(k -> new SqlExpr.JsonArrayAgg.Key(
                                            unqualify(k.expr()), k.desc()))
                                    .toList());
            case SqlAgg.Reducer r -> new SqlAgg.Reducer(r.fn(),
                    r.args().stream().map(UnqualifyPivotArgs::unqualify).toList(),
                    r.distinct(),
                    r.orderBy().stream().map(k -> new SqlSelect.SortKey(
                            unqualify(k.expr()), k.ascending(), k.nullOrder(),
                            k.outputName())).toList());
            case SqlExpr.FoldCall f -> new SqlExpr.FoldCall(
                    unqualify(f.source()), f.lambda(), unqualify(f.init()),
                    f.accIsList(), f.homogeneous());
            case SqlExpr.WindowCall w -> new SqlExpr.WindowCall(w.fn(),
                    w.partitionBy().stream().map(UnqualifyPivotArgs::unqualify).toList(),
                    w.orderBy(), w.frame());
            // subqueries own their aliases; lambdas bind their own params;
            // leaves carry no qualifier (audit 15: exhaustive, no default)
            case SqlExpr.Exists x -> x;
            case SqlExpr.ScalarSubquery ss -> ss;
            case SqlExpr.InSubquery i -> i;
            case SqlExpr.Quantified q -> q;
            case SqlExpr.CheckedOne co -> co;
            case SqlExpr.CompactList cl -> cl;
            case SqlExpr.DeferredTdsString dtds -> dtds;
            case SqlExpr.Lambda l -> l;
            case SqlExpr.Star st -> st;
            case SqlExpr.StarExcept se -> se;
            case SqlExpr.Group g -> new SqlExpr.Group(unqualify(g.inner()));
            case SqlExpr.PlanParam ignored -> e;
            case SqlExpr.StringLit ignored -> e;
            case SqlExpr.IntLit ignored -> e;
            case SqlExpr.FloatLit ignored -> e;
            case SqlExpr.DecimalLit ignored -> e;
            case SqlExpr.BoolLit ignored -> e;
            case SqlExpr.NullLit ignored -> e;
            case SqlExpr.DateLit ignored -> e;
            case SqlExpr.TimestampLit ignored -> e;
            case SqlExpr.FormatLit fml -> e;
        };
    }
}
