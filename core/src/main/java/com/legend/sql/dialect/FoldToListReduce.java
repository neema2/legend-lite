package com.legend.sql.dialect;

import com.legend.sql.SqlExpr;
import com.legend.sql.SqlFn;
import com.legend.sql.SqlRewriter;

import java.util.List;

/**
 * DuckDB's fold encoding as a NAMED MIR pass (remediation T3.2 step 3):
 * {@code SqlExpr.FoldCall} becomes a plain {@code LIST_REDUCE} call tree
 * before the writer runs — the render layer no longer synthesizes IR.
 * Pure's lambda is {@code (element, acc)}; DuckDB's is {@code (acc,
 * element)} — the parameters SWAP here, at the one place that knows
 * DuckDB's convention. {@code list_reduce} demands init type == list
 * child type: a LIST accumulator wraps each element as a single-item
 * list and unwraps refs in the body (master's Path 4); a scalar
 * accumulator over a non-decomposed body cannot be encoded — DuckDB's
 * limitation, stated LOUDLY by DuckDB's pass.
 */
final class FoldToListReduce extends SqlRewriter {

    @Override
    protected SqlExpr expr(SqlExpr e) {
        if (!(e instanceof SqlExpr.FoldCall f)) {
            return e;
        }
        String elem = f.lambda().params().get(0);
        String acc = f.lambda().params().get(1);
        if (!f.accIsList()) {
            if (!f.homogeneous()) {
                throw new IllegalStateException("fold body is not decomposable and the"
                        + " accumulator is scalar — rewrite accumulator-first"
                        + " ({e, a | $a <op> ...}) so the reduction can decompose");
            }
            SqlExpr.Lambda swapped = new SqlExpr.Lambda(List.of(acc, elem), f.lambda().body());
            // fold over the EMPTY (SQL NULL) collection is the INIT value —
            // list_reduce(NULL, ...) is NULL
            return SqlExpr.Call.of(SqlFn.LIST_REDUCE,
                    SqlExpr.Call.of(SqlFn.COALESCE, f.source(),
                            new SqlExpr.ArrayLit(List.of())),
                    swapped, f.init());
        }
        // List accumulator: wrap elements as single-item lists ([e] — the
        // semantic ArrayLit), unwrap refs via LIST_GET(e, 1).
        SqlExpr wrapped = new SqlExpr.Call(SqlFn.LIST_TRANSFORM, List.of(f.source(),
                new SqlExpr.Lambda(List.of(elem),
                        new SqlExpr.ArrayLit(List.of(SqlExpr.Column.derived(null, elem))))));
        SqlExpr body = unwrapElemRefs(f.lambda().body(), elem);
        SqlExpr.Lambda swapped = new SqlExpr.Lambda(List.of(acc, elem), body);
        return SqlExpr.Call.of(SqlFn.LIST_REDUCE, wrapped, swapped, f.init());
    }

    /**
     * Replace bare refs to {@code elem} with {@code LIST_GET(elem, 1)} —
     * EXHAUSTIVE over the expression tree (a ref nested under a cast, case,
     * array, or inner lambda must unwrap too; javac keeps this honest).
     */
    private static SqlExpr unwrapElemRefs(SqlExpr e, String elem) {
        return switch (e) {
            case SqlExpr.TempTableInSplice t -> t;
            case SqlExpr.Column c when c.table() == null && elem.equals(c.name()) ->
                    SqlExpr.Call.of(SqlFn.LIST_GET,
                            SqlExpr.Column.derived(null, elem), new SqlExpr.IntLit(1));
            case SqlExpr.Column c -> c;
            case SqlExpr.RowOrder r2 -> r2;
            case SqlExpr.ReduceCollection rc -> rc;
            case SqlExpr.Membership m2 -> m2;
            case SqlExpr.StarExcept se -> se;
            case SqlExpr.OrderedListAgg ola -> ola;   // no elem refs inside
            case SqlExpr.Call call -> new SqlExpr.Call(call.fn(),
                    call.args().stream().map(x -> unwrapElemRefs(x, elem)).toList());
            case SqlExpr.Cast c -> new SqlExpr.Cast(unwrapElemRefs(c.value(), elem),
                    c.target());
            case SqlExpr.ArrayLit a -> new SqlExpr.ArrayLit(
                    a.elements().stream().map(x -> unwrapElemRefs(x, elem)).toList());
            case SqlExpr.Case cs -> new SqlExpr.Case(
                    cs.whens().stream().map(w -> new SqlExpr.Case.When(
                            unwrapElemRefs(w.condition(), elem),
                            unwrapElemRefs(w.then(), elem))).toList(),
                    cs.otherwise() == null ? null : unwrapElemRefs(cs.otherwise(), elem));
            case SqlExpr.StructLit s -> new SqlExpr.StructLit(s.fields().stream()
                    .map(fld -> new SqlExpr.StructLit.Field(fld.name(),
                            unwrapElemRefs(fld.value(), elem),
                            fld.declared())).toList());
            case SqlExpr.StructGet g -> new SqlExpr.StructGet(
                    unwrapElemRefs(g.source(), elem), g.field());
            case SqlExpr.Lambda l -> l.params().contains(elem)
                    ? l   // inner lambda shadows the element name
                    : new SqlExpr.Lambda(l.params(), unwrapElemRefs(l.body(), elem));
            case SqlExpr.FoldCall f -> new SqlExpr.FoldCall(
                    unwrapElemRefs(f.source(), elem),
                    f.lambda().params().contains(elem) ? f.lambda()
                            : new SqlExpr.Lambda(f.lambda().params(),
                                    unwrapElemRefs(f.lambda().body(), elem)),
                    unwrapElemRefs(f.init(), elem), f.accIsList(), f.homogeneous());
            // Leaves and structures that cannot contain the element ref:
            case SqlExpr.Star st -> st;
            case SqlExpr.Group g ->
                    new SqlExpr.Group(unwrapElemRefs(g.inner(), elem));
            case SqlExpr.PlanParam v -> v;
            case SqlExpr.StringLit v -> v;
            case SqlExpr.IntLit v -> v;
            case SqlExpr.FloatLit v -> v;
            case SqlExpr.DecimalLit v -> v;
            case SqlExpr.BoolLit v -> v;
            case SqlExpr.NullLit v -> v;
            case SqlExpr.DateLit v -> v;
            case SqlExpr.TimestampLit v -> v;
            case SqlExpr.FormatLit v -> v;
            // KNOWN HOLE (audit of Blockers 1+2): a correlated subquery
            // inside a fold body could reference the element — these two
            // arms do not rewrite it (pre-existing; the fold recognizer
            // does not currently produce the combination).
            case SqlExpr.Exists x -> x;
            case SqlExpr.ScalarSubquery x -> x;
            case SqlExpr.InSubquery x -> x;
            case SqlExpr.Quantified x -> x;
            // guards RECURSE — the audit found these two shallow (same
            // class as SqlRewriter's CheckedOne arm, fixed b0a163af):
            // an elem ref under a value-lane toOne guard inside a fold
            // body must unwrap like any other ref.
            case SqlExpr.CheckedOne co -> new SqlExpr.CheckedOne(
                    unwrapElemRefs(co.list(), elem),
                    co.scalarCarrier(), co.atLeastOnly());
            case SqlExpr.CompactList cl -> new SqlExpr.CompactList(
                    unwrapElemRefs(cl.list(), elem));
            case SqlExpr.DeferredTdsString dtds -> dtds;
            case SqlExpr.WindowCall w -> w;
            // JSON envelope nodes never appear inside fold bodies (the
            // serialize envelope is a projection-level construct).
            case SqlExpr.JsonObject j -> j;
            case SqlExpr.JsonArray j -> j;
            case SqlExpr.JsonArrayAgg j -> j;
            case com.legend.sql.SqlAgg.Reducer r -> r;
        };
    }
}
