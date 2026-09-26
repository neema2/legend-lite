// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.lowering;

import com.legend.compiler.element.type.Multiplicity;
import com.legend.compiler.element.type.Type;
import com.legend.sql.OutputCol;
import com.legend.sql.SqlExpr;
import com.legend.sql.SqlQuery;
import com.legend.sql.SqlSelect;
import com.legend.sql.SqlSource;
import com.legend.sql.SqlType;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * E1 (JAVA_EVICTION_PLAN): compose the PCT wire-render wrapper around an
 * already-staticized plan — the PLAN emits the wire TDS text
 * ({@link Render#pctTds}). Pure SQL composition (no connection): the
 * caller passes the LIMIT-0 metadata probe's result for pivot plans,
 * whose concrete output columns only exist post-staticize.
 *
 * <p>Pivot column TYPES come from the plan's own TEMPLATE outputs
 * (typed at lowering — backend-independent), never from the probe's
 * aggregate type: the G7 H2 ledger caught the fallback typing SUM
 * columns Decimal on H2 and Integer on DuckDB.
 */
public final class PctTdsWrap {

    private PctTdsWrap() {
    }

    public static SqlQuery wrap(SqlQuery plan, Type.RelationType schema,
            com.legend.sql.@com.legend.base.Nullable PlanProbe probe,
            java.util.function.Function<String, Type> sqlTypeToPure) {
        List<SqlSelect.SortKey> lifted = List.of();
        SqlQuery inner0 = plan;
        if (plan instanceof SqlSelect ps && !ps.orderBy().isEmpty()) {
            lifted = ps.orderBy();
            inner0 = ps.withOrderBy(List.of());
        }
        List<OutputCol> outs = probe != null ? probe.outs()
                : inner0.outputs();
        List<Type.Column> typedCols = typedColumns(outs, schema, plan,
                probe, sqlTypeToPure);
        String alias = "_pct0";
        Lift lift = liftOrder(lifted, outs, inner0, alias);
        inner0 = lift.inner0();
        outs = lift.outs();
        SqlSelect inner = SqlSelect.starOf(
                        new SqlSource.Subselect(inner0, alias, null))
                .withOutputs(outs)
                .withOrderBy(lift.remapped());
        return Render.pctTds(inner, typedCols, "_pct1");
    }

    /** Per-output pure types: static schema by name → the plan's pivot
     * TEMPLATE outputs (quote-stripped separator tail) → the probe's
     * JDBC type name. */
    private static List<Type.Column> typedColumns(List<OutputCol> outs,
            Type.RelationType schema, SqlQuery plan,
            com.legend.sql.@com.legend.base.Nullable PlanProbe probe,
            java.util.function.Function<String, Type> sqlTypeToPure) {
        Map<String, Type.Column> byName = new LinkedHashMap<>();
        for (Type.Column c : schema.columns()) {
            byName.put(c.name(), c);
        }
        // NO name parsing: the pivot's USING aliases are a CLOSED
        // vocabulary our own lowering minted (the concrete column is
        // <value> + <using alias>, e.g. '2011' + '_|__newCol'), and the
        // plan's template outputs carry their LOWERING-typed slots —
        // backend-independent. Decode = endsWith against that
        // vocabulary (the quote-bearing identity wrapper stripped);
        // a pivot-plan column matching nothing is a loud wall, never a
        // backend-typed guess (the G7 H2 ledger caught exactly that:
        // SUM columns typed Decimal on H2, Integer on DuckDB).
        List<SqlSource.Pivot.Using> templates = new ArrayList<>();
        for (SqlSource.Pivot p : pivots(plan)) {
            templates.addAll(p.usings());
        }
        List<Type.Column> typedCols = new ArrayList<>(outs.size());
        for (OutputCol oc : outs) {
            Type.Column st = byName.get(oc.name());
            if (st != null) {
                typedCols.add(st);
                continue;
            }
            String bare = stripIdentityQuotes(oc.name());
            List<SqlSource.Pivot.Using> hits = templates.stream()
                    .filter(t -> bare.endsWith(t.alias())).toList();
            if (hits.size() == 1) {
                typedCols.add(new Type.Column(oc.name(),
                        sqlTypeToPure.apply(slotName(hits.get(0).type())),
                        Multiplicity.Bounded.ONE));
                continue;
            }
            if (!templates.isEmpty()) {
                throw new IllegalStateException("PCT render: pivot column '"
                        + oc.name() + "' matches " + hits.size()
                        + " using aliases " + templates.stream()
                                .map(SqlSource.Pivot.Using::alias).toList()
                        + " — the minted-name vocabulary must decode"
                        + " exactly one");
            }
            typedCols.add(new Type.Column(oc.name(),
                    sqlTypeToPure.apply(probe != null
                            ? probe.typeNames().getOrDefault(oc.name(),
                                    slotName(oc.type()))
                            : slotName(oc.type())),
                    Multiplicity.Bounded.ONE));
        }
        return typedCols;
    }

    /** The quote-bearing pivot IDENTITY wrapper ({@code '2011__|__c'})
     * stripped — one balanced outer pair only, never inner quotes. */
    private static String stripIdentityQuotes(String name) {
        return name.length() > 1 && name.startsWith("'") && name.endsWith("'")
                ? name.substring(1, name.length() - 1) : name;
    }

    private record Lift(SqlQuery inner0, List<OutputCol> outs,
            List<SqlSelect.SortKey> remapped) {
    }

    /** Lift a top-level ORDER BY onto the wrapper: output-column keys
     * remap; a key the projection dropped (extend→sort→project) or an
     * expression key carries out as a HIDDEN {@code _pct_ord}
     * projection — it still scopes inside the plan's own select. */
    private static Lift liftOrder(List<SqlSelect.SortKey> lifted,
            List<OutputCol> outs, SqlQuery inner0, String alias) {
        java.util.Set<String> outNames = new java.util.LinkedHashSet<>();
        outs.forEach(o -> outNames.add(o.name()));
        List<SqlSelect.SortKey> remapped = new ArrayList<>(lifted.size());
        List<SqlSelect.Projection> hiddenProjs = new ArrayList<>();
        List<OutputCol> hiddenOuts = new ArrayList<>();
        int ord = 0;
        for (SqlSelect.SortKey k : lifted) {
            if (k.expr() instanceof SqlExpr.Column kc
                    && outNames.contains(kc.name())) {
                remapped.add(new SqlSelect.SortKey(
                        SqlExpr.Column.derived(alias, kc.name()), k.ascending(),
                        k.nullOrder(), k.outputName()));
                continue;
            }
            if (!(inner0 instanceof SqlSelect ps2)
                    || ps2.projections().isEmpty()) {
                throw new com.legend.error.NotImplementedException(
                        "PCT render: a non-output sort key over a"
                        + " star-projected plan cannot hoist");
            }
            String hn = "_pct_ord" + ord++;
            hiddenProjs.add(new SqlSelect.Projection(k.expr(), hn,
                    new OutputCol(hn, SqlType.Scalar.VARCHAR, true)));
            hiddenOuts.add(new OutputCol(hn, SqlType.Scalar.VARCHAR, true));
            remapped.add(new SqlSelect.SortKey(
                    SqlExpr.Column.derived(alias, hn), k.ascending(),
                    k.nullOrder(), null));
        }
        if (hiddenProjs.isEmpty()) {
            return new Lift(inner0, outs, remapped);
        }
        SqlSelect ps2 = (SqlSelect) inner0;
        List<SqlSelect.Projection> allProjs =
                new ArrayList<>(ps2.projections());
        allProjs.addAll(hiddenProjs);
        List<OutputCol> outs2 = new ArrayList<>(outs);
        outs2.addAll(hiddenOuts);
        return new Lift(ps2.withProjections(allProjs), outs2,
                remapped);
    }

    /** Every {@code Pivot} source in the plan. */
    public static List<SqlSource.Pivot> pivots(SqlQuery q) {
        List<SqlSource.Pivot> out = new ArrayList<>();
        java.util.ArrayDeque<Object> work = new java.util.ArrayDeque<>();
        work.add(q);
        while (!work.isEmpty()) {
            Object x = work.poll();
            if (x instanceof SqlSelect s) {
                work.add(s.from());
            } else if (x instanceof com.legend.sql.SqlUnion u) {
                work.addAll(u.branches());
            } else if (x instanceof SqlSource.Pivot p) {
                out.add(p);
                work.add(p.source());
            } else if (x instanceof SqlSource.Subselect sub) {
                work.add(sub.inner());
            } else if (x instanceof SqlSource.Join j) {
                work.add(j.left());
                work.add(j.right());
            }
        }
        return out;
    }

    private static String slotName(@com.legend.base.Nullable SqlType t) {
        if (t instanceof SqlType.Scalar sc) {
            return sc.name();
        }
        // audit theater #8 (documented-debts 2026-08-18): this was
        // `: "VARCHAR"` — the banned default->"String" shape respelled
        // as a ternary, invisible to the fallback guard. An untyped
        // slot is an upstream typed-slot gap and WALLS
        throw new IllegalStateException("PCT render: output column"
                + " carries no scalar SqlType (" + t + ") — fix the"
                + " typed slot upstream; never a silent VARCHAR");
    }
}
