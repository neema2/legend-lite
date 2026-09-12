// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.lowering;

import com.legend.compiler.element.type.Type;
import com.legend.compiler.spec.typed.TypedLambda;
import com.legend.compiler.spec.typed.TypedNativeCall;
import com.legend.compiler.spec.typed.TypedSort;
import com.legend.compiler.spec.typed.TypedSortBy;
import com.legend.lowering.Resolvers.Resolution;
import com.legend.sql.SqlExpr;
import com.legend.sql.SqlSelect;

import java.util.ArrayList;
import java.util.List;

/**
 * ORDER BY lowering — the sort family, split out of {@link Lowerer}
 * whole (the {@code Pivots.lower(this, …)} collaborator pattern; the
 * shape guardrail's real-split rule): {@code sort(SortInfo)} resolves
 * column-name keys into the base select (isolating on an unfoldable
 * ref), {@code sortBy} lowers a per-row key LAMBDA expression, and the
 * bare single-column {@code sort()} is natural ascending order.
 */
final class Sorts {

    private Sorts() {
    }

    static SqlSelect sort(Lowerer lw, TypedSort s) {
        SqlSelect src = lw.relation(s.source());
        SqlSelect base = Fold.sortFolds(src) ? src : lw.isolate(src);
        List<SqlSelect.SortKey> keys = new ArrayList<>(s.keys().size());
        for (TypedSort.TypedSortKey k : s.keys()) {
            SqlExpr e = Fold.resolveInto(base, k.column());
            if (e == null || !Fold.referencesColumn(e)) {
                base = lw.isolate(base);
                return sortOnto(base, s);
            }
            // engine TEXT spells the OUTPUT column (order by "name" asc);
            // execution renders e — sortBy stays physical in both
            keys.add(new SqlSelect.SortKey(e, k.ascending(), nullsOf(k, s.pureNullOrder()),
                    k.column()));
        }
        return base.withOrderBy(keys);
    }

    /** The key's null placement: EXPLICIT (emptyFirst/emptyLast, the NullOrder
     *  argument) wins; else the Pure-language placement when the sort carries
     *  it; else bare (the engine's canonical placement, the renderer's). */
    static SqlSelect.SortKey.@com.legend.Nullable NullOrder nullsOf(TypedSort.TypedSortKey k,
            boolean pureNullOrder) {
        if (k.nullOrder() != null) {
            return k.nullOrder() == com.legend.compiler.spec.typed.TypedSortInfo.NullOrder.FIRST
                    ? SqlSelect.SortKey.NullOrder.NULLS_FIRST : SqlSelect.SortKey.NullOrder.NULLS_LAST;
        }
        return pureNullOrder ? Fold.sortNulls(k.ascending()) : null;
    }

    /**
     * {@code sortBy(rel, key-lambda)} — ORDER BY over the lowered key
     * EXPRESSION (TypedSort is column-name-keyed; sortBy's key is a
     * per-row lambda). Fold.sortFolds decides extend-vs-isolate, same as
     * sort; the key expression resolves against the base select's row.
     */
    static SqlSelect sortBy(Lowerer lw, TypedSortBy sb) {
        SqlSelect src = lw.relation(sb.source());
        SqlSelect base = Fold.sortFolds(src) ? src : lw.isolate(src);
        // One isolate retry on an unfoldable key ref (a computed projection
        // column): behind the subselect it is a plain output column.
        SqlSelect fin1 = base;
        if (Resolution.attempt(() -> lw.scalar(Lowerer.last(sb.key()),
                (v, name) -> lw.resolveOrThrow(fin1, name)))
                instanceof Resolution.Resolved r) {
            // TDS/collection sortBy = the engine-drop-in surface: NO null
            // placement (backend default), per the corpus expected values
            return base.withOrderBy(List.of(
                    new SqlSelect.SortKey(r.expr(), sb.ascending(), null, null)));
        }
        SqlSelect iso = lw.isolate(base);
        SqlExpr key = lw.scalar(Lowerer.last(sb.key()),
                (v, name) -> lw.resolveOrThrow(iso, name));
        return iso.withOrderBy(List.of(
                new SqlSelect.SortKey(key, sb.ascending(), null, null)));
    }

    private static SqlSelect sortOnto(SqlSelect base, TypedSort s) {
        List<SqlSelect.SortKey> keys = new ArrayList<>(s.keys().size());
        for (TypedSort.TypedSortKey k : s.keys()) {
            SqlExpr.Column e = Fold.sourceColumn(base.from(), k.column());
            if (e == null) {
                throw new IllegalStateException("sort key '" + k.column()
                        + "' cannot be resolved after isolation");
            }
            keys.add(new SqlSelect.SortKey(e, k.ascending(), nullsOf(k, s.pureNullOrder()), null));
        }
        return base.withOrderBy(keys);
    }

    /** Natural ascending order on the stream's one column. The stream
     * may be a wrapped table or the bare rows view (schemaView). */
    static SqlSelect naturalSort(Lowerer lw, TypedNativeCall nc) {
        Type.RelationType srt = java.util.Objects.requireNonNull(
                Type.schemaView(nc.args().get(0).info().type()),
                "naturalSort needs a relation-ish stream");
        return lw.relation(nc.args().get(0)).withOrderBy(List.of(
                SqlSelect.SortKey.asc(SqlExpr.Column.derived(null,
                        srt.columns().get(0).name()))));
    }
}
