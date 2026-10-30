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
                return sortOnto(base, s, src);
            }
            // engine TEXT spells the OUTPUT column (order by "name" asc);
            // execution renders e — sortBy stays physical in both
            keys.add(new SqlSelect.SortKey(e, k.ascending(), nullsOf(k, s.pureNullOrder()),
                    k.column()));
        }
        keys.addAll(carried(src, base, keys));
        return base.withOrderBy(keys);
    }

    /** Whether {@link #isolateOrdered} carries the ORDER BY of {@code src}
     * onto the select over it (an ordered subselect orders nothing outside
     * it): every key read by a name {@code src} shows it under ({@link
     * #carriedName}); not an expression key nor a value {@code src} does not
     * show (IQ island, 2026-10-16, the landing's review). */
    static boolean keepsOrder(SqlSelect src) {
        for (SqlSelect.SortKey k : src.orderBy()) {
            if (carriedName(src, k) == null) {
                return false;
            }
        }
        return true;
    }

    /** {@code src} isolated, its ORDER BY carried onto the select over it
     * when {@link #keepsOrder} (every key, or none: a part of an order is
     * another order, the review's finding: carrying the keys it could,
     * a rename's frame re-sorted by its second key alone); else the
     * subselect's own order rides alone, as every isolation's did before
     * 2026-10-16. */
    static SqlSelect isolateOrdered(Lowerer lw, SqlSelect src) {
        SqlSelect base = lw.isolate(src);
        if (src.orderBy().isEmpty() || !keepsOrder(src)) {
            return base;
        }
        List<SqlSelect.SortKey> keys = new ArrayList<>();
        for (SqlSelect.SortKey k : src.orderBy()) {
            String name = java.util.Objects.requireNonNull(carriedName(src, k));
            SqlExpr.Column c = Fold.sourceColumn(base.from(), name);
            if (c == null) {
                throw new IllegalStateException("sort key '" + name + "' cannot be resolved after isolation");
            }
            keys.add(new SqlSelect.SortKey(c, k.ascending(), k.nullOrder(), k.outputName()));
        }
        return base.withOrderBy(keys);
    }

    /** The name {@code src} shows {@code k}'s value under: the key's output
     * name when {@code src} shows it; else, for a plain column key, the name
     * of the projection of that very column (its alias, a fresh internal one
     * too), or the column's own through a star; null when there is none. */
    static @com.legend.base.Nullable String carriedName(SqlSelect src, SqlSelect.SortKey k) {
        if (k.outputName() != null && Fold.resolveInto(src, k.outputName()) != null) {
            return k.outputName();
        }
        if (!(k.expr() instanceof SqlExpr.Column col)) {
            return null;
        }
        for (SqlSelect.Projection p : src.projections()) {
            if (p.expr() instanceof SqlExpr.Column shown && p.outputName() != null
                    && java.util.Objects.equals(shown.table(), col.table()) && shown.name().equals(col.name())) {
                return p.outputName();
            }
        }
        return Fold.resolveInto(src, col.name()) instanceof SqlExpr.Column shown
                && java.util.Objects.equals(shown.table(), col.table()) && shown.name().equals(col.name())
                ? col.name() : null;
    }

    /** A sort over a SORTED source COMPOSES: the new keys first, the
     * source's keys after them as tie-breakers — Pure's sort is stable, so
     * {@code sort(name)->sort(address)} is the engine's
     * {@code ORDER BY address, name} (testDoubleSortAsc1Chain's golden SQL).
     * The source was isolated (an ordered subselect orders nothing outside
     * it), so a carried key is re-addressed by its OUTPUT name; a key with
     * no output name (a sortBy expression) cannot be carried. Nothing to
     * carry when the sort folded (the source had no ORDER BY). A carried
     * key already among the new keys is dropped (the new key decides). */
    private static List<SqlSelect.SortKey> carried(SqlSelect src, SqlSelect base,
            List<SqlSelect.SortKey> fresh) {
        List<SqlSelect.SortKey> out = new ArrayList<>();
        if (base == src) {
            return out;
        }
        for (SqlSelect.SortKey k : src.orderBy()) {
            String name = k.outputName();
            if (name == null) {
                continue;
            }
            boolean shadowed = false;
            for (SqlSelect.SortKey f : fresh) {
                if (name.equals(f.outputName())) {
                    shadowed = true;
                    break;
                }
            }
            if (shadowed) {
                continue;
            }
            SqlExpr.Column c = Fold.sourceColumn(base.from(), name);
            if (c != null) {
                out.add(new SqlSelect.SortKey(c, k.ascending(), k.nullOrder(), name));
            }
        }
        return out;
    }

    /** The key's null placement: EXPLICIT (emptyFirst/emptyLast, the NullOrder
     *  argument) wins; else the Pure-language placement when the sort carries
     *  it; else bare (the engine's canonical placement, the renderer's). */
    static SqlSelect.SortKey.@com.legend.base.Nullable NullOrder nullsOf(TypedSort.TypedSortKey k,
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
        if (Resolution.attempt(() -> lw.scalar(LambdaBinding.last(sb.key()),
                (v, name) -> lw.resolveOrThrow(fin1, name)))
                instanceof Resolution.Resolved r) {
            // TDS/collection sortBy = the engine-drop-in surface: NO null
            // placement (backend default), per the corpus expected values
            return base.withOrderBy(List.of(
                    new SqlSelect.SortKey(r.expr(), sb.ascending(), null, null)));
        }
        SqlSelect iso = lw.isolate(base);
        SqlExpr key = lw.scalar(LambdaBinding.last(sb.key()),
                (v, name) -> lw.resolveOrThrow(iso, name));
        return iso.withOrderBy(List.of(
                new SqlSelect.SortKey(key, sb.ascending(), null, null)));
    }

    private static SqlSelect sortOnto(SqlSelect base, TypedSort s, SqlSelect src) {
        List<SqlSelect.SortKey> keys = new ArrayList<>(s.keys().size());
        for (TypedSort.TypedSortKey k : s.keys()) {
            SqlExpr.Column e = Fold.sourceColumn(base.from(), k.column());
            if (e == null) {
                throw new IllegalStateException("sort key '" + k.column()
                        + "' cannot be resolved after isolation");
            }
            keys.add(new SqlSelect.SortKey(e, k.ascending(), nullsOf(k, s.pureNullOrder()), null));
        }
        keys.addAll(carried(src, base, keys));
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
