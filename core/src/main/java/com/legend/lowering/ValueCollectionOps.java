// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.lowering;

import com.legend.builtin.Pure;
import com.legend.compiler.element.type.Type;
import com.legend.compiler.spec.typed.TypedDistinct;
import com.legend.compiler.spec.typed.TypedNativeCall;
import com.legend.compiler.spec.typed.TypedSort;
import com.legend.compiler.spec.typed.TypedSpec;

import java.util.List;

/**
 * Value-collection natives over a SINGLE-COLUMN RELATION read (class-
 * rooted frame reads: {@code $result.values.legalName
 * ->removeDuplicates()->sort()}): dedup and order happen in RELATION
 * space — SELECT DISTINCT / ORDER BY inside the list subquery. The
 * list-space rules would re-embed the list subquery inside a SQL
 * lambda, which DuckDB's binder rejects (corpus testIsNotEmpty).
 */
final class ValueCollectionOps {

    private ValueCollectionOps() {
    }

    /** The relation-space rewrite of {@code n}, or null when {@code n} is
     * not a 1-arg removeDuplicates/sort over a single-column relation. */
    /** Bare ->sort() over a SINGLE-COLUMN relation stream (assert
     * vocabulary: $result.rows.values->sort()) — a multi-column relation
     * has no natural order and stays at the loud frontier default. */
    static boolean isBareSingleColumnSort(TypedNativeCall nc) {
        return "meta::pure::functions::collection::sort"
                        .equals(nc.callee().qualifiedName())
                && nc.args().size() == 1
                && com.legend.compiler.element.type.Type
                        .schemaView(nc.args().get(0).info().type())
                        instanceof com.legend.compiler.element.type
                                .Type.RelationType srt
                && srt.columns().size() == 1;
    }

    /** Relation-POSITION removeDuplicates over a single-column relation
     * (the corpus assert idiom {@code $r.rows.values->removeDuplicates()}):
     * rewrite to TypedDistinct — whole-row DISTINCT is only cell dedup
     * when there is exactly one column; multi-column stays at the loud
     * frontier wall. */
    static @com.legend.Nullable com.legend.compiler.spec.typed.TypedDistinct
            relationDistinct(TypedNativeCall n) {
        return "meta::pure::functions::collection::removeDuplicates"
                        .equals(n.callee().qualifiedName())
                && n.args().size() == 1
                && com.legend.compiler.element.type.Type
                        .schemaView(n.args().get(0).info().type())
                        instanceof com.legend.compiler.element.type
                                .Type.RelationType rt
                && rt.columns().size() == 1
                ? new com.legend.compiler.spec.typed.TypedDistinct(
                        n.args().get(0), java.util.List.of(),
                        n.args().get(0).info())
                : null;
    }

    static @com.legend.Nullable TypedSpec relationSpaceRewrite(TypedNativeCall n) {
        if (n.args().size() != 1
                || !(Type.schemaView(n.args().get(0).info().type())
                        instanceof Type.RelationType rt)
                || rt.columns().size() != 1) {
            return null;
        }
        String key = n.callee().signatureKey();
        if (Pure.nativeNamed("removeDuplicates", key)) {
            return new TypedDistinct(n.args().get(0),
                    List.of(rt.columns().get(0).name()),
                    n.args().get(0).info());
        }
        if (Pure.nativeNamed("sort", key)) {
            // value-collection sort observes PURE semantics (null largest)
            return new TypedSort(n.args().get(0),
                    List.of(new TypedSort.TypedSortKey(
                            rt.columns().get(0).name(), true, null)),
                    true, n.args().get(0).info());
        }
        return null;
    }
}
