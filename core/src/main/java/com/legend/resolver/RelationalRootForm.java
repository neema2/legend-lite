// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.resolver;

import com.legend.compiler.element.ModelContext;
import com.legend.compiler.spec.typed.TypedSpec;
import com.legend.compiler.spec.typed.TypedTableReference;
import com.legend.model.MappingDefinition;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

/**
 * The PRIMARY-KEY RULE of a class root, legend-engine's resolvePrimaryKey order: the mapping's declared
 * {@code ~primaryKey} columns first, the root table's PRIMARY KEY flags only when the mapping declares none (audit 19:
 * the mapping's declared keys come FIRST). The product resolver's key rule on executed paths: an object reference's
 * decode, a class source's keys, a correlated subselect's and a graph emission's order keys.
 *
 * <p>Its flat relational form of a bare class root ({@code apply}, the SELECT legend-engine assembles objects from:
 * {@code pk_0}.. plus the property leaves), which only legend-engine's {@code toSQLString} text read, was deleted with
 * that text (A2 of the compatibility program, 2026-11-19, CP-4: upstream's SQL text is never promised).
 */
public final class RelationalRootForm {

    private RelationalRootForm() {
    }

    /** The GraphEmission order-key entry (same rule, pre-node). */
    static List<String> primaryKeyColumns(@com.legend.base.Nullable String classFqn, TypedSpec source,
            @com.legend.base.Nullable String mappingFqn, ModelContext ctx) {
        if (mappingFqn != null && classFqn != null) {
            var mapping = ctx.findMapping(mappingFqn).orElse(null);
            if (mapping != null) {
                // through the INCLUDES (own first): a class bound by an
                // included mapping declares its ~primaryKey there, and this
                // walked the queried mapping's own bindings only — the
                // fourth "find the binding" rule of audit 2026-09-15 P2-3
                for (MappingDefinition.ClassBinding cb
                        : mapping.classBindingsWithIncludes(classFqn, ctx::findMapping)) {
                    if (!cb.primaryKeyColumns().isEmpty()) {
                        return dedup(cb.primaryKeyColumns());
                    }
                }
            }
        }
        TypedSpec cur = source;
        TypedTableReference tref = null;
        while (tref == null) {
            if (cur instanceof TypedTableReference tr) {
                tref = tr;
            } else if (cur.children().isEmpty()) {
                return List.of();
            } else {
                cur = cur.children().get(0);
            }
        }
        var td = ctx.findTableDefinition(tref.store(), tref.table())
                .orElse(null);
        if (td == null) {
            return List.of();
        }
        List<String> out = new ArrayList<>();
        for (var cd : td.columns()) {
            if (cd.primaryKey()) {
                out.add(cd.name());
            }
        }
        return out;
    }

    /** Quote-bearing row columns match their bare store pk spelling. */
    static String stripQ(String n) {
        return n.length() > 1 && n.startsWith("\"") && n.endsWith("\"")
                ? n.substring(1, n.length() - 1) : n;
    }

    private static List<String> dedup(List<String> names) {
        Set<String> seen = new LinkedHashSet<>(names);
        return new ArrayList<>(seen);
    }
}
