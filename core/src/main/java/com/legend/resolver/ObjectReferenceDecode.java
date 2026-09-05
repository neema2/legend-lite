// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0
package com.legend.resolver;

import com.legend.compiler.element.ModelContext;
import com.legend.compiler.element.TypedParameter;
import com.legend.compiler.element.type.ExprType;
import com.legend.compiler.element.type.Multiplicity;
import com.legend.compiler.element.type.Type;
import com.legend.compiler.spec.typed.TypedCString;
import com.legend.compiler.spec.typed.TypedMap;
import com.legend.compiler.spec.typed.TypedNativeCall;
import com.legend.compiler.spec.typed.TypedSerializeGraph;
import com.legend.compiler.spec.typed.TypedSpec;
import com.legend.error.NotImplementedException;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * The engine's {@code decodeObjectReferencesAndGetPkMap(version, ref,
 * ext)} (core_relational objectReference.pure) as a DATABASE expression
 * (batch 72b): the reference decodes to {@code {"pathToMapping": m,
 * "pkMap": {col: v, …}, "setId": s}} where the positional {@code pk$_i}
 * keys of the pk segment become the set's pk COLUMN NAMES (the engine's
 * resolvePrimaryKeysNames). The names are MODEL FACTS: the call always
 * reads references out of a serialized frame, so the pass finds the
 * {@link TypedSerializeGraph} the map's source carries, takes the DEFINING
 * mapping its reference prefix names, and spells that mapping's
 * include-closure sets ({@code setId -> [pk columns]}) as a literal
 * argument of the lite reader {@code asorDecodePkMap(ref, table)} — the
 * SQL then selects the arm by the decoded setId. Constants ride the
 * query; the decode itself is the database's.
 */
final class ObjectReferenceDecode {

    static final String DECODE_FQN =
            "meta::alloy::objectReference::decodeObjectReferencesAndGetPkMap";

    private ObjectReferenceDecode() {
    }

    static TypedSpec rewrite(TypedSpec stmt, ModelContext ctx, ClassSources sources) {
        return walk(stmt, null, ctx, sources);
    }

    private static TypedSpec walk(TypedSpec n, @com.legend.Nullable TypedSerializeGraph graph,
            ModelContext ctx, ClassSources sources) {
        if (n instanceof TypedNativeCall c && DECODE_FQN.equals(c.callee().qualifiedName())
                && c.args().size() == 3) {
            if (graph == null || graph.objectRefPrefix() == null) {
                throw new NotImplementedException("decodeObjectReferencesAndGetPkMap"
                        + " needs a serialized frame with an objectReference channel"
                        + " as its reference source");
            }
            String mapping = AsorRef.prefixMapping(graph.objectRefPrefix());
            if (mapping == null) {
                throw new IllegalStateException("objectReference prefix names no mapping: "
                        + graph.objectRefPrefix());
            }
            var reader = ObjectReferenceArms.liteCallee(com.legend.builtin.Pure.Lite.ASOR_DECODE_PK_MAP,
                    List.of(new TypedParameter("ref", Type.Primitive.STRING, Multiplicity.Bounded.ONE),
                            new TypedParameter("pkNames", Type.Primitive.STRING,
                                    Multiplicity.Bounded.ONE)),
                    Type.Primitive.STRING);
            var str1 = new ExprType(Type.Primitive.STRING, Multiplicity.Bounded.ONE);
            return new TypedNativeCall(reader, List.of(
                    walk(c.args().get(1), graph, ctx, sources),
                    new TypedCString(pkNamesJson(ctx, sources, mapping), str1)), c.info());
        }
        if (n instanceof TypedMap m) {
            TypedSerializeGraph g = findGraph(m.source());
            TypedSpec src = walk(m.source(), graph, ctx, sources);
            TypedSpec mapper = walk(m.mapper(), g != null ? g : graph, ctx, sources);
            return src == m.source() && mapper == m.mapper() ? m
                    : new TypedMap(src, (com.legend.compiler.spec.typed.TypedLambda) mapper, m.info());
        }
        List<TypedSpec> kids = n.children();
        List<TypedSpec> out = new ArrayList<>(kids.size());
        boolean same = true;
        for (TypedSpec k : kids) {
            TypedSpec r = walk(k, graph, ctx, sources);
            same &= r == k;
            out.add(r);
        }
        return same ? n : n.withChildren(out);
    }

    private static @com.legend.Nullable TypedSerializeGraph findGraph(TypedSpec n) {
        if (n instanceof TypedSerializeGraph g) {
            return g;
        }
        for (TypedSpec k : n.children()) {
            TypedSerializeGraph g = findGraph(k);
            if (g != null) {
                return g;
            }
        }
        return null;
    }

    /** {@code {"<setId>": ["pkCol", …], …}} over the mapping's include
     * closure — the set id is the binding's own id or the class FQN
     * mangled ({@code ::} → {@code _}), the same spelling the reference
     * prefix carries (GraphEmission.asorPrefix). */
    static String pkNamesJson(ModelContext ctx, ClassSources sources, String mappingFqn) {
        Map<String, List<String>> table = new LinkedHashMap<>();
        collect(ctx, sources, mappingFqn, table, new java.util.HashSet<>());
        StringBuilder sb = new StringBuilder("{");
        boolean first = true;
        for (var e : table.entrySet()) {
            sb.append(first ? "" : ",").append('"').append(e.getKey()).append("\":[");
            first = false;
            for (int i = 0; i < e.getValue().size(); i++) {
                sb.append(i > 0 ? "," : "").append('"')
                        .append(e.getValue().get(i).replace("\"", "")).append('"');
            }
            sb.append(']');
        }
        return sb.append('}').toString();
    }

    /** The set's TABLE key when no ~primaryKey is declared (the engine's
     * resolvePrimaryKey): the binding's resolved pipeline names its main
     * table through the typed tree — the same reader the objectReferenceIn
     * registries use. Relational bindings only (a Pure/M2M set has no
     * table key). */
    private static List<String> tableKey(ClassSources sources, ModelContext ctx,
            String mappingFqn, String classFqn) {
        ClassSource cs = sources.get(mappingFqn, classFqn, null);
        return RelationalRootForm.primaryKeyColumns(classFqn, cs.pipeline(),
                mappingFqn, ctx);
    }

    private static void collect(ModelContext ctx, ClassSources sources, String mappingFqn,
            Map<String, List<String>> table, java.util.Set<String> seen) {
        if (!seen.add(mappingFqn)) {
            return;
        }
        var m = ctx.findMapping(mappingFqn).orElse(null);
        if (m == null) {
            return;
        }
        for (var cb : m.classBindings()) {
            // the declared ~primaryKey, else the set's TABLE key (the
            // engine's resolvePrimaryKey: a set without a declared key
            // takes its main table's primary key columns)
            List<String> pks = !cb.primaryKeyColumns().isEmpty() ? cb.primaryKeyColumns()
                    : cb instanceof com.legend.model.MappingDefinition.ClassBinding.Relational
                            ? tableKey(sources, ctx, mappingFqn, cb.classFqn()) : List.of();
            if (pks.isEmpty()) {
                continue;
            }
            String setId = cb.setId() != null && !cb.setId().isEmpty()
                    ? cb.setId() : cb.classFqn().replace("::", "_");
            table.putIfAbsent(setId, pks);
            table.putIfAbsent(cb.classFqn().replace("::", "_"), pks);
        }
        for (var inc : m.includes()) {
            collect(ctx, sources, inc.mappingPath(), table, seen);
        }
    }
}
