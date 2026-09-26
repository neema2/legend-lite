// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.resolver;

import com.legend.builtin.Pure;
import com.legend.compiler.spec.typed.TypedLambda;
import com.legend.compiler.spec.typed.TypedNativeCall;
import com.legend.compiler.spec.typed.TypedPropertyAccess;
import com.legend.compiler.spec.typed.TypedSpec;
import com.legend.compiler.spec.typed.TypedVariable;

import java.util.List;

/** The foreign-key identity of a navigate step (ClassMapping.foreignKeyBinding):
 * the ROW column a two-parameter step condition equates with the target's
 * ONE declared key column, or null when the condition is any other shape. */
final class ForeignKeyIdentity {

    private ForeignKeyIdentity() {
    }

    /** Register the FOREIGN-KEY IDENTITY pseudo-bindings of a set's row
     * (ClassMapping.foreignKeyBinding): for every to-one class-typed
     * navigate slot whose step condition is ONE equality between a row
     * column and the TARGET set's declared key column, the target's
     * identity IS that row column (D2) — element identity equality
     * (ChainNormalizer) reads it without the join, in every scope a plain
     * row read resolves (group F burn 2026-09-02). */
    static void register(java.util.Map<String, TypedSpec> bindings,
            com.legend.compiler.spec.typed.TypedNewInstance ctor, String rowVar,
            TypedSpec pipeline, com.legend.compiler.element.type.Type.RelationType rowType,
            com.legend.model.MappingDefinition mapping,
            com.legend.compiler.element.ModelContext ctx) {
        var steps = Pipelines.navSteps(pipeline);
        for (var e : ctor.properties().entrySet()) {
            String alias = InnerDemand.navSlotAlias(e.getValue(), rowVar, steps.keySet());
            var step = alias == null ? null : steps.get(alias);
            if (step == null || !(step.target()
                    instanceof com.legend.compiler.spec.typed.TypedGetAll tg)) {
                continue;
            }
            List<String> tgtKeys = List.of();
            for (var tcb : mapping.classBindingsWithIncludes(tg.classFqn(), ctx::findMapping)) {
                tgtKeys = tcb.primaryKeyColumns();
                break;
            }
            String fkCol = sourceKeyColumn(step.predicate(), tgtKeys);
            for (var c : rowType.columns()) {
                if (fkCol != null && c.name().equals(fkCol)) {
                    bindings.putIfAbsent(
                            com.legend.model.ClassMapping.foreignKeyBinding(e.getKey()),
                            new TypedPropertyAccess(
                                    new TypedVariable(rowVar,
                                            com.legend.compiler.element.type.ExprType.one(rowType)),
                                    c.name(), new com.legend.compiler.element.type.ExprType(
                                            c.type(), c.multiplicity())));
                }
            }
        }
    }

    static @com.legend.base.Nullable String sourceKeyColumn(TypedLambda cond, List<String> targetKeys) {
        if (targetKeys.size() != 1 || cond.parameters().size() != 2 || cond.body().isEmpty()) {
            return null;
        }
        TypedSpec body = cond.body().get(cond.body().size() - 1);
        if (!(body instanceof TypedNativeCall c) || c.args().size() != 2
                || !Pure.nativeNamed("equal", c.callee().signatureKey())) {
            return null;
        }
        String row = cond.parameters().get(0);
        String tgt = cond.parameters().get(1);
        String src = null;
        String key = null;
        for (TypedSpec side : c.args()) {
            if (!(side instanceof TypedPropertyAccess pa)
                    || !(pa.source() instanceof TypedVariable v)) {
                return null;
            }
            if (v.name().equals(row)) {
                src = pa.property();
            } else if (v.name().equals(tgt)) {
                key = pa.property();
            } else {
                return null;
            }
        }
        return src != null && targetKeys.get(0).equals(key) ? src : null;
    }
}
