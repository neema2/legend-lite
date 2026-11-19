// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.edge;

import com.legend.builtin.NativeFn;
import com.legend.compiler.element.type.Type;
import com.legend.compiler.spec.typed.Calls;
import com.legend.compiler.spec.typed.TypedSpec;

import java.util.List;

/**
 * UPSTREAM COUNTS A RELATION AS ONE VALUE (register S137, the user's ruling of 2026-11-05; census C1, the
 * orchestrator's ruling of 2026-11-17): upstream's {@code assertSize}, {@code assertEmpty} and {@code assertNotEmpty}
 * take a collection ({@code Any[*]}), so over a relation they count the relation itself. The edge's form of the
 * two-form kind {@link com.legend.compiler.spec.RelationCounting}, moved from {@code RelationCountAsserts}.
 */
public enum UpstreamRelationCounting implements com.legend.compiler.spec.RelationCounting {
    INSTANCE;

    @Override
    public List<TypedSpec> counted(List<TypedSpec> body) {
        List<TypedSpec> r = countedAsOne(body);
        // H6 PROTOTYPE (MEASUREMENT ONLY): a rewrite is S137 deciding; off, lite's refusal answers
        if (r != body) {
            com.legend.diagnostics.LeniencyProbe.changed("S137", "a relation counted as one value");
            if (com.legend.diagnostics.LeniencyProbe.off("S137")) {
                return com.legend.compiler.spec.RelationCountAsserts.REFUSED.counted(body);
            }
        }
        return r;
    }

    /**
     * Backwards-compatibility mode: {@code body} with each relation an {@code assertSize} or {@code assertNotEmpty}
     * counts replaced by ONE value, as upstream counts it -- {@code assertSize($rel, n)} holds only for {@code n == 1},
     * failing with {@code expected size: n, actual size: 1}; {@code assertNotEmpty($rel)} always holds. Both judges plan
     * the rewritten call. {@code assertEmpty($rel)} always fails upstream with {@code '%s is not empty'} printing the
     * relation itself, a text lite does not spell: it is refused by name (the verdict is a failure either way). The
     * body itself where nothing is rewritten.
     */
    static List<TypedSpec> countedAsOne(List<TypedSpec> body) {
        List<TypedSpec> out = new java.util.ArrayList<>(body.size());
        boolean changed = false;
        for (TypedSpec statement : body) {
            TypedSpec r = asOne(statement);
            changed |= r != statement;
            out.add(r);
        }
        return changed ? out : body;
    }

    private static TypedSpec asOne(TypedSpec n) {
        List<TypedSpec> kids = n.children();
        List<TypedSpec> rebuilt = new java.util.ArrayList<>(kids.size());
        boolean changed = false;
        for (TypedSpec k : kids) {
            TypedSpec r = asOne(k);
            changed |= r != k;
            rebuilt.add(r);
        }
        TypedSpec node = changed ? n.withChildren(rebuilt) : n;
        NativeFn.Verdict fn = NativeFn.Verdict.of(Calls.calleeIdOf(node)).orElse(null);
        if (node instanceof com.legend.compiler.spec.typed.TypedNativeCall call
                && (fn == NativeFn.Verdict.ASSERT_SIZE || fn == NativeFn.Verdict.ASSERT_EMPTY
                        || fn == NativeFn.Verdict.ASSERT_NOT_EMPTY)
                && !call.args().isEmpty() && Type.isRelation(call.args().get(0).info().type())) {
            if (fn == NativeFn.Verdict.ASSERT_EMPTY) {
                throw new com.legend.error.ModelException(com.legend.error.LegendCompileException.Phase.TYPE,
                        "assertEmpty over a relation: upstream counts the relation as ONE value, so it fails with a"
                                + " message printing the relation itself, which lite does not spell; say"
                                + " assertEquals(0, $relation->size())");
            }
            List<TypedSpec> args = new java.util.ArrayList<>(call.args());
            args.set(0, new com.legend.compiler.spec.typed.TypedCInteger(1L,
                    com.legend.compiler.element.type.ExprType.one(Type.Primitive.INTEGER)));
            return call.withChildren(args);
        }
        return node;
    }

}
