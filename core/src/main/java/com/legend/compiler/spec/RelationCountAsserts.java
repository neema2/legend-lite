// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.compiler.spec;

import com.legend.builtin.NativeFn;
import com.legend.compiler.element.type.Type;
import com.legend.compiler.spec.typed.Calls;
import com.legend.compiler.spec.typed.TypedSpec;
import java.util.List;

/**
 * A RELATION COUNTED AS ONE VALUE (register S137, the user's ruling of 2026-11-05): upstream's {@code assertSize},
 * {@code assertEmpty} and {@code assertNotEmpty} take a collection ({@code Any[*]}), so over a relation they count the
 * relation itself -- {@code assertEmpty($rel)} always fails and {@code assertNotEmpty($rel)} always passes, whatever
 * rows it holds. Backwards-compatibility mode does exactly that; lite's own runs refuse the three over a relation at
 * planning, naming the assert that says what the test means.
 */
public final class RelationCountAsserts {

    private RelationCountAsserts() {
    }

    /** Refuses an {@code assertSize}, {@code assertEmpty} or {@code assertNotEmpty} over a relation anywhere in
     *  {@code body} (a lambda's too). */
    public static void refuse(List<? extends TypedSpec> body) {
        for (TypedSpec statement : body) {
            walk(statement, null);
        }
    }

    /** Backwards-compatibility mode's measurement: each such assert's name, refusing nothing -- upstream's one-value
     *  count is not built there yet (lite counts the rows). */
    public static List<String> found(List<? extends TypedSpec> body) {
        List<String> out = new java.util.ArrayList<>();
        for (TypedSpec statement : body) {
            walk(statement, out);
        }
        return out;
    }

    private static void walk(TypedSpec n, @com.legend.base.Nullable List<String> found) {
        NativeFn.Verdict fn = NativeFn.Verdict.of(Calls.calleeIdOf(n)).orElse(null);
        if (fn == NativeFn.Verdict.ASSERT_SIZE || fn == NativeFn.Verdict.ASSERT_EMPTY
                || fn == NativeFn.Verdict.ASSERT_NOT_EMPTY) {
            List<TypedSpec> args = Calls.argsOf(n);
            if (!args.isEmpty() && Type.isRelation(args.get(0).info().type())) {
                if (found != null) {
                    found.add(fn.bareName());
                    return;
                }
                String instead = fn == NativeFn.Verdict.ASSERT_SIZE ? "assertEquals(<size>, $relation->size())"
                        : fn == NativeFn.Verdict.ASSERT_EMPTY ? "assertEquals(0, $relation->size())"
                        : "assertNotEquals(0, $relation->size())";
                throw new com.legend.error.ModelException(com.legend.error.LegendCompileException.Phase.TYPE,
                        fn.bareName() + " over a relation counts the relation as ONE value, whatever rows it holds"
                                + " (upstream's collection is Any[*]); lite refuses it outside backwards-compatibility"
                                + " mode: say " + instead);
            }
        }
        for (TypedSpec child : n.children()) {
            walk(child, found);
        }
    }
}
