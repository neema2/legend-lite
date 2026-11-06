// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.compiler.spec;

import com.legend.builtin.NativeFn;
import com.legend.compiler.element.type.ExprType;
import com.legend.compiler.element.type.Multiplicity;
import com.legend.compiler.element.type.Type;
import com.legend.compiler.spec.typed.Calls;
import com.legend.compiler.spec.typed.TypedCBoolean;
import com.legend.compiler.spec.typed.TypedLet;
import com.legend.compiler.spec.typed.TypedSpec;

import java.util.ArrayList;
import java.util.List;

/**
 * THE LIFT (the asserts-as-plan-nodes program, step 4, 2026-11-04; docs/ASSERT_NODES_PLAN_2026_11_04.md): an assert's
 * value, whenever execution continues past it, is {@code true} -- it holds, or it raises and nothing after it runs. So
 * an assert that sits in a position the planner plans as a value is lifted to a statement of its own at its place, and
 * {@code true} stands where it stood: the judges plan it as any statement-root assert, the value's readers read
 * {@code true}. One rule, no shapes.
 *
 * <p>Here: an assert a {@code let} binds ({@code let ok = assertEquals(1, 1); assert($ok);} is
 * {@code assertEquals(1, 1); let ok = true; assert($ok);}). Planning only: a rewrite of the typed body, nothing
 * evaluated.
 */
public final class AssertLift {

    private AssertLift() {
    }

    /** {@code body} with its let-bound asserts lifted, or {@code body} itself where it binds none. */
    public static List<TypedSpec> lift(List<TypedSpec> body) {
        List<TypedSpec> out = null;
        for (int i = 0; i < body.size(); i++) {
            TypedSpec statement = body.get(i);
            if (statement instanceof TypedLet let && isAssert(let.value())) {
                if (out == null) {
                    out = new ArrayList<>(body.subList(0, i));
                }
                out.add(let.value());
                out.add(new TypedLet(let.name(), new TypedCBoolean(true,
                        new ExprType(Type.Primitive.BOOLEAN, Multiplicity.Bounded.ONE)), let.info()));
            } else if (out != null) {
                out.add(statement);
            }
        }
        return out == null ? body : List.copyOf(out);
    }

    /** Whether {@code n} is a call of the assert family (a verdict native: {@code assert}, {@code assertEquals}, ...). */
    static boolean isAssert(TypedSpec n) {
        return NativeFn.Verdict.of(Calls.calleeIdOf(n)).filter(v -> v != NativeFn.Verdict.TO_CSV).isPresent();
    }
}
