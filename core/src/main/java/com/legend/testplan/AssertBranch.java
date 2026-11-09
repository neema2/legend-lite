// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.testplan;

import com.legend.TypedQuery;
import com.legend.base.Nullable;
import com.legend.compiler.spec.SpecCompiler;
import com.legend.compiler.spec.typed.TypedCBoolean;
import com.legend.compiler.spec.typed.TypedCDate;
import com.legend.compiler.spec.typed.TypedCDecimal;
import com.legend.compiler.spec.typed.TypedCFloat;
import com.legend.compiler.spec.typed.TypedCInteger;
import com.legend.compiler.spec.typed.TypedCString;
import com.legend.compiler.spec.typed.TypedCollection;
import com.legend.compiler.spec.typed.TypedIf;
import com.legend.compiler.spec.typed.TypedSpec;
import com.legend.planner.QueryDoor;

import java.util.EnumSet;
import java.util.List;

/**
 * AN ASSERT UNDER AN {@code if} (the asserts-as-plan-nodes program, step 4, 2026-11-04;
 * docs/ASSERT_NODES_PLAN_2026_11_04.md): a statement {@code if(condition, |then, |else)} whose sides hold asserts is
 * planned as lite's {@link com.legend.testexecutionplan.TestPlan.Branch}: its condition a side the runner reads, each
 * side a BODY of its own -- the side's statement over the lets before the branch -- planned by the same judge's planner
 * as any body, so an assert under an {@code if} is planned exactly as one at a statement's root, and the side not taken
 * is never sent, as upstream never evaluates it. Planning only: the typed tree read, nothing evaluated.
 */
final class AssertBranch {

    private AssertBranch() {
    }

    /** A branch statement's parts: its condition, and each side's statements -- the side itself where it holds an
     *  assert (a statement-root assert, an {@code assertError}, a branch again), none where it asserts nothing and
     *  writes a constant (nothing to run) or is absent. */
    record Parts(TypedSpec condition, List<TypedSpec> then, List<TypedSpec> otherwise) {
        Parts {
            then = List.copyOf(then);
            otherwise = List.copyOf(otherwise);
        }
    }

    /**
     * The branch {@code statement} is, or null: an {@code if} at the statement's root (not bound by a let) whose
     * condition holds no test-only kind (an assert or an effect in the condition is a value's position, not a side's)
     * and whose each side is a statement a judge plans or a constant. A constant side's value is read only where the
     * {@code if} is the body's LAST statement (the body's answer), so there each side must hold an assert -- the
     * answer is then the asserts' value, {@code true}, as upstream's; a constant there is left to the refusal.
     */
    static @Nullable Parts of(TypedSpec statement, boolean last, QueryDoor.Scan scan, SpecCompiler specs) {
        if (!(statement instanceof TypedIf branch) || !scan.kinds(branch.condition()).isEmpty()) {
            return null;
        }
        List<TypedSpec> then = side(branch.thenBranch(), last, scan, specs);
        List<TypedSpec> otherwise = branch.elseBranch().isPresent()
                ? side(branch.elseBranch().get(), last, scan, specs) : List.of();
        if (then == null || otherwise == null || then.isEmpty() && otherwise.isEmpty()) {
            return null;
        }
        if (last && (then.isEmpty() || otherwise.isEmpty())) {
            return null;
        }
        return new Parts(branch.condition(), then, otherwise);
    }

    /** {@code side}'s statements (above), or null where it is neither. A side that is the literal {@code true} where
     *  the branch is the body's last statement answers what an assert answers: it is planned as {@code assert(true)},
     *  an assert that holds (2026-11-04, the assert bar: {@code if(c, |assertEquals(1, 1), |true)} closing a test
     *  body was refused). */
    private static @Nullable List<TypedSpec> side(TypedSpec side, boolean last, QueryDoor.Scan scan,
            SpecCompiler specs) {
        EnumSet<QueryDoor.TestOnly> kinds = scan.kinds(side);
        if (kinds.equals(EnumSet.of(QueryDoor.TestOnly.ASSERT)) || QueryDoor.assertErrorRoot(side, specs)) {
            return List.of(side);
        }
        if (last && side instanceof TypedCBoolean b && b.value()) {
            return List.of(com.legend.compiler.spec.AssertMessages.holds(specs));
        }
        return kinds.isEmpty() && constant(side) ? List.of() : null;
    }

    /** Whether {@code n} is a constant the program wrote: a literal, or a collection of them -- nothing to run. */
    private static boolean constant(TypedSpec n) {
        return switch (n) {
            case TypedCString s -> true;
            case TypedCInteger i -> true;
            case TypedCFloat f -> true;
            case TypedCDecimal d -> true;
            case TypedCBoolean b -> true;
            case TypedCDate d -> true;
            case TypedCollection c -> c.elements().stream().allMatch(AssertBranch::constant);
            default -> false;
        };
    }

    /** A side as a body of its own: its statements, under the body's model and compiler (the lets before the branch
     *  are the planner's prefix, handed to it, not statements of this body). */
    static TypedQuery body(TypedQuery typed, List<TypedSpec> side) {
        return typed.withBody(side);
    }
}
