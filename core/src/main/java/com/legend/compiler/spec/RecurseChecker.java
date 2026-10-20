// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0
package com.legend.compiler.spec;

import com.legend.compiler.spec.typed.TypedRecurse;
import com.legend.compiler.spec.typed.TypedSpec;
import com.legend.protocol.spec.AppliedFunction;

/**
 * A recursion over a relation ({@code meta::legend::lite::recurse(base, r | step)}, 2026-10-17, the Cypher island's
 * unbounded variable-length patterns, docs/CYPHER_STUDY_2026_10_15.md §12.67): the base's rows and, repeatedly, the
 * rows the step yields from the previous round's, until a round yields none. The signature types it: the step's
 * parameter is a relation of the base's type and its result one too ({@code Relation<T>} unified across the three),
 * so a step projecting other columns is a type error here, never a database's. The lowerer spells the database's
 * recursive common table expression; the node is emission over the generic check, as {@code filter} is.
 */
final class RecurseChecker {
    private RecurseChecker() {
    }

    static TypedSpec check(Typer t, AppliedFunction af, Env env) {
        Application a = t.checkGeneric(af, env);
        return new TypedRecurse(a.args().get(0), Args.lambda(a, 1), a.out());
    }
}
