// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.compiler.spec;

import com.legend.error.TypeInferenceException;

import com.legend.compiler.spec.typed.TypedExtendAgg;
import com.legend.compiler.spec.typed.TypedExtendWindow;
import com.legend.compiler.spec.typed.TypedSpec;
import com.legend.protocol.spec.AppliedFunction;

/**
 * A SQL island's inline view ({@code meta::legend::lite::inlineView}, IQ island, 2026-10-16): a derived table or a
 * common table expression whose own query computes window functions. SQL computes them over all of the view's rows,
 * before the enclosing query's WHERE; Pure's relation composition folds a later filter under a window (legend-engine's
 * PCT {@code testExtendFilterOutNull}, {@code Fold.filterSlot}), which the relation semantics keep. The view types as
 * its relation, its outermost windows stamped as a boundary ({@link TypedExtendWindow#withExtentBoundary}), the rule
 * {@code ClassSources} applies to a class extent's windows: the lowerer closes their select, so the enclosing query's
 * operators land outside it. The marker leaves no node of its own.
 */
final class InlineViewChecker {

    private InlineViewChecker() {
    }

    static TypedSpec check(Typer t, AppliedFunction af, Env env) {
        if (af.parameters().size() != 1) {
            throw new TypeInferenceException("inlineView takes one relation, not " + af.parameters().size()
                    + " arguments");
        }
        TypedSpec view = t.synth(af.parameters().get(0), env);
        if (!com.legend.compiler.element.type.Type.relationValued(view.info())) {
            throw new TypeInferenceException("inlineView takes a relation");
        }
        return sealed(view);
    }

    /** {@code n} with the outermost window on each of its paths a boundary: the windows under it fold into its
     *  select, which the boundary closes (2026-10-16, the landing's review: a boundary on every window cost a subquery
     *  level each, and 1,100 windows passed DuckDB's expression depth). */
    private static TypedSpec sealed(TypedSpec n) {
        return switch (n) {
            case TypedExtendWindow w -> w.withExtentBoundary();
            case TypedExtendAgg a -> a.withExtentBoundary();
            default -> n.mapChildren(InlineViewChecker::sealed);
        };
    }
}
