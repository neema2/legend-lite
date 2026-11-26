// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.compiler.spec;

import com.legend.compiler.spec.typed.TypedCrossTypeComparison;
import com.legend.compiler.spec.typed.TypedSpec;

/**
 * LITE'S COMPARISON SETTING ({@link CrossTypeRule}), the type-checker's own unless a door hands it another. A
 * comparison across types is a mistake wherever it is written: in the query, and in every body the query reaches --
 * a function's, a derived property's, a constraint's, a mapping's (the edition program's step 4, 2026-11-26: until
 * then a body's was the database's comparison). One exception is no mistake at all: a number literal written against
 * a value of another number type reads as that type when it is exact there ({@link NumberLiterals}; the user's ruling
 * of 2026-11-26), so {@code $x.qty > 0} over a Float is {@code > 0.0}.
 *
 * <p>A function generic over relations is typed into each call, over that call's columns: its column positions are
 * generic, so a comparison of two of them is never a mistake of the code as written ({@link #inGenericBody}).
 *
 * <p>Every other comparison across types is typed as its marker, never raised where it is typed (the type-checker
 * tries a call's overloads in turn, and a comparison met in a candidate it then drops was never the program's). The
 * marker is refused by name, naming the two types and the fix: in the query by the planner before it plans anything
 * ({@link CrossTypeComparisons#refuse}); in a body when the body is typed whole ({@code SpecCompiler.compile}), so a
 * model's compile check and a query's run agree.
 */
public final class LiteCrossTypeRule implements CrossTypeRule {

    /** Lite's setting. */
    public static final LiteCrossTypeRule INSTANCE = new LiteCrossTypeRule();

    private LiteCrossTypeRule() {
    }

    @Override
    public TypedSpec inQuery(TypedCrossTypeComparison crossing) {
        TypedSpec read = NumberLiterals.read(crossing);
        return read != null ? read : crossing;
    }

    @Override
    public boolean readsBodies() {
        return true;
    }

    @Override
    public TypedSpec inBody(TypedCrossTypeComparison crossing) {
        return inQuery(crossing);
    }

    /** A generic position is typed only when the query runs (the equality rule's second item), so a comparison of
     *  two of them is no compile error, whatever this call's columns are: it is the call, compared when the query
     *  runs (the database's comparison until the run-time comparison is built, step 6). A number literal against a
     *  generic position still reads as this call's column type, which is what a literal is for. Found by step 4's
     *  measurement, 2026-11-26: the timeseries library's meeting of two lines compares their period columns, and
     *  refuses a day against a date-time itself, when the query runs, naming the fix; a compile error here pre-empted
     *  that refusal. */
    @Override
    public TypedSpec inGenericBody(TypedCrossTypeComparison crossing) {
        TypedSpec read = NumberLiterals.read(crossing);
        return read != null ? read : crossing.comparison();
    }
}
