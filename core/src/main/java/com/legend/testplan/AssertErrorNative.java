// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.testplan;

import com.legend.testexecutionplan.Failure;

import com.legend.compiler.spec.typed.TypedCInteger;
import com.legend.compiler.spec.typed.TypedCString;
import com.legend.compiler.spec.typed.TypedCollection;
import com.legend.compiler.spec.typed.TypedLambda;
import com.legend.compiler.spec.typed.TypedNativeCall;
import com.legend.compiler.spec.typed.TypedSpec;

import java.util.ArrayList;
import java.util.List;

/**
 * The {@code assertError} K-arm (One-Platform Plan Phase 4): the platform
 * definition of {@code meta::pure::functions::asserts::assertError} — a
 * {@code PCT.platformOnly} native whose reference contract is interpreted
 * {@code AssertError.java}: run {@code f}; no error is itself the failure
 * ("No error was thrown"); a caught error hands (message, source info) to
 * the matcher. Here {@code f}'s body executes IN THE DATABASE through the
 * ordinary statement pipeline (tenet #1 — the database raises the error);
 * the orchestrator catches the database error, whose message arrives
 * clean from the provenance funnel and whose source position — when the
 * raise emission carried its span ({@code PureSql.raise}) — arrives as
 * {@code RaisedErrors.Positioned} (leg 2: interpreted
 * {@code AssertError.java:68} hands the matcher the raising expression's
 * source info; ours rides the U+001E prefix inside the U+001F envelope),
 * and adjudicates with the pure {@code assertError/4} body's EXACT
 * failure spellings (assertError.pure:24-26). This is the HOST judge's
 * comparison; under the database judge and BOTH the database compares
 * what the function raised in its own verdict statement (the user's
 * ruling S135, 2026-11-05: the host catches, the database judges;
 * {@code VerdictSql.assertError}, {@code PlanRunner}'s raising verdict).
 */
final class AssertErrorNative {

    private AssertErrorNative() {
    }

    /** The parsed arguments of an {@code assertError} call: {@code f}, the expected message, line and column; the walk's
     *  checks, refused by name where the shape is none the arm takes. */
    record Args(TypedLambda f, String expected, @com.legend.base.Nullable Long line,
            @com.legend.base.Nullable Long column) {
    }

    static Args args(TypedNativeCall ae) {
        return args(ae, List.of());
    }

    /** {@link #args(TypedNativeCall)} with the lets before the call: an expected message the compiler folds to a String
     *  over them -- a literal, a let bound to one, a join of them -- is that String (2026-11-04, the asserts-as-plan-nodes
     *  program: {@code let m = 'boom'; assertError(|fail($m), $m)} was refused); one computed at run time is refused by
     *  name, the comparison Java's (ruling N3). */
    static Args args(TypedNativeCall ae, List<com.legend.compiler.spec.typed.TypedSpec> lets) {
        if (!(ae.args().get(0) instanceof TypedLambda f)
                || !f.parameters().isEmpty()) {
            throw new com.legend.error.NotImplementedException(
                    "assertError whose function argument is not a"
                    + " zero-parameter lambda literal");
        }
        Object expected = ae.args().get(1) instanceof TypedCString exp ? exp.value()
                : com.legend.compiler.spec.typed.Literals.fold(ae.args().get(1), lets);
        if (!(expected instanceof String message)) {
            throw new com.legend.error.NotImplementedException(
                    "assertError whose message argument is computed at run time (not a String the compiler folds)");
        }
        return new Args(f, message, optionalInt(ae, 2), optionalInt(ae, 3));
    }

    /** The error {@code f} raised, as the arm catches it: a database's error (its message clean, its cause the raise's
     *  position where the raise was ours) or a deferred guard's model error; any other exception is no answer of the
     *  arm's (a wall stays loud) and is null here. */
    static com.legend.testexecutionplan.@com.legend.base.Nullable Failure caught(RuntimeException e, String where) {
        return e instanceof com.legend.error.DataError || e instanceof com.legend.error.ModelException
                ? com.legend.testexecutionplan.Failures.of(e, where) : null;
    }

    /** {@code Integer[0..1]} argument: a literal, {@code []}, or absent
     * (the 2-arg overload). Null = empty. */
    private static @com.legend.base.Nullable Long optionalInt(TypedNativeCall ae,
            int i) {
        if (ae.args().size() <= i) {
            return null;
        }
        TypedSpec a = ae.args().get(i);
        if (a instanceof TypedCollection c && c.elements().isEmpty()) {
            return null;
        }
        if (a instanceof TypedCInteger n) {
            return n.value().longValue();
        }
        throw new com.legend.error.NotImplementedException(
                "assertError line/column argument must be an integer"
                + " literal or []");
    }

}
