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
        if (ae.args().get(1) instanceof TypedLambda) {
            // upstream's matcher form, assertError(f, {message, sourceInformation | ...}): no matcher is planned (8
            // calls upstream, all in legend-pure's own tests; item 3's S8: until then refused as a computed message)
            throw new com.legend.error.NotImplementedException("assertError with a matcher function: not planned");
        }
        // the expected text is COMPARED: the program's own token (a literal, a let bound to one), never a join the
        // compiler would compute (item 3's S7, the user's Q-D and Q-S7b: a compared constant is the database's, a joined
        // expected text refused by name until a use appears -- none on any lane)
        Object expected = ae.args().get(1) instanceof TypedCString exp ? exp.value()
                : com.legend.compiler.spec.typed.Literals.tokens(ae.args().get(1), lets);
        if (!(expected instanceof String message)) {
            throw new com.legend.error.NotImplementedException(com.legend.compiler.spec.typed.Literals.isConstant(
                    ae.args().get(1), lets)
                    ? "assertError whose expected message is a join of literals: a compared text is the database's to"
                            + " compute, not planned"
                    : "assertError whose message argument is computed at run time (not a String the compiler folds)");
        }
        Long line = optionalInt(ae, 2);
        Long column = optionalInt(ae, 3);
        if (com.legend.testexecutionplan.HostJudgment.AssertError.expectsUnraised(message)
                && (line != null || column != null)) {
            // upstream compares the assertError call's own position there (assertError.pure:73), which no plan reads
            // (item 3's S8, Q-B: refused by name; no use upstream)
            throw new com.legend.error.NotImplementedException("assertError expecting \"" + message
                    + "\" at a line or column: upstream compares the assertError call's own position"
                    + " (assertError.pure:73); not planned");
        }
        return new Args(f, message, line, column);
    }

    /** The error {@code f} raised, as the arm catches it: an error upstream raises when {@code f} runs
     *  ({@code Failures.atItsTurn}: a database's error, an assert's failure, a code raised at run), as upstream's
     *  assertError catches it (the BE audit's finding 6, 2026-11-21: until then a DataError or a ModelException alone,
     *  so a row's cell offset escaped the arm); any other is null here, and refuses the body where its caller plans
     *  it (a mapping error among them: the user's option 1, register S393). */
    static com.legend.testexecutionplan.@com.legend.base.Nullable Failure caught(RuntimeException e, String where) {
        // the kinded part as data: an audience's words of it are worded at the verdict's turn, from the record (stage
        // B's B1, 2026-11-20, errors as records: until then the plan's style worded it here, PlanStyle.errorWords)
        return com.legend.testexecutionplan.Failures.atItsTurn(e) ? com.legend.testexecutionplan.Failures.of(e, where)
                : null;
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
