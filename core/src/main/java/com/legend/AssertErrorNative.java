// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend;

import com.legend.compiler.spec.SpecCompiler;
import com.legend.compiler.spec.typed.TypedCInteger;
import com.legend.compiler.spec.typed.TypedCString;
import com.legend.compiler.spec.typed.TypedCollection;
import com.legend.compiler.spec.typed.TypedLambda;
import com.legend.compiler.spec.typed.TypedNativeCall;
import com.legend.compiler.spec.typed.TypedSpec;
import com.legend.exec.ExecutionResult;

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
 * failure spellings (assertError.pure:24-26). Adjudication of
 * orchestration artifacts is host-side by charter (the
 * {@code exec.PureAsserts} precedent).
 */
final class AssertErrorNative {

    private AssertErrorNative() {
    }

    static ExecutionResult run(TypedNativeCall ae, List<TypedSpec> letPrefix,
            SpecCompiler specs, StatementExecutor.ExecEnv env,
            java.util.Deque<String> frames) {
        if (!(ae.args().get(0) instanceof TypedLambda f)
                || !f.parameters().isEmpty()) {
            throw new com.legend.error.NotImplementedException(
                    "assertError whose function argument is not a"
                    + " zero-parameter lambda literal");
        }
        if (!(ae.args().get(1) instanceof TypedCString exp)) {
            throw new com.legend.error.NotImplementedException(
                    "assertError whose message argument is not a string"
                    + " literal");
        }
        String expected = exp.value();
        Long expectedLine = optionalInt(ae, 2);
        Long expectedColumn = optionalInt(ae, 3);
        // the seam: the database's error arrives as DataError whose
        // CAUSE is the unwrapped (Positioned when the raise was ours)
        String caughtMessage = null;
        Throwable caughtCause = null;
        try {
            StatementExecutor.executeStatements(f.body(),
                    new ArrayList<>(letPrefix), specs, env);
        } catch (com.legend.error.DataError e) {
            caughtMessage = String.valueOf(e.getMessage());
            caughtCause = e.getCause();
        } catch (com.legend.error.ModelException e) {
            // A DEFERRED-BODY guard (the lowering's dynamic validations,
            // e.g. timeBucket's duration-unit check): interpreted pure
            // raises these lazily at EVAL, and for the assertError
            // lambda our lowering IS the eval — the guard's message is
            // the raised error (spec witness: standard-suite
            // testTimeBucketSeconds/Minutes/Hours, which the engine's
            // relational executor passes the same way). Every other
            // exception kind stays LOUD — walls are never adjudicated.
            caughtMessage = String.valueOf(e.getMessage());
        }
        if (caughtMessage == null) {
            // interpreted AssertError.java: PureAssertFail("No error was
            // thrown") — a FAIL, not an orchestration error
            throw new com.legend.error.AssertFailed("No error was thrown");
        }
        // B7 (RaisedErrors): messages arrive ALREADY clean — the
        // Executor funnel unwraps the transport envelope from
        // platform-raised text (provenance-sentinel scoped); a native
        // error keeps its class and envelope, and a mismatch against
        // pure's expectation is then an HONEST failure, never a strip
        // coincidence. The old broad prefix regex is deleted.
        String actual = caughtMessage;
        if (!actual.equals(expected)) {
            // assertError.pure:24 — the /4 body's assertEquals format,
            // verbatim
            throw new com.legend.error.AssertFailed(
                    "Execution error message mismatch.\nThe actual message"
                    + " was \"" + actual + "\"\nwhere the expected"
                    + " message was:\"" + expected + "\"");
        }
        // POSITION adjudication (leg 2, assertError.pure:25-26 — message
        // first, then line, then column, each only when expected): the
        // raise emission threads the raising call's source span through
        // the provenance envelope (PureSql.raise -> RaisedErrors
        // .Positioned). Interpreted pure's matcher receives the raising
        // expression's source info the same way (AssertError.java:68).
        // An expectation over an error WITHOUT a captured span is LOUD —
        // interpreted si.line->toOne() on empty raises too, and a quiet
        // pass here would launder a native error's missing provenance.
        if (expectedLine != null || expectedColumn != null) {
            if (!(caughtCause instanceof com.legend.exec.RaisedErrors.Positioned p)) {
                throw new com.legend.error.AssertFailed(
                        "assertError line/column: the caught error carries"
                        + " no source position (a native database error, or"
                        + " a raise emission without provenance)");
            }
            if (expectedLine != null && p.line() != expectedLine) {
                // assertError.pure:25 — the /4 body's format, verbatim
                throw new com.legend.error.AssertFailed(
                        "Execution error line mismatch. Actual: " + p.line()
                        + " where expected: " + expectedLine);
            }
            if (expectedColumn != null && p.column() != expectedColumn) {
                // assertError.pure:26 — the /4 body's format, verbatim
                throw new com.legend.error.AssertFailed(
                        "Execution error column mismatch. Actual: "
                        + p.column() + " where expected: " + expectedColumn);
            }
        }
        return new ExecutionResult.Scalar(Boolean.TRUE,
                com.legend.compiler.element.type.Type.Primitive.BOOLEAN);
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

    /** Strip the backend's error-kind prefix ({@code "Invalid Input
     * Error: "} etc. — single-line kind, anchored at the start): the
     * pure-level MESSAGE is what the spec compares. */
}
