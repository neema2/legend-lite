// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.testing;

import com.legend.Compiler;
import com.legend.ExecuteOptions;
import com.legend.Execution;
import com.legend.compiler.element.ModelContext;
import com.legend.exec.AssertListener;
import com.legend.exec.Sessions;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.List;

/**
 * A TEST BODY run through the one entry for a program ({@link Execution#runProgram}; the result boundary program's
 * track B, B2) for the tests OF the body driver's judging -- the assert arms, the verdict channel, the effect natives,
 * instance identity (track T, 2026-10-29): the body typed once, run on the caller's connection, every verdict the
 * platform reports kept in order. A test of a VALUE reads the product's output instead ({@link Lean}); a test of a
 * verdict reads what the platform judged, exactly as the corpus runner does: the body typed by the one front door
 * ({@code Compiler.resolveQuery}, then {@code queryResolved}), as the old door and the corpus runner type it. Test
 * code only.
 */
public final class Program {

    private Program() {
    }

    /** One verdict the platform reported: the assert's name, whether it passed, its detail. */
    public record Verdict(String name, boolean pass, @com.legend.base.Nullable String detail) {
    }

    /** What a program left: its verdicts in order, and the last statement's side read through the twin's reader
     *  ({@link Lean#of}); null for a body whose last statement answers nothing. */
    public record Report(List<Verdict> verdicts, Lean.@com.legend.base.Nullable Result last) {

        /** Every verdict passed and there was at least one. */
        public boolean passed() {
            return !verdicts.isEmpty() && verdicts.stream().allMatch(Verdict::pass);
        }

        /** The last statement's side, which must exist. */
        public Lean.Result side() {
            if (last == null) {
                throw new AssertionError("the body's last statement answered nothing");
            }
            return last;
        }
    }

    /** {@code body} (a lambda text: {@code |assertEquals(1, 1)} or {@code {|let x = 1; assert(...);}}) over
     *  {@code model}, on {@code runtime}, on the caller's connection {@code c}, with the platform's default options. */
    public static Report run(String model, String body, @com.legend.base.Nullable String runtime, Connection c) {
        return run(model, body, runtime, c, ExecuteOptions.NONE);
    }

    /** The last statement's side of {@code body} run through the program entry, read through the twin's reader: for
     *  a body only the program entry runs (a read of the system metamodel store, a DDL native, a staged let). */
    public static Lean.Result last(String model, String body, @com.legend.base.Nullable String runtime, Connection c) {
        return run(model, body, runtime, c).side();
    }

    /** {@link #last(String, String, String, Connection)} with the caller's execute {@code options}. */
    public static Lean.Result last(String model, String body, @com.legend.base.Nullable String runtime, Connection c,
            ExecuteOptions options) {
        return run(model, body, runtime, c, options).side();
    }

    /** {@link #run(String, String, String, Connection)} with the caller's execute {@code options} (a judge mode). */
    public static Report run(String model, String body, @com.legend.base.Nullable String runtime, Connection c,
            ExecuteOptions options) {
        ModelContext ctx = Compiler.compileModel(LaneFloor.model(model, c));
        List<Verdict> verdicts = new ArrayList<>();
        AssertListener listener = new AssertListener() {
            @Override
            public void verdict(String assertName, boolean pass, @com.legend.base.Nullable String detail) {
                verdicts.add(new Verdict(assertName, pass, detail));
            }
        };
        // typed as the old door and the corpus runner type a body (Compiler.resolveQuery: the names, the statement
        // splice, the desugars; then queryResolved), never by Compiler.query alone (the T-b audit's finding 7)
        com.legend.protocol.spec.ValueSpecification parsed = Compiler.parseQuery(body);
        List<com.legend.protocol.spec.ValueSpecification> statements =
                parsed instanceof com.legend.protocol.spec.LambdaFunction lf && lf.parameters().isEmpty()
                        ? lf.body() : List.of(parsed);
        com.legend.protocol.spec.ValueSpecification resolved = Compiler.resolveQuery(statements,
                new com.legend.model.ImportScope(List.of()), ctx);
        // the body's last statement streams to the twin's capturing sink (lite's JSON rows), read back by the one
        // reader a query's answer is read by (the doors, 2026-10-29; until then the collected side re-read here)
        java.io.StringWriter out = new java.io.StringWriter();
        Lean.Capturing sink = new Lean.Capturing(out);
        Execution.runProgram(Compiler.queryResolved(ctx, resolved), runtime, Sessions.given(c), listener, null,
                options, sink::of);
        return new Report(List.copyOf(verdicts), sink.answered());
    }
}
