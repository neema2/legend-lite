// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.driver;

import com.legend.compiler.spec.SpecCompiler;
import com.legend.compiler.spec.typed.TypedSpec;
import com.legend.error.AssertFailed;
import com.legend.judge.TypedSide;

import java.util.List;
import java.util.function.Supplier;

/**
 * THE AGREEMENT GATE (the plan path program's step 6, docs/PLAN_PATH_PROGRAM_2026_10_17.md §6, 2026-10-18): under
 * {@link ExecuteOptions.JudgeMode#BOTH} every statement-root assert is judged by both arms -- the host judge over each
 * side's plan result, the database judge over its verdict plan -- and the verdict of record is the host's, which the
 * database's must match. A disagreement FAILS the assert naming both verdicts: a finding on a named test (a verdict
 * statement's composition, a decode misreading a carrier), never a quiet pass. The routing is the host's: a shape the
 * host arm leaves to the generic path (null) is not put to the database arm. A shape the database arm leaves to the
 * generic path, or reports UNJUDGED (its own wall included: a shape its arm names it cannot judge), is told to the
 * listener and FAILS the assert naming why and what the host said -- the host never decides alone (the user,
 * 2026-11-05: no fallback mode); a database statement that errors is a failure, since the judges did not agree.
 */
final class AgreementArm implements VerdictArm {

    static final VerdictArm ARM = new AgreementArm();

    private final VerdictArm host = HostJudge.ARM;
    private final VerdictArm database = DatabaseJudge.ARM;

    private AgreementArm() {
    }

    /** The database arm's outcome, as data. */
    private sealed interface Outcome permits Held, Declined, Failed, Errored {
    }

    private record Held(TypedSide result) implements Outcome {
    }

    private record Declined() implements Outcome {
    }

    private record Failed(AssertFailed failure) implements Outcome {
    }

    private record Errored(RuntimeException failure) implements Outcome {
    }

    private static Outcome databaseOutcome(Supplier<@com.legend.base.Nullable TypedSide> arm) {
        try {
            TypedSide r = arm.get();
            return r == null ? new Declined() : new Held(r);
        } catch (AssertFailed e) {
            return new Failed(e);
        } catch (com.legend.error.NotImplementedException e) {
            // a shape the database judge's arm says it cannot judge (its own wall, named): UNJUDGED by that name --
            // the listener is told and the assert fails as undecided (agree), never as a disagreement
            return new Failed(AssertFailed.unjudged(first(e.getMessage()), String.valueOf(e.getMessage())));
        } catch (RuntimeException e) {
            return new Errored(e);
        }
    }

    private @com.legend.base.Nullable TypedSide agree(String name, BodyDriver.ExecEnv env,
            Supplier<@com.legend.base.Nullable TypedSide> hostArm,
            Supplier<@com.legend.base.Nullable TypedSide> databaseArm) {
        return agree(name, env.assertListener(), hostArm, databaseArm);
    }

    /** Both arms over the same sides; the host's verdict of record, the database's agreement checked. */
    static @com.legend.base.Nullable TypedSide agree(String name,
            com.legend.judge.@com.legend.base.Nullable AssertListener listener,
            Supplier<@com.legend.base.Nullable TypedSide> hostArm,
            Supplier<@com.legend.base.Nullable TypedSide> databaseArm) {
        TypedSide hostHeld;
        AssertFailed hostFailed = null;
        try {
            hostHeld = hostArm.get();
        } catch (AssertFailed e) {
            hostHeld = null;
            hostFailed = e;
        }
        if (hostHeld == null && hostFailed == null) {
            return null;   // not this shape for the host: the generic path continues, the database arm unasked
        }
        Outcome db = databaseOutcome(databaseArm);
        switch (db) {
            case Held h -> {
                if (hostFailed != null) {
                    throw disagreement(name, "the host judge fails (" + first(hostFailed.getMessage())
                            + "), the database judge holds");
                }
            }
            case Failed f -> {
                String unjudged = f.failure().unjudgedReason();
                if (unjudged != null) {
                    com.legend.diagnostics.Diagnostics.judgeDecline("unjudged", name, unjudged);
                    if (listener != null) {
                        listener.unjudged(name, unjudged);
                    }
                    // STRICT (the user, 2026-11-05: "The database judge should not have a fallback mode at all, it
                    // should hard fail if cannot judge something and then we need to fix"): never the host's alone
                    throw undecided(name, "could not judge it (" + unjudged + ")", hostFailed);
                } else if (hostFailed == null) {
                    throw disagreement(name, "the host judge holds, the database judge fails ("
                            + first(f.failure().getMessage()) + ")");
                }
            }
            case Declined d -> {
                com.legend.diagnostics.Diagnostics.judgeDecline("declined", name, "database judge: not this shape");
                if (listener != null) {
                    listener.declined(name, "database judge: not this shape");
                }
                throw undecided(name, "declined it (not this shape)", hostFailed);
            }
            case Errored e -> throw new AssertFailed(name + ": THE JUDGES DID NOT AGREE — the database judge's"
                    + " statement failed (" + e.failure().getClass().getSimpleName() + ": "
                    + first(e.failure().getMessage()) + "), where the host judge "
                    + (hostFailed == null ? "holds" : "fails (" + first(hostFailed.getMessage()) + ")"));
        }
        if (hostFailed != null) {
            throw hostFailed;
        }
        return hostHeld;
    }

    /** The database judge decided nothing: the assert fails naming why, and what the host judge said. */
    private static AssertFailed undecided(String name, String why, @com.legend.base.Nullable AssertFailed hostFailed) {
        return new AssertFailed(name + ": THE DATABASE JUDGE " + why + ", where the host judge "
                + (hostFailed == null ? "holds" : "fails (" + first(hostFailed.getMessage()) + ")"));
    }

    private static AssertFailed disagreement(String name, String how) {
        return new AssertFailed(name + ": THE JUDGES DISAGREE — " + how);
    }

    private static String first(@com.legend.base.Nullable String message) {
        if (message == null) {
            return "";
        }
        String one = message.strip().replaceAll("\\s*\\R\\s*", " | ");
        return one.length() > 400 ? one.substring(0, 400) + "..." : one;
    }

    @Override
    public @com.legend.base.Nullable TypedSide tdsEquivalent(String name, List<TypedSpec> targs,
            List<TypedSpec> letPrefix, SpecCompiler specs, BodyDriver.ExecEnv env,
            @com.legend.base.Nullable AssertVerdicts.SpliceHook hook) {
        return agree(name, env, () -> host.tdsEquivalent(name, targs, letPrefix, specs, env, hook),
                () -> database.tdsEquivalent(name, targs, letPrefix, specs, env, hook));
    }

    @Override
    public TypedSide size(String name, List<TypedSpec> args, List<TypedSpec> letPrefix, SpecCompiler specs,
            BodyDriver.ExecEnv env, @com.legend.base.Nullable AssertVerdicts.SpliceHook hook) {
        return java.util.Objects.requireNonNull(agree(name, env,
                () -> host.size(name, args, letPrefix, specs, env, hook),
                () -> database.size(name, args, letPrefix, specs, env, hook)));
    }

    @Override
    public @com.legend.base.Nullable TypedSide contains(String name, List<TypedSpec> args,
            List<TypedSpec> letPrefix, SpecCompiler specs, BodyDriver.ExecEnv env,
            @com.legend.base.Nullable AssertVerdicts.SpliceHook hook) {
        return agree(name, env, () -> host.contains(name, args, letPrefix, specs, env, hook),
                () -> database.contains(name, args, letPrefix, specs, env, hook));
    }

    @Override
    public TypedSide tolerance(String name, List<TypedSpec> args, List<TypedSpec> letPrefix,
            SpecCompiler specs, BodyDriver.ExecEnv env, @com.legend.base.Nullable AssertVerdicts.SpliceHook hook) {
        return java.util.Objects.requireNonNull(agree(name, env,
                () -> host.tolerance(name, args, letPrefix, specs, env, hook),
                () -> database.tolerance(name, args, letPrefix, specs, env, hook)));
    }

    @Override
    public TypedSide condition(String name, TypedSpec cond, boolean wantTrue, List<TypedSpec> letPrefix,
            SpecCompiler specs, BodyDriver.ExecEnv env, @com.legend.base.Nullable AssertVerdicts.SpliceHook hook) {
        return java.util.Objects.requireNonNull(agree(name, env,
                () -> host.condition(name, cond, wantTrue, letPrefix, specs, env, hook),
                () -> database.condition(name, cond, wantTrue, letPrefix, specs, env, hook)));
    }

    @Override
    public TypedSide empty(String name, TypedSpec arg, boolean wantEmpty, List<TypedSpec> letPrefix,
            SpecCompiler specs, BodyDriver.ExecEnv env, @com.legend.base.Nullable AssertVerdicts.SpliceHook hook) {
        return java.util.Objects.requireNonNull(agree(name, env,
                () -> host.empty(name, arg, wantEmpty, letPrefix, specs, env, hook),
                () -> database.empty(name, arg, wantEmpty, letPrefix, specs, env, hook)));
    }

    @Override
    public TypedSide eq(String name, List<TypedSpec> args, List<TypedSpec> letPrefix, SpecCompiler specs,
            BodyDriver.ExecEnv env, @com.legend.base.Nullable AssertVerdicts.SpliceHook hook) {
        return java.util.Objects.requireNonNull(agree(name, env,
                () -> host.eq(name, args, letPrefix, specs, env, hook),
                () -> database.eq(name, args, letPrefix, specs, env, hook)));
    }

    @Override
    public @com.legend.base.Nullable TypedSide instanceOf(String name, List<TypedSpec> args,
            List<TypedSpec> letPrefix, SpecCompiler specs, BodyDriver.ExecEnv env,
            @com.legend.base.Nullable AssertVerdicts.SpliceHook hook) {
        return agree(name, env, () -> host.instanceOf(name, args, letPrefix, specs, env, hook),
                () -> database.instanceOf(name, args, letPrefix, specs, env, hook));
    }

    @Override
    public @com.legend.base.Nullable TypedSide is(String name, List<TypedSpec> args, List<TypedSpec> letPrefix,
            SpecCompiler specs, BodyDriver.ExecEnv env, @com.legend.base.Nullable AssertVerdicts.SpliceHook hook) {
        return agree(name, env, () -> host.is(name, args, letPrefix, specs, env, hook),
                () -> database.is(name, args, letPrefix, specs, env, hook));
    }

    @Override
    public TypedSide sameElements(String name, List<TypedSpec> args, boolean gridPair,
            List<TypedSpec> letPrefix, SpecCompiler specs, BodyDriver.ExecEnv env,
            @com.legend.base.Nullable AssertVerdicts.SpliceHook hook) {
        return java.util.Objects.requireNonNull(agree(name, env,
                () -> host.sameElements(name, args, gridPair, letPrefix, specs, env, hook),
                () -> database.sameElements(name, args, gridPair, letPrefix, specs, env, hook)));
    }

    @Override
    public TypedSide quantified(String fqn, TypedSpec predMap, boolean wantTrue, String message,
            List<TypedSpec> letPrefix, SpecCompiler specs, BodyDriver.ExecEnv env,
            @com.legend.base.Nullable AssertVerdicts.SpliceHook hook) {
        return java.util.Objects.requireNonNull(agree(fqn, env,
                () -> host.quantified(fqn, predMap, wantTrue, message, letPrefix, specs, env, hook),
                () -> database.quantified(fqn, predMap, wantTrue, message, letPrefix, specs, env, hook)));
    }

    @Override
    public @com.legend.base.Nullable TypedSide quantifiedVector(String fqn, TypedSpec predMap, TypedSpec source,
            com.legend.compiler.spec.typed.TypedLambda lam, TypedSpec root, List<TypedSpec> letPrefix,
            SpecCompiler specs, BodyDriver.ExecEnv env, @com.legend.base.Nullable AssertVerdicts.SpliceHook hook) {
        return agree(fqn, env, () -> host.quantifiedVector(fqn, predMap, source, lam, root, letPrefix, specs, env, hook),
                () -> database.quantifiedVector(fqn, predMap, source, lam, root, letPrefix, specs, env, hook));
    }

    @Override
    public TypedSide perElement(String name, TypedSpec pairs, com.legend.compiler.element.type.ExprType expected,
            com.legend.compiler.element.type.ExprType actual, List<TypedSpec> letPrefix, SpecCompiler specs,
            BodyDriver.ExecEnv env, @com.legend.base.Nullable AssertVerdicts.SpliceHook hook) {
        return java.util.Objects.requireNonNull(agree(name, env,
                () -> host.perElement(name, pairs, expected, actual, letPrefix, specs, env, hook),
                () -> database.perElement(name, pairs, expected, actual, letPrefix, specs, env, hook)),
                "both judges answer a per-element equality");
    }

    @Override
    public TypedSide equals(String name, boolean wantEqual, List<TypedSpec> args, boolean incidental,
            boolean gridPair, List<TypedSpec> letPrefix, SpecCompiler specs, BodyDriver.ExecEnv env,
            @com.legend.base.Nullable AssertVerdicts.SpliceHook hook) {
        return java.util.Objects.requireNonNull(agree(name, env,
                () -> host.equals(name, wantEqual, args, incidental, gridPair, letPrefix, specs, env, hook),
                () -> database.equals(name, wantEqual, args, incidental, gridPair, letPrefix, specs, env, hook)));
    }

    @Override
    public TypedSide gridPair(String name, boolean wantEqual, List<TypedSpec> args, boolean sorted,
            List<TypedSpec> letPrefix, SpecCompiler specs, BodyDriver.ExecEnv env,
            @com.legend.base.Nullable AssertVerdicts.SpliceHook hook) {
        return java.util.Objects.requireNonNull(agree(name, env,
                () -> host.gridPair(name, wantEqual, args, sorted, letPrefix, specs, env, hook),
                () -> database.gridPair(name, wantEqual, args, sorted, letPrefix, specs, env, hook)));
    }

    @Override
    public @com.legend.base.Nullable TypedSide jsonStringsEqual(String name, List<TypedSpec> args,
            List<TypedSpec> letPrefix, SpecCompiler specs, BodyDriver.ExecEnv env,
            @com.legend.base.Nullable AssertVerdicts.SpliceHook hook) {
        return agree(name, env, () -> host.jsonStringsEqual(name, args, letPrefix, specs, env, hook),
                () -> database.jsonStringsEqual(name, args, letPrefix, specs, env, hook));
    }

    @Override
    public TypedSide rendered(String name, boolean wantEqual, List<TypedSpec> args, String form,
            TypedSpec rendered, @com.legend.base.Nullable String eForm, @com.legend.base.Nullable String aForm,
            boolean orderedForm, List<TypedSpec> letPrefix, SpecCompiler specs, BodyDriver.ExecEnv env,
            @com.legend.base.Nullable AssertVerdicts.SpliceHook hook) {
        return java.util.Objects.requireNonNull(agree(name, env,
                () -> host.rendered(name, wantEqual, args, form, rendered, eForm, aForm, orderedForm, letPrefix,
                        specs, env, hook),
                () -> database.rendered(name, wantEqual, args, form, rendered, eForm, aForm, orderedForm, letPrefix,
                        specs, env, hook)));
    }

    @Override
    public void staticallyDecided(String name) {
        host.staticallyDecided(name);
        database.staticallyDecided(name);
    }
}
