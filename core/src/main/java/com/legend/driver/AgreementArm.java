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
 * listener and the host decides; a database statement that errors is a failure, since the judges did not agree.
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
            // the host decides, the listener is told; never a disagreement
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
                    if (listener != null) {
                        listener.unjudged(name, unjudged);
                    }
                } else if (hostFailed == null) {
                    throw disagreement(name, "the host judge holds, the database judge fails ("
                            + first(f.failure().getMessage()) + ")");
                }
            }
            case Declined d -> {
                if (listener != null) {
                    listener.declined(name, "database judge: not this shape");
                }
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
    public @com.legend.base.Nullable TypedSide quantifiedVector(String fqn, TypedSpec predMap,
            List<TypedSpec> letPrefix, SpecCompiler specs, BodyDriver.ExecEnv env,
            @com.legend.base.Nullable AssertVerdicts.SpliceHook hook) {
        // the host arm fetches and unrolls by design (null here): the quantified arm then judges both
        return agree(fqn, env, () -> host.quantifiedVector(fqn, predMap, letPrefix, specs, env, hook),
                () -> database.quantifiedVector(fqn, predMap, letPrefix, specs, env, hook));
    }

    @Override
    public @com.legend.base.Nullable TypedSide cellPool(String name, TypedSpec cellsE, TypedSpec cellsA,
            List<TypedSpec> letPrefix, SpecCompiler specs, BodyDriver.ExecEnv env,
            @com.legend.base.Nullable AssertVerdicts.SpliceHook hook) {
        return agree(name, env, () -> host.cellPool(name, cellsE, cellsA, letPrefix, specs, env, hook),
                () -> database.cellPool(name, cellsE, cellsA, letPrefix, specs, env, hook));
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
