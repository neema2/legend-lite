// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0
package com.legend.driver;

import com.legend.compiler.spec.SpecCompiler;
import com.legend.compiler.spec.typed.TypedSpec;
import com.legend.exec.PlanRunner;
import com.legend.exec.PlanSessions;
import com.legend.exec.Sessions;
import com.legend.executionplan.ExecutionPlan;
import com.legend.judge.TypedSide;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * The host judge's sides as a verdict step's BASE (the planner-driver seam program's S2f, 2026-10-30; the plan's
 * §3.3): an arm names its sides before any run -- each the argument and the three flags its fetch passed (the
 * identity lane, the fold, the splice hook) -- they are planned ({@link JudgePlans#plannedSide}) into one
 * {@link ExecutionPlan.Verdict}'s base (typed rows, a keyed result, or the program's constant), run through
 * {@link PlanRunner#program} under {@link PlanRunner.Judging#HOST} on the sides' one session, and each is handed
 * back by its position, shaped as the one-side route shapes it ({@link JudgePlans#shape}). One statement per side,
 * in the arm's order, under the arm's origin: the sequence the one-side route sent. The host judge stays the
 * receiver: this class carries rows to it and judges nothing (the user's ruling D7: the host judge is the exception
 * the receiver calls, never Java in the plan). A group of constants alone sends nothing: each is read from its own
 * text, as the runner hands a constant side's.
 */
final class HostSides {

    private HostSides() {
    }

    /** One side an arm asks for: the argument and the flags {@link JudgePlans#evalSide} took for it. */
    record Ask(TypedSpec arg, boolean identity, boolean fold,
            AssertVerdicts.@com.legend.base.Nullable SpliceHook hook) {
    }

    /**
     * An arm PLANNED (the test-is-the-product plan's M6, brief B3): the sides it asks for, in its order, and its
     * judgment over them once they ran -- the two halves of a host arm, so a side is never planned at judging time.
     * The walk runs them back to back ({@link #run}); the test door's program places the asks as a verdict step's base
     * ({@link #plan}) and the body router judges at that step. The judgment is DATA (the plan's M8a, 2026-11-01): one
     * {@link HostJudgment} record naming the comparison, with the few facts the comparison needs beyond the rows -- a
     * side's static type, its order view, a class's key tree, the subtype relation -- every one read from the typed
     * tree and the model when the arm is PLANNED, never at judging time; {@link HostJudge#judge} runs the one function
     * the record names over the rows. The answer is null where the arm declines the sides it read (the walk's generic
     * path continues; the program road refuses it by name, a shape {@code AssertVerdicts.planHost} leaves on the walk).
     */
    record Check(List<Ask> asks, HostJudgment judgment) {
        Check {
            asks = List.copyOf(asks);
        }
    }

    /** {@code check} run in place (the walk): its sides fetched, then judged; null where there is no check (the arm
     *  declined the shape before any side) or the judgment declined. */
    static @com.legend.base.Nullable TypedSide run(String name, @com.legend.base.Nullable Check check,
            List<TypedSpec> letPrefix, SpecCompiler specs, BodyDriver.ExecEnv env) {
        if (check == null) {
            return null;
        }
        // judged by the run's rules (ExecuteOptions.judgingRules), as the program road's step names them
        return HostJudge.judge(check.judgment(), fetchChecked(name, check, letPrefix, specs, env),
                env.options().judgingRules());
    }

    /** {@code check}'s sides fetched in place (the walk): a caller's MESSAGE (HostJudgment.Messaged, the check's last
     *  side) planned and read only where the judgment asks for it -- the assert failing -- as upstream evaluates it only
     *  there and as the program road reads it (the assert bar, 2026-11-04: the walk's message read first lost the
     *  corpus's 55 test-data generation bodies, whose messages read a query the program cannot plan). */
    static List<TypedSide> fetchChecked(String name, Check check, List<TypedSpec> letPrefix, SpecCompiler specs,
            BodyDriver.ExecEnv env) {
        if (!(check.judgment() instanceof HostJudgment.Messaged m) || m.unplanned() != null
                || m.innerSides() != check.asks().size() - 1) {
            return fetch(name, check.asks(), letPrefix, specs, env);
        }
        List<TypedSide> inner = fetch(name, check.asks().subList(0, m.innerSides()), letPrefix, specs, env);
        Ask message = check.asks().get(m.innerSides());
        return lazyLast(inner, () -> fetch(name, List.of(message), letPrefix, specs, env).get(0));
    }

    /** {@code inner}, then one more side read through {@code last} only where it is asked for (once). */
    private static List<TypedSide> lazyLast(List<TypedSide> inner, java.util.function.Supplier<TypedSide> last) {
        int n = inner.size();
        return new java.util.AbstractList<TypedSide>() {
            private @com.legend.base.Nullable TypedSide read;

            @Override
            public TypedSide get(int i) {
                if (i < n) {
                    return inner.get(i);
                }
                if (i != n) {
                    throw new IndexOutOfBoundsException(i);
                }
                if (read == null) {
                    read = last.get();
                }
                return read;
            }

            @Override
            public int size() {
                return n + 1;
            }
        };
    }

    /** {@code check}'s sides planned as one verdict's base, with the check they plan: a caller's message whose
     *  planning raised is no side, its failure the judgment's raise where the assert fails ({@link #withoutTheMessage}). */
    record PlannedCheck(Check check, Planned planned) {
    }

    static PlannedCheck planChecked(String name, Check check, List<TypedSpec> letPrefix, SpecCompiler specs,
            BodyDriver.ExecEnv env) {
        try {
            return new PlannedCheck(check, plan(name, check.asks(), letPrefix, specs, env));
        } catch (RuntimeException e) {
            Check c = withoutTheMessage(check, e);
            if (c == null) {
                throw e;
            }
            return new PlannedCheck(c, plan(name, c.asks(), letPrefix, specs, env));
        }
    }

    /** {@code check} with its caller's message taken off its sides, {@code failure} (the message's planning raised it,
     *  or the sides' did -- planning the check's own sides again raises theirs) raised where the assert fails: upstream
     *  evaluates the message only there (2026-11-04, the assert bar); null where {@code check} carries no message
     *  side. */
    private static @com.legend.base.Nullable Check withoutTheMessage(Check check, RuntimeException failure) {
        if (!(check.judgment() instanceof HostJudgment.Messaged m) || m.unplanned() != null
                || m.innerSides() != check.asks().size() - 1) {
            return null;
        }
        return new Check(check.asks().subList(0, m.innerSides()),
                new HostJudgment.Messaged(m.inner(), m.innerSides(), failure));
    }

    /** {@code check} run in place where its judgment always decides (an arm with no declining shape). */
    static TypedSide judged(String name, Check check, List<TypedSpec> letPrefix, SpecCompiler specs,
            BodyDriver.ExecEnv env) {
        return java.util.Objects.requireNonNull(run(name, check, letPrefix, specs, env),
                () -> name + ": an arm that always decides declined its sides");
    }

    /** An arm's sides PLANNED, in its order: one verdict's base and what shaping each needs once it ran; {@code on}
     *  the session the group runs on, null where every side is a constant (nothing sent: each is read from its own
     *  text, and no side is a to-one reader's). */
    record Planned(List<JudgePlans.PlannedSide> sides, List<ExecutionPlan.Side> base,
            Sessions.@com.legend.base.Nullable Session on) {
        Planned {
            sides = List.copyOf(sides);
            base = List.copyOf(base);
        }

        /** No side: an assert planned as a raise at its turn asks for none. */
        static final Planned NONE = new Planned(List.of(), List.of(), null);
    }

    /** The arm's sides planned as one verdict's base, run nowhere yet. */
    static Planned plan(String name, List<Ask> asks, List<TypedSpec> letPrefix, SpecCompiler specs,
            BodyDriver.ExecEnv env) {
        List<JudgePlans.PlannedSide> planned = new ArrayList<>(asks.size());
        List<ExecutionPlan.Side> base = new ArrayList<>(asks.size());
        // the group's one session: where a side reading a store was routed (two such sides on two sessions are
        // refused by name: the one-target rule, the plan's §3.2); a side reading no store runs on ANY session
        // (BodyDriver.WrappedSide), so it joins the store-reading side's -- the metamodel bodies pair a side on the
        // system database with a literal side, 33 of them on the DuckDB host pass
        Sessions.Session on = null;
        Sessions.Session any = null;
        for (Ask a : asks) {
            JudgePlans.PlannedSide p = JudgePlans.plannedSide(a.arg(), letPrefix, specs, env, a.identity(),
                    a.fold(), a.hook());
            planned.add(p);
            base.add(p.side());
            if (p.wrapped() != null) {
                Sessions.Session s = p.wrapped().session();
                if (p.wrapped().storeFree()) {
                    any = any == null ? s : any;
                } else if (on == null) {
                    on = s;
                } else if (on != s) {
                    throw new com.legend.error.NotImplementedException("the sides of " + name
                            + " read stores on two sessions: a verdict step's sides run on one");
                }
            }
        }
        return new Planned(planned, base, on != null ? on : any);
    }

    /** The rows the runner read for {@code planned}'s base, each shaped by its position as the arm takes it; the
     *  to-one reader's check where a side was sent. */
    static List<TypedSide> shaped(String name, Planned planned, List<PlanRunner.Rows> rows) {
        if (rows.size() != planned.sides().size()) {
            throw new IllegalStateException(name + ": the runner handed " + rows.size() + " sides for "
                    + planned.sides().size() + " asked");
        }
        List<TypedSide> out = new ArrayList<>(rows.size());
        for (int i = 0; i < rows.size(); i++) {
            JudgePlans.PlannedSide p = planned.sides().get(i);
            TypedSide shaped = JudgePlans.shape(p, rows.get(i));
            if (planned.on() != null) {
                JudgePlans.toOneReader(p.root(), shaped);
            }
            out.add(shaped);
        }
        return out;
    }

    /** {@code planned}'s sides shaped for its verdict: the rows the runner read for the base, and, where the verdict
     *  carries its caller's MESSAGE ({@code message} non-null: the planned check's last side), that side read through
     *  {@code message} only where the judgment asks for it -- the assert failing (HostJudgment.Messaged), as upstream
     *  evaluates the message only there; an error reading it is the failure (the assert bar, 2026-11-04). */
    static List<TypedSide> shaped(String name, Planned planned, List<PlanRunner.Rows> rows,
            java.util.function.@com.legend.base.Nullable Supplier<PlanRunner.Rows> message) {
        if (message == null) {
            return shaped(name, planned, rows);
        }
        int n = rows.size();
        List<TypedSide> inner = shaped(name, new Planned(planned.sides().subList(0, n), planned.base().subList(0, n),
                planned.on()), rows);
        JudgePlans.PlannedSide asked = planned.sides().get(n);
        return lazyLast(inner, () -> {
            TypedSide shaped = JudgePlans.shape(asked, message.get());
            if (planned.on() != null) {
                JudgePlans.toOneReader(asked.root(), shaped);
            }
            return shaped;
        });
    }

    /** The arm's sides, in its order: planned, run as one verdict's base, shaped. */
    static List<TypedSide> fetch(String name, List<Ask> asks, List<TypedSpec> letPrefix, SpecCompiler specs,
            BodyDriver.ExecEnv env) {
        return fetched(name, plan(name, asks, letPrefix, specs, env), env);
    }

    /** {@code planned}'s sides run as one verdict's base, shaped. */
    private static List<TypedSide> fetched(String name, Planned planned, BodyDriver.ExecEnv env) {
        Sessions.Session on = planned.on();
        List<TypedSide> out = new ArrayList<>(planned.sides().size());
        if (on == null) {
            for (JudgePlans.PlannedSide p : planned.sides()) {
                out.add(JudgePlans.shape(p, JudgePlans.runSide(p, env)));
            }
            return out;
        }
        ExecutionPlan.Verdict verdict = new ExecutionPlan.Verdict(name, name, planned.base(), null);
        ExecutionPlan.Verdicts group = new ExecutionPlan.Verdicts(List.of(verdict), null, env.options().judgingRules());
        List<List<PlanRunner.Rows>> got = new ArrayList<>(1);
        try {
            PlanRunner.program(new ExecutionPlan(List.of(), group), Map.of(), PlanSessions.of(on, env.trace()),
                    PlanRunner.Judging.HOST, new Sink(got));
        } catch (java.io.IOException e) {
            throw new java.io.UncheckedIOException(e);
        }
        if (got.size() != 1) {
            throw new IllegalStateException(name + ": the runner handed " + got.size() + " verdicts' sides for one");
        }
        return shaped(name, planned, got.get(0));
    }

    /** The receiver of one verdict's sides; a verdict group of host sides answers no rows and is judged by no
     *  database. */
    private static final class Sink implements PlanRunner.ProgramSink {
        private final List<List<PlanRunner.Rows>> got;

        Sink(List<List<PlanRunner.Rows>> got) {
            this.got = got;
        }

        @Override
        public void sides(ExecutionPlan.Verdict verdict, List<PlanRunner.Rows> sides) {
            got.add(sides);
        }

        @Override
        public void verdict(ExecutionPlan.Verdict verdict, @com.legend.base.Nullable List<Object> row) {
            throw new IllegalStateException("the host judge's sides are judged by no database");
        }

        @Override
        public void fused(ExecutionPlan.Verdicts group, com.legend.error.@com.legend.base.Nullable DataError rejected) {
            throw new IllegalStateException("a group of host sides has no fused statement");
        }

        @Override
        public void columns(List<ExecutionPlan.Column> columns) {
            throw new IllegalStateException("a group of host sides answers no rows");
        }

        @Override
        public void batch(com.legend.result.Batch batch) {
            throw new IllegalStateException("a group of host sides answers no rows");
        }

        @Override
        public void end() {
            throw new IllegalStateException("a group of host sides answers no rows");
        }
    }
}
