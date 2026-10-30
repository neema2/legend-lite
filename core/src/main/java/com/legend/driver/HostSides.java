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

    /** The arm's sides, in its order: planned, run as one verdict's base, shaped. */
    static List<TypedSide> fetch(String name, List<Ask> asks, List<TypedSpec> letPrefix, SpecCompiler specs,
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
        if (on == null) {
            on = any;
        }
        List<TypedSide> out = new ArrayList<>(asks.size());
        if (on == null) {
            for (JudgePlans.PlannedSide p : planned) {
                out.add(JudgePlans.shape(p, JudgePlans.runSide(p, env)));
            }
            return out;
        }
        ExecutionPlan.Verdict verdict = new ExecutionPlan.Verdict(name, name, true, base, null);
        ExecutionPlan.Verdicts group = new ExecutionPlan.Verdicts(List.of(verdict), null);
        List<List<PlanRunner.Rows>> got = new ArrayList<>(1);
        try {
            PlanRunner.program(new ExecutionPlan(List.of(), group), Map.of(), PlanSessions.of(on, env.trace()),
                    PlanRunner.Judging.HOST, new Sink(got));
        } catch (java.io.IOException e) {
            throw new java.io.UncheckedIOException(e);
        }
        if (got.size() != 1 || got.get(0).size() != planned.size()) {
            throw new IllegalStateException(name + ": the runner handed " + (got.isEmpty() ? 0 : got.get(0).size())
                    + " sides for " + planned.size() + " asked");
        }
        for (int i = 0; i < planned.size(); i++) {
            TypedSide shaped = JudgePlans.shape(planned.get(i), got.get(0).get(i));
            JudgePlans.toOneReader(planned.get(i).root(), shaped);
            out.add(shaped);
        }
        return out;
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
