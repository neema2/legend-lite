// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0
package com.legend.driver;

import com.legend.compiler.spec.SpecCompiler;
import com.legend.compiler.spec.typed.TypedSpec;
import com.legend.exec.PlanRunner;
import com.legend.executionplan.ExecutionPlan;
import com.legend.testexecutionplan.TestPlan;
import com.legend.judge.TypedSide;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * The host judge's sides as a verdict step's BASE (the planner-driver seam program's S2f, 2026-10-30; the plan's
 * §3.3): an arm names its sides before any run -- each the argument and the three flags its fetch passed (the
 * identity lane, the fold, the splice hook) -- they are planned ({@link JudgePlans#plannedSide}) into one
 * {@link TestPlan.Verdict}'s base (typed rows, a keyed result, or the program's constant), run through
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
            AssertVerdicts.@com.legend.base.Nullable SpliceHook hook, BodyDriver.@com.legend.base.Nullable ExecEnv at) {
        /** An ask planned under the step's environment. */
        Ask(TypedSpec arg, boolean identity, boolean fold, AssertVerdicts.@com.legend.base.Nullable SpliceHook hook) {
            this(arg, identity, fold, hook, null);
        }
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
        // the message's planning failure is read for its text alone (AssertMessages.unevaluated), never raised: its
        // text, not a failure record (the slice 1 audit, 2026-11-08: a class off the closed list must not fail here)
        return new Check(check.asks().subList(0, m.innerSides()), new HostJudgment.Messaged(m.inner(), m.innerSides(),
                String.valueOf(failure.getMessage())));
    }

    /** An arm's sides PLANNED, in its order: one verdict's base and what shaping each needs once it ran; {@code on}
     *  the session the group runs on, null where every side is a constant (nothing sent: each is read from its own
     *  text, and no side is a to-one reader's). */
    record Planned(List<JudgePlans.PlannedSide> sides, List<TestPlan.Side> base,
            ExecutionPlan.@com.legend.base.Nullable Target on) {
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
        List<TestPlan.Side> base = new ArrayList<>(asks.size());
        // the group's one target: where a side reading a store was routed (two such sides on two targets are refused
        // by name: the one-target rule, the plan's §3.2); a side reading no store runs on ANY target
        // (BodyDriver.WrappedSide), so it joins the store-reading side's -- the metamodel bodies pair a side on the
        // system database with a literal side, 33 of them on the DuckDB host pass
        ExecutionPlan.Target on = null;
        ExecutionPlan.Target any = null;
        for (Ask a : asks) {
            // an ask the arm read under an environment of its own (`at`: a text side of a runtime-dialect form, its
            // frame and table replaces rebound; 2026-11-06) is planned there, as the arm read it
            JudgePlans.PlannedSide p = JudgePlans.plannedSide(a.arg(), letPrefix, specs, a.at() != null ? a.at() : env,
                    a.identity(), a.fold(), a.hook());
            planned.add(p);
            base.add(p.side());
            if (p.wrapped() != null) {
                ExecutionPlan.Target s = p.wrapped().target();
                if (p.wrapped().storeFree()) {
                    any = any == null ? s : any;
                } else if (on == null) {
                    on = s;
                } else if (!on.equals(s)) {
                    throw new com.legend.error.NotImplementedException("the sides of " + name
                            + " read stores on two databases: a verdict step's sides run on one");
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
            // a relation's toOne() is the database's to check (D2, 2026-11-07: ToOneGuard in the side's SQL)
            out.add(JudgePlans.shape(p, rows.get(i)));
        }
        return out;
    }

    /** {@code planned}'s BASE sides shaped from the rows the runner read for them: where the verdict carries its
     *  caller's message apart ({@code TestPlan.Verdict.message}, the planned check's last side), the rows are one
     *  fewer than the planned sides, and the message is {@link #message}'s to read. */
    static List<TypedSide> shapedBase(String name, Planned planned, List<PlanRunner.Rows> rows) {
        int n = rows.size();
        return n == planned.sides().size() ? shaped(name, planned, rows)
                : shaped(name, new Planned(planned.sides().subList(0, n), planned.base().subList(0, n), planned.on()),
                        rows);
    }

    /** The reader of a verdict's caller's message: the planned check's last side, read through {@code read} (the
     *  program's reader) and shaped as any side -- only where the judgment asks for it, the assert having failed. */
    static java.util.function.Supplier<TypedSide> message(Planned planned,
            java.util.function.Supplier<PlanRunner.Rows> read) {
        JudgePlans.PlannedSide asked = planned.sides().get(planned.sides().size() - 1);
        return () -> JudgePlans.shape(asked, read.get());
    }
}
