// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.driver;

import com.legend.TypedQuery;
import com.legend.base.Nullable;
import com.legend.compiler.spec.SpecCompiler;
import com.legend.compiler.spec.typed.Lets;
import com.legend.compiler.spec.typed.TypedLet;
import com.legend.compiler.spec.typed.TypedSpec;
import com.legend.executionplan.ExecutionPlan;
import com.legend.planner.LetPlanner;
import com.legend.planner.PlanAllocations;
import com.legend.planner.QueryDoor;
import com.legend.testplan.TestDoor;

import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumSet;
import java.util.IdentityHashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * THE HOST JUDGE'S ASSERTS, PLANNED BEFORE THE PROGRAM IS (the test-is-the-product plan's M6, brief B3): each
 * statement-root assert of an assert program ({@code BodyClass.ASSERT_PROGRAM}) planned as the walk plans it --
 * classified ({@link AssertVerdicts#planHost}), its sides planned on the body's environment with the lets before it
 * ({@link HostSides#plan}) -- as one verdict step the test door places at the assert's turn, its judgment beside it
 * here, never in the plan. Planning sends nothing and counts nothing; a body holding an assert of a shape the host
 * checks do not plan yet has none ({@link #plan} answers null) and stays on the walk. An assert whose planning raises
 * is planned as that raise at its turn ({@link AssertVerdicts.HostAssert#raising}): the walk raises it there, after
 * the asserts before it ran and reported.
 */
final class HostAsserts implements TestDoor.Asserts {

    /** One assert planned: its classification and judgment, its sides' plans (what shaping each needs), and its
     *  verdict step. */
    record Planned(AssertVerdicts.HostAssert assertion, HostSides.Planned sides, ExecutionPlan.Verdicts step) {

        ExecutionPlan.Verdict verdict() {
            return step.verdicts().get(0);
        }
    }

    /** The planned asserts by the statement's place in the body. */
    private final Map<Integer, Planned> byStatement;
    /** The same, by the verdict the runner hands back (a record compares by value: two asserts alike are two
     *  verdicts). */
    private final Map<ExecutionPlan.Verdict, Planned> byVerdict;

    private HostAsserts(Map<Integer, Planned> byStatement) {
        this.byStatement = Collections.unmodifiableMap(new LinkedHashMap<>(byStatement));
        Map<ExecutionPlan.Verdict, Planned> verdicts = new IdentityHashMap<>();
        byStatement.values().forEach(p -> verdicts.put(p.verdict(), p));
        this.byVerdict = Collections.unmodifiableMap(verdicts);
    }

    /**
     * {@code typed}'s asserts planned on {@code env} (the body's environment, as the walk makes it), each with the
     * lets before it as the walk holds them -- every let of an assert program the program rides ({@code VALUE}): its
     * handles registered, then the let on the prefix. Null where an assert is of a shape the host checks do not plan
     * yet: the walk runs the body.
     */
    static @Nullable HostAsserts plan(TypedQuery typed, String runtime, BodyDriver.ExecEnv env) {
        SpecCompiler specs = typed.specs();
        QueryDoor.Scan scan = new QueryDoor.Scan(typed, runtime);
        List<TypedSpec> body = typed.body();
        List<TypedSpec> letPrefix = new ArrayList<>();
        Map<Integer, Planned> planned = new LinkedHashMap<>();
        for (int i = 0; i < body.size(); i++) {
            TypedSpec statement = body.get(i);
            boolean last = i == body.size() - 1;
            if (statement instanceof TypedLet let && !last) {
                PlanAllocations.registerHandlesIn(let.name(), LetPlanner.rhs(let), letPrefix, specs, env.plan());
                letPrefix.add(let);
                continue;
            }
            if (!scan.kinds(statement).equals(EnumSet.of(QueryDoor.TestOnly.ASSERT))
                    || statement instanceof TypedLet) {
                continue;
            }
            // the lets before the assert, as they stood at its turn (its judgment reads them after the program ran)
            List<TypedSpec> before = new ArrayList<>(letPrefix);
            BodyDriver.ExecEnv at = BodyDriver.frameReplaceEnv(statement, Map.of(), env, before, specs);
            TypedSpec bare = Lets.bare(statement);
            AssertVerdicts.HostAssert assertion;
            try {
                assertion = AssertVerdicts.planHost(bare, before, specs, at,
                        BodyDriver.spliceHook(Map.of(), before, specs, at));
            } catch (RuntimeException e) {
                // the classification raised: raised at the assert's turn, as the walk raises it there
                assertion = AssertVerdicts.HostAssert.raising(AssertVerdicts.listenerName(bare), null, e);
            }
            if (assertion == null) {
                return null;
            }
            HostSides.Planned sides;
            try {
                sides = HostSides.plan(assertion.name(), assertion.check().asks(), before, specs, at);
            } catch (RuntimeException e) {
                // a side's planning raised: raised at the assert's turn, once its family is counted (the walk plans
                // the sides there, after the asserts before it ran)
                assertion = AssertVerdicts.HostAssert.raising(assertion.listenerName(), assertion.family(), e);
                sides = new HostSides.Planned(List.of(), List.of(), null);
            }
            ExecutionPlan.Verdict verdict = new ExecutionPlan.Verdict(assertion.listenerName(), assertion.name(),
                    assertion.wantEqual(), sides.base(), null);
            planned.put(i, new Planned(assertion, sides, new ExecutionPlan.Verdicts(List.of(verdict), null)));
        }
        return new HostAsserts(planned);
    }

    @Override
    public ExecutionPlan.@Nullable Verdicts at(int statement) {
        Planned p = byStatement.get(statement);
        return p == null ? null : p.step();
    }

    /** The assert the runner handed {@code verdict} for. */
    Planned of(ExecutionPlan.Verdict verdict) {
        Planned p = byVerdict.get(verdict);
        if (p == null) {
            throw new IllegalStateException("a verdict step no assert of the body planned: " + verdict.name());
        }
        return p;
    }
}
