// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.driver;

import com.legend.testexecutionplan.HostJudgment;
import com.legend.ExecuteOptions;
import com.legend.TypedQuery;
import com.legend.base.Nullable;
import com.legend.compiler.spec.typed.TypedSpec;
import com.legend.error.AssertFailed;
import com.legend.exec.PlanRunner;
import com.legend.executionplan.ExecutionPlan;
import com.legend.testexecutionplan.TestPlan;
import com.legend.judge.AssertListener;
import com.legend.judge.TypedSide;
import com.legend.testplan.TestDoor;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * THE AGREEMENT JUDGE'S ASSERTS, PLANNED BEFORE THE PROGRAM IS (the walker-deletion program, 2026-11-02; the plan path
 * program's agreement gate, step 6, on the program road): every statement-root assert of a body judged by BOTH judges
 * ({@link ExecuteOptions.JudgeMode#BOTH}, channel B) is ONE verdict step of the body's plan that carries what both
 * judges need -- the host judge's base sides ({@link HostAsserts}) and the database judge's verdict statement over
 * them ({@link DatabaseAsserts#planEach}: the assert's own root, its wrapper and, where it has several members, their
 * fused statement), the host's judgment beside the plan with its judge. The runner reads the sides for the host and the
 * rows for the database at the assert's turn ({@code PlanRunner.Judging.BOTH}); the router's sink judges both and holds
 * the agreement rule ({@link AgreementArm#agree}: the host's verdict of record, the database's checked; a database
 * decline or UNJUDGED told to the listener and a failure of the assert, never the host's verdict alone; a database
 * statement that errors a failure naming both), so the walk's agreement arm and this step decide alike.
 *
 * <p>As the walk's agreement arm: no batch is held across asserts and every frame runs at its let (the host's check
 * step), each assert's database statement reading the frames' CTEs. Null where the host judge's planner plans an
 * assert of the body as no step ({@link HostAsserts#plan} null): the body is refused by name.
 */
final class AgreementAsserts implements TestDoor.Asserts {

    /** One assert: its host half, its database half (null: the database judge declines it) and its steps, the last
     *  carrying the pairing ({@link com.legend.testexecutionplan.Agreement}) and the database half's root loop -- the
     *  run reads both from the plan ({@link AgreementRuns}; the plan-as-data design's step 6, 2026-11-08). */
    static final class One {
        final HostAsserts.Planned host;
        final DatabaseAsserts.@Nullable Each database;
        /** The assert's steps, in order: its one step; an assertError over an assert's two (the host's own, then the
         *  database's raising one). */
        final List<TestPlan.Verdicts> steps;

        /** An assertError's steps for both judges (S135): the database's raising verdict, whose base the host reads --
         *  but where the function IS an assert, each judge decides that assert (2026-11-05, the deep audit: "both"
         *  asks both): the host's own step first, its sides read and judged as the host judge judges an assertError
         *  over an assert, then the database's raising step, its statement comparing what the database's verdict of the
         *  inner assert raised; the agreement rule decides once both are in. */
        One(HostAsserts.Planned host, TestPlan.Verdicts raising) {
            this.host = host;
            this.database = null;
            boolean overAnAssert = HostJudgment.core(host.assertion().check().judgment())
                    instanceof HostJudgment.AssertError ae && ae.inner() != null;
            TestPlan.Verdict hostVerdict = overAnAssert ? host.verdict() : raising.verdicts().get(0);
            TestPlan.Verdicts step = raising.withAgreement(new com.legend.testexecutionplan.Agreement(hostVerdict.id(),
                    raising.verdicts().get(0).id()));
            this.steps = overAnAssert ? List.of(host.step(), step) : List.of(step);
        }

        One(HostAsserts.Planned host, DatabaseAsserts.@Nullable Each database) {
            this.host = host;
            this.database = database;
            TestPlan.Verdict hv = host.verdict();
            TestPlan.Verdicts group = database == null ? null : database.group();
            TestPlan.Verdicts step;
            TestPlan.Verdict hostVerdict;
            if (group == null) {
                step = host.step();
                hostVerdict = hv;
            } else {
                // the host's base on the database's first member: one verdict carries both judges' statements
                TestPlan.Verdict first = group.verdicts().get(0);
                // it replaces the database's first member in the plan, so it keeps that member's id
                TestPlan.Verdict both = new TestPlan.Verdict(first.id(), hv.name(), hv.arm(), hv.base(),
                        first.wrapper(), hv.message()).withHost(hv.host());
                List<TestPlan.Verdict> verdicts = new ArrayList<>(group.verdicts().size());
                verdicts.add(both);
                verdicts.addAll(group.verdicts().subList(1, group.verdicts().size()));
                step = new TestPlan.Verdicts(verdicts, group.fused(), group.savepoint(), group.rules());
                hostVerdict = both;
            }
            // the database half's root loop rides the step it closes (null: the database judge declines the assert)
            this.steps = List.of(step.withRoots(database == null ? null : database.planned().loop())
                    .withAgreement(new com.legend.testexecutionplan.Agreement(hostVerdict.id(),
                            com.legend.testexecutionplan.RootLoop.NO_VERDICT)));
        }
    }

    private final HostAsserts host;
    private final Map<Integer, One> byStatement;
    /** The branches (the asserts-as-plan-nodes program, step 4), by the statement's place: the host half's branch,
     *  each side's asserts assembled for both judges. */
    private final Map<Integer, TestPlan.Branch> branches;
    private AgreementAsserts(HostAsserts host, Map<Integer, One> byStatement, Map<Integer, TestPlan.Branch> branches) {
        this.host = host;
        this.byStatement = Collections.unmodifiableMap(new LinkedHashMap<>(byStatement));
        this.branches = Collections.unmodifiableMap(new LinkedHashMap<>(branches));
    }

    /**
     * {@code typed}'s asserts planned for both judges on {@code env} (the body's environment; its session the program's
     * one target), {@code bodyEnv} the body's own where it is SYSTEM-ROUTED ({@link HostAsserts#plan}'s): the host half
     * as the host judge plans it, then each assert's database half as the walk's agreement arm asks it. Null where the
     * host half plans none (the walk's).
     */
    static @Nullable AgreementAsserts plan(TypedQuery typed, String runtime, BodyDriver.ExecEnv env,
            BodyDriver.@Nullable ExecEnv bodyEnv, boolean systemRouted) {
        BodyDriver.ExecEnv hostEnv = env.withOptions(env.options().withJudgeMode(ExecuteOptions.JudgeMode.HOST));
        HostAsserts host = HostAsserts.plan(typed, runtime, hostEnv, bodyEnv == null ? null
                : bodyEnv.withOptions(bodyEnv.options().withJudgeMode(ExecuteOptions.JudgeMode.HOST)));
        if (host == null) {
            return null;
        }
        BodyDriver.ExecEnv databaseEnv = env.withOptions(env.options().withJudgeMode(
                ExecuteOptions.JudgeMode.DATABASE));
        return assemble(host, typed, runtime, databaseEnv, systemRouted, List.of());
    }

    /** {@code host}'s asserts over {@code typed}'s statements (following {@code lets}) each joined by its database
     *  half; a branch the host half planned assembled side by side, each side a body of its own over the lets the
     *  host half planned it over. */
    private static AgreementAsserts assemble(HostAsserts host, TypedQuery typed, String runtime,
            BodyDriver.ExecEnv databaseEnv, boolean systemRouted, List<TypedSpec> lets) {
        Map<Integer, DatabaseAsserts.Each> database = DatabaseAsserts.planEach(typed, runtime, databaseEnv,
                systemRouted, host::frameStamp, lets);
        Map<Integer, One> byStatement = new LinkedHashMap<>();
        Map<Integer, TestPlan.Branch> branches = new LinkedHashMap<>();
        for (int i = 0; i < typed.body().size(); i++) {
            HostAsserts.Branched b = host.branch(i);
            if (b != null) {
                AgreementAsserts then = assemble(b.then(), AssertBranch.body(typed, b.parts().then()), runtime,
                        databaseEnv, systemRouted, b.lets());
                AgreementAsserts otherwise = assemble(b.otherwise(), AssertBranch.body(typed, b.parts().otherwise()),
                        runtime, databaseEnv, systemRouted, b.lets());
                branches.put(i, new TestPlan.Branch(b.condition(), then.steps(b.parts().then().size()),
                        otherwise.steps(b.parts().otherwise().size())));
                continue;
            }
            HostAsserts.Planned p = host.planned(i);
            if (p != null) {
                TestPlan.Verdicts raising = HostJudgment.core(p.assertion().check().judgment())
                        instanceof HostJudgment.AssertError
                        ? DatabaseAsserts.errorVerdictFor(p, typed.body().get(i), lets, typed.specs(), databaseEnv)
                        : null;
                byStatement.put(i, raising != null ? new One(p, raising)
                        : new One(p, p.assertion().beforeTheArm() != null ? null : database.get(i)));
            }
        }
        return new AgreementAsserts(host, byStatement, branches);
    }

    @Override
    public @Nullable List<? extends TestPlan.TestStep> at(int statement) {
        TestPlan.Branch b = branches.get(statement);
        if (b != null) {
            return List.of(b);
        }
        One one = byStatement.get(statement);
        return one == null ? null : one.steps;
    }

    @Override
    public TestPlan.Judge judge() {
        return TestPlan.Judge.AGREEMENT;
    }

    /** The steps of this body's {@code statements} statements, in order. */
    private List<TestPlan.TestStep> steps(int statements) {
        List<TestPlan.TestStep> out = new ArrayList<>();
        for (int i = 0; i < statements; i++) {
            List<? extends TestPlan.TestStep> at = at(i);
            if (at == null) {
                throw new IllegalStateException("a branch side's statement " + i + " planned no step");
            }
            out.addAll(at);
        }
        return out;
    }

    @Override
    public @Nullable String frameStamp(int statement) {
        return host.frameStamp(statement);
    }

}
