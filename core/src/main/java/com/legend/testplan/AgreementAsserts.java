// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.testplan;

import com.legend.testexecutionplan.HostJudgment;
import com.legend.TypedQuery;
import com.legend.base.Nullable;
import com.legend.compiler.spec.typed.TypedSpec;
import com.legend.testexecutionplan.TestPlan;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * THE AGREEMENT JUDGE'S ASSERTS, PLANNED BEFORE THE PROGRAM IS (the walker-deletion program, 2026-11-02; the plan path
 * program's agreement gate, step 6, on the program road): every statement-root assert of a body judged by BOTH judges
 * ({@link PlanningOptions.JudgeMode#BOTH}, channel B) is ONE verdict step of the body's plan that carries what both
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
            TestPlan.Verdicts step = raising.withAgreements(List.of(new com.legend.testexecutionplan.Agreement(0,
                    hostVerdict.id(), raising.verdicts().get(0).id())));
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
                    .withAgreements(List.of(new com.legend.testexecutionplan.Agreement(0, hostVerdict.id(),
                            com.legend.testexecutionplan.RootLoop.NO_VERDICT))));
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
        BodyDriver.ExecEnv hostEnv = env.withOptions(env.options().withJudgeMode(PlanningOptions.JudgeMode.HOST));
        HostAsserts host = HostAsserts.plan(typed, runtime, hostEnv, bodyEnv == null ? null
                : bodyEnv.withOptions(bodyEnv.options().withJudgeMode(PlanningOptions.JudgeMode.HOST)));
        if (host == null) {
            return null;
        }
        BodyDriver.ExecEnv databaseEnv = env.withOptions(env.options().withJudgeMode(
                PlanningOptions.JudgeMode.DATABASE));
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
                DatabaseAsserts.Each each = raising != null || p.assertion().beforeTheArm() != null ? null
                        : database.get(i);
                byStatement.put(i, raising != null ? new One(p, raising) : new One(p, each));
                joinAppeals(p, each, databaseEnv.appeals());
            }
        }
        return new AgreementAsserts(host, byStatement, branches);
    }

    /** One assert, one appeal (A1, the slice 3 follow-up audit, 2026-11-08): where the host half holds an engine-text
     *  appeal for the assert, each appeal its database half's rows name is a second hearing of it -- joined at run,
     *  decided once. Until then each half held one of its own, and the referee replayed twice. */
    private static void joinAppeals(HostAsserts.Planned p, DatabaseAsserts.@Nullable Each database,
            EngineTextAppeals appeals) {
        if (database == null) {
            return;
        }
        for (com.legend.testexecutionplan.RootLoop.Root r : database.planned().loop().roots()) {
            joinAppeals(p, r, appeals);
        }
    }

    /** {@link #joinAppeals(HostAsserts.Planned, DatabaseAsserts.Each, EngineTextAppeals)} for one root of the
     *  database half's loop, the assert {@code p}'s. */
    private static void joinAppeals(HostAsserts.Planned p, com.legend.testexecutionplan.RootLoop.Root r,
            EngineTextAppeals appeals) {
        if (!(HostJudgment.core(p.assertion().check().judgment()) instanceof HostJudgment.EngineText e)
                || e.appeal() == com.legend.testexecutionplan.RootLoop.NO_APPEAL) {
            return;
        }
        for (com.legend.testexecutionplan.RootLoop.Step s : r.steps()) {
            if (s instanceof com.legend.testexecutionplan.RootLoop.Row row
                    && row.appeal() != com.legend.testexecutionplan.RootLoop.NO_APPEAL) {
                appeals.join(row.appeal(), e.appeal());
            }
        }
    }

    // ---- both judges over the DATABASE judge's real program (E-5c step 6, the user's ruling O5 of 2026-11-13) ----

    /**
     * {@code typed}'s asserts planned for both judges as the DATABASE judge's real program (E-5c step 6, the user's
     * ruling O5 of 2026-11-13; the same parameters as {@link #plan}): the host half as the host judge plans it, then
     * the database judge's segments of fused verdict groups with the host's half attached
     * ({@link DatabaseAsserts#planWithHost}) -- each root's host verdict on its first fused row, its single-value sides
     * columns of that row ({@link TestPlan.Side.FusedCell}), each segment's closing group pairing its roots
     * ({@link #fusedSegment}). No frame runs at its let: the frames' readers read their CTEs in the fused statements,
     * as the database judge's do. Null where the host half plans none, or the database judge no program. Not yet the
     * test planner's: {@link #plan} still is.
     */
    static TestDoor.@Nullable Asserts planFused(TypedQuery typed, String runtime, BodyDriver.ExecEnv env,
            BodyDriver.@Nullable ExecEnv bodyEnv, boolean systemRouted) {
        BodyDriver.ExecEnv hostEnv = env.withOptions(env.options().withJudgeMode(PlanningOptions.JudgeMode.HOST));
        HostAsserts host = HostAsserts.plan(typed, runtime, hostEnv, bodyEnv == null ? null
                : bodyEnv.withOptions(bodyEnv.options().withJudgeMode(PlanningOptions.JudgeMode.HOST)));
        if (host == null) {
            return null;
        }
        BodyDriver.ExecEnv databaseEnv = env.withOptions(env.options().withJudgeMode(
                PlanningOptions.JudgeMode.DATABASE));
        DatabaseAsserts database = DatabaseAsserts.planWithHost(typed, runtime, databaseEnv, systemRouted,
                bodyEnv == null ? null
                        : bodyEnv.withOptions(bodyEnv.options().withJudgeMode(PlanningOptions.JudgeMode.DATABASE)),
                host);
        return database == null ? null : new Fused(database);
    }

    /** The asserts {@link #planFused} planned: the database judge's program, judged by both. */
    private static final class Fused implements TestDoor.Asserts {
        private final DatabaseAsserts database;

        Fused(DatabaseAsserts database) {
            this.database = database;
        }

        @Override
        public @Nullable List<? extends TestPlan.TestStep> at(int statement) {
            return database.at(statement);
        }

        @Override
        public @Nullable String frameStamp(int statement) {
            return database.frameStamp(statement);
        }

        @Override
        public boolean runsFrames() {
            return false;
        }

        @Override
        public TestPlan.Judge judge() {
            return TestPlan.Judge.AGREEMENT;
        }
    }

    /**
     * One segment's groups for both judges (E-5c step 6): {@code planned}, the database judge's batch, its roots
     * {@code roots} in root order. Each root's CARRIER verdict ({@link VerdictBatch.Planned#hostCarriers}) becomes the
     * host's verdict on the database's row -- its id the carrier's, its host facts, name, arm and message the host's,
     * its base the host's with each single-value side a {@link TestPlan.Side.FusedCell} of the carrier's row -- every
     * other verdict the database's; the segment's last group carries the root loop and one agreement per root, in root
     * order; each root's engine-text appeal joined to its database rows' ({@link #joinAppeals}).
     */
    static List<TestPlan.Verdicts> fusedSegment(VerdictBatch.Planned planned, List<DatabaseAsserts.HostRoot> roots,
            BodyDriver.ExecEnv env) {
        if (planned.groups().isEmpty()) {
            throw new IllegalStateException("a segment with the host's half attached and no verdict group");
        }
        Map<Integer, Integer> rootOf = new java.util.HashMap<>();
        List<com.legend.testexecutionplan.Agreement> agreements = new ArrayList<>(roots.size());
        for (int k = 0; k < roots.size(); k++) {
            Integer carrier = planned.hostCarriers().get(k);
            if (carrier == null) {
                throw new IllegalStateException(roots.get(k).host().assertion().listenerName() + ": an agreement root"
                        + " with no carrier verdict and not uncarried");
            }
            rootOf.put(carrier, k);
            agreements.add(new com.legend.testexecutionplan.Agreement(k, carrier,
                    com.legend.testexecutionplan.RootLoop.NO_VERDICT));
        }
        List<TestPlan.Verdicts> steps = new ArrayList<>(planned.groups().size());
        for (VerdictBatch.Group g : planned.groups()) {
            TestPlan.Verdicts step = g.step();
            List<TestPlan.Verdict> verdicts = new ArrayList<>(step.verdicts().size());
            for (TestPlan.Verdict v : step.verdicts()) {
                Integer k = rootOf.get(v.id());
                verdicts.add(k == null ? v : carried(v, roots.get(k), g, env));
            }
            steps.add(new TestPlan.Verdicts(verdicts, step.fused(), step.savepoint(), step.rules()));
        }
        int last = steps.size() - 1;
        steps.set(last, steps.get(last).withRoots(planned.loop()).withAgreements(agreements));
        for (int k = 0; k < roots.size(); k++) {
            joinAppeals(roots.get(k).host(), planned.loop().roots().get(k), env.appeals());
        }
        return steps;
    }

    /** The carrier verdict {@code v} of root {@code r}, in group {@code g}, as the host's verdict on the database's
     *  row: refused by name where the host half reads no side (its verdict is a raise or an outcome the host decided,
     *  which no fused row carries) or a fused side reads a store on another session than the group's. */
    private static TestPlan.Verdict carried(TestPlan.Verdict v, DatabaseAsserts.HostRoot r, VerdictBatch.Group g,
            BodyDriver.ExecEnv env) {
        TestPlan.Verdict hv = r.host().verdict();
        String name = r.host().assertion().listenerName();
        if (hv.base().isEmpty()) {
            throw new com.legend.error.NotImplementedException(name + ": an agreement root whose host half reads no"
                    + " side, carried by a fused statement (E-5c)");
        }
        if (r.storeBound() && !g.on().equals(env.target())) {
            throw new com.legend.error.NotImplementedException(name + ": an agreement root whose host columns read the"
                    + " body's store, its fused row on another session (E-5c)");
        }
        List<TestPlan.Side> base = new ArrayList<>(hv.base().size());
        for (int j = 0; j < hv.base().size(); j++) {
            List<com.legend.executionplan.ExecutionPlan.Column> cell = r.cells().get(j);
            base.add(cell == null ? hv.base().get(j) : new TestPlan.Side.FusedCell(cell));
        }
        return new TestPlan.Verdict(v.id(), hv.name(), hv.arm(), base, v.wrapper(), hv.message()).withHost(hv.host());
    }

    /** An assertError's steps for both judges over the database judge's program (E-5c step 6), as {@link #assemble}
     *  plans them: the host's step and the database's raising verdict ({@link One}); the host's alone where the host
     *  half planned no comparison of an error; null -- the body refused, as the database judge refuses it -- where it
     *  is an assertError whose raising verdict the database judge cannot plan. */
    static @Nullable List<TestPlan.Verdicts> assertErrorSteps(HostAsserts.Planned p, TypedSpec statement,
            List<TypedSpec> lets, com.legend.compiler.spec.SpecCompiler specs, BodyDriver.ExecEnv env) {
        boolean assertError = HostJudgment.core(p.assertion().check().judgment()) instanceof HostJudgment.AssertError;
        TestPlan.Verdicts raising = assertError ? DatabaseAsserts.errorVerdictFor(p, statement, lets, specs, env)
                : null;
        if (raising != null) {
            return new One(p, raising).steps;
        }
        if (assertError && p.assertion().beforeTheArm() == null) {
            com.legend.diagnostics.Diagnostics.judgeDecline("unplanned", p.assertion().listenerName(),
                    "the database judge cannot plan this assertError as its verdict");
            return null;
        }
        return new One(p, (DatabaseAsserts.Each) null).steps;
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
