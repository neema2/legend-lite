// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.testplan;

import com.legend.testexecutionplan.HostJudgment;
import com.legend.TypedQuery;
import com.legend.base.Nullable;
import com.legend.compiler.spec.typed.TypedSpec;
import com.legend.testexecutionplan.TestPlan;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * THE AGREEMENT JUDGE'S ASSERTS, ON ONE ROAD (E-5c, the user's ruling O5 of 2026-11-13; DE4, the orchestrator's ruling
 * under the user's delegation of 2026-11-15): every statement-root assert of a body judged by BOTH judges
 * ({@link PlanningOptions.JudgeMode#BOTH}, channel B) is planned as the DATABASE judge's own program
 * ({@link #planFused}): its fused verdict statements, the host reading each single-value side as a typed column of them
 * ({@link #fusedSegment}); a segment decided at planning, a keyed member and the split rung judged by the host on its
 * own sides beside it; an assertError's own steps ({@link #assertErrorSteps}). The run decides each root's agreement
 * once both halves are in (AgreementRuns: the host's verdict of record, the database's checked). A body the database
 * judge plans no program for is refused by name: the second road -- each assert's own database statement beside the
 * host's sides (planEach) -- is deleted (DE4, 2026-11-15).
 */
final class AgreementAsserts {

    /** One assert: its host half, its database half (null: the database judge declines it) and its steps, the last
     *  carrying the pairing ({@link com.legend.testexecutionplan.Agreement}) and the database half's root loop -- the
     *  run reads both from the plan ({@link AgreementRuns}; the plan-as-data design's step 6, 2026-11-08). */
    static final class One {
        final HostAsserts.Planned host;
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
            boolean overAnAssert = HostJudgment.core(host.assertion().check().judgment())
                    instanceof HostJudgment.AssertError ae && ae.inner() != null;
            TestPlan.Verdict hostVerdict = overAnAssert ? host.verdict() : raising.verdicts().get(0);
            TestPlan.Verdicts step = raising.withAgreements(List.of(new com.legend.testexecutionplan.Agreement(0,
                    hostVerdict.id(), raising.verdicts().get(0).id())));
            this.steps = overAnAssert ? List.of(host.step(), step) : List.of(step);
        }

        /** An assert the database judge plans no verdict for (an assertError with no raising verdict): the host's
         *  own step, its agreement pairing the host's verdict with no database half. */
        One(HostAsserts.Planned host) {
            this.host = host;
            this.steps = List.of(host.step().withRoots(null).withAgreements(List.of(
                    new com.legend.testexecutionplan.Agreement(0, host.verdict().id(),
                            com.legend.testexecutionplan.RootLoop.NO_VERDICT))));
        }
    }

    private AgreementAsserts() {
    }

    /** One root's engine-text appeal joined to its database rows' (A1, the slice 3 follow-up audit, 2026-11-08): where
     *  the host half holds an engine-text appeal for the assert, each appeal its database root's rows name is a second
     *  hearing of it -- joined at run, decided once. */
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
        // DE4 (b, c): a root whose rows ride no fused statement -- a keyed member (a raw grid's report, its rows only
        // after exec expands its keyed region) or the split rung (a member the dialect cannot spell, its own statement)
        // -- keeps the database judge's program as it is; its host verdict reads its own sides, as the host judge reads
        // them (a keyed side's rows expanded by the runner), in a group of its own that closes the segment
        List<TestPlan.Verdict> ownSides = new ArrayList<>();
        for (int k = 0; k < roots.size(); k++) {
            Integer carrier = planned.hostCarriers().get(k);
            if (carrier == null) {
                if (!planned.hostUncarried().contains(k)) {
                    throw new IllegalStateException(roots.get(k).host().assertion().listenerName() + ": an agreement"
                            + " root with no carrier verdict and not uncarried");
                }
                TestPlan.Verdict hv = roots.get(k).host().verdict();
                int id = env.verdictIds().next();
                ownSides.add(new TestPlan.Verdict(id, hv.name(), hv.arm(), hv.base(), null, hv.message())
                        .withHost(hv.host()));
                agreements.add(new com.legend.testexecutionplan.Agreement(k, id,
                        com.legend.testexecutionplan.RootLoop.NO_VERDICT));
                continue;
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
        if (!ownSides.isEmpty()) {
            steps.add(new TestPlan.Verdicts(ownSides, null, null, env.options().judgingRules()));
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
        // a host half that reads no side (DE4 (a): its outcome decided at planning, a planned raise) rides the carrier
        // with no base: the host judges it from the plan, the database half from the fused row
        if (r.storeBound() && !g.on().equals(env.target())) {
            throw new com.legend.error.NotImplementedException(name + ": an agreement root whose host columns read the"
                    + " body's store, its fused row on another session (E-5c)");
        }
        List<TestPlan.Side> base = new ArrayList<>(hv.base().size());
        for (int j = 0; j < hv.base().size(); j++) {
            List<com.legend.executionplan.ExecutionPlan.Column> cell = r.cells().get(j);
            base.add(cell == null ? hv.base().get(j) : new TestPlan.Side.FusedCell(cell));
        }
        // the database judge's name for the verdict its fused statement carries: a statement the database rejects is
        // reported by the names it held, the same under both judges as under the database judge (PR1's finding A,
        // 2026-11-16: the host's qualified names had made BOTH's suffix differ from DATABASE's)
        return new TestPlan.Verdict(v.id(), v.name(), hv.arm(), base, v.wrapper(), hv.message()).withHost(hv.host());
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
        return new One(p).steps;
    }
}
