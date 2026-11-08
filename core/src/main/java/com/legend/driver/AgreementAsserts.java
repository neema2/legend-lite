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

    /** One assert: its host half, its database half (null: the database judge declines it), its verdict step, the
     *  step's verdict that carries the host's base, each of the step's verdicts by its database member, and the run's
     *  state once the host judged its sides. */
    static final class One {
        final HostAsserts.Planned host;
        final DatabaseAsserts.@Nullable Each database;
        final TestPlan.Verdicts step;
        /** The assert's steps, in order: its one step; an assertError over an assert's two (the host's own, then the
         *  database's raising one). */
        final List<TestPlan.Verdicts> steps;
        final TestPlan.Verdict hostVerdict;
        final Map<Integer, TestPlan.Verdict> members;
        AssertVerdicts.@Nullable HostHeld held;
        /** The database half's root loop as it runs (the plan-as-data design's step 5): its rows by verdict, read for
         *  the loop the half planned (its batch's data); null where the database judge declines the assert. */
        final com.legend.judge.RootLoops.@Nullable Run databaseRun;
        /** An assertError's (S135): the step is its raising verdict, and the database half's row is the statement's. */
        final boolean raising;
        /** An assertError's verdict whose row is the database half's (its raising step's). */
        final TestPlan.@Nullable Verdict raisingVerdict;
        @Nullable List<Object> raisingRow;
        /** The host's facts the held verdict was judged by: the plan's, from the verdict the runner handed. */
        TestPlan.@Nullable Host heldHost;

        /** An assertError's steps for both judges (S135): the database's raising verdict, whose base the host reads --
         *  but where the function IS an assert, each judge decides that assert (2026-11-05, the deep audit: "both"
         *  asks both): the host's own step first, its sides read and judged as the host judge judges an assertError
         *  over an assert, then the database's raising step, its statement comparing what the database's verdict of the
         *  inner assert raised; the agreement rule decides once both are in. */
        One(HostAsserts.Planned host, TestPlan.Verdicts raising) {
            this.host = host;
            this.database = null;
            this.databaseRun = null;
            boolean overAnAssert = HostJudgment.core(host.assertion().check().judgment())
                    instanceof HostJudgment.AssertError ae && ae.inner() != null;
            this.step = raising;
            this.steps = overAnAssert ? List.of(host.step(), raising) : List.of(raising);
            this.hostVerdict = overAnAssert ? host.verdict() : raising.verdicts().get(0);
            this.raisingVerdict = raising.verdicts().get(0);
            this.members = Map.of();
            this.raising = true;
        }

        One(HostAsserts.Planned host, DatabaseAsserts.@Nullable Each database) {
            this.raising = false;
            this.raisingVerdict = null;
            this.host = host;
            this.database = database;
            this.databaseRun = database == null ? null : new com.legend.judge.RootLoops.Run(database.planned().loop());
            Map<Integer, TestPlan.Verdict> m = new java.util.HashMap<>();
            TestPlan.Verdict hv = host.verdict();
            TestPlan.Verdicts group = database == null ? null : database.group();
            if (group == null) {
                this.step = host.step();
                this.hostVerdict = hv;
            } else {
                // the host's base on the database's first member: one verdict carries both judges' statements
                TestPlan.Verdict first = group.verdicts().get(0);
                // it replaces the database's first member in the plan, so it keeps that member's id
                TestPlan.Verdict both = new TestPlan.Verdict(first.id(), hv.name(), hv.arm(), hv.base(),
                        first.wrapper(), hv.message()).withHost(hv.host());
                List<TestPlan.Verdict> verdicts = new ArrayList<>(group.verdicts().size());
                verdicts.add(both);
                m.put(both.id(), first);
                for (TestPlan.Verdict v : group.verdicts().subList(1, group.verdicts().size())) {
                    verdicts.add(v);
                    m.put(v.id(), v);
                }
                this.step = new TestPlan.Verdicts(verdicts, group.fused(), group.savepoint(), group.rules());
                this.hostVerdict = both;
            }
            this.steps = List.of(step);
            this.members = m;
        }

        /** Whether {@code v} is the step's last verdict: both judges have read what they read once it is in. */
        boolean closes(TestPlan.Verdict v) {
            // by id: the plan read back from its JSON holds equal verdicts, never the planner's objects (the slice 1
            // audit, 2026-11-08)
            return step.verdicts().get(step.verdicts().size() - 1).id() == v.id();
        }
    }

    private final HostAsserts host;
    private final Map<Integer, One> byStatement;
    /** The branches (the asserts-as-plan-nodes program, step 4), by the statement's place: the host half's branch,
     *  each side's asserts assembled for both judges. */
    private final Map<Integer, TestPlan.Branch> branches;
    private final Map<Integer, One> byVerdict;

    private AgreementAsserts(HostAsserts host, Map<Integer, One> byStatement, Map<Integer, TestPlan.Branch> branches,
            List<AgreementAsserts> sides) {
        this.host = host;
        this.byStatement = Collections.unmodifiableMap(new LinkedHashMap<>(byStatement));
        this.branches = Collections.unmodifiableMap(new LinkedHashMap<>(branches));
        Map<Integer, One> verdicts = new java.util.HashMap<>();
        for (One one : byStatement.values()) {
            for (TestPlan.Verdicts step : one.steps) {
                for (TestPlan.Verdict v : step.verdicts()) {
                    verdicts.put(v.id(), one);
                }
            }
        }
        // a branch side's asserts are this body's: the runner hands their verdicts back as any other's
        for (AgreementAsserts side : sides) {
            verdicts.putAll(side.byVerdict);
        }
        this.byVerdict = Collections.unmodifiableMap(verdicts);
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
        List<AgreementAsserts> sides = new ArrayList<>();
        for (int i = 0; i < typed.body().size(); i++) {
            HostAsserts.Branched b = host.branch(i);
            if (b != null) {
                AgreementAsserts then = assemble(b.then(), AssertBranch.body(typed, b.parts().then()), runtime,
                        databaseEnv, systemRouted, b.lets());
                AgreementAsserts otherwise = assemble(b.otherwise(), AssertBranch.body(typed, b.parts().otherwise()),
                        runtime, databaseEnv, systemRouted, b.lets());
                branches.put(i, new TestPlan.Branch(b.condition(), then.steps(b.parts().then().size()),
                        otherwise.steps(b.parts().otherwise().size())));
                sides.add(then);
                sides.add(otherwise);
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
        return new AgreementAsserts(host, byStatement, branches, sides);
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

    /** The assert the runner handed {@code verdict} for. */
    One of(TestPlan.Verdict verdict) {
        One one = byVerdict.get(verdict.id());
        if (one == null) {
            throw new IllegalStateException("a verdict step no assert of the body planned: " + verdict.name());
        }
        return one;
    }

    /** Whether {@code verdict} is {@code planned}: one verdict of the plan by its id (the plan the run reads may be
     *  the planner's read back from its JSON: guard 3, the slice 1 audit, 2026-11-08). */
    private static boolean same(TestPlan.Verdict verdict, TestPlan.@Nullable Verdict planned) {
        return planned != null && planned.id() == verdict.id();
    }

    /** The host half's sides for {@code verdict} as the runner read them ({@code rows}: their rows, or the error
     *  reading them raised, the judgment deciding it), judged and HELD ({@link AssertVerdicts#heldAtItsStep}): the
     *  verdict of record is decided once the database half's rows are in ({@link #received}); a database member's
     *  (no base, so nothing read) is nothing to the host, but an error reading it is raised, never dropped. */
    void hostSides(TestPlan.Verdict verdict,
            java.util.function.Supplier<List<com.legend.exec.PlanRunner.Rows>> rows,
            java.util.function.@Nullable Supplier<com.legend.exec.PlanRunner.Rows> message,
            @Nullable AssertListener listener, TestPlan.JudgingRules rules) {
        One one = of(verdict);
        if (!same(verdict, one.hostVerdict)) {
            if (!same(verdict, one.raisingVerdict)) {
                rows.get();
            }
            // the database's raising step is read for the database half: what it raised is its statement's, bound
            return;
        }
        // the host's facts as the plan carries them on the verdict the runner handed (step 4, 2026-11-08)
        TestPlan.Host h = java.util.Objects.requireNonNull(verdict.host(), "a host verdict the plan carries no host for");
        one.held = AssertVerdicts.heldAtItsStep(h, listener,
                () -> HostSides.shapedBase(h.assertion().name(), h.shapes(), rows.get()), rules,
                message == null ? null : HostSides.message(h.shapes(), message));
        one.heldHost = h;
    }

    /** The database half's row for {@code verdict} to its root; null where {@code verdict} is not the step's last.
     *  At the last, both judges have read: the agreement rule decides, the listener is told the verdict of record, and
     *  a failure raises (the walk's tryAdjudicate over its agreement arm). */
    @Nullable TypedSide received(TestPlan.Verdict verdict, @Nullable List<Object> row,
            @Nullable AssertListener listener, com.legend.judge.@Nullable SideReader reader) {
        One one = of(verdict);
        if (one.raising) {
            if (!same(verdict, one.raisingVerdict)) {
                return null;   // the host's own step: its rows are the host's, read at hostSides
            }
            one.raisingRow = row;
            return decided(one, listener, reader);
        }
        TestPlan.Verdict member = one.members.get(verdict.id());
        if (member != null) {
            java.util.Objects.requireNonNull(one.databaseRun).received(member, row);
        }
        if (!one.closes(verdict)) {
            return null;
        }
        return decided(one, listener, reader);
    }

    /** The runner's word on a group's fused statement, to the assert's database root. */
    void fused(TestPlan.Verdicts group, com.legend.error.@Nullable DataError rejected) {
        One one = of(group.verdicts().get(0));
        java.util.Objects.requireNonNull(one.databaseRun, "a fused statement for an assert the database declined")
                .fused(rejected);
    }

    /** {@code one} decided at its last row, inside the program's run: a row its database half planned ahead read
     *  through the program's {@code reader} (THE WALL, 2026-11-06: no run of its own). */
    private static TypedSide decided(One one, @Nullable AssertListener listener,
            com.legend.judge.@Nullable SideReader reader) {
        AssertVerdicts.HostHeld held = one.held;
        TestPlan.Host h = one.heldHost;
        if (held == null || h == null) {
            throw new IllegalStateException(one.hostVerdict.name() + ": the database half's rows arrived before the"
                    + " host half judged its sides");
        }
        com.legend.testexecutionplan.Assertion a = h.assertion();
        DatabaseAsserts.Each database = one.database;
        return AssertVerdicts.agreedAtItsStep(a, listener, () -> AgreementArm.agree(a.name(), listener,
                () -> {
                    AssertFailed failed = held.failed();
                    if (failed != null) {
                        throw failed;
                    }
                    return held.verdict();
                },
                () -> {
                    if (one.raising) {
                        // an assertError (S135): the database's statement compared what the function raised
                        return DatabaseJudge.verdictOf(a.name(), java.util.Objects.requireNonNull(one.raisingRow,
                                "an assertError's verdict statement answered no row"));
                    }
                    if (database == null) {
                        return null;   // the database judge declines: not this shape -- strict, a failure
                    }
                    // the verdict alone: a failure both judges agree on is told in the host's words, its message the
                    // host's side on the assert's verdict (AgreementArm)
                    java.util.Objects.requireNonNull(one.databaseRun).judge(null, reader, false,
                            DatabaseJudge::verdictOf, EngineTextAppeals::rowAppeal);
                    return AssertVerdicts.ok();
                }));
    }
}
