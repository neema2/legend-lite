// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.driver;

import com.legend.ExecuteOptions;
import com.legend.TypedQuery;
import com.legend.base.Nullable;
import com.legend.error.AssertFailed;
import com.legend.executionplan.ExecutionPlan;
import com.legend.judge.AssertListener;
import com.legend.judge.TypedSide;
import com.legend.testplan.TestDoor;

import java.util.ArrayList;
import java.util.Collections;
import java.util.IdentityHashMap;
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
 * decline or UNJUDGED told to the listener and the host decides; a database statement that errors a failure naming
 * both), so the walk's agreement arm and this step decide alike.
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
        final ExecutionPlan.Verdicts step;
        final ExecutionPlan.Verdict hostVerdict;
        final Map<ExecutionPlan.Verdict, ExecutionPlan.Verdict> members;
        AssertVerdicts.@Nullable HostHeld held;

        One(HostAsserts.Planned host, DatabaseAsserts.@Nullable Each database) {
            this.host = host;
            this.database = database;
            Map<ExecutionPlan.Verdict, ExecutionPlan.Verdict> m = new IdentityHashMap<>();
            ExecutionPlan.Verdict hv = host.verdict();
            ExecutionPlan.Verdicts group = database == null ? null : database.group();
            if (group == null) {
                this.step = host.step();
                this.hostVerdict = hv;
            } else {
                // the host's base on the database's first member: one verdict carries both judges' statements
                ExecutionPlan.Verdict first = group.verdicts().get(0);
                ExecutionPlan.Verdict both = new ExecutionPlan.Verdict(hv.name(), hv.arm(), hv.base(),
                        first.wrapper());
                List<ExecutionPlan.Verdict> verdicts = new ArrayList<>(group.verdicts().size());
                verdicts.add(both);
                m.put(both, first);
                for (ExecutionPlan.Verdict v : group.verdicts().subList(1, group.verdicts().size())) {
                    verdicts.add(v);
                    m.put(v, v);
                }
                this.step = new ExecutionPlan.Verdicts(verdicts, group.fused(), group.savepoint());
                this.hostVerdict = both;
            }
            this.members = m;
        }

        /** Whether {@code v} is the step's last verdict: both judges have read what they read once it is in. */
        boolean closes(ExecutionPlan.Verdict v) {
            return step.verdicts().get(step.verdicts().size() - 1) == v;
        }
    }

    private final HostAsserts host;
    private final Map<Integer, One> byStatement;
    private final Map<ExecutionPlan.Verdict, One> byVerdict;

    private AgreementAsserts(HostAsserts host, Map<Integer, One> byStatement) {
        this.host = host;
        this.byStatement = Collections.unmodifiableMap(new LinkedHashMap<>(byStatement));
        Map<ExecutionPlan.Verdict, One> verdicts = new IdentityHashMap<>();
        for (One one : byStatement.values()) {
            for (ExecutionPlan.Verdict v : one.step.verdicts()) {
                verdicts.put(v, one);
            }
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
        Map<Integer, DatabaseAsserts.Each> database = DatabaseAsserts.planEach(typed, runtime, databaseEnv,
                systemRouted, host::frameStamp);
        Map<Integer, One> byStatement = new LinkedHashMap<>();
        for (int i = 0; i < typed.body().size(); i++) {
            HostAsserts.Planned p = host.planned(i);
            if (p != null) {
                byStatement.put(i, new One(p, p.assertion().beforeTheArm() != null ? null : database.get(i)));
            }
        }
        return new AgreementAsserts(host, byStatement);
    }

    @Override
    public @Nullable List<ExecutionPlan.Verdicts> at(int statement) {
        One one = byStatement.get(statement);
        return one == null ? null : List.of(one.step);
    }

    @Override
    public @Nullable String frameStamp(int statement) {
        return host.frameStamp(statement);
    }

    /** The assert the runner handed {@code verdict} for. */
    One of(ExecutionPlan.Verdict verdict) {
        One one = byVerdict.get(verdict);
        if (one == null) {
            throw new IllegalStateException("a verdict step no assert of the body planned: " + verdict.name());
        }
        return one;
    }

    /** The host half's sides for {@code verdict}, judged and HELD ({@link AssertVerdicts#heldAtItsStep}): the
     *  verdict of record is decided once the database half's rows are in ({@link #received}); a database member's
     *  (no base) is nothing to the host. */
    void hostSides(ExecutionPlan.Verdict verdict, List<com.legend.exec.PlanRunner.Rows> rows,
            @Nullable AssertListener listener) {
        One one = of(verdict);
        if (verdict != one.hostVerdict) {
            return;
        }
        AssertVerdicts.HostAssert a = one.host.assertion();
        one.held = AssertVerdicts.heldAtItsStep(a, listener, () -> HostSides.shaped(a.name(), one.host.sides(), rows));
    }

    /** The database half's row for {@code verdict} to its root; null where {@code verdict} is not the step's last.
     *  At the last, both judges have read: the agreement rule decides, the listener is told the verdict of record, and
     *  a failure raises (the walk's tryAdjudicate over its agreement arm). */
    @Nullable TypedSide received(ExecutionPlan.Verdict verdict, @Nullable List<Object> row,
            @Nullable AssertListener listener) {
        One one = of(verdict);
        ExecutionPlan.Verdict member = one.members.get(verdict);
        if (member != null) {
            java.util.Objects.requireNonNull(one.database).planned().received(member, row);
        }
        if (!one.closes(verdict)) {
            return null;
        }
        return decided(one, listener);
    }

    /** The runner's word on a group's fused statement, to the assert's database root. */
    void fused(ExecutionPlan.Verdicts group, com.legend.error.@Nullable DataError rejected) {
        One one = of(group.verdicts().get(0));
        java.util.Objects.requireNonNull(one.database, "a fused statement for an assert the database declined")
                .planned().fused(group, rejected);
    }

    /** A host side of {@code verdict}'s that could not be read: counted and reported as the walk counts and reports an
     *  assert whose side raised. */
    RuntimeException sidesFailed(ExecutionPlan.Verdict verdict, RuntimeException failure,
            @Nullable AssertListener listener) {
        return AssertVerdicts.sideRefused(of(verdict).host.assertion(), listener, failure);
    }

    private static TypedSide decided(One one, @Nullable AssertListener listener) {
        AssertVerdicts.HostAssert a = one.host.assertion();
        AssertVerdicts.HostHeld held = one.held;
        if (held == null) {
            throw new IllegalStateException(a.listenerName() + ": the database half's rows arrived before the host"
                    + " half judged its sides");
        }
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
                    if (database == null) {
                        return null;   // the database judge declines: not this shape
                    }
                    database.planned().judge(null);
                    return AssertVerdicts.ok();
                }));
    }
}
