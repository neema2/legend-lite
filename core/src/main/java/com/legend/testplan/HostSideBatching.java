// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.testplan;

import com.legend.base.Nullable;
import com.legend.executionplan.ExecutionPlan;
import com.legend.lowering.HostSideBatch;
import com.legend.sql.SqlQuery;
import com.legend.testexecutionplan.TestPlan;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * THE HOST PLAN'S OWN SIDE BATCHES (the orchestrator's (1), ruled 2026-11-17): isolated BOTH sent each host assert's
 * sides as statements of their own, and the host's round trips were most of its cost (DuckDB channel B essential 2275
 * round trips against the fused agreement's 892). Each run of consecutive plain host asserts -- one verdict, no
 * savepoint, no raise, no error for an answer, nothing planned between them; one assert is a run too -- has its sides
 * read by ONE statement, the {@link HostSideBatch} of them, a step at the run's last statement carrying every assert's
 * verdict, each batched side a fused cell: its rows the batch's rows of its index. No verdict in it: the host judges the
 * cells as it judges a side's own rows. A run whose batch is longer than the database takes is batched in halves. The
 * split rung is at planning: a batch the dialect refuses to write leaves the run as it was (nothing retried at run,
 * G7). A side that raises fails the batch, told to the run's first batched assert, as a fused statement's rejection
 * failed the program.
 */
final class HostSideBatching {

    private HostSideBatching() {
    }

    /** The steps by statement: each batched run's merged step at its last statement and nothing at the others; every
     *  other statement absent (its own step stands). */
    static Map<Integer, List<TestPlan.TestStep>> steps(Map<Integer, HostAsserts.Planned> byStatement,
            BodyDriver.ExecEnv env) {
        Map<Integer, List<TestPlan.TestStep>> out = new LinkedHashMap<>();
        List<Integer> run = new ArrayList<>();
        int previous = Integer.MIN_VALUE;
        for (Map.Entry<Integer, HostAsserts.Planned> e : byStatement.entrySet()) {
            boolean plain = plain(e.getValue());
            if (!plain || e.getKey() != previous + 1) {
                close(run, byStatement, env, out);
                run.clear();
            }
            if (plain) {
                run.add(e.getKey());
            }
            previous = e.getKey();
        }
        close(run, byStatement, env, out);
        return out;
    }

    /** A plain host assert: one verdict, judged by the host, no savepoint, no raise -- and no judgment whose answer is
     *  an error (an assertError's side's raise is its answer, on a database with no savepoint as on one with, so it is
     *  read alone; a planned raise or an engine text's appeal is no side read). */
    private static boolean plain(HostAsserts.Planned p) {
        TestPlan.Verdicts step = p.step();
        TestPlan.@Nullable Host host = p.verdict().host();
        return step.verdicts().size() == 1 && step.savepoint() == null && step.fused() == null
                && !p.verdict().raises() && host != null && p.verdict().wrapper() == null
                && p.assertion().beforeTheArm() == null
                && !(host.judgment() instanceof com.legend.testexecutionplan.HostJudgment.AssertError)
                && !(host.judgment() instanceof com.legend.testexecutionplan.HostJudgment.Raise)
                && !(host.judgment() instanceof com.legend.testexecutionplan.HostJudgment.EngineText);
    }

    /**
     * {@code run} batched in groups that keep upstream's order of failure (the orchestrator's ruling of 2026-11-17: a
     * failing test names the assert upstream names, the first raise in body order): a group holds at most ONE side
     * that may raise ({@link HostSideBatch#mayRaise}), batched, its assert the group's first and the side its first
     * branch -- so a raise is that side's, and every assert before it was decided by an earlier statement. An assert
     * with a side that may raise starts a group; one with two such sides, or one it cannot batch, is no group's: its
     * sides read one by one, in order.
     */
    private static void close(List<Integer> run, Map<Integer, HostAsserts.Planned> byStatement,
            BodyDriver.ExecEnv env, Map<Integer, List<TestPlan.TestStep>> out) {
        List<Integer> group = new ArrayList<>();
        int raising = -1;
        for (int statement : run) {
            HostAsserts.Planned p = java.util.Objects.requireNonNull(byStatement.get(statement));
            List<Integer> mayRaise = raising(p, env);
            if (mayRaise == null || !mayRaise.isEmpty()) {
                batch(group, raising, byStatement, env, out);
                group = new ArrayList<>();
                raising = -1;
            }
            if (mayRaise == null) {
                continue;
            }
            if (!mayRaise.isEmpty()) {
                raising = mayRaise.get(0);
            }
            group.add(statement);
        }
        batch(group, raising, byStatement, env, out);
    }

    /** The places of {@code p}'s base sides that may raise -- at most one, batchable -- or null where it has more, or
     *  one the batch cannot read. */
    private static @Nullable List<Integer> raising(HostAsserts.Planned p, BodyDriver.ExecEnv env) {
        Map<Integer, TestPlan.Side.FusedCell> batchable = columns(p, env, new ArrayList<>(), new ArrayList<>(), -1);
        List<TestPlan.Side> base = p.verdict().base();
        List<JudgePlans.PlannedSide> planned = p.sides().sides();
        List<Integer> out = new ArrayList<>();
        for (int k = 0; k < base.size(); k++) {
            if (base.get(k) instanceof TestPlan.Side.Constant) {
                continue;
            }
            BodyDriver.@Nullable WrappedSide w = k < planned.size() && planned.get(k).side() == base.get(k)
                    ? planned.get(k).wrapped() : null;
            if (w != null && !HostSideBatch.mayRaise(w.plan())) {
                continue;
            }
            if (!batchable.containsKey(k) || !out.isEmpty()) {
                return null;
            }
            out.add(k);
        }
        return out;
    }

    /** {@code run}, one group, as one batch: {@code raising} the place of its first assert's side that may raise (-1:
     *  none), written as the batch's first branch. */
    private static void batch(List<Integer> run, int raising, Map<Integer, HostAsserts.Planned> byStatement,
            BodyDriver.ExecEnv env, Map<Integer, List<TestPlan.TestStep>> out) {
        if (run.isEmpty()) {
            return;
        }
        List<HostAsserts.Planned> planned = new ArrayList<>(run.size());
        for (int statement : run) {
            planned.add(java.util.Objects.requireNonNull(byStatement.get(statement)));
        }
        List<HostSideBatch.BatchSide> sides = new ArrayList<>();
        List<com.legend.compiler.element.type.Type.RelationType.Column> schema = new ArrayList<>();
        var one = new com.legend.compiler.element.type.Multiplicity.Bounded(1, 1);
        schema.add(new com.legend.compiler.element.type.Type.RelationType.Column(HostSideBatch.INDEX,
                com.legend.compiler.element.type.Type.Primitive.INTEGER, one));
        schema.add(new com.legend.compiler.element.type.Type.RelationType.Column(HostSideBatch.ORDER,
                com.legend.compiler.element.type.Type.Primitive.INTEGER, one));
        List<Map<Integer, TestPlan.Side.FusedCell>> cells = new ArrayList<>(run.size());
        for (int i = 0; i < planned.size(); i++) {
            cells.add(columns(planned.get(i), env, sides, schema, i == 0 ? raising : -1));
        }
        if (sides.size() < 2) {
            return;
        }
        SqlQuery batch = HostSideBatch.batch(sides);
        if (JudgePlans.runner(env).chars(batch) > JudgePlans.runner(env).fusedBudget()) {
            if (run.size() == 1) {
                return;
            }
            // longer than the database takes in one statement: the group in two halves, each batched on its own (the
            // second's first assert raises nothing)
            int half = run.size() / 2;
            batch(new ArrayList<>(run.subList(0, half)), raising, byStatement, env, out);
            batch(new ArrayList<>(run.subList(half, run.size())), -1, byStatement, env, out);
            return;
        }
        ExecutionPlan.TdsResult statement;
        try {
            if (!(JudgePlans.rowsPlan(JudgePlans.withFrames(batch, env),
                    new com.legend.compiler.element.type.Type.RelationType(schema), env)
                    instanceof ExecutionPlan.TdsResult t)) {
                return;
            }
            statement = t;
        } catch (com.legend.sql.DialectCapability refused) {
            // the split rung at planning: a batch the dialect cannot write leaves each side its own statement
            return;
        }
        List<TestPlan.Verdict> verdicts = new ArrayList<>(run.size());
        for (int i = 0; i < run.size(); i++) {
            TestPlan.Verdict v = planned.get(i).verdict();
            List<TestPlan.Side> base = new ArrayList<>(v.base());
            cells.get(i).forEach(base::set);
            verdicts.add(new TestPlan.Verdict(v.id(), v.name(), v.arm(), base, v.wrapper(), v.message(), v.raises(),
                    v.host()));
        }
        for (int i = 0; i < run.size() - 1; i++) {
            out.put(run.get(i), List.of());
        }
        out.put(run.get(run.size() - 1), List.of(new TestPlan.Verdicts(verdicts, statement, null,
                planned.get(0).step().rules())));
    }

    /** Assert {@code p}'s sides that can be batched, each added to {@code sides} (its columns named by its place there,
     *  their types to {@code schema}), as its fused cells by the side's place in the base; the rest keep their own
     *  statements. {@code first}: the side added first (-1: none), the others in the base's order. */
    private static Map<Integer, TestPlan.Side.FusedCell> columns(HostAsserts.Planned p, BodyDriver.ExecEnv env,
            List<HostSideBatch.BatchSide> sides,
            List<com.legend.compiler.element.type.Type.RelationType.Column> schema, int first) {
        List<TestPlan.Side> base = p.verdict().base();
        List<JudgePlans.PlannedSide> planned = p.sides().sides();
        Map<Integer, TestPlan.Side.FusedCell> own = new LinkedHashMap<>();
        List<Integer> order = new ArrayList<>();
        if (first >= 0) {
            order.add(first);
        }
        for (int k = 0; k < base.size() && k < planned.size(); k++) {
            if (k != first) {
                order.add(k);
            }
        }
        for (int k : order) {
            JudgePlans.PlannedSide side = planned.get(k);
            BodyDriver.@Nullable WrappedSide w = side.wrapped();
            if (side.side() != base.get(k) || w == null || !(side.side() instanceof TestPlan.Side.Rows rows)
                    || !w.storeFree() && !w.target().equals(env.target())) {
                continue;
            }
            List<com.legend.compiler.element.type.Type.RelationType.Column> typed = switch (side.kind()) {
                case VALUES -> com.legend.lowering.TypedRows.schema(w.shapeInfo()).columns();
                case TABULAR -> side.schema() == null ? null : side.schema().columns();
                default -> null;
            };
            List<ExecutionPlan.Column> columns = rows.result().columns();
            if (typed == null || columns.isEmpty() || typed.size() != columns.size()
                    || columns.stream().anyMatch(c -> c.slot() == null)) {
                continue;
            }
            // the side's statement as it is planned on its own, the body's frames attached to the batch instead
            SqlQuery statement = com.legend.lowering.VerdictSql.inBranchOrder(w.plan());
            List<com.legend.sql.OutputCol> outputs = statement.outputs();
            if (outputs.size() != columns.size()) {
                continue;
            }
            int at = sides.size();
            List<String> names = new ArrayList<>(columns.size());
            boolean byName = true;
            for (int c = 0; c < columns.size(); c++) {
                byName &= outputs.get(c).name().equals(columns.get(c).name());
                names.add("__h" + at + "_" + c);
            }
            HostSideBatch.BatchSide batched = !byName ? null
                    : HostSideBatch.side(statement, "__h" + at + "_", names);
            if (batched == null) {
                continue;
            }
            List<ExecutionPlan.Column> renamed = new ArrayList<>(columns.size());
            for (int c = 0; c < columns.size(); c++) {
                ExecutionPlan.Column col = columns.get(c);
                renamed.add(new ExecutionPlan.Column(names.get(c), col.type(), col.multiplicity(), col.slot(),
                        col.store(), col.precision()));
                schema.add(new com.legend.compiler.element.type.Type.RelationType.Column(names.get(c),
                        typed.get(c).type(), new com.legend.compiler.element.type.Multiplicity.Bounded(0, 1)));
            }
            sides.add(batched);
            own.put(k, new TestPlan.Side.FusedCell(at, renamed));
        }
        return own;
    }
}
