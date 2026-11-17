// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.testplan;

import com.legend.base.Nullable;
import com.legend.executionplan.ExecutionPlan;
import com.legend.lowering.HostSideBatch;
import com.legend.testexecutionplan.TestPlan;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * THE HOST PLAN'S OWN SIDE BATCHES (the orchestrator's (1), ruled 2026-11-17): isolated BOTH sent each host assert's
 * sides as statements of their own, and the host's round trips were most of its cost (DuckDB channel B essential 2275
 * round trips against the fused agreement's 892). Each run of consecutive plain host asserts -- one verdict, no
 * savepoint, no raise, nothing planned between them -- has its SINGLE-VALUE sides read by ONE statement, the
 * {@link HostSideBatch} of their rows, a step at the run's last statement carrying every assert's verdict, each batched
 * side a fused cell of its row. No verdict in it: the host judges the cells as it judges a side's own rows. The split
 * rung is at planning: a batch the dialect refuses to write leaves the run as it was (nothing retried at run, G7).
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

    /** A plain host assert: one verdict, judged by the host, no savepoint, no raise. */
    private static boolean plain(HostAsserts.Planned p) {
        TestPlan.Verdicts step = p.step();
        return step.verdicts().size() == 1 && step.savepoint() == null && step.fused() == null
                && !p.verdict().raises() && p.verdict().host() != null && p.verdict().wrapper() == null;
    }

    private static void close(List<Integer> run, Map<Integer, HostAsserts.Planned> byStatement,
            BodyDriver.ExecEnv env, Map<Integer, List<TestPlan.TestStep>> out) {
        if (run.size() < 2) {
            return;
        }
        List<HostAsserts.Planned> planned = new ArrayList<>(run.size());
        for (int statement : run) {
            planned.add(java.util.Objects.requireNonNull(byStatement.get(statement)));
        }
        List<List<HostSideBatch.HostColumn>> rows = new ArrayList<>(run.size());
        List<com.legend.compiler.element.type.Type.RelationType.Column> schema = new ArrayList<>();
        schema.add(new com.legend.compiler.element.type.Type.RelationType.Column(HostSideBatch.INDEX,
                com.legend.compiler.element.type.Type.Primitive.INTEGER,
                new com.legend.compiler.element.type.Multiplicity.Bounded(1, 1)));
        List<Map<Integer, List<ExecutionPlan.Column>>> cells = new ArrayList<>(run.size());
        int batched = 0;
        for (int i = 0; i < run.size(); i++) {
            Map<Integer, List<ExecutionPlan.Column>> own = new LinkedHashMap<>();
            rows.add(columns(planned.get(i), "__h" + i + "_", env, own, schema));
            cells.add(own);
            batched += own.size();
        }
        if (batched < 2) {
            return;
        }
        ExecutionPlan.TdsResult statement;
        try {
            if (!(JudgePlans.rowsPlan(HostSideBatch.batch(rows),
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
            cells.get(i).forEach((k, columns) -> base.set(k, new TestPlan.Side.FusedCell(columns)));
            verdicts.add(new TestPlan.Verdict(v.id(), v.name(), v.arm(), base, v.wrapper(), v.message(), v.raises(),
                    v.host()));
        }
        TestPlan.Verdicts first = planned.get(0).step();
        for (int i = 0; i < run.size() - 1; i++) {
            out.put(run.get(i), List.of());
        }
        out.put(run.get(run.size() - 1), List.of(new TestPlan.Verdicts(verdicts, statement, null, first.rules())));
    }

    /** Assert {@code p}'s single-value sides as batch columns named under {@code prefix}, each side's renamed columns
     *  recorded in {@code own} by the side's place in the base; the rest keep their own statements. */
    private static List<HostSideBatch.HostColumn> columns(HostAsserts.Planned p, String prefix, BodyDriver.ExecEnv env,
            Map<Integer, List<ExecutionPlan.Column>> own,
            List<com.legend.compiler.element.type.Type.RelationType.Column> schema) {
        List<TestPlan.Side> base = p.verdict().base();
        List<JudgePlans.PlannedSide> sides = p.sides().sides();
        List<HostSideBatch.HostColumn> out = new ArrayList<>();
        for (int k = 0; k < base.size() && k < sides.size(); k++) {
            JudgePlans.PlannedSide side = sides.get(k);
            BodyDriver.@Nullable WrappedSide w = side.wrapped();
            if (side.side() != base.get(k) || side.kind() != JudgePlans.Kind.VALUES || w == null
                    || !JudgePlans.shapeOf(side).scalar() || !(side.side() instanceof TestPlan.Side.Rows rows)
                    || !w.storeFree() && !w.target().equals(env.target())) {
                continue;
            }
            List<ExecutionPlan.Column> columns = rows.result().columns();
            // the side as its own statement is planned: with the body's frames (a frame read inside a subquery is
            // a CTE the batch would not carry, so a framed side is a WITH and keeps its own statement)
            com.legend.sql.SqlQuery statement = JudgePlans.withFrames(w.plan(), env);
            List<com.legend.compiler.element.type.Type.RelationType.Column> typed =
                    com.legend.lowering.TypedRows.schema(w.shapeInfo()).columns();
            List<com.legend.sql.OutputCol> outputs = statement.outputs();
            if (typed.size() != columns.size() || outputs.size() != columns.size() || columns.stream().anyMatch(c -> c.slot() == null)) {
                continue;
            }
            List<String> names = new ArrayList<>(columns.size());
            boolean byName = true;
            for (int c = 0; c < columns.size(); c++) {
                byName &= outputs.get(c).name().equals(columns.get(c).name());
                names.add(prefix + k + "_" + c);
            }
            List<HostSideBatch.HostColumn> hc = !byName ? null
                    : HostSideBatch.singleValueColumns(statement, prefix + k + "_", names);
            if (hc == null) {
                continue;
            }
            List<ExecutionPlan.Column> renamed = new ArrayList<>(columns.size());
            for (int c = 0; c < columns.size(); c++) {
                ExecutionPlan.Column col = columns.get(c);
                renamed.add(new ExecutionPlan.Column(names.get(c), col.type(), col.multiplicity(), col.slot(),
                        col.store(), col.precision()));
            }
            out.addAll(hc);
            for (int c = 0; c < typed.size(); c++) {
                schema.add(new com.legend.compiler.element.type.Type.RelationType.Column(names.get(c),
                        typed.get(c).type(), typed.get(c).multiplicity()));
            }
            own.put(k, renamed);
        }
        return out;
    }
}
