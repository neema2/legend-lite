// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0
package com.legend.testcases;

import com.legend.database.Databases;
import com.legend.executionplan.ExecutionPlan;
import com.legend.executionplan.PlanJson;
import com.legend.model.ConnectionDefinition.DatabaseType;
import com.legend.sql.dialect.RenderedStatement;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The plan path program's round-trip gate (docs/PLAN_PATH_PROGRAM_2026_10_17.md §2, step 1b): a plan written to its
 * JSON and read back is the same plan, its typed SQL tree included, and the tree read back renders the same statement
 * with as many bindings on the plan's own dialect. Every test that makes a plan can judge it so; a plan with no typed
 * tree is a planner defect here, never a pass.
 */
public final class PlanRoundTrip {
    private PlanRoundTrip() {
    }

    public static void assertRoundTrips(ExecutionPlan plan, String what) {
        String json = PlanJson.write(plan);
        ExecutionPlan back = PlanJson.read(json);
        assertEquals(plan, back, what + ": the plan read back differs");
        assertRenders(back.root(), what);
        recordSizes(plan, json, what);
    }

    /** The {@code plan-sizes} measurement (docs/PLAN_AND_RESULT_MODEL_2026_10_18.md §8): one line per plan in
     *  {@code plan-sizes.tsv} in the test's outputs -- the JSON's size with its tree, without it, and the statements'
     *  characters -- the distribution that decides whether a plan ever wants compressing. */
    private static void recordSizes(ExecutionPlan plan, String json, String what) {
        if (!com.legend.diagnostics.Diagnostics.on("plan-sizes")) {
            return;
        }
        String dir = System.getenv("TEST_UNDECLARED_OUTPUTS_DIR");
        if (dir == null) {
            return;
        }
        ExecutionPlan bare = new ExecutionPlan(plan.parameters(), withoutTrees(plan.root()));
        long statementChars = statementChars(plan.root());
        String line = what.replace('\t', ' ') + "\t" + json.length() + "\t" + PlanJson.write(bare).length() + "\t"
                + statementChars + "\n";
        try {
            java.nio.file.Files.writeString(java.nio.file.Path.of(dir, "plan-sizes.tsv"), line,
                    java.nio.file.StandardOpenOption.CREATE, java.nio.file.StandardOpenOption.APPEND);
        } catch (java.io.IOException e) {
            throw new java.io.UncheckedIOException(e);
        }
    }

    private static ExecutionPlan.Node withoutTrees(ExecutionPlan.Node node) {
        return switch (node) {
            case ExecutionPlan.Sequence s -> new ExecutionPlan.Sequence(s.steps().stream().map(PlanRoundTrip::withoutTrees).toList());
            case ExecutionPlan.TdsResult t -> new ExecutionPlan.TdsResult(t.columns(), withoutTree(t.sql()));
            case ExecutionPlan.TextResult t -> new ExecutionPlan.TextResult(t.format(), t.type(), withoutTree(t.sql()));
            case ExecutionPlan.Keyed k -> new ExecutionPlan.Keyed(withoutTree(k.keys()), k.keyFields(), k.format(),
                    k.outputs(),
                    withoutTree(k.body()), k.snapshot(), k.maxColumns());
        };
    }

    private static ExecutionPlan.Sql withoutTree(ExecutionPlan.Sql sql) {
        return new ExecutionPlan.Sql(sql.statement(), sql.slots(), sql.target(), null, sql.regions());
    }

    private static long statementChars(ExecutionPlan.Node node) {
        return switch (node) {
            case ExecutionPlan.Sequence s -> s.steps().stream().mapToLong(PlanRoundTrip::statementChars).sum();
            case ExecutionPlan.TdsResult t -> t.sql().statement().length();
            case ExecutionPlan.TextResult t -> t.sql().statement().length();
            case ExecutionPlan.Keyed k -> k.keys().statement().length() + k.body().statement().length();
        };
    }

    private static void assertRenders(ExecutionPlan.Node node, String what) {
        switch (node) {
            case ExecutionPlan.Sequence s -> s.steps().forEach(step -> assertRenders(step, what));
            case ExecutionPlan.TdsResult t -> assertRenders(t.sql(), what);
            case ExecutionPlan.TextResult t -> assertRenders(t.sql(), what);
            case ExecutionPlan.Keyed k -> {
                assertRenders(k.keys(), what);
                assertRenders(k.body(), what);
            }
        }
    }

    private static void assertRenders(ExecutionPlan.Sql sql, String what) {
        assertTrue(sql.tree() != null, what + ": the plan carries no typed tree");
        DatabaseType type = switch (sql.target().database()) {
            case ExecutionPlan.Database.Declared d -> d.connection().databaseType();
            case ExecutionPlan.Database.Platform p -> p.type();
        };
        RenderedStatement rendered = Databases.dialect(type).renderStatement(sql.tree());
        assertEquals(sql.statement(), rendered.sql(), what + ": the tree read back renders differently");
        assertEquals(sql.slots().size(), rendered.binds().size(), what + ": the tree read back binds differently");
        assertEquals(sql.regions().size(), rendered.regions().size(), what + ": the tree read back has other regions");
        for (int i = 0; i < sql.regions().size(); i++) {
            ExecutionPlan.Region a = sql.regions().get(i);
            RenderedStatement.Region b = rendered.regions().get(i);
            assertEquals(List.of(a.start(), a.end(), a.firstSlot(), a.slotCount(), a.counters()),
                    List.of(b.start(), b.end(), b.firstBind(), b.bindCount(), b.counters()),
                    what + ": per-key region " + i + " read back differs");
        }
    }
}
