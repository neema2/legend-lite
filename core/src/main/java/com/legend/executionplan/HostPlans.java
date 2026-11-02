// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0
package com.legend.executionplan;

import java.util.ArrayList;
import java.util.List;

/**
 * WHAT A HOST RUNS, refused at planning (docs/HOST_EXECUTORS_PROGRAM_2026_11_02.md D6, R2): a host -- the tab, Python --
 * runs a plan itself, as carriage, so the planner hands it only a plan of a shape it runs, and refuses any other here,
 * by name, before anything runs. Two shapes, by what the host asked for:
 *
 * <ul>
 *   <li>{@link #read}: a query's rows (a grid, an execute) -- one typed result over a statement of text, which the
 *       host runs on its own database (its data there already: a plan's setup steps are the server's, never a
 *       host's). Anything else -- a pivot whose columns its data names (a keyed result, its keys read first), a
 *       program that writes, a statement that binds values -- is legend-lite's server's to run.</li>
 *   <li>{@link #program}: a program that writes (DataCube's Output into a table, docs/DATACUBE_FLOW_MODE_2026_10_24.md
 *       M6) -- its statements in order, in one transaction, a count bound where a later statement takes it, the one
 *       typed result read once the transaction has committed: no parameters; a typed result, or a sequence of updates
 *       and checks with one typed result; every statement on one database, for every server version, set up by
 *       nothing first (it runs on the host's own); no per-key regions; every bound value one value, typed by its
 *       statement. The words are the ones the tab's runner refused with at run time until 2026-11-02
 *       ({@code engine-client/src/plan-runner.ts}), which keeps only its format and version check, the refusal of a
 *       step it does not know, and that the plan's database is its session's.</li>
 * </ul>
 *
 * <p>The execute route's own plan ({@code PureV1Api.jsonPlan}) makes the read check in the engine's error kinds
 * (its words the same). PURE: the record alone.
 */
public final class HostPlans {
    private HostPlans() {
    }

    /** {@code plan}, when it is one typed result over a statement of text (a query's rows, which a host runs on its
     *  own database); otherwise refused by name ({@link IllegalArgumentException}). */
    public static ExecutionPlan read(ExecutionPlan plan) {
        if (!(plan.root() instanceof ExecutionPlan.TdsResult result)) {
            throw refused("a host runs one statement, and this query's plan is not one ("
                    + plan.root().getClass().getSimpleName() + ": a pivot whose columns the data names, or a program "
                    + "that writes); legend-lite's server runs it");
        }
        if (!result.sql().slots().isEmpty()) {
            throw refused("a host runs a statement's text, and this one binds values (" + result.sql().slots().size()
                    + "); legend-lite's server runs it");
        }
        return plan;
    }

    /** {@code plan}, when a host's runner runs it as a program; otherwise refused by name
     *  ({@link IllegalArgumentException}). */
    public static ExecutionPlan program(ExecutionPlan plan) {
        if (!plan.parameters().isEmpty()) {
            throw refused("a plan that takes parameters is not run here: its program binds none");
        }
        List<ExecutionPlan.Node> steps = switch (plan.root()) {
            case ExecutionPlan.Sequence s -> s.steps();
            case ExecutionPlan.TdsResult t -> List.of(t);
            case ExecutionPlan.Keyed k -> throw refused("a plan whose columns its data names (a dynamic pivot's, its "
                    + "keys read first) is not run here: legend-lite's server runs it");
            case ExecutionPlan.Update u -> throw refused("a plan's root is a sequence or a typed result, not an update");
            case ExecutionPlan.Check c -> throw refused("a plan's root is a sequence or a typed result, not a check");
            case ExecutionPlan.Effect e -> throw refused("a plan's root is a sequence or a typed result, not an effect");
        };
        List<ExecutionPlan.Sql> sqls = new ArrayList<>();
        int results = 0;
        for (ExecutionPlan.Node step : steps) {
            switch (step) {
                case ExecutionPlan.Update u -> sqls.add(u.sql());
                case ExecutionPlan.Check c -> sqls.add(c.sql());
                case ExecutionPlan.TdsResult t -> {
                    sqls.add(t.sql());
                    results++;
                }
                case ExecutionPlan.Sequence s -> throw refused("a sequence inside a sequence is not run here");
                case ExecutionPlan.Keyed k -> throw refused("a plan's step is an update, a check or a typed result, not a "
                        + "keyed result (a dynamic pivot's: legend-lite's server runs it)");
                case ExecutionPlan.Effect e -> throw refused("a plan's step is an update, a check or a typed result, not an"
                        + " effect");
            }
        }
        if (results != 1) {
            throw refused("a plan answers with one typed result, not " + results);
        }
        ExecutionPlan.Target target = sqls.get(0).target();
        for (ExecutionPlan.Sql sql : sqls) {
            if (!sql.target().equals(target)) {
                throw refused("a plan's steps run on one database: these name several");
            }
            if (!sql.regions().isEmpty()) {
                throw refused("a statement of per-key regions (a dynamic pivot's) is not run here");
            }
            for (ExecutionPlan.Slot slot : sql.slots()) {
                switch (slot.binding()) {
                    case ExecutionPlan.Binding.One one -> {
                        if (one.hole() != null) {
                            throw refused("'" + slot.parameter() + "' takes its type from its value, which is not run here");
                        }
                    }
                    case ExecutionPlan.Binding.Array a -> throw refused("'" + slot.parameter() + "' binds a list, which is "
                            + "not run here");
                    case ExecutionPlan.Binding.Key k -> throw refused("'" + slot.parameter() + "' binds a key's cell, which "
                            + "is not run here");
                }
            }
        }
        if (!(target.servers() instanceof ExecutionPlan.Servers.Every)) {
            throw refused("a plan for some servers' versions alone is not run here");
        }
        if (!target.setup().isEmpty()) {
            throw refused("a plan that sets its database up first is not run here: it runs on the host's own");
        }
        return plan;
    }

    private static IllegalArgumentException refused(String why) {
        return new IllegalArgumentException(why);
    }
}
