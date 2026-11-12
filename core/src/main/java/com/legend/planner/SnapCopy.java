// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.planner;

import com.legend.TypedQuery;
import com.legend.base.Nullable;
import com.legend.error.NotImplementedException;
import com.legend.executionplan.ExecutionPlan;
import com.legend.sql.dialect.HostOperation;
import com.legend.sql.dialect.HostOperations;
import com.legend.sql.dialect.SqlDialect;

import java.util.ArrayList;
import java.util.List;

/**
 * A SNAPSHOT'S COPY, AS ONE PLAN (the final audit's 1(b), 2026-11-12; the user's ruling and B3's contract): a pull --
 * a Pure query over the host's own store, planned HERE -- copied into a table of the host's database. One writing plan
 * on the host's database: the schema made where one is named, the table dropped where it is, and the table made AS the
 * pull's own statement ({@link PlanMaker#copyInto}: its tree written by the dialect, its {@code data:} payloads bound as
 * the pull binds them), every statement on a target that carries the database's transaction, so a host runs the three
 * as one: a copy that fails leaves the table as it was. Until then the host made the copy as host operations around a
 * query a planner wrote and the host handed back as text ({@code HostOperation.Source.Query}, deleted).
 *
 * <p>Refused by name: a database whose host opens no transaction; a pull that is not one statement (a keyed result, a
 * program, a value the program wrote) or answers no relation; one that loads data before it runs; one that binds
 * anything but its own {@code data:} payloads (a declared parameter, a list, a key's cell, a source's path).
 */
public final class SnapCopy {

    private SnapCopy() {
    }

    /** {@code pull} copied into {@code [schema.]table} of {@code dialect}'s database (of {@code type}), as one plan. */
    public static ExecutionPlan plan(TypedQuery pull, String runtime, SqlDialect dialect,
            com.legend.executionplan.DatabaseType type, @Nullable String schema, String table) {
        HostOperations host = HostOperations.of(dialect);
        if (host.transaction() == null) {
            throw new NotImplementedException("a snapshot's copy is made in one transaction, and " + type
                    + " declares none");
        }
        if (!pull.parameters().isEmpty()) {
            throw new NotImplementedException(binds(pull.parameters().get(0).name(), "a declared parameter"));
        }
        ExecutionPlan pulled = pull.executionPlan(runtime);
        if (!(pulled.root() instanceof ExecutionPlan.TdsResult answer)) {
            throw new NotImplementedException("the pull copied into " + table + " is not one statement: "
                    + pulled.root().getClass().getSimpleName());
        }
        if (answer.shape() != ExecutionPlan.TdsResult.Shape.RELATION) {
            throw new NotImplementedException("the pull copied into " + table + " answers no relation: its answer is "
                    + answer.shape());
        }
        if (!answer.sql().target().loads().isEmpty()) {
            throw new NotImplementedException("the pull copied into " + table + " loads data before it runs ("
                    + answer.sql().target().loads().stream().map(ExecutionPlan.Load::identity).toList()
                    + "): a snapshot's copy reads what its host's database holds");
        }
        for (ExecutionPlan.Slot slot : answer.sql().slots()) {
            String kind = switch (slot.binding()) {
                case ExecutionPlan.Binding.Payload p -> null;
                case ExecutionPlan.Binding.One o -> slot.parameter().startsWith(ExecutionPlan.STAMP)
                        ? "an execution's stamp" : "a declared parameter";
                case ExecutionPlan.Binding.Array a -> "a list";
                case ExecutionPlan.Binding.Key k -> "a key's cell";
                case ExecutionPlan.Binding.Source s -> "a source's path";
            };
            if (kind != null) {
                throw new NotImplementedException(binds(slot.parameter(), kind));
            }
        }
        ExecutionPlan.Target target = Targets.of(new ExecutionPlan.Database.Platform(type, null), dialect);
        ExecutionPlan.Sql copy = PlanMaker.copyInto(pull, runtime, dialect, target, schema, table);
        if (!copy.slots().equals(answer.sql().slots()) || !copy.content().equals(answer.sql().content())) {
            throw new IllegalStateException("the copy into " + table + " binds " + copy.slots()
                    + ", and its pull's statement binds " + answer.sql().slots() + ": a copy is its pull's statement");
        }
        List<ExecutionPlan.Node> steps = new ArrayList<>();
        if (schema != null) {
            effects(host.statements(new HostOperation.MakeSchema(null, schema)), target, steps);
        }
        effects(host.statements(new HostOperation.Drop(HostOperation.Kind.TABLE,
                new HostOperation.Name(null, schema, table, false))), target, steps);
        steps.add(new ExecutionPlan.Effect(copy));
        return ExecutionPlan.writing(List.of(), new ExecutionPlan.Sequence(steps));
    }

    /** The refusal of a pull that binds {@code parameter}, of {@code kind}. */
    private static String binds(String parameter, String kind) {
        return "a snapshot's copy binds its pull's data: payloads alone, and '" + parameter + "' binds " + kind;
    }

    private static void effects(List<String> statements, ExecutionPlan.Target target, List<ExecutionPlan.Node> into) {
        for (String sql : statements) {
            into.add(new ExecutionPlan.Effect(ExecutionPlan.Sql.of(sql, target)));
        }
    }
}
