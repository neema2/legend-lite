// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.executionplan;

import com.legend.model.ConnectionDefinition;

import java.util.List;
import java.util.Objects;

/**
 * THE EXECUTION PLAN (docs/EXECUTION_PLAN_BOUNDARY_2026_10_05.md): what the planner produces and the executor runs,
 * with no model in between. PHYSICAL — every statement is rendered at plan time for its node's target and carries typed
 * parameter slots; the executor binds, runs and shapes, never compiles. legend-engine's plan protocol is the model for its
 * node kinds; this is lite's clean in-memory form, written as the lite JSON format by {@link PlanJson} (legend-engine's
 * own format is a later, separate serialisation of the same plan).
 *
 * @param parameters the plan's declared parameters, validated and converted by the executor before anything runs
 * @param root       what the plan computes
 */
public record ExecutionPlan(List<Parameter> parameters, Node root) {

    public ExecutionPlan {
        parameters = List.copyOf(parameters);
        Objects.requireNonNull(root, "root");
    }

    /** A declared parameter: what a caller's value is checked against and converted to (legend-engine's
     *  {@code function-parameters-validation} facts — type name, multiplicity, an enum's values — and no model).
     *
     * @param type       a Pure primitive ({@code String}, {@code Integer}, {@code Float}, {@code Decimal},
     *                   {@code Boolean}, {@code StrictDate}, {@code DateTime}, {@code Date}) or an enumeration's path
     * @param enumValues for an enumeration: each value and what the database stores for it; empty otherwise
     */
    public record Parameter(String name, String type, Multiplicity multiplicity, List<EnumValue> enumValues) {
        public Parameter {
            Objects.requireNonNull(name, "name");
            Objects.requireNonNull(type, "type");
            Objects.requireNonNull(multiplicity, "multiplicity");
            enumValues = List.copyOf(enumValues);
        }
    }

    /** {@code [lower..upper]}; {@code upper} null is {@code *}. */
    public record Multiplicity(int lower, @com.legend.base.Nullable Integer upper) {
        public Multiplicity {
            if (lower < 0 || (upper != null && upper < lower)) {
                throw new IllegalArgumentException("multiplicity [" + lower + ".." + upper + "]");
            }
        }
    }

    /** An enumeration value and the database values it stands for (an enumeration mapping may map one value to
     *  several): each a {@code String} or a {@code Long}. */
    public record EnumValue(String name, List<Object> databaseValues) {
        public EnumValue {
            Objects.requireNonNull(name, "name");
            databaseValues = List.copyOf(databaseValues);
            for (Object v : databaseValues) {
                if (!(v instanceof String) && !(v instanceof Long)) {
                    throw new IllegalArgumentException("enum value '" + name + "': a database value is a String or"
                            + " a Long, not " + v.getClass().getSimpleName());
                }
            }
        }
    }

    /** A node of the plan. */
    public sealed interface Node permits Sequence, TdsResult, JsonResult {
    }

    /** Runs its steps in order; the plan's answer is the last step's. */
    public record Sequence(List<Node> steps) implements Node {
        public Sequence {
            steps = List.copyOf(steps);
            if (steps.isEmpty()) {
                throw new IllegalArgumentException("a sequence has at least one step");
            }
        }
    }

    /** A tabular result: the statement's rows, with these columns (legend-engine's {@code relationalTdsInstantiation}). */
    public record TdsResult(List<TdsColumn> columns, Sql sql) implements Node {
        public TdsResult {
            columns = List.copyOf(columns);
            Objects.requireNonNull(sql, "sql");
        }
    }

    /** A column of a tabular result: its Pure type and the SQL type the database answers with. */
    public record TdsColumn(String name, String type, String sqlType) {
    }

    /** A result the DATABASE builds as JSON text (a class, a graph, a scalar or a collection; lite's own node — legend-
     *  engine builds such results in generated Java). {@code type} and {@code multiplicity} say what the JSON is of. */
    public record JsonResult(String type, Multiplicity multiplicity, Sql sql) implements Node {
    }

    /**
     * One statement, final for its target: the text with bind placeholders, the parameter each placeholder binds (in
     * order), and the target it runs on. {@code tree} is the typed SQL tree it was rendered from — metadata for lineage
     * and explanation, never read by the executor; it is not written to the lite JSON yet (a later step), so a plan read
     * back from JSON has none.
     */
    public record Sql(String statement, List<Slot> slots, Target target,
            com.legend.sql.@com.legend.base.Nullable SqlQuery tree) {
        public Sql {
            Objects.requireNonNull(statement, "statement");
            slots = List.copyOf(slots);
            Objects.requireNonNull(target, "target");
        }
    }

    /** A bind placeholder: the declared parameter it takes its value from; {@code arrayElementSqlType} when the slot
     *  binds the parameter's whole collection as one array (the target's form of a list, e.g. {@code = ANY(?)}). */
    public record Slot(String parameter, @com.legend.base.Nullable String arrayElementSqlType) {
    }

    /**
     * Where a statement runs: the connection, the statements that ESTABLISH it once per session before anything else
     * (a connection's declared test data, rendered at plan time), and the identity of an in-memory database (which
     * requests share it: computed at plan time from the model's store declarations and the connection).
     */
    public record Target(ConnectionDefinition connection, List<String> setup, String identity) {
        public Target {
            Objects.requireNonNull(connection, "connection");
            setup = List.copyOf(setup);
            Objects.requireNonNull(identity, "identity");
        }
    }
}
