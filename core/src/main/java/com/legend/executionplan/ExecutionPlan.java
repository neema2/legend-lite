// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.executionplan;

import com.legend.model.ConnectionDefinition;

import java.util.List;
import java.util.Map;
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

    /** A declared parameter: what a caller's value is checked against (legend-engine's
     *  {@code function-parameters-validation} facts — type name, multiplicity, an enum's values — and no model).
     *  An enum value travels as its NAME: the statement translates it to what the database stores, at each place it is
     *  compared (docs/EXECUTION_PLAN_BOUNDARY_2026_10_05.md §9).
     *
     * @param type       a Pure primitive ({@code String}, {@code Integer}, {@code Float}, {@code Decimal},
     *                   {@code Number}, {@code Boolean}, {@code StrictDate}, {@code DateTime}, {@code Date}) or an
     *                   enumeration's path
     * @param enumValues for an enumeration: the names a value may take, in declaration order; empty otherwise
     */
    public record Parameter(String name, String type, Multiplicity multiplicity, List<String> enumValues) {
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

    /** A node of the plan. */
    public sealed interface Node permits Sequence, TdsResult, TextResult {
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

    /** A result the DATABASE writes as finished text; the runner passes it on (lite's own node — legend-engine builds
     *  such results in Java). The format is the plan's, fixed when the plan is made: the database writes it. */
    public record TextResult(Format format, ResultType type, Sql sql) implements Node {
        public TextResult {
            Objects.requireNonNull(format, "format");
            Objects.requireNonNull(type, "type");
            Objects.requireNonNull(sql, "sql");
        }
    }

    /** How the database writes a {@link TextResult}. */
    public enum Format {
        /** the whole result as one CSV text */
        CSV,
        /** the whole result as one JSON text */
        JSON,
        /** one JSON object per row; the runner writes the array's brackets and commas */
        JSON_PER_ROW
    }

    /** What a {@link TextResult} is of: a relation's columns, or a value's type and multiplicity. */
    public sealed interface ResultType permits Relation, Value {
    }

    /** A relation, its columns in order (a scalar's or a collection's text is the one-column relation {@code value}). */
    public record Relation(List<Column> columns) implements ResultType {
        public Relation {
            columns = List.copyOf(columns);
        }
    }

    /** A column of a relation the database writes as text: its name and its Pure type as the typer names it (a
     *  primitive's name, {@code Decimal(10,2)}, an enumeration's path) — the database answers with the text, so no
     *  column has a SQL type of its own. */
    public record Column(String name, String type) {
        public Column {
            Objects.requireNonNull(name, "name");
            Objects.requireNonNull(type, "type");
        }
    }

    /** Values of a class — its instances, a graph fetch's objects — of {@code type} at {@code multiplicity}. */
    public record Value(String type, Multiplicity multiplicity) implements ResultType {
        public Value {
            Objects.requireNonNull(type, "type");
            Objects.requireNonNull(multiplicity, "multiplicity");
        }
    }

    /**
     * One statement, final for its target: the text with bind placeholders, the parameter each placeholder binds and how
     * (in order), and the target it runs on. Where a placeholder's type is its value's ({@link TypeHole}: H2), the text
     * has no type at the hole's place, and the runner writes the value's there before it prepares the statement.
     * {@code tree} is the typed SQL tree it was rendered from — metadata for lineage and explanation, never read by the
     * executor; it is not written to the lite JSON yet (a later step), so a plan read back from JSON has none.
     */
    public record Sql(String statement, List<Slot> slots, Target target,
            com.legend.sql.@com.legend.base.Nullable SqlQuery tree) {
        public Sql {
            Objects.requireNonNull(statement, "statement");
            slots = List.copyOf(slots);
            Objects.requireNonNull(target, "target");
            int previous = -1;
            for (Slot slot : slots) {
                if (slot.binding() instanceof Binding.One one && one.hole() != null) {
                    int at = one.hole().at();
                    if (at <= previous || at > statement.length()) {
                        throw new IllegalArgumentException("type hole of '" + slot.parameter() + "' at " + at + " is not"
                                + " after the previous one (" + previous + ") within the statement ("
                                + statement.length() + ")");
                    }
                    previous = at;
                }
            }
        }
    }

    /** A bind placeholder: the declared parameter it takes its value from, and how its value is bound (decided when
     *  the plan was made, for the target's database: the runner only follows it). */
    public record Slot(String parameter, Binding binding) {
        public Slot {
            Objects.requireNonNull(parameter, "parameter");
            Objects.requireNonNull(binding, "binding");
        }
    }

    /** How a placeholder is bound. */
    public sealed interface Binding permits Binding.One, Binding.Array {

        /** One value, as the caller's checked value is; an absent one a null of {@code nullType} (a
         *  {@code java.sql.JDBCType}'s name). {@code hole}: set where the statement must name the value's type (H2 types
         *  a placeholder when it prepares the statement), the hole the runner writes it into. */
        record One(String nullType, @com.legend.base.Nullable TypeHole hole) implements Binding {
            public One {
                Objects.requireNonNull(nullType, "nullType");
            }
        }

        /** The parameter's whole list, as one array of {@code elementSqlType} (the target's form of a list, e.g.
         *  {@code = ANY(?)}). */
        record Array(String elementSqlType) implements Binding {
            public Array {
                Objects.requireNonNull(elementSqlType, "elementSqlType");
            }
        }
    }

    /**
     * Where a placeholder's type is its value's: the statement's text has none at {@code at}, and the runner writes the
     * value's there, spelled by {@code types} for the value's kind ({@code absent}'s for an absent value) -- the type the
     * database gives a literal of that value, so the placeholder answers as the literal does.
     */
    public record TypeHole(int at, Map<ValueKind, TypeSpelling> types, ValueKind absent) {
        public TypeHole {
            if (types.isEmpty()) {
                throw new IllegalArgumentException("a type hole with no spelling");
            }
            // in the kinds' declared order: the order a plan's JSON writes them in
            types = java.util.Collections.unmodifiableMap(new java.util.EnumMap<>(types));
            if (!types.containsKey(absent)) {
                throw new IllegalArgumentException("an absent value's kind " + absent + " has no spelling in " + types);
            }
        }
    }

    /** The kinds of value a parameter typed by its value takes (a Float's, a Decimal's, a Number's, a Date's, a
     *  DateTime's). */
    public enum ValueKind {
        /** A whole number (a {@code Long}). */
        INTEGER,
        /** A decimal of its own digits (a {@code BigDecimal}: its precision and scale). */
        DECIMAL,
        /** A float at an extreme magnitude (a {@code Double}: its own digits). */
        FLOATING,
        /** A date (a {@code LocalDate}). */
        DATE,
        /** A date-time to the microsecond (a {@code LocalDateTime}). */
        DATE_TIME,
        /** A date-time with digits finer than a microsecond (a {@code LocalDateTime}). */
        DATE_TIME_NANOS
    }

    /** A type's spelling for one kind of value: its name, followed by the value's own digits as {@code digits} says.
     *  {@code fraction}: for a date-time, the digits of a second its value keeps, finer ones cut, as its literal's are
     *  (Postgres keeps six); null where the value keeps its own. */
    public record TypeSpelling(String name, Digits digits, @com.legend.base.Nullable Integer fraction) {
        public TypeSpelling {
            Objects.requireNonNull(name, "name");
            Objects.requireNonNull(digits, "digits");
            if (fraction != null && (fraction < 0 || fraction > 9)) {
                throw new IllegalArgumentException("a second's fraction of " + fraction + " digits");
            }
        }

        public TypeSpelling(String name, Digits digits) {
            this(name, digits, null);
        }
    }

    /** The value's own digits a type spelling takes: none, {@code (precision)}, or {@code (precision,scale)}. */
    public enum Digits {
        NONE, PRECISION, PRECISION_AND_SCALE
    }

    /**
     * Where a statement runs: the database; the server versions its statements are written for (a session reporting
     * another is refused, never run); the statements every connection to it runs first ({@code session}: its settings,
     * such as the time zone); and the steps that ESTABLISH it once, when its database is opened, before anything else
     * ({@code setup}: a connection's declared test data, written at plan time for the target's database). Two runs share
     * an in-memory database when their targets are equal (decision A, docs/EXECUTION_PLAN_BOUNDARY_2026_10_05.md §9).
     */
    public record Target(Database database, Servers servers, List<String> session, List<SetupStep> setup) {
        public Target {
            Objects.requireNonNull(database, "database");
            Objects.requireNonNull(servers, "servers");
            session = List.copyOf(session);
            setup = List.copyOf(setup);
        }
    }

    /** The database a target runs on: one a connection declares, or the platform's own engine. */
    public sealed interface Database permits Database.Declared, Database.Platform {

        /** The database's type: what its session must be. */
        ConnectionDefinition.DatabaseType type();

        /** The database a connection declares: its session is opened by this definition (or a caller's is checked
         *  against its type). */
        record Declared(ConnectionDefinition connection) implements Database {
            public Declared {
                Objects.requireNonNull(connection, "connection");
            }

            @Override
            public ConnectionDefinition.DatabaseType type() {
                return connection.databaseType();
            }
        }

        /** The platform's own in-process engine, for a runtime that binds no database, only model data
         *  (docs/SEMANTICS_REGISTER.md S27). */
        record Platform(ConnectionDefinition.DatabaseType type) implements Database {
            public Platform {
                Objects.requireNonNull(type, "type");
            }
        }
    }

    /** The server versions a target's statements are written for, as a session reports its version. */
    public sealed interface Servers permits Servers.Every, Servers.Versions {

        /** Every version: the statements' spelling does not depend on the server's version. */
        record Every() implements Servers {
        }

        /** The versions that begin with one of {@code prefixes} (H2's engine-parity spelling: {@code 2.1}, {@code 2.2}). */
        record Versions(List<String> prefixes) implements Servers {
            public Versions {
                prefixes = List.copyOf(prefixes);
                if (prefixes.isEmpty()) {
                    throw new IllegalArgumentException("a statement written for some server versions names at least one");
                }
            }
        }
    }

    /** One step that establishes a target: a statement, or rows the database's own bulk loader takes. */
    public sealed interface SetupStep permits SetupStep.Statement, SetupStep.Rows {

        /** A statement, run as it is. */
        record Statement(String sql) implements SetupStep {
            public Statement {
                Objects.requireNonNull(sql, "sql");
            }
        }

        /**
         * Rows for the database's bulk loader, every cell TEXT (null is SQL NULL): appended into a staging table of
         * text columns, then copied into the target table by a statement that casts (the database types every value,
         * exactly as for a string literal). Every statement is written at plan time, for the target's database.
         *
         * @param stagingTable  the staging table the cells are appended to
         * @param createStaging creates it (temporary, one text column per cell)
         * @param copy          copies it into the target table, casting
         * @param dropStaging   drops it
         */
        record Rows(String stagingTable, String createStaging, String copy, String dropStaging,
                List<List<String>> rows) implements SetupStep {
            public Rows {
                Objects.requireNonNull(stagingTable, "stagingTable");
                Objects.requireNonNull(createStaging, "createStaging");
                Objects.requireNonNull(copy, "copy");
                Objects.requireNonNull(dropStaging, "dropStaging");
                if (rows.isEmpty()) {
                    throw new IllegalArgumentException("rows for " + stagingTable + ": none (a load of no rows is no step)");
                }
                int width = rows.get(0).size();
                if (width == 0) {
                    throw new IllegalArgumentException("rows for " + stagingTable + ": a row of no cells");
                }
                List<List<String>> copied = new java.util.ArrayList<>(rows.size());
                for (List<String> row : rows) {
                    if (row.size() != width) {
                        throw new IllegalArgumentException("rows for " + stagingTable + ": a row of " + row.size()
                                + " cells among rows of " + width);
                    }
                    // a cell may be null (SQL NULL), so each row is copied as a list that holds nulls
                    copied.add(java.util.Collections.unmodifiableList(new java.util.ArrayList<>(row)));
                }
                rows = java.util.Collections.unmodifiableList(copied);
            }
        }
    }
}
