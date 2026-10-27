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
    public sealed interface Node permits Sequence, TdsResult, TextResult, Keyed, Update, Check {
    }

    /**
     * Runs its steps in order, on one session. A sequence holding an {@link Update} runs in ONE transaction of its
     * session (docs/WRITES_DESIGN_2026_10_18.md §7, D7, and §8, leg 7): its changes all land or none do, and a
     * caller's open transaction is joined, never committed. Its answer is its one result step's (the steps after it
     * drop what the steps before it staged), passed on once its changes have committed: no answer leaves for a change
     * rolled back.
     */
    public record Sequence(List<Node> steps) implements Node {
        public Sequence {
            steps = List.copyOf(steps);
            if (steps.isEmpty()) {
                throw new IllegalArgumentException("a sequence has at least one step");
            }
        }
    }

    /** The name a write's count is bound by in the steps after it ({@link Update#count}): no declared parameter can
     *  be named so (a Pure parameter's name is an identifier). */
    public static final String COUNT = "#count";

    /**
     * A statement run for the change it makes -- a write into a store table, a staged table created or dropped -- its
     * answer the count of rows the database reports (docs/WRITES_DESIGN_2026_10_18.md §8, leg 7).
     *
     * @param count where a later step reads that count (a write that is the program's value), the name its slots bind
     *              it by ({@link #COUNT}); null where none does
     */
    public record Update(Sql sql, @com.legend.base.Nullable String count) implements Node {
        public Update {
            Objects.requireNonNull(sql, "sql");
        }
    }

    /** A query run for the error its database may raise -- a Cypher write's guard (Neo4j's DeleteConnectedNode) -- its
     *  rows read and dropped: a program's statement whose value no step keeps. */
    public record Check(Sql sql) implements Node {
        public Check {
            Objects.requireNonNull(sql, "sql");
        }
    }

    /**
     * A result the runner answers as TYPED ROWS (the plan path program's step 6, docs/PLAN_PATH_PROGRAM_2026_10_17.md
     * §6; its seam the typed method of docs/PLAN_AND_RESULT_MODEL_2026_10_18.md §3, step 7): {@code columns} the
     * relation's columns with their three type facts, and {@code sql} a statement that answers those columns in
     * their order. The session reads each row's cells from its driver by the plan's columns -- the slot first, the
     * Pure type where the slot is none -- and the runner holds the plan's facts, no model and no dialect. (Until
     * step 7 the statement wrote each row as one JSON object the runner parsed back; a document's row still reads
     * that way, {@code exec.RowDecode}.)
     */
    public record TdsResult(List<Column> columns, Sql sql) implements Node {
        public TdsResult {
            columns = List.copyOf(columns);
            Objects.requireNonNull(sql, "sql");
        }
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

    /**
     * A result whose columns are partly NAMED BY THE DATA: a dynamic pivot's (the plan path program's probe plan,
     * 2026-10-20, docs/PLAN_PATH_PROGRAM_2026_10_17.md §9). {@code keys} runs first and answers one row per key, in the
     * order the key's columns take, every cell a text the database writes: cell 0 the key, which the body's per-key
     * placeholders bind ({@link Binding.Key}), and then each per-key output's name for a key ({@link PerKey#name}),
     * written under {@code keyFields} (2026-10-20: a session reading a cell by name reads it so).
     * {@code body} is written once, with per-key regions ({@link Sql#regions}) the runner repeats once per key; its
     * rows' cells are {@code outputs} in order, a per-key output once per key, key by key. Both statements run on one
     * session, inside one {@code snapshot}, so the keys are the body's. {@code format} null: the cells are typed by their
     * columns' facts and answered as typed rows; else each cell is the format's text of its value, written by the
     * database, and the runner writes the format's punctuation (the header from the outputs' names, a row's separators,
     * the brackets: docs/PLAN_AND_RESULT_MODEL_2026_10_18.md §5, F1). {@code maxColumns}: the most columns the database
     * answers, a body asked for more refused by name before it is sent; null where the database has no limit said.
     */
    public record Keyed(Sql keys, List<String> keyFields, @com.legend.base.Nullable Format format, List<Output> outputs,
            Sql body, Snapshot snapshot, @com.legend.base.Nullable Integer maxColumns) implements Node {
        public Keyed {
            Objects.requireNonNull(keys, "keys");
            keyFields = List.copyOf(keyFields);
            outputs = List.copyOf(outputs);
            Objects.requireNonNull(body, "body");
            Objects.requireNonNull(snapshot, "snapshot");
            if (!keys.regions().isEmpty()) {
                throw new IllegalArgumentException("a keys probe is written once: it has no per-key region");
            }
            if (body.regions().isEmpty()) {
                throw new IllegalArgumentException("a keyed body with no per-key region: it is a plain result");
            }
            if (!keys.target().equals(body.target())) {
                throw new IllegalArgumentException("a keys probe and its body run on one session: one target");
            }
            boolean perKey = false;
            for (Output o : outputs) {
                if (o instanceof PerKey) {
                    perKey = true;
                } else if (perKey) {
                    throw new IllegalArgumentException("a fixed output after the per-key ones: the body answers its"
                            + " fixed columns first");
                }
            }
            if (!perKey) {
                throw new IllegalArgumentException("a keyed result with no per-key output");
            }
            long perKeyOutputs = outputs.stream().filter(o -> o instanceof PerKey).count();
            if (keyFields.size() != perKeyOutputs + 1) {
                throw new IllegalArgumentException("a keys probe answers the key's text and each per-key output's name:"
                        + " " + keyFields.size() + " fields for " + perKeyOutputs + " per-key outputs");
            }
            if (format == Format.JSON_PER_ROW) {
                throw new IllegalArgumentException("a keyed text is written row by row in every format: its JSON is"
                        + " JSON, the runner writing the brackets either way");
            }
        }

        /** One output column of the body; {@code field}, the name the body's statement writes its cells under (a
         *  session reading a cell by name reads it so; JDBC reads by place). */
        public sealed interface Output permits Fixed, PerKey {
            Column column();

            String field();
        }

        /** A column answered once: its facts, and {@code name}, the format's spelling of its name (a TDS header
         *  entry, a CSV header field, a JSON member's name token; the column's own name for typed rows). */
        public record Fixed(Column column, String name, String field) implements Output {
            public Fixed {
                Objects.requireNonNull(column, "column");
                Objects.requireNonNull(name, "name");
                Objects.requireNonNull(field, "field");
            }
        }

        /** A column answered once per key, its facts {@code column}'s for every key (its name the template's stem),
         *  each key's name the keys row's cell {@code name}; the body writes the key at position {@code k}'s cells
         *  under {@code "_pk" + k + "_" + field} (the per-key name every dialect writes). */
        public record PerKey(Column column, int name, String field) implements Output {
            public PerKey {
                Objects.requireNonNull(column, "column");
                Objects.requireNonNull(field, "field");
                if (name < 1) {
                    throw new IllegalArgumentException("a per-key name is a keys row's cell after the key: " + name);
                }
            }
        }

        /** How the two statements see one state of the data: a transaction at the isolation that makes its reads one
         *  snapshot ({@code TRANSACTION} where every transaction is one, the database's own isolation kept), or
         *  {@code NONE} where the database has no transaction across statements, a difference the register names. */
        public enum Snapshot { TRANSACTION, REPEATABLE_READ, SERIALIZABLE, NONE }
    }

    /** How the database writes a {@link TextResult}. */
    public enum Format {
        /** the whole result as one CSV text */
        CSV,
        /** the whole result as one JSON text */
        JSON,
        /** one JSON object per row; the runner writes the array's brackets and commas */
        JSON_PER_ROW,
        /** the whole result as Pure's TDS text: a header of {@code name:Type[multiplicity]}, each row on its own
         *  line, cells in Pure's print forms (the form legend-pure's {@code stringToTDS} reads; 2026-10-17) */
        TDS_TEXT
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

    /** A column of a relation the database writes as text, with its three type facts
     *  (docs/PLAN_PATH_PROGRAM_2026_10_17.md §3): {@code type}, its Pure type as the typer names it (a primitive's
     *  name, {@code Decimal(10,2)}, an enumeration's path), and its {@code multiplicity}; {@code slot}, the SQL type
     *  the database answers with (the lowering's typed slot: {@code VARCHAR}, {@code DECIMAL(10,2)},
     *  {@code TIMESTAMP}) -- null when the column has no one slot (a nested column the query flattens into prefixed
     *  outputs; a pivot's generated column whose template carries no type); and {@code store}, the column's declared
     *  type in its store when the value is a store column read through ({@code VARCHAR(20)}, as the relational
     *  grammar spells it) -- null when the column is computed. Each null is said, never guessed. */
    public record Column(String name, String type, Multiplicity multiplicity, @com.legend.base.Nullable String slot,
            @com.legend.base.Nullable String store) {
        public Column {
            Objects.requireNonNull(name, "name");
            Objects.requireNonNull(type, "type");
            Objects.requireNonNull(multiplicity, "multiplicity");
        }

        /** The physical column holding slot {@code i} of a tagged column ({@code TAGGED<kind slot, ...>},
         *  docs/RESULTS_AND_RENDERERS_2026_10_22.md §5a): the SQL tree's one owner of the name
         *  ({@link com.legend.sql.SqlType.Tagged#slotColumn}), reached from here by the runner, which reads no SQL
         *  tree itself. */
        public static String taggedSlotColumn(String column, int i) {
            return com.legend.sql.SqlType.Tagged.slotColumn(column, i);
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
            com.legend.sql.@com.legend.base.Nullable SqlQuery tree, List<Region> regions) {
        public Sql {
            Objects.requireNonNull(statement, "statement");
            slots = List.copyOf(slots);
            Objects.requireNonNull(target, "target");
            regions = List.copyOf(regions);
            Region.check(statement.length(), slots, regions, s -> s.binding() instanceof Binding.Key);
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

        /** A statement with no per-key region. */
        public Sql(String statement, List<Slot> slots, Target target,
                com.legend.sql.@com.legend.base.Nullable SqlQuery tree) {
            this(statement, slots, target, tree, List.of());
        }
    }

    /**
     * A run of a statement written ONCE PER KEY of its plan's keys probe ({@link Keyed}): the text
     * {@code [start, end)}, written once for each key in the probe's order, {@code lead} before the first copy and
     * {@code separator} between copies, the key's position in the probe written at each of {@code counters} (positions
     * in the statement), and the {@code slotCount} slots from {@code firstSlot} bound again in each copy. With no key
     * the run is not written, {@code lead} with it, and {@code empty} stands in its place; a null {@code empty}: the
     * statement has no form without a key, refused by name. The dialect wrote every character when the plan was made.
     */
    public record Region(int start, int end, String lead, String separator, @com.legend.base.Nullable String empty,
            int firstSlot, int slotCount, List<Integer> counters) {
        public Region {
            Objects.requireNonNull(lead, "lead");
            Objects.requireNonNull(separator, "separator");
            counters = List.copyOf(counters);
            if (start < 0 || end < start || firstSlot < 0 || slotCount < 0) {
                throw new IllegalArgumentException("a per-key region [" + start + ", " + end + "), slots from "
                        + firstSlot + ", " + slotCount + " of them");
            }
            for (int at : counters) {
                if (at < start || at > end) {
                    throw new IllegalArgumentException("a key's position at " + at + " outside its region [" + start
                            + ", " + end + ")");
                }
            }
        }

        /** {@code regions} in order within a statement of {@code length}, each one's slots among {@code slots}, and
         *  every slot {@code keyed} says binds a key's cell inside one (outside one there is no key to bind). */
        static <S> void check(int length, List<S> slots, List<Region> regions, java.util.function.Predicate<S> keyed) {
            int previousEnd = 0;
            int previousSlots = 0;
            java.util.BitSet inRegion = new java.util.BitSet(slots.size());
            for (Region r : regions) {
                if (r.start() < previousEnd || r.end() > length) {
                    throw new IllegalArgumentException("per-key region [" + r.start() + ", " + r.end() + ") is not"
                            + " after the previous one (" + previousEnd + ") within the statement (" + length + ")");
                }
                if (r.firstSlot() < previousSlots || r.firstSlot() + r.slotCount() > slots.size()) {
                    throw new IllegalArgumentException("per-key region's slots " + r.firstSlot() + ".."
                            + (r.firstSlot() + r.slotCount()) + " are not after the previous region's ("
                            + previousSlots + ") among the statement's " + slots.size());
                }
                inRegion.set(r.firstSlot(), r.firstSlot() + r.slotCount());
                previousEnd = r.end();
                previousSlots = r.firstSlot() + r.slotCount();
            }
            for (int i = 0; i < slots.size(); i++) {
                if (keyed.test(slots.get(i)) && !inRegion.get(i)) {
                    throw new IllegalArgumentException("slot " + (i + 1) + " binds a key's cell outside a per-key"
                            + " region: no key to bind");
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
    public sealed interface Binding permits Binding.One, Binding.Array, Binding.Key {

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

        /** A key's cell, in a per-key region ({@link Region}): the keys row's {@code cell} of the key the region's copy
         *  is of, bound as its text; the slot names no declared parameter. */
        record Key(int cell) implements Binding {
            public Key {
                if (cell < 0) {
                    throw new IllegalArgumentException("a key's cell " + cell);
                }
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
