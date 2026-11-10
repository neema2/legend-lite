// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.executionplan;



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
 * @param writes     whether the plan changes a store: a plan that writes runs on its statements' own sessions (decision
 *                   A, docs/WRITES_DESIGN_2026_10_18.md §8), one that reads on the session its answer's target names
 *                   (the plan-as-data design's slice 5, 2026-11-09: a fact of the plan, so a plan read back from its
 *                   JSON runs alone -- until then the door read it off the typed query and handed it beside the plan)
 */
public record ExecutionPlan(List<Parameter> parameters, Node root, boolean writes) {

    public ExecutionPlan {
        parameters = List.copyOf(parameters);
        Objects.requireNonNull(root, "root");
    }

    /** A plan that reads: it changes no store. */
    public static ExecutionPlan reading(List<Parameter> parameters, Node root) {
        return new ExecutionPlan(parameters, root, false);
    }

    /** A plan that writes: a program that changes a store. */
    public static ExecutionPlan writing(List<Parameter> parameters, Node root) {
        return new ExecutionPlan(parameters, root, true);
    }

    /** The shape of the typed rows this plan answers (docs/RESULT_BOUNDARY_PROGRAM_2026_10_26.md, the end state of
     *  2026-10-27): its result's -- a keyed result's is a relation's -- or, for a plan of steps, its one result step's.
     *  A plan that answers no rows is refused by name: a writer has nothing to write. A constant answer is a value's
     *  (the test-is-the-product plan's M2, B11). */
    public TdsResult.Shape answerShape() {
        return switch (answer(root)) {
            case TdsResult t -> t.shape();
            case Keyed k -> k.shape();
            case Constant c -> TdsResult.Shape.VALUE;
            default -> throw new IllegalStateException("not reached: answer() keeps typed results only");
        };
    }

    /** The statement whose rows are this plan's typed answer: its result's, a keyed result's body, or a plan of
     *  steps' one result step's ({@link #answerShape}'s rule). A constant answer sends no statement, and is refused
     *  by name ({@link #sendsTheAnswer} asks first). */
    public Sql answerSql() {
        return switch (answer(root)) {
            case TdsResult t -> t.sql();
            case Keyed k -> k.body();
            case Constant c -> throw new IllegalStateException("a constant answer sends no statement: the plan wrote"
                    + " its value");
            default -> throw new IllegalStateException("not reached: answer() keeps typed results only");
        };
    }

    /** Whether this plan answers at all: a program whose steps hold no result step (a test body ending on an assert,
     *  the test-is-the-product plan's M6) answers no rows, its verdicts going to the program's sink. */
    public boolean answers() {
        return !(root instanceof Sequence s) || s.steps().stream()
                .anyMatch(x -> x instanceof TdsResult || x instanceof Keyed || x instanceof Constant);
    }

    /** Whether this plan's answer is a statement's rows ({@link #answerSql}), not a value the plan wrote
     *  ({@link Constant}). */
    public boolean sendsTheAnswer() {
        return !(answer(root) instanceof Constant);
    }

    /** The columns of this plan's typed answer where the plan states them: its result's (or its one result step's);
     *  null for a keyed result, whose per-key columns its keys name when it runs. */
    public @com.legend.base.Nullable List<Column> answerColumns() {
        return switch (answer(root)) {
            case TdsResult t -> t.columns();
            case Constant c -> c.columns();
            case Keyed k -> null;
            default -> throw new IllegalStateException("not reached: answer() keeps typed results only");
        };
    }

    private static Node answer(Node n) {
        return switch (n) {
            case TdsResult t -> t;
            case Keyed k -> k;
            case Constant c -> c;
            case Sequence s -> {
                List<Node> answers = s.steps().stream()
                        .filter(x -> x instanceof TdsResult || x instanceof Keyed || x instanceof Constant).toList();
                if (answers.size() != 1) {
                    throw new IllegalStateException("a plan of steps answers through one result step; this one has "
                            + answers.size());
                }
                yield answers.get(0);
            }
            case Update u -> throw new IllegalStateException("a plan that only changes its database answers no rows");
            case Effect e -> throw new IllegalStateException("a plan that only sends an effect answers no rows");
            case Check c -> throw new IllegalStateException("a plan that only checks answers no rows");
            case Load u -> throw new IllegalStateException("a plan that only seeds answers no rows");
        };
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

    /** An execution's trace comment, as legend-engine's executor writes one per execution: the run stamps a
     *  statement with one, and planning writes the one a frame's activity row names (2026-11-09: one spelling for
     *  both halves). */
    public static String traceComment(java.util.UUID id) {
        return "-- \"executionTraceID\" : \"" + id + "\"";
    }

    /** {@code [lower..upper]}; {@code upper} null is {@code *}. */
    public record Multiplicity(int lower, @com.legend.base.Nullable Integer upper) {
        public Multiplicity {
            if (lower < 0 || (upper != null && upper < lower)) {
                throw new IllegalArgumentException("multiplicity [" + lower + ".." + upper + "]");
            }
        }

        /** Source-style spelling, as the compiler's {@code Multiplicity.Bounded.text} spells it: {@code [1]},
         *  {@code [0..1]}, {@code [*]}, {@code [1..*]} (the run's own, 2026-11-09: the walk's contract message reads
         *  it, and the run names no compiler type). */
        public String text() {
            if (upper == null) {
                return lower == 0 ? "[*]" : "[" + lower + "..*]";
            }
            return lower == upper ? "[" + lower + "]" : "[" + lower + ".." + upper + "]";
        }
    }

    /** A node of the plan. A test body's verdict groups and branches are its test plan's (the plan-as-data design's
     *  step 3, 2026-11-08: {@code testexecutionplan.TestPlan}), never the product's. */
    public sealed interface Node permits Sequence, TdsResult, Keyed, Update, Check, Effect, Load, Constant {
    }

    /**
     * A VALUE the program wrote, answered with nothing sent (the test-is-the-product plan's M2, B11, MV12, 2026-10-31):
     * its one column {@code value} with its type facts and its elements as TEXT cells, each the literal's spelling as
     * the program wrote it (null an absent value), read back by the column's declared type -- the same form a
     * constant verdict side carries ({@link Side.Constant}). A query whose answer the planner knows as written: a
     * String or Boolean literal (the kinds whose database spelling cannot differ from the literal's), a DDL text the
     * model gives.
     */
    public record Constant(List<Column> columns, List<List<String>> cells) implements Node {
        public Constant {
            columns = List.copyOf(columns);
            if (columns.size() != 1 || !columns.get(0).name().equals("value")) {
                throw new IllegalArgumentException("a constant answer is one column, value: given "
                        + columns.stream().map(Column::name).toList());
            }
            List<List<String>> copied = new java.util.ArrayList<>(cells.size());
            for (List<String> row : cells) {
                if (row.size() != 1) {
                    throw new IllegalArgumentException("a constant answer's row of " + row.size() + " cells");
                }
                // a cell may be null (an absent value), which List.of refuses
                copied.add(java.util.Collections.unmodifiableList(new java.util.ArrayList<>(row)));
            }
            cells = java.util.Collections.unmodifiableList(copied);
        }
    }

    /**
     * Runs its steps in order, on one session. A sequence holding an {@link Update} runs in ONE transaction of its
     * session (docs/WRITES_DESIGN_2026_10_18.md §7, D7, and §8, leg 7): its changes all land or none do, and a
     * caller's open transaction is joined, never committed. Its answer is its one result step's (the steps after it
     * drop what the steps before it staged), passed on once its changes have committed: no answer leaves for a change
     * rolled back. A keyed result may be a step (the seam program's S1a, 2026-10-30: a test body's dynamic pivot
     * beside its lets and its verdicts): its snapshot is the sequence's transaction, opened at the isolation the keyed
     * result asks where the session owns it. A verdict group may be a step (S1b): a program's, run by
     * {@code PlanRunner.program}, whose answer may be no rows at all (a body ending in an assert).
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

    /**
     * A statement sent for what it DOES, its kind the statement's own -- a raw statement a program wrote
     * ({@code executeInDb}), a DDL, a script of several, a seed's rows -- which may answer rows it does not read: the
     * body driver's effects (docs/RESULT_BOUNDARY_PROGRAM_2026_10_26.md, phase 5: every statement a body sends is a
     * plan's). The runner says whether it answered rows.
     */
    public record Effect(Sql sql, @com.legend.base.Nullable Script script) implements Node {
        public Effect {
            Objects.requireNonNull(sql, "sql");
            if (script != null && !sql.slots().isEmpty()) {
                throw new IllegalArgumentException("an effect script binds no slot: its statements are its text");
            }
        }

        /** One statement sent for what it does. */
        public Effect(Sql sql) {
            this(sql, null);
        }

        /**
         * A SCRIPT of consecutive effect statements a program wrote (the test-is-the-product plan's B12, R7 decision
         * 14): {@code statements} as written; {@code bracketed}, the script in its dialect's bracket (DuckDB's
         * {@code BEGIN TRANSACTION ... COMMIT}, so a failure applies nothing; H2 sends it bare), sent where the session
         * owns its transaction and the program opened none; {@code abort}, the statement that closes a bracket a
         * failure left open, sent before the failure is raised (null where the dialect brackets nothing). Elsewhere
         * the effect's own {@code sql} -- the statements joined, bare -- is sent. The texts are the dialect's
         * ({@code SqlDialect.script}, {@code scriptAbort}); the runner writes none.
         */
        public record Script(List<String> statements, String bracketed, @com.legend.base.Nullable String abort) {
            public Script {
                statements = List.copyOf(statements);
                if (statements.isEmpty()) {
                    throw new IllegalArgumentException("an effect script holds at least one statement");
                }
                Objects.requireNonNull(bracketed, "bracketed");
            }
        }
    }

    /**
     * A LOAD (docs/DATA_IN_THE_PLAN_2026_11_09.md 2.2): data entering a program's database at its turn, run on the
     * program's session (the system metamodel store's on the system database's). {@code cadence} says when it runs;
     * {@code identity} names what it loads (today the planner's spelling of its sources, or of its file's digest);
     * {@code target} is the program's (the M3 audit's B3: a program of loads and a constant answer opens its session by
     * it). A test door's step (refused on the product's query door, B6). Until 2026-11-09 the program's seed step
     * ({@code Setup}), the seeds' cadence alone.
     */
    public record Load(String identity, Cadence cadence, List<LoadStep> steps, Target target) implements Node {
        /** The prefix every load identity carries (the system store's too since L4a, 2026-11-09). */
        public static final String IDENTITY_PREFIX = "load:";

        /** A load's CANONICAL content, the text its identity digests (docs/DATA_IN_THE_PLAN_2026_11_09.md 2.2: one
         *  scheme for every load): its cadence, then each step's content -- a statement's text, a rows step's statements
         *  and cells, a file's reference (namespace, name, digest, size, format), size limit and delivery -- each field
         *  length-prefixed and a cell's absence marked, so no two loads that run differently spell one text (the design
         *  audit's H1, 2026-11-02). Data, no digest: the planner and exec each hash it with their own. */
        public static String canonical(Cadence cadence, List<LoadStep> steps) {
            StringBuilder sb = new StringBuilder();
            field(sb.append('C'), cadence.name());
            for (LoadStep step : steps) {
                switch (step) {
                    case LoadStep.Statement st -> field(sb.append('S'), st.sql());
                    case LoadStep.Rows r -> {
                        field(field(field(field(sb.append('R'), r.stagingTable()), r.createStaging()), r.copy()),
                                r.dropStaging());
                        sb.append(r.rows().size()).append(':');
                        for (List<String> row : r.rows()) {
                            sb.append(row.size()).append(':');
                            for (String cell : row) {
                                if (cell == null) {
                                    sb.append('N');
                                } else {
                                    field(sb.append('V'), cell);
                                }
                            }
                        }
                    }
                    case LoadStep.File f -> {
                        field(field(field(sb.append('F'), f.ref().namespace().name()), f.ref().name()), f.ref().digest());
                        sb.append(f.ref().size()).append(':').append(f.sizeLimitMegabytes()).append(':');
                        switch (f.ref().format()) {
                            case DataRef.Csv csv -> sb.append("csv").append(csv.delimiter()).append(csv.quote());
                        }
                        switch (f.delivery()) {
                            case LoadStep.Delivery.Reads r -> {
                                sb.append("reads").append(r.statements().size()).append(':');
                                for (String statement : r.statements()) {
                                    field(sb, statement);
                                }
                            }
                            case LoadStep.Delivery.Streamed st -> field(sb.append("streamed").append(st.columns())
                                    .append(':'), st.insert());
                        }
                    }
                }
            }
            return sb.toString();
        }

        private static StringBuilder field(StringBuilder sb, String text) {
            return sb.append(text.length()).append(':').append(text);
        }

        public Load {
            Objects.requireNonNull(target, "target");
            Objects.requireNonNull(cadence, "cadence");
            Objects.requireNonNull(identity, "identity");
            if (identity.isEmpty()) {
                throw new IllegalArgumentException("a load names its identity");
            }
            steps = List.copyOf(steps);
            if (steps.isEmpty()) {
                throw new IllegalArgumentException("a load with no step: plan none");
            }
        }

        /** When a load runs (the design's 2.2; the cadences a target's loads take arrive with its slice L2). */
        public enum Cadence {
            /** The SEEDS a program's next statement reads (B12, D35: a {@code from()}'s inline test data, its named
             *  runtime's, the program's runtime's): skipped where the session holds {@code identity} and has run no
             *  write since (the seeds' memo, {@code SessionState}); else run, and the identity recorded. */
            BEFORE_STEP,
            /** A program's own load at its place ({@code loadCsvToDbTable}): run every time it is reached, a write as
             *  an effect is, never skipped. */
            AT_STEP,
            /** A target's load (a connection's declared test data, L2): established when a session is acquired for the
             *  target, a write, as legend-engine establishes a test connection on each acquisition. Per source (the
             *  contract exec honours or refuses: the M1 tenets audit's finding 7, 2026-11-09): a caller's session
             *  ({@code PlanSessions.acquired}: the product door's, every query) -- once per run, on the run's first open;
             *  a database the platform makes and keeps for the target ({@code PlanSessions.shared}, the PCT lanes'
             *  {@code lane}) -- once, when it is made; a private database for a write ({@code openToChange}) -- once,
             *  when it is made; a source that establishes none (a caller's session as it is) refuses a target that has
             *  one; a program node of this cadence is refused. */
            PER_ACQUISITION,
            /** A project's Data rows (L3, S171): made once, when the platform makes the project's database the target
             *  names, and kept with it for the process; never run on a database the plan does not make. The system
             *  catalog's tables and the platform's rows alike (L4b): made once per engine per process. */
            ONCE,
            /** A model's registration in the process's system catalog (L4b, docs/SYSTEM_CATALOG_DESIGN_2026_11_08.md 4.5):
             *  a program's node, run inside the run's own transaction on the catalog -- which is rolled back when the
             *  run ends, so no other run sees it -- once per run (the seeds' memo, which the rollback clears); refused
             *  by name on a session that holds no such transaction. */
            PER_TRANSACTION
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
     * that way, {@code result.RowDecode}.) A RELATION of NO column is a raw grid's (the planner-driver seam
     * program's S3c, 2026-10-30): the columns its result reports, read from the real run by the session
     * ({@code Statements.reportedRows}) and handed on before its rows, never read at planning.
     */
    public record TdsResult(Shape shape, List<Column> columns, Sql sql) implements Node {
        public TdsResult {
            Objects.requireNonNull(shape, "shape");
            columns = List.copyOf(columns);
            Objects.requireNonNull(sql, "sql");
            if (!sql.regions().isEmpty()) {
                throw new IllegalArgumentException("a typed result's statement is written once: per-key regions are a"
                        + " keyed result's body");
            }
            if (shape != Shape.RELATION && columns.size() != 1) {
                throw new IllegalArgumentException("a " + shape + " answer is one column, given "
                        + columns.stream().map(Column::name).toList());
            }
        }

        /**
         * What the rows ARE (docs/RESULT_BOUNDARY_PROGRAM_2026_10_26.md §2: the plan describes types, never an output
         * format): a relation's rows; a value's elements, one per row in the one column {@code value}, whose type and
         * multiplicity are the value's; or a graph fetch's objects, one per row in the one column whose type is their
         * class and whose multiplicity is the fetch's, each cell the object's JSON text as the database built it (a
         * class instance's typed encoding, R6). A writer reads the shape to choose its layout; nothing else does.
         */
        public enum Shape { RELATION, VALUE, OBJECTS }
    }

    /**
     * A result whose columns are partly NAMED BY THE DATA: a dynamic pivot's (the plan path program's probe plan,
     * 2026-10-20, docs/PLAN_PATH_PROGRAM_2026_10_17.md §9). {@code keys} runs first and answers one row per key, in the
     * order the key's columns take, every cell a text the database writes: cell 0 the key, which the body's per-key
     * placeholders bind ({@link Binding.Key}), and then each per-key output's name for a key ({@link PerKey#name}),
     * written under {@code keyFields} (2026-10-20: a session reading a cell by name reads it so).
     * {@code body} is written once, with per-key regions ({@link Sql#regions}) the runner repeats once per key; its
     * rows' cells are {@code outputs} in order, a per-key output once per key, key by key. Both statements run on one
     * session, inside one {@code snapshot}, so the keys are the body's. The cells are typed by their columns' facts and
     * answered as typed rows. {@code perKeyAt}: how many of the fixed columns the answer places before the per-key ones
     * -- the relation's own column order (Pure's pivot answers its grouping columns, then the per-key ones, then any
     * column a later step adds), where the body answers every fixed column first and the runner places each cell
     * (2026-10-27, the result boundary program's phase 5: an extend after a pivot answered before the per-key
     * columns). {@code maxColumns}: the most columns the database
     * answers, a body asked for more refused by name before it is sent; null where the database has no limit said.
     * {@code shape}: the answer's shape, as a typed result's ({@link TdsResult.Shape}): a pivot's is a relation; a raw
     * grid's report read as a value (its column names, its rows' cells) is a value (the test-is-the-product plan's
     * M2, its audit: a writer chooses its layout by it, and a value answered as a relation was written as one).
     */
    public record Keyed(Sql keys, KeysOf keysOf, List<String> admitted, List<String> keyFields, List<Output> outputs,
            int perKeyAt, Sql body, Snapshot snapshot, @com.legend.base.Nullable Integer maxColumns,
            TdsResult.Shape shape) implements Node {
        public Keyed {
            Objects.requireNonNull(keys, "keys");
            Objects.requireNonNull(shape, "shape");
            if (shape == TdsResult.Shape.OBJECTS) {
                throw new IllegalArgumentException("a keyed result answers a relation or a value, not objects");
            }
            Objects.requireNonNull(keysOf, "keysOf");
            admitted = List.copyOf(admitted);
            if (keysOf == KeysOf.ROWS && !admitted.isEmpty()) {
                throw new IllegalArgumentException("a keyed result whose keys are rows admits no reported kinds");
            }
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
            if (!perKey && keysOf == KeysOf.ROWS) {
                throw new IllegalArgumentException("a keyed result with no per-key output");
            }
            if (keysOf == KeysOf.COLUMNS && !keys.slots().isEmpty()) {
                throw new IllegalArgumentException("a statement read for its reported columns binds nothing");
            }
            long fixedOutputs = outputs.stream().filter(o -> o instanceof Fixed).count();
            if (perKeyAt < 0 || perKeyAt > fixedOutputs) {
                throw new IllegalArgumentException("the per-key columns placed after " + perKeyAt + " of "
                        + fixedOutputs + " fixed ones");
            }
            long perKeyOutputs = outputs.stream().filter(o -> o instanceof PerKey).count();
            if (keyFields.size() != perKeyOutputs + 1) {
                throw new IllegalArgumentException("a keys probe answers the key's text and each per-key output's name:"
                        + " " + keyFields.size() + " fields for " + perKeyOutputs + " per-key outputs");
            }
        }

        /** A keyed result whose keys are a statement's ROWS (a dynamic pivot's distinct values). */
        public Keyed(Sql keys, List<String> keyFields, List<Output> outputs, int perKeyAt, Sql body,
                Snapshot snapshot, @com.legend.base.Nullable Integer maxColumns) {
            this(keys, KeysOf.ROWS, List.of(), keyFields, outputs, perKeyAt, body, snapshot, maxColumns,
                    TdsResult.Shape.RELATION);
        }

        /**
         * What a key IS: a row the keys statement answers ({@code ROWS}: a dynamic pivot's, its cells the key and each
         * per-key output's name), or a column its result REPORTS ({@code COLUMNS}: a raw grid's, the planner-driver
         * seam program's S3c, 2026-10-30), read as the session reads a result's columns ({@code Statements.columns},
         * the keys statement a {@code LIMIT 0} the planner wrote), each key one cell, the column's label, in the
         * result's order. A body keyed by columns answers fixed columns alone where its regions spell rows (a grid's
         * names, its cells). {@code admitted}: the slots a reported column may be in for the body to spell its cells
         * exactly (the cell's JSON keeps an integer's, a text's, a boolean's, a double's kind; the database's text is
         * the engine's for an integer and a text alone), every other column refused by name before the body is sent
         * -- never spelled approximately (AGENTS.md invariant 4); empty: any column (the names alone are read).
         */
        public enum KeysOf { ROWS, COLUMNS }

        /** One output column of the body; {@code field}, the name the body's statement writes its cells under (a
         *  session reading a cell by name reads it so; JDBC reads by place). */
        public sealed interface Output permits Fixed, PerKey {
            Column column();

            String field();
        }

        /** A column answered once: its facts. */
        public record Fixed(Column column, String field) implements Output {
            public Fixed {
                Objects.requireNonNull(column, "column");
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

    /** A column of a plan's typed rows, with its three type facts
     *  (docs/PLAN_PATH_PROGRAM_2026_10_17.md §3): {@code type}, its Pure type as the typer names it (a primitive's
     *  name, {@code Decimal(10,2)}, an enumeration's path), and its {@code multiplicity}; {@code slot}, the SQL type
     *  the database answers with (the lowering's typed slot: {@code VARCHAR}, {@code DECIMAL(10,2)},
     *  {@code TIMESTAMP}) -- null when the column has no one slot (a nested column the query flattens into prefixed
     *  outputs; a pivot's generated column whose template carries no type); and {@code store}, the column's declared
     *  type in its store when the value is a store column read through ({@code VARCHAR(20)}, as the relational
     *  grammar spells it) -- null when the column is computed. Each null is said, never guessed. A fourth fact,
     *  {@code precision} (2026-10-28, the result boundary program's R2 and its S1.4 item 1): how a temporal cell's
     *  WRITTEN precision is known, which the writers spell by ({@link Precision}; the plan's fact, where the writers
     *  matched the slot's spelling for it) -- null when no temporal stands in the column itself. */
    public record Column(String name, String type, Multiplicity multiplicity, @com.legend.base.Nullable String slot,
            @com.legend.base.Nullable String store, @com.legend.base.Nullable Precision precision) {
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

    /**
     * How a temporal cell's WRITTEN precision is known (docs/RESULT_BOUNDARY_PROGRAM_2026_10_26.md R2: a fact that only
     * Pure knows travels in the plan or in the data, never in a text; the start page's S1.4 item 1, 2026-10-28). A
     * column's fact, set by {@link ColumnFacts#precision} from its Pure type, its slot and its result's shape, read
     * by the writers ({@code result/CellText}) in place of the slot's spelling they matched until then. The spellings
     * are upstream's, read from the pinned sources 2026-10-29 (the upstream-fidelity tenet, AGENTS.md).
     */
    public enum Precision {
        /** The cell carries its own precision, as the value was written (a literal's spelling, a temporal-text
         *  carrier: a year, a month, a day, an hour, a minute, the subsecond digits as written); spelled as it is,
         *  as legend-pure prints a value it holds. */
        WRITTEN,
        /** A day: a DATE slot, a StrictDate column. Spelled as its day ({@code yyyy-MM-dd}: legend-engine's
         *  {@code ValueTransformer} for a {@code java.sql.Date}, legend-pure's StrictDate). */
        DAY,
        /** A relation's time-bearing cell, whatever carried it (a TIMESTAMP's nine digits as the driver hands them,
         *  a literal's text): spelled as legend-engine's TDS spells it, nine fraction digits and {@code +0000}
         *  ({@code ValueTransformer}: its JSON and its transformed CSV, {@code yyyy-MM-dd'T'HH:mm:ss.nnnnnnnnnZ};
         *  legend-pure reads a SQL timestamp the same way, {@code DateFunctions.fromSQLTimestamp}), never as a day.
         *  The product's spelling of a relation; the PCT's in-memory relation ({@code TestTDS}, millisecond dates,
         *  three digits) is its harness's. */
        INSTANT,
        /** A value root's cell the database computed (the plan's VALUE shape: a scalar, a collection's element): an
         *  instant whose written precision the database lost, spelled at the precision its digits show (the
         *  fraction's trailing zeros dropped, a whole second with none): the value lane's canon of 2026-10-18 and
         *  the closest text to legend-pure's, which prints the precision written. A divergence, ledgered:
         *  docs/SEMANTICS_REGISTER.md S118 (2026-10-29). */
        MINIMAL
    }

    /**
     * One statement, final for its target: the text with bind placeholders, the parameter each placeholder binds and how
     * (in order), and the target it runs on. The runner sends the text as it is: a placeholder is typed by its declared
     * type when the plan is made (D1; P4, 2026-11-09 -- until then H2's text had a type hole the runner filled).
     * {@code tree} is the typed SQL tree it was rendered from — metadata for lineage and explanation, never read by the
     * executor; it is not written to the lite JSON yet (a later step), so a plan read back from JSON has none.
     * {@code stamp}, where given, is the parameter the statement's own execution-trace stamp binds (the
     * test-is-the-product plan's M3f, R17 C4): the runner mints the comment the statement is sent under before it sends
     * it and binds it under that name, for the statement's own slots and the steps after (an activity row records the
     * comment of the statement that ran its frame, minted per execution as legend-engine mints it).
     */
    public record Sql(String statement, List<Slot> slots, Target target,
            com.legend.sql.@com.legend.base.Nullable SqlQuery tree, List<Region> regions,
            @com.legend.base.Nullable String stamp) {
        public Sql {
            Objects.requireNonNull(statement, "statement");
            slots = List.copyOf(slots);
            Objects.requireNonNull(target, "target");
            regions = List.copyOf(regions);
            Region.check(statement.length(), slots, regions, s -> s.binding() instanceof Binding.Key);
        }

        /** A statement whose stamp nothing reads. */
        public Sql(String statement, List<Slot> slots, Target target,
                com.legend.sql.@com.legend.base.Nullable SqlQuery tree, List<Region> regions) {
            this(statement, slots, target, tree, regions, null);
        }

        /** A statement with no per-key region. */
        public Sql(String statement, List<Slot> slots, Target target,
                com.legend.sql.@com.legend.base.Nullable SqlQuery tree) {
            this(statement, slots, target, tree, List.of());
        }

        /** This statement, its own execution-trace stamp bound under {@code name}. */
        public Sql stamped(String name) {
            return new Sql(statement, slots, target, tree, regions, name);
        }

        /** A statement of no slot and no tree, final for {@code target}: a schema probe's, a setup's, a session
         *  statement's (the doors, 2026-10-29: every statement a body or a seed sends is a plan statement). */
        public static Sql of(String statement, Target target) {
            return new Sql(statement, List.of(), target, null, List.of());
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
         *  {@code java.sql.JDBCType}'s name). {@code asText}: set where the placeholder is a date or date-time cast to
         *  its declared type's SQL type, which takes the value as its text (D1; P4, 2026-11-09: until then a type hole
         *  the runner wrote into the statement). */
        record One(String nullType, @com.legend.base.Nullable AsText asText) implements Binding {
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

    /** A date or date-time bound as its TEXT to a placeholder cast to its declared type's SQL type: {@code fraction}
     *  the digits of a second the text keeps, the finer ones cut, as the database's literal keeps them (Postgres six);
     *  null for all nine. */
    public record AsText(@com.legend.base.Nullable Integer fraction) {
        public AsText {
            if (fraction != null && (fraction < 0 || fraction > 9)) {
                throw new IllegalArgumentException("a second's fraction of " + fraction + " digits");
            }
        }
    }

    /**
     * Where a statement runs: the database; the statements every connection to it runs first ({@code session}: its
     * settings, such as the time zone); and the LOADS that establish it when a session is acquired for it, before
     * anything else ({@code loads}: a connection's declared test data, written at plan time for the target's database;
     * docs/DATA_IN_THE_PLAN_2026_11_09.md L2, 2026-11-09 -- until then the target's {@code setup} steps). Each is a
     * {@link Load.Cadence#PER_ACQUISITION} load, after any {@link Load.Cadence#ONCE} one (a project's Data, made with its
     * database: L3), whose own target is this one's database and session with no load (a
     * plan's JSON writes each load once, in its load section, and a target names it by identity). Two runs share an
     * in-memory database when their targets are equal (decision A, docs/EXECUTION_PLAN_BOUNDARY_2026_10_05.md §9): the
     * loads by their identities, which digest their content. No server version: every database's statements are spelled
     * alike for every version lite runs (2026-11-05, one H2).
     */
    public record Target(Database database, List<String> session, List<Load> loads) {
        public Target {
            Objects.requireNonNull(database, "database");
            session = List.copyOf(session);
            loads = List.copyOf(loads);
            java.util.Set<String> identities = new java.util.HashSet<>();
            boolean acquired = false;
            for (Load load : loads) {
                // the project's Data first (made once with the database), then what each acquisition establishes
                if (load.cadence() == Load.Cadence.PER_ACQUISITION) {
                    acquired = true;
                } else if (load.cadence() != Load.Cadence.ONCE || acquired) {
                    throw new IllegalArgumentException("a target's loads are its ONCE loads, then its PER_ACQUISITION"
                            + " ones: " + load.identity() + " is " + load.cadence());
                }
                if (!load.target().loads().isEmpty() || !load.target().database().equals(database)
                        || !load.target().session().equals(session)) {
                    throw new IllegalArgumentException("a target's load loads into the target's own database and"
                            + " session, and names no load of its own: " + load.identity());
                }
                if (!identities.add(load.identity())) {
                    throw new IllegalArgumentException("a target names a load twice: " + load.identity());
                }
            }
        }

        /** This target with no load: where its loads load. */
        public Target bare() {
            return loads.isEmpty() ? this : new Target(database, session, List.of());
        }
    }

    /**
     * A PROJECT'S OWN DATABASE (docs/ONE_PLATFORM_PROGRAM_2026_10_12.md C5; the plan-as-data design's hole 7, the user's
     * go of 2026-11-08): what the database holds when it is made, part of its identity -- named by what it holds, so
     * an edited row or table is a fresh database, and two models whose stores differ never share one (type audit D100).
     * What fills it is the target's {@link Load.Cadence#ONCE} load (the project's Data rows,
     * docs/DATA_IN_THE_PLAN_2026_11_09.md L3, S171): made once where the platform makes the database, and kept; until
     * L3 the project's own {@code made} steps.
     *
     * @param key  the model's stores' and Data rows' content key ({@code ProjectData.contentKey})
     * @param refused why the model's Data rows cannot make it (a table its Databases declare none or more than once
     *                of), or null: raised where the runner would make it, never by a run on another database (the
     *                plan-as-data design's slice 5, 2026-11-09: planned as a failure until then, every query of such
     *                a model refused at planning; before hole 7 the server refused it as it made the database)
     */
    public record Project(String key, @com.legend.base.Nullable String refused) {
        public Project {
            Objects.requireNonNull(key, "key");
        }
    }

    /** The database a target runs on: one a connection declares, the platform's own engine, or the graph's system
     *  database on an engine. A declared or platform database is a PROJECT'S own where {@code project} is non-null
     *  (the server's queries), else the database itself (the test road; a URL; a database the user names). */
    public sealed interface Database permits Database.Declared, Database.Platform, Database.System {

        /** The database's type: what its session must be. */
        DatabaseType type();

        /** The database a connection declares: its session is opened by this definition (or a caller's is checked
         *  against its type). */
        record Declared(Connection connection, @com.legend.base.Nullable Project project) implements Database {
            public Declared {
                Objects.requireNonNull(connection, "connection");
            }

            @Override
            public DatabaseType type() {
                return connection.databaseType();
            }
        }

        /** The platform's own in-process engine, for a runtime that binds no database, only model data
         *  (docs/SEMANTICS_REGISTER.md S27). */
        record Platform(DatabaseType type, @com.legend.base.Nullable Project project) implements Database {
            public Platform {
                Objects.requireNonNull(type, "type");
            }
        }

        /** THE GRAPH'S SYSTEM DATABASE on {@code type} (the test-is-the-product plan's M10a, the narrow M4,
         *  2026-11-01): the metamodel store's tables, written once per graph by the one who opens it -- the sessions
         *  source a caller hands the runner; the runner knows no database (the user ruling of 2026-09-02: separate
         *  from every user connection). A test body reading the metamodel store runs its program here; the product's
         *  own metamodel carriage is unchanged (the user, 2026-11-01: a product follow-up). {@code graphKey} names WHICH
         *  store (the plan-as-data design's step 7, 2026-11-08, 2.6): a hash of the store's content -- every table's
         *  definition and the graph's rows -- so two plans of one graph name one store, and the plan, not the compiled
         *  graph object beside it, says which. */
        record System(DatabaseType type, String graphKey) implements Database {
            public System {
                Objects.requireNonNull(type, "type");
                Objects.requireNonNull(graphKey, "graphKey");
            }
        }
    }

    /** One step of a load or of a target's setup: a statement, rows the database's own bulk loader takes, or a file
     *  by its reference (docs/DATA_IN_THE_PLAN_2026_11_09.md 2.3; until 2026-11-09 {@code SetupStep}). */
    public sealed interface LoadStep permits LoadStep.Statement, LoadStep.Rows, LoadStep.File {

        /** A statement, run as it is. */
        record Statement(String sql) implements LoadStep {
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
                List<List<String>> rows) implements LoadStep {
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

        /**
         * A FILE'S ROWS into a table, the file named by its reference, never by a machine's path and never by its rows
         * (the design's 2.1 and 2.5, the user's go of 2026-11-09): the runner resolves {@code ref} through the stores
         * it is given, refuses bytes whose digest is not the reference's, and delivers exactly the verified bytes as
         * {@code delivery} says, which the planner chose for the target's database. A CSV file's first line is its
         * header; its cells are text the database casts into the table's columns, positionally. {@code sizeLimitMegabytes}:
         * the largest file the load admits, refused at planning and again at the run on the bytes it reads (the user's
         * ruling, 2026-11-09: upstream's {@code loadCsvToDbTable} admits 500 MB).
         */
        record File(DataRef ref, Delivery delivery, int sizeLimitMegabytes) implements LoadStep {
            public File {
                Objects.requireNonNull(ref, "ref");
                Objects.requireNonNull(delivery, "delivery");
                if (sizeLimitMegabytes < 1) {
                    throw new IllegalArgumentException("a file's load admits at least a megabyte: " + sizeLimitMegabytes);
                }
            }
        }

        /** How a file's verified bytes reach its database: chosen at planning for the target's database, as every
         *  spelling is; no delivery edits a statement at run (the copy's path is bound). */
        sealed interface Delivery permits Delivery.Reads, Delivery.Streamed {

            /**
             * THE DATABASE READS THE FILE ITSELF (the user's ruling, 2026-11-09: "reject options we cannot support
             * instead of building our own streamer"): the runner writes the verified bytes to a copy of its own and sends
             * {@code statements} in order, each {@code ?} in them bound to that copy's path -- its database's own reader
             * spelled by its dialect, the declared format in that reader's options (DuckDB's {@code read_csv}). A format a
             * reader cannot express is refused at planning. A database with no reader that reads the format as upstream
             * does takes {@link Streamed} (the user's ruling of 2026-11-10, "Okay I guess that can be used for databases that have no csv read then").
             */
            record Reads(List<String> statements) implements Delivery {
                public Reads {
                    statements = List.copyOf(statements);
                    if (statements.isEmpty()) {
                        throw new IllegalArgumentException("a file's load of no statement");
                    }
                }
            }

            /**
             * THE RUNNER STREAMS THE FILE'S CELLS, split by upstream's own rule ({@code base.CsvCells}, Commons CSV 1.5
             * {@code EXCEL}, measured equal on 2,000,000 random texts) -- upstream's own mechanism, which reads the file
             * in Java and inserts its rows (the user's ruling of 2026-11-10, "Okay I guess that can be used for databases that have no csv read then"): for every database with no reader that reads the format as upstream does
             * (H2's {@code CSVREAD} pads and cuts records in silence, S185 withdrawn). {@code insert} is one row's
             * insert, a {@code ?} per column, sent in one batch with each row's {@code columns} cells bound as text.
             */
            record Streamed(String insert, int columns) implements Delivery {
                public Streamed {
                    Objects.requireNonNull(insert, "insert");
                    if (columns < 1) {
                        throw new IllegalArgumentException("a streamed load of no column");
                    }
                }
            }
        }
    }
}
