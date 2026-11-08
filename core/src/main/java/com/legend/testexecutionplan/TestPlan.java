// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.testexecutionplan;

import com.legend.executionplan.ExecutionPlan;
import com.legend.executionplan.ExecutionPlan.Column;
import com.legend.executionplan.ExecutionPlan.Keyed;
import com.legend.executionplan.ExecutionPlan.Node;
import com.legend.executionplan.ExecutionPlan.TdsResult;

import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * A TEST BODY'S PLAN (docs/PLAN_AS_DATA_2026_11_07.md 2.1, the design's step 3, 2026-11-08): the product's plan
 * ({@link ExecutionPlan}) keeps what a product query or service needs; a test body's program is this record around it --
 * its steps the product's nodes ({@link Run}), a frame let's eager run ({@link FrameRun}), and the steps only a test
 * holds: its asserts as verdict groups ({@link Verdicts}) and its branches over them ({@link Branch}). Data alone, read
 * back from its JSON ({@code TestPlanJson}) whole: the run reads nothing else. The judge it was planned for is the
 * plan's ({@link Judge}): one body is planned by one judge.
 *
 * @param parameters the body's declared parameters, the product plan's own
 * @param steps      the program's steps, in order
 * @param judge      the judge the body's asserts were planned for
 * @param extensionsByStep    the compatibility edge's records planned at a step, by the step's index (an effect's or a
 *                            seed's ledger texts: the plan-as-data design's step 8, 2.7); empty under lite's own rules
 * @param extensionsByVerdict the edge's records planned for a verdict, by its id; empty under lite's own rules
 * @param answersLastVerdict  the body's last statement is an assert: its verdict is the body's answer, written as host
 *                            rows to the caller's sink (a fact the planner decides; the slice 2 invariant audit's hole
 *                            3, 2026-11-08: the router decided it after the run from the planner's objects)
 */
public record TestPlan(List<ExecutionPlan.Parameter> parameters, List<TestStep> steps, Judge judge,
        Map<Integer, List<Extension>> extensionsByStep, Map<Integer, List<Extension>> extensionsByVerdict,
        boolean answersLastVerdict) {

    public TestPlan {
        parameters = List.copyOf(parameters);
        steps = List.copyOf(steps);
        Objects.requireNonNull(judge, "judge");
        if (steps.isEmpty()) {
            throw new IllegalArgumentException("a test plan has at least one step");
        }
        extensionsByStep = copied(extensionsByStep);
        extensionsByVerdict = copied(extensionsByVerdict);
    }

    /** A plan with nothing planned for the edge (lite's own rules), whose last statement answers no verdict. */
    public TestPlan(List<ExecutionPlan.Parameter> parameters, List<TestStep> steps, Judge judge) {
        this(parameters, steps, judge, Map.of(), Map.of(), false);
    }

    /** A plan whose last statement answers no verdict. */
    public TestPlan(List<ExecutionPlan.Parameter> parameters, List<TestStep> steps, Judge judge,
            Map<Integer, List<Extension>> extensionsByStep, Map<Integer, List<Extension>> extensionsByVerdict) {
        this(parameters, steps, judge, extensionsByStep, extensionsByVerdict, false);
    }

    /** The same plan carrying {@code byVerdict}, its verdicts' records (the appeals they name). */
    public TestPlan withExtensionsByVerdict(Map<Integer, List<Extension>> byVerdict) {
        return new TestPlan(parameters, steps, judge, extensionsByStep, byVerdict, answersLastVerdict);
    }

    private static Map<Integer, List<Extension>> copied(Map<Integer, List<Extension>> m) {
        java.util.TreeMap<Integer, List<Extension>> out = new java.util.TreeMap<>();
        m.forEach((k, v) -> out.put(k, List.copyOf(v)));
        return java.util.Collections.unmodifiableMap(out);
    }

    /** Which judge a body's verdict steps were planned for: the DATABASE judge (each group's fused statement, or each
     *  verdict's wrapper), the HOST judge (each verdict's base sides), or both, the AGREEMENT judge. */
    public enum Judge { HOST, DATABASE, AGREEMENT }

    /** A step of a test body's program. */
    public sealed interface TestStep permits Run, FrameRun, Verdicts, Branch {
    }

    /** A product step as the product's plan holds it (never a sequence: a program's steps are one list). */
    public record Run(Node node) implements TestStep {
        public Run {
            Objects.requireNonNull(node, "node");
            if (node instanceof ExecutionPlan.Sequence) {
                throw new IllegalArgumentException("a test plan's step is one node, never a sequence");
            }
        }
    }

    /** A frame let's EAGER RUN (the test-is-the-product plan's M9): a check sent where the let stands, its rows
     *  dropped, under the statement origin the walk marked for it, LET -- a fact of the plan, where it was the sink's
     *  answer for the step's place until the design's step 3. */
    public record FrameRun(ExecutionPlan.Check check) implements TestStep {
        public FrameRun {
            Objects.requireNonNull(check, "check");
        }
    }

    /** Whether this plan answers: a step that runs a typed result or a constant (a body ending on an assert answers
     *  no rows, its verdicts judged at their steps). */
    public boolean answers() {
        return steps.stream().anyMatch(s -> s instanceof Run r && answering(r.node()));
    }

    /** The product node that answers: its one answering step's, refused by name where it has none or several. */
    public Node answer() {
        List<Node> answers = steps.stream().filter(s -> s instanceof Run r && answering(r.node()))
                .map(s -> ((Run) s).node()).toList();
        if (answers.size() != 1) {
            throw new IllegalStateException("a test plan answers through one result step; this one has "
                    + answers.size());
        }
        return answers.get(0);
    }

    /** The shape of this plan's answer (the product's rule: {@link ExecutionPlan#answerShape}). */
    public TdsResult.Shape answerShape() {
        return new ExecutionPlan(parameters, answer()).answerShape();
    }

    /** Whether this plan's answer is a statement's rows, not a value the plan wrote. */
    public boolean sendsTheAnswer() {
        return !(answer() instanceof ExecutionPlan.Constant);
    }

    /** The statements a verdict group or a branch may read, each a typed or keyed result: a group's fused statement,
     *  each verdict's wrapper, base sides and message; a branch's condition and both sides' steps' (whichever side
     *  runs). What the runner's callback for the step declares; a product step or a frame run has none here. */
    public static List<Node> statements(TestStep step) {
        List<Node> out = new java.util.ArrayList<>();
        statementsOf(step, out);
        return out;
    }

    private static void statementsOf(TestStep step, List<Node> out) {
        switch (step) {
            case Verdicts v -> {
                if (v.fused() != null) {
                    out.add(v.fused());
                }
                for (Verdict each : v.verdicts()) {
                    if (each.wrapper() != null) {
                        out.add(each.wrapper());
                    }
                    each.base().forEach(side -> sideStatement(side, out));
                    if (each.message() != null) {
                        sideStatement(each.message(), out);
                    }
                }
            }
            case Branch b -> {
                sideStatement(b.condition(), out);
                b.then().forEach(inner -> statementsOf(inner, out));
                b.otherwise().forEach(inner -> statementsOf(inner, out));
            }
            case Run r -> throw new IllegalArgumentException("a product step's statements are the product plan's");
            case FrameRun f -> throw new IllegalArgumentException("a frame run's statement is its check's");
        }
    }

    private static void sideStatement(Side side, List<Node> out) {
        switch (side) {
            case Side.Rows r -> out.add(r.result());
            case Side.KeyedRows k -> out.add(k.keyed());
            case Side.Constant c -> {
            }
        }
    }

    private static boolean answering(Node n) {
        return n instanceof TdsResult || n instanceof Keyed || n instanceof ExecutionPlan.Constant;
    }

    /**
     * A run of consecutive ASSERTS as one step (the planner-driver seam program's S1b, 2026-10-30,
     * docs/PLANNER_DRIVER_SEAM_PROGRAM_2026_10_30.md §3.3: a test body's verdicts as steps the database executes; a
     * test plan's step, run through the runner's callback, never a plan's answer; in the product's plan until the
     * plan-as-data design's step 3, 2026-11-08). Each {@link Verdict} is one assert.
     * {@code fused}: the group's wrapper over its verdicts' wrappers -- the verdict statements as ONE statement
     * answering one row per verdict, the verdict's index in the group first (the lowering's fusion, composed when the
     * plan was made); null where the planner made none, and then each verdict's own wrapper runs. The test run sends
     * what the plan's judge asks ({@link Judge}) through the runner's open program; the runner judges nothing: the comparison rules and the messages are the judges' own code, reading the plan's
     * facts (the agreement's pairing among them, {@code agreement}). {@code roots}: the database judge's root loop of the segment this group closes (the plan-as-data
     * design's step 5, 2026-11-08), judged once the group's last verdict is in -- under the agreement judge, the
     * database half's loop of the assert the group closes; null on every other group. {@code agreement}: under the
     * agreement judge, the pairing of the assert this group closes (step 6), null otherwise.
     */
    public record Verdicts(List<Verdict> verdicts, @com.legend.base.Nullable TdsResult fused,
            @com.legend.base.Nullable Savepoint savepoint, JudgingRules rules,
            @com.legend.base.Nullable RootLoop roots, @com.legend.base.Nullable Agreement agreement)
            implements TestStep {
        public Verdicts {
            Objects.requireNonNull(rules, "a verdict group names the rules it is judged by");
            verdicts = List.copyOf(verdicts);
            if (verdicts.isEmpty()) {
                throw new IllegalArgumentException("a verdict group holds at least one verdict");
            }
            if (fused != null && fused.shape() != TdsResult.Shape.RELATION) {
                throw new IllegalArgumentException("a fused verdict statement answers a relation, one row per verdict");
            }
        }

        /** A group with no savepoint: its database's transaction survives a failing statement, the group runs
         *  outside any transaction, or no failure of its statements is one the program survives but the fused
         *  statement's. */
        public Verdicts(List<Verdict> verdicts, @com.legend.base.Nullable TdsResult fused, JudgingRules rules) {
            this(verdicts, fused, null, rules, null, null);
        }

        /** A group that closes no database segment and pairs no assert. */
        public Verdicts(List<Verdict> verdicts, @com.legend.base.Nullable TdsResult fused,
                @com.legend.base.Nullable Savepoint savepoint, JudgingRules rules) {
            this(verdicts, fused, savepoint, rules, null, null);
        }

        /** A group that pairs no assert. */
        public Verdicts(List<Verdict> verdicts, @com.legend.base.Nullable TdsResult fused,
                @com.legend.base.Nullable Savepoint savepoint, JudgingRules rules,
                @com.legend.base.Nullable RootLoop roots) {
            this(verdicts, fused, savepoint, rules, roots, null);
        }

        /** The same group closing a database segment, judged by {@code r} once its last verdict is in. */
        public Verdicts withRoots(@com.legend.base.Nullable RootLoop r) {
            return new Verdicts(verdicts, fused, savepoint, rules, r, agreement);
        }

        /** The same group closing the agreement judge's assert {@code a}. */
        public Verdicts withAgreement(@com.legend.base.Nullable Agreement a) {
            return new Verdicts(verdicts, fused, savepoint, rules, roots, a);
        }

        /**
         * F11 (the test-is-the-product plan's B12): on a database that ABORTS a transaction on a failing statement,
         * each statement of the group whose failure the program survives, sent inside a transaction, is guarded by a
         * savepoint -- {@code set} before it, {@code rollbackTo} when the database rejects it, {@code release}
         * otherwise: each verdict's host side read (an {@code assertError}'s side, whose error is its verdict's
         * answer; the walker-deletion program, 2026-11-03). The fused statement has none: the split rung that retried
         * it is planning's alone, and a fused statement the database rejects fails the program (P1.2, 2026-11-07).
         * The texts are the dialect's ({@code SqlDialect.verdictSavepoint}); the runner sends them only inside a
         * transaction.
         */
        public record Savepoint(String set, String rollbackTo, String release) {
            public Savepoint {
                Objects.requireNonNull(set, "set");
                Objects.requireNonNull(rollbackTo, "rollbackTo");
                Objects.requireNonNull(release, "release");
            }
        }
    }

    /**
     * THE RULES A VERDICT GROUP IS JUDGED BY (the user, 2026-11-04: "explicit rule"): chosen upfront per run
     * ({@code ExecuteOptions.withEngineCompat}), stamped on every group by its planner, read by both judges -- the
     * database judge's statement was planned under them, the host judge compares by them. A reader of the plan sees
     * which rules it judges by.
     * <ul>
     *   <li>{@link #LITE}: lite's own -- what a product run and every core test judge by.</li>
     *   <li>{@link #COMPILED_PURE_COMPAT}: backwards compatibility with legend-pure's compiled runtime and
     *       legend-engine's goldens (PCT channel B, the corpus), each leniency a register row: S132 (a String's float
     *       tokens within 2 ULP), S127 (an Integer equal to a Decimal by text), S15/S132 (a Float within 2 ULP), S139 (a Decimal's scale part of its value).</li>
     * </ul>
     */
    public enum JudgingRules {
        LITE, COMPILED_PURE_COMPAT;

        /** Whether two Floats within 2 ULP are equal: compatibility mode's tolerance alone; lite compares a Float
         *  exactly (S15/S132, the user's ruling of 2026-11-05, built that day). */
        public boolean floatTolerance() {
            return this == COMPILED_PURE_COMPAT;
        }

        /** Whether two Decimals of one value and different scales differ ({@code 0.3D} and {@code 0.30D}): compiled
         *  Pure's assert, compatibility mode's alone; lite compares a Decimal by value (S139, the user's ruling of
         *  2026-11-05). */
        public boolean decimalScale() {
            return this == COMPILED_PURE_COMPAT;
        }
    }

    /**
     * One assert of a {@link Verdicts} group, as data: {@code name}, the label the test listener reports under;
     * {@code arm}, the assert function's name as the program spells it ({@code assertEquals},
     * {@code assertSameElements}, ...), which comparison the judges apply -- the judges' own vocabulary, never read by
     * the runner (the assert's polarity is no longer here: the database judge's row answers holds and message itself
     * and the host judgment record carries its own, the test-is-the-product plan's M8b); {@code base}, the sides
     * in the assert's order, each a statement with its column facts (what the HOST judge runs) or a typed constant
     * the program wrote; {@code wrapper}, the verdict statement the planner composed around the base for the
     * DATABASE judge -- the canonical spelling of the cells and the comparison, answering one row of the verdict
     * schema (the verdict, the expected and actual texts, an unjudged reason, a leniency flag) -- null where the
     * database judge cannot express the arm, and then the database judges nothing here (the row the sink receives is
     * null); {@code message}, the caller's failure message where it READS (a let, a query: upstream's
     * {@code assert(condition, | message)}), a side the runner never reads on its own: the judge asks for it, through
     * the runner's reader (the run's open program), only where the verdict fails -- upstream evaluates the
     * message only there, so a message that would raise where the assert holds raises nothing (the assert bar,
     * 2026-11-04); null where the message is the judge's own. {@code raises}: an assertError's verdict (the user's
     * ruling S135, 2026-11-05: the host catches, the database judges) -- the runner reads {@code base}, the asserted
     * function's sides, under the group's savepoint and binds what reading them RAISED (its message, line and column,
     * each absent where it raised nothing or carried no position) to the parameters {@link #RAISED_MESSAGE},
     * {@link #RAISED_LINE} and {@link #RAISED_COLUMN} before it runs the wrapper, the verdict statement comparing them.
     */
    public record Verdict(int id, String name, String arm, List<Side> base,
            @com.legend.base.Nullable Node wrapper, @com.legend.base.Nullable Side message, boolean raises,
            @com.legend.base.Nullable Host host) {
        /** The parameter a raising verdict's statement reads the message its base raised from (absent: none). */
        public static final String RAISED_MESSAGE = "__raised_message";
        /** The parameter a raising verdict's statement reads the raise's line from (absent: none or no position). */
        public static final String RAISED_LINE = "__raised_line";
        /** The parameter a raising verdict's statement reads the raise's column from (absent: none or no position). */
        public static final String RAISED_COLUMN = "__raised_column";

        public Verdict {
            // the verdict's id, unique in its plan (the plan-as-data design's step 0, 2026-11-08): every reference to
            // a verdict -- the runner's row for it, its rules, its batch member, its agreement pair -- is by id, never
            // by the Java object, so a plan read back from its JSON names the same verdicts
            if (id < 1) {
                throw new IllegalArgumentException("a verdict's id is a positive number, unique in its plan: " + id);
            }
            Objects.requireNonNull(name, "name");
            Objects.requireNonNull(arm, "arm");
            if (arm.isEmpty()) {
                throw new IllegalArgumentException("a verdict names its arm");
            }
            base = List.copyOf(base);
            // a wrapper answers the verdict's one row: a typed relation, or a keyed one where it reads a raw grid's
            // column names or cells (the planner-driver seam program's S3c, 2026-10-30)
            if (wrapper != null && !(wrapper instanceof TdsResult t && t.shape() == TdsResult.Shape.RELATION)
                    && !(wrapper instanceof Keyed)) {
                throw new IllegalArgumentException("a verdict's wrapper answers a relation: the verdict's one row");
            }
            if (raises && (wrapper == null || message != null)) {
                throw new IllegalArgumentException("a raising verdict's statement compares what its base raised: it"
                        + " has one, and its message is the statement's");
            }
        }

        /** A verdict that reads its base as any assert does. */
        public Verdict(int id, String name, String arm, List<Side> base, @com.legend.base.Nullable Node wrapper,
                @com.legend.base.Nullable Side message) {
            this(id, name, arm, base, wrapper, message, false, null);
        }

        /** A verdict that reads its base as any assert does, or a raising one ({@code raises}); no host facts. */
        public Verdict(int id, String name, String arm, List<Side> base, @com.legend.base.Nullable Node wrapper,
                @com.legend.base.Nullable Side message, boolean raises) {
            this(id, name, arm, base, wrapper, message, raises, null);
        }

        /** This verdict judged by the HOST judge with {@code h}'s facts. */
        public Verdict withHost(@com.legend.base.Nullable Host h) {
            return new Verdict(id, name, arm, base, wrapper, message, raises, h);
        }

        /** A verdict whose failure message is its judge's own (the family's, or one the plan spelled). */
        public Verdict(int id, String name, String arm, List<Side> base, @com.legend.base.Nullable Node wrapper) {
            this(id, name, arm, base, wrapper, null);
        }
    }

    /**
     * WHAT THE HOST JUDGE JUDGES A VERDICT BY (the plan-as-data design's step 4, 2026-11-08: 2.2 and 2.3, beside the
     * plan until then): the assert's facts (its listener name, family, polarity, a failure before its arm), its
     * judgment -- one of the closed list of comparisons, as data -- and how each side the step reads is shaped once read
     * (the base's sides, then a caller's message where the verdict carries one). Null on a verdict the database judge
     * alone judges.
     */
    public record Host(Assertion assertion, HostJudgment judgment, List<SideShape> shapes) {
        public Host {
            Objects.requireNonNull(assertion, "assertion");
            Objects.requireNonNull(judgment, "judgment");
            shapes = List.copyOf(shapes);
        }
    }

    /** A side of an assert: a result the database answers (typed rows, or a keyed result's), or a constant the
     *  program wrote. */
    public sealed interface Side permits Side.Rows, Side.KeyedRows, Side.Constant {

        /** A statement with its column facts, run as any typed result is. */
        record Rows(TdsResult result) implements Side {
            public Rows {
                Objects.requireNonNull(result, "result");
            }
        }

        /** A keyed result (a dynamic pivot's), its keys read first. */
        record KeyedRows(Keyed keyed) implements Side {
            public KeyedRows {
                Objects.requireNonNull(keyed, "keyed");
            }
        }

        /**
         * A constant the program wrote (a literal side: {@code assertEquals(3, ...)}'s 3): its columns with their
         * type facts, and its rows as TEXT cells, each cell the literal's spelling as the program wrote it (null an
         * absent value); nothing is sent for it, the sink reads the cells by the columns' facts. The program's own
         * text, never a value the runner computed. {@code folded} (the seam program's S2f, the user's ruling of
         * 2026-10-30: "it's okay to join CONSTANT strings only"): the cells are the COMPILER's join of the program's
         * constant tokens ({@code 'a' + 'b'}, {@code joinStrings}, {@code replace} over literals, nothing else:
         * {@code compiler.spec.typed.Literals}, its admitted set pinned), not the tokens verbatim; a reader of the plan
         * knows which.
         */
        record Constant(List<Column> columns, List<List<String>> cells, boolean folded) implements Side {
            public Constant {
                columns = List.copyOf(columns);
                if (columns.isEmpty()) {
                    throw new IllegalArgumentException("a constant side has at least one column");
                }
                List<List<String>> copied = new java.util.ArrayList<>(cells.size());
                for (List<String> row : cells) {
                    if (row.size() != columns.size()) {
                        throw new IllegalArgumentException("a constant side's row of " + row.size() + " cells under "
                                + columns.size() + " columns");
                    }
                    // a cell may be null (an absent value), which List.of refuses
                    copied.add(java.util.Collections.unmodifiableList(new java.util.ArrayList<>(row)));
                }
                cells = java.util.Collections.unmodifiableList(copied);
            }
        }
    }

    /**
     * A BRANCH of a program (the asserts-as-plan-nodes program, step 4, 2026-11-04; docs/ASSERT_NODES_PLAN_2026_11_04.md:
     * the user's word, "we own the 'new' lite plan so we should do whatever we think is the cleanest way to represent
     * in our plan"): a test body's {@code if(condition, |then, |else)} whose sides hold asserts. The runner reads
     * {@code condition} -- one Boolean, a statement's one row or a constant the program wrote -- and runs the steps of
     * the side it names, and only those: the side not taken is never sent, as upstream never evaluates it. A side's
     * steps are verdict groups and branches (the asserts a side holds, as their judge planned them), possibly none (a
     * side that asserts nothing). A test plan's step, run through the runner's callback, never a plan's answer; a
     * product query's {@code if} stays the database's {@code CASE}.
     */
    public record Branch(Side condition, List<TestStep> then, List<TestStep> otherwise) implements TestStep {
        public Branch {
            Objects.requireNonNull(condition, "condition");
            then = List.copyOf(then);
            otherwise = List.copyOf(otherwise);
            for (TestStep step : then) {
                sideStep(step);
            }
            for (TestStep step : otherwise) {
                sideStep(step);
            }
        }

        private static void sideStep(TestStep step) {
            if (!(step instanceof Verdicts) && !(step instanceof Branch)) {
                throw new IllegalArgumentException("a branch's side holds verdict groups and branches, not "
                        + step.getClass().getSimpleName());
            }
        }
    }

}
