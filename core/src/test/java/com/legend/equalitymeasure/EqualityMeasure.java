// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0
package com.legend.equalitymeasure;

import com.legend.Compiler;
import com.legend.ExecuteOptions;
import com.legend.TestExecution;
import com.legend.TypedQuery;
import com.legend.compiler.element.ModelContext;
import com.legend.error.AssertFailed;
import com.legend.exec.SessionSource;
import com.legend.model.ConnectionDefinition.DatabaseType;
import com.legend.model.ImportScope;
import com.legend.protocol.spec.LambdaFunction;
import com.legend.protocol.spec.ValueSpecification;
import com.legend.result.Output;
import com.legend.testbackend.StorelessRuntime;
import com.legend.testbackend.TestBackends;
import com.legend.testexec.NoEdgeRun;
import com.legend.testplan.PlanningOptions.JudgeMode;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * THE EQUALITY MEASUREMENT (step 6.0 of the edition program; tooling, never a test, never product code): every body of
 * {@code bodies.tsv} (tools/equality-measure/collect.py) run as written and rewritten to its plain comparison, under
 * the host judge and the database judge, on every database the platform runs in-process (the test backends'
 * {@code TestBackends.inProcess()}), each outcome one row of {@code results.tsv}:
 * PASS, FAIL (an assert failed), UNJUDGED (the database judge declined), REFUSED (a compile or planning refusal),
 * ERROR (anything else), TIMEOUT. Then the product probes ({@code probes.tsv}): each expression's answer per database.
 *
 * <p>Args: {@code <bodies.tsv> <out dir> [<id regex>]}.
 */
public final class EqualityMeasure {

    /** The databases measured: the test backends' in-process ones, in their order (the first takes the census). */
    private static final List<DatabaseType> DATABASES = TestBackends.inProcess().stream()
            .map(com.legend.testbackend.TestBackend::type).toList();
    private static final String NAMES = String.join("|", DATABASES.stream().map(DatabaseType::name).toList());
    private static final List<JudgeMode> JUDGES = List.of(JudgeMode.HOST, JudgeMode.DATABASE);
    private static final Pattern RUNTIME = Pattern.compile("(?m)^\\s*Runtime\\s+([\\w:]+)");
    private static final Pattern ONE_DATABASE = Pattern.compile("specification:\\s*(" + NAMES + ")\\s*\\{");
    private static final long TIMEOUT_SECONDS = Long.getLong("measure.timeout", 60);

    private EqualityMeasure() {
    }

    private record Body(String id, String status, List<String> imports, String model, String asWritten,
            String rewritten) {
    }

    private record Outcome(String kind, String detail) {
    }

    public static void main(String[] args) throws Exception {
        if (args.length >= 2 && args[0].equals("--positions")) {
            ExecutorService[] one = {Executors.newSingleThreadExecutor(daemon())};
            positions(Path.of(args[1]), one, args.length > 2 ? Pattern.compile(args[2]) : null);
            one[0].shutdownNow();
            System.exit(0);
        }
        if (args.length >= 2 && args[0].equals("--sql")) {
            // the product's SQL for one body over the position matrix's model, per database: probe, don't guess
            for (DatabaseType db : DATABASES) {
                String sql;
                try {
                    sql = typed(Compiler.compileModel(StorelessRuntime.with(positionModel(), db)), args[1], List.of())
                            .plan(StorelessRuntime.RUNTIME).sql();
                } catch (RuntimeException e) {
                    sql = e.getClass().getSimpleName() + ": " + e.getMessage();
                }
                System.out.println("@@ " + db.name() + "\n" + sql + "\n");
            }
            System.exit(0);
        }
        Path in = Path.of(args[0]);
        Path out = Path.of(args[1]);
        Pattern only = args.length > 2 ? Pattern.compile(args[2]) : null;
        Files.createDirectories(out);
        List<Body> bodies = read(in);
        ExecutorService[] pool = {Executors.newSingleThreadExecutor(daemon())};
        Map<String, Object> contexts = new HashMap<>();
        long t0 = System.nanoTime();
        int done = 0;
        try (PrintWriter w = new PrintWriter(Files.newBufferedWriter(out.resolve("results.tsv"), StandardCharsets.UTF_8));
                PrintWriter census = new PrintWriter(Files.newBufferedWriter(out.resolve("census.tsv"),
                        StandardCharsets.UTF_8))) {
            w.println("id\tdatabase\tform\tjudge\toutcome\tdetail");
            census.println("id\tform\tcall\ttypes");
            for (Body b : bodies) {
                if (only != null && !only.matcher(b.id()).find()) {
                    continue;
                }
                if (!b.status().equals("rewritten") && !b.status().equals("unchanged")) {
                    continue;
                }
                for (DatabaseType db : DATABASES) {
                    String model = modelOn(b.model(), db);
                    String runtime = runtimeOf(b.model(), model);
                    Matcher only1 = ONE_DATABASE.matcher(b.model());
                    boolean skipped = only1.find() && !only1.group(1).equalsIgnoreCase(db.name());
                    Object ctx = skipped ? new Outcome("SKIPPED", "the model's connection is " + only1.group(1) + "'s")
                            : contexts.computeIfAbsent(model, m -> {
                        try {
                            return Compiler.compileModel(m);
                        } catch (Throwable t) {
                            return outcomeOf(t, "REFUSED");
                        }
                    });
                    for (String form : List.of("written", "rewritten")) {
                        String body = form.equals("written") ? b.asWritten() : b.rewritten();
                        if (db == DATABASES.get(0) && ctx instanceof ModelContext mc) {
                            Outcome typedSides = timed(pool, () -> {
                                StringBuilder sb = new StringBuilder();
                                for (com.legend.compiler.spec.typed.TypedSpec s : typed(mc, body, b.imports()).body()) {
                                    comparisons(s, sides -> census.println(b.id() + "\t" + form + "\t" + sides));
                                }
                                return new Outcome("OK", "");
                            });
                            if (!typedSides.kind().equals("OK")) {
                                census.println(b.id() + "\t" + form + "\t(untyped)\t" + typedSides.kind());
                            }
                        }
                        for (JudgeMode judge : JUDGES) {
                            Outcome o;
                            if (ctx instanceof Outcome refused) {
                                o = new Outcome(refused.kind(), refused.kind().equals("SKIPPED") ? refused.detail()
                                        : "model: " + refused.detail());
                            } else if (form.equals("rewritten") && b.status().equals("unchanged")) {
                                o = new Outcome("SAME", "unchanged body");
                            } else {
                                o = timed(pool, () -> run((ModelContext) ctx, body, b.imports(), runtime, db, judge));
                            }
                            w.println(String.join("\t", b.id(), db.name(), form, judge.name(), o.kind(),
                                    clean(o.detail())));
                        }
                    }
                }
                w.flush();
                if (++done % 50 == 0) {
                    System.err.printf("%d bodies, %.0f s%n", done, (System.nanoTime() - t0) / 1e9);
                }
            }
        }
        probes(out, pool);
        pool[0].shutdownNow();
        System.err.printf("done: %d bodies in %.0f s%n", done, (System.nanoTime() - t0) / 1e9);
        System.exit(0);
    }

    /** The product's own answers to the expressions of the brief's 4e, as the last statement of a body (the test
     *  planner plans a statement with no assert as a product query), per database. */
    private static void probes(Path out, ExecutorService[] pool) throws IOException {
        String model = "###Pure\nEnum t::E { A, B }\nClass t::P { n: Number[1]; d: Date[1]; }\n";
        List<String> exprs = List.of(
                "[1, 2]->contains(1.0)",
                "1.0->in([1, 2])",
                "['A', 'B']->indexOf(t::E.A)",
                "['A', 'B']->contains(t::E.A)",
                "[] == [1, 2]->filter(x | $x > 5)",
                "[1, 2]->filter(x | $x > 5) == []",
                "[1, 2] == [1.0, 2.0]",
                "[1.0D, 'a'] == [1.00D, 'a']",
                "1.0D == 1.00D",
                "pair(1, 1D) == pair(1, 1)",
                "%2020-01-01 == %2020-01-01T00:00:00",
                "[%2020-01-01] == [%2020-01-01T00:00:00]",
                "[%2020-01-01, 'a'] == [%2020-01-01T00:00:00, 'a']",
                "[%2020-01-01, %2021-01-01]->contains(%2020-01-01T00:00:00)",
                "%2020-01-01T10:00:00.000 == %2020-01-01T10:00:00",
                "1 == 1.0",
                "[1, 'a'] == [1.0, 'a']",
                "[1, 2.0]->max() == 2",
                // the same pairs with one side typed only at run time (Any): past the strict rule's compile check
                "[1, 2]->cast(@Any)->contains(1.0)",
                "1.0->cast(@Any)->in([1, 2]->cast(@Any))",
                "['A', 'B']->cast(@Any)->indexOf(t::E.A)",
                "['A', 'B']->cast(@Any)->contains(t::E.A)",
                "[1, 2]->cast(@Any) == [1.0, 2.0]->cast(@Any)",
                "1->cast(@Any) == 1.0->cast(@Any)",
                "%2020-01-01->cast(@Any) == %2020-01-01T00:00:00->cast(@Any)",
                "[%2020-01-01, %2021-01-01]->cast(@Any)->contains(%2020-01-01T00:00:00)",
                "[1, 2]->cast(@Number)->contains(1.0)",
                "%2020-01-01->cast(@Date) == %2020-01-01T00:00:00->cast(@Date)");
        try (PrintWriter w = new PrintWriter(Files.newBufferedWriter(out.resolve("probes.tsv"), StandardCharsets.UTF_8))) {
            w.println("expression\tdatabase\toutcome\tanswer");
            for (String e : exprs) {
                for (DatabaseType db : DATABASES) {
                    String m = StorelessRuntime.with(model, db);
                    StringWriter answer = new StringWriter();
                    Outcome o = timed(pool, () -> {
                        ModelContext ctx = Compiler.compileModel(m);
                        TypedQuery typed = typed(ctx, "{|" + e + "}", List.of());
                        TestExecution.runProgram(typed, StorelessRuntime.RUNTIME,
                                new SessionSource.Fresh(TestBackends.of(db)::open), null, NoEdgeRun.NONE,
                                ExecuteOptions.NONE.withJudgeMode(JudgeMode.HOST), new Output.JsonRows(answer));
                        return new Outcome("ANSWER", answer.toString());
                    });
                    w.println(String.join("\t", e, db.name(), o.kind(), clean(o.detail())));
                }
            }
        }
    }

    /** One place a comparison can stand: the body it makes of the two sides' texts; whether the body answers the
     *  comparison's negation; whether the body is an assert (its outcome a verdict, under each judge). */
    private record Position(String id, boolean negated, boolean asserted,
            java.util.function.BinaryOperator<String> body) {
    }

    /** One comparison: its name and its two sides, as Pure text. */
    private record Sides(String id, String left, String right) {
    }

    /** The position matrix's model, read from {@code tools/equality-measure/positions-model.txt} of the workspace
     *  ({@code bazel run} names it: BUILD_WORKSPACE_DIRECTORY). It is no string literal of this file on purpose: the
     *  parser-equivalence corpus extracts every model text written as a Java literal in the test sources, and as a
     *  literal here this one failed {@code //parser-equivalence:parser_parity} (2026-11-27). */
    private static String positionModel() {
        String workspace = System.getenv("BUILD_WORKSPACE_DIRECTORY");
        if (workspace == null) {
            throw new IllegalStateException("the position matrix reads its model from the workspace: run it with"
                    + " `bazel run //core:equality_measure -- --positions <out dir>`");
        }
        try {
            return Files.readString(Path.of(workspace, "tools", "equality-measure", "positions-model.txt"),
                    StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new java.io.UncheckedIOException(e);
        }
    }

    private static final String ONE_ROW = "#TDS\n id\n 1\n#";

    private static List<Position> positionList() {
        // (each side in parentheses: `1 + 1 == 2` is read as `1 + (1 == 2)`)
        java.util.function.BinaryOperator<String> eq = (l, r) -> "(" + l + ") == (" + r + ")";
        return List.of(
                new Position("statement", false, false, (l, r) -> "{|" + eq.apply(l, r) + "}"),
                new Position("not", true, false, (l, r) -> "{|!(" + eq.apply(l, r) + ")}"),
                new Position("if", false, false, (l, r) -> "{|if(" + eq.apply(l, r) + ", |true, |false)}"),
                new Position("and", false, false, (l, r) -> "{|(" + eq.apply(l, r) + ") && true}"),
                new Position("or", false, false, (l, r) -> "{|(" + eq.apply(l, r) + ") || false}"),
                new Position("lets", false, false, (l, r) -> "{|let a = " + l + "; let b = " + r + "; $a == $b;}"),
                new Position("kept-in-map", false, false,
                        (l, r) -> "{|[1]->map(x | " + eq.apply(l, r) + ")->at(0)}"),
                new Position("list-filter", false, false,
                        (l, r) -> "{|[1]->filter(x | " + eq.apply(l, r) + ")->isNotEmpty()}"),
                new Position("list-exists", false, false, (l, r) -> "{|[1]->exists(x | " + eq.apply(l, r) + ")}"),
                new Position("list-forAll", false, false, (l, r) -> "{|[1]->forAll(x | " + eq.apply(l, r) + ")}"),
                new Position("reads-element", false, false,
                        (l, r) -> "{|[0]->map(x | (" + l + ")->filter(e | $x == 0) == (" + r + "))->at(0)}"),
                new Position("function-any", false, false, (l, r) -> "{|t::sameAny(" + l + ", " + r + ")}"),
                new Position("function-generic", false, false, (l, r) -> "{|t::sameT(" + l + ", " + r + ")}"),
                new Position("table-filter", false, false,
                        (l, r) -> "{|" + ONE_ROW + "->filter(row | " + eq.apply(l, r) + ")}"),
                new Position("table-column", false, false,
                        (l, r) -> "{|" + ONE_ROW + "->extend(~same: row | " + eq.apply(l, r) + ")->select(~same)}"),
                new Position("assertEquals", false, true, (l, r) -> "{|assertEquals(" + l + ", " + r + ")}"),
                new Position("assert", false, true, (l, r) -> "{|assert(" + eq.apply(l, r) + ")}"));
    }

    private static List<Sides> sidesList() {
        String ab = "#TDS\n id, s\n 1, x\n 2, y\n#";
        List<Sides> out = new ArrayList<>();
        // single values of one type
        out.add(new Sides("int same", "1", "1"));
        out.add(new Sides("int differ", "1", "2"));
        out.add(new Sides("int computed", "1 + 1", "2"));
        out.add(new Sides("float same", "1.5", "1.5"));
        out.add(new Sides("string same", "'a'", "'a'"));
        out.add(new Sides("string differ", "'a'", "'b'"));
        out.add(new Sides("boolean same", "true", "true"));
        out.add(new Sides("enum same", "t::E.A", "t::E.A"));
        out.add(new Sides("enum differ", "t::E.A", "t::E.B"));
        // exact numbers of two types and two scales
        out.add(new Sides("int and decimal", "1", "1D"));
        out.add(new Sides("decimal two scales", "1.0D", "1.00D"));
        out.add(new Sides("decimal computed two scales", "1.0D + 1.0D", "2.00D"));
        out.add(new Sides("int and float", "1", "1.0"));
        // dates
        out.add(new Sides("day same", "%2020-01-01", "%2020-01-01"));
        out.add(new Sides("day and date-time", "%2020-01-01", "%2020-01-01T00:00:00"));
        out.add(new Sides("date-time two written precisions", "%2020-01-01T10:00:00.000", "%2020-01-01T10:00:00"));
        out.add(new Sides("date-time hour and minute", "%2020-01-01T10", "%2020-01-01T10:00"));
        out.add(new Sides("year same", "%2020", "%2020"));
        out.add(new Sides("year and computed year", "%2020", "date(2020)"));
        out.add(new Sides("year-month same", "%2020-01", "%2020-01"));
        out.add(new Sides("year and year-month", "%2020", "%2020-01"));
        out.add(new Sides("built to the hour and literal", "date(2015, 4, 16, 14)", "%2015-04-16T14:00:00"));
        out.add(new Sides("day computed", "%2020-01-01->adjust(1, DurationUnit.DAYS)", "%2020-01-02"));
        // a type known only when the query runs
        out.add(new Sides("Number same", "1->cast(@Number)", "1->cast(@Number)"));
        out.add(new Sides("Number int and float", "1->cast(@Number)", "1.0->cast(@Number)"));
        out.add(new Sides("Number int and decimal", "1->cast(@Number)", "1D->cast(@Number)"));
        out.add(new Sides("Any same", "1->cast(@Any)", "1->cast(@Any)"));
        out.add(new Sides("Any int and string", "1->cast(@Any)", "'1'->cast(@Any)"));
        out.add(new Sides("Any day and date-time", "%2020-01-01->cast(@Any)", "%2020-01-01T00:00:00->cast(@Any)"));
        out.add(new Sides("Date day and date-time", "%2020-01-01->cast(@Date)", "%2020-01-01T00:00:00->cast(@Date)"));
        out.add(new Sides("max of a mixed list", "[1, 2.0]->max()", "2.0"));
        // empty sides
        out.add(new Sides("empty and empty", "[]", "[]"));
        out.add(new Sides("empty and value", "[]", "1"));
        out.add(new Sides("computed empty and written empty", "[1, 2]->filter(x | $x > 5)", "[]"));
        out.add(new Sides("two computed empties", "[1, 2]->filter(x | $x > 5)", "[3]->filter(x | $x > 5)"));
        out.add(new Sides("computed empty and value", "[1, 2]->filter(x | $x > 5)", "1"));
        out.add(new Sides("optional none and value", "[1, 2]->filter(x | $x > 5)->first()", "1"));
        out.add(new Sides("two optionals none", "[1, 2]->filter(x | $x > 5)->first()",
                "[3]->filter(x | $x > 5)->first()"));
        out.add(new Sides("optional one and value", "[1, 2]->filter(x | $x > 1)->first()", "2"));
        // lists
        out.add(new Sides("written lists same", "[1, 2]", "[1, 2]"));
        out.add(new Sides("written lists other order", "[1, 2]", "[2, 1]"));
        out.add(new Sides("computed list same", "[1, 2]->map(x | $x + 1)", "[2, 3]"));
        out.add(new Sides("computed list other order", "[1, 2]->map(x | $x + 1)", "[3, 2]"));
        out.add(new Sides("two computed lists", "[1, 2]->map(x | $x + 1)", "[2, 3]->filter(x | $x > 0)"));
        out.add(new Sides("computed list shorter", "[1, 2]->filter(x | $x > 1)", "[1, 2]"));
        out.add(new Sides("list int and decimal", "[1, 2]->filter(x | $x > 0)", "[1.0D, 2.00D]"));
        out.add(new Sides("list decimal two scales", "[1.0D, 10.125D]->filter(x | $x > 0D)", "[1.000D, 10.125D]"));
        out.add(new Sides("written list decimal two scales", "[1.0D, 10.125D]", "[1.000D, 10.125D]"));
        out.add(new Sides("list strings computed", "['a', 'b']->map(s | $s + 'x')", "['ax', 'bx']"));
        out.add(new Sides("list date-times two precisions", "[%2020-01-01T10:00:00.000]->filter(d | true)",
                "[%2020-01-01T10:00:00]"));
        out.add(new Sides("list days computed", "[%2020-01-01, %2020-01-02]->filter(d | true)",
                "[%2020-01-01, %2020-01-02]"));
        out.add(new Sides("one value and list of one", "1", "[1, 2]->filter(x | $x < 2)"));
        out.add(new Sides("mixed list written same", "[1, 'a']", "[1, 'a']"));
        out.add(new Sides("mixed list int and float", "[1, 'a']", "[1.0, 'a']"));
        out.add(new Sides("mixed list decimal two scales", "[1.0D, 'a']", "[1.00D, 'a']"));
        out.add(new Sides("mixed list computed", "[1, 'a']->filter(x | true)", "[1, 'a']"));
        out.add(new Sides("mixed list with dates", "[%2020-01-01, 'a']", "[%2020-01-01T00:00:00, 'a']"));
        out.add(new Sides("Number lists int and float", "[1, 2]->cast(@Number)", "[1.0, 2.0]->cast(@Number)"));
        out.add(new Sides("Number lists int and decimal", "[1, 2]->cast(@Number)", "[1D, 2D]->cast(@Number)"));
        out.add(new Sides("Any lists same", "[1, 2]->cast(@Any)", "[1, 2]->cast(@Any)"));
        out.add(new Sides("nested lists", "list([1, 2])", "list([1, 2])"));
        // maps, pairs, instances
        out.add(new Sides("maps same order", "newMap([pair('a', 1), pair('b', 2)])",
                "newMap([pair('a', 1), pair('b', 2)])"));
        out.add(new Sides("maps other order", "newMap([pair('a', 1), pair('b', 2)])",
                "newMap([pair('b', 2), pair('a', 1)])"));
        out.add(new Sides("maps a value differs", "newMap([pair('a', 1), pair('b', 2)])",
                "newMap([pair('a', 1), pair('b', 3)])"));
        out.add(new Sides("pairs same", "pair(1, 'a')", "pair(1, 'a')"));
        out.add(new Sides("pairs int and decimal", "pair(1, 1D)", "pair(1, 1)"));
        out.add(new Sides("list of pairs computed", "[pair(1, 'a'), pair(2, 'b')]->filter(p | true)",
                "[pair(1, 'a'), pair(2, 'b')]"));
        out.add(new Sides("instances written alike", "^t::K(id=1, name='a')", "^t::K(id=1, name='a')"));
        // tables
        out.add(new Sides("tables same", ab, ab));
        out.add(new Sides("tables rows other order", ab, "#TDS\n id, s\n 2, y\n 1, x\n#"));
        out.add(new Sides("tables a cell differs", ab, "#TDS\n id, s\n 1, x\n 2, z\n#"));
        out.add(new Sides("tables a column's name differs", "#TDS\n id\n 1\n#", "#TDS\n ix\n 1\n#"));
        return out;
    }

    /**
     * THE POSITION MATRIX (step 6B's measure, 2026-11-27): one comparison, {@code L == R}, asked in every place a
     * comparison can stand -- a query's answer, under {@code !}, an {@code if}'s condition, an operand of
     * {@code &&} and {@code ||}, over two lets, a kept value and a condition inside a list's lambda, a side that
     * reads the lambda's element, inside a function whose parameters are typed when it is called, a table's filter
     * and a table's column, and a test's {@code assertEquals} and {@code assert} under each judge -- per in-process
     * database. {@code positions.tsv} holds every outcome as it came; {@code tools/equality-measure/positions.py}
     * reads it and lists the comparisons whose answer depends on where they stand. Nothing is expected here: the
     * measure is whether the answers agree. Run: {@code bazel run //core:equality_measure -- --positions <out dir>
     * [<comparison name regex>]}.
     */
    private static void positions(Path out, ExecutorService[] pool, @com.legend.base.Nullable Pattern only)
            throws IOException {
        Files.createDirectories(out);
        Map<DatabaseType, ModelContext> contexts = new HashMap<>();
        for (DatabaseType db : DATABASES) {
            contexts.put(db, Compiler.compileModel(StorelessRuntime.with(positionModel(), db)));
        }
        long t0 = System.nanoTime();
        int runs = 0;
        try (PrintWriter w = new PrintWriter(Files.newBufferedWriter(out.resolve("positions.tsv"),
                StandardCharsets.UTF_8))) {
            w.println("comparison\tleft\tright\tposition\tnegated\tdatabase\tjudge\toutcome\tanswer");
            for (Sides s : sidesList()) {
                if (only != null && !only.matcher(s.id()).find()) {
                    continue;
                }
                for (Position p : positionList()) {
                    String body = p.body().apply(s.left(), s.right());
                    for (DatabaseType db : DATABASES) {
                        ModelContext ctx = java.util.Objects.requireNonNull(contexts.get(db));
                        for (JudgeMode judge : p.asserted() ? JUDGES : List.of(JudgeMode.HOST)) {
                            Outcome o = timed(pool, () -> p.asserted()
                                    ? run(ctx, body, List.of(), StorelessRuntime.RUNTIME, db, judge)
                                    : answer(ctx, body, db));
                            w.println(String.join("\t", s.id(), clean(s.left()), clean(s.right()), p.id(),
                                    String.valueOf(p.negated()), db.name(), p.asserted() ? judge.name() : "-",
                                    o.kind(), clean(o.detail())));
                            runs++;
                        }
                    }
                }
                w.flush();
            }
        }
        System.err.printf("positions: %d runs in %.0f s%n", runs, (System.nanoTime() - t0) / 1e9);
    }

    /** {@code body}'s answer on {@code db}: a body with no assert, which the test planner plans as a product query
     *  (as {@link #probes} runs its expressions). A refusal where it is planned is {@code REFUSED}. */
    private static Outcome answer(ModelContext ctx, String body, DatabaseType db) {
        TypedQuery typed;
        try {
            typed = typed(ctx, body, List.of());
            TestExecution.planBody(typed, StorelessRuntime.RUNTIME, ExecuteOptions.NONE.withJudgeMode(JudgeMode.HOST));
        } catch (Throwable t) {
            return outcomeOf(t, "REFUSED");
        }
        StringWriter answer = new StringWriter();
        try {
            TestExecution.runProgram(typed, StorelessRuntime.RUNTIME, new SessionSource.Fresh(TestBackends.of(db)::open),
                    null, NoEdgeRun.NONE, ExecuteOptions.NONE.withJudgeMode(JudgeMode.HOST),
                    new Output.JsonRows(answer));
        } catch (Throwable t) {
            return outcomeOf(t, "ERROR");
        }
        return new Outcome("ANSWER", answer.toString());
    }

    private static final java.util.Set<String> COMPARING = java.util.Set.of("assertEquals", "assertNotEquals",
            "assertSameElements", "assertEq", "assertNotEq", "assertIs", "assertIsNot", "assertContains",
            "assertNotContains", "assertEqWithinTolerance", "equal", "eq", "is", "contains", "in", "indexOf");

    /** Each comparing call under {@code n} (an assert of a pair, or the product's {@code ==}, {@code eq},
     *  {@code contains}, {@code in}): its name and its two compared sides' static types. */
    private static void comparisons(com.legend.compiler.spec.typed.TypedSpec n,
            java.util.function.Consumer<String> sink) {
        String name = null;
        List<com.legend.compiler.spec.typed.TypedSpec> args = List.of();
        if (n instanceof com.legend.compiler.spec.typed.TypedNativeCall c) {
            name = c.callee().qualifiedName();
            args = c.args();
        } else if (n instanceof com.legend.compiler.spec.typed.TypedUserCall c) {
            name = c.callee().qualifiedName();
            args = c.args();
        }
        if (name != null) {
            String simple = name.substring(name.lastIndexOf(':') + 1);
            if (COMPARING.contains(simple) && args.size() >= 2) {
                sink.accept(simple + "\t" + args.get(0).info().type().typeName() + " | "
                        + args.get(1).info().type().typeName());
            }
        }
        for (com.legend.compiler.spec.typed.TypedSpec c : n.children()) {
            comparisons(c, sink);
        }
    }

    private static Outcome run(ModelContext ctx, String body, List<String> imports, String runtime, DatabaseType db,
            JudgeMode judge) {
        ExecuteOptions options = ExecuteOptions.NONE.withJudgeMode(judge);
        TypedQuery typed;
        try {
            typed = typed(ctx, body, imports);
            TestExecution.planBody(typed, runtime, options);
        } catch (Throwable t) {
            return outcomeOf(t, "REFUSED");
        }
        int[] verdicts = {0, 0};
        try {
            TestExecution.runProgram(typed, runtime, new SessionSource.Fresh(TestBackends.of(db)::open),
                    new com.legend.testexecutionplan.AssertListener() {
                        @Override
                        public void verdict(String name, boolean pass, @com.legend.base.Nullable String detail) {
                            verdicts[pass ? 0 : 1]++;
                        }
                    }, NoEdgeRun.NONE, options, new Output.JsonRows(new StringWriter()));
        } catch (Throwable t) {
            return outcomeOf(t, "ERROR");
        }
        return new Outcome("PASS", verdicts[0] + " verdicts");
    }

    private static TypedQuery typed(ModelContext ctx, String body, List<String> imports) {
        ValueSpecification parsed = Compiler.parseQuery(body);
        List<ValueSpecification> statements = parsed instanceof LambdaFunction lf && lf.parameters().isEmpty()
                ? lf.body() : List.of(parsed);
        return Compiler.queryResolved(ctx, Compiler.resolveQuery(statements,
                new ImportScope(imports), ctx));
    }

    private static Outcome outcomeOf(Throwable t, String otherwise) {
        if (t instanceof AssertFailed f) {
            return f.unjudgedReason() != null ? new Outcome("UNJUDGED", f.unjudgedReason() + " | " + f.getMessage())
                    : new Outcome("FAIL", String.valueOf(f.getMessage()));
        }
        return new Outcome(otherwise, t.getClass().getSimpleName() + ": " + t.getMessage());
    }

    private static Outcome timed(ExecutorService[] pool, java.util.concurrent.Callable<Outcome> work) {
        Future<Outcome> f = pool[0].submit(work);
        try {
            return f.get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
        } catch (TimeoutException e) {
            f.cancel(true);
            pool[0].shutdownNow();
            pool[0] = Executors.newSingleThreadExecutor(daemon());
            return new Outcome("TIMEOUT", TIMEOUT_SECONDS + " s");
        } catch (java.util.concurrent.ExecutionException e) {
            return outcomeOf(e.getCause(), "ERROR");
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return new Outcome("ERROR", "interrupted");
        }
    }

    private static java.util.concurrent.ThreadFactory daemon() {
        return r -> {
            Thread t = new Thread(r, "measure");
            t.setDaemon(true);
            return t;
        };
    }

    private static String modelOn(String model, DatabaseType db) {
        if (model.contains("%s")) {
            return model.replace("%s", db.name()).replace("%%", "%");
        }
        if (RUNTIME.matcher(model).find()) {
            // a model whose connection names one in-process database: run on each of the three
            return model.replaceAll("type:\\s*(" + NAMES + ")\\s*;", "type: " + db.name() + ";");
        }
        return StorelessRuntime.with(model.isBlank() ? "###Pure\n" : model, db);
    }

    private static String runtimeOf(String declared, String model) {
        Matcher m = RUNTIME.matcher(declared);
        return m.find() ? m.group(1) : StorelessRuntime.RUNTIME;
    }

    private static String clean(@com.legend.base.Nullable String s) {
        String t = String.valueOf(s).replace("\t", " ").replace("\r", "").replace("\n", "\\n");
        return t.length() > 400 ? t.substring(0, 400) + "..." : t;
    }

    private static List<Body> read(Path in) throws IOException {
        List<Body> out = new ArrayList<>();
        List<String> lines = Files.readAllLines(in, StandardCharsets.UTF_8);
        for (String line : lines.subList(1, lines.size())) {
            String[] f = line.split("\t", -1);
            out.add(new Body(f[0], f[2], f[3].isEmpty() ? List.of() : List.of(f[3].split(",")), unesc(f[4]),
                    unesc(f[5]), unesc(f[6])));
        }
        return out;
    }

    private static String unesc(String s) {
        StringBuilder b = new StringBuilder();
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            if (c == '\\' && i + 1 < s.length()) {
                char n = s.charAt(++i);
                b.append(n == 'n' ? '\n' : n == 't' ? '\t' : n);
            } else {
                b.append(c);
            }
        }
        return b.toString();
    }
}
