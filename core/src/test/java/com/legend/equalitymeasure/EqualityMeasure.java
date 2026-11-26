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
