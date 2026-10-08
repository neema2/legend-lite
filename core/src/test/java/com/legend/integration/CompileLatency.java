package com.legend.integration;

import com.legend.Compiler;
import com.legend.TypedQuery;
import com.legend.compiler.element.ModelContext;
import com.legend.model.ImportScope;
import com.legend.model.ServiceDefinition;
import com.legend.protocol.Protocol;
import com.legend.protocol.spec.AppliedFunction;
import com.legend.protocol.spec.CString;
import com.legend.protocol.spec.LambdaFunction;
import com.legend.protocol.spec.ValueSpecification;
import com.legend.testing.Programs;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * COMPILE-ONLY LATENCY (the compiler plan's W1.0b, under D25: the stress corpus and the eager probe are the measure;
 * nothing new is built to measure). One query's compile with no database, in four stages: names (the resolver, once:
 * {@link Compiler#resolveQuery} then {@link Compiler#queryResolved}; for a query set, the parse too), type (the body
 * typed, {@link TypedQuery#body}), lower (inline, store resolution, lowering: {@link TypedQuery#lower}) and render
 * (the runtime's dialect over the plan). Nothing is typed or resolved twice inside the clock.
 * <pre>
 *   bazel run //core:compile_latency -- --corpus stress [--passes 2] [--out DIR]
 *   bazel run //core:compile_latency -- --model FILE.pure --queries NAME-TAB-QUERY.tsv --runtime FQN [--out DIR]
 * </pre>
 * The stress corpus: every {@code stress::} service's test, built exactly as {@link com.legend.test.ServiceTestRunner}
 * builds it (its parameters as let-bound variables, then the body) and planned for the service's DECLARED runtime —
 * the runner overlays a test runtime whose connection carries the session's database type; a declared connection of
 * another type (a few declare H2 or a cloud warehouse) makes this tool's refusal differ from the runner's, so the
 * eleven or so cases it cannot plan are listed by reason, not hidden. The query set: each line's query against the
 * model, as {@code //wasm:jvm_answers} plans it (its refusals are the differential's expected errors).
 *
 * <p>Every case runs {@code --passes} times in order. Pass 1 starts with the model's demand caches empty (classes,
 * functions, layouts, mapping closures are filled on first use; the first case's time is printed as the cold number);
 * the LAST pass is the report: a warm session's query, every per-model cache filled. Today's shipped entry points
 * compile the model per request, so a user's first query of a session pays part of pass 1; a session that keeps the
 * model sees the last pass's numbers. Per-case timings go to {@code --out} as {@code compile-latency-<set>.tsv}. Run
 * alone on a quiet machine; never a test. Percentiles: the value at rank floor(p·(n−1)) of the sorted times.
 */
public final class CompileLatency {

    private CompileLatency() {}

    /** One query to time: resolved statements for a service test, or a query text for the set. */
    private record Case(String name, @com.legend.base.Nullable List<ValueSpecification> statements,
            @com.legend.base.Nullable String query, String runtime) {}

    /** One case's stage times (nanoseconds) and its status: {@code ok}, {@code skip: …} or {@code fail: …}. */
    private record Timing(String name, long names, long type, long lower, long render, String status) {
        long total() {
            return names + type + lower + render;
        }
    }

    public static void main(String[] args) throws Exception {
        String corpus = option(args, "--corpus", "");
        String model = option(args, "--model", "");
        String queries = option(args, "--queries", "");
        String runtime = option(args, "--runtime", "");
        int passes = Integer.parseInt(option(args, "--passes", "2"));
        Path outDir = Files.createDirectories(Programs.argument(option(args, "--out", "compile-latency-out")));
        List<Case> cases;
        ModelContext ctx;
        String set;
        long t0 = System.nanoTime();
        if ("stress".equals(corpus)) {
            set = "stress";
            var module = Compiler.parseSources(StressCorpus.sources(List.of()));
            ctx = Compiler.buildModel(module.model());
            System.out.printf("[latency] model: %d elements, parse+build %d ms%n",
                    module.model().elements().size(), (System.nanoTime() - t0) / 1_000_000);
            cases = stressCases(module.model().elements());
        } else if (!model.isEmpty() && !queries.isEmpty() && !runtime.isEmpty()) {
            set = "queries";
            ctx = Compiler.compileModel(Files.readString(Programs.argument(model), StandardCharsets.UTF_8));
            System.out.printf("[latency] model: %s, parse+build %d ms%n", Path.of(model).getFileName(),
                    (System.nanoTime() - t0) / 1_000_000);
            cases = new ArrayList<>();
            for (String line : Files.readAllLines(Programs.argument(queries), StandardCharsets.UTF_8)) {
                int tab = line.indexOf('\t');
                if (line.isBlank() || line.startsWith("#") || tab < 0) {
                    continue;
                }
                cases.add(new Case(line.substring(0, tab), null, line.substring(tab + 1), runtime));
            }
        } else {
            throw new IllegalArgumentException("--corpus stress, or --model FILE --queries TSV --runtime FQN");
        }
        List<Timing> last = List.of();
        for (int pass = 1; pass <= passes; pass++) {
            long p0 = System.nanoTime();
            List<Timing> ts = new ArrayList<>(cases.size());
            for (Case c : cases) {
                ts.add(time(c, ctx));
            }
            System.out.printf("[latency] pass=%d of %d: %d cases in %d ms wall%s%n", pass, passes, ts.size(),
                    (System.nanoTime() - p0) / 1_000_000, pass == 1 ? " (the model's demand caches empty at its start)" : "");
            if (pass == 1 && !ts.isEmpty()) {
                Timing first = ts.get(0);
                System.out.printf("[latency] cold first case: %s ms (%s, %s)%n", ms(first.total()), first.name(), first.status());
            }
            last = ts;
        }
        report(set, "stress".equals(set) ? "names" : "parse+names", last, outDir);
    }

    /** Every test of every suite of every {@code stress::} service, as the runner builds its program. */
    private static List<Case> stressCases(List<? extends com.legend.model.PackageableElement> elements) {
        List<Case> out = new ArrayList<>();
        List<ServiceDefinition> services = elements.stream()
                .filter(el -> el instanceof ServiceDefinition svc && svc.qualifiedName().startsWith("stress::")
                        && svc.testSuites() != null)
                .map(el -> (ServiceDefinition) el)
                .sorted(Comparator.comparing(ServiceDefinition::qualifiedName))
                .toList();
        for (ServiceDefinition svc : services) {
            for (Protocol.PServiceTestSuite suite : svc.testSuites()) {
                for (Protocol.PServiceTestSuite.PSuiteTest test : suite.tests()) {
                    List<ValueSpecification> statements = new ArrayList<>();
                    if (test.parameters() != null) {
                        for (Protocol.PServiceTestSuite.PSuiteParam p : test.parameters()) {
                            statements.add(new AppliedFunction("letFunction",
                                    List.of(new CString(p.name()), p.value())));
                        }
                    }
                    ValueSpecification fb = svc.functionBody();
                    statements.addAll(fb instanceof LambdaFunction lf && lf.parameters().isEmpty()
                            ? lf.body() : List.of(fb));
                    String name = svc.qualifiedName() + " / " + suite.id() + " / " + test.id();
                    String rt = svc.runtimeRef();
                    out.add(new Case(name, statements, null, rt == null ? "" : rt));
                }
            }
        }
        return out;
    }

    /** One case, compile only: names, type, lower (inline, store resolution, lowering), render. The effects check
     *  (which types the body again to look for statement effects) runs AFTER the clock, on the resolved spec the clock
     *  produced, so nothing warms a case before it is timed; a case with effects keeps its times but is a skip. */
    private static Timing time(Case c, ModelContext ctx) {
        if (c.runtime().isEmpty()) {
            return new Timing(c.name(), 0, 0, 0, 0, "skip: the service names no runtime (a multi-execution service, or none)");
        }
        Timing t = stages(c, ctx);
        if (t.status().equals("ok") && c.statements() != null) {
            ValueSpecification resolved = Compiler.resolveQuery(c.statements(), new ImportScope(List.of()), ctx);
            if (Compiler.hasStatementEffects(resolved, ctx)) {
                return new Timing(t.name(), t.names(), t.type(), t.lower(), t.render(),
                        "skip: statement effects (the runner executes them one by one, not as one plan)");
            }
        }
        return t;
    }

    private static Timing stages(Case c, ModelContext ctx) {
        long n0 = System.nanoTime();
        TypedQuery q;
        try {
            q = c.statements() != null
                    ? Compiler.queryResolved(ctx, Compiler.resolveQuery(c.statements(), new ImportScope(List.of()), ctx))
                    : Compiler.query(ctx, java.util.Objects.requireNonNull(c.query()));
        } catch (RuntimeException e) {
            return new Timing(c.name(), System.nanoTime() - n0, 0, 0, 0, "fail: names: " + bucket(e));
        }
        long n1 = System.nanoTime();
        try {
            q.body();
        } catch (RuntimeException e) {
            return new Timing(c.name(), n1 - n0, System.nanoTime() - n1, 0, 0, "fail: type: " + bucket(e));
        }
        long n2 = System.nanoTime();
        Compiler.LoweredQuery l;
        try {
            l = q.lower(c.runtime(), false);
        } catch (RuntimeException e) {
            return new Timing(c.name(), n1 - n0, n2 - n1, System.nanoTime() - n2, 0, "fail: lower: " + bucket(e));
        }
        long n3 = System.nanoTime();
        try {
            com.legend.database.Databases.dialect(Compiler.executesOn(ctx, c.runtime()).type()).render(l.plan());
        } catch (RuntimeException e) {
            return new Timing(c.name(), n1 - n0, n2 - n1, n3 - n2, System.nanoTime() - n3, "fail: render: " + bucket(e));
        }
        long n4 = System.nanoTime();
        return new Timing(c.name(), n1 - n0, n2 - n1, n3 - n2, n4 - n3, "ok");
    }

    private static void report(String set, String namesStage, List<Timing> ts, Path outDir) throws java.io.IOException {
        List<String> tsv = new ArrayList<>();
        tsv.add("name\t" + namesStage + "_ms\ttype_ms\tlower_ms\trender_ms\ttotal_ms\tstatus");
        for (Timing t : ts) {
            tsv.add(t.name() + "\t" + ms(t.names()) + "\t" + ms(t.type()) + "\t" + ms(t.lower()) + "\t" + ms(t.render())
                    + "\t" + ms(t.total()) + "\t" + t.status());
        }
        Files.write(outDir.resolve("compile-latency-" + set + ".tsv"), tsv, StandardCharsets.UTF_8);
        List<Timing> ok = ts.stream().filter(t -> t.status().equals("ok")).toList();
        long skipped = ts.stream().filter(t -> t.status().startsWith("skip")).count();
        long failed = ts.stream().filter(t -> t.status().startsWith("fail")).count();
        System.out.printf("[latency] set=%s cases=%d ok=%d skipped=%d failed=%d%n", set, ts.size(), ok.size(), skipped, failed);
        stage("total", ok.stream().mapToLong(Timing::total).toArray());
        stage(namesStage, ok.stream().mapToLong(Timing::names).toArray());
        stage("type", ok.stream().mapToLong(Timing::type).toArray());
        stage("lower", ok.stream().mapToLong(Timing::lower).toArray());
        stage("render", ok.stream().mapToLong(Timing::render).toArray());
        Map<String, Integer> buckets = new TreeMap<>();
        for (Timing t : ts) {
            if (!t.status().equals("ok")) {
                buckets.merge(t.status(), 1, Integer::sum);
            }
        }
        buckets.entrySet().stream().sorted((a, b) -> b.getValue() - a.getValue())
                .forEach(e -> System.out.printf("[latency] %s %d %s%n", e.getKey().startsWith("skip") ? "skip" : "fail",
                        e.getValue(), e.getKey().substring(e.getKey().indexOf(": ") + 2)));
        System.out.println("[latency] per-case timings: " + outDir.resolve("compile-latency-" + set + ".tsv"));
    }

    private static void stage(String name, long[] ns) {
        if (ns.length == 0) {
            System.out.printf("[latency] stage=%s p50=- p95=- p99=- max=- mean=- sum=-%n", name);
            return;
        }
        long[] s = ns.clone();
        java.util.Arrays.sort(s);
        long sum = 0;
        for (long v : s) {
            sum += v;
        }
        System.out.printf("[latency] stage=%s p50=%s p95=%s p99=%s max=%s mean=%s sum=%s%n", name,
                ms(s[(int) Math.floor(0.50 * (s.length - 1))]), ms(s[(int) Math.floor(0.95 * (s.length - 1))]),
                ms(s[(int) Math.floor(0.99 * (s.length - 1))]), ms(s[s.length - 1]), ms(sum / s.length), ms(sum));
    }

    private static String ms(long ns) {
        return String.format(java.util.Locale.ROOT, "%.2f", ns / 1_000_000.0);
    }

    /** A failure reason with its specifics elided, so alike failures count together. */
    private static String bucket(RuntimeException e) {
        String r = String.valueOf(e.getMessage()).replaceAll("'[^']*'", "'…'").replaceAll("\\$[^ ]*", "\\$…")
                .replaceAll("\\d+", "N").replace('\n', ' ');
        return e.getClass().getSimpleName() + ": " + (r.length() > 120 ? r.substring(0, 120) : r);
    }

    private static String option(String[] args, String name, String fallback) {
        String v = Programs.option(args, name);
        return v == null ? fallback : v;
    }
}
