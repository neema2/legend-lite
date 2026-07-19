package com.legend.pctcorpus;

import com.legend.Compiler;
import com.legend.compiler.element.ModelContext;
import com.legend.harness.TestBody;
import com.legend.model.FunctionDefinition;
import com.legend.model.ImportScope;
import com.legend.model.PackageableElement;
import com.legend.model.ParsedModel;
import com.legend.model.StereotypeApplication;
import com.legend.model.spec.AppliedFunction;
import com.legend.model.spec.CString;
import com.legend.model.spec.LambdaFunction;
import com.legend.model.spec.ValueSpecification;
import com.legend.model.spec.Variable;
import com.legend.parser.ElementParser;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.regex.Pattern;
import java.util.stream.Stream;

/**
 * PCT-as-data (docs/PCT_NATIVE_PLAN.md Mode A): read every PCT suite
 * .pure file IN PLACE from the checkouts, discover {@code <<PCT.test>>}
 * functions through the REAL parser, and EXECUTE each test through core
 * — the test's own body statements driven by {@link TestBody} with the
 * adapter parameter bound to the literal in-memory adapter
 * {@code {g | $g->eval()}} (pct_core.pure's reference adapter). The
 * platform's UserCallInliner &beta;-reduces the eval chain; every eval
 * payload lowers to SQL on DuckDB. Zero munging: the runner constructs
 * exactly two synthetic AST nodes (the {@code let f = <adapter>} binding
 * and the module's {@code pctcorpus::Rt} runtime source) — never edits
 * the corpus text.
 *
 * <p>Classification follows rcorpus: PASS / FAIL (wrong values) /
 * ERROR (loud named wall) / SHAPE (runner vocabulary gap — accounted,
 * never silently skipped). Regenerates {@code docs/PCT_CORPUS.md}.
 */
class PctCorpusRunner {

    /** Textual census: the stereotype applications as spelled in source.
     * {@code \b} keeps {@code PCT.testQualifierProfile} out. This is the
     * honesty check on parser discovery (it also counts commented-out
     * tests — see the discovery-mismatch section), not the discovery. */
    private static final Pattern TEXTUAL_TEST = Pattern.compile("PCT\\.test\\b");
    private static final Pattern TEXTUAL_NATIVE = Pattern.compile("PCT\\.function\\b");

    private static final String RT_FQN = "pctcorpus::Rt";
    private static final String RT_SOURCE =
            "Runtime " + RT_FQN + " { mappings: []; connections: []; }";

    private record PctTest(String fqn, FunctionDefinition fn, ImportScope imports) { }

    private record FileCensus(String suite, String rel, String text,
                              int textualTests, int textualNatives,
                              int parsedElements, List<PctTest> tests,
                              String parseWall) { }

    private enum Outcome { PASS, FAIL, ERROR, SHAPE }

    private record TestResult(String fqn, String file, Outcome outcome, String detail) { }

    private record SuiteCensus(PctCorpus.Suite suite, List<FileCensus> files,
                               String buildFailure, int elementWalls,
                               Map<String, Integer> elementWallBuckets,
                               int bodyWalls,
                               Map<String, Integer> bodyWallBuckets,
                               List<TestResult> results) { }

    @Test
    void run() throws Exception {
        Assumptions.assumeTrue(PctCorpus.available(),
                "legend-pure/legend-engine checkouts not found — set "
                        + "-Dlegend.pure.root / -Dlegend.engine.root");
        List<SuiteCensus> results = new ArrayList<>();
        for (PctCorpus.Suite suite : PctCorpus.suites()) {
            results.add(runSuite(suite));
        }
        Path doc = PctCorpus.REPO_ROOT.resolve("docs/PCT_CORPUS.md");
        Files.writeString(doc, scoreboard(results));
        for (SuiteCensus s : results) {
            System.out.println(summaryLine(s));
        }
        System.out.println("scoreboard -> " + doc);
    }

    private static SuiteCensus runSuite(PctCorpus.Suite suite) throws Exception {
        List<FileCensus> files = new ArrayList<>();
        try (Stream<Path> walk = Files.walk(suite.root())) {
            walk.filter(p -> p.toString().endsWith(".pure"))
                    .sorted(Comparator.comparing(Path::toString))
                    .forEach(p -> files.add(censusFile(suite, p)));
        }

        // Module probe: the suite's parseable files compiled TOGETHER (the
        // way the upstream repo means them; cross-file references are
        // normal) plus the synthesized runtime. buildModule is tolerant
        // (poison, don't drop); compileAllBodies is the eager-G census.
        String buildFailure = null;
        Map<String, String> elementWalls = Map.of();
        Map<String, String> bodyWalls = Map.of();
        ModelContext ctx = null;
        List<Compiler.ModelSource> sources = new ArrayList<>(files.stream()
                .filter(f -> f.parseWall() == null)
                .map(f -> new Compiler.ModelSource(f.rel(), f.text()))
                .toList());
        sources.add(new Compiler.ModelSource("pctcorpus_rt.pure", RT_SOURCE));
        try {
            Compiler.ParsedModule module = Compiler.parseSources(sources);
            Compiler.BuiltModule built = Compiler.buildModule(module.model());
            ctx = built.context();
            elementWalls = built.walls();
            bodyWalls = Compiler.compileAllBodies(ctx);
        } catch (Throwable t) {
            buildFailure = bucket(t.getClass().getSimpleName() + ": " + t.getMessage());
        }

        List<TestResult> testResults = new ArrayList<>();
        if (ctx != null) {
            for (FileCensus f : files) {
                for (PctTest t : f.tests()) {
                    testResults.add(runTest(ctx, f, t));
                }
            }
        }
        return new SuiteCensus(suite, files, buildFailure,
                elementWalls.size(), bucketCounts(elementWalls.values()),
                bodyWalls.size(), bucketCounts(bodyWalls.values()),
                testResults);
    }

    /**
     * One test: bind the adapter parameter to the literal in-memory
     * adapter and drive the test's OWN body statements through TestBody.
     * {@code $f->eval(|expr)} substitutes to
     * {@code ({g|$g->eval()})->eval(|expr)} and the platform's inliner
     * &beta;-reduces both evals down to {@code expr}.
     */
    private static TestResult runTest(ModelContext ctx, FileCensus f, PctTest t) {
        try (Connection conn = DriverManager.getConnection("jdbc:duckdb:")) {
            try (Statement st = conn.createStatement()) {
                st.execute("SET TimeZone='UTC'");
            }
            List<ValueSpecification> stmts = new ArrayList<>();
            if (t.fn().parameters().size() == 1) {
                String param = t.fn().parameters().get(0).name();
                stmts.add(new AppliedFunction("letFunction", List.of(
                        new CString(param), identityAdapter())));
            } else if (!t.fn().parameters().isEmpty()) {
                return new TestResult(t.fqn(), f.rel(), Outcome.SHAPE,
                        "PCT test with " + t.fn().parameters().size()
                                + " parameters (expected the single adapter param)");
            }
            stmts.addAll(t.fn().body());
            TestBody.Outcome out = TestBody.run(ctx, stmts,
                    importScopeOf(t), RT_FQN, conn, false);
            return classify(t, f, out);
        } catch (RuntimeException | java.sql.SQLException e) {
            String msg = e.getMessage() == null
                    ? e.getClass().getSimpleName()
                    : e.getMessage();
            return new TestResult(t.fqn(), f.rel(), Outcome.ERROR, bucket(msg));
        }
    }

    /** {@code {g | $g->eval()}} — pct_core.pure's in-memory adapter as AST. */
    private static LambdaFunction identityAdapter() {
        return new LambdaFunction(
                List.of(new Variable("g")),
                List.of(new AppliedFunction("eval", List.of(new Variable("g")))));
    }

    /** The test's import scope: its file's section imports + its own
     * package (implicit in real pure) — the rcorpus importScopeOf rule. */
    private static ImportScope importScopeOf(PctTest t) {
        List<String> wildcards = new ArrayList<>();
        Map<String, String> typeImports = new LinkedHashMap<>();
        if (t.imports() != null) {
            wildcards.addAll(t.imports().wildcards());
            typeImports.putAll(t.imports().typeImports());
        }
        int cut = t.fqn().lastIndexOf("::");
        if (cut > 0) {
            wildcards.add(t.fqn().substring(0, cut));
        }
        return new ImportScope(wildcards, typeImports);
    }

    private static TestResult classify(PctTest t, FileCensus f, TestBody.Outcome out) {
        return switch (out) {
            case TestBody.Outcome.Unsupported u ->
                    new TestResult(t.fqn(), f.rel(), Outcome.SHAPE, bucket(u.reason()));
            case TestBody.Outcome.Ran r -> {
                if (!r.failures().isEmpty()) {
                    yield new TestResult(t.fqn(), f.rel(), Outcome.FAIL,
                            bucket(r.failures().get(0)));
                }
                if (r.verified() == 0 && r.advisory() > 0) {
                    yield new TestResult(t.fqn(), f.rel(), Outcome.SHAPE,
                            "sql-only: " + r.advisory() + " advisory assert(s)");
                }
                if (r.verified() == 0) {
                    yield new TestResult(t.fqn(), f.rel(), Outcome.SHAPE,
                            "no verifying assertions");
                }
                yield new TestResult(t.fqn(), f.rel(), Outcome.PASS, "");
            }
        };
    }

    private static FileCensus censusFile(PctCorpus.Suite suite, Path file) {
        String rel = suite.root().relativize(file).toString();
        String text;
        try {
            text = Files.readString(file);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        int textualTests = (int) TEXTUAL_TEST.matcher(text).results().count();
        int textualNatives = (int) TEXTUAL_NATIVE.matcher(text).results().count();
        try {
            ParsedModel unit = ElementParser.parse(text);
            List<PctTest> tests = new ArrayList<>();
            for (PackageableElement el : unit.elements()) {
                if (el instanceof FunctionDefinition fn && isPctTest(fn)) {
                    tests.add(new PctTest(fn.qualifiedName(), fn,
                            unit.elementImports().get(fn.qualifiedName())));
                }
            }
            return new FileCensus(suite.name(), rel, text, textualTests,
                    textualNatives, unit.elements().size(), tests, null);
        } catch (RuntimeException e) {
            return new FileCensus(suite.name(), rel, text, textualTests,
                    textualNatives, 0, List.of(),
                    bucket(String.valueOf(e.getMessage())));
        }
    }

    /** The PCT test marker: stereotype {@code test} on profile
     * {@code meta::pure::test::pct::PCT} — bare or FQN spelling, the same
     * dual-accept rcorpus's {@code testKindOf} uses for its profile. */
    private static boolean isPctTest(FunctionDefinition f) {
        for (StereotypeApplication st : f.stereotypes()) {
            String profile = st.profileName();
            if ((profile.equals("PCT") || profile.equals("meta::pure::test::pct::PCT"))
                    && st.stereotypeName().equals("test")) {
                return true;
            }
        }
        return false;
    }

    // ---- reporting ----

    /** One bucket key per distinct failure SHAPE: first line, capped. */
    private static String bucket(String message) {
        String first = message == null ? "null" : message.lines().findFirst().orElse("");
        return first.length() > 140 ? first.substring(0, 140) + "…" : first;
    }

    private static Map<String, Integer> bucketCounts(Iterable<String> messages) {
        Map<String, Integer> counts = new LinkedHashMap<>();
        for (String m : messages) {
            counts.merge(bucket(m), 1, Integer::sum);
        }
        return counts;
    }

    private static long count(SuiteCensus s, Outcome o) {
        return s.results().stream().filter(r -> r.outcome() == o).count();
    }

    private static String summaryLine(SuiteCensus s) {
        int files = s.files().size();
        long walled = s.files().stream().filter(f -> f.parseWall() != null).count();
        int discovered = s.files().stream().mapToInt(f -> f.tests().size()).sum();
        return "%-12s files %3d (walled %2d) tests %4d | PASS %4d FAIL %3d ERROR %4d SHAPE %4d%s"
                .formatted(s.suite().name(), files, walled, discovered,
                        count(s, Outcome.PASS), count(s, Outcome.FAIL),
                        count(s, Outcome.ERROR), count(s, Outcome.SHAPE),
                        s.buildFailure() == null ? "" : "  BUILD: " + s.buildFailure());
    }

    private static String scoreboard(List<SuiteCensus> results) {
        StringBuilder sb = new StringBuilder();
        sb.append("# PCT Corpus — scoreboard\n\n");
        sb.append("> GENERATED by `PctCorpusRunner` (Mode A — see "
                + "`docs/PCT_NATIVE_PLAN.md`). Never hand-edit; regenerate "
                + "with `mvn -pl pct-corpus test`.\n>\n");
        sb.append("> Suites are read IN PLACE from the legend-pure / "
                + "legend-engine checkouts (`-Dlegend.pure.root` / "
                + "`-Dlegend.engine.root`); the denominator is whatever is "
                + "on disk. Tests execute FULLY through core: body driven "
                + "by TestBody, adapter bound to the literal in-memory "
                + "adapter, eval payloads lowered to SQL on DuckDB.\n\n");

        sb.append("## Suites\n\n");
        sb.append("| suite | files | parse-walled | `<<PCT.test>>` (textual) "
                + "| discovered | **PASS** | FAIL | ERROR | SHAPE | element walls | body walls |\n");
        sb.append("|---|---|---|---|---|---|---|---|---|---|---|\n");
        int tFiles = 0;
        int tWalled = 0;
        int tTextual = 0;
        int tDiscovered = 0;
        long tPass = 0;
        long tFail = 0;
        long tError = 0;
        long tShape = 0;
        for (SuiteCensus s : results) {
            int files = s.files().size();
            long walled = s.files().stream().filter(f -> f.parseWall() != null).count();
            int textual = s.files().stream().mapToInt(FileCensus::textualTests).sum();
            int discovered = s.files().stream().mapToInt(f -> f.tests().size()).sum();
            long pass = count(s, Outcome.PASS);
            long fail = count(s, Outcome.FAIL);
            long error = count(s, Outcome.ERROR);
            long shape = count(s, Outcome.SHAPE);
            tFiles += files;
            tWalled += (int) walled;
            tTextual += textual;
            tDiscovered += discovered;
            tPass += pass;
            tFail += fail;
            tError += error;
            tShape += shape;
            sb.append("| %s | %d | %d | %d | %d | **%d** | %d | %d | %d | %d | %d |\n"
                    .formatted(s.suite().name(), files, walled, textual,
                            discovered, pass, fail, error, shape,
                            s.elementWalls(), s.bodyWalls()));
        }
        sb.append(("| **total** | **%d** | **%d** | **%d** | **%d** | **%d** "
                + "| **%d** | **%d** | **%d** | | |\n\n")
                .formatted(tFiles, tWalled, tTextual, tDiscovered,
                        tPass, tFail, tError, tShape));

        for (SuiteCensus s : results) {
            if (s.buildFailure() != null) {
                sb.append("**BUILD FAILURE — ").append(s.suite().name())
                        .append(":** ").append(s.buildFailure()).append("\n\n");
            }
        }

        sb.append("## Top ERROR buckets\n\n");
        appendBuckets(sb, results, s -> bucketCounts(
                s.results().stream().filter(r -> r.outcome() == Outcome.ERROR)
                        .map(TestResult::detail).toList()));

        sb.append("## Top SHAPE buckets\n\n");
        appendBuckets(sb, results, s -> bucketCounts(
                s.results().stream().filter(r -> r.outcome() == Outcome.SHAPE)
                        .map(TestResult::detail).toList()));

        sb.append("## FAIL ledger\n\n");
        for (SuiteCensus s : results) {
            for (TestResult r : s.results()) {
                if (r.outcome() == Outcome.FAIL) {
                    sb.append("- FAIL ").append(r.fqn()).append(" [")
                            .append(s.suite().name()).append(':').append(r.file())
                            .append("]: ").append(r.detail()).append('\n');
                }
            }
        }
        sb.append('\n');

        sb.append("## ERROR ledger\n\n");
        for (SuiteCensus s : results) {
            for (TestResult r : s.results()) {
                if (r.outcome() == Outcome.ERROR) {
                    sb.append("- ERROR ").append(r.fqn()).append(" [")
                            .append(s.suite().name()).append(':').append(r.file())
                            .append("]: ").append(r.detail()).append('\n');
                }
            }
        }
        sb.append('\n');

        sb.append("## Discovery mismatches (parsed files where textual count != discovered)\n\n");
        for (SuiteCensus s : results) {
            for (FileCensus f : s.files()) {
                if (f.parseWall() == null && f.textualTests() != f.tests().size()) {
                    sb.append("- `").append(s.suite().name()).append(':')
                            .append(f.rel()).append("` — textual ")
                            .append(f.textualTests()).append(", discovered ")
                            .append(f.tests().size())
                            .append(" (commented-out tests count textually)\n");
                }
            }
        }
        sb.append('\n');

        sb.append("## Parse-walled files\n\n");
        for (SuiteCensus s : results) {
            for (FileCensus f : s.files()) {
                if (f.parseWall() != null) {
                    sb.append("- `").append(s.suite().name()).append(':')
                            .append(f.rel()).append("` — ").append(f.parseWall())
                            .append('\n');
                }
            }
        }
        return sb.toString();
    }

    private static void appendBuckets(StringBuilder sb, List<SuiteCensus> results,
            java.util.function.Function<SuiteCensus, Map<String, Integer>> extract) {
        Map<String, Integer> merged = new LinkedHashMap<>();
        for (SuiteCensus s : results) {
            extract.apply(s).forEach((k, v) -> merged.merge(k, v, Integer::sum));
        }
        // count-desc, message-asc for a stable, diffable ordering
        Map<String, Integer> sorted = new TreeMap<>(
                Comparator.<String>comparingInt(k -> -merged.get(k)).thenComparing(k -> k));
        sorted.putAll(merged);
        int shown = 0;
        sb.append("| count | detail |\n|---|---|\n");
        for (Map.Entry<String, Integer> e : sorted.entrySet()) {
            sb.append("| ").append(e.getValue()).append(" | `")
                    .append(e.getKey().replace("|", "\\|")).append("` |\n");
            if (++shown == 30) {
                int rest = sorted.size() - shown;
                if (rest > 0) {
                    sb.append("| … | (").append(rest).append(" more buckets) |\n");
                }
                break;
            }
        }
        sb.append('\n');
    }
}
