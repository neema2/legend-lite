package com.legend.pctcorpus;

import com.legend.Compiler;
import com.legend.model.FunctionDefinition;
import com.legend.model.PackageableElement;
import com.legend.model.ParsedModel;
import com.legend.model.StereotypeApplication;
import com.legend.parser.ElementParser;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.regex.Pattern;
import java.util.stream.Stream;

/**
 * A0 census (docs/PCT_NATIVE_PLAN.md): read every PCT suite .pure file IN
 * PLACE from the checkouts, discover {@code <<PCT.test>>} functions through
 * the REAL parser, probe how far core gets today (parse, element build,
 * eager body type-check), and regenerate {@code docs/PCT_CORPUS.md}.
 *
 * <p>The census never fails the build — it is an instrument, not a gate.
 * Classification semantics follow rcorpus: a wall is LOUD and accounted,
 * never silently skipped; execution is not attempted at A0 (that is A2's
 * adapter binding + eval reduction), so every discovered test scores
 * SHAPE for now.
 */
class PctCorpusRunner {

    /** Textual census: the stereotype applications as spelled in source.
     * {@code \b} keeps {@code PCT.testQualifierProfile} out. Labeled
     * "textual" in the scoreboard — it is the honesty check on parser
     * discovery, not the discovery itself. */
    private static final Pattern TEXTUAL_TEST = Pattern.compile("PCT\\.test\\b");
    private static final Pattern TEXTUAL_NATIVE = Pattern.compile("PCT\\.function\\b");

    private record FileCensus(String suite, String rel, String text,
                              int textualTests, int textualNatives,
                              int parsedElements, List<String> discovered,
                              String parseWall) { }

    private record SuiteCensus(PctCorpus.Suite suite, List<FileCensus> files,
                               String buildFailure, int elementWalls,
                               Map<String, Integer> elementWallBuckets,
                               int bodyWalls,
                               Map<String, Integer> bodyWallBuckets) { }

    @Test
    void census() throws IOException {
        Assumptions.assumeTrue(PctCorpus.available(),
                "legend-pure/legend-engine checkouts not found — set "
                        + "-Dlegend.pure.root / -Dlegend.engine.root");
        List<SuiteCensus> results = new ArrayList<>();
        for (PctCorpus.Suite suite : PctCorpus.suites()) {
            results.add(censusSuite(suite));
        }
        Path doc = PctCorpus.REPO_ROOT.resolve("docs/PCT_CORPUS.md");
        Files.writeString(doc, scoreboard(results));
        for (SuiteCensus s : results) {
            System.out.println(summaryLine(s));
        }
        System.out.println("scoreboard -> " + doc);
    }

    private static SuiteCensus censusSuite(PctCorpus.Suite suite) throws IOException {
        List<FileCensus> files = new ArrayList<>();
        try (Stream<Path> walk = Files.walk(suite.root())) {
            walk.filter(p -> p.toString().endsWith(".pure"))
                    .sorted(Comparator.comparing(Path::toString))
                    .forEach(p -> files.add(censusFile(suite, p)));
        }

        // Module probe: the suite's parseable files compiled TOGETHER, the
        // way the upstream repo means them (cross-file references are
        // normal). buildModule is tolerant (poison, don't drop) — element
        // walls come back as a map; compileAllBodies is the eager-G census
        // over every body.
        String buildFailure = null;
        Map<String, String> elementWalls = Map.of();
        Map<String, String> bodyWalls = Map.of();
        List<Compiler.ModelSource> sources = files.stream()
                .filter(f -> f.parseWall() == null)
                .map(f -> new Compiler.ModelSource(f.rel(), f.text()))
                .toList();
        try {
            Compiler.ParsedModule module = Compiler.parseSources(sources);
            Compiler.BuiltModule built = Compiler.buildModule(module.model());
            elementWalls = built.walls();
            bodyWalls = Compiler.compileAllBodies(built.context());
        } catch (Throwable t) {
            buildFailure = bucket(t.getClass().getSimpleName() + ": " + t.getMessage());
        }
        return new SuiteCensus(suite, files, buildFailure,
                elementWalls.size(), bucketCounts(elementWalls.values()),
                bodyWalls.size(), bucketCounts(bodyWalls.values()));
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
            List<String> discovered = new ArrayList<>();
            for (PackageableElement el : unit.elements()) {
                if (el instanceof FunctionDefinition f && isPctTest(f)) {
                    discovered.add(f.qualifiedName());
                }
            }
            return new FileCensus(suite.name(), rel, text, textualTests,
                    textualNatives, unit.elements().size(), discovered, null);
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

    private static String summaryLine(SuiteCensus s) {
        int files = s.files().size();
        long walled = s.files().stream().filter(f -> f.parseWall() != null).count();
        int textual = s.files().stream().mapToInt(FileCensus::textualTests).sum();
        int discovered = s.files().stream().mapToInt(f -> f.discovered().size()).sum();
        return "%-12s files %3d (parse-walled %3d)  PCT.test textual %3d discovered %3d  elementWalls %3d bodyWalls %3d%s"
                .formatted(s.suite().name(), files, walled, textual, discovered,
                        s.elementWalls(), s.bodyWalls(),
                        s.buildFailure() == null ? "" : "  BUILD: " + s.buildFailure());
    }

    private static String scoreboard(List<SuiteCensus> results) {
        StringBuilder sb = new StringBuilder();
        sb.append("# PCT Corpus — census scoreboard\n\n");
        sb.append("> GENERATED by `PctCorpusRunner` (A0 census — see "
                + "`docs/PCT_NATIVE_PLAN.md`). Never hand-edit; regenerate "
                + "with `mvn -pl pct-corpus test`.\n>\n");
        sb.append("> Suites are read IN PLACE from the legend-pure / "
                + "legend-engine checkouts (`-Dlegend.pure.root` / "
                + "`-Dlegend.engine.root`); the denominator is whatever is "
                + "on disk.\n\n");

        sb.append("## Suites\n\n");
        sb.append("| suite | files | parse-walled | elements | `<<PCT.test>>` (textual) "
                + "| discovered | `<<PCT.function>>` (textual) | element walls | body walls |\n");
        sb.append("|---|---|---|---|---|---|---|---|---|\n");
        int tFiles = 0;
        int tWalled = 0;
        int tTextual = 0;
        int tDiscovered = 0;
        for (SuiteCensus s : results) {
            int files = s.files().size();
            long walled = s.files().stream().filter(f -> f.parseWall() != null).count();
            int elements = s.files().stream().mapToInt(FileCensus::parsedElements).sum();
            int textual = s.files().stream().mapToInt(FileCensus::textualTests).sum();
            int natives = s.files().stream().mapToInt(FileCensus::textualNatives).sum();
            int discovered = s.files().stream().mapToInt(f -> f.discovered().size()).sum();
            tFiles += files;
            tWalled += (int) walled;
            tTextual += textual;
            tDiscovered += discovered;
            sb.append("| %s | %d | %d | %d | %d | %d | %d | %d | %d |\n".formatted(
                    s.suite().name(), files, walled, elements, textual,
                    discovered, natives, s.elementWalls(), s.bodyWalls()));
        }
        sb.append("| **total** | **%d** | **%d** | | **%d** | **%d** | | | |\n\n"
                .formatted(tFiles, tWalled, tTextual, tDiscovered));

        for (SuiteCensus s : results) {
            if (s.buildFailure() != null) {
                sb.append("**BUILD FAILURE — ").append(s.suite().name())
                        .append(":** ").append(s.buildFailure()).append("\n\n");
            }
        }

        sb.append("## Parse-wall buckets\n\n");
        appendBuckets(sb, results, r -> bucketCounts(
                r.files().stream().map(FileCensus::parseWall)
                        .filter(w -> w != null).toList()));

        sb.append("## Element-wall buckets\n\n");
        appendBuckets(sb, results, SuiteCensus::elementWallBuckets);

        sb.append("## Body-wall buckets (eager G over every body)\n\n");
        appendBuckets(sb, results, SuiteCensus::bodyWallBuckets);

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
        sb.append("| count | failure |\n|---|---|\n");
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
