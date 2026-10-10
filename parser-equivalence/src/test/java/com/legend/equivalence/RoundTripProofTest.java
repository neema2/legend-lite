// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.equivalence;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.legend.json.Json;
import com.legend.parser.PmcdParser;
import com.legend.protocol.ModelComposer;
import com.legend.protocol.PureComposer;
import com.legend.protocol.SourceInformation;
import com.legend.testing.Runfile;
import com.legend.testing.TestOutputs;
import org.finos.legend.engine.language.pure.grammar.from.PureGrammarParser;
import org.finos.legend.engine.language.pure.grammar.to.PureGrammarComposer;
import org.finos.legend.engine.language.pure.grammar.to.PureGrammarComposerContext;
import org.finos.legend.engine.protocol.pure.v1.model.context.PureModelContextData;
import org.finos.legend.engine.shared.core.ObjectMapperFactory;
import org.finos.legend.engine.shared.core.api.grammar.RenderStyle;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * THE ROUND TRIP, PROVEN (docs/PROTOCOL_PROGRAM_2026_10_05.md §4, leg 5): every model of three inputs survives lite's
 * own text to JSON to text to JSON unchanged, in both render styles -- legend-engine's test collection (the corpus the
 * parity tests read, and its grammar round-trip tests' texts), lite's own projects ({@code projects/}), and the
 * upstream Legend showcase projects (MODULE.bazel, pinned by commit).
 *
 * <p>A TEXT round-trips when the JSON lite parses from it equals the JSON lite parses from what lite prints of that
 * JSON (no source information; in STANDARD and in PRETTY). A PROJECT -- one element per file, as an SDLC keeps it --
 * round-trips as Studio opens one: each file parsed on its own, its elements gathered into one model with no section
 * index, that model printed and the print parsed again, the same elements back (by kind and path).
 *
 * <p>What is not counted as a pass: a text or file the engine's own parser refuses ("engine refuses": nothing to
 * prove); one lite refuses by name ("refused", listed with its reason: the printer's and the reader's named refusals,
 * S36); one whose round trip the engine fails too ("upstream", listed: the engine printing what it cannot read back).
 * Anything else is a FAILURE, listed. The counts are pinned per input, matched up-only and failed down-only.
 */
class RoundTripProofTest {

    /** Matched up-only; failed down-only, 0 the target; refused down-only; upstream down-only (a text lite stopped
     *  round-tripping would otherwise hide among the engine's own). Measured 2026-10-09 (leg 5). */
    private static final Map<String, int[]> PINS = Map.of(
            // input -> {min matched, max failed, max refused, max upstream}
            "engine corpus", new int[]{6905, 0, 40, 270},
            "lite projects", new int[]{222, 0, 0, 0},
            "showcase projects", new int[]{131, 0, 0, 0});

    /** The declared list of the projects' files (parser-equivalence/BUILD.bazel: round_trip_projects). */
    private static final String PROJECTS_PROPERTY = "legend.roundtrip.projects";

    private static final List<PureComposer.Style> STYLES = List.of(PureComposer.Style.STANDARD, PureComposer.Style.PRETTY);

    private final PureGrammarParser engineParser = PureGrammarParser.newInstance();
    private final ObjectMapper mapper = ObjectMapperFactory.getNewStandardObjectMapperWithPureProtocolExtensionSupports();
    private final Map<PureComposer.Style, PureGrammarComposer> engineComposer = Map.of(
            PureComposer.Style.STANDARD, engineComposer(RenderStyle.STANDARD),
            PureComposer.Style.PRETTY, engineComposer(RenderStyle.PRETTY));

    private static PureGrammarComposer engineComposer(RenderStyle style) {
        return PureGrammarComposer.newInstance(PureGrammarComposerContext.Builder.newInstance().withRenderStyle(style).build());
    }

    /** One input's counts, and what it did not pass, named. */
    private static final class Tally {
        final String name;
        int matched;
        int refused;
        int upstream;
        int failed;
        int engineRefuses;
        final Map<String, Integer> refusals = new TreeMap<>();
        final List<String> upstreamCases = new ArrayList<>();
        final List<String> failures = new ArrayList<>();

        Tally(String name) {
            this.name = name;
        }
    }

    @Test
    void everyModelRoundTrips() throws IOException {
        Tally corpus = new Tally("engine corpus");
        for (Corpus.Source src : Corpus.all()) {
            text(corpus, src.id(), src.text());
        }
        for (Path file : ModelComposerParityTest.roundtripTestFiles()) {
            int i = 0;
            for (String run : InlineSnippets.literalRuns(Files.readString(file))) {
                text(corpus, "roundtrip:" + file.getFileName() + "#" + i++, run);
            }
        }
        Tally lite = new Tally("lite projects");
        Tally showcase = new Tally("showcase projects");
        for (Map.Entry<String, List<Path>> project : projects().entrySet()) {
            Tally t = project.getKey().startsWith("showcase:") ? showcase : lite;
            project(t, project.getKey(), project.getValue());
        }

        StringBuilder out = new StringBuilder();
        for (Tally t : List.of(corpus, lite, showcase)) {
            report(t, out);
        }
        Files.writeString(TestOutputs.file("round-trip-proof.txt"), out.toString());
        for (Tally t : List.of(corpus, lite, showcase)) {
            int[] pin = PINS.get(t.name);
            assertTrue(t.matched >= pin[0], t.name + ": matched " + t.matched + " < " + pin[0]);
            assertTrue(t.failed <= pin[1], t.name + ": failed " + t.failed + " > " + pin[1] + "\n" + String.join("\n\n", t.failures));
            assertTrue(t.refused <= pin[2], t.name + ": refused " + t.refused + " > " + pin[2] + " " + t.refusals);
            assertTrue(t.upstream <= pin[3], t.name + ": upstream " + t.upstream + " > " + pin[3] + "\n"
                    + String.join("\n", t.upstreamCases));
        }
    }

    // ---------------------------------------------------------------------
    // A text
    // ---------------------------------------------------------------------

    /** One model text: text to JSON to text to JSON, in each style. */
    private void text(Tally t, String id, String text) {
        if (!engineParses(text)) {
            t.engineRefuses++;
            return;
        }
        String j1;
        try {
            j1 = liteJson(text);
        } catch (RuntimeException e) {
            fail(t, id, "lite cannot parse what the engine parses: " + e, text);
            return;
        }
        if (object(j1, DEEP).getArr("elements").items().stream()
                .allMatch(e -> "sectionIndex".equals(((Json.Obj) e).getStringOr("_type", "")))) {
            return;   // no element, only the section index: not a model (an expected message, a resource's name)
        }
        for (PureComposer.Style style : STYLES) {
            String printed;
            try {
                printed = ModelComposer.model(j1, style);
            } catch (IllegalArgumentException e) {
                t.refused++;
                t.refusals.merge(String.valueOf(e.getMessage()), 1, Integer::sum);
                return;
            }
            String j2;
            try {
                j2 = liteJson(printed);
            } catch (RuntimeException e) {
                failOrUpstream(t, id, style, text, "lite cannot parse its own print: " + e + "\n--- print\n" + printed);
                return;
            }
            if (!j1.equals(j2)) {
                failOrUpstream(t, id, style, text, "the JSON changed at " + firstDifference(object(j1, DEEP), object(j2, DEEP), "$")
                        + "\n--- print\n" + abbreviate(printed));
                return;
            }
        }
        t.matched++;
    }

    /** lite's JSON for a text with every span removed, the named ones and the ones the engine keeps inside a test's
     *  parameter values included: a span records where a text put a thing, and the round trip compares the things. */
    private static String liteJson(String text) {
        return SourceInformation.stripAll(PmcdParser.parseDocument(text));
    }

    // ---------------------------------------------------------------------
    // A project
    // ---------------------------------------------------------------------

    /**
     * One project: each file round-trips as a text; then the project as Studio opens it -- every file's elements in one
     * model with no section index, printed in each style, the print parsed, the same elements back.
     */
    private void project(Tally t, String name, List<Path> files) throws IOException {
        List<Json.Node> elements = new ArrayList<>();
        boolean whole = true;
        for (Path f : files) {
            String text = Files.readString(f, StandardCharsets.UTF_8);
            String id = name + "/" + f.getFileName();
            int failedBefore = t.failed + t.refused + t.upstream + t.engineRefuses;
            text(t, id, text);
            if (t.failed + t.refused + t.upstream + t.engineRefuses != failedBefore) {
                whole = false;   // the file did not pass alone: the project cannot be tried whole
                continue;
            }
            for (Json.Node e : object(liteJson(text), DEEP).getArr("elements").items()) {
                if (!"sectionIndex".equals(((Json.Obj) e).getStringOr("_type", ""))) {
                    elements.add(e);
                }
            }
        }
        if (!whole) {
            return;
        }
        LinkedHashMap<String, Json.Node> model = new LinkedHashMap<>();
        model.put("_type", new Json.Str("data"));
        model.put("elements", new Json.Arr(elements));
        String json = Json.toCompact(new Json.Obj(model));
        Map<String, String> before = byKindAndPath(elements);
        for (PureComposer.Style style : STYLES) {
            String printed;
            try {
                printed = ModelComposer.model(json, style);
            } catch (IllegalArgumentException e) {
                t.refused++;
                t.refusals.merge(String.valueOf(e.getMessage()), 1, Integer::sum);
                return;
            }
            Map<String, String> after;
            try {
                List<Json.Node> back = new ArrayList<>();
                for (Json.Node e : object(liteJson(printed), DEEP).getArr("elements").items()) {
                    if (!"sectionIndex".equals(((Json.Obj) e).getStringOr("_type", ""))) {
                        back.add(e);
                    }
                }
                after = byKindAndPath(back);
            } catch (RuntimeException e) {
                projectFailOrUpstream(t, name, style, json, "lite cannot parse its own print of the project: " + e
                        + "\n--- print\n" + abbreviate(printed));
                return;
            }
            if (!before.equals(after)) {
                projectFailOrUpstream(t, name, style, json, "the project's elements changed: " + difference(before, after)
                        + "\n--- print\n" + abbreviate(printed));
                return;
            }
        }
        t.matched++;
    }

    /** A whole model's JSON nests far deeper than the JSON library's default limit. */
    private static final Json.Config DEEP = new Json.Config(4096);

    private static Json.Obj object(String json, Json.Config config) {
        return (Json.Obj) Json.parse(json, config);
    }

    /** Each element's JSON by its kind and path: the order a print groups elements in is not the files'. */
    private static Map<String, String> byKindAndPath(List<Json.Node> elements) {
        Map<String, String> out = new TreeMap<>();
        for (Json.Node n : elements) {
            Json.Obj e = (Json.Obj) n;
            String key = e.getStringOr("_type", "?") + " " + e.getStringOr("package", "") + "::" + e.getStringOr("name", "");
            if (out.put(key, Json.toCompact(e)) != null) {
                throw new IllegalStateException("two elements are " + key);
            }
        }
        return out;
    }

    /** Where two JSON trees first differ: the path, then each side's value there (abbreviated). */
    private static String firstDifference(Json.Node a, Json.Node b, String path) {
        if (a instanceof Json.Obj oa && b instanceof Json.Obj ob) {
            for (String k : oa.fields().keySet()) {
                if (!ob.has(k)) {
                    return path + "." + k + " (gone)";
                }
                if (!Json.toCompact(oa.get(k)).equals(Json.toCompact(ob.get(k)))) {
                    return firstDifference(oa.get(k), ob.get(k), path + "." + k);
                }
            }
            for (String k : ob.fields().keySet()) {
                if (!oa.has(k)) {
                    return path + "." + k + " (new)";
                }
            }
            return path + " (key order)";
        }
        if (a instanceof Json.Arr xa && b instanceof Json.Arr xb) {
            for (int i = 0; i < Math.min(xa.items().size(), xb.items().size()); i++) {
                if (!Json.toCompact(xa.items().get(i)).equals(Json.toCompact(xb.items().get(i)))) {
                    return firstDifference(xa.items().get(i), xb.items().get(i), path + "[" + i + "]");
                }
            }
            return path + " (length " + xa.items().size() + " -> " + xb.items().size() + ")";
        }
        return path + "\n  before " + abbreviate(Json.toCompact(a)) + "\n  after  " + abbreviate(Json.toCompact(b));
    }

    private static String difference(Map<String, String> before, Map<String, String> after) {
        List<String> out = new ArrayList<>();
        for (String k : before.keySet()) {
            if (!after.containsKey(k)) {
                out.add("lost " + k);
            } else if (!before.get(k).equals(after.get(k))) {
                out.add("changed " + k + "\n  before " + abbreviate(before.get(k)) + "\n  after  " + abbreviate(after.get(k)));
            }
        }
        for (String k : after.keySet()) {
            if (!before.containsKey(k)) {
                out.add("gained " + k);
            }
        }
        return String.join("\n", out);
    }

    // ---------------------------------------------------------------------
    // The engine: what it reads, and whether it round-trips itself
    // ---------------------------------------------------------------------

    private boolean engineParses(String text) {
        try {
            engineParser.parseModel(text, "", 0, 0, false);
            return true;
        } catch (Throwable t) {
            return false;
        }
    }

    /** Whether the engine's own text round trip keeps a text's JSON, in this style. */
    private boolean engineRoundTrips(String text, PureComposer.Style style) {
        try {
            PureModelContextData first = engineParser.parseModel(text, "", 0, 0, false);
            String printed = engineComposer.get(style).renderPureModelContextData(first);
            PureModelContextData second = engineParser.parseModel(printed, "", 0, 0, false);
            return mapper.writeValueAsString(first).equals(mapper.writeValueAsString(second));
        } catch (Throwable t) {
            return false;
        }
    }

    /** Whether the engine's own project round trip keeps a model's elements, in this style. */
    private boolean engineProjectRoundTrips(String json, PureComposer.Style style) {
        try {
            PureModelContextData model = mapper.readValue(json, PureModelContextData.class);
            String printed = engineComposer.get(style).renderPureModelContextData(model);
            PureModelContextData back = engineParser.parseModel(printed, "", 0, 0, false);
            List<Json.Node> elements = new ArrayList<>();
            for (Json.Node e : object(mapper.writeValueAsString(back), DEEP).getArr("elements").items()) {
                if (!"sectionIndex".equals(((Json.Obj) e).getStringOr("_type", ""))) {
                    elements.add(e);
                }
            }
            List<Json.Node> before = object(json, DEEP).getArr("elements").items();
            return byKindAndPath(engineElements(before)).equals(byKindAndPath(elements));
        } catch (Throwable t) {
            return false;
        }
    }

    /** The engine's own JSON for lite's elements (its serializer's spelling), to compare like with like. */
    private List<Json.Node> engineElements(List<Json.Node> liteElements) throws IOException {
        LinkedHashMap<String, Json.Node> model = new LinkedHashMap<>();
        model.put("_type", new Json.Str("data"));
        model.put("elements", new Json.Arr(liteElements));
        PureModelContextData read = mapper.readValue(Json.toCompact(new Json.Obj(model)), PureModelContextData.class);
        return object(mapper.writeValueAsString(read), DEEP).getArr("elements").items();
    }

    private void failOrUpstream(Tally t, String id, PureComposer.Style style, String text, String why) {
        if (!engineRoundTrips(text, style)) {
            t.upstream++;
            t.upstreamCases.add(id + " (" + style + ")");
            return;
        }
        fail(t, id + " (" + style + ")", why, text);
    }

    private void projectFailOrUpstream(Tally t, String name, PureComposer.Style style, String json, String why) {
        if (!engineProjectRoundTrips(json, style)) {
            t.upstream++;
            t.upstreamCases.add(name + " (project, " + style + ")");
            return;
        }
        t.failed++;
        t.failures.add(name + " (project, " + style + ")\n" + why);
    }

    private static void fail(Tally t, String id, String why, String text) {
        t.failed++;
        t.failures.add(id + "\n" + why + "\n--- text\n" + abbreviate(text));
    }

    // ---------------------------------------------------------------------
    // The projects, from the declared list
    // ---------------------------------------------------------------------

    /**
     * The projects' files by project, from the declared list: a line of lite's own is {@code _main/projects/<name>/...},
     * a showcase project's {@code <its repository>/<module>/src/main/pure/...} (the repository named
     * {@code ...legend_showcase_<name>}); each project's files in their path order.
     */
    static Map<String, List<Path>> projects() throws IOException {
        Path list = Runfile.property(PROJECTS_PROPERTY);
        Map<String, List<Path>> out = new TreeMap<>();
        for (String line : Files.readAllLines(list, StandardCharsets.UTF_8)) {
            if (line.isBlank()) {
                continue;
            }
            String[] parts = line.split("/");
            String key;
            int showcase = parts[0].indexOf("legend_showcase_");
            if (showcase >= 0) {
                key = "showcase:" + parts[0].substring(showcase + "legend_showcase_".length());
            } else if (parts.length >= 3 && "projects".equals(parts[1])) {
                key = "projects:" + parts[2];
            } else {
                throw new IllegalStateException("a round-trip input that is neither a lite project's nor a showcase's: " + line);
            }
            out.computeIfAbsent(key, k -> new ArrayList<>()).add(Runfile.of(line));
        }
        if (out.keySet().stream().noneMatch(k -> k.startsWith("showcase:")) || out.keySet().stream().noneMatch(k -> k.startsWith("projects:"))) {
            throw new IllegalStateException("the declared list holds no showcase or no lite project: " + out.keySet());
        }
        return out;
    }

    // ---------------------------------------------------------------------
    // Reporting
    // ---------------------------------------------------------------------

    private static void report(Tally t, StringBuilder out) {
        String line = "[round-trip] " + t.name + ": matched=" + t.matched + " refused=" + t.refused + " upstream=" + t.upstream
                + " failed=" + t.failed + " engineRefuses=" + t.engineRefuses;
        System.out.println(line);
        out.append(line).append('\n');
        t.refusals.forEach((m, n) -> {
            String r = "[round-trip] " + t.name + " refused " + n + " x " + m;
            System.out.println(r);
            out.append(r).append('\n');
        });
        for (String u : t.upstreamCases) {
            out.append("[round-trip] ").append(t.name).append(" upstream: ").append(u).append('\n');
        }
        t.failures.stream().limit(20).forEach(f -> System.out.println("[round-trip] " + t.name + " FAILED " + f));
        for (String f : t.failures) {
            out.append("[round-trip] ").append(t.name).append(" FAILED ").append(f).append("\n\n");
        }
    }

    private static String abbreviate(String s) {
        return s.length() > 2000 ? s.substring(0, 2000) + "..." : s;
    }
}
