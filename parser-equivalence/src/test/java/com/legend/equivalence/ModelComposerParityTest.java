// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.equivalence;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.legend.json.Json;
import com.legend.protocol.ModelComposer;
import com.legend.protocol.ModelReader;
import com.legend.protocol.PureComposer;
import com.legend.testing.TestOutputs;
import org.finos.legend.engine.language.pure.grammar.from.PureGrammarParser;
import org.finos.legend.engine.language.pure.grammar.to.PureGrammarComposer;
import org.finos.legend.engine.language.pure.grammar.to.PureGrammarComposerContext;
import org.finos.legend.engine.protocol.pure.m3.PackageableElement;
import org.finos.legend.engine.protocol.pure.v1.model.context.PureModelContextData;
import org.finos.legend.engine.shared.core.ObjectMapperFactory;
import org.finos.legend.engine.shared.core.api.grammar.RenderStyle;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * lite's MODEL printer ({@link ModelComposer}, the engine's {@code jsonToGrammar/model}) against
 * UPSTREAM'S, byte for byte (docs/STUDIO_FULL_PLAN_2026_10_04.md, B1). The oracle is upstream's own
 * {@code PureGrammarComposer} on the same protocol JSON: every source of the reference corpus the
 * engine's parser accepts, plus every text of upstream's own grammar round-trip tests
 * ({@code TestGrammarRoundtrip} subclasses), printed three ways -- the whole model with its section
 * index (a text's own sections), the whole model WITHOUT it (entity JSON from a real SDLC carries
 * none), and each element alone in its section ({@code PureGrammarComposer.render(element, parser)}).
 * Each is printed in both render styles: STANDARD (the composer's own default) and PRETTY
 * ({@code jsonToGrammar/model}'s default; the protocol program's leg 4).
 *
 * <p>Where upstream prints, lite prints the same bytes or REFUSES, naming the {@code _type} it cannot
 * print yet; it never prints something approximate. Where upstream itself cannot print (it throws,
 * or writes its {@code /* Unsupported ...} / {@code /* Can't transform ...} comment) there is nothing to
 * compare. The counts per element {@code _type} are printed as a table, one per style; matched counts are
 * pinned up-only, mismatches down-only.
 */
class ModelComposerParityTest {

    /** Elements printed alone, byte-equal. Up-only. */
    private static final int MIN_ELEMENTS_MATCHED = 31452;   // 2026-10-05, B1: every element kind of the corpus
    /** Elements printed alone, different bytes. Down-only. */
    private static final int MAX_ELEMENTS_MISMATCHED = 0;
    /** Whole models (with and without the section index), byte-equal. Up-only. */
    private static final int MIN_MODELS_MATCHED = 14383;   // 2026-10-05, B1
    /** Whole models, different bytes. Down-only. */
    private static final int MAX_MODELS_MISMATCHED = 0;

    /** The same four, in PRETTY. */
    private static final int MIN_PRETTY_ELEMENTS_MATCHED = 31452;   // 2026-10-09, the protocol program's leg 4
    private static final int MAX_PRETTY_ELEMENTS_MISMATCHED = 0;
    private static final int MIN_PRETTY_MODELS_MATCHED = 14383;   // 2026-10-09, leg 4
    private static final int MAX_PRETTY_MODELS_MISMATCHED = 0;

    /** Prints (elements and whole models) where lite keeps a lambda's braces that upstream's dropping changes the
     *  reading of, per style (docs/SEMANTICS_REGISTER.md S38; {@link #keepsBracesUpstreamDrops}). Counted among the
     *  matched, as the exact decimals are, and pinned both ways. 2026-10-09, the protocol program's leg 5: 442 for a
     *  sequence's first statement, 454 with receivers, operators and a lone column spec's lambda, 475 with a lambda
     *  whose one statement is a parameterless lambda (upstream's ||, which does not parse) -- the same day. */
    private static final int BRACES_KEPT = 475;

    /** Prints where lite writes a typed column spec's multiplicity, which upstream's print leaves out (S39), per style;
     *  counted among the matched and pinned both ways. 2026-10-09, the protocol program's leg 5. */
    private static final int MULTIPLICITIES_KEPT = 24;

    /** A whole model's JSON nests far deeper than one request's default limit. */
    private static final Json.Config DEEP = new Json.Config(4096);

    private final ObjectMapper mapper = ObjectMapperFactory.getNewStandardObjectMapperWithPureProtocolExtensionSupports();

    private final Pass standard = new Pass(RenderStyle.STANDARD, PureComposer.Style.STANDARD);
    private final Pass pretty = new Pass(RenderStyle.PRETTY, PureComposer.Style.PRETTY);

    /** Every case lite does not match, as JSON lines, when MODEL_COMPOSER_DUMP is set: a work list. */
    private java.io.Writer dump;

    /**
     * Where lite DELIBERATELY prints differently (ComposerParityTest's EXACT_DECIMAL): a decimal's exact
     * digits, {@code 10.10D} where upstream's reader lost the trailing zero. Counted, not compared.
     */
    private static final String EXACT_DECIMAL_UPSTREAM = "10.1D->divide(";
    private static final String EXACT_DECIMAL_LITE = "10.10D->divide(";
    private static final String EXACT_DECIMAL_JSON = "\"value\":10.10";

    /** Upstream's parser: which prints read alike ({@link #keepsBracesUpstreamDrops}). */
    private final PureGrammarParser parser = PureGrammarParser.newInstance();

    /**
     * Where lite DELIBERATELY prints differently (docs/SEMANTICS_REGISTER.md, "a lambda's braces where dropping them
     * changes the reading"; the protocol program's leg 5, the user's choice, 2026-10-09): lite's print is upstream's
     * with braces added, nothing else; upstream's own parser reads the two prints differently, or cannot read
     * upstream's at all ({@code ||}, a lambda's body opening with a parameterless lambda, lexes as the or operator) --
     * a brace-less lambda takes in what follows it -- and reads lite's with the statement boundaries of the JSON
     * printed ({@link #statementShapes}): the braces keep each lambda whole where it was. (The whole reading need not
     * equal the JSON printed: an element can carry another of upstream's non-round-trips, such as {@code a + 2 == b}
     * reading back as {@code a + (2 == b)}; those move no lambda.) Counted, not compared.
     */
    private int keepsBracesUpstreamDrops(String json, String expected, String actual) {
        int kinds = added(expected, actual);
        if (kinds == 0) {
            return 0;
        }
        String liteReads;
        try {
            liteReads = mapper.writeValueAsString(parser.parseModel(actual, "", 0, 0, false));
        } catch (RuntimeException | IOException e) {
            return 0;
        }
        String upstreamReads;
        try {
            upstreamReads = mapper.writeValueAsString(parser.parseModel(expected, "", 0, 0, false));
        } catch (RuntimeException | IOException e) {
            upstreamReads = null;   // upstream's print does not parse
        }
        // a multiplicity counted as kept must be one upstream's print lost: its reading's column multiplicities differ
        boolean multiplicitiesLost = (kinds & MULTIPLICITIES) == 0 || upstreamReads == null
                || !columnMultiplicities(upstreamReads).equals(columnMultiplicities(json));
        return !liteReads.equals(upstreamReads) && multiplicitiesLost
                && statementShapes(liteReads).equals(statementShapes(json))
                && columnMultiplicities(liteReads).equals(columnMultiplicities(json)) ? kinds : 0;
    }

    /** Braces lite added ({@link #keepsBracesUpstreamDrops}'s kinds): S38. */
    static final int BRACES = 1;
    /** A typed column spec's multiplicity lite added: S39. */
    static final int MULTIPLICITIES = 2;

    /**
     * What {@code actual} adds to {@code expected}, nothing else changed: braces ({@link #BRACES}), whole multiplicity
     * segments ({@code [1]}, {@code [*]}, {@code [0..1]}: {@link #MULTIPLICITIES}), or both; 0 when it is not that.
     */
    static int added(String expected, String actual) {
        int i = 0;
        int kinds = 0;
        int k = 0;
        while (k < actual.length()) {
            char c = actual.charAt(k);
            if (i < expected.length() && expected.charAt(i) == c) {
                i++;
                k++;
            } else if (c == '{' || c == '}') {
                kinds |= BRACES;
                k++;
            } else if (c == '[' && actual.indexOf(']', k) > k
                    && actual.substring(k + 1, actual.indexOf(']', k)).matches("[0-9]+|\\*|[0-9]+\\.\\.([0-9]+|\\*)")) {
                kinds |= MULTIPLICITIES;
                k = actual.indexOf(']', k) + 1;
            } else {
                return 0;
            }
        }
        return i == expected.length() ? kinds : 0;
    }

    /** Every column spec's declared multiplicity, depth first, by element (as {@link #statementShapes}): what S39's
     *  printed multiplicities must read back as. */
    static List<String> columnMultiplicities(String json) {
        Json.Obj o = (Json.Obj) Json.parse(json, DEEP);
        List<Json.Node> elements = o.fields().containsKey("elements") ? o.getArr("elements").items() : List.of(o);
        List<String> out = new ArrayList<>();
        for (Json.Node e : elements) {
            Json.Obj element = (Json.Obj) e;
            if (!"sectionIndex".equals(element.getStringOr("_type", ""))) {
                List<String> found = new ArrayList<>();
                columnMultiplicities(element, found);
                out.add(element.getStringOr("package", "") + "::" + element.getStringOr("name", "") + " " + found);
            }
        }
        out.sort(String::compareTo);
        return out;
    }

    private static void columnMultiplicities(Json.Node node, List<String> out) {
        if (node instanceof Json.Obj o) {
            if ("classInstance".equals(o.getStringOr("_type", "")) && o.fields().get("value") instanceof Json.Obj value) {
                String type = o.getStringOr("type", "");
                if ("colSpec".equals(type)) {
                    out.add(String.valueOf(value.fields().get("multiplicity")));
                } else if ("colSpecArray".equals(type) && value.fields().get("colSpecs") instanceof Json.Arr specs) {
                    for (Json.Node spec : specs.items()) {
                        out.add(String.valueOf(((Json.Obj) spec).fields().get("multiplicity")));
                    }
                }
            }
            for (Json.Node f : o.fields().values()) {
                columnMultiplicities(f, out);
            }
        } else if (node instanceof Json.Arr a) {
            for (Json.Node item : a.items()) {
                columnMultiplicities(item, out);
            }
        }
    }

    /**
     * Each element's statement boundaries: every statement sequence in it (each {@code body}: a function's, a derived
     * property's, a lambda's) by where it sits -- its path from the element, keys and indices -- and its length; by
     * element path, the section index aside, sorted. A lambda the braces kept whole sits where the JSON printed has
     * it, at its length; one that took in what followed it sits elsewhere or runs longer. {@code json} is a model or
     * one element.
     */
    static List<String> statementShapes(String json) {
        Json.Obj o = (Json.Obj) Json.parse(json, DEEP);
        List<Json.Node> elements = o.fields().containsKey("elements") ? o.getArr("elements").items() : List.of(o);
        List<String> out = new ArrayList<>();
        for (Json.Node e : elements) {
            Json.Obj element = (Json.Obj) e;
            if (!"sectionIndex".equals(element.getStringOr("_type", ""))) {
                List<String> bodies = new ArrayList<>();
                bodies(element, "", bodies);
                bodies.sort(String::compareTo);
                out.add(element.getStringOr("package", "") + "::" + element.getStringOr("name", "") + " " + bodies);
            }
        }
        out.sort(String::compareTo);
        return out;
    }

    private static void bodies(Json.Node node, String path, List<String> out) {
        if (node instanceof Json.Obj o) {
            for (Map.Entry<String, Json.Node> f : o.fields().entrySet()) {
                String at = path + "." + f.getKey();
                if ("body".equals(f.getKey()) && f.getValue() instanceof Json.Arr body) {
                    out.add(at + "=" + body.items().size());
                }
                bodies(f.getValue(), at, out);
            }
        } else if (node instanceof Json.Arr a) {
            for (int i = 0; i < a.items().size(); i++) {
                bodies(a.items().get(i), path + "[" + i + "]", out);
            }
        }
    }

    /** Whether {@code actual} is {@code expected} with '{' and '}' added, and nothing else. */
    static boolean onlyBracesAdded(String expected, String actual) {
        int i = 0;
        for (int k = 0; k < actual.length(); k++) {
            char c = actual.charAt(k);
            if (i < expected.length() && expected.charAt(i) == c) {
                i++;
            } else if (c != '{' && c != '}') {
                return false;
            }
        }
        return i == expected.length() && actual.length() > expected.length();
    }

    /** One render style's comparison: upstream's composer in that style, lite's printer in it, and the counts. */
    private final class Pass {
        final RenderStyle style;
        final PureComposer.Style lite;
        final PureGrammarComposer upstream;
        /** Per element _type: matched, mismatched, refused, upstream could not print, lite-only. */
        final Map<String, int[]> byType = new TreeMap<>();
        final Map<String, Integer> refusals = new TreeMap<>();
        final List<String> diffs = new ArrayList<>();
        final int[] models = new int[5];
        final int[] total = new int[5];
        int deliberate;
        int bracesKept;
        int multiplicitiesKept;

        Pass(RenderStyle style, PureComposer.Style lite) {
            this.style = style;
            this.lite = lite;
            this.upstream = PureGrammarComposer.newInstance(PureGrammarComposerContext.Builder.newInstance()
                    .withRenderStyle(style).build());
        }

        String tag() {
            return "[model-composer-parity " + style + "]";
        }
    }

    @Test
    void litePrintsEveryModelAsUpstreamDoes() throws Exception {
        if (System.getenv("MODEL_COMPOSER_DUMP") != null) {
            dump = Files.newBufferedWriter(TestOutputs.file("model-composer-dump.jsonl"));
        }
        PureGrammarParser oracle = PureGrammarParser.newInstance();
        int sources = 0;
        for (Corpus.Source src : Corpus.all()) {
            PureModelContextData parsed;
            try {
                parsed = oracle.parseModel(src.text());
            } catch (Throwable t) {
                continue;   // the oracle refuses the source: nothing to print
            }
            sources++;
            compare(src.id(), mapper.writeValueAsString(parsed));
        }
        int roundtripTexts = 0;
        for (Path file : roundtripTestFiles()) {
            for (String run : InlineSnippets.literalRuns(Files.readString(file))) {
                PureModelContextData parsed;
                try {
                    parsed = oracle.parseModel(run, "", 0, 0, false);
                } catch (Throwable t) {
                    continue;   // not a model text (an expected message, a JSON resource name)
                }
                if (parsed.getElements().size() <= 1) {
                    continue;   // only the section index: nothing to print
                }
                roundtripTexts++;
                compare("roundtrip:" + file.getFileName(), mapper.writeValueAsString(parsed));
            }
        }

        List<String> allDiffs = new ArrayList<>();
        for (Pass p : List.of(standard, pretty)) {
            report(p, sources, roundtripTexts);
            allDiffs.addAll(p.diffs);
        }
        Files.writeString(TestOutputs.file("model-composer-diffs.txt"), String.join("\n\n", allDiffs));
        if (dump != null) {
            dump.close();
        }
        pin(standard, MIN_ELEMENTS_MATCHED, MAX_ELEMENTS_MISMATCHED, MIN_MODELS_MATCHED, MAX_MODELS_MISMATCHED);
        pin(pretty, MIN_PRETTY_ELEMENTS_MATCHED, MAX_PRETTY_ELEMENTS_MISMATCHED, MIN_PRETTY_MODELS_MATCHED,
                MAX_PRETTY_MODELS_MISMATCHED);
        for (Pass p : List.of(standard, pretty)) {
            assertEquals(BRACES_KEPT, p.bracesKept, p.style + ": the prints where lite keeps braces upstream drops (S38) moved");
            assertEquals(MULTIPLICITIES_KEPT, p.multiplicitiesKept,
                    p.style + ": the prints where lite keeps a column spec's multiplicity upstream drops (S39) moved");
        }
    }

    private void report(Pass p, int sources, int roundtripTexts) {
        // upstream-x: upstream cannot print it (it throws, or writes its unsupported comment); of those,
        // lite-only: lite prints it anyway (nothing to compare it with)
        String row = "%-40s %8s %8s %8s %10s %9s%n";
        StringBuilder table = new StringBuilder(String.format(row, "_type", "matched", "mismatch", "refused", "upstream-x", "lite-only"));
        p.byType.forEach((type, c) -> {
            table.append(String.format(row, type, c[0], c[1], c[2], c[3], c[4]));
            for (int i = 0; i < 5; i++) {
                p.total[i] += c[i];
            }
        });
        table.append(String.format(row, "TOTAL", p.total[0], p.total[1], p.total[2], p.total[3], p.total[4]));
        System.out.println(p.tag() + " sources=" + sources + " roundtripTexts=" + roundtripTexts
                + " models: matched=" + p.models[0] + " mismatched=" + p.models[1] + " refused=" + p.models[2]
                + " upstreamCannot=" + p.models[3] + " liteOnly=" + p.models[4]);
        System.out.print(table);
        p.refusals.forEach((m, n) -> System.out.println(p.tag() + " refused " + n + " x " + m));
        p.diffs.stream().limit(30).forEach(d -> System.out.println(p.tag() + " DIFF " + d));
        System.out.println(p.tag() + " deliberate (exact decimals) " + p.deliberate);
        System.out.println(p.tag() + " deliberate (braces kept where upstream's dropping them changes the reading) "
                + p.bracesKept);
        System.out.println(p.tag() + " deliberate (a column spec's multiplicity kept, which upstream's print drops) "
                + p.multiplicitiesKept);
    }

    private static void pin(Pass p, int minElements, int maxElementsMismatched, int minModels, int maxModelsMismatched) {
        assertTrue(p.total[0] >= minElements, p.style + ": elements matched " + p.total[0] + " < " + minElements);
        assertTrue(p.total[1] <= maxElementsMismatched,
                p.style + ": elements mismatched " + p.total[1] + " > " + maxElementsMismatched);
        assertTrue(p.models[0] >= minModels, p.style + ": models matched " + p.models[0] + " < " + minModels);
        assertTrue(p.models[1] <= maxModelsMismatched,
                p.style + ": models mismatched " + p.models[1] + " > " + maxModelsMismatched);
    }

    /** One model three ways -- with its section index, without it, and element by element -- in each style. */
    private void compare(String id, String json) throws IOException {
        Json.Obj wire = (Json.Obj) Json.parse(json, DEEP);
        PureModelContextData pmcd = mapper.readValue(json, PureModelContextData.class);
        List<Json.Node> elements = wire.getArr("elements").items();
        List<Json.Node> kept = new ArrayList<>();
        for (Json.Node e : elements) {
            if (!"sectionIndex".equals(((Json.Obj) e).getStringOr("_type", ""))) {
                kept.add(e);
            }
        }
        Json.Obj stripped = null;
        PureModelContextData strippedPmcd = null;
        if (kept.size() != elements.size()) {
            LinkedHashMap<String, Json.Node> f = new LinkedHashMap<>(wire.fields());
            f.put("elements", new Json.Arr(kept));
            stripped = new Json.Obj(f);
            strippedPmcd = mapper.readValue(Json.toCompact(stripped), PureModelContextData.class);
        }
        Map<String, java.util.ArrayDeque<String>> parserOf = parserNames(wire);
        for (Pass p : List.of(standard, pretty)) {
            compareModel(p, id, json, wire, pmcd);
            if (stripped != null && strippedPmcd != null) {
                compareModel(p, id + " (no section index)", Json.toCompact(stripped), stripped, strippedPmcd);
            }
            Map<String, java.util.ArrayDeque<String>> parsers = new LinkedHashMap<>();
            parserOf.forEach((path, names) -> parsers.put(path, new java.util.ArrayDeque<>(names)));
            for (int i = 0; i < elements.size(); i++) {
                Json.Obj e = (Json.Obj) elements.get(i);
                String type = e.getStringOr("_type", "?");
                if ("sectionIndex".equals(type)) {
                    continue;
                }
                PackageableElement element = pmcd.getElements().get(i);
                int[] counts = p.byType.computeIfAbsent(type, k -> new int[5]);
                java.util.ArrayDeque<String> names = parsers.get(element.getPath());
                String parser = names == null || names.isEmpty() ? null : names.size() == 1 ? names.peek() : names.poll();
                String elementId = id + " element " + element.getPath();
                java.util.function.Supplier<String> lite = () -> ModelComposer.element(ModelReader.readElement(e), p.lite);
                String expected;
                try {
                    expected = parser == null ? p.upstream.render(element) : p.upstream.render(element, parser);
                } catch (Throwable t) {
                    upstreamCannot(counts, elementId, Json.toCompact(e), String.valueOf(t), lite);
                    continue;
                }
                if (cannotPrint(expected)) {
                    upstreamCannot(counts, elementId, Json.toCompact(e), expected, lite);
                    continue;
                }
                counts[judge(p, elementId, Json.toCompact(e), expected, lite)]++;
            }
        }
    }

    private void compareModel(Pass p, String id, String json, Json.Obj wire, PureModelContextData pmcd) {
        java.util.function.Supplier<String> lite = () -> ModelComposer.model(ModelReader.read(wire), p.lite);
        String expected;
        try {
            expected = p.upstream.renderPureModelContextData(pmcd);
        } catch (Throwable t) {
            upstreamCannot(p.models, id, json, String.valueOf(t), lite);
            return;
        }
        if (cannotPrint(expected)) {
            upstreamCannot(p.models, id, json, expected, lite);
            return;
        }
        p.models[judge(p, id, json, expected, lite)]++;
    }

    /** Upstream cannot print it: counted, and whether lite prints it anyway is counted too (and dumped). */
    private void upstreamCannot(int[] counts, String id, String json, String why, java.util.function.Supplier<String> lite) {
        counts[3]++;
        try {
            String printed = lite.get();
            record(id, json, "UPSTREAM CANNOT PRINT (lite printed " + printed.length() + " chars): " + (why.length() > 500 ? why.substring(0, 500) : why));
        } catch (RuntimeException e) {
            return;   // lite does not print it either
        }
        counts[4]++;
    }

    /** 0 matched, 1 mismatched, 2 refused. */
    private int judge(Pass p, String id, String json, String expected, java.util.function.Supplier<String> lite) {
        String actual;
        try {
            actual = lite.get();
        } catch (IllegalArgumentException e) {
            p.refusals.merge(String.valueOf(e.getMessage()), 1, Integer::sum);
            record(p.style + " " + id, json, expected);
            return 2;
        } catch (RuntimeException e) {
            p.diffs.add(p.style + " " + id + "\n  lite CRASHED: " + e + "\n  json: " + abbreviate(json));
            record(p.style + " " + id, json, expected);
            return 1;
        }
        if (expected.equals(actual)) {
            return 0;
        }
        // only where the JSON holds the exact 10.10: there lite's 10.10D is the value, and upstream's 10.1D the loss
        if (json.contains(EXACT_DECIMAL_JSON) && actual.equals(expected.replace(EXACT_DECIMAL_UPSTREAM, EXACT_DECIMAL_LITE))) {
            p.deliberate++;
            return 0;
        }
        int kept = keepsBracesUpstreamDrops(json, expected, actual);
        if (kept != 0) {
            p.bracesKept += (kept & BRACES) != 0 ? 1 : 0;
            p.multiplicitiesKept += (kept & MULTIPLICITIES) != 0 ? 1 : 0;
            return 0;
        }
        record(p.style + " " + id, json, expected);
        p.diffs.add(p.style + " " + id + "\n--- upstream\n" + expected + "\n--- lite\n" + actual + "\n--- json\n" + abbreviate(json));
        return 1;
    }

    private void record(String id, String json, String expected) {
        java.io.Writer w = dump;
        if (w == null) {
            return;
        }
        try {
            w.write(Json.toCompact(Json.of(Map.of("id", id, "json", json, "expected", expected))));
            w.write('\n');
        } catch (IOException e) {
            throw new java.io.UncheckedIOException(e);
        }
    }

    /** Upstream's own marks of an element it cannot print: there is nothing to compare. */
    private static boolean cannotPrint(String text) {
        return text.contains("/* Unsupported transformation") || text.contains("/* Can't transform element")
                || text.contains(" composers for the Element ");
    }

    /**
     * Each path's section parsers, in section order, from the model's section index: a source may declare
     * one path twice in two sections (a class and a diagram both named {@code anything::class}), and the
     * elements come in the same order, so each element takes the next one.
     */
    private static Map<String, java.util.ArrayDeque<String>> parserNames(Json.Obj wire) {
        Map<String, java.util.ArrayDeque<String>> out = new LinkedHashMap<>();
        for (Json.Node n : wire.getArr("elements").items()) {
            Json.Obj e = (Json.Obj) n;
            if (!"sectionIndex".equals(e.getStringOr("_type", ""))) {
                continue;
            }
            for (Json.Node s : e.getArr("sections").items()) {
                Json.Obj section = (Json.Obj) s;
                for (String path : section.getStringArrayOr("elements", List.of())) {
                    out.computeIfAbsent(path, k -> new java.util.ArrayDeque<>()).add(section.getString("parserName"));
                }
            }
        }
        return out;
    }

    /** Every grammar round-trip test of the reference checkout: their texts are the printer's spec (and the
     *  model reader's, ModelReaderParityTest). */
    static List<Path> roundtripTestFiles() throws IOException {
        Path root = Corpus.engineRoot();
        try (Stream<Path> s = Files.walk(root)) {
            return s.filter(p -> p.toString().endsWith(".java"))
                    .filter(p -> Corpus.within(root, p).contains("/src/test/"))
                    .filter(ModelComposerParityTest::extendsRoundtrip)
                    .sorted(java.util.Comparator.comparing(Corpus::slashed))
                    .toList();
        }
    }

    private static boolean extendsRoundtrip(Path p) {
        try {
            return Files.readString(p).contains("extends TestGrammarRoundtrip");
        } catch (IOException e) {
            throw new java.io.UncheckedIOException(e);
        }
    }

    private static String abbreviate(String s) {
        return s.length() > 3000 ? s.substring(0, 3000) + "..." : s;
    }
}
