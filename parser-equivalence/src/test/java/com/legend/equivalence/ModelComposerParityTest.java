// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.equivalence;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.legend.json.Json;
import com.legend.protocol.ModelComposer;
import com.legend.testing.TestOutputs;
import org.finos.legend.engine.language.pure.grammar.from.PureGrammarParser;
import org.finos.legend.engine.language.pure.grammar.to.PureGrammarComposer;
import org.finos.legend.engine.language.pure.grammar.to.PureGrammarComposerContext;
import org.finos.legend.engine.protocol.pure.m3.PackageableElement;
import org.finos.legend.engine.protocol.pure.v1.model.context.PureModelContextData;
import org.finos.legend.engine.shared.core.ObjectMapperFactory;
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

import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * lite's MODEL printer ({@link ModelComposer}, the engine's {@code jsonToGrammar/model}) against
 * UPSTREAM'S, byte for byte (docs/STUDIO_FULL_PLAN_2026_10_04.md, B1). The oracle is upstream's own
 * {@code PureGrammarComposer} on the same protocol JSON: every source of the reference corpus the
 * engine's parser accepts, plus every text of upstream's own grammar round-trip tests
 * ({@code TestGrammarRoundtrip} subclasses), printed three ways -- the whole model with its section
 * index (a text's own sections), the whole model WITHOUT it (entity JSON from a real SDLC carries
 * none), and each element alone in its section ({@code PureGrammarComposer.render(element, parser)}).
 *
 * <p>Where upstream prints, lite prints the same bytes or REFUSES, naming the {@code _type} it cannot
 * print yet; it never prints something approximate. Where upstream itself cannot print (it throws,
 * or writes its {@code /* Unsupported ...} / {@code /* Can't transform ...} comment) there is nothing to
 * compare. The counts per element {@code _type} are printed as a table; matched counts are pinned
 * up-only, mismatches down-only.
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

    /** A whole model's JSON nests far deeper than one request's default limit. */
    private static final Json.Config DEEP = new Json.Config(4096);

    private final ObjectMapper mapper = ObjectMapperFactory.getNewStandardObjectMapperWithPureProtocolExtensionSupports();
    private final PureGrammarComposer upstream = PureGrammarComposer.newInstance(PureGrammarComposerContext.Builder.newInstance().build());

    /** Per element _type: matched, mismatched, refused, upstream could not print. */
    private final Map<String, int[]> byType = new TreeMap<>();
    private final Map<String, Integer> refusals = new TreeMap<>();
    private final List<String> diffs = new ArrayList<>();
    private final int[] models = new int[5];
    /** Every case lite does not match, as JSON lines, when MODEL_COMPOSER_DUMP is set: a work list. */
    private java.io.Writer dump;
    private int deliberate;

    /**
     * Where lite DELIBERATELY prints differently (ComposerParityTest's EXACT_DECIMAL): a decimal's exact
     * digits, {@code 10.10D} where upstream's reader lost the trailing zero. Counted, not compared.
     */
    private static final String EXACT_DECIMAL_UPSTREAM = "10.1D->divide(";
    private static final String EXACT_DECIMAL_LITE = "10.10D->divide(";
    private static final String EXACT_DECIMAL_JSON = "\"value\":10.10";

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

        int[] total = new int[5];
        // upstream-x: upstream cannot print it (it throws, or writes its unsupported comment); of those,
        // lite-only: lite prints it anyway (nothing to compare it with)
        String row = "%-40s %8s %8s %8s %10s %9s%n";
        StringBuilder table = new StringBuilder(String.format(row, "_type", "matched", "mismatch", "refused", "upstream-x", "lite-only"));
        byType.forEach((type, c) -> {
            table.append(String.format(row, type, c[0], c[1], c[2], c[3], c[4]));
            for (int i = 0; i < 5; i++) {
                total[i] += c[i];
            }
        });
        table.append(String.format(row, "TOTAL", total[0], total[1], total[2], total[3], total[4]));
        System.out.println("[model-composer-parity] sources=" + sources + " roundtripTexts=" + roundtripTexts
                + " models: matched=" + models[0] + " mismatched=" + models[1] + " refused=" + models[2]
                + " upstreamCannot=" + models[3] + " liteOnly=" + models[4]);
        System.out.print(table);
        refusals.forEach((m, n) -> System.out.println("[model-composer-parity] refused " + n + " x " + m));
        diffs.stream().limit(30).forEach(d -> System.out.println("[model-composer-parity] DIFF " + d));
        System.out.println("[model-composer-parity] deliberate (exact decimals) " + deliberate);
        Files.writeString(TestOutputs.file("model-composer-diffs.txt"), String.join("\n\n", diffs));
        if (dump != null) {
            dump.close();
        }
        assertTrue(total[0] >= MIN_ELEMENTS_MATCHED, "elements matched " + total[0] + " < " + MIN_ELEMENTS_MATCHED);
        assertTrue(total[1] <= MAX_ELEMENTS_MISMATCHED, "elements mismatched " + total[1] + " > " + MAX_ELEMENTS_MISMATCHED);
        assertTrue(models[0] >= MIN_MODELS_MATCHED, "models matched " + models[0] + " < " + MIN_MODELS_MATCHED);
        assertTrue(models[1] <= MAX_MODELS_MISMATCHED, "models mismatched " + models[1] + " > " + MAX_MODELS_MISMATCHED);
    }

    /** One model three ways: with its section index, without it, and element by element. */
    private void compare(String id, String json) throws IOException {
        Json.Obj wire = (Json.Obj) Json.parse(json, DEEP);
        compareModel(id, json, wire);
        List<Json.Node> elements = wire.getArr("elements").items();
        List<Json.Node> kept = new ArrayList<>();
        for (Json.Node e : elements) {
            if (!"sectionIndex".equals(((Json.Obj) e).getStringOr("_type", ""))) {
                kept.add(e);
            }
        }
        if (kept.size() != elements.size()) {
            LinkedHashMap<String, Json.Node> f = new LinkedHashMap<>(wire.fields());
            f.put("elements", new Json.Arr(kept));
            Json.Obj stripped = new Json.Obj(f);
            compareModel(id + " (no section index)", Json.toCompact(stripped), stripped);
        }
        PureModelContextData pmcd = mapper.readValue(json, PureModelContextData.class);
        Map<String, java.util.ArrayDeque<String>> parserOf = parserNames(wire);
        for (int i = 0; i < elements.size(); i++) {
            Json.Obj e = (Json.Obj) elements.get(i);
            String type = e.getStringOr("_type", "?");
            if ("sectionIndex".equals(type)) {
                continue;
            }
            PackageableElement element = pmcd.getElements().get(i);
            int[] counts = byType.computeIfAbsent(type, k -> new int[5]);
            java.util.ArrayDeque<String> parsers = parserOf.get(element.getPath());
            String parser = parsers == null || parsers.isEmpty() ? null : parsers.size() == 1 ? parsers.peek() : parsers.poll();
            String elementId = id + " element " + element.getPath();
            String expected;
            try {
                expected = parser == null ? upstream.render(element) : upstream.render(element, parser);
            } catch (Throwable t) {
                upstreamCannot(counts, elementId, Json.toCompact(e), String.valueOf(t), () -> ModelComposer.element(e));
                continue;
            }
            if (cannotPrint(expected)) {
                upstreamCannot(counts, elementId, Json.toCompact(e), expected, () -> ModelComposer.element(e));
                continue;
            }
            counts[judge(elementId, Json.toCompact(e), expected, () -> ModelComposer.element(e))]++;
        }
    }

    private void compareModel(String id, String json, Json.Obj wire) throws IOException {
        String expected;
        try {
            expected = upstream.renderPureModelContextData(mapper.readValue(json, PureModelContextData.class));
        } catch (Throwable t) {
            upstreamCannot(models, id, json, String.valueOf(t), () -> ModelComposer.model(wire));
            return;
        }
        if (cannotPrint(expected)) {
            upstreamCannot(models, id, json, expected, () -> ModelComposer.model(wire));
            return;
        }
        models[judge(id, json, expected, () -> ModelComposer.model(wire))]++;
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
    private int judge(String id, String json, String expected, java.util.function.Supplier<String> lite) {
        String actual;
        try {
            actual = lite.get();
        } catch (IllegalArgumentException e) {
            refusals.merge(String.valueOf(e.getMessage()), 1, Integer::sum);
            record(id, json, expected);
            return 2;
        } catch (RuntimeException e) {
            diffs.add(id + "\n  lite CRASHED: " + e + "\n  json: " + abbreviate(json));
            record(id, json, expected);
            return 1;
        }
        if (expected.equals(actual)) {
            return 0;
        }
        // only where the JSON holds the exact 10.10: there lite's 10.10D is the value, and upstream's 10.1D the loss
        if (json.contains(EXACT_DECIMAL_JSON) && actual.equals(expected.replace(EXACT_DECIMAL_UPSTREAM, EXACT_DECIMAL_LITE))) {
            deliberate++;
            return 0;
        }
        record(id, json, expected);
        diffs.add(id + "\n--- upstream\n" + expected + "\n--- lite\n" + actual + "\n--- json\n" + abbreviate(json));
        return 1;
    }

    private void record(String id, String json, String expected) {
        if (dump == null) {
            return;
        }
        try {
            dump.write(Json.toCompact(Json.of(Map.of("id", id, "json", json, "expected", expected))));
            dump.write('\n');
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
