// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.equivalence;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.legend.json.Json;
import com.legend.parser.PmcdParser;
import com.legend.protocol.ModelReader;
import com.legend.protocol.ProtocolEmitter;
import com.legend.protocol.ProtocolReader;
import com.legend.protocol.ProtocolUpgrade;
import com.legend.protocol.SourceInformation;
import com.legend.testing.TestOutputs;
import org.finos.legend.engine.language.pure.grammar.from.PureGrammarParser;
import org.finos.legend.engine.language.pure.grammar.from.domain.DomainParser;
import org.finos.legend.engine.protocol.pure.m3.PackageableElement;
import org.finos.legend.engine.protocol.pure.v1.model.context.PureModelContextData;
import org.finos.legend.engine.shared.core.ObjectMapperFactory;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.Writer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * THE READ LEG'S ORACLE (docs/PROTOCOL_PROGRAM_2026_10_05.md §3): lite's MODEL READER
 * ({@link ModelReader}, protocol JSON to records) is the exact inverse of its emitter
 * ({@link ProtocolEmitter}, records to JSON).
 *
 * <ol>
 *   <li><b>Own JSON.</b> For every corpus source lite parses, J is lite's own document
 *       ({@code PmcdParser.parseDocument}): {@code emit(read(J))} is J byte for byte, element by element and
 *       as a whole, with its source information and without any of it, named spans included
 *       ({@code SourceInformation.stripAll}).</li>
 *   <li><b>The engine's JSON.</b> For every corpus source the engine's parser accepts (with source
 *       information) and every text of upstream's grammar round-trip tests (without), J2 is the engine's
 *       serialized model: where lite's own emit of lite's parse is J2 (lite's emitter already matches the
 *       engine there), {@code emit(read(J2))} is J2. Where it is not, J2 is still read and emitted, and the
 *       outcome is counted apart ("engine-only"): it measures the reader, not the emitter.</li>
 * </ol>
 *
 * <p>Every outcome is counted per element {@code _type}: matched, mismatched (different bytes, or the reader
 * or emitter crashed), refused (the reader named what it has no rule for, an {@link IllegalArgumentException}).
 * Matched counts are pinned up-only, mismatches down-only; the refusals are listed by reason.
 * {@code MODEL_READER_DUMP} set writes every J and J2 element as JSON lines (a work list).
 */
class ModelReaderParityTest {

    /** Own-JSON elements read and emitted to the same bytes, with source information. Up-only. */
    private static final int MIN_OWN_MATCHED = 37739;   // 2026-10-05: every element of the corpus (2 more upgraded)
    /** Own-JSON elements matched without source information. Up-only. */
    private static final int MIN_OWN_STRIPPED_MATCHED = 37739;   // 2026-10-05
    /** Engine-JSON elements matched (where lite's emitter matches the engine). Up-only. */
    private static final int MIN_ENGINE_MATCHED = 38702;   // 2026-10-05 (2 more upgraded); every J2 is lite's J
    /** Whole documents matched (own, with and without source information). Up-only. */
    private static final int MIN_DOCS_MATCHED = 13514;   // 2026-10-05 (4 more upgraded)
    /** Upstream's lambda round-trip texts read and emitted to the engine's bytes, each mode. Up-only. */
    private static final int MIN_LAMBDAS_MATCHED = 205;   // 2026-10-05: every lambda text the engine parses
    /** Mismatches anywhere (own or engine, either mode, elements or documents). Down-only. */
    private static final int MAX_MISMATCHED = 0;
    /** Refusals anywhere: every element _type of the corpus has its reader rule. Down-only. */
    private static final int MAX_REFUSED = 0;

    private static final Json.Config DEEP = new Json.Config(4096);

    private final ObjectMapper mapper = ObjectMapperFactory.getNewStandardObjectMapperWithPureProtocolExtensionSupports();

    /** Per table, per element _type: matched, mismatched, refused. */
    private final Map<String, Map<String, int[]>> tables = new LinkedHashMap<>();
    private final Map<String, Integer> refusals = new TreeMap<>();
    private final List<String> diffs = new ArrayList<>();
    private int mismatched;
    private Writer dump;

    @Test
    void theReaderIsTheEmittersExactInverse() throws Exception {
        if (System.getenv("MODEL_READER_DUMP") != null) {
            dump = Files.newBufferedWriter(TestOutputs.file("model-reader-dump.jsonl"));
        }
        PureGrammarParser oracle = PureGrammarParser.newInstance();
        int liteSources = 0;
        int engineSources = 0;
        for (Corpus.Source src : Corpus.all()) {
            String lite = liteDocument(src.text());
            if (lite != null) {
                liteSources++;
                own(src.id(), lite);
            }
            PureModelContextData parsed;
            try {
                parsed = oracle.parseModel(src.text());
            } catch (Throwable t) {
                continue;   // the engine refuses the source: no J2
            }
            engineSources++;
            engine(src.id(), parsed, lite, false);
        }
        int roundtripTexts = 0;
        for (Path file : ModelComposerParityTest.roundtripTestFiles()) {
            for (String run : InlineSnippets.literalRuns(Files.readString(file))) {
                PureModelContextData parsed;
                try {
                    parsed = oracle.parseModel(run, "", 0, 0, false);
                } catch (Throwable t) {
                    continue;   // not a model text
                }
                if (parsed.getElements().size() <= 1) {
                    continue;
                }
                roundtripTexts++;
                engine("roundtrip:" + file.getFileName(), parsed, liteDocument(run), true);
            }
        }
        // upstream's own LAMBDA round-trip texts (the printer's spec, ComposerParityTest): the engine's
        // lambda JSON read back as lambda records, emitted, with spans and without
        int lambdaTexts = 0;
        for (String rel : ComposerParityTest.ROUNDTRIP_TESTS) {
            for (String run : InlineSnippets.literalRuns(Files.readString(Corpus.engineRoot().resolve(rel)))) {
                String j2;
                try {
                    j2 = mapper.writeValueAsString(new DomainParser().parseLambda(run, "", 0, 0, true));
                } catch (Throwable t) {
                    continue;   // not a lambda (an expected-output text, a message)
                }
                lambdaTexts++;
                judgeLambda("engine-lambdas", rel, j2, false);
                judgeLambda("engine-lambdas-stripped", rel, SourceInformation.stripAll(j2), true);
            }
        }
        if (dump != null) {
            dump.close();
        }
        System.out.println("[model-reader-parity] lambdaTexts=" + lambdaTexts);
        report(liteSources, engineSources, roundtripTexts);
        assertTrue(total("own").matched >= MIN_OWN_MATCHED, "own matched " + total("own").matched + " < " + MIN_OWN_MATCHED);
        assertTrue(total("own-stripped").matched >= MIN_OWN_STRIPPED_MATCHED,
                "own stripped matched " + total("own-stripped").matched + " < " + MIN_OWN_STRIPPED_MATCHED);
        assertTrue(total("engine").matched >= MIN_ENGINE_MATCHED,
                "engine matched " + total("engine").matched + " < " + MIN_ENGINE_MATCHED);
        assertTrue(total("documents").matched >= MIN_DOCS_MATCHED,
                "documents matched " + total("documents").matched + " < " + MIN_DOCS_MATCHED);
        assertTrue(total("engine-lambdas").matched >= MIN_LAMBDAS_MATCHED
                        && total("engine-lambdas-stripped").matched >= MIN_LAMBDAS_MATCHED,
                "lambdas matched " + total("engine-lambdas") + " / " + total("engine-lambdas-stripped") + " < "
                        + MIN_LAMBDAS_MATCHED);
        assertTrue(mismatched <= MAX_MISMATCHED, "mismatched " + mismatched + " > " + MAX_MISMATCHED
                + " -- see model-reader-diffs.txt");
        int refused = refusals.values().stream().mapToInt(Integer::intValue).sum();
        assertTrue(refused <= MAX_REFUSED, "refused " + refused + " > " + MAX_REFUSED + ": " + refusals.keySet());
    }

    // ---------------------------------------------------------------------
    // The two oracles
    // ---------------------------------------------------------------------

    /** Lite's own document: each element, then the whole, with and without source information. */
    private void own(String id, String doc) throws IOException {
        for (String element : elementsOf(doc)) {
            String type = typeOf(element);
            record("lite", id, type, element);
            judge("own", id, type, element, false);
            judge("own-stripped", id, type, SourceInformation.stripAll(element), true);
        }
        judgeDocument(id, doc, false);
        judgeDocument(id + " (stripped)", SourceInformation.stripAll(doc), true);
    }

    /** The engine's model: compared where lite's emit of lite's parse is the same JSON. */
    private void engine(String id, PureModelContextData parsed, String lite, boolean stripped) throws IOException {
        String j2 = mapper.writeValueAsString(parsed);
        boolean liteMatches = lite != null
                && (stripped ? SourceInformation.stripAll(lite).equals(canonical(j2)) : lite.equals(j2));
        String table = liteMatches ? "engine" : "engine-only";
        for (PackageableElement e : parsed.getElements()) {
            String element = mapper.writeValueAsString(e);
            String type = typeOf(element);
            record("engine", id, type, element);
            judge(table, id, type, stripped ? canonical(element) : element, stripped);
        }
    }

    /** One element: read, emit, compare (canonical JSON when stripped). */
    private void judge(String table, String id, String type, String json, boolean stripped) {
        int[] counts = tables.computeIfAbsent(table, k -> new TreeMap<>()).computeIfAbsent(type, k -> new int[4]);
        String emitted;
        try {
            emitted = ProtocolEmitter.emitElement(ModelReader.readElement(json));
        } catch (IllegalArgumentException refused) {
            counts[2]++;
            refusals.merge(table + ": " + type + ": " + refused.getMessage(), 1, Integer::sum);
            return;
        } catch (RuntimeException crash) {
            counts[1]++;
            miss(table, id, type, json, "CRASHED: " + crash);
            return;
        }
        String actual = stripped ? canonical(emitted) : emitted;
        if (actual.equals(json)) {
            counts[0]++;
        } else if (upgradedTo(json, actual)) {
            counts[3]++;
        } else {
            counts[1]++;
            miss(table, id, type, json, actual);
        }
    }

    /**
     * The reader brings older wire forms current on read, as upstream's converters do
     * ({@code ProtocolUpgrade}): where the upgrade changes J, the reader's contract is the UPGRADED model, so
     * {@code emit(read(J))} is compared with {@code upgrade(J)} as a JSON tree (the upgrade builds its nodes in
     * its own key order). Counted apart, as "upgraded".
     */
    private static boolean upgradedTo(String json, String actual) {
        Json.Node original = Json.parse(json, DEEP);
        Json.Node upgraded = ProtocolUpgrade.upgrade(original);
        return !upgraded.equals(original) && upgraded.equals(Json.parse(actual, DEEP));
    }

    /** One lambda: {@code ProtocolReader.lambda}, then {@code ProtocolEmitter.emitLambda}, compared. */
    private void judgeLambda(String table, String id, String json, boolean stripped) {
        int[] counts = tables.computeIfAbsent(table, k -> new TreeMap<>()).computeIfAbsent("lambda", k -> new int[4]);
        String emitted;
        try {
            emitted = ProtocolEmitter.emitLambda(ProtocolReader.lambda(json));
        } catch (IllegalArgumentException refused) {
            counts[2]++;
            refusals.merge(table + ": lambda: " + refused.getMessage(), 1, Integer::sum);
            return;
        } catch (RuntimeException crash) {
            counts[1]++;
            miss(table, id, "lambda", json, "CRASHED: " + crash);
            return;
        }
        String actual = stripped ? canonical(emitted) : emitted;
        if (actual.equals(json)) {
            counts[0]++;
        } else if (upgradedTo(json, actual)) {
            counts[3]++;
        } else {
            counts[1]++;
            miss(table, id, "lambda", json, actual);
        }
    }

    private void judgeDocument(String id, String doc, boolean stripped) {
        int[] counts = tables.computeIfAbsent("documents", k -> new TreeMap<>())
                .computeIfAbsent(stripped ? "stripped" : "kept", k -> new int[4]);
        String emitted;
        try {
            emitted = ProtocolEmitter.emit(ModelReader.read(doc));
        } catch (IllegalArgumentException refused) {
            counts[2]++;
            refusals.merge("documents: " + refused.getMessage(), 1, Integer::sum);
            return;
        } catch (RuntimeException crash) {
            counts[1]++;
            miss("documents", id, "-", doc, "CRASHED: " + crash);
            return;
        }
        String actual = stripped ? canonical(emitted) : emitted;
        if (actual.equals(doc)) {
            counts[0]++;
        } else if (upgradedTo(doc, actual)) {
            counts[3]++;
        } else {
            counts[1]++;
            miss("documents", id, "-", doc, actual);
        }
    }

    private void miss(String table, String id, String type, String expected, String actual) {
        mismatched++;
        if (diffs.size() < 400) {
            diffs.add(table + " " + type + " " + id + "\n" + divergence(expected, actual));
        }
    }

    // ---------------------------------------------------------------------
    // Helpers
    // ---------------------------------------------------------------------

    private static String liteDocument(String text) {
        try {
            return PmcdParser.parseDocument(text);
        } catch (Throwable t) {
            return null;   // lite cannot parse it: no own J
        }
    }

    /**
     * The document's elements, each exactly as written (not a re-emission): {@code doc} is
     * {@code {"_type":"data","elements":[...]}}, split at the top-level commas of its array.
     */
    private static List<String> elementsOf(String doc) {
        List<String> out = new ArrayList<>();
        int start = doc.indexOf('[') + 1;
        if (doc.charAt(start) == ']') {
            return out;
        }
        int depth = 0;
        boolean inString = false;
        for (int i = start; i < doc.length(); i++) {
            char c = doc.charAt(i);
            if (inString) {
                if (c == '\\') {
                    i++;
                } else if (c == '"') {
                    inString = false;
                }
            } else if (c == '"') {
                inString = true;
            } else if (c == '{' || c == '[') {
                depth++;
            } else if (c == '}' || c == ']') {
                if (depth == 0) {
                    out.add(doc.substring(start, i));
                    break;
                }
                depth--;
            } else if (c == ',' && depth == 0) {
                out.add(doc.substring(start, i));
                start = i + 1;
            }
        }
        return out;
    }

    private static String typeOf(String element) {
        return String.valueOf(((Json.Obj) Json.parse(element, DEEP)).getStringOr("_type", "?"));
    }

    private static String canonical(String json) {
        return SourceInformation.stripAll(json);
    }

    private void record(String side, String id, String type, String json) throws IOException {
        if (dump != null) {
            dump.write(Json.toCompact(Json.of(Map.of("side", side, "id", id, "type", type, "json", json))));
            dump.write('\n');
        }
    }

    private static String divergence(String expected, String actual) {
        int n = Math.min(expected.length(), actual.length());
        int i = 0;
        while (i < n && expected.charAt(i) == actual.charAt(i)) {
            i++;
        }
        int from = Math.max(0, i - 120);
        return "  at " + i + "\n  expected ..." + expected.substring(from, Math.min(expected.length(), i + 200))
                + "\n  actual   ..." + actual.substring(from, Math.min(actual.length(), i + 200));
    }

    private record Total(int matched, int mismatched, int refused, int upgraded) {
    }

    private Total total(String table) {
        int[] t = new int[4];
        for (int[] c : tables.getOrDefault(table, Map.of()).values()) {
            for (int i = 0; i < 4; i++) {
                t[i] += c[i];
            }
        }
        return new Total(t[0], t[1], t[2], t[3]);
    }

    private void report(int liteSources, int engineSources, int roundtripTexts) throws IOException {
        StringBuilder out = new StringBuilder("[model-reader-parity] liteSources=" + liteSources + " engineSources="
                + engineSources + " roundtripTexts=" + roundtripTexts + " mismatched=" + mismatched + "\n");
        String row = "%-48s %9s %9s %9s %9s%n";
        tables.forEach((table, byType) -> {
            out.append("[model-reader-parity] TABLE ").append(table).append('\n');
            out.append(String.format(row, "_type", "matched", "mismatch", "refused", "upgraded"));
            byType.forEach((type, c) -> out.append(String.format(row, type, c[0], c[1], c[2], c[3])));
            Total t = total(table);
            out.append(String.format(row, "TOTAL", t.matched(), t.mismatched(), t.refused(), t.upgraded()));
        });
        refusals.forEach((m, n) -> out.append("[model-reader-parity] refused ").append(n).append(" x ").append(m)
                .append('\n'));
        System.out.print(out);
        diffs.stream().limit(40).forEach(d -> System.out.println("[model-reader-parity] DIFF " + d));
        Files.writeString(TestOutputs.file("model-reader-report.txt"), out);
        Files.writeString(TestOutputs.file("model-reader-diffs.txt"), String.join("\n\n", diffs));
    }
}
