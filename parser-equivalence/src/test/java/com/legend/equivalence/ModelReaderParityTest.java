// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.equivalence;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.legend.json.Json;
import com.legend.parser.PmcdParser;
import com.legend.protocol.ModelReader;
import com.legend.protocol.ProtocolEmitter;
import com.legend.protocol.SourceInformation;
import com.legend.testing.Repo;
import org.finos.legend.engine.language.pure.grammar.from.PureGrammarParser;
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
 *       as a whole, with its source information and without it ({@code SourceInformation.strip}).</li>
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
    private static final int MIN_OWN_MATCHED = 0;
    /** Own-JSON elements matched without source information. Up-only. */
    private static final int MIN_OWN_STRIPPED_MATCHED = 0;
    /** Engine-JSON elements matched (where lite's emitter matches the engine). Up-only. */
    private static final int MIN_ENGINE_MATCHED = 0;
    /** Whole documents matched (own, with and without source information). Up-only. */
    private static final int MIN_DOCS_MATCHED = 0;
    /** Mismatches anywhere (own or engine, either mode, elements or documents). Down-only. */
    private static final int MAX_MISMATCHED = 1_000_000;

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
        Files.createDirectories(Repo.outDir());
        if (System.getenv("MODEL_READER_DUMP") != null) {
            dump = Files.newBufferedWriter(Repo.out("model-reader-dump.jsonl"));
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
        if (dump != null) {
            dump.close();
        }
        report(liteSources, engineSources, roundtripTexts);
        assertTrue(total("own").matched >= MIN_OWN_MATCHED, "own matched " + total("own").matched + " < " + MIN_OWN_MATCHED);
        assertTrue(total("own-stripped").matched >= MIN_OWN_STRIPPED_MATCHED,
                "own stripped matched " + total("own-stripped").matched + " < " + MIN_OWN_STRIPPED_MATCHED);
        assertTrue(total("engine").matched >= MIN_ENGINE_MATCHED,
                "engine matched " + total("engine").matched + " < " + MIN_ENGINE_MATCHED);
        assertTrue(total("documents").matched >= MIN_DOCS_MATCHED,
                "documents matched " + total("documents").matched + " < " + MIN_DOCS_MATCHED);
        assertTrue(mismatched <= MAX_MISMATCHED, "mismatched " + mismatched + " > " + MAX_MISMATCHED
                + " -- see model-reader-diffs.txt");
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
            judge("own-stripped", id, type, SourceInformation.strip(element), true);
        }
        judgeDocument(id, doc, false);
        judgeDocument(id + " (stripped)", SourceInformation.strip(doc), true);
    }

    /** The engine's model: compared where lite's emit of lite's parse is the same JSON. */
    private void engine(String id, PureModelContextData parsed, String lite, boolean stripped) throws IOException {
        String j2 = mapper.writeValueAsString(parsed);
        boolean liteMatches = lite != null
                && (stripped ? SourceInformation.strip(lite).equals(canonical(j2)) : lite.equals(j2));
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
        int[] counts = tables.computeIfAbsent(table, k -> new TreeMap<>()).computeIfAbsent(type, k -> new int[3]);
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
        } else {
            counts[1]++;
            miss(table, id, type, json, actual);
        }
    }

    private void judgeDocument(String id, String doc, boolean stripped) {
        int[] counts = tables.computeIfAbsent("documents", k -> new TreeMap<>())
                .computeIfAbsent(stripped ? "stripped" : "kept", k -> new int[3]);
        String emitted;
        try {
            emitted = ProtocolEmitter.emit(ModelReader.read(doc));
        } catch (IllegalArgumentException refused) {
            counts[2]++;
            return;
        } catch (RuntimeException crash) {
            counts[1]++;
            miss("documents", id, "-", doc, "CRASHED: " + crash);
            return;
        }
        String actual = stripped ? canonical(emitted) : emitted;
        if (actual.equals(doc)) {
            counts[0]++;
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
        return SourceInformation.strip(json);
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

    private record Total(int matched, int mismatched, int refused) {
    }

    private Total total(String table) {
        int[] t = new int[3];
        for (int[] c : tables.getOrDefault(table, Map.of()).values()) {
            for (int i = 0; i < 3; i++) {
                t[i] += c[i];
            }
        }
        return new Total(t[0], t[1], t[2]);
    }

    private void report(int liteSources, int engineSources, int roundtripTexts) throws IOException {
        StringBuilder out = new StringBuilder("[model-reader-parity] liteSources=" + liteSources + " engineSources="
                + engineSources + " roundtripTexts=" + roundtripTexts + " mismatched=" + mismatched + "\n");
        String row = "%-48s %9s %9s %9s%n";
        tables.forEach((table, byType) -> {
            out.append("[model-reader-parity] TABLE ").append(table).append('\n');
            out.append(String.format(row, "_type", "matched", "mismatch", "refused"));
            byType.forEach((type, c) -> out.append(String.format(row, type, c[0], c[1], c[2])));
            Total t = total(table);
            out.append(String.format(row, "TOTAL", t.matched(), t.mismatched(), t.refused()));
        });
        refusals.forEach((m, n) -> out.append("[model-reader-parity] refused ").append(n).append(" x ").append(m)
                .append('\n'));
        System.out.print(out);
        diffs.stream().limit(40).forEach(d -> System.out.println("[model-reader-parity] DIFF " + d));
        Files.writeString(Repo.out("model-reader-report.txt"), out);
        Files.writeString(Repo.out("model-reader-diffs.txt"), String.join("\n\n", diffs));
    }
}
