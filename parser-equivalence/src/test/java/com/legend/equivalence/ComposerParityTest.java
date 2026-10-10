// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.equivalence;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.legend.json.Json;
import com.legend.protocol.PureComposer;
import org.finos.legend.engine.language.pure.grammar.from.PureGrammarParser;
import org.finos.legend.engine.language.pure.grammar.from.domain.DomainParser;
import org.finos.legend.engine.language.pure.grammar.to.DEPRECATED_PureGrammarComposerCore;
import org.finos.legend.engine.protocol.pure.m3.function.LambdaFunction;
import org.finos.legend.engine.shared.core.ObjectMapperFactory;
import org.finos.legend.engine.shared.core.api.grammar.RenderStyle;
import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * lite's printer ({@link PureComposer}, served as {@code pure/v1/grammar/jsonToGrammar/lambda})
 * against UPSTREAM'S printer, byte for byte, in both styles (docs/DATACUBE_TYPES_TO_SERVER_2026_09_27.md,
 * T4a). The oracle is upstream's own composer on the same JSON: every lambda of every source the
 * reference corpus's oracle accepts (a function body, a derived property, a mapping transform, a
 * service query, a constraint ...), plus every text in upstream's own lambda round-trip tests.
 * Where upstream prints, lite must print the same bytes; where lite refuses, it names the node.
 */
class ComposerParityTest {

    /** Lambdas both printers print, byte-equal, summed over both styles. Up-only. */
    private static final int MIN_MATCHED = 56968;   // 2026-09-28: 56,990 less the 2 prints of EXACT_DECIMAL below (the user chose exact decimals); upstream threw on 2. 2026-10-09: less the 20 prints of BRACES_KEPT (the user chose S38, the protocol program's leg 5; 18 of them upstream's unparseable ||)

    /** The prints where lite keeps a lambda's braces that upstream's dropping changes the reading of
     *  (docs/SEMANTICS_REGISTER.md S38; {@link #keepsBracesUpstreamDrops}). Pinned both ways, as EXACT_DECIMAL is. */
    private static final int BRACES_KEPT = 20;

    /**
     * Where lite DELIBERATELY prints differently: a decimal's exact digits. Upstream's reader
     * ({@code CDecimal.CDecimalDeserializer}) reads the value through a Jackson tree node, where a JSON
     * fraction is a double, so {@code 10.10} comes back {@code 10.1} -- while upstream's own test of this
     * very lambda (javaPlatformBinding mathLibraryTests.pure:65) expects {@code new BigDecimal("10.10")}.
     * Upstream's print on the left, lite's on the right. Pinned both ways: each must still occur, and
     * nothing else may differ.
     */
    private static final Map<String, String> EXACT_DECIMAL = Map.of(
            "|10.1D->divide(2.1D, 1)", "|10.10D->divide(2.1D, 1)",
            "|10.1D->divide(\n  2.1D,\n  1\n)", "|10.10D->divide(\n  2.1D,\n  1\n)");

    /** A whole model's JSON nests far deeper than one request's default limit. */
    private static final Json.Config DEEP = new Json.Config(4096);

    /** Upstream's own lambda round-trip tests: their texts are the printer's spec too. */
    static final List<String> ROUNDTRIP_TESTS = List.of(
            "legend-engine-core/legend-engine-core-base/legend-engine-core-language-pure/legend-engine-language-pure-grammar/src/test/java/org/finos/legend/engine/language/pure/grammar/test/roundtrip/TestLambdaRoundtrip.java",
            "legend-engine-core/legend-engine-core-base/legend-engine-core-language-pure/legend-engine-language-pure-grammar/src/test/java/org/finos/legend/engine/language/pure/grammar/test/roundtrip/TestLambdaPrettyRendering.java",
            "legend-engine-core/legend-engine-core-base/legend-engine-core-language-pure/legend-engine-language-pure-grammar/src/test/java/org/finos/legend/engine/language/pure/grammar/test/roundtrip/TestRelation.java");

    @Test
    void litePrintsEveryLambdaAsUpstreamDoes() throws Exception {
        ObjectMapper mapper = ObjectMapperFactory.getNewStandardObjectMapperWithPureProtocolExtensionSupports();
        PureGrammarParser oracle = PureGrammarParser.newInstance();

        List<Json.Obj> lambdas = new ArrayList<>();
        int sources = 0;
        for (Corpus.Source src : Corpus.all()) {
            String json;
            try {
                json = mapper.writeValueAsString(oracle.parseModel(src.text()));
            } catch (Throwable t) {
                continue;
            }
            sources++;
            collectLambdas(Json.parse(json, DEEP), lambdas);
        }
        int corpusLambdas = lambdas.size();
        for (String rel : ROUNDTRIP_TESTS) {
            for (String run : InlineSnippets.literalRuns(Files.readString(Corpus.engineRoot().resolve(rel)))) {
                try {
                    LambdaFunction parsed = new DomainParser().parseLambda(run, "", 0, 0, true);
                    collectLambdas(Json.parse(mapper.writeValueAsString(parsed), DEEP), lambdas);
                } catch (Throwable t) {
                    // not a lambda (an expected-output text, a message): nothing to print
                }
            }
        }

        int matched = 0;
        int upstreamThrew = 0;
        int bracesKept = 0;
        java.util.Set<String> deliberate = new java.util.TreeSet<>();
        List<String> diffs = new ArrayList<>();
        Map<String, Integer> refusals = new TreeMap<>();
        for (Json.Obj lambda : lambdas) {
            String wire = Json.toCompact(lambda);
            for (RenderStyle style : List.of(RenderStyle.STANDARD, RenderStyle.PRETTY)) {
                String expected;
                try {
                    expected = mapper.readValue(wire, LambdaFunction.class)
                            .accept(DEPRECATED_PureGrammarComposerCore.Builder.newInstance().withRenderStyle(style).build());
                } catch (Throwable t) {
                    upstreamThrew++;
                    continue;
                }
                String actual;
                try {
                    actual = PureComposer.lambda(lambda, style == RenderStyle.PRETTY
                            ? PureComposer.Style.PRETTY : PureComposer.Style.STANDARD);
                } catch (IllegalArgumentException e) {
                    refusals.merge(e.getMessage(), 1, Integer::sum);
                    continue;
                }
                if (expected.equals(actual)) {
                    matched++;
                } else if (actual.equals(EXACT_DECIMAL.get(expected))) {
                    deliberate.add(expected);
                } else if (keepsBracesUpstreamDrops(oracle, wire, expected, actual)) {
                    bracesKept++;
                } else if (diffs.size() < 40) {
                    diffs.add(style + "\n  upstream: " + expected + "\n  lite:     " + actual
                            + "\n  wire:     " + (wire.length() > 1500 ? wire.substring(0, 1500) : wire));
                }
            }
        }
        System.out.println("[composer-parity] sources=" + sources + " corpusLambdas=" + corpusLambdas
                + " roundtripLambdas=" + (lambdas.size() - corpusLambdas) + " matched=" + matched
                + " deliberate=" + deliberate.size() + " bracesKept=" + bracesKept + " diffs=" + diffs.size() + " refused=" + refusals.values().stream().mapToInt(Integer::intValue).sum()
                + " upstreamThrew=" + upstreamThrew);
        refusals.forEach((m, n) -> System.out.println("[composer-parity] refused " + n + " x " + m));
        diffs.forEach(d -> System.out.println("[composer-parity] DIFF " + d));
        assertEquals(List.of(), diffs, "lite printed a lambda differently from upstream");
        assertEquals(new java.util.TreeSet<>(EXACT_DECIMAL.keySet()), deliberate,
                "a deliberate exact-decimal difference no longer occurs: take it off EXACT_DECIMAL");
        assertEquals(BRACES_KEPT, bracesKept, "the prints where lite keeps braces upstream drops (S38) moved");
        assertEquals(Map.of(), refusals, "lite refused a lambda upstream prints");
        assertTrue(matched >= MIN_MATCHED, "matched " + matched + " < " + MIN_MATCHED);
    }

    /**
     * Where lite DELIBERATELY prints differently (docs/SEMANTICS_REGISTER.md, "a lambda's braces where dropping them
     * changes the reading"; ModelComposerParityTest's rule): lite's print is upstream's with braces added, nothing
     * else; upstream's own parser reads the two prints differently or cannot read upstream's, and reads lite's with
     * the statement boundaries of the lambda printed (ModelComposerParityTest.statementShapes). Counted, not compared.
     */
    private static boolean keepsBracesUpstreamDrops(PureGrammarParser oracle, String wire, String expected,
            String actual) {
        if (!ModelComposerParityTest.onlyBracesAdded(expected, actual)) {
            return false;
        }
        ObjectMapper mapper = ObjectMapperFactory.getNewStandardObjectMapperWithPureProtocolExtensionSupports();
        String liteReads;
        try {
            liteReads = mapper.writeValueAsString(oracle.parseLambda(actual, "", 0, 0, false));
        } catch (RuntimeException | java.io.IOException e) {
            return false;
        }
        String upstreamReads;
        try {
            upstreamReads = mapper.writeValueAsString(oracle.parseLambda(expected, "", 0, 0, false));
        } catch (RuntimeException | java.io.IOException e) {
            upstreamReads = null;   // upstream's print does not parse
        }
        return !liteReads.equals(upstreamReads)
                && ModelComposerParityTest.statementShapes(liteReads).equals(ModelComposerParityTest.statementShapes(wire));
    }

    private static void collectLambdas(Json.Node node, List<Json.Obj> out) {
        if (node instanceof Json.Obj o) {
            if ("lambda".equals(o.getStringOr("_type", null))) {
                out.add(o);
            }
            for (Json.Node child : o.fields().values()) {
                collectLambdas(child, out);
            }
        } else if (node instanceof Json.Arr a) {
            for (Json.Node child : a.items()) {
                collectLambdas(child, out);
            }
        }
    }
}
