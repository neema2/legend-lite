// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.equivalence;

import org.finos.legend.engine.language.pure.grammar.from.PureGrammarParser;
import org.finos.legend.engine.shared.core.ObjectMapperFactory;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * The SIBLING test-corpus fixtures as a RATCHET (handoff
 * FixtureCorpusParityTest, adopted 2026-08-14): 215 hand-built
 * negatives + 51 grammar-first positive fixtures, vendored under
 * {@code src/test/resources/sibling-corpus} from the test-corpus
 * branch. Unlike the harvested corpus these were WRITTEN to cover
 * every .g4 keyword and every walker validation — the corpus sweep
 * structurally cannot contain what the engine repos never spell
 * (that blind spot produced the entire 2026-08-14 fix wave).
 *
 * <p>Three hard zeros and one NAMED allowlist:
 * <ul>
 *   <li>ENGINE-ACCEPTS-LITE-REFUSES == 0 — a missing construct;</li>
 *   <li>both-accept BYTE-DIFF == 0 — a wire divergence;</li>
 *   <li>lite-internal-error == 0 — always a bug;</li>
 *   <li>ENGINE-REFUSES-LITE-ACCEPTS ⊆ the named residue: the
 *       handoff's do-not-chase ordering rows (protocol-check proved
 *       their wire VALID; enforcing order globally over-corrects into
 *       the worse failure direction) and the two quarantined rows
 *       where the ENGINE crashes with an unpositioned NPE and lite
 *       parses cleanly (F23: matching parity would mean reproducing
 *       a crash).</li>
 * </ul>
 */
class FixtureCorpusParityTest {

    /** ENGINE-REFUSES-LITE-ACCEPTS residue, each row adjudicated
     *  2026-08-14 (ordering class per handoff §6.2 measurement; NPE
     *  rows from the 2026-08-12 quarantine review — its tsv was dead
     *  data and is deleted, F3.7). Shrink-only. */
    private static final java.util.Set<String> LENIENT_RESIDUE =
            java.util.Set.of(
                    // ordering/placement — engine encodes order in rule
                    // structure; wire is order-free (do-not-chase)
                    "neg-connection-relationalmapper-section-order.pure",
                    "neg-core-latest-outside-milestoning.pure",
                    "neg-diagram-import-after-elements.pure",
                    "neg-generation-node-id-after-element.pure",
                    "neg-mapping-aggregationaware-mainmapping-first.pure",
                    "neg-mapping-pure-src-after-property.pure",
                    "neg-mongodbmapping-clause-order.pure",
                    "neg-persistence-test-field-order.pure",
                    "neg-relational-include-after-element.pure",
                    "neg-relational-milestoning-after-columns.pure",
                    "ordering-view-modifiers.pure",
                    // engine NPE crashes (lite is RIGHT — F23)
                    "neg-persistence-graphfetch-keys-identifier.pure",
                    "neg-persistence-tds-keys-navigation-path.pure");

    private static final PureGrammarParser ORACLE =
            PureGrammarParser.newInstance();

    private static final com.fasterxml.jackson.databind.ObjectMapper MAPPER =
            ObjectMapperFactory
                    .getNewStandardObjectMapperWithPureProtocolExtensionSupports();

    @Test
    void siblingCorpusParity() throws Exception {
        List<String> missingConstructs = new ArrayList<>();
        List<String> byteDiffs = new ArrayList<>();
        List<String> internalErrors = new ArrayList<>();
        List<String> newLenient = new ArrayList<>();
        java.util.Set<String> residueSeen = new java.util.HashSet<>();
        int agree = 0;
        for (String dir : new String[] {"negative", "fixtures"}) {
            for (String name : listResources(dir)) {
                String src = readResource(dir + "/" + name);
                String engine;
                boolean engineAccepts;
                try {
                    engine = MAPPER.writeValueAsString(
                            ORACLE.parseModel(src));
                    engineAccepts = true;
                } catch (Throwable t) {
                    engine = "";
                    engineAccepts = false;
                }
                String lite;
                boolean liteAccepts;
                try {
                    lite = com.legend.parser.PmcdParser.parseDocument(src);
                    liteAccepts = true;
                } catch (com.legend.parser.ParseException e) {
                    lite = "";
                    liteAccepts = false;
                } catch (Throwable t) {
                    internalErrors.add(name + " :: " + t);
                    continue;
                }
                if (engineAccepts && !liteAccepts) {
                    missingConstructs.add(name);
                } else if (!engineAccepts && liteAccepts) {
                    if (LENIENT_RESIDUE.contains(name)) {
                        residueSeen.add(name);
                    } else {
                        newLenient.add(name);
                    }
                } else if (engineAccepts && !engine.equals(lite)) {
                    byteDiffs.add(name);
                } else {
                    agree++;
                }
            }
        }
        assertEquals(List.of(), internalErrors,
                "lite threw a NON-ParseException on fixture input");
        assertEquals(List.of(), missingConstructs,
                "ENGINE-ACCEPTS-LITE-REFUSES: a construct went missing");
        assertEquals(List.of(), byteDiffs,
                "both-accept BYTE-DIFF: wire divergence on a fixture");
        assertEquals(List.of(), newLenient,
                "NEW lenient row (engine refuses, lite accepts) outside "
                        + "the adjudicated residue — adjudicate or fix");
        // shrink-only: a residue row that now agrees must LEAVE the list
        List<String> stale = LENIENT_RESIDUE.stream()
                .filter(r -> !residueSeen.contains(r)).sorted().toList();
        assertEquals(List.of(), stale,
                "residue rows now agree — remove them from LENIENT_RESIDUE");
        // the deck is 266 sources; everything must be accounted for
        assertEquals(266 - LENIENT_RESIDUE.size(), agree,
                "fixture-deck accounting drifted");
    }

    private static List<String> listResources(String dir) throws Exception {
        // listed as FILES of this module's test resources, not by turning the
        // classpath URL into a Path: under Bazel the resources are packed in a
        // jar, and a jar: URI has no default FileSystem (2026-09-22)
        var path = com.legend.testing.Repo.module(
                "src/test/resources/sibling-corpus", dir);
        try (var files = java.nio.file.Files.list(path)) {
            return files.map(p -> p.getFileName().toString())
                    .filter(n -> n.endsWith(".pure"))
                    .sorted().toList();
        }
    }

    private static String readResource(String rel) throws Exception {
        try (var in = FixtureCorpusParityTest.class.getResourceAsStream(
                "/sibling-corpus/" + rel)) {
            return new String(java.util.Objects.requireNonNull(in)
                    .readAllBytes(), java.nio.charset.StandardCharsets.UTF_8);
        }
    }
}
