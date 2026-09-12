// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.equivalence;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * THE OWN-CORPUS BYTE PARITY (upstream boundary batch 6, workstream F): every
 * Pure snippet our own tests embed (core / pct / nlq, the same extraction the
 * mirror corpus uses) goes through the SAME positional byte comparison the
 * upstream corpus gets — the oracle's element JSON against ours, element by
 * element ({@link ParserEquivalence#compare}). Until this test the own corpus
 * reached the oracle for ACCEPTANCE only ({@link OwnCorpusConformanceTest});
 * the wire shape of what our tests write was pinned by 17 hand-copied JSON
 * strings in core, captured at engine 4.133.0 and re-derived by nothing.
 *
 * <p>The verdicts: every source the oracle accepts and we parse must MATCH;
 * a DIFF is a row in {@code docs/own-corpus-protocol-diffs.tsv} with a reason
 * (shrink-only: a row that no longer diffs is stale and fails); PARSE_FAIL
 * and REFERENCE_REJECTED are counted, never silent (the mirror corpus
 * classifies the refusals). {@code -Downcorpus.generate=1} rewrites the
 * ledger from the current diffs, reasons kept where the row survives.
 */
class OwnCorpusParityTest {

    static final Path LEDGER = Path.of("..", "docs", "own-corpus-protocol-diffs.tsv");
    /** EXACT pin on MATCHED elements (measured 2026-09-11). 2292 → 2296
     *  (batch 7a): the product test runner's proof model — four functions
     *  in a core test — joined the own corpus and matched. */
    static final int MIN_MATCHED = 2312;   // 4.145.0 bump (batch 8): +16 —
                                           // DocumentationTest's snippets
                                           // joined the own corpus

    @Test
    @DisplayName("every own-corpus snippet the oracle accepts emits the oracle's bytes, element by element")
    void ownSnippetsMatchTheOracleByteForByte() throws IOException {
        List<Corpus.Source> ours = OwnCorpusConformanceTest.ownSnippets();
        assertTrue(ours.size() > 500, "own corpus floor: only " + ours.size() + " snippets extracted");
        ParserEquivalence eq = new ParserEquivalence();
        Map<String, Integer> kinds = new TreeMap<>();
        Map<String, String> diffs = new TreeMap<>();
        int matched = 0;
        for (Corpus.Source src : ours) {
            for (ParserEquivalence.Verdict v : eq.compare(src)) {
                kinds.merge(v.kind().name(), 1, Integer::sum);
                if (v.kind() == ParserEquivalence.Kind.MATCH) {
                    matched++;
                } else if (v.kind() == ParserEquivalence.Kind.DIFF) {
                    diffs.put(v.sourceId() + v.element(), v.detail());
                }
            }
        }
        System.out.println("[own-parity] " + kinds + " matched=" + matched + " diffs=" + diffs.size());
        Files.createDirectories(Path.of("target"));
        StringBuilder report = new StringBuilder();
        diffs.forEach((k, d) -> report.append(k).append('\t').append(d).append('\n'));
        Files.writeString(Path.of("target", "own-corpus-protocol-diffs.txt"), report.toString());
        Map<String, String> ledger = readLedger();
        if ("1".equals(System.getProperty("owncorpus.generate"))) {
            writeLedger(diffs, ledger);
            return;
        }
        List<String> unledgered = new ArrayList<>();
        diffs.forEach((k, d) -> {
            if (!ledger.containsKey(k)) {
                unledgered.add(k + " — " + d);
            }
        });
        List<String> stale = new ArrayList<>();
        ledger.keySet().forEach(k -> {
            if (!diffs.containsKey(k)) {
                stale.add(k);
            }
        });
        assertEquals(List.of(), unledgered, "own-corpus protocol DIFFs not in the ledger (target/own-corpus-protocol-diffs.txt has"
                + " the JSON path of each; fix the emitter, or add the row WITH A REASON)");
        assertEquals(List.of(), stale, "ledger rows that no longer diff — remove them (a stale row is a ledger lying)");
        assertTrue(matched >= MIN_MATCHED, "own-corpus matched elements fell: " + matched + " < " + MIN_MATCHED);
        assertTrue(matched == MIN_MATCHED,
                "own-corpus matched elements moved to " + matched + " — re-pin MIN_MATCHED (headroom is not a pin)");
    }

    static Map<String, String> readLedger() throws IOException {
        Map<String, String> out = new LinkedHashMap<>();
        if (!Files.exists(LEDGER)) {
            return out;
        }
        for (String line : Files.readAllLines(LEDGER, StandardCharsets.UTF_8)) {
            if (line.startsWith("#") || line.isBlank()) {
                continue;
            }
            String[] f = line.split("\t", 3);
            out.put(f[0], f.length > 2 ? f[2] : "");
        }
        return out;
    }

    private static void writeLedger(Map<String, String> diffs, Map<String, String> previous) throws IOException {
        StringBuilder sb = new StringBuilder();
        sb.append("# OWN-CORPUS PROTOCOL DIFFS — every element of our own test snippets whose wire JSON differs from the\n");
        sb.append("# oracle's (OwnCorpusParityTest; upstream boundary batch 6). THE RATCHET IS THIS FILE: a diff not listed\n");
        sb.append("# here is red; a listed row that no longer diffs is red (stale). Columns: source#element, first divergence\n");
        sb.append("# (JSON path), REASON — the reason is the review; 'TODO' is not a reason. Regenerate: -Downcorpus.generate=1.\n");
        diffs.forEach((k, d) -> sb.append(k).append('\t').append(d.replace('\t', ' ')).append('\t')
                .append(previous.getOrDefault(k, "TODO: adjudicate")).append('\n'));
        Files.writeString(LEDGER, sb.toString(), StandardCharsets.UTF_8);
        System.out.println("[own-parity] ledger regenerated: " + diffs.size() + " rows");
    }
}
