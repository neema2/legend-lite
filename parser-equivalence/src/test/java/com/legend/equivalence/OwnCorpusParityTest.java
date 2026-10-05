// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.equivalence;

import com.legend.testing.Repo;
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
 * Pure snippet our own tests embed (core / spec / pct, the same extraction the
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
 * classifies the refusals). {@link OwnCorpusLedgerDraft} writes a DRAFT of the
 * ledger from the current diffs, reasons kept where the row survives and new rows
 * marked for a human ({@code bazel run //docs:draft_own_corpus_ledger}) — never
 * part of //:update_generated, because the reasons ARE the review.
 */
class OwnCorpusParityTest {

    static final Path LEDGER = com.legend.testing.Runfile.property("ledger.own-corpus-protocol-diffs");
    // The MATCHED elements are MEASURED (OwnCorpusLedgerDraft.diffs) into this package's generated ratchets.tsv
    // (own_corpus.matched; //parser-equivalence:update_ratchets, diff-tested in //:generated): a test model joining the
    // own corpus moves the count there, as a reviewed diff, instead of a hand re-pin here. Its history -- 2292 on
    // 2026-09-11 to 2685 on 2026-10-04, each step a test's models joining and matching -- is in git (Bazel workplan
    // P2-16, D9).

    @Test
    @DisplayName("every own-corpus snippet the oracle accepts emits the oracle's bytes, element by element")
    void ownSnippetsMatchTheOracleByteForByte() throws Exception {
        OwnCorpusLedgerDraft.Diffs pass = OwnCorpusLedgerDraft.diffs();
        Map<String, Integer> kinds = pass.kinds();
        Map<String, String> diffs = pass.diffs();
        int matched = pass.matched();
        System.out.println("[own-parity] " + kinds + " matched=" + matched + " diffs=" + diffs.size());
        Files.createDirectories(Repo.outDir());
        StringBuilder report = new StringBuilder();
        diffs.forEach((k, d) -> report.append(k).append('\t').append(d).append('\n'));
        Files.writeString(Repo.out("own-corpus-protocol-diffs.txt"), report.toString());
        Map<String, String> ledger = readLedger();
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
                + " the JSON path of each; fix the emitter, or add the row WITH A REASON — a draft with every"
                + " current diff: bazel run //docs:draft_own_corpus_ledger)");
        assertEquals(List.of(), stale, "ledger rows that no longer diff — remove them (a stale row is a ledger lying)");
        assertEquals(PeRatchets.measured("own_corpus.matched"), matched, "own-corpus matched elements moved -- a"
                + " model that joined and matched, or one that stopped matching: bazel run"
                + " //parser-equivalence:update_ratchets, and say which in the commit");
    }

    static Map<String, String> readLedger() throws IOException {
        return readLedger(LEDGER);
    }

    static Map<String, String> readLedger(Path ledger) throws IOException {
        Map<String, String> out = new LinkedHashMap<>();
        if (!Files.exists(ledger)) {
            return out;
        }
        for (String line : Files.readAllLines(ledger, StandardCharsets.UTF_8)) {
            if (line.startsWith("#") || line.isBlank()) {
                continue;
            }
            String[] f = line.split("\t", 3);
            out.put(f[0], f.length > 2 ? f[2] : "");
        }
        return out;
    }

}
