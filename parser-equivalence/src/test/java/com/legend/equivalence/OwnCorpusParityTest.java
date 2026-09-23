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

    static final Path LEDGER = Repo.path("docs", "own-corpus-protocol-diffs.tsv");
    /** EXACT pin on MATCHED elements (measured 2026-09-11). 2292 → 2296
     *  (batch 7a): the product test runner's proof model — four functions
     *  in a core test — joined the own corpus and matched. */
    // 2312 → 2318 (corpus-zero cluster A, 2026-09-12): the by-tree isDistinct
    // witness's model (2 classes, a database, a mapping, a connection, a
    // runtime) joined the own corpus and matched
    // 2318 → 2324 (reducers over navigations in derived leaves, 2026-09-12):
    // the constraint-reducer witness's model (2 classes, a database, a
    // mapping, a connection, a runtime) joined the own corpus and matched
    // 2324 → 2331 (nested-constraint witness re-landed, 2026-09-12): its
    // model's seven elements joined the own corpus and matched
    static final int MIN_MATCHED = 2522;   // 2518 -> 2522 (2026-09-22, nlq deleted + the diagram reads every tag: DiagramServiceTest's fixtures were rewritten onto doc.doc and gained three edge-case models — a documentation block, a doc tag on another profile, an ambiguous bare doc — net +4 matched elements; nlq's deleted tests contributed NONE, measured by removing nlq alone from b51322e04, which left the count at 2518);   // 2519 -> 2518 (2026-09-22, block-compiler stage 4: HostChannelPredicateTest and its one snippet deleted with the host seam);   // 2518 -> 2519 (2026-09-20: the ladder's rung-12 test joined the own corpus and matched);   // 2503 -> 2518 (2026-09-20: the lean SQL ladder's inline model — one class, eleven rung tests, a store, a mapping, a runtime);   // 2488 -> 2503 (test-corpus merge, 2026-09-16: the branch's test fixtures and the gap-test snippets joined the own corpus and matched); 2475 -> 2488 (audit fix A10b, 2026-09-15: the P5-4 witnesses' models joined the own corpus and matched); 2470 -> 2475 (audit fix A9, 2026-09-15: the root-scope witness's two mappings joined the own corpus and matched); 2466 -> 2470 (audit fix A6, 2026-09-15: the per-set poison witness's model joined the own corpus and matched); 2459 -> 2466 (audit fix A5, 2026-09-15: the self-reference and inline-cycle witnesses' models joined the own corpus and matched); 2451 -> 2459 (audit fix A4, 2026-09-15: the join-isolation and slot-minter witnesses' models joined the own corpus and matched — the isolation witness's shared text carries a JOINTYPE placeholder and is not a snippet; net +8); 2444 -> 2451 (audit fix A3, 2026-09-15: the two ~distinct witnesses' models — seven elements — joined the own corpus and matched); 2434 -> 2444 (audit fix A2, 2026-09-15: the dropped-verdict and declaration-order witnesses' models — ten elements — joined the own corpus and matched); 2429 -> 2434 (leg 6g, 2026-09-15: the two-owner route witness's model — five elements — joined the own corpus and matched); 2422 -> 2429 (leg 6c, 2026-09-15: MainTableInferenceTest fixtures); 2425 -> 2422 (clean-sheet B3.3, 2026-09-13: the graph-wide mapped-class witness left with its shape — its first model's four elements — and the closure-local witness's include mapping joined and matched: net −3) | 2405 -> 2425 (clean-sheet B2, 2026-09-13: the include-rules witness's models — twenty elements — joined the own corpus and matched) | 2398 -> 2405 (T4.1 steps 5-6, 2026-09-13: the validation-line witness's two models — seven elements — joined the own corpus and matched) | 2392 -> 2398 (T4.1 step 4b, 2026-09-13: the stamped-surface-facts witness's model — six elements — joined the own corpus and matched) | 2381 -> 2392 (T4.1 step 4a, 2026-09-13: the include-closure witness's model — eleven elements — joined the own corpus and matched) | 2379 -> 2381 (T4.1 step 3d, 2026-09-13: the kernel's store witness — two databases — joined the own corpus and matched) | 2376 -> 2379 (T4.1 step 3a, 2026-09-13: the kernel's bare-superclass witness — three classes — joined the own corpus and matched) | 2358 -> 2376 (T4.1 step 2, 2026-09-13: the one-index and mapped-class witnesses' models — eighteen elements — joined the own corpus and matched) | 2349 -> 2358 (T4.1 step 1, 2026-09-13: the association qualified-property adoption witness's three models — nine elements — joined the own corpus and matched) | 2347 -> 2349 (the include-cycle witness: two cyclic mappings joined the own corpus and matched) | 2338 -> 2347 (store-substitution witness, 2026-09-13: its databases and mappings joined the own corpus and matched) | 2337 -> 2338 (non-uniform union witness, 2026-09-13: the Contractor union joined the own corpus and matched) | 2331 -> 2337 (lean union join witness, 2026-09-13: its model's six elements joined the own corpus and matched) | 4.145.0 bump (batch 8): +16 —
                                           // DocumentationTest's snippets
                                           // joined the own corpus

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
        assertTrue(matched >= MIN_MATCHED, "own-corpus matched elements fell: " + matched + " < " + MIN_MATCHED);
        assertTrue(matched == MIN_MATCHED,
                "own-corpus matched elements moved to " + matched + " — re-pin MIN_MATCHED (headroom is not a pin)");
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
