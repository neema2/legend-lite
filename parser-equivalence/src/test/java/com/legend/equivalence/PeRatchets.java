// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.equivalence;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.TreeMap;

/**
 * PARSER-EQUIVALENCE'S MEASURED RATCHET VALUES (Bazel workplan P2-16, decision D9 (b)): counts tests pinned by
 * hand-copied constants -- the own-corpus elements matching the referee, the mutant deck's size -- measured by this
 * program into {@code ratchets.tsv} ({@code key<TAB>value}, sorted). {@code //parser-equivalence:ratchets} runs it,
 * {@code bazel run //parser-equivalence:update_ratchets} writes the committed copy and {@code //:generated} fails when
 * it is stale; a test compares its live value with the committed one ({@link #measured}). Allowances (the per-host
 * and per-file pins) stay hand-owned in Java.
 *
 * <pre>
 *   PeRatchets &lt;output&gt;
 * </pre>
 */
public final class PeRatchets {

    private PeRatchets() {}

    /** Each move of a value, dated and with its reason (AGENTS.md: a pin moves only with one; the A delta audit's note
     *  16, 2026-11-11), written into the file as comments above the values. A move appends a line here in the change
     *  that regenerates the file. */
    static final java.util.List<String> HISTORY = java.util.List.of(
            "own_corpus.matched 3090 -> 3091 (2026-11-09, 8ff49fc0): the follow-up audit's tests' models",
            "own_corpus.matched 3091 -> 3093 (2026-11-10, 59758819): the A2-A5 tests' models",
            "own_corpus.matched 3093 -> 3096 (2026-11-10, b8337475): A6's time-zone and number-text tests' models",
            "own_corpus.matched 3096 -> 3100 (2026-11-10, 1e2c23b3): A7's date-time fraction tests' models",
            "own_corpus.matched 3100 -> 3106 (2026-11-10, c755071b): the A checkpoint's tests' models (QueryRuntimeTest,"
                    + " RowLoadCellsTest, TextPlanSourceTest and the rest)",
            "own_corpus.matched 3106 -> 3116 (2026-11-11, B2e): EmbeddedDataKindsTest's ten elements (a Database and three"
                    + " Data elements; a service's model of six)",
            "own_corpus.matched 3116 -> 3117 (2026-11-11, B2a): JsonSourceFrameArrayTextTest's class, the frame's model");

    public static void main(String[] args) throws Exception {
        if (args.length != 1) {
            throw new IllegalArgumentException("usage: PeRatchets <output>");
        }
        Map<String, Integer> out = new TreeMap<>();
        out.put("mutation.deck", MutationFuzzTest.deckSize());
        OwnCorpusLedgerDraft.Diffs own = OwnCorpusLedgerDraft.diffs();
        out.put("own_corpus.matched", own.matched());
        // the count's verdicts by kind on one line: two measurements that differ diff to one line
        System.out.println("[pe-ratchets] own_corpus " + own.kinds());
        StringBuilder text = new StringBuilder("# parser-equivalence's measured ratchet values (PeRatchets) --"
                + " regenerate: bazel run //parser-equivalence:update_ratchets\n");
        HISTORY.forEach(h -> text.append("# ").append(h).append('\n'));
        out.forEach((k, v) -> text.append(k).append('\t').append(v).append('\n'));
        Files.writeString(Path.of(args[0]), text.toString(), StandardCharsets.UTF_8);
    }

    /** The committed file, read on first use only: the generator ({@link #main}) never reads it, so a damaged copy
     *  cannot block its own regeneration. */
    private static final class Committed {
        static final Map<String, Integer> VALUES = load();

        private static Map<String, Integer> load() {
            Map<String, Integer> m = new TreeMap<>();
            try (InputStream in = PeRatchets.class.getResourceAsStream("ratchets.tsv")) {
                if (in == null) {
                    throw new IllegalStateException("ratchets.tsv is not on the classpath -- bazel run //parser-equivalence:update_ratchets");
                }
                int n = 0;
                for (String line : new String(in.readAllBytes(), StandardCharsets.UTF_8).split("\n")) {
                    n++;
                    if (line.isBlank() || line.startsWith("#")) {
                        continue;
                    }
                    String[] kv = line.split("\t", -1);
                    if (kv.length != 2 || !kv[1].matches("-?\\d+")) {
                        throw new IllegalStateException("ratchets.tsv line " + n + " is no key<TAB>integer: " + line
                                + " -- bazel run //parser-equivalence:update_ratchets");
                    }
                    m.put(kv[0], Integer.parseInt(kv[1]));
                }
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
            return m;
        }
    }

    /** The committed measured value for {@code key}. */
    static int measured(String key) {
        Integer v = Committed.VALUES.get(key);
        if (v == null) {
            throw new IllegalStateException("ratchets.tsv has no " + key
                    + " -- bazel run //parser-equivalence:update_ratchets");
        }
        return v;
    }
}
