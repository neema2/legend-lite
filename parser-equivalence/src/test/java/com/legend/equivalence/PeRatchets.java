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
 * hand-copied constants -- the own-corpus elements matching the oracle, the mutant deck's size -- measured by this
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

    public static void main(String[] args) throws Exception {
        if (args.length != 1) {
            throw new IllegalArgumentException("usage: PeRatchets <output>");
        }
        Map<String, Integer> out = new TreeMap<>();
        out.put("mutation.deck", MutationFuzzTest.deckSize());
        out.put("own_corpus.matched", OwnCorpusLedgerDraft.diffs().matched());
        StringBuilder text = new StringBuilder("# parser-equivalence's measured ratchet values (PeRatchets) --"
                + " regenerate: bazel run //parser-equivalence:update_ratchets\n");
        out.forEach((k, v) -> text.append(k).append('\t').append(v).append('\n'));
        Files.writeString(Path.of(args[0]), text.toString(), StandardCharsets.UTF_8);
    }

    private static final Map<String, Integer> COMMITTED = load();

    private static Map<String, Integer> load() {
        Map<String, Integer> m = new TreeMap<>();
        try (InputStream in = PeRatchets.class.getResourceAsStream("ratchets.tsv")) {
            if (in == null) {
                return m;
            }
            for (String line : new String(in.readAllBytes(), StandardCharsets.UTF_8).split("\n")) {
                if (line.isBlank() || line.startsWith("#")) {
                    continue;
                }
                String[] kv = line.split("\t", -1);
                m.put(kv[0], Integer.parseInt(kv[1]));
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return m;
    }

    /** The committed measured value for {@code key}. */
    static int measured(String key) {
        Integer v = COMMITTED.get(key);
        if (v == null) {
            throw new IllegalStateException("ratchets.tsv has no " + key
                    + " -- bazel run //parser-equivalence:update_ratchets");
        }
        return v;
    }
}
