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
        // the own corpus's refusal classes against the oracle (OwnCorpusConformanceTest; Bazel workplan P3-30)
        OwnCorpusConformanceTest.measure().byClass()
                .forEach((cls, n) -> out.put(OwnCorpusConformanceTest.CLASS_KEY + cls, n));
        // the own corpus at legend-lite, per host (OwnDialectCensusTest; P3-30)
        OwnDialectCensusTest.Census dialect = OwnDialectCensusTest.measure();
        dialect.platformHosted().forEach((h, n) -> out.put(OwnDialectCensusTest.PLATFORM_KEY + h, n));
        dialect.extensionHosted().forEach((h, n) -> out.put(OwnDialectCensusTest.EXTENSION_KEY + h, n));
        StringBuilder text = new StringBuilder("# parser-equivalence's measured ratchet values (PeRatchets) --"
                + " regenerate: bazel run //parser-equivalence:update_ratchets\n");
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

    /** Every committed measured value whose key starts with {@code prefix}, by the rest of its key. */
    static Map<String, Integer> measuredWithPrefix(String prefix) {
        Map<String, Integer> out = new TreeMap<>();
        Committed.VALUES.forEach((k, v) -> {
            if (k.startsWith(prefix)) {
                out.put(k.substring(prefix.length()), v);
            }
        });
        return out;
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
