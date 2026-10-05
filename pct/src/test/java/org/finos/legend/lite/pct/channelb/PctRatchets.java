// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package org.finos.legend.lite.pct.channelb;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Map;
import java.util.LinkedHashMap;

/**
 * PCT'S MEASURED RATCHET VALUES (Bazel workplan P2-16, decision D9 (b)): each Channel B suite's discovery count, which
 * its test pinned by a hand-copied constant, measured by running the suite into {@code ratchets.tsv}
 * ({@code key<TAB>value}, one fixed order: the PCT module carries no comparison machinery, PctDisciplineTest). {@code //pct:ratchets} runs it -- manual, like the suites' cost: it compiles the
 * platform once per suite -- and {@code bazel run //pct:update_ratchets} writes the committed copy. Each Channel B test
 * compares its live discovery with the committed value, so gate 9 fails when the file is stale. Pass floors and
 * divergence ceilings stay hand-owned in the tests.
 *
 * <pre>
 *   PctRatchets &lt;output&gt;
 * </pre>
 */
public final class PctRatchets {

    private PctRatchets() {}

    public static void main(String[] args) throws Exception {
        if (args.length != 1) {
            throw new IllegalArgumentException("usage: PctRatchets <output>");
        }
        Map<String, Integer> out = new LinkedHashMap<>();
        out.put("channel_b.essential.discovered", ChannelBEssentialTest.runSuite(new ArrayList<>()).size());
        out.put("channel_b.grammar.discovered", ChannelBGrammarTest.runSuite(new ArrayList<>()).size());
        out.put("channel_b.relation.discovered", ChannelBRelationTest.runSuite(new ArrayList<>()).size());
        out.put("channel_b.standard.discovered", ChannelBStandardTest.runSuite(new ArrayList<>()).size());
        out.put("channel_b.unclassified.discovered", ChannelBUnclassifiedTest.runSuite(new ArrayList<>()).size());
        StringBuilder text = new StringBuilder("# pct's measured ratchet values (PctRatchets) -- regenerate: bazel run"
                + " //pct:update_ratchets\n");
        out.forEach((k, v) -> text.append(k).append('\t').append(v).append('\n'));
        Files.writeString(Path.of(args[0]), text.toString(), StandardCharsets.UTF_8);
    }

    /** The committed file, read on first use only: the generator ({@link #main}) never reads it, so a damaged copy
     *  cannot block its own regeneration. */
    private static final class Committed {
        static final Map<String, Integer> VALUES = load();

        private static Map<String, Integer> load() {
            Map<String, Integer> m = new LinkedHashMap<>();
            try (InputStream in = PctRatchets.class.getResourceAsStream("ratchets.tsv")) {
                if (in == null) {
                    throw new IllegalStateException("ratchets.tsv is not on the classpath -- bazel run //pct:update_ratchets");
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
                                + " -- bazel run //pct:update_ratchets");
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
            throw new IllegalStateException("ratchets.tsv has no " + key + " -- bazel run //pct:update_ratchets");
        }
        return v;
    }
}
