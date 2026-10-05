// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.generators;

import com.legend.rcorpus.MinimalCorpus;
import com.legend.rcorpus.MinimalCorpusTest;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.TreeMap;

/**
 * SPEC'S MEASURED RATCHET VALUES (Bazel workplan P2-16, decision D9 (b)): counts that tests used to pin by hand-copied
 * constants, now MEASURED by this program into {@code ratchets.tsv} (one {@code key<TAB>value} per line, sorted) --
 * {@code //spec:ratchets} runs it, {@code bazel run //spec:update_ratchets} writes the committed copy, and
 * {@code //:generated} fails when it is stale. A test compares its live measurement with the committed value
 * ({@link #measured}); ceilings and floors stay hand-owned in Java with their dated reasons. A move is a reviewed diff
 * of this file, its reason in the commit.
 *
 * <pre>
 *   SpecRatchets &lt;output&gt;
 * </pre>
 */
public final class SpecRatchets {

    private SpecRatchets() {}

    public static void main(String[] args) throws IOException {
        if (args.length != 1) {
            throw new IllegalArgumentException("usage: SpecRatchets <output>");
        }
        Map<String, Integer> out = new TreeMap<>();
        // the relational corpus's census, by its text scan
        MinimalCorpus.Census c = MinimalCorpusTest.scanCensus();
        out.put("corpus.census.declared", c.declared());
        out.put("corpus.census.excluded", c.excluded());
        out.put("corpus.census.discovered", c.discovered());
        // the dynafunctions the platform cannot translate (DynaFnRegistryTest holds the ceiling)
        out.put("dynafn.unsupported", com.legend.builtin.DynaFn.withResolution(
                com.legend.builtin.DynaFn.Resolution.UNSUPPORTED).size());
        // the implementation table's rows per kind
        ImplementationTableTest.kindsOf(ImplementationTableTest.build().impl())
                .forEach((kind, n) -> out.put("implementation.kinds." + kind, n));
        // the hardcoded upstream paths the tests resolve
        out.put("upstream.paths", UpstreamPathManifestTest.manifest().size());
        StringBuilder text = new StringBuilder("# spec's measured ratchet values (SpecRatchets) -- regenerate: bazel run"
                + " //spec:update_ratchets\n");
        out.forEach((k, v) -> text.append(k).append('\t').append(v).append('\n'));
        Files.writeString(Path.of(args[0]), text.toString(), StandardCharsets.UTF_8);
    }

    private static final Map<String, Integer> COMMITTED = load();

    private static Map<String, Integer> load() {
        Map<String, Integer> m = new TreeMap<>();
        try (InputStream in = SpecRatchets.class.getResourceAsStream("ratchets.tsv")) {
            if (in == null) {
                return m;   // the generator's own run reads none
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
    public static int measured(String key) {
        Integer v = COMMITTED.get(key);
        if (v == null) {
            throw new IllegalStateException("ratchets.tsv has no " + key + " -- bazel run //spec:update_ratchets");
        }
        return v;
    }

    /** Every committed measured value under {@code prefix}, keyed by the rest of the key. */
    public static Map<String, Integer> measuredWithPrefix(String prefix) {
        Map<String, Integer> out = new TreeMap<>();
        COMMITTED.forEach((k, v) -> {
            if (k.startsWith(prefix)) {
                out.put(k.substring(prefix.length()), v);
            }
        });
        if (out.isEmpty()) {
            throw new IllegalStateException("ratchets.tsv has nothing under " + prefix
                    + " -- bazel run //spec:update_ratchets");
        }
        return out;
    }
}
