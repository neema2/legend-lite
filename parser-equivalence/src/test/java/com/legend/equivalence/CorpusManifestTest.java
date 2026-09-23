// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.equivalence;

import com.legend.testing.Repo;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * CORPUS AS DATA (HARNESS_SIMPLIFICATION_PLAN Phase 6): the corpus is
 * pinned by a checked-in MANIFEST — one row per distinct source: SHA-256
 * of its text, tier, id. Nothing about what was measured is implicit:
 * changing an upstream checkout, the snippet extractor, the dedupe rule
 * or the fixture snapshot without regenerating the manifest fails this
 * test, so corpus drift is a REVIEWED DIFF, never a silent renumbering.
 *
 * <p>Regenerate: {@code bazel run //:update_generated} ({@link ManifestGenerator}).
 * The diff is the
 * review.
 */
class CorpusManifestTest {

    private static final Path MANIFEST =
            Repo.module("src/test/resources/corpus-manifest.tsv");

    @Test
    void corpusMatchesTheCommittedManifest() throws Exception {
        List<Corpus.Source> sources = Corpus.all();
        Assumptions.assumeTrue(!sources.isEmpty(),
                "no corpus on disk — set -Dlegend.engine.root / -Dlegend.pure.root");

        Map<String, String> actual = ManifestGenerator.rows(sources);
        Files.writeString(Repo.out("corpus-manifest.tsv"), ManifestGenerator.text(actual));
        System.out.println("corpus: " + sources.size() + " distinct sources, "
                + Corpus.DEDUPED.get() + " exact-text duplicates dropped, "
                + Corpus.UNREADABLE.size() + " unreadable files");
        assertTrue(Files.exists(MANIFEST), "no committed corpus manifest at " + MANIFEST
                + " — regenerate: bazel run //:update_generated");

        Map<String, String> pinned = new LinkedHashMap<>();
        for (String line : Files.readAllLines(MANIFEST)) {
            if (line.isBlank()) {
                continue;
            }
            String[] f = line.split("\t", 3);
            pinned.put(f[2], f[0] + "\t" + f[1]);
        }
        List<String> missing = new ArrayList<>();
        List<String> changed = new ArrayList<>();
        List<String> extra = new ArrayList<>();
        pinned.forEach((id, rest) -> {
            String a = actual.get(id);
            if (a == null) {
                missing.add(id);
            } else if (!a.equals(rest)) {
                changed.add(id);
            }
        });
        actual.keySet().stream().filter(id -> !pinned.containsKey(id))
                .forEach(extra::add);
        assertEquals(0, missing.size() + changed.size() + extra.size(),
                () -> "corpus drift vs the committed manifest — regenerate"
                        + " (bazel run //:update_generated) and REVIEW the"
                        + " diff:\n  missing " + missing.size()
                        + (missing.isEmpty() ? "" : " e.g. " + missing.get(0))
                        + "\n  changed " + changed.size()
                        + (changed.isEmpty() ? "" : " e.g. " + changed.get(0))
                        + "\n  extra   " + extra.size()
                        + (extra.isEmpty() ? "" : " e.g. " + extra.get(0)));
    }
}
