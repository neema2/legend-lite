package com.legend.equivalence;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * GENERATES corpus-manifest.tsv — one row per distinct corpus source: the SHA-256
 * of its text, its tier, its id ({@link CorpusManifestTest} says why the corpus is
 * pinned as data). The corpus includes tier C6, the engine fixture snapshot, so in
 * a bump the manifest is generated from the NEW snapshot
 * ({@code -Dlegend.engine.fixtures}, parser-equivalence's :gen_manifest).
 *
 * <pre>
 *   ManifestGenerator &lt;output&gt;
 *     -Dlegend.engine.root / -Dlegend.pure.root, -Dlegend.repo.root / -Dlegend.repo.module
 * </pre>
 */
public final class ManifestGenerator {

    private ManifestGenerator() {}

    public static void main(String[] args) throws Exception {
        if (args.length != 1) {
            throw new IllegalArgumentException("usage: ManifestGenerator <output>");
        }
        Map<String, String> rows = rows(Corpus.all());
        Files.writeString(Path.of(args[0]), text(rows), StandardCharsets.UTF_8);
        System.out.println("[manifest] " + rows.size() + " sources, " + Corpus.DEDUPED.get()
                + " exact-text duplicates dropped, " + Corpus.UNREADABLE.size() + " unreadable files");
    }

    /** id → "sha256 \t tier", in corpus order. */
    static Map<String, String> rows(List<Corpus.Source> sources) throws Exception {
        MessageDigest md = MessageDigest.getInstance("SHA-256");
        Map<String, String> actual = new LinkedHashMap<>();
        for (Corpus.Source s : sources) {
            byte[] h = md.digest(s.text().getBytes(StandardCharsets.UTF_8));
            StringBuilder hex = new StringBuilder(64);
            for (byte b : h) {
                hex.append(Character.forDigit((b >> 4) & 0xf, 16))
                        .append(Character.forDigit(b & 0xf, 16));
            }
            actual.put(s.id(), hex + "\t" + s.tier());
        }
        return actual;
    }

    /** The manifest file's text: "sha256 \t tier \t id" per row. */
    static String text(Map<String, String> rows) {
        StringBuilder out = new StringBuilder();
        rows.forEach((id, rest) -> out.append(rest.split("\t")[0])
                .append('\t').append(rest.split("\t")[1])
                .append('\t').append(id).append('\n'));
        return out.toString();
    }
}
