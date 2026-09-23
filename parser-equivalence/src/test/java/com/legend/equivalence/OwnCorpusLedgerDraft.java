package com.legend.equivalence;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * Writes a DRAFT of docs/own-corpus-protocol-diffs.tsv: every current own-corpus
 * protocol DIFF, each surviving row keeping its reason, each new row marked
 * "TODO: adjudicate" for a human. A draft, not a generated file — the reasons are
 * the review — so it is NOT part of //:update_generated
 * ({@code bazel run //docs:draft_own_corpus_ledger}).
 *
 * <pre>
 *   OwnCorpusLedgerDraft &lt;committed ledger&gt; &lt;output&gt;
 * </pre>
 */
public final class OwnCorpusLedgerDraft {

    private OwnCorpusLedgerDraft() {}

    /** One pass of the own corpus through the byte comparison. */
    public record Diffs(Map<String, Integer> kinds, Map<String, String> diffs, int matched) {
    }

    public static Diffs diffs() throws Exception {
        List<Corpus.Source> ours = OwnCorpusConformanceTest.ownSnippets();
        if (ours.size() <= 500) {
            throw new IllegalStateException("own corpus floor: only " + ours.size() + " snippets extracted");
        }
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
        return new Diffs(kinds, diffs, matched);
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            throw new IllegalArgumentException("usage: OwnCorpusLedgerDraft <committed ledger> <output>");
        }
        Diffs pass = diffs();
        Map<String, String> previous = OwnCorpusParityTest.readLedger(Path.of(args[0]));
        StringBuilder sb = new StringBuilder();
        sb.append("# OWN-CORPUS PROTOCOL DIFFS — every element of our own test snippets whose wire JSON differs from the\n");
        sb.append("# oracle's (OwnCorpusParityTest; upstream boundary batch 6). THE RATCHET IS THIS FILE: a diff not listed\n");
        sb.append("# here is red; a listed row that no longer diffs is red (stale). Columns: source#element, first divergence\n");
        sb.append("# (JSON path), REASON — the reason is the review; 'TODO' is not a reason. Draft: bazel run //docs:draft_own_corpus_ledger.\n");
        pass.diffs().forEach((k, d) -> sb.append(k).append('\t').append(d.replace('\t', ' ')).append('\t')
                .append(previous.getOrDefault(k, "TODO: adjudicate")).append('\n'));
        Files.writeString(Path.of(args[1]), sb.toString(), StandardCharsets.UTF_8);
        System.out.println("[own-parity] ledger draft: " + pass.diffs().size() + " rows");
    }
}
