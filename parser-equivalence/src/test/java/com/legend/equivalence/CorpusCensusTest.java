package com.legend.equivalence;

import com.legend.testing.Repo;
import org.finos.legend.engine.language.pure.grammar.from.PureGrammarParser;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * THE HONEST DENOMINATOR (merge-readiness audit §4.1/§4.2, "the denominator
 * is constructed"). The parity gate reports 100% of what it compares; this
 * census reports what it never compares, and why, over the WHOLE corpus
 * rather than the sentinel's section-scoped 1,114 files.
 *
 * <p>Every source lands in exactly one bucket:
 *
 * <ul>
 *   <li><b>BOTH-PARSE</b> — the comparable population.</li>
 *   <li><b>OUR-DEFECT</b> — the reference accepts it and we fail. These are
 *       drop-in defects and nothing else; this is the parse-more worklist,
 *       ranked by error so families are visible.</li>
 *   <li><b>REFERENCE-REFUSES</b> — the reference cannot parse it either, so
 *       it can never enter the denominator. Ranked by the REFERENCE's own
 *       message, which is what tells an honest story: a file written in
 *       another tool's dialect (legend-pure's ###Diagram, whose grammar has
 *       typeView where the engine's has classView) is correctly excluded,
 *       whereas a bucket of ordinary Pure would mean the corpus is quietly
 *       shrinking around our gaps.</li>
 * </ul>
 *
 * <p>Reports only — this test asserts nothing about the counts, because its
 * job is to make the number visible, not to freeze it. The ratchets live in
 * {@link CorpusEquivalenceTest} and {@link SectionParseSentinelTest}.
 */
class CorpusCensusTest {

    @Test
    void reportWhatTheGateNeverCompares() throws Exception {
        List<Corpus.Source> sources = Corpus.all();
        Assumptions.assumeTrue(!sources.isEmpty(),
                "no corpus on disk — set -Dlegend.engine.root / -Dlegend.pure.root");

        PureGrammarParser reference = PureGrammarParser.newInstance();
        int bothParse = 0;
        int fullyParsed = 0;
        int parsedOnlyBySkipping = 0;
        int ourDefect = 0;
        int referenceRefuses = 0;
        int onlyWeCanRead = 0;
        int neitherReads = 0;
        Map<String, Integer> onlyWeReadByOrigin = new TreeMap<>();
        Map<String, Integer> onlyWeReadByEngineError = new TreeMap<>();
        Map<String, Integer> neitherByOrigin = new TreeMap<>();
        Map<String, Integer> defectsByMessage = new TreeMap<>();
        Map<String, Integer> refusalsByReferenceMessage = new TreeMap<>();
        Map<String, Integer> defectsByTier = new TreeMap<>();
        Map<String, Integer> skipReasons = new TreeMap<>();
        List<String> defectLines = new ArrayList<>();

        for (Corpus.Source src : sources) {
            boolean referenceAccepts;
            String referenceMessage = "";
            try {
                reference.parseModel(src.text());
                referenceAccepts = true;
            } catch (Throwable t) {
                referenceAccepts = false;
                referenceMessage = normalize(t);
            }
            if (!referenceAccepts) {
                referenceRefuses++;
                refusalsByReferenceMessage.merge(referenceMessage, 1, Integer::sum);
                // Do WE read it? The engine refusing a file does not settle
                // whether legend-lite should: most of these are legend-pure
                // PLATFORM sources, and legend-lite is a blend of the two.
                // A file only we can read is the superset thesis with
                // evidence; the alternative is that nobody can read it.
                try {
                    Surfaces.platform(src.text());
                    onlyWeCanRead++;
                    onlyWeReadByOrigin.merge(origin(src), 1, Integer::sum);
                    onlyWeReadByEngineError.merge(referenceMessage, 1, Integer::sum);
                } catch (Throwable neitherCanRead) {
                    neitherReads++;
                    neitherByOrigin.merge(origin(src), 1, Integer::sum);
                }
                continue;
            }
            try {
                // FULLY read: parseStrict refuses a section no grammar claims,
                // so this counts only files we actually read end to end.
                Surfaces.engine(src.text());
                fullyParsed++;
                bothParse++;
            } catch (Throwable strictFailed) {
                // ...but the internal pipeline may still load it by skipping
                // the sections it cannot read. That is a real capability and
                // a real limitation, so it gets its OWN row rather than being
                // folded into either success or failure.
                try {
                    Surfaces.platform(src.text());
                    parsedOnlyBySkipping++;
                    bothParse++;
                    skipReasons.merge(normalize(strictFailed), 1, Integer::sum);
                    continue;
                } catch (Throwable alsoFails) {
                    // fall through to the defect path below
                }
                Throwable t = strictFailed;
                ourDefect++;
                String msg = normalize(t);
                defectsByMessage.merge(msg, 1, Integer::sum);
                defectsByTier.merge(src.tier(), 1, Integer::sum);
                defectLines.add(src.id() + " :: " + msg);
            }
        }

        int comparable = bothParse + ourDefect;
        StringBuilder b = new StringBuilder();
        b.append("CORPUS CENSUS — what the parity gate never compares, and why\n")
                .append("=".repeat(72)).append('\n')
                .append(String.format("corpus sources          : %d%n", sources.size()))
                .append(String.format("  BOTH-PARSE            : %d%n", bothParse))
                .append(String.format("    fully read          : %d"
                        + "  (every section parsed)%n", fullyParsed))
                .append(String.format("    read by SKIPPING    : %d"
                        + "  (loaded, but a section was ignored)%n",
                        parsedOnlyBySkipping))
                .append(String.format("  OUR-DEFECT            : %d"
                        + "  (reference accepts, we fail)%n", ourDefect))
                .append(String.format("  REFERENCE-REFUSES     : %d"
                        + "  (never comparable, at any effort)%n", referenceRefuses))
                .append(String.format("    of which WE read    : %d"
                        + "  (legend-lite is broader here)%n", onlyWeCanRead))
                .append(String.format("    neither reads       : %d%n",
                        neitherReads))
                .append(String.format("%nreachable population    : %d of %d sources (%.1f%%)%n",
                        comparable, sources.size(),
                        100.0 * comparable / sources.size()))
                .append(String.format("we parse                : %d of %d reachable (%.1f%%)%n",
                        bothParse, comparable,
                        comparable == 0 ? 0.0 : 100.0 * bothParse / comparable));

        b.append("\nONLY-WE-READ-IT by ORIGIN — provenance is the evidence we have\n")
                .append("  (a file living in legend-pure's tree is one legend-pure\n")
                .append("   COMPILES in its own CI; we have not run its parser here —\n")
                .append("   it has no standalone parse API, only a booted runtime)\n")
                .append("-".repeat(72)).append('\n');
        onlyWeReadByOrigin.entrySet().stream()
                .sorted((x, y) -> y.getValue() - x.getValue())
                .forEach(e -> b.append(String.format("  %5d  %s%n",
                        e.getValue(), e.getKey())));

        b.append("\nONLY-WE-READ-IT by WHY THE ENGINE REFUSED\n")
                .append("-".repeat(72)).append('\n');
        onlyWeReadByEngineError.entrySet().stream()
                .sorted((x, y) -> y.getValue() - x.getValue())
                .limit(12)
                .forEach(e -> b.append(String.format("  %5d  %s%n",
                        e.getValue(), e.getKey())));

        b.append("\nNEITHER-READS-IT by ORIGIN\n")
                .append("-".repeat(72)).append('\n');
        neitherByOrigin.entrySet().stream()
                .sorted((x, y) -> y.getValue() - x.getValue())
                .forEach(e -> b.append(String.format("  %5d  %s%n",
                        e.getValue(), e.getKey())));

        b.append("\nREAD-BY-SKIPPING by the section we could not read\n")
                .append("-".repeat(72)).append('\n');
        skipReasons.entrySet().stream()
                .sorted((x, y) -> y.getValue() - x.getValue())
                .forEach(e -> b.append(String.format("  %5d  %s%n",
                        e.getValue(), e.getKey())));

        b.append("\nOUR-DEFECT by error — THE PARSE-MORE WORKLIST\n")
                .append("-".repeat(72)).append('\n');
        defectsByMessage.entrySet().stream()
                .sorted((x, y) -> y.getValue() - x.getValue())
                .forEach(e -> b.append(String.format("  %5d  %s%n",
                        e.getValue(), e.getKey())));

        b.append("\nOUR-DEFECT by corpus tier\n").append("-".repeat(72)).append('\n');
        defectsByTier.entrySet().stream()
                .sorted((x, y) -> y.getValue() - x.getValue())
                .forEach(e -> b.append(String.format("  %5d  %s%n",
                        e.getValue(), e.getKey())));

        b.append("\nREFERENCE-REFUSES by the REFERENCE's own error\n")
                .append("-".repeat(72)).append('\n');
        refusalsByReferenceMessage.entrySet().stream()
                .sorted((x, y) -> y.getValue() - x.getValue())
                .limit(40)
                .forEach(e -> b.append(String.format("  %5d  %s%n",
                        e.getValue(), e.getKey())));

        Files.writeString(Repo.out("corpus-census.txt"), b.toString());
        Files.writeString(Repo.out("corpus-census-defects.txt"),
                String.join("\n", defectLines));
        System.out.println(b);
    }

    /** Which project SHIPS this source — the cheapest real evidence about a
     *  file legend-lite reads and legend-engine refuses. */
    private static String origin(Corpus.Source src) {
        boolean pure = src.tier().contains("pure");
        boolean inline = src.tier().contains("inline");
        return (pure ? "legend-pure" : "legend-engine")
                + (inline ? " (inline snippet)" : " (.pure file)");
    }

    private static String normalize(Throwable t) {
        Throwable root = t;
        while (root.getCause() != null && root.getCause() != root) {
            root = root.getCause();
        }
        String msg = String.valueOf(root.getMessage()).replaceAll("\\s+", " ");
        // positions differ per file; the SHAPE of the error is the bucket
        msg = msg.replaceAll("\\[\\d+:\\d+\\]", "[L:C]")
                .replaceAll("line \\d+", "line N")
                .replaceAll("column \\d+", "column N");
        return msg.substring(0, Math.min(110, msg.length()));
    }
}
