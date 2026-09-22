package com.legend.equivalence;

import com.legend.testing.Repo;
import org.finos.legend.engine.language.pure.grammar.from.PureGrammarParser;
import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The NEGATIVE corpus: upstream test files pin inputs the engine parser REJECTS
 * ({@code test(code, "PARSER error at [l:c]: ...")}). A drop-in must reject them too —
 * accepting an input the engine refuses is a silent divergence no positive corpus can
 * catch. Messages are engine-shaped and deliberately NOT compared; the LINE of the error
 * is compared informationally (reported, not gated — column conventions differ).
 *
 * <p>Pins pair by adjacency: a literal run spelling {@code PARSER error at [l:c]}
 * pairs with the run before it in the same file — the extractor's coverage numbers keep
 * the pairing honest. Pins whose input the CURRENT engine accepts (stale pins) are
 * counted and skipped, never silently dropped.
 */
class RejectionParityTest {

    private record Pin(String id, String input, int line, int col) {
    }

    /** Engine spelling {@code PARSER error at [l:c]} and legend-pure spelling
     *  {@code Parser error at (resource:... line:l column:c)}. */
    private static final Pattern ERROR_PIN = Pattern.compile(
            "^PARSER error at \\[(\\d+):(\\d+)(?:-[0-9:]+)?\\]"
                    + "|^Parser error at \\(resource:\\S+ line:(\\d+) column:(\\d+)\\)");

    /** Sections the Pure-only filter admits — mirrors ParserEquivalence.compare. */
    private static final Pattern SECTION = Pattern.compile("(?m)^###(\\w+)");

    /** Pins whose input has non-Pure sections — no parity claim until section
     *  parity lands, but NEVER a silent hole: counted and reported. */
    private int skippedNonPure;

    @Test
    void everyEngineRejectedInputIsRejectedHereToo() throws Exception {
        List<Pin> pins = extractPins();
        PureGrammarParser reference = PureGrammarParser.newInstance();

        int engineAccepts = 0;
        int rejectMatch = 0;
        int lineMatch = 0;
        int colMatch = 0;
        int colOffByOne = 0;
        int mispairedPins = 0;
        List<String> staleIds = new ArrayList<>();
        List<String> misses = new ArrayList<>();
        List<String> lineDiverges = new ArrayList<>();
        for (Pin p : pins) {
            // THE LIVE ORACLE (implementation audit §3.5): the scraped literal
            // only SELECTS the input — the position we hold ourselves to is the
            // engine's ACTUAL thrown position, because the extractor pairs
            // runs by adjacency and 17 of 43 scraped positions cannot exist in
            // the snippet they ride with
            int engineLine = -1;
            int engineCol = -1;
            try {
                reference.parseModel(p.input());
                engineAccepts++;                    // stale pin — current engine accepts
                // NAMED, not anonymous: a stale pin's input is engine-
                // accepted, so the BYTE gates own it; the pin itself is a
                // scrape artifact awaiting re-extraction
                staleIds.add(p.id());
                continue;
            } catch (Throwable expected) {
                if (expected instanceof org.finos.legend.engine.shared.core.operational
                        .errorManagement.EngineException ee
                        && ee.getSourceInformation() != null) {
                    engineLine = ee.getSourceInformation().startLine;
                    engineCol = ee.getSourceInformation().startColumn;
                }
            }
            if (engineLine != p.line() || engineCol != p.col()) {
                mispairedPins++;                    // the adjacency artifact, made visible
            }
            try {
                parseLegendEngine(p.input());
                misses.add(p.id() + " [engine error at " + engineLine + ":" + engineCol + "]");
            } catch (Throwable t) {
                rejectMatch++;
                String m = String.valueOf(t.getMessage());
                Matcher pos = Pattern.compile("\\[(\\d+):(\\d+)\\]").matcher(m);
                if (pos.find() && engineLine > 0) {
                    int ourLine = Integer.parseInt(pos.group(1));
                    int ourCol = Integer.parseInt(pos.group(2));
                    if (ourLine == engineLine) {
                        lineMatch++;
                        if (ourCol == engineCol) {
                            colMatch++;
                        } else if (ourCol == engineCol - 1) {
                            colOffByOne++;          // TokenStreamCursor 0-base (Phase B)
                        }
                    } else {
                        lineDiverges.add(p.id() + " engine " + engineLine + ":" + engineCol
                                + " ours " + ourLine + ":" + ourCol
                                + "\t" + m.replaceAll("\\s+", " "));
                    }
                }
            }
        }

        StringBuilder report = new StringBuilder();
        report.append("REJECTION PARITY — inputs the engine parser refuses\n")
                .append("=".repeat(72)).append('\n')
                .append(String.format("error pins extracted  : %d%n", pins.size()))
                .append(String.format("stale (engine accepts): %d%s%n", engineAccepts,
                        staleIds.isEmpty() ? "" : " — " + staleIds))
                .append(String.format("REJECT_MATCH          : %d%n", rejectMatch))
                .append(String.format("REJECT_MISS (BUG)     : %d%n", misses.size()))
                .append(String.format("error-line agreement  : %d of %d (vs the engine's LIVE position)%n",
                        lineMatch, rejectMatch))
                .append(String.format("  column exact        : %d%n", colMatch))
                .append(String.format("  column engine-1     : %d (TokenStreamCursor 0-base)%n",
                        colOffByOne))
                .append(String.format("scraped pin mispaired : %d (adjacency artifact, not parser signal)%n",
                        mispairedPins))
                .append(String.format("non-Pure pins skipped : %d (section-parity worklist)%n",
                        skippedNonPure));
        lineDiverges.stream().limit(10)
                .forEach(d -> report.append("  LINE-DIVERGE ").append(d).append('\n'));
        try {
            java.nio.file.Files.createDirectories(Repo.outDir());
            java.nio.file.Files.writeString(
                    Repo.out("position-diverges.tsv"),
                    String.join("\n", lineDiverges) + "\n");
        } catch (java.io.IOException e) {
            throw new java.io.UncheckedIOException(e);
        }
        if (!misses.isEmpty()) {
            report.append("\nMISSES — we accept what the engine refuses\n")
                    .append("-".repeat(72)).append('\n');
            misses.stream().limit(40).forEach(m -> report.append("  ").append(m).append('\n'));
        }
        Files.writeString(Repo.out("rejection-report.txt"), report.toString());
        System.out.println(report);

        // POSITION FLOOR (position-exactness lane): line agreement is a
        // down-only ratchet — 135 -> 417 of 423 on 2026-08-14 (ES leg landed); the
        // remaining divergences are censused in
        // target/position-diverges.tsv with messages
        assertTrue(pins.size() >= MIN_PINS,
                "negative corpus shrank: " + pins.size() + " pins < baseline " + MIN_PINS);
        assertEquals(0, misses.size(),
                "inputs the engine parser rejects were ACCEPTED here:\n"
                        + String.join("\n", misses.subList(0, Math.min(10, misses.size()))));
        assertTrue(lineMatch >= MIN_LINE_AGREEMENT,
                "error-line agreement with the engine's live position dropped: "
                        + lineMatch + " < " + MIN_LINE_AGREEMENT);
        assertTrue(colMatch >= MIN_COLUMN_EXACT,
                "exact column agreement with the engine dropped: " + colMatch
                        + " < " + MIN_COLUMN_EXACT);
    }

    /** After the 1-based column fix (audit §3.5 / Phase B): the 28 off-by-one
     *  pins all became exact. The 12 line-agreeing, column-different pins are
     *  genuinely different tokens — ours later, ANTLR's at the first token
     *  that cannot start an alternative. */
    private static final int MIN_COLUMN_EXACT = 335;   // ratcheted to measured 2026-08-14; 337 -> 335 on 2026-09-10 (upstream boundary batch 1): 4 negative pins left with the SOURCE pin's move to the 4.138.2 tag (MIN_PINS 424 -> 420), two of them column-exact — a denominator move

    /** Against the engine's LIVE thrown position (the scraped literals are 40%
     *  mispaired — audit §3.5). Bumped as error positioning improves. */
    private static final int MIN_LINE_AGREEMENT = 413;   // ratcheted to measured 2026-08-14 (deep audit #2 §2e: was 40 with 377 slack); 417 -> 413 on 2026-09-10 (upstream boundary batch 1): 4 negative pins left with the SOURCE pin's move to the 4.138.2 tag (MIN_PINS 424 -> 420); 413 of 419 agree — the same 6 divergences as before, a denominator move

    /** The STRICT drop-in surface: the full parse plus every element site through the
     *  same {@code ElementParser.at} path the byte-comparison uses (protocol-only
     *  constructs like test suites live there). */
    private static void parseLegendEngine(String text) {
        Surfaces.engine(text);
        var ts = com.legend.lexer.Lexer.tokenize(text);
        for (com.legend.lexer.TokenType marker : new com.legend.lexer.TokenType[]{
                com.legend.lexer.TokenType.CLASS, com.legend.lexer.TokenType.ENUM,
                com.legend.lexer.TokenType.PROFILE, com.legend.lexer.TokenType.ASSOCIATION,
                com.legend.lexer.TokenType.FUNCTION}) {
            for (int i : com.legend.parser.ElementParser.topLevelIndexes(ts, marker)) {
                var p = Surfaces.engineAt(ts, i);
                switch (marker) {
                    case CLASS -> p.parseClassDefinition(false);
                    case ENUM -> p.parseEnumDefinition();
                    case PROFILE -> p.parseProfileDefinition();
                    case ASSOCIATION -> p.parseAssociationDefinition();
                    default -> p.parseFunctionProtocol();
                }
            }
        }
    }

    /** Bumped deliberately as extraction improves. Lowering it requires saying why. */
    private static final int MIN_PINS = 420;   // 43 -> 424: the sectioned pins joined 2026-08-13 (REJECT_MISS 37 -> 0); 424 -> 420 on 2026-09-10 (upstream boundary batch 1): the SOURCE pin moved from 4.137.0+36 back to the 4.138.2 TAG, 20 commits of newer test sources left the corpus (C4 engine-inline rows 8,891 -> 8,834 in the manifest) and took 4 negative pins with them — a denominator move, not a parity loss (REJECT_MISS stays 0)

    private List<Pin> extractPins() {
        List<Pin> pins = new ArrayList<>();
        for (Path root : new Path[]{Corpus.engineRoot(), Corpus.pureRoot()}) {
            for (InlineSnippets.FileRuns fr : InlineSnippets.literalRunsByFile(root)) {
                List<String> runs = fr.runs();
                for (int i = 1; i < runs.size(); i++) {
                    Matcher m = ERROR_PIN.matcher(runs.get(i));
                    if (!m.find()) {
                        continue;
                    }
                    String input = runs.get(i - 1);
                    if (input.length() < 10) {
                        continue;                   // not a code snippet
                    }
                    // SECTIONED pins run through the SAME machinery since
                    // 2026-08-13 — the old skip ("non-Pure sections lex
                    // opaquely") predates the strict flip; the document
                    // surface now parses every section strictly, and these
                    // are the engine's own refusal fixtures hunting
                    // OVER-ACCEPTANCE in the thin-policy grammars.
                    String lineGroup = m.group(1) != null ? m.group(1) : m.group(3);
                    String colGroup = m.group(2) != null ? m.group(2) : m.group(4);
                    pins.add(new Pin(fr.id() + "#" + i, input,
                            Integer.parseInt(lineGroup), Integer.parseInt(colGroup)));
                }
            }
        }
        return pins;
    }
}
