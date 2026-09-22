package com.legend.equivalence;

import com.legend.testing.Repo;
import com.legend.lexer.Lexer;
import com.legend.lexer.TokenStream;
import com.legend.lexer.TokenType;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * HOW BIG IS THE UNFINISHED MIGRATION? (PARSER_COMPLETENESS_PLAN.md §1)
 *
 * <p>legend-lite is supposed to parse ONCE into protocol and transform into the
 * model through {@code FromProtocol} — that is landed for every ###Pure element
 * and never landed for Mapping and Relational, which still carry legacy
 * parsers of their own ({@code MappingGrammarParser}, 1,614 lines;
 * {@code RelationalGrammarParser}, 807). The compiler depends on the legacy
 * side; the byte-parity gate exercises the protocol side. Neither gate sees the
 * other's parser, which is how {@code ~primaryKey} stayed readable by one and
 * unreadable by the other for months.
 *
 * <p>This test does not assert. It measures the divergence in BOTH directions
 * over every corpus file carrying those sections, so "finish the migration" can
 * be costed instead of guessed:
 *
 * <ul>
 *   <li><b>BOTH</b> — the two agree; migrating these files is mechanical.</li>
 *   <li><b>PROTOCOL-ONLY</b> — the protocol parser reads it and the legacy one
 *       does not. Every such file is a construct the compiler cannot see TODAY
 *       and would gain for free by completing the migration.</li>
 *   <li><b>LEGACY-ONLY</b> — the legacy parser reads it and the protocol one
 *       does not. THIS is the real cost: work the protocol parser must learn
 *       before the legacy parser can be deleted.</li>
 *   <li><b>NEITHER</b> — a gap in both; unrelated to the migration.</li>
 * </ul>
 */
class MigrationSizingTest {

    private static final Pattern DUAL_SECTIONS =
            Pattern.compile("(?m)^###(Mapping|Relational)\\b");

    @Test
    void sizeTheUnfinishedProtocolMigration() throws Exception {
        List<Corpus.Source> sources = Corpus.all();
        Assumptions.assumeTrue(!sources.isEmpty(),
                "no corpus on disk — set -Dlegend.engine.root / -Dlegend.pure.root");

        int both = 0;
        int protocolOnly = 0;
        int legacyOnly = 0;
        int neither = 0;
        Map<String, Integer> legacyOnlyWhy = new TreeMap<>();
        Map<String, Integer> protocolOnlyWhy = new TreeMap<>();
        List<String> legacyOnlyFiles = new ArrayList<>();

        for (Corpus.Source src : sources) {
            if (!DUAL_SECTIONS.matcher(src.text()).find()) {
                continue;
            }
            String legacyErr = legacyPathError(src.text());
            String protocolErr = protocolPathError(src.text());
            if (legacyErr == null && protocolErr == null) {
                both++;
            } else if (legacyErr == null) {
                legacyOnly++;
                legacyOnlyWhy.merge(protocolErr, 1, Integer::sum);
                legacyOnlyFiles.add(src.id() + " :: " + protocolErr);
            } else if (protocolErr == null) {
                protocolOnly++;
                protocolOnlyWhy.merge(legacyErr, 1, Integer::sum);
            } else {
                neither++;
            }
        }

        StringBuilder b = new StringBuilder();
        b.append("UNFINISHED MIGRATION — legacy model parser vs protocol parser\n")
                .append("=".repeat(72)).append('\n')
                .append(String.format("files with ###Mapping/###Relational : %d%n",
                        both + protocolOnly + legacyOnly + neither))
                .append(String.format("  BOTH read it                     : %d"
                        + "   (mechanical to migrate)%n", both))
                .append(String.format("  PROTOCOL-ONLY                    : %d"
                        + "   (compiler GAINS these for free)%n", protocolOnly))
                .append(String.format("  LEGACY-ONLY                      : %d"
                        + "   (THE COST: protocol must learn these)%n", legacyOnly))
                .append(String.format("  NEITHER                          : %d%n", neither));

        b.append("\nTHE COST — what the protocol parser must learn\n")
                .append("-".repeat(72)).append('\n');
        legacyOnlyWhy.entrySet().stream()
                .sorted((x, y) -> y.getValue() - x.getValue())
                .forEach(e -> b.append(String.format("  %5d  %s%n",
                        e.getValue(), e.getKey())));

        b.append("\nTHE GAIN — constructs the compiler cannot see today\n")
                .append("-".repeat(72)).append('\n');
        protocolOnlyWhy.entrySet().stream()
                .sorted((x, y) -> y.getValue() - x.getValue())
                .limit(20)
                .forEach(e -> b.append(String.format("  %5d  %s%n",
                        e.getValue(), e.getKey())));

        Files.writeString(Repo.out("migration-sizing.txt"), b.toString());
        Files.writeString(Repo.out("migration-legacy-only.txt"),
                String.join("\n", legacyOnlyFiles));
        System.out.println(b);
    }

    /** The path the COMPILER uses. */
    private static String legacyPathError(String text) {
        try {
            Surfaces.engine(text);
            return null;
        } catch (Throwable t) {
            return normalize(t);
        }
    }

    /** The path the BYTE-PARITY gate uses: every Mapping/Database site through
     *  the protocol parsers, exactly as ParserEquivalence drives them. */
    private static String protocolPathError(String text) {
        try {
            TokenStream ts = Lexer.tokenize(text);
            for (int i = 0; i < ts.count(); i++) {
                if (ts.type(i) == TokenType.MAPPING && atDeclarationPosition(ts, i)) {
                    // AggregationAware span emulation needs the enclosing
                    // section's first content line, exactly as ParserEquivalence
                    // computes it — passing -1 fabricates 31 false "costs"
                    Surfaces.platformMapping(ts, i,
                            sectionStartLine(text, ts.start(i)));
                } else if (ts.type(i) == TokenType.DATABASE
                        && atDeclarationPosition(ts, i)) {
                    Surfaces.databaseAt(ts, i);
                }
            }
            return null;
        } catch (Throwable t) {
            return normalize(t);
        }
    }

    /** The line AFTER the {@code ###Mapping} header enclosing this offset. */
    private static int sectionStartLine(String text, int offset) {
        Matcher m = Pattern.compile("(?m)^###Mapping\\b").matcher(text);
        int header = -1;
        while (m.find() && m.start() < offset) {
            header = m.start();
        }
        if (header < 0) {
            return -1;
        }
        int line = 1;
        for (int i = 0; i < header; i++) {
            if (text.charAt(i) == '\n') {
                line++;
            }
        }
        return line + 1;
    }

    /** A header token only starts an element at depth 0 after a terminator. */
    private static boolean atDeclarationPosition(TokenStream ts, int i) {
        if (i == 0) {
            return true;
        }
        TokenType prev = ts.type(i - 1);
        return prev == TokenType.BRACE_CLOSE || prev == TokenType.SEMI_COLON
                || prev == TokenType.PAREN_CLOSE;
    }

    private static String normalize(Throwable t) {
        Throwable root = t;
        while (root.getCause() != null && root.getCause() != root) {
            root = root.getCause();
        }
        String msg = String.valueOf(root.getMessage()).replaceAll("\\s+", " ")
                .replaceAll("\\[\\d+:\\d+\\]", "[L:C]");
        return msg.substring(0, Math.min(100, msg.length()));
    }
}
