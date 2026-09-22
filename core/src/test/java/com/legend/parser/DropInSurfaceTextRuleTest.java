package com.legend.parser;

import com.legend.testing.Repo;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * THE TEXT RULE over the drop-in surface {@code {lexer, parser, protocol}}
 * (text-surgery audit 2026-08 + user mandate 2026-08-07): a regex inside a
 * hand-written recursive-descent parser is a second, undocumented grammar
 * running beside the first — with its own idea of what an identifier is and
 * no token positions. Byte parity is a property of the token parser; every
 * regex is surface that property does not cover.
 *
 * <p>Three rules, all ratcheted DOWN only:
 * <ol>
 *   <li>{@code com.legend.lexer}: ZERO regex-family calls. Unconditional.</li>
 *   <li>{@code com.legend.parser} + {@code com.legend.protocol}: regex-family
 *       calls confined to the NAMED whitelist below, frozen at the 2026-08-07
 *       census; the target is an empty whitelist (then rule 1 covers all
 *       three packages).</li>
 *   <li>{@code com.legend.parser}: {@code tokens.source()} reads (re-reading
 *       chars the lexer already tokenized) confined to their own whitelist —
 *       the assertion-island reparse emulation legitimately needs raw chars
 *       because it reproduces the engine's raw-text mechanism
 *       (HelperTestAssertionGrammarParser); the audit §5.5 re-parse sites do
 *       not, and burn.</li>
 * </ol>
 *
 * <p>Lexeme extraction ({@code substring}, {@code charAt}, the escape
 * decoder) is the parser's JOB, not surgery, and is deliberately not policed
 * here — the enforceable line is the regex family plus source re-reads.
 */
class DropInSurfaceTextRuleTest {

    /** The regex family: each match is one shadow-grammar call site. */
    private static final Pattern TEXT_SURGERY = Pattern.compile(
            "Pattern\\.compile|Pattern\\.matches|Pattern\\.quote"
                    + "|\\.matches\\(|\\.replaceAll\\(|\\.replaceFirst\\(|\\.split\\(");

    private static final Pattern SOURCE_REREAD = Pattern.compile(
            "tokens\\.source\\(\\)|tokens\\(\\)\\.source\\(\\)");

    /** 2026-08-07 census — file -> allowed regex-family call sites. DOWN only;
     *  a new site in a clean file or a count above the frozen number fails. */
    // EMPTY 2026-08-07: the whole drop-in surface {lexer, parser, protocol}
    // is at ZERO regex-family call sites — the rule is now unconditional.
    private static final Map<String, Integer> REGEX_WHITELIST = Map.of();

    /** 2026-08-07 census — parser files allowed to read raw source chars.
     *  PERMANENT, not debt (adjudicated per audit §5.5 after the depth-aware
     *  island-scan fix): every remaining site is either engine-mechanism
     *  emulation (the engine ALSO extracts island text and reparses raw
     *  chars — assertion-island spans, ExternalFormat/CSV island bodies) or
     *  coarse-chunk necessity (the island lexer emits content as chunks;
     *  names live INSIDE chunk strings, so graph-fetch span scanning must
     *  walk chars between token-true bounds). What burned was the DEFECT:
     *  flat island scans that a nested #{ }# truncated. New sites still
     *  need a whitelist edit — and this comment's justification. */
    private static final Map<String, Integer> SOURCE_WHITELIST = Map.of(
            // 5 island-content reads + 1 OVERLAY handoff: an opaque overlay
            // section is raw-skipped by the lexer, so its SectionSource text
            // has no token form to read from — the raw slice IS the SPI
            // contract (Phase M step 2)
            // 5 -> 6 (2026-08-09, Diagram RAW grammar): a raw-skipped
            // section's text has no token form — the SectionSource handoff
            // to a RawSectionGrammar is the same necessity as the OVERLAY
            // handoff above.
            "parser/ElementParser.java", 6,
            // reconstructText itself — MOVED here from ElementParser
            // (2026-08-09, Runtime migration) so section grammars outside
            // com.legend.parser can extract island chunks; the one source
            // read is the same coarse-chunk necessity it always was
            "parser/TokenStreamCursor.java", 1,
            // 2: the graph-fetch island + the GQL island — GraphQL is a
            // FOREIGN language (commas-as-whitespace, $, @, ...), so its
            // content parses from the raw slice by design, like the URL
            // fix in the ES connection
            "parser/SpecParser.java", 2,
            // ISLAND-CONTENT re-lex: mapping test-suite #{ }# data blocks
            // arrive as RAW island chunks — the content is re-lexed with
            // position padding (the engine reparses the same text via its
            // data sub-parsers); same category as ElementParser's island
            // reads; +1: aggregate-lambda padded re-lex (engine reparses
            // the expr text standalone with the tilde-anchor quirk);
            // +1: Relation-island CSV char walk (a raw sub-format the
            // engine also parses from chars)
            // +1 (phase M): the VERBATIM testSuites slice.
            // LegacyMappingDefinition stores testSuites as RAW TEXT because
            // Phase C parses them lazily, and parsed protocol cannot be
            // turned back into source — so the transform needs the source
            // span, exactly as MappingGrammarParser:83-88 reconstructs it
            // today. This is the same necessity as the OVERLAY handoff
            // above, and it DIES with the legacy mapping parser only if the
            // model stops storing suites as text.
            "parser/MappingProtocolParser.java", 4);

    @Test
    void regexFamilyIsConfinedToTheFrozenWhitelist() throws IOException {
        Map<String, Integer> found = census(TEXT_SURGERY,
                "lexer", "parser", "protocol");
        for (Map.Entry<String, Integer> e : found.entrySet()) {
            assertTrue(e.getKey().startsWith("parser/")
                            || e.getKey().startsWith("protocol/"),
                    "REGEX IN THE LEXER — the one package that was clean: "
                            + e.getKey() + " (" + e.getValue() + " sites)");
            int allowed = REGEX_WHITELIST.getOrDefault(e.getKey(), 0);
            assertTrue(e.getValue() <= allowed,
                    "regex-family sites GREW in " + e.getKey() + ": "
                            + e.getValue() + " > frozen " + allowed
                            + " — the drop-in surface does not grow shadow"
                            + " grammars; parse from tokens instead");
        }
    }

    @Test
    void sourceRereadsAreConfinedToTheFrozenWhitelist() throws IOException {
        Map<String, Integer> found = census(SOURCE_REREAD, "parser");
        for (Map.Entry<String, Integer> e : found.entrySet()) {
            int allowed = SOURCE_WHITELIST.getOrDefault(e.getKey(), 0);
            assertTrue(e.getValue() <= allowed,
                    "raw source() re-reads GREW in " + e.getKey() + ": "
                            + e.getValue() + " > frozen " + allowed
                            + " — the lexer already tokenized that region");
        }
    }

    @Test
    void whitelistsShrinkWithTheCode() throws IOException {
        // the ratchet's other edge: a burned site must LEAVE the whitelist,
        // so the frozen numbers cannot quietly become slack for new sites
        Map<String, Integer> regex = census(TEXT_SURGERY,
                "lexer", "parser", "protocol");
        for (Map.Entry<String, Integer> e : REGEX_WHITELIST.entrySet()) {
            assertEquals(e.getValue(), regex.getOrDefault(e.getKey(), 0),
                    "whitelist stale for " + e.getKey()
                            + " — sites were burned; lower the frozen count");
        }
        Map<String, Integer> src = census(SOURCE_REREAD, "parser");
        for (Map.Entry<String, Integer> e : SOURCE_WHITELIST.entrySet()) {
            assertEquals(e.getValue(), src.getOrDefault(e.getKey(), 0),
                    "source-reread whitelist stale for " + e.getKey()
                            + " — sites were burned; lower the frozen count");
        }
    }

    private static Map<String, Integer> census(Pattern needle, String... pkgs)
            throws IOException {
        Path root = Repo.module("src", "main", "java", "com", "legend");
        assertTrue(Files.isDirectory(root), "core's sources are not visible at " + root
                + " (Bazel: declare them as data of the test target)");
        Map<String, Integer> out = new TreeMap<>();
        for (String pkg : pkgs) {
            try (Stream<Path> files = Files.walk(root.resolve(pkg))) {
                for (Path f : files.filter(p -> p.toString().endsWith(".java")).toList()) {
                    int n = 0;
                    for (String line : List.copyOf(Files.readAllLines(f))) {
                        String code = line.strip();
                        if (code.startsWith("//") || code.startsWith("*")
                                || code.startsWith("/*")) {
                            continue;               // prose is not a call site
                        }
                        var m = needle.matcher(line);
                        while (m.find()) {
                            n++;
                        }
                    }
                    if (n > 0) {
                        out.put(pkg + "/" + f.getFileName(), n);
                    }
                }
            }
        }
        return out;
    }
}
