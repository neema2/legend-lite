// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * CODE-SHAPE GUARDRAILS — build-failing, not aspirational. Added after
 * the audit-window review found a 1,207-line method and a 4,974-line
 * file that no scoreboard had ever surfaced: the corpus measures
 * behavior, THIS measures whether the next reader can hold the code.
 *
 * <p>The allowlists are a BURN-DOWN ledger, not a loophole: every entry
 * names a known offender with a planned split. Shrink them; never grow
 * them — a new entry needs the same justification a corpus regression
 * would.
 */
class CodeShapeGuardrailTest {

    private static final int METHOD_LIMIT = 250;
    private static final int FILE_LIMIT = 3500;

    /** Known oversized METHODS, pending their planned splits — ceilings
     * at measured size + small slack; SHRINK only. */
    private static final Map<String, Integer> METHOD_ALLOWLIST = Map.of();

    /** Known oversized FILES, pending their planned splits. */
    private static final Map<String, Integer> FILE_ALLOWLIST = Map.of();

    /** Mutable instance fields that are DELIBERATE: hand-rolled parser
     * cursors (Lexer/ElementParser/SpecParser walk positions and scope
     * stacks), per-resolution frames, fresh-name counters and one
     * memo cache. Everything else must be final or become part of an
     * explicit frame object. */
    private static final Set<String> MUTABLE_FIELD_ALLOWLIST = Set.of(
            // parser cursors + scope state
            "Lexer.pos", "Lexer.islandDepth", "Lexer.types", "Lexer.starts",
            "Lexer.ends", "Lexer.count",
            "ElementParser.pos", "ElementParser.currentMappingScope",
            "ElementParser.currentTargetSets", "ElementParser.currentScopeBlock",
            "SpecParser.pos",
            // ';'-ambiguity context for unbraced lambda code blocks: inside
            // call args/collections a ';' is unambiguously the lambda's —
            // a cursor-adjacent depth, same lifecycle as pos
            "SpecParser.boundedDepth",
            // per-resolution frames + counters
            "StoreResolver.freshVarCounter", "StoreResolver.temporal",
            "SyntheticHeads.count", "Lowerer.tdsCounter", "Lowerer.aliasCounter",
            "UserCallInliner.fresh", "Bindings.contravariantDepth",
            // render mode toggle + import memo
            "AnsiSqlRenderer.inlineMode", "ModelOrchestrator.cachedImports",
            // normalizer emission frame: Pipeline IS the frame object; expr
            // is the accumulating pipeline AST
            "Pipeline.expr",
            // regex artifact, NOT mutable: implicitly public-static-final
            // interface constant (interfaces cannot spell 'final' on fields)
            "TokenStreamCursor.IDENTIFIER_TOKENS",
            // nested-class cursors: ExecJson JSON reader, PureDateLiteral
            // date parser (keys are filename-scoped)
            "TestBody.i", "PureDateLiteral.pos");

    private static final Pattern SIG = Pattern.compile(
            "^    (?! )(?:private |public |protected |static |final |synchronized )*"
            + "[\\w.<>\\[\\], ?]+ (\\w+)\\(");

    /** Class-level non-final instance fields, ANY visibility — package-
     * private has no keyword, so the visibility group is optional (audit 15:
     * the private-only pattern let two refactors widen mutable fields out
     * of the scan unaudited). */
    private static final Pattern MUTABLE_FIELD = Pattern.compile(
            "^    (?! )(?:(?:private|protected|public) )?"
            + "(?!static |final |private |protected |public |record |class"
            + " |interface |enum |abstract |sealed |non-sealed )(?! )"
            + "[\\w.<>\\[\\], ?]+ (\\w+)( =.*)?;");

    /** Nested-class fields (8-space indent). A visibility modifier is
     * REQUIRED here to distinguish fields from method locals — declare
     * nested-class fields private/protected/public, never package-private. */
    private static final Pattern NESTED_MUTABLE_FIELD = Pattern.compile(
            "^        (?! )(?:private|protected|public) (?!static |final )(?! )"
            + "[\\w.<>\\[\\], ?]+ (\\w+)( =.*)?;");

    /** Static mutable state is banned outright — NO allowlist. */
    private static final Pattern STATIC_MUTABLE_FIELD = Pattern.compile(
            "^\\s*(?:(?:private|protected|public) )?static (?!final )"
            + "[\\w.<>\\[\\], ?]+ (\\w+)( =.*)?;");

    /** Strip string/char literals and comments so braces inside them
     * never skew the counts (parseDerivedProperty false-positived at
     * 3,064 lines from a brace inside a string). */
    private static String sanitize(String line) {
        StringBuilder out = new StringBuilder(line.length());
        boolean inStr = false;
        boolean inChar = false;
        for (int i = 0; i < line.length(); i++) {
            char c = line.charAt(i);
            if ((inStr || inChar) && c == '\\') {
                i++;
                continue;
            }
            if (!inChar && c == '"') {
                inStr = !inStr;
                continue;
            }
            if (!inStr && c == '\'') {
                inChar = !inChar;
                continue;
            }
            if (!inStr && !inChar) {
                if (c == '/' && i + 1 < line.length()
                        && line.charAt(i + 1) == '/') {
                    break;
                }
                out.append(c);
            }
        }
        return out.toString();
    }

    private static List<Path> mainSources() throws IOException {
        Path root = Path.of("src/main/java");
        try (Stream<Path> s = Files.walk(root)) {
            return s.filter(p -> p.toString().endsWith(".java")).toList();
        }
    }

    @Test
    void noMethodBeyondTheLimit() throws IOException {
        List<String> violations = new ArrayList<>();
        for (Path p : mainSources()) {
            String cls = p.getFileName().toString().replace(".java", "");
            List<String> lines = Files.readAllLines(p);
            for (int i = 0; i < lines.size(); i++) {
                Matcher m = SIG.matcher(lines.get(i));
                if (!m.find() || lines.get(i).contains(";")) {
                    continue;
                }
                int depth = 0;
                boolean started = false;
                int j = i;
                while (j < lines.size()) {
                    String ln = sanitize(lines.get(j));
                    depth += count(ln, '{') - count(ln, '}');
                    if (ln.indexOf('{') >= 0) {
                        started = true;
                    }
                    if (started && depth == 0) {
                        break;
                    }
                    j++;
                }
                int len = j - i + 1;
                String key = cls + "." + m.group(1);
                int limit = METHOD_ALLOWLIST.getOrDefault(key, METHOD_LIMIT);
                if (len > limit) {
                    violations.add(key + " is " + len + " lines (limit "
                            + limit + ") — split it; the numbered-comment"
                            + " sections are the seams");
                }
                i = j;
            }
        }
        assertTrue(violations.isEmpty(), String.join("\n", violations));
    }

    @Test
    void noFileBeyondTheLimit() throws IOException {
        List<String> violations = new ArrayList<>();
        for (Path p : mainSources()) {
            long len = Files.lines(p).count();
            String name = p.getFileName().toString();
            long limit = FILE_ALLOWLIST.getOrDefault(name, FILE_LIMIT);
            if (len > limit) {
                violations.add(name + " is " + len + " lines (limit "
                        + limit + ")");
            }
        }
        assertTrue(violations.isEmpty(), String.join("\n", violations));
    }

    @Test
    void mutableInstanceStateIsExplicit() throws IOException {
        List<String> violations = new ArrayList<>();
        for (Path p : mainSources()) {
            String cls = p.getFileName().toString().replace(".java", "");
            for (String ln : Files.readAllLines(p)) {
                Matcher m = MUTABLE_FIELD.matcher(ln);
                if (!m.find()) {
                    m = NESTED_MUTABLE_FIELD.matcher(ln);
                    if (!m.find()) {
                        continue;
                    }
                }
                // an enum-constant list (EQ, NEQ, GTE;) is commas outside
                // generics — a field's type only holds commas inside <>
                String beforeName = ln.substring(0, m.start(1));
                if (beforeName.contains(",") && !beforeName.contains("<")) {
                    continue;
                }
                if (!MUTABLE_FIELD_ALLOWLIST.contains(cls + "." + m.group(1))) {
                    violations.add(cls + "." + m.group(1)
                            + " is a mutable instance field — make it final,"
                            + " move it into an explicit frame object, or"
                            + " allowlist it WITH the reason");
                }
            }
        }
        assertTrue(violations.isEmpty(), String.join("\n", violations));
    }

    @Test
    void noStaticMutableState() throws IOException {
        List<String> violations = new ArrayList<>();
        for (Path p : mainSources()) {
            String cls = p.getFileName().toString().replace(".java", "");
            for (String ln : Files.readAllLines(p)) {
                Matcher m = STATIC_MUTABLE_FIELD.matcher(ln);
                if (m.find()) {
                    violations.add(cls + "." + m.group(1)
                            + " is STATIC MUTABLE state — no allowlist for"
                            + " this one; make it final or design it away");
                }
            }
        }
        assertTrue(violations.isEmpty(), String.join("\n", violations));
    }

    private static int count(String s, char c) {
        int n = 0;
        for (int i = 0; i < s.length(); i++) {
            if (s.charAt(i) == c) {
                n++;
            }
        }
        return n;
    }
}
