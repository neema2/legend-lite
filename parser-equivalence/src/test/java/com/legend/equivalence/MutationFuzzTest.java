// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.equivalence;

import org.finos.legend.engine.language.pure.grammar.from.PureGrammarParser;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * MUTATION differential fuzz — the sibling handoff's mutants harness as
 * a LIVE standing gate (adopted 2026-08-14). Their manifest recorded the
 * engine's verdict out-of-process; the oracle lives in THIS JVM, so the
 * mutants are generated deterministically and verdict-compared directly —
 * no manifest to rot.
 *
 * <p>Seeds are the vendored positive fixtures (51, all byte-parity
 * green). Operators are text-level and POSITIONAL (no randomness — the
 * same run every time), a port of the handoff's highest-signal set:
 * field deletion/duplication/swap/semicolon, island delimiters,
 * truncation, final-delimiter drop, package separator, keyword case,
 * empty bodies. A mutant is interesting only for its VERDICT: both
 * accept or both refuse = agreement (byte parity of accepted mutants is
 * the corpus sweep's job, not this one's); a split is a divergence and
 * must be adjudicated in {@link #ADJUDICATED} or fixed.
 */
class MutationFuzzTest {

    private static final int MAX_SITES = 3;

    /** F3.7 total accounting: the exact mutant-deck size (deterministic —
     *  fixtures x operators x sites). A fixture or operator change moves
     *  this number DELIBERATELY, in the same commit. */
    private static final int TOTAL_MUTANTS = 950;   // measured 2026-08-16

    /** Divergence classes reviewed 2026-08-14, keyed
     *  {@code operator :: fixture}. Shrink-only. */
    private static final java.util.Set<String> ADJUDICATED =
            java.util.Set.of(
                    // F19 (2026-08-12 quarantine review; its tsv is
                    // deleted, F3.7): the ENGINE tolerates an
                    // unterminated final element in the Connection-family
                    // sections; lite requires balance. Lite is RIGHT.
                    "drop-final-delimiter :: connection-auth.pure",
                    "drop-final-delimiter :: connection-model.pure",
                    "drop-final-delimiter :: connection-postprocessor.pure",
                    "drop-final-delimiter :: connection-snowflake.pure",
                    "drop-final-delimiter :: t2-authentication.pure",
                    "drop-final-delimiter :: t2-databricks.pure",
                    "drop-final-delimiter :: t2-deephaven.pure",
                    "drop-final-delimiter :: t2-duckdb.pure",
                    // the same F19 engine tolerance, the three vendor-spec
                    // fixtures whose connection is the file's LAST element
                    // (2026-09-16, gate 8 first run after the Athena/Aurora/
                    // GlobalAurora/MemSql/Oracle specs landed)
                    "drop-final-delimiter :: t2-athena.pure",
                    "drop-final-delimiter :: t2-aurora.pure",
                    "drop-final-delimiter :: t2-oracle.pure",
                    "drop-final-delimiter :: t2-elasticsearch.pure",
                    "drop-final-delimiter :: t2-mongodb.pure",
                    "drop-final-delimiter :: t2-redshift.pure",
                    "drop-final-delimiter :: t2-spanner.pure",
                    "drop-final-delimiter :: t2-trino.pure",
                    // same engine tolerance reached by an 85% cut that
                    // lands inside the final Connection element
                    "truncate :: connection-auth.pure");

    /** ONE oracle instance for all 950 mutants — newInstance() rebuilds
     *  the extension list per call (the sweep's own precedent; the
     *  per-mutant spelling cost ~seconds of gate 8). */
    private static final PureGrammarParser ORACLE =
            PureGrammarParser.newInstance();

    @Test
    void mutantVerdictParity() throws Exception {
        List<String> divergences = new ArrayList<>();
        List<String> internal = new ArrayList<>();
        java.util.Set<String> pardonedSeen = new java.util.TreeSet<>();
        int agreed = 0;
        int total = 0;
        for (String name : listFixtures()) {
            String src = readFixture(name);
            for (Mutant m : mutants(src)) {
                total++;
                boolean engineAccepts;
                try {
                    ORACLE.parseModel(m.text());
                    engineAccepts = true;
                } catch (Throwable t) {
                    engineAccepts = false;
                }
                boolean liteAccepts;
                try {
                    com.legend.parser.PmcdParser.parseDocument(m.text());
                    liteAccepts = true;
                } catch (com.legend.parser.ParseException e) {
                    liteAccepts = false;
                } catch (Throwable t) {
                    internal.add(m.op() + " :: " + name + " :: " + t);
                    continue;
                }
                if (engineAccepts == liteAccepts) {
                    agreed++;
                } else if (ADJUDICATED.contains(m.op() + " :: " + name)) {
                    pardonedSeen.add(m.op() + " :: " + name);
                } else {
                    divergences.add(m.op() + " :: " + name + " ["
                            + m.site() + "] engine="
                            + (engineAccepts ? "ACCEPTS" : "REFUSES"));
                }
            }
        }
        assertEquals(List.of(), internal,
                "lite threw a NON-ParseException on a mutant");
        assertEquals(List.of(), divergences.stream().sorted().toList(),
                "mutant verdict divergences (agreed " + agreed + "/" + total
                        + ")");
        // F3.7: stale-row assert — a pardon that no longer diverges must
        // LEAVE the ledger (the FixtureCorpusParity shape)
        List<String> stale = ADJUDICATED.stream()
                .filter(r -> !pardonedSeen.contains(r)).sorted().toList();
        assertEquals(List.of(), stale,
                "ADJUDICATED mutants now agree — remove their rows");
        // F3.7: total accounting — the mutant deck is deterministic, so a
        // silent shrink (a fixture or operator dropping out) is a review
        // event, not a pass
        assertEquals(TOTAL_MUTANTS, total,
                "mutant-deck accounting drifted — re-pin with review");
        assertEquals(total - pardonedSeen.size(), agreed,
                "mutant accounting drifted (agreed + pardoned != total)");
    }

    // ------------------------------------------------------------------
    // Deterministic operators (positional; comment/string-guarded where
    // the mutation would otherwise land in prose)
    // ------------------------------------------------------------------

    private record Mutant(String op, String site, String text) {
    }

    private static List<Mutant> mutants(String src) {
        List<Mutant> out = new ArrayList<>();
        String[] lines = src.split("\n", -1);
        List<Integer> fields = fieldLines(lines);
        int n = 0;
        for (int i : fields) {
            if (n++ >= MAX_SITES) {
                break;
            }
            out.add(new Mutant("delete-field", lines[i].strip(),
                    joinWithout(lines, i)));
            out.add(new Mutant("duplicate-field", lines[i].strip(),
                    joinDuplicating(lines, i)));
            String dropped = lines[i].stripTrailing();
            if (dropped.endsWith(";")) {
                String[] copy = lines.clone();
                copy[i] = dropped.substring(0, dropped.length() - 1);
                out.add(new Mutant("drop-semicolon", lines[i].strip(),
                        String.join("\n", copy)));
            }
        }
        n = 0;
        for (int i = 0; i + 1 < lines.length; i++) {
            if (fields.contains(i) && fields.contains(i + 1)
                    && n < MAX_SITES) {
                n++;
                String[] copy = lines.clone();
                String t = copy[i];
                copy[i] = copy[i + 1];
                copy[i + 1] = t;
                out.add(new Mutant("swap-siblings", lines[i].strip(),
                        String.join("\n", copy)));
            }
        }
        // island delimiters (code-guarded: skip lines that are comments)
        addOffsetOp(out, src, "}#", "}", "island-close-plain");
        addOffsetOp(out, src, "#{", "{", "island-open-plain");
        // truncation
        out.add(new Mutant("truncate", "50%",
                src.substring(0, src.length() / 2)));
        out.add(new Mutant("truncate", "85%",
                src.substring(0, (int) (src.length() * 0.85))));
        // final delimiter
        String trimmed = src.stripTrailing();
        if (trimmed.endsWith(")") || trimmed.endsWith("}")) {
            out.add(new Mutant("drop-final-delimiter",
                    trimmed.substring(trimmed.length() - 1),
                    trimmed.substring(0, trimmed.length() - 1)));
        }
        // package separator
        addOffsetOp(out, src, "::", ".", "package-dot");
        // keyword lowercase: first capitalised word of length >= 4 at a
        // line start (an element keyword, deterministically the first)
        for (int i = 0; i < lines.length; i++) {
            String s = lines[i].strip();
            if (s.length() >= 4 && Character.isUpperCase(s.charAt(0))
                    && s.chars().limit(4).allMatch(Character::isLetter)
                    && !lines[i].startsWith("//")) {
                String[] copy = lines.clone();
                copy[i] = lines[i].replaceFirst(
                        java.util.regex.Pattern.quote(s.substring(0, 1)),
                        s.substring(0, 1).toLowerCase(java.util.Locale.ROOT));
                out.add(new Mutant("keyword-lowercase", s.split("\\s")[0],
                        String.join("\n", copy)));
                break;
            }
        }
        return out;
    }

    /** A "field line": ends with ';' at nesting depth >= 1, not a
     *  comment — the same heuristic as the handoff's _field_lines. */
    private static List<Integer> fieldLines(String[] lines) {
        List<Integer> out = new ArrayList<>();
        int depth = 0;
        for (int i = 0; i < lines.length; i++) {
            String s = lines[i].strip();
            boolean comment = s.startsWith("//") || s.startsWith("*");
            if (!comment && depth >= 1 && s.endsWith(";")
                    && !s.contains("{") && !s.contains("}")
                    && !s.contains("#")) {
                out.add(i);
            }
            for (char c : lines[i].toCharArray()) {
                if (c == '{' || c == '(') {
                    depth++;
                } else if (c == '}' || c == ')') {
                    depth--;
                }
            }
        }
        return out;
    }

    private static void addOffsetOp(List<Mutant> out, String src,
            String needle, String replacement, String op) {
        int from = 0;
        int n = 0;
        while (n < MAX_SITES) {
            int i = src.indexOf(needle, from);
            if (i < 0) {
                break;
            }
            int lineStart = src.lastIndexOf('\n', i) + 1;
            String linePrefix = src.substring(lineStart, i).strip();
            if (!linePrefix.startsWith("//")) {
                out.add(new Mutant(op, "offset " + i,
                        src.substring(0, i) + replacement
                                + src.substring(i + needle.length())));
                n++;
            }
            from = i + needle.length();
        }
    }

    private static String joinWithout(String[] lines, int i) {
        StringBuilder b = new StringBuilder();
        for (int k = 0; k < lines.length; k++) {
            if (k == i) {
                continue;
            }
            if (b.length() > 0) {
                b.append('\n');
            }
            b.append(lines[k]);
        }
        return b.toString();
    }

    private static String joinDuplicating(String[] lines, int i) {
        StringBuilder b = new StringBuilder();
        for (int k = 0; k < lines.length; k++) {
            if (b.length() > 0) {
                b.append('\n');
            }
            b.append(lines[k]);
            if (k == i) {
                b.append('\n').append(lines[k]);
            }
        }
        return b.toString();
    }

    private static List<String> listFixtures() throws Exception {
        // listed as FILES of this module's test resources, not by turning the
        // classpath URL into a Path: under Bazel the resources are packed in a
        // jar, and a jar: URI has no default FileSystem (2026-09-22)
        var path = com.legend.testing.Repo.module(
                "src/test/resources/sibling-corpus/fixtures");
        try (var files = java.nio.file.Files.list(path)) {
            return files.map(p -> p.getFileName().toString())
                    .filter(f -> f.endsWith(".pure")).sorted().toList();
        }
    }

    private static String readFixture(String name) throws Exception {
        try (var in = MutationFuzzTest.class.getResourceAsStream(
                "/sibling-corpus/fixtures/" + name)) {
            return new String(java.util.Objects.requireNonNull(in)
                    .readAllBytes(), java.nio.charset.StandardCharsets.UTF_8);
        }
    }
}
