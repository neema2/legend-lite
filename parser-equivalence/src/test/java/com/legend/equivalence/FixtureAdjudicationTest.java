// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.equivalence;

import org.finos.legend.engine.language.pure.grammar.from.PureGrammarParser;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * OUR OWN FIXTURES, ADJUDICATED BY THE REAL ENGINE PARSER.
 *
 * <p>Every other tier points the reference parser at legend-engine's and
 * legend-pure's files. This one points it at OURS. That matters because a
 * corpus sweep structurally cannot find a disagreement about a form the
 * corpus never contains: engine's files never write a bare {@code ~filter}
 * on a class mapping, an anonymous {@code EnumerationMapping:}, or a bare
 * column under {@code ~mainTable}, so all three of those leniencies lived
 * for months PINNED BY OUR OWN TESTS with no sweep able to see them. The
 * only oracle that can is the reference parser reading our fixtures.
 *
 * <p><b>The report splits by assertThrows context, because the two halves
 * mean opposite things:</b>
 * <ul>
 *   <li>a NEGATIVE fixture (inside {@code assertThrows}) that engine
 *       ACCEPTS is OUR OVER-STRICTNESS — we refuse valid Legend;</li>
 *   <li>a POSITIVE fixture that engine REFUSES is OUR LENIENCY — we accept
 *       what Legend does not define.</li>
 * </ul>
 *
 * <p><b>Two things this cannot settle, and does not pretend to.</b> Until
 * 2026-09-10 the oracle jars (5.88.1, then 5.92.0) sat against a
 * non-release checkout (5.92.1-SNAPSHOT / 4.137.0+36), so some apparent
 * leniency was version skew — a form the newer grammar added. The jars and
 * the checkout are now ONE release (tools/oracle-pins.env), so that
 * explanation is gone: what remains is ours to adjudicate.
 * And legend-engine is not the only Legend: legend-PURE's compiler accepts
 * things engine's ANTLR grammar rejects (a missing comma in an xStore
 * mapping is the live example, and the corpus depends on the lenient
 * reading). So a row here is a QUESTION TO ADJUDICATE, not a verdict —
 * which is why the ratchets below bound the counts rather than requiring
 * zero.
 */
class FixtureAdjudicationTest {

    /**
     * Ratchets — DEBT CEILINGS measured at introduction (2026-08-08), not
     * targets. Lower them; do not raise them without naming the cluster.
     *
     * <p><b>What the 268 lenient rows actually are</b>, clustered by the
     * reference parser's own message, so the next person starts from the
     * analysis rather than the number:
     * <pre>
     *   105  Unsupported Data Source Specification type   InMemory &c —
     *          legend-lite test connection specs engine has no extension for
     *    39  Unexpected token (bare)                      mixed; needs eyes
     *    33  Unexpected token, 8 alternatives             mixed
     *    18  Unexpected token, 3 alternatives             mixed
     *    13  Column data type VARCHAR requires 1 parameter (size)
     *    12  no viable alternative at input
     *    12  Type/multiplicity parameters not authorized in Legend Engine
     *     8  Field 'X' is required                        walker-level
     *     5  Unsupported column data type
     *     2  The type {T[1]->U[1]} is not supported yet
     *     2  No parser for AssociationMapping             our clean-sheet form
     * </pre>
     *
     * <p><b>The ratchet counts KINDS, not fixtures.</b> A first cut counted
     * fixtures and immediately taxed test-writing: adding two legitimate
     * tests for legend-lite's Variant accessor pushed 268 to 270 and turned
     * the gate red for writing tests of an ALREADY-COUNTED construct. What
     * should fail the build is a NEW KIND of disagreement, so the ceiling is
     * on the set of distinct reference-parser messages (literals stripped).
     * The per-fixture list is still printed, because that is what you act on.
     *
     * <p><b>The one to chase first is VARCHAR.</b> Bare {@code VARCHAR}
     * without a size is a construct legend-engine owns and requires a
     * parameter for — no carve-out applies, so those 13 are the same family
     * as the three leniencies this test exists to have caught. The
     * {@code Type/multiplicity parameters} and {@code {T[1]->U[1]}} rows are
     * the OPPOSITE: legend-pure defines them and engine subsets them away,
     * which is the defensible superset.
     *
     * <p><b>Why over-strictness is only 6, and why they may all be fine.</b>
     * {@code parseModel} PARSES; it does not compile. Engine defers a great
     * deal to its compiler — a duplicate element, a derived property naming
     * a missing function, a column with no reachable database — so a fixture
     * we refuse at parse time and engine accepts at parse time is usually
     * refused by engine a phase later, not accepted. All six current rows
     * have that shape. Treat this direction as a prompt to check WHERE the
     * rejection happens, not as evidence we refuse valid Legend.
     */
    /** F3.7: the adjudicated leniency KINDS, pinned BY NAME (a bare
     *  ceiling left silent headroom — kinds could vanish and be replaced
     *  by NEW unreviewed kinds under the same count). A new kind fails;
     *  a vanished kind fails until its row is removed here. */
    private static final java.util.Set<String> LENIENCY_KINDS =
            java.util.Set.of(   // measured 2026-08-16: 10 kinds (the old
                                // ceiling of 21 was stale by ELEVEN)
                    "No parser for AssociationMapping",
                    "The type {T[N]->U[N]} is not supported yet",
                    "Type and/or multiplicity parameters are not authorized"
                            + " in Legend Engine",
                    "Unexpected token",
                    "Unexpected token 'X'. Valid alternatives: ['X', 'X',"
                            + " 'X', 'X', 'X', 'X', 'X', 'X', 'X', 'tags…",
                    "Unexpected token 'X'. Valid alternatives: ['X', 'X',"
                            + " 'X', 'X', 'X', 'X', 'X', 'X', 'X'…",
                    // the same family, truncated one token earlier: a
                    // one-character offending token ('+') shifts the
                    // oracle's message cut into the tenth alternative
                    // (2026-09-04)
                    "Unexpected token 'X'. Valid alternatives: ['X', 'X',"
                            + " 'X', 'X', 'X', 'X', 'X', 'X', 'X', '…",
                    "Unexpected token 'X'. Valid alternatives: ['X', 'X',"
                            + " 'X', 'X', 'X', 'X', 'X', 'X']",
                    "Unexpected token 'X'. Valid alternatives: ['X', 'X',"
                            + " 'X']",
                    "Unexpected token 'X'. Valid alternatives: ['X']",
                    "no viable alternative at input 'X'");

    /** F3.7: over-strictness rows pinned per HOST FILE (fixture ids are
     *  line-positional, so exact ids would tax unrelated edits). */
    private static final java.util.Map<String, Integer> OVER_STRICT_PINS =
            java.util.Map.of(   // measured 2026-08-16: 5 rows (the old
                                // ceiling of 6 was stale by one)
                    "ElementParserTest.java", 2,
                    "ModelIndexerTest.java", 1,
                    "PureModelContextTest.java", 2);

    /** A leniency KIND: the reference's message with literals stripped, so
     *  ten fixtures of one construct count once. */
    private static String kindOf(String message) {
        return oneLine(message).replaceAll("'[^']*'", "'X'")
                .replaceAll("\\d+", "N");
    }

    /** The section a top-level keyword belongs to. */
    private static String sectionOf(String keyword) {
        return switch (keyword) {
            case "Mapping" -> "Mapping";
            case "Database" -> "Relational";
            case "Runtime", "SingleConnectionRuntime" -> "Runtime";
            case "Service" -> "Service";
            case "Data" -> "Data";
            case "RelationalDatabaseConnection", "JsonModelConnection",
                 "XmlModelConnection", "ModelChainConnection" -> "Connection";
            case "Class", "Enum", "Association", "Profile", "Measure",
                 "function", "native", "Primitive", "import" -> "Pure";
            default -> null;
        };
    }

    private record Fixture(String id, String source, boolean negative) {
    }

    @Test
    void adjudicateOurFixturesAgainstTheReferenceParser() {
        List<Fixture> fixtures = new ArrayList<>();
        int files = 0;
        int runs = 0;
        int unadjudicable = 0;
        for (Path p : liteTestSources()) {
            files++;
            String text;
            try {
                text = Files.readString(p);
            } catch (Exception e) {
                continue;               // non-UTF8; visible in the counters
            }
            for (Run r : literalRuns(text)) {
                runs++;
                String wrapped = withSectionHeaders(r.text());
                if (wrapped == null) {
                    unadjudicable++;
                    continue;
                }
                fixtures.add(new Fixture(p.getFileName() + "#" + r.line(),
                        wrapped, isNegativeContext(text, r.start())));
            }
        }

        PureGrammarParser reference = PureGrammarParser.newInstance();
        List<String> leniency = new ArrayList<>();
        List<String> overStrict = new ArrayList<>();
        java.util.Set<String> kinds = new java.util.TreeSet<>();
        int agree = 0;
        for (Fixture f : fixtures) {
            boolean engineAccepts;
            String why = "";
            try {
                reference.parseModel(f.source());
                engineAccepts = true;
            } catch (Exception e) {
                engineAccepts = false;
                why = String.valueOf(e.getMessage());
            }
            if (f.negative() && engineAccepts) {
                overStrict.add(f.id() + " :: " + oneLine(f.source()));
            } else if (!f.negative() && !engineAccepts) {
                leniency.add(f.id() + " :: " + oneLine(why)
                        + " :: " + oneLine(f.source()));
                kinds.add(kindOf(why));
            } else {
                agree++;
            }
        }

        // Say WHOSE files and WHAT is being counted. An earlier wording put
        // a bare "24802 not" next to CorpusEquivalenceTest's "25472
        // MATCH" in the same gate, and the two read as the same quantity —
        // they are not remotely: that one counts legend-engine ELEMENTS
        // byte-compared, this one counts JAVA STRING LITERALS in
        // legend-lite's own test tree that are not Pure source at all.
        System.out.println("[fixture-oracle] scanned " + files
                + " of OUR OWN test files: " + fixtures.size()
                + " Pure fixtures adjudicable"
                + " (of " + runs + " java string literals, " + unadjudicable
                + " are not Pure source — prose, SQL, JSON, fragments)");
        System.out.println("[fixture-oracle] NB these are legend-lite test"
                + " fixtures, NOT corpus elements — CorpusEquivalenceTest"
                + " owns the element counts");
        System.out.println("[fixture-oracle] agree " + agree
                + " | OUR LENIENCY " + leniency.size() + " fixtures in "
                + kinds.size() + " KINDS"
                + " | OUR OVER-STRICTNESS " + overStrict.size());
        System.out.println("[fixture-oracle] --- LENIENCY KINDS (what the"
                + " ratchet guards) ---");
        kinds.forEach(k -> System.out.println("[fixture-oracle][kind] " + k));
        System.out.println("[fixture-oracle] --- OUR LENIENCY (positive"
                + " fixtures the reference REFUSES) ---");
        leniency.forEach(s ->
                System.out.println("[fixture-oracle][lenient] " + s));
        System.out.println("[fixture-oracle] --- OUR OVER-STRICTNESS"
                + " (assertThrows fixtures the reference ACCEPTS) ---");
        overStrict.forEach(s ->
                System.out.println("[fixture-oracle][strict] " + s));

        // F3.7: exact named-set accounting, both directions
        org.junit.jupiter.api.Assertions.assertEquals(
                new java.util.TreeSet<>(LENIENCY_KINDS), kinds,
                "leniency KINDS drifted — a NEW kind needs adjudication;"
                        + " a VANISHED kind is a stale pin, remove it in"
                        + " the same commit");
        java.util.Map<String, Integer> overStrictByFile =
                new java.util.TreeMap<>();
        for (String s : overStrict) {
            overStrictByFile.merge(
                    s.substring(0, s.indexOf('#')), 1, Integer::sum);
        }
        org.junit.jupiter.api.Assertions.assertEquals(
                new java.util.TreeMap<>(OVER_STRICT_PINS), overStrictByFile,
                "over-strictness rows drifted — GROWTH needs adjudication"
                        + " (check WHERE engine rejects, parse vs compile);"
                        + " SHRINKAGE means ratchet the pin down");
    }

    /** legend-lite's own test tree — this module's parent, per Corpus's
     *  root convention. */
    private static List<Path> liteTestSources() {
        Path root = Path.of(System.getProperty("legend.lite.root",
                Path.of("").toAbsolutePath().getParent() == null
                        ? "." : Path.of("").toAbsolutePath().getParent()
                                .toString()));
        List<Path> out = new ArrayList<>();
        // TWO modules, both REQUIRED: the "engine" module was deleted
        // (cd31b9f3) and the silent isDirectory-skip made this walk half-dead
        // (deep-audit #2); batch 7b (2026-09-11) moved the corpus harness and
        // the generators — and the Pure fixtures their tests embed — to spec
        for (String module : new String[] {"core", "spec"}) {
            Path dir = root.resolve(module).resolve("src/test/java");
            if (!Files.isDirectory(dir)) {
                throw new IllegalStateException(module + " test tree missing at "
                        + dir + " — set -Dlegend.lite.root");
            }
            try (Stream<Path> s = Files.walk(dir)) {
                s.filter(f -> f.toString().endsWith(".java"))
                        .filter(f -> !Corpus.slashed(f).contains("/target/"))
                        .sorted(java.util.Comparator.comparing(Corpus::slashed))
                        .forEach(out::add);
            } catch (IOException e) {
                throw new IllegalStateException("cannot walk " + dir, e);
            }
        }
        return out;
    }

    private record Run(String text, int start, int line) {
    }

    /**
     * String-literal RUNS: ordinary literals {@code +}-joined across lines,
     * and text blocks. Tolerant by design — the reference parser adjudicates
     * every candidate, so a garbled extraction contributes nothing rather
     * than a false row.
     */
    private static List<Run> literalRuns(String src) {
        List<Run> out = new ArrayList<>();
        StringBuilder cur = new StringBuilder();
        int runStart = -1;
        int i = 0;
        while (i < src.length()) {
            char c = src.charAt(i);
            if (c == '/' && i + 1 < src.length() && src.charAt(i + 1) == '/') {
                while (i < src.length() && src.charAt(i) != '\n') {
                    i++;
                }
                continue;
            }
            if (c == '/' && i + 1 < src.length() && src.charAt(i + 1) == '*') {
                int end = src.indexOf("*/", i + 2);
                i = end < 0 ? src.length() : end + 2;
                continue;
            }
            if (c == '"' && src.startsWith("\"\"\"", i)) {
                int end = src.indexOf("\"\"\"", i + 3);
                if (end < 0) {
                    break;
                }
                if (runStart < 0) {
                    runStart = i;
                }
                cur.append(src, i + 3, end);
                int afterBlock = end + 3;
                int cont = skipJoin(src, afterBlock);
                if (cont < 0 || !isLiteralStart(src, cont)) {
                    flush(out, cur, runStart, src);
                    runStart = -1;
                    i = afterBlock;         // NEVER rewind — that is an
                } else {                    // infinite loop, not a re-scan
                    i = cont;
                }
                continue;
            }
            if (c == '"') {
                int j = i + 1;
                StringBuilder lit = new StringBuilder();
                while (j < src.length() && src.charAt(j) != '"') {
                    if (src.charAt(j) == '\\' && j + 1 < src.length()) {
                        char n = src.charAt(j + 1);
                        lit.append(switch (n) {
                            case 'n' -> "\n";
                            case 't' -> "\t";
                            case '"' -> "\"";
                            case '\\' -> "\\";
                            default -> "";
                        });
                        j += 2;
                        continue;
                    }
                    lit.append(src.charAt(j));
                    j++;
                }
                if (runStart < 0) {
                    runStart = i;
                }
                cur.append(lit);
                i = j + 1;
                int after = skipJoin(src, i);
                if (after < 0 || !isLiteralStart(src, after)) {
                    flush(out, cur, runStart, src);
                    runStart = -1;
                } else {
                    i = after;
                }
                continue;
            }
            i++;
        }
        flush(out, cur, runStart, src);
        return out;
    }

    /** Past whitespace and a {@code +} continuation; -1 if neither. */
    private static int skipJoin(String src, int i) {
        int j = i;
        while (j < src.length() && Character.isWhitespace(src.charAt(j))) {
            j++;
        }
        if (j < src.length() && src.charAt(j) == '+') {
            j++;
            while (j < src.length() && Character.isWhitespace(src.charAt(j))) {
                j++;
            }
            return j;
        }
        return -1;
    }

    private static boolean isLiteralStart(String src, int i) {
        return i >= 0 && i < src.length() && src.charAt(i) == '"';
    }

    private static void flush(List<Run> out, StringBuilder cur, int start,
            String src) {
        if (start >= 0 && !cur.isEmpty()) {
            out.add(new Run(cur.toString(), start, lineOf(src, start)));
        }
        cur.setLength(0);
    }

    private static int lineOf(String src, int offset) {
        int line = 1;
        for (int i = 0; i < offset && i < src.length(); i++) {
            if (src.charAt(i) == '\n') {
                line++;
            }
        }
        return line;
    }

    /**
     * Is this literal an assertThrows argument? Scans back to the statement
     * boundary — a literal inside {@code assertThrows(X.class, () -> ...)}
     * is a fixture we EXPECT to be rejected.
     */
    private static boolean isNegativeContext(String src, int start) {
        int from = Math.max(0, start - 600);
        String before = src.substring(from, start);
        int stmt = Math.max(Math.max(before.lastIndexOf(';'),
                before.lastIndexOf('{')), before.lastIndexOf('}'));
        String window = stmt < 0 ? before : before.substring(stmt + 1);
        return window.contains("assertThrows");
    }

    /**
     * Prefix each run of same-section elements with its {@code ###} header,
     * so a section-less fixture fragment becomes a source the reference
     * parser can read. Returns {@code null} when the text has no top-level
     * keyword to place — counted as unadjudicable rather than guessed at.
     */
    private static @com.legend.Nullable String withSectionHeaders(String raw) {
        String t = raw.strip();
        if (!looksLikeSource(t)) {
            return null;
        }
        if (t.startsWith("###")) {
            return t + "\n";
        }
        // element starts, at brace/paren depth 0
        Map<Integer, String> starts = new LinkedHashMap<>();
        int depth = 0;
        boolean atElementStart = true;
        int docStart = -1;      // a '''...''' block at element position belongs to the NEXT element
        for (int i = 0; i < t.length(); i++) {
            char c = t.charAt(i);
            if (depth == 0 && atElementStart && t.startsWith("'''", i)) {
                // documentation (4.145.0): skip the literal whole; the section
                // header goes BEFORE it, never between it and its declaration
                int close = t.indexOf("'''", i + 3);
                if (close < 0) {
                    return null;
                }
                if (docStart < 0) {
                    docStart = i;
                }
                i = close + 2;
                continue;
            }
            if (c == '{' || c == '(' || c == '[') {
                depth++;
            } else if (c == '}' || c == ')' || c == ']') {
                depth--;
                atElementStart = depth == 0;
                continue;
            } else if (c == ';' && depth == 0) {
                atElementStart = true;      // `import x::*;` ends an element
                continue;                   // without ever opening a bracket
            }
            if (depth != 0) {
                continue;
            }
            if (atElementStart && Character.isLetter(c)) {
                int j = i;
                while (j < t.length() && (Character.isLetterOrDigit(t.charAt(j))
                        || t.charAt(j) == '_')) {
                    j++;
                }
                String kw = t.substring(i, j);
                String section = sectionOf(kw);
                if (section == null) {
                    return null;        // an unknown head — do not guess
                }
                starts.put(docStart >= 0 ? docStart : i, section);
                docStart = -1;
                atElementStart = false;
                i = j - 1;
            }
        }
        if (starts.isEmpty()) {
            return null;
        }
        StringBuilder b = new StringBuilder();
        String open = null;
        int prev = 0;
        for (Map.Entry<Integer, String> e : starts.entrySet()) {
            if (e.getKey() > prev) {
                b.append(t, prev, e.getKey());
            }
            if (!e.getValue().equals(open)) {
                b.append("\n###").append(e.getValue()).append('\n');
                open = e.getValue();
            }
            prev = e.getKey();
        }
        b.append(t.substring(prev));
        return b.append('\n').toString();
    }

    /**
     * Is this literal plausibly Pure SOURCE rather than prose or a fragment?
     *
     * <p>Without this the report is ~95% noise: an error-message string like
     * "native class FQN outside expected packages:" starts with a top-level
     * keyword, and a deliberately-truncated fixture ("Database test::DB (
     * Table (BROKEN") is a NEGATIVE fixture that lives outside any
     * assertThrows. Both would be filed as leniency. A row nobody can act on
     * is worse than no row.
     *
     * <p>So: brackets must BALANCE and there must be at least one, quotes
     * must pair, and the text must end where an element ends.
     */
    private static boolean looksLikeSource(String t) {
        if (t.length() < 12) {
            return false;
        }
        char last = t.charAt(t.length() - 1);
        if (last != '}' && last != ')' && last != ';') {
            return false;
        }
        int braces = 0;
        int parens = 0;
        int brackets = 0;
        int opened = 0;
        int quotes = 0;
        for (int i = 0; i < t.length(); i++) {
            switch (t.charAt(i)) {
                case '{' -> { braces++; opened++; }
                case '}' -> braces--;
                case '(' -> { parens++; opened++; }
                case ')' -> parens--;
                case '[' -> { brackets++; opened++; }
                case ']' -> brackets--;
                case '\'' -> quotes++;
                default -> { }
            }
            if (braces < 0 || parens < 0 || brackets < 0) {
                return false;               // closes before it opens
            }
        }
        return braces == 0 && parens == 0 && brackets == 0
                && opened > 0 && quotes % 2 == 0;
    }

    private static String oneLine(String s) {
        String x = s.replaceAll("\\s+", " ").strip();
        return x.length() > 150 ? x.substring(0, 150) + "…" : x;
    }
}
