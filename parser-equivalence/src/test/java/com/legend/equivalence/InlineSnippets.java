package com.legend.equivalence;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import java.util.stream.Stream;

/**
 * The C4/C5 corpus tier: Pure snippets embedded as Java string literals in upstream TEST sources
 * (~3k in legend-engine, ~3k in legend-pure). A tolerant scanner extracts every string-literal
 * RUN (ordinary literals and text blocks, {@code +}-joined across lines); a run is a candidate
 * when it looks like Pure declarations. Extraction fidelity is NOT load-bearing: every candidate
 * goes through the same differential compare as file sources, so the REFERENCE parser
 * adjudicates — a garbled extraction is rejected by it and contributes nothing, while any text
 * both parsers can read identically is a valid differential input.
 *
 * <p>Coverage is self-reported (files scanned / literal runs / candidates) so a silently missed
 * corpus shows up as a number, not as silence.
 */
final class InlineSnippets {

    private InlineSnippets() {
    }

    /** Lines that start a Pure declaration — the candidate heuristic. The
     *  corpus universe (ledger totals, catalog totality) is pinned on THIS
     *  pattern; never widen it in place. */
    private static final Pattern PURE_DECL = Pattern.compile(
            "(?m)^\\s*(Class|Enum|Association|Profile|Measure|function|native\\s+function|import)\\s");

    /** The OWN-CORPUS candidate heuristic: PURE_DECL plus snippets that
     *  START with a store/mapping element (Database-first fixtures carry
     *  no domain decl at all). Wider than the corpus pattern on purpose —
     *  our own conformance surface should see every model our tests
     *  compile; the engine-corpus universe stays pinned above. */
    static final Pattern OWN_DECL = Pattern.compile(
            "(?m)^\\s*(Class|Enum|Association|Profile|Measure|function|native\\s+function|import"
                    + "|Database|Mapping|Runtime|RelationalDatabaseConnection|Service)\\s");

    /** C12 candidate (harvest-scope extension 2026-08-14): SECTION-ONLY
     *  documents — connection/store/auth test sources carry no domain
     *  decl, so PURE_DECL never saw them (the census's 75-keyword hole).
     *  Additive tier; the C4 universe pin above stays untouched. */
    static final Pattern SECTION_DOC = Pattern.compile(
            "(?m)^\\s*###[A-Za-z]");

    /** One test file's literal runs, in source order — the rejection-parity pairing
     *  walks these directly. */
    record FileRuns(String id, List<String> runs) {
    }

    /** Every Java test file's literal runs under a root (same walk as {@link #extract}). */
    static List<FileRuns> literalRunsByFile(Path root) {
        List<FileRuns> out = new ArrayList<>();
        if (!Files.isDirectory(root)) {
            throw new IllegalStateException("InlineSnippets root " + root + " is not among its inputs: declare it (Bazel workplan P3-14: a missing root failed silently)");
        }
        try (Stream<Path> s = Files.walk(root)) {
            for (Path p : s.filter(f -> f.toString().endsWith(".java"))
                    .filter(f -> Corpus.within(root, f).contains("/src/test/"))
                    .filter(f -> !Corpus.within(root, f).contains("/target/"))
                    .sorted(java.util.Comparator.comparing(Corpus::slashed))
                    .toList()) {
                try {
                    out.add(new FileRuns(Corpus.slashed(root.relativize(p)),
                            literalRuns(Files.readString(p))));
                } catch (Exception ignored) {
                    // non-UTF8 — visible via extract()'s counters
                }
            }
        } catch (IOException e) {
            throw new IllegalStateException("cannot walk " + root, e);
        }
        return out;
    }

    static List<Corpus.Source> extract(Path root, String tier) {
        return extract(root, tier, PURE_DECL);
    }

    /** {@link #extract(Path, String)} with an explicit candidate pattern —
     *  the own-corpus surface passes {@link #OWN_DECL}. */
    static List<Corpus.Source> extract(Path root, String tier,
            Pattern candidate) {
        List<Path> javaFiles = new ArrayList<>();
        if (!Files.isDirectory(root)) {
            throw new IllegalStateException("InlineSnippets root " + root
                    + " is not among its inputs: declare it (Bazel workplan P3-14: a missing root failed silently)");
        }
        {
            try (Stream<Path> s = Files.walk(root)) {
                s.filter(p -> p.toString().endsWith(".java"))
                        .filter(p -> Corpus.within(root, p).contains("/src/test/"))
                        .filter(p -> !Corpus.within(root, p).contains("/target/"))
                        .sorted(java.util.Comparator.comparing(Corpus::slashed))
                        .forEach(javaFiles::add);
            } catch (IOException e) {
                throw new IllegalStateException("cannot walk " + root, e);
            }
        }
        // dedupe identical snippets (fixtures repeat across tests) — first occurrence wins the id
        Map<String, String> byText = new LinkedHashMap<>();
        int runs = 0;
        for (Path p : javaFiles) {
            String text;
            try {
                text = Files.readString(p);
            } catch (Exception e) {
                continue;                        // non-UTF8 — visible via the scanned-files count
            }
            List<String> found = literalRuns(text);
            runs += found.size();
            int idx = 0;
            for (String run : found) {
                if (run.length() > 20 && candidate.matcher(run).find()) {
                    // slashed(): this id is matched against the census's
                    // forward-slash host rows — a platform-separator id
                    // quarantines nothing (Windows CI gate 8, 2026-09-09)
                    byText.putIfAbsent(run,
                            Corpus.slashed(root.relativize(p)) + "#" + idx);
                }
                idx++;
            }
        }
        List<Corpus.Source> out = new ArrayList<>(byText.size());
        for (Map.Entry<String, String> e : byText.entrySet()) {
            out.add(new Corpus.Source(e.getValue(), e.getKey(), tier));
        }
        System.out.printf("inline snippets [%s]: %d java files, %d literal runs, %d candidates%n",
                tier, javaFiles.size(), runs, out.size());
        return out;
    }

    /**
     * Every maximal string-literal run in a Java source: a literal, optionally followed by
     * ({@code +} literal)* — whitespace and comments may sit between. A non-literal operand ends
     * the run.
     */
    static List<String> literalRuns(String s) {   // package-visible: the
        // section-normalize rewriter verifies its edits with THIS scanner

        List<String> out = new ArrayList<>();
        int i = 0;
        int n = s.length();
        StringBuilder run = null;
        while (i < n) {
            char c = s.charAt(i);
            if (c == '/' && i + 1 < n && s.charAt(i + 1) == '/') {
                while (i < n && s.charAt(i) != '\n') {
                    i++;
                }
                continue;
            }
            if (c == '/' && i + 1 < n && s.charAt(i + 1) == '*') {
                int end = s.indexOf("*/", i + 2);
                i = end < 0 ? n : end + 2;
                continue;
            }
            if (c == '\'') {                     // char literal — skip, handles '\''
                i++;
                while (i < n && s.charAt(i) != '\'') {
                    i += s.charAt(i) == '\\' ? 2 : 1;
                }
                i++;
                continue;
            }
            if (c == '"') {
                int[] cursor = {i};
                String lit = s.startsWith("\"\"\"", i)
                        ? scanTextBlock(s, cursor)
                        : scanOrdinary(s, cursor);
                i = cursor[0];
                if (run == null) {
                    run = new StringBuilder();
                }
                run.append(lit);
                // continue the run only over  [ws/comments] '+' [ws/comments] '"'
                int j = skipWsAndComments(s, i);
                if (j < n && s.charAt(j) == '+') {
                    j = skipWsAndComments(s, j + 1);
                    if (j < n && s.charAt(j) == '"') {
                        i = j;
                        continue;
                    }
                }
                out.add(run.toString());
                run = null;
                continue;
            }
            i++;
        }
        if (run != null) {
            out.add(run.toString());
        }
        return out;
    }

    private static int skipWsAndComments(String s, int i) {
        int n = s.length();
        while (i < n) {
            char c = s.charAt(i);
            if (Character.isWhitespace(c)) {
                i++;
            } else if (c == '/' && i + 1 < n && s.charAt(i + 1) == '/') {
                while (i < n && s.charAt(i) != '\n') {
                    i++;
                }
            } else if (c == '/' && i + 1 < n && s.charAt(i + 1) == '*') {
                int end = s.indexOf("*/", i + 2);
                i = end < 0 ? n : end + 2;
            } else {
                break;
            }
        }
        return i;
    }

    /** Ordinary literal at cursor (on the opening quote); decodes standard escapes. */
    private static String scanOrdinary(String s, int[] cursor) {
        int i = cursor[0] + 1;
        int n = s.length();
        StringBuilder b = new StringBuilder();
        while (i < n && s.charAt(i) != '"') {
            char c = s.charAt(i);
            if (c == '\\' && i + 1 < n) {
                i += decodeEscape(s, i, b);
            } else {
                b.append(c);
                i++;
            }
        }
        cursor[0] = Math.min(i + 1, n);
        return b.toString();
    }

    /** Text block at cursor (on the first of three quotes): incidental-indent stripping per the
     *  Java spec (minimum indent over non-blank lines and the closing delimiter line). */
    private static String scanTextBlock(String s, int[] cursor) {
        int start = s.indexOf('\n', cursor[0] + 3);        // content starts after the opening line
        int end = s.indexOf("\"\"\"", start < 0 ? cursor[0] + 3 : start);
        if (start < 0 || end < 0) {
            cursor[0] = s.length();
            return "";
        }
        String body = s.substring(start + 1, end);
        cursor[0] = end + 3;
        String[] lines = body.split("\n", -1);
        int minIndent = Integer.MAX_VALUE;
        for (int k = 0; k < lines.length; k++) {
            String line = lines[k];
            boolean lastLine = k == lines.length - 1;      // holds the closing delimiter's indent
            if (line.isBlank() && !lastLine) {
                continue;
            }
            int indent = 0;
            while (indent < line.length() && Character.isWhitespace(line.charAt(indent))) {
                indent++;
            }
            minIndent = Math.min(minIndent, indent);
        }
        StringBuilder b = new StringBuilder();
        for (int k = 0; k < lines.length; k++) {
            String line = lines[k];
            if (k == lines.length - 1 && line.isBlank()) {
                break;                                     // the closing-delimiter line itself
            }
            String stripped = line.length() > minIndent ? line.substring(minIndent)
                    : line.strip();
            // decode the escapes text blocks honor
            for (int i = 0; i < stripped.length(); i++) {
                char c = stripped.charAt(i);
                if (c == '\\' && i + 1 < stripped.length()) {
                    i += decodeEscape(stripped, i, b) - 1;
                } else {
                    b.append(c);
                }
            }
            b.append('\n');
        }
        return b.toString();
    }

    /** Decode one backslash escape at {@code i}; returns chars consumed. Unknown escapes are
     *  kept raw — the reference parser adjudicates whatever comes out. */
    private static int decodeEscape(String s, int i, StringBuilder b) {
        char e = s.charAt(i + 1);
        switch (e) {
            case 'n' -> b.append('\n');
            case 't' -> b.append('\t');
            case 'r' -> b.append('\r');
            case 'b' -> b.append('\b');
            case 'f' -> b.append('\f');
            case 's' -> b.append(' ');
            case '"' -> b.append('"');
            case '\'' -> b.append('\'');
            case '\\' -> b.append('\\');
            case 'u' -> {
                if (i + 5 < s.length()) {
                    try {
                        b.append((char) Integer.parseInt(s.substring(i + 2, i + 6), 16));
                        return 6;
                    } catch (NumberFormatException ignored) {
                        // fall through to raw
                    }
                }
                b.append('\\').append(e);
            }
            default -> b.append('\\').append(e);
        }
        return 2;
    }
}
