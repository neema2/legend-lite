// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.tools.meaning;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

/**
 * UPSTREAM'S VOCABULARY, DERIVED (census B2, 2026-11-19; the goal: "derived, not hand-listed, where it can be"):
 * legend-engine's and legend-pure's own names and words, read from the pinned sources (@legend_engine_src,
 * @legend_pure_src) -- never recalled. A program run as a build action (java_run, tools/meaning/BUILD.bazel) over the
 * two trees' main sources, writing one TSV the meaning census reads:
 *
 * <pre>term \t category \t surface \t occurrences \t one upstream file it comes from</pre>
 *
 * Categories: TYPE (a Java class, interface, enum or record a main source declares), EXCEPTION (a TYPE that is an
 * exception or error), FIELD (a field of a protocol class), SUBTYPE (a protocol subtype's JSON name: {@code _type}),
 * ROUTE (a JAX-RS resource's path: class {@code @Path} then method {@code @Path}), IDENT (an identifier-shaped string
 * literal: a name upstream spells as data, e.g. a SQL alias in its Pure), SENTENCE (a literal's words between its
 * placeholders). Surface: LANGUAGE (the model's own format -- the grammar, the compiler, the protocol of model
 * elements and value specifications, legend-pure's M3 and platform declarations: the language lite implements and
 * parses as-is) or EXECUTION (everything else: execution plans and their results, the result serializers, the server's
 * routes, the query store, test data, the relational Pure that writes legend-engine's SQL, legend-pure's runtime).
 *
 * <p>Mechanical filters only, each stated: a TYPE, FIELD or SUBTYPE is camel-case with at least two words (one word is
 * English, not upstream's); an IDENT has 6+ characters and two words or an underscore; a SENTENCE has 3+ words (or
 * 2, the first capitalised: "Assert failed"), 12 to {@link #MAX} characters, and appears in at most {@link #RARE}
 * upstream literals (2 for two words): a phrase upstream writes everywhere is the language's English, not one of its
 * messages. The census drops more at read time (lite's own Pure names, the
 * LANGUAGE surface's names), never here: this file is upstream's alone.
 */
public final class VocabularyGenerator {

    /** A sentence fragment in more upstream literals than this is generic English (a two-word one: more than 2). */
    static final int RARE = 5;

    /** A whole capitalised literal (a message of its own) in more upstream literals than this is no message. */
    static final int COMMON = 25;

    /** A fragment longer than this is a template or a document, not a message a product would hold whole. */
    static final int MAX = 160;

    private VocabularyGenerator() {
    }

    record Term(String term, String category, String surface) {
    }

    public static void main(String[] args) throws IOException {
        Path engine = Path.of(args[0]);
        Path pure = Path.of(args[1]);
        Path out = Path.of(args[2]);
        Map<Term, int[]> counts = new TreeMap<>((a, b) -> {
            int c = a.term().compareTo(b.term());
            if (c != 0) {
                return c;
            }
            c = a.category().compareTo(b.category());
            return c != 0 ? c : a.surface().compareTo(b.surface());
        });
        Map<Term, String> example = new TreeMap<>((a, b) -> (a.term() + a.category() + a.surface())
                .compareTo(b.term() + b.category() + b.surface()));
        Map<String, Integer> sentenceLiterals = new TreeMap<>();
        Map<String, String> sentenceSurface = new TreeMap<>();
        Map<String, String> sentenceFile = new TreeMap<>();
        java.util.Set<String> wholeLiterals = new java.util.HashSet<>();
        java.util.Set<String> texts = new java.util.TreeSet<>();
        for (Path root : List.of(engine, pure)) {
            String tree = root == engine ? "legend-engine" : "legend-pure";
            List<Path> files;
            try (Stream<Path> s = Files.walk(root)) {
                files = s.filter(Files::isRegularFile).filter(p -> p.toString().replace('\\', '/').contains("/src/main/"))
                        .filter(p -> p.toString().endsWith(".java") || p.toString().endsWith(".pure")).sorted().toList();
            }
            for (Path f : files) {
                String rel = tree + "/" + root.relativize(f).toString().replace('\\', '/');
                // the file as cited: its module (the path before /src/) and its name
                String cite = rel.substring(0, rel.indexOf("/src/main/")) + "/.../" + f.getFileName();
                String text = Files.readString(f, StandardCharsets.UTF_8);
                String surface = surface(rel);
                List<String> literals = new ArrayList<>();
                String code = rel.endsWith(".java") ? javaCode(text, literals) : pureCode(text, literals);
                if (rel.endsWith(".pure")) {
                    // a Pure name is the language's where its file is: legend-pure's platform, legend-engine's Pure
                    // function library; a name an execution file declares (a result's TDSColumn.relationalType) is not
                    pureNames(code, cite, surface.equals("LANGUAGE") || rel.contains("-functions") ? "LANGUAGE"
                            : "EXECUTION", counts, example);
                }
                if (rel.endsWith(".java")) {
                    types(code, cite, surface, counts, example);
                    if (rel.contains("/protocol/")) {
                        fields(code, cite, surface, counts, example);
                    }
                    subtypes(text, cite, surface, counts, example);
                    routes(code, text, cite, counts, example);
                }
                for (String lit : new java.util.LinkedHashSet<>(literals)) {
                    String flat = lit.replaceAll("\\s+", " ").strip();
                    if (flat.length() >= 6 && flat.length() <= MAX * 2) {
                        texts.add(flat);
                    }
                    if (isIdent(lit)) {
                        add(counts, example, new Term(lit, "IDENT", surface), cite);
                    }
                    for (String s : fragments(lit)) {
                        if (s.equals(lit.strip())) {
                            wholeLiterals.add(s);
                        }
                        sentenceLiterals.merge(s, 1, Integer::sum);
                        sentenceSurface.merge(s, surface, (a, b) -> a.equals(b) ? a : "EXECUTION");
                        sentenceFile.putIfAbsent(s, cite);
                    }
                }
            }
        }
        sentenceLiterals.forEach((s, n) -> {
            boolean twoWords = s.split("\\s+").length == 2;
            // a message whole (a literal of its own, capitalised) stays however often upstream writes it: "Assert failed"
            boolean message = wholeLiterals.contains(s) && Character.isUpperCase(s.charAt(0)) && n <= COMMON;
            // a fragment of a longer literal needs 4 words: three are English ("not supported yet")
            boolean fragmentOnly = !wholeLiterals.contains(s) && s.split("\\s+").length < 4;
            // under four words, a message starts as one: capitalised ("Assert failed"); "not supported yet" is English
            boolean lowerShort = s.split("\\s+").length < 4 && !Character.isUpperCase(s.charAt(0));
            fragmentOnly |= lowerShort;
            if (!fragmentOnly && (message || n <= (twoWords ? 2 : RARE))) {
                Term t = new Term(s, "SENTENCE", sentenceSurface.get(s));
                counts.put(t, new int[] {n});
                example.put(t, sentenceFile.get(s));
            }
        });
        StringBuilder b = new StringBuilder();
        b.append("# UPSTREAM'S VOCABULARY (tools/meaning/VocabularyGenerator.java): term, category, surface, upstream ")
                .append("occurrences, one upstream file. Generated from the pinned legend-engine and legend-pure ")
                .append("sources; never edited by hand.\n");
        counts.forEach((t, n) -> b.append(t.term().replace('\t', ' ').replace('\n', ' ')).append('\t')
                .append(t.category()).append('\t').append(t.surface()).append('\t').append(n[0]).append('\t')
                .append(example.get(t)).append('\n'));
        // every upstream literal's text, whitespace folded: what the census verifies the edge's own strings against (an
        // edge string upstream also holds is upstream's; one it does not is lite's own, or the edge's composition)
        for (String t : texts) {
            b.append(t.replace('\t', ' ')).append("\tTEXT\t-\t1\t-\n");
        }
        Files.writeString(out, b.toString(), StandardCharsets.UTF_8);
    }

    private static void add(Map<Term, int[]> counts, Map<Term, String> example, Term t, String file) {
        counts.computeIfAbsent(t, k -> new int[1])[0]++;
        example.putIfAbsent(t, file);
    }

    /** The surface a source belongs to, by where it sits in its tree (EXECUTION's markers first: a protocol module's
     *  execution-plan classes are execution). */
    static String surface(String rel) {
        if (rel.matches(".*(/executionPlan/|/executionplan/|/result/|-execution[-/]|-executionPlan|-server[-/]|"
                + "-application-query|test-data-generation|-pct[-/]|/sqlQueryToString/|legend-pure-runtime-java|"
                + "/router/|-shared-core).*")
                || (rel.contains("relationalStore") && rel.endsWith(".pure"))) {
            return "EXECUTION";
        }
        if (rel.matches(".*(-grammar|-protocol|-compiler|legend-pure-m3|legend-pure-m4|/platform/pure/|"
                + "/m3/|/metamodel/|-language-pure-dsl|/packageableElement/|/valueSpecification/|/model/context/|"
                + "/model/domain/|/model/data/|/model/test/).*")) {
            return "LANGUAGE";
        }
        return "EXECUTION";
    }

    private static final Pattern TYPE_DECL = Pattern.compile("\\b(?:class|interface|enum|record|@interface)\\s+"
            + "([A-Z][A-Za-z0-9_]*)");
    private static final Pattern CAMEL2 = Pattern.compile("[A-Za-z][a-z0-9]*(?:[A-Z][A-Za-z0-9]*)+");
    private static final Pattern FIELD_DECL = Pattern.compile("(?m)^\\s*(?:@\\w+(?:\\([^)]*\\))?\\s*)*"
            + "(?:public|protected|private)?\\s*(?:static\\s+)?(?:final\\s+)?(?:transient\\s+)?"
            + "[A-Z][\\w<>,\\s\\[\\]?.]*?\\s+([a-z_][A-Za-z0-9_]*)\\s*(?:=[^;]*)?;");

    private static final Pattern PURE_DECL = Pattern.compile("(?:\\bfunction\\s+(?:<<[^>]*>>\\s*)?(?:\\{[^}]*\\}\\s*)?"
            + "(?:[\\w]+::)*(\\w+)\\s*(?:<[^>]*>)?\\s*\\()"
            + "|(?:\\b(?:Class|Enum|Profile|Association|Measure|Primitive)\\s+(?:<<[^>]*>>\\s*)?(?:\\{[^}]*\\}\\s*)?"
            + "(?:[\\w]+::)*(\\w+))"
            + "|(?m)(?:^\\s*(?:<<[^>]*>>\\s*)?(?:\\{[^}]*\\}\\s*)?(\\w+)\\s*(?:\\([^)]*\\))?\\s*:\\s*[\\w:<>\\[\\]]+\\s*\\[)");

    /** A Pure file's declared names -- its functions, classes, enumerations, profiles, associations, measures and
     *  properties: in a language file, the language upstream's Pure library spells, which lite implements (PURE_NAME,
     *  LANGUAGE); in an execution file, upstream's execution model's names (PURE_NAME, EXECUTION). */
    private static void pureNames(String code, String rel, String surface, Map<Term, int[]> counts,
            Map<Term, String> example) {
        Matcher m = PURE_DECL.matcher(code);
        while (m.find()) {
            String n = m.group(1) != null ? m.group(1) : m.group(2) != null ? m.group(2) : m.group(3);
            if (n != null && n.length() >= 3) {
                add(counts, example, new Term(n, "PURE_NAME", surface), rel);
            }
        }
    }

    private static void types(String code, String rel, String surface, Map<Term, int[]> counts,
            Map<Term, String> example) {
        Matcher m = TYPE_DECL.matcher(code);
        while (m.find()) {
            String n = m.group(1);
            if (!CAMEL2.matcher(n).matches()) {
                continue;
            }
            boolean exception = n.endsWith("Exception") || n.endsWith("Error");
            add(counts, example, new Term(n, exception ? "EXCEPTION" : "TYPE", surface), rel);
        }
    }

    private static void fields(String code, String rel, String surface, Map<Term, int[]> counts,
            Map<Term, String> example) {
        Matcher m = FIELD_DECL.matcher(code);
        while (m.find()) {
            String n = m.group(1);
            if (CAMEL2.matcher(n).matches() && !n.equals(n.toUpperCase(java.util.Locale.ROOT))) {
                add(counts, example, new Term(n, "FIELD", surface), rel);
            }
        }
    }

    private static final Pattern SUBTYPE = Pattern.compile("(?:@JsonSubTypes\\.Type\\([^)]*name\\s*=\\s*\"([^\"]+)\")"
            + "|(?:@JsonTypeName\\(\\s*\"([^\"]+)\"\\s*\\))"
            + "|(?:Tuples\\.pair\\(\\s*[\\w.]+\\.class\\s*,\\s*\"([^\"]+)\"\\s*\\))"
            + "|(?:withSubtype\\(\\s*[\\w.]+\\.class\\s*,\\s*\"([^\"]+)\"\\s*\\))");

    private static void subtypes(String text, String rel, String surface, Map<Term, int[]> counts,
            Map<Term, String> example) {
        Matcher m = SUBTYPE.matcher(text);
        while (m.find()) {
            String n = null;
            for (int g = 1; g <= 4 && n == null; g++) {
                n = m.group(g);
            }
            if (n != null && CAMEL2.matcher(n).matches()) {
                add(counts, example, new Term(n, "SUBTYPE", surface), rel);
            }
        }
    }

    private static final Pattern PATH = Pattern.compile("@Path\\(\\s*\"([^\"]*)\"\\s*\\)");

    /** A resource class's routes: its own {@code @Path} (before {@code class}) joined to each method's. */
    private static void routes(String code, String text, String rel, Map<Term, int[]> counts,
            Map<Term, String> example) {
        if (!text.contains("@Path(")) {
            return;
        }
        int classAt = -1;
        Matcher c = Pattern.compile("\\bclass\\s+[A-Z]").matcher(text);
        if (c.find()) {
            classAt = c.start();
        }
        String base = "";
        Matcher p = PATH.matcher(text);
        List<String> methods = new ArrayList<>();
        while (p.find()) {
            if (classAt >= 0 && p.start() < classAt) {
                base = p.group(1);
            } else {
                methods.add(p.group(1));
            }
        }
        if (methods.isEmpty() && !base.isEmpty()) {
            methods.add("");
        }
        for (String mp : methods) {
            String route = (strip(base) + "/" + strip(mp)).replaceAll("/+", "/");
            route = strip(route);
            int brace = route.indexOf('{');
            if (brace >= 0) {
                route = strip(route.substring(0, brace));
            }
            if (route.contains("/")) {
                add(counts, example, new Term(route, "ROUTE", "EXECUTION"), rel);
            }
        }
    }

    private static String strip(String s) {
        String t = s;
        while (t.startsWith("/")) {
            t = t.substring(1);
        }
        while (t.endsWith("/")) {
            t = t.substring(0, t.length() - 1);
        }
        return t;
    }

    private static final Pattern IDENT = Pattern.compile("[A-Za-z_][A-Za-z0-9_]*");

    /** An identifier-shaped literal of 6+ characters, camel-case with two words or holding an underscore beside a
     *  lower-case letter (a SQL alias's prefix, {@code joinleft_}); never all capitals (SQL's keywords and types), never
     *  lower snake case (SQL's function names). */
    static boolean isIdent(String lit) {
        if (lit.length() < 6 || !IDENT.matcher(lit).matches() || lit.equals(lit.toUpperCase(java.util.Locale.ROOT))) {
            return false;
        }
        // lower snake case is SQL's vocabulary (date_trunc, percentile_cont): the database's, not upstream's
        if (lit.matches("[a-z][a-z0-9]*(?:_[a-z0-9]+)+")) {
            return false;
        }
        return CAMEL2.matcher(lit).matches() || (lit.contains("_") && lit.matches(".*[a-z].*"));
    }

    private static final Pattern PLACEHOLDER = Pattern.compile("%[-#+ 0,(]*\\d*(?:\\.\\d+)?[a-zA-Z]|\\{\\d*\\}"
            + "|\\\\[ntr]|\\$\\{[^}]*\\}|\\[\\]|''");

    /** A literal's sentence fragments: the runs between its placeholders, trimmed, of 3+ words and 12+ characters with
     *  a lower-case letter. */
    static List<String> fragments(String lit) {
        List<String> out = new ArrayList<>();
        for (String part : PLACEHOLDER.split(lit)) {
            String s = part.strip();
            while (!s.isEmpty() && ":,.;'\"(".indexOf(s.charAt(0)) >= 0) {
                s = s.substring(1).strip();
            }
            while (!s.isEmpty() && ":,;'\"(".indexOf(s.charAt(s.length() - 1)) >= 0) {
                s = s.substring(0, s.length() - 1).strip();
            }
            int words = s.split("\\s+").length;
            boolean sentence = words >= 3 || (words == 2 && Character.isUpperCase(s.charAt(0)));
            if (s.length() >= 12 && s.length() <= MAX && sentence && s.chars().anyMatch(Character::isLowerCase)
                    && s.chars().filter(Character::isLetter).count() * 2 >= s.length()) {
                out.add(s);
            }
        }
        return out;
    }

    /** Java code with comments removed and each string literal's text collected (text blocks too); the literals
     *  blanked in what is returned. */
    static String javaCode(String s, List<String> literals) {
        StringBuilder out = new StringBuilder(s.length());
        int i = 0;
        int n = s.length();
        while (i < n) {
            char c = s.charAt(i);
            if (s.startsWith("\"\"\"", i)) {
                int j = s.indexOf("\"\"\"", i + 3);
                j = j < 0 ? n : j;
                literals.add(s.substring(i + 3, j));
                out.append("\"\"");
                i = Math.min(n, j + 3);
            } else if (c == '"') {
                int j = i + 1;
                StringBuilder lit = new StringBuilder();
                while (j < n && s.charAt(j) != '"' && s.charAt(j) != '\n') {
                    if (s.charAt(j) == '\\' && j + 1 < n) {
                        lit.append(unescape(s.charAt(j + 1)));
                        j += 2;
                    } else {
                        lit.append(s.charAt(j++));
                    }
                }
                literals.add(lit.toString());
                out.append("\"\"");
                i = j + 1;
            } else if (c == '\'') {
                int j = i + 1;
                while (j < n && s.charAt(j) != '\'' && s.charAt(j) != '\n') {
                    j += s.charAt(j) == '\\' ? 2 : 1;
                }
                out.append("' '");
                i = j + 1;
            } else if (s.startsWith("//", i)) {
                while (i < n && s.charAt(i) != '\n') {
                    i++;
                }
            } else if (s.startsWith("/*", i)) {
                int j = s.indexOf("*/", i + 2);
                i = j < 0 ? n : j + 2;
            } else {
                out.append(c);
                i++;
            }
        }
        return out.toString();
    }

    private static char unescape(char c) {
        return switch (c) {
            case 'n' -> '\n';
            case 't' -> '\t';
            case 'r' -> '\r';
            default -> c;
        };
    }

    /** Pure code with comments removed and each single-quoted literal's text collected. */
    static String pureCode(String s, List<String> literals) {
        StringBuilder out = new StringBuilder(s.length());
        int i = 0;
        int n = s.length();
        while (i < n) {
            char c = s.charAt(i);
            if (c == '\'') {
                int j = i + 1;
                StringBuilder lit = new StringBuilder();
                while (j < n && s.charAt(j) != '\'') {
                    if (s.charAt(j) == '\\' && j + 1 < n) {
                        lit.append(unescape(s.charAt(j + 1)));
                        j += 2;
                    } else {
                        lit.append(s.charAt(j++));
                    }
                }
                literals.add(lit.toString());
                out.append("''");
                i = j + 1;
            } else if (s.startsWith("//", i)) {
                while (i < n && s.charAt(i) != '\n') {
                    i++;
                }
            } else if (s.startsWith("/*", i)) {
                int j = s.indexOf("*/", i + 2);
                i = j < 0 ? n : j + 2;
            } else {
                out.append(c);
                i++;
            }
        }
        return out.toString();
    }

    static void requireDirectory(Path p) {
        if (!Files.isDirectory(p)) {
            throw new UncheckedIOException(new IOException(p + " is not a directory"));
        }
    }
}
