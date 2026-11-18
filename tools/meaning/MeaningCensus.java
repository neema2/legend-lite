// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.tools.meaning;

import com.legend.tools.meaning.ClassFiles.ClassFile;
import com.legend.tools.meaning.ClassFiles.Constant;
import com.legend.tools.meaning.ClassFiles.Field;
import com.legend.tools.meaning.ClassFiles.Method;
import com.legend.tools.meaning.HostSources.Import;
import com.legend.tools.meaning.HostSources.ImportKind;
import com.legend.tools.meaning.HostSources.Kind;
import com.legend.tools.meaning.HostSources.Source;
import com.legend.tools.meaning.HostSources.Token;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.function.Predicate;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * THE MEANING CENSUS (census B2, 2026-11-19): the lenses that read the product -- core's Java, its resources, the
 * hosts' Java, the TypeScript product and the Python product -- for compatibility meaning still in it. Each lens is a
 * function over its inputs, so the same code runs over the real product and over a planted violator; each answers
 * {@link Hit}s and judges none (the classification is the census test's, against its committed inventory).
 * <ul>
 *   <li>M1 TEXT: a product string holding one of upstream's names, routes or sentences (the derived vocabulary,
 *       {@link Vocabulary}) or one of the edge's own distinctive strings;</li>
 *   <li>M2 NAMES: a product declaration or name that names upstream itself ({@link #SELF_NAMED});</li>
 *   <li>M3 FORMS: {@link FormsLens};</li>
 *   <li>M4 PRINT BODIES: a product Pure resource's {@code toString} body, a print form shipped as Pure;</li>
 *   <li>M5 PROVENANCE: a product comment declaring a reproduction of upstream ({@link #REPRODUCTION});</li>
 *   <li>M6 EDGE-ONLY: a TypeScript or Python product export whose only users are the edge's modules;</li>
 *   <li>M7 K2: a product module or class that reaches the edge at load -- statically -- and every opt-in mechanism
 *       (a dynamic or guarded import, a ServiceLoader lookup) reported as one.</li>
 * </ul>
 */
public final class MeaningCensus {

    private MeaningCensus() {
    }

    /** One hit: its lens, its category, where (a file or class, with a line where there is one), the term or name,
     *  and what it is. */
    public record Hit(String lens, String category, String where, String term, String detail) {

        /** The key a classification row matches: lens, file (no line), term. */
        public String file() {
            int colon = where.lastIndexOf(':');
            return colon > 0 && where.substring(colon + 1).chars().allMatch(Character::isDigit)
                    ? where.substring(0, colon) : where;
        }
    }

    // -----------------------------------------------------------------------------------------------------------------
    // The vocabulary

    /** Upstream's vocabulary as the census reads it: the generated terms, the LANGUAGE surface's names and lite's own
     *  Pure names left out, the edge's distinctive strings added. */
    public static final class Vocabulary {

        /** term -> its categories ("TYPE/EXECUTION" ...) */
        final Map<String, String> names = new HashMap<>();
        /** the names a code name (a property read in TypeScript or Python) is matched against: the execution surface's
         *  protocol fields and subtypes, the JSON shapes of upstream's plans, results and server */
        final Map<String, String> shapes = new HashMap<>();
        /** the LANGUAGE surface's names and upstream Pure's declared names: the language lite implements */
        final Set<String> language = new HashSet<>();
        /** upstream's literal texts, whitespace folded (an edge string is upstream's when upstream holds it too) */
        final StringBuilder texts = new StringBuilder("\n");
        final Set<String> textSet = new HashSet<>();
        final Map<String, String> routes = new HashMap<>();
        final Map<String, String> sentences = new HashMap<>();
        final Map<String, String> edge = new HashMap<>();
        int languageDropped;
        int liteDropped;
        int jdkDropped;
        int edgeUnverified;

        /** Reads the generated TSV ({@link VocabularyGenerator}), leaving out each name of the language (upstream's
         *  LANGUAGE surface, its Pure library's declarations, lite's own Pure) and each name of the JDK's API. */
        public static Vocabulary of(List<String> lines, Set<String> liteNames, Set<String> jdkNames) {
            return of(lines, liteNames, jdkNames, Set.of());
        }

        /** As {@link #of(List, Set, Set)}, a TYPE or EXCEPTION also left out when lite's product declares a class of
         *  that name ({@code liteClasses}): a class lite declares is lite's own, whatever upstream calls one. */
        public static Vocabulary of(List<String> lines, Set<String> liteNames, Set<String> jdkNames,
                Set<String> liteClasses) {
            Vocabulary v = new Vocabulary();
            List<String[]> rows = new ArrayList<>();
            for (String line : lines) {
                if (line.isBlank() || line.startsWith("#")) {
                    continue;
                }
                String[] f = line.split("\t", -1);
                rows.add(f);
                if (f[2].equals("LANGUAGE") && !f[1].equals("SENTENCE") && !f[1].equals("EXCEPTION")) {
                    v.language.add(f[0]);
                } else if (f[1].equals("TEXT")) {
                    v.texts.append(f[0]).append('\n');
                    v.textSet.add(f[0]);
                }
            }
            for (String[] f : rows) {
                String term = f[0];
                String category = f[1];
                String tag = category + "/" + f[2] + " (" + f[4] + ")";
                switch (category) {
                    case "ROUTE" -> v.routes.putIfAbsent(term, tag);
                    case "SENTENCE" -> v.sentences.putIfAbsent(term, tag);
                    case "PURE_NAME", "TEXT" -> { }
                    default -> {
                        if (v.language.contains(term)) {
                            v.languageDropped++;
                        } else if (liteNames.contains(term) && !category.equals("FIELD") && !category.equals("SUBTYPE")) {
                            // a protocol field or subtype is a wire SHAPE (generatePlan's rootExecutionNode, a TDS
                            // column's relationalType): reading it is upstream's shape whatever lite's Pure also names
                            v.liteDropped++;
                        } else if (jdkNames.contains(term)) {
                            v.jdkDropped++;
                        } else if ((category.equals("TYPE") || category.equals("EXCEPTION")) && liteClasses.contains(term)) {
                            v.liteDropped++;
                        } else {
                            v.names.merge(term, tag, (a, b) -> a.length() <= b.length() ? a : b);
                            if (category.equals("FIELD") || category.equals("SUBTYPE")) {
                                v.shapes.merge(term, tag, (a, b) -> a.length() <= b.length() ? a : b);
                            }
                        }
                    }
                }
            }
            return v;
        }

        /** Adds the edge's own strings: each distinctive one ({@link #distinctive}), not a name of the language or of
         *  lite's Pure, that upstream also holds (an identifier as a literal of its own or a name above; a sentence
         *  inside one of upstream's literals). An edge string upstream does not hold is lite's own wording or the edge's
         *  composition, not upstream's vocabulary. */
        public Vocabulary withEdge(Map<String, String> edgeStrings, Set<String> liteNames) {
            String all = texts.toString();
            edgeStrings.forEach((raw, where) -> {
                String s = raw.strip();
                if (!distinctive(s) || liteNames.contains(s) || language.contains(s) || edge.containsKey(s)) {
                    return;
                }
                boolean verified = IDENTISH.matcher(s).matches() ? textSet.contains(s) || names.containsKey(s)
                        : all.contains(s.replaceAll("\\s+", " "));
                if (verified) {
                    edge.put(s, where);
                } else {
                    edgeUnverified++;
                }
            });
            return this;
        }

        public int size() {
            return names.size() + routes.size() + sentences.size() + edge.size();
        }

        /** The names (term -> its category, surface and one upstream file). */
        public Map<String, String> names() {
            return names;
        }

        public Map<String, String> sentences() {
            return sentences;
        }

        public Map<String, String> routes() {
            return routes;
        }

        /** The edge's own strings upstream also holds (string -> the edge class or module holding it). */
        public Map<String, String> edge() {
            return edge;
        }

        public Map<String, Integer> counts() {
            Map<String, Integer> out = new LinkedHashMap<>();
            out.put("names", names.size());
            out.put("routes", routes.size());
            out.put("sentences", sentences.size());
            out.put("edge strings", edge.size());
            out.put("names dropped as the LANGUAGE surface's", languageDropped);
            out.put("names dropped as lite's own Pure", liteDropped);
            out.put("names dropped as the JDK's API", jdkDropped);
            out.put("edge strings upstream does not hold", edgeUnverified);
            return out;
        }
    }

    private static final Pattern IDENTISH = Pattern.compile("[A-Za-z_][A-Za-z0-9_]*");
    private static final Pattern SQLISH = Pattern.compile("^\\s*[A-Z]{2,}[ (].*");

    /** An edge string worth matching: identifier-shaped (6+ characters, camel-case or with an underscore, not all
     *  capitals), a sentence (12+ characters, 2+ words, a lower-case letter, not a SQL statement), or a route. */
    static boolean distinctive(String s) {
        String t = s.strip();
        if (t.startsWith("/api/") && t.length() > 8) {
            return true;
        }
        if (IDENTISH.matcher(t).matches()) {
            return VocabularyGenerator.isIdent(t);
        }
        long words = java.util.Arrays.stream(t.split("\\s+")).filter(w -> w.matches("[A-Za-z][a-z']+[,.:]?")).count();
        return t.length() >= 12 && t.length() <= 200 && words >= 2
                && t.chars().anyMatch(Character::isLowerCase) && !SQLISH.matcher(t).matches()
                && t.chars().filter(Character::isLetter).count() * 2 >= t.length();
    }

    /** The names lite's own Pure spells: every identifier of the product's Pure resources (the prelude, the platform's
     *  declarations) and of the builtin tables (the native registry's membership). A name the language lite implements
     *  spells is lite's own, however upstream also spells it. */
    public static Set<String> liteNames(Map<String, String> productResources) {
        Set<String> out = new HashSet<>();
        productResources.forEach((name, text) -> {
            if (name.endsWith(".pure") || (name.contains("/builtin/") && name.endsWith(".tsv"))) {
                Matcher m = IDENTISH.matcher(text);
                while (m.find()) {
                    out.add(m.group());
                }
            }
        });
        return out;
    }

    /** The JDK's API names: every public class's simple name, and every public method's and field's name, of java.base's
     *  public classes in the packages a product's code calls, read from the running JDK's own class files (jrt:/). Upstream's code generators spell
     *  them as data ({@code 'flatMap'}); they are Java's, not upstream's. */
    /** The class names lite's product declares for its own code: each product class's simple name and each TypeScript
     *  or Python product class (a capitalised export). Matched against upstream's TYPE and EXCEPTION names only. */
    public static Set<String> liteDeclared(Map<String, ClassFile> productClasses, List<Source> hostProduct) {
        Set<String> out = new HashSet<>();
        for (ClassFile c : productClasses.values()) {
            String simple = c.name().substring(c.name().lastIndexOf('/') + 1);
            out.add(simple.substring(simple.lastIndexOf('$') + 1));
        }
        // a host's exported CLASS (a capitalised export), never a function: relation-type.ts's tdsColumns reads
        // upstream's field under upstream's name, which is the finding, not lite's own name
        for (Source s : hostProduct) {
            s.exports().stream().map(Token::text).filter(n -> !n.isEmpty() && Character.isUpperCase(n.charAt(0)))
                    .forEach(out::add);
        }
        return out;
    }

    public static Set<String> jdkNames() {
        Set<String> out = new HashSet<>();
        try {
            java.nio.file.FileSystem jrt = java.nio.file.FileSystems.getFileSystem(java.net.URI.create("jrt:/"));
            java.nio.file.Path base = jrt.getPath("/modules/java.base/java");
            try (java.util.stream.Stream<java.nio.file.Path> s = java.nio.file.Files.walk(base)) {
                for (java.nio.file.Path p : s.filter(x -> x.toString().endsWith(".class")).toList()) {
                    String pkg = p.getParent().toString();
                    if (!pkg.matches("/modules/java.base/java/(lang|util|util/stream|util/function|time|io|nio|math|text"
                            + "|net|nio/file)")) {
                        continue;
                    }
                    ClassFile c = ClassFiles.read(java.nio.file.Files.readAllBytes(p));
                    if ((c.access() & 0x0001) == 0) {
                        continue;
                    }
                    // the class's own simple name too (IllegalArgumentException, StringBuilder): Java's
                    String simple = c.name().substring(c.name().lastIndexOf('/') + 1);
                    if (!simple.contains("$")) {
                        out.add(simple);
                    }
                    for (Method m : c.methods()) {
                        if ((m.access() & 0x0001) != 0) {
                            out.add(m.name());
                        }
                    }
                    for (Field f : c.fields()) {
                        if ((f.access() & 0x0001) != 0) {
                            out.add(f.name());
                        }
                    }
                }
            }
        } catch (java.io.IOException e) {
            throw new java.io.UncheckedIOException(e);
        }
        return out;
    }

    // -----------------------------------------------------------------------------------------------------------------
    // M1 TEXT

    /** A string the product holds, and where. */
    public record Text(String text, String where, boolean isName) {
    }

    /** The strings of the product's classes: each pool string constant (a concatenation's recipe split at its
     *  arguments). Its names in code are M2's. */
    public static List<Text> javaStrings(Map<String, ClassFile> classes) {
        List<Text> out = new ArrayList<>();
        for (ClassFile c : classes.values()) {
            String where = FormsLens.where(c, 0).replaceAll(":0$", "");
            for (String s : new LinkedHashSet<>(c.strings())) {
                for (String part : s.split("[\u0001\u0002]")) {
                    if (!part.isEmpty()) {
                        out.add(new Text(part, where, false));
                    }
                }
            }
        }
        return out;
    }

    /** The strings of the product's text resources: a Pure file's single-quoted literals, any other text file's
     *  lines. */
    public static List<Text> resourceStrings(Map<String, String> resources) {
        List<Text> out = new ArrayList<>();
        resources.forEach((name, text) -> {
            if (name.endsWith(".pure")) {
                String[] lines = text.split("\n", -1);
                for (int k = 0; k < lines.length; k++) {
                    Matcher m = Pattern.compile("'((?:[^'\\\\]|\\\\.)*)'").matcher(lines[k]);
                    while (m.find()) {
                        out.add(new Text(m.group(1), name + ":" + (k + 1), false));
                    }
                }
            } else if (name.matches(".*\\.(json|tsv|txt|properties|csv|yaml|yml|md|sql|html)$")) {
                String[] lines = text.split("\n", -1);
                for (int k = 0; k < lines.length; k++) {
                    if (!lines[k].isBlank()) {
                        out.add(new Text(lines[k], name + ":" + (k + 1), false));
                    }
                }
            }
        });
        return out;
    }

    /** The strings of TypeScript or Python product sources: each literal, and each name in code. */
    public static List<Text> hostStrings(List<Source> sources) {
        List<Text> out = new ArrayList<>();
        for (Source s : sources) {
            for (Token t : s.tokens()) {
                if (t.kind() == Kind.LITERAL) {
                    out.add(new Text(t.text(), s.path() + ":" + t.line(), false));
                } else if (t.kind() == Kind.NAME || t.kind() == Kind.PROPERTY) {
                    out.add(new Text(t.text(), s.path() + ":" + t.line(), true));
                }
            }
        }
        return out;
    }

    /** M1 over {@code texts}. A name is matched whole (a name in code), or as a whole word of a string (8+ characters;
     *  shorter, the string itself); a route inside a string, bounded; a sentence or an edge string inside a string. */
    public static List<Hit> text(List<Text> texts, Vocabulary v) {
        Index sentences = new Index(v.sentences.keySet());
        Set<String> edgeLong = new HashSet<>();
        Set<String> edgeShort = new HashSet<>();
        v.edge.keySet().forEach(e -> (e.length() >= 12 && !IDENTISH.matcher(e).matches() ? edgeLong : edgeShort).add(e));
        Index edgeIndex = new Index(edgeLong);
        List<Hit> out = new ArrayList<>();
        Set<String> seen = new HashSet<>();
        for (Text t : texts) {
            String s = t.text();
            if (t.isName()) {
                // a name in code is upstream's shape from 10 characters (tdsColumns, resultType, rootExecutionNode):
                // a shorter one is the host language's own (the DOM's className, varName)
                String tag = s.length() >= 10 ? v.shapes.get(s) : null;
                if (tag != null && seen.add("NAME|" + t.where() + "|" + s)) {
                    out.add(new Hit("M1", "NAME", t.where(), s, tag));
                }
                continue;
            }
            String whole = v.names.get(s.strip());
            if (whole != null) {
                if (seen.add("NAME|" + t.where() + "|" + s.strip())) {
                    out.add(new Hit("M1", "NAME", t.where(), s.strip(), whole));
                }
            } else if (s.length() > 8) {
                Matcher m = IDENTISH.matcher(s);
                while (m.find()) {
                    String w = m.group();
                    String tag = w.length() >= 8 ? v.names.get(w) : null;
                    if (tag != null && seen.add("NAME|" + t.where() + "|" + w)) {
                        out.add(new Hit("M1", "NAME", t.where(), w, tag));
                    }
                }
            }
            for (Map.Entry<String, String> r : v.routes.entrySet()) {
                int at = s.indexOf(r.getKey());
                if (at >= 0 && (at == 0 || s.charAt(at - 1) == '/')) {
                    int end = at + r.getKey().length();
                    if ((end == s.length() || "/?#\"' ".indexOf(s.charAt(end)) >= 0)
                            && seen.add("ROUTE|" + t.where() + "|" + r.getKey())) {
                        out.add(new Hit("M1", "ROUTE", t.where(), r.getKey(), r.getValue()));
                    }
                }
            }
            for (String found : sentences.within(s)) {
                if (seen.add("SENTENCE|" + t.where() + "|" + found)) {
                    out.add(new Hit("M1", "SENTENCE", t.where(), found, v.sentences.get(found)));
                }
            }
            for (String found : edgeIndex.within(s)) {
                if (seen.add("EDGE|" + t.where() + "|" + found)) {
                    out.add(new Hit("M1", "EDGE", t.where(), found, "the edge's own string, in " + v.edge.get(found)));
                }
            }
            if (edgeShort.contains(s.strip()) && seen.add("EDGE|" + t.where() + "|" + s.strip())) {
                out.add(new Hit("M1", "EDGE", t.where(), s.strip(), "the edge's own string, in " + v.edge.get(s.strip())));
            }
        }
        return out;
    }

    /** Which of a set of phrases a text holds: an index on each phrase's first 8 characters. */
    static final class Index {

        private static final int K = 8;
        private final Map<String, List<String>> byPrefix = new HashMap<>();

        Index(Set<String> phrases) {
            for (String p : phrases) {
                if (p.length() >= K) {
                    byPrefix.computeIfAbsent(p.substring(0, K), k -> new ArrayList<>()).add(p);
                }
            }
        }

        List<String> within(String s) {
            List<String> out = new ArrayList<>();
            for (int i = 0; i + K <= s.length(); i++) {
                List<String> c = byPrefix.get(s.substring(i, i + K));
                if (c != null) {
                    for (String p : c) {
                        if (s.startsWith(p, i) && !out.contains(p)) {
                            out.add(p);
                        }
                    }
                }
            }
            return out;
        }
    }

    // -----------------------------------------------------------------------------------------------------------------
    // M2 NAMES

    /** A name that names upstream itself: upstream, legend-engine, legend-pure, pure/v1, or "engine" inside a longer
     *  name (lite's own one-word Engine is no hit). Lite's own naming convention, not upstream's vocabulary: a small
     *  token list, stated as one (census B2's shape note). */
    public static final Pattern SELF_NAMED = Pattern.compile("(?:[Uu]pstream|UPSTREAM|[Ll]egend_?[Ee]ngine|LEGEND_ENGINE"
            + "|[Ll]egend_?[Pp]ure|LEGEND_PURE|[Pp]ure_?[Vv]1|PURE_V1)"
            + "|(?:^|[a-z0-9_])(?:Engine|ENGINE|engine)(?:[A-Z_0-9]|$)|^(?:engine|ENGINE)[A-Z_0-9]");

    static boolean selfNamed(String name) {
        return name.length() > 6 && SELF_NAMED.matcher(name).find();
    }

    /** In the TypeScript and Python hosts "engine" is lite's own word for the thing that runs a plan (QueryEngine,
     *  DuckDbEngine, Python's Engine): a host name names upstream only by legend-engine, legend-pure, upstream or
     *  pure/v1 itself. */
    public static final Pattern HOST_SELF_NAMED = Pattern.compile("[Uu]pstream|UPSTREAM|[Ll]egend_?[Ee]ngine"
            + "|LEGEND_ENGINE|[Ll]egend_?[Pp]ure|LEGEND_PURE|[Pp]ure_?[Vv]1|PURE_V1");

    /** M2 over the product's classes: each class whose own name, or a field or method it declares, names upstream. */
    public static List<Hit> javaNames(Map<String, ClassFile> classes) {
        List<Hit> out = new ArrayList<>();
        for (ClassFile c : classes.values()) {
            String where = FormsLens.where(c, 0).replaceAll(":0$", "");
            String simple = c.name().substring(c.name().lastIndexOf('/') + 1);
            Set<String> names = new LinkedHashSet<>();
            names.add(simple.contains("$") ? simple.substring(simple.lastIndexOf('$') + 1) : simple);
            for (Field f : c.fields()) {
                if ((f.access() & 0x1000) == 0) {
                    names.add(f.name());
                }
            }
            for (Method m : c.methods()) {
                if (!m.isSynthetic() && !m.name().startsWith("<") && !m.name().startsWith("lambda$")) {
                    names.add(m.name());
                }
            }
            for (String n : names) {
                if (!n.matches("\\d+") && selfNamed(n)) {
                    out.add(new Hit("M2", "NAME", where, n, "a declaration naming upstream"));
                }
            }
        }
        return out;
    }

    /** M2 over TypeScript or Python product sources: each distinct name in code that names upstream. */
    public static List<Hit> hostNames(List<Source> sources) {
        List<Hit> out = new ArrayList<>();
        for (Source s : sources) {
            Map<String, Integer> first = new LinkedHashMap<>();
            for (Token t : s.names()) {
                if (t.text().length() > 6 && HOST_SELF_NAMED.matcher(t.text()).find()) {
                    first.putIfAbsent(t.text(), t.line());
                }
            }
            first.forEach((n, line) -> out.add(new Hit("M2", "NAME", s.path() + ":" + line, n, "a name naming upstream")));
        }
        return out;
    }

    // -----------------------------------------------------------------------------------------------------------------
    // M4 PRINT BODIES

    private static final Pattern TO_STRING = Pattern.compile("\\btoString\\s*\\(\\s*\\)\\s*\\{");
    private static final Pattern CLASS_DECL = Pattern.compile("(?m)^\\s*Class\\s+(?:<<[^>]*>>\\s*)?([\\w:]+)");

    private static final Pattern FUNCTION_DECL = Pattern.compile("(?m)^\\s*function\\s+(?:<<[^>]*>>\\s*)?"
            + "(?:\\{[^}]*\\}\\s*)?([\\w:]+)\\s*(?:<[^>]*>)?\\s*\\(");
    private static final Pattern QUALIFIED = Pattern.compile("(?m)^\\s*(\\w+)\\s*\\(([^()]*)\\)\\s*\\{");
    private static final Set<String> NOT_PROPERTIES = Set.of("if", "let", "assert", "fail", "map", "filter", "match",
            "fold", "forAll", "exists", "toString");

    /** M4's BODY: each Pure body a product resource ships -- a function with a body (no native one) and a class's
     *  qualified property -- the world-map tenet's "the platform ships declarations ... no other Pure bodies" (a body
     *  is residue unless it is named). One hit per body, the function's path or the class's and property's names. */
    public static List<Hit> pureBodies(Map<String, String> productResources) {
        List<Hit> out = new ArrayList<>();
        productResources.forEach((name, text) -> {
            if (!name.endsWith(".pure")) {
                return;
            }
            Matcher f = FUNCTION_DECL.matcher(text);
            while (f.find()) {
                int brace = text.indexOf('{', f.end());
                int semi = text.indexOf(';', f.end());
                if (brace >= 0 && (semi < 0 || brace < semi)) {
                    int line = text.substring(0, f.start()).split("\n", -1).length;
                    out.add(new Hit("M4", "BODY", name + ":" + line, f.group(1), "a function with a Pure body"));
                }
            }
            Matcher q = QUALIFIED.matcher(text);
            while (q.find()) {
                if (NOT_PROPERTIES.contains(q.group(1))) {
                    continue;
                }
                String before = text.substring(0, q.start());
                Matcher c = CLASS_DECL.matcher(before);
                String cls = null;
                int at = -1;
                while (c.find()) {
                    cls = c.group(1);
                    at = c.start();
                }
                // a qualified property sits in a class's braces, after its declaration and before any later element
                if (cls == null || before.substring(at).contains("\nfunction ") || before.substring(at).contains("\n###")) {
                    continue;
                }
                int line = before.split("\n", -1).length;
                out.add(new Hit("M4", "BODY", name + ":" + line, cls + "." + q.group(1), "a qualified property's Pure body"));
            }
        });
        return out;
    }

    /** M4: each {@code toString} body of a product Pure resource, with the class it prints. */
    public static List<Hit> printBodies(Map<String, String> productResources) {
        List<Hit> out = new ArrayList<>();
        productResources.forEach((name, text) -> {
            if (!name.endsWith(".pure")) {
                return;
            }
            Matcher m = TO_STRING.matcher(text);
            while (m.find()) {
                String before = text.substring(0, m.start());
                Matcher c = CLASS_DECL.matcher(before);
                String cls = "?";
                while (c.find()) {
                    cls = c.group(1);
                }
                int line = before.split("\n", -1).length;
                int close = text.indexOf('}', m.end());
                String body = text.substring(m.end(), close < 0 ? text.length() : close).strip().replaceAll("\\s+", " ");
                out.add(new Hit("M4", "TOSTRING", name + ":" + line, cls, "toString(){" + body + "}"));
            }
        });
        return out;
    }

    // -----------------------------------------------------------------------------------------------------------------
    // M5 PROVENANCE

    /** A comment declaring that the code reproduces upstream: a quirk, a gap, legend-engine's model, a golden kept as
     *  upstream's run wrote it. Lite's own commenting convention -- the code cites upstream where it copies it -- so a
     *  small marker list, stated as one (census B2's shape note). */
    public static final Pattern REPRODUCTION = Pattern.compile("(?i)\\bquirk|\\bengine gap\\b|\\(engine model\\)"
            + "|as (?:legend-engine|the engine|upstream) (?:writes|does|prints|spells|answers|orders|names|emits)"
            + "|legend-engine's (?:harness|defect|bug|gap|double|order)|reproduc\\w* (?:legend-engine|the engine)"
            + "|replayed here reads as it did in the engine");

    /** M5 over Java sources (path -> text): each comment line holding a marker. */
    public static List<Hit> javaComments(Map<String, String> sources) {
        List<Hit> out = new ArrayList<>();
        sources.forEach((path, text) -> {
            String[] lines = text.split("\n", -1);
            boolean block = false;
            for (int k = 0; k < lines.length; k++) {
                String l = lines[k];
                String comment = null;
                if (block) {
                    comment = l;
                    if (l.contains("*/")) {
                        block = false;
                    }
                } else {
                    int sl = l.indexOf("//");
                    int bl = l.indexOf("/*");
                    if (bl >= 0 && (sl < 0 || bl < sl)) {
                        comment = l.substring(bl);
                        block = !l.substring(bl).contains("*/");
                    } else if (sl >= 0 && !inString(l, sl)) {
                        comment = l.substring(sl);
                    }
                }
                if (comment != null) {
                    Matcher m = REPRODUCTION.matcher(comment);
                    if (m.find()) {
                        out.add(new Hit("M5", "COMMENT", path + ":" + (k + 1), m.group(), comment.strip()));
                    }
                }
            }
        });
        return out;
    }

    private static boolean inString(String line, int at) {
        int quotes = 0;
        for (int i = 0; i < at; i++) {
            if (line.charAt(i) == '"' && (i == 0 || line.charAt(i - 1) != '\\')) {
                quotes++;
            }
        }
        return quotes % 2 == 1;
    }

    /** M5 over TypeScript or Python sources. */
    public static List<Hit> hostComments(List<Source> sources) {
        List<Hit> out = new ArrayList<>();
        for (Source s : sources) {
            for (Token t : s.tokens()) {
                if (t.kind() == Kind.COMMENT || t.kind() == Kind.DOCSTRING) {
                    Matcher m = REPRODUCTION.matcher(t.text());
                    if (m.find()) {
                        out.add(new Hit("M5", "COMMENT", s.path() + ":" + t.line(), m.group(),
                                t.text().strip().replaceAll("\\s+", " ")));
                    }
                }
            }
        }
        return out;
    }

    // -----------------------------------------------------------------------------------------------------------------
    // Host modules: resolution

    /** The module path an import names, resolved against the importing file (TypeScript relative imports, a Python
     *  package's absolute and relative ones), or null when it names no file of {@code known}. */
    static String resolve(Source from, Import imp, Set<String> known) {
        String m = imp.module();
        if (from.lang() == HostSources.Lang.TYPESCRIPT) {
            if (!m.startsWith(".")) {
                return null;
            }
            String dir = from.path().contains("/") ? from.path().substring(0, from.path().lastIndexOf('/')) : "";
            String joined = normalize(dir + "/" + m);
            for (String candidate : List.of(joined, joined.replaceAll("\\.js$", ".ts"), joined + ".ts",
                    joined.replaceAll("\\.mjs$", ".mts"), joined + "/index.ts")) {
                if (known.contains(candidate)) {
                    return candidate;
                }
            }
            return null;
        }
        // Python: the package root is the directory above the top-level package (python/)
        String path = from.path();
        String module;
        if (m.startsWith(".")) {
            int dots = 0;
            while (dots < m.length() && m.charAt(dots) == '.') {
                dots++;
            }
            String pkg = path.substring(0, path.lastIndexOf('/'));
            for (int d = 1; d < dots; d++) {
                pkg = pkg.substring(0, pkg.lastIndexOf('/'));
            }
            String rest = m.substring(dots).replace('.', '/');
            module = rest.isEmpty() ? pkg : pkg + "/" + rest;
        } else {
            String root = pythonRoot(path);
            module = root + m.replace('.', '/');
        }
        for (String candidate : List.of(module + ".py", module + "/__init__.py")) {
            if (known.contains(candidate)) {
                return candidate;
            }
        }
        return null;
    }

    /** The directory holding a Python file's top-level package ("python/" for python/legend_lite/x.py). */
    static String pythonRoot(String path) {
        String[] parts = path.split("/");
        StringBuilder root = new StringBuilder();
        for (String p : parts) {
            if (p.startsWith("legend_lite")) {
                break;
            }
            root.append(p).append('/');
        }
        return root.toString();
    }

    static String normalize(String path) {
        Deque<String> parts = new ArrayDeque<>();
        for (String p : path.split("/")) {
            if (p.isEmpty() || p.equals(".")) {
                continue;
            }
            if (p.equals("..") && !parts.isEmpty() && !parts.peekLast().equals("..")) {
                parts.removeLast();
            } else {
                parts.addLast(p);
            }
        }
        return String.join("/", parts);
    }

    // -----------------------------------------------------------------------------------------------------------------
    // M6 EDGE-ONLY

    /** M6: each export of a product module whose only users, among the non-test sources, are edge modules. A
     *  TypeScript export is used where a module imports it by name (a namespace import uses every export); a Python
     *  one where another module spells its name. */
    public static List<Hit> edgeOnly(List<Source> product, List<Source> edge, Predicate<String> isTest) {
        List<Hit> out = new ArrayList<>();
        Map<String, Source> byPath = new HashMap<>();
        product.forEach(s -> byPath.put(s.path(), s));
        edge.forEach(s -> byPath.put(s.path(), s));
        Set<String> edgePaths = new HashSet<>();
        edge.forEach(s -> edgePaths.add(s.path()));
        Set<String> known = byPath.keySet();
        // TypeScript: module -> export -> users
        Map<String, Map<String, Set<String>>> users = new HashMap<>();
        Set<String> namespaced = new HashSet<>();
        for (Source s : byPath.values()) {
            if (s.lang() != HostSources.Lang.TYPESCRIPT || isTest.test(s.path())) {
                continue;
            }
            for (Import imp : s.imports()) {
                String target = resolve(s, imp, known);
                if (target == null) {
                    continue;
                }
                if (imp.names().contains("*")) {
                    namespaced.add(target);
                }
                for (String n : imp.names()) {
                    users.computeIfAbsent(target, k -> new HashMap<>()).computeIfAbsent(n, k -> new TreeSet<>())
                            .add(s.path());
                }
            }
        }
        for (Source s : product) {
            if (s.lang() == HostSources.Lang.TYPESCRIPT && !isTest.test(s.path()) && !namespaced.contains(s.path())) {
                Map<String, Set<String>> u = users.getOrDefault(s.path(), Map.of());
                for (Token e : s.exports()) {
                    Set<String> who = u.get(e.text());
                    if (who != null && !who.isEmpty() && edgePaths.containsAll(who)) {
                        out.add(new Hit("M6", "EXPORT", s.path() + ":" + e.line(), e.text(), "used by the edge alone: "
                                + who));
                    }
                }
            }
        }
        // Python: a top-level name of a product module spelled by other modules: edge ones alone
        Map<String, Set<String>> spelled = new HashMap<>();
        for (Source s : byPath.values()) {
            if (s.lang() != HostSources.Lang.PYTHON || isTest.test(s.path())) {
                continue;
            }
            for (Token t : s.names()) {
                spelled.computeIfAbsent(t.text(), k -> new TreeSet<>()).add(s.path());
            }
            for (Import imp : s.imports()) {
                for (String n : imp.names()) {
                    spelled.computeIfAbsent(n, k -> new TreeSet<>()).add(s.path());
                }
            }
        }
        for (Source s : product) {
            if (s.lang() != HostSources.Lang.PYTHON || isTest.test(s.path())) {
                continue;
            }
            for (Token e : s.exports()) {
                Set<String> who = new TreeSet<>(spelled.getOrDefault(e.text(), Set.of()));
                who.remove(s.path());
                if (!who.isEmpty() && edgePaths.containsAll(who)) {
                    out.add(new Hit("M6", "EXPORT", s.path() + ":" + e.line(), e.text(), "used by the edge alone: "
                            + who));
                }
            }
        }
        return out;
    }

    // -----------------------------------------------------------------------------------------------------------------
    // M7 K2

    /** M7 over TypeScript or Python sources: each product import of an edge module or of an opt-in package -- STATIC
     *  a reach at load (a violation of "edge off loads no edge"), DYNAMIC or GUARDED an opt-in mechanism, reported as
     *  one -- and each dynamic import whose module is computed. */
    public static List<Hit> hostReach(List<Source> product, Set<String> edgePaths, Set<String> known,
            Predicate<String> isTest, List<String> optInPackages) {
        List<Hit> out = new ArrayList<>();
        for (Source s : product) {
            if (isTest.test(s.path())) {
                continue;
            }
            for (Import imp : s.imports()) {
                String target = resolve(s, imp, known);
                boolean edge = target != null && edgePaths.contains(target);
                String pkg = imp.module().startsWith(".") ? "" : imp.module().split("\\.", 2)[0];
                boolean optInPackage = s.lang() == HostSources.Lang.PYTHON && optInPackages.contains(pkg);
                if (!edge && !optInPackage) {
                    continue;
                }
                String category = imp.kind() == ImportKind.STATIC ? "STATIC" : imp.kind() == ImportKind.TYPE_ONLY
                        ? "TYPE_ONLY" : "OPT_IN";
                out.add(new Hit("M7", category, s.path() + ":" + imp.line(), imp.module(), (edge ? "the edge's "
                        + target : "the opt-in package " + pkg) + ", " + imp.kind().name().toLowerCase(java.util.Locale.ROOT)
                        + " import" + (imp.names().isEmpty() ? "" : " of " + imp.names())));
            }
        }
        return out;
    }

    /** M7 over Java: each edge class a product root reaches through class references (its pool's classes and the
     *  types its descriptors name), with the path; and each product class that looks a provider up by
     *  ServiceLoader (an opt-in mechanism, reported as one). */
    public static List<Hit> javaReach(Map<String, ClassFile> all, Set<String> roots, Predicate<String> isEdge,
            String host) {
        List<Hit> out = new ArrayList<>();
        Map<String, String> parent = new HashMap<>();
        Deque<String> work = new ArrayDeque<>();
        for (String r : roots) {
            if (all.containsKey(r)) {
                parent.put(r, null);
                work.add(r);
            }
        }
        Set<String> reportedEdge = new TreeSet<>();
        while (!work.isEmpty()) {
            String c = work.poll();
            ClassFile cf = all.get(c);
            if (cf == null) {
                continue;
            }
            if (isEdge.test(c)) {
                reportedEdge.add(c);
                continue;
            }
            for (String ref : references(cf)) {
                if (!parent.containsKey(ref) && all.containsKey(ref)) {
                    parent.put(ref, c);
                    work.add(ref);
                }
            }
            for (Method m : cf.methods()) {
                for (ClassFiles.Insn i : m.code()) {
                    if (i.op() == 184) {
                        ClassFiles.Ref r = cf.ref(i.operand());
                        if (r.owner().equals("java/util/ServiceLoader") && r.name().equals("load")) {
                            out.add(new Hit("M7", "OPT_IN", FormsLens.where(cf, m.lineAt(i.pc())), "ServiceLoader.load",
                                    host + ": " + cf.binaryName() + "#" + m.name() + " looks a provider up"));
                        }
                    }
                }
            }
        }
        for (String e : reportedEdge) {
            List<String> path = new ArrayList<>();
            for (String p = e; p != null; p = parent.get(p)) {
                path.add(0, p.substring(p.lastIndexOf('/') + 1));
            }
            out.add(new Hit("M7", "STATIC", e.replace('/', '.'), e.substring(e.lastIndexOf('/') + 1), host
                    + " reaches the edge at load: " + String.join(" -> ", path)));
        }
        return out;
    }

    /** M7's doors: each edge class with a {@code public static void main(String[])} -- an entry to upstream's routes
     *  that no declared opt-in loads (HOSTS-7: {@code java -cp server_deploy.jar com.legend.edge.EdgeServer}). */
    public static List<Hit> edgeDoors(Map<String, ClassFile> edge) {
        List<Hit> out = new ArrayList<>();
        for (ClassFile c : edge.values()) {
            for (Method m : c.methods()) {
                if (m.name().equals("main") && m.descriptor().equals("([Ljava/lang/String;)V")
                        && (m.access() & 0x0009) == 0x0009) {
                    out.add(new Hit("M7", "DOOR", FormsLens.where(c, m.lineAt(m.code().isEmpty() ? 0
                            : m.code().get(0).pc())), c.binaryName() + "#main", "an edge class a JVM can start by"
                            + " name: legend-engine's routes with no declared opt-in"));
                }
            }
        }
        return out;
    }

    /** The classes a class file names: its pool's CONSTANT_Class entries and every L...; type in its descriptors. */
    static Set<String> references(ClassFile c) {
        Set<String> out = new LinkedHashSet<>();
        Constant[] pool = c.pool();
        for (Constant k : pool) {
            if (k == null) {
                continue;
            }
            if (k.tag() == 7) {
                String n = (String) pool[k.a()].value();
                out.add(n.startsWith("[") ? n.replaceAll("^\\[+L?", "").replaceAll(";$", "") : n);
            } else if (k.tag() == 12 || k.tag() == 16) {
                String d = (String) pool[k.tag() == 12 ? k.b() : k.a()].value();
                Matcher m = Pattern.compile("L([^;]+);").matcher(d);
                while (m.find()) {
                    out.add(m.group(1));
                }
            }
        }
        for (Field f : c.fields()) {
            Matcher m = Pattern.compile("L([^;]+);").matcher(f.descriptor());
            while (m.find()) {
                out.add(m.group(1));
            }
        }
        for (Method mm : c.methods()) {
            Matcher m = Pattern.compile("L([^;]+);").matcher(mm.descriptor());
            while (m.find()) {
                out.add(m.group(1));
            }
        }
        if (c.superName() != null) {
            out.add(c.superName());
        }
        out.addAll(c.interfaces());
        return out;
    }

    /** M7 over a Bazel query's answer (genquery: one label per line): each target there, a direct reach of an edge
     *  target from a shipped artifact's closure that no declared opt-in carries. */
    public static List<Hit> buildReach(List<String> labels, String query) {
        List<Hit> out = new ArrayList<>();
        for (String l : labels) {
            if (!l.isBlank()) {
                out.add(new Hit("M7", "BUILD", l.strip(), l.strip(), query
                        + ": a shipped artifact's target depends directly on an edge target"));
            }
        }
        return out;
    }

    /** Hits by lens and category, counted. */
    public static Map<String, Integer> counts(List<Hit> hits) {
        Map<String, Integer> out = new TreeMap<>();
        for (Hit h : hits) {
            out.merge(h.lens() + " " + h.category(), 1, Integer::sum);
        }
        return out;
    }
}
