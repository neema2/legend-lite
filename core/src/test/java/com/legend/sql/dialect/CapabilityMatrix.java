// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.sql.dialect;

import com.legend.model.ConnectionDefinition.DatabaseType;
import com.legend.sql.SqlAgg;
import com.legend.sql.SqlFn;

import java.io.IOException;
import java.lang.reflect.Modifier;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

/**
 * THE CAPABILITY MATRIX (the user's decision P1b, 2026-10-13; docs/DIALECT_LEAK_CENSUS_AND_PLAN_2026_10_12.md, Phase
 * 1b): what each concrete dialect does with every function ({@link SqlFn}) and every aggregate ({@link SqlAgg.Fn}),
 * read from its own tables, and how many PCT cases reach each one there. {@code //core:capability_matrix} writes it
 * (an overview, then the functions' and the aggregates' files, a section per dialect, and {@code matrix.tsv}, every
 * row of them as data) and {@code bazel run //docs:update_dialect_capability} copies it into
 * {@code docs/dialect-capability/}. The data's diff test fails when a dialect's table moves and the matrix was not
 * regenerated; the pages are written with it and never tested (no test reads Markdown: G17, MarkdownInputsTest). Its files are fixed and its
 * dialects are read from the source directory, so a new dialect needs no line here or in a BUILD file.
 *
 * <p>Each decision is read from the arm the dialect's switch gives the function ({@code call}'s
 * {@code switch (c.fn())}, {@code reducer}'s {@code switch (r.fn())}). Every arm is one expression
 * (DialectVocabularyTest), so the arm says it all:
 * <ul>
 *   <li><b>refused</b>: the arm throws, by name (its message quoted), or calls a method that only throws;</li>
 *   <li><b>spelled</b>: the arm writes one SQL function, keyword or operator by name
 *       ({@code writer.function("upper", a)}, {@code aggregate(writer, "SUM", r)}, {@code infix(writer, c, p)});</li>
 *   <li><b>emulated</b>: anything else -- a composed expression, or a named helper that builds one.</li>
 * </ul>
 * An arm that hands the function to the dialect's parent ({@code super.call}) is read in the parent; an arm that hands
 * it to a helper deciding by function ({@code listCall(writer, c.fn(), a)}, with its own switch on that function) is
 * read in the helper, the helper found as Java finds it, from the dialect's own class up. A reading this cannot make
 * (no source, no arm) fails the build: the matrix never guesses.
 *
 * <p>The reach comes from {@code docs/dialect-capability/reach.tsv}, measured by {@link CapabilityReach} over the PCT
 * lanes' recorded cases, for each dialect a database type is planned with: a function no case reaches there is an arm
 * no PCT test exercises on that database.
 */
public final class CapabilityMatrix {

    private CapabilityMatrix() {
    }

    /** What a dialect does with a function: spelled, emulated, refused by name (a capability it lacks), or NOT HERE,
     *  an arm the IR never reaches (a window function, which {@code windowCall} writes; a lowering marker, composed
     *  before rendering), which throws the renderer's "a caller's bug" ({@code IllegalStateException}) and is no
     *  decision of the dialect's. */
    enum Kind { SPELLED, EMULATED, REFUSED, NOT_HERE }

    /** A decision: its kind, what it writes (a name, a template, a helper) or why it refuses, and what it was read
     *  through (a parent, a helper). */
    record Decision(Kind kind, String detail, List<String> via) {
    }

    /**
     * One vocabulary: its title, its name in reach.tsv, the dialect method deciding it, the parameter that method
     * decides on (its type and name, a pattern), the switch it decides in, and the types a helper's parameter has
     * when the helper decides by the same function.
     */
    record Vocabulary(String title, String key, String method, String param, String sw, String helperTypes,
            List<String> members) {
    }

    static final List<Vocabulary> VOCABULARIES = List.of(
            new Vocabulary("functions", "call", "call", "SqlExpr\\.Call\\s+c\\b", "switch (c.fn())",
                    "(?:com\\.legend\\.sql\\.)?(?:SqlFn|SqlExpr\\.Call)",
                    Arrays.stream(SqlFn.values()).map(Enum::name).toList()),
            new Vocabulary("aggregates", "reducer", "reducer", "(?:com\\.legend\\.sql\\.)?SqlAgg\\.Reducer\\s+r\\b",
                    "switch (r.fn())", "(?:com\\.legend\\.sql\\.)?SqlAgg\\.(?:Fn|Reducer)",
                    Arrays.stream(SqlAgg.Fn.values()).map(Enum::name).toList()));

    /** The database types each dialect is planned with ({@code Databases.dialect}), by dialect class; a dialect no
     *  database type names (a server-version profile, legend-engine's text) has none. */
    static Map<Class<?>, List<DatabaseType>> databases() {
        Map<Class<?>, List<DatabaseType>> out = new LinkedHashMap<>();
        for (DatabaseType t : DatabaseType.values()) {
            try {
                out.computeIfAbsent(com.legend.database.Databases.dialect(t).getClass(), k -> new ArrayList<>()).add(t);
            } catch (com.legend.error.NotImplementedException noDialect) {
                // a database type with no dialect yet: no column of the matrix
            }
        }
        return out;
    }

    /**
     * usage: CapabilityMatrix &lt;out dir&gt; &lt;reach.tsv&gt; &lt;dialect source dir&gt; -- writes README.md,
     * functions.md and aggregates.md for every concrete dialect in the source directory.
     */
    public static void main(String[] args) throws IOException, ClassNotFoundException {
        Path out = Path.of(args[0]);
        Map<String, Map<String, Integer>> reach = readReach(Path.of(args[1]));
        Map<String, String> sources = new TreeMap<>();
        try (Stream<Path> files = Files.list(Path.of(args[2]))) {
            for (Path p : files.toList()) {
                String file = p.getFileName().toString();
                if (file.endsWith(".java") && !file.equals("package-info.java")) {
                    sources.put(file.substring(0, file.length() - ".java".length()), stripComments(Files.readString(p)));
                }
            }
        }
        Files.createDirectories(out);
        for (Map.Entry<String, String> f : write(sources, dialects(sources.keySet()), reach).entrySet()) {
            Files.writeString(out.resolve(f.getKey()), f.getValue(), StandardCharsets.UTF_8);
        }
    }

    /**
     * The concrete dialects among {@code names}, in the matrix's order: each database type's dialect in the order
     * the database types are declared, then each dialect no database type names, after its nearest ancestor among them
     * (EngineStyleDB2 after EngineStyleH2), the rest at the end by name.
     */
    static List<Class<?>> dialects(Set<String> names) throws ClassNotFoundException {
        Set<Class<?>> concrete = new LinkedHashSet<>();
        for (String name : new TreeSet<>(names)) {
            Class<?> cls = Class.forName("com.legend.sql.dialect." + name, false,
                    CapabilityMatrix.class.getClassLoader());
            if (cls != AnsiSqlRenderer.class && AnsiSqlRenderer.class.isAssignableFrom(cls)
                    && !Modifier.isAbstract(cls.getModifiers())) {
                concrete.add(cls);
            }
        }
        List<Class<?>> order = new ArrayList<>();
        for (Class<?> d : databases().keySet()) {
            if (!concrete.contains(d)) {
                throw new IllegalStateException(d.getSimpleName() + " is a database's dialect with no source read");
            }
            order.add(d);
        }
        List<Class<?>> rest = new ArrayList<>(concrete);
        rest.removeAll(order);
        // a parent before its children: the fewer ancestors first
        rest.sort(java.util.Comparator.comparingInt(CapabilityMatrix::depth)
                .thenComparing(Class::getSimpleName));
        for (Class<?> d : rest) {
            int at = -1;
            for (Class<?> a = d.getSuperclass(); a != null && at < 0; a = a.getSuperclass()) {
                at = order.lastIndexOf(a);
                for (int i = at + 1; at >= 0 && i < order.size() && a.isAssignableFrom(order.get(i)); i++) {
                    at = i;
                }
            }
            order.add(at < 0 ? order.size() : at + 1, d);
        }
        return order;
    }

    private static int depth(Class<?> c) {
        int n = 0;
        for (Class<?> a = c; a != null; a = a.getSuperclass()) {
            n++;
        }
        return n;
    }

    /** Every file of the matrix, by name: the overview, then each vocabulary's, a section per dialect. */
    static Map<String, String> write(Map<String, String> sources, List<Class<?>> dialects,
            Map<String, Map<String, Integer>> reach) {
        Map<Class<?>, List<DatabaseType>> dbs = databases();
        Map<Class<?>, Map<String, Map<String, Decision>>> all = new LinkedHashMap<>();
        for (Class<?> d : dialects) {
            Map<String, Map<String, Decision>> byVocab = new LinkedHashMap<>();
            for (Vocabulary v : VOCABULARIES) {
                Map<String, Decision> decisions = new LinkedHashMap<>();
                for (String m : v.members()) {
                    decisions.put(m, decide(sources, d, v, m));
                }
                byVocab.put(v.title(), decisions);
            }
            all.put(d, byVocab);
        }
        Map<String, String> files = new LinkedHashMap<>();
        files.put("matrix.tsv", data(all, dbs, reach));
        files.put("README.md", overview(all, dbs, reach));
        for (Vocabulary v : VOCABULARIES) {
            files.put(v.title() + ".md", vocabulary(v, all, dbs, reach));
        }
        return files;
    }

    /**
     * The matrix as data, one row per dialect and member: everything the pages are made of, so the pages move only
     * when it does. It, not a page, is what the diff test reads: prose is never a test input (G17,
     * MarkdownInputsTest).
     */
    static String data(Map<Class<?>, Map<String, Map<String, Decision>>> all, Map<Class<?>, List<DatabaseType>> dbs,
            Map<String, Map<String, Integer>> reach) {
        StringBuilder b = new StringBuilder("""
                # The capability matrix as data (CapabilityMatrix; docs/dialect-capability/README.md): GENERATED by
                # `bazel run //docs:update_dialect_capability`, which writes the pages beside it from the same rows.
                # dialect TAB database types TAB vocabulary TAB member TAB decision TAB writes, or why not TAB
                # read through TAB PCT cases (- where no PCT lane runs the dialect)
                """);
        for (Map.Entry<Class<?>, Map<String, Map<String, Decision>>> d : all.entrySet()) {
            String name = d.getKey().getSimpleName();
            boolean measured = dbs.containsKey(d.getKey());
            String types = String.join(",", dbs.getOrDefault(d.getKey(), List.of()).stream().map(Enum::name).toList());
            for (Vocabulary v : VOCABULARIES) {
                for (Map.Entry<String, Decision> e : d.getValue().get(v.title()).entrySet()) {
                    Decision dec = e.getValue();
                    b.append(name).append('\t').append(types.isEmpty() ? "-" : types).append('\t').append(v.key())
                            .append('\t').append(e.getKey()).append('\t')
                            .append(dec.kind().name().toLowerCase(Locale.ROOT)).append('\t')
                            .append(dec.detail().replace('\t', ' ')).append('\t').append(String.join(", ", dec.via()))
                            .append('\t').append(measured ? String.valueOf(reached(reach, name, v, e.getKey())) : "-")
                            .append('\n');
                }
            }
        }
        return b.toString();
    }

    // ---- reading a decision -----------------------------------------------------------------------------------

    /** The decision {@code dialect}'s table makes for {@code member}: its own arm, or its parent's where it hands the
     *  member on. */
    static Decision decide(Map<String, String> sources, Class<?> dialect, Vocabulary v, String member) {
        List<String> via = new ArrayList<>();
        for (Class<?> c = dialect; c != AnsiSqlRenderer.class; c = c.getSuperclass()) {
            String body = decider(source(sources, c), v);
            if (body == null) {
                throw new IllegalStateException(c.getSimpleName() + " declares no " + v.method() + " deciding in "
                        + v.sw());
            }
            String arm = arm(body, v.sw(), member);
            if (arm == null) {
                throw new IllegalStateException(c.getSimpleName() + "." + v.method() + " has no arm for " + member);
            }
            if (!arm.contains("super." + v.method() + "(")) {
                return classify(sources, dialect, v, member, arm, via, 0);
            }
            via.add("as " + c.getSuperclass().getSimpleName());
        }
        throw new IllegalStateException(dialect.getSimpleName() + " hands " + member + " to the base renderer");
    }

    /** The body of {@code v}'s deciding method in {@code src}: the method itself, or the one it hands every point to
     *  (SQLite.calls, ClickHouse.reducerIn), as DialectVocabularyTest finds it; null when there is none. */
    static String decider(String src, Vocabulary v) {
        String body = methodBody(src, v.method(), v.param());
        if (body != null && !body.contains(v.sw())) {
            Matcher to = Pattern.compile("return\\s+(\\w+)\\(writer,\\s*[cr]\\b").matcher(body);
            body = to.find() ? methodBody(src, to.group(1), v.param()) : null;
        }
        return body;
    }

    /** The body of the method {@code name} whose second parameter is {@code param} ({@code (SqlWriter writer,
     *  SqlExpr.Call c, ...)}); null when the class has none. */
    static String methodBody(String src, String name, String param) {
        Matcher m = Pattern.compile("\\b" + Pattern.quote(name) + "\\s*\\(\\s*SqlWriter\\s+writer\\s*,\\s*" + param)
                .matcher(src);
        while (m.find()) {
            int open = src.indexOf('{', m.end());
            int semi = src.indexOf(';', m.end());
            if (open >= 0 && (semi < 0 || open < semi)) {
                return block(src, open);
            }
        }
        return null;
    }

    private static final Pattern LEADING_CALL = Pattern.compile("^(?:this\\.|([A-Z]\\w*)\\.)?([a-z]\\w*)\\s*\\(");

    private static final Pattern STRING = Pattern.compile("\"((?:[^\"\\\\]|\\\\.)*)\"");

    /** A literal that names one function or keyword: {@code upper}, {@code upper(}, {@code pi()}. */
    private static final Pattern NAME = Pattern.compile("([A-Za-z_][\\w.]*)\\s*(?:\\(\\s*\\)?)?");

    /** An unqualified call, or a call on a class of the dialect package: a helper. */
    private static final Pattern CALL = Pattern.compile("(?<![.\\w])(?:([A-Z]\\w*)\\.)?([a-z]\\w*)\\s*\\(");

    /** The helpers that write an operator as it is, by name: an operator is spelled. */
    private static final Set<String> OPERATORS =
            Set.of("infix", "notPrefix", "isNullTest", "isNotNullTest", "negation");

    /** What an arm calls, unqualified, that is no helper: Java's own words, the precedence wrapper ({@code op}), and
     *  the guard an ordered aggregate is refused by ({@code unordered}, noted on the spelling). The writer's own
     *  methods are always called on it ({@code writer.function(...)}), so are never unqualified. */
    private static final Set<String> NOT_HELPERS = Set.of("op", "switch", "new", "throw", "return", "yield", "if",
            "requireNonNull", "unordered", "super", "this");

    static Decision classify(Map<String, String> sources, Class<?> dialect, Vocabulary v, String member, String arm,
            List<String> via, int depth) {
        String t = arm.strip();
        if (t.startsWith("{")) {
            // a helper's block arm (a table's own are refused by DialectVocabularyTest): it refuses, or composes
            return t.replaceAll("\\s+", " ").matches("\\{ ?throw [^;]*; ?\\}")
                    ? refusal(sources, dialect, t, via)
                    : new Decision(Kind.EMULATED, "composed in a block", via);
        }
        if (t.startsWith("throw ")) {
            return refusal(sources, dialect, t, via);
        }
        List<String> branches = alternatives(t);
        if (branches.size() > 1) {
            // a choice made as it writes, by its operands (their types, a literal's value): each alternative's text
            return new Decision(Kind.EMULATED, "by its operands: " + String.join(" / ",
                    branches.stream().map(CapabilityMatrix::template).toList()), via);
        }
        Matcher lead = LEADING_CALL.matcher(t);
        if (lead.find() && !NOT_HELPERS.contains(lead.group(2))) {
            String helper = lead.group(2);
            String args = callArguments(t, lead.end() - 1);
            List<String> names = literals(args);
            if (OPERATORS.contains(helper)) {
                return new Decision(Kind.SPELLED, "an operator (" + helper + ")", via);
            }
            if (helper.equals("aggregate") && !names.isEmpty()) {
                return new Decision(Kind.SPELLED, names.get(0) + (args.contains("unordered(") ? " (unordered)" : ""),
                        via);
            }
            Helper h = helper(sources, dialect, lead.group(1), helper);
            boolean handed = Pattern.compile("[(,]\\s*[cr](?:\\.fn\\(\\))?\\s*[,)]").matcher("(" + args + ")")
                    .find();
            List<String> through = new ArrayList<>(via);
            for (int hops = 0; h != null && hops < 4; hops++) {
                through.add("via " + h.owner() + "." + helper);
                if (h.onlyThrows()) {
                    return refusal(sources, dialect, h.body(), through);
                }
                if (!handed || depth >= 4) {
                    break;
                }
                // a helper deciding by this very function: its own arm for the member
                String sw = h.switchOn(v);
                String sub = sw == null ? null : arm(h.body(), sw, member);
                if (sub != null) {
                    return classify(sources, dialect, v, member, sub, through, depth + 1);
                }
                // a helper handing the function on whole ({ return list(writer, fn, args, null); })
                String fn = h.functionParam(v);
                Matcher on = fn == null ? null : Pattern.compile("^\\{\\s*return\\s+(\\w+)\\s*\\(\\s*writer\\s*,\\s*"
                        + fn + "\\b[^;]*;\\s*}$").matcher(h.body());
                if (on == null || !on.find()) {
                    break;
                }
                helper = on.group(1);
                h = helper(sources, dialect, null, helper);
            }
            return new Decision(Kind.EMULATED, lead.group(2) + (names.isEmpty() ? "" : ": " + String.join(", ", names)),
                    via);
        }
        // the writer's own calls: one name written, nothing else but punctuation and arguments, is a spelling
        List<String> lits = literals(t);
        List<String> named = new ArrayList<>();
        boolean onlyPunctuation = true;
        for (String s : lits) {
            Matcher n = NAME.matcher(s.strip());
            if (n.matches()) {
                named.add(n.group(1));
            } else if (!s.matches("[(),\\s]*")) {
                onlyPunctuation = false;
            }
        }
        List<String> helpers = new ArrayList<>();
        Matcher call = CALL.matcher(blankStrings(t));
        while (call.find()) {
            boolean dialectClass = call.group(1) == null || sources.containsKey(call.group(1));
            if (dialectClass && !NOT_HELPERS.contains(call.group(2)) && !helpers.contains(call.group(2))) {
                helpers.add(call.group(2));
            }
        }
        if (named.size() == 1 && onlyPunctuation && helpers.isEmpty()) {
            return new Decision(Kind.SPELLED, named.get(0) + (t.contains("unordered(") ? " (unordered)" : ""), via);
        }
        String template = template(t);
        Matcher infix = INFIX.matcher(template);
        if (helpers.isEmpty() && infix.matches()) {
            return new Decision(Kind.SPELLED, "an operator (" + infix.group(1) + ")", via);
        }
        return new Decision(Kind.EMULATED, helpers.isEmpty() ? template
                : String.join(", ", helpers) + (lits.isEmpty() ? "" : ": " + template), via);
    }

    /** The alternatives of an arm that is a conditional expression ({@code c ? a : b ? d : e}): its branches, the
     *  condition dropped; the arm alone when it is none. */
    static List<String> alternatives(String t) {
        int q = topLevel(t, '?', 0);
        if (q < 0) {
            return List.of(t);
        }
        int colon = topLevel(t, ':', q + 1);
        if (colon < 0) {
            return List.of(t);
        }
        List<String> out = new ArrayList<>();
        out.add(t.substring(q + 1, colon).strip());
        out.addAll(alternatives(t.substring(colon + 1).strip()));
        return out;
    }

    /** The first {@code ch} at depth 0 from {@code from} (outside parentheses, brackets, braces and literals; a
     *  {@code ::} is no colon, and a nested conditional's {@code :} belongs to it); -1 when there is none. */
    private static int topLevel(String t, char ch, int from) {
        int depth = 0;
        int nested = 0;
        for (int i = from; i < t.length(); i++) {
            char c = t.charAt(i);
            if (c == '"' || c == '\'') {
                i = skipQuoted(t, i);
            } else if (c == '(' || c == '[' || c == '{') {
                depth++;
            } else if (c == ')' || c == ']' || c == '}') {
                depth--;
            } else if (depth == 0 && c == ':' && (t.startsWith("::", i) || (i > 0 && t.charAt(i - 1) == ':'))) {
                i++;
            } else if (depth == 0 && ch == ':' && c == '?') {
                nested++;
            } else if (depth == 0 && c == ch) {
                if (ch == ':' && nested > 0) {
                    nested--;
                } else {
                    return i;
                }
            }
        }
        return -1;
    }

    /** An operator written between its operands, as a template: {@code (… & …)}, {@code … IS DISTINCT FROM …}. */
    private static final Pattern INFIX =
            Pattern.compile("\\(?… ?([^\\w\\s…()'\"]+|[A-Za-z]+(?: [A-Za-z]+)*) ?…\\)?");

    /** A throwing arm: the exception it makes and the literals of its message, read where it is made (the arm, or
     *  the method it names). {@code IllegalStateException} is the renderer's "a caller's bug": NOT HERE. */
    private static Decision refusal(Map<String, String> sources, Class<?> dialect, String text, List<String> via) {
        String made = text;
        Matcher maker = Pattern.compile("throw\\s+(?:([A-Z]\\w*)\\.)?([a-z]\\w*)\\s*\\(").matcher(text);
        if (maker.find()) {
            Helper h = helper(sources, dialect, maker.group(1), maker.group(2));
            if (h == null) {
                throw new IllegalStateException("a refusal made by " + maker.group(2) + ", which no source declares");
            }
            made = text + h.body();
        }
        Matcher exception = Pattern.compile("new\\s+(?:[\\w.]+\\.)?([A-Z]\\w*)\\s*\\(").matcher(made);
        if (!exception.find()) {
            throw new IllegalStateException("a refusal whose exception is not read: " + text);
        }
        String r = String.join("", literals(made)).replaceAll("\\s+", " ").strip();
        r = r.isEmpty() ? exception.group(1) : r;
        r = r.length() > 140 ? r.substring(0, 137) + "..." : r;
        return new Decision(exception.group(1).equals("IllegalStateException") ? Kind.NOT_HERE : Kind.REFUSED, r, via);
    }

    /** What an arm writes, its literals in order, each stretch of code that writes an expression between them
     *  marked {@code …}. */
    private static String template(String t) {
        StringBuilder b = new StringBuilder();
        Matcher m = STRING.matcher(t);
        int last = 0;
        while (m.find()) {
            if (writesExpression(t.substring(last, m.start()))) {
                b.append('…');
            }
            b.append(m.group(1).replace("\\\"", "\"").replace("\\\\", "\\"));
            last = m.end();
        }
        if (writesExpression(t.substring(last))) {
            b.append('…');
        }
        String s = b.toString().replaceAll("\\s+", " ").strip();
        if (s.isEmpty() || s.equals("…")) {
            return "composed";
        }
        return s.length() > 80 ? s.substring(0, 77) + "..." : s;
    }

    private static boolean writesExpression(String code) {
        return Pattern.compile("\\b(?:expr|function|list|piece|get)\\s*\\(|(?<![.\\w])[a-z]\\w*\\s*\\(writer\\b")
                .matcher(code).find();
    }

    /** The text inside the call's parentheses, the one opening at {@code open}. */
    private static String callArguments(String t, int open) {
        int depth = 0;
        for (int i = open; i < t.length(); i++) {
            char ch = t.charAt(i);
            if (ch == '"' || ch == '\'') {
                i = skipQuoted(t, i);
            } else if (ch == '(') {
                depth++;
            } else if (ch == ')' && --depth == 0) {
                return t.substring(open + 1, i);
            }
        }
        return t.substring(open + 1);
    }

    private static List<String> literals(String s) {
        List<String> out = new ArrayList<>();
        Matcher m = STRING.matcher(s);
        while (m.find()) {
            out.add(m.group(1).replace("\\\"", "\"").replace("\\\\", "\\"));
        }
        return out;
    }

    private static String blankStrings(String s) {
        return STRING.matcher(s).replaceAll("\"\"");
    }

    /** A helper an arm calls: its parameters, its body, the class declaring it. */
    record Helper(String params, String body, String owner) {

        /** Its body is one {@code throw}: it refuses whatever it is given. */
        boolean onlyThrows() {
            return body.replaceAll("\\s+", " ").matches("\\{ ?throw [^;]*; ?\\}");
        }

        /** The name of its parameter that is the function it is handed ({@code SqlFn fn}, {@code SqlExpr.Call c});
         *  null when it has none. */
        String functionParam(Vocabulary v) {
            Matcher p = Pattern.compile("(?:final\\s+)?" + v.helperTypes() + "\\s+(\\w+)").matcher(params);
            return p.find() ? p.group(1) : null;
        }

        /** The switch on the function it is handed, when it decides by it ({@code switch (fn)} for a parameter
         *  {@code SqlFn fn}, {@code switch (c.fn())} for {@code SqlExpr.Call c}); null otherwise. */
        String switchOn(Vocabulary v) {
            Matcher p = Pattern.compile("(?:final\\s+)?" + v.helperTypes() + "\\s+(\\w+)").matcher(params);
            while (p.find()) {
                String whole = p.group();
                String name = p.group(1);
                for (String sw : List.of("switch (" + name + ")", "switch (" + name + ".fn())")) {
                    if (body.contains(sw) && (sw.endsWith(".fn())") == (whole.contains("Call")
                            || whole.contains("Reducer")))) {
                        return sw;
                    }
                }
            }
            return null;
        }
    }

    /** The method {@code name} as a call in {@code dialect} finds it: in {@code qualifier} when the call names a
     *  class, else from the dialect's own class up (an override wins). Among overloads, one deciding by function
     *  first. Null when no source declares it with a body. */
    static Helper helper(Map<String, String> sources, Class<?> dialect, String qualifier, String name) {
        List<String> owners = new ArrayList<>();
        if (qualifier != null) {
            owners.add(qualifier);
        } else {
            for (Class<?> c = dialect; c != null && c != Object.class; c = c.getSuperclass()) {
                owners.add(c.getSimpleName());
            }
        }
        for (String owner : owners) {
            String src = sources.get(owner);
            if (src == null) {
                continue;
            }
            Matcher m = Pattern.compile("(?m)^\\s+(?:(?:public|protected|private|static|final)\\s+)*"
                    + "[\\w.<>\\[\\], ?@]+\\s+" + Pattern.quote(name) + "\\s*\\(").matcher(src);
            List<Helper> found = new ArrayList<>();
            while (m.find()) {
                String params = callArguments(src, m.end() - 1);
                int afterParams = m.end() - 1 + params.length() + 2;
                int open = src.indexOf('{', afterParams);
                int semi = src.indexOf(';', afterParams);
                if (open < 0 || (semi >= 0 && semi < open)) {
                    continue;
                }
                found.add(new Helper(params, block(src, open), owner));
            }
            if (!found.isEmpty()) {
                return found.stream().filter(h -> h.body().contains("switch (")).findFirst().orElse(found.get(0));
            }
        }
        return null;
    }

    private static String source(Map<String, String> sources, Class<?> c) {
        String src = sources.get(c.getSimpleName());
        if (src == null) {
            throw new IllegalStateException("no source for " + c.getSimpleName());
        }
        return src;
    }

    /** The text of the block opening at {@code open}, braces matched (strings and characters skipped). */
    static String block(String src, int open) {
        int depth = 0;
        for (int i = open; i < src.length(); i++) {
            char ch = src.charAt(i);
            if (ch == '"' || ch == '\'') {
                i = skipQuoted(src, i);
            } else if (ch == '{') {
                depth++;
            } else if (ch == '}' && --depth == 0) {
                return src.substring(open, i + 1);
            }
        }
        throw new IllegalStateException("an unclosed block at " + open);
    }

    /** The arm {@code member} has in the first {@code sw} of {@code body}: the text after its {@code ->}, up to the
     *  next arm; null when the switch has no arm naming it (the tables have no {@code default}). */
    static String arm(String body, String sw, String member) {
        int at = body.indexOf(sw);
        if (at < 0) {
            return null;
        }
        String sb = block(body, body.indexOf('{', at));
        List<Integer> starts = new ArrayList<>();
        int depth = 0;
        for (int i = 0; i < sb.length(); i++) {
            char ch = sb.charAt(i);
            if (ch == '"' || ch == '\'') {
                i = skipQuoted(sb, i);
            } else if (ch == '{' || ch == '(' || ch == '[') {
                depth++;
            } else if (ch == '}' || ch == ')' || ch == ']') {
                depth--;
            } else if (depth == 1 && (sb.startsWith("case ", i) || sb.startsWith("default", i))
                    && !Character.isJavaIdentifierPart(sb.charAt(i - 1))) {
                starts.add(i);
            }
        }
        for (int k = 0; k < starts.size(); k++) {
            int s = starts.get(k);
            String whole = sb.substring(s, k + 1 < starts.size() ? starts.get(k + 1) : sb.length() - 1);
            int arrow = whole.indexOf("->");
            if (arrow < 0 || !whole.startsWith("case ")) {
                continue;
            }
            for (String l : whole.substring("case ".length(), arrow).split(",")) {
                if (l.strip().equals(member)) {
                    String text = whole.substring(arrow + 2).strip();
                    return text.endsWith(";") ? text.substring(0, text.length() - 1) : text;
                }
            }
        }
        return null;
    }

    private static int skipQuoted(String s, int i) {
        char q = s.charAt(i);
        if (q == '"' && s.startsWith("\"\"\"", i)) {
            int end = s.indexOf("\"\"\"", i + 3);
            return end < 0 ? s.length() : end + 2;
        }
        for (int j = i + 1; j < s.length(); j++) {
            char ch = s.charAt(j);
            if (ch == '\\') {
                j++;
            } else if (ch == q) {
                return j;
            }
        }
        return s.length();
    }

    /** {@code src} without its comments (strings and characters kept as they are). */
    static String stripComments(String src) {
        StringBuilder out = new StringBuilder(src.length());
        for (int i = 0; i < src.length(); i++) {
            char ch = src.charAt(i);
            if (ch == '"' || ch == '\'') {
                int end = skipQuoted(src, i);
                out.append(src, i, Math.min(end + 1, src.length()));
                i = end;
            } else if (src.startsWith("//", i)) {
                while (i < src.length() && src.charAt(i) != '\n') {
                    i++;
                }
                out.append('\n');
            } else if (src.startsWith("/*", i)) {
                int end = src.indexOf("*/", i + 2);
                for (int j = i; j < (end < 0 ? src.length() : end + 2); j++) {
                    if (src.charAt(j) == '\n') {
                        out.append('\n');
                    }
                }
                i = end < 0 ? src.length() : end + 1;
            } else {
                out.append(ch);
            }
        }
        return out.toString();
    }

    // ---- the reach ------------------------------------------------------------------------------------------

    /** reach.tsv: dialect TAB vocabulary TAB member TAB cases, a {@code #} line a comment. */
    static Map<String, Map<String, Integer>> readReach(Path tsv) throws IOException {
        Map<String, Map<String, Integer>> out = new LinkedHashMap<>();
        for (String line : Files.readAllLines(tsv, StandardCharsets.UTF_8)) {
            if (line.isBlank() || line.startsWith("#")) {
                continue;
            }
            String[] f = line.split("\t");
            out.computeIfAbsent(f[0], k -> new LinkedHashMap<>()).put(f[1] + ":" + f[2], Integer.parseInt(f[3]));
        }
        return out;
    }

    // ---- writing ----------------------------------------------------------------------------------------------

    private static final String HEADER = """
            <!-- GENERATED by com.legend.sql.dialect.CapabilityMatrix from the dialects' own tables and reach.tsv:
                 regenerate with `bazel run //docs:update_dialect_capability`; do not edit by hand. -->
            """;

    private static String symbol(Decision d) {
        return switch (d.kind()) {
            case SPELLED -> "S";
            case EMULATED -> "E";
            case REFUSED -> "R";
            case NOT_HERE -> "–";
        };
    }

    private static String runs(Class<?> d, List<DatabaseType> types) {
        return types.isEmpty()
                ? "no database type's own (it extends `" + d.getSuperclass().getSimpleName() + "`)"
                : String.join(", ", types.stream().map(Enum::name).toList());
    }

    static String overview(Map<Class<?>, Map<String, Map<String, Decision>>> all,
            Map<Class<?>, List<DatabaseType>> dbs, Map<String, Map<String, Integer>> reach) {
        StringBuilder b = new StringBuilder(HEADER);
        b.append("""

                # The capability matrix

                What each concrete dialect does with every function and aggregate of the SQL IR, read from its own
                tables by `CapabilityMatrix` (the dialect program's Phase 1b; docs/DIALECT_LEAK_CENSUS_AND_PLAN_2026_10_12.md):

                - **S**, spelled: one SQL function, keyword or operator, by name;
                - **E**, emulated: a composed expression, or a helper that builds one;
                - **R**, refused: refused by name, a capability the database lacks (the arm throws
                  `DialectCapability`, or the like), its reason in the vocabulary's file;
                - **–**, not here: an arm the IR never reaches (it throws the renderer's "a caller's bug",
                  `IllegalStateException`): a window function, which `windowCall` writes by its name; a lowering marker,
                  composed before rendering; a function another arm writes. No decision of the dialect's;
                - a trailing **·**: an arm that writes SQL (S or E) and that no recorded PCT case reaches on that
                  dialect's database, so no PCT test exercises it there. A dialect no database type is planned with has
                  no PCT lane, so no reach.

                [functions.md](functions.md) and [aggregates.md](aggregates.md) give each dialect's every decision:
                what the arm writes (a name, or a template with `…` for each expression it writes), the helper it uses or
                its reason, what it was read through (a parent, a helper), and the cases reaching it.

                The reach is a MEASUREMENT, `reach.tsv`: each case the five PCT suites record on DuckDB
                (`-Dlegend.diagnostics=pct-cases`) planned as its lane plans it, up to the dialect (the storeless
                runtime, the plan-only seam `TypedQuery.lower`, a relation result wrapped in the PCT wire text), then put
                through each dialect's own passes; a function is reached when it is still in the case's IR. Not applied:
                the lane's feature flags and its database-side probes (a dynamically pivoting relation stays unwrapped).
                A reached function may still be one its lane pins as failing. Re-measure when the lowering, the passes
                or the suites move:
                `bazel run //core:capability_reach -- docs/dialect-capability/reach.tsv <pct-cases.tsv>...`, then
                `bazel run //docs:update_dialect_capability`. The decisions need no measuring. Every row of these
                pages is in `matrix.tsv`, the matrix as data, written with them: its diff test fails when a dialect's
                table moves and this was not regenerated (the data is tested, never these pages: prose is no test
                input).

                """);
        b.append("| dialect | database types | spelled | emulated | refused | not here | written, not reached by PCT |\n")
                .append("|---|---|---:|---:|---:|---:|---:|\n");
        for (Map.Entry<Class<?>, Map<String, Map<String, Decision>>> d : all.entrySet()) {
            String name = d.getKey().getSimpleName();
            boolean measured = dbs.containsKey(d.getKey());
            int[] n = new int[5];
            for (Vocabulary v : VOCABULARIES) {
                for (Map.Entry<String, Decision> e : d.getValue().get(v.title()).entrySet()) {
                    n[e.getValue().kind().ordinal()]++;
                    if (measured && writes(e.getValue()) && reached(reach, name, v, e.getKey()) == 0) {
                        n[4]++;
                    }
                }
            }
            b.append("| ").append(name).append(" ([functions](functions.md#").append(anchor(name))
                    .append("), [aggregates](aggregates.md#").append(anchor(name)).append(")) | ")
                    .append(runs(d.getKey(), dbs.getOrDefault(d.getKey(), List.of()))).append(" | ").append(n[0])
                    .append(" | ").append(n[1]).append(" | ").append(n[2]).append(" | ").append(n[3]).append(" | ")
                    .append(measured ? String.valueOf(n[4]) : "—").append(" |\n");
        }
        for (Vocabulary v : VOCABULARIES) {
            b.append("\n## ").append(title(v)).append("\n\n| ").append(noun(v));
            for (Class<?> d : all.keySet()) {
                b.append(" | ").append(d.getSimpleName());
            }
            b.append(" |\n|---").append("|:-:".repeat(all.size())).append("|\n");
            for (String m : v.members()) {
                b.append("| `").append(m).append('`');
                for (Map.Entry<Class<?>, Map<String, Map<String, Decision>>> d : all.entrySet()) {
                    Decision dec = d.getValue().get(v.title()).get(m);
                    boolean unreached = dbs.containsKey(d.getKey()) && writes(dec)
                            && reached(reach, d.getKey().getSimpleName(), v, m) == 0;
                    b.append(" | ").append(symbol(dec)).append(unreached ? "·" : "");
                }
                b.append(" |\n");
            }
        }
        return b.toString();
    }

    /** The arm writes SQL: spelled or emulated. */
    private static boolean writes(Decision d) {
        return d.kind() == Kind.SPELLED || d.kind() == Kind.EMULATED;
    }

    private static int reached(Map<String, Map<String, Integer>> reach, String dialect, Vocabulary v, String m) {
        return reach.getOrDefault(dialect, Map.of()).getOrDefault(v.key() + ":" + m, 0);
    }

    /** A vocabulary's file: a section per dialect, a row per member. */
    static String vocabulary(Vocabulary v, Map<Class<?>, Map<String, Map<String, Decision>>> all,
            Map<Class<?>, List<DatabaseType>> dbs, Map<String, Map<String, Integer>> reach) {
        StringBuilder b = new StringBuilder(HEADER);
        b.append("\n# The capability matrix: ").append(v.title()).append("\n\n")
                .append("What each dialect does with each ").append(noun(v)).append(" of the SQL IR. The legend, the ")
                .append("method and the summary are in [README.md](README.md).\n\n");
        for (Class<?> d : all.keySet()) {
            b.append("- [").append(d.getSimpleName()).append("](#").append(anchor(d.getSimpleName())).append(")\n");
        }
        for (Map.Entry<Class<?>, Map<String, Map<String, Decision>>> d : all.entrySet()) {
            String name = d.getKey().getSimpleName();
            boolean measured = dbs.containsKey(d.getKey());
            b.append("\n## ").append(name).append("\n\nDatabase types planned with it: ")
                    .append(runs(d.getKey(), dbs.getOrDefault(d.getKey(), List.of()))).append('.')
                    .append(measured ? "" : " No PCT lane runs it, so its arms have no measured reach.")
                    .append("\n\n| ").append(noun(v))
                    .append(" | decision | writes, or why not | read through | PCT cases |\n|---|---|---|---|---:|\n");
            for (String m : v.members()) {
                Decision dec = d.getValue().get(v.title()).get(m);
                b.append("| `").append(m).append("` | ").append(dec.kind().name().toLowerCase(Locale.ROOT))
                        .append(" | ").append(cell(dec.detail())).append(" | ")
                        .append(cell(String.join(", ", dec.via()))).append(" | ")
                        .append(measured ? String.valueOf(reached(reach, name, v, m)) : "—").append(" |\n");
            }
        }
        return b.toString();
    }

    private static String title(Vocabulary v) {
        return Character.toUpperCase(v.title().charAt(0)) + v.title().substring(1);
    }

    private static String noun(Vocabulary v) {
        return v.title().substring(0, v.title().length() - 1);
    }

    /** A heading's anchor as GitHub writes it. */
    private static String anchor(String heading) {
        return heading.toLowerCase(Locale.ROOT);
    }

    private static String cell(String s) {
        String c = s.replace("|", "\\|").replace("\n", " ").replace("`", "'");
        return c.isEmpty() ? "" : "`" + c + "`";
    }
}
