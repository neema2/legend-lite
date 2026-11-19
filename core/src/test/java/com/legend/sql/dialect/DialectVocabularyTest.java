// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.sql.dialect;

import com.legend.sql.SqlExpr;
import com.legend.testing.SourceFiles;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.lang.reflect.Modifier;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * EVERY DIALECT DECIDES EVERY POINT OF ITS VOCABULARY (the user's decision P1b, 2026-10-13;
 * docs/DIALECT_LEAK_CENSUS_AND_PLAN_2026_10_12.md Phase 1b). Each concrete dialect class, a family member's included,
 * declares its own {@code call} and {@code reducer} and spells each {@code SqlFn} and {@code SqlAgg.Fn} in a switch
 * EXPRESSION with no {@code default} arm, so javac makes it decide every function and aggregate: a new one breaks the
 * build in every dialect until each spells it, refuses it by name, or names it among those it takes from its parent.
 * It declares its own {@code formatText} too: which date-format vocabulary it writes is its choice, never a default;
 * and its own {@code scalarTypeName}, every scalar type's name or a refusal, in a switch with no {@code default}. A
 * {@code default} would let a dialect inherit a spelling no one chose for it, which is how a DuckDB spelling reached
 * databases that never ran it (decision D1).
 */
@Tag("guardrail")
class DialectVocabularyTest {

    private static final String DIALECTS = "core/src/main/java/com/legend/sql/dialect";
    /** The edge's own dialects (2026-11-13, phase D's O3: legend-engine's text printers, EngineStyle*, moved here from
     *  sql/dialect; still dialects, so still held to deciding their whole vocabulary themselves). */
    private static final String EDGE_DIALECTS = "core/src/main/java/com/legend/edge";

    /** A vocabulary: the method each dialect declares, its parameters, and the switch that decides it (none where a
     *  dialect may take another's whole table, as a date format's parts are). */
    private record Vocabulary(String method, Class<?>[] params, @com.legend.base.Nullable String sw) {
    }

    private static final List<Vocabulary> VOCABULARIES = List.of(
            new Vocabulary("call", new Class<?>[] {SqlWriter.class, SqlExpr.Call.class, int.class}, "switch (c.fn())"),
            new Vocabulary("reducer", new Class<?>[] {SqlWriter.class, com.legend.sql.SqlAgg.Reducer.class},
                    "switch (r.fn())"),
            new Vocabulary("formatText", new Class<?>[] {SqlExpr.FormatLit.class}, null),
            new Vocabulary("scalarTypeName", new Class<?>[] {com.legend.sql.SqlType.Scalar.class}, "switch (s)"));

    @Test
    void everyConcreteDialectDecidesEveryFunctionAndAggregateItself() throws IOException, ClassNotFoundException {
        List<String> wrong = new ArrayList<>();
        int dialects = 0;
        List<Path> files = new ArrayList<>(SourceFiles.in(DIALECTS));
        files.addAll(SourceFiles.in(EDGE_DIALECTS));
        for (Path f : files) {
            String file = f.getFileName().toString();
            if (!file.endsWith(".java") || file.equals("package-info.java")) {
                continue;
            }
            String name = file.substring(0, file.length() - ".java".length());
            String pkg = f.toString().replace('\\', '/').contains("/com/legend/edge/") ? "com.legend.edge."
                    : "com.legend.sql.dialect.";
            Class<?> cls = Class.forName(pkg + name, false, DialectVocabularyTest.class.getClassLoader());
            if (cls == AnsiSqlRenderer.class || !AnsiSqlRenderer.class.isAssignableFrom(cls)
                    || Modifier.isAbstract(cls.getModifiers())) {
                continue;
            }
            dialects++;
            String src = Files.readString(f);
            for (Vocabulary v : VOCABULARIES) {
                boolean declared = java.util.Arrays.stream(cls.getDeclaredMethods())
                        .anyMatch(m -> m.getName().equals(v.method())
                                && java.util.Arrays.equals(m.getParameterTypes(), v.params()));
                if (!declared) {
                    wrong.add(name + ": declares no " + v.method() + "() of its own (it takes the whole vocabulary from"
                            + " its parent unasked)");
                    continue;
                }
                if (v.sw() == null) {
                    continue;
                }
                String decider = decider(src, v);
                if (decider == null) {
                    wrong.add(name + ": its " + v.method() + "() was not found in its source");
                    continue;
                }
                int switches = 0;
                for (int at = decider.indexOf(v.sw()); at >= 0; at = decider.indexOf(v.sw(), at + 1)) {
                    switches++;
                    String before = decider.substring(Math.max(0, at - 12), at).strip();
                    if (!(before.endsWith("return") || before.endsWith("yield") || before.endsWith("->")
                            || before.endsWith("="))) {
                        wrong.add(name + ": its " + v.method() + "()'s " + v.sw() + " is a statement, which javac does"
                                + " not check");
                    }
                    if (hasDefault(decider, decider.indexOf('{', at))) {
                        wrong.add(name + ": its " + v.method() + "()'s " + v.sw() + " has a default arm");
                    }
                }
                if (switches == 0) {
                    wrong.add(name + ": its " + v.method() + "() decides in no " + v.sw());
                }
            }
        }
        assertTrue(dialects >= 15, "the dialect package's concrete dialects were not found: " + dialects);
        assertTrue(wrong.isEmpty(), "a dialect that does not decide its whole vocabulary itself: " + wrong);
    }

    /**
     * THE BASE SPELLS NO FUNCTION (decision D1, 2026-10-13) AND NO AGGREGATE (Phase 1b step 3): a dialect whose parent
     * is the base renderer names every function's and aggregate's spelling itself, calling the base's shared helpers
     * by name ({@code aggregate(writer, "SUM", r)}), and never hands one to the base's {@code call} or {@code reducer}
     * (only a family member hands a point to its parent, a concrete dialect). The base is abstract in every
     * vocabulary point ({@code call}, {@code reducer}, {@code scalarTypeName}, {@code formatText},
     * {@code structTypeName}, {@code passes}: Phase 1a), so javac refuses a dialect that leaves one undecided.
     */
    @Test
    void noDialectTakesAFunctionFromTheBase() throws IOException, ClassNotFoundException {
        List<String> wrong = new ArrayList<>();
        List<Path> files = new ArrayList<>(SourceFiles.in(DIALECTS));
        files.addAll(SourceFiles.in(EDGE_DIALECTS));
        for (Path f : files) {
            String file = f.getFileName().toString();
            if (!file.endsWith(".java") || file.equals("package-info.java")) {
                continue;
            }
            String name = file.substring(0, file.length() - ".java".length());
            String pkg = f.toString().replace('\\', '/').contains("/com/legend/edge/") ? "com.legend.edge."
                    : "com.legend.sql.dialect.";
            Class<?> cls = Class.forName(pkg + name, false, DialectVocabularyTest.class.getClassLoader());
            String code = code(Files.readString(f));
            for (String vocabulary : List.of("call", "reducer")) {
                if (cls.getSuperclass() == AnsiSqlRenderer.class && code.contains("super." + vocabulary + "(")) {
                    wrong.add(name + " hands a point to the base renderer (super." + vocabulary + ")");
                }
            }
        }
        // the base is abstract in every vocabulary point, so javac refuses a dialect that does not decide one (the
        // dialect program's Phase 1a, 2026-10-13)
        if (!Modifier.isAbstract(AnsiSqlRenderer.class.getModifiers())) {
            wrong.add("the base renderer is not abstract");
        }
        for (String point : List.of("call", "reducer", "scalarTypeName", "formatText", "structTypeName", "passes")) {
            boolean abstractPoint = java.util.Arrays.stream(AnsiSqlRenderer.class.getDeclaredMethods())
                    .filter(m -> m.getName().equals(point))
                    .anyMatch(m -> Modifier.isAbstract(m.getModifiers()));
            if (!abstractPoint) {
                wrong.add("the base renderer's " + point + "() is not abstract: it spells something a dialect did not"
                        + " choose");
            }
        }
        assertTrue(wrong.isEmpty(), "a function spelled by the base renderer for a dialect that did not name it: "
                + wrong);
    }

    /**
     * EACH DIALECT'S FUNCTION TABLE IS ONE SWITCH OF ONE-EXPRESSION ARMS (the user's decision, 2026-10-13, "let's do
     * it right"; docs/DIALECT_LEAK_CENSUS_AND_PLAN_2026_10_12.md Phase 1b step 3). The switch that decides a
     * dialect's functions is its whole table: each arm is one expression (the spelling, its names written inline; a
     * call to a named method; a refusal), never a block, and nothing in the deciding method tests which function it
     * has, before the switch or inside an arm, so an arm is all there is to read for its function. A rearrangement
     * before the switch that applies to a SET of functions (a set's {@code contains}) tests no one function and stays.
     */
    @Test
    void eachFunctionTableIsOneSwitchOfOneExpressionArms() throws IOException, ClassNotFoundException {
        List<String> wrong = new ArrayList<>();
        int tables = 0;
        List<Path> files = new ArrayList<>(SourceFiles.in(DIALECTS));
        files.addAll(SourceFiles.in(EDGE_DIALECTS));
        for (Path f : files) {
            String file = f.getFileName().toString();
            if (!file.endsWith(".java") || file.equals("package-info.java")) {
                continue;
            }
            String name = file.substring(0, file.length() - ".java".length());
            String pkg = f.toString().replace('\\', '/').contains("/com/legend/edge/") ? "com.legend.edge."
                    : "com.legend.sql.dialect.";
            Class<?> cls = Class.forName(pkg + name, false, DialectVocabularyTest.class.getClassLoader());
            if (cls == AnsiSqlRenderer.class || !AnsiSqlRenderer.class.isAssignableFrom(cls)
                    || Modifier.isAbstract(cls.getModifiers())) {
                continue;
            }
            String code = code(Files.readString(f));
            for (Vocabulary v : VOCABULARIES) {
                if (!ONE_EXPRESSION_TABLES.contains(v.method())) {
                    continue;
                }
                String decider = decider(code, v);
                if (decider == null) {
                    continue;   // the first test names it
                }
                tables++;
                wrong.addAll(oneExpressionArms(name + "." + v.method() + "()", decider, v.sw()));
            }
        }
        assertTrue(tables >= 15, "the dialects' function tables were not found: " + tables);
        assertTrue(wrong.isEmpty(), "a function table that is not one switch of one-expression arms: " + wrong);
    }

    /** The two checks see every form a function table could take: a colon-form arm, a colon-form {@code default}, and
     *  the function tested before the switch on either side of {@code ==}, each planted as the PR #39 audit
     *  (2026-10-14) planted them; and a ternary inside an arm is no colon-form arm. */
    @Test
    void theChecksSeeEveryFormOfArmDefaultAndTest() {
        String sw = "switch (c.fn())";
        String colon = "{ return switch (c.fn()) { case ABS: yield f(); default: { g(); yield h(x); } }; }";
        assertTrue(oneExpressionArms("colon", colon, sw).stream().anyMatch(w -> w.contains("colon-form")),
                String.valueOf(oneExpressionArms("colon", colon, sw)));
        assertTrue(hasDefault(colon, colon.indexOf('{', colon.indexOf(sw))));
        for (String test : List.of("SqlFn.ABS == c.fn()", "c.fn() == SqlFn.ABS", "SqlFn.ABS != c.fn()",
                "SqlFn.ABS.equals(c.fn())", "c.fn().equals(SqlFn.ABS)")) {
            String tested = "{ if (" + test + ") { return x; } return switch (c.fn()) { case ABS -> f(); }; }";
            assertTrue(oneExpressionArms("tested", tested, sw).stream().anyMatch(w -> w.contains("tests which")),
                    test);
        }
        String clean = "{ return switch (c.fn()) { case ABS -> a ? b : d; case NEG, POS -> f(x, Foo::bar); }; }";
        assertTrue(oneExpressionArms("clean", clean, sw).isEmpty(), String.valueOf(oneExpressionArms("clean", clean, sw)));
        assertTrue(!hasDefault(clean, clean.indexOf('{', clean.indexOf(sw))));
    }

    /** The vocabularies whose deciding switch is held to one-expression arms: the functions and the aggregates. */
    private static final java.util.Set<String> ONE_EXPRESSION_TABLES = java.util.Set.of("call", "reducer");

    /** What is wrong with {@code decider} (its source with comments and literals blanked) as one switch {@code sw} of
     *  one-expression arms, each wrong thing named by {@code where}. */
    static List<String> oneExpressionArms(String where, String decider, String sw) {
        List<String> wrong = new ArrayList<>();
        int at = decider.indexOf(sw);
        if (at < 0 || decider.indexOf(sw, at + 1) >= 0) {
            wrong.add(where + " decides in " + (at < 0 ? "no " : "more than one ") + sw);
            return wrong;
        }
        // the function tested by name: c.fn() == / != (r.fn() for an aggregate), either side, or by equals
        String subject = sw.substring(sw.indexOf('(') + 1, sw.indexOf('.')).strip();
        String fn = "\\b" + subject + "\\s*\\.\\s*fn\\s*\\(\\s*\\)";
        if (java.util.regex.Pattern.compile(fn + "\\s*([!=]=|\\.\\s*equals\\b)|([!=]=|\\bequals\\s*\\()\\s*" + fn)
                .matcher(decider).find()) {
            wrong.add(where + " tests which function it has (" + subject + ".fn() ==): give that function its own arm");
        }
        int open = decider.indexOf('{', at);
        int depth = 0;
        for (int i = open; i < decider.length(); i++) {
            char ch = decider.charAt(i);
            if (ch == '{' || ch == '(') {
                depth++;
            } else if (ch == '}' || ch == ')') {
                if (--depth == 0) {
                    break;
                }
            } else if (depth == 1 && decider.startsWith("case", i) && !Character.isJavaIdentifierPart(decider.charAt(i - 1))
                    && Character.isWhitespace(decider.charAt(i + 4)) && colonLabel(decider, i + 4)) {
                // a colon-form arm is a statement group, which javac does not hold to one expression (the PR #39
                // audit, 2026-10-14: this check read only '->')
                String arm = decider.substring(i, decider.indexOf(':', i)).replaceAll("\\s+", " ").strip();
                wrong.add(where + "'s arm '" + arm + "' is colon-form: write it as one '->' expression");
            } else if (depth == 1 && decider.startsWith("->", i)
                    && decider.substring(i + 2).stripLeading().startsWith("{")) {
                String arm = decider.substring(decider.lastIndexOf("case ", i), i).replaceAll("\\s+", " ").strip();
                wrong.add(where + "'s arm '" + arm + "' is a block: make it a named method");
            }
        }
        return wrong;
    }

    /** Whether the case label starting at {@code from} ends in {@code :} (a colon-form arm) rather than {@code ->}: the
     *  first of the two at the label's own depth (a pattern's parentheses nest). */
    private static boolean colonLabel(String src, int from) {
        int depth = 0;
        for (int i = from; i < src.length(); i++) {
            char ch = src.charAt(i);
            if (ch == '(' || ch == '{' || ch == '[') {
                depth++;
            } else if (ch == ')' || ch == '}' || ch == ']') {
                if (--depth < 0) {
                    return false;
                }
            } else if (depth == 0 && src.startsWith("->", i)) {
                return false;
            } else if (depth == 0 && ch == ':' && !src.startsWith("::", i)) {
                return true;
            }
        }
        return false;
    }

    /** The switch that decides vocabulary {@code v} in {@code src}: the method itself, or the one it hands every point
     *  to (SQLite.calls, ClickHouse.reducerIn); a helper's own switch over a subset is not it. Null when not found. */
    private static @com.legend.base.Nullable String decider(String src, Vocabulary v) {
        String first = "\\s*\\(\\s*[\\w.]*" + v.params()[0].getSimpleName() + "\\b";
        String decider = body(src, "\\b" + v.method() + first);
        if (decider != null && v.sw() != null && !decider.contains(v.sw())) {
            java.util.regex.Matcher to = java.util.regex.Pattern
                    .compile("return\\s+(\\w+)\\(writer,\\s*[cr]\\b").matcher(decider);
            decider = to.find() ? body(src, "\\b" + to.group(1) + first) : null;
        }
        return decider;
    }

    /** {@code src} with every comment and literal blanked, so a search finds only code. */
    private static String code(String src) {
        StringBuilder out = new StringBuilder(src.length());
        for (int i = 0; i < src.length(); i++) {
            int skipped = skip(src, i);
            if (skipped != i) {
                out.append(" ".repeat(skipped - i));
                i = skipped - 1;
            } else {
                out.append(src.charAt(i));
            }
        }
        return out.toString();
    }

    /** The body of the method whose declaration {@code declaration} (a regex) finds, its braces matched past
     *  comments and literals; null when there is none. */
    private static @com.legend.base.Nullable String body(String src, String declaration) {
        java.util.regex.Matcher m = java.util.regex.Pattern.compile(declaration).matcher(src);
        while (m.find()) {
            int open = src.indexOf('{', m.end());
            int semi = src.indexOf(';', m.end());
            if (open < 0 || (semi >= 0 && semi < open)) {
                continue;   // a call, not the declaration
            }
            int depth = 0;
            for (int i = open; i < src.length(); i++) {
                int skipped = skip(src, i);
                if (skipped != i) {
                    i = skipped - 1;
                    continue;
                }
                if (src.charAt(i) == '{') {
                    depth++;
                } else if (src.charAt(i) == '}' && --depth == 0) {
                    return src.substring(open, i + 1);
                }
            }
        }
        return null;
    }

    /** Whether the switch body opening at {@code open} has a {@code default} arm of its own (not a nested switch's). */
    private static boolean hasDefault(String src, int open) {
        int depth = 0;
        for (int i = open; i < src.length(); i++) {
            int skipped = skip(src, i);
            if (skipped != i) {
                i = skipped - 1;
                continue;
            }
            char c = src.charAt(i);
            if (c == '{' || c == '(') {
                depth++;
            } else if (c == '}' || c == ')') {
                depth--;
                if (depth == 0) {
                    return false;
                }
            } else if (depth == 1 && src.startsWith("default", i) && !Character.isJavaIdentifierPart(src.charAt(i - 1))
                    && (src.substring(i + "default".length()).stripLeading().startsWith("->")
                            || src.substring(i + "default".length()).stripLeading().startsWith(":"))) {
                return true;
            }
        }
        return false;
    }

    /** Past a comment or a string or char literal starting at {@code i}; {@code i} itself when none starts there. */
    private static int skip(String src, int i) {
        if (src.startsWith("//", i)) {
            int end = src.indexOf('\n', i);
            return end < 0 ? src.length() : end;
        }
        if (src.startsWith("/*", i)) {
            return src.indexOf("*/", i + 2) + 2;
        }
        if (src.startsWith("\"\"\"", i)) {
            return src.indexOf("\"\"\"", i + 3) + 3;
        }
        char c = src.charAt(i);
        if (c == '"' || c == '\'') {
            int j = i + 1;
            while (src.charAt(j) != c) {
                j += src.charAt(j) == '\\' ? 2 : 1;
            }
            return j + 1;
        }
        return i;
    }

}
