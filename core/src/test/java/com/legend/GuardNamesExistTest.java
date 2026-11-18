// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend;

import com.legend.testing.SourceFiles;
import com.tngtech.archunit.core.domain.JavaClass;
import com.tngtech.archunit.core.domain.JavaClasses;
import com.tngtech.archunit.core.importer.ClassFileImporter;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.function.Predicate;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * NO GUARD NAMES WHAT IS NOT THERE (the edge audit's K16, 2026-11-17: "stale text" -- ArchitectureTest's V3 rule named
 * the deleted {@code com.legend.judge} package and S144's the deleted driver package, so each read nothing; the planning
 * rule's list named four CanonicalDivergence members that had moved to VerdictCensus). A guard's allowlist, exemption or
 * pattern that names a class or member no jar holds passes whatever the code does, and nothing said so. Two checks, over
 * every census and guardrail class's source:
 * <ul>
 *   <li>each {@code com.legend...} class a string literal names is a class of the product's, the authored doors', the
 *       edge's or the tests' jars (a driver source set's file counts), and each {@code #member} it names is one of that
 *       class's members;</li>
 *   <li>each pattern ArchitectureTest hands {@code haveNameMatching} (whole literals, joined by {@code +}) matches a class
 *       of those jars.</li>
 * </ul>
 * A name that is absent on purpose -- a planted test's made-up class, a tripwire against a deleted one's return -- is
 * listed exactly ({@link #ABSENT_ON_PURPOSE}) with its reason; a listed name that comes back, or goes from its file, is
 * named too. Its limits: a name built at run time, or written without its {@code com.legend} prefix (the short
 * {@code planner.X#m} rows, which their own guards resolve), is not read.
 */
@Tag("guardrail")
class GuardNamesExistTest {

    /** "file | name" -> why the name is absent on purpose. Measured 2026-11-17. */
    static final Map<String, String> ABSENT_ON_PURPOSE = Map.ofEntries(
            // tripwires: no util package anywhere (AGENTS.md invariant 8), named should one appear (read since the
            // review of 091b9bde36, 2026-11-18: a package literal led by '..')
            Map.entry("ArchitectureTest.java | package ..util..", "tripwire (invariant 8), 2026-11-18"),
            Map.entry("ArchitectureTest.java | package ..utils..", "tripwire (invariant 8), 2026-11-18"),
            // tripwires: THE WALL (invariant 8: no com.legend class depends on the deleted engine module's com.gs.legend)
            // and the reference checkouts' code (legend-engine's and legend-pure's org.finos.legend, never a runtime
            // component) -- read since the review of dd5deb781f, 2026-11-18, which had every package literal read
            Map.entry("ArchitectureTest.java | package com.gs.legend..", "tripwire (THE WALL), 2026-11-18"),
            Map.entry("ArchitectureTest.java | package org.finos.legend..", "tripwire (the reference checkouts),"
                    + " 2026-11-18"),
            // tripwires: the referee and the appeals, moved to the edge (census C2), named should one return
            Map.entry("EdgeSeparationTest.java | com.legend.testexec.Appeals", "tripwire (C2), 2026-11-17"),
            Map.entry("EdgeSeparationTest.java | com.legend.testexec.SqlReplayReferee", "tripwire (C2), 2026-11-17"),
            Map.entry("EdgeSeparationTest.java | com.legend.testexecutionplan.Appeal", "tripwire (C2), 2026-11-17"),
            Map.entry("EdgeSeparationTest.java | com.legend.testexecutionplan.PlannedAppeals",
                    "tripwire (C2), 2026-11-17"),
            Map.entry("EdgeSeparationTest.java | com.legend.testplan.AppealsByVerdict", "tripwire (C2), 2026-11-17"),
            Map.entry("EdgeSeparationTest.java | com.legend.testplan.EngineTextAppeals", "tripwire (C2), 2026-11-17"),
            // the type hole's deleted names, held deleted (the total wall's M2, P4)
            Map.entry("TypeHoleDeletedTest.java | com.legend.executionplan.ExecutionPlan$LoadStep$Delivery$Bulk",
                    "held deleted, 2026-11-17"),
            Map.entry("TypeHoleDeletedTest.java | com.legend.executionplan.ExecutionPlan$LoadStep$Delivery$DatabaseReads",
                    "held deleted, 2026-11-17"),
            Map.entry("TypeHoleDeletedTest.java | com.legend.executionplan.ExecutionPlan$Setup",
                    "held deleted, 2026-11-17"),
            Map.entry("TypeHoleDeletedTest.java | com.legend.executionplan.ExecutionPlan$TypeHole",
                    "held deleted, 2026-11-17"),
            Map.entry("TypeHoleDeletedTest.java | com.legend.executionplan.ExecutionPlan$TypeSpelling",
                    "held deleted, 2026-11-17"),
            Map.entry("TypeHoleDeletedTest.java | com.legend.executionplan.ExecutionPlan$ValueKind",
                    "held deleted, 2026-11-17"),
            Map.entry("TypeHoleDeletedTest.java | com.legend.sql.ValueKind", "held deleted, 2026-11-17"),
            Map.entry("TypeHoleDeletedTest.java | com.legend.sql.ValueTyping", "held deleted, 2026-11-17"),
            Map.entry("TypeHoleDeletedTest.java | com.legend.sql.dialect.RenderedStatement$TypeHole",
                    "held deleted, 2026-11-17"),
            // the planted cases' made-up names: each guard's bite test plants a class that is nowhere
            Map.entry("HookRulingsAgreeTest.java | com.legend.lowering.Repr", "planted (Repr deleted, B12 2b), 2026-11-17"),
            Map.entry("HookRulingsAgreeTest.java | com.legend.planner.PlantedHook", "planted, 2026-11-17"),
            // (JudgingRulesReachTest's planted rules, a prefix: gone 2026-11-18 with the test, E-Java's 070c4ea3e, train 18)
            Map.entry("OpenLineMarkedTest.java | com.legend.edge.Moved", "planted, 2026-11-17"),
            Map.entry("OpenLineMarkedTest.java | com.legend.lowering.Imported", "planted, 2026-11-17"),
            Map.entry("OpenLineMarkedTest.java | com.legend.lowering.Repr", "planted, 2026-11-17"),
            Map.entry("OpenLineMarkedTest.java | com.legend.lowering.Representation", "planted, 2026-11-17"),
            Map.entry("OpenLineMarkedTest.java | com.legend.lowering.Spaced", "planted, 2026-11-17"),
            Map.entry("OpenLineMarkedTest.java | com.legend.planner.Moved", "planted, 2026-11-17"),
            Map.entry("OpenLineMarkedTest.java | com.legend.planner.X", "planted, 2026-11-17"),
            Map.entry("ServiceLoaderSitesTest.java | com.legend.edge.Planted", "planted, 2026-11-17"),
            Map.entry("TaggedClassesHaveATargetTest.java | com.legend.CommentedGuardTest", "planted, 2026-11-17"),
            Map.entry("TaggedClassesHaveATargetTest.java | com.legend.EdgeGuardTest", "planted, 2026-11-17"),
            Map.entry("TaggedClassesHaveATargetTest.java | com.legend.Other", "planted, 2026-11-17"),
            Map.entry("TaggedClassesHaveATargetTest.java | com.legend.slow.ArgDropTest", "planted, 2026-11-17"),
            Map.entry("TaggedClassesHaveATargetTest.java | com.legend.slow.CommentedOutTest", "planted, 2026-11-17"),
            Map.entry("TaggedClassesHaveATargetTest.java | com.legend.slow.DroppedTest", "planted, 2026-11-17"),
            Map.entry("TaggedClassesHaveATargetTest.java | com.legend.slow.HashedTest", "planted, 2026-11-17"),
            Map.entry("TaggedClassesHaveATargetTest.java | com.legend.slow.HeavyOnlyTest", "planted, 2026-11-17"),
            // the review of the guard line, G, MUST-FIX 2a: a junit_test Bazel never evaluates selects it
            Map.entry("TaggedClassesHaveATargetTest.java | com.legend.slow.GhostTest", "planted, 2026-11-18"),
            Map.entry("TaggedClassesHaveATargetTest.java | com.legend.slow.JoinedDropTest", "planted, 2026-11-17"),
            Map.entry("TaggedClassesHaveATargetTest.java | com.legend.slow.KeptTest", "planted, 2026-11-17"),
            Map.entry("TaggedClassesHaveATargetTest.java | com.legend.slow.ManualListedTest", "planted, 2026-11-17"),
            Map.entry("TaggedClassesHaveATargetTest.java | com.legend.slow.ManualOnlyTest", "planted, 2026-11-17"),
            Map.entry("TaggedClassesHaveATargetTest.java | com.legend.slow.NamedOutTest", "planted, 2026-11-17"),
            Map.entry("TaggedClassesHaveATargetTest.java | com.legend.slow.OffGateTest", "planted, 2026-11-17"),
            Map.entry("TaggedClassesHaveATargetTest.java | com.legend.slow.ProbeCheck", "planted, 2026-11-17"),
            // a class that is there, in a module whose jars this target does not read (spec's generator, which
            // ArchitectureTest lists among the build-time tools)
            Map.entry("ArchitectureTest.java | com.legend.generators.PreludeGenerator",
                    "outside core: spec/src/gen/java, 2026-11-17"));

    private static final String TEST_ROOT = "core/src/test/java/";

    /** ArchitectureTest's name-pattern sites handed a name or a call, which this test does not read: the runner's
     *  pattern twice, SESSION_SEAM, TEMPORAL_LITERAL, and three makers(...) lists. Measured 2026-11-18. */
    static final int NAME_HANDED_PATTERN_SITES = 7;

    private static final Pattern TAG = Pattern.compile("@Tag\\(\"(?:census|guardrail)\"\\)");

    /** A class name a literal writes, with its nested classes and one member. */
    private static final Pattern NAME = Pattern.compile(
            "(?<![\\w.$])(com\\.legend(?:\\.[a-z_]\\w*)*\\.[A-Z]\\w*(?:\\$\\w+)*)(?:#([\\w<>*]+))?");

    /** A string literal of a source: where it starts (its opening quote), where it ends (past its closing quote), and
     *  its contents with their escapes read. */
    record Lit(int from, int end, String text) {
    }

    /** {@code source}'s string literals, text blocks included, comments left out, in order. */
    static List<Lit> literals(String source) {
        List<Lit> out = new ArrayList<>();
        int i = 0;
        while (i < source.length()) {
            char c = source.charAt(i);
            if (source.startsWith("//", i)) {
                int end = source.indexOf('\n', i);
                i = end < 0 ? source.length() : end;
            } else if (source.startsWith("/*", i)) {
                int end = source.indexOf("*/", i + 2);
                i = end < 0 ? source.length() : end + 2;
            } else if (c == '\'') {
                i += source.charAt(i + 1) == '\\' ? 4 : 3;
                while (i < source.length() && source.charAt(i - 1) != '\'') {
                    i++;
                }
            } else if (c == '"') {
                boolean block = source.startsWith("\"\"\"", i);
                int from = i;
                i += block ? 3 : 1;
                StringBuilder text = new StringBuilder();
                while (i < source.length()) {
                    char d = source.charAt(i);
                    if (d == '\\' && i + 1 < source.length()) {
                        char e = source.charAt(i + 1);
                        text.append(switch (e) {
                            case 'n' -> '\n';
                            case 't' -> '\t';
                            default -> e;
                        });
                        i += 2;
                    } else if (block ? source.startsWith("\"\"\"", i) : d == '"' || d == '\n') {
                        i += block ? 3 : 1;
                        break;
                    } else {
                        text.append(d);
                        i++;
                    }
                }
                out.add(new Lit(from, i, text.toString()));
            } else {
                i++;
            }
        }
        return out;
    }

    /** The class and member names {@code source}'s literals write, "name" or "name#member". */
    static Set<String> names(String source) {
        Set<String> out = new TreeSet<>();
        for (Lit lit : literals(source)) {
            Matcher m = NAME.matcher(lit.text());
            while (m.find()) {
                out.add(m.group(2) == null ? m.group(1) : m.group(1) + "#" + m.group(2));
            }
        }
        return out;
    }

    /** The patterns {@code source} hands {@code haveNameMatching}: an argument of literals joined by {@code +}, read
     *  whole; any other argument (a name, a call) is not read. */
    static List<String> namePatterns(String source) {
        List<String> out = new ArrayList<>();
        Map<Integer, Lit> byStart = new TreeMap<>();
        literals(source).forEach(l -> byStart.put(l.from(), l));
        Matcher call = Pattern.compile("haveName(?:Not)?Matching\\(").matcher(source);
        while (call.find()) {
            StringBuilder joined = new StringBuilder();
            int at = skip(source, call.end());
            boolean read = false;
            for (Lit lit = byStart.get(at); lit != null; lit = byStart.get(at)) {
                joined.append(lit.text());
                read = true;
                at = skip(source, lit.end());
                if (at < source.length() && source.charAt(at) == '+') {
                    at = skip(source, at + 1);
                } else {
                    break;
                }
            }
            if (read && at < source.length() && source.charAt(at) == ')') {
                out.add(joined.toString());
            }
        }
        return out;
    }

    /** {@code source} with its comments left out (its strings kept). */
    static String uncommented(String source) {
        StringBuilder out = new StringBuilder();
        int i = 0;
        while (i < source.length()) {
            if (source.startsWith("//", i)) {
                int end = source.indexOf('\n', i);
                i = end < 0 ? source.length() : end;
            } else if (source.startsWith("/*", i)) {
                int end = source.indexOf("*/", i + 2);
                i = end < 0 ? source.length() : end + 2;
            } else if (source.charAt(i) == '"') {
                int end = i + 1;
                while (end < source.length() && source.charAt(end) != '"') {
                    end += source.charAt(end) == '\\' ? 2 : 1;
                }
                out.append(source, i, Math.min(end + 1, source.length()));
                i = end + 1;
            } else {
                out.append(source.charAt(i));
                i++;
            }
        }
        return out.toString();
    }

    /** The first index at or after {@code at} that is neither whitespace nor a comment. */
    private static int skip(String source, int at) {
        while (at < source.length()) {
            if (Character.isWhitespace(source.charAt(at))) {
                at++;
            } else if (source.startsWith("//", at)) {
                int end = source.indexOf('\n', at);
                at = end < 0 ? source.length() : end;
            } else if (source.startsWith("/*", at)) {
                int end = source.indexOf("*/", at + 2);
                at = end < 0 ? source.length() : end + 2;
            } else {
                break;
            }
        }
        return at;
    }

    /** The packages {@code source}'s package rules name in literals ({@code resideInAPackage},
     *  {@code resideInAnyPackage}, {@code resideOutsideOfPackage(s)}), each as written: com.legend's
     *  ({@code "com.legend.parser.."}) and any package's ({@code "..util.."}). The review of 11de9ae229, 2026-11-18: none
     *  was read; the review of 091b9bde36, the same day, MUST-FIX 1: {@code resideInAPackage}, the commonest, was not
     *  read either, nor a literal led by {@code ..}. */
    static List<String> packageLiterals(String source) {
        List<String> out = new ArrayList<>();
        Map<Integer, Lit> byStart = new TreeMap<>();
        literals(source).forEach(l -> byStart.put(l.from(), l));
        Matcher call = Pattern.compile("reside(?:InA|InAny|OutsideOf)Packages?\\(").matcher(source);
        while (call.find()) {
            // every argument of the site read, or the site a finding (review F of the guard line, 2026-11-18, SHOULD 2:
            // an argument neither a literal nor a bare constant -- Other.GONE, a lower-case name, a call -- was skipped
            // silently, and a literal after a handed constant went unread)
            int at = skip(source, call.end());
            while (at < source.length() && source.charAt(at) != ')') {
                Lit lit = byStart.get(at);
                Matcher handed = Pattern.compile("([A-Z][A-Z0-9_]*)\\s*(?=[,)])").matcher(source)
                        .region(at, source.length());
                if (lit != null) {
                    // every literal of lite's namespace or a pattern (the review of dd5deb781f, 2026-11-18: "com.*.x"
                    // and "*.legend.gone.." were skipped); a third party's (java.sql.., org.duckdb..) is not in the jars
                    if (ours(lit.text())) {
                        out.add(lit.text());
                    }
                    at = skip(source, lit.end());
                } else if (handed.lookingAt()) {
                    // a constant handed to the rule (resideInAnyPackage(RUN_SIDE)): its array's literals, read from
                    // this file (the review of dd5deb781f, 2026-11-18, SHOULD); one unresolved is a finding
                    List<String> resolved = arrayLiterals(source, handed.group(1));
                    if (resolved == null) {
                        out.add("?" + handed.group(1));
                    } else {
                        resolved.stream().filter(GuardNamesExistTest::ours).forEach(out::add);
                    }
                    at = skip(source, handed.end());
                } else {
                    int close = source.indexOf(')', at);
                    out.add("?" + source.substring(at, close < 0 ? source.length() : close).strip());
                    break;
                }
                if (at < source.length() && source.charAt(at) == ',') {
                    at = skip(source, at + 1);
                } else if (at >= source.length() || source.charAt(at) != ')') {
                    int close = source.indexOf(')', at);
                    out.add("?" + source.substring(at, close < 0 ? source.length() : close).strip());
                    break;
                }
            }
        }
        return out;
    }

    /** Whether a package literal is one the jars read can answer: lite's own namespace (or a tripwire naming another
     *  legend's), a leading '..', or a wildcard. A third party's -- the JDK's, a driver's -- is not. */
    static boolean ours(String pkg) {
        return pkg.contains("legend") || pkg.startsWith("..") || pkg.contains("*");
    }

    /** {@code source} with every comment and every string, text block and char literal spaced out, its length and lines
     *  kept: its code alone, by {@link #literals}' lexer. */
    static String javaCode(String source) {
        char[] out = source.toCharArray();
        Map<Integer, Lit> byStart = new TreeMap<>();
        literals(source).forEach(l -> byStart.put(l.from(), l));
        int i = 0;
        while (i < out.length) {
            int end;
            Lit lit = byStart.get(i);
            if (lit != null) {
                end = lit.end();
            } else if (source.startsWith("//", i)) {
                end = source.indexOf('\n', i);
                end = end < 0 ? out.length : end;
            } else if (source.startsWith("/*", i)) {
                end = source.indexOf("*/", i + 2);
                end = end < 0 ? out.length : end + 2;
            } else if (out[i] == '\'' && i + 1 < out.length) {
                end = i + (out[i + 1] == '\\' ? 4 : 3);
                while (end <= out.length && out[end - 1] != '\'') {
                    end++;
                }
                end = Math.min(end, out.length);
            } else {
                i++;
                continue;
            }
            for (int k = i; k < end; k++) {
                out[k] = out[k] == '\n' ? '\n' : ' ';
            }
            i = end;
        }
        return new String(out);
    }

    /** The string literals of {@code static final String[] name = {...};} in {@code source}, or null when there is no
     *  such array, or when {@code name} is declared more than once in the file: the declaration is read over the code
     *  alone, and a nested class's counts (the review of the guard line, G, SHOULD 2, 2026-11-18: a decoy inside a text
     *  block, or a nested class's array of the same name, ahead of the real one, was read). */
    static List<String> arrayLiterals(String source, String name) {
        // read in comment-free code, to the brace that closes the initializer (review F of the guard line, 2026-11-18,
        // SHOULD 2: a comment holding "};" ended the array early, and the literals after it went unread)
        String code = javaCode(source);
        // a declaration: a type's last token, then the name, then its initializer or its end (a keyword before the name
        // -- return NAME; -- is a use)
        if (Pattern.compile("(\\w+|[\\]>])\\s+" + Pattern.quote(name) + "\\s*(=(?!=)|;)").matcher(code).results()
                .filter(r -> !Set.of("return", "throw", "yield", "case", "else", "assert").contains(r.group(1)))
                .count() != 1) {
            return null;
        }
        Matcher m = Pattern.compile("static\\s+final\\s+String\\[\\]\\s+" + Pattern.quote(name) + "\\s*=\\s*\\{")
                .matcher(code);
        if (!m.find()) {
            return null;
        }
        int depth = 1;
        int end = m.end();
        for (; end < code.length() && depth > 0; end++) {
            char c = code.charAt(end);
            if (c == '{') {
                depth++;
            } else if (c == '}') {
                depth--;
            }
        }
        if (depth != 0) {
            return null;
        }
        List<String> out = new ArrayList<>();
        literals(source.substring(m.end(), end - 1)).forEach(l -> out.add(l.text()));
        return out;
    }

    /** Whether package rule {@code pkg} names a package some class of {@code classes} is in, read as ArchUnit reads it
     *  ({@code PackageMatcher}): {@code ..} any packages (leading, inner or trailing), {@code *} any text within one
     *  package; a capture group is refused by name. The review of 091b9bde36, 2026-11-18: a leading {@code ..} was
     *  skipped and an inner one ({@code com.legend..sql}) read as a literal, a false finding. */
    static boolean packageThere(String pkg, java.util.Collection<String> classes) {
        Pattern p = packagePattern(pkg);
        return classes.stream().map(c -> c.contains(".") ? c.substring(0, c.lastIndexOf('.')) : "")
                .anyMatch(cp -> p.matcher(cp).matches());
    }

    /** ArchUnit's package syntax as a regular expression over a package's name. */
    static Pattern packagePattern(String pkg) {
        StringBuilder re = new StringBuilder();
        int i = 0;
        while (i < pkg.length()) {
            if (pkg.startsWith("..", i)) {
                re.append(i == 0 ? "(?:.*\\.)?" : i + 2 == pkg.length() ? "(?:\\..*)?" : "(?:\\.|\\..*\\.)");
                i += 2;
            } else if (pkg.charAt(i) == '*') {
                // ArchUnit's PackageMatcher: '*' is \w+ (the review of dd5deb781f, 2026-11-18: [^.]* read
                // "com.legend.sql*" as com.legend.sql itself)
                re.append("\\w+");
                i++;
            } else if (pkg.charAt(i) == '(' || pkg.charAt(i) == ')') {
                throw new IllegalStateException("a package rule's capture group the check does not read: " + pkg);
            } else {
                re.append(Pattern.quote(String.valueOf(pkg.charAt(i))));
                i++;
            }
        }
        return Pattern.compile(re.toString());
    }

    /** Every branch of {@code regex}: each alternation of each group expanded to one alternative, the rest kept (a
     *  character class and an escape read as one piece). A pattern whose branches number past 256 is its own one
     *  branch. The review of 11de9ae229 (2026-11-18): a pattern was checked whole, so a dead alternative beside a live
     *  one ({@code driver\\.X|testdatagen\\.Y}) passed. Its limit, named (the review of the guard line, G, NOTE 4,
     *  2026-11-18): an optional GROUP is read both ways, but a character made optional alone ({@code SqlFn\\$?D?e?a?d?})
     *  is not -- each {@code ?} after a plain character keeps the branch that skips it, so a dead name spelled one
     *  optional character at a time passes; no guard writes one. */
    static List<String> branches(String regex) {
        List<String> out = expand(regex, 0, regex.length());
        return out.size() > 256 ? List.of(regex) : out;
    }

    /** The index past a quantifier at {@code i} whose minimum is 0 -- ?, *, {0}, {0,n}, {0,}, each lazy (?) or
     *  possessive (+) -- or {@code i} when there is none. */
    static int optionalQuantifier(String r, int i, int to) {
        if (i >= to) {
            return i;
        }
        int end;
        if (r.charAt(i) == '?' || r.charAt(i) == '*') {
            end = i + 1;
        } else {
            // the minimum read as a number (review F of the guard line, 2026-11-18, SHOULD 3: {00,1} passed unread)
            Matcher m = Pattern.compile("\\{(\\d+)(?:,\\d*)?\\}").matcher(r).region(i, to);
            if (!m.lookingAt() || new java.math.BigInteger(m.group(1)).signum() != 0) {
                return i;
            }
            end = m.end();
        }
        return end < to && (r.charAt(end) == '?' || r.charAt(end) == '+') ? end + 1 : end;
    }

    private static List<String> expand(String r, int from, int to) {
        List<List<String>> alternatives = new ArrayList<>();
        List<String> current = new ArrayList<>(List.of(""));
        int i = from;
        while (i < to) {
            char c = r.charAt(i);
            if (c == '\\') {
                String piece = r.substring(i, Math.min(i + 2, to));
                current.replaceAll(x -> x + piece);
                i += 2;
            } else if (c == '[') {
                int j = i + 1;
                while (j < to && r.charAt(j) != ']') {
                    j += r.charAt(j) == '\\' ? 2 : 1;
                }
                String piece = r.substring(i, Math.min(j + 1, to));
                current.replaceAll(x -> x + piece);
                i = j + 1;
            } else if (c == '(') {
                int depth = 0;
                int j = i;
                for (; j < to; j++) {
                    char d = r.charAt(j);
                    if (d == '\\') {
                        j++;
                    } else if (d == '[') {
                        while (j < to && r.charAt(j) != ']') {
                            j += r.charAt(j) == '\\' ? 2 : 1;
                        }
                    } else if (d == '(') {
                        depth++;
                    } else if (d == ')' && --depth == 0) {
                        break;
                    }
                }
                int inner = r.startsWith("?:", i + 1) ? i + 3 : i + 1;
                List<String> groups = expand(r, inner, j);
                List<String> next = new ArrayList<>();
                for (String x : current) {
                    for (String g : groups) {
                        next.add(x + "(?:" + g + ")");
                    }
                }
                current = next;
                i = j + 1;
                // a group a quantifier may skip (?, *, {0}, {0,n}, lazy or possessive): read present, so a dead piece
                // does not pass as the group's absence (the review of 091b9bde36, 2026-11-18: a(b|c)?d let a dead b
                // through as "ad"; the review of dd5deb781f: (\$Gone)?, {0,1} and ?? did too) -- unless every piece
                // holds a wildcard ((\$.*)?, a class's nested classes if it has any), which stays optional
                int q = optionalQuantifier(r, i, to);
                // a piece that is exactly a wildcard after one separator ((\$.*)?, a class's nested classes if it has
                // any) stays optional; a piece merely holding one ((\$Dead.*)?) does not (review F of the guard line,
                // 2026-11-18, SHOULD 3)
                boolean wild = groups.stream().allMatch(g -> g.matches("(?:\\\\[$#.]|[#])\\.[*+]"));
                if (q > i && !wild) {
                    i = q;
                }
            } else if (c == '|') {
                alternatives.add(current);
                current = new ArrayList<>(List.of(""));
                i++;
            } else {
                String piece = String.valueOf(c);
                current.replaceAll(x -> x + piece);
                i++;
            }
        }
        alternatives.add(current);
        List<String> out = new ArrayList<>();
        alternatives.forEach(out::addAll);
        return out;
    }

    /** Whether {@code name} ("a.B$C" or "a.B$C#m") is there: a class of {@code classes} (or, for a top-level class, a
     *  file of {@code sources}), with the member it names. */
    static boolean there(String name, Map<String, JavaClass> classes, Predicate<String> source) {
        String cls = name.contains("#") ? name.substring(0, name.indexOf('#')) : name;
        String member = name.contains("#") ? name.substring(name.indexOf('#') + 1) : null;
        JavaClass c = classes.get(cls);
        if (c == null) {
            return member == null && !cls.contains("$") && source.test(cls.replace('.', '/') + ".java");
        }
        if (member == null || member.equals("*")) {
            return true;
        }
        if (member.equals("<clinit>")) {
            return c.getStaticInitializer().isPresent();
        }
        return c.getMembers().stream().anyMatch(m -> m.getName().equals(member))
                || (member.equals("<init>") && !c.getConstructors().isEmpty());
    }

    /** What is wrong with {@code sources} ("file" -> text): each absent name not listed, "file | name: absent"; each
     *  listed name present, or gone from its file, "file | name: listed"; each pattern no class name matches, "file |
     *  /pattern/: matches nothing" -- a pattern's own entry in {@code absent} ("file | /pattern/") excuses it. */
    static List<String> stale(Map<String, String> sources, Map<String, JavaClass> classes, Predicate<String> source,
                              Map<String, String> absent) {
        List<String> out = new ArrayList<>();
        Set<String> seen = new TreeSet<>();
        sources.forEach((file, text) -> {
            for (String name : names(text)) {
                String key = file + " | " + name;
                boolean is = there(name, classes, source);
                if (absent.containsKey(key)) {
                    seen.add(key);
                    if (is) {
                        out.add(key + ": listed absent, and there (delete its entry)");
                    }
                } else if (!is) {
                    out.add(key + ": absent (a stale name: point it at the class where it lives, or delete it)");
                }
            }
            if (file.equals("ArchitectureTest.java")) {
                for (String p : namePatterns(text)) {
                    for (String branch : branches(p)) {
                        Pattern compiled = Pattern.compile(branch);
                        String key = file + " | /" + branch + "/";
                        boolean matches = classes.keySet().stream().anyMatch(n -> compiled.matcher(n).matches());
                        if (absent.containsKey(key)) {
                            seen.add(key);
                        } else if (!matches) {
                            out.add(key + ": matches nothing (the rule names no class, so it reads nothing)");
                        }
                    }
                }
                for (String pkg : packageLiterals(text)) {
                    if (pkg.startsWith("?")) {
                        out.add(file + " | " + pkg.substring(1) + ": a package rule handed a constant the check does not"
                                + " read (a String[] of this file's, of literals)");
                        continue;
                    }
                    String key = file + " | package " + pkg;
                    if (absent.containsKey(key)) {
                        seen.add(key);
                        // checked both ways, as a name is (the review of dd5deb781f, 2026-11-18, NOTE)
                        if (packageThere(pkg, classes.keySet())) {
                            out.add(key + ": listed absent, and there (delete its entry)");
                        }
                    } else if (!packageThere(pkg, classes.keySet())) {
                        out.add(key + ": no class is in it (the rule names no package, so it reads nothing)");
                    }
                }
            }
        });
        new TreeSet<>(absent.keySet()).stream().filter(k -> !seen.contains(k))
                .forEach(k -> out.add(k + ": listed, and no longer written there (delete its entry)"));
        return out;
    }

    private static Map<String, JavaClass> classes(String... properties) {
        List<java.util.jar.JarFile> jars = new ArrayList<>();
        try {
            for (String property : properties) {
                for (Path jar : com.legend.testing.Runfile.listed(property)) {
                    jars.add(new java.util.jar.JarFile(jar.toFile()));
                }
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        JavaClasses imported = new ClassFileImporter().importJars(jars);
        Map<String, JavaClass> out = new TreeMap<>();
        imported.forEach(c -> out.put(c.getName(), c));
        return out;
    }

    private static String read(Path p) {
        try {
            return Files.readString(p);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Test
    void everyNameAGuardWritesIsThere() {
        Map<String, String> sources = new TreeMap<>();
        Set<String> mainFiles = new TreeSet<>();
        for (Path p : SourceFiles.under("core/src/test/java")) {
            String path = SourceFiles.path(p);
            String text = path.endsWith(".java") ? read(p) : "";
            // (this class is the register of the names absent on purpose, and plants its own: not read)
            if (TAG.matcher(text).find() && !path.endsWith("/GuardNamesExistTest.java")) {
                sources.put(path.substring(path.lastIndexOf('/') + 1), text);
            }
        }
        for (Path p : SourceFiles.under("core/src/main")) {
            String path = SourceFiles.path(p);
            // a driver source set's class (core/src/main/<set>/com/legend/..., not core/src/main/java: a class there is
            // in a jar this test reads, or the rule naming it reads nothing -- the review of 11de9ae229, 2026-11-18)
            int at = path.indexOf("/com/legend/");
            if (path.endsWith(".java") && at >= 0 && !path.startsWith("core/src/main/java/")) {
                mainFiles.add(path.substring(at + 1));
            }
        }
        Map<String, JavaClass> classes = classes("legend.product.jars", "legend.authored.jars", "legend.edge.jars",
                "legend.test.jars");
        assertTrue(sources.containsKey("ArchitectureTest.java") && sources.size() > 40 && classes.size() > 3000
                && mainFiles.size() >= 3, "the guards and the jars are read: " + sources.size() + ", "
                + classes.size() + ", " + mainFiles.size());
        // every pattern site read but those handed a name (a runner's pattern), pinned exactly so a new name-handed site
        // is seen (the review of 11de9ae229, 2026-11-18: a floor let one go unread); measured 2026-11-18
        String arch = sources.get("ArchitectureTest.java");
        int sites = (int) Pattern.compile("haveName(?:Not)?Matching\\(").matcher(uncommented(arch)).results().count();
        assertEquals(sites - NAME_HANDED_PATTERN_SITES, namePatterns(arch).size(), "ArchitectureTest's pattern sites"
                + " handed a name (not read) number " + NAME_HANDED_PATTERN_SITES + ": " + namePatterns(arch));
        assertTrue(packageLiterals(arch).size() > 40, "ArchitectureTest's package literals are read");
        assertEquals(List.of(), stale(sources, classes, mainFiles::contains, ABSENT_ON_PURPOSE), "a guard names what is"
                + " not there: it reads nothing for that name");
    }

    /** Planted: a class gone, a member gone, a nested class gone, a pattern that matches nothing, a listed name that is
     *  there and a listed name no longer written are each named; a commented-out name, a name in a text block's
     *  neighbour comment and a pattern joined by {@code +} are read as what they are. */
    @Test
    void theCheckBites() {
        Map<String, JavaClass> classes = new TreeMap<>();
        new ClassFileImporter().importClasses(GuardNamesExistTest.class, com.legend.sql.SqlFn.class)
                .forEach(c -> classes.put(c.getName(), c));
        String planted = "class P {\n"
                + "  private static final String[] HANDED = {\"com.legend..\", \"com.legend.handed.gone..\","
                + " \"org.duckdb..\"};\n"
                + "  // \"com.legend.commented.Out\"\n"
                + "  static final Set<String> A = Set.of(\"com.legend.gone.Class\", \"com.legend.sql.SqlFn#ABS\",\n"
                + "      \"com.legend.sql.SqlFn#NO_SUCH\", \"com.legend.sql.SqlFn$Gone\", \"com.legend.listed.There\");\n"
                + "  void r() { x.haveNameMatching(\"com\\\\.legend\\\\.gone\\\\.\" + \"(A|B)\"); "
                + "y.haveNameMatching(\"com\\\\.legend\\\\.sql\\\\.\" + \"SqlFn\"); "
                + "z.haveNameNotMatching(\"com\\\\.legend\\\\.(sql\\\\.SqlFn|gone\\\\.C)\"); "
                + "p.resideInAnyPackage(\"com.legend.sql..\", \"com.legend.gone..\"); "
                // the review of 091b9bde36, 2026-11-18: resideInAPackage, a leading '..', an inner '..'
                + "q.resideInAPackage(\"com.legend.lost..\"); q.resideInAPackage(\"..sql..\"); "
                + "q.resideInAPackage(\"..nowhere..\"); q.resideOutsideOfPackages(\"com.legend..sql\", \"com.*.sql\"); "
                // the review of dd5deb781f (2026-11-18): '*' is \\w+, so com.legend.sql* names no package; a literal
                // led by anything but com.legend is read; a JDK literal is not
                + "q.resideInAPackage(\"com.legend.sql*\"); q.resideInAnyPackage(\"com.*.nowhere\", \"java..\"); "
                // a handed constant, resolved (one live, one gone, one a third party's) and one unresolved
                + "q.resideInAnyPackage(HANDED); q.resideInAnyPackage(NOWHERE); "
                // an optional group of alternatives, each dead: neither passes as the group's absence
                + "o.haveNameMatching(\"com\\\\.legend\\\\.sql\\\\.SqlFn(\\\\$Gone|\\\\$Lost)?\"); "
                // one dead optional piece, a {0,1} and a lazy ?? (the review of dd5deb781f, 2026-11-18)
                + "o.haveNameMatching(\"com\\\\.legend\\\\.sql\\\\.SqlFn(\\\\$Dead)?\"); "
                + "o.haveNameMatching(\"com\\\\.legend\\\\.sql\\\\.SqlFn(\\\\$Zero){0,1}\"); "
                + "o.haveNameMatching(\"com\\\\.legend\\\\.sql\\\\.SqlFn(\\\\$Lazy)??\"); "
                // review F of the guard line (2026-11-18, SHOULD 3): a dead piece holding a wildcard, a {00,1}
                + "o.haveNameMatching(\"com\\\\.legend\\\\.sql\\\\.SqlFn(\\\\$Wild.*)?\"); "
                + "o.haveNameMatching(\"com\\\\.legend\\\\.sql\\\\.SqlFn(\\\\$Padded){00,1}\"); "
                // (SHOULD 2): an argument neither a literal nor a bare constant, and a literal after a handed constant
                // and an array whose comment holds the initializer's end
                + "q.resideInAnyPackage(Other.GONE); q.resideInAnyPackage(COMMENTED, \"com.legend.after.gone..\"); "
                + "q.resideInAnyPackage(LISTED.toArray(new String[0])); "
                // the review of the guard line, G, SHOULD 2 (2026-11-18): a decoy of the handed array inside a text
                // block, ahead of the real one, is not read; a name declared twice (a nested class's) is not read
                + "q.resideInAnyPackage(BLOCKED); q.resideInAnyPackage(TWICE); }\n"
                + "  static final String[] COMMENTED = {\"com.legend..\", // the list ends here: };\n"
                + "      \"com.legend.commented.gone..\"};\n"
                + "  static final String NOTE = \"\"\"\n      static final String[] BLOCKED = {\"com.legend..\"};\n      \"\"\";\n"
                + "  static final String[] BLOCKED = {\"com.legend.blocked.gone..\"};\n"
                + "  static final class Old { static final String[] TWICE = {\"com.legend..\"}; }\n"
                + "  static final String[] TWICE = {\"com.legend.twice.gone..\"};\n}\n";
        Map<String, String> listed = Map.of("ArchitectureTest.java | com.legend.listed.There", "planted",
                "ArchitectureTest.java | com.legend.not.Written", "planted",
                // a package listed absent that is there (the review of dd5deb781f, 2026-11-18)
                "ArchitectureTest.java | package com.legend.sql..", "planted");
        assertEquals(List.of("ArchitectureTest.java | com.legend.gone.Class: absent (a stale name: point it at the class"
                        + " where it lives, or delete it)",
                "ArchitectureTest.java | com.legend.listed.There: listed absent, and there (delete its entry)",
                "ArchitectureTest.java | com.legend.sql.SqlFn#NO_SUCH: absent (a stale name: point it at the class"
                        + " where it lives, or delete it)",
                "ArchitectureTest.java | com.legend.sql.SqlFn$Gone: absent (a stale name: point it at the class"
                        + " where it lives, or delete it)",
                "ArchitectureTest.java | /com\\.legend\\.gone\\.(?:A)/: matches nothing (the rule names no class, so"
                        + " it reads nothing)",
                "ArchitectureTest.java | /com\\.legend\\.gone\\.(?:B)/: matches nothing (the rule names no class, so"
                        + " it reads nothing)",
                "ArchitectureTest.java | /com\\.legend\\.(?:gone\\.C)/: matches nothing (the rule names no class, so it"
                        + " reads nothing)",
                "ArchitectureTest.java | /com\\.legend\\.sql\\.SqlFn(?:\\$Gone)/: matches nothing (the rule names no"
                        + " class, so it reads nothing)",
                "ArchitectureTest.java | /com\\.legend\\.sql\\.SqlFn(?:\\$Lost)/: matches nothing (the rule names no"
                        + " class, so it reads nothing)",
                "ArchitectureTest.java | /com\\.legend\\.sql\\.SqlFn(?:\\$Dead)/: matches nothing (the rule names no"
                        + " class, so it reads nothing)",
                "ArchitectureTest.java | /com\\.legend\\.sql\\.SqlFn(?:\\$Zero)/: matches nothing (the rule names no"
                        + " class, so it reads nothing)",
                "ArchitectureTest.java | /com\\.legend\\.sql\\.SqlFn(?:\\$Lazy)/: matches nothing (the rule names no"
                        + " class, so it reads nothing)",
                "ArchitectureTest.java | /com\\.legend\\.sql\\.SqlFn(?:\\$Wild.*)/: matches nothing (the rule names no"
                        + " class, so it reads nothing)",
                "ArchitectureTest.java | /com\\.legend\\.sql\\.SqlFn(?:\\$Padded)/: matches nothing (the rule names no"
                        + " class, so it reads nothing)",
                "ArchitectureTest.java | package com.legend.sql..: listed absent, and there (delete its entry)",
                "ArchitectureTest.java | package com.legend.gone..: no class is in it (the rule names no package, so it"
                        + " reads nothing)",
                "ArchitectureTest.java | package com.legend.lost..: no class is in it (the rule names no package, so it"
                        + " reads nothing)",
                "ArchitectureTest.java | package ..nowhere..: no class is in it (the rule names no package, so it"
                        + " reads nothing)",
                "ArchitectureTest.java | package com.legend.sql*: no class is in it (the rule names no package, so it"
                        + " reads nothing)",
                "ArchitectureTest.java | package com.*.nowhere: no class is in it (the rule names no package, so it"
                        + " reads nothing)",
                "ArchitectureTest.java | package com.legend.handed.gone..: no class is in it (the rule names no package,"
                        + " so it reads nothing)",
                "ArchitectureTest.java | NOWHERE: a package rule handed a constant the check does not read (a String[]"
                        + " of this file's, of literals)",
                "ArchitectureTest.java | Other.GONE: a package rule handed a constant the check does not read (a String[]"
                        + " of this file's, of literals)",
                "ArchitectureTest.java | package com.legend.commented.gone..: no class is in it (the rule names no"
                        + " package, so it reads nothing)",
                "ArchitectureTest.java | package com.legend.after.gone..: no class is in it (the rule names no package, so"
                        + " it reads nothing)",
                "ArchitectureTest.java | LISTED.toArray(new String[0]: a package rule handed a constant the check does"
                        + " not read (a String[] of this file's, of literals)",
                "ArchitectureTest.java | package com.legend.blocked.gone..: no class is in it (the rule names no package,"
                        + " so it reads nothing)",
                "ArchitectureTest.java | TWICE: a package rule handed a constant the check does not read (a String[] of"
                        + " this file's, of literals)",
                "ArchitectureTest.java | com.legend.not.Written: listed, and no longer written there (delete its"
                        + " entry)"),
                stale(Map.of("ArchitectureTest.java", planted), classes, f -> f.equals("com/legend/listed/There.java"),
                        listed));
    }
}
