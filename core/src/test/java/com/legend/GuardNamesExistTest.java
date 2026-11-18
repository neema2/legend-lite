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
            Map.entry("JudgingRulesReachTest.java | com.legend.testexec.PlantedRules", "planted (a prefix), 2026-11-17"),
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
            Map.entry("TaggedClassesHaveATargetTest.java | com.legend.slow.ArgDropTest", "planted, 2026-11-17"),
            Map.entry("TaggedClassesHaveATargetTest.java | com.legend.slow.CommentedOutTest", "planted, 2026-11-17"),
            Map.entry("TaggedClassesHaveATargetTest.java | com.legend.slow.DroppedTest", "planted, 2026-11-17"),
            Map.entry("TaggedClassesHaveATargetTest.java | com.legend.slow.HashedTest", "planted, 2026-11-17"),
            Map.entry("TaggedClassesHaveATargetTest.java | com.legend.slow.HeavyOnlyTest", "planted, 2026-11-17"),
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
        Matcher call = Pattern.compile("haveNameMatching\\(").matcher(source);
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

    /** The first index at or after {@code at} that is not whitespace. */
    private static int skip(String source, int at) {
        while (at < source.length() && Character.isWhitespace(source.charAt(at))) {
            at++;
        }
        return at;
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
                    Pattern compiled = Pattern.compile(p);
                    String key = file + " | /" + p + "/";
                    boolean matches = classes.keySet().stream().anyMatch(n -> compiled.matcher(n).matches());
                    if (absent.containsKey(key)) {
                        seen.add(key);
                    } else if (!matches) {
                        out.add(key + ": matches nothing (the rule names no class, so it reads nothing)");
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
            // a driver source set's class (core/src/main/<set>/com/legend/...), by its package path
            int at = path.indexOf("/com/legend/");
            if (path.endsWith(".java") && at >= 0) {
                mainFiles.add(path.substring(at + 1));
            }
        }
        Map<String, JavaClass> classes = classes("legend.product.jars", "legend.authored.jars", "legend.edge.jars",
                "legend.test.jars");
        assertTrue(sources.containsKey("ArchitectureTest.java") && sources.size() > 40 && classes.size() > 3000
                && mainFiles.size() > 1000, "the guards and the jars are read: " + sources.size() + ", "
                + classes.size() + ", " + mainFiles.size());
        // its thirteen sites, less the two handed a name (a runner's pattern), measured 2026-11-17
        assertTrue(namePatterns(sources.get("ArchitectureTest.java")).size() >= 11, "ArchitectureTest's patterns are"
                + " read: " + namePatterns(sources.get("ArchitectureTest.java")));
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
                + "  // \"com.legend.commented.Out\"\n"
                + "  static final Set<String> A = Set.of(\"com.legend.gone.Class\", \"com.legend.sql.SqlFn#ABS\",\n"
                + "      \"com.legend.sql.SqlFn#NO_SUCH\", \"com.legend.sql.SqlFn$Gone\", \"com.legend.listed.There\");\n"
                + "  void r() { x.haveNameMatching(\"com\\\\.legend\\\\.gone\\\\.\" + \"(A|B)\"); "
                + "y.haveNameMatching(\"com\\\\.legend\\\\.sql\\\\.\" + \"SqlFn\"); }\n}\n";
        Map<String, String> listed = Map.of("ArchitectureTest.java | com.legend.listed.There", "planted",
                "ArchitectureTest.java | com.legend.not.Written", "planted");
        assertEquals(List.of("ArchitectureTest.java | com.legend.gone.Class: absent (a stale name: point it at the class"
                        + " where it lives, or delete it)",
                "ArchitectureTest.java | com.legend.listed.There: listed absent, and there (delete its entry)",
                "ArchitectureTest.java | com.legend.sql.SqlFn#NO_SUCH: absent (a stale name: point it at the class"
                        + " where it lives, or delete it)",
                "ArchitectureTest.java | com.legend.sql.SqlFn$Gone: absent (a stale name: point it at the class"
                        + " where it lives, or delete it)",
                "ArchitectureTest.java | /com\\.legend\\.gone\\.(A|B)/: matches nothing (the rule names no class, so it"
                        + " reads nothing)",
                "ArchitectureTest.java | com.legend.not.Written: listed, and no longer written there (delete its"
                        + " entry)"),
                stale(Map.of("ArchitectureTest.java", planted), classes, f -> f.equals("com/legend/listed/There.java"),
                        listed));
    }
}
