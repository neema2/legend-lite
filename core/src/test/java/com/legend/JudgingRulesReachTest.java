// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend;

import com.legend.testexecutionplan.TestPlan.JudgingRules;
import com.tngtech.archunit.core.domain.AccessTarget;
import com.tngtech.archunit.core.domain.JavaAccess;
import com.tngtech.archunit.core.domain.JavaClass;
import com.tngtech.archunit.core.importer.ClassFileImporter;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * THE JUDGING RULES' TYPE, NOT ONLY ITS COMPATIBILITY CONSTANT (the GUARD stream, step 2's C.3; the edge audit of
 * 2026-11-17, section C.3: "the JudgingRules TYPE flagged, not just COMPILED_PURE_COMPAT"). The census
 * ({@link EdgeSeparationTest}) counts a product class that names {@code JudgingRules.COMPILED_PURE_COMPAT}; a class that
 * is handed the rules and asks them a BEHAVIOUR ({@code rules.legendPureWords()}) selects legend-pure's behaviour by the
 * same flag without naming it, and was counted nowhere. Two scans, each exact:
 * <ol>
 *   <li><b>Reaches</b> (ArchUnit: field accesses, calls, method and constructor references). Every member of
 *   {@link JudgingRules} -- each field by its name, each method by its signature, each nested class -- is sorted into
 *   one kind (the orchestrator's rulings of 2026-11-17, 16:40 and 17:24, census C1):
 *   <ul>
 *     <li>{@link Kind#DATA}: a data parameter the product's rules keep as a value (ruled (a)): free to read;</li>
 *     <li>{@link Kind#BEHAVIOUR}: legend-pure's behaviour chosen by the flag (ruled (b): it moves behind an edge hook):
 *     every product class that asks it is an open entry of census line C1 ({@link #BEHAVIOUR_OPEN}, counted by the
 *     census), its reaches pinned by number ({@link #REACHES_PINNED});</li>
 *     <li>{@link Kind#CONSTANT}: {@code COMPILED_PURE_COMPAT}, counted by the census itself (not twice);</li>
 *     <li>{@link Kind#CARRIER}: the type carried and enumerated ({@code LITE}, {@code values()}, an Enum method): every
 *     product reach pinned by number ({@link #REACHES_PINNED}), so a new {@code rules == LITE} or
 *     {@code rules.ordinal()} -- the flag read without a fact -- fails until it is ruled.</li>
 *   </ul></li>
 *   <li><b>Names</b> (the constant pool, {@link ConstantPools}): a dependency graph does not see a pattern switch's labels
 *   ({@code SwitchBootstraps.enumSwitch} takes them as strings), a class literal handed to {@code Enum.valueOf},
 *   {@code EnumSet.allOf} or reflection, a value compared after a call that returned it, a string concatenated by
 *   {@code invokedynamic}. Each leaves the type's name or the constant's in the class's pool, so every product class
 *   whose pool names the type -- or holds the compatibility constant's spelling at all -- is pinned with the
 *   distinctive tokens it holds ({@link #NAMES_PINNED}): a new class naming the type, or a new token in one, fails.</li>
 * </ol>
 * The edge's classes and the type's own are not scanned. Guards catch drift, not adversaries; what remains unseen is
 * named: a second use of a token a class's pool already holds (a reach is counted, a pool is a set), and a value reached
 * through {@code Object} alone in a class that never names the type.
 */
@Tag("guardrail")
class JudgingRulesReachTest {

    enum Kind { DATA, BEHAVIOUR, CONSTANT, CARRIER }

    private static final String OWNER = JudgingRules.class.getName();

    private static final String OWNER_INTERNAL = OWNER.replace('.', '/');

    /** Every declared member of the type (synthetic ones aside), with its kind: exact. A field by its name, a method by
     *  its name and parameters, a nested class as "Name (class)" (none today: one added is sorted here). */
    static final Map<String, Kind> MEMBERS = Map.ofEntries(
            Map.entry("LITE", Kind.CARRIER),
            Map.entry("COMPILED_PURE_COMPAT", Kind.CONSTANT),
            // the private field every fact reads: reached only by the type itself
            Map.entry("upstream", Kind.CARRIER),
            Map.entry("values()", Kind.CARRIER),
            Map.entry("valueOf(java.lang.String)", Kind.CARRIER),
            // ruled (a), 2026-11-17 16:40: the data parameters that stay the product's
            Map.entry("floatTolerance()", Kind.DATA),
            Map.entry("decimalScale()", Kind.DATA),
            // ruled (b), 2026-11-17 16:40 and 17:24: behaviours that move behind an edge hook
            // (integerDecimalByText() and renderedTextRuled(): gone from the type 2026-11-17 with D's train 4 -- C1 2c's
            // CrossKindNumbers and the rendered-text rule's two-form kind; their reaches closed with them)
            Map.entry("legendPureWords()", Kind.BEHAVIOUR));
            // (edgeAppeals(): gone from the type 2026-11-17 with E-Java's C1, 278188ddcb, D's train 6)

    /** The methods every enum inherits, by signature: reached on the type, they carry or enumerate it. */
    static final Set<String> ENUM_METHODS = Set.of("name()", "ordinal()", "compareTo(java.lang.Enum)",
            "equals(java.lang.Object)", "hashCode()", "toString()", "getDeclaringClass()", "describeConstable()");

    /** BEHAVIOUR: "source class -> JudgingRules.member()", each an open entry of census line C1 (counted by the census's
     *  {@code linesLeft}; its reaches pinned by number in {@link #REACHES_PINNED}). */
    static final Map<String, String> BEHAVIOUR_OPEN = Map.ofEntries(
            // the host judge's words and its rendered-text rule (2026-11-17: found by this guard, counted nowhere before)
            Map.entry("com.legend.testexec.HostJudgments -> JudgingRules.legendPureWords()", "C1 2026-11-17"),
            // found on d-int6 (2026-11-17, E-Java's C1 2c): the assert-error runs ask the rules for lite's sentences, as
            // HostJudgments does -- counted on C1, reported to the orchestrator
            Map.entry("com.legend.testexec.AssertErrorRuns -> JudgingRules.legendPureWords()", "C1 2026-11-17"),
            // (IsolatedRun -> legendPureWords() CLOSED 2026-11-17 by C5 step B: legend-pure's words at run are the
            // hearing's PureWords hook, NoEdgeRun refusing them by name; its reach and its pool's mention gone)
            Map.entry("com.legend.testplan.DatabaseJudge -> JudgingRules.legendPureWords()", "C1 2026-11-17"));
            // (PlanningOptions -> edgeAppeals() CLOSED 2026-11-17 by E-Java's C1, 278188ddcb, D's train 6: the member gone)

    /** BEHAVIOUR and CARRIER: "source class -> JudgingRules.member" with the number of its reaches, exact (a reach
     *  added -- a second legendPureWords() in a class already counted -- fails until it is ruled; a reach gone lowers
     *  its pin). Measured 2026-11-17. */
    static final Map<String, Integer> REACHES_PINNED = Map.ofEntries(
            // the behaviours (BEHAVIOUR_OPEN's entries), one reach each; the carriers: the test plan's JSON (a verdict
            // group's rules spelled by a switch -- LITE twice, ordinal three times and values once through its switch
            // maps -- an isolated step's by name/valueOf) and the one selector, engineCompat ? COMPILED_PURE_COMPAT : LITE
            Map.entry("com.legend.testexec.AssertErrorRuns -> JudgingRules.legendPureWords()", 1),
            Map.entry("com.legend.testexec.HostJudgments -> JudgingRules.legendPureWords()", 1),
            // carriers found on d-int6 (2026-11-17, E-Java's C1): the host judge's rules, lite's by default (HostRules);
            // the product's edge hook records under lite's rules (NoEdge)
            Map.entry("com.legend.testexec.HostRules -> JudgingRules.LITE", 1),
            Map.entry("com.legend.testexecutionplan.NoEdge -> JudgingRules.LITE", 1),
            Map.entry("com.legend.testexecutionplan.TestPlanJson -> JudgingRules.LITE", 2),
            Map.entry("com.legend.testexecutionplan.TestPlanJson -> JudgingRules.name()", 1),
            Map.entry("com.legend.testexecutionplan.TestPlanJson -> JudgingRules.ordinal()", 3),
            Map.entry("com.legend.testexecutionplan.TestPlanJson -> JudgingRules.valueOf(java.lang.String)", 1),
            Map.entry("com.legend.testexecutionplan.TestPlanJson -> JudgingRules.values()", 1),
            Map.entry("com.legend.testplan.DatabaseJudge -> JudgingRules.legendPureWords()", 1),
            Map.entry("com.legend.testplan.PlanningOptions -> JudgingRules.LITE", 1));
            // (PlanningOptions -> edgeAppeals(): gone with the member, 278188ddcb)

    /** The distinctive tokens a pool may hold: the constants' and the facts' names, and the plan JSON's spelling of the
     *  compatibility rules. */
    static final Set<String> TOKENS = Set.of("COMPILED_PURE_COMPAT", "compiledPureCompat", "LITE", "floatTolerance",
            "decimalScale", "integerDecimalByText", "renderedTextRuled", "legendPureWords", "edgeAppeals");

    /** The pseudo-token of a pool that names the type itself. */
    static final String TYPE = "(the type)";

    /** NAMES: every product class (with its nested ones) whose constant pool names the type or holds the compatibility
     *  constant's spelling, with the tokens it holds: exact. Measured 2026-11-17. */
    static final Map<String, Set<String>> NAMES_PINNED = Map.ofEntries(
            Map.entry("com.legend.ExecuteOptions", Set.of(TYPE)),
            // + legendPureWords (2026-11-17, found on d-int6, E-Java's C1: lite's sentences asked of the rules)
            Map.entry("com.legend.testexec.AssertErrorRuns", Set.of(TYPE, "legendPureWords")),
            Map.entry("com.legend.testexec.AssertRuns", Set.of(TYPE)),
            Map.entry("com.legend.testexec.BodyRun", Set.of(TYPE)),
            Map.entry("com.legend.testexec.EdgeHearing", Set.of(TYPE)),
            // (testexec.Equality, PureAsserts, TdsCompare: no longer name the type 2026-11-17, E-Java's C1 2c on d-int6 --
            // the host judge carries the plan's rules with the run's forms, HostRules)
            Map.entry("com.legend.testexec.HostRules", Set.of(TYPE, "LITE", "decimalScale", "floatTolerance")),
            // (renderedTextRuled gone 2026-11-17 with D's train 4, its member deleted from the type)
            Map.entry("com.legend.testexec.HostJudgments", Set.of(TYPE, "legendPureWords")),
            Map.entry("com.legend.testexec.NoEdgeRun", Set.of(TYPE)),
            Map.entry("com.legend.testexec.TestRunner", Set.of(TYPE)),
            Map.entry("com.legend.testexecutionplan.TestPlan", Set.of(TYPE)),
            // the typed edge hook and the product's answer (census C1): the ledger's recorder names the rules it records
            // under; NoEdge answers lite's
            Map.entry("com.legend.testexecutionplan.Edge", Set.of(TYPE)),
            Map.entry("com.legend.testexecutionplan.NoEdge", Set.of(TYPE, "LITE")),
            Map.entry("com.legend.testexecutionplan.TestPlanJson",
                    Set.of(TYPE, "COMPILED_PURE_COMPAT", "LITE", "compiledPureCompat")),
            // (testplan.AssertVerdicts: gone 2026-11-17 with renderedTextRuled, D's train 4)
            Map.entry("com.legend.testplan.DatabaseAsserts", Set.of(TYPE)),
            // + LITE (2026-11-17, D's train 4): DateLiteralTexts.Product.LITE, a two-form kind's product form -- the
            // whole-word scan's look-alike, not the judging rules' constant (a reach of it would be counted above)
            Map.entry("com.legend.testplan.DatabaseJudge",
                    Set.of(TYPE, "decimalScale", "floatTolerance", "legendPureWords", "LITE")),
            Map.entry("com.legend.testplan.EngineTextPlanning", Set.of(TYPE)),
            Map.entry("com.legend.testplan.HostAsserts", Set.of(TYPE)),
            Map.entry("com.legend.testplan.HostSideBatching", Set.of(TYPE)),
            Map.entry("com.legend.testplan.JudgePlans", Set.of(TYPE)),
            // (- edgeAppeals: gone with the member, 278188ddcb)
            Map.entry("com.legend.testplan.PlanningOptions", Set.of(TYPE, "COMPILED_PURE_COMPAT", "LITE")),
            Map.entry("com.legend.testplan.TestPlanner", Set.of(TYPE)),
            Map.entry("com.legend.testplan.VerdictBatch", Set.of(TYPE)));

    // ---- the scans ---------------------------------------------------------------------------------------------

    private static String top(String n) {
        int dollar = n.indexOf('$');
        return dollar < 0 ? n : n.substring(0, dollar);
    }

    private static boolean scanned(String n) {
        return !n.startsWith("com.legend.edge.") && !n.equals(OWNER) && !n.startsWith(OWNER + "$");
    }

    /** An access's member of the type, as {@link #MEMBERS} keys it, or null when it reaches another type. */
    private static String memberOf(JavaAccess<?> a) {
        if (!a.getTargetOwner().getName().equals(OWNER)) {
            return null;
        }
        if (a.getTarget() instanceof AccessTarget.CodeUnitAccessTarget unit) {
            return a.getName() + unit.getRawParameterTypes().stream().map(JavaClass::getName)
                    .collect(Collectors.joining(", ", "(", ")"));
        }
        return a.getName();
    }

    /** Every reach of the type's members from {@code classes} (the edge's and the type's own aside), "source ->
     *  JudgingRules.member" with its count, a nested class's reaches its top class's. */
    static Map<String, Integer> reaches(Iterable<JavaClass> classes) {
        Map<String, Integer> out = new TreeMap<>();
        for (JavaClass c : classes) {
            if (!scanned(c.getName())) {
                continue;
            }
            Stream.of(c.getFieldAccessesFromSelf(), c.getMethodCallsFromSelf(), c.getConstructorCallsFromSelf(),
                            c.getMethodReferencesFromSelf(), c.getConstructorReferencesFromSelf())
                    .flatMap(Set::stream)
                    .forEach(a -> {
                        String m = memberOf(a);
                        if (m != null) {
                            out.merge(top(c.getName()) + " -> JudgingRules." + m, 1, Integer::sum);
                        }
                    });
        }
        return out;
    }

    /** The kind of a reached member: its sorted kind, an Enum method a carrier, anything else null (unsorted). */
    static Kind kindOf(String member) {
        Kind k = MEMBERS.get(member);
        if (k != null) {
            return k;
        }
        return ENUM_METHODS.contains(member) ? Kind.CARRIER : null;
    }

    /** {@code reaches} sorted: the unsorted ones, the behaviour and carrier ones by count (DATA and CONSTANT dropped). */
    record Sorted(List<String> unsorted, Map<String, Integer> pinned, Set<String> behaviour) {
    }

    static Sorted sort(Map<String, Integer> reaches) {
        List<String> unsorted = new ArrayList<>();
        Map<String, Integer> pinned = new TreeMap<>();
        Set<String> behaviour = new TreeSet<>();
        reaches.forEach((reach, n) -> {
            Kind k = kindOf(reach.substring(reach.indexOf(" -> JudgingRules.") + " -> JudgingRules.".length()));
            if (k == null) {
                unsorted.add(reach);
            } else if (k == Kind.BEHAVIOUR || k == Kind.CARRIER) {
                pinned.put(reach, n);
                if (k == Kind.BEHAVIOUR) {
                    behaviour.add(reach);
                }
            }
        });
        return new Sorted(unsorted, pinned, behaviour);
    }

    /** The classes of {@code pools} (binary name -> its pool's UTF-8 entries) that name the type or hold the
     *  compatibility constant's spelling, by top class, each with the tokens it holds ({@link #TYPE} when it names the
     *  type). */
    static Map<String, Set<String>> names(Map<String, Set<String>> pools) {
        Map<String, Set<String>> out = new TreeMap<>();
        pools.forEach((name, utf8) -> {
            if (!scanned(name)) {
                return;
            }
            boolean type = utf8.stream().anyMatch(u -> u.contains(OWNER_INTERNAL) || u.contains(OWNER));
            // a token as a whole word inside any entry: a string that embeds it ("{\"rules\":\"compiledPureCompat\"..."),
            // a concatenation's recipe, a descriptor (the review of 08adb5c923, 2026-11-17)
            Set<String> tokens = new TreeSet<>();
            for (String t : TOKENS) {
                if (utf8.stream().anyMatch(u -> ConstantPools.hasWord(u, t))) {
                    tokens.add(t);
                }
            }
            if (type || tokens.contains("COMPILED_PURE_COMPAT") || tokens.contains("compiledPureCompat")) {
                Set<String> held = out.computeIfAbsent(top(name), k -> new TreeSet<>());
                held.addAll(tokens);
                if (type) {
                    held.add(TYPE);
                }
            }
        });
        return out;
    }

    private static List<java.nio.file.Path> productJars() {
        return ConstantPools.jars("legend.product.jars", "legend.authored.jars");
    }

    private static Iterable<JavaClass> importJars(List<java.nio.file.Path> jars) {
        List<java.util.jar.JarFile> files = new ArrayList<>();
        try {
            for (java.nio.file.Path jar : jars) {
                files.add(new java.util.jar.JarFile(jar.toFile()));
            }
        } catch (java.io.IOException e) {
            throw new java.io.UncheckedIOException(e);
        }
        return new ClassFileImporter().importJars(files);
    }

    // ---- the tests ---------------------------------------------------------------------------------------------

    /** The type's declared members are exactly the sorted ones: a member added -- an overload, a nested class -- is
     *  sorted here with its ruling. */
    @Test
    void everyMemberIsSorted() {
        Set<String> declared = new TreeSet<>();
        Arrays.stream(JudgingRules.class.getDeclaredFields()).filter(f -> !f.isSynthetic())
                .forEach(f -> declared.add(f.getName()));
        for (Method m : JudgingRules.class.getDeclaredMethods()) {
            if (!m.isSynthetic() && !m.isBridge()) {
                declared.add(m.getName() + Arrays.stream(m.getParameterTypes()).map(Class::getTypeName)
                        .collect(Collectors.joining(", ", "(", ")")));
            }
        }
        Arrays.stream(JudgingRules.class.getDeclaredClasses()).forEach(c -> declared.add(c.getSimpleName() + " (class)"));
        assertEquals(new TreeSet<>(MEMBERS.keySet()), declared, "JudgingRules' members and their kinds differ: a new"
                + " member is sorted DATA, BEHAVIOUR or CARRIER here, with its ruling");
    }

    @Test
    void theProductsReachesAreTheSortedOnes() {
        Sorted s = sort(reaches(importJars(productJars())));
        assertEquals(List.of(), s.unsorted(), "a reach of an unsorted member of JudgingRules");
        assertEquals(new TreeSet<>(BEHAVIOUR_OPEN.keySet()), s.behaviour(), "BEHAVIOUR: a product class asks the"
                + " judging rules for legend-pure's behaviour outside its counted entry (a new one is a regression; a gone"
                + " one closes, delete its entry)");
        assertEquals(new TreeMap<>(REACHES_PINNED), s.pinned(), "BEHAVIOUR and CARRIER reaches by number: one added"
                + " asks a behaviour again or reads the flag without a fact (rule it); one gone lowers its pin");
    }

    @Test
    void theClassesNamingTheTypeArePinned() {
        assertEquals(new TreeMap<>(NAMES_PINNED), names(ConstantPools.ofJars(productJars())), "NAMES: a product class"
                + " names the judging rules' type, or holds a token of it, that its pin does not list (a pattern switch's"
                + " labels, a class literal, a value returned by a call, reflection): rule it; one gone lowers its pin");
    }

    /** Each behaviour entry names its census line and date. */
    @Test
    void everyBehaviourEntryIsDated() {
        BEHAVIOUR_OPEN.forEach((e, line) -> assertTrue(line.matches("C1 \\d{4}-\\d{2}-\\d{2}.*"), e + ": " + line));
    }

    /**
     * The planted product classes (com.legend.testexec, the edge's test library, read as bytecode alone):
     * {@code PlantedRulesReach} asks a behaviour twice, takes one by method reference, reads {@code != LITE},
     * {@code ordinal()} and a classic switch (its {@code $SwitchMap} in a nested class, merged into its top class), a data
     * parameter and the constant; {@code PlantedRulesSwitch} holds a pattern switch alone, which no reach shows and its
     * pool does; {@code PlantedRulesByClass} names the type by class literal alone; {@code PlantedRulesText} holds the
     * rules' wire spelling inside a longer string and never names the type.
     */
    @Test
    void theGuardBites() {
        String prefix = "com.legend.testexec.PlantedRules";
        List<JavaClass> planted = new ArrayList<>();
        new ClassFileImporter().importPackagesOf(com.legend.testexec.PlantedRulesReach.class).forEach(c -> {
            if (c.getName().startsWith(prefix)) {
                planted.add(c);
            }
        });
        Sorted s = sort(reaches(planted));
        String r = prefix + "Reach -> JudgingRules.";
        assertEquals(List.of(), s.unsorted());
        assertEquals(Map.of(r + "legendPureWords()", 3, r + "LITE", 2,
                r + "ordinal()", 3, r + "values()", 1), s.pinned(), "behaviour and carrier reaches counted, the switch"
                + " map's merged into its top class; the data parameter and the constant dropped");
        assertEquals(Set.of(r + "legendPureWords()"), s.behaviour());
        assertTrue(reaches(planted).containsKey(r + "floatTolerance()") && reaches(planted).containsKey(
                r + "COMPILED_PURE_COMPAT"), "the data parameter and the constant are reached, then dropped by kind");
        assertEquals(Map.of(), reaches(planted.stream().filter(c -> c.getName().startsWith(prefix + "Switch")).toList()),
                "a pattern switch is no reach: the pool scan below is what sees it");

        Map<String, Set<String>> pools = new TreeMap<>();
        for (JavaClass c : planted) {
            try {
                pools.put(c.getName(), ConstantPools.utf8(ConstantPools.bytesOf(
                        Class.forName(c.getName(), false, getClass().getClassLoader()))));
            } catch (ClassNotFoundException e) {
                throw new AssertionError(e);
            }
        }
        Map<String, Set<String>> names = names(pools);
        assertEquals(Set.of(TYPE, "COMPILED_PURE_COMPAT", "LITE"), names.get(prefix + "Switch"),
                "a pattern switch's labels are in its pool");
        assertEquals(Set.of(TYPE, "COMPILED_PURE_COMPAT"), names.get(prefix + "ByClass"),
                "a class literal and a constant's name as a string are in its pool");
        assertEquals(Set.of("compiledPureCompat"), names.get(prefix + "Text"),
                "a token inside a longer string, in a class that never names the type, is found as a word");
        assertTrue(names.get(prefix + "Reach").containsAll(Set.of(TYPE, "legendPureWords",
                "LITE", "floatTolerance", "COMPILED_PURE_COMPAT")), "the reaching class's pool: " + names);
        assertEquals(null, kindOf("floatTolerance(int)"), "an overload is no sorted member");
    }
}
