// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend;

import com.legend.testexecutionplan.TestPlan.JudgingRules;
import com.tngtech.archunit.core.domain.JavaAccess;
import com.tngtech.archunit.core.domain.JavaClass;
import com.tngtech.archunit.core.domain.JavaClasses;
import com.tngtech.archunit.core.importer.ClassFileImporter;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Member;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * THE JUDGING RULES' TYPE, NOT ONLY ITS COMPATIBILITY CONSTANT (the GUARD stream, step 2's C.3; the edge audit of
 * 2026-11-17, section C.3: "the JudgingRules TYPE flagged, not just COMPILED_PURE_COMPAT"). The census
 * ({@link EdgeSeparationTest}) counts a product class that names {@code JudgingRules.COMPILED_PURE_COMPAT}; a class that
 * is handed the rules and asks them a BEHAVIOUR ({@code rules.integerDecimalByText()}) selects legend-pure's behaviour by
 * the same flag without naming it, and was counted nowhere. Here every member of {@link JudgingRules} is sorted, exactly,
 * into one kind (the orchestrator's rulings of 2026-11-17, 16:40 and 17:24, census C1):
 * <ul>
 *   <li>{@link Kind#DATA}: a data parameter the product's rules keep as a value (ruled (a)): free to read;</li>
 *   <li>{@link Kind#BEHAVIOUR}: legend-pure's behaviour chosen by the flag (ruled (b): it moves behind an edge hook):
 *   every product class that asks it is an open entry of census line C1, counted ({@link #BEHAVIOUR_OPEN});</li>
 *   <li>{@link Kind#CONSTANT}: {@code COMPILED_PURE_COMPAT}, counted by the census itself (not twice);</li>
 *   <li>{@link Kind#CARRIER}: the type carried and enumerated ({@code LITE}, {@code values}, an Enum method): every
 *   product reach is pinned by its count ({@link #CARRIER_PINNED}), so a new {@code rules == LITE} or
 *   {@code rules.ordinal()} -- the flag read without a fact -- fails until it is ruled.</li>
 * </ul>
 * A member added to the type fails until it is sorted here.
 */
@Tag("guardrail")
class JudgingRulesReachTest {

    enum Kind { DATA, BEHAVIOUR, CONSTANT, CARRIER }

    private static final String OWNER = JudgingRules.class.getName();

    /** Every declared member of the type (synthetic ones aside), with its kind: exact. */
    static final Map<String, Kind> MEMBERS = Map.ofEntries(
            Map.entry("LITE", Kind.CARRIER),
            Map.entry("COMPILED_PURE_COMPAT", Kind.CONSTANT),
            // the private field every fact reads: reached only by the type itself
            Map.entry("upstream", Kind.CARRIER),
            Map.entry("values", Kind.CARRIER),
            Map.entry("valueOf", Kind.CARRIER),
            // ruled (a), 2026-11-17 16:40: the data parameters that stay the product's
            Map.entry("floatTolerance", Kind.DATA),
            Map.entry("decimalScale", Kind.DATA),
            // ruled (b), 2026-11-17 16:40 and 17:24: behaviours that move behind an edge hook
            Map.entry("integerDecimalByText", Kind.BEHAVIOUR),
            Map.entry("renderedTextRuled", Kind.BEHAVIOUR),
            Map.entry("legendPureWords", Kind.BEHAVIOUR),
            Map.entry("edgeAppeals", Kind.BEHAVIOUR));

    /** BEHAVIOUR: "source class -> JudgingRules.member", each an open entry of census line C1 (counted by the census's
     *  {@code linesLeft}). */
    static final Map<String, String> BEHAVIOUR_OPEN = Map.ofEntries(
            // the host judge's words and its rendered-text rule (2026-11-17: found by this guard, counted nowhere before)
            Map.entry("com.legend.testexec.HostJudgments -> JudgingRules.legendPureWords", "C1 2026-11-17"),
            Map.entry("com.legend.testexec.HostJudgments -> JudgingRules.renderedTextRuled", "C1 2026-11-17"),
            Map.entry("com.legend.testexec.IsolatedRun -> JudgingRules.legendPureWords", "C1 2026-11-17"),
            Map.entry("com.legend.testplan.AssertVerdicts -> JudgingRules.renderedTextRuled", "C1 2026-11-17"),
            Map.entry("com.legend.testplan.DatabaseJudge -> JudgingRules.legendPureWords", "C1 2026-11-17"),
            Map.entry("com.legend.testplan.PlanningOptions -> JudgingRules.edgeAppeals", "C1 2026-11-17"));

    /** CARRIER: "source class -> JudgingRules.member" with the number of its reaches, pinned exactly. */
    static final Map<String, Integer> CARRIER_PINNED = Map.ofEntries(
            // the test plan's JSON (2026-11-17): a verdict group's rules spelled "lite"/"compiledPureCompat" by a switch
            // (LITE twice, ordinal three times through the switch maps, values once), an isolated step's by name/valueOf
            Map.entry("com.legend.testexecutionplan.TestPlanJson -> JudgingRules.LITE", 2),
            Map.entry("com.legend.testexecutionplan.TestPlanJson -> JudgingRules.name", 1),
            Map.entry("com.legend.testexecutionplan.TestPlanJson -> JudgingRules.ordinal", 3),
            Map.entry("com.legend.testexecutionplan.TestPlanJson -> JudgingRules.valueOf", 1),
            Map.entry("com.legend.testexecutionplan.TestPlanJson -> JudgingRules.values", 1),
            // the one selector (2026-11-17): engineCompat ? COMPILED_PURE_COMPAT : LITE, chosen upfront per run
            Map.entry("com.legend.testplan.PlanningOptions -> JudgingRules.LITE", 1));

    // ---- the scan ----------------------------------------------------------------------------------------------

    private static JavaClasses classes(String... properties) {
        List<java.util.jar.JarFile> jars = new ArrayList<>();
        try {
            for (String property : properties) {
                for (java.nio.file.Path jar : com.legend.testing.Runfile.listed(property)) {
                    jars.add(new java.util.jar.JarFile(jar.toFile()));
                }
            }
        } catch (java.io.IOException e) {
            throw new java.io.UncheckedIOException(e);
        }
        return new ClassFileImporter().importJars(jars);
    }

    private static String top(String n) {
        int dollar = n.indexOf('$');
        return dollar < 0 ? n : n.substring(0, dollar);
    }

    /** The member name an access to the type reaches, or null when it reaches another type. */
    private static String memberOf(JavaAccess<?> a) {
        return a.getTargetOwner().getName().equals(OWNER) ? a.getTarget().getName() : null;
    }

    /** Every reach of the type's members from {@code classes} (the edge and the type itself aside), "source -> member"
     *  with its count. */
    static Map<String, Integer> reaches(JavaClasses classes) {
        Map<String, Integer> out = new TreeMap<>();
        for (JavaClass c : classes) {
            String n = c.getName();
            if (n.startsWith("com.legend.edge.") || n.equals(OWNER) || n.startsWith(OWNER + "$")) {
                continue;
            }
            Stream.of(c.getFieldAccessesFromSelf(), c.getMethodCallsFromSelf(), c.getConstructorCallsFromSelf(),
                    c.getMethodReferencesFromSelf(), c.getConstructorReferencesFromSelf())
                    .flatMap(Set::stream)
                    .forEach(a -> {
                        String m = memberOf(a);
                        if (m != null) {
                            out.merge(top(n) + " -> JudgingRules." + m, 1, Integer::sum);
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

    /** The methods every enum inherits: reached on the type, they carry or enumerate it. */
    static final Set<String> ENUM_METHODS = Set.of("name", "ordinal", "compareTo", "equals", "hashCode", "toString",
            "getDeclaringClass", "describeConstable");

    private static Map<String, Integer> productReaches() {
        return reaches(classes("legend.product.jars", "legend.authored.jars"));
    }

    // ---- the tests ---------------------------------------------------------------------------------------------

    /** The type's declared members are exactly the sorted ones: a member added is sorted here with its ruling. */
    @Test
    void everyMemberIsSorted() {
        Set<String> declared = new TreeSet<>();
        Stream.<Member>concat(Stream.of(JudgingRules.class.getDeclaredFields()),
                        Stream.of(JudgingRules.class.getDeclaredMethods()))
                .filter(m -> !m.isSynthetic())
                .forEach(m -> declared.add(m.getName()));
        assertEquals(new TreeSet<>(MEMBERS.keySet()), declared, "JudgingRules' members and their kinds differ: a new"
                + " member is sorted DATA, BEHAVIOUR or CARRIER here, with its ruling");
    }

    @Test
    void theProductsReachesAreTheSortedOnes() {
        Map<String, Integer> found = productReaches();
        Set<String> behaviour = new TreeSet<>();
        Map<String, Integer> carrier = new TreeMap<>();
        List<String> unsorted = new ArrayList<>();
        found.forEach((reach, n) -> {
            Kind k = kindOf(reach.substring(reach.indexOf("JudgingRules.") + "JudgingRules.".length()));
            if (k == null) {
                unsorted.add(reach);
            } else if (k == Kind.BEHAVIOUR) {
                behaviour.add(reach);
            } else if (k == Kind.CARRIER) {
                carrier.put(reach, n);
            }
        });
        assertEquals(List.of(), unsorted, "a reach of an unsorted member of JudgingRules");
        assertEquals(new TreeSet<>(BEHAVIOUR_OPEN.keySet()), behaviour, "BEHAVIOUR: a product class asks the judging"
                + " rules for legend-pure's behaviour outside its counted entry (a new one is a regression; a gone one"
                + " closes, delete its entry)");
        assertEquals(new TreeMap<>(CARRIER_PINNED), carrier, "CARRIER: the judging rules carried or enumerated"
                + " differently (a reach added reads the flag without a fact: rule it; a reach gone: lower its pin)");
    }

    /** A planted product class that asks a behaviour, takes one by method reference, reads the flag as {@code != LITE}
     *  and as an ordinal, and reads a data parameter: each reach but the data one is found, sorted by its kind. */
    @Test
    void theGuardBites() {
        Map<String, Integer> found = reaches(new ClassFileImporter().importClasses(
                com.legend.testexec.PlantedRulesReach.class));
        String p = "com.legend.testexec.PlantedRulesReach -> JudgingRules.";
        assertEquals(Map.of(p + "integerDecimalByText", 1, p + "legendPureWords", 1, p + "LITE", 1, p + "ordinal", 1,
                p + "floatTolerance", 1), found, "the scan sees a call, a method reference, a constant and an Enum method");
        assertEquals(Kind.BEHAVIOUR, kindOf("integerDecimalByText"));
        assertEquals(Kind.BEHAVIOUR, kindOf("legendPureWords"));
        assertEquals(Kind.CARRIER, kindOf("LITE"));
        assertEquals(Kind.CARRIER, kindOf("ordinal"));
        assertEquals(Kind.DATA, kindOf("floatTolerance"));
        assertEquals(null, kindOf("aMemberAddedLater"), "an unsorted member is no kind");
    }

    /** Each behaviour entry names its census line and date. */
    @Test
    void everyBehaviourEntryIsDated() {
        BEHAVIOUR_OPEN.forEach((e, line) -> assertTrue(line.matches("C1 \\d{4}-\\d{2}-\\d{2}.*"), e + ": " + line));
    }
}
