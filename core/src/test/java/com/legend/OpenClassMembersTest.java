// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Constructor;
import java.lang.reflect.Executable;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * AN OPEN CLASS CANNOT GROW UNSEEN (the GUARD stream, step 2's C.4; the edge audit of 2026-11-17, section C.4: "open
 * classes pinned by member set or access sites"). The census ({@link EdgeSeparationTest#COMPAT_CLASSES}) counts a
 * compatibility class standing in a product package ONCE, and each product class depending on it once: a member added
 * to it -- more legend-pure behaviour, or a product helper parked beside it -- moves no count. Here each open class's
 * declared members (its nested classes' too; synthetic and bridge members aside) are an exact set, by full signature:
 * a member added fails until it is listed with its dated reason, a member gone is deleted here, and a class leaving
 * {@code COMPAT_CLASSES} (moved or ruled) leaves this map with it.
 */
@Tag("guardrail")
class OpenClassMembersTest {

    /** Each open class's declared members, exact: a field by its name, a method or constructor by its name and its
     *  parameters' type names, a nested class's members behind its simple name and {@code #}. Measured 2026-11-17. */
    static final Map<String, Set<String>> MEMBERS = Map.ofEntries(
            // B8 (a ruled hook)
            Map.entry("com.legend.lowering.PureOnlyTexts", Set.of(
                    "formatPadded(com.legend.sql.SqlExpr, com.legend.sql.SqlExpr$IntLit)",
                    "instanceText(com.legend.sql.SqlExpr)",
                    "representation(com.legend.sql.SqlExpr)")),
            // B17 (the listed facade of the relation text: ArchitectureTest.PLANNER_ENTRIES)
            Map.entry("com.legend.lowering.RelationTexts", Set.of(
                    "csvProperty(com.legend.compiler.spec.typed.TypedPropertyAccess, java.util.function.Function, java.lang.String)",
                    "kindType(java.lang.String)",
                    "pureToString(com.legend.lowering.PureOnlyTexts, com.legend.lowering.ListTexts, com.legend.compiler.element.type.Type, com.legend.sql.SqlExpr)",
                    "relationToString(com.legend.compiler.spec.typed.TypedNativeCall, java.util.function.Function, java.lang.String, java.util.Map)",
                    "toCsv(com.legend.compiler.spec.typed.TypedNativeCall, java.util.function.Function, java.lang.String, java.util.Map)")),
            // C5
            Map.entry("com.legend.lowering.Repr", Set.of(
                    "<init>()",
                    "of(com.legend.lowering.Styled, com.legend.compiler.element.type.Type, com.legend.sql.SqlExpr)",
                    "of(com.legend.lowering.Styled, com.legend.compiler.spec.typed.TypedSpec, com.legend.sql.SqlExpr)")),
            // B5 (a ruled hook)
            Map.entry("com.legend.lowering.ResultEnvelopes", Set.of(
                    "classes(com.legend.sql.SqlExpr, java.lang.String)",
                    "tds(java.util.List, java.util.List, java.util.List, java.lang.String)",
                    "values(com.legend.sql.SqlExpr)")),
            // B2
            Map.entry("com.legend.planner.UpstreamPlans", Set.of(
                    "activitySql(java.util.List, java.lang.String, com.legend.compiler.spec.SpecCompiler, com.legend.planner.PlanEnv, boolean)",
                    "ddlText(com.legend.compiler.spec.typed.TypedNativeCall, com.legend.planner.PlanEnv)",
                    "engineSeedSql(java.lang.String, com.legend.model.DatabaseDefinition, java.util.function.Function)",
                    "engineSeedSqlFromRecords(java.util.List, com.legend.model.DatabaseDefinition, java.util.function.Function)",
                    "generatedTestDataPlan(com.legend.compiler.spec.typed.TypedTestDataGen, com.legend.compiler.element.ModelContext)",
                    "planModel(com.legend.compiler.spec.typed.TypedNativeCall, com.legend.compiler.spec.SpecCompiler, com.legend.planner.PlanEnv)",
                    "routineText(com.legend.builtin.NativeFn$JavaRoutine, com.legend.compiler.spec.NativeDispatch$RoutineCall, java.util.List, com.legend.compiler.spec.SpecCompiler, com.legend.planner.PlanEnv)",
                    "testDataTimestampText()")),
            // B12
            Map.entry("com.legend.sql.dialect.DateTimePrintText", Set.of(
                    "<init>()",
                    "Expansion (class)",
                    "Expansion#<init>()",
                    "Expansion#expr(com.legend.sql.SqlExpr)",
                    "of(com.legend.sql.SqlExpr)")),
            // C4 (a ruled hook)
            Map.entry("com.legend.testplan.EngineTestText", Set.of(
                    "batchTexts()",
                    "isRoot(java.lang.String)",
                    "notAConstant(com.legend.testplan.HostSides$Check)",
                    "plan(com.legend.testplan.TextAsk)",
                    "reachesUpstreamText(java.lang.String, boolean, java.util.List, java.util.List, com.legend.compiler.spec.SpecCompiler)",
                    "textsHeld(com.legend.testexecutionplan.Extension)",
                    "tryArm(java.lang.String, boolean, java.util.List, java.util.List, com.legend.compiler.spec.SpecCompiler, com.legend.testplan.BodyDriver$ExecEnv, com.legend.testplan.AssertVerdicts$SpliceHook)",
                    "tryArmDualGolden(com.legend.compiler.spec.typed.TypedUserCall, java.util.List, com.legend.compiler.spec.SpecCompiler, com.legend.testplan.BodyDriver$ExecEnv, com.legend.testplan.AssertVerdicts$SpliceHook)",
                    "tryArmSameSql(com.legend.compiler.spec.typed.TypedUserCall, java.util.List, com.legend.compiler.spec.SpecCompiler, com.legend.testplan.BodyDriver$ExecEnv, com.legend.testplan.AssertVerdicts$SpliceHook)",
                    "why(com.legend.testplan.HostSides$Check)")),
            // C4 (the listed facade of the engine-text arm: ArchitectureTest.PLANNER_ENTRIES)
            Map.entry("com.legend.testplan.EngineTextPlanning", Set.of(
                    "<init>()",
                    "active(com.legend.testplan.VerdictBatch)",
                    "appeal(com.legend.testplan.BodyDriver$ExecEnv, java.util.function.IntFunction)",
                    "appealOn(com.legend.testplan.BodyDriver$ExecEnv)",
                    "ask(com.legend.compiler.spec.typed.TypedSpec, boolean, boolean, com.legend.testplan.AssertVerdicts$SpliceHook)",
                    "ask(com.legend.compiler.spec.typed.TypedSpec, boolean, boolean, com.legend.testplan.AssertVerdicts$SpliceHook, com.legend.testplan.BodyDriver$ExecEnv)",
                    "bare(com.legend.testplan.TextAsk)",
                    "boundContext(com.legend.compiler.spec.typed.TypedSpec, java.util.List, com.legend.compiler.spec.SpecCompiler)",
                    "calleeFqn(com.legend.compiler.spec.typed.TypedSpec)",
                    "check(java.util.List, com.legend.testexecutionplan.HostJudgment)",
                    "collector(com.legend.testplan.BodyDriver$ExecEnv)",
                    "ctx(com.legend.testplan.BodyDriver$ExecEnv)",
                    "defer(com.legend.testplan.VerdictBatch, java.lang.String, boolean, com.legend.sql.SqlQuery, com.legend.executionplan.ExecutionPlan$Target)",
                    "defer(com.legend.testplan.VerdictBatch, java.lang.String, boolean, com.legend.sql.SqlQuery, com.legend.executionplan.ExecutionPlan$Target, int, java.lang.String)",
                    "dialect(com.legend.planner.SqlTextInputs)",
                    "enumsOf(com.legend.compiler.element.ModelContext, java.lang.String, java.lang.String, com.legend.testexecutionplan.SideShape)",
                    "env(com.legend.testplan.TextAsk)",
                    "evalSide(com.legend.compiler.spec.typed.TypedSpec, java.util.List, com.legend.compiler.spec.SpecCompiler, com.legend.testplan.BodyDriver$ExecEnv, boolean, boolean, com.legend.testplan.AssertVerdicts$SpliceHook)",
                    "extentSubset(com.legend.testplan.AssertVerdicts$ReplayGates)",
                    "frame(com.legend.testplan.BodyDriver$ExecEnv)",
                    "hook(com.legend.testplan.TextAsk)",
                    "inZone(java.lang.String, java.lang.String)",
                    "isoTimestamp(com.legend.values.PureDateLiteral)",
                    "judgingRules(com.legend.testplan.BodyDriver$ExecEnv)",
                    "judgment(com.legend.testplan.HostSides$Check)",
                    "letPrefix(com.legend.testplan.TextAsk)",
                    "mapping(com.legend.planner.SqlTextInputs)",
                    "marking(com.legend.testplan.BodyDriver$ExecEnv, java.lang.String, boolean)",
                    "noReplayGates()",
                    "notAConstantError(java.lang.String)",
                    "ordered(com.legend.testplan.AssertVerdicts$ReplayGates)",
                    "planSide(com.legend.compiler.spec.typed.TypedSpec, java.util.List, com.legend.compiler.spec.SpecCompiler, com.legend.testplan.BodyDriver$ExecEnv, com.legend.testplan.AssertVerdicts$SpliceHook)",
                    "planned(com.legend.testplan.HostSides$Check)",
                    "plannedSide(com.legend.compiler.spec.typed.TypedSpec, java.util.List, com.legend.compiler.spec.SpecCompiler, com.legend.testplan.BodyDriver$ExecEnv, boolean, boolean, com.legend.testplan.AssertVerdicts$SpliceHook)",
                    "query(com.legend.planner.SqlTextInputs)",
                    "replayFacts(com.legend.compiler.spec.typed.TypedSpec, java.util.List)",
                    "restoreMarking(com.legend.testplan.BodyDriver$ExecEnv, java.lang.String)",
                    "runtime(com.legend.planner.SqlTextInputs)",
                    "scalarRow(com.legend.testplan.DatabaseJudge$SideRows)",
                    "shapeOf(com.legend.testplan.JudgePlans$PlannedSide, com.legend.database.PlanStyle)",
                    "side(com.legend.testplan.JudgePlans$PlannedSide)",
                    "sortKeys(com.legend.testplan.AssertVerdicts$ReplayGates)",
                    "specs(com.legend.testplan.TextAsk)",
                    "splice(com.legend.testplan.AssertVerdicts$SpliceHook, com.legend.compiler.spec.typed.TypedSpec, java.util.Set)",
                    "sqlTextInputs(com.legend.compiler.spec.NativeDispatch$RoutineCall, java.util.List)",
                    "tableReplace(com.legend.testplan.BodyDriver$ExecEnv)",
                    "target(com.legend.testplan.BodyDriver$ExecEnv)",
                    "textEquals(java.lang.String, com.legend.sql.SqlQuery)",
                    "textEquals(java.lang.String, java.lang.String)",
                    "unpagedRead(com.legend.compiler.spec.typed.TypedSpec)",
                    "values(com.legend.testplan.JudgePlans$PlannedSide)",
                    "verdictBatch(com.legend.testplan.BodyDriver$ExecEnv)",
                    "why(com.legend.testplan.DatabaseJudge$SideRows)",
                    "withCollector(com.legend.testplan.BodyDriver$ExecEnv, com.legend.testplan.TextCollector)",
                    "withFrame(com.legend.testplan.BodyDriver$ExecEnv, com.legend.compiler.spec.typed.ExecutionContext)",
                    "withOwnLets(com.legend.testplan.BodyDriver$ExecEnv)",
                    "withTableReplace(com.legend.testplan.BodyDriver$ExecEnv, java.util.Map)")),
            // C4 (the closed list of kinds)
            Map.entry("com.legend.testplan.PlanStyleFacets", Set.of(
                    "<init>()",
                    "KINDS",
                    "TWO_FORMS")));

    /** The declared members of {@code c} as {@link #MEMBERS} spells them. */
    static Set<String> membersOf(Class<?> c) {
        Set<String> out = new TreeSet<>();
        collect(c, "", out);
        return out;
    }

    private static void collect(Class<?> c, String prefix, Set<String> out) {
        for (Field f : c.getDeclaredFields()) {
            if (!f.isSynthetic()) {
                out.add(prefix + f.getName());
            }
        }
        for (Method m : c.getDeclaredMethods()) {
            if (!m.isSynthetic() && !m.isBridge()) {
                out.add(prefix + signature(m.getName(), m));
            }
        }
        for (Constructor<?> k : c.getDeclaredConstructors()) {
            if (!k.isSynthetic()) {
                out.add(prefix + signature("<init>", k));
            }
        }
        for (Class<?> n : c.getDeclaredClasses()) {
            if (!n.isSynthetic()) {
                out.add(prefix + n.getSimpleName() + " (class)");
                collect(n, prefix + n.getSimpleName() + "#", out);
            }
        }
    }

    private static String signature(String name, Executable e) {
        return name + Arrays.stream(e.getParameterTypes()).map(Class::getTypeName)
                .collect(Collectors.joining(", ", "(", ")"));
    }

    /** What differs between {@code pinned} and the members {@code actual} holds, per class: "+ member" added, "- member"
     *  gone, and a class on one side only. */
    static List<String> differences(Map<String, Set<String>> pinned, Map<String, Set<String>> actual) {
        List<String> out = new ArrayList<>();
        Set<String> classes = new TreeSet<>(pinned.keySet());
        classes.addAll(actual.keySet());
        for (String c : classes) {
            if (!pinned.containsKey(c)) {
                out.add(c + ": an open class with no pinned member set " + actual.get(c));
                continue;
            }
            if (!actual.containsKey(c)) {
                out.add(c + ": pinned, but no longer an open class (delete its set)");
                continue;
            }
            for (String m : actual.get(c)) {
                if (!pinned.get(c).contains(m)) {
                    out.add(c + ": + " + m);
                }
            }
            for (String m : pinned.get(c)) {
                if (!actual.get(c).contains(m)) {
                    out.add(c + ": - " + m);
                }
            }
        }
        return out;
    }

    private static Map<String, Set<String>> openClassesMembers() {
        Map<String, Set<String>> out = new TreeMap<>();
        for (String name : EdgeSeparationTest.COMPAT_CLASSES.keySet()) {
            try {
                out.put(name, membersOf(Class.forName(name, false, OpenClassMembersTest.class.getClassLoader())));
            } catch (ClassNotFoundException e) {
                throw new AssertionError(name + ": an open class of the census that does not exist", e);
            }
        }
        return out;
    }

    @Test
    void eachOpenClassHoldsItsPinnedMembersExactly() {
        assertEquals(List.of(), differences(MEMBERS, openClassesMembers()), "an open compatibility class's members"
                + " differ from the pinned set: a member added is listed here with its dated reason (or it does not"
                + " belong in an open class); a member gone is deleted here");
    }

    /** THE FACADES LIST THEIR MEMBERS (C.4's second half, 2026-11-17: the orchestrator's step-A ruling, "one named
     *  facade, members listed, no #*"): no row of {@code ArchitectureTest.PLANNER_ENTRIES} admits a whole class, and
     *  each read the edge's real classes make of the two facades keys by its full signature, listed for its caller --
     *  so a member the edge newly reaches is a row before it passes. Red if a "#*" key comes back. */
    @Test
    void theFacadesAreListedByMember() {
        assertEquals(List.of(), ArchitectureTest.PLANNER_ENTRIES.keySet().stream().filter(k -> k.endsWith("#*"))
                .toList(), "a planner entry admits a whole class");
        Set<String> facades = Set.of("com.legend.testplan.EngineTextPlanning", "com.legend.lowering.RelationTexts");
        List<String> unlisted = new ArrayList<>();
        int reads = 0;
        List<Class<?>> edgeReaders = new ArrayList<>();
        for (String n : List.of("SqlTextVerdicts", "EdgeEngineTestText", "RelationTextLowering", "PureListTexts")) {
            try {
                edgeReaders.add(Class.forName("com.legend.edge." + n, false, OpenClassMembersTest.class.getClassLoader()));
            } catch (ClassNotFoundException e) {
                throw new AssertionError("the edge's facade reader " + n + " is gone: update this check", e);
            }
        }
        for (com.tngtech.archunit.core.domain.JavaClass c : new com.tngtech.archunit.core.importer.ClassFileImporter()
                .importClasses(edgeReaders)) {
            for (com.tngtech.archunit.core.domain.JavaAccess<?> a : c.getAccessesFromSelf()) {
                if (facades.contains(a.getTargetOwner().getName())) {
                    reads++;
                    String key = ArchitectureTest.entryKey(a);
                    if (key.endsWith("#*") || !ArchitectureTest.PLANNER_ENTRIES.getOrDefault(key, Set.of())
                            .contains(ArchitectureTest.callerOf(a))) {
                        unlisted.add(c.getName() + " -> " + key);
                    }
                }
            }
        }
        assertTrue(reads > 50, "the edge's facade reads are read: " + reads);
        assertEquals(List.of(), unlisted, "an edge read of a facade keyed as a whole class, or unlisted");
    }

    /** A member added to a pinned class, a member gone, an open class with no set, and a set with no open class are each
     *  found. */
    @Test
    void theGuardBites() {
        Set<String> real = membersOf(Planted.class);
        assertEquals(Set.of("held", "text(java.lang.String, int)", "<init>()", "Inner (class)", "Inner#value()",
                "Inner#<init>(int)", "Inner#value"), real, "the members are read, nested ones included");
        Set<String> less = new TreeSet<>(real);
        less.remove("text(java.lang.String, int)");
        less.add("gone()");
        List<String> found = differences(Map.of("Planted", less, "Pinned.Only", Set.of()),
                Map.of("Planted", real, "Open.Only", Set.of()));
        assertEquals(List.of("Open.Only: an open class with no pinned member set []",
                "Pinned.Only: pinned, but no longer an open class (delete its set)",
                "Planted: + text(java.lang.String, int)", "Planted: - gone()"), found, "the pin bites");
    }

    /** A planted open class: a field, a method, its constructor and a nested class (synthetic members aside). */
    @SuppressWarnings("unused")
    private static final class Planted {
        private final int held = 1;

        String text(String s, int n) {
            return s.repeat(n);
        }

        static final class Inner {
            final int value;

            Inner(int value) {
                this.value = value;
            }

            int value() {
                return value;
            }
        }
    }
}
