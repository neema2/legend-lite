// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Constructor;
import java.lang.reflect.Executable;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
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

    /** Each open class's declared members, exact, as {@link #collect} reads them (modifiers and generic types; nested,
     *  anonymous and local classes; a static initializer; the number of lambda bodies). Measured 2026-11-17. */
    static final Map<String, Set<String>> MEMBERS = Map.ofEntries(
            // B8 (a ruled hook)
            Map.entry("com.legend.lowering.PureOnlyTexts", Set.of(
                    // + formatR, representationOf (2026-11-17, C5 step A merged from D's d-int3: Repr's two forms asked
                    // of the ruled hook, the edge answering -- edge.EdgePureOnlyTexts)
                    "public abstract com.legend.sql.SqlExpr formatR(com.legend.lowering.Styled, com.legend.compiler.spec.typed.TypedSpec, com.legend.sql.SqlExpr)",
                    "public abstract com.legend.sql.SqlExpr representationOf(com.legend.lowering.Styled, com.legend.compiler.element.type.Type, com.legend.sql.SqlExpr)",
                    "public abstract com.legend.sql.SqlExpr formatPadded(com.legend.sql.SqlExpr, com.legend.sql.SqlExpr$IntLit)",
                    "public abstract com.legend.sql.SqlExpr instanceText(com.legend.sql.SqlExpr)",
                    "public abstract com.legend.sql.SqlExpr representation(com.legend.sql.SqlExpr)")),
            // B17 (the listed facade of the relation text: ArchitectureTest.PLANNER_ENTRIES)
            Map.entry("com.legend.lowering.RelationTexts", Set.of(
                    // re-cut 2026-11-17 on d-stage c37207858: train 7's J26 (af4c03ba5c) and E-SQL's pforms (449c51793a)
                    // -- each facet method takes the planner's own Styled, pureToString takes it in place of the two
                    // facets, and the substitute's lambda went with J26; relationToString and toCsv lost their map of
                    // relation types 2026-11-18 with E-SQL's J40 (9822fd89c8: DeferredTdsString out of the product MIR,
                    // the edge's relation text typing its own) -- train 17
                    "public abstract com.legend.sql.SqlExpr csvProperty(com.legend.lowering.Styled, com.legend.compiler.spec.typed.TypedPropertyAccess, java.util.function.Function<com.legend.compiler.spec.typed.TypedSpec, com.legend.sql.SqlSelect>, java.lang.String)",
                    // (relationToString: gone 2026-11-24, stage B's B2b S5a -- a relation's toString is lite's own text,
                    // computed by the database on every plan style, lowering.ProductRelationText; a shrink)
                    "public abstract com.legend.sql.SqlExpr toCsv(com.legend.lowering.Styled, com.legend.compiler.spec.typed.TypedNativeCall, java.util.function.Function<com.legend.compiler.spec.typed.TypedSpec, com.legend.sql.SqlSelect>, java.lang.String)",
                    "public static com.legend.compiler.element.type.Type kindType(java.lang.String)",
                    "public static com.legend.sql.SqlExpr pureToString(com.legend.lowering.Styled, com.legend.compiler.element.type.Type, com.legend.sql.SqlExpr)")),
            // (com.legend.lowering.Repr: gone 2026-11-17, C5 step A -- legend-pure's representation moved to the
            // edge, edge.EdgePureOnlyTexts; its product parts the listed facade lowering.ReprParts)
            // B5 (a ruled hook)
            Map.entry("com.legend.lowering.ResultEnvelopes", Set.of(
                    // (values: deleted 2026-11-18, E-Java's b70d85ab9, train 18 -- a serialized root's string-entry value
                    // is the plan style's ExecutedValues two-form kind; tds and classes: replaced 2026-11-18 by one
                    // types-only method, L-B.TypedJsonResult, the orchestrator's ruling (A), E-Java's ef7ee2b69d)
                    "public abstract com.legend.sql.SqlExpr styled(com.legend.compiler.element.type.Type, java.lang.String, com.legend.lowering.EnvelopeParts)")),
            // B2
            Map.entry("com.legend.planner.UpstreamPlans", Set.of(
                    // planModel -> planRows (2026-11-17, J4/K3 by 0001a56387, D's train 4: the facet answers the
                    // handle's rows; its signature names no com.legend.plan type)
                    "public abstract java.util.Map<java.lang.String, java.util.List<java.util.List<java.lang.String>>> planRows(java.lang.String, com.legend.compiler.spec.typed.TypedNativeCall, java.lang.String, java.lang.String, java.util.List<java.lang.String>, com.legend.compiler.spec.SpecCompiler, com.legend.planner.PlanEnv)",
                    // (activitySql: gone 2026-11-18, J40.PlanStyle -- the two-form ActivityTexts, the edge's form EnginePlans.activitySql)
                    "public abstract java.lang.String ddlText(com.legend.compiler.spec.typed.TypedNativeCall, com.legend.planner.PlanEnv)",
                    "public abstract java.lang.String generatedTestDataPlan(com.legend.compiler.spec.typed.TypedTestDataGen, com.legend.compiler.element.ModelContext)",
                    "public abstract java.lang.String routineText(com.legend.builtin.NativeFn$JavaRoutine, com.legend.compiler.spec.NativeDispatch$RoutineCall, java.util.List<com.legend.compiler.spec.typed.TypedSpec>, com.legend.compiler.spec.SpecCompiler, com.legend.planner.PlanEnv)",
                    "public abstract java.util.List<java.lang.String> engineSeedSql(java.lang.String, com.legend.model.DatabaseDefinition, java.util.function.Function<java.lang.String, java.util.Optional<com.legend.model.DatabaseDefinition>>)",
                    "public abstract java.util.List<java.lang.String> engineSeedSqlFromRecords(java.util.List<java.util.List<java.lang.String>>, com.legend.model.DatabaseDefinition, java.util.function.Function<java.lang.String, java.util.Optional<com.legend.model.DatabaseDefinition>>)",
                    "public abstract java.util.function.UnaryOperator<com.legend.sql.SqlExpr> testDataTimestampText()")),
            // (B12, sql.dialect.DateTimePrintText: deleted 2026-11-17 with train 7's B12 close, a8b7e2e32e -- the edge's
            // PureDateTimePrint composes legend-pure's DateTime print)
            // C4 (a ruled hook)
            Map.entry("com.legend.testplan.EngineTestText", Set.of(
                    "public abstract boolean isRoot(java.lang.String)",
                    "public abstract boolean notAConstant(com.legend.testplan.HostSides$Check)",
                    "public abstract boolean reachesUpstreamText(java.lang.String, boolean, java.util.List<com.legend.compiler.spec.typed.TypedSpec>, java.util.List<com.legend.compiler.spec.typed.TypedSpec>, com.legend.compiler.spec.SpecCompiler)",
                    // (tryArm, tryArmDualGolden, tryArmSameSql and batchTexts, the database judge's asks: deleted,
                    // NARROWS A HOOK THE USER RULED -- A4 of the compatibility program, stage A ratified by the user
                    // 2026-11-19; moved down by the user's ruling of 2026-11-19 ~20:45 ("Approved to move guards
                    // down"))
                    "public abstract com.legend.testplan.TextPlanned plan(com.legend.testplan.TextAsk)",
                    // (J41's gate(Extension, Supplier), the edge's engine-text arm answering its own database gate:
                    // deleted, NARROWS A HOOK THE USER RULED -- A4 of the compatibility program, stage A ratified by
                    // the user 2026-11-19; moved down by the user's ruling of 2026-11-19 ~20:45 ("Approved to move
                    // guards down"))
                    "public abstract java.lang.String why(com.legend.testplan.HostSides$Check)")),
                    // (K10b's members: recording and siteWords, a fresh recorder of one body's planning call and the
                    // test door's one read of its raise-site words; sizeWords, a required side's no-rows refusal in
                    // legend-pure's words -- deleted, stage B's B1, 2026-11-20, errors as records: the run raises the refusal as its record; a down move, the user's standing approval and D2 of 2026-11-20 ~17:00)
            // C4 (the listed facade of the engine-text arm: ArchitectureTest.PLANNER_ENTRIES)
            // (EngineTextPlanning's active, defer, planSide, scalarRow, both textEquals, verdictBatch and why: the
            // edge's engine-text rows under the database judge, deleted with its appeal path -- A4 of the compatibility
            // program, stage A ratified by the user 2026-11-19; moved down by the user's ruling of 2026-11-19 ~20:45
            // ("Approved to move guards down"))
            Map.entry("com.legend.testplan.EngineTextPlanning", Set.of(
                    // judgingRules replaced by style 2026-11-18 (E-Java's c732a61c4, train 18: the appeal asked of the
                    // style, the judging rules deleted whole, 070c4ea3e)
                    "public static com.legend.database.PlanStyle style(com.legend.testplan.BodyDriver$ExecEnv)",
                    // (notAConstantError: deleted 2026-11-18 by E-Java's train-17 item 5, 645c3e4464 -- NotAConstant is the
                    // edge's, raised by its collector)
                    "private <init>()",
                    // 11 forwarders gone with train 16 b836ff636f (2026-11-18, the GUARD stream's re-pin): F's J40 census
                    // (boundContext, sqlTextInputs, query, mapping, dialect, runtime) and E-Java's J40 (replayFacts,
                    // noReplayGates, extentSubset, ordered, sortKeys) -- the referee's gates and the SQL-text inputs are
                    // the edge's
                    "public static boolean values(com.legend.testplan.JudgePlans$PlannedSide)",
                    "public static com.legend.compiler.element.ModelContext ctx(com.legend.testplan.BodyDriver$ExecEnv)",
                    "public static com.legend.compiler.spec.SpecCompiler specs(com.legend.testplan.TextAsk)",
                    "public static com.legend.compiler.spec.typed.ExecutionContext frame(com.legend.testplan.BodyDriver$ExecEnv)",
                    "public static com.legend.compiler.spec.typed.TypedSpec bare(com.legend.testplan.TextAsk)",
                    "public static com.legend.compiler.spec.typed.TypedSpec splice(com.legend.testplan.AssertVerdicts$SpliceHook, com.legend.compiler.spec.typed.TypedSpec, java.util.Set<java.lang.String>)",
                    "public static com.legend.compiler.spec.typed.TypedSpec unpagedRead(com.legend.compiler.spec.typed.TypedSpec)",
                    "public static com.legend.executionplan.ExecutionPlan$Target target(com.legend.testplan.BodyDriver$ExecEnv)",
                    "public static com.legend.testexecutionplan.EnumFacts enumsOf(com.legend.compiler.element.ModelContext, java.lang.String, java.lang.String, com.legend.testexecutionplan.SideShape)",
                    "public static com.legend.testexecutionplan.HostJudgment judgment(com.legend.testplan.HostSides$Check)",
                    "public static com.legend.testexecutionplan.SideShape shapeOf(com.legend.testplan.JudgePlans$PlannedSide, com.legend.database.PlanStyle)",
                    "public static com.legend.testexecutionplan.TestPlan$Side side(com.legend.testplan.JudgePlans$PlannedSide)",
                    "public static com.legend.testexecutionplan.TypedSide evalSide(com.legend.compiler.spec.typed.TypedSpec, java.util.List<com.legend.compiler.spec.typed.TypedSpec>, com.legend.compiler.spec.SpecCompiler, com.legend.testplan.BodyDriver$ExecEnv, boolean, boolean, com.legend.testplan.AssertVerdicts$SpliceHook)",
                    "public static com.legend.testplan.AssertVerdicts$SpliceHook hook(com.legend.testplan.TextAsk)",
                    "public static com.legend.testplan.BodyDriver$ExecEnv appealOn(com.legend.testplan.BodyDriver$ExecEnv)",
                    "public static com.legend.testplan.BodyDriver$ExecEnv env(com.legend.testplan.TextAsk)",
                    "public static com.legend.testplan.BodyDriver$ExecEnv withCollector(com.legend.testplan.BodyDriver$ExecEnv, com.legend.testplan.TextCollector)",
                    "public static com.legend.testplan.BodyDriver$ExecEnv withFrame(com.legend.testplan.BodyDriver$ExecEnv, com.legend.compiler.spec.typed.ExecutionContext)",
                    "public static com.legend.testplan.BodyDriver$ExecEnv withOwnLets(com.legend.testplan.BodyDriver$ExecEnv)",
                    "public static com.legend.testplan.BodyDriver$ExecEnv withTableReplace(com.legend.testplan.BodyDriver$ExecEnv, java.util.Map<java.lang.String, java.lang.String>)",
                    "public static com.legend.testplan.HostSides$Ask ask(com.legend.compiler.spec.typed.TypedSpec, boolean, boolean, com.legend.testplan.AssertVerdicts$SpliceHook)",
                    "public static com.legend.testplan.HostSides$Ask ask(com.legend.compiler.spec.typed.TypedSpec, boolean, boolean, com.legend.testplan.AssertVerdicts$SpliceHook, com.legend.testplan.BodyDriver$ExecEnv)",
                    "public static com.legend.testplan.HostSides$Check check(java.util.List<com.legend.testplan.HostSides$Ask>, com.legend.testexecutionplan.HostJudgment)",
                    "public static com.legend.testplan.JudgePlans$PlannedSide plannedSide(com.legend.compiler.spec.typed.TypedSpec, java.util.List<com.legend.compiler.spec.typed.TypedSpec>, com.legend.compiler.spec.SpecCompiler, com.legend.testplan.BodyDriver$ExecEnv, boolean, boolean, com.legend.testplan.AssertVerdicts$SpliceHook)",
                    "public static com.legend.testplan.TextCollector collector(com.legend.testplan.BodyDriver$ExecEnv)",
                    "public static com.legend.testplan.TextPlanned planned(com.legend.testplan.HostSides$Check)",
                    "public static int appeal(com.legend.testplan.BodyDriver$ExecEnv, java.util.function.IntFunction<com.legend.testexecutionplan.Extension>)",
                    "public static java.lang.String calleeFqn(com.legend.compiler.spec.typed.TypedSpec)",
                    "public static java.lang.String inZone(java.lang.String, java.lang.String)",
                    "public static java.lang.String isoTimestamp(com.legend.values.PureDateLiteral)",
                    "public static java.lang.String marking(com.legend.testplan.BodyDriver$ExecEnv, java.lang.String, boolean)",
                    "public static java.util.List<com.legend.compiler.spec.typed.TypedSpec> letPrefix(com.legend.testplan.TextAsk)",
                    "public static java.util.Map<java.lang.String, java.lang.String> tableReplace(com.legend.testplan.BodyDriver$ExecEnv)",
                    "public static void restoreMarking(com.legend.testplan.BodyDriver$ExecEnv, java.lang.String)")),
            // C4 (the closed list of kinds)
            Map.entry("com.legend.testplan.PlanStyleFacets", Set.of(
                    "private <init>()",
                    "public static final java.util.List<java.lang.Class<?>> KINDS",
                    "public static final java.util.List<java.lang.Class<?>> TWO_FORMS",
                    "static {}")));

    /** The declared members of {@code c} as {@link #MEMBERS} spells them. */
    static Set<String> membersOf(Class<?> c) {
        Set<String> out = new TreeSet<>();
        collect(c, "", out);
        return out;
    }

    /**
     * What a class declares, each with its modifiers and generic types (a widened visibility, a changed field or return
     * type, changed type arguments each change its entry): its fields, its methods and constructors, its member classes
     * (recursively, behind their simple names and {@code #}), its anonymous and local classes (named by its own constant
     * pool's InnerClasses entries, which reflection does not list: {@code $1}, {@code $1Local}, recursively), its static
     * initializer ({@code static {}}, when its pool names {@code <clinit>}) and the number of its lambda bodies. What is
     * not read: the code inside an existing member or initializer -- the census's dependency and spelling scans read that.
     */
    private static void collect(Class<?> c, String prefix, Set<String> out) {
        for (Field f : c.getDeclaredFields()) {
            if (!f.isSynthetic()) {
                out.add(prefix + modifiers(f.getModifiers() & Modifier.fieldModifiers()) + f.getGenericType().getTypeName()
                        + " " + f.getName());
            }
        }
        int lambdas = 0;
        for (Method m : c.getDeclaredMethods()) {
            if (m.isSynthetic() && m.getName().startsWith("lambda$")) {
                lambdas++;
            } else if (!m.isSynthetic() && !m.isBridge()) {
                out.add(prefix + modifiers(m.getModifiers() & Modifier.methodModifiers())
                        + m.getGenericReturnType().getTypeName() + " " + signature(m.getName(), m));
            }
        }
        if (lambdas > 0) {
            out.add(prefix + "(lambda bodies) " + lambdas);
        }
        for (Constructor<?> k : c.getDeclaredConstructors()) {
            if (!k.isSynthetic()) {
                out.add(prefix + modifiers(k.getModifiers() & Modifier.constructorModifiers()) + signature("<init>", k));
            }
        }
        Set<String> pool = ConstantPools.utf8(ConstantPools.bytesOf(c));
        if (pool.contains("<clinit>")) {
            out.add(prefix + "static {}");
        }
        for (Class<?> n : c.getDeclaredClasses()) {
            if (!n.isSynthetic()) {
                out.add(prefix + modifiers(n.getModifiers() & Modifier.classModifiers()) + n.getSimpleName() + " (class)");
                collect(n, prefix + n.getSimpleName() + "#", out);
            }
        }
        String own = c.getName().replace('.', '/') + "$";
        for (String entry : pool) {
            if (entry.startsWith(own) && entry.substring(own.length()).matches("\\d+[A-Za-z0-9_]*")) {
                String local = entry.substring(own.length());
                out.add(prefix + "$" + local + " (anonymous or local class)");
                try {
                    collect(Class.forName(entry.replace('/', '.'), false, c.getClassLoader()), prefix + "$" + local + "#",
                            out);
                } catch (ClassNotFoundException e) {
                    throw new AssertionError(entry + ": named by " + c.getName() + "'s pool, not loadable", e);
                }
            }
        }
    }

    private static String modifiers(int m) {
        String s = Modifier.toString(m);
        return s.isEmpty() ? "" : s + " ";
    }

    private static String signature(String name, Executable e) {
        return name + Arrays.stream(e.getGenericParameterTypes()).map(java.lang.reflect.Type::getTypeName)
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
     *  facade, members listed, no #*"; widened 2026-11-17 to every owner): every row of
     *  {@code ArchitectureTest.PLANNER_ENTRIES} names one member by its full signature or field name (no whole-class key,
     *  however spelt), and every read any class of the edge's jar makes of the planners -- nested classes included --
     *  keys, through {@code ArchitectureTest.entryKey}, exactly as this test spells the member itself, and is listed for
     *  its caller. Red if a whole-class keying comes back. */
    @Test
    void theFacadesAreListedByMember() {
        // every row names one member: a method or constructor by its signature, a field by its name (2026-11-17, after
        // the merge of C5 step A: lowering.ReprParts is listed by member too, so the rule is every owner's, not two)
        List<String> wide = new ArrayList<>();
        for (String key : ArchitectureTest.PLANNER_ENTRIES.keySet()) {
            if (!key.matches("[\\w.$]+#(?:[\\w<>$]+\\(.*\\)|[A-Za-z_$][\\w$]*)") || key.endsWith("#*")) {
                wide.add(key);
            }
        }
        assertEquals(List.of(), wide, "a planner entry that names no one member by its signature or field name");
        List<String> unlisted = new ArrayList<>();
        int reads = 0;
        for (com.tngtech.archunit.core.domain.JavaClass c : importJars("legend.edge.jars")) {
            for (com.tngtech.archunit.core.domain.JavaAccess<?> a : c.getAccessesFromSelf()) {
                if (!ArchitectureTest.plannersPackage(a.getTargetOwner().getPackageName())) {
                    continue;
                }
                reads++;
                String own = a.getTargetOwner().getName().substring("com.legend.".length()) + "#" + a.getName();
                if (a.getTarget() instanceof com.tngtech.archunit.core.domain.AccessTarget.CodeUnitAccessTarget unit) {
                    own += unit.getRawParameterTypes().stream().map(com.tngtech.archunit.core.domain.JavaClass::getName)
                            .collect(Collectors.joining(", ", "(", ")"));
                }
                String key = ArchitectureTest.entryKey(a);
                if (!key.equals(own) || !ArchitectureTest.PLANNER_ENTRIES.getOrDefault(own, Set.of())
                        .contains(ArchitectureTest.callerOf(a))) {
                    unlisted.add(c.getName() + " -> " + own + " (keyed " + key + ")");
                }
            }
        }
        assertTrue(reads > 100, "the edge's reads of the planners are read, its nested classes' too: " + reads);
        assertEquals(List.of(), unlisted, "an edge read of the planners keyed other than by its member, or unlisted");
    }

    private static com.tngtech.archunit.core.domain.JavaClasses importJars(String property) {
        List<java.util.jar.JarFile> jars = new ArrayList<>();
        try {
            for (java.nio.file.Path jar : com.legend.testing.Runfile.listed(property)) {
                jars.add(new java.util.jar.JarFile(jar.toFile()));
            }
        } catch (java.io.IOException e) {
            throw new java.io.UncheckedIOException(e);
        }
        return new com.tngtech.archunit.core.importer.ClassFileImporter().importJars(jars);
    }

    /** A member added to a pinned class, a member gone, an open class with no set, and a set with no open class are each
     *  found; and the walk sees what a member set alone missed (the review of 6afb9a2d2d, 2026-11-17): a widened
     *  visibility, a static initializer, a field's generic type, a lambda body, an anonymous class and its method. */
    @Test
    void theGuardBites() {
        Set<String> real = membersOf(Planted.class);
        assertTrue(real.containsAll(Set.of("private final int held", "java.lang.String text(java.lang.String, int)",
                "static final Inner (class)", "Inner#final int value", "Inner#int value()", "Inner#<init>(int)")),
                "the members are read, nested ones included: " + real);
        Set<String> grown = membersOf(PlantedGrown.class);
        Set<String> added = new TreeSet<>(grown);
        added.removeAll(real);
        assertTrue(added.containsAll(Set.of("public java.lang.String text(java.lang.String, int)", "static {}",
                "static final java.util.List<java.lang.String> NAMES", "(lambda bodies) 1", "java.lang.Runnable later()",
                "$1 (anonymous or local class)", "$1#public java.lang.String toString()")), "what grew is read: " + added);
        Set<String> gone = new TreeSet<>(real);
        gone.removeAll(grown);
        assertEquals(Set.of("java.lang.String text(java.lang.String, int)"), gone, "the narrower text() is gone");
        Set<String> less = new TreeSet<>(real);
        less.remove("java.lang.String text(java.lang.String, int)");
        less.add("gone()");
        List<String> found = differences(Map.of("Planted", less, "Pinned.Only", Set.of()),
                Map.of("Planted", real, "Open.Only", Set.of()));
        assertEquals(List.of("Open.Only: an open class with no pinned member set []",
                "Pinned.Only: pinned, but no longer an open class (delete its set)",
                "Planted: + java.lang.String text(java.lang.String, int)", "Planted: - gone()"), found, "the pin bites");
    }

    /** {@link Planted} grown: text() widened to public, a static field and its initializer, a lambda, an anonymous
     *  class with a method of its own. */
    @SuppressWarnings("unused")
    private static final class PlantedGrown {
        static final java.util.List<String> NAMES = new java.util.ArrayList<>();

        static {
            NAMES.add("planted");
        }

        private final int held = 1;

        public String text(String s, int n) {
            return s.repeat(n);
        }

        Runnable later() {
            return () -> NAMES.clear();
        }

        Object anonymous() {
            return new Object() {
                @Override
                public String toString() {
                    return "planted";
                }
            };
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
