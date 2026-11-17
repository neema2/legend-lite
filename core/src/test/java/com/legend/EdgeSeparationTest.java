// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend;

import com.tngtech.archunit.core.domain.JavaClass;
import com.tngtech.archunit.core.domain.JavaClasses;
import com.tngtech.archunit.core.importer.ClassFileImporter;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * THE EDGE IS SEPARATED, PROVABLY (the user, 2026-11-17: "Focus on the total and provable edge separation first";
 * the separation census, docs/records/f-separation-census-2026-11-17.md). Three umbrella guards, one per finish-line
 * item, each with a SHRINK-ONLY allowlist that is exactly the census's open lines, each entry naming its line:
 * <ol>
 *   <li>BUILD ({@link #theBuildReachesNoEdgeButItsOpenLines}): no product target reaches an edge target
 *   (the direct dependents of {@code //core:edge}, {@code :edge_server}, {@code :edge_service_tests} and
 *   {@code :edge_store_words} within the product's roots' closure, a genquery: a target reaching the edge transitively
 *   reaches it through one of them);</li>
 *   <li>CODE ({@link #theProductsCodeNamesNoCompatibilityButItsOpenLines}): no product class depends on an edge class,
 *   a compatibility class standing in a product package, a SqlFn edge-only node, the judges' compatibility mode or the
 *   referee and the appeals;</li>
 *   <li>TEST SIDE ({@link #theTestSideNamesNoCompatibilityButItsOpenLines}): the same over the test infrastructure
 *   outside the edge.</li>
 * </ol>
 * A census line is CLOSED when its entries are deleted and the guard stays green; reintroducing the dependency turns
 * it red ({@link #eachGuardBites}). "Lines left" is the number of allowlist entries ({@link #linesLeft}); separation is
 * DONE when all three are empty.
 *
 * <p>LIMIT, named: a text spelling ({@code "TDSNull"}, {@code "+0000"}, legend-engine's sentences) is a string
 * constant javac inlines, invisible to a dependency graph; its guard is {@link CompatSpellingGuardTest}'s source scan
 * (shrink-only, every row stepped), which this umbrella cites rather than repeats.
 */
@Tag("guardrail")
class EdgeSeparationTest {

    // ---- the vocabulary ---------------------------------------------------------------------------------------

    /** The compatibility classes that stand in product packages today (census B and C lines): a class here is itself an
     *  open line until it moves to the edge or is ruled; a product class depending on one is an entry too. */
    static final Map<String, String> COMPAT_CLASSES = Map.ofEntries(
            Map.entry("com.legend.planner.UpstreamPlans", "B2"),
            // B17 (2026-11-17): Render's relation text moved to the edge (edge.RelationTextLowering); what stays in
            // Render is the product's row helpers, and the facet the edge answers stands here until hooks are ruled
            Map.entry("com.legend.lowering.RelationTexts", "B17"),
            // B9 -> C5 (E-P5) 2026-11-17: the judges' canon's alone (LiteralSpelling.temporalCanon)
            Map.entry("com.legend.sql.dialect.DateTimePrintText", "B12"),
            Map.entry("com.legend.lowering.ResultEnvelopes", "B5"),
            Map.entry("com.legend.lowering.PureOnlyTexts", "B8"),
            Map.entry("com.legend.lowering.Repr", "C5"),
            Map.entry("com.legend.testplan.EngineTestText", "C4"),
            Map.entry("com.legend.testplan.EngineTextPlanning", "C4"),
            Map.entry("com.legend.testplan.PlanStyleFacets", "C4"));

    /** The SqlFn members only the edge spells (census B8). */
    static final Set<String> EDGE_ONLY_SQLFN = Set.of("DATETIME_PRINT");

    /** The referee and the appeals in the product packages they stood in (census C2, closed 2026-11-17: moved to
     *  com.legend.edge, whose classes the CODE guard already keeps out of the product): a tripwire, should one return. */
    static final Set<String> REFEREE = Set.of("com.legend.testexec.SqlReplayReferee", "com.legend.testexec.Appeals",
            "com.legend.testexecutionplan.Appeal", "com.legend.testexecutionplan.PlannedAppeals",
            "com.legend.testplan.AppealsByVerdict", "com.legend.testplan.EngineTextAppeals");

    /** The test infrastructure's packages (the TEST SIDE guard's); every other product package is the CODE guard's. */
    static final List<String> TEST_SIDE = List.of("com.legend.testplan", "com.legend.testexec",
            "com.legend.testexecutionplan", "com.legend.test");

    // ---- the allowlists: exactly the census's open lines --------------------------------------------------------

    /** CODE: "source class -> target", each entry its census line and date. */
    static final Map<String, String> CODE_OPEN = Map.ofEntries(
            // B8 -> B12 2026-11-17 (census B9, the orchestrator's ruling): format's %r arm keeps legend-pure's DateTime
            // print form until it is refused in the product by B12's 2b (F's; the user's §10 item 8)
            Map.entry("com.legend.lowering.Scalars -> SqlFn.DATETIME_PRINT",
                    "B12 2026-11-17 (format %r arm: refused in the product by B12 2b, the user's §10 item 8)"),
            // B9 -> C5 (E-P5) 2026-11-17 (the orchestrator's ruling (a)): the composition the judges' canon alone emits
            // (LiteralSpelling.temporalCanon), closed by C5's step C
            Map.entry("com.legend.sql.dialect.AnsiSqlRenderer -> com.legend.sql.dialect.DateTimePrintText",
                    "B12 2026-11-17 (format %r arm, LiteralSpelling.temporalCanon's gone by C5 step C)"),
            Map.entry("com.legend.sql.dialect.DateTimePrintText -> (stands in a product package)",
                    "B12 2026-11-17 (format %r arm, LiteralSpelling.temporalCanon's gone by C5 step C)"),
            Map.entry("com.legend.lowering.Repr -> (stands in a product package)", "B12 2026-11-17"),
            // planner.Boundary -> com.legend.edge.HostAnswers, PureText, PureV1Routes: gone 2026-11-17 (A1, E-P5): the
            // pure/v1 route and execute's halves moved to com.legend.edge.wasm.EdgeBoundary (//wasm:edge_boundary)
            // B7 CLOSED 2026-11-17 (E-P5, D's split by content): server/PureV1Api is the product's own API kit,
            // server/LiteV1Api in //core:lite_v1 (lite's routes, answer and refusal kinds, LiteV1Api.RefusalKind);
            // legend-engine's ExecuteInput binding moved to the edge (edge.ExecuteInputs); its 7 lines left with it
            // lowering.VerdictSql -> lowering.Repr: gone 2026-11-17 (C5 step A, the PureOnlyTexts facet's representation)
            Map.entry("com.legend.lowering.Scalars -> com.legend.lowering.Repr", "B12 2026-11-17"));

    /** TEST SIDE: the same. */
    static final Map<String, String> TEST_OPEN = Map.ofEntries(
            Map.entry("com.legend.testexec.Equality -> JudgingRules.COMPILED_PURE_COMPAT", "C1 2026-11-17"),
            Map.entry("com.legend.testexecutionplan.TestPlan -> JudgingRules.COMPILED_PURE_COMPAT", "C1 2026-11-17"),
            Map.entry("com.legend.testexecutionplan.TestPlanJson -> JudgingRules.COMPILED_PURE_COMPAT", "C1 2026-11-17"),
            // (AgreementAsserts -> EngineTextAppeals closed: 2026-11-17, (b) of the orchestrator's order: the fused agreement path deleted)
            Map.entry("com.legend.testplan.PlanningOptions -> JudgingRules.COMPILED_PURE_COMPAT", "C1 2026-11-17"));
            // (census C2 CLOSED 2026-11-17: the referee and the appeals moved to com.legend.edge -- the run hook EdgeRun
            // in the referee's slot, the planner holding the edge's records by number -- its 30 entries deleted)

    /** HOOKS ARE TERMINAL (the orchestrator's ruling of 2026-11-17, on the user's "the edge's typed extension point"):
     *  a product hook that is (a) an interface of signatures only, (b) refused by the product's style by name, (c)
     *  implemented in the edge alone and (d) pinned (HookRulingTest) is the separation, not a leak -- allowed, and
     *  not counted. The hooks that fail a criterion stay open in the maps above, the criterion named here:
     *  UpstreamPlans (B2) met (a) 2026-11-17 (its routine table moved to planner.PlanTextRoutines) and is ruled below;
     *  RelationTexts (B17) meets (a) under the facade rule since 2026-11-17 (its refusal moved to TextRefusals; its
     *  forwarders reach product helpers alone) and is ruled below;
     *  EngineTestText (C4) fails (a): its static gates (armFor, reachesUpstreamText, marking); EngineTextPlanning (C4)
     *  EngineTestText (C4) meets (a) since 2026-11-17 (its gates the edge's, its records top-level) and
     *  EngineTextPlanning is the listed facade, forwarding alone -- both ruled below. PlanStyleFacets meets (a) (the orchestrator's ruling of 2026-11-17: a
     *  final class holding only the closed list of kind types, the list (d) lives on); its edges to the kinds stay
     *  counted under each kind's own line until that kind is ruled. */
    static final Map<String, String> HOOKS_RULED = Map.ofEntries(
            // B1, B3: CLOSED 2026-11-17 (Phase 6): EngineTextDialect is com.legend.edge's, PlanStyle names no engine
            // text; their two entries gone
            // B2, 2026-11-17 (Phase 6): UpstreamPlans, signatures only since its routine table moved to
            // planner.PlanTextRoutines -- the interface, and the product's code that only asks the style for it
            Map.entry("com.legend.planner.UpstreamPlans -> (stands in a product package)", "B2 HOOK-RULED 2026-11-17"),
            Map.entry("com.legend.planner.BodyPlanner -> com.legend.planner.UpstreamPlans", "B2 HOOK-RULED 2026-11-17"),
            Map.entry("com.legend.planner.PlanAllocations -> com.legend.planner.UpstreamPlans",
                    "B2 HOOK-RULED 2026-11-17"),
            Map.entry("com.legend.planner.PlanTextRoutines -> com.legend.planner.UpstreamPlans",
                    "B2 HOOK-RULED 2026-11-17"),
            Map.entry("com.legend.testplan.BodyDriver -> com.legend.planner.UpstreamPlans", "B2 HOOK-RULED 2026-11-17"),
            Map.entry("com.legend.testplan.PlanStyleFacets -> com.legend.planner.UpstreamPlans",
                    "B2 HOOK-RULED 2026-11-17"),
            Map.entry("com.legend.testplan.Seeds -> com.legend.planner.UpstreamPlans", "B2 HOOK-RULED 2026-11-17"),
            // SeedSqlForms: the test planner's arm of legend-engine's setUpDataSQLs (toDDL.pure): it reads the call's
            // arguments and asks the hook for the statements' text, holding none itself
            Map.entry("com.legend.testplan.SeedSqlForms -> com.legend.planner.UpstreamPlans", "B2 HOOK-RULED 2026-11-17"),
            // TestPlanner: asks the hook for the test-data generator's timestamp text when one is printed (B13)
            Map.entry("com.legend.testplan.TestPlanner -> com.legend.planner.UpstreamPlans", "B2 HOOK-RULED 2026-11-17"),
            // B5, 2026-11-17 (Phase 6): legend-engine's result envelope, a hook from birth -- the interface, and the
            // product's code that only asks the style for it with the call's refusal
            Map.entry("com.legend.lowering.ResultEnvelopes -> (stands in a product package)", "B5 HOOK-RULED 2026-11-17"),
            Map.entry("com.legend.lowering.Lowerer -> com.legend.lowering.ResultEnvelopes", "B5 HOOK-RULED 2026-11-17"),
            Map.entry("com.legend.lowering.JsonEmission -> com.legend.lowering.ResultEnvelopes",
                    "B5 HOOK-RULED 2026-11-17"),
            Map.entry("com.legend.planner.PlanEnv -> com.legend.lowering.ResultEnvelopes", "B5 HOOK-RULED 2026-11-17"),
            Map.entry("com.legend.testplan.PlanStyleFacets -> com.legend.lowering.ResultEnvelopes",
                    "B5 HOOK-RULED 2026-11-17"),
            Map.entry("com.legend.testplan.PlanStyleFacets -> (stands in a product package)",
                    "C4 HOOK-RULED 2026-11-17"),
            // B8 (i), 2026-11-17: the texts only legend-pure writes, a hook from birth -- the interface, and the
            // product's code that only asks the style for it with the call's refusal (the lowering, its rules, the
            // planner's configuration, the facet list)
            Map.entry("com.legend.lowering.PureOnlyTexts -> (stands in a product package)", "B8 HOOK-RULED 2026-11-17"),
            Map.entry("com.legend.lowering.Scalars -> com.legend.lowering.PureOnlyTexts", "B8 HOOK-RULED 2026-11-17"),
            // B17, the relation-text facet (the orchestrator's ruling of 2026-11-17: its signatures, and forwarders to
            // product helpers the product uses itself; HookRulingTest pins both and its one implementer)
            Map.entry("com.legend.lowering.Lowerer -> com.legend.lowering.RelationTexts", "B17 HOOK-RULED 2026-11-17"),
            Map.entry("com.legend.lowering.RelationTexts -> com.legend.lowering.PureOnlyTexts", "B17 HOOK-RULED 2026-11-17"),
            Map.entry("com.legend.lowering.RelationTexts -> (stands in a product package)", "B17 HOOK-RULED 2026-11-17"),
            Map.entry("com.legend.planner.PlanEnv -> com.legend.lowering.RelationTexts", "B17 HOOK-RULED 2026-11-17"),
            Map.entry("com.legend.testplan.PlanStyleFacets -> com.legend.lowering.RelationTexts", "B17 HOOK-RULED 2026-11-17"),
            // the style's text facets as the lowering's static helpers read them (B8, the Styled seam, 2026-11-17)
            Map.entry("com.legend.lowering.Styled -> com.legend.lowering.PureOnlyTexts", "B8 HOOK-RULED 2026-11-17"),
            Map.entry("com.legend.planner.PlanEnv -> com.legend.lowering.PureOnlyTexts", "B8 HOOK-RULED 2026-11-17"),
            Map.entry("com.legend.testplan.PlanStyleFacets -> com.legend.lowering.PureOnlyTexts",
                    "B8 HOOK-RULED 2026-11-17"),
            // C4, the engine-text assert's arm (the orchestrator's ruling of 2026-11-17): EngineTestText a bare hook (its
            // records top-level, its gates the edge's), asked only where the style judges engine text
            // (PlanStyle.judgesEngineText, EngineTextGates); EngineTextPlanning the listed facade, forwarding alone
            Map.entry("com.legend.testplan.AssertVerdicts -> com.legend.testplan.EngineTestText", "C4 HOOK-RULED 2026-11-17"),
            Map.entry("com.legend.testplan.EngineTestText -> (stands in a product package)", "C4 HOOK-RULED 2026-11-17"),
            Map.entry("com.legend.testplan.EngineTextPlanning -> (stands in a product package)", "C4 HOOK-RULED 2026-11-17"),
            Map.entry("com.legend.testplan.PlanStyleFacets -> com.legend.testplan.EngineTestText", "C4 HOOK-RULED 2026-11-17"),
            Map.entry("com.legend.testplan.EngineTextGates -> com.legend.testplan.EngineTestText", "C4 HOOK-RULED 2026-11-17"));

    // ---- the guards ---------------------------------------------------------------------------------------------

    private static JavaClasses classes(String... properties) {
        List<java.util.jar.JarFile> jars = new java.util.ArrayList<>();
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

    private static String top(JavaClass c) {
        String n = c.getName();
        int dollar = n.indexOf('$');
        return dollar < 0 ? n : n.substring(0, dollar);
    }

    private static boolean inEdge(String name) {
        return name.startsWith("com.legend.edge.");
    }

    private static boolean testSide(String name) {
        return TEST_SIDE.stream().anyMatch(p -> name.startsWith(p + ".")) || name.equals("com.legend.TestExecution");
    }

    /** Every "source -> target" naming the vocabulary, over {@code classes}, for the sources {@code side} admits. */
    static Set<String> found(JavaClasses classes, java.util.function.Predicate<String> side) {
        Set<String> out = new TreeSet<>();
        for (JavaClass c : classes) {
            String src = top(c);
            if (inEdge(src) || !side.test(src)) {
                continue;
            }
            if (COMPAT_CLASSES.containsKey(src) || REFEREE.contains(src)) {
                out.add(src + " -> (stands in a product package)");
            }
            c.getDirectDependenciesFromSelf().forEach(d -> {
                String target = top(d.getTargetClass());
                if (target.equals(src)) {
                    return;
                }
                if (inEdge(target)) {
                    out.add(src + " -> " + target);
                } else if (COMPAT_CLASSES.containsKey(target) || REFEREE.contains(target)) {
                    out.add(src + " -> " + target);
                }
            });
            if (!src.startsWith("com.legend.sql.")) {
                c.getFieldAccessesFromSelf().forEach(a -> {
                    String owner = a.getTarget().getOwner().getName();
                    String field = a.getTarget().getName();
                    if (owner.equals("com.legend.sql.SqlFn") && EDGE_ONLY_SQLFN.contains(field)) {
                        out.add(src + " -> SqlFn." + field);
                    } else if (owner.endsWith("TestPlan$JudgingRules") && field.equals("COMPILED_PURE_COMPAT")) {
                        out.add(src + " -> JudgingRules.COMPILED_PURE_COMPAT");
                    }
                });
            }
        }
        return out;
    }

    @Test
    void theProductsCodeNamesNoCompatibilityButItsOpenLines() {
        Set<String> f = found(classes("legend.product.jars", "legend.authored.jars"), n -> !testSide(n));
        f.removeAll(HOOKS_RULED.keySet());
        assertEquals(new TreeSet<>(CODE_OPEN.keySet()), f, "CODE: a product class names the compatibility vocabulary"
                + " outside its census line (a new entry is a regression; a gone one closes, delete its entry)");
    }

    /** Each ruled hook entry still stands (a gone one is deleted from {@link #HOOKS_RULED}, not left to read as held). */
    @Test
    void everyRuledHookEntryStands() {
        Set<String> f = found(classes("legend.product.jars", "legend.authored.jars"), n -> true);
        assertEquals(new TreeSet<>(HOOKS_RULED.keySet()), new TreeSet<>(f.stream().filter(HOOKS_RULED::containsKey)
                .toList()), "a ruled hook's entry is gone: delete it from HOOKS_RULED");
    }

    @Test
    void theTestSideNamesNoCompatibilityButItsOpenLines() {
        Set<String> f = found(classes("legend.product.jars"), EdgeSeparationTest::testSide);
        f.removeAll(HOOKS_RULED.keySet());
        assertEquals(new TreeSet<>(TEST_OPEN.keySet()), f, "TEST SIDE: the test infrastructure names the compatibility"
                + " vocabulary outside its census line");
    }

    /** TEXT (limit (a)): CompatSpellingGuardTest's open rows -- every row not STAYS or RULED -- each with its census line,
     *  so the spellings a dependency graph cannot see count too ("file | spelling" as the register writes it). Exact both
     *  ways with the register ({@link #everyOpenCompatRowIsCounted}). */
    static final Map<String, String> CSG_OPEN = Map.ofEntries(
            // B9 -> C5 (E-P5) 2026-11-17: the judges' canon's alone (LiteralSpelling.temporalCanon)
            Map.entry("sql/dialect/DateTimePrintText.java | +0000", "B12 2026-11-17 (format %r arm, LiteralSpelling.temporalCanon's gone by C5 step C)"),
            Map.entry("compiler/element/type/PlatformTypes.java | TDS_NULL_CELL", "B10 2026-11-17"),
            // B11 -> C5 2026-11-17 (the orchestrator's ruling): the judges' canon (temporalCanon x2, dateTimeLiteralAt via
            // CanonicalRenderSql), closed by C5's step C (E-SQL's)
            Map.entry("lowering/Repr.java | Repr", "B12 2026-11-17"),
            Map.entry("lowering/Scalars.java | Repr", "B12 2026-11-17"),
            Map.entry("lowering/VerdictSql.java | TDS_NULL_CELL", "C5 2026-11-17"),
            Map.entry("sql/SqlExpr.java | TDS_NULL_CELL", "B10 2026-11-17"),
            Map.entry("testexec/AssertRuns.java | EngineText", "C3 2026-11-17"),
            Map.entry("testexec/CanonicalForm.java | +0000", "C5 2026-11-17"),
            Map.entry("testexec/HostJudgments.java | EngineText", "C3 2026-11-17"),
            // testexec/SideShapes.java | Cannot cast a collection of size: GONE 2026-11-17 (C5's step E, E-P5), worded at
            // planning by the plan's style (SideShape.sizeWords, TestPlan.answerSizeWords)
            Map.entry("testexec/SideShapes.java | TDS_NULL_CELL", "C5 2026-11-17"),
            Map.entry("testexecutionplan/HostJson.java | EngineText", "C3 2026-11-17"),
            Map.entry("testexecutionplan/HostJudgment.java | EngineText", "C3 2026-11-17"),
            Map.entry("testexecutionplan/NamedExtensions.java | EngineText", "C3 2026-11-17"),
            Map.entry("testexecutionplan/Verdicts.java | EngineText", "C3 2026-11-17"),
            // B11 -> C5 2026-11-17 (the orchestrator's ruling): the host judge's +0000, C5's step B (E-P5's)
            Map.entry("testexecutionplan/Verdicts.java | +0000", "C5 2026-11-17 (E-P5's step B)"),
            // (testplan/AgreementAsserts.java | EngineText closed: 2026-11-17, (b) of the orchestrator's order: the fused agreement path deleted)
            Map.entry("testplan/AssertVerdicts.java | EngineText", "C3 2026-11-17"),
            Map.entry("compiler/spec/VerdictQueries.java | TDSNull", "B10 2026-11-17"),
            Map.entry("lowering/CanonicalRenderSql.java | TDSNull", "C5 2026-11-17"),
            Map.entry("lowering/VerdictSql.java | TDSNull", "B10 2026-11-17"),
            Map.entry("testexec/Equality.java | TDSNull", "B10 2026-11-17"),
            Map.entry("testexec/Equality.java | COMPILED_PURE_COMPAT", "C1 2026-11-17"),
            Map.entry("testexecutionplan/TestPlan.java | COMPILED_PURE_COMPAT", "C1 2026-11-17"),
            Map.entry("testexecutionplan/TestPlanJson.java | COMPILED_PURE_COMPAT", "C1 2026-11-17"),
            Map.entry("testplan/PlanningOptions.java | COMPILED_PURE_COMPAT", "C1 2026-11-17"));

    /** The census's open lines (docs/records/f-separation-census-2026-11-17.md; closed: A4, B14 ruled; A2, A3 moved to
     *  the edge 2026-11-17 (A4 extended to the native image and the page module); A5, A6 split off the edge 2026-11-17; A1 the embedded hosts' engine routes to the edge 
     *  B1, B3, B17 HOOK-RULED 2026-11-17 (EngineTextDialect, {@link #HOOKS_RULED}), then moved to the edge by Phase 6 (2026-11-17); B4, B6, B13
     *  CLOSED 2026-11-17 by Phase 6 (RawSqlBoundary, PurePrint and UpstreamRelationType, the test-data generator's text
     *  moved to the edge); B2 HOOK-RULED 2026-11-17 (UpstreamPlans, signatures only); B5 CLOSED 2026-11-17
     *  (the lowering's shape options, the trace comment and the result envelope the edge's); B7 CLOSED 2026-11-17 (the
     *  product's API kit LiteV1Api, legend-engine's ExecuteInput binding the edge's); B11 CLOSED 2026-11-17 (its last rows the judges' canon and the host judge's +0000, relabelled C5);
     *  B9 CLOSED 2026-11-17 (the Float rename its last entries; its hooks ruled terminal, its DateTime family the
     *  two-form hook DateTimeTexts, its canon composition relabelled C5);
     *  B8 CLOSED 2026-11-17 (its last entry, LiteralSpelling -> DATETIME_PRINT, gone with C5's step C: the judges'
     *  canon is lite's DATETIME_CANON_TEXT);
     *  records only:
     *  B15, B16, C6, C7). Each must hold at least one entry in the count ({@link #everyOpenLineIsCounted}), so an empty
     *  count cannot read DONE with a line left. A line closes by leaving this set with its last entry. */
    static final Set<String> OPEN_LINES = Set.of(
            "B10", "B12", "C1", "C3", "C5");

    private static Set<String> linesOf(Map<String, String> m) {
        Set<String> out = new TreeSet<>();
        m.values().forEach(v -> out.add(v.split(" ")[0]));
        return out;
    }

    @Test
    void everyOpenCompatRowIsCounted() {
        Set<String> open = new TreeSet<>();
        for (String row : CompatSpellingGuardTest.REGISTER) {
            String[] p = row.split("\\|");
            String step = p[3].trim();
            if (!step.equals("STAYS") && !step.equals("RULED") && !step.equals("HOOK-RULED")) {
                open.add(p[0].trim() + " | " + p[1].trim());
            }
        }
        assertEquals(open, new TreeSet<>(CSG_OPEN.keySet()), "TEXT: CompatSpellingGuardTest's open rows and this"
                + " count's text entries differ (a row gone: delete its entry; a row added: give it its census line)");
    }

    @Test
    void everyOpenLineIsCounted() {
        Set<String> counted = new TreeSet<>();
        for (Map<String, String> m : List.of(BUILD_OPEN, CODE_OPEN, TEST_OPEN, CSG_OPEN)) {
            counted.addAll(linesOf(m));
        }
        assertEquals(new TreeSet<>(OPEN_LINES), counted, "the census's open lines and the lines this count holds"
                + " differ: an open line with no entry would read DONE with work left; an entry of a closed or"
                + " unknown line is a stale entry");
    }

    /** BUILD: the direct dependents of an edge target, within the product's roots' closure (core/BUILD.bazel's
     *  {@code edge_direct_dependents} genquery), each with its census line. */
    static final Map<String, String> BUILD_OPEN = Map.ofEntries(
            // A5, A6 closed 2026-11-17 (E-P5): the product's test library, its cases and its test targets carry no edge
            // library (the edge's tests are core_edge_tests_lib's, run in targets tagged edge_test;
            // //tools/deps:edge_separation_closure_test holds every untagged core test target to it)
            // //wasm:boundary (A1) gone 2026-11-17 (E-P5): legend-engine's routes are //wasm:edge_boundary's, the
            // module's entry //wasm:edge_tab's; no product target depends on an edge target now
            );

    static Set<String> buildFound() {
        return buildFound("legend.edge_dependents");
    }

    static Set<String> buildFound(String property) {
        try {
            Set<String> out = new TreeSet<>();
            for (String l : java.nio.file.Files.readAllLines(com.legend.testing.Runfile.property(property))) {
                if (!l.isBlank()) {
                    out.add(l.trim());
                }
            }
            return out;
        } catch (java.io.IOException e) {
            throw new java.io.UncheckedIOException(e);
        }
    }

    @Test
    void theBuildReachesNoEdgeButItsOpenLines() {
        assertEquals(new TreeSet<>(BUILD_OPEN.keySet()), buildFound(), "BUILD: a product target depends on an edge"
                + " target outside its census line (a new one is a regression; a gone one closes: delete its entry)");
    }

    /** Each guard turns red when its dependency is reintroduced: an entry taken off the allowlist while its dependency
     *  stands is a violation, and a planted product class reaching the edge, a SqlFn edge-only node and the judges'
     *  compatibility mode is found, on each side. */
    @Test
    void eachGuardBites() {
        // the same query with a planted product-shaped root that depends on the edge (core/BUILD.bazel
        // :planted_edge_reach; A1, 2026-11-17: no real product target reaches the edge to bite on any more)
        Set<String> plantedBuild = buildFound("legend.edge_dependents_planted");
        assertTrue(plantedBuild.contains("//core:planted_edge_reach")
                        && !plantedBuild.equals(new TreeSet<>(BUILD_OPEN.keySet())),
                "the BUILD guard bites: a planted root's edge dependency is found, " + plantedBuild);
        JavaClasses planted = new ClassFileImporter().importClasses(
                com.legend.planner.PlantedCompatReach.class);
        Set<String> code = found(planted, n -> true);
        for (String want : List.of("com.legend.planner.PlantedCompatReach -> com.legend.edge.Presenter",
                "com.legend.planner.PlantedCompatReach -> SqlFn.DATETIME_PRINT",
                "com.legend.planner.PlantedCompatReach -> JudgingRules.COMPILED_PURE_COMPAT")) {
            assertFalse(!code.contains(want), "the CODE and TEST SIDE guards bite: " + want + " in " + code);
        }
    }

    /** "Lines left": the allowlists' entries, counted by the code, and by census line. */
    @Test
    void linesLeft() {
        Map<String, Integer> byLine = new TreeMap<>();
        for (Map<String, String> m : List.of(BUILD_OPEN, CODE_OPEN, TEST_OPEN, CSG_OPEN)) {
            m.values().forEach(v -> byLine.merge(v.split(" ")[0], 1, Integer::sum));
        }
        System.out.println("[edge separation] lines left: " + (BUILD_OPEN.size() + CODE_OPEN.size() + TEST_OPEN.size()
                + CSG_OPEN.size()) + " (BUILD " + BUILD_OPEN.size() + ", CODE " + CODE_OPEN.size() + ", TEST SIDE "
                + TEST_OPEN.size() + ", TEXT " + CSG_OPEN.size() + "); by census line " + byLine);
    }
}
