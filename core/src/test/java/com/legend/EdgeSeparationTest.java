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
            // (sql.dialect.DateTimePrintText: deleted 2026-11-17, census B12 -- the edge composes legend-pure's DateTime
            // print, edge.PureDateTimePrint)
            Map.entry("com.legend.lowering.ResultEnvelopes", "B5"),
            Map.entry("com.legend.lowering.PureOnlyTexts", "B8"),
            // (com.legend.lowering.Repr: deleted 2026-11-17, census B12's 2b -- legend-pure's representation the edge's
            // EdgePureOnlyTexts, toRepresentation's since C5 step A, format's %r's since 2b)
            Map.entry("com.legend.testplan.EngineTestText", "C4"),
            Map.entry("com.legend.testplan.EngineTextPlanning", "C4"),
            Map.entry("com.legend.testplan.PlanStyleFacets", "C4"));
    // (com.legend.testexec.PureAsserts, legend-pure's assert family: decisions only since C5's step B, its every word
    // the PureWords hook's -- mapped 2026-11-18, census J17, as CompatSpellingGuardTest.WORDED_BY_HOOK, whose check
    // reads its source; a dependency graph cannot see a string)

    /** The SqlFn members only the edge spells (census B8). */
    static final Set<String> EDGE_ONLY_SQLFN = Set.of();   // DATETIME_PRINT gone 2026-11-17 (census B12), the last

    // (UPSTREAM_WORDED, the records whose upstream component held legend-pure's words of a raise site: gone 2026-11-17,
    // K10b -- ExecutionPlan.Site and RaiseSite carry no upstream, the words the edge's records (edge.SiteWords), so no
    // product read of them can return)

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
            // lowering.Scalars -> SqlFn.DATETIME_PRINT: gone 2026-11-17, census B12's 2b (F's; the user's §10 item 8):
            // format's %r refused in the product by name, the edge's EdgePureOnlyTexts.formatR its text
            // B9 -> C5 (E-P5) 2026-11-17 (the orchestrator's ruling (a)): the composition the judges' canon alone emits
            // (LiteralSpelling.temporalCanon), closed by C5's step C
            // sql.dialect.AnsiSqlRenderer -> DateTimePrintText, DateTimePrintText -> (stands in a product package): gone
            // 2026-11-17, census B12 (the composition the edge's, edge.PureDateTimePrint; DATETIME_PRINT left the MIR)
            // planner.Boundary -> com.legend.edge.HostAnswers, PureText, PureV1Routes: gone 2026-11-17 (A1, E-P5): the
            // pure/v1 route and execute's halves moved to com.legend.edge.wasm.EdgeBoundary (//wasm:edge_boundary)
            // B7 CLOSED 2026-11-17 (E-P5, D's split by content): server/PureV1Api is the product's own API kit,
            // server/LiteV1Api in //core:lite_v1 (lite's routes, answer and refusal kinds, LiteV1Api.RefusalKind);
            // legend-engine's ExecuteInput binding moved to the edge (edge.ExecuteInputs); its 7 lines left with it
            // lowering.VerdictSql -> lowering.Repr: gone 2026-11-17 (C5 step A, the PureOnlyTexts facet's representation)
            // lowering.Repr -> (stands in a product package), lowering.Scalars -> lowering.Repr: gone 2026-11-17 with
            // Repr, census B12's 2b
            );

    /** TEST SIDE: the same. */
    static final Map<String, String> TEST_OPEN = Map.ofEntries(
            // (AgreementAsserts -> EngineTextAppeals closed: 2026-11-17, (b) of the orchestrator's order: the fused agreement path deleted)
            // (TestPlan, TestPlanJson, PlanningOptions -> JudgingRules.COMPILED_PURE_COMPAT closed: 2026-11-17, census C1
            // -- the judging rules are data, the compatibility preset the edge's CompatRules)
            );
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
            // TestPlanner -> UpstreamPlans gone (census C1, 2026-11-17: the test-data timestamp text is the two-form
            // TestDataPlanner.Printing kind, asked of the style)
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
            // C5 step A (2026-11-17): an assert message's element text, legend-pure's representation, asked of the facet
            // with its refusal (the lowering's VerdictSql, as Scalars asks it for toRepresentation)
            Map.entry("com.legend.lowering.VerdictSql -> com.legend.lowering.PureOnlyTexts", "C5 HOOK-RULED 2026-11-17"),
            // B17, the relation-text facet (the orchestrator's ruling of 2026-11-17: its signatures, and forwarders to
            // product helpers the product uses itself; HookRulingTest pins both and its one implementer)
            Map.entry("com.legend.lowering.Lowerer -> com.legend.lowering.RelationTexts", "B17 HOOK-RULED 2026-11-17"),
            // (lowering.RelationTexts -> PureOnlyTexts: gone 2026-11-17, the edge audit's J26 -- the facet hands the planner's
            // own Styled through, no substitute built from the edge's texts)
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
            // (AssertVerdicts -> EngineTestText, C4 HOOK-RULED 2026-11-17: the database arm's asks of the facet
            // deleted, A4 of the compatibility program, stage A ratified by the user 2026-11-19)
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
        return found(classes, side, EDGE_ONLY_SQLFN);
    }

    /** {@link #found(JavaClasses, java.util.function.Predicate)} with {@code edgeOnlyFns} the SqlFn constants the
     *  edge alone may write. A read of one is no write where it is a renderer's arm, javac's switch map or SqlFn's own
     *  initializer ({@code SqlFnSortedTest}'s rule) -- the one exemption: until 2026-11-17 every {@code com.legend.sql}
     *  class's field reads were skipped, its MIR helpers' and its codec's with its renderers' (the edge audit's K16). */
    static Set<String> found(JavaClasses classes, java.util.function.Predicate<String> side, Set<String> edgeOnlyFns) {
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
            // a dialect's renderer (the class or one enclosing it a subclass of AnsiSqlRenderer, in
            // com.legend.sql.dialect), in a code unit that carries no value out (SqlFnSortedTest.carriesOut: it
            // returns what a render returns, builds no node, sets no field that can hold one); javac's switch map (a
            // synthetic $SwitchMap$ field) in the sql package alone; SqlFn's own code -- its initializer, its synthetic
            // $values, and a member carrying nothing out -- nothing else. The review of 11de9ae229, 2026-11-18,
            // MUST-FIX 1: a renderer in any package, an ordinary field named $SwitchMap$ and every member of SqlFn were
            // exempt; the review of 091b9bde36, the same day, MUST-FIX 3 and SHOULD-FIX 4: a helper nesting a class
            // implementing SqlDialect, a renderer's method or lambda handing the function back, and a renderer's own
            // node were exempt, and SqlFn's $values and its predicates were not.
            boolean rendererClass = SqlFnSortedTest.renderer(c) && c.getName().startsWith("com.legend.sql.dialect.");
            boolean switchMap = src.startsWith("com.legend.sql.") && SqlFnSortedTest.switchMap(c);
            boolean sqlFn = c.getName().equals("com.legend.sql.SqlFn");
            c.getFieldAccessesFromSelf().forEach(a -> {
                String owner = a.getTarget().getOwner().getName();
                String field = a.getTarget().getName();
                com.tngtech.archunit.core.domain.JavaCodeUnit u = a.getOrigin();
                boolean own = sqlFn && (u instanceof com.tngtech.archunit.core.domain.JavaStaticInitializer
                        || (u.getName().equals("$values") && u.getModifiers().contains(
                                com.tngtech.archunit.core.domain.JavaModifier.SYNTHETIC))
                        || !SqlFnSortedTest.carriesOut(u));
                boolean reads = switchMap || own || (rendererClass && !SqlFnSortedTest.carriesOut(u));
                if (owner.equals("com.legend.sql.SqlFn") && edgeOnlyFns.contains(field) && !reads) {
                    out.add(src + " -> SqlFn." + field);
                }
                // (the judges' compatibility rules: a field of the edge's since 2026-11-17, census C1 --
                // edge.CompatRules.COMPILED_PURE, an edge reach the class rule above finds)
            });
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
            // sql/dialect/DateTimePrintText.java | +0000: gone 2026-11-17 with the class, census B12
            // B11 -> C5 2026-11-17 (the orchestrator's ruling): the judges' canon (temporalCanon x2, dateTimeLiteralAt via
            // CanonicalRenderSql), closed by C5's step C (E-SQL's)
            // lowering/Repr.java | Repr, lowering/Scalars.java | Repr: gone 2026-11-17, census B12's 2b (Repr deleted)
            // (testexec/CanonicalForm.java | +0000 closed: 2026-11-17, census C5, E-Java -- the instrument's own date text)
            );
            // testexec/SideShapes.java | Cannot cast a collection of size: GONE 2026-11-17 (C5's step E, E-P5), worded at
            // planning by the plan's style (SideShape.sizeWords, TestPlan.answerSizeWords)
            // B11 -> C5 2026-11-17 (the orchestrator's ruling): the host judge's +0000, C5's step B (E-P5's)
            // (testplan/AgreementAsserts.java | EngineText closed: 2026-11-17, (b) of the orchestrator's order: the fused agreement path deleted)
            // (TestPlan, TestPlanJson, PlanningOptions | COMPILED_PURE_COMPAT closed: 2026-11-17, census C1 -- the
            // judging rules are data, the compatibility preset the edge's CompatRules)

    /** The census's open lines (docs/records/f-separation-census-2026-11-17.md; closed: A4, B14 ruled; A2, A3 moved to
     *  the edge 2026-11-17 (A4 extended to the native image and the page module), A3's page module then the product's
     *  again, at lite's own route in lite's words (lite's API v1, H6, 2026-11-17: a root the build's query holds); A5, A6 split off the edge 2026-11-17; A1 the embedded hosts' engine routes to the edge 
     *  B1, B3, B17 HOOK-RULED 2026-11-17 (EngineTextDialect, {@link #HOOKS_RULED}), then moved to the edge by Phase 6 (2026-11-17); B4, B6, B13
     *  CLOSED 2026-11-17 by Phase 6 (RawSqlBoundary, PurePrint and UpstreamRelationType, the test-data generator's text
     *  moved to the edge); B2 HOOK-RULED 2026-11-17 (UpstreamPlans, signatures only); B5 CLOSED 2026-11-17
     *  (the lowering's shape options, the trace comment and the result envelope the edge's); B7 CLOSED 2026-11-17 (the
     *  product's API kit LiteV1Api, legend-engine's ExecuteInput binding the edge's); B11 CLOSED 2026-11-17 (its last rows the judges' canon and the host judge's +0000, relabelled C5);
     *  B9 CLOSED 2026-11-17 (the Float rename its last entries; its hooks ruled terminal, its DateTime family the
     *  two-form hook DateTimeTexts, its canon composition relabelled C5);
     *  B8 CLOSED 2026-11-17 (its last entry, LiteralSpelling -> DATETIME_PRINT, gone with C5's step C: the judges'
     *  canon is lite's DATETIME_CANON_TEXT);
     *  B10 CLOSED 2026-11-17 (the judges' TDSNull sentinel gone with C5's step C, C-2: an expected null cell is a
     *  typed TypedNullCell, its canon lite's bare null, its wire SqlExpr.NULL_CELL_WIRE);
     *  B12 CLOSED 2026-11-17 (F's 2b: format's %r refused in the product, lowering.Repr deleted; legend-pure's DateTime
     *  print the edge's: the product's DATETIME_TO_TEXT printed by the edge's dialects, a literal its written text,
     *  edge.PureDateTimePrint, sql.dialect.DateTimePrintText deleted);
     *  records only:
     *  B15, B16, C6, C7). Each must hold at least one entry in the count ({@link #everyOpenLineIsCounted}), so an empty
     *  count cannot read DONE with a line left. A line closes by leaving this set with its last entry. */
    static final Set<String> OPEN_LINES = Set.of(
            // C1 closed 2026-11-17 (E-Java): the judges' behaviours the style's and the run hook's, the judging rules
            // data, the compatibility preset the edge's CompatRules
            // C5 closed 2026-11-17 (its last line, CanonicalForm's +0000, E-Java's); B12 closed by train 7
            // K10 (E-P5, 2026-11-18): a raise site's legend-pure words never entered this count on train 8's base --
            // ExecutionPlan.Site and RaiseSite carry no upstream, the words the edge's records (edge.SiteWords)
            );

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
            // the register's one terminal predicate (2026-11-17, the GUARD stream: no second copy to widen)
            if (!CompatSpellingGuardTest.TERMINAL_STEPS.contains(step)) {
                open.add(p[0].trim() + " | " + p[1].trim());
            }
        }
        assertEquals(open, new TreeSet<>(CSG_OPEN.keySet()), "TEXT: CompatSpellingGuardTest's open rows and this"
                + " count's text entries differ (a row gone: delete its entry; a row added: give it its census line)");
    }

    @Test
    void everyOpenLineIsCounted() {
        Set<String> counted = new TreeSet<>();
        // (RULES, the GUARD stream's C.3 of 2026-11-17: gone 2026-11-18 with the judging rules themselves, train 17 --
        // JudgingRulesReachTest's bucket read 0)
        for (Map<String, String> m : List.of(BUILD_OPEN, CODE_OPEN, TEST_OPEN, CSG_OPEN)) {
            counted.addAll(linesOf(m));
        }
        // the edge audit's open items (2026-11-17, EdgeAuditOpenTest), each its own line: none is a census line
        Set<String> audit = EdgeAuditOpenTest.lines();
        assertTrue(java.util.Collections.disjoint(audit, OPEN_LINES), "an audit id is a census line: " + audit);
        counted.addAll(audit);
        Set<String> open = new TreeSet<>(OPEN_LINES);
        open.addAll(audit);
        // J40 (2026-11-18, EdgeOnlyMembersTest): a public product member the edge alone uses, counted one line per class
        // ("J40.<Class>"), none a census line
        Set<String> edgeOnly = linesOf(EdgeOnlyMembersTest.OPEN);
        assertTrue(java.util.Collections.disjoint(edgeOnly, OPEN_LINES), "an edge-only member's id is a census line: "
                + edgeOnly);
        counted.addAll(edgeOnly);
        open.addAll(edgeOnly);
        // the lens guards (2026-11-18, the orchestrator's 06:40 rulings): L-A, a two-form hook fed upstream's form or a
        // product branch on upstream's shape, and L-B, the plans' and the MIR's data only the edge uses -- each its own
        // line, none a census line
        Set<String> lens = lensLines();
        assertTrue(java.util.Collections.disjoint(lens, OPEN_LINES), "a lens line is a census line: " + lens);
        counted.addAll(lens);
        open.addAll(lens);
        assertEquals(open, counted, "the census's open lines and the lines this count holds"
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
            // (2026-11-18, the user's K2 ruling, "Product root, edge opt-in": the module's entry is //wasm:tab's again,
            // lite's own -- planner.TabExports, a root here -- and //wasm:edge_tab is the engine module's entry alone,
            // com.legend.edge.wasm.EngineExports, legend-engine's four routes)
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
                "com.legend.planner.PlantedCompatReach -> com.legend.edge.CompiledPureHostForms")) {
            assertFalse(!code.contains(want), "the CODE and TEST SIDE guards bite: " + want + " in " + code);
        }
        // the edge audit's K16 (2026-11-17): a com.legend.sql class's reads are read too -- an edge-only function (ABS
        // planted as one) -- and a renderer's arm and a switch's map are not
        JavaClasses sqlPlanted = new ClassFileImporter().withImportOption(l -> l.contains("/PlantedSql")
                && !l.contains("SwitchNamed")).importPackages("com.legend.sql");
        assertTrue(sqlPlanted.size() >= 4, "the planted classes are imported: " + sqlPlanted.size());
        Set<String> sql = found(sqlPlanted, n -> true, Set.of("ABS"));
        assertEquals(Set.of("com.legend.sql.PlantedSqlReach -> SqlFn.ABS"), sql, "the CODE guard reads the sql package");
        // the review of 11de9ae229 (2026-11-18): a renderer nested in a product class outside the dialects, and a field
        // merely named $SwitchMap$, exempt nothing
        JavaClasses hiding = new ClassFileImporter().importClasses(com.legend.planner.PlantedRendererHide.class,
                com.legend.planner.PlantedRendererHide.R.class, com.legend.sql.PlantedSqlSwitchNamed.class);
        assertTrue(hiding.size() == 3, "the planted classes are imported: " + hiding.size());
        assertEquals(Set.of("com.legend.planner.PlantedRendererHide -> SqlFn.ABS",
                "com.legend.sql.PlantedSqlSwitchNamed -> SqlFn.ABS"), found(hiding, n -> true, Set.of("ABS")),
                "a renderer outside the dialects and a field named $SwitchMap$ exempt nothing");
        // the review of 091b9bde36 (2026-11-18, MUST-FIX 3): a helper nesting a SqlDialect two levels down, and a dialect
        // handing the function back -- returned, by a lambda in a field, as a node it builds, kept in a field -- are
        // named; a dialect's arm writing text is not
        JavaClasses laundering = new ClassFileImporter().importClasses(
                com.legend.sql.dialect.PlantedSqlRendererArm.class, com.legend.sql.dialect.PlantedRendererNest.class,
                com.legend.sql.dialect.PlantedRendererNest.A.class, com.legend.sql.dialect.PlantedRendererNest.A.R.class,
                com.legend.sql.dialect.PlantedLaunderReturn.class, com.legend.sql.dialect.PlantedLaunderLambda.class,
                com.legend.sql.dialect.PlantedLaunderBuild.class, com.legend.sql.dialect.PlantedLaunderField.class,
                com.legend.sql.dialect.PlantedRendererByInterface.class,
                // the review of dd5deb781f (MUST-FIX 2): an array store, a collection's add, a throw, a setter
                com.legend.sql.dialect.PlantedLaunderArray.class, com.legend.sql.dialect.PlantedLaunderAdd.class,
                com.legend.sql.dialect.PlantedLaunderThrow.class, com.legend.sql.dialect.PlantedLaunderSetter.class,
                // review F of the guard line (SHOULD 1): a varargs keeper, an array keeper, an ordinal and a name kept --
                // one shape a class, and the number or the name handed to a keeper (the review of the guard line, G,
                // SHOULD 1, 2026-11-18: two shapes in one class left a half of the rule unpinned)
                com.legend.sql.dialect.PlantedLaunderVarargs.class, com.legend.sql.dialect.PlantedLaunderArrayKeeper.class,
                com.legend.sql.dialect.PlantedLaunderOrdinal.class, com.legend.sql.dialect.PlantedLaunderName.class,
                com.legend.sql.dialect.PlantedLaunderOrdinalHanded.class,
                com.legend.sql.dialect.PlantedLaunderNameHanded.class);
        assertEquals(19, laundering.size(), "the planted classes are imported");
        assertEquals(Set.of("com.legend.sql.dialect.PlantedRendererNest -> SqlFn.ABS",
                "com.legend.sql.dialect.PlantedRendererByInterface -> SqlFn.ABS",
                "com.legend.sql.dialect.PlantedLaunderReturn -> SqlFn.ABS",
                "com.legend.sql.dialect.PlantedLaunderLambda -> SqlFn.ABS",
                "com.legend.sql.dialect.PlantedLaunderBuild -> SqlFn.ABS",
                "com.legend.sql.dialect.PlantedLaunderField -> SqlFn.ABS",
                "com.legend.sql.dialect.PlantedLaunderArray -> SqlFn.ABS",
                "com.legend.sql.dialect.PlantedLaunderAdd -> SqlFn.ABS",
                "com.legend.sql.dialect.PlantedLaunderThrow -> SqlFn.ABS",
                "com.legend.sql.dialect.PlantedLaunderSetter -> SqlFn.ABS",
                "com.legend.sql.dialect.PlantedLaunderVarargs -> SqlFn.ABS",
                "com.legend.sql.dialect.PlantedLaunderArrayKeeper -> SqlFn.ABS",
                "com.legend.sql.dialect.PlantedLaunderOrdinal -> SqlFn.ABS",
                "com.legend.sql.dialect.PlantedLaunderName -> SqlFn.ABS",
                "com.legend.sql.dialect.PlantedLaunderOrdinalHanded -> SqlFn.ABS",
                "com.legend.sql.dialect.PlantedLaunderNameHanded -> SqlFn.ABS"), found(laundering, n -> true, Set.of("ABS")),
                "a dialect's read carrying the function out is named, its arm is not");
        // the review of 091b9bde36 (SHOULD-FIX 4): SqlFn's own code -- its initializer, $values, its predicates --
        // reads every constant and is never named, so an edge-only function does not turn the guard on the enum itself
        JavaClasses sqlFn = new ClassFileImporter().importClasses(com.legend.sql.SqlFn.class);
        assertEquals(1, sqlFn.size(), "SqlFn is imported");
        Set<String> every = new TreeSet<>();
        for (com.legend.sql.SqlFn f : com.legend.sql.SqlFn.values()) {
            every.add(f.name());
        }
        assertEquals(Set.of(), found(sqlFn, n -> true, every), "SqlFn's own code reads its constants");
    }

    /** "Lines left": the allowlists' entries, counted by the code, and by census line. */
    @Test
    void linesLeft() {
        Map<String, Integer> byLine = new TreeMap<>();
        for (Map<String, String> m : List.of(BUILD_OPEN, CODE_OPEN, TEST_OPEN, CSG_OPEN,
                EdgeOnlyMembersTest.OPEN)) {
            m.values().forEach(v -> byLine.merge(v.split(" ")[0], 1, Integer::sum));
        }
        // the edge audit's open items (2026-11-17): what no guard sees yet, counted all the same
        EdgeAuditOpenTest.AUDIT_OPEN.forEach(i -> byLine.merge(i.id(), 1, Integer::sum));
        int audit = EdgeAuditOpenTest.AUDIT_OPEN.size();
        // (RULES: gone 2026-11-18, train 18 -- JudgingRulesReachTest deleted with the judging rules, E-Java's 070c4ea3e)
        // a public product member the edge alone uses (J40 of the edge audit, 2026-11-18: EdgeOnlyMembersTest)
        int edgeOnly = EdgeOnlyMembersTest.lines().size();   // one line per class (the ruling's (1))
        // the lens guards' lines (2026-11-18): L-A's and L-B's, one per line
        Set<String> lens = lensLines();
        lens.forEach(l -> byLine.merge(l, 1, Integer::sum));
        System.out.println("[edge separation] lines left: " + (BUILD_OPEN.size() + CODE_OPEN.size() + TEST_OPEN.size()
                + CSG_OPEN.size() + audit + edgeOnly + lens.size()) + " (BUILD " + BUILD_OPEN.size() + ", CODE "
                + CODE_OPEN.size() + ", TEST SIDE " + TEST_OPEN.size() + ", TEXT " + CSG_OPEN.size()
                + ", AUDIT " + audit + ", EDGE-ONLY MEMBERS " + edgeOnly + ", LENS " + lens.size() + "); by census line "
                + byLine);
    }

    /** The lens guards' census lines (the orchestrator's rulings of 2026-11-18, 06:40): L-A's two-form hooks fed
     *  upstream's form (ProductFormsUseTheirArgumentsTest) and its branches on upstream's shape (UpstreamBranchesTest),
     *  and L-B's edge-only data (EdgeOnlyDataTest). */
    static Set<String> lensLines() {
        Set<String> out = new TreeSet<>(ProductFormsUseTheirArgumentsTest.lines());
        out.addAll(UpstreamBranchesTest.lines());
        out.addAll(EdgeOnlyDataTest.lines());
        return out;
    }
}
