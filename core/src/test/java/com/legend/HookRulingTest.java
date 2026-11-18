// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend;

import com.legend.database.PlanStyle;
import com.legend.error.NotImplementedException;
import com.tngtech.archunit.core.domain.JavaClass;
import com.tngtech.archunit.core.domain.JavaClasses;
import com.tngtech.archunit.core.importer.ClassFileImporter;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * HOOKS ARE TERMINAL (the orchestrator's ruling of 2026-11-17, on the user's "the edge's typed extension point",
 * docs/PLAN_AS_DATA_2026_11_07.md, 2026-11-08). A product hook is the separation, not a leak, when ALL of these hold:
 * (a) an interface in the product, types and signatures only, no compatibility logic or text; (b) the PRODUCT style's
 * answer refuses it by name; (c) every non-refusing implementation lives in {@code com.legend.edge}; (d) a test pins
 * its implementers -- this one. Each hook listed here is {@code EdgeSeparationTest.HOOKS_RULED}'s, and stops counting
 * as an open line; a hook that fails a criterion stays open, the criterion named beside its entry there.
 */
@Tag("guardrail")
class HookRulingTest {

    /** The ruled hooks, each with the PRODUCT style's calls that would hand one out. */
    static final Map<Class<?>, List<Executable>> RULED = Map.ofEntries(
            // census C2, 2026-11-17 (the orchestrator's ruling that day: NoEdgeRun is the product's refusal, by name):
            // the edge's run hook, and the hearing it opens
            Map.entry(com.legend.testexec.EdgeRun.class, List.of(
                    () -> com.legend.testexec.NoEdgeRun.NONE.open(planNaming(), null, java.util.UUID.randomUUID()))),
            Map.entry(com.legend.testexec.EdgeHearing.class, List.of(
                    () -> productHearing().judged(null, List.of(), null, false),
                    () -> productHearing().rowAppealed(0, List.of(), false),
                    () -> productHearing().pureWords())),
            // C5's step B, 2026-11-17 (the orchestrator's ruling (1)): legend-pure's words of an assert's failure at run,
            // the hearing's member (NoEdgeRun's hearing refuses it by name)
            Map.entry(com.legend.testexec.PureWords.class, List.of(
                    () -> productHearing().pureWords())),
            // census C1, 2026-11-17 (the orchestrator's ruling that day: the edge hook typed, the product's refusal
            // where a null selected lite's rules): the ledger's recorder, each ask refused by the product's NoEdge
            Map.entry(com.legend.testexecutionplan.Edge.class, List.of(
                    () -> com.legend.testexecutionplan.NoEdge.NONE.effect(List.of(), List.of()),
                    () -> com.legend.testexecutionplan.NoEdge.NONE.seed(List.of(), Map.of()),
                    () -> com.legend.testexecutionplan.NoEdge.NONE.seeded(List.of(), List.of()),
                    () -> com.legend.testexecutionplan.NoEdge.NONE.effectRan(List.of()))),
            // census B3 and B1 (EngineTextDialect and PlanStyle's signatures naming it): CLOSED 2026-11-17 by Phase 6 --
            // the interface is com.legend.edge's and the style names no engine text; its entry gone
            // census B2, 2026-11-17 (Phase 6): upstream's plan text, the planner's facet -- signatures only since its
            // routine table moved to planner.PlanTextRoutines
            Map.entry(com.legend.planner.UpstreamPlans.class, List.of(
                    () -> PlanStyle.PRODUCT.facet(com.legend.planner.UpstreamPlans.class))),
            // census B5, 2026-11-17 (Phase 6): legend-engine's result envelope, composed at lowering
            Map.entry(com.legend.lowering.ResultEnvelopes.class, List.of(
                    () -> PlanStyle.PRODUCT.facet(com.legend.lowering.ResultEnvelopes.class))),
            // census B8 (i), 2026-11-17: the texts only legend-pure writes, composed at lowering
            Map.entry(com.legend.lowering.PureOnlyTexts.class, List.of(
                    () -> PlanStyle.PRODUCT.facet(com.legend.lowering.PureOnlyTexts.class))),
            // census B17, 2026-11-17: a relation's legend-pure text, composed at lowering (a facade: FACADES)
            Map.entry(com.legend.lowering.RelationTexts.class, List.of(
                    () -> PlanStyle.PRODUCT.facet(com.legend.lowering.RelationTexts.class))),
            // census C4, 2026-11-17: the engine-text assert's arm, and the two hooks only its answers hand out (the
            // collector and the database judge's batch): each reached through the facet the product refuses
            Map.entry(com.legend.testplan.EngineTestText.class, List.of(
                    () -> PlanStyle.PRODUCT.facet(com.legend.testplan.EngineTestText.class))),
            Map.entry(com.legend.testplan.TextCollector.class, List.of(
                    () -> PlanStyle.PRODUCT.facet(com.legend.testplan.EngineTestText.class))),
            Map.entry(com.legend.testplan.BatchTexts.class, List.of(
                    () -> PlanStyle.PRODUCT.facet(com.legend.testplan.EngineTestText.class))));

    /** C4's CAPABILITY (the orchestrator's ruling of 2026-11-17): the product's style does not judge legend-engine's
     *  text -- the test planner asks it before any engine-text gate, and takes the ordinary path -- the edge's does. */
    @Test
    void onlyTheEdgeJudgesEngineText() {
        assertFalse(PlanStyle.PRODUCT.judgesEngineText());
        assertTrue(com.legend.edge.PureText.UPSTREAM.judgesEngineText());
    }

    /** A ruled hook's static FORWARDERS (the orchestrator's facade rule, 2026-11-17): each forwards one call to a
     *  helper the product itself uses, and the set is pinned exactly -- shrink-only, a new one is seen. Every other
     *  method of a ruled hook is a bare signature. */
    static final Map<Class<?>, java.util.Set<String>> FACADES = Map.of(
            // pureToString (Scalars', every value's text), dateTimeText (LiteralSpelling's, the product's cells),
            // kindType (MixedEncoding's, the lowering's mixed lists)
            com.legend.lowering.RelationTexts.class, java.util.Set.of("pureToString", "kindType"));   // dateTimeText gone 2026-11-17 (census B9: the edge's own, edge.EdgeDateTimeTexts)

    /** The product's refusals of a ruled hook (criterion (b)): its own implementations, which refuse by name, pinned. */
    static final java.util.Set<String> PRODUCT_REFUSALS = java.util.Set.of(
            "com.legend.testexec.NoEdgeRun", "com.legend.testexec.NoEdgeRun$Hearing",
            // census C1, 2026-11-17: the product's edge hook, which records no ledger
            "com.legend.testexecutionplan.NoEdge");

    /** A test plan naming one appeal, by its verdict's number: what the product's run refuses to open. */
    private static com.legend.testexecutionplan.TestPlan planNaming() {
        return plan(Map.of(1, List.of(new com.legend.edge.EffectTexts(List.of()))));
    }

    /** The product's hearing, opened on a plan naming no appeal. */
    private static com.legend.testexec.EdgeHearing productHearing() {
        return com.legend.testexec.NoEdgeRun.NONE.open(plan(Map.of()), null, java.util.UUID.randomUUID());
    }

    private static com.legend.testexecutionplan.TestPlan plan(
            Map<Integer, List<com.legend.testexecutionplan.Extension>> byVerdict) {
        var target = new com.legend.executionplan.ExecutionPlan.Target(
                new com.legend.executionplan.ExecutionPlan.Database.Platform(
                        com.legend.planner.ConnectionPlans.type(com.legend.database.Databases.PLATFORM), null),
                List.of(), List.of(), null, null, null);
        var verdict = new com.legend.testexecutionplan.TestPlan.Verdict(1, "assert", "assert", List.of(), null, null,
                false);
        var step = new com.legend.testexecutionplan.TestPlan.Verdicts(List.of(verdict), null, null,
                com.legend.testexecutionplan.TestPlan.JudgingRules.LITE);
        return new com.legend.testexecutionplan.TestPlan(List.of(), List.of(step),
                com.legend.testexecutionplan.TestPlan.Judge.HOST, Map.of(), byVerdict, false, target,
                new com.legend.testexecutionplan.BodyFacts(false, false, true, "A"), null, false);
    }


    /** (a): an interface declaring signatures only -- no static method, no default method, no field. */
    @Test
    void eachRuledHookIsAnInterfaceOfSignaturesOnly() {
        for (Class<?> hook : RULED.keySet()) {
            assertTrue(hook.isInterface(), hook + " is no interface");
            java.util.Set<String> forwarders = new java.util.TreeSet<>();
            for (Method m : hook.getDeclaredMethods()) {
                if (m.isSynthetic()) {
                    continue;   // a lambda's body (a forwarder's style answering the edge's texts), no member
                }
                if (Modifier.isStatic(m.getModifiers())) {
                    forwarders.add(m.getName());
                    continue;
                }
                assertTrue(Modifier.isAbstract(m.getModifiers()) && !m.isDefault(),
                        hook.getSimpleName() + "." + m.getName() + " is not a bare signature");
            }
            assertEquals(new java.util.TreeSet<>(FACADES.getOrDefault(hook, java.util.Set.of())), forwarders,
                    hook.getSimpleName() + "'s static forwarders are not its pinned product-helper set");
            assertEquals(0, hook.getDeclaredFields().length, hook + " declares a field");
            assertEquals(0, hook.getDeclaredClasses().length, hook + " declares a nested type");
        }
    }

    /** The closed list of the plan style's facet kinds (the orchestrator's ruling of 2026-11-17: a registry meets (a)
     *  when it holds only the kinds' class literals): a private constructor and its lists of interfaces, KINDS and (since
     *  2026-11-17, census B9: PlanStyle.twoForm's closed list, the orchestrator's ruling) TWO_FORMS; no method, no nested
     *  type. */
    @Test
    void theFacetRegistryHoldsNothingButItsList() {
        Class<?> registry = com.legend.testplan.PlanStyleFacets.class;
        assertEquals(0, registry.getDeclaredMethods().length, "PlanStyleFacets declares a method");
        assertEquals(0, registry.getDeclaredClasses().length, "PlanStyleFacets declares a nested type");
        assertEquals(List.of("KINDS", "TWO_FORMS"), java.util.Arrays.stream(registry.getDeclaredFields())
                .filter(f -> !f.isSynthetic()).map(java.lang.reflect.Field::getName).toList());
        for (java.lang.reflect.Constructor<?> c : registry.getDeclaredConstructors()) {
            assertTrue(Modifier.isPrivate(c.getModifiers()) && c.getParameterCount() == 0, c.toString());
        }
        for (Class<?> kind : com.legend.testplan.PlanStyleFacets.KINDS) {
            assertTrue(kind.isInterface(), kind + " is no interface");
        }
        for (Class<?> kind : com.legend.testplan.PlanStyleFacets.TWO_FORMS) {
            assertTrue(kind.isInterface(), kind + " is no interface");
        }
    }

    /** A TWO-FORM HOOK (the orchestrator's ruling of 2026-11-17, B8 (ii)): the product's style answers its own lite
     *  form, holding no upstream knowledge; the upstream form is in the edge's override alone. An execution's trace
     *  text: the product's the id itself, the edge's legend-engine's comment around it. */
    @Test
    void theTwoFormHooksAnswerTheProductsOwnFormAndTheEdgesUpstreamOne() {
        assertEquals(null, PlanStyle.PRODUCT.traceCell(), "the product writes the id alone");
        assertEquals(new PlanStyle.TraceCell("-- \"executionTraceID\" : \"", "\""),
                com.legend.edge.PureText.UPSTREAM.traceCell());
        // the raises a plan writes plain (B7, 2026-11-17; the two-form LoweringForms since the audit's K5): the
        // product's none, PCT's and the corpus's none (legend-pure's guarded words), the pure/v1 route's legend-engine's
        // census class A
        java.util.Set<com.legend.sql.RaiseKind> classA = java.util.EnumSet.of(com.legend.sql.RaiseKind.DIVIDE_BY_ZERO,
                com.legend.sql.RaiseKind.SQRT_OF_NEGATIVE, com.legend.sql.RaiseKind.SHIFT_OUT_OF_RANGE,
                com.legend.sql.RaiseKind.SIZE_NOT_MULTIPLICITY, com.legend.sql.RaiseKind.SLICE_BOUNDS_INVERTED,
                com.legend.sql.RaiseKind.RANGE_STEP_ZERO, com.legend.sql.RaiseKind.CAST_IMPOSSIBLE,
                com.legend.sql.RaiseKind.TEXT_NOT_INTEGER, com.legend.sql.RaiseKind.TEXT_NOT_FLOAT,
                com.legend.sql.RaiseKind.TEXT_NOT_DATE);
        assertEquals(java.util.Set.of(), plainOf(PlanStyle.PRODUCT), "the product writes every check of its own");
        assertEquals(java.util.Set.of(), plainOf(com.legend.edge.PureText.UPSTREAM), "PCT keeps legend-pure's words");
        assertEquals(classA, plainOf(com.legend.edge.PureText.PURE_V1),
                "pure/v1 writes legend-engine's unchecked raises plain");
    }

    /** PlanStyle.twoForm (the orchestrator's ruling of 2026-11-17, census B9): its kinds are a CLOSED list, pinned here
     *  as an exact set (empty until the first family adds its kind, with a dated reason); the product's style answers the
     *  caller's own product form, whatever the kind; the edge's refuses a kind outside the list by name. */
    @Test
    void theTwoFormKindsAreAClosedListTheProductAnswersItsOwnForm() {
        // the two-form kinds, an exact set; each added with its family and its dated reason:
        // DateTimeTexts (census B9, 2026-11-17: a DateTime's text in a message); ListTexts (census B8, 2026-11-17,
        // TEXT_FORMS: a list's text, the product's JSON form, the edge's legend-pure one); JsonGrammar (census C1,
        // 2026-11-17, register S144: a judge's JSON grammar, the product's strict JSON, the edge's json-simple);
        // RelationCounting (census C1, 2026-11-17, register S137: the product refuses a relation counted as one, the
        // edge counts it as upstream does)
        assertEquals(List.of(com.legend.lowering.DateTimeTexts.class, com.legend.lowering.ListTexts.class,
                com.legend.testexecutionplan.JsonGrammar.class, com.legend.compiler.spec.RelationCounting.class,
                // TestDataPlanner.Printing (census C1, 2026-11-17, L9: a test-data timestamp's text)
                com.legend.testdatagen.TestDataPlanner.Printing.class,
                // CrossKindNumbers (census C1, 2026-11-17, register S127: the product never equals two number kinds,
                // the edge equals an Integer and a Decimal by their texts, as compiled legend-pure does)
                com.legend.testplan.CrossKindNumbers.class,
                // RenderedTextRule (census C1, 2026-11-17, registers S131, S132: the product's two strings, the edge's
                // ruled comparison where they differ)
                com.legend.testplan.RenderedTextRule.class,
                // DateLiteralTexts (census C1, 2026-11-17: a date literal's message text, lite's or legend-pure's)
                com.legend.testplan.DateLiteralTexts.class,
                // GoldenNullWord (census C1, 2026-11-17: legend-engine's TDSNull read as a golden's null cell, the edge's)
                com.legend.compiler.spec.GoldenNullWord.class,
                // RoutedQueries (J5 of the edge audit, 2026-11-17: an execute's routing activity, none in the product,
                // legend-engine's AggregationAwareActivity at the edge)
                com.legend.planner.RoutedQueries.class,
                // LoweringForms (the audit's K5, 2026-11-17: the lowering's two-form choices, a raise, a view, a wire cast)
                com.legend.lowering.LoweringForms.class,
                // FailureWords (census C1, 2026-11-17: whose words the database judge plans a failure in)
                com.legend.testplan.FailureWords.class,
                // DatabaseSettingsForm (the audit's J22, RULED by the user 2026-11-17: a database's settings)
                com.legend.sql.dialect.DatabaseSettingsForm.class),
                com.legend.testplan.PlanStyleFacets.TWO_FORMS,
                "the two-form kinds, an exact set: a kind is added here with its family and its dated reason");
        com.legend.planner.RoutedQueries noRouting = (chain, ctx, specs) -> null;
        assertTrue(PlanStyle.PRODUCT.twoForm(com.legend.planner.RoutedQueries.class, noRouting) == noRouting,
                "the product records no routing activity");
        Object edgeRouted = com.legend.edge.PureText.UPSTREAM.twoForm(com.legend.planner.RoutedQueries.class, noRouting);
        assertTrue(edgeRouted != noRouting && edgeRouted.getClass().getPackageName().equals("com.legend.edge"),
                "the edge answers legend-engine's routed query: " + edgeRouted.getClass());
        assertTrue(PlanStyle.PRODUCT.twoForm(com.legend.lowering.DateTimeTexts.class,
                com.legend.lowering.Styled.LITE_DATE_TIMES) == com.legend.lowering.Styled.LITE_DATE_TIMES,
                "the product's DateTime text is lite's");
        Object edgeDateTimes = com.legend.edge.PureText.UPSTREAM.twoForm(com.legend.lowering.DateTimeTexts.class,
                com.legend.lowering.Styled.LITE_DATE_TIMES);
        assertTrue(edgeDateTimes != com.legend.lowering.Styled.LITE_DATE_TIMES
                && edgeDateTimes.getClass().getPackageName().equals("com.legend.edge"),
                "the edge answers its own DateTime text: " + edgeDateTimes.getClass());
        assertTrue(PlanStyle.PRODUCT.twoForm(com.legend.lowering.ListTexts.class,
                com.legend.lowering.ProductListTexts.INSTANCE) == com.legend.lowering.ProductListTexts.INSTANCE,
                "the product answers its own list text");
        assertTrue(com.legend.edge.PureText.UPSTREAM.twoForm(com.legend.lowering.ListTexts.class,
                com.legend.lowering.ProductListTexts.INSTANCE) != com.legend.lowering.ProductListTexts.INSTANCE,
                "the edge answers legend-pure's list text");
        assertTrue(PlanStyle.PRODUCT.twoForm(com.legend.testexecutionplan.JsonGrammar.class,
                com.legend.testexecutionplan.JudgeJson.STRICT) == com.legend.testexecutionplan.JudgeJson.STRICT,
                "the product reads strict JSON");
        assertTrue(com.legend.edge.PureText.UPSTREAM.twoForm(com.legend.testexecutionplan.JsonGrammar.class,
                com.legend.testexecutionplan.JudgeJson.STRICT) == com.legend.edge.JsonSimpleGrammar.INSTANCE,
                "the edge reads upstream's json-simple grammar");
        assertTrue(PlanStyle.PRODUCT.twoForm(com.legend.compiler.spec.RelationCounting.class,
                com.legend.compiler.spec.RelationCountAsserts.REFUSED)
                == com.legend.compiler.spec.RelationCountAsserts.REFUSED, "the product refuses a relation counted as one");
        assertTrue(com.legend.edge.PureText.UPSTREAM.twoForm(com.legend.compiler.spec.RelationCounting.class,
                com.legend.compiler.spec.RelationCountAsserts.REFUSED) == com.legend.edge.UpstreamRelationCounting.INSTANCE,
                "the edge counts a relation as upstream does");
        assertTrue(PlanStyle.PRODUCT.twoForm(com.legend.testplan.CrossKindNumbers.class,
                com.legend.testplan.LiteCrossKindNumbers.NEVER) == com.legend.testplan.LiteCrossKindNumbers.NEVER,
                "the product never equals two number kinds");
        assertTrue(com.legend.edge.PureText.UPSTREAM.twoForm(com.legend.testplan.CrossKindNumbers.class,
                com.legend.testplan.LiteCrossKindNumbers.NEVER) == com.legend.edge.CompiledPureNumbers.INSTANCE,
                "the edge equals an Integer and a Decimal as compiled legend-pure does");
        assertTrue(PlanStyle.PRODUCT.twoForm(com.legend.testplan.RenderedTextRule.class,
                com.legend.testplan.LiteRenderedTextRule.STRINGS)
                == com.legend.testplan.LiteRenderedTextRule.STRINGS, "the product's two strings are the verdict");
        assertTrue(com.legend.edge.PureText.UPSTREAM.twoForm(com.legend.testplan.RenderedTextRule.class,
                com.legend.testplan.LiteRenderedTextRule.STRINGS)
                == com.legend.edge.UpstreamRenderedTextRule.INSTANCE,
                "the edge plans upstream's ruled comparison where the strings differ");
        assertEquals("2020-01-02", PlanStyle.PRODUCT.twoForm(com.legend.testplan.DateLiteralTexts.class,
                com.legend.testplan.LiteDateLiteralTexts.LITE).text(com.legend.values.PureDateLiteral.parse("2020-01-02")),
                "the product writes lite's date text");
        assertEquals("%2020-01-02", com.legend.edge.PureText.UPSTREAM.twoForm(com.legend.testplan.DateLiteralTexts.class,
                com.legend.testplan.LiteDateLiteralTexts.LITE).text(com.legend.values.PureDateLiteral.parse("2020-01-02")),
                "the edge writes legend-pure's");
        assertFalse(PlanStyle.PRODUCT.twoForm(com.legend.compiler.spec.GoldenNullWord.class,
                com.legend.compiler.spec.LiteGoldenNullWord.NONE).isNullWord("TDSNull"), "the product reads no TDSNull");
        assertTrue(com.legend.edge.PureText.UPSTREAM.twoForm(com.legend.compiler.spec.GoldenNullWord.class,
                com.legend.compiler.spec.LiteGoldenNullWord.NONE).isNullWord("TDSNull"), "the edge reads upstream's");
        // Printing's members (J7/K12 of the edge audit, 2026-11-17): the product answers lite's form; the edge its own,
        // the server routes' rules a third form of the edge's (edge.TestDataRoutes.ROUTE), never a boolean in the product
        assertTrue(PlanStyle.PRODUCT.twoForm(com.legend.testdatagen.TestDataPlanner.Printing.class,
                com.legend.testdatagen.LitePrinting.INSTANCE)
                == com.legend.testdatagen.LitePrinting.INSTANCE, "the product prints the generator's text its way");
        Object edgePrinting = com.legend.edge.PureText.UPSTREAM.twoForm(
                com.legend.testdatagen.TestDataPlanner.Printing.class, com.legend.testdatagen.LitePrinting.INSTANCE);
        assertTrue(edgePrinting != com.legend.testdatagen.LitePrinting.INSTANCE
                && edgePrinting.getClass().getPackageName().equals("com.legend.edge"),
                "the edge answers its own generator text: " + edgePrinting.getClass());
        assertTrue(com.legend.edge.TestDataRoutes.ROUTE.getClass().getPackageName().equals("com.legend.edge"),
                "the server routes' form is the edge's");
        assertTrue(PlanStyle.PRODUCT.twoForm(com.legend.lowering.LoweringForms.class,
                com.legend.lowering.ProductLoweringForms.INSTANCE) == com.legend.lowering.ProductLoweringForms.INSTANCE,
                "the product lowers in its own forms");
        assertTrue(PlanStyle.PRODUCT.twoForm(com.legend.sql.dialect.DatabaseSettingsForm.class,
                com.legend.sql.dialect.ProductDatabaseSettings.INSTANCE) == com.legend.sql.dialect.ProductDatabaseSettings.INSTANCE,
                "the product's database has its own settings");
        Object edgeSettings = com.legend.edge.PureText.UPSTREAM.twoForm(com.legend.sql.dialect.DatabaseSettingsForm.class,
                com.legend.sql.dialect.ProductDatabaseSettings.INSTANCE);
        assertTrue(edgeSettings != com.legend.sql.dialect.ProductDatabaseSettings.INSTANCE
                && edgeSettings.getClass().getPackageName().equals("com.legend.edge"), "the edge's database has legend-engine's");
        Object edgeForms = com.legend.edge.PureText.UPSTREAM.twoForm(com.legend.lowering.LoweringForms.class,
                com.legend.lowering.ProductLoweringForms.INSTANCE);
        assertTrue(edgeForms != com.legend.lowering.ProductLoweringForms.INSTANCE
                && edgeForms.getClass().getPackageName().equals("com.legend.edge"),
                "the edge answers its own lowering forms: " + edgeForms.getClass());
        assertFalse(PlanStyle.PRODUCT.twoForm(com.legend.testplan.FailureWords.class,
                com.legend.testplan.LiteFailureWords.LITE).upstreamWords(), "the product plans lite's words");
        assertTrue(com.legend.edge.PureText.UPSTREAM.twoForm(com.legend.testplan.FailureWords.class,
                com.legend.testplan.LiteFailureWords.LITE).upstreamWords(), "the edge plans legend-pure's");
        Runnable productForm = () -> { };
        assertTrue(PlanStyle.PRODUCT.twoForm(Runnable.class, productForm) == productForm,
                "the product answers the caller's own form");
        NotImplementedException refused = assertThrows(NotImplementedException.class,
                () -> com.legend.edge.PureText.UPSTREAM.twoForm(Runnable.class, productForm));
        assertTrue(refused.getMessage().contains("none of the declared kinds"), refused.getMessage());
        assertThrows(NullPointerException.class, () -> PlanStyle.PRODUCT.twoForm(Runnable.class, null),
                "no default: the caller passes the product form");
    }

    /** (b): the product's style refuses each way of reaching the hook, by name, pointing to the edge. */
    /** The hooks' capabilities decide, never a null or the rules (census C1, the orchestrator's ruling of 2026-11-17):
     *  the product's edge hook records no ledger and the edge's does; the product's run hook compares both isolated
     *  judges' words (lite's sentences), the edge's the verdicts alone (the words upstream's, the host's). */
    @Test
    void theHooksCapabilitiesAreTheProductsAndTheEdges() {
        assertFalse(com.legend.testexecutionplan.NoEdge.NONE.records(), "the product records no ledger");
        assertTrue(com.legend.testexecutionplan.NoEdge.NONE.rules()
                == com.legend.testexecutionplan.TestPlan.JudgingRules.LITE, "the product's runs judge by lite's rules");
        assertTrue(new com.legend.edge.CompatEdge(new com.legend.edge.RawLedger.Recorder(),
                com.legend.executionplan.DataStores.none()).records(), "the edge's recorder records the ledger");
        assertTrue(com.legend.testexec.NoEdgeRun.NONE.wordsAlike(), "lite's judges write alike");
        assertTrue(productHearing().renderedTexts() == com.legend.testexec.LiteRenderedTexts.STRINGS,
                "lite's runs hold two differing strings as differing");
        assertTrue(productHearing().hostForms() == com.legend.testexec.LiteHostForms.STRICT,
                "lite's runs never equal two number kinds");
        assertFalse(com.legend.edge.AppealRun.with(null).wordsAlike(), "the edge's words are the host's alone");
        assertTrue(com.legend.ExecuteOptions.NONE.edge() == com.legend.testexecutionplan.NoEdge.NONE,
                "the product's options carry the product's edge hook, never a null");
    }

    /**
     * NO PRODUCT DOOR IS PLANNED IN THE EDGE'S STYLE (census C1's 2c handover, the orchestrator's ruling of 2026-11-17).
     * Where each door's style is chosen:
     * <ul>
     * <li>{@code Execution.run}/{@code runOnProjectDatabases} and {@code TypedQuery.executionPlan}/{@code projectPlan}:
     * the overloads without a style plan with {@code PlanStyle.PRODUCT}; those taking one are called from the edge alone
     * ({@code ArchitectureTest.onlyTheEdgeCallsTheSpellingDoors}, bitten by a planted caller);</li>
     * <li>{@code TestExecution}'s doors: the style the caller's {@code ExecuteOptions} carry -- {@code NONE}'s is the
     * product's, every derivation but {@code withStyle} keeps it (pinned here), and {@code withStyle} is a spelling door
     * the edge alone calls (the same rule);</li>
     * <li>the hosts (the server's runner, {@code ConnectionResolver}; the test runner {@code PureTestRunner}, whose
     * options the caller hands it): no product class can name an edge style at all
     * ({@code ArchitectureTest.noProductClassReachesTheEdge}, G5, and {@code theHostsDoorsReachNoEdge...}); the
     * product's one style is {@code PlanStyle.PRODUCT}, every other implementer the edge's (pinned here).</li>
     * </ul>
     */
    @Test
    void noProductDoorIsPlannedInTheEdgesStyle() {
        JavaClasses all = classes("legend.product.jars", "legend.edge.jars");
        TreeSet<String> outsideTheEdge = new TreeSet<>();
        for (JavaClass c : all) {
            if (!c.isInterface() && c.isAssignableTo(PlanStyle.class.getName())
                    && !c.getPackageName().startsWith("com.legend.edge")) {
                outsideTheEdge.add(c.getName());
            }
        }
        // the product's one style, PlanStyle.PRODUCT (an anonymous class of the interface)
        assertEquals(new TreeSet<>(List.of(PlanStyle.PRODUCT.getClass().getName())), outsideTheEdge,
                "a style outside the edge other than the product's own");
        com.legend.ExecuteOptions none = com.legend.ExecuteOptions.NONE;
        for (com.legend.ExecuteOptions derived : List.of(none,
                none.withJudgeMode(com.legend.testplan.PlanningOptions.JudgeMode.DATABASE),
                none.withFeatures(java.util.Set.of()), none.withOptions(java.util.Set.of()),
                none.judgedAs(none))) {
            assertTrue(derived.planning().style() == PlanStyle.PRODUCT, "a derivation left the product's style");
        }
    }

    @Test
    void theProductRefusesEachRuledHookByName() {
        RULED.forEach((hook, calls) -> calls.forEach(call -> {
            NotImplementedException e = assertThrows(NotImplementedException.class, call, hook.getName());
            assertTrue(String.valueOf(e.getMessage()).contains("compatibility edge")
                    && e.getMessage().contains("com.legend.edge"),
                    hook + ": " + e.getMessage());
        }));
    }

    /** (c) and (d): every implementer of a ruled hook, in the product's and the edge's jars, is an edge class -- pinned
     *  by name, so a new one is seen. */
    @Test
    void everyImplementerOfARuledHookIsTheEdges() {
        JavaClasses all = classes("legend.product.jars", "legend.edge.jars");
        TreeSet<String> implementers = new TreeSet<>();
        for (Class<?> hook : RULED.keySet()) {
            for (JavaClass c : all) {
                if (!c.getName().equals(hook.getName()) && c.isAssignableTo(hook.getName())
                        && !PRODUCT_REFUSALS.contains(c.getName())) {
                    assertTrue(c.getPackageName().startsWith("com.legend.edge"),
                            c.getName() + " implements " + hook.getName() + " outside the edge");
                    implementers.add(hook.getSimpleName() + " <- " + c.getName());
                }
            }
        }
        assertEquals(new TreeSet<>(List.of(
                "EdgeHearing <- com.legend.edge.Appeals",
                "EdgeRun <- com.legend.edge.AppealRun",
                "Edge <- com.legend.edge.CompatEdge",
                "PureOnlyTexts <- com.legend.edge.EdgePureOnlyTexts",
                "PureWords <- com.legend.edge.EdgePureWords",
                "RelationTexts <- com.legend.edge.RelationTextLowering",
                "ResultEnvelopes <- com.legend.edge.EngineResultEnvelopes",
                "UpstreamPlans <- com.legend.edge.EnginePlans$1",
                "EngineTestText <- com.legend.edge.EdgeEngineTestText",
                "TextCollector <- com.legend.edge.SqlTextVerdicts$Deferring",
                "TextCollector <- com.legend.edge.SqlTextVerdicts$Collecting",
                "TextCollector <- com.legend.edge.SqlTextVerdicts$BatchTexts",
                "BatchTexts <- com.legend.edge.SqlTextVerdicts$BatchTexts")), implementers);
        assertFalse(implementers.isEmpty());
        // the product's refusals are the ones named, and each implements a ruled hook
        for (String refusal : PRODUCT_REFUSALS) {
            JavaClass c = all.get(refusal);
            assertTrue(RULED.keySet().stream().anyMatch(h -> c.isAssignableTo(h.getName())), refusal);
        }
    }

    /** THE RENDERER'S SPELLING HOOKS (census B9, ruling (d) of 2026-11-17): a product dialect's protected method the
     *  compatibility edge's dialects override to spell upstream's text, each declared once in the product
     *  ({@code com.legend.sql.dialect}) with lite's own default. A spelling hook is terminal when its product default is
     *  pinned ({@link #eachSpellingHooksProductDefault}) and every override outside its declaring class is an edge
     *  class ({@link #onlyTheEdgeOverridesASpellingHook}). Named by method; each one's product declarer is found. */
    static final List<String> SPELLING_HOOKS = List.of("raw", "spellingPasses", "raiseMessage", "scanOrderPasses",
            "dateTimeFormat");

    /** (c) for the spelling hooks: every class of the product's and the edge's jars that overrides one, other than its
     *  one product declarer, is an edge class. */
    @Test
    void onlyTheEdgeOverridesASpellingHook() {
        JavaClasses all = classes("legend.product.jars", "legend.edge.jars");
        TreeSet<String> overriders = new TreeSet<>();
        for (String name : SPELLING_HOOKS) {
            List<JavaClass> declarers = new java.util.ArrayList<>();
            for (JavaClass c : all) {
                boolean declares = c.getMethods().stream().anyMatch(m -> m.getOwner().equals(c)
                        && m.getName().equals(name) && m.isAnnotatedWith(com.legend.sql.dialect.SpellingArm.class));
                if (declares && c.getPackageName().equals("com.legend.sql.dialect")) {
                    declarers.add(c);
                }
            }
            assertEquals(1, declarers.size(), name + " is declared once in the product: " + declarers);
            JavaClass declarer = declarers.get(0);
            for (JavaClass c : all) {
                if (c.equals(declarer) || !c.isAssignableTo(declarer.getName())) {
                    continue;
                }
                if (c.getMethods().stream().anyMatch(m -> m.getOwner().equals(c) && m.getName().equals(name))) {
                    assertTrue(c.getPackageName().startsWith("com.legend.edge"),
                            c.getName() + " overrides the spelling hook " + name + " outside the edge");
                    overriders.add(name + " <- " + c.getName());
                }
            }
        }
        assertFalse(overriders.isEmpty());
    }

    /** The product's default of each spelling hook, pinned on every product dialect that has it: a raw statement as
     *  written, no spelling pass, no scan order, and a DateTime format of lite's own (six digits and Z). */
    @Test
    void eachSpellingHooksProductDefault() throws Exception {
        for (com.legend.model.ConnectionDefinition.DatabaseType t : com.legend.database.Databases.implemented()) {
            com.legend.sql.dialect.SqlDialect d = com.legend.database.Databases.dialect(t);
            assertEquals("Create Table f(x FLOAT)", d.raw("Create Table f(x FLOAT)", "ENGINE_TEXT"),
                    t + ": the product runs a raw statement as written");
            for (String name : List.of("spellingPasses", "scanOrderPasses")) {
                Object passes = invoke(d, name);
                if (passes != NONE) {
                    assertEquals(List.of(), passes, t + ": the product's " + name + " adds none");
                }
            }
            Object format = invoke(d, "dateTimeFormat");
            if (format != NONE) {
                assertTrue(String.valueOf(format).endsWith("Z") && !String.valueOf(format).contains("+0000"),
                        t + ": the product's DateTime format is lite's, " + format);
            }
        }
    }

    private static final Object NONE = new Object();

    /** {@code name()} of {@code target}, declared on its class or a superclass; {@link #NONE} where it has none. */
    private static Object invoke(Object target, String name) throws Exception {
        for (Class<?> k = target.getClass(); k != null; k = k.getSuperclass()) {
            for (Method m : k.getDeclaredMethods()) {
                if (m.getName().equals(name) && m.getParameterCount() == 0) {
                    m.setAccessible(true);
                    return m.invoke(target);
                }
            }
        }
        return NONE;
    }

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
        return new ClassFileImporter().importJars(jars.toArray(new java.util.jar.JarFile[0]));
    }

    /** The raises {@code style}'s lowering forms write plain ({@code LoweringForms#raise}), asked kind by kind. */
    private static java.util.Set<com.legend.sql.RaiseKind> plainOf(PlanStyle style) {
        com.legend.lowering.LoweringForms forms = style.twoForm(com.legend.lowering.LoweringForms.class,
                com.legend.lowering.ProductLoweringForms.INSTANCE);
        com.legend.sql.SqlExpr checked = new com.legend.sql.SqlExpr.StringLit("checked");
        com.legend.sql.SqlExpr plain = new com.legend.sql.SqlExpr.StringLit("plain");
        java.util.Set<com.legend.sql.RaiseKind> out = java.util.EnumSet.noneOf(com.legend.sql.RaiseKind.class);
        for (com.legend.sql.RaiseKind k : com.legend.sql.RaiseKind.values()) {
            if (forms.raise(k, () -> checked, plain) == plain) {
                out.add(k);
            }
        }
        return out;
    }
}
