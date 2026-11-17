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
    static final Map<Class<?>, List<Executable>> RULED = Map.of(
            // census C2, 2026-11-17 (the orchestrator's ruling that day: NoEdgeRun is the product's refusal, by name):
            // the edge's run hook, and the hearing it opens
            com.legend.testexec.EdgeRun.class, List.of(
                    () -> com.legend.testexec.NoEdgeRun.NONE.open(planNaming(), null, java.util.UUID.randomUUID())),
            com.legend.testexec.EdgeHearing.class, List.of(
                    () -> productHearing().judged(null, List.of(), null, false),
                    () -> productHearing().rowAppealed(0, List.of(), false),
                    () -> productHearing().pureWords()),
            // C5's step B, 2026-11-17 (the orchestrator's ruling (1)): legend-pure's words of an assert's failure at run,
            // the hearing's member (NoEdgeRun's hearing refuses it by name)
            com.legend.testexec.PureWords.class, List.of(
                    () -> productHearing().pureWords()),
            // census B3 and B1 (EngineTextDialect and PlanStyle's signatures naming it): CLOSED 2026-11-17 by Phase 6 --
            // the interface is com.legend.edge's and the style names no engine text; its entry gone
            // census B2, 2026-11-17 (Phase 6): upstream's plan text, the planner's facet -- signatures only since its
            // routine table moved to planner.PlanTextRoutines
            com.legend.planner.UpstreamPlans.class, List.of(
                    () -> PlanStyle.PRODUCT.facet(com.legend.planner.UpstreamPlans.class)),
            // census B5, 2026-11-17 (Phase 6): legend-engine's result envelope, composed at lowering
            com.legend.lowering.ResultEnvelopes.class, List.of(
                    () -> PlanStyle.PRODUCT.facet(com.legend.lowering.ResultEnvelopes.class)),
            // census B8 (i), 2026-11-17: the texts only legend-pure writes, composed at lowering
            com.legend.lowering.PureOnlyTexts.class, List.of(
                    () -> PlanStyle.PRODUCT.facet(com.legend.lowering.PureOnlyTexts.class)),
            // census B17, 2026-11-17: a relation's legend-pure text, composed at lowering (a facade: FACADES)
            com.legend.lowering.RelationTexts.class, List.of(
                    () -> PlanStyle.PRODUCT.facet(com.legend.lowering.RelationTexts.class)),
            // census C4, 2026-11-17: the engine-text assert's arm, and the two hooks only its answers hand out (the
            // collector and the database judge's batch): each reached through the facet the product refuses
            com.legend.testplan.EngineTestText.class, List.of(
                    () -> PlanStyle.PRODUCT.facet(com.legend.testplan.EngineTestText.class)),
            com.legend.testplan.TextCollector.class, List.of(
                    () -> PlanStyle.PRODUCT.facet(com.legend.testplan.EngineTestText.class)),
            com.legend.testplan.BatchTexts.class, List.of(
                    () -> PlanStyle.PRODUCT.facet(com.legend.testplan.EngineTestText.class)));

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
            "com.legend.testexec.NoEdgeRun", "com.legend.testexec.NoEdgeRun$Hearing");

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
                new com.legend.testexecutionplan.BodyFacts(false, false, true, "A"), null);
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
        // the raises a plan writes plain (B7, 2026-11-17): the product's none, PCT's and the corpus's none (legend-pure's
        // guarded words), the pure/v1 route's legend-engine's census class A
        assertEquals(java.util.Set.of(), PlanStyle.PRODUCT.plainRaises(), "the product writes every check of its own");
        assertEquals(java.util.Set.of(), com.legend.edge.PureText.UPSTREAM.plainRaises(), "PCT keeps legend-pure's words");
        assertEquals(java.util.EnumSet.of(com.legend.sql.RaiseKind.DIVIDE_BY_ZERO, com.legend.sql.RaiseKind.SQRT_OF_NEGATIVE,
                com.legend.sql.RaiseKind.SHIFT_OUT_OF_RANGE, com.legend.sql.RaiseKind.SIZE_NOT_MULTIPLICITY,
                com.legend.sql.RaiseKind.SLICE_BOUNDS_INVERTED, com.legend.sql.RaiseKind.RANGE_STEP_ZERO,
                com.legend.sql.RaiseKind.CAST_IMPOSSIBLE, com.legend.sql.RaiseKind.TEXT_NOT_INTEGER,
                com.legend.sql.RaiseKind.TEXT_NOT_FLOAT, com.legend.sql.RaiseKind.TEXT_NOT_DATE),
                com.legend.edge.PureText.PURE_V1.plainRaises(), "pure/v1 writes legend-engine's unchecked raises plain");
    }

    /** PlanStyle.twoForm (the orchestrator's ruling of 2026-11-17, census B9): its kinds are a CLOSED list, pinned here
     *  as an exact set (empty until the first family adds its kind, with a dated reason); the product's style answers the
     *  caller's own product form, whatever the kind; the edge's refuses a kind outside the list by name. */
    @Test
    void theTwoFormKindsAreAClosedListTheProductAnswersItsOwnForm() {
        // the two-form kinds, an exact set; each added with its family and its dated reason:
        // DateTimeTexts (census B9, 2026-11-17: a DateTime's text in a message); ListTexts (census B8, 2026-11-17,
        // TEXT_FORMS: a list's text, the product's JSON form, the edge's legend-pure one); JsonGrammar (census C1,
        // 2026-11-17, register S144: a judge's JSON grammar, the product's strict JSON, the edge's json-simple)
        assertEquals(List.of(com.legend.lowering.DateTimeTexts.class, com.legend.lowering.ListTexts.class,
                com.legend.testexecutionplan.JsonGrammar.class),
                com.legend.testplan.PlanStyleFacets.TWO_FORMS,
                "the two-form kinds, an exact set: a kind is added here with its family and its dated reason");
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
}
