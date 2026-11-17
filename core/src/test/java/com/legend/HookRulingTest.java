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
                    () -> productHearing().engineText(null, List.of(), null, false),
                    () -> productHearing().rowAppealed(0, List.of(), false)),
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
                    () -> PlanStyle.PRODUCT.facet(com.legend.lowering.PureOnlyTexts.class)));

    /** The product's refusals of a ruled hook (criterion (b)): its own implementations, which refuse by name, pinned. */
    static final java.util.Set<String> PRODUCT_REFUSALS = java.util.Set.of(
            "com.legend.testexec.NoEdgeRun", "com.legend.testexec.NoEdgeRun$Hearing");

    /** A test plan naming one appeal, by its verdict's number: what the product's run refuses to open. */
    private static com.legend.testexecutionplan.TestPlan planNaming() {
        return plan(Map.of(0, List.of(new com.legend.testexecutionplan.Extension() { })));
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
        return new com.legend.testexecutionplan.TestPlan(List.of(), List.of(),
                com.legend.testexecutionplan.TestPlan.Judge.HOST, Map.of(), byVerdict, false, target,
                new com.legend.testexecutionplan.BodyFacts(false, false, true, "A"));
    }


    /** (a): an interface declaring signatures only -- no static method, no default method, no field. */
    @Test
    void eachRuledHookIsAnInterfaceOfSignaturesOnly() {
        for (Class<?> hook : RULED.keySet()) {
            assertTrue(hook.isInterface(), hook + " is no interface");
            for (Method m : hook.getDeclaredMethods()) {
                assertTrue(Modifier.isAbstract(m.getModifiers()) && !Modifier.isStatic(m.getModifiers())
                        && !m.isDefault(), hook.getSimpleName() + "." + m.getName() + " is not a bare signature");
            }
            assertEquals(0, hook.getDeclaredFields().length, hook + " declares a field");
            assertEquals(0, hook.getDeclaredClasses().length, hook + " declares a nested type");
        }
    }

    /** The closed list of the plan style's facet kinds (the orchestrator's ruling of 2026-11-17: a registry meets (a)
     *  when it holds only the kinds' class literals): a private constructor and one field, KINDS, a list of interfaces;
     *  no method, no nested type. */
    @Test
    void theFacetRegistryHoldsNothingButItsList() {
        Class<?> registry = com.legend.testplan.PlanStyleFacets.class;
        assertEquals(0, registry.getDeclaredMethods().length, "PlanStyleFacets declares a method");
        assertEquals(0, registry.getDeclaredClasses().length, "PlanStyleFacets declares a nested type");
        assertEquals(List.of("KINDS"), java.util.Arrays.stream(registry.getDeclaredFields())
                .filter(f -> !f.isSynthetic()).map(java.lang.reflect.Field::getName).toList());
        for (java.lang.reflect.Constructor<?> c : registry.getDeclaredConstructors()) {
            assertTrue(Modifier.isPrivate(c.getModifiers()) && c.getParameterCount() == 0, c.toString());
        }
        for (Class<?> kind : com.legend.testplan.PlanStyleFacets.KINDS) {
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
                "ResultEnvelopes <- com.legend.edge.EngineResultEnvelopes",
                "UpstreamPlans <- com.legend.edge.EnginePlans$1")), implementers);
        assertFalse(implementers.isEmpty());
        // the product's refusals are the ones named, and each implements a ruled hook
        for (String refusal : PRODUCT_REFUSALS) {
            JavaClass c = all.get(refusal);
            assertTrue(RULED.keySet().stream().anyMatch(h -> c.isAssignableTo(h.getName())), refusal);
        }
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
