// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend;

import com.legend.database.PlanStyle;
import com.tngtech.archunit.core.domain.JavaClass;
import com.tngtech.archunit.core.domain.JavaCodeUnit;
import com.tngtech.archunit.core.importer.ClassFileImporter;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * A HOOK IMPLEMENTED BY A LAMBDA IS STILL AN IMPLEMENTATION (the GUARD stream, step 2's C.12; the edge audit of
 * 2026-11-17, section C.12: "single-method hooks (lambdas) and twoForm's callers pinned"). HookRulingTest finds a ruled
 * hook's implementers, and PlanStyleSurfaceTest a style's, as CLASSES: a lambda or a method reference implementing a
 * single-method hook ({@code x -> ...} typed as {@code DateTimeTexts}) makes no class, so a product body could answer
 * upstream's form through one unseen. Each invokedynamic's call-site type is read from the pool
 * ({@link ConstantPools.Pool#indyTypes}); every product class (the product's and the hosts' jars, the edge's aside) that
 * makes a lambda of a hook type -- a ruled hook, a facet kind, a two-form kind, or PlanStyle itself -- is pinned with
 * those types, exact ({@link #LAMBDA_HOOKS}). And every product method that asks a style for a two-form kind
 * ({@code PlanStyle.twoForm}) is pinned with the class objects it names, exact ({@link #TWO_FORM_CALLERS}).
 */
@Tag("guardrail")
class HookLambdasTest {

    /** Product class (top) -> the hook types its lambdas implement. Measured 2026-11-17. */
    static final Map<String, Set<String>> LAMBDA_HOOKS = Map.ofEntries(
            // two-form kinds' PRODUCT forms, written as lambdas (no ruled hook, facet kind or style is implemented by a
            // product lambda): a DateTime's text, lite's (census B9); a test-data timestamp's, lite's clean form (L9)
            Map.entry("com.legend.lowering.Styled", Set.of("com.legend.lowering.DateTimeTexts")),
            Map.entry("com.legend.testdatagen.TestDataPlanner", Set.of("com.legend.testdatagen.TestDataPlanner$Printing")));

    /** "caller#method" -> the class objects named beside its twoForm call. Measured 2026-11-17. */
    static final Map<String, Set<String>> TWO_FORM_CALLERS = Map.ofEntries(
            // the lowering's style: a DateTime's and a list's texts (and the PureOnlyTexts facet beside them)
            Map.entry("com.legend.planner.PlanEnv#styled()", Set.of("com.legend.lowering.DateTimeTexts",
                    "com.legend.lowering.ListTexts", "com.legend.lowering.PureOnlyTexts")),
            // the database judge: a JSON golden's grammar (S144), two number kinds (S127)
            Map.entry("com.legend.testplan.DatabaseJudge#jsonStringsEqual(java.lang.String, java.util.List,"
                    + " java.util.List, com.legend.compiler.spec.SpecCompiler, com.legend.testplan.BodyDriver$ExecEnv,"
                    + " com.legend.testplan.AssertVerdicts$SpliceHook)", Set.of("com.legend.testexecutionplan.JsonGrammar")),
            Map.entry("com.legend.testplan.DatabaseJudge#numbers(com.legend.testplan.BodyDriver$ExecEnv)",
                    Set.of("com.legend.testplan.CrossKindNumbers")),
            // the test planner: a relation counted by an assert (S137); a test-data timestamp's text (L9)
            Map.entry("com.legend.testplan.TestPlanner#lifted(com.legend.TypedQuery, com.legend.testplan.PlanningOptions)",
                    Set.of("com.legend.compiler.spec.RelationCounting", "com.legend.platform.PlanOption")),
            Map.entry("com.legend.testplan.TestPlanner#plan(com.legend.TypedQuery, java.lang.String,"
                    + " com.legend.testplan.PlanningOptions)", Set.of("com.legend.testdatagen.TestDataPlanner$Printing")));

    /** The hook types: the ruled hooks, the facet kinds, the two-form kinds, the plan style. */
    static Set<String> hookTypes() {
        Set<String> out = new TreeSet<>();
        HookRulingTest.RULED.keySet().forEach(c -> out.add(c.getName()));
        com.legend.testplan.PlanStyleFacets.KINDS.forEach(c -> out.add(c.getName()));
        com.legend.testplan.PlanStyleFacets.TWO_FORMS.forEach(c -> out.add(c.getName()));
        out.add(PlanStyle.class.getName());
        return out;
    }

    private static String top(String n) {
        int dollar = n.indexOf('$');
        return dollar < 0 ? n : n.substring(0, dollar);
    }

    /** Each class of {@code pools} (the edge's aside) making a lambda of one of {@code hooks}, by top class. */
    static Map<String, Set<String>> lambdaHooks(Map<String, ConstantPools.Pool> pools, Set<String> hooks) {
        Map<String, Set<String>> out = new TreeMap<>();
        pools.forEach((name, pool) -> {
            if (name.startsWith("com.legend.edge.")) {
                return;
            }
            for (String t : pool.indyTypes()) {
                if (hooks.contains(t)) {
                    out.computeIfAbsent(top(name), k -> new TreeSet<>()).add(t);
                }
            }
        });
        return out;
    }

    /** Each method of {@code classes} (the edge's aside) calling twoForm on a style, with the class objects it names. */
    static Map<String, Set<String>> twoFormCallers(Iterable<JavaClass> classes) {
        Map<String, Set<String>> out = new TreeMap<>();
        for (JavaClass c : classes) {
            if (c.getName().startsWith("com.legend.edge.")) {
                continue;
            }
            for (JavaCodeUnit u : c.getCodeUnits()) {
                // a call or a method reference, each method by its full signature (the review of the loader sites'
                // twin, 9702998c08, 2026-11-17: a reference was unseen, overloads shared a key)
                boolean asks = u.getMethodCallsFromSelf().stream().anyMatch(call -> call.getName().equals("twoForm")
                        && call.getTargetOwner().isAssignableTo(PlanStyle.class.getName()))
                        || u.getMethodReferencesFromSelf().stream().anyMatch(ref -> ref.getName().equals("twoForm")
                        && ref.getTargetOwner().isAssignableTo(PlanStyle.class.getName()));
                if (asks) {
                    Set<String> objects = new TreeSet<>();
                    u.getReferencedClassObjects().forEach(o -> objects.add(o.getRawType().getName()));
                    out.put(ServiceLoaderSitesTest.key(c, u), objects);
                }
            }
        }
        return out;
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

    @Test
    void theProductsHookLambdasArePinned() {
        Map<String, Set<String>> found = lambdaHooks(ConstantPools.poolsOfJars(ConstantPools.jars("legend.product.jars",
                "legend.authored.jars")), hookTypes());
        assertEquals(new TreeMap<>(LAMBDA_HOOKS), found, "a product class implements a hook type by a lambda or a method"
                + " reference beyond its pin: a ruled hook, a facet kind or a style is the edge's to implement; a"
                + " two-form kind's product form is pinned here with its reason");
    }

    @Test
    void theTwoFormCallersArePinned() {
        Map<String, Set<String>> found = twoFormCallers(importJars(ConstantPools.jars("legend.product.jars",
                "legend.authored.jars")));
        assertEquals(new TreeMap<>(TWO_FORM_CALLERS), found, "a product method asks a style for a two-form kind beyond"
                + " its pin");
    }

    /** Planted: lambdas of a hook type are found (lambdas of other functional types are not); a twoForm call is found
     *  with the kind it names. */
    @Test
    void theGuardBites() {
        Map<String, ConstantPools.Pool> pools = new TreeMap<>();
        for (Class<?> c : List.of(PlantedHookLambda.class)) {
            pools.put(c.getName(), ConstantPools.pool(ConstantPools.bytesOf(c)));
        }
        String top = HookLambdasTest.class.getName();
        assertEquals(Map.of(top, Set.of("com.legend.lowering.DateTimeTexts")), lambdaHooks(pools, hookTypes()),
                "the hook lambdas are read; a Supplier, a Function or a Runnable of other types is no hook");
        assertEquals(Map.of(PlantedHookLambda.class.getName() + "#ask(com.legend.database.PlanStyle)",
                Set.of("com.legend.lowering.DateTimeTexts")), twoFormCallers(new ClassFileImporter()
                .importClasses(PlantedHookLambda.class)));
    }

    /** A planted product body: a two-form kind by lambda (twice), lambdas of other functional types, and a twoForm
     *  call. */
    @SuppressWarnings("unused")
    static final class PlantedHookLambda {
        static com.legend.lowering.DateTimeTexts text() {
            return (x, written) -> x;
        }

        static java.util.function.Supplier<PlanStyle> style() {
            return () -> PlanStyle.PRODUCT;
        }

        static PlanStyle styleRef(java.util.function.Function<String, PlanStyle> f) {
            return f.apply("x");
        }

        static PlanStyle asStyle() {
            java.util.function.Function<String, PlanStyle> f = s -> PlanStyle.PRODUCT;
            return styleRef(f);
        }

        static Runnable plain() {
            return () -> { };
        }

        static com.legend.lowering.DateTimeTexts ask(PlanStyle style) {
            return style.twoForm(com.legend.lowering.DateTimeTexts.class, (x, written) -> x);
        }
    }
}
