// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend;

import com.legend.database.PlanStyle;
import com.tngtech.archunit.core.domain.JavaClass;
import com.tngtech.archunit.core.domain.JavaClasses;
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
 * those types, exact ({@link #LAMBDA_HOOKS}), a sub-interface of a hook type a hook type too. Every product class
 * implementing or extending a hook type is pinned with them ({@link #CLASS_HOOKS}: no other guard held a two-form kind's
 * implementers), and no product method makes an interface by {@code LambdaMetafactory} or {@code MethodHandleProxies}
 * directly ({@link #DIRECT_FACTORIES}; a proxy is ServiceLoaderSitesTest's). And every product method that asks a style
 * for a two-form kind ({@code PlanStyle.twoForm}) is pinned with the class objects it names and its number of reaches,
 * exact ({@link #TWO_FORM_CALLERS}). The review of C.12 and C.13, 2026-11-17, widened all four.
 */
@Tag("guardrail")
class HookLambdasTest {

    /** Product class (top) -> the hook types its lambdas implement, each with its number of call sites (a second lambda
     *  of a pinned type in a pinned class -- upstream's form beside the product's -- changes the number: the review of
     *  C.12, 2026-11-17). Measured 2026-11-17. */
    static final Map<String, Map<String, Integer>> LAMBDA_HOOKS = Map.ofEntries(
            // two-form kinds' PRODUCT forms, written as lambdas (no ruled hook, facet kind or style is implemented by a
            // product lambda): a DateTime's text, lite's (census B9); a test-data timestamp's, lite's clean form (L9)
            Map.entry("com.legend.lowering.Styled", Map.of("com.legend.lowering.DateTimeTexts", 1)),
            // (TestDataPlanner -> Printing: gone 2026-11-17 with K12, a095d0b56 -- Printing is no functional interface now,
            // its product form the class LitePrinting); a routed query's activity, the product's (J5, fb8c299b9e)
            Map.entry("com.legend.planner.PlanAllocations", Map.of("com.legend.planner.RoutedQueries", 1)));

    /** "caller#method(params)" -> the class objects named beside its twoForm call, and its number of reaches (calls and
     *  references). Measured 2026-11-17. */
    static final Map<String, Set<String>> TWO_FORM_CALLERS = Map.ofEntries(
            // the lowering's style: a DateTime's and a list's texts (and the PureOnlyTexts facet beside them)
            Map.entry("com.legend.planner.PlanEnv#styled()", Set.of("com.legend.lowering.DateTimeTexts",
                    "com.legend.lowering.ListTexts", "com.legend.lowering.PureOnlyTexts",
                    // + LoweringForms (K5, train 5)
                    "com.legend.lowering.LoweringForms", "(twoForm reaches 3)")),
            // a routed query's activity rows (J5, fb8c299b9e, train 5)
            Map.entry("com.legend.planner.PlanAllocations#registerActivityRows("
                    + "com.legend.compiler.spec.typed.TypedNativeCall, com.legend.compiler.spec.typed.TypedSpec,"
                    + " java.util.List, com.legend.compiler.spec.SpecCompiler, java.lang.String, java.lang.String,"
                    + " com.legend.planner.PlanEnv)", Set.of("com.legend.planner.RoutedQueries", "(twoForm reaches 1)")),
            // the database judge: a JSON golden's grammar (S144), two number kinds (S127)
            Map.entry("com.legend.testplan.DatabaseJudge#jsonStringsEqual(java.lang.String, java.util.List,"
                    + " java.util.List, com.legend.compiler.spec.SpecCompiler, com.legend.testplan.BodyDriver$ExecEnv,"
                    + " com.legend.testplan.AssertVerdicts$SpliceHook)", Set.of("com.legend.testexecutionplan.JsonGrammar",
                    "(twoForm reaches 1)")),
            // D's train 4 (census C1, 2026-11-17): the rendered golden's null word and the rendered-text rule asked by
            // the database judge's asserts; a date literal's text asked where a written literal is spelled
            Map.entry("com.legend.testplan.AssertVerdicts#renderedRows(java.lang.String, boolean, java.util.List,"
                    + " java.util.List, com.legend.compiler.spec.SpecCompiler, com.legend.testplan.BodyDriver$ExecEnv,"
                    + " com.legend.testplan.AssertVerdicts$SpliceHook, boolean, boolean,"
                    + " com.legend.compiler.spec.VerdictQueries$RenderedSide, java.lang.String)",
                    Set.of("com.legend.compiler.spec.GoldenNullWord", "(twoForm reaches 1)")),
            Map.entry("com.legend.testplan.AssertVerdicts#textFirst(java.lang.String, boolean, com.legend.sql.SqlQuery,"
                    + " com.legend.testplan.BodyDriver$ExecEnv, java.util.function.Supplier)",
                    Set.of("com.legend.testplan.RenderedTextRule", "(twoForm reaches 1)")),
            Map.entry("com.legend.testplan.DatabaseJudge#written(com.legend.lowering.CanonRider, int, boolean,"
                    + " com.legend.sql.SqlQuery, com.legend.compiler.spec.typed.TypedSpec,"
                    + " com.legend.testplan.BodyDriver$ExecEnv)", Set.of("com.legend.testplan.DateLiteralTexts",
                    "(twoForm reaches 1)")),
            Map.entry("com.legend.testplan.DatabaseJudge#numbers(com.legend.testplan.BodyDriver$ExecEnv)",
                    Set.of("com.legend.testplan.CrossKindNumbers", "(twoForm reaches 1)")),
            // the test planner: a relation counted by an assert (S137); a test-data timestamp's text (L9)
            Map.entry("com.legend.testplan.TestPlanner#lifted(com.legend.TypedQuery, com.legend.testplan.PlanningOptions)",
                    Set.of("com.legend.compiler.spec.RelationCounting", "com.legend.platform.PlanOption",
                            "(twoForm reaches 1)")),
            Map.entry("com.legend.testplan.TestPlanner#plan(com.legend.TypedQuery, java.lang.String,"
                    + " com.legend.testplan.PlanningOptions)", Set.of("com.legend.testdatagen.TestDataPlanner$Printing",
                    "(twoForm reaches 1)")));

    /** Product or host class -> the hook types it implements or extends as a class (the ruled hooks', the facet kinds'
     *  and the styles' implementers are HookRulingTest's and PlanStyleSurfaceTest's too; the two-form kinds' product
     *  forms are pinned here alone). Measured 2026-11-17. */
    static final Map<String, Set<String>> CLASS_HOOKS = Map.ofEntries(
            // the two-form kinds' product forms: a relation counted by an assert, refused (S137); a list's text, a JSON
            // array (F's B8); a judge's JSON grammar, strict JSON (S144); two number kinds compared (S127)
            Map.entry("com.legend.compiler.spec.RelationCountAsserts", Set.of("com.legend.compiler.spec.RelationCounting")),
            Map.entry("com.legend.lowering.ProductListTexts", Set.of("com.legend.lowering.ListTexts")),
            Map.entry("com.legend.testexecutionplan.JudgeJson", Set.of("com.legend.testexecutionplan.JsonGrammar")),
            Map.entry("com.legend.testplan.CrossKindNumbers$Product", Set.of("com.legend.testplan.CrossKindNumbers")),
            // trains 5 and 6 (2026-11-17): the lowering forms' product answer, an anonymous class in the kind (K5,
            // fd1df9928, a criterion (a) exception); the test-data printing's product form, its own class (K12, a095d0b56)
            Map.entry("com.legend.lowering.LoweringForms$1", Set.of("com.legend.lowering.LoweringForms")),
            Map.entry("com.legend.testdatagen.LitePrinting", Set.of("com.legend.testdatagen.TestDataPlanner$Printing")),
            // D's train 4 (census C1, 2026-11-17): a rendered golden's null word, none (c013b2565c); a date literal's
            // message text, lite's (4bc94e2d8f); the rendered-text rule, strings (a9c14bb4ca) -- each nested in its kind
            Map.entry("com.legend.compiler.spec.GoldenNullWord$Product", Set.of("com.legend.compiler.spec.GoldenNullWord")),
            Map.entry("com.legend.testplan.DateLiteralTexts$Product", Set.of("com.legend.testplan.DateLiteralTexts")),
            Map.entry("com.legend.testplan.RenderedTextRule$Product", Set.of("com.legend.testplan.RenderedTextRule")),
            // the product's style, PlanStyle.PRODUCT (PlanStyleSurfaceTest's OVERRIDERS)
            Map.entry("com.legend.database.PlanStyle$1", Set.of("com.legend.database.PlanStyle")),
            // the ruled hooks' product answers, each refusing by name (census C1, C2; HookRulingsAgreeTest's
            // RULED_UNCOUNTED): the run hook and its hearing, the test plan's edge
            Map.entry("com.legend.testexec.NoEdgeRun", Set.of("com.legend.testexec.EdgeRun")),
            Map.entry("com.legend.testexec.NoEdgeRun$Hearing", Set.of("com.legend.testexec.EdgeHearing")),
            Map.entry("com.legend.testexecutionplan.NoEdge", Set.of("com.legend.testexecutionplan.Edge")));

    /** The product methods making an interface's instance by LambdaMetafactory or MethodHandleProxies directly: none
     *  (pinned empty, 2026-11-17). */
    static final Set<String> DIRECT_FACTORIES = Set.of();

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

    /** Whether {@code type} is a hook type: one of {@code hooks}, or assignable to one (a sub-interface of a hook is
     *  a hook too: the review of C.12, 2026-11-17). */
    static boolean hook(String type, Set<String> hooks, JavaClasses classes) {
        return hooks.contains(type) || classes.contain(type) && hooks.stream().anyMatch(h -> classes.get(type)
                .isAssignableTo(h));
    }

    /** Each class of {@code pools} (the edge's aside) making a lambda of a hook type, by top class, with the number
     *  of its call sites per type (its nested classes' summed in). */
    static Map<String, Map<String, Integer>> lambdaHooks(Map<String, ConstantPools.Pool> pools, Set<String> hooks,
                                                         JavaClasses classes) {
        Map<String, Map<String, Integer>> out = new TreeMap<>();
        pools.forEach((name, pool) -> {
            if (name.startsWith("com.legend.edge.")) {
                return;
            }
            pool.indySites().forEach((t, n) -> {
                if (hook(t, hooks, classes)) {
                    out.computeIfAbsent(top(name), k -> new TreeMap<>()).merge(t, n, Integer::sum);
                }
            });
        });
        return out;
    }

    /** Each class of {@code pools} (the edge's aside) whose pool names MethodHandleProxies at all -- a call, a class
     *  literal, a reflective lookup (the review of C.12: the call scan alone misses the last two; LambdaMetafactory
     *  cannot be read this way, every lambda's bootstrap names it). */
    static Set<String> proxiesNamed(Map<String, ConstantPools.Pool> pools) {
        Set<String> out = new TreeSet<>();
        pools.forEach((name, pool) -> {
            if (!name.startsWith("com.legend.edge.") && pool.utf8().stream().anyMatch(t -> t.contains(
                    "java/lang/invoke/MethodHandleProxies") || t.contains("java.lang.invoke.MethodHandleProxies"))) {
                out.add(name);
            }
        });
        return out;
    }

    /** Each class of {@code classes} (the edge's aside, the hook types themselves aside) that implements or extends a
     *  hook type, with the hook types it is assignable to. */
    static Map<String, Set<String>> classHooks(JavaClasses classes, Set<String> hooks) {
        Map<String, Set<String>> out = new TreeMap<>();
        for (JavaClass c : classes) {
            if (c.getName().startsWith("com.legend.edge.") || hooks.contains(c.getName())) {
                continue;
            }
            Set<String> to = new TreeSet<>();
            hooks.stream().filter(c::isAssignableTo).forEach(to::add);
            if (!to.isEmpty()) {
                out.put(c.getName(), to);
            }
        }
        return out;
    }

    /** The owners whose methods make an instance of an interface with no class and no lambda the pool shows. */
    static final Set<String> FACTORIES = Set.of("java.lang.invoke.LambdaMetafactory",
            "java.lang.invoke.MethodHandleProxies");

    /** Each method of {@code classes} (the edge's aside) calling or referencing {@link #FACTORIES}' methods directly (a
     *  lambda's bootstrap is no call: the pool's indy types read it). */
    static Set<String> directFactories(Iterable<JavaClass> classes) {
        Set<String> out = new TreeSet<>();
        for (JavaClass c : classes) {
            if (c.getName().startsWith("com.legend.edge.")) {
                continue;
            }
            for (JavaCodeUnit u : c.getCodeUnits()) {
                if (u.getMethodCallsFromSelf().stream().anyMatch(x -> FACTORIES.contains(x.getTargetOwner().getName()))
                        || u.getMethodReferencesFromSelf().stream().anyMatch(x -> FACTORIES.contains(x.getTargetOwner()
                        .getName()))) {
                    out.add(ServiceLoaderSitesTest.key(c, u));
                }
            }
        }
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
                // each with its number of reaches (a second ask in a pinned method changes it: the review of C.12)
                long reaches = u.getMethodCallsFromSelf().stream().filter(call -> call.getName().equals("twoForm")
                        && call.getTargetOwner().isAssignableTo(PlanStyle.class.getName())).count()
                        + u.getMethodReferencesFromSelf().stream().filter(ref -> ref.getName().equals("twoForm")
                        && ref.getTargetOwner().isAssignableTo(PlanStyle.class.getName())).count();
                if (reaches > 0) {
                    Set<String> objects = new TreeSet<>();
                    u.getReferencedClassObjects().forEach(o -> objects.add(o.getRawType().getName()));
                    objects.add("(twoForm reaches " + reaches + ")");
                    out.put(ServiceLoaderSitesTest.key(c, u), objects);
                }
            }
        }
        return out;
    }

    private static JavaClasses importJars(List<java.nio.file.Path> jars) {
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
        List<java.nio.file.Path> jars = ConstantPools.jars("legend.product.jars", "legend.authored.jars");
        Map<String, Map<String, Integer>> found = lambdaHooks(ConstantPools.poolsOfJars(jars), hookTypes(),
                importJars(jars));
        assertEquals(new TreeMap<>(LAMBDA_HOOKS), found, "a product class implements a hook type by a lambda or a method"
                + " reference beyond its pin: a ruled hook, a facet kind or a style is the edge's to implement; a"
                + " two-form kind's product form is pinned here with its reason");
    }

    @Test
    void theProductsHookClassesArePinned() {
        Map<String, Set<String>> found = classHooks(importJars(ConstantPools.jars("legend.product.jars",
                "legend.authored.jars")), hookTypes());
        assertEquals(new TreeMap<>(CLASS_HOOKS), found, "a product or host class implements a hook type beyond its pin:"
                + " a ruled hook, a facet kind or a style is the edge's to implement; a two-form kind's product form is"
                + " pinned here with its reason");
    }

    @Test
    void noProductClassMakesAnInterfaceByAFactory() {
        List<java.nio.file.Path> jars = ConstantPools.jars("legend.product.jars", "legend.authored.jars");
        assertEquals(DIRECT_FACTORIES, directFactories(importJars(jars)), "a product method calls LambdaMetafactory or"
                + " MethodHandleProxies directly: a hook implemented with no class and no lambda the pool shows; pin it"
                + " with its reason");
        assertEquals(Set.of(), proxiesNamed(ConstantPools.poolsOfJars(jars)), "a product class names"
                + " MethodHandleProxies (a class literal, a reflective lookup): pin it with its reason");
    }

    @Test
    void theTwoFormCallersArePinned() {
        Map<String, Set<String>> found = twoFormCallers(importJars(ConstantPools.jars("legend.product.jars",
                "legend.authored.jars")));
        assertEquals(new TreeMap<>(TWO_FORM_CALLERS), found, "a product method asks a style for a two-form kind beyond"
                + " its pin");
    }

    /** Planted: lambdas of a hook type, and of a sub-interface of one, are found (lambdas of other functional types are
     *  not); a class implementing a hook type, and a sub-interface, are found; a direct factory call is found, a lambda's
     *  bootstrap is not; a twoForm call, a method reference to it and an overload are each found, with the kinds named
     *  and the number of reaches. */
    @Test
    void theGuardBites() {
        JavaClasses classes = new ClassFileImporter().importClasses(PlantedHookLambda.class, PlantedSubTexts.class,
                PlantedTwoForm.class, PlantedFactory.class);
        Map<String, ConstantPools.Pool> pools = new TreeMap<>();
        for (Class<?> c : List.of(PlantedHookLambda.class, PlantedHookLambda.Twice.class)) {
            pools.put(c.getName(), ConstantPools.pool(ConstantPools.bytesOf(c)));
        }
        String top = HookLambdasTest.class.getName();
        assertEquals(Map.of(top, Map.of("com.legend.lowering.DateTimeTexts", 4, PlantedSubTexts.class.getName(), 1)),
                lambdaHooks(pools, hookTypes(), classes), "the hook lambdas are read with their sites (text's, ask's and"
                        + " ask(int)'s, the nested Twice's), a sub-interface's too; a Supplier, a Function, a BiFunction"
                        + " or a Runnable of other types is no hook");
        assertEquals(Map.of(PlantedSubTexts.class.getName(), Set.of("com.legend.lowering.DateTimeTexts"),
                PlantedTwoForm.class.getName(), Set.of("com.legend.lowering.ListTexts")), classHooks(classes,
                hookTypes()), "a class implementing a two-form kind and a sub-interface of one are read");
        assertEquals(Set.of(PlantedFactory.class.getName() + "#proxy()", PlantedFactory.class.getName()
                + "#metafactory()", PlantedFactory.class.getName() + "#byReference()"), directFactories(classes),
                "a direct factory call or reference is read; a lambda is not one");
        assertEquals(Set.of(PlantedFactory.class.getName()), proxiesNamed(Map.of(PlantedFactory.class.getName(),
                ConstantPools.pool(ConstantPools.bytesOf(PlantedFactory.class)), PlantedHookLambda.class.getName(),
                ConstantPools.pool(ConstantPools.bytesOf(PlantedHookLambda.class)))), "MethodHandleProxies named in a"
                + " pool is read; a lambda's pool does not name it");
        String p = PlantedHookLambda.class.getName();
        assertEquals(Map.of(p + "#ask(com.legend.database.PlanStyle)", Set.of("com.legend.lowering.DateTimeTexts",
                        "(twoForm reaches 1)"),
                p + "#ask(com.legend.database.PlanStyle, int)", Set.of("com.legend.lowering.DateTimeTexts",
                        "com.legend.lowering.ListTexts", "(twoForm reaches 2)"),
                p + "#byReference(com.legend.database.PlanStyle)", Set.of("(twoForm reaches 1)")),
                twoFormCallers(new ClassFileImporter().importClasses(PlantedHookLambda.class)));
    }

    /** A planted sub-interface of a two-form kind. */
    interface PlantedSubTexts extends com.legend.lowering.DateTimeTexts {
    }

    /** A planted class implementing a two-form kind. */
    abstract static class PlantedTwoForm implements com.legend.lowering.ListTexts {
    }

    /** Planted direct factories: an interface's instance with no class and no lambda. */
    @SuppressWarnings("unused")
    static final class PlantedFactory {
        static Object proxy() {
            return java.lang.invoke.MethodHandleProxies.asInterfaceInstance(Runnable.class, null);
        }

        static Object metafactory() throws Exception {
            return java.lang.invoke.LambdaMetafactory.metafactory(null, null, null, null, null, null);
        }

        static Object byReference() {
            java.util.function.BiFunction<Class<Runnable>, java.lang.invoke.MethodHandle, Runnable> f =
                    java.lang.invoke.MethodHandleProxies::asInterfaceInstance;
            return f.apply(Runnable.class, null);
        }
    }

    /** A planted product body: a two-form kind by lambda (twice) and by a sub-interface's lambda, lambdas of other
     *  functional types, twoForm calls (two overloads) and a reference to twoForm. */
    @SuppressWarnings("unused")
    static final class PlantedHookLambda {
        static com.legend.lowering.DateTimeTexts text() {
            return (x, written) -> x;
        }

        static PlantedSubTexts sub() {
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

        static Object ask(PlanStyle style, int n) {
            return List.of(style.twoForm(com.legend.lowering.DateTimeTexts.class, (x, written) -> x),
                    style.twoForm(com.legend.lowering.ListTexts.class, null));
        }

        static java.util.function.BiFunction<Class<com.legend.lowering.DateTimeTexts>, com.legend.lowering.DateTimeTexts,
                com.legend.lowering.DateTimeTexts> byReference(PlanStyle style) {
            return style::twoForm;
        }

        /** A second lambda of the pinned type in a nested class: its site counted under the top class. */
        static final class Twice {
            static com.legend.lowering.DateTimeTexts again() {
                return (x, written) -> written == null ? x : x;
            }
        }
    }
}
