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
            // the failure words the judges write (C1 item 4, 3120418b2a, train 8: legendPureWords retired -- one choice
            // per run, the style's FailureWords two-form kind; measured 2026-11-18 on d-int8 fdf001bee); census J29
            // (E-Java's 21a9734475, train 14) renamed the kind VerdictWords (lowering) and DatabaseJudge#lite #words --
            // the same two callers, re-measured 2026-11-18 on train 15 1931d419b1 by the GUARD stream's re-cut
            Map.entry("com.legend.testplan.DatabaseJudge#words(com.legend.testplan.BodyDriver$ExecEnv)",
                    Set.of("com.legend.lowering.VerdictWords", "(twoForm reaches 1)")),
            Map.entry("com.legend.testplan.TestDoor#planned(com.legend.TypedQuery, java.lang.String, java.util.Set,"
                    + " com.legend.testplan.TestDoor$Asserts, boolean, com.legend.executionplan.DataStores,"
                    + " com.legend.testexecutionplan.Edge, com.legend.database.PlanStyle,"
                    + " com.legend.testexecutionplan.BodyFacts)",
                    Set.of("com.legend.lowering.VerdictWords", "(twoForm reaches 1)")),
            // the test planner: a relation counted by an assert (S137); a test-data timestamp's text (L9)
            Map.entry("com.legend.testplan.TestPlanner#lifted(com.legend.TypedQuery,"
                    + " com.legend.testplan.PlanningOptions)",
                    Set.of("com.legend.compiler.spec.RelationCounting", "com.legend.platform.PlanOption",
                            "(twoForm reaches 1)")),
            Map.entry("com.legend.testplan.TestPlanner#plan(com.legend.TypedQuery, java.lang.String,"
                    + " com.legend.testplan.PlanningOptions)", Set.of("com.legend.testdatagen.TestDataPlanner$Printing",
                    "(twoForm reaches 1)")));

    /** Product or host class -> the hook types it implements or extends as a class (the ruled hooks', the facet kinds'
     *  and the styles' implementers are HookRulingTest's and PlanStyleSurfaceTest's too; the two-form kinds' product
     *  forms are pinned here alone). Measured 2026-11-17. */
    static final Map<String, Set<String>> CLASS_HOOKS = Map.ofEntries(
            // re-cut 2026-11-17 on d-stage c37207858: train 6 (E-Java's C.9) moved each two-form kind's product form out
            // of the kind into its own class (Lite*, ProductLoweringForms) -- the same forms, renamed
            // the two-form kinds' product forms: a relation counted by an assert, refused (S137); a list's text, a JSON
            // array (F's B8); a judge's JSON grammar, strict JSON (S144); two number kinds compared (S127)
            Map.entry("com.legend.compiler.spec.RelationCountAsserts", Set.of("com.legend.compiler.spec.RelationCounting")),
            Map.entry("com.legend.lowering.ProductListTexts", Set.of("com.legend.lowering.ListTexts")),
            Map.entry("com.legend.testexecutionplan.JudgeJson", Set.of("com.legend.testexecutionplan.JsonGrammar")),
            Map.entry("com.legend.testplan.LiteCrossKindNumbers", Set.of("com.legend.testplan.CrossKindNumbers")),
            // trains 5 and 6 (2026-11-17): the lowering forms' product answer, an anonymous class in the kind (K5,
            // fd1df9928, a criterion (a) exception); the test-data printing's product form, its own class (K12, a095d0b56)
            Map.entry("com.legend.lowering.ProductLoweringForms", Set.of("com.legend.lowering.LoweringForms")),
            Map.entry("com.legend.testdatagen.LitePrinting", Set.of("com.legend.testdatagen.TestDataPlanner$Printing")),
            // the failure words' product form (C1 item 4, 3120418b2a, train 8; measured 2026-11-18); census J29 (train 14's
            // 21a9734475) renamed it LiteVerdictWords, of the VerdictWords kind (re-measured 2026-11-18 on 1931d419b1)
            Map.entry("com.legend.lowering.LiteVerdictWords", Set.of("com.legend.lowering.VerdictWords")),
            // a database's settings' product form, H2's own defaults (J22, the user's ruling of 2026-11-17 23:05;
            // 59719a0153's train, train 11; HookRulingTest pins the kind; measured 2026-11-18)
            Map.entry("com.legend.sql.dialect.ProductDatabaseSettings", Set.of("com.legend.sql.dialect.DatabaseSettingsForm")),
            // the run's forms' product forms (J36 of the edge audit, 2026-11-18, RUN_FORMS): a cell's, S312's dates and a
            // structure's JSON (CellText.PRODUCT); lite's host judge, two number kinds never equal (S127); lite's
            // rendered-text rule, two strings that differ never held
            Map.entry("com.legend.result.CellText$1", Set.of("com.legend.result.CellText$Forms")),
            Map.entry("com.legend.testexec.LiteHostForms", Set.of("com.legend.testexec.HostForms")),
            Map.entry("com.legend.testexec.LiteRenderedTexts", Set.of("com.legend.testexec.RenderedTexts")),
            // D's train 4 (census C1, 2026-11-17): a rendered golden's null word, none (c013b2565c); a date literal's
            // message text, lite's (4bc94e2d8f); the rendered-text rule, strings (a9c14bb4ca) -- each nested in its kind
            Map.entry("com.legend.compiler.spec.LiteGoldenNullWord", Set.of("com.legend.compiler.spec.GoldenNullWord")),
            Map.entry("com.legend.testplan.LiteDateLiteralTexts", Set.of("com.legend.testplan.DateLiteralTexts")),
            Map.entry("com.legend.testplan.LiteRenderedTextRule", Set.of("com.legend.testplan.RenderedTextRule")),
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

    /** EVERY WAY THE PRODUCT REACHES A FACET HOOK (J16 of the edge audit, 2026-11-18, = K4's guard half): HookRulingTest's
     *  criterion (b) called the product style's facet for each facet hook and saw it refused -- but that style refuses
     *  every kind alike, so the call proved nothing about the ways a product body reaches the hook. A facet hook's value
     *  has four possible origins in the product, and each is pinned: a class implementing it ({@link #CLASS_HOOKS}: none);
     *  a lambda of it ({@link #LAMBDA_HOOKS}: none); a style's facet, asked here, each asking method with the kinds it
     *  names and its number of reaches, exact; and a product method answering one ({@link #FACET_FORWARDERS}), each a
     *  forwarder of those asks. A reflective or loaded instance is ServiceLoaderSitesTest's. So every way is a pinned
     *  ask of the style, which the product's style refuses by name (HookRulingTest.RULED). "caller#method(params)" ->
     *  the class objects named beside its facet asks, and its number of reaches. */
    // measured 2026-11-18 (J16, the batch's build: the pin was written empty and unmeasured): eleven asks, each a hook
    // the product's style refuses by name and the edge's answers
    static final Map<String, Set<String>> FACET_CALLERS = Map.ofEntries(
            // a toDDL generator's text, alone or in a collection: legend-engine's spelling from the compiled store
            Map.entry("com.legend.planner.BodyPlanner#prepare(java.util.List, com.legend.planner.PlanEnv)",
                    Set.of("(facet reaches 2)", "com.legend.planner.UpstreamPlans")),
            // a plan-rows native (legend-engine's plan as rows), asked before the walk's refusals are caught (G7)
            Map.entry("com.legend.planner.PlanAllocations#registerPlanRows(com.legend.compiler.spec.typed.TypedNativeCall,"
                    + " java.util.List, com.legend.compiler.spec.SpecCompiler, com.legend.planner.PlanEnv)",
                    Set.of("(facet reaches 1)", "com.legend.planner.UpstreamPlans")),
            // an activity's upstream SQL text, asked only where the style picks the engine's (W9's planner half)
            Map.entry("com.legend.planner.PlanAllocations$1#engines()",
                    Set.of("(facet reaches 1)", "com.legend.planner.UpstreamPlans")),
            // the Lowerer's relation-text and result-envelope facets, each asked with its call's refusal (B8)
            Map.entry("com.legend.planner.PlanEnv#configure(com.legend.lowering.Lowerer)",
                    Set.of("(facet reaches 2)", "com.legend.lowering.RelationTexts",
                            "com.legend.lowering.ResultEnvelopes")),
            // the lowering helpers' Pure-only texts, asked with its call's refusal (B8); the three kinds beside it are
            // twoForm's, named in the same method
            Map.entry("com.legend.planner.PlanEnv#styled()",
                    Set.of("(facet reaches 1)", "com.legend.lowering.DateTimeTexts", "com.legend.lowering.ListTexts",
                            "com.legend.lowering.LoweringForms", "com.legend.lowering.PureOnlyTexts")),
            // the plan-text natives' routines, each asking when called: a body naming none plans on any style
            Map.entry("com.legend.planner.PlanTextRoutines#of(com.legend.compiler.spec.SpecCompiler,"
                    + " com.legend.planner.PlanEnv)",
                    Set.of("(facet reaches 1)", "com.legend.planner.UpstreamPlans")),
            // a body binding a plan handle: the product's style refuses it by name before either judge plans it
            Map.entry("com.legend.testplan.BodyDriver#refusePlanHandlesOffTheEdge(com.legend.TypedQuery,"
                    + " com.legend.testplan.PlanningOptions)",
                    Set.of("(facet reaches 1)", "com.legend.planner.UpstreamPlans")),
            // the test-data generator's plan, asked when a body generates test data
            Map.entry("com.legend.testplan.BodyDriver$ExecEnv#of(com.legend.compiler.element.ModelContext,"
                    + " java.lang.String, com.legend.sql.dialect.SqlDialect, com.legend.testplan.PlanningOptions)",
                    Set.of("(facet reaches 1)", "com.legend.planner.UpstreamPlans")),
            // the engine-text facet, asked only where the style judges engine text (census C4)
            Map.entry("com.legend.testplan.EngineTextGates#facet(com.legend.testplan.BodyDriver$ExecEnv)",
                    Set.of("(facet reaches 1)", "com.legend.testplan.EngineTestText")),
            // legend-engine's seed SQL from records or from CSV, a seed text a body asserts
            Map.entry("com.legend.testplan.SeedSqlForms#assertForm(java.util.List,"
                    + " com.legend.compiler.spec.typed.TypedNativeCall, com.legend.testplan.BodyDriver$ExecEnv)",
                    Set.of("(facet reaches 2)", "com.legend.planner.UpstreamPlans")),
            // a DDL text source's generators, spelled at planning
            Map.entry("com.legend.testplan.Seeds#ddlTexts(java.util.List, java.util.List,"
                    + " com.legend.compiler.element.ModelContext, com.legend.database.PlanStyle)",
                    Set.of("(facet reaches 1)", "com.legend.planner.UpstreamPlans")));

    /** The product methods outside the hook types answering a facet hook (the facet kinds, and the hooks only a facet's
     *  answers hand out), each with the hook it answers: forwarders of a facet ask, pinned exact (J16, 2026-11-18). */
    // measured 2026-11-18 (J16, the batch's build: the pin was written empty and unmeasured): five, each handing on what
    // an ask pinned in FACET_CALLERS returned
    static final Map<String, String> FACET_FORWARDERS = Map.ofEntries(
            // the Lowerer's result-envelope request, the style's facet that PlanEnv.configure hands in
            Map.entry("com.legend.lowering.Lowerer#resultEnvelopes(java.util.function.Supplier)",
                    "com.legend.lowering.ResultEnvelopes"),
            // the environment's accessor for the collector the engine-text facet handed out (none on the product's style)
            Map.entry("com.legend.testplan.BodyDriver$ExecEnv#engineText()", "com.legend.testplan.TextCollector"),
            // the database judge's batch, from the engine-text facet, asked only where a gate read an engine-text side
            Map.entry("com.legend.testplan.EngineTextGates#batchTexts(com.legend.testplan.BodyDriver$ExecEnv)",
                    "com.legend.testplan.BatchTexts"),
            // the engine-text facet itself, asked only where the style judges engine text
            Map.entry("com.legend.testplan.EngineTextGates#facet(com.legend.testplan.BodyDriver$ExecEnv)",
                    "com.legend.testplan.EngineTestText"),
            // the environment's collector, handed on to the test planner's judges
            Map.entry("com.legend.testplan.EngineTextPlanning#collector(com.legend.testplan.BodyDriver$ExecEnv)",
                    "com.legend.testplan.TextCollector"));

    /** The facet hooks: the facet kinds, and the two hooks only the engine-text facet's answers hand out (the collector
     *  and the database judge's batch, HookRulingTest.RULED's). */
    static Set<String> facetHooks() {
        Set<String> out = new TreeSet<>();
        com.legend.testplan.PlanStyleFacets.KINDS.forEach(c -> out.add(c.getName()));
        out.add(com.legend.testplan.TextCollector.class.getName());
        out.add(com.legend.testplan.BatchTexts.class.getName());
        return out;
    }

    /** Each method of {@code classes} answering one of {@code hooks} (by its declared return type, a sub-interface's
     *  too), outside the edge and outside the hook types themselves (a hook's own method handing out another hook is
     *  the hook's: the engine-text facet's collector), each with the hook it answers. */
    static Map<String, String> facetForwarders(JavaClasses classes, Set<String> hooks, Set<String> hookTypes) {
        Map<String, String> out = new TreeMap<>();
        for (JavaClass c : classes) {
            if (c.getName().startsWith("com.legend.edge.") || hookTypes.stream().anyMatch(h -> c.isAssignableTo(h))) {
                continue;
            }
            for (JavaCodeUnit u : c.getCodeUnits()) {
                String type = u.getRawReturnType().getName();
                if (hook(type, hooks, classes)) {
                    out.put(ServiceLoaderSitesTest.key(c, u), type);
                }
            }
        }
        return out;
    }

    /** The forms a run's hearing or a writer is handed, the edge's beside the product's, which are none of the ruled
     *  hooks, facet kinds or two-form kinds (J36 of the edge audit, 2026-11-18: "unpinned hooks"): a cell's forms, the
     *  host judge's behaviours and the rendered-text rule. Each is a hook type here, so its product implementers are
     *  pinned in {@link #CLASS_HOOKS} and a product lambda of one would be {@link #LAMBDA_HOOKS}'s. */
    static final List<Class<?>> RUN_FORMS = List.of(com.legend.result.CellText.Forms.class,
            com.legend.testexec.HostForms.class, com.legend.testexec.RenderedTexts.class);

    /** The hook types: the ruled hooks, the facet kinds, the two-form kinds, the run's forms, the plan style. */
    static Set<String> hookTypes() {
        Set<String> out = new TreeSet<>();
        HookRulingTest.RULED.keySet().forEach(c -> out.add(c.getName()));
        com.legend.testplan.PlanStyleFacets.KINDS.forEach(c -> out.add(c.getName()));
        com.legend.testplan.PlanStyleFacets.TWO_FORMS.forEach(c -> out.add(c.getName()));
        RUN_FORMS.forEach(c -> out.add(c.getName()));
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
        return styleCallers(classes, "twoForm");
    }

    /** Each method of {@code classes} (the edge's aside) calling facet on a style, either overload, with the class
     *  objects it names (J16, 2026-11-18). */
    static Map<String, Set<String>> facetCallers(Iterable<JavaClass> classes) {
        return styleCallers(classes, "facet");
    }

    /** Each method of {@code classes} (the edge's aside) calling or referencing {@code ask} on a style, with the class
     *  objects it names and its number of reaches. */
    static Map<String, Set<String>> styleCallers(Iterable<JavaClass> classes, String ask) {
        Map<String, Set<String>> out = new TreeMap<>();
        for (JavaClass c : classes) {
            if (c.getName().startsWith("com.legend.edge.")) {
                continue;
            }
            for (JavaCodeUnit u : c.getCodeUnits()) {
                // a call or a method reference, each method by its full signature (the review of the loader sites'
                // twin, 9702998c08, 2026-11-17: a reference was unseen, overloads shared a key)
                // each with its number of reaches (a second ask in a pinned method changes it: the review of C.12)
                long reaches = u.getMethodCallsFromSelf().stream().filter(call -> call.getName().equals(ask)
                        && call.getTargetOwner().isAssignableTo(PlanStyle.class.getName())).count()
                        + u.getMethodReferencesFromSelf().stream().filter(ref -> ref.getName().equals(ask)
                        && ref.getTargetOwner().isAssignableTo(PlanStyle.class.getName())).count();
                if (reaches > 0) {
                    Set<String> objects = new TreeSet<>();
                    u.getReferencedClassObjects().forEach(o -> objects.add(o.getRawType().getName()));
                    objects.add("(" + ask + " reaches " + reaches + ")");
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
    void theFacetCallersAndForwardersArePinned() {
        JavaClasses classes = importJars(ConstantPools.jars("legend.product.jars", "legend.authored.jars"));
        assertEquals(new TreeMap<>(FACET_CALLERS), facetCallers(classes), "a product method asks a style for a facet"
                + " beyond its pin: a new way to reach a facet hook (J16), pinned here with its reason");
        assertEquals(new TreeMap<>(FACET_FORWARDERS), facetForwarders(classes, facetHooks(), hookTypes()), "a product"
                + " method answers a facet hook beyond its pin: an origin of the hook that is no ask of the style (J16)");
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
                PlantedTwoForm.class, PlantedFactory.class, PlantedRunForm.class);
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
                PlantedTwoForm.class.getName(), Set.of("com.legend.lowering.ListTexts"),
                PlantedRunForm.class.getName(), Set.of("com.legend.testexec.HostForms")), classHooks(classes,
                hookTypes()), "a class implementing a two-form kind, a sub-interface of one and a run's form are read");
        assertEquals(Set.of(PlantedFactory.class.getName() + "#proxy()", PlantedFactory.class.getName()
                + "#metafactory()", PlantedFactory.class.getName() + "#byReference()"), directFactories(classes),
                "a direct factory call or reference is read; a lambda is not one");
        Map<String, ConstantPools.Pool> proxyPools = new TreeMap<>();
        for (Class<?> c : List.of(PlantedFactory.class, PlantedProxyLiteral.class, PlantedProxyByName.class,
                PlantedHookLambda.class)) {
            proxyPools.put(c.getName(), ConstantPools.pool(ConstantPools.bytesOf(c)));
        }
        assertEquals(Set.of(PlantedFactory.class.getName(), PlantedProxyLiteral.class.getName(),
                PlantedProxyByName.class.getName()), proxiesNamed(proxyPools), "MethodHandleProxies named in a pool --"
                + " a call, a class literal, its dotted name -- is read; a lambda's pool does not name it");
        String p = PlantedHookLambda.class.getName();
        assertEquals(Map.of(p + "#ask(com.legend.database.PlanStyle)", Set.of("com.legend.lowering.DateTimeTexts",
                        "(twoForm reaches 1)"),
                p + "#ask(com.legend.database.PlanStyle, int)", Set.of("com.legend.lowering.DateTimeTexts",
                        "com.legend.lowering.ListTexts", "(twoForm reaches 2)"),
                p + "#byReference(com.legend.database.PlanStyle)", Set.of("(twoForm reaches 1)")),
                twoFormCallers(new ClassFileImporter().importClasses(PlantedHookLambda.class)));
        // J16 (2026-11-18): a facet's asks, either overload or a reference, with the kinds named; a forwarder of one
        JavaClasses facets = new ClassFileImporter().importClasses(PlantedFacetReach.class);
        String f = PlantedFacetReach.class.getName();
        assertEquals(Map.of(f + "#plans(com.legend.database.PlanStyle)", Set.of("com.legend.planner.UpstreamPlans",
                        "(facet reaches 1)"),
                f + "#both(com.legend.database.PlanStyle)", Set.of("com.legend.lowering.PureOnlyTexts",
                        "com.legend.lowering.RelationTexts", "(facet reaches 2)"),
                f + "#byReference(com.legend.database.PlanStyle)", Set.of("(facet reaches 1)")),
                facetCallers(facets), "a facet's asks are read: a call of either overload and a reference");
        assertEquals(Map.of(f + "#plans(com.legend.database.PlanStyle)", "com.legend.planner.UpstreamPlans",
                f + "#collector()", "com.legend.testplan.TextCollector"),
                facetForwarders(facets, facetHooks(), hookTypes()), "a product method answering a facet hook is read");
    }

    /** A planted class naming MethodHandleProxies by a class literal alone. */
    @SuppressWarnings("unused")
    static final class PlantedProxyLiteral {
        static Object type() {
            return java.lang.invoke.MethodHandleProxies.class;
        }
    }

    /** A planted class naming MethodHandleProxies by its dotted name alone (a reflective lookup). */
    @SuppressWarnings("unused")
    static final class PlantedProxyByName {
        static Object type() throws ReflectiveOperationException {
            return Class.forName("java.lang.invoke.MethodHandleProxies");
        }
    }

    /** A planted sub-interface of a two-form kind. */
    interface PlantedSubTexts extends com.legend.lowering.DateTimeTexts {
    }

    /** Planted: a product form of a run's form (J36, 2026-11-18) -- the host judge's behaviours. */
    abstract static class PlantedRunForm implements com.legend.testexec.HostForms {
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

    /** A planted product body reaching facet hooks (J16, 2026-11-18): facet asks (both overloads, two in one method)
     *  and a reference to facet; forwarders answering a facet kind and a hook only a facet's answers hand out. */
    @SuppressWarnings("unused")
    static final class PlantedFacetReach {
        static com.legend.planner.UpstreamPlans plans(PlanStyle style) {
            return style.facet(com.legend.planner.UpstreamPlans.class);
        }

        static Object both(PlanStyle style) {
            return List.of(style.facet(com.legend.lowering.PureOnlyTexts.class),
                    style.facet(com.legend.lowering.RelationTexts.class, IllegalStateException::new));
        }

        static java.util.function.Function<Class<com.legend.lowering.ResultEnvelopes>,
                com.legend.lowering.ResultEnvelopes> byReference(PlanStyle style) {
            return style::facet;
        }

        static com.legend.testplan.TextCollector collector() {
            return null;
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
            return style.twoForm(com.legend.lowering.DateTimeTexts.class, (x, written) -> written == null ? x : null);
        }

        static Object ask(PlanStyle style, int n) {
            // each lambda's body its own: a site count that holds without line numbers too (-g:none dedupes equal ones)
            return List.of(style.twoForm(com.legend.lowering.DateTimeTexts.class, (x, written) -> written != null ? x
                    : null),
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
