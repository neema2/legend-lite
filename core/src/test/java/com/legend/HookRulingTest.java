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
import java.util.Set;
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
            // K2's server half, 2026-11-18 (the user's ruling: "Product root, edge opt-in"; the orchestrator's addition
            // (a)): the server's opt-in module, types only, implemented at the edge alone (edge.EngineMount). The
            // product hands none out: it has no implementation to refuse with, and an engine no module answers is
            // refused by name at boot (LegendHttpServer.configured, ServerBootTest) -- so no product call is listed
            Map.entry(com.legend.server.MountProvider.class, List.of()),
            // census C2, 2026-11-17 (the orchestrator's ruling that day: NoEdgeRun is the product's refusal, by name):
            // the edge's run hook, and the hearing it opens
            Map.entry(com.legend.testexec.EdgeRun.class, List.of(
                    () -> com.legend.testexec.NoEdgeRun.NONE.open(planNaming(), null, java.util.UUID.randomUUID()))),
            Map.entry(com.legend.testexec.EdgeHearing.class, List.of(
                    () -> productHearing().judged(null, List.of(), false))),
                    // (rowAppealed, the database judge's row appeal: the member deleted with its path, NARROWS A HOOK
                    // THE USER RULED -- A4 of the compatibility program, stage A ratified by the user 2026-11-19; moved
                    // down by the user's ruling of 2026-11-19 ~20:45 ("Approved to move guards down"))
                    // (raisedWords, legend-pure's words of a planned raise site looked up in the plan's edge records,
                    // K10b of 2026-11-17: the member deleted 2026-11-20 with stage B's B1 -- the edge words a raise
                    // from its site's record after the run; NARROWS A HOOK, a down move, the user's ~20:45 ruling)
            // (com.legend.testexec.PureWords, C5's step B's hook: deleted 2026-11-18, census J29 -- the host judge's
            // words are the run's HostWords, lite's form the product's and legend-pure's the edge's, never refused)
            // census C1, 2026-11-17 (the orchestrator's ruling that day: the edge hook typed, the product's refusal
            // where a null selected lite's rules): the ledger's recorder, each ask refused by the product's NoEdge
            Map.entry(com.legend.testexecutionplan.Edge.class, List.of(
                    () -> com.legend.testexecutionplan.NoEdge.NONE.effect(List.of(), List.of()),
                    () -> com.legend.testexecutionplan.NoEdge.NONE.seed(List.of(), Map.of()),
                    () -> com.legend.testexecutionplan.NoEdge.NONE.seeded(List.of(), List.of()),
                    () -> com.legend.testexecutionplan.NoEdge.NONE.effectRan(List.of()))),
                    // (siteWords, a raise site's words planned by the edge, K10 of 2026-11-18: the member deleted
                    // 2026-11-20 with stage B's B1, NARROWS A HOOK, a down move)
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
            // (census B17, 2026-11-17, lowering.RelationTexts: gone 2026-11-25 with the facet, stage B's B2b S5c; a shrink)
            // census C4, 2026-11-17: the engine-text assert's arm, and the hook only its answers hand out (the
            // collector): each reached through the facet the product refuses. (The database judge's batch, BatchTexts,
            // the second such hook: deleted, NARROWS A HOOK THE USER RULED -- A4 of the compatibility program, stage A
            // ratified by the user 2026-11-19; moved down by the user's ruling of 2026-11-19 ~20:45 ("Approved to move
            // guards down"))
            Map.entry(com.legend.testplan.EngineTestText.class, List.of(
                    () -> PlanStyle.PRODUCT.facet(com.legend.testplan.EngineTestText.class))),
            Map.entry(com.legend.testplan.TextCollector.class, List.of(
                    () -> PlanStyle.PRODUCT.facet(com.legend.testplan.EngineTestText.class))));

    /** C4's CAPABILITY (the orchestrator's ruling of 2026-11-17): the product's style does not judge legend-engine's
     *  text -- the test planner asks it before any engine-text gate, and takes the ordinary path -- the edge's does. */
    @Test
    void onlyTheEdgeJudgesEngineText() {
        assertFalse(PlanStyle.PRODUCT.judgesEngineText());
        assertTrue(com.legend.edge.PureText.UPSTREAM.judgesEngineText());
    }

    private static final String PARTS = "2026-11-18: the ResultEnvelopes facet's argument types: the product's own toJSON"
            + " names, types and cells and its graph emission, filled by the product and read only by the facet's forms"
            + " (orchestrator, 2026-11-18)";

    private static final String SITES = "2026-11-18: LoweringForms.existsJoin's argument type: the lowering's alias"
            + " minter and each conjunct's resolver zones, filled by the product and read only by the edge's form that"
            + " writes legend-engine's EXISTS join (the parts' ruling, orchestrator, 2026-11-18)";

    /** THE HOOK ENTRIES (the orchestrator's ruling of 2026-11-18, (6)): a product method that differs only by taking a
     *  hook type -- a PlanStyle or a facet -- and runs the product's own pipeline with it is the designed hook entry, not
     *  edge work in the product (EdgeOnlyMembersTest reads this list; its members are not counted). Each checked to hold
     *  no legend-engine logic in its body, and pinned with its reason. */
    static final Map<String, String> HOOK_ENTRIES = Map.ofEntries(
            Map.entry("com.legend.Execution#runOnProjectDatabases(com.legend.TypedQuery, java.lang.String, java.lang.String, com.legend.database.PlanStyle, com.legend.result.Output)",
                    "2026-11-18: a hook entry -- the product's own pipeline handed a PlanStyle, no legend-engine logic in its body (the orchestrator's ruling (6))"),
            Map.entry("com.legend.TypedQuery#projectPlan(java.lang.String, com.legend.database.PlanStyle)",
                    "2026-11-18: a hook entry -- the product's own pipeline handed a PlanStyle, no legend-engine logic in its body (the orchestrator's ruling (6))"),
            // the orchestrator's ruling on E-SQL's five (2026-11-18): a hook's own result type, constructed only by the
            // edge, is not a finding -- the product answers none (PlanStyle.traceCell())
            Map.entry("com.legend.database.PlanStyle$TraceCell#<init>(java.lang.String, java.lang.String)",
                    "2026-11-18: a hook type -- PlanStyle.traceCell()'s result, constructed by the edge's styles alone (the orchestrator's ruling on E-SQL's five)"),
            // the orchestrator's ruling of 2026-11-18 (train 18, L-B.TypedJsonResult, E-Java's ef7ee2b69d): HOOK_ENTRIES,
            // not READ_API -- the parts exist only as the ResultEnvelopes facet's argument types; READ_API would announce
            // them as general product API
            Map.entry("com.legend.lowering.EnvelopeParts#relation()", PARTS),
            Map.entry("com.legend.lowering.EnvelopeParts#objects()", PARTS),
            Map.entry("com.legend.lowering.RelationParts#names()", PARTS),
            Map.entry("com.legend.lowering.RelationParts#typeNames()", PARTS),
            Map.entry("com.legend.lowering.RelationParts#cells()", PARTS),
            // E-SQL's fold move (612b194708, 2026-11-18, the lens audit's item 1): the statement's sites, the argument
            // type of the LoweringForms two-form kind's existsJoin, beside it -- the parts' case (filed here by the GUARD
            // stream on that ruling, in place of the READ_API rows the branch proposed)
            Map.entry("com.legend.lowering.ExistsSites#alias()", SITES),
            // ExistsSites#zones() -> #keysWhere() (2026-11-18, F's L10 61e5205d1: the same argument type's accessor, renamed)
            Map.entry("com.legend.lowering.ExistsSites#keysWhere()", SITES));

    /** The hooks: PlanStyle, the facet kinds and the two-form kinds. */
    static List<Class<?>> hooks() {
        List<Class<?>> hooks = new java.util.ArrayList<>(List.of(com.legend.database.PlanStyle.class));
        hooks.addAll(com.legend.testplan.PlanStyleFacets.KINDS);
        hooks.addAll(com.legend.testplan.PlanStyleFacets.TWO_FORMS);
        return hooks;
    }

    /** A hook's ARGUMENT types declared beside it -- in the hook's own package -- and the types their methods return,
     *  also beside it (the orchestrator's ruling of 2026-11-18 on the ResultEnvelopes facet's parts): the product fills
     *  them and hands them to the hook. A general type (Type, SqlExpr, String) is in another package and is none. */
    static Set<Class<?>> hookArgumentTypes(List<Class<?>> hooks) {
        Set<Class<?>> out = new java.util.HashSet<>();
        for (Class<?> h : hooks) {
            for (java.lang.reflect.Method m : h.getDeclaredMethods()) {
                for (Class<?> p : m.getParameterTypes()) {
                    if (p.getPackageName().equals(h.getPackageName()) && !hooks.contains(p)) {
                        out.add(p);
                        for (java.lang.reflect.Method pm : p.getMethods()) {
                            if (pm.getReturnType().getPackageName().equals(h.getPackageName())) {
                                out.add(pm.getReturnType());
                            }
                        }
                    }
                }
            }
        }
        return out;
    }

    /** The types a hook's method returns: PlanStyle's, the facet kinds' and the two-form kinds'. */
    static Set<Class<?>> hookResultTypes() {
        List<Class<?>> hooks = hooks();
        Set<Class<?>> out = new java.util.HashSet<>();
        for (Class<?> h : hooks) {
            for (java.lang.reflect.Method m : h.getMethods()) {
                out.add(m.getReturnType());
            }
        }
        return out;
    }

    /** Each hook entry takes a PlanStyle, or constructs a hook's own result type (the orchestrator's ruling on E-SQL's
     *  five, 2026-11-18: PlanStyle$TraceCell): it is a hook entry by its signature, not by its name. */
    @Test
    void eachHookEntryTakesAHookType() throws ClassNotFoundException {
        for (String entry : HOOK_ENTRIES.keySet()) {
            Class<?> owner = Class.forName(entry.substring(0, entry.indexOf('#')));
            String name = entry.substring(entry.indexOf('#') + 1, entry.indexOf('('));
            String params = entry.substring(entry.indexOf('(') + 1, entry.length() - 1);
            if (name.equals("<init>")) {
                boolean ctor = java.util.Arrays.stream(owner.getConstructors()).anyMatch(c -> java.util.Arrays
                        .stream(c.getParameterTypes()).map(Class::getName)
                        .collect(java.util.stream.Collectors.joining(", ")).equals(params));
                org.junit.jupiter.api.Assertions.assertTrue(ctor && hookResultTypes().contains(owner), entry
                        + " is a public constructor of a type a hook's method returns");
                continue;
            }
            boolean found = false;
            if (hookArgumentTypes(hooks()).contains(owner)) {
                found = java.util.Arrays.stream(owner.getMethods()).anyMatch(m -> m.getName().equals(name)
                        && java.util.Arrays.stream(m.getParameterTypes()).map(Class::getName)
                        .collect(java.util.stream.Collectors.joining(", ")).equals(params));
                org.junit.jupiter.api.Assertions.assertTrue(found, entry + " is a public method of a hook's argument"
                        + " type, declared beside the hook");
                continue;
            }
            for (java.lang.reflect.Method m : owner.getMethods()) {
                String sig = java.util.Arrays.stream(m.getParameterTypes()).map(Class::getName)
                        .collect(java.util.stream.Collectors.joining(", "));
                if (m.getName().equals(name) && sig.equals(params)) {
                    found = java.util.Arrays.asList(m.getParameterTypes())
                            .contains(com.legend.database.PlanStyle.class);
                }
            }
            org.junit.jupiter.api.Assertions.assertTrue(found, entry + " is a public method taking a PlanStyle");
        }
    }

    /** The argument-type shape is tight: the facet's parts are admitted, a general type in a hook's signature is not. */
    @Test
    void aHooksArgumentTypesAreThoseBesideIt() {
        Set<Class<?>> args = hookArgumentTypes(hooks());
        org.junit.jupiter.api.Assertions.assertTrue(args.contains(com.legend.lowering.EnvelopeParts.class)
                && args.contains(com.legend.lowering.RelationParts.class)
                && args.contains(com.legend.lowering.ExistsSites.class), "the facet's parts, the forms' sites: " + args);
        org.junit.jupiter.api.Assertions.assertFalse(args.contains(com.legend.compiler.element.type.Type.class)
                || args.contains(String.class) || args.contains(com.legend.sql.SqlExpr.class), "a general type is no"
                + " hook argument type: " + args);
    }

    /** A ruled hook's static FORWARDERS (the orchestrator's facade rule, 2026-11-17): each forwards one call to a
     *  helper the product itself uses, and the set is pinned exactly -- shrink-only, a new one is seen. Every other
     *  method of a ruled hook is a bare signature. */
    static final Map<Class<?>, java.util.Set<String>> FACADES = Map.of(
            // (lowering.RelationTexts' pureToString and kindType: gone 2026-11-25 with the facet, stage B's B2b S5c; a
            // shrink -- its dateTimeText went 2026-11-17, census B9)
            );

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
        var step = new com.legend.testexecutionplan.TestPlan.Verdicts(List.of(verdict), null, null);
        return new com.legend.testexecutionplan.TestPlan(List.of(), List.of(step),
                com.legend.testexecutionplan.TestPlan.Judge.HOST, Map.of(), byVerdict, false, target,
                new com.legend.testexecutionplan.BodyFacts(false, false, true, "A"));
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

    // (theRaiseSitesWordsAreTheProductsNoneAndTheEdgesRecordersAlone, the product's and the edge's answers to
    // PlanStyle.siteWords and the edge's recorder: deleted 2026-11-20 with the hook, stage B's B1 -- no words are
    // recorded at planning; the edge words a raise after the run, PureMessagesWordedTest)

    /** PlanStyle.twoForm (the orchestrator's ruling of 2026-11-17, census B9): its kinds are a CLOSED list, pinned here
     *  as an exact set (empty until the first family adds its kind, with a dated reason); the product's style answers the
     *  caller's own product form, whatever the kind; the edge's refuses a kind outside the list by name. */
    @Test
    void theTwoFormKindsAreAClosedListTheProductAnswersItsOwnForm() {
        // the two-form kinds, an exact set; each added with its family and its dated reason:
        // DateTimeTexts (census B9, 2026-11-17: a DateTime's text in a message); ListTexts (census B8, 2026-11-17,
        // TEXT_FORMS: a list's text, the product's JSON form, the edge's legend-pure one); JsonGrammar (census C1,
        // 2026-11-17, register S144: a judge's JSON grammar, the product's strict JSON, the edge's json-simple);
        // (RelationCounting, census C1, 2026-11-17, register S137: left 2026-11-24, B3 step 2 -- the planner refuses a
        // relation counted as one, upstream's count a body rewritten by the edge's test-body door before planning)
        assertEquals(List.of(com.legend.lowering.DateTimeTexts.class, com.legend.lowering.ListTexts.class,
                // A4 of the compatibility program (stage A ratified by the user 2026-11-19; moved down by the user's
                // ruling of 2026-11-19 ~20:45 ("Approved to move guards down"); rule 4 of the rules of record):
                // JsonGrammar, CrossKindNumbers, VerdictNumbers, RenderedTextRule, DateLiteralTexts and GoldenNullWord
                // left with the database judge's compatibility arms (each was asked by the database judge alone)
                // TestDataPlanner.Printing (census C1, 2026-11-17, L9: a test-data timestamp's text)
                com.legend.testdatagen.TestDataPlanner.Printing.class,
                // ExecutedValues (train 17 item 2, 2026-11-18: a serialized root's string-entry value -- refused in the
                // product, legend-engine's json-builder envelope at the edge)
                com.legend.planner.ExecutedValues.class,
                // RoutedQueries (J5 of the edge audit, 2026-11-17: an execute's routing activity, none in the product,
                // legend-engine's AggregationAwareActivity at the edge)
                com.legend.planner.RoutedQueries.class,
                // ActivityTexts (J40.PlanStyle, the orchestrator's ruling of 2026-11-18: an execute's activity SQL, lite's
                // own in the product, legend-engine's text at the edge; until then PlanStyle.activitySql picked one of two)
                com.legend.planner.ActivityTexts.class,
                // LoweringForms (the audit's K5, 2026-11-17: the lowering's two-form choices, a raise among them)
                com.legend.lowering.LoweringForms.class,
                // (VerdictWords, census J29's: left 2026-11-21, item 3's S3 -- one catalog words every assert failure)
                // DatabaseSettingsForm (the audit's J22, RULED by the user 2026-11-17: a database's settings)
                com.legend.sql.dialect.DatabaseSettingsForm.class,
                // Edition (the edition program's step 2, 2026-11-25;
                // docs/records/edition-plan-to-finish-stage-b-2026-11-25.md sections 2.2 and 2.3: per native, the product
                // rule a door's audience means, and where the audience's own code runs; the product's has no row and
                // stamps nothing. The kind the other lowering forms fold into. 2026-11-26: the planner's Edition in the
                // lowering's EditionTable's place -- it makes the table for the statement's database, which the
                // lowering must not see: one kind before and after)
                com.legend.planner.Edition.class),
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
        // (VerdictWords' two forms -- lite's for the product, legend-pure's for the edge, one name in the plan -- went
        // 2026-11-21, item 3's S3: one catalog words every assert failure, so the style answers no words)
        // census J29's reads() went with A4 of the compatibility program (stage A ratified by the user 2026-11-19;
        // moved down by the user's ruling of 2026-11-19 ~20:45 ("Approved to move guards down")): the database judge
        // plans for the product alone, so it reads each side WRITTEN, as lite's sentences do
        Runnable productForm = () -> { };
        assertTrue(PlanStyle.PRODUCT.twoForm(Runnable.class, productForm) == productForm,
                "the product answers the caller's own form");
        NotImplementedException refused = assertThrows(NotImplementedException.class,
                () -> com.legend.edge.PureText.UPSTREAM.twoForm(Runnable.class, productForm));
        assertTrue(refused.getMessage().contains("none of the declared kinds"), refused.getMessage());
        assertThrows(NullPointerException.class, () -> PlanStyle.PRODUCT.twoForm(Runnable.class, null),
                "no default: the caller passes the product form");
    }

    /** The hooks' capabilities decide, never a null or the rules (census C1, the orchestrator's ruling of 2026-11-17):
     *  the product's edge hook records no ledger and the edge's does. */
    @Test
    void theHooksCapabilitiesAreTheProductsAndTheEdges() {
        assertFalse(com.legend.testexecutionplan.NoEdge.NONE.records(), "the product records no ledger");
        assertTrue(new com.legend.edge.CompatEdge(new com.legend.edge.RawLedger.Recorder(),
                com.legend.executionplan.DataStores.none()).records(), "the edge's recorder records the ledger");
        // (EdgeRun.wordsAlike, whose two answers -- lite's judges alike, the edge's words the host's alone -- were
        // asserted here: deleted, NARROWS A HOOK THE USER RULED, A4 of the compatibility program, stage A ratified by
        // the user 2026-11-19; moved down by the user's ruling of 2026-11-19 ~20:45 ("Approved to move guards down"):
        // the host judge is the only judge in compatibility, so no compatibility body runs both judges isolated)
        // (renderedTexts, lite's runs holding two differing strings as differing: the hook deleted 2026-11-24, B3 step
        // 2b -- the two texts are the verdict on every route)
        assertTrue(productHearing().hostForms() == com.legend.testexec.LiteHostForms.STRICT,
                "lite's runs never equal two number kinds");
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
                    && !inEdge(c.getPackageName())) {
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

    /** (b): the product's style refuses each way of reaching the hook, by name, pointing to the edge. For a facet hook,
     *  one ask per hook stands for every way (J16 of the edge audit, 2026-11-18): each product way to one is a pinned
     *  ask of a style's facet ({@code HookLambdasTest.FACET_CALLERS}, {@code FACET_FORWARDERS}; no product class or
     *  lambda implements one, {@code CLASS_HOOKS}, {@code LAMBDA_HOOKS}), and the product's style refuses every facet
     *  ask alike ({@link #theProductStyleRefusesEveryFacetAskOfEitherOverload}). */
    @Test
    void theProductRefusesEachRuledHookByName() {
        RULED.forEach((hook, calls) -> calls.forEach(call -> {
            NotImplementedException e = assertThrows(NotImplementedException.class, call, hook.getName());
            assertTrue(String.valueOf(e.getMessage()).contains("compatibility edge")
                    && e.getMessage().contains("com.legend.edge"),
                    hook + ": " + e.getMessage());
        }));
    }

    /** (b) for a facet hook (J16, 2026-11-18): the product's style refuses both of facet's overloads for every facet
     *  hook -- the one-argument ask by name, pointing to the edge; the other with the asker's own refusal, as the
     *  separation census's B17 ruled (a call's own sentence, its phase). */
    @Test
    void theProductStyleRefusesEveryFacetAskOfEitherOverload() throws ClassNotFoundException {
        for (String name : HookLambdasTest.facetHooks()) {
            Class<?> kind = Class.forName(name);
            NotImplementedException e = assertThrows(NotImplementedException.class, () -> PlanStyle.PRODUCT.facet(kind),
                    name);
            assertTrue(String.valueOf(e.getMessage()).contains("com.legend.edge"), name + ": " + e.getMessage());
            IllegalStateException asker = new IllegalStateException("the asker's own refusal");
            assertTrue(assertThrows(IllegalStateException.class, () -> PlanStyle.PRODUCT.facet(kind, () -> asker),
                    name) == asker, name + ": the product answers the asker's refusal");
        }
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
                    assertTrue(inEdge(c.getPackageName()),
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
                "ResultEnvelopes <- com.legend.edge.EngineResultEnvelopes",
                "UpstreamPlans <- com.legend.edge.EnginePlans$1",
                "EngineTestText <- com.legend.edge.EdgeEngineTestText",
                "TextCollector <- com.legend.edge.SqlTextVerdicts$Deferring",
                "TextCollector <- com.legend.edge.SqlTextVerdicts$Collecting",
                // (SqlTextVerdicts' database-judge collector, a TextCollector and a BatchTexts: deleted with the hook,
                // NARROWS A HOOK THE USER RULED -- A4 of the compatibility program, stage A ratified by the user
                // 2026-11-19; moved down by the user's ruling of 2026-11-19 ~20:45 ("Approved to move guards down"))
                // K2's server half, 2026-11-18: legend-engine's opt-in module of the shipped server
                "MountProvider <- com.legend.edge.EngineMount")), implementers);
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
                    assertTrue(inEdge(c.getPackageName()),
                            c.getName() + " overrides the spelling hook " + name + " outside the edge");
                    overriders.add(name + " <- " + c.getName());
                }
            }
        }
        assertFalse(overriders.isEmpty());
    }

    /** Where a planned raise stands in a statement: the projection, a CASE's ELSE branch, an operand of AND (the review
     *  of dd5deb781f, 2026-11-18, SHOULD: the projection alone was pinned). A raise the renderer builds itself -- the
     *  guarded parseFloat's, written through raiseAs -- has no reference to swap in; named, not pinned. */
    enum RaisePosition { PROJECTION, CASE_BRANCH, AND_OPERAND }

    /** A statement raising {@code kind} at {@code at}, the raise's message {@code ERROR}'s first argument, as the
     *  lowering writes one. */
    private static com.legend.sql.SqlSelect raising(com.legend.sql.RaiseKind kind, RaisePosition at) {
        return raisingWith(new com.legend.sql.SqlExpr.RaiseText(kind, List.of()), at);
    }

    /** The same statement with {@code message} as the raise's first argument: with the site's bare token as a string
     *  literal it is the reference the product's raise must equal, character for character. */
    private static com.legend.sql.SqlSelect raisingWith(com.legend.sql.SqlExpr message, RaisePosition at) {
        com.legend.sql.OutputCol v = new com.legend.sql.OutputCol("v", at == RaisePosition.AND_OPERAND
                ? com.legend.sql.SqlType.Scalar.BOOLEAN : com.legend.sql.SqlType.Scalar.VARCHAR, true,
                com.legend.sql.OutputCol.Origin.DERIVED);
        com.legend.sql.SqlExpr raise = com.legend.sql.SqlExpr.Call.of(com.legend.sql.SqlFn.ERROR, message,
                new com.legend.sql.SqlExpr.StringLit("3:7"));
        com.legend.sql.SqlExpr placed = switch (at) {
            case PROJECTION -> raise;
            case CASE_BRANCH -> new com.legend.sql.SqlExpr.Case(List.of(new com.legend.sql.SqlExpr.Case.When(
                    new com.legend.sql.SqlExpr.BoolLit(false), new com.legend.sql.SqlExpr.StringLit("x"))), raise,
                    com.legend.sql.SqlExpr.Reason.USER);
            case AND_OPERAND -> com.legend.sql.SqlExpr.Call.of(com.legend.sql.SqlFn.AND,
                    new com.legend.sql.SqlExpr.BoolLit(true), raise);
        };
        return new com.legend.sql.SqlSelect(List.of(new com.legend.sql.SqlSelect.Projection(placed, "v", v)), false,
                new com.legend.sql.SqlSource.Dual(), null, List.of(), null, null, List.of(), null, null, List.of(v));
    }

    /** The token alone, quoted as a literal (a string's quotes, any of the dialects'): nothing else in the message. */
    private static final java.util.regex.Pattern QUOTED_TOKEN = java.util.regex.Pattern.compile("(['\"`])"
            // the token's mark as each dialect spells it inside a literal (raw, or its escape: BigQuery's \x1c)
            + "(?:" + java.util.regex.Pattern.quote(String.valueOf(com.legend.sql.RaiseKind.MARK))
            + "|\\\\x1[cC]|\\\\u001[cC]|\\\\034)" + java.util.regex.Pattern.quote("LG#1") + "\\1");

    /** What differs from the product's raise in {@code d}'s statement raising each kind but a user's fail (the edge
     *  audit's K16, 2026-11-17: raiseMessage was the one spelling hook whose product default no test pinned; the review of
     *  11de9ae229, 2026-11-18: every kind, the message exactly the token): the product writes its site's bare token
     *  ({@code LG#1}) as the whole literal, and words nothing upstream's way in its site ({@code upstream} null). A
     *  refusal is put to {@code refused} ("dialect kind"), never dropped. */
    static List<String> productRaiseDiffers(java.util.function.Supplier<com.legend.sql.dialect.SqlDialect> dialect,
            java.util.Set<String> refused) {
        java.util.Set<com.legend.sql.RaiseKind> kinds = java.util.EnumSet.allOf(com.legend.sql.RaiseKind.class);
        kinds.remove(com.legend.sql.RaiseKind.USER_FAIL);
        return productRaiseDiffers(dialect, refused, kinds);
    }

    /** {@link #productRaiseDiffers(java.util.function.Supplier, java.util.Set)} over {@code kinds}. Each statement is
     *  rendered on a fresh dialect: a dialect may number what it writes per instance (ClickHouse's {@code rand(n)}), so
     *  two renders on one instance differ by that number alone. */
    static List<String> productRaiseDiffers(java.util.function.Supplier<com.legend.sql.dialect.SqlDialect> dialect,
            java.util.Set<String> refused, java.util.Set<com.legend.sql.RaiseKind> kinds) {
        List<String> out = new java.util.ArrayList<>();
        for (com.legend.sql.RaiseKind kind : kinds) {
          for (RaisePosition at : RaisePosition.values()) {
            com.legend.sql.dialect.SqlDialect d = dialect.get();
            com.legend.sql.dialect.RenderedStatement s;
            try {
                s = d.renderStatement(raising(kind, at));
            } catch (com.legend.sql.DialectCapability | NotImplementedException | UnsupportedOperationException e) {
                refused.add(d.getClass().getName() + " " + kind + " " + at);
                continue;
            }
            // the statement exactly as with the site's bare token as a string literal (the review of 091b9bde36,
            // 2026-11-18, SHOULD-FIX 5: the token found somewhere passed concat(token, ' words') and a comment holding
            // it); the reference itself holds the quoted token, so it is no empty comparison
            com.legend.sql.dialect.RenderedStatement reference = dialect.get().renderStatement(raisingWith(
                    new com.legend.sql.SqlExpr.StringLit(com.legend.sql.RaiseKind.token(1)), at));
            if (!QUOTED_TOKEN.matcher(reference.sql()).find()) {
                throw new IllegalStateException(d.getClass().getName() + " writes the reference without its token: "
                        + reference.sql());
            }
            // the text, and what it binds (the review of dd5deb781f, 2026-11-18, MUST-FIX 4: words bound at a ? the
            // reference binds empty read as the bare token's)
            if (!s.sql().equals(reference.sql()) || !s.binds().equals(reference.binds())
                    || !s.regions().equals(reference.regions())) {
                out.add(d.getClass().getName() + " " + kind + " " + at + " writes " + s.sql() + " binding " + s.binds());
            // (a site's upstream words: gone from the plan with K10, 2026-11-18 -- the edge hook's records hold them)
            } else if (s.sites().size() != 1) {
                out.add(d.getClass().getName() + " " + kind + " " + at + " words its site " + s.sites());
            }
          }
        }
        return out;
    }

    /** The product's default of each spelling hook, pinned on every product dialect that has it: a raw statement as
     *  written, no spelling pass, no scan order, a DateTime format of lite's own (six digits and Z), and a raise its
     *  site's bare token, worded nowhere upstream's way (raiseMessage: the edge audit's K16, 2026-11-17). */
    @Test
    void eachSpellingHooksProductDefault() throws Exception {
        for (com.legend.model.ConnectionDefinition.DatabaseType t : com.legend.database.Databases.implemented()) {
            com.legend.sql.dialect.SqlDialect d = com.legend.database.Databases.dialect(t);
            java.util.Set<String> refused = new java.util.TreeSet<>();
            assertEquals(List.of(), productRaiseDiffers(() -> com.legend.database.Databases.dialect(t), refused),
                    t + ": the product raises its site's token");
            // no product dialect refuses a raise today (measured 2026-11-18): one that does is pinned here, by kind
            assertEquals(java.util.Set.of(), refused, t + ": a product dialect refuses a raise");
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

    /** The raise pin bites a product dialect writing words beside its token ({@code CONCAT(token, ' Division by
     *  zero')}, the review of 091b9bde36, 2026-11-18): the token is there, quoted, and the statement is not the bare
     *  token's. */
    @Test
    void theRaisePinBitesWordsBesideTheToken() {
        java.util.function.Supplier<com.legend.sql.dialect.SqlDialect> worded =
                com.legend.sql.dialect.PlantedWordedRaise::new;
        List<String> found = productRaiseDiffers(worded, new java.util.TreeSet<>(),
                java.util.Set.of(com.legend.sql.RaiseKind.DIVIDE_BY_ZERO));
        // named at each position the raise is placed (the projection, a CASE branch, an AND operand)
        assertEquals(RaisePosition.values().length, found.size(), found.toString());
        found.forEach(f -> assertTrue(f.contains("Division by zero"), f));
        // the same text as the bare token's, the words bound (the review of dd5deb781f, 2026-11-18, MUST-FIX 4)
        List<String> bound = productRaiseDiffers(com.legend.sql.dialect.PlantedBoundRaise::new, new java.util.TreeSet<>(),
                java.util.Set.of(com.legend.sql.RaiseKind.DIVIDE_BY_ZERO));
        assertEquals(RaisePosition.values().length, bound.size(), bound.toString());
        bound.forEach(f -> assertTrue(f.contains("Division by zero"), f));
        assertEquals(List.of(), productRaiseDiffers(com.legend.sql.dialect.PlantedWordedRaise::plain,
                new java.util.TreeSet<>(),
                java.util.Set.of(com.legend.sql.RaiseKind.DIVIDE_BY_ZERO)), "the product's own raise is the token's");
    }

    /** Since K10 the edge's dialects raise as the product does: no edge dialect words a raise upstream's way, in its SQL
     *  or its site -- legend-pure's words are the edge's recorder's own data (K10b of the edge audit, the orchestrator's
     *  ruling (A) of 2026-11-17). Until train 19 this test was the pin's bite on the real edge dialects (each named, at
     *  least three); K10 removed what it bit on, so it now holds K10's outcome, and the pin's bite is the planted
     *  dialects' (theRaisePinBitesWordsBesideTheToken). Re-cut 2026-11-18 for train 19 by the GUARD stream. */
    @Test
    void theEdgesDialectsRaiseAsTheProductSinceK10() {
        List<String> worded = new java.util.ArrayList<>();
        for (com.legend.model.ConnectionDefinition.DatabaseType t : com.legend.database.Databases.implemented()) {
            // a division by zero, which the edge worded without operands before K10 (its other kinds' sentences read theirs)
            worded.addAll(productRaiseDiffers(() -> com.legend.edge.PureText.dialect(t), new java.util.TreeSet<>(),
                    java.util.Set.of(com.legend.sql.RaiseKind.DIVIDE_BY_ZERO)));
        }
        assertEquals(List.of(), worded, "an edge dialect words a raise in its SQL or its site: K10 moved legend-pure's"
                + " words to the edge's recorder");
    }

    /** What {@code dialects} gives a database type that is not the edge's own dialect (the edge audit's K16, 2026-11-17:
     *  PureText.dialect hands a database it has no dialect for to the product's Databases.dialect, which refuses it by
     *  name today -- a product dialect for one of them would then reach the edge's planning silently, a fallback):
     *  "type -> class" for each type answered by a class outside {@code com.legend.edge}; a refusal by name holds. */
    static List<String> notTheEdges(java.util.function.Function<com.legend.model.ConnectionDefinition.DatabaseType,
            com.legend.sql.dialect.SqlDialect> dialects) {
        List<String> out = new java.util.ArrayList<>();
        for (com.legend.model.ConnectionDefinition.DatabaseType t
                : com.legend.model.ConnectionDefinition.DatabaseType.values()) {
            com.legend.sql.dialect.SqlDialect d;
            try {
                d = dialects.apply(t);
            } catch (NotImplementedException refused) {
                continue;
            }
            if (!inEdge(d.getClass().getPackageName())) {
                out.add(t + " -> " + d.getClass().getName());
            }
        }
        return out;
    }

    /** Every database the edge plans for has the edge's own dialect, or is refused by name -- never the product's. */
    @Test
    void theEdgesDialectIsNeverTheProducts() {
        assertEquals(List.of(), notTheEdges(com.legend.edge.PureText::dialect), "a database the edge plans with the"
                + " product's dialect: write the edge's (PureText), or refuse it by name");
        List<String> product = notTheEdges(com.legend.database.Databases::dialect);
        assertTrue(product.size() >= 3 && product.stream().allMatch(p -> p.contains(" -> com.legend.sql.dialect.")),
                "the check bites: the product's dialects are named, " + product);
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

    /** Whether {@code pkg} is the edge's package or one under it -- its prefix with its dot (the GUARD stream's C.14,
     *  2026-11-17: "com.legend.edgecase" is no edge). */
    private static boolean inEdge(String pkg) {
        return pkg.equals("com.legend.edge") || pkg.startsWith("com.legend.edge.");
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
