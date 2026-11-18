// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.testplan;

import java.util.List;

/**
 * THE CLOSED LIST of the plan style's test-planning facets (the orchestrator's condition of 2026-11-16 on
 * {@code PlanStyle.facet}: a typed accessor, never an open service locator). Every style answers exactly these kinds --
 * the product refusing each by name, the edge answering each -- and refuses any other kind by name
 * ({@code PlanStyleFacetsTest} holds every implementer to this list). A new kind is added here, with its reason.
 */
public final class PlanStyleFacets {

    private PlanStyleFacets() {
    }

    /** The facet kinds, in the order they were added: the engine-text assert's arm (W9's judge half, 2026-11-16);
     *  upstream's plan text at planning (W9's planner half, 2026-11-16: {@code planner.UpstreamPlans}); a relation's
     *  text at lowering (the separation census's B17, 2026-11-17: {@code lowering.RelationTexts}, asked with its
     *  call's refusal); a text only legend-pure writes at lowering (B8 (i), 2026-11-17: {@code lowering.PureOnlyTexts}); legend-engine's result
     *  envelope at lowering (Phase 6's B5, 2026-11-17: {@code lowering.ResultEnvelopes}, asked with its call's refusal). */
    public static final List<Class<?>> KINDS = List.of(EngineTestText.class, com.legend.planner.UpstreamPlans.class,
            com.legend.lowering.RelationTexts.class, com.legend.lowering.PureOnlyTexts.class,
            com.legend.lowering.ResultEnvelopes.class);

    /** The TWO-FORM kinds ({@code PlanStyle.twoForm}, the orchestrator's ruling of 2026-11-17, census B9): each a value
     *  the product answers in its own canonical form and the edge in upstream's. A closed list, each kind added with its
     *  family and its dated reason: a DateTime's text in a message (census B9, 2026-11-17: {@code lowering.DateTimeTexts},
     *  lite's DATETIME_TO_TEXT in the product, legend-pure's print form at the edge). */
    public static final List<Class<?>> TWO_FORMS = List.of(com.legend.lowering.DateTimeTexts.class,
            // a list's text (F's B8, 2026-11-17: TEXT_FORMS left the MIR): the product's JSON array, the edge's [a, b]
            com.legend.lowering.ListTexts.class,
            // a judge's JSON grammar (census C1, the orchestrator's ruling of 2026-11-17, register S144): the product's
            // strict JSON, the edge's upstream json-simple 1.1.1
            com.legend.testexecutionplan.JsonGrammar.class,
            // how a test counts a relation passed to assertSize/assertEmpty/assertNotEmpty (census C1, 2026-11-17,
            // register S137): the product refuses it, the edge counts it as one value
            com.legend.compiler.spec.RelationCounting.class,
            // a test-data generator's timestamp text (census C1, 2026-11-17, L9): the product's clean form, the
            // edge's legend-engine test-data CSV
            com.legend.testdatagen.TestDataPlanner.Printing.class,
            // two numbers of different kinds at planning (census C1, 2026-11-17, register S127): never equal in the
            // product, compiled legend-pure's Integer/Decimal-by-text at the edge
            CrossKindNumbers.class,
            // how the database judge plans a rendered text's equality (census C1, 2026-11-17, registers S131, S132):
            // the product's two strings, the edge's ruled comparison where they differ
            RenderedTextRule.class,
            // a date literal's text in the database judge's failure message (census C1, 2026-11-17): lite's own, the
            // edge's legend-pure toRepresentation
            DateLiteralTexts.class,
            // the word a rendered golden writes for a null cell (census C1, 2026-11-17): none in the product (lite
            // writes it empty), legend-engine's TDSNull at the edge
            com.legend.compiler.spec.GoldenNullWord.class,
            // an execute's routing activity (J5 of the edge audit, 2026-11-17): the product records none, the edge
            // legend-engine's AggregationAwareActivity, the routed query's print
            com.legend.planner.RoutedQueries.class,
            // the lowering's two-form choices (the audit's K5, the orchestrator's ruling of 2026-11-17: the product
            // branched on edge-only flags): a raise (B7's plain set), a view's nesting and a wire cast -- the product
            // writes every check, lowers a view flat and writes the cast; the edge's forms legend-engine's
            com.legend.lowering.LoweringForms.class,
            // the database judge's words of a failure (census J29, 2026-11-18; census C1's FailureWords, which named
            // whose words they were by a flag): lite's sentences, the edge's legend-pure words
            com.legend.lowering.VerdictWords.class,
            // the settings an H2 database has (the audit's J22, RULED by the user 2026-11-17): the product's H2's own
            // defaults, the edge's legend-engine's MODE=LEGACY and DEFAULT_NULL_ORDERING=HIGH
            com.legend.sql.dialect.DatabaseSettingsForm.class);
}
