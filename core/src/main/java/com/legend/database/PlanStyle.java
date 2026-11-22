// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.database;

import com.legend.model.ConnectionDefinition.DatabaseType;
import com.legend.sql.dialect.SqlDialect;

/**
 * THE PLAN STYLE (renamed from SqlSpelling 2026-11-12, the orchestrator's ruling: it will carry the lowering strategies
 * too, D's lowering hook). The spelling hook (phase D's D1.0, docs/data-delivery-homework/D_BRIEF_2026_11_10.md §0;
 * the design approved by the orchestrator 2026-11-12): where a plan's statements take their dialect from, handed to the planner by its door. The
 * product's doors write {@link #PRODUCT} -- the product's own dialects, lite's own spellings -- with no null,
 * no mode and no default branch anywhere below them. The compatibility edge ({@code com.legend.edge}) is the one other
 * supplier: its dialects subclass the product's and override only the arms that spell legend-pure's or legend-engine's
 * text, so the same typed plan is spelled upstream's way for PCT, the corpus and pure/v1 (G5: the edge depends on the
 * product, never the reverse). A memo of anything a dialect wrote keys on the dialect it was written in (the plan
 * caches' rule, the orchestrator's addition 1).
 */
public interface PlanStyle {

    /** The dialect a statement on {@code type} is written in; a database with none refused by name, as
     *  {@link Databases#dialect} refuses it. */
    SqlDialect dialect(DatabaseType type);

    /** AN EXECUTION'S TRACE TEXT in a TDS cell, around the execution's id bound when the plan runs (the separation
     *  census's B8 (ii), 2026-11-17; D1.4, 2026-11-15): the product's is the id itself (null: nothing around it); the
     *  compatibility edge's is legend-engine's trace comment around it. A two-form hook (the orchestrator's ruling of
     *  2026-11-17): the product answers its own lite form, the edge's override alone holds the upstream form. */
    @com.legend.base.Nullable
    TraceCell traceCell();

    // (plainRaises, B7's set of the raises a plan writes plain: gone 2026-11-17, the audit's K5 -- the two-form kind
    // lowering.LoweringForms, asked by twoForm: the product's writes every check, the edge's pure/v1 form legend-engine's
    // census class A plain, docs/records/pure-v1-engine-census-2026-11-17.md)

    /** The text written before and after an execution's id in a trace cell. */
    record TraceCell(String before, String after) {
    }

    /** A TWO-FORM HOOK's form (the orchestrator's ruling of 2026-11-17, census B9; F's design): a value with a
     *  PRODUCT form, lite's one canonical form ({@code productForm}, the caller's, passed explicitly every time: no
     *  default), and an upstream form only the compatibility edge holds. The product's style answers
     *  {@code productForm}; the edge's answers its own implementation of {@code kind}. The kinds are a CLOSED list,
     *  declared beside the test planner ({@code testplan.PlanStyleFacets.TWO_FORMS}: this layer cannot name them); a
     *  kind outside it is refused by name by the edge, and {@code HookRulingTest} pins the list as an exact set. */
    <F> F twoForm(Class<F> kind, F productForm);

    /** A graph fetch's key argument that is a date, from its written text (no zone): the product's text of the type
     *  (a DateTime ISO 8601 in UTC with nine fraction digits and {@code Z}, a date as written), the edge legend-engine's
     *  ({@code +0000} after a time) (phase D's D1.2, 2026-11-15; register S312). */
    String graphKeyDate(String written);

    /** A graph fetch's key argument that is a String: the product's text, the String itself; the edge legend-engine's,
     *  quoted (D1.2). */
    String graphKeyString(String value);

    // (errorWords, an error's message in the style's words where a test plans it as data: deleted 2026-11-20
    // with stage B's B1 -- a planned failure carries lite's message and its kinded part as data, and every audience
    // words it after the run)

    // (siteWords, the words of a statement's raise sites offered at planning to the edge's recorder: deleted 2026-11-20
    // with stage B's B1 -- a site's record carries its operands, and every audience words it after the run)

    /** A TEST-PLANNING FACET of the style (W9's judge half, the orchestrator's ruling of 2026-11-16): the one hook's
     *  typed accessor for the parts of a test's planning that are upstream's -- an engine-text assert's arm. The kinds
     *  are a CLOSED list, declared beside the test planner ({@code testplan.PlanStyleFacets.KINDS}: this layer cannot
     *  name them); a style answers exactly those, and a kind outside the list fails BY NAME at the call, never null and
     *  never a default. The product answers none: each is the compatibility edge's, refused by name. */
    <F> F facet(Class<F> kind);

    /** Whether this style judges an assert over legend-engine's text (census C4, the orchestrator's ruling of
     *  2026-11-17: a capability, types only): the edge's does, through its {@code EngineTestText} facet; the
     *  product's does not, so such an assert takes the ordinary path and its upstream text native is refused by name
     *  at lowering. */
    boolean judgesEngineText();

    /** {@link #facet(Class)}, asked where the call that needs it is known (the separation census's B17, 2026-11-17: a
     *  lowering facet, {@code lowering.RelationTexts}): the product's style throws {@code refusal}'s exception -- the
     *  call's own sentence, a user can act on, and its phase -- the edge's answers as {@link #facet(Class)} does. */
    <F> F facet(Class<F> kind, java.util.function.Supplier<? extends RuntimeException> refusal);

    /** THE PRODUCT'S STYLE (phase D's D1.0): each declared type's statements written in the product's own dialect
     *  ({@link Databases#dialect}), and none of legend-engine's text; the compatibility edge hands its own (G5). */
    PlanStyle PRODUCT = new PlanStyle() {
        @Override
        public SqlDialect dialect(DatabaseType type) {
            return Databases.dialect(type);
        }


        @Override
        public boolean judgesEngineText() {
            return false;
        }

        @Override
        public <F> F facet(Class<F> kind) {
            throw new com.legend.error.NotImplementedException("the test-planning facet " + kind.getName()
                    + ": upstream's forms are the compatibility edge's (com.legend.edge), never the product's");
        }

        @Override
        public <F> F facet(Class<F> kind, java.util.function.Supplier<? extends RuntimeException> refusal) {
            throw refusal.get();
        }



        @Override
        public @com.legend.base.Nullable TraceCell traceCell() {
            return null;
        }

        /** The product's form, the caller's own: lite's canonical form, no upstream knowledge. */
        @Override
        public <F> F twoForm(Class<F> kind, F productForm) {
            return kind.cast(java.util.Objects.requireNonNull(productForm, "a two-form hook's product form"));
        }

        @Override
        public String graphKeyDate(String written) {
            return dateTimeText(written);
        }

        @Override
        public String graphKeyString(String value) {
            return value;
        }
    };

    /** A literal date's written text ({@code 2023-10-15}, {@code 2023-10-15T10}, {@code 2023-10-15T10:30:00.5}) as the
     *  product's text of its type: a date with no time as written; a DateTime ISO 8601 in UTC with nine fraction digits
     *  and {@code Z}, its missing parts zero (the literal is already in UTC; a tenth digit is refused when the literal is
     *  lowered). A constant spelled at planning, as its written text was (register S312, amended 2026-11-22). */
    private static String dateTimeText(String written) {
        int t = written.indexOf('T');
        if (t < 0) {
            return written;
        }
        String[] hms = written.substring(t + 1).split(":", -1);
        String seconds = hms.length > 2 ? hms[2] : "00";
        int dot = seconds.indexOf('.');
        String whole = dot < 0 ? seconds : seconds.substring(0, dot);
        String fraction = ((dot < 0 ? "" : seconds.substring(dot + 1)) + "000000000").substring(0, 9);
        return written.substring(0, t) + "T" + hms[0] + ":" + (hms.length > 1 ? hms[1] : "00") + ":" + whole + "."
                + fraction + "Z";
    }

}
