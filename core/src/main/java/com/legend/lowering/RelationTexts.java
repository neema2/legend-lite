// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.lowering;

import com.legend.compiler.element.type.Type;
import com.legend.compiler.spec.typed.TypedNativeCall;
import com.legend.compiler.spec.typed.TypedPropertyAccess;
import com.legend.compiler.spec.typed.TypedSpec;
import com.legend.sql.SqlExpr;
import com.legend.sql.SqlSelect;

import java.util.Map;
import java.util.function.Function;

/**
 * A RELATION'S TEXT, A LOWERING FACET OF THE PLAN STYLE (the separation census's B17, 2026-11-17; the orchestrator's
 * approval of the design note, the shape of {@code PlanStyle.facet}): {@code toCSV} (legend-pure's helperFunctions
 * CSV), a TDS's {@code csv} property and a relation's {@code toString} (its {@code #TDS} text) are legend-pure's texts
 * of rows, so their composition is the compatibility edge's ({@code edge.RelationTextLowering}); the product writes a
 * relation's rows by its result writers and refuses each by name AT LOWERING ({@code TextRefusals.relation}, the call site's words). The Lowerer asks the
 * planner's style for this facet only when it meets one of the three calls, carrying the call's own refusal.
 *
 * <p>Its static members are the lowering's helpers the edge's composition reads (each forwards one call;
 * {@code ArchitectureTest.PLANNER_ENTRIES} lists each one the edge reaches, by its full signature, as
 * {@code testplan.EngineTextPlanning}'s: the GUARD stream's C.4, 2026-11-17): the product's text of a value and a
 * mixed column's kind -- each a product helper the product itself uses (the
 * orchestrator's ruling of 2026-11-17 on facades; the date texts only a relation's text wrote moved to the edge with
 * it, edge.RelationTextLowering).
 */
@com.legend.base.OpenLine("B17") public interface RelationTexts {

    /** {@code toCSV} of a relation ({@code tc}), its rows read by {@code relation}, under the planner's {@code styled}
     *  (each member is handed the style its cells are printed under; the edge audit's J26, 2026-11-17). */
    SqlExpr toCsv(Styled styled, TypedNativeCall tc, Function<TypedSpec, SqlSelect> relation, String alias);

    /** A TDS's {@code csv} property over a relation ({@code read}). */
    SqlExpr csvProperty(Styled styled, TypedPropertyAccess read, Function<TypedSpec, SqlSelect> relation, String alias);

    /** A relation's {@code toString} ({@code tc}): its {@code #TDS} text. */
    SqlExpr relationToString(Styled styled, TypedNativeCall tc, Function<TypedSpec, SqlSelect> relation, String alias);

    // (refused(what): moved 2026-11-17 to TextRefusals.relation, the call site words its refusal -- the
    // orchestrator's ruling: the hook holds its signatures and its product-helper forwarders alone)

    // ---- the lowering's helpers the edge's composition reads (each one call) ----------------------------------------

    /** The text of a value of type {@code t} under the planner's style, as it handed it to the facet
     *  ({@code Scalars.pureToString}; the edge audit's J26, 2026-11-17: the style itself, no substitute built here). */
    static SqlExpr pureToString(Styled styled, Type t, SqlExpr x) {
        return Scalars.pureToString(styled, t, x);
    }




    /** A mixed column's kind's type ({@code MixedEncoding.kindType}). */
    static Type kindType(String kind) {
        return MixedEncoding.kindType(kind);
    }
}
