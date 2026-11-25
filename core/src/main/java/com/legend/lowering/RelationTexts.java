// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.lowering;

import com.legend.compiler.element.type.Type;
import com.legend.compiler.spec.typed.TypedPropertyAccess;
import com.legend.compiler.spec.typed.TypedSpec;
import com.legend.sql.SqlExpr;
import com.legend.sql.SqlSelect;

import java.util.Map;
import java.util.function.Function;

/**
 * A RELATION'S TEXT, A LOWERING FACET OF THE PLAN STYLE (the separation census's B17, 2026-11-17; the orchestrator's
 * approval of the design note, the shape of {@code PlanStyle.facet}): a TDS's {@code csv} property is legend-engine's
 * text of rows, so its composition is the compatibility edge's ({@code edge.RelationTextLowering}); the product refuses
 * it by name AT LOWERING ({@code TextRefusals.relation}, the call site's words). The Lowerer asks the planner's style
 * for this facet only when it meets the call, carrying the call's own refusal. (A relation's {@code toString} and the
 * helper's {@code toCSV} left the facet with stage B's B2b S5a, 2026-11-24, and S5b, 2026-11-25: lite's own texts,
 * computed by the database on every plan style, ProductRelationText, their Float and DateTime cells the edge's
 * until S6. The facet goes with S5c.)
 *
 * <p>Its static members are the lowering's helpers the edge's composition reads (each forwards one call;
 * {@code ArchitectureTest.PLANNER_ENTRIES} lists each one the edge reaches, by its full signature, as
 * {@code testplan.EngineTextPlanning}'s: the GUARD stream's C.4, 2026-11-17): the product's text of a value and a
 * mixed column's kind -- each a product helper the product itself uses (the
 * orchestrator's ruling of 2026-11-17 on facades; the date texts only a relation's text wrote moved to the edge with
 * it, edge.RelationTextLowering).
 */
@com.legend.base.OpenLine("B17") public interface RelationTexts {

    // (toCsv: gone 2026-11-25, stage B's B2b S5b -- lite's own CSV, computed by the database on every plan style,
    // ProductRelationText.csv)

    /** A TDS's {@code csv} property over a relation ({@code read}), its rows read by {@code relation}, under the
     *  planner's {@code styled} (each member is handed the style its cells are printed under; the edge audit's J26,
     *  2026-11-17). */
    SqlExpr csvProperty(Styled styled, TypedPropertyAccess read, Function<TypedSpec, SqlSelect> relation, String alias);

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
