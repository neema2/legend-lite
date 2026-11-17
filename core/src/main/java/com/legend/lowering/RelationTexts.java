// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.lowering;

import com.legend.compiler.element.type.Type;
import com.legend.compiler.spec.typed.TypedNativeCall;
import com.legend.compiler.spec.typed.TypedPropertyAccess;
import com.legend.compiler.spec.typed.TypedSpec;
import com.legend.error.LegendCompileException;
import com.legend.error.ModelException;
import com.legend.sql.SqlExpr;
import com.legend.sql.SqlSelect;

import java.util.Map;
import java.util.function.Function;

/**
 * A RELATION'S TEXT, A LOWERING FACET OF THE PLAN STYLE (the separation census's B17, 2026-11-17; the orchestrator's
 * approval of the design note, the shape of {@code PlanStyle.facet}): {@code toCSV} (legend-pure's helperFunctions
 * CSV), a TDS's {@code csv} property and a relation's {@code toString} (its {@code #TDS} text) are legend-pure's texts
 * of rows, so their composition is the compatibility edge's ({@code edge.RelationTextLowering}); the product writes a
 * relation's rows by its result writers and refuses each by name AT LOWERING ({@link #refused}). The Lowerer asks the
 * planner's style for this facet only when it meets one of the three calls, carrying the call's own refusal.
 *
 * <p>Its static members are the lowering's helpers the edge's composition reads (each forwards one call; one
 * {@code ArchitectureTest.PLANNER_ENTRIES} row admits them, as {@code testplan.EngineTextPlanning}'s): the product's
 * text of a value, the date texts and a mixed column's kind.
 */
public interface RelationTexts {

    /** {@code toCSV} of a relation ({@code tc}), its rows read by {@code relation}. */
    SqlExpr toCsv(TypedNativeCall tc, Function<TypedSpec, SqlSelect> relation, String alias,
            Map<Integer, Type.RelationType> deferredTds);

    /** A TDS's {@code csv} property over a relation ({@code read}). */
    SqlExpr csvProperty(TypedPropertyAccess read, Function<TypedSpec, SqlSelect> relation, String alias);

    /** A relation's {@code toString} ({@code tc}): its {@code #TDS} text. */
    SqlExpr relationToString(TypedNativeCall tc, Function<TypedSpec, SqlSelect> relation, String alias,
            Map<Integer, Type.RelationType> deferredTds);

    /** The product's refusal of {@code what} of a relation, at lowering, naming the call and the product's way (the
     *  text the product's renderer gave it until B17: dialect.PureOnlyText's). */
    static ModelException refused(String what) {
        return new ModelException(LegendCompileException.Phase.LOWER, what + " of a relation is legend-pure's text,"
                + " the compatibility edge's: the product writes a relation's rows by its result writers (JSON, CSV,"
                + " Arrow)");
    }

    // ---- the lowering's helpers the edge's composition reads (each one call) ----------------------------------------

    /** The product's text of a value of type {@code t} ({@code Scalars.pureToString}). */
    static SqlExpr pureToString(PureOnlyTexts texts, Type t, SqlExpr x) {
        return Scalars.pureToString(new Styled(refusal -> texts), t, x);
    }

    /** {@code LiteralSpelling.dateTimeText}. */
    static SqlExpr dateTimeText(SqlExpr x) {
        return LiteralSpelling.dateTimeText(x);
    }

    /** {@code LiteralSpelling.dateTimeTextTds}. */
    static SqlExpr dateTimeTextTds(SqlExpr x) {
        return LiteralSpelling.dateTimeTextTds(x);
    }

    /** {@code LiteralSpelling.dateTimeTextOfText}. */
    static SqlExpr dateTimeTextOfText(SqlExpr text) {
        return LiteralSpelling.dateTimeTextOfText(text);
    }

    /** {@code LiteralSpelling.timed}. */
    static SqlExpr timed(SqlExpr c) {
        return LiteralSpelling.timed(c);
    }

    /** A mixed column's kind's type ({@code MixedEncoding.kindType}). */
    static Type kindType(String kind) {
        return MixedEncoding.kindType(kind);
    }
}
