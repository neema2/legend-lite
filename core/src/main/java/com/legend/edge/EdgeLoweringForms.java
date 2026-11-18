// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.edge;

import com.legend.sql.RaiseKind;
import com.legend.sql.SqlExpr;
import com.legend.sql.SqlSelect;
import com.legend.sql.SqlSource;

import java.util.Set;
import java.util.function.Supplier;

/**
 * THE EDGE'S FORMS OF THE LOWERING'S TWO-FORM CHOICES ({@link com.legend.lowering.LoweringForms}, the audit's K5,
 * 2026-11-17): legend-engine's choices, held here so the product branches on none of them.
 *
 * @param plain         the raises written as the plain operation, the database's own error passing through: none for
 *                      PCT and the corpus (legend-pure's guarded words), legend-engine's census class A for the pure/v1
 *                      route (B7, the user's ruling of 2026-11-17: "pure/v1 matches legend-engine")
 * @param engineSqlShape legend-engine's statement shape (the engine SQL's lowering, {@code EnginePlans}): a view an
 *                      aliased derived table ({@code ViewSelectSQLQuery}), a wire coercion left to the reader, and none
 *                      of the product's lean SQL (a relation at each read, keyed counts as written, unread computed
 *                      projections kept: J23)
 */
record EdgeLoweringForms(Set<RaiseKind> plain, boolean engineSqlShape) implements com.legend.lowering.LoweringForms {

    /** PCT's and the corpus's style ({@link PureText#UPSTREAM}): every check written, views flat, wire casts written. */
    static final EdgeLoweringForms PCT = new EdgeLoweringForms(Set.of(), false);

    /** The pure/v1 route's style ({@link PureText#PURE_V1}): legend-engine's unchecked raises plain. */
    static final EdgeLoweringForms PURE_V1 = new EdgeLoweringForms(PureText.ENGINE_UNCHECKED, false);

    /** legend-engine's statement shape, the engine SQL's ({@code EnginePlans.UPSTREAM_SHAPE}): asked for its views and
     *  wire casts; its raises are the plan style's, never this shape's. */
    static final EdgeLoweringForms ENGINE_SQL_SHAPE = new EdgeLoweringForms(Set.of(), true);

    EdgeLoweringForms {
        plain = plain.isEmpty() ? Set.of() : java.util.Collections.unmodifiableSet(java.util.EnumSet.copyOf(plain));
    }

    @Override
    public SqlExpr raise(RaiseKind kind, Supplier<SqlExpr> checked, SqlExpr plainOperation) {
        if (engineSqlShape) {
            throw new IllegalStateException("a raise asked of legend-engine's statement shape: the raises are the plan"
                    + " style's (Styled.forms), the shape's forms its views and wire casts");
        }
        return plain.contains(kind) ? plainOperation : checked.get();
    }

    @Override
    public SqlSelect view(SqlSelect body, Supplier<String> alias, String view) {
        return engineSqlShape ? SqlSelect.starOf(new SqlSource.Subselect(body, alias.get(), view)) : body;
    }

    @Override
    public SqlExpr wireCast(SqlExpr value, Supplier<SqlExpr> written) {
        return engineSqlShape ? value : written.get();
    }

    /** legend-engine's protocol connection segment for its test database (H2's, as the goldens pin it), which its
     *  object references carry whatever the database (moved from model.AsorRef, the audit's J12, 2026-11-17). */
    static final String ENGINE_TEST_CONNECTION =
            "{\"_type\":\"RelationalDatabaseConnection\","
            + "\"authenticationStrategy\":{\"_type\":\"h2Default\"},"
            + "\"datasourceSpecification\":{\"_type\":\"h2Local\"},"
            + "\"element\":\"\",\"postProcessorWithParameter\":[],"
            + "\"postProcessors\":[],\"timeZone\":\"GMT\","
            + "\"type\":\"H2\"}";

    @Override
    public String objectReferenceConnection() {
        if (engineSqlShape) {
            throw new IllegalStateException("an object reference's connection asked of legend-engine's statement"
                    + " shape: it is the plan style's (Styled.forms)");
        }
        return ENGINE_TEST_CONNECTION;
    }

    /** legend-engine's JsonBuilder cardinality rule (the engine's {@code execute()} contract; the audit's J3,
     *  2026-11-17): exactly one object bare, zero or several as the array. */
    @Override
    public SqlSelect executeValues(Supplier<SqlSelect> array, Supplier<SqlSelect> oneBare) {
        if (engineSqlShape) {
            throw new IllegalStateException("an execute's values asked of legend-engine's statement shape: the root's"
                    + " form is the plan style's (Styled.forms), the shape's forms its views and wire casts");
        }
        return oneBare.get();
    }

    /** legend-engine's statement shape writes a relation at each read; every other edge form, the product's lean SQL
     *  (the edge audit's J23, 2026-11-17). */
    @Override
    public com.legend.sql.SqlQuery relations(Supplier<com.legend.sql.SqlQuery> shared,
            Supplier<com.legend.sql.SqlQuery> unshared) {
        return engineSqlShape ? unshared.get() : shared.get();
    }

    /** legend-engine's statement shape counts a keyed relation's rows as written (J23). */
    @Override
    public com.legend.sql.SqlQuery keyedCounts(com.legend.sql.SqlQuery q,
            java.util.function.UnaryOperator<com.legend.sql.SqlQuery> fold) {
        return engineSqlShape ? q : fold.apply(q);
    }

    /** legend-engine's statement shape keeps an unread computed projection (J23). */
    @Override
    public com.legend.sql.SqlQuery computedPruning(Supplier<com.legend.sql.SqlQuery> pruned,
            Supplier<com.legend.sql.SqlQuery> kept) {
        return engineSqlShape ? kept.get() : pruned.get();
    }

    /** legend-engine's one-argument parseDecimal on H2: its hardcoded {@code decimal(5, 2)}
     *  (h2Extension2_1_214.pure transformParseDecimalH2; its goldens round through it, 123.450021 to 123.45; the audit's
     *  J1, moved from the product's lowering). */
    @Override
    public SqlExpr parsedDecimal(SqlExpr text) {
        return new SqlExpr.Cast(text, new com.legend.sql.SqlType.Decimal(5, 2), SqlExpr.Reason.USER);
    }

    /** legend-engine's parser's one tolerance (Json.parseOne lenient: corpus goldens with a missing comma between array
     *  object elements, testSubTypeAtRootLevelWithInheritanceMapping's expected text): {@code '}{'} reads as
     *  {@code '},{'} before the strict JSON cast (the audit's J2, moved from the product's lowering). */
    @Override
    public SqlExpr parsedJson(SqlExpr text) {
        return new SqlExpr.Cast(SqlExpr.Call.of(com.legend.sql.SqlFn.REGEXP_REPLACE, text,
                new SqlExpr.StringLit("\\}\\s*\\{"), new SqlExpr.StringLit("},{"), new SqlExpr.StringLit("g")),
                com.legend.sql.SqlType.Scalar.JSON, SqlExpr.Reason.CARRIER);
    }
}
