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
 *
 * <p>legend-engine's statement shape (the engine SQL's lowering, {@code EnginePlans}) is {@link EngineShape}: the plan
 * style's forms with its own five (J40, 2026-11-18, the orchestrator's ruling (7): until then a LoweringShape the
 * Lowerer held beside the plan style's forms, a second source).
 */
record EdgeLoweringForms(Set<RaiseKind> plain) implements com.legend.lowering.LoweringForms {

    /** PCT's and the corpus's style ({@link PureText#UPSTREAM}): every check written, views flat, wire casts written. */
    static final EdgeLoweringForms PCT = new EdgeLoweringForms(Set.of());

    /** The pure/v1 route's style ({@link PureText#PURE_V1}): legend-engine's unchecked raises plain. */
    static final EdgeLoweringForms PURE_V1 = new EdgeLoweringForms(PureText.ENGINE_UNCHECKED);

    EdgeLoweringForms {
        plain = plain.isEmpty() ? Set.of() : java.util.Collections.unmodifiableSet(java.util.EnumSet.copyOf(plain));
    }

    @Override
    public SqlExpr raise(RaiseKind kind, Supplier<SqlExpr> checked, SqlExpr plainOperation) {
        return plain.contains(kind) ? plainOperation : checked.get();
    }

    @Override
    public SqlSelect view(SqlSelect body, Supplier<String> alias, String view) {
        return body;
    }

    @Override
    public SqlExpr wireCast(SqlExpr value, Supplier<SqlExpr> written) {
        return written.get();
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
        return ENGINE_TEST_CONNECTION;
    }

    /** legend-engine's JsonBuilder cardinality rule (the engine's {@code execute()} contract; the audit's J3,
     *  2026-11-17): exactly one object bare, zero or several as the array. */
    @Override
    public SqlSelect executeValues(Supplier<SqlSelect> array, Supplier<SqlSelect> wrapped) {
        return oneBare(wrapped.get());
    }

    /** legend-engine's JsonBuilder cardinality rule over a wrapped array's values: a singleton the bare object, anything
     *  else the array (the corpus goldens pin the dynamic unwrap). Moved from lowering.SnapshotEnvelope.fold (the
     *  orchestrator's ruling on L6, 2026-11-18: a two-form kind holds types only, and the fold is upstream's). */
    static SqlSelect oneBare(SqlSelect wrapped) {
        SqlSelect.Projection p = wrapped.projections().get(0);
        if (!(p.expr() instanceof SqlExpr.JsonArrayAgg ja)) {
            return wrapped;
        }
        SqlExpr count = new com.legend.sql.SqlAgg.Reducer(com.legend.sql.SqlAgg.Fn.COUNT, java.util.List.of(), false,
                java.util.List.of());
        SqlExpr sole = new com.legend.sql.SqlAgg.Reducer(com.legend.sql.SqlAgg.Fn.MIN, java.util.List.of(ja.value()),
                false, java.util.List.of());
        SqlExpr chosen = new SqlExpr.Case(java.util.List.of(new SqlExpr.Case.When(new SqlExpr.Call(
                com.legend.sql.SqlFn.EQUAL, java.util.List.of(count, new SqlExpr.IntLit(1))), sole)), ja,
                SqlExpr.Reason.CARRIER);
        return wrapped.withProjections(java.util.List.of(new SqlSelect.Projection(chosen, "result",
                new com.legend.sql.OutputCol("result", com.legend.sql.SqlType.Scalar.VARCHAR, false))));
    }

    /** legend-engine's statement shape writes a relation at each read; every other edge form, the product's lean SQL
     *  (the edge audit's J23, 2026-11-17). */
    @Override
    public com.legend.sql.SqlQuery relations(Supplier<com.legend.sql.SqlQuery> shared,
            Supplier<com.legend.sql.SqlQuery> unshared) {
        return shared.get();
    }

    /** legend-engine's statement shape counts a keyed relation's rows as written (J23). */
    @Override
    public com.legend.sql.SqlQuery keyedCounts(com.legend.sql.SqlQuery q,
            java.util.function.UnaryOperator<com.legend.sql.SqlQuery> fold) {
        return fold.apply(q);
    }

    /** legend-engine's statement shape keeps an unread computed projection (J23). */
    @Override
    public com.legend.sql.SqlQuery computedPruning(com.legend.sql.SqlQuery q) {
        return com.legend.lowering.SubselectPrune.prune(q, true);
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

    /** legend-engine's "second exists emission", on every edge style, where the planner's statement may take it (J19,
     *  register S367). */
    @Override
    public SqlSelect existsJoin(SqlSelect filtered, com.legend.lowering.ExistsSites sites) {
        return com.legend.sql.ExistsJoins.rewrite(filtered, sites.alias(), sites.zones(), ex -> true);
    }

    /** legend-engine's H2 isNumeric ({@code lower(s) = upper(s)}: a text with no cased letter), every edge style's
     *  (census J20, 2026-11-18: the product's is a real numeric check). */
    @Override
    public SqlExpr isNumeric(SqlExpr s) {
        return SqlExpr.Call.of(com.legend.sql.SqlFn.EQUAL, SqlExpr.Call.of(com.legend.sql.SqlFn.LOWER, s),
                SqlExpr.Call.of(com.legend.sql.SqlFn.UPPER, s));
    }

    /** {@code style} with legend-engine's statement shape: its forms {@link EngineShape} over the style's own. */
    static com.legend.lowering.Styled engineSql(com.legend.lowering.Styled style) {
        if (!(style.forms() instanceof EdgeLoweringForms base)) {
            throw new IllegalStateException("legend-engine's statement shape asked over a style that is not the edge's"
                    + ": only the edge plans engine SQL");
        }
        return new com.legend.lowering.Styled(style.pureOnly(), new EngineShape(base), style.dateTimes(),
                style.lists());
    }

    /**
     * legend-engine's STATEMENT SHAPE (the engine SQL's lowering, {@code EnginePlans}; J40, 2026-11-18, the orchestrator's
     * ruling (7)): a relation pasted at each read, keyed counts as written, unread computed projections kept (J23), a
     * view an aliased derived table ({@code ViewSelectSQLQuery}), a wire coercion left to its reader; every other form the plan style's own ({@code base}),
     * as the Lowerer asked them of the style before -- its raises, wire casts, EXISTS joins and the rest.
     */
    record EngineShape(EdgeLoweringForms base) implements com.legend.lowering.LoweringForms {

        @Override
        public com.legend.sql.SqlQuery relations(Supplier<com.legend.sql.SqlQuery> shared,
                Supplier<com.legend.sql.SqlQuery> unshared) {
            return unshared.get();
        }

        @Override
        public com.legend.sql.SqlQuery keyedCounts(com.legend.sql.SqlQuery q,
                java.util.function.UnaryOperator<com.legend.sql.SqlQuery> fold) {
            return q;
        }

        @Override
        public com.legend.sql.SqlQuery computedPruning(com.legend.sql.SqlQuery q) {
            return com.legend.lowering.SubselectPrune.prune(q, false);
        }

        @Override
        public SqlSelect view(SqlSelect body, Supplier<String> alias, String view) {
            return SqlSelect.starOf(new SqlSource.Subselect(body, alias.get(), view));
        }

        @Override
        public SqlExpr raise(RaiseKind kind, Supplier<SqlExpr> checked, SqlExpr plain) {
            return base.raise(kind, checked, plain);
        }

        /** legend-engine's SQL leaves a mapping's wire coercion to its reader, which converts on the wire. */
        @Override
        public SqlExpr wireCast(SqlExpr value, Supplier<SqlExpr> written) {
            return value;
        }

        @Override
        public SqlSelect executeValues(Supplier<SqlSelect> array, Supplier<SqlSelect> wrapped) {
            return base.executeValues(array, wrapped);
        }

        @Override
        public SqlExpr isNumeric(SqlExpr s) {
            return base.isNumeric(s);
        }

        @Override
        public String objectReferenceConnection() {
            return base.objectReferenceConnection();
        }

        @Override
        public SqlExpr parsedDecimal(SqlExpr text) {
            return base.parsedDecimal(text);
        }

        @Override
        public SqlExpr parsedJson(SqlExpr text) {
            return base.parsedJson(text);
        }

        @Override
        public SqlSelect existsJoin(SqlSelect filtered, com.legend.lowering.ExistsSites sites) {
            return base.existsJoin(filtered, sites);
        }
    }
}
