// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.edge;

import com.legend.sql.SqlExpr;
import com.legend.sql.SqlSelect;


/**
 * THE EDGE'S FORMS OF THE LOWERING'S TWO-FORM CHOICES ({@link com.legend.lowering.LoweringForms}, the audit's K5,
 * 2026-11-17): legend-engine's choices, held here so the product branches on none of them. (The raises a door writes
 * as the plain operation, B7, were a set held here until the edition program's step 3, 2026-11-26: the door's
 * edition's table says which checks are written, {@code EngineTable.checks}.)
 *
 * <p>legend-engine's statement shape for its SQL text ({@code EngineShape}, J40) went with that text (A2 of the
 * compatibility program, 2026-11-19, CP-4): the edge's SQL text is lite's own statement.
 */
final class EdgeLoweringForms implements com.legend.lowering.LoweringForms {

    /** The compatibility doors' forms, PCT's, the corpus's and pure/v1's alike: which checks a door's audience writes
     *  is its edition's table's since the edition program's step 3 (2026-11-26; until then a set of raise kinds here
     *  set pure/v1's apart). */
    static final EdgeLoweringForms INSTANCE = new EdgeLoweringForms();

    private EdgeLoweringForms() {
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
    public SqlSelect executeValues(SqlSelect own) {
        // a many graph's array folded to one object bare where it holds one; a to-one graph's object is already bare
        return oneBare(own);
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
        return com.legend.sql.ExistsJoins.rewrite(filtered, sites.alias(), sites.keysWhere(), ex -> true);
    }

    /** legend-engine's conjunct zones, on every edge style (L10): its exists form reads them ({@code existsJoin}, J19 --
     *  user conditions, then correlation, then temporal). */
    @Override
    public com.legend.lowering.WhereZones whereZones() {
        return new EngineWhereZones();
    }
}
