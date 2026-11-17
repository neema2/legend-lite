// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.planner;

import com.legend.compiler.spec.NativeDispatch;
import com.legend.compiler.spec.SpecCompiler;
import com.legend.compiler.spec.typed.TypedNativeCall;
import com.legend.compiler.spec.typed.TypedSpec;

import java.util.List;

/**
 * UPSTREAM'S PLAN TEXT, A FACET OF THE PLAN STYLE (W9's planner half, docs/records/d-plan-2026-11-16.md, 2026-11-16):
 * what legend-engine prints at planning -- a {@code planToString} or {@code toSQLString} call's text, a deprecated
 * {@code toDDL} statement's, the node model behind a {@code $plan} handle's rows, an execute's activity SQL. All of it is
 * compatibility, so the compatibility edge implements it ({@code com.legend.edge}) and the product's style refuses it by
 * name ({@code PlanStyle.facet}); the planner names none of upstream's text. A declared kind of the closed list
 * ({@code testplan.PlanStyleFacets.KINDS}). A hook of signatures only (the terminal-hook criteria, 2026-11-17): the
 * plan-text natives' routine table that asks it is the planner's, {@link PlanTextRoutines}.
 */
public interface UpstreamPlans {

    /** {@code planToString(executionPlan(...))}: the engine's plan text. */
    String planToString(NativeDispatch.RoutineCall call, List<TypedSpec> letPrefix, SpecCompiler specs, PlanEnv env);

    /** {@code toSQLString} and its forms: the engine's SQL text of a query. */
    String toSqlString(NativeDispatch.RoutineCall call, List<TypedSpec> letPrefix, SpecCompiler specs, PlanEnv env);

    /** A deprecated {@code toDDL} statement's text ({@code createTableStatement} and its family). */
    String ddlText(TypedNativeCall ds, PlanEnv env);

    /** The node model of an {@code executionPlan} call, for its handle's rows. */
    com.legend.plan.PlanNode planModel(TypedNativeCall executionPlan, SpecCompiler specs, PlanEnv env);

    /** An execute's activity SQL in legend-engine's text, {@code recorded} its runtime's post-processors applied. */
    String activitySql(List<TypedSpec> chain, @com.legend.base.Nullable String mappingFqn, SpecCompiler specs,
            PlanEnv env, boolean recorded);

    /** legend-engine's setup SQL for a seed's CSV on its test database ({@code setUpDataSqls}): the DDL and the
     *  inserts as its text prints them (Phase 6's A, 2026-11-17: moved off PlanStyle, which names no engine text). */
    List<String> engineSeedSql(String csv, com.legend.model.DatabaseDefinition db,
            java.util.function.Function<String, java.util.Optional<com.legend.model.DatabaseDefinition>> lookup);
    /** {@link #engineSeedSql} for a seed written as records. */
    List<String> engineSeedSqlFromRecords(List<List<String>> records, com.legend.model.DatabaseDefinition db,
            java.util.function.Function<String, java.util.Optional<com.legend.model.DatabaseDefinition>> lookup);
    /** The test-data generator's plan text, naming legend-engine's test connection, as every engine test of it does. */
    String generatedTestDataPlan(com.legend.compiler.spec.typed.TypedTestDataGen tg,
            com.legend.compiler.element.ModelContext ctx);
    /** A timestamp cell's text as legend-engine's test-data CSV writes it ({@code toCSVString}'s
     *  {@code SimpleDateTimeFormat}, {@code yyyy-MM-dd HH:mm:ss}, its fraction cut; legend-engine helperFunctions.pure:214-243),
     *  for the test-data generator's printing under the compatibility rules and the server's routes (Phase 6's B13,
     *  2026-11-17); lite's own spelling is the generator's {@code Printing.CLEAN}. */
    java.util.function.UnaryOperator<com.legend.sql.SqlExpr> testDataTimestampText();
}
