// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

/**
 * THE PLANNER'S MACHINERY (step C, D73, 2026-10-30; docs/RESULT_BOUNDARY_START_HERE.md, "The seam and the wall"): a
 * typed query to its plan -- the plan maker ({@link com.legend.planner.PlanMaker}: a typed query, a runtime and a
 * server version to an execution plan), the system metamodel store's rows ({@link com.legend.planner.MetamodelSeeds},
 * {@code OpSeeds}), a program's facts ({@link com.legend.planner.ProgramFacts}), a query's parameters
 * ({@link com.legend.planner.QueryParameters}) and the cross-store guard. Part of {@code //core:planner} with the
 * root's three plan-side doors ({@code Compiler}, {@code TypedQuery}, {@code HeldModels}), which are the product's API;
 * this package is theirs and the driver's. Until D73 these files shared the root package with the doors and with the
 * driver, and Bazel alone told the two sides apart; the package line makes the planner-driver wall the language's too
 * (nothing here depends on {@code com.legend.driver}: the build has no such edge, and
 * {@code ArchitectureTest.plannerIsExecutionFree} says it at the class level). A closed register
 * ({@code JavaEvalLedgerTest.PLANNER_CLASSES}).
 */
package com.legend.planner;
