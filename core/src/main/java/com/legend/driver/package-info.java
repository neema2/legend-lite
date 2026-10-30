// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

/**
 * THE DRIVER (step C, D73, 2026-10-30; docs/RESULT_BOUNDARY_START_HERE.md, "The seam and the wall"): the execution
 * side's machinery behind the root's two execution doors ({@code Execution}, {@code ExecuteOptions}) -- the body
 * driver ({@link com.legend.driver.BodyDriver}, a test body as a sequence of plans, called by {@code Execution} alone:
 * {@code ArchitectureTest.theDriverHasOneCaller}), the body compiler, the judges' arms and their plans
 * ({@code HostJudge}, {@code DatabaseJudge}, {@code AgreementArm}, {@code AssertVerdicts}, {@code JudgePlans},
 * {@code SqlTextVerdicts}), the seeds and loads ({@code CsvLoad}, {@code SeedSqlForms}, {@code PlanEnvelope},
 * {@code PlanAllocations}), the schema probes ({@code ColumnProbe}, {@code GridProbe}, {@code DynamicPivot},
 * {@code WireProbe}: a probe asks the plan runner) and the graph's system database ({@code SystemDatabase}). This is
 * the one layer that holds a compiled model and a session at once: it depends on the planner and on the runtime
 * ({@code exec}), and neither depends on it. Until D73 these files shared the root package with the doors and with the
 * planner's machinery; the package line makes the wall the language's. A closed register
 * ({@code JavaEvalLedgerTest.DRIVER_CLASSES}); no JDBC ({@code ArchitectureTest.theBodyDriverAndTheJudgesHoldNoJdbc}).
 */
package com.legend.driver;
