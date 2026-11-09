// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

/**
 * THE TEST'S RUN (2026-11-09, the plan-as-data design's slice 5, docs/PLAN_AS_DATA_2026_11_07.md): a test plan and a
 * session to its verdicts, and nothing else. {@link com.legend.testexec.TestRunner#run} is the one entry: it hands
 * the plan's program to exec's runner and judges each verdict step's rows as the plan's data names them -- the host
 * lattice ({@link com.legend.testexec.PureAsserts}, {@link com.legend.testexec.Equality}), the grid compare
 * ({@link com.legend.testexec.TdsCompare}), the canonical instruments, the database judge's root loop, the agreement
 * rule and the held appeals decided after the run. It reads the plan (the records of
 * {@code com.legend.testexecutionplan}) and the rows the database returned; it reaches no planning library
 * ({@code ArchitectureTest.theRunReachesNoPlanning}) and planning reaches none of it
 * ({@code ArchitectureTest.planningReachesNoRun}). The judge and driver packages' run halves until slice 5; a closed
 * register ({@code JavaEvalLedgerTest.TESTEXEC_CLASSES}).
 */
package com.legend.testexec;
