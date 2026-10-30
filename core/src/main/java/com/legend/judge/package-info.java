// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

/**
 * THE JUDGE LAYER (D71, 2026-10-30, the seam-and-wall program's B1; docs/RESULT_BOUNDARY_START_HERE.md, "The seam
 * and the wall"): the verdict of a test body's assertion, and nothing a product query runs on. The host lattice
 * ({@link com.legend.judge.PureAsserts}, {@link com.legend.judge.Equality}), the grid compare
 * ({@link com.legend.judge.TdsCompare}), the canonical instruments ({@link com.legend.judge.CanonicalForm},
 * {@link com.legend.judge.CanonicalDivergence}), the verdict batch, the SQL replay referee, the judges' collected
 * side ({@link com.legend.judge.TypedSide}), the canon rider, the instance ids, the type and primitive censuses and
 * the assert listener lived in {@code com.legend.exec} until D71: they read the compiler's types and render through
 * the dialect, planning-side work the runtime ({@code exec}: a plan and a session to typed rows) must not carry.
 * Reachable only from the verdict seam ({@code ArchitectureTest.hostVerdictIsReachableOnlyFromTheVerdictSeam}); a
 * closed register ({@code JavaEvalLedgerTest.JUDGE_CLASSES}); no JDBC
 * ({@code ArchitectureTest.theBodyDriverAndTheJudgesHoldNoJdbc}).
 */
package com.legend.judge;
