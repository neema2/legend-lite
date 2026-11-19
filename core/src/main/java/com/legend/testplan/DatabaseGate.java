// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.testplan;

/**
 * THE DATABASE JUDGE'S STATIC GATE for an assert: whether the database judge plans the assert's verdict without reading
 * a side at planning ({@link #PLANS}), raises at planning ({@link #RAISES}), or does not plan it ({@link #UNPLANNED},
 * the body is given back). Data, nothing evaluated.
 *
 * <p>An enum again since A4 of the compatibility program (stage A ratified by the user 2026-11-19). From J41
 * (2026-11-18) it was a record that also said whether the arm planned under the style's text collector
 * ({@code collects}) and whether its verdict was appealed at its step ({@code appealed}), so that the edge's engine-text
 * arm could answer its own gate. The host judge is the only judge in compatibility, so the database judge plans no
 * engine text and hears no appeal: both facts went with the database judge's appeal path.
 */
enum DatabaseGate { PLANS, RAISES, UNPLANNED }
