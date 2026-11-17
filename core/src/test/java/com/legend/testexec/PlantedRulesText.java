// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.testexec;

/**
 * A PLANTED VIOLATOR for {@code JudgingRulesReachTest} (the GUARD stream, C.3's second review, 2026-11-17): the
 * compatibility rules' wire spelling inside a longer string, concatenated by {@code invokedynamic}, in a class that never
 * names the judging rules' type -- no pool entry equals the token, one holds it as a word. Never called; read as
 * bytecode only (the edge's test library).
 */
public final class PlantedRulesText {

    private PlantedRulesText() {
    }

    static String json(int v) {
        return "{\"rules\":\"compiledPureCompat\",\"v\":" + v + "}";
    }
}
