// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.sql.dialect;

/**
 * THE SETTINGS AN H2 DATABASE HAS, a two-form kind (the audit's J22, RULED by the user 2026-11-17, "Yes to the rule";
 * the orchestrator's approved design): the product's H2's own defaults ({@link ProductH2Settings}), legend-engine's its
 * {@code MODE=LEGACY} and {@code DEFAULT_NULL_ORDERING=HIGH} (the edge's). Types only.
 */
public interface H2Settings {

    /** The settings, as the plan's target carries them (exec writes them on the URL of an in-memory database it opens
     *  and reads them back from a session it did not). */
    SqlDialect.DatabaseSettings settings();
}
