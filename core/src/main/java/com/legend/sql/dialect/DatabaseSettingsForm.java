// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.sql.dialect;

/**
 * THE SETTINGS AN H2 DATABASE HAS, a two-form kind (the audit's J22, RULED by the user 2026-11-17, "Yes to the rule";
 * the orchestrator's approved design): the product's H2's own defaults ({@link ProductDatabaseSettings}), legend-engine's its
 * {@code MODE=LEGACY} and {@code DEFAULT_NULL_ORDERING=HIGH} (the edge's). Types only.
 */
public interface DatabaseSettingsForm {

    /** The settings the database has, by name and value; its dialect writes the statement that reads them back. */
    java.util.List<SqlDialect.Setting> required();
}
