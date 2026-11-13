// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.exec;

/**
 * An interface PLANTED in exec's package whose methods take text, for one test alone (the third re-audit, 2026-11-12):
 * {@code ArchitectureTest.theNoTextCheckFindsAPlantedTextParameter} shows the check of
 * {@code ArchitectureTest.aSessionsStatementsTakeNoText} finds both. A test interface: the real check reads
 * {@code Statements}.
 */
public interface PlantedTextStatements {

    /** A statement as a String. */
    boolean execute(String sql);

    /** A statement as another text. */
    boolean executeText(CharSequence sql);

    /** The plan's statement: no text. */
    boolean executePlanned(com.legend.executionplan.PlannedSql statement);
}
