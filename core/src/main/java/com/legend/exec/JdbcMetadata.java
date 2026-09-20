// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.exec;

/**
 * The driver's ONE metadata read, and the JDBC boundary it names.
 *
 * <p>This lived in {@code Compiler}, whose own comment already called
 * it the place "java.sql stops ... like at every other boundary". It
 * moved here because of what that co-location COSTS: the JVM verifier
 * resolves a method's {@code catch} clause types when the class is
 * linked, so {@code catch (java.sql.SQLException)} made
 * {@code Compiler} unloadable without the {@code java.sql} module —
 * even for a caller that only ever plans.
 *
 * <p>That matters beyond tidiness. {@code Compiler.plan} needs no
 * database, and the planner packages ({@code parser}, {@code lexer},
 * {@code compiler}, {@code lowering}, {@code sql}, {@code model},
 * {@code resolver}, …) depend on {@code java.base} ALONE — jdeps
 * confirms it. The single thing standing between that and a planner
 * that runs anywhere a JVM subset does — a WASM target, a jlink image
 * with no java.sql — was this one catch clause.
 */
public final class JdbcMetadata {

    private JdbcMetadata() {
    }

    /** Product name, or product version when {@code product} is false. */
    public static String read(java.sql.Connection connection,
            boolean product) {
        try {
            return product
                    ? connection.getMetaData().getDatabaseProductName()
                    : connection.getMetaData().getDatabaseProductVersion();
        } catch (java.sql.SQLException e) {
            throw new com.legend.error.DataError(
                    String.valueOf(e.getMessage()), e);
        }
    }
}
