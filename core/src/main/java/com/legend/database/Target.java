// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.database;

import com.legend.model.ConnectionDefinition;
import com.legend.model.ConnectionDefinition.DatabaseType;

import java.util.List;

/**
 * WHERE a query executes, as the compiler decides it from the compiled model ({@code Compiler.executesOn}) — the
 * decision upstream makes in plan generation ({@code connectionByElement}) before its executor opens a connection
 * (docs/PLAN_EXECUTION_SPLIT_AND_DATABASE_OWNER_2026_10_03.md C3b). The execution side opens what this names
 * ({@code com.legend.exec.Sessions}); a caller that hands its own session has it checked against {@link #type()}.
 */
public sealed interface Target {

    /** The database the query's SQL is written for and its session must be. */
    DatabaseType type();

    /** The database the runtime's connections declare, with those connection definitions (distinct, sorted by name):
     *  every relational connection the runtime binds declares the same {@code type}. */
    record Declared(DatabaseType type, List<ConnectionDefinition> connections) implements Target {
        public Declared {
            connections = List.copyOf(connections);
            if (connections.isEmpty()) {
                throw new IllegalArgumentException("a declared target names at least one connection");
            }
        }
    }

    /** No database at all — the runtime binds only model data: the platform's own engine ({@link Databases#PLATFORM},
     *  SEMANTICS_REGISTER S27). */
    record Platform() implements Target {
        @Override
        public DatabaseType type() {
            return Databases.PLATFORM;
        }
    }
}
