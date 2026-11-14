// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.compiler;

import com.legend.model.DatabaseDefinition;

import java.util.Optional;

/**
 * THE store lookups every reader of store facts shares &mdash; the model
 * index answers them ({@code ModelBuilder}, the normalizer's view of the
 * model) and the compile context relays them ({@link ModelContext}). All
 * three are INCLUDE-CLOSURE aware: an including database resolves the
 * included database's tables and views, own first.
 */
public interface StoreLookups {

    /** The full store definition of {@code dbFqn}. */
    Optional<DatabaseDefinition> findDatabase(@com.legend.base.Nullable String dbFqn);

    /** The TABLE {@code name} ({@code T} or {@code SCHEMA.T}) reached from
     *  {@code dbFqn}, with its declared columns. */
    Optional<DatabaseDefinition.TableDefinition> findTableDefinition(String dbFqn, String name);

    /** The TABLE {@code table} of schema {@code schema} ({@code default}: a top-level table, then an explicit
     *  {@code Schema default}'s) reached from {@code dbFqn}: the two names apart, so a name holding a dot is itself
     *  ({@link #findTableDefinition(String, String)} splits {@code SCHEMA.T} at its first dot; stream S, 2026-11-14). */
    Optional<DatabaseDefinition.TableDefinition> findTableDefinition(String dbFqn, String schema, String table);

    /** The TABLE {@code name} declared in {@code dbFqn} ITSELF (its
     *  includes not consulted) — which database OWNS a table. */
    Optional<DatabaseDefinition.TableDefinition> findOwnTableDefinition(String dbFqn, String name);

    /** The VIEW {@code name} ({@code V} or {@code SCHEMA.V}) reached from {@code dbFqn}. */
    Optional<DatabaseDefinition.ViewDefinition> findView(String dbFqn, String name);
}
