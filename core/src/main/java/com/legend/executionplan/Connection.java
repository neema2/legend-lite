// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.executionplan;

import java.util.Objects;

/**
 * A CONNECTION AS THE PLAN DESCRIBES IT (docs/PLAN_AS_DATA_2026_11_07.md 2.6, the user's ruling of 2026-11-08: the plan
 * stops holding the model's {@code ConnectionDefinition} and holds its own, FAITHFUL): the definition's own fields --
 * its qualified name (every refusal names it), its store, the database it declares, its specification (each kept as its
 * own variant: {@code LocalH2} is not {@code InMemory}) and its authentication (each kept distinct: references, never a
 * secret -- a vault key or a {@code kind:key} reference read at open). Exec's connector renders the URL at open. The
 * planner copies it from the model's definition ({@code planner/ConnectionPlans}).
 */
public record Connection(String qualifiedName, @com.legend.base.Nullable String storeName, DatabaseType databaseType,
        ConnectionSpecification specification, AuthenticationSpec authentication) {

    public Connection {
        Objects.requireNonNull(qualifiedName, "Qualified name cannot be null");
        Objects.requireNonNull(databaseType, "Database type cannot be null");
        Objects.requireNonNull(specification, "Specification cannot be null");
        Objects.requireNonNull(authentication, "Authentication cannot be null");
    }
}
