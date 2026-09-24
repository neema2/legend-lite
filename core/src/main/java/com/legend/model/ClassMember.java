// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.model;

import java.util.Objects;

/**
 * A class member by its declaration: the owning class's FQN and the member's
 * name. The identity of a derived property or constraint the platform
 * implements — matched against a lifted function's {@link FunctionDefinition#synthesizedFrom()
 * provenance}, never against the lifted name's spelling.
 */
public record ClassMember(String owner, String name) {
    public ClassMember {
        Objects.requireNonNull(owner, "owner");
        Objects.requireNonNull(name, "name");
    }
}
