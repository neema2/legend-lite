// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.testexecutionplan;

import com.legend.base.Nullable;

import java.util.List;
import java.util.Objects;

/**
 * A KEYED CLASS'S EQUALITY KEYS, AS THE PLAN CARRIES THEM (docs/PLAN_AS_DATA_2026_11_07.md 2.2, the design's step 4,
 * 2026-11-08): the keys an instance pair's equality restricts to (X5: the engine's own relation for keyed classes),
 * read from the model when the assert is planned -- the compiler's {@code EqualityKeys}, copied into the test plan's
 * vocabulary so the run reads no model type.
 *
 * @param classFqn the keyed class
 * @param keys     its key properties, in the model's order
 */
public record KeyTree(String classFqn, List<Key> keys) {

    public KeyTree {
        Objects.requireNonNull(classFqn, "classFqn");
        keys = List.copyOf(keys);
    }

    /**
     * One key property: {@code many} a to-many key (the values compared as collections, in order); {@code nested} the
     * keys of the keyed class the property is declared as, where it is one; {@code objectOnly} the property is
     * declared as a model class other than Any (a value in it is an instance or empty); {@code declaredClass} the class
     * the property is declared as (Any included), null for a primitive slot.
     */
    public record Key(String name, boolean many, @Nullable KeyTree nested, boolean objectOnly,
            @Nullable String declaredClass) {
        public Key {
            Objects.requireNonNull(name, "name");
        }
    }
}
