// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.arrowipc;

import java.util.List;
import java.util.Map;

/**
 * One field of an Arrow schema (Arrow's {@code Field}): its name, type, nullability, its children (a list's element,
 * a struct's fields, a map's entries, a union's members) and its custom metadata, in order. Pure data.
 */
public record ArrowField(String name, ArrowType type, boolean nullable, List<ArrowField> children,
        Map<String, String> metadata) {

    public ArrowField {
        children = List.copyOf(children);
        // in declaration order: the key-value vector is written in it, and the bytes are the schema's
        metadata = java.util.Collections.unmodifiableMap(new java.util.LinkedHashMap<>(metadata));
    }

    /** A field with no children and no metadata. */
    public ArrowField(String name, ArrowType type, boolean nullable) {
        this(name, type, nullable, List.of(), Map.of());
    }
}
