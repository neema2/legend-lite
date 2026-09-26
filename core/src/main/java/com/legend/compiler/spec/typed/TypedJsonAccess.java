// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.compiler.spec.typed;

import com.legend.compiler.element.type.ExprType;

import java.util.List;

/**
 * JSON-TREE NAVIGATION over a {@code meta::json} value (real json.pure:32-70
 * classes: JSONObject.keyValuePairs, JSONArray.values, JSONString/
 * JSONNumber/JSONBoolean.value, JSONKeyValue.key/value). The value's
 * REPRESENTATION is the database's JSON (the variant lane): an object
 * member IS its value (the key is spelled by the access), an array is the
 * list of its elements, a scalar element extracts to its primitive. The
 * lowerer switches on {@link Op}; the typer ({@code JsonChecker}) mints
 * these from the property reads and the {@code keyValuePairs->filter(kv|
 * $kv.key.value == key)} member idiom.
 */
public record TypedJsonAccess(TypedSpec source, Op op,
        @com.legend.base.Nullable TypedSpec key, ExprType info) implements TypedSpec {

    public enum Op {
        /** {@code keyValuePairs->filter(kv|$kv.key.value == key)} / getValue(key): one member. */
        MEMBER,
        /** {@code keyValuePairs} unfiltered: every member value. */
        MEMBERS,
        /** {@code JSONArray.values}: the array's elements. */
        ELEMENTS,
        /** {@code JSONString.value}. */
        TEXT,
        /** {@code JSONNumber.value}. */
        NUMBER,
        /** {@code JSONBoolean.value}. */
        BOOLEAN,
        /** {@code JSONKeyValue.value}: the member IS its value. */
        IDENTITY
    }

    /** A node the resolver treats as a JSON-lane WRAPPER: a tree read,
     * the result envelope, a {@code ^JSONArray(...)} instance, or a map
     * over JSON elements (the auto-map of a read over a many receiver). */
    public static boolean isJsonNode(TypedSpec n) {
        return n instanceof TypedJsonAccess || n instanceof TypedJsonResult
                || n instanceof TypedNewInstance ni
                        && com.legend.compiler.element.type.PlatformTypes
                                .isJsonElement(ni.info().type())
                || n instanceof TypedMap m
                        && com.legend.compiler.element.type.PlatformTypes
                                .isJsonElement(m.source().info().type());
    }

    @Override
    public List<TypedSpec> children() {
        return key == null ? List.of(source) : List.of(source, key);
    }

    @Override
    public TypedSpec withChildren(List<TypedSpec> kids) {
        TypedSpec.expectChildren(kids, key == null ? 1 : 2, "TypedJsonAccess");
        return new TypedJsonAccess(kids.get(0), op,
                key == null ? null : kids.get(1), info);
    }
    @Override
    public TypedSpec withInfo(ExprType info) {
        return new TypedJsonAccess(source, op, key, info);
    }
}
