// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.compiler.spec;

import com.legend.compiler.element.type.ExprType;
import com.legend.compiler.element.type.Type;
import com.legend.compiler.spec.typed.TypedCInteger;
import com.legend.compiler.spec.typed.TypedNewInstance;
import com.legend.compiler.spec.typed.TypedSpec;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;

/**
 * m3's PACKAGEABLE INSTANCE constants as typed values. The engine's own
 * programs reference {@code PureOne} / {@code ZeroMany} bare (a
 * {@code PackageableMultiplicity} instance under
 * {@code meta::pure::metamodel::multiplicity}, legend-pure m3.pure:1411);
 * the platform spells each as the instance literal m3 declares —
 * {@code ^Multiplicity(lowerBound = ^MultiplicityValue(value = n),
 * upperBound = ^MultiplicityValue(value = m))}, an unbounded upper bound
 * carrying no value. Equality against a node's multiplicity is then the
 * structural equality of two spelled instances.
 */
public final class PlatformConstants {

    private static final String PKG = "meta::pure::metamodel::multiplicity::";
    private static final String MULTIPLICITY = PKG + "Multiplicity";
    private static final String MULTIPLICITY_VALUE = PKG + "MultiplicityValue";

    /** name → (lower, upper; null = unbounded) — m3.pure:1411 onward. */
    /** A multiplicity constant's bounds ({@code upper} -1: unbounded): a value, where an array was (2026-11-09, no
     *  mutable static state). */
    private record Bounds(int lower, int upper) {
    }

    private static final Map<String, Bounds> BOUNDS = Map.of(
            "PureOne", new Bounds(1, 1),
            "PureZero", new Bounds(0, 0),
            "ZeroOne", new Bounds(0, 1),
            "ZeroMany", new Bounds(0, -1),
            "OneMany", new Bounds(1, -1));

    private PlatformConstants() {
    }

    /** The multiplicity constant {@code path} names (bare or under its m3
     * package), as a spelled instance; empty for any other path. */
    public static Optional<TypedSpec> multiplicity(String path) {
        String name = path.startsWith(PKG) ? path.substring(PKG.length()) : path;
        Bounds b = BOUNDS.get(name);
        if (b == null || name.contains("::")) {
            return Optional.empty();
        }
        Map<String, TypedSpec> props = new LinkedHashMap<>();
        props.put("lowerBound", value(b.lower()));
        props.put("upperBound", value(b.upper()));
        return Optional.of(new TypedNewInstance(MULTIPLICITY, props,
                ExprType.one(new Type.ClassType(MULTIPLICITY))));
    }

    private static TypedSpec value(int v) {
        Map<String, TypedSpec> props = new LinkedHashMap<>();
        if (v >= 0) {
            props.put("value", new TypedCInteger((long) v, ExprType.one(Type.Primitive.INTEGER)));
        }
        return new TypedNewInstance(MULTIPLICITY_VALUE, props,
                ExprType.one(new Type.ClassType(MULTIPLICITY_VALUE)));
    }
}
