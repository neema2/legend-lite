// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.model;

import com.legend.model.Function;
import com.legend.model.SignatureMangle;

import java.util.Objects;

/**
 * A function's IDENTITY: its engine signature id, qualified
 * ({@code meta::pure::functions::boolean::and_Boolean_1__Boolean_1__Boolean_1_}).
 * Upstream names every function element by exactly this id; it is generated
 * from the declaration ({@link SignatureMangle#mangle}) and compared whole —
 * never parsed back apart.
 */
public record FunctionId(String qualified) implements Comparable<FunctionId> {

    public FunctionId {
        Objects.requireNonNull(qualified, "qualified");
    }

    /** The id of {@code declaration}. */
    public static FunctionId of(Function declaration) {
        return new FunctionId(SignatureMangle.mangle(declaration));
    }

    /** The identities of {@code declarations}, in order — computed ONCE where a
     *  group is declared (the catalog's generated {@code AT_…} groups, or an
     *  explicit subset of constants), never per call: identity is a property of
     *  the declaration (execution plan step 2, 2026-09-26). */
    public static java.util.List<FunctionId> ofAll(Function... declarations) {
        java.util.List<FunctionId> out = new java.util.ArrayList<>(declarations.length);
        for (Function f : declarations) {
            out.add(of(f));
        }
        return java.util.List.copyOf(out);
    }

    /** Several identity groups as one list, for a registration that names
     *  more than one group. Registration-time only; a per-call membership
     *  test asks each group's own {@code contains}. */
    @SafeVarargs
    public static java.util.List<FunctionId> all(java.util.List<FunctionId>... groups) {
        if (groups.length == 1) {
            return groups[0];
        }
        java.util.List<FunctionId> out = new java.util.ArrayList<>();
        for (java.util.List<FunctionId> g : groups) {
            out.addAll(g);
        }
        return java.util.List.copyOf(out);
    }

    @Override
    public int compareTo(FunctionId other) {
        return qualified.compareTo(other.qualified);
    }

    @Override
    public String toString() {
        return qualified;
    }
}
