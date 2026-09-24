// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.platform;

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

    @Override
    public int compareTo(FunctionId other) {
        return qualified.compareTo(other.qualified);
    }

    @Override
    public String toString() {
        return qualified;
    }
}
