// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.testexecutionplan;

import com.legend.base.Nullable;

import java.util.Objects;

/**
 * A STATEMENT-ROOT ASSERT'S FACTS, AS THE PLAN CARRIES THEM (docs/PLAN_AS_DATA_2026_11_07.md 2.1, the design's step 4,
 * 2026-11-08): what the run needs to count and report the assert, beside its judgment -- the listener's name for it,
 * the family the census counts it under (null where its classification raised before naming one), whether it holds on
 * equality, and the failure its statement raised BEFORE its arm (a let's handle registration, its frame environment:
 * raised at its turn with no census and no listener), else null.
 */
public record Assertion(String listenerName, @Nullable String family, boolean wantEqual,
        @Nullable Failure beforeTheArm) {

    public Assertion {
        Objects.requireNonNull(listenerName, "listenerName");
    }

    /** The name the assert's sides go by: its family, or its listener name where none was classified. */
    public String name() {
        return family != null ? family : listenerName;
    }
}
