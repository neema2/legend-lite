// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend;

/**
 * A PLANTED BORROWER for {@code CompatConstantPoolTest} (the GUARD stream, C.6's review, 2026-11-17): it holds two
 * compatibility spellings no literal of this file writes -- one borrowed from {@link PlantedPoolLender}'s constant
 * (javac inlines it into this class's pool), one folded from two literals -- and names a type whose simple name is a
 * listed symbol (in its pool as a name and a descriptor, never as a string). Never called; read as bytecode and source.
 */
final class PlantedPoolBorrower {

    private PlantedPoolBorrower() {
    }

    static final String BORROWED = PlantedPoolLender.SESSION;

    static final String FOLDED = "TDS" + "Null";

    static Repr repr() {
        return null;
    }

    static final class Repr {
    }
}
