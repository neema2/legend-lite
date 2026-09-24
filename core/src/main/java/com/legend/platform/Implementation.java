// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.platform;

import com.legend.compiler.spec.CoreFn;

import java.util.List;
import java.util.Objects;
import java.util.Set;

/**
 * WHAT EXECUTES ONE DECLARATION — exactly one of five kinds (platform
 * architecture untangle, step 2). A declaration's implementation is a fact about
 * the declaration, keyed by its {@link FunctionId}; it never moves the
 * declaration and never depends on how the function's name is spelled.
 */
public sealed interface Implementation {

    /** A language form: its own checker and tree node. A form may also carry
     * lowering registrations it delegates its generic shape to
     * ({@code collection::distinct} through a scalar rule), and families that
     * co-register the same function (recorded, so the co-ownership is visible). */
    record Form(CoreFn form, Set<Position> alsoLowered, List<String> alsoFamilies)
            implements Implementation {
        public Form {
            Objects.requireNonNull(form, "form");
            alsoLowered = Set.copyOf(alsoLowered);
            alsoFamilies = List.copyOf(alsoFamilies);
        }
    }

    /** The platform's own implementation; any upstream body is not used. One
     * function may lower in several POSITIONS (max: a scalar rule over a
     * collection, and a SQL aggregate inside a group) — one implementation. */
    record Intrinsic(Set<Position> positions, List<String> families) implements Implementation {
        public Intrinsic {
            positions = Set.copyOf(positions);
            families = List.copyOf(families);
            if (positions.isEmpty() && families.isEmpty()) {
                throw new IllegalArgumentException("an intrinsic registers at least one position or family");
            }
        }
    }

    /** Upstream's Pure body, compiled and inlined by the platform. */
    record Body() implements Implementation {
    }

    /** Declared (an upstream native) with no implementation here. */
    record Unimplemented() implements Implementation {
    }

    /** A body or native the platform deliberately does not run. */
    record Refused(Reason reason, String why) implements Implementation {
        public Refused {
            Objects.requireNonNull(reason, "reason");
            Objects.requireNonNull(why, "why");
        }
    }

    /** Where a registered lowering applies. */
    enum Position {
        /** Scalars.RULES — a SQL expression. */
        SCALAR,
        /** Aggregates.REDUCERS — a SQL aggregate. */
        AGGREGATE,
        /** Windows.FNS — a window function. */
        WINDOW,
        /** Windows.AGGREGATES — a window-only aggregate. */
        WINDOW_AGGREGATE
    }

    /** Why a declaration is refused. */
    enum Reason {
        /** the engine's machinery for a concern the platform serves itself (its
         *  SQL printer, its planner): WalledBodies */
        ENGINE_MACHINERY,
        /** a program whose value is never needed here: Subsumed */
        MOOT,
        /** a native the platform cannot implement (an effect with no database
         *  meaning): Pure.WALLED_NATIVES */
        CANNOT_IMPLEMENT
    }
}
