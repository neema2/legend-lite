// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.platform;

import com.legend.builtin.NativeFn;

import java.util.Objects;
import java.util.Set;

/**
 * WHAT EXECUTES ONE DECLARATION — exactly one of six kinds (platform
 * architecture untangle, step 2; the platform's own Pure added by the build
 * rebuild's Phase 3b, item 1b). A declaration's implementation is a fact about
 * the declaration, keyed by its {@link FunctionId}; it never moves the
 * declaration and never depends on how the function's name is spelled.
 */
public sealed interface Implementation {

    /** A language form: its own checker and tree node. A form may also carry
     * lowering registrations it delegates its generic shape to
     * ({@code collection::distinct} through a scalar rule), and families that
     * co-register the same function (recorded, so the co-ownership is visible). */
    record Form(CoreFn form, Set<Position> alsoLowered, Set<Class<? extends NativeFn.Member>> alsoFamilies)
            implements Implementation {
        public Form {
            Objects.requireNonNull(form, "form");
            alsoLowered = java.util.Collections.unmodifiableSet(new java.util.LinkedHashSet<>(alsoLowered));
            alsoFamilies = java.util.Collections.unmodifiableSet(new java.util.LinkedHashSet<>(alsoFamilies));
        }
    }

    /** The platform's own implementation; any upstream body is not used. One
     * function may lower in several POSITIONS (max: a scalar rule over a
     * collection, and a SQL aggregate inside a group) — one implementation. A
     * feature flag may select another scalar rule for it. */
    record Intrinsic(Set<Position> positions, Set<Feature> featureOverrides,
            Set<Class<? extends NativeFn.Member>> families) implements Implementation {
        public Intrinsic {
            positions = java.util.Collections.unmodifiableSet(new java.util.LinkedHashSet<>(positions));
            featureOverrides = java.util.Collections.unmodifiableSet(new java.util.LinkedHashSet<>(featureOverrides));
            families = java.util.Collections.unmodifiableSet(new java.util.LinkedHashSet<>(families));
            if (positions.isEmpty() && families.isEmpty()) {
                throw new IllegalArgumentException("an intrinsic registers at least one position or family");
            }
        }
    }

    /** Upstream's Pure body, compiled and inlined by the platform. */
    record Body() implements Implementation {
    }

    /** The platform's own Pure: the system metamodel's body implements this
     * id over the platform's own rows ({@code PlatformPure}). A program that
     * declares the id (upstream's declaration, loaded or generated into the
     * default world) keeps its declaration and takes that body; the
     * platform's own declaration serves where no program declares the id.
     * Compiled and inlined like a body. */
    record PlatformPure() implements Implementation {
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

    /** Whether {@code row} runs the declaration by the platform's own rule or
     *  form — never by the declaration's body. Null (undeclared) is not. */
    static boolean byRule(@com.legend.base.Nullable Implementation row) {
        return row instanceof Intrinsic || row instanceof Form;
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
         *  meaning), or a capability it does not model (reflection) */
        CANNOT_IMPLEMENT,
        /** a version of a function the platform declares (a catalog native at its name) or implements in its own
         *  Pure (Phase 3b, item 1b), with no row of its own: its upstream body is the spec, never the platform's
         *  implementation (build rebuild Phase 3); a call that reaches it fails, naming it, until it has one */
        NO_ROW
    }
}
