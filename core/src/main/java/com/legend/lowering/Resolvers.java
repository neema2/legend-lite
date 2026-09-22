// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.lowering;

import com.legend.sql.SqlExpr;

import java.util.Objects;
import java.util.function.Supplier;

/**
 * The Lowerer's resolve-or-fold PROTOCOL, extracted whole (file-size
 * seam): a {@link ColumnResolver} maps (lambda variable, property) to a
 * SQL expression; an unresolvable reference throws the
 * {@link UnfoldableRef} SIGNAL; try boundaries convert it to a
 * {@link Resolution} via {@link Resolution#attempt} or retry after
 * isolation ({@code Lowerer.foldOrIsolate}) — never caught anywhere else.
 */
final class Resolvers {

    private Resolvers() {
    }

    /**
     * Resolves (lambda variable, property) to a SQL expression in scope.
     * {@code propOrNull == null} means a bare {@code $var} reference;
     * {@code var == null} means a bare-name read. Returning null =
     * "not this scope's read"; may throw {@link UnfoldableRef}.
     */
    @FunctionalInterface
    interface ColumnResolver {
        @com.legend.base.Nullable SqlExpr resolve(@com.legend.base.Nullable String var,
                @com.legend.base.Nullable String propOrNull);
    }

    /** The resolve-or-fold outcome at a try boundary. */
    sealed interface Resolution {
        record Resolved(SqlExpr expr) implements Resolution { }

        record Unfoldable(String column) implements Resolution { }

        /** The EXPRESSION-LEVEL {@link UnfoldableRef} catch site: run
         * the attempt, name the outcome. */
        static Resolution attempt(Supplier<SqlExpr> attemptFn) {
            try {
                return new Resolved(attemptFn.get());
            } catch (UnfoldableRef e) {
                return new Unfoldable(Objects.requireNonNull(
                        e.getMessage(), "UnfoldableRef column"));
            }
        }
    }

    /**
     * The resolve-or-fold SIGNAL (not an error): thrown per unresolvable
     * reference. Stack traces are suppressed — this is control flow in a
     * hot path, pending the sealed-Resolution redesign
     * (docs/DESIGN_DEBT.md).
     */
    static final class UnfoldableRef extends RuntimeException {
        UnfoldableRef(String column) {
            super(column);
        }

        @Override
        public synchronized Throwable fillInStackTrace() {
            return this;
        }
    }

}
