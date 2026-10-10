// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.sql.dialect;

import com.legend.sql.ValueKind;

import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * A statement as a dialect renders it for execution with bound values: the SQL text with a {@code ?} placeholder for each
 * bound parameter, and the parameters in placeholder order ({@link SqlDialect#renderStatement}), each with how it is
 * bound (docs/EXECUTION_PLAN_BOUNDARY_2026_10_05.md §9, step 3).
 *
 * @param sql   the statement text; where a placeholder's type is its value's ({@link TypeHole}), the type is missing
 *              from the text at the hole's place, written there when the value is known
 * @param binds one per {@code ?}, in order
 */
public record RenderedStatement(String sql, List<Bind> binds) {

    public RenderedStatement {
        Objects.requireNonNull(sql, "sql");
        binds = List.copyOf(binds);
        int previous = -1;
        for (Bind b : binds) {
            if (b.binding() instanceof Binding.One one && one.hole() != null) {
                int at = one.hole().at();
                if (at <= previous || at > sql.length()) {
                    throw new IllegalArgumentException("type hole of '" + b.parameter() + "' at " + at + " is not after"
                            + " the previous one (" + previous + ") within the statement (" + sql.length() + ")");
                }
                previous = at;
            }
        }
    }

    /** What one placeholder binds: a declared parameter's value, as {@code binding} says. */
    public record Bind(String parameter, Binding binding) {
        public Bind {
            Objects.requireNonNull(parameter, "parameter");
            Objects.requireNonNull(binding, "binding");
        }
    }

    /** How a placeholder is bound. */
    public sealed interface Binding permits Binding.One, Binding.Array {

        /** One value, as the caller's checked value is; an absent one a null of {@code nullType} (a
         *  {@code java.sql.JDBCType}'s name). {@code hole}: set where the database must be told the value's type in the
         *  statement (H2 types a placeholder when it prepares it), the hole the type is written into. */
        record One(String nullType, @com.legend.base.Nullable TypeHole hole) implements Binding {
            public One {
                Objects.requireNonNull(nullType, "nullType");
            }
        }

        /** The parameter's whole list, as one array of {@code elementSqlType} (the target's form of a list). */
        record Array(String elementSqlType) implements Binding {
            public Array {
                Objects.requireNonNull(elementSqlType, "elementSqlType");
            }
        }
    }

    /**
     * Where a placeholder's type is its value's: the statement's text has no type at {@code at}, and the value's is
     * written there, spelled by {@code types} for the value's kind ({@code absent}'s for an absent value) -- the type the
     * database gives a literal of that value, so the placeholder answers as the literal does.
     */
    public record TypeHole(int at, Map<ValueKind, TypeSpelling> types, ValueKind absent) {
        public TypeHole {
            if (types.isEmpty()) {
                throw new IllegalArgumentException("a type hole with no spelling");
            }
            // in the kinds' declared order: the order a plan's JSON writes them in
            types = java.util.Collections.unmodifiableMap(new java.util.EnumMap<>(types));
            if (!types.containsKey(absent)) {
                throw new IllegalArgumentException("an absent value's kind " + absent + " has no spelling in " + types);
            }
        }
    }

    /** A type's spelling for one kind of value: its name, followed by the value's own digits as {@code digits} says.
     *  {@code fraction}: for a date-time, the digits of a second its value keeps, finer ones cut, as its literal's are
     *  (Postgres keeps six); null where the value keeps its own. */
    public record TypeSpelling(String name, Digits digits, @com.legend.base.Nullable Integer fraction) {
        public TypeSpelling {
            Objects.requireNonNull(name, "name");
            Objects.requireNonNull(digits, "digits");
            if (fraction != null && (fraction < 0 || fraction > 9)) {
                throw new IllegalArgumentException("a second's fraction of " + fraction + " digits");
            }
        }

        public TypeSpelling(String name, Digits digits) {
            this(name, digits, null);
        }
    }

    /** The value's own digits a type spelling takes: none, {@code (precision)}, or {@code (precision, scale)}. */
    public enum Digits {
        NONE, PRECISION, PRECISION_AND_SCALE
    }
}
