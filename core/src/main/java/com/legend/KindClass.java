// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend;

import com.legend.compiler.element.EqualityKeys;
import com.legend.compiler.element.type.Type;

import java.util.List;
import java.util.Locale;

/** Pure's EQUALITY KIND CLASSES over stamps (canon spec §3: the numeric
 * tower is ONE class; everything else compares within its kind) — a CLOSED
 * type (audit §4y: the stringly keys {@code "numeric"} / {@code "enum:fqn"}
 * / {@code "instance:fqn"} deleted). {@link #toString()} spells the census
 * vocabulary the registers' witnesses cite. */
sealed interface KindClass {

    /** The kinds whose static inequality IS the engine's answer (X4: the
     * engine has no cross-kind PRIMITIVE equality); an instance class is a
     * declaration over values whose equality the instance canon decides. */
    boolean primitive();

    enum Primitive implements KindClass {
        NUMERIC("numeric"), STRING("string"), BOOLEAN("boolean"), TEMPORAL("temporal"),
        /** a type written as a VALUE ({@code String}, {@code [Car, Bicycle]}) */
        TYPE("type");

        private final String key;

        Primitive(String key) {
            this.key = key;
        }

        @Override
        public boolean primitive() {
            return true;
        }

        @Override
        public String toString() {
            return key;
        }
    }

    /** Per-ENUMERATION: values of different enums are never equal in pure,
     * and an enum never equals its name string — the fqn IS the kind. */
    record Enum(String fqn) implements KindClass {
        @Override
        public boolean primitive() {
            return true;
        }

        @Override
        public String toString() {
            return "enum:" + fqn;
        }
    }

    /** X5: instance equality is per-CLASS (EqualityUtilities — the
     * classifiers must match exactly, so the fqn IS the kind; a
     * parameterized GenericType names the same classifier); keyed-ness
     * adjudicates at the wrap (a keyless class declines with its own
     * reason). */
    record Instance(String fqn) implements KindClass {
        @Override
        public boolean primitive() {
            return false;
        }

        @Override
        public String toString() {
            return "instance:" + fqn;
        }
    }

    /** A tracked metamodel element class: the name-valued kind. */
    record Element(String fqn) implements KindClass {
        @Override
        public boolean primitive() {
            return true;
        }

        @Override
        public String toString() {
            return "element:" + fqn;
        }
    }

    /** The kind class of a stamp; null for a shape with no equality kind. */
    static @com.legend.base.Nullable KindClass of(Type t) {
        if (t == Type.Primitive.INTEGER || t == Type.Primitive.FLOAT
                || t == Type.Primitive.DECIMAL
                // NUMBER is the numeric tower's supertype — the concrete
                // render refines from the PLAN's SQL type (V6 burn)
                || t == Type.Primitive.NUMBER || t instanceof Type.PrecisionDecimal) {
            return Primitive.NUMERIC;
        }
        if (t instanceof Type.EnumType et) {
            return new Enum(et.fqn());
        }
        if (t == Type.Primitive.STRING) {
            return Primitive.STRING;
        }
        if (t == Type.Primitive.BOOLEAN) {
            return Primitive.BOOLEAN;
        }
        if (t == Type.Primitive.STRICT_DATE || t == Type.Primitive.DATE_TIME
                || t == Type.Primitive.DATE) {
            return Primitive.TEMPORAL;
        }
        String fqn = EqualityKeys.fqnOf(t);
        return fqn != null ? new Instance(fqn) : null;
    }

    /** The numeric tower's FINE kinds (pure's own Number dispatch): a
     * refined stamp names one; an unrefined Number resolves from the
     * runtime value kinds — selection, never evaluation. */
    enum Fine {
        INTEGER, FLOAT, DECIMAL;

        @Override
        public String toString() {
            return name().toLowerCase(Locale.ROOT);
        }

        /** The fine kind a refined stamp names; null for an unrefined
         * Number — decline, never guess. */
        static @com.legend.base.Nullable Fine ofType(Type t) {
            if (t == Type.Primitive.INTEGER) {
                return INTEGER;
            }
            if (t == Type.Primitive.FLOAT) {
                return FLOAT;
            }
            if (t == Type.Primitive.DECIMAL || t instanceof Type.PrecisionDecimal) {
                return DECIMAL;
            }
            return null;
        }

        /** The RUNTIME numeric kind of a side's fetched values (uniform, or
         * null when empty / unknowable — the mixed case gated earlier). */
        static @com.legend.base.Nullable Fine ofValues(List<Object> vals) {
            Fine kind = null;
            for (Object v : vals) {
                Fine k = v instanceof java.math.BigDecimal ? DECIMAL
                        : (v instanceof Double || v instanceof Float) ? FLOAT
                        : integral(v) ? INTEGER : null;
                if (k == null) {
                    return null;
                }
                kind = k;
            }
            return kind;
        }

        /** Integral and floating values side by side (unsound under SQL
         * column promotion: pure refuses 1 == 1.0 element-wise). */
        static boolean mixed(List<Object> vals) {
            boolean integral = false;
            boolean floating = false;
            for (Object v : vals) {
                if (integral(v)) {
                    integral = true;
                } else if (v instanceof Double || v instanceof Float) {
                    floating = true;
                }
            }
            return integral && floating;
        }

        private static boolean integral(Object v) {
            return v instanceof Long || v instanceof Integer || v instanceof Short
                    || v instanceof Byte || v instanceof java.math.BigInteger;
        }
    }
}
