// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.testexecutionplan;



/** Pure's EQUALITY KIND CLASSES over stamps (canon spec §3: the numeric
 * tower is ONE class; everything else compares within its kind) — a CLOSED
 * type (audit §4y: the stringly keys {@code "numeric"} / {@code "enum:fqn"}
 * / {@code "instance:fqn"} deleted). {@link #toString()} spells the census
 * vocabulary the registers' witnesses cite. A fact of a side, decided at planning from its type and carried by the
 * test plan (docs/PLAN_AS_DATA_2026_11_07.md 2.2, the design's step 4, 2026-11-08). */
public sealed interface KindClass {

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

}
