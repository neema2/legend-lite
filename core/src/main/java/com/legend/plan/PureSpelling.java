// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.plan;

/**
 * Pure's own spelling of a type and a multiplicity ({@code Integer}, {@code [0..1]}): the language's names, not
 * upstream's text (Phase 6, 2026-11-17: moved off PlanText and PurePrint, legend-engine's printers, which go to the
 * edge; the planner's query parameters and the edge's plan lines both spell a type this way).
 */
public final class PureSpelling {

    private PureSpelling() {
    }

    /** A type's Pure name ({@code String}, {@code Integer}, ...). */
    public static String typeName(
            com.legend.compiler.element.type.Type t) {
        if (t == com.legend.compiler.element.type.Type.Primitive.STRING) {
            return "String";
        }
        if (t == com.legend.compiler.element.type.Type.Primitive.INTEGER) {
            return "Integer";
        }
        if (t == com.legend.compiler.element.type.Type.Primitive.FLOAT) {
            return "Float";
        }
        if (t == com.legend.compiler.element.type.Type.Primitive.BOOLEAN) {
            return "Boolean";
        }
        if (t == com.legend.compiler.element.type.Type.Primitive.STRICT_DATE) {
            return "StrictDate";
        }
        if (t == com.legend.compiler.element.type.Type.Primitive.DATE_TIME) {
            return "DateTime";
        }
        if (t == com.legend.compiler.element.type.Type.Primitive.DATE) {
            return "Date";
        }
        if (t == com.legend.compiler.element.type.Type.Primitive.DECIMAL) {
            return "Decimal";
        }
        if (t == com.legend.compiler.element.type.Type.Primitive.NUMBER) {
            return "Number";
        }
        // enum-typed columns/parameters spell the enumeration FQN
        if (t instanceof com.legend.compiler.element.type.Type.EnumType et) {
            return et.fqn();
        }
        throw new com.legend.error.NotImplementedException("plan: pure type name for " + t);
    }

    /** The plan's multiplicity spelling: {@code 1}, {@code 0..1},
     * {@code *}, {@code n..*} (inside the brackets). */
    public static String sizeRange(
            com.legend.compiler.element.type.Multiplicity m) {
        if (m instanceof com.legend.compiler.element.type.Multiplicity
                .Bounded b) {
            if (b.upper() != null) {
                return b.lower() == b.upper() ? String.valueOf(b.lower())
                        : b.lower() + ".." + b.upper();
            }
            // unbounded: [*] (lower 0) or [n..*]
            return b.lower() == 0 ? "*" : b.lower() + "..*";
        }
        throw new com.legend.error.NotImplementedException(
                "plan: multiplicity spelling for " + m + " pending");
    }

    /** The root's columns: a relation's schema, or the one-column {@code value}
     *  relation of a scalar or collection root (the wire's scalarRoot contract). */
    public static java.util.List<com.legend.compiler.element.type.Type.Column> relationColumns(
            com.legend.compiler.element.type.ExprType root) {
        com.legend.compiler.element.type.Type.RelationType schema =
                com.legend.compiler.element.type.Type.schemaView(root.type());
        return schema != null
                ? schema.columns()
                : java.util.List.of(new com.legend.compiler.element.type.Type.Column("value", root.type(),
                        root.multiplicity()));
    }

    /** A column type's name as a client reads it: its path, a decimal as {@code Decimal}. */
    public static String typePath(com.legend.compiler.element.type.Type t) {
        return t instanceof com.legend.compiler.element.type.Type.PrecisionDecimal ? "Decimal" : t.typeName();
    }
}
