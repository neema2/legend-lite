// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.plan;

import com.legend.compiler.element.type.ExprType;
import com.legend.compiler.element.type.Multiplicity;
import com.legend.compiler.element.type.Type;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * A query's result columns AS THE COMPILER TYPED THEM, in legend-engine's own
 * {@code RelationType} JSON shape (upstream {@code pure/v1/compilation/lambdaRelationType},
 * docs/UPSTREAM_ENDPOINTS_DESIGN_2026_09_27.md). ONE renderer for every surface that
 * hands a type to a client: the {@code pure/v1} endpoints and the browser planner module,
 * so a client reads one shape whichever answered. In {@code plan} beside
 * {@link PreciseTypes}: plan facts on compiler types, no execution.
 *
 * <p>Plain maps in upstream's key order ({@code _type} first, then alphabetical);
 * each surface serialises them with its own writer. The TYPE NAMES are legend-lite's
 * typer's ({@code Integer}, {@code Decimal} with its precision and scale as
 * {@code typeVariableValues}, {@code DateTime}); legend-engine names relational columns
 * with precise primitives ({@code Varchar}, {@code Int}, {@code Numeric},
 * {@code Timestamp}) &mdash; the recorded vocabulary gap, the untangle's compiler work,
 * which clients read both sides of meanwhile.
 */
public final class UpstreamRelationType {

    private UpstreamRelationType() {
    }

    /** {@code {"_type":"relationType","columns":[...]}} for a query's root. */
    public static Map<String, Object> of(ExprType root) {
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("_type", "relationType");
        List<Map<String, Object>> columns = new ArrayList<>();
        for (Type.Column c : columns(root)) {
            Map<String, Object> col = new LinkedHashMap<>();
            col.put("genericType", genericType(c.type()));
            col.put("multiplicity", multiplicity(c.multiplicity()));
            col.put("name", c.name());
            columns.add(col);
        }
        out.put("columns", columns);
        return out;
    }

    /** The root's columns: a relation's schema, or the one-column {@code value}
     *  relation of a scalar or collection root (the wire's scalarRoot contract). */
    public static List<Type.Column> columns(ExprType root) {
        Type.RelationType schema = Type.schemaView(root.type());
        return schema != null
                ? schema.columns()
                : List.of(new Type.Column("value", root.type(), root.multiplicity()));
    }

    /**
     * A TDS column's type as execute's builder and a plan's tdsColumns write it: an enumeration
     * column is {@code String} there (measured, 4.145.0, 2026-09-30), though lambdaRelationType
     * names the enumeration.
     */
    public static String tdsTypePath(Type t) {
        return t instanceof Type.EnumType ? "String" : typePath(t);
    }

    /** A column type's name as a client reads it: its path, a decimal as {@code Decimal}. */
    public static String typePath(Type t) {
        return t instanceof Type.PrecisionDecimal ? "Decimal" : t.typeName();
    }

    /**
     * The SQL type a plan's TDS column spells for a type: legend-engine's
     * {@code pureTypeToDataTypeMap} (pureToRelational.pure:48-56) on the PLAIN type,
     * probed against 4.145.0's {@code generatePlan} 2026-09-27 (a precise
     * {@code Numeric} spells {@code FLOAT}, {@code TinyInt} {@code INTEGER},
     * {@code Varchar} {@code VARCHAR(1024)} -- their plain parents'), and a Variant's 2026-10-28 (PARK-34). Any
     * other type is refused, as the engine refuses it.
     */
    public static String relationalSpelling(Type t) {
        if (t instanceof Type.EnumType) {
            // an enumeration column is its store column's string (measured, 4.145.0, 2026-09-30:
            // VARCHAR(n) of the physical column); lite has no physical provenance, so it spells
            // the plain String's VARCHAR(1024), as it does every computed string (recorded)
            return "VARCHAR(1024)";
        }
        return switch (typePath(t)) {
            case "Integer" -> "INTEGER";
            case "Float", "Decimal", "Number" -> "FLOAT";
            case "String" -> "VARCHAR(1024)";
            case "Boolean" -> "BIT";
            case "StrictDate" -> "DATE";
            case "DateTime", "Date" -> "TIMESTAMP";
            // a Variant column, a SEMISTRUCTURED store column's and a computed toVariant()'s alike (measured,
            // 4.145.0, 2026-10-28: PARK-34's probe, its execute on DuckDB and its plan on DuckDB and H2)
            case com.legend.compiler.element.type.PlatformTypes.VARIANT -> "SEMISTRUCTURED";
            // the engine spells no other (measured, 4.145.0, 2026-10-28: it refuses a StrictTime, a list and a
            // VARBINARY column before it executes), so neither does lite
            default -> throw new com.legend.error.NotImplementedException(
                    "plan: legend-engine has no relational spelling of " + typePath(t)
                            + " (it refuses such a column before it executes)");
        };
    }

    static Map<String, Object> genericType(Type t) {
        Map<String, Object> raw = new LinkedHashMap<>();
        raw.put("_type", "packageableType");
        raw.put("fullPath", typePath(t));
        List<Map<String, Object>> typeVariableValues = new ArrayList<>();
        if (t instanceof Type.PrecisionDecimal d) {
            typeVariableValues.add(integer(d.precision()));
            typeVariableValues.add(integer(d.scale()));
        }
        Map<String, Object> g = new LinkedHashMap<>();
        g.put("multiplicityArguments", List.of());
        g.put("rawType", raw);
        g.put("typeArguments", List.of());
        g.put("typeVariableValues", typeVariableValues);
        return g;
    }

    private static Map<String, Object> integer(int v) {
        Map<String, Object> i = new LinkedHashMap<>();
        i.put("_type", "integer");
        i.put("value", v);
        return i;
    }

    private static Map<String, Object> multiplicity(Multiplicity m) {
        Multiplicity.Bounded b = m.requireBounded("a result column");
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("lowerBound", b.lower());
        if (b.upper() != null) {
            out.put("upperBound", b.upper());
        }
        return out;
    }
}
