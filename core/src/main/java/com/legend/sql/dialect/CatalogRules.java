// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.sql.dialect;

import java.util.Locale;
import java.util.Map;

/**
 * How a database's dialect reads ITS OWN catalog into what a Pure Database declares
 * ({@link SqlDialect#catalogType}), as DATA: one algorithm over per-database decisions. The one writer
 * that reads them is {@link CatalogModel}, which DataCube and Python call through the compiler's
 * boundary (planner.Wasm.tableModelOrError).
 *
 * <p>The algorithm, in order: a column's own type name that is an ALIAS ({@code aliases}, upper-cased:
 * DuckDB's JSON, an alias of VARCHAR); its canonical type when that is the database's exact decimal
 * ({@code decimal}) -- {@code DECIMAL(p,s)} from the catalog's precision and scale, or
 * {@code unsizedDecimal} when the catalog gives none (null: refused); the canonical type's decision
 * ({@code types}); else refused, with its recorded reason ({@code refused}) or as a type this dialect
 * does not read. Canonical type names are compared upper-cased.
 *
 * @param database       the database's name, for messages ({@code DuckDB}, {@code Postgres})
 * @param types          every canonical type the dialect declares, and how
 * @param aliases        type aliases that are not their canonical type's, by the column's own type name
 * @param refused        canonical types refused, each with its reason
 * @param decimal        the canonical name of the exact decimal type, declared from its precision and scale
 * @param unsizedDecimal how an exact decimal the catalog gives no precision and scale is declared; null:
 *                       refused
 */
public record CatalogRules(String database, Map<String, CatalogType> types, Map<String, CatalogType> aliases,
        Map<String, String> refused, String decimal, @com.legend.base.Nullable CatalogType unsizedDecimal) {

    public CatalogRules {
        types = java.util.Collections.unmodifiableMap(new java.util.LinkedHashMap<>(types));
        aliases = java.util.Collections.unmodifiableMap(new java.util.LinkedHashMap<>(aliases));
        refused = java.util.Collections.unmodifiableMap(new java.util.LinkedHashMap<>(refused));
    }

    /** The column's declaration, or a {@link DialectCapability} naming why there is none. */
    public CatalogType typeOf(CatalogModel.Column column) {
        CatalogType alias = aliases.get(column.dataType().trim().toUpperCase(Locale.ROOT));
        if (alias != null) {
            return alias;
        }
        String logical = column.logicalType() == null ? null : column.logicalType().toUpperCase(Locale.ROOT);
        if (logical != null && logical.equals(decimal)) {
            if (column.precision() != null && column.scale() != null) {
                return CatalogType.asStored("DECIMAL(" + column.precision() + "," + column.scale() + ")");
            }
            if (unsizedDecimal != null) {
                return unsizedDecimal;
            }
            throw new DialectCapability("a " + decimal + " column whose catalog gives no precision and scale ('"
                    + column.dataType() + "') cannot be declared");
        }
        CatalogType known = logical == null ? null : types.get(logical);
        if (known != null) {
            return known;
        }
        String why = logical == null ? null : refused.get(logical);
        throw new DialectCapability("a column of " + database + " type '" + column.dataType()
                + "' cannot be declared in a Pure Database ("
                + (why != null ? why : logical == null ? "its catalog names no canonical type" : "a type this dialect does not read")
                + ")");
    }
}
