// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.compiler.spec.typed;

import com.legend.compiler.element.type.ExprType;
import com.legend.sql.SqlQuery;

import java.util.List;
import java.util.Map;

/**
 * A RELATION READ OVER OTHER ROWS FOR ITS TABLES (B2d.2 of the total-wall program, 2026-11-11; the user's ruling of the
 * same day, "Read only reload is fine"): {@code source}, a relation, with each table named in {@code tables} read from
 * the planned statement beside it instead of the database's table -- the test-data generator's slice reloaded, read
 * only: upstream's {@code loadAndTestExecution} drops and re-creates every table of the database from the generated
 * text and runs the query again; here the query is planned again with each of those tables swapped for the slice's
 * rows (no row for a table the slice does not hold), and nothing is written. The lowering swaps each physical table
 * the source reads by its typed reference whose {@code store|physical name} is one of {@code reads} (a table of the
 * reloaded database or of one it includes, whatever database names it: what upstream's reload replaces), by its
 * physical name (the audit's M1, 2026-11-11).
 * The {@link TypedFrameRef} precedent: a typed node carrying statements planned already, never a Pure node in the IR.
 *
 * @param source the relation read
 * @param reads  the table references the reload replaces, each {@code store|physical name}
 * @param tables each swapped table's rows by its physical name ({@code TABLE} in the default schema, else
 *               {@code SCHEMA.TABLE}): a self-contained statement of the table's columns, named as the table names
 *               them
 * @param info   the source's type
 */
public record TypedTableSwap(TypedSpec source, java.util.Set<String> reads, Map<String, SqlQuery> tables, ExprType info)
        implements TypedSpec {

    public TypedTableSwap {
        reads = java.util.Collections.unmodifiableSet(new java.util.LinkedHashSet<>(reads));
        tables = java.util.Collections.unmodifiableMap(new java.util.LinkedHashMap<>(tables));
    }

    @Override
    public List<TypedSpec> children() {
        return List.of(source);
    }

    @Override
    public TypedSpec withChildren(List<TypedSpec> kids) {
        TypedSpec.expectChildren(kids, 1, "TypedTableSwap");
        return new TypedTableSwap(kids.get(0), reads, tables, info);
    }

    @Override
    public TypedSpec withInfo(ExprType info) {
        return new TypedTableSwap(source, reads, tables, info);
    }
}
