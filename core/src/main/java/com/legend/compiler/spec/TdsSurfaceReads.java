// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.compiler.spec;

import com.legend.compiler.element.type.ExprType;
import com.legend.compiler.element.type.Multiplicity;
import com.legend.compiler.element.type.Type;
import com.legend.compiler.spec.typed.TypedSpec;
import com.legend.protocol.spec.AppliedProperty;

/**
 * The TDS SURFACE over relation values (engine TabularDataSet) and over
 * ROW values (bare struct — Row-vs-Relation): {@code .rows}, {@code
 * .values}, {@code .columns}, {@code .columnNames}. Extracted from
 * Typer.accessProperty (file-size guardrail, batch 166). Null when the
 * property is none of them — the ordinary column read follows.
 */
final class TdsSurfaceReads {

    private TdsSurfaceReads() {
    }

    static @com.legend.base.Nullable TypedSpec read(Typer t, TypedSpec source,
            AppliedProperty ap, Type.RelationType rt2) {
        // TDS surface over relation values (engine TabularDataSet)
        // and over ROW values (bare struct — Row-vs-Relation):
        // .rows IS the relation viewed as its row collection; bare
        // .columns is the column-name collection (assertSize targets)
        if (ap.property().equals("rows") && Type.isRelation(source.info().type())) {
            // .rows IS the row collection — typed AS one (engine:
            // TDSRow[*]; Row-vs-Relation: bare struct, many stamp —
            // Type.relationValued reads it back). The node SURVIVES
            // as a MARKER: the statement executor's result frame must
            // tell `$r.values.rows->at(k)` (a REAL row index) from
            // `$r.values->at(k)` (the Result envelope, k=0 only) —
            // erasing here made the two spellings collide (audit
            // 19d B2). The K-side splice hook erases the marker after
            // disambiguation.
            return new com.legend.compiler.spec.typed.TypedPropertyAccess(
                    source, "rows", new ExprType(rt2,
                            Multiplicity.Bounded.ZERO_MANY));
        }
        TypedSpec lateBound = Typer.lateBoundGridMarker(source, ap, rt2);
        if (lateBound != null) {
            return lateBound;
        }
        // Row-vs-Relation: on a bare ROW a DECLARED column of that
        // name is the read (a union row carrying a navigate slot
        // named `columns` — the store's Relation.columns, batch 166);
        // the reflection surface below serves a TABLE always (engine
        // TabularDataSet.columns) and a row only when it declares no
        // such column (the lite bare-row extension, assertSize targets)
        // the NAME test first: the column scan runs only for the three
        // reflection names, never on every property read (batch 166 put the
        // scan before the name test — a per-read cost on every row access,
        // the batch-170 bisect's 7 s)
        String prop = ap.property();
        boolean reflectionName = prop.equals("values") || prop.equals("columns")
                || prop.equals("columnNames");
        boolean declaredColumn = reflectionName && !Type.isRelation(source.info().type())
                && rt2.columns().stream().anyMatch(c -> c.name().equals(prop));
        if (!declaredColumn && ap.property().equals("values")) {
            return t.tdsValuesRead(source, rt2);
        }
        if (!declaredColumn && ap.property().equals("columns")) {
            return ColumnsMetaFold.columnsMeta(rt2, false);
        }
        // the ResultSet surface's name collection over a DECLARED
        // schema (§4bZ-U leg 4): a fetchDb/executeInDb grid with
        // compile-time columns answers .columnNames statically —
        // the same literal collection the late-bound marker path
        // resolves at the boundary for probe-stamped grids
        if (!declaredColumn && ap.property().equals("columnNames")) {
            return ColumnsMetaFold.columnsMeta(rt2, false);
        }
        return null;
    }
}
