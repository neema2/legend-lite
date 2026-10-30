// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0
package com.legend.executionplan;

import com.legend.sql.OutputCol;
import com.legend.sql.SqlDdl;
import com.legend.sql.SqlExpr;
import com.legend.sql.SqlQuery;
import com.legend.sql.SqlSelect;
import com.legend.sql.SqlSource;
import com.legend.sql.SqlType;
import com.legend.sql.SqlUnion;

import com.legend.sql.SqlWith;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * The three type facts of a result column (docs/PLAN_PATH_PROGRAM_2026_10_17.md §3), read from the lowered query:
 * the Pure type and multiplicity the typer gave the column ({@link Pure}, handed in), the SQL type of its output slot
 * ({@code slot}: the lowering's typed fact, what the database answers with), and its store's declared type by
 * PROVENANCE ({@code store}: a projection that reads a column of a base table, through subselects, joins and a
 * pivot's keys, is that column's declared type as the relational grammar spells it; anything computed has none and
 * says so). Nothing is guessed: a slot is always known for a lowered output, and a store type is found or absent.
 */
public final class ColumnFacts {
    private ColumnFacts() {
    }

    /** What the typer says of a column: its name, its Pure type's name, its multiplicity. */
    public record Pure(String name, String type, ExecutionPlan.Multiplicity multiplicity) {
    }

    /**
     * The plan's columns for {@code pure}, the typed schema, over {@code lowered}'s outputs, in a result of
     * {@code shape} (a relation's cells, a value root's elements: the written-precision fact differs,
     * {@link #precision}). A column is matched to its output by name. A column no output is named after is one of two
     * things, each said in the column: a pivot's generated column (its slot the template's type, when the template
     * carries one); or, when no column at all matches by name and the counts agree, a value root's one output, which
     * the wire names {@code value} (matched by position). Any other difference is a planner defect, refused by name
     * (a row-struct column reaches here flattened, {@code TypedRows.flat}: until 2026-10-29 one unflattened passed
     * with no slot, and the plan declared one column where the statement answered its parts).
     */
    public static List<ExecutionPlan.Column> columns(List<Pure> pure, SqlQuery lowered,
            ExecutionPlan.TdsResult.Shape shape) {
        List<OutputCol> outs = lowered.outputs();
        boolean anyByName = pure.stream().anyMatch(p -> outs.stream().anyMatch(o -> o.name().equals(p.name())));
        boolean positional = !anyByName && pure.size() == outs.size();
        List<SqlSource.Pivot.Using> templates = pivotTemplates(lowered);
        List<ExecutionPlan.Column> cols = new ArrayList<>(pure.size());
        for (int i = 0; i < pure.size(); i++) {
            Pure p = pure.get(i);
            OutputCol out = positional ? outs.get(i)
                    : outs.stream().filter(o -> o.name().equals(p.name())).findFirst().orElse(null);
            if (out != null) {
                SqlDdl.ColumnType stored = stored(lowered, out.name(), Map.of());
                cols.add(new ExecutionPlan.Column(p.name(), p.type(), p.multiplicity(), slot(out.type()),
                        stored == null ? null : store(stored), precision(p.type(), out.type(), shape)));
                continue;
            }
            String bare = p.name().length() > 1 && p.name().startsWith("'") && p.name().endsWith("'")
                    ? p.name().substring(1, p.name().length() - 1) : p.name();
            List<SqlSource.Pivot.Using> hits = templates.stream().filter(t -> bare.endsWith(t.alias())).toList();
            if (hits.size() == 1) {
                SqlType t = hits.get(0).type();
                cols.add(new ExecutionPlan.Column(p.name(), p.type(), p.multiplicity(), t == null ? null : slot(t),
                        null, precision(p.type(), t, shape)));
            } else {
                // (until 2026-10-29 a column whose name prefixed some outputs -- an unflattened row-struct column --
                // passed here with no slot, and the plan declared one column where the statement answered its parts;
                // the planner flattens such a column before it reaches here, TypedRows.flat, and nothing else may)
                throw new IllegalStateException("the plan's column '" + p.name() + "' is none of the query's outputs "
                        + outs.stream().map(OutputCol::name).toList() + " and no pivot template's: the lowering's"
                        + " typed slots do not cover the typed schema");
            }
        }
        return cols;
    }

    /** The pivots' templates in {@code q}, the usings a generated column's name ends with. */
    private static List<SqlSource.Pivot.Using> pivotTemplates(SqlQuery q) {
        List<SqlSource.Pivot.Using> out = new ArrayList<>();
        switch (q) {
            case SqlSelect s -> pivotTemplates(s.from(), out);
            case SqlUnion u -> u.branches().forEach(b -> out.addAll(pivotTemplates(b)));
            case SqlWith w -> {
                w.ctes().forEach(c -> out.addAll(pivotTemplates(c.query())));
                out.addAll(pivotTemplates(w.body()));
            }
        }
        return out;
    }

    private static void pivotTemplates(SqlSource src, List<SqlSource.Pivot.Using> out) {
        switch (src) {
            case SqlSource.Pivot p -> {
                out.addAll(p.usings());
                pivotTemplates(p.source(), out);
            }
            case SqlSource.Subselect sub -> out.addAll(pivotTemplates(sub.inner()));
            case SqlSource.Join j -> {
                pivotTemplates(j.left(), out);
                pivotTemplates(j.right(), out);
            }
            case SqlSource.Table t -> { }
            case SqlSource.Cte c -> { }
            case SqlSource.Values v -> { }
            case SqlSource.Dual d -> { }
            case SqlSource.SourceUrl u -> { }
            case SqlSource.RawSql r -> { }
            case SqlSource.Catalog c -> { }
            case SqlSource.VarSetPlaceholder v -> { }
        }
    }

    /** A slot's SQL type spelled: a scalar's name, {@code DECIMAL(p,s)}, {@code ARRAY<...>}, {@code MAP<k,v>},
     *  {@code STRUCT<name type, ...>}. */
    public static String slot(SqlType t) {
        return switch (t) {
            case SqlType.Scalar s -> s.name();
            case SqlType.Decimal d -> "DECIMAL(" + d.precision() + "," + d.scale() + ")";
            case SqlType.Array a -> "ARRAY<" + slot(a.element()) + ">";
            case SqlType.Map m -> "MAP<" + slot(m.key()) + "," + slot(m.value()) + ">";
            case SqlType.Struct s -> {
                StringBuilder b = new StringBuilder("STRUCT<");
                for (int i = 0; i < s.fields().size(); i++) {
                    SqlType.Struct.Field f = s.fields().get(i);
                    b.append(i > 0 ? ", " : "").append(f.name()).append(' ').append(slot(f.type()));
                }
                yield b.append('>').toString();
            }
            // a tagged column's slots, each its Pure kind and its SQL type; the physical columns are named by
            // SqlType.Tagged.slotColumn, which the runner's decode reads
            // (docs/RESULTS_AND_RENDERERS_2026_10_22.md §5a)
            case SqlType.Tagged tg -> {
                StringBuilder b = new StringBuilder("TAGGED<");
                for (int i = 0; i < tg.slots().size(); i++) {
                    SqlType.Tagged.Slot s = tg.slots().get(i);
                    b.append(i > 0 ? ", " : "").append(s.kind()).append(' ').append(slot(s.type()));
                }
                yield b.append('>').toString();
            }
        };
    }

    /** How a temporal cell of a column of Pure type {@code type} in slot {@code slot} (null: no one slot), in a
     *  result of {@code shape}, has its written precision ({@link ExecutionPlan.Precision}; the start page's S1.4
     *  item 1, 2026-10-28, corrected 2026-10-29 under the upstream-fidelity tenet). A tagged column (a mixed value's,
     *  {@code TAGGED<kind slot, ...>}) has no one precision: its slots' kinds carry it ({@code DateTime(n)}), and the
     *  decoders present each cell at its own, so it is null here. A DATE slot or a StrictDate column holds a day:
     *  DAY. A time-bearing temporal otherwise -- a TIMESTAMP slot, a temporal-text carrier, a Date or
     *  DateTime column in any other slot (a text a database holds its dates as, no slot at all) -- is, in a
     *  relation, a cell legend-engine's TDS spells with nine digits whatever carried it (INSTANT); in a value root,
     *  legend-pure's value: the precision written where the cell carries it (a temporal-text carrier: WRITTEN), the
     *  precision its digits show where the database computed it (MINIMAL, the value lane's canon, register S118).
     *  Any other column has no temporal of its own: null, said. */
    public static ExecutionPlan.@com.legend.base.Nullable Precision precision(String type,
            @com.legend.base.Nullable SqlType slot, ExecutionPlan.TdsResult.Shape shape) {
        if (slot instanceof SqlType.Tagged) {
            // whatever its Pure type (Any, Date, DateTime: a mixed temporal selection's root): its slots' kinds carry
            // the precision (the L2 audit, 2026-10-29: a Date-typed tagged column answered MINIMAL here)
            return null;
        }
        if (type.equals("StrictDate") || slot == SqlType.Scalar.DATE) {
            return ExecutionPlan.Precision.DAY;
        }
        boolean value = shape == ExecutionPlan.TdsResult.Shape.VALUE;
        if (slot == SqlType.Scalar.TEMPORAL_TEXT) {
            return value ? ExecutionPlan.Precision.WRITTEN : ExecutionPlan.Precision.INSTANT;
        }
        if (slot == SqlType.Scalar.TIMESTAMP || slot == SqlType.Scalar.TIMESTAMPTZ || type.equals("Date")
                || type.equals("DateTime")) {
            return value ? ExecutionPlan.Precision.MINIMAL : ExecutionPlan.Precision.INSTANT;
        }
        return null;
    }

    /** A declared column type as the relational grammar spells it: {@code INTEGER}, {@code VARCHAR(20)},
     *  {@code DECIMAL(10,2)}. */
    public static String store(SqlDdl.ColumnType t) {
        return switch (t) {
            case SqlDdl.ColumnType.Plain p -> p.kind().name();
            case SqlDdl.ColumnType.Sized s -> s.kind() + "(" + s.size() + ")";
            case SqlDdl.ColumnType.Scaled s -> s.kind() + "(" + s.precision() + "," + s.scale() + ")";
        };
    }

    /** The declared store type of {@code q}'s output {@code name}, by provenance, {@code ctes} the WITH queries in
     *  scope by name; null when the output is computed or its source is not a base table. */
    public static SqlDdl.@com.legend.base.Nullable ColumnType stored(SqlQuery q, String name,
            Map<String, SqlQuery> ctes) {
        return switch (q) {
            case SqlSelect s -> storedInSelect(s, name, ctes);
            case SqlUnion u -> {
                // every branch delivers the same declared column, or the union's column is nobody's
                SqlDdl.ColumnType first = null;
                for (int i = 0; i < u.branches().size(); i++) {
                    SqlDdl.ColumnType t = stored(u.branches().get(i), name, ctes);
                    if (t == null || (first != null && !first.equals(t))) {
                        yield null;
                    }
                    first = t;
                }
                yield first;
            }
            case SqlWith w -> {
                Map<String, SqlQuery> scope = new HashMap<>(ctes);
                for (SqlWith.Cte c : w.ctes()) {
                    scope.put(c.name(), c.query());
                }
                yield stored(w.body(), name, scope);
            }
        };
    }

    private static SqlDdl.@com.legend.base.Nullable ColumnType storedInSelect(SqlSelect s, String name,
            Map<String, SqlQuery> ctes) {
        boolean star = false;
        for (SqlSelect.Projection p : s.projections()) {
            if (p.expr() instanceof SqlExpr.Star || p.expr() instanceof SqlExpr.StarExcept) {
                star = true;
            } else if (name.equals(p.outputName())) {
                return storedInExpr(p.expr(), s.from(), ctes);
            }
        }
        return star ? storedInSource(s.from(), null, name, ctes) : null;
    }

    /** A plain column read, or a stored read, is its column's declared type; any other expression is computed. */
    private static SqlDdl.@com.legend.base.Nullable ColumnType storedInExpr(SqlExpr e, SqlSource from,
            Map<String, SqlQuery> ctes) {
        if (e instanceof SqlExpr.StoredRead r) {
            return r.stored();
        }
        if (e instanceof SqlExpr.Column c) {
            return storedInSource(from, c.table(), c.name(), ctes);
        }
        return null;
    }

    /** The declared type of {@code col} as {@code src} delivers it: a base table's declaration, a subselect's or a
     *  CTE's own provenance, a join's side, a pivot's key column through its source; a VALUES, a scalar select's
     *  dual, an authored SQL text, a source URL and a placeholder declare nothing. */
    private static SqlDdl.@com.legend.base.Nullable ColumnType storedInSource(SqlSource src,
            @com.legend.base.Nullable String alias, String col, Map<String, SqlQuery> ctes) {
        return switch (src) {
            case SqlSource.Table t -> alias == null || alias.equals(t.alias()) || alias.equals(t.name())
                    ? t.storedTypes().get(col) : null;
            case SqlSource.Subselect sub -> alias == null || alias.equals(sub.alias())
                    ? stored(sub.inner(), col, ctes) : null;
            case SqlSource.Cte c -> {
                SqlQuery defined = ctes.get(c.name());
                yield defined != null && (alias == null || alias.equals(c.alias())) ? stored(defined, col, ctes)
                        : null;
            }
            case SqlSource.Join j -> {
                SqlDdl.ColumnType left = storedInSource(j.left(), alias, col, ctes);
                yield left != null ? left : storedInSource(j.right(), alias, col, ctes);
            }
            case SqlSource.Pivot p -> alias == null || alias.equals(p.alias())
                    ? storedInSource(p.source(), null, col, ctes) : null;
            case SqlSource.Values v -> null;
            case SqlSource.Dual d -> null;
            case SqlSource.SourceUrl u -> null;
            case SqlSource.RawSql r -> null;
            case SqlSource.Catalog c -> null;   // the database's own system tables: declared by nobody's store
            case SqlSource.VarSetPlaceholder v -> null;
        };
    }
}
