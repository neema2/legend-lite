// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.testexec;

import static com.legend.testexecutionplan.Verdicts.constantValue;
import com.legend.testexecutionplan.Column;
import com.legend.testexecutionplan.TypedSide;
import com.legend.exec.PlanRunner;
import com.legend.testexecutionplan.Row;
import com.legend.executionplan.ExecutionPlan;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

final class SideShapes {

    private SideShapes() {
    }


    /** A side the body driver holds in Java (the doors, door 1, 2026-10-29) written to the caller's sink through a batcher, as a host's rows are: its
     *  columns as plan columns of the side's Pure types (no slot: the writer spells a cell by its carrier's own kind),
     *  its rows the side's values; a graph side one row per object of its array (the reader's own reading). */
    static void host(TypedSide side, Answer answer) {
        com.legend.executionplan.ExecutionPlan.Multiplicity one = new com.legend.executionplan.ExecutionPlan
                .Multiplicity(1, 1);
        com.legend.executionplan.ExecutionPlan.TdsResult.Shape shape;
        java.util.List<com.legend.executionplan.ExecutionPlan.Column> columns;
        java.util.List<java.util.List<Object>> rows = new java.util.ArrayList<>();
        switch (side) {
            case TypedSide.Scalar s -> {
                // the driver's Scalar(null) is Pure's [] (an empty optional value): no row, as the plan path answers;
                // a value is one, [1] as Pure types it (the test-is-the-product plan's §9 item 2; the user,
                // 2026-11-01: "match upstream engine/pure"): until then [0..1] whatever it held
                shape = com.legend.executionplan.ExecutionPlan.TdsResult.Shape.VALUE;
                columns = java.util.List.of(hostColumn("value", s.returnType(),
                        s.value() == null ? new com.legend.executionplan.ExecutionPlan.Multiplicity(0, 1) : one));
                if (s.value() != null) {
                    rows.add(hostRow(s.value()));
                }
            }
            case TypedSide.Collection c -> {
                shape = com.legend.executionplan.ExecutionPlan.TdsResult.Shape.VALUE;
                columns = java.util.List.of(hostColumn("value", c.returnType(),
                        new com.legend.executionplan.ExecutionPlan.Multiplicity(0, null)));
                for (Object v : c.values()) {
                    rows.add(hostRow(v));
                }
            }
            case TypedSide.Tabular t -> {
                shape = com.legend.executionplan.ExecutionPlan.TdsResult.Shape.RELATION;
                java.util.List<com.legend.executionplan.ExecutionPlan.Column> cs = new java.util.ArrayList<>();
                for (com.legend.testexecutionplan.Column c : t.columns()) {
                    // the side's column carries the typer's multiplicity where the judge had one; one otherwise
                    cs.add(hostColumn(c.name(), c.pureType(), c.multiplicity() == null ? one : c.multiplicity()));
                }
                columns = cs;
                for (com.legend.testexecutionplan.Row r : t.rows()) {
                    rows.add(new java.util.ArrayList<>(r.values()));
                }
            }
            case TypedSide.Graph g -> {
                shape = com.legend.executionplan.ExecutionPlan.TdsResult.Shape.OBJECTS;
                columns = java.util.List.of(hostColumn("json", "String", one));
                com.legend.json.Json.Node parsed = com.legend.json.Json.parse(g.json());
                if (parsed instanceof com.legend.json.Json.Obj sole) {
                    // an engine execute()'s values of exactly one object, serialized bare (the engine JsonBuilder's
                    // cardinality rule, compatibility only: TypedSerializeGraph.engineExecuteCardinality): one object
                    rows.add(hostRow(com.legend.json.Json.toCompact(sole)));
                } else if (parsed instanceof com.legend.json.Json.Arr items) {
                    for (com.legend.json.Json.Node item : items.items()) {
                        rows.add(hostRow(com.legend.json.Json.toCompact(item)));
                    }
                } else {
                    throw new IllegalStateException("a graph side's JSON is an array of objects or one object; got "
                            + g.json());
                }
            }
        }
        try {
            com.legend.result.Batcher batcher = new com.legend.result.Batcher(columns, answer.of(shape),
                    com.legend.result.Batcher.DEFAULT_CAPACITY);
            batcher.start();
            for (java.util.List<Object> row : rows) {
                batcher.row(row);
            }
            batcher.end();
        } catch (java.io.IOException e) {
            throw new java.io.UncheckedIOException(e);
        }
    }

    private static com.legend.executionplan.ExecutionPlan.Column hostColumn(String name, String type,
            com.legend.executionplan.ExecutionPlan.Multiplicity multiplicity) {
        return new com.legend.executionplan.ExecutionPlan.Column(name, type, multiplicity, null, null, null);
    }

    /** A row of one cell, which may be null (a list holding a null, which {@code List.of} refuses). */
    private static java.util.List<Object> hostRow(@com.legend.base.Nullable Object cell) {
        java.util.List<Object> row = new java.util.ArrayList<>(1);
        row.add(cell);
        return row;
    }

    /** {@code sink} holding the walk's answer contract ({@link Cardinality}): for the body router, whose moved body's
     *  answer holds the walk's cardinality until the plan carries it (the test-is-the-product plan's §9, D30). */
    static com.legend.result.ResultSink holding(com.legend.result.ResultSink sink, boolean atMostOne, long atLeast,
            String multiplicity) {
        return new Cardinality(sink, atMostOne, atLeast, multiplicity);
    }

    /** A sink that counts the rows on their way to {@code sink} and refuses, by the collected side's own words, a
     *  scalar of more than one row and a required root of none. (A relation's toOne() is the database's to check
     *  since D2, 2026-11-07: ToOneGuard.) */
    private static final class Cardinality implements com.legend.result.ResultSink {
        private final com.legend.result.ResultSink sink;
        private final boolean atMostOne;
        private final long atLeast;
        private final String multiplicity;
        private long rows;

        Cardinality(com.legend.result.ResultSink sink, boolean atMostOne, long atLeast, String multiplicity) {
            this.sink = sink;
            this.atMostOne = atMostOne;
            this.atLeast = atLeast;
            this.multiplicity = multiplicity;
        }

        @Override
        public void columns(List<ExecutionPlan.Column> columns) throws java.io.IOException {
            sink.columns(columns);
        }

        @Override
        public void batch(com.legend.result.Batch batch) throws java.io.IOException {
            rows += batch.size();
            if (atMostOne && rows > 1) {
                throw new IllegalStateException("scalar-shaped result returned more than one row — the to-one"
                        + " contract was not enforced upstream");
            }
            sink.batch(batch);
        }

        @Override
        public void end() throws java.io.IOException {
            if (rows == 0 && atLeast >= 1) {
                throw new IllegalStateException("Cannot cast a collection of size 0 to multiplicity " + multiplicity);
            }
            sink.end();
        }
    }

    /** {@code rows}, read for a side the plan shapes as {@code s}: the legacy executor's shaping rules, read from the
     *  plan's facts alone (the plan-as-data design's step 4, 2026-11-08; the second copy over the planner's side,
     *  shape(PlannedSide), deleted by the slice 2 audit, 2026-11-08 -- a planned constant decodes as it did). */
    static TypedSide shape(com.legend.testexecutionplan.SideShape s, PlanRunner.Rows rows) {
        return switch (s.kind()) {
            case CONSTANT -> {
                List<Object> values = new ArrayList<>(rows.rows().size());
                for (List<Object> row : rows.rows()) {
                    Object cell = row.get(0);
                    values.add(switch (cell) {
                        case null -> null;
                        case String text -> constantValue(text, s.typeName());
                        default -> throw new IllegalStateException("a constant side's cell is its text, not a "
                                + cell.getClass().getSimpleName());
                    });
                }
                yield s.scalar() ? new TypedSide.Scalar(values.isEmpty() ? null : values.get(0), s.typeName())
                        : new TypedSide.Collection(values, s.typeName());
            }
            case GRAPH -> new TypedSide.Graph(objectsShape(rows), s.typeName());
            case TABULAR -> {
                List<Column> columns = new ArrayList<>(s.columns().size());
                for (com.legend.testexecutionplan.SideShape.Column c : s.columns()) {
                    columns.add(new Column(c.name(), c.type(), c.multiplicity()));
                }
                if (s.columns().isEmpty()) {
                    // a raw grid's: the columns its result reported, each typed by the Pure type the wire table named
                    for (ExecutionPlan.Column c : rows.columns()) {
                        columns.add(new Column(c.name(), reportedType(c),
                                new ExecutionPlan.Multiplicity(c.multiplicity().lower() == 0 ? 0 : 1, 1)));
                    }
                }
                yield new TypedSide.Tabular(columns, rowsOf(rows), s.typeName(), s.oneCarrier());
            }
            case KEYED -> {
                List<Column> columns = new ArrayList<>(rows.columns().size());
                for (ExecutionPlan.Column c : rows.columns()) {
                    com.legend.testexecutionplan.SideShape.Column fixed = s.columns().stream()
                            .filter(x -> x.name().equals(c.name())).findFirst().orElse(null);
                    columns.add(fixed != null ? new Column(fixed.name(), fixed.type(), fixed.multiplicity())
                            : new Column(com.legend.sql.SqlSource.Pivot.presented(c.name()), pivotColumnType(s, c.name())));
                }
                yield new TypedSide.Tabular(columns, rowsOf(rows), s.typeName(), s.oneCarrier());
            }
            case VALUES -> valuesShape(valueLane(rows), s);
        };
    }

    private static List<Row> rowsOf(PlanRunner.Rows rows) {
        List<Row> out = new ArrayList<>(rows.rows().size());
        for (List<Object> r : rows.rows()) {
            out.add(new Row(r));
        }
        return out;
    }

    /** {@link Type.RelationType#pivotColumnType}'s one rule over the plan's columns: a fixed column by its name (the
     *  presented name's quotes stripped), a per-key one by the template its generated name ends with. */
    private static String pivotColumnType(com.legend.testexecutionplan.SideShape s, String rawName) {
        String name = rawName.length() >= 2 && rawName.startsWith("'") && rawName.endsWith("'")
                && rawName.contains(com.legend.sql.SqlSource.Pivot.SEPARATOR)
                ? rawName.substring(1, rawName.length() - 1) : rawName;
        for (com.legend.testexecutionplan.SideShape.Column c : s.columns()) {
            if (c.name().equals(name) || c.name().equals(rawName)) {
                return c.type();
            }
        }
        int sep = name.lastIndexOf(com.legend.sql.SqlSource.Pivot.SEPARATOR);
        if (sep >= 0 && !s.templates().isEmpty()) {
            String template = name.substring(sep + com.legend.sql.SqlSource.Pivot.SEPARATOR.length());
            return s.templates().stream().filter(c -> c.name().equals(template)).findFirst()
                    .map(com.legend.testexecutionplan.SideShape.Column::type)
                    .orElseThrow(() -> new IllegalStateException("pivot column '" + name + "' matches no aggregate"
                            + " template " + s.templates().stream()
                                    .map(com.legend.testexecutionplan.SideShape.Column::name).toList()));
        }
        throw new IllegalStateException("the keyed side's column '" + rawName + "' is neither among the schema's"
                + " columns " + s.columns() + " nor a per-key column of its templates " + s.templates());
    }

    /** A value side's rules, read from the plan's shape (a required root of no rows refuses; a scalar of two rows
     *  refuses; a null cell in a collection is the TDSNull value under a TDSNull root, a null slot on the Any and
     *  variant lanes, a lowering defect otherwise). */
    private static TypedSide valuesShape(PlanRunner.Rows rows, com.legend.testexecutionplan.SideShape s) {
        long lower = s.lower();
        if (s.scalar()) {
            if (rows.rows().isEmpty() && lower >= 1) {
                throw new IllegalStateException("Cannot cast a collection of size 0 to multiplicity "
                        + s.multiplicity());
            }
            if (rows.rows().size() > 1) {
                throw new IllegalStateException("scalar-shaped result returned more than one row — the to-one"
                        + " contract was not enforced upstream");
            }
            Object v = rows.rows().isEmpty() ? null : rows.rows().get(0).get(0);
            if (v instanceof List<?> l && !s.listCarrier()) {
                return new TypedSide.Collection(new ArrayList<>(l), s.typeName());
            }
            return new TypedSide.Scalar(v, s.typeName());
        }
        List<Object> values = new ArrayList<>(rows.rows().size());
        for (List<Object> r : rows.rows()) {
            Object v = r.get(0);
            if (v == null) {
                if (s.tdsNullRoot()) {
                    values.add(com.legend.sql.SqlExpr.TDS_NULL_CELL);
                } else if (s.nullableRoot()) {
                    values.add(null);
                } else {
                    throw new IllegalStateException("NULL cell reached COLLECTION egress — the lowerer owns the"
                            + " null-drop (COMPILER_SHORTCUT_AUDIT §5); a NULL here is a lowering defect, never an"
                            + " empty");
                }
            } else {
                values.add(v);
            }
        }
        if (rows.rows().isEmpty() && lower >= 1) {
            throw new IllegalStateException("Cannot cast a collection of size 0 to multiplicity " + s.multiplicity());
        }
        if (values.size() < lower) {
            throw new IllegalStateException("collection-shaped result holds " + values.size()
                    + " values, below its declared lower bound " + lower
                    + " — NULL cells were dropped past the type's own contract");
        }
        return new TypedSide.Collection(values, s.typeName());
    }

    private static String objectsShape(PlanRunner.Rows rows) {
        if (rows.rows().size() != 1) {
            throw new IllegalStateException("a graph fetch side's statement answered " + rows.rows().size()
                    + " rows: its objects' array is one row");
        }
        return switch (rows.rows().get(0).get(0)) {
            case null -> "";
            case String objects -> objects;
            case Object other -> throw new IllegalStateException("a graph fetch side's objects' array is no text: "
                    + other.getClass().getSimpleName());
        };
    }

    /** A reported column's Pure type, in the plan's spelling: the primitive the wire table named
     *  ({@code WireTypes.NAMES}), or Any (its cells the driver's kind). */
    private static String reportedType(ExecutionPlan.Column c) {
        if (com.legend.exec.WireTypes.NAMES.contains(c.type())) {
            return c.type();
        }
        if (c.type().equals("Any")) {
            return com.legend.result.RowDecode.ANY;
        }
        throw new IllegalStateException("a reported column's type '" + c.type() + "' is no primitive the wire table"
                + " names");
    }

    /** A value root's rows in the VALUE lane's carrier: a timestamp slot's cell with its digits trimmed
     *  ({@link com.legend.values.PureDateLiteral#trimmed}) -- the database judge's value-lane canon spells a
     *  timestamp so ({@code dateTimeText}: a whole second none, {@code .123} three), and so did the JSON rows
     *  wire's carrier spelling of a value root (step 6), where a grid's cell keeps the nine digits of the engine's
     *  relational read (its GRID_FETCH canon; step 7, 2026-10-18). A text-carried temporal (a literal's own digits)
     *  is read as written and stays. */
    private static PlanRunner.Rows valueLane(PlanRunner.Rows rows) {
        List<ExecutionPlan.Column> columns = rows.columns();
        boolean any = columns.stream().anyMatch(c -> c.slot() != null && c.slot().startsWith("TIMESTAMP"));
        if (!any) {
            return rows;
        }
        List<List<Object>> out = new ArrayList<>(rows.rows().size());
        for (List<Object> r : rows.rows()) {
            List<Object> cells = new ArrayList<>(r);
            for (int i = 0; i < columns.size(); i++) {
                String slot = columns.get(i).slot();
                if (slot != null && slot.startsWith("TIMESTAMP")
                        && cells.get(i) instanceof com.legend.values.PureDateLiteral d) {
                    cells.set(i, d.trimmed());
                }
            }
            out.add(cells);
        }
        return new PlanRunner.Rows(columns, out);
    }
}
