// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.result;

import com.legend.executionplan.ExecutionPlan;
import com.legend.json.Json;

import java.io.IOException;
import java.io.Writer;
import java.util.List;

/**
 * legend-engine's {@code pure/v1} execute answer, written at the edge (docs/RESULT_BOUNDARY_PROGRAM_2026_10_26.md, the
 * LEGACY writer of D6; 2026-10-27): a relation as the engine's TDS result, byte for byte in its streaming serializer's
 * layout -- {@code builder} and {@code activities} as the caller made them, then the columns' names and each row's
 * {@code values} in the engine's tokens ({@link CellText#engine}); a graph fetch's objects as the engine's JSON
 * result, the array or the one object bare. The rows stream as they arrive; nothing is buffered but a graph fetch's
 * first object (whether it is alone decides the layout).
 */
public final class PureV1Writer implements ResultSink {

    /** A relation's envelope: the TDS builder's JSON (each column's name, Pure type and relational spelling), the
     *  activities' JSON (the relational activity carrying the SQL), and the columns' names in the engine's order, the
     *  order each row's values are written in. */
    public record Envelope(String builder, String activities, List<String> names) {
        public Envelope {
            names = List.copyOf(names);
        }
    }

    /** A column's type as the engine's TDS builder spells it ({@code type}, {@code relationalType}), or why the engine
     *  has no spelling of it ({@code refusal}, raised where the column is answered). */
    public record ColumnType(@com.legend.base.Nullable String type, @com.legend.base.Nullable String relationalType,
            @com.legend.base.Nullable String refusal) {
        public ColumnType {
            if ((refusal == null) == (type == null || relationalType == null)) {
                throw new IllegalArgumentException("a column type is its two spellings or a refusal, not both");
            }
        }

        public static ColumnType spelled(String type, String relationalType) {
            return new ColumnType(type, relationalType, null);
        }

        public static ColumnType refused(String why) {
            return new ColumnType(null, null, why);
        }
    }

    /**
     * WHAT THE ENVELOPE KNOWS OF THE QUERY, as data read before the run (2026-11-09, the user's question "why do we have
     * any callbacks?": the writer was a factory the run called with the plan, reading the compiled query): each column
     * the compiler typed, by name, its builder spelling; each primitive by the plan's short name, for a column only the
     * data names (a dynamic pivot's generated one, named with {@code pivotSeparator}); and the refusal of a value's
     * answer, whose serialization by the engine is unprobed.
     */
    public record Facts(java.util.Map<String, ColumnType> declared, java.util.Map<String, ColumnType> primitives,
            String pivotSeparator, String valueRefusal) {
        public Facts {
            declared = java.util.Collections.unmodifiableMap(new java.util.LinkedHashMap<>(declared));
            primitives = java.util.Collections.unmodifiableMap(new java.util.LinkedHashMap<>(primitives));
            java.util.Objects.requireNonNull(pivotSeparator, "pivotSeparator");
            java.util.Objects.requireNonNull(valueRefusal, "valueRefusal");
        }
    }

    /** A relation's envelope for the columns the run announced: each column's builder entry by the type the compiler
     *  gave it (a column only the data names -- a dynamic pivot's -- by its plan type, a primitive), the relational
     *  activity carrying {@code sql}, and the columns' names as the engine labels them. Moved from the server's
     *  pure/v1 API with the facts it reads (2026-11-09). */
    public static Envelope envelope(Facts facts, String sql, List<ExecutionPlan.Column> columns) {
        List<java.util.Map<String, Object>> builderColumns = new java.util.ArrayList<>();
        List<String> names = new java.util.ArrayList<>();
        for (var c : columns) {
            // a column's type is the compiler's, under its name as the plan spells it or as a cast presents it (a
            // pivot's generated name quoted); a column only the data names -- a dynamic pivot's generated one -- has
            // none, and is typed by its plan type, a primitive; any other column the compiler did not type is refused
            String name = CellText.presentedName(c.name());
            ColumnType t = facts.declared().containsKey(c.name()) ? facts.declared().get(c.name())
                    : facts.declared().containsKey(name) ? facts.declared().get(name)
                    : c.name().contains(facts.pivotSeparator())
                            // (the plan names a primitive by its short name, the table by its path: every dynamic
                            // pivot's engine answer was refused until 2026-10-27, found by the PCT read as the engine's
                            // adapter reads it, the result boundary program's stage 1, step 3)
                            ? primitive(facts, c)
                            : null;
            if (t == null) {
                throw new IllegalStateException("column '" + c.name() + "' is none the compiler typed: "
                        + facts.declared().keySet());
            }
            if (t.refusal() != null) {
                throw new com.legend.error.NotImplementedException(t.refusal());
            }
            java.util.Map<String, Object> col = new java.util.LinkedHashMap<>();
            col.put("name", name);
            col.put("type", t.type());
            col.put("relationalType", t.relationalType());
            builderColumns.add(col);
            names.add(name);
        }
        java.util.Map<String, Object> builder = new java.util.LinkedHashMap<>();
        builder.put("_type", "tdsBuilder");
        builder.put("columns", builderColumns);
        return new Envelope(Json.toCompact(builder), Json.toCompact(List.of(activity(sql, true))), names);
    }

    /** The Arrow answer's schema metadata for the columns the run announced (measured against legend-engine 4.145.0,
     *  2026-10-08): {@code legend.builder}, the TDS builder as the JSON answer's; {@code legend.activities}, the
     *  activities WITHOUT their {@code _type}; {@code legend.columns}, the column names. */
    public static java.util.Map<String, String> arrowMetadata(Facts facts, String sql,
            List<ExecutionPlan.Column> columns) {
        Envelope e = envelope(facts, sql, columns);
        java.util.Map<String, String> m = new java.util.LinkedHashMap<>();
        m.put("legend.builder", e.builder());
        m.put("legend.activities", Json.toCompact(List.of(activity(sql, false))));
        m.put("legend.columns", Json.toCompact(e.names()));
        return m;
    }

    /**
     * The relational activity, carrying the SQL: with its {@code _type} first, as the JSON answer writes it, or
     * without, as the Arrow answer's metadata does (measured, 4.145.0).
     */
    public static java.util.Map<String, Object> activity(String sql, boolean typed) {
        java.util.Map<String, Object> activity = new java.util.LinkedHashMap<>();
        if (typed) {
            activity.put("_type", "relational");
        }
        activity.put("comment", "-- \"executionTraceID\" : \"" + java.util.UUID.randomUUID() + "\"");
        activity.put("sql", sql);
        return activity;
    }

    private static ColumnType primitive(Facts facts, ExecutionPlan.Column c) {
        ColumnType t = facts.primitives().get(c.type());
        if (t == null) {
            throw new IllegalStateException("the generated column '" + c.name() + "' of type " + c.type()
                    + " is no primitive");
        }
        return t;
    }

    private final Writer out;
    private final ExecutionPlan.TdsResult.Shape shape;
    private final java.util.function.Function<List<ExecutionPlan.Column>, Envelope> envelope;
    private List<ExecutionPlan.Column> columns = List.of();
    private int[] order = new int[0];
    private long rows;
    private @com.legend.base.Nullable String firstObject;

    private PureV1Writer(Writer out, ExecutionPlan.TdsResult.Shape shape,
            java.util.function.Function<List<ExecutionPlan.Column>, Envelope> envelope) {
        this.out = out;
        this.shape = shape;
        this.envelope = envelope;
    }

    /** A relation's answer, its envelope made from the columns the stream announces (a dynamic pivot's columns are
     *  known only once its keys are read). */
    public static PureV1Writer relation(Writer out,
            java.util.function.Function<List<ExecutionPlan.Column>, Envelope> envelope) {
        return new PureV1Writer(out, ExecutionPlan.TdsResult.Shape.RELATION, envelope);
    }

    /** A graph fetch's answer: the objects, no builder of columns. */
    public static PureV1Writer objects(Writer out) {
        return new PureV1Writer(out, ExecutionPlan.TdsResult.Shape.OBJECTS, columns -> {
            throw new IllegalStateException("not reached: a graph fetch's answer has no envelope of columns");
        });
    }

    @Override
    public void columns(List<ExecutionPlan.Column> columns) throws IOException {
        this.columns = List.copyOf(columns);
        if (shape == ExecutionPlan.TdsResult.Shape.OBJECTS) {
            return;
        }
        Envelope e = envelope.apply(this.columns);
        // each row's values in the engine's column order, read from the plan's columns by the name the engine labels
        // each with (CellText.presentedName: a pivot's generated column quoted)
        List<String> names = e.names();
        order = new int[names.size()];
        for (int i = 0; i < names.size(); i++) {
            order[i] = -1;
            for (int j = 0; j < columns.size(); j++) {
                if (CellText.presentedName(columns.get(j).name()).equals(names.get(i))) {
                    order[i] = j;
                }
            }
            if (order[i] < 0) {
                throw new IllegalStateException("the engine's column '" + names.get(i) + "' is none of the plan's "
                        + columns.stream().map(ExecutionPlan.Column::name).toList());
            }
        }
        out.write("{\"builder\": " + e.builder() + ", \"activities\": " + e.activities()
                + ", \"result\" : {\"columns\" : " + Json.toCompact(names) + ", \"rows\" : [");
    }

    @Override
    public void batch(Batch batch) throws IOException {
        if (shape == ExecutionPlan.TdsResult.Shape.OBJECTS) {
            for (int r = 0; r < batch.size(); r++) {
                if (!(batch.get(r, 0) instanceof String object)) {
                    throw new IllegalStateException("a graph fetch's row holds no object's JSON text: " + batch.get(r, 0));
                }
                if (rows == 0) {
                    firstObject = object;
                } else {
                    if (rows == 1) {
                        out.write("{\"builder\":{\"_type\":\"json\"},\"values\":[" + firstObject);
                    }
                    out.write("," + object);
                }
                rows++;
            }
            out.flush();
            return;
        }
        StringBuilder b = new StringBuilder();
        // each row's values written into the batch's text as they are made (the bytes of Json.toCompact over the
        // row's list: a compact writer at the root writes its arrays back to back), not a list and a text per row
        // (the payoff measurement, 2026-11-01)
        Json.Writer values = Json.compactWriter(b);
        for (int r = 0; r < batch.size(); r++) {
            b.append(rows == 0 ? "" : ",").append("{\"values\": ");
            values.beginArray();
            for (int i : order) {
                CellText.writeEngine(values, batch.get(r, i), columns.get(i));
            }
            values.endArray();
            b.append('}');
            rows++;
        }
        out.write(b.toString());
        out.flush();
    }

    @Override
    public void end() throws IOException {
        if (shape == ExecutionPlan.TdsResult.Shape.OBJECTS) {
            if (rows == 0) {
                out.write("{\"builder\":{\"_type\":\"json\"},\"values\":[]}");
            } else if (rows == 1) {
                // one object, bare (as Pure's serialize writes a one-element collection)
                out.write("{\"builder\":{\"_type\":\"json\"},\"values\":"
                        + Json.toCompact(Json.parse(java.util.Objects.requireNonNull(firstObject))) + "}");
            } else {
                out.write("]}");
            }
            out.flush();
            return;
        }
        out.write("]}}");
        out.flush();
    }
}
