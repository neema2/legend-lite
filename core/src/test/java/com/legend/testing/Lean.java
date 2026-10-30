// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.testing;

import com.legend.Execution;
import com.legend.exec.PlanSessions;
import com.legend.exec.Sessions;
import com.legend.executionplan.ExecutionPlan;
import com.legend.json.Json;
import com.legend.result.Batch;
import com.legend.result.CellText;
import com.legend.result.JsonRowsWriter;
import com.legend.result.ResultSink;
import com.legend.result.RowDecode;

import java.io.IOException;
import java.io.StringWriter;
import java.io.UncheckedIOException;
import java.sql.Connection;
import java.util.ArrayList;
import java.util.List;

/**
 * THE LEAN TWIN'S READER (the result boundary program's track T, 2026-10-29; the user's ruling of 2026-10-28: the
 * same queries and logic, judged on the platform output users run, never the backwards-compatibility mode). A query
 * run by the product's own route -- planned as typed rows ({@link Execution#run}), run by the plan's runner, written
 * as lite's JSON rows ({@link JsonRowsWriter}: what the query service serves) -- and read back as a typed client reads
 * them: the columns the runner handed the writer (name, Pure type, multiplicity: a keyed plan's are known only when
 * its keys have run), and each cell decoded from the JSON by its column's three type facts with the product's own
 * row decoder ({@link RowDecode}: an Integer a long, a Float a double, a Decimal an exact BigDecimal, a date its
 * {@code PureDateLiteral}, a String its text). A reader of the product's output, never a copy of the old door's
 * {@code ExecutionResult} (deleted in track D); it keeps the accessor names the tests use ({@code rows().get(0).get(0)},
 * {@code columns().get(0).name()}, {@code rowCount()}). Test code only.
 */
public final class Lean {

    private Lean() {
    }

    /** {@code query} over {@code model} on {@code runtime} (null: the query names its own), on the caller's connection
     *  {@code c} as the caller set it up (its tables, its data): lite's JSON rows, read back. */
    public static Result run(String model, String query, @com.legend.base.Nullable String runtime, Connection c) {
        StringWriter out = new StringWriter();
        Capturing sink = new Capturing(out);
        ExecutionPlan plan;
        try {
            plan = Execution.run(LaneFloor.model(model, c), query, runtime, com.legend.Execution.Source.given(com.legend.testbackend.TestSessions.over(c)), PlanSessions.of(com.legend.testbackend.TestSessions.over(c)),
                    p -> sink.of(p));
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return Result.of(plan.answerShape(), sink.columns, out.toString());
    }

    /** {@code query} over {@code model} on {@code runtime} as the server runs it with no caller's connection (the
     *  route {@code QueryService.execute(model, query, runtime, sinkOf)} was until D67, 2026-10-30): on the connection
     *  the runtime declares, leased from the server's resolver ({@code ConnectionResolver.SOURCE}), the plan sessions
     *  shared -- the session the COMPILER decided, which is what a test of the server's sessions judges. Lite's JSON
     *  rows, read back. */
    public static Result service(String model, String query, @com.legend.base.Nullable String runtime) {
        StringWriter out = new StringWriter();
        Capturing sink = new Capturing(out);
        ExecutionPlan plan;
        try {
            plan = Execution.run(LaneFloor.model(model), query, runtime, com.legend.server.ConnectionResolver.SOURCE,
                    PlanSessions.shared(), p -> sink.of(p));
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return Result.of(plan.answerShape(), sink.columns, out.toString());
    }

    /** {@code query} over {@code model} on {@code runtime}, on the caller's connection {@code c}, the runtime's
     *  DECLARED test data established on it first, as the engine-shaped API establishes a connection on every
     *  acquisition ({@code ConnectionResolver.RUNNER}: {@code Execution.Establish.DECLARED_SETUP}); a test of the
     *  declared data's loading judges this route. Lite's JSON rows, read back. */
    public static Result established(String model, String query, @com.legend.base.Nullable String runtime,
            Connection c) {
        StringWriter out = new StringWriter();
        Capturing sink = new Capturing(out);
        ExecutionPlan plan;
        try {
            plan = Execution.run(com.legend.Compiler.query(com.legend.Compiler.compileModel(LaneFloor.model(model, c)), query),
                    runtime,
                    com.legend.Execution.Source.given(com.legend.testbackend.TestSessions.over(c)), PlanSessions.of(com.legend.testbackend.TestSessions.over(c)), Execution.Establish.DECLARED_SETUP, p -> sink.of(p));
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return Result.of(plan.answerShape(), sink.columns, out.toString());
    }

    /** The JSON rows writer with the columns the runner handed it kept: what the plan that RAN names and types; a
     *  program's answer the same way, its shape as the body driver decided it ({@link Program}; the doors,
     *  2026-10-29). */
    static final class Capturing implements ResultSink {
        private final StringWriter out;
        private JsonRowsWriter writer;
        private List<ExecutionPlan.Column> columns = List.of();
        private ExecutionPlan.TdsResult.@com.legend.base.Nullable Shape shape;

        Capturing(StringWriter out) {
            this.out = out;
        }

        /** The sink for a program's answer of {@code shape} ({@link Execution.Answer}). */
        ResultSink of(ExecutionPlan.TdsResult.Shape shape) {
            this.shape = shape;
            writer = new JsonRowsWriter(out, shape);
            return this;
        }

        /** The answer read back, once the body has answered; null for a body whose last statement answered nothing. */
        @com.legend.base.Nullable Result answered() {
            return shape == null ? null : Result.of(shape, columns, out.toString());
        }

        ResultSink of(ExecutionPlan plan) {
            writer = new JsonRowsWriter(out, plan.answerShape());
            return this;
        }

        @Override
        public void columns(List<ExecutionPlan.Column> columns) throws IOException {
            this.columns = List.copyOf(columns);
            writer.columns(columns);
        }

        @Override
        public void batch(Batch batch) throws IOException {
            writer.batch(batch);
        }

        @Override
        public void end() throws IOException {
            writer.end();
        }
    }

    /**
     * A query's answer as a typed client reads lite's JSON rows: the plan's shape ({@code RELATION}; {@code VALUE},
     * a scalar or a collection, one column {@code value}; {@code OBJECTS}, a graph fetch's objects), its columns as
     * the plan that ran names and types them, its rows with each cell decoded by its column ({@link RowDecode#cell}).
     * {@code text} is the JSON as written.
     */
    public record Result(ExecutionPlan.TdsResult.Shape shape, List<Column> columns, List<List<Object>> rows,
            String text) {

        static Result of(ExecutionPlan.TdsResult.Shape shape, List<ExecutionPlan.Column> planColumns, String json) {
            List<Column> columns = new ArrayList<>(planColumns.size());
            for (ExecutionPlan.Column c : planColumns) {
                columns.add(new Column(c.name(), c.type(), c.multiplicity()));
            }
            Json.Node parsed = Json.parse(json);
            if (!(parsed instanceof Json.Arr rowsJson)) {
                throw new IllegalStateException("lite's JSON rows are an array; got " + json);
            }
            List<List<Object>> rows = new ArrayList<>(rowsJson.items().size());
            for (Json.Node r : rowsJson.items()) {
                if (shape == ExecutionPlan.TdsResult.Shape.OBJECTS) {
                    // a graph fetch's row is one object, kept as its JSON text
                    List<Object> one = new ArrayList<>(1);
                    one.add(Json.toCompact(r));
                    rows.add(one);
                    continue;
                }
                if (!(r instanceof Json.Obj o)) {
                    throw new IllegalStateException("a JSON row is an object keyed by the plan's columns; got "
                            + Json.toCompact(r));
                }
                List<Object> cells = new ArrayList<>(planColumns.size());
                for (ExecutionPlan.Column c : planColumns) {
                    // the writer keys a row by the column's PRESENTED name (a generated pivot column in its quotes,
                    // as the engine labels it: CellText.presentedName); the reader looks it up the same way
                    Json.Node node = o.fields().get(CellText.presentedName(c.name()));
                    if (node == null) {
                        throw new IllegalStateException("the row has no field for the column '" + c.name()
                                + "' (it has " + o.fields().keySet() + ")");
                    }
                    if (c.type().equals(RowDecode.VARIANT) && node instanceof Json.Str s) {
                        // lite's JSON rows write a Variant as a string holding its JSON (legend-engine's measured
                        // form, CellText): the string's value is the variant's JSON text (rule 4)
                        cells.add(s.value());
                        continue;
                    }
                    // a TAGGED column (a mixed selection, docs/RESULTS_AND_RENDERERS_2026_10_22.md §5a) reaches the
                    // writer as the one cell the session resolved from its tag and slot columns; its JSON token is
                    // read by the column's declared Pure type (a Number: a whole number a Long, any other an exact
                    // decimal; a Date: its text parsed), as the decoder reads any slotless cell (rule 3)
                    cells.add(RowDecode.cell(node, RowDecode.slotWord(c.slot()).equals("TAGGED")
                            ? new ExecutionPlan.Column(c.name(), c.type(), c.multiplicity(), null, null, null) : c));
                }
                rows.add(cells);
            }
            return new Result(shape, List.copyOf(columns), List.copyOf(rows), json);
        }

        public int rowCount() {
            return rows.size();
        }

        public int columnCount() {
            return columns.size();
        }

        /** The one cell of the one row (a scalar answer); null for a null cell; refused for any other shape. */
        public @com.legend.base.Nullable Object one() {
            if (rows.size() != 1 || rows.get(0).size() != 1) {
                throw new AssertionError("one value expected, got " + rows.size() + " row(s) of "
                        + columns.size() + " column(s)");
            }
            return rows.get(0).get(0);
        }

        /** The answer as a client reads a VALUE: a to-one column's one cell (null when no row was written: Pure's
         *  {@code []}), a to-many column's cells in order; refused for any other shape. */
        public @com.legend.base.Nullable Object value() {
            if (shape != ExecutionPlan.TdsResult.Shape.VALUE || columns.size() != 1) {
                throw new AssertionError("value() reads a VALUE answer's one column; this answer is " + shape
                        + " of " + columns.size() + " column(s)");
            }
            Integer upper = columns.get(0).multiplicity().upper();
            if (upper != null && upper <= 1) {
                return rows.isEmpty() ? null : one();
            }
            return values();
        }

        /** Column 0 of every row (a collection answer). */
        public List<Object> values() {
            List<Object> out = new ArrayList<>(rows.size());
            for (List<Object> r : rows) {
                out.add(r.get(0));
            }
            return out;
        }

        /** A graph fetch's objects as written: the JSON array text. */
        public String json() {
            if (shape != ExecutionPlan.TdsResult.Shape.OBJECTS) {
                throw new AssertionError("json() reads a graph fetch's objects; this answer is " + shape);
            }
            return text;
        }

        public int columnIndex(String name) {
            for (int i = 0; i < columns.size(); i++) {
                if (columns.get(i).name().equals(name)) {
                    return i;
                }
            }
            throw new AssertionError("Column '" + name + "' not found in " + columns);
        }
    }

    /** A column as the plan that ran names and types it. */
    public record Column(String name, String type, ExecutionPlan.Multiplicity multiplicity) {
    }
}
