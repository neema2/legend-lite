// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.result;

import com.legend.executionplan.ExecutionPlan;

import java.io.OutputStream;
import java.io.Writer;

/**
 * WHERE A RUN'S ANSWER GOES, as data (2026-11-09, the user's question "why do we have any callbacks, esp dangerous
 * ones that can break our tenet?"): the caller names a format and the stream it is written to, and the result library
 * makes the writer from the answer the plan states -- its shape, its columns, its statement -- before any statement
 * runs, so a format with no form for the answer refuses it untouched (a program's writes never land behind a refused
 * answer). Until then the caller handed the run a factory it called with the plan: the caller's code deciding inside
 * the run. {@link Rows}, the caller's own receiver of the answer, is output only: it is told the answer's shape and
 * handed its columns and rows, and decides nothing the run does. The set is closed: a new format is a new variant here.
 */
public sealed interface Output
        permits Output.JsonRows, Output.Csv, Output.Engine, Output.Arrow, Output.Rows {

    /** lite's JSON rows ({@link JsonRowsWriter}). */
    record JsonRows(Writer out) implements Output {
    }

    /** CSV ({@link CsvWriter}). */
    record Csv(Writer out) implements Output {
    }

    /** legend-engine's {@code pure/v1} execute answer ({@link PureV1Writer}): a relation's envelope from the facts read
     *  before the run, a graph fetch's objects, a value refused as the facts say. */
    record Engine(Writer out, PureV1Writer.Facts facts) implements Output {
    }

    /** legend-engine's Arrow answer ({@link ArrowWriter}): a relation under the envelope's metadata; a graph fetch and
     *  a value refused. */
    record Arrow(OutputStream out, PureV1Writer.Facts facts) implements Output {
    }

    /** The caller's own receiver of the answer, output only ({@link ResultSink#answers}). */
    record Rows(ResultSink sink) implements Output {
    }

    /** The execution a run named before its columns, refused by name where it named none: an answer's activity reports
     *  the execution that ran its SQL. */
    private static java.util.UUID ran(java.util.@com.legend.base.Nullable UUID execution) {
        if (execution == null) {
            throw new IllegalStateException("the run named no execution before its columns: the activity reports the"
                    + " execution that ran its SQL (ResultSink.execution)");
        }
        return execution;
    }

    /** The writer of {@code plan}'s answer in this format, made before any of the plan's statements runs. */
    default ResultSink writerFor(ExecutionPlan plan) {
        ExecutionPlan.TdsResult.Shape shape = plan.answerShape();
        return switch (this) {
            case Engine e -> switch (shape) {
                case OBJECTS -> PureV1Writer.objects(e.out());
                case RELATION -> {
                    String sql = plan.answerSql().statement();
                    java.util.List<ExecutionPlan.Column> stated = plan.answerColumns();
                    // the envelope from the plan's stated columns BEFORE any statement runs, so a column with no
                    // builder entry refuses before a write program changes anything; made from the announced columns
                    // only where the plan states none (a dynamic pivot's: the phase 3 audit, 2026-10-27)
                    if (stated == null) {
                        yield PureV1Writer.relation(e.out(), columns -> PureV1Writer.envelope(e.facts(), sql, columns));
                    }
                    PureV1Writer.Envelope made = PureV1Writer.envelope(e.facts(), sql, stated);
                    yield PureV1Writer.relation(e.out(), columns -> made);
                }
                case VALUE -> throw new com.legend.error.NotImplementedException(e.facts().valueRefusal());
            };
            case Arrow a -> switch (shape) {
                case OBJECTS -> throw new IllegalArgumentException("ARROW_IPC answers a relation query; a graph fetch "
                        + "under it is refused (upstream's answer unprobed)");
                case RELATION -> {
                    String sql = plan.answerSql().statement();
                    java.util.List<ExecutionPlan.Column> stated = plan.answerColumns();
                    // the JSON route's rule (Engine above): the metadata from the plan's stated columns before any
                    // statement runs; from the announced columns only where the plan states none
                    // the metadata's activity names the execution the run names before its columns (C1's second half)
                    if (stated == null) {
                        yield new ArrowWriter(a.out(), (java.util.function.BiFunction<java.util.List<ExecutionPlan.Column>,
                                java.util.UUID, java.util.Map<String, String>>) (columns, execution) ->
                                PureV1Writer.arrowMetadata(a.facts(), sql, columns, ran(execution)));
                    }
                    PureV1Writer.envelope(a.facts(), sql, stated);
                    yield new ArrowWriter(a.out(), (java.util.function.BiFunction<java.util.List<ExecutionPlan.Column>,
                            java.util.UUID, java.util.Map<String, String>>) (columns, execution) ->
                            PureV1Writer.arrowMetadata(a.facts(), sql, stated, ran(execution)));
                }
                case VALUE -> throw new com.legend.error.NotImplementedException(a.facts().valueRefusal());
            };
            case JsonRows j -> new JsonRowsWriter(j.out(), shape);
            case Csv c -> new CsvWriter(c.out(), shape);
            case Rows r -> told(r.sink(), shape);
        };
    }

    /** The writer of an answer of {@code shape} that no plan states (a test body's last statement, a host side's
     *  rows): lite's own formats; the engine's need the plan's statement and columns and are refused by name. */
    default ResultSink writerFor(ExecutionPlan.TdsResult.Shape shape) {
        return switch (this) {
            case JsonRows j -> new JsonRowsWriter(j.out(), shape);
            case Csv c -> new CsvWriter(c.out(), shape);
            case Rows r -> told(r.sink(), shape);
            case Engine e -> throw new IllegalArgumentException(
                    "the engine's answer needs a plan's statement and columns: a test body's answer is written in "
                            + "lite's formats");
            case Arrow a -> throw new IllegalArgumentException(
                    "the engine's Arrow answer needs a plan's statement and columns: a test body's answer is written "
                            + "in lite's formats");
        };
    }

    private static ResultSink told(ResultSink sink, ExecutionPlan.TdsResult.Shape shape) {
        sink.answers(shape);
        return sink;
    }
}
