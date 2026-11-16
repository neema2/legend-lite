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
        permits Output.JsonRows, Output.Csv, Output.Rows {

    /** lite's JSON rows ({@link JsonRowsWriter}). */
    record JsonRows(Writer out) implements Output {
    }

    /** CSV ({@link CsvWriter}). */
    record Csv(Writer out) implements Output {
    }

    // legend-engine's pure/v1 and Arrow answers are the compatibility edge's receivers (edge.EngineAnswers, W8 of
    // docs/records/d-plan-2026-11-16.md, 2026-11-16), written through Rows: told the plan's stated answer before any
    // statement runs (ResultSink.planned)

    /** The caller's own receiver of the answer, output only ({@link ResultSink#answers}). */
    record Rows(ResultSink sink) implements Output {
    }

    /** The writer of {@code plan}'s answer in this format, made before any of the plan's statements runs -- or after,
     *  from the rows a run collected -- for {@code execution}, the id the run's door minted, which an engine answer's
     *  activity reports (refused by name where there is none: 2026-11-12, the exec-id item -- until then the run told
     *  the writer, and one made after the run was told nothing). */
    default ResultSink writerFor(ExecutionPlan plan, java.util.@com.legend.base.Nullable UUID execution) {
        ExecutionPlan.TdsResult.Shape shape = plan.answerShape();
        return switch (this) {
            case JsonRows j -> new JsonRowsWriter(j.out(), shape);
            case Csv c -> new CsvWriter(c.out(), shape);
            case Rows r -> {
                ResultSink sink = told(r.sink(), shape);
                if (execution != null) {
                    sink.execution(execution);
                }
                sink.planned(plan);
                yield sink;
            }
        };
    }

    /** The writer of an answer of {@code shape} that no plan states (a test body's last statement, a host side's
     *  rows): lite's own formats; the engine's need the plan's statement and columns and are refused by name. */
    default ResultSink writerFor(ExecutionPlan.TdsResult.Shape shape) {
        return switch (this) {
            case JsonRows j -> new JsonRowsWriter(j.out(), shape);
            case Csv c -> new CsvWriter(c.out(), shape);
            case Rows r -> told(r.sink(), shape);
        };
    }

    private static ResultSink told(ResultSink sink, ExecutionPlan.TdsResult.Shape shape) {
        sink.answers(shape);
        return sink;
    }
}
