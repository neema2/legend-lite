// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.testexec;

/** Where a program's last statement answers: the sink for its shape, made once the shape is known (a relation,
 *  a value of one column, a graph fetch's objects) -- a writer at the boundary, as a product answer's
 *  (the product door's {@code sinkOf}). The test's run streams a planned statement's typed rows through the plan
 *  runner to it; an answer it holds in Java (a host constant the prelude gave, a verdict, a staged let's rows, a
 *  graph fetch's objects read as one cell) is written through a batcher as a host's rows are. */
@FunctionalInterface
public interface Answer {
    com.legend.result.ResultSink of(com.legend.executionplan.ExecutionPlan.TdsResult.Shape shape);
}
