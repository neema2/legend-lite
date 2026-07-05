/**
 * Phase K &mdash; execution: rendered SQL + a JDBC connection &rarr; a typed
 * {@link com.legend.exec.ExecutionResult} (PHASE_K_EXECUTION.md). The result
 * SHAPE is classified ONCE from the query root's Pure type
 * ({@link com.legend.exec.ResultShape}); cell values are raw JDBC objects and
 * the Pure {@code Type} on the result is the semantic carrier &mdash; consumers
 * convert, never sniff SQL types. Representation-identical to the engine
 * module's result types BY DESIGN: the QueryService bridge is a bijection and
 * dies with the engine module.
 */
package com.legend.exec;
