package com.gs.legend.compiled;

/**
 * The kind of a {@link CompiledConnection} — distinguishes a relational
 * (JDBC / DuckDB / etc.) connection from a model-chain connection that
 * feeds one mapping's output into another as a source.
 */
public enum ConnectionKind {
    /** Relational connection to a database store. */
    RELATIONAL,
    /** Model-chain connection: the "source" is another mapping's output. */
    MODEL_CHAIN
}
