package com.legend.warehouse.server.duck;

/** A statement DuckDB refused or failed: its message, and DuckDB's kind for it (duckdb_error_type). */
public final class DuckException extends Exception {

    private final int kind;

    DuckException(int kind, String message) {
        super(message);
        this.kind = kind;
    }

    /** A statement stopped before it began (a cancel or a timeout while it waited). */
    public static DuckException cancelledBeforeStart() {
        return new DuckException(Duck.ERROR_INTERRUPT, "INTERRUPT Error: cancelled before start");
    }

    public boolean parse() {
        return kind == Duck.ERROR_PARSER;
    }

    /** A binder or catalog error: the statement names something that is not there. */
    public boolean bind() {
        return kind == Duck.ERROR_BINDER || kind == Duck.ERROR_CATALOG;
    }

    public boolean interrupted() {
        return kind == Duck.ERROR_INTERRUPT;
    }
}
