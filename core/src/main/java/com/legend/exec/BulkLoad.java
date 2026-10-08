package com.legend.exec;

import com.legend.setup.RowLoad;

import java.sql.Connection;
import java.sql.SQLException;

/**
 * An engine's own bulk-load API, joined through {@code ServiceLoader} (core
 * compiles against no driver: an implementation lives beside the drivers and
 * is found at run time). An engine with none loads through
 * {@link RowLoad#values}.
 *
 * <p>The contract: the rows land exactly as the text path would land them
 * &mdash; each cell cast by the DATABASE from its text to the column's type.
 * The loader appends every cell, as text, into the {@link Staging} table and
 * runs the statements it is handed; it spells no SQL and types no value.
 */
public interface BulkLoad {

    /** The staging table and its three statements, rendered by the session's
     *  dialect: create it (one text column per cell, temporary), copy it into
     *  the target (the database casts), drop it. */
    record Staging(String table, String create, String copy, String drop) {
    }

    /** Whether this loader speaks {@code connection}'s engine. */
    boolean accepts(Connection connection) throws SQLException;

    /** Loads {@code load} (not empty) through {@code staging}. */
    void load(Connection connection, RowLoad load, Staging staging) throws SQLException;
}
