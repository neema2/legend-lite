package com.legend.exec;

import com.legend.executionplan.ExecutionPlan;

import java.sql.Connection;
import java.sql.SQLException;

/**
 * An engine's own bulk-load API, joined through {@code ServiceLoader} (core
 * compiles against no driver: an implementation lives beside the drivers and
 * is found at run time). An engine with none loads through
 * one INSERT ({@code RowLoad.values}).
 *
 * <p>The contract: the rows land exactly as the text path would land them
 * &mdash; each cell cast by the DATABASE from its text to the column's type.
 * The loader appends every cell, as text, into the staging table of the
 * {@link ExecutionPlan.SetupStep.Rows} it is handed and runs that step's
 * statements; it spells no SQL and types no value.
 */
public interface BulkLoad {

    /** Whether this loader speaks {@code connection}'s engine. */
    boolean accepts(Connection connection) throws SQLException;

    /** Loads {@code rows}: creates their staging table, appends the cells, copies them into the target, drops it. */
    void load(Connection connection, ExecutionPlan.SetupStep.Rows rows) throws SQLException;
}
