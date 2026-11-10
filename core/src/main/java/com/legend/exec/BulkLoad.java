package com.legend.exec;

import com.legend.executionplan.ExecutionPlan;

import java.sql.Connection;
import java.sql.SQLException;

/**
 * An engine's own bulk-load API, joined through {@code ServiceLoader} (core
 * compiles against no driver: an implementation lives beside the drivers and
 * is found at run time). An engine with none loads through
 * the streamed delivery's chunked inserts the plan names instead.
 *
 * <p>The contract: the rows land exactly as the text path would land them
 * &mdash; each cell cast by the DATABASE from its text to the column's type.
 * The loader appends every cell, as text, into the staging table of the
 * {@link ExecutionPlan.LoadStep.Delivery.Staged} it is handed and runs that form's
 * statements -- the staging table dropped first and after, a failure's or not; it spells no SQL and types no value.
 */
public interface BulkLoad {

    /** Whether this loader speaks {@code connection}'s engine. */
    boolean accepts(Connection connection) throws SQLException;

    /** Loads {@code rows}: drops any staging table left of the name ({@code dropStaging}), creates it
     *  ({@code createStaging}, named {@code stagingTable}), appends the cells, copies them into the target
     *  ({@code copy}), and drops it whether the copy ran or failed -- a streamed delivery's staged form. */
    void load(Connection connection, String stagingTable, String createStaging, String copy, String dropStaging,
            java.util.List<java.util.List<String>> rows) throws SQLException;
}
