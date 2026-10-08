package com.legend.exec;

import com.legend.setup.RowLoad;
import org.duckdb.DuckDBAppender;
import org.duckdb.DuckDBConnection;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

/**
 * DuckDB's Appender, keeping the typing in the database: the Appender takes
 * only values of the column's own type (a string into an INTEGER column is
 * refused), so every cell is appended as TEXT into the staging table and the
 * staging copy casts it into the target &mdash; the cast DuckDB applies to the
 * text path's string literal. Every statement is the dialect's, handed in;
 * this class spells none. ~20x faster than the multi-row insert at 10K-100K
 * rows, the rows identical.
 */
public final class DuckDbAppenderLoad implements BulkLoad {

    /** Where DuckDB keeps a temporary table: catalog {@code temp}, schema {@code main}. */
    private static final String TEMP_CATALOG = "temp";
    private static final String TEMP_SCHEMA = "main";

    /** The staging table's drop, as a try-with-resources resource. */
    private interface StagingDrop extends AutoCloseable {
        @Override
        void close() throws SQLException;
    }

    @Override
    public boolean accepts(Connection connection) throws SQLException {
        return connection.isWrapperFor(DuckDBConnection.class);
    }

    @Override
    public void load(Connection connection, RowLoad load, Staging staging) throws SQLException {
        try (Statement st = connection.createStatement()) {
            st.execute(staging.create());
        }
        // the staging drop runs as the try's resource, on its OWN statement: DuckDB closes a statement that raised,
        // and a drop through it threw "Statement was closed" in place of the real error, leaving the staging table
        // behind for every later load (rebuild D23, found by the damaged data). As a resource, a failed drop rides
        // the load's own error as suppressed, whatever that error is, and is thrown only when the load succeeded.
        try (StagingDrop ignored = () -> {
            try (Statement drop = connection.createStatement()) {
                drop.execute(staging.drop());
            }
        }) {
            try (DuckDBAppender appender = connection.unwrap(DuckDBConnection.class)
                    .createAppender(TEMP_CATALOG, TEMP_SCHEMA, staging.table())) {
                for (List<String> row : load.rows()) {
                    appender.beginRow();
                    for (String cell : row) {
                        if (cell == null) {
                            appender.appendNull();
                        } else {
                            appender.append(cell);
                        }
                    }
                    appender.endRow();
                }
            }
            try (Statement st = connection.createStatement()) {
                st.execute(staging.copy());
            }
        }
    }
}
