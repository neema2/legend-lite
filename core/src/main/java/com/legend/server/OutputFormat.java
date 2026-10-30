package com.legend.server;

/**
 * The text {@link QueryService#execute(String, String, String, java.sql.Connection, java.io.OutputStream,
 * OutputFormat)} writes a query's typed rows in: lite's CSV ({@link com.legend.result.CsvWriter}) or its JSON rows
 * ({@link com.legend.result.JsonRowsWriter}), each written batch by batch as the rows arrive.
 */
public enum OutputFormat {
    JSON,
    CSV
}
