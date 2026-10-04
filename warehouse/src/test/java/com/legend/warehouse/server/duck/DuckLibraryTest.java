package com.legend.warehouse.server.duck;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import org.junit.jupiter.api.Test;

/** DuckDB's library is given, never extracted from its JDBC jar into a temporary directory (Bazel workplan P1-16). */
class DuckLibraryTest {

    @Test
    void aJvmWithoutTheLibraryFailsNamingTheFlag() {
        IOException e = assertThrows(IOException.class, () -> DuckLibrary.load(null));
        assertTrue(e.getMessage().contains("--duckdb-library"), e.getMessage());
    }
}
