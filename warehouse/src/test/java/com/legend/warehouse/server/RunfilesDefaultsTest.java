package com.legend.warehouse.server;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import com.legend.testing.Runfile;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;

/**
 * Started by Bazel with no DuckDB flag, the server finds //warehouse:duckdb_library in its runfiles (Bazel workplan
 * P1-16): the default that `bazel run //warehouse:server` and a test's child JVM rely on.
 */
class RunfilesDefaultsTest {

    @Test
    void withNoFlagTheLibraryIsTheOneInTheRunfiles() throws Exception {
        Path found = WarehouseServer.commandLine(new String[] {}, null).config().duckdbLibrary();
        assertNotNull(found, "no DuckDB library found in this test's runfiles");
        assertEquals(Runfile.property("warehouse.duckdb.library").toRealPath(), found.toRealPath());
    }
}
