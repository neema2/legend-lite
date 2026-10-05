// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.warehouse.sqlapi;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;

import com.legend.testing.Runfile;
import java.nio.file.Files;
import java.util.Arrays;
import org.junit.jupiter.api.Test;

/** The SQL API compiles to WebAssembly (Bazel workplan P3-24): this test's data is the compiled module, so building
 *  it is the compile; the module is checked to be WebAssembly. */
class WasmBuildTest {

    @Test
    void theSqlApiCompilesToWebAssembly() throws Exception {
        java.nio.file.Path wasm = Runfile.envList("SQLAPI_WASM").stream()
                .filter(p -> p.getFileName().toString().endsWith(".wasm")).findFirst().orElseThrow();
        byte[] module = Files.readAllBytes(wasm);
        assertArrayEquals(new byte[] {0, 'a', 's', 'm'}, Arrays.copyOf(module, 4), "a WebAssembly module's magic");
    }
}
