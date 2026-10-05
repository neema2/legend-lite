// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.warehouse.sqlapi;

import java.util.List;

/**
 * The SQL API's WebAssembly entry (Bazel workplan P3-24): what //warehouse:sqlapi_wasm compiles with TeaVM, so a class
 * the API reaches that TeaVM's class library lacks fails the build, not a browser (wasm/README.md). It reaches the
 * binding, the JSON codec and the Arrow reader behind branches on {@code args}, which the compiler cannot fold away.
 * Compiled, never run.
 */
public final class WasmEntry {

    private WasmEntry() {}

    public static void main(String[] args) {
        SqlApiBinding binding = new NativeBinding();
        StringBuilder out = new StringBuilder();
        if (args.length > 0) {
            out.append(binding.login(args[0], args[0]));
        }
        if (args.length > 1) {
            out.append(ApiJson.login(args[0], args[1]));
        }
        if (args.length > 2) {
            out.append(ArrowIpcReader.read(args[2].getBytes(java.nio.charset.StandardCharsets.UTF_8), List.of()).size());
        }
        System.out.println(out);
    }
}
