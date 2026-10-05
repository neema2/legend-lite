// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.warehouse.sqlapi;

import java.util.List;

/**
 * The SQL API's WebAssembly entry (Bazel workplan P3-24): what //warehouse:sqlapi_wasm compiles with TeaVM, so a class
 * the API reaches that TeaVM's class library lacks fails the build, not a browser (wasm/README.md). TeaVM compiles only
 * what is reachable, so the entry reaches EVERY method of the binding (and through them the JSON codec and its parser),
 * the codec's request writer and reader, and the Arrow reader, each behind a branch on {@code args}, which the compiler
 * cannot fold away. Compiled, never run.
 */
public final class WasmEntry {

    private WasmEntry() {}

    public static void main(String[] args) {
        if (args.length < 2) {
            return;
        }
        SqlApiBinding binding = new NativeBinding();
        String a = args[1];
        SqlApiBinding.HttpResult result = new SqlApiBinding.HttpResult(200, a);
        Object out = switch (args[0]) {
            case "login" -> binding.login(a, a);
            case "token" -> binding.token(result);
            case "refresh" -> binding.refresh(a);
            case "submit" -> binding.submit(ApiJson.parseStatementRequest(a), a);
            case "next" -> binding.next(result, a);
            case "fetchChunk" -> binding.fetchChunk(a, a.length(), a);
            case "chunk" -> binding.chunk(result);
            case "arrowChunk" -> ArrowIpcReader.read(binding.arrowChunk(result), List.of()).size();
            case "cancel" -> binding.cancel(a, a);
            case "closeStatement" -> binding.closeStatement(a, a);
            case "openSession" -> binding.openSession(a, a);
            case "session" -> binding.session(result);
            case "closeSession" -> binding.closeSession(a, a);
            case "historyCall" -> binding.history(a.length(), a);
            case "history" -> binding.history(result);
            case "objectsCall" -> binding.objects(a, a);
            case "allObjects" -> binding.allObjects(a);
            case "objects" -> binding.objects(result);
            case "statementRequest" -> ApiJson.statementRequest(ApiJson.parseStatementRequest(a));
            default -> "";
        };
        System.out.println(out);
    }
}
