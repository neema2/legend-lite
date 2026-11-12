// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.diagnostics;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;

/**
 * {@code -Dlegend.diagnostics=capture-queries=FILE} (MEASUREMENT ONLY; E3 of the total-wall program, the
 * orchestrator's ruling of 2026-11-12, docs/LEAN_PRODUCT_SQL_DESIGN_2026_11_12.md §7): every product query planned
 * from a model's TEXT and a query's TEXT ({@code Compiler.compileModel(String)}, {@code Compiler.query(ctx, String)})
 * appended to FILE as one JSON line, {@code {"model", "query", "runtime"}}, so the lean-SQL baseline can plan the same
 * queries again on every database. The runtime is its NAME, as the query names it; the model is the text the caller
 * gave, which names a connection's credential and never holds one (nothing is resolved here).
 *
 * <p>It decides nothing: off (every normal run), each hook returns at once; on, a hook records text beside the
 * objects the planner made and appends a line after the plan is made, and the planner reads nothing back. A query
 * typed from a protocol model or an already parsed query has no text and is not recorded. It writes only to the file
 * the switch names: the switch without a file is refused by name.
 */
public final class QueryCapture {

    private QueryCapture() {
    }

    /** The file the switch names; null when the switch is off. */
    private static final @com.legend.base.Nullable String FILE = file();

    /** Each recorded object's source text: a model context's model, a typed query's query (by identity: neither
     *  defines equality; weak, so a captured run keeps no object alive). */
    private static final java.util.Map<Object, String> SOURCES =
            java.util.Collections.synchronizedMap(new java.util.WeakHashMap<>());

    private static @com.legend.base.Nullable String file() {
        if (!Diagnostics.on("capture-queries")) {
            return null;
        }
        String f = Diagnostics.value("capture-queries");
        if (f == null || f.strip().isEmpty()) {
            throw new IllegalArgumentException("-Dlegend.diagnostics=capture-queries=FILE: the switch names the file it"
                    + " writes to, never a default path");
        }
        return f;
    }

    /** Whether the switch is on. */
    public static boolean on() {
        return FILE != null;
    }

    /** {@code of} (a model context, a typed query) was made from {@code text}. */
    public static void source(Object of, String text) {
        if (FILE != null) {
            SOURCES.put(of, text);
        }
    }

    /** {@code query}, typed over {@code model}, was planned for the runtime named {@code runtime}: one line, when both
     *  were made from text. */
    public static void planned(Object model, Object query, @com.legend.base.Nullable String runtime) {
        if (FILE == null) {
            return;
        }
        String m = SOURCES.get(model);
        String q = SOURCES.get(query);
        if (m == null || q == null) {
            return;
        }
        String line = "{\"model\":" + quoted(m) + ",\"query\":" + quoted(q) + ",\"runtime\":"
                + quoted(runtime == null ? "" : runtime) + "}\n";
        // java.io, not java.nio.file: the planner compiles to WebAssembly too (//wasm), whose class library has it
        synchronized (QueryCapture.class) {
            try (java.io.OutputStream out = new java.io.FileOutputStream(FILE, true)) {
                out.write(line.getBytes(StandardCharsets.UTF_8));
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
    }

    /** {@code text} as a JSON string (this switchboard depends on {@code //base} alone, so it writes its own). */
    static String quoted(String text) {
        StringBuilder b = new StringBuilder(text.length() + 2).append('"');
        for (int i = 0; i < text.length(); i++) {
            char c = text.charAt(i);
            switch (c) {
                case '"' -> b.append("\\\"");
                case '\\' -> b.append("\\\\");
                case '\n' -> b.append("\\n");
                case '\r' -> b.append("\\r");
                case '\t' -> b.append("\\t");
                default -> {
                    if (c < 0x20) {
                        b.append(String.format(java.util.Locale.ROOT, "\\u%04x", (int) c));
                    } else {
                        b.append(c);
                    }
                }
            }
        }
        return b.append('"').toString();
    }
}
