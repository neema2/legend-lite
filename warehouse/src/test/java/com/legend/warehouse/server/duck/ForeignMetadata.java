// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.warehouse.server.duck;

import java.io.IOException;
import java.lang.foreign.AddressLayout;
import java.lang.foreign.FunctionDescriptor;
import java.lang.foreign.MemoryLayout;
import java.lang.foreign.PaddingLayout;
import java.lang.foreign.StructLayout;
import java.lang.foreign.ValueLayout;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.TreeSet;

/**
 * THE NATIVE IMAGE'S FFM METADATA, from the code that makes the calls (Bazel workplan P2-09): every downcall
 * signature in {@link Duck#DOWNCALLS} (and an Arrow release callback's) and every callback in
 * {@link AuthenticatedUser#UPCALLS}, rendered in GraalVM's reachability-metadata notation ({@code void*},
 * {@code jlong}, {@code struct(jlong,jlong)}, {@code padding(6)}). {@code //warehouse:foreign_metadata} runs it;
 * {@code bazel run //warehouse:update_foreign_metadata} writes the committed file, and {@code //:generated} fails when
 * a signature changed without it. The file is its own metadata directory (com.legend/warehouse-foreign), beside the
 * recorded one: GraalVM reads every directory, and no section of this one is written by hand.
 *
 * <pre>
 *   ForeignMetadata &lt;output&gt;
 * </pre>
 */
public final class ForeignMetadata {

    private ForeignMetadata() {}

    public static void main(String[] args) throws IOException {
        if (args.length != 1) {
            throw new IllegalArgumentException("usage: ForeignMetadata <output>");
        }
        // distinct signatures, in a stable order (GraalVM keys a downcall by its signature, not its symbol)
        TreeSet<String> downcalls = new TreeSet<>();
        for (FunctionDescriptor d : Duck.DOWNCALLS.values()) {
            downcalls.add(entry(null, null, d));
        }
        downcalls.add(entry(null, null, Duck.RELEASE));
        List<String> upcalls = new ArrayList<>();
        for (AuthenticatedUser.Upcall u : AuthenticatedUser.UPCALLS) {
            upcalls.add(entry(AuthenticatedUser.class.getName(), u.method(), u.descriptor()));
        }
        upcalls.sort(null);
        String json = "{\n  \"foreign\": {\n    \"downcalls\": [\n" + String.join(",\n", downcalls)
                + "\n    ],\n    \"directUpcalls\": [\n" + String.join(",\n", upcalls) + "\n    ]\n  }\n}\n";
        Files.writeString(Path.of(args[0]), json, StandardCharsets.UTF_8);
    }

    private static String entry(String cls, String method, FunctionDescriptor d) {
        StringBuilder sb = new StringBuilder("      {");
        if (cls != null) {
            sb.append("\"class\": \"").append(cls).append("\", \"method\": \"").append(method).append("\", ");
        }
        sb.append("\"returnType\": \"").append(d.returnLayout().map(ForeignMetadata::type).orElse("void"))
                .append("\", \"parameterTypes\": [");
        List<String> ps = new ArrayList<>();
        for (MemoryLayout l : d.argumentLayouts()) {
            ps.add("\"" + type(l) + "\"");
        }
        return sb.append(String.join(", ", ps)).append("]}").toString();
    }

    /** A layout in GraalVM's notation. */
    static String type(MemoryLayout l) {
        if (l instanceof AddressLayout) {
            return "void*";
        }
        if (l instanceof ValueLayout v) {
            Class<?> c = v.carrier();
            if (c == int.class) return "jint";
            if (c == long.class) return "jlong";
            if (c == byte.class) return "jbyte";
            if (c == short.class) return "jshort";
            if (c == char.class) return "jchar";
            if (c == float.class) return "jfloat";
            if (c == double.class) return "jdouble";
            if (c == boolean.class) return "jboolean";
        }
        if (l instanceof PaddingLayout p) {
            return "padding(" + p.byteSize() + ")";
        }
        if (l instanceof StructLayout s) {
            List<String> members = new ArrayList<>();
            for (MemoryLayout m : s.memberLayouts()) {
                members.add(type(m));
            }
            return "struct(" + String.join(",", members) + ")";
        }
        throw new IllegalStateException("no GraalVM notation for the layout " + l);
    }
}
