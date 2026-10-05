// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.protocol;

import com.legend.json.Json;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

import static com.legend.protocol.Composing.convertIdentifier;
import static com.legend.protocol.Composing.convertPath;
import static com.legend.protocol.Composing.elementPath;
import static com.legend.protocol.Composing.objs;
import static com.legend.protocol.Composing.str;
import static com.legend.protocol.Composing.tab;

/**
 * {@code ###Runtime}'s runtimes and the runtime values services embed, as upstream prints them
 * ({@code DEPRECATED_PureGrammarComposerCore.visit(PackageableRuntime)} and {@code HelperRuntimeGrammarComposer}).
 */
final class RuntimeComposer {

    private static final String SINGLE_CONNECTION = "localEngineRuntime";

    private RuntimeComposer() {
    }

    static String runtime(Json.Obj runtime) {
        Json.Obj value = runtime.getObj("runtimeValue");
        return (SINGLE_CONNECTION.equals(Composing.type(value)) ? "SingleConnectionRuntime " : "Runtime ")
                + elementPath(runtime) + "\n{" + runtimeValue(value, 1, false, "") + "\n}";
    }

    /**
     * {@code renderRuntimeValue}: {@code indentation} is the printing composer's own, which an embedded
     * connection's body is indented from.
     */
    static String runtimeValue(Json.Obj rt, int base, boolean embedded, String indentation) {
        StringBuilder b = new StringBuilder();
        List<Json.Node> mappings = Composing.items(rt, "mappings");
        if (!embedded || !mappings.isEmpty()) {
            List<String> ms = new ArrayList<>();
            for (Json.Node m : mappings) {
                ms.add(tab(base + 1) + DatabaseComposer.pointerPath(m));
            }
            b.append("\n").append(tab(base)).append("mappings:\n").append(tab(base)).append("[\n")
                    .append(String.join(",\n", ms)).append(ms.isEmpty() ? "" : "\n").append(tab(base)).append("];");
        }
        List<Json.Obj> connectionStores = objs(rt, "connectionStores");
        if (SINGLE_CONNECTION.equals(Composing.type(rt))) {
            if (!connectionStores.isEmpty()) {
                b.append("\n").append(tab(base)).append("connection: ")
                        .append(convertPath(connectionStores.get(0).getObj("connectionPointer").getString("connection"))).append(";");
            }
            return b.toString();
        }
        List<String> connections = new ArrayList<>();
        for (Json.Obj sc : objs(rt, "connections")) {
            List<Json.Obj> ics = objs(sc, "storeConnections");
            if (!ics.isEmpty()) {
                List<String> out = new ArrayList<>();
                for (Json.Obj ic : ics) {
                    out.add(identifiedConnection(ic, base + 2, indentation));
                }
                connections.add(tab(base + 1) + convertPath(DatabaseComposer.pointerPath(sc.get("store"))) + ":\n"
                        + tab(base + 1) + "[\n" + String.join(",\n", out) + "\n" + tab(base + 1) + "]");
            }
        }
        if (!connections.isEmpty()) {
            b.append("\n").append(tab(base)).append("connections:\n").append(tab(base)).append("[\n")
                    .append(String.join(",\n", connections)).append("\n").append(tab(base)).append("];");
        }
        if (!connectionStores.isEmpty()) {
            List<String> stores = new ArrayList<>();
            for (Json.Obj cs : connectionStores) {
                List<Json.Obj> pointers = objs(cs, "storePointers");
                if (!pointers.isEmpty()) {
                    List<String> ps = new ArrayList<>();
                    for (Json.Obj p : pointers) {
                        ps.add(tab(base + 2) + elementPointer(p));
                    }
                    stores.add(tab(base + 1) + convertPath(cs.getObj("connectionPointer").getString("connection")) + ":\n"
                            + tab(base + 1) + "[\n" + String.join(",\n", ps) + "\n" + tab(base + 1) + "]");
                }
            }
            b.append("\n").append(tab(base)).append("connectionStores:\n").append(tab(base)).append("[\n")
                    .append(String.join(",\n", stores)).append("\n").append(tab(base)).append("];");
        }
        return b.toString();
    }

    private static String identifiedConnection(Json.Obj ic, int base, String indentation) {
        Json.Obj connection = ic.getObj("connection");
        String id = convertIdentifier(ic.getString("id"));
        if ("connectionPointer".equals(Composing.type(connection))) {
            return tab(base) + id + ": " + convertPath(connection.getString("connection"));
        }
        ConnectionComposer.Kind kind = ConnectionComposer.kind(connection);
        return tab(base) + id + ":\n" + tab(base) + "#{\n"
                + tab(base + 1) + kind.keyword() + "\n"
                + kind.body().apply(connection, indentation + tab(base + 1)) + "\n"
                + tab(base) + "}#";
    }

    /** {@code renderPackageableElementPointer}: a store's bare, any other kind's prefixed by its kind. */
    static String elementPointer(Json.Obj p) {
        String type = str(p, "type");
        return (type == null || "STORE".equals(type) ? "" : "(" + type.toLowerCase(Locale.ROOT) + ") ") + convertPath(p.getString("path"));
    }
}
