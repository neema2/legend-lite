// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.protocol;

import com.legend.json.Json;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

import static com.legend.protocol.Composing.convertIdentifier;
import static com.legend.protocol.Composing.convertPath;
import static com.legend.protocol.Composing.str;
import static com.legend.protocol.Composing.tab;

/**
 * {@code ###Runtime}'s runtimes and the runtime values services embed, as upstream prints them
 * ({@code DEPRECATED_PureGrammarComposerCore.visit(PackageableRuntime)} and {@code HelperRuntimeGrammarComposer}) --
 * over the records ({@link Protocol.PRuntime}, {@link Protocol.PEmbeddedRuntime}; the protocol program's leg 2,
 * step 3).
 */
final class RuntimeComposer {

    private RuntimeComposer() {
    }

    static String runtime(Protocol.PRuntime runtime) {
        return (runtime.single() ? "SingleConnectionRuntime " : "Runtime ")
                + Composing.elementPath(runtime.pkg(), runtime.name()) + "\n{"
                + runtimeValue(runtime.mappings(), runtime.connections(), runtime.connectionStores(), runtime.single(),
                        1, false, "")
                + "\n}";
    }

    /** {@link #runtime(Protocol.PRuntime)} of the JSON, read first. */
    static String runtime(Json.Obj runtime) {
        return runtime(Composing.element(runtime, Protocol.PRuntime.class));
    }

    /** A runtime a service embeds, at {@code base}; {@code indentation} is the printing composer's own. */
    static String embedded(Protocol.PEmbeddedRuntime runtime, int base, String indentation) {
        return runtimeValue(runtime.mappings(), runtime.connections(), runtime.connectionStores(), false, base, true,
                indentation);
    }

    /**
     * {@code renderRuntimeValue}: {@code indentation} is the printing composer's own, which an embedded
     * connection's body is indented from.
     */
    private static String runtimeValue(List<Protocol.PPointer> mappings, List<Protocol.PStoreConnections> storeConnections,
            List<Protocol.PConnectionStores> connectionStores, boolean single, int base, boolean embedded,
            String indentation) {
        StringBuilder b = new StringBuilder();
        if (!embedded || !mappings.isEmpty()) {
            List<String> ms = new ArrayList<>();
            for (Protocol.PPointer m : mappings) {
                ms.add(tab(base + 1) + m.path());
            }
            b.append("\n").append(tab(base)).append("mappings:\n").append(tab(base)).append("[\n")
                    .append(String.join(",\n", ms)).append(ms.isEmpty() ? "" : "\n").append(tab(base)).append("];");
        }
        if (single) {
            if (!connectionStores.isEmpty()) {
                b.append("\n").append(tab(base)).append("connection: ")
                        .append(convertPath(pointer(connectionStores.get(0)))).append(";");
            }
            return b.toString();
        }
        List<String> connections = new ArrayList<>();
        for (Protocol.PStoreConnections sc : storeConnections) {
            if (!sc.storeConnections().isEmpty()) {
                List<String> out = new ArrayList<>();
                for (Protocol.PIdentifiedConnection ic : sc.storeConnections()) {
                    out.add(identifiedConnection(ic, base + 2, indentation));
                }
                connections.add(tab(base + 1) + convertPath(sc.store().path()) + ":\n"
                        + tab(base + 1) + "[\n" + String.join(",\n", out) + "\n" + tab(base + 1) + "]");
            }
        }
        if (!connections.isEmpty()) {
            b.append("\n").append(tab(base)).append("connections:\n").append(tab(base)).append("[\n")
                    .append(String.join(",\n", connections)).append("\n").append(tab(base)).append("];");
        }
        if (!connectionStores.isEmpty()) {
            List<String> stores = new ArrayList<>();
            for (Protocol.PConnectionStores cs : connectionStores) {
                if (!cs.storePointers().isEmpty()) {
                    List<String> ps = new ArrayList<>();
                    for (Protocol.PStorePointer p : cs.storePointers()) {
                        ps.add(tab(base + 2) + elementPointer(p.type(), p.path()));
                    }
                    stores.add(tab(base + 1) + convertPath(pointer(cs)) + ":\n"
                            + tab(base + 1) + "[\n" + String.join(",\n", ps) + "\n" + tab(base + 1) + "]");
                }
            }
            b.append("\n").append(tab(base)).append("connectionStores:\n").append(tab(base)).append("[\n")
                    .append(String.join(",\n", stores)).append("\n").append(tab(base)).append("];");
        }
        return b.toString();
    }

    /** The connection a {@code connectionStores} group names: a pointer, by the grammar. */
    private static String pointer(Protocol.PConnectionStores cs) {
        if (!(cs.connectionPointer() instanceof Protocol.PConnectionPointer p)) {
            throw Composing.refused("a connectionStores group whose connection is not a pointer");
        }
        return p.connection();
    }

    private static String identifiedConnection(Protocol.PIdentifiedConnection ic, int base, String indentation) {
        String id = convertIdentifier(ic.id());
        if (ic.connection() instanceof Protocol.PConnectionPointer p) {
            return tab(base) + id + ": " + convertPath(p.connection());
        }
        return tab(base) + id + ":\n" + tab(base) + "#{\n"
                + tab(base + 1) + ConnectionComposer.keyword(ic.connection()) + "\n"
                + ConnectionComposer.body(ic.connection(), indentation + tab(base + 1)) + "\n"
                + tab(base) + "}#";
    }

    /** {@code renderPackageableElementPointer}: a store's bare, any other kind's prefixed by its kind. */
    static String elementPointer(@com.legend.base.Nullable String type, String path) {
        return (type == null || "STORE".equals(type) ? "" : "(" + type.toLowerCase(Locale.ROOT) + ") ") + convertPath(path);
    }

    /** {@link #elementPointer(String, String)} of a JSON pointer, for the printers not yet moved onto records. */
    static String elementPointer(Json.Obj p) {
        return elementPointer(str(p, "type"), p.getString("path"));
    }
}
