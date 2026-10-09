// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.protocol;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static com.legend.protocol.Composing.TAB;
import static com.legend.protocol.Composing.convertPath;
import static com.legend.protocol.Composing.tab;

/**
 * {@code ###Deephaven}'s store and app, and its connection, as upstream prints them
 * ({@code DeephavenGrammarComposerExtension}) -- over the records ({@link Protocol.PDeephavenDatabase}, the
 * {@code DeephavenApp} {@link Protocol.PFunctionActivator}, {@link Protocol.PDeephavenConnection}; the protocol
 * program's leg 2, step 3).
 */
final class DeephavenComposer {

    private static final String APP = "DeephavenApp";

    /** A column type's keyword, by its {@code _type}. */
    private static final Map<String, String> COLUMN_TYPES = Map.of(
            "stringType", "STRING", "intType", "INT", "booleanType", "BOOLEAN", "floatType", "FLOAT",
            "doubleType", "DOUBLE", "timestampType", "TIMESTAMP", "dateTimeType", "DATETIME");

    private DeephavenComposer() {
    }

    /** The section's two kinds. Upstream's free section prints stores only: an app with no section index is dropped. */
    static String element(Protocol.Element e) {
        return switch (e) {
            case Protocol.PDeephavenDatabase store -> store(store);
            case Protocol.PFunctionActivator app when APP.equals(app.kind()) -> app(app);
            default -> throw Composing.refused("no Deephaven printer for a " + e.getClass().getSimpleName());
        };
    }

    private static String store(Protocol.PDeephavenDatabase store) {
        List<String> tables = new ArrayList<>();
        for (Protocol.PDeephavenDatabase.PDeephavenTable t : store.tables()) {
            List<String> columns = new ArrayList<>();
            for (Protocol.PDeephavenColumn c : t.columns()) {
                columns.add(tab(3) + c.name() + ": " + columnType(c));
            }
            tables.add(tab(2) + "Table " + t.name() + "\n" + tab(2) + "(\n"
                    + (columns.isEmpty() ? "" : String.join(",\n", columns) + "\n") + tab(2) + ")");
        }
        return "Deephaven " + Composing.elementPath(store.pkg(), store.name()) + "\n(\n"
                + (tables.isEmpty() ? "" : String.join("\n", tables) + "\n") + ")";
    }

    private static String columnType(Protocol.PDeephavenColumn c) {
        String t = c.kind();
        if ("decimalType".equals(t)) {
            if (c.precision() == null || c.scale() == null) {
                throw Composing.refused("a Deephaven DECIMAL column without its precision and scale");
            }
            return "DECIMAL(" + c.precision() + ", " + c.scale() + ")";
        }
        String keyword = COLUMN_TYPES.get(t);
        if (keyword == null) {
            throw Composing.refused("no composer rule for a Deephaven column type of _type '" + t + "'");
        }
        return keyword;
    }

    private static String app(Protocol.PFunctionActivator app) {
        String name = app.scalars().get("applicationName");
        if (name == null) {
            throw Composing.refused("a DeephavenApp without its applicationName");
        }
        String description = app.scalars().get("description");
        return "DeephavenApp " + Composing.elementPath(app.pkg(), app.name()) + "\n{\n"
                + TAB + "applicationName: '" + name + "';\n"
                + TAB + "function: " + app.functionPath() + ";\n"
                + (description != null ? TAB + "description: '" + description + "';\n" : "")
                + (app.ownerId() != null ? TAB + "ownership: Deployment { identifier: '" + app.ownerId() + "' };\n" : "")
                + "}";
    }

    static String connection(Protocol.PDeephavenConnection c, String i) {
        if (c.element() == null) {
            throw Composing.refused("a Deephaven connection with no store");
        }
        return i + "{\n"
                + i + TAB + "store: " + convertPath(c.element()) + ";\n"
                + i + TAB + "serverUrl: '" + c.serverUrl() + "'\n"
                + i + TAB + "authentication: " + AuthenticationComposer.authentication(new Protocol.PPskAuth(c.psk()), 1, i)
                + ";\n"
                + i + "}";
    }
}
