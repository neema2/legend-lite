// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.protocol;

import com.legend.json.Json;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static com.legend.protocol.Composing.TAB;
import static com.legend.protocol.Composing.convertPath;
import static com.legend.protocol.Composing.elementPath;
import static com.legend.protocol.Composing.objs;
import static com.legend.protocol.Composing.str;
import static com.legend.protocol.Composing.tab;

/**
 * {@code ###Deephaven}'s store and app, and its connection, as upstream prints them
 * ({@code DeephavenGrammarComposerExtension}).
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
    static String element(Json.Obj e) {
        return APP.equals(Composing.type(e)) ? app(e) : store(e);
    }

    private static String store(Json.Obj store) {
        List<String> tables = new ArrayList<>();
        for (Json.Obj t : objs(store, "tables")) {
            List<String> columns = new ArrayList<>();
            for (Json.Obj c : objs(t, "columns")) {
                columns.add(tab(3) + c.getString("name") + ": " + columnType(c.getObj("type")));
            }
            tables.add(tab(2) + "Table " + t.getString("name") + "\n" + tab(2) + "(\n"
                    + (columns.isEmpty() ? "" : String.join(",\n", columns) + "\n") + tab(2) + ")");
        }
        return "Deephaven " + elementPath(store) + "\n(\n" + (tables.isEmpty() ? "" : String.join("\n", tables) + "\n") + ")";
    }

    private static String columnType(Json.Obj type) {
        String t = Composing.type(type);
        if ("decimalType".equals(t)) {
            return "DECIMAL(" + RelationalConnectionComposer.raw(type.get("precision")) + ", " + RelationalConnectionComposer.raw(type.get("scale")) + ")";
        }
        String keyword = COLUMN_TYPES.get(t);
        if (keyword == null) {
            throw Composing.refused("no composer rule for a Deephaven column type of _type '" + t + "'");
        }
        return keyword;
    }

    private static String app(Json.Obj app) {
        String description = str(app, "description");
        Json.Obj owner = Composing.objOr(app, "ownership");
        return "DeephavenApp " + elementPath(app) + "\n{\n"
                + TAB + "applicationName: '" + app.getString("applicationName") + "';\n"
                + TAB + "function: " + app.getObj("function").getString("path") + ";\n"
                + (description != null ? TAB + "description: '" + description + "';\n" : "")
                + (owner != null && "DeploymentOwner".equals(Composing.type(owner))
                        ? TAB + "ownership: Deployment { identifier: '" + owner.getString("id") + "' };\n" : "")
                + "}";
    }

    static String connection(Json.Obj c, String i) {
        return i + "{\n"
                + i + TAB + "store: " + convertPath(c.getString("element")) + ";\n"
                + i + TAB + "serverUrl: '" + c.getObj("sourceSpec").getString("url") + "'\n"
                + i + TAB + "authentication: " + AuthenticationComposer.authentication(c.getObj("authSpec"), 1, i) + ";\n"
                + i + "}";
    }
}
