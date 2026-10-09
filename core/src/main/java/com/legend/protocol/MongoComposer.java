// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.protocol;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static com.legend.protocol.Composing.TAB;
import static com.legend.protocol.Composing.tab;

/**
 * {@code ###MongoDB}'s database store, its class mapping and its connection as upstream prints them
 * ({@code MongoDBGrammarComposerExtension}, {@code MongoDBSchemaComposer}, {@code MongoDBMappingComposer},
 * and the JSON-schema printer {@code BaseTypeVisitorImpl} a collection's validator goes through) -- over the records
 * ({@link Protocol.PMongoDatabase}, {@link Protocol.PClassMappingMongoDb}, {@link Protocol.PMongoDbConnection}; the
 * protocol program's leg 2, step 3). The schema settings the reader has no rule for (numeric minimums and maximums,
 * item and property counts, an additional-properties schema) are refused when read.
 */
final class MongoComposer {

    /** A schema node kind's printer, given the node and its indent level. */
    private interface SchemaPrinter {
        String print(Protocol.PBsonSchema node, int level);
    }

    /** The scalar BSON types: their {@code bsonType}, and whether the node's length bounds print. */
    private record Scalar(String bsonType, boolean lengthBounds) {
    }

    private static final Map<String, Scalar> SCALARS = Map.of(
            "stringType", new Scalar("string", true),
            "intType", new Scalar("int", false),
            "longType", new Scalar("long", false),
            "decimalType", new Scalar("decimal", false),
            "boolType", new Scalar("bool", false));

    private static final Map<String, SchemaPrinter> SCHEMA_PRINTERS = Map.of(
            "objectIdType", (n, level) -> "{\n" + tab(level + 1) + key("bsonType") + quoted("objectId") + "\n" + tab(level) + "}",
            "objectType", (n, level) -> "{\n" + tab(level + 1) + key("bsonType") + quoted("object") + objectBody(n, level + 1)
                    + "\n" + tab(level) + "}",
            "schema", (n, level) -> tab(level) + "{\n" + tab(level + 1) + key("bsonType") + quoted("object") + objectBody(n, level + 1)
                    + "\n" + tab(level) + "}",
            "arrayType", MongoComposer::array);

    private MongoComposer() {
    }

    // ---------------------------------------------------------------------
    // The store
    // ---------------------------------------------------------------------

    static String store(Protocol.PMongoDatabase store) {
        StringBuilder b = new StringBuilder("Database ").append(Composing.elementPath(store.pkg(), store.name()))
                .append("\n(\n");
        for (Protocol.PMongoDatabase.PMongoCollection c : store.collections()) {
            b.append(TAB).append("Collection ").append(DatabaseComposer.convertIdentifierDoubleQuoted(c.name())).append("\n")
                    .append(TAB).append("(\n")
                    .append(tab(2)).append("validationLevel: ").append(c.validationLevel()).append(";\n")
                    .append(tab(2)).append("validationAction: ").append(c.validationAction()).append(";\n")
                    .append(tab(2)).append("jsonSchema: ").append(schema(c.schema(), 2).trim()).append(";\n")
                    .append(TAB).append(")\n");
        }
        return b.append(")").toString();
    }

    /** {@code BaseTypeVisitorImpl}: one schema node at {@code level}. */
    private static String schema(Protocol.PBsonSchema node, int level) {
        String type = node.wireType();
        Scalar scalar = SCALARS.get(type);
        if (scalar != null) {
            return scalar(node, scalar, level);
        }
        SchemaPrinter printer = SCHEMA_PRINTERS.get(type);
        if (printer == null) {
            // upstream's visitor answers null for every other BSON type and prints the word 'null'
            throw Composing.refused("no composer rule for a MongoDB schema node of _type '" + type + "'");
        }
        return printer.print(node, level);
    }

    private static String scalar(Protocol.PBsonSchema node, Scalar scalar, int level) {
        StringBuilder b = new StringBuilder("{\n").append(tab(level + 1)).append(key("bsonType")).append(quoted(scalar.bsonType()));
        description(b, node, level + 1);
        if (scalar.lengthBounds()) {
            bound(b, "minLength", node.minLength(), level + 1);
            bound(b, "maxLength", node.maxLength(), level + 1);
        }
        return b.append("\n").append(tab(level)).append("}").toString();
    }

    private static void description(StringBuilder b, Protocol.PBsonSchema node, int level) {
        if (node.description() != null) {
            b.append(",\n").append(tab(level)).append(key("description")).append(quoted(node.description()));
        }
    }

    /** A numeric bound, printed as Java prints the boxed integer. */
    private static void bound(StringBuilder b, String name, @com.legend.base.Nullable Long value, int level) {
        if (value != null) {
            b.append(",\n").append(tab(level)).append(key(name)).append(value);
        }
    }

    private static String array(Protocol.PBsonSchema node, int level) {
        int values = level + 1;
        StringBuilder b = new StringBuilder("{\n").append(tab(values)).append(key("bsonType")).append(quoted("array")).append(",\n");
        if (node.description() != null) {
            b.append(tab(values)).append(key("description")).append(quoted(node.description())).append(",\n");
        }
        List<Protocol.PBsonSchema> itemNodes = node.items() == null ? List.of() : node.items();
        if (itemNodes.size() == 1) {
            b.append(tab(values)).append(key("items")).append(schema(itemNodes.get(0), values));
        } else if (itemNodes.size() > 1) {
            List<String> out = new ArrayList<>();
            for (Protocol.PBsonSchema i : itemNodes) {
                out.add(schema(i, values + 1));
            }
            b.append(tab(values)).append(key("items")).append("[\n").append(String.join(",\n", out)).append("\n").append(tab(values)).append("]");
        }
        return b.append("\n").append(tab(level)).append("}").toString();
    }

    /** {@code renderObjectType}: an object's (or the schema root's) members, at {@code level}. */
    private static String objectBody(Protocol.PBsonSchema node, int level) {
        StringBuilder b = new StringBuilder();
        if (node.title() != null) {
            b.append(",\n").append(tab(level)).append(key("title")).append(quoted(node.title()));
        }
        description(b, node, level);
        if (!node.properties().isEmpty()) {
            List<String> out = new ArrayList<>();
            for (Map.Entry<String, Protocol.PBsonSchema> p : node.properties()) {
                out.add(tab(level + 1) + key(p.getKey()) + schema(p.getValue(), level + 1));
            }
            b.append(",\n").append(tab(level)).append(key("properties")).append("{\n").append(String.join(",\n", out)).append("\n")
                    .append(tab(level)).append("}");
        }
        if (!node.required().isEmpty()) {
            List<String> out = new ArrayList<>();
            for (String r : node.required()) {
                out.add("\"" + r + "\"");
            }
            String at = tab(level + 1);
            b.append(",\n").append(tab(level)).append(key("required")).append("[")
                    .append("\n").append(at).append(String.join(",\n" + at, out)).append("\n")
                    .append(tab(level)).append("]");
        }
        b.append(",\n").append(tab(level)).append(key("additionalProperties"))
                .append(Boolean.TRUE.equals(node.additionalPropertiesAllowed()) ? "true" : "false");
        return b.toString();
    }

    private static String quoted(String s) {
        return "\"" + s + "\"";
    }

    private static String key(String s) {
        return quoted(s) + ": ";
    }

    // ---------------------------------------------------------------------
    // Class mapping and connection
    // ---------------------------------------------------------------------

    /** The class mapping's body, from its {@code :} on. */
    static String classMapping(Protocol.PClassMappingMongoDb cm) {
        StringBuilder b = new StringBuilder(": MongoDB\n").append(TAB).append("{\n");
        b.append(tab(2)).append("~mainCollection [").append(cm.storePath()).append("] ")
                .append(Composing.convertIdentifier(cm.mainCollectionName())).append("\n");
        if (cm.bindingPath() != null) {
            b.append(tab(2)).append("~binding ").append(cm.bindingPath()).append("\n");
        }
        return b.append(TAB).append("}").toString();
    }

    /** The connection's body at the context's indentation {@code i}. */
    static String connection(Protocol.PMongoDbConnection c, String i) {
        String store = c.element();
        if (store == null) {
            // upstream prints 'store: null;', which does not read back as the same connection
            throw Composing.refused("a MongoDBConnection with no store (upstream prints the word 'null')");
        }
        List<String> urls = new ArrayList<>();
        for (Protocol.PMongoServerUrl u : c.serverUrls()) {
            urls.add(u.baseUrl() + ":" + u.port());
        }
        return i + "{\n"
                + i + TAB + "database: " + c.databaseName() + ";\n"
                + i + TAB + "store: " + store + ";\n"
                + i + TAB + "serverURLs: [" + String.join(", ", urls) + "];\n"
                + i + TAB + "authentication: " + AuthenticationComposer.authentication(c.auth(), 1, i) + ";\n"
                + i + "}";
    }
}
