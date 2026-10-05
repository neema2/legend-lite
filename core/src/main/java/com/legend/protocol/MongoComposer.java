// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.protocol;

import com.legend.json.Json;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static com.legend.protocol.Composing.TAB;
import static com.legend.protocol.Composing.elementPath;
import static com.legend.protocol.Composing.objOr;
import static com.legend.protocol.Composing.objs;
import static com.legend.protocol.Composing.str;
import static com.legend.protocol.Composing.tab;

/**
 * {@code ###MongoDB}'s database store, its class mapping and its connection as upstream prints them
 * ({@code MongoDBGrammarComposerExtension}, {@code MongoDBSchemaComposer}, {@code MongoDBMappingComposer},
 * and the JSON-schema printer {@code BaseTypeVisitorImpl} a collection's validator goes through).
 */
final class MongoComposer {

    /** A schema node kind's printer, given the node and its indent level. */
    private interface SchemaPrinter {
        String print(Json.Obj node, int level);
    }

    /** The scalar BSON types: their {@code bsonType} and the numeric bounds each prints, in order. */
    private record Scalar(String bsonType, List<String> bounds) {
    }

    private static final Map<String, Scalar> SCALARS = Map.of(
            "stringType", new Scalar("string", List.of("minLength", "maxLength")),
            "intType", new Scalar("int", List.of("minimum", "maximum")),
            "longType", new Scalar("long", List.of("minimum", "maximum")),
            "decimalType", new Scalar("decimal", List.of("minimum", "maximum")),
            "boolType", new Scalar("bool", List.of()));

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

    static String store(Json.Obj store) {
        StringBuilder b = new StringBuilder("Database ").append(elementPath(store)).append("\n(\n");
        for (Json.Obj c : objs(store, "collections")) {
            Json.Obj validator = c.getObj("validator");
            b.append(TAB).append("Collection ").append(DatabaseComposer.convertIdentifierDoubleQuoted(c.getString("name"))).append("\n")
                    .append(TAB).append("(\n")
                    .append(tab(2)).append("validationLevel: ").append(validator.getString("validationLevel")).append(";\n")
                    .append(tab(2)).append("validationAction: ").append(validator.getString("validationAction")).append(";\n")
                    .append(tab(2)).append("jsonSchema: ").append(validatorExpression(validator.getObj("validatorExpression"), 2).trim()).append(";\n")
                    .append(TAB).append(")\n");
        }
        return b.append(")").toString();
    }

    /** {@code MongoDBOperationElementVisitorImpl.visit(JsonSchemaExpression)}: the schema, at {@code level}. */
    private static String validatorExpression(Json.Obj expression, int level) {
        if (!"jsonSchemaExpression".equals(Composing.type(expression))) {
            throw Composing.refused("no composer rule for a MongoDB validator expression of _type '" + Composing.type(expression) + "'");
        }
        return schema(expression.getObj("schemaExpression"), level);
    }

    /** {@code BaseTypeVisitorImpl}: one schema node at {@code level}. */
    private static String schema(Json.Obj node, int level) {
        String type = Composing.type(node);
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

    private static String scalar(Json.Obj node, Scalar scalar, int level) {
        StringBuilder b = new StringBuilder("{\n").append(tab(level + 1)).append(key("bsonType")).append(quoted(scalar.bsonType()));
        description(b, node, level + 1);
        for (String bound : scalar.bounds()) {
            bound(b, node, bound, level + 1);
        }
        return b.append("\n").append(tab(level)).append("}").toString();
    }

    private static void description(StringBuilder b, Json.Obj node, int level) {
        String description = str(node, "description");
        if (description != null) {
            b.append(",\n").append(tab(level)).append(key("description")).append(quoted(description));
        }
    }

    /** A numeric bound, printed as Java prints the boxed integer. */
    private static void bound(StringBuilder b, Json.Obj node, String name, int level) {
        Json.Node v = Composing.value(node, name);
        if (v == null) {
            return;
        }
        if (!(v instanceof Json.Num n) || !n.isInteger()) {
            throw Composing.refused("a MongoDB schema bound '" + name + "' that is not an integer: " + v);
        }
        b.append(",\n").append(tab(level)).append(key(name)).append(n.longValue());
    }

    private static String array(Json.Obj node, int level) {
        int values = level + 1;
        StringBuilder b = new StringBuilder("{\n").append(tab(values)).append(key("bsonType")).append(quoted("array")).append(",\n");
        String description = str(node, "description");
        if (description != null) {
            b.append(tab(values)).append(key("description")).append(quoted(description)).append(",\n");
        }
        List<Json.Obj> itemNodes = objs(node, "items");
        if (itemNodes.size() == 1) {
            b.append(tab(values)).append(key("items")).append(schema(itemNodes.get(0), values));
        } else if (itemNodes.size() > 1) {
            List<String> out = new ArrayList<>();
            for (Json.Obj i : itemNodes) {
                out.add(schema(i, values + 1));
            }
            b.append(tab(values)).append(key("items")).append("[\n").append(String.join(",\n", out)).append("\n").append(tab(values)).append("]");
        }
        bound(b, node, "minItems", values);
        bound(b, node, "maxItems", values);
        if (node.getBoolOr("uniqueItems", false)) {
            b.append(",\n").append(tab(values)).append(key("uniqueItems")).append("true");
        }
        return b.append("\n").append(tab(level)).append("}").toString();
    }

    /** {@code renderObjectType}: an object's (or the schema root's) members, at {@code level}. */
    private static String objectBody(Json.Obj node, int level) {
        StringBuilder b = new StringBuilder();
        String title = str(node, "title");
        if (title != null) {
            b.append(",\n").append(tab(level)).append(key("title")).append(quoted(title));
        }
        description(b, node, level);
        List<Json.Obj> properties = objs(node, "properties");
        if (!properties.isEmpty()) {
            List<String> out = new ArrayList<>();
            for (Json.Obj p : properties) {
                out.add(tab(level + 1) + key(p.getString("key")) + schema(p.getObj("value"), level + 1));
            }
            b.append(",\n").append(tab(level)).append(key("properties")).append("{\n").append(String.join(",\n", out)).append("\n")
                    .append(tab(level)).append("}");
        }
        bound(b, node, "minProperties", level);
        bound(b, node, "maxProperties", level);
        List<String> required = node.getStringArrayOr("required", List.of());
        if (!required.isEmpty()) {
            List<String> out = new ArrayList<>();
            for (String r : required) {
                out.add("\"" + r + "\"");
            }
            String at = tab(level + 1);
            b.append(",\n").append(tab(level)).append(key("required")).append("[")
                    .append("\n").append(at).append(String.join(",\n" + at, out)).append("\n")
                    .append(tab(level)).append("]");
        }
        b.append(",\n").append(tab(level)).append(key("additionalProperties"));
        if (node.getBoolOr("additionalPropertiesAllowed", false)) {
            if (Composing.value(node, "additionalProperties") != null) {
                throw Composing.refused("a MongoDB schema object with an additional-properties schema (upstream prints 'not supported')");
            }
            b.append("true");
        } else {
            b.append("false");
        }
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
    static String classMapping(Json.Obj cm) {
        StringBuilder b = new StringBuilder(": MongoDB\n").append(TAB).append("{\n");
        String collection = str(cm, "mainCollectionName");
        String store = str(cm, "storePath");
        if (collection != null && store != null) {
            b.append(tab(2)).append("~mainCollection [").append(store).append("] ").append(Composing.convertIdentifier(collection)).append("\n");
        }
        String binding = str(cm, "bindingPath");
        if (binding != null) {
            b.append(tab(2)).append("~binding ").append(binding).append("\n");
        }
        return b.append(TAB).append("}").toString();
    }

    /** The connection's body at the context's indentation {@code i}. */
    static String connection(Json.Obj c, String i) {
        String store = str(c, "element");
        if (store == null) {
            // upstream prints 'store: null;', which does not read back as the same connection
            throw Composing.refused("a MongoDBConnection with no store (upstream prints the word 'null')");
        }
        Json.Obj source = c.getObj("dataSourceSpecification");
        List<String> urls = new ArrayList<>();
        for (Json.Obj u : objs(source, "serverURLs")) {
            urls.add(u.getString("baseUrl") + ":" + RelationalConnectionComposer.raw(u.get("port")));
        }
        Json.Obj auth = objOr(c, "authenticationSpecification");
        if (auth == null) {
            throw Composing.refused("a MongoDBConnection with no authentication specification (upstream cannot print it)");
        }
        return i + "{\n"
                + i + TAB + "database: " + source.getString("databaseName") + ";\n"
                + i + TAB + "store: " + store + ";\n"
                + i + TAB + "serverURLs: [" + String.join(", ", urls) + "];\n"
                + i + TAB + "authentication: " + AuthenticationComposer.authentication(auth, 1, i) + ";\n"
                + i + "}";
    }
}
