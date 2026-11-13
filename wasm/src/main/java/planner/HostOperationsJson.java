// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package planner;

import com.legend.json.Json;
import com.legend.sql.dialect.HostOperation;

import java.util.ArrayList;
import java.util.List;

/**
 * A host's named operations as its JSON writes them (docs/ONE_PLATFORM_PROGRAM_2026_10_12.md D5; engine-client's
 * host-operations.ts and Python's compiler.host_operations write the same), read into {@link HostOperation}s -- each
 * field by its kind, never cast (the tab's WebAssembly traps on a failed cast), and anything else refused by name.
 */
final class HostOperationsJson {

    private HostOperationsJson() {
    }

    /** A JSON array of operations. */
    static List<HostOperation> read(String json) {
        if (!(Json.parse(json) instanceof Json.Arr arr)) {
            throw new IllegalArgumentException("a host's operations are a JSON array");
        }
        List<HostOperation> out = new ArrayList<>();
        for (Json.Node n : arr.items()) {
            out.add(operation(object(n, "an operation")));
        }
        return out;
    }

    /** The keys an operation once carried SQL in, each refused by name (the final audit, 2026-11-12, the user's ruling:
     *  a host operation takes no SQL; a source's {@code query} is refused where a source is read). */
    private static final List<String> SQL_KEYS = List.of("selectList", "replacing");

    private static HostOperation operation(Json.Obj o) {
        for (String key : SQL_KEYS) {
            if (o.has(key)) {
                throw new IllegalArgumentException("a host operation takes no SQL, and " + key + " is SQL: a copy's"
                        + " conversions are written by the platform from its columns' catalog (convert)");
            }
        }
        String op = string(o, "op");
        // each operation closed over its own keys, any other refused by name (the next PR's item 1, "separation made
        // foolproof", 2026-11-13: an unknown key is refused, G7, so no field can ride in one unread)
        java.util.List<String> keys = KEYS.get(op);
        if (keys != null) {
            only(o, "a host's " + op + " operation", keys);
        }
        return switch (op) {
            case "relation" -> new HostOperation.MakeRelation(kind(string(o, "kind")), name(object(o, "target")),
                    bool(o, "replace"), convert(o), source(object(o, "from")));
            // rows put into a declared table come from a person's file: a planned load's (B3: Boundary.fileLoad)
            case "insert" -> throw new IllegalArgumentException("rows put into a declared table are a planned load's"
                    + " (fileLoad), not an operation");
            case "drop" -> new HostOperation.Drop(kind(string(o, "kind")), name(object(o, "target")));
            case "schema" -> new HostOperation.MakeSchema(stringOrNull(o, "database"), string(o, "name"));
            case "dropSchema" -> new HostOperation.DropSchema(stringOrNull(o, "database"), string(o, "name"));
            case "count" -> new HostOperation.Count(source(object(o, "from")));
            // a remote file's secret is a step of its planned mount, its fields bound from the host's store (B3's
            // Q3, 2026-11-11: Boundary.remoteMount), never an operation
            case "secret" -> throw new IllegalArgumentException("a remote file's secret is made by its planned mount"
                    + " (remoteMount), not an operation");
            case "dropSecret" -> new HostOperation.DropSecret(string(o, "name"));
            case "resetVariable" -> new HostOperation.ResetVariable(string(o, "held"));
            case "extensions" -> new HostOperation.Extensions(stringList(o, "names"));
            case "extensionRepository" -> new HostOperation.ExtensionRepository(string(o, "url"));
            case "extensionDirectory" -> new HostOperation.ExtensionDirectory(string(o, "folder"));
            case "fileSearchPath" -> new HostOperation.FileSearchPath(string(o, "folder"));
            case "transaction" -> new HostOperation.Transaction(switch (string(o, "step")) {
                case "begin" -> HostOperation.Transaction.Step.BEGIN;
                case "commit" -> HostOperation.Transaction.Step.COMMIT;
                case "rollback" -> HostOperation.Transaction.Step.ROLLBACK;
                default -> throw new IllegalArgumentException("a transaction's step is begin, commit or rollback, not "
                        + string(o, "step"));
            });
            case "nameTaken" -> new HostOperation.NameTaken(string(o, "name"));
            case "relationExists" -> new HostOperation.RelationExists(name(object(o, "name")));
            case "tablesIn" -> new HostOperation.TablesIn(stringOrNull(o, "database"), string(o, "schema"));
            case "schemaExists" -> new HostOperation.SchemaExists(stringOrNull(o, "database"), string(o, "schema"));
            case "currentDatabase" -> new HostOperation.CurrentDatabase();
            case "platform" -> new HostOperation.Platform();
            case "icebergManifestLists" -> new HostOperation.IcebergManifestLists(string(o, "url"));
            case "icebergManifests" -> new HostOperation.IcebergManifests(stringList(o, "lists"));
            case "icebergDataFiles" -> new HostOperation.IcebergDataFiles(string(o, "url"));
            default -> throw new IllegalArgumentException("a host's operation is not one this platform names: " + op);
        };
    }

    /** Each operation's keys, its {@code op} among them: what {@link #operation} reads of it, and nothing more. An
     *  operation refused by name (insert, secret) or unnamed is refused before its keys are read. */
    private static final java.util.Map<String, java.util.List<String>> KEYS = java.util.Map.ofEntries(
            java.util.Map.entry("relation", List.of("op", "kind", "target", "replace", "convert", "from")),
            java.util.Map.entry("drop", List.of("op", "kind", "target")),
            java.util.Map.entry("schema", List.of("op", "database", "name")),
            java.util.Map.entry("dropSchema", List.of("op", "database", "name")),
            java.util.Map.entry("count", List.of("op", "from")),
            java.util.Map.entry("dropSecret", List.of("op", "name")),
            java.util.Map.entry("resetVariable", List.of("op", "held")),
            java.util.Map.entry("extensions", List.of("op", "names")),
            java.util.Map.entry("extensionRepository", List.of("op", "url")),
            java.util.Map.entry("extensionDirectory", List.of("op", "folder")),
            java.util.Map.entry("fileSearchPath", List.of("op", "folder")),
            java.util.Map.entry("transaction", List.of("op", "step")),
            java.util.Map.entry("nameTaken", List.of("op", "name")),
            java.util.Map.entry("relationExists", List.of("op", "name")),
            java.util.Map.entry("tablesIn", List.of("op", "database", "schema")),
            java.util.Map.entry("schemaExists", List.of("op", "database", "schema")),
            java.util.Map.entry("currentDatabase", List.of("op")),
            java.util.Map.entry("platform", List.of("op")),
            java.util.Map.entry("icebergManifestLists", List.of("op", "url")),
            java.util.Map.entry("icebergManifests", List.of("op", "lists")),
            java.util.Map.entry("icebergDataFiles", List.of("op", "url")));

    /** {@code o}'s keys among {@code keys}, any other refused by name ({@code what} names the object). */
    private static void only(Json.Obj o, String what, List<String> keys) {
        for (String key : o.fields().keySet()) {
            if (!keys.contains(key)) {
                throw new IllegalArgumentException(what + " has no key " + key + " (names and fields only, never a"
                        + " value)");
            }
        }
    }

    private static void only(Json.Obj o, String what, String... keys) {
        only(o, what, List.of(keys));
    }

    private static HostOperation.Kind kind(String kind) {
        return switch (kind) {
            case "table" -> HostOperation.Kind.TABLE;
            case "view" -> HostOperation.Kind.VIEW;
            default -> throw new IllegalArgumentException("a relation's kind is table or view, not " + kind);
        };
    }

    private static HostOperation.Name name(Json.Obj o) {
        only(o, "a relation's name", "database", "schema", "name", "declared");
        return new HostOperation.Name(stringOrNull(o, "database"), stringOrNull(o, "schema"), string(o, "name"),
                bool(o, "declared"));
    }

    private static HostOperation.Source source(Json.Obj o) {
        if (o.has("file")) {
            // a host's own file is read by a planned load (B3, 2026-11-11: Boundary.fileLoad, run by the hosts'
            // runner), never by an operation naming its path
            throw new IllegalArgumentException("a host's file is read by a planned load (fileLoad), not an operation");
        }
        if (o.has("remote")) {
            // a remote file is mounted by a planned load (B3's Q3, 2026-11-11: Boundary.remoteMount), its bearer URL
            // and its keys bound from the host's store, never written in an operation
            throw new IllegalArgumentException("a remote file is mounted by a planned load (remoteMount), not an"
                    + " operation");
        }
        if (o.has("query")) {
            // a host hands an operation no SQL (the final audit, 2026-11-12, the user's ruling): a pull is planned
            throw new IllegalArgumentException("a query is planned from its pull (snapCopy, or a Pure query's plan),"
                    + " never handed to a host operation");
        }
        if (o.has("relation")) {
            // the refusals above first, each by its own name; then the relation alone (2026-11-13: until then a
            // relation beside any other key was read and the other key dropped)
            only(o, "a source", "relation");
            return new HostOperation.Source.Relation(name(object(o, "relation")));
        }
        throw new IllegalArgumentException("a source is a relation");
    }

    /** A copy's columns as its source's catalog answers them -- {@code {"name", "dataType", "logicalType"?,
     *  "precision"?, "scale"?, "notNull"?}}, the rows {@code Boundary.tableModel} takes -- whose conversions the platform
     *  writes ({@link HostOperation.MakeRelation#convert}); none when absent. */
    private static List<com.legend.sql.dialect.CatalogModel.Column> convert(Json.Obj o) {
        List<com.legend.sql.dialect.CatalogModel.Column> out = new ArrayList<>();
        if (!o.has("convert")) {
            return out;
        }
        if (!(o.get("convert") instanceof Json.Arr arr)) {
            throw new IllegalArgumentException("a relation's convert is an array of catalog columns");
        }
        for (Json.Node n : arr.items()) {
            Json.Obj c = object(n, "a catalog column");
            only(c, "a catalog column", "name", "dataType", "logicalType", "precision", "scale", "notNull");
            out.add(new com.legend.sql.dialect.CatalogModel.Column(string(c, "name"), string(c, "dataType"),
                    stringOrNull(c, "logicalType"), intOrNull(c, "precision"), intOrNull(c, "scale"),
                    bool(c, "notNull")));
        }
        return out;
    }

    private static @com.legend.base.Nullable Integer intOrNull(Json.Obj o, String field) {
        if (!o.has(field) || o.get(field) instanceof Json.Null) {
            return null;
        }
        if (!(o.get(field) instanceof Json.Num n) || !n.isInteger()) {
            throw new IllegalArgumentException("an operation's " + field + " is a whole number");
        }
        return Integer.valueOf(Math.toIntExact(n.longValue()));
    }

    private static List<String> stringList(Json.Obj o, String field) {
        if (!(o.has(field) && o.get(field) instanceof Json.Arr arr)) {
            throw new IllegalArgumentException("an operation's " + field + " is an array of strings");
        }
        List<String> out = new ArrayList<>();
        for (Json.Node n : arr.items()) {
            if (!(n instanceof Json.Str s)) {
                throw new IllegalArgumentException("an operation's " + field + " is an array of strings");
            }
            out.add(s.value());
        }
        return out;
    }

    private static Json.Obj object(Json.Node n, String what) {
        if (!(n instanceof Json.Obj o)) {
            throw new IllegalArgumentException(what + " is a JSON object");
        }
        return o;
    }

    private static Json.Obj object(Json.Obj o, String field) {
        if (!o.has(field)) {
            throw new IllegalArgumentException("an operation names no " + field);
        }
        return object(o.get(field), "an operation's " + field);
    }

    private static String string(Json.Obj o, String field) {
        if (!(o.has(field) && o.get(field) instanceof Json.Str s)) {
            throw new IllegalArgumentException("an operation's " + field + " is a string");
        }
        return s.value();
    }

    private static @com.legend.base.Nullable String stringOrNull(Json.Obj o, String field) {
        if (!o.has(field) || o.get(field) instanceof Json.Null) {
            return null;
        }
        return string(o, field);
    }

    private static boolean bool(Json.Obj o, String field) {
        if (!o.has(field)) {
            return false;
        }
        if (!(o.get(field) instanceof Json.Bool b)) {
            throw new IllegalArgumentException("an operation's " + field + " is true or false");
        }
        return b.value();
    }
}
