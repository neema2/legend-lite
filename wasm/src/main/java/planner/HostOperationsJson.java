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

    private static HostOperation operation(Json.Obj o) {
        String op = string(o, "op");
        return switch (op) {
            case "relation" -> new HostOperation.MakeRelation(kind(string(o, "kind")), name(object(o, "target")),
                    bool(o, "replace"), stringOrNull(o, "selectList"), conversions(o), source(object(o, "from")));
            case "insert" -> new HostOperation.Insert(name(object(o, "target")), columns(o), source(object(o, "from")));
            case "drop" -> new HostOperation.Drop(kind(string(o, "kind")), name(object(o, "target")));
            case "schema" -> new HostOperation.MakeSchema(stringOrNull(o, "database"), string(o, "name"));
            case "dropSchema" -> new HostOperation.DropSchema(stringOrNull(o, "database"), string(o, "name"));
            case "count" -> new HostOperation.Count(source(object(o, "from")));
            case "secret" -> {
                // credentials by name (B3, 2026-11-11): an operation names the secret's fields, never their values,
                // which the host binds from its own store
                if (o.has("credentials")) {
                    throw new IllegalArgumentException("a secret's operation names its fields, never their values: "
                            + "send {name, scope, fields}, the values bound from the host's own store");
                }
                yield new HostOperation.Secret(string(o, "name"), string(o, "scope"), stringList(o, "fields"));
            }
            case "dropSecret" -> new HostOperation.DropSecret(string(o, "name"));
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

    private static HostOperation.Kind kind(String kind) {
        return switch (kind) {
            case "table" -> HostOperation.Kind.TABLE;
            case "view" -> HostOperation.Kind.VIEW;
            default -> throw new IllegalArgumentException("a relation's kind is table or view, not " + kind);
        };
    }

    private static HostOperation.Name name(Json.Obj o) {
        return new HostOperation.Name(stringOrNull(o, "database"), stringOrNull(o, "schema"), string(o, "name"),
                bool(o, "declared"));
    }

    private static HostOperation.Source source(Json.Obj o) {
        if (o.has("relation")) {
            return new HostOperation.Source.Relation(name(object(o, "relation")));
        }
        if (o.has("file")) {
            Json.Obj f = object(o, "file");
            String format = string(f, "format");
            return new HostOperation.Source.File(string(f, "at"), switch (format) {
                case "csv" -> HostOperation.FileFormat.CSV;
                case "csvText" -> HostOperation.FileFormat.CSV_TEXT;
                case "parquet" -> HostOperation.FileFormat.PARQUET;
                case "json" -> HostOperation.FileFormat.JSON;
                default -> throw new IllegalArgumentException("a file is read as csv, csvText, parquet or json, not " + format);
            });
        }
        if (o.has("remote")) {
            Json.Obj r = object(o, "remote");
            String format = string(r, "format");
            return new HostOperation.Source.Remote(string(r, "url"), switch (format) {
                case "csv" -> HostOperation.RemoteFormat.CSV;
                case "parquet" -> HostOperation.RemoteFormat.PARQUET;
                case "iceberg" -> HostOperation.RemoteFormat.ICEBERG;
                default -> throw new IllegalArgumentException("a remote file is csv, parquet or iceberg, not " + format);
            });
        }
        if (o.has("query")) {
            return new HostOperation.Source.Query(string(o, "query"));
        }
        throw new IllegalArgumentException("a source is a relation, a file, a remote file or a query");
    }

    private static List<HostOperation.Conversion> conversions(Json.Obj o) {
        List<HostOperation.Conversion> out = new ArrayList<>();
        if (!o.has("replacing")) {
            return out;
        }
        if (!(o.get("replacing") instanceof Json.Arr arr)) {
            throw new IllegalArgumentException("a relation's replacing is an array");
        }
        for (Json.Node n : arr.items()) {
            Json.Obj c = object(n, "a conversion");
            out.add(new HostOperation.Conversion(string(c, "column"), string(c, "sql")));
        }
        return out;
    }

    private static List<HostOperation.Column> columns(Json.Obj o) {
        if (!(o.has("columns") && o.get("columns") instanceof Json.Arr arr)) {
            throw new IllegalArgumentException("rows put in name their columns, an array");
        }
        List<HostOperation.Column> out = new ArrayList<>();
        for (Json.Node n : arr.items()) {
            Json.Obj c = object(n, "a column");
            out.add(new HostOperation.Column(string(c, "target"), string(c, "from")));
        }
        return out;
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
