// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.sql.dialect;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * DuckDB's host operations ({@link HostOperations}).
 *
 * <p>A project's own database (C5) is an in-memory database ATTACHed to the host's -- a catalog of its own, holding
 * the model's schemas as declared -- and a connection points at it with USE, which is that connection's alone (ATTACH
 * is the instance's). DETACH drops it with everything in it. The corpus's workspaces are the same three statements
 * (spec's {@code DuckWorkspaces}, the session-topology ruling of 2026-08-14), and DuckDB-WASM reads them as DuckDB
 * does (measured 2026-10-13: DuckDB 1.5.5 and DuckDB-WASM's 1.5.4).
 *
 * <p>The named operations (D5) are the statements the tab and Python's host spelled themselves before, each as it was:
 * DuckDB's readers (read_csv with its sniffer and a header, read_parquet, read_json with its records' keys as columns,
 * read_csv_auto and iceberg_scan for a remote file), `CREATE ... AS SELECT`, `SELECT * REPLACE`, its secrets, its
 * extensions' settings, and its catalog functions (duckdb_tables, duckdb_views, duckdb_schemas, the Iceberg extension's
 * metadata). A model's declared name is spelled as its seed made it ({@link DuckDb#physicalName}); every other name is
 * quoted; every value is a literal.
 */
final class DuckDbHostOperations implements HostOperations {

    static final DuckDbHostOperations INSTANCE = new DuckDbHostOperations();

    private static final DuckDb DIALECT = new DuckDb();

    /** S3 credentials' names as a secret takes them; USE_SSL a boolean, every other a string. */
    private static final Set<String> CREDENTIALS = Set.of("KEY_ID", "SECRET", "SESSION_TOKEN", "REGION", "ENDPOINT",
            "URL_STYLE", "USE_SSL");

    private DuckDbHostOperations() {
    }

    @Override
    public List<String> openDatabase(String name) {
        return List.of("ATTACH '" + DuckDb.IN_MEMORY + "' AS " + quoted(name));
    }

    @Override
    public List<String> useDatabase(String name) {
        return List.of("USE " + quoted(name));
    }

    @Override
    public List<String> closeDatabase(String name) {
        return List.of("DETACH DATABASE IF EXISTS " + quoted(name));
    }

    @Override
    public List<String> statements(HostOperation operation) {
        return switch (operation) {
            case HostOperation.MakeRelation m -> List.of("CREATE " + (m.replace() ? "OR REPLACE " : "") + kind(m.kind())
                    + " " + name(m.target()) + " AS SELECT " + columns(m) + " FROM " + source(m.from()));
            case HostOperation.Insert i -> List.of("INSERT INTO " + name(i.target()) + " ("
                    + String.join(", ", i.columns().stream().map(c -> DIALECT.physicalName(c.target())).toList())
                    + ") SELECT " + String.join(", ", i.columns().stream().map(c -> quoted(c.from())).toList())
                    + " FROM " + source(i.from()));
            case HostOperation.Upsert u -> List.of(upsert(u));
            case HostOperation.Bound b -> List.of(bound(b));
            case HostOperation.Drop d -> List.of("DROP " + kind(d.kind()) + " IF EXISTS " + name(d.target()));
            case HostOperation.MakeSchema m -> List.of("CREATE SCHEMA IF NOT EXISTS " + schema(m.database(), m.name()));
            case HostOperation.DropSchema d -> List.of("DROP SCHEMA IF EXISTS " + schema(d.database(), d.name()));
            case HostOperation.Count c -> List.of("SELECT count(*) AS n FROM " + source(c.from()));
            case HostOperation.Secret s -> secret(s);
            case HostOperation.Extensions e -> {
                List<String> out = new ArrayList<>();
                for (String name : e.names()) {
                    String extension = extensionName(name);
                    out.add("INSTALL " + extension);
                    out.add("LOAD " + extension);
                }
                yield out;
            }
            case HostOperation.ExtensionRepository r -> List.of(
                    "SET custom_extension_repository = " + literal(r.url()),
                    "SET autoinstall_extension_repository = " + literal(r.url()));
            case HostOperation.ExtensionDirectory d -> List.of(
                    "SET extension_directory = " + literal(d.folder()),
                    "SET custom_extension_repository = " + literal(d.folder()),
                    "SET autoinstall_known_extensions = false");
            case HostOperation.FileSearchPath f -> List.of("SET file_search_path = " + literal(f.folder()));
            case HostOperation.Transaction t -> List.of(switch (t.step()) {
                case BEGIN -> "BEGIN TRANSACTION";
                case COMMIT -> "COMMIT";
                case ROLLBACK -> "ROLLBACK";
            });
            case HostOperation.NameTaken t -> List.of("SELECT"
                    + " (SELECT count(*) FROM duckdb_tables() WHERE database_name IN (current_database(), 'temp')"
                    + " AND lower(table_name) = lower(" + literal(t.name()) + "))"
                    + " + (SELECT count(*) FROM duckdb_views() WHERE NOT internal"
                    + " AND database_name IN (current_database(), 'temp')"
                    + " AND lower(view_name) = lower(" + literal(t.name()) + ")) AS n");
            case HostOperation.RelationExists r -> {
                String where = databaseIs(r.name().database()) + " AND lower(schema_name) = lower("
                        + literal(r.name().schema() == null ? "main" : r.name().schema()) + ")";
                yield List.of("SELECT"
                        + " (SELECT count(*) FROM duckdb_tables() WHERE " + where
                        + " AND lower(table_name) = lower(" + literal(r.name().name()) + "))"
                        + " + (SELECT count(*) FROM duckdb_views() WHERE NOT internal AND " + where
                        + " AND lower(view_name) = lower(" + literal(r.name().name()) + ")) AS n");
            }
            case HostOperation.TablesIn t -> List.of("SELECT count(*) AS n FROM duckdb_tables() WHERE "
                    + databaseIs(t.database()) + " AND lower(schema_name) = lower(" + literal(t.schema()) + ")");
            case HostOperation.SchemaExists e -> List.of("SELECT count(*) AS n FROM duckdb_schemas() WHERE "
                    + databaseIs(e.database()) + " AND lower(schema_name) = lower(" + literal(e.schema()) + ")");
            case HostOperation.CurrentDatabase c -> List.of("SELECT current_database() AS name");
            case HostOperation.Platform p -> List.of("PRAGMA platform");
            case HostOperation.IcebergManifestLists l -> List.of(
                    "SELECT manifest_list AS path FROM iceberg_snapshots(" + literal(l.url()) + ")");
            case HostOperation.IcebergManifests m -> List.of("SELECT DISTINCT manifest_path AS path FROM read_avro(["
                    + String.join(", ", m.lists().stream().map(DuckDbHostOperations::literal).toList()) + "])");
            case HostOperation.IcebergDataFiles f -> List.of(
                    "SELECT file_path AS path FROM iceberg_metadata(" + literal(f.url()) + ")");
        };
    }

    private static String kind(HostOperation.Kind kind) {
        return switch (kind) {
            case TABLE -> "TABLE";
            case VIEW -> "VIEW";
        };
    }

    /** A relation made's columns: the planner's copy select list, every column with conversions in place, or all. */
    private static String columns(HostOperation.MakeRelation m) {
        if (m.selectList() != null) {
            return m.selectList();
        }
        if (m.replacing().isEmpty()) {
            return "*";
        }
        return "* REPLACE (" + String.join(", ",
                m.replacing().stream().map(c -> c.sql() + " AS " + quoted(c.column())).toList()) + ")";
    }

    private static String source(HostOperation.Source source) {
        return switch (source) {
            case HostOperation.Source.Relation r -> name(r.name());
            case HostOperation.Source.File f -> switch (f.format()) {
                // the sniffer takes delimiter, quoting and types; upstream's DataCube asks for a header, as this does
                case CSV -> "read_csv(" + literal(f.at()) + ", AUTO_DETECT=TRUE, HEADER=TRUE)";
                // a declared table's file: every column as text, each converted to its column's type as it goes in
                case CSV_TEXT -> "read_csv(" + literal(f.at()) + ", header = true, all_varchar = true)";
                case PARQUET -> "read_parquet(" + literal(f.at()) + ")";
                // each record's top-level keys its columns; what is nested below them a STRUCT or a LIST, kept
                case JSON -> "read_json(" + literal(f.at()) + ", auto_detect=true)";
            };
            case HostOperation.Source.Remote r -> switch (r.format()) {
                case CSV -> "read_csv_auto(" + literal(r.url()) + ")";
                case PARQUET -> "read_parquet(" + literal(r.url()) + ")";
                case ICEBERG -> "iceberg_scan(" + literal(r.url()) + ")";
            };
            case HostOperation.Source.Query q -> "(" + q.sql() + ")";
        };
    }

    /** A secret for S3, scoped to a source's own path; none when there are no credentials at all. */
    private static List<String> secret(HostOperation.Secret s) {
        List<String> parts = new ArrayList<>();
        parts.add("TYPE S3");
        for (Map.Entry<String, String> c : s.credentials().entrySet()) {
            if (!CREDENTIALS.contains(c.getKey())) {
                throw new IllegalArgumentException("a secret's credential is one of " + new java.util.TreeSet<>(CREDENTIALS)
                        + ", not " + c.getKey());
            }
            parts.add(c.getKey() + " " + ("USE_SSL".equals(c.getKey())
                    ? Boolean.toString(Boolean.parseBoolean(c.getValue())) : literal(c.getValue())));
        }
        if (parts.size() == 1) {
            return List.of();
        }
        parts.add("SCOPE " + literal(s.scope()));
        return List.of("CREATE OR REPLACE SECRET " + quoted(s.name()) + " (" + String.join(", ", parts) + ")");
    }

    /** An extension's name, as INSTALL takes it: a word, never text a host spliced in. */
    private static String extensionName(String name) {
        if (!name.matches("[a-z][a-z0-9_]*")) {
            throw new IllegalArgumentException("an extension's name is a word of lower-case letters, digits and '_', not "
                    + name);
        }
        return name;
    }

    /** A catalog question's database: the one named, or the current one. */
    /** The source's last row of each key (its row order: a table's, as its rows went in), each replacing the target's
     *  row of the key; the key's source columns those its target columns come from. */
    private static String upsert(HostOperation.Upsert u) {
        List<String> by = u.key().stream().map(k -> quoted(u.columns().stream().filter(c -> c.target().equals(k))
                .findFirst().orElseThrow().from())).toList();
        return "INSERT OR REPLACE INTO " + name(u.target()) + " ("
                + String.join(", ", u.columns().stream().map(c -> DIALECT.physicalName(c.target())).toList())
                + ") SELECT " + String.join(", ", u.columns().stream().map(c -> quoted(c.from())).toList())
                + " FROM (SELECT *, row_number() OVER (PARTITION BY " + String.join(", ", by)
                + " ORDER BY rowid DESC) AS \"__latest\" FROM " + name(u.from().name()) + ") WHERE \"__latest\" = 1";
    }

    /** Every row older than the oldest a stream keeps: its latest rows' last, or its latest value less the span. */
    private static String bound(HostOperation.Bound b) {
        String table = name(b.target());
        String column = b.target().declared() ? DIALECT.physicalName(b.column()) : quoted(b.column());
        String oldest = switch (b.keep()) {
            case HostOperation.Bound.Keep.Rows r -> "(SELECT " + column + " FROM " + table + " ORDER BY " + column
                    + " DESC LIMIT 1 OFFSET " + (r.rows() - 1) + ")";
            case HostOperation.Bound.Keep.Within w -> "(SELECT max(" + column + ") - to_milliseconds(" + w.millis()
                    + ") FROM " + table + ")";
        };
        return "DELETE FROM " + table + " WHERE " + column + " < " + oldest;
    }

    private static String databaseIs(@com.legend.base.Nullable String database) {
        return database == null ? "database_name = current_database()" : "database_name = " + literal(database);
    }

    /** A relation's name: a model's declared one as its seed spelled it, any other quoted; its database, schema too. */
    private static String name(HostOperation.Name n) {
        String last = n.declared() ? DIALECT.physicalName(n.name()) : quoted(n.name());
        String schema = n.schema() == null ? null : n.declared() ? DIALECT.physicalName(n.schema()) : quoted(n.schema());
        StringBuilder out = new StringBuilder();
        if (n.database() != null) {
            out.append(quoted(n.database())).append('.');
        }
        if (schema != null) {
            out.append(schema).append('.');
        }
        return out.append(last).toString();
    }

    private static String schema(@com.legend.base.Nullable String database, String name) {
        return (database == null ? "" : quoted(database) + ".") + quoted(name);
    }

    /** A name as DuckDB reads it, quoted: the name is a host's or a person's, never trusted to need none. */
    private static String quoted(String name) {
        return "\"" + name.replace("\"", "\"\"") + "\"";
    }

    /** A value as DuckDB reads it: a string literal. */
    private static String literal(String value) {
        return "'" + value.replace("'", "''") + "'";
    }
}
