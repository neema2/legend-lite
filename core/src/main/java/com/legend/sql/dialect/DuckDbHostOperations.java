// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.sql.dialect;

import java.util.ArrayList;
import java.util.List;
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
 * quoted; every value is a literal but a secret's, every one of which is bound (B3, 2026-11-11).
 */
final class DuckDbHostOperations implements HostOperations {

    static final DuckDbHostOperations INSTANCE = new DuckDbHostOperations();

    /** A name as DuckDB stores it (one dialect per use, 2026-11-09: a dialect keeps state while it writes, so none is
     *  shared statically). */
    private static String physicalName(String name) {
        return new DuckDb().physicalName(name);
    }

    /** S3 credentials' names as a secret takes them (each bound: USE_SSL as a boolean or its text, every other a
     *  string). */
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
    public TransactionStatements transaction() {
        return new TransactionStatements(statements(new HostOperation.Transaction(HostOperation.Transaction.Step.BEGIN)),
                statements(new HostOperation.Transaction(HostOperation.Transaction.Step.COMMIT)),
                statements(new HostOperation.Transaction(HostOperation.Transaction.Step.ROLLBACK)));
    }

    @Override
    public List<String> statements(HostOperation operation) {
        return switch (operation) {
            case HostOperation.MakeRelation m -> List.of("CREATE " + (m.replace() ? "OR REPLACE " : "") + kind(m.kind())
                    + " " + name(m.target()) + " AS SELECT " + columns(m) + " FROM " + source(m.from()));
            case HostOperation.Insert i -> List.of("INSERT INTO " + name(i.target()) + " ("
                    + String.join(", ", i.columns().stream().map(c -> physicalName(c.target())).toList())
                    + ") SELECT " + String.join(", ", i.columns().stream().map(c -> quoted(c.from())).toList())
                    + " FROM " + source(i.from()));
            // the database makes every row: range(count)'s i, each column its formula of i (SYNTHESIZE, register S303)
            case HostOperation.Synthesize g -> List.of("INSERT INTO " + name(g.target()) + " ("
                    + String.join(", ", g.columns().stream().map(f -> physicalName(f.column())).toList())
                    + ") SELECT " + String.join(", ", g.columns().stream().map(DuckDbHostOperations::formula).toList())
                    + " FROM range(" + g.count() + ") AS t(i)");
            case HostOperation.Drop d -> List.of("DROP " + kind(d.kind()) + " IF EXISTS " + name(d.target()));
            case HostOperation.MakeSchema m -> List.of("CREATE SCHEMA IF NOT EXISTS " + schema(m.database(), m.name()));
            case HostOperation.DropSchema d -> List.of("DROP SCHEMA IF EXISTS " + schema(d.database(), d.name()));
            case HostOperation.Count c -> List.of("SELECT count(*) AS n FROM " + source(c.from()));
            case HostOperation.Secret s -> secret(s);
            case HostOperation.DropSecret d -> List.of("DROP TEMPORARY SECRET IF EXISTS " + quoted(d.name()));
            case HostOperation.SetVariable v -> List.of("SET VARIABLE " + HostOperation.variable(v.held()) + " = ?");
            case HostOperation.ResetVariable v -> List.of("RESET VARIABLE " + HostOperation.variable(v.held()));
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

    /** A column's formula of the row's number {@code i} ({@link HostOperation.Formula}), in DuckDB's arithmetic: BIGINT
     *  {@code %} and {@code *} (every product below 2^63 for fewer than 2^32 rows), a list indexed from 1, a date plus
     *  days, a timestamp plus an interval of seconds. */
    private static String formula(HostOperation.Formula f) {
        return switch (f) {
            case HostOperation.Formula.Cycle c -> "([" + String.join(", ", c.values().stream()
                    .map(v -> v.number() ? v.text() : literal(v.text())).toList()) + "])[" + turn(c.every(),
                    c.values().size()) + " + 1]";
            case HostOperation.Formula.Numbered n -> literal(n.prefix()) + " || CAST(" + turn(n.every(), n.k())
                    + " + 1 AS VARCHAR)";
            case HostOperation.Formula.Spread r -> {
                String step = java.math.BigDecimal.ONE.movePointLeft(r.scale()).toPlainString();
                String lo = r.lo().toPlainString();
                String n = "(CAST(round((" + r.hi().toPlainString() + " - " + lo + ") / " + step + ") AS BIGINT) + 1)";
                yield "(" + lo + " + (" + spread(r.multiplier()) + " % " + n + ") * " + step + ")";
            }
            case HostOperation.Formula.Days d -> {
                String days = "(DATE " + literal(d.to()) + " - DATE " + literal(d.from()) + " + 1)";
                yield d.seconds()
                        ? "(CAST(DATE " + literal(d.from()) + " AS TIMESTAMP) + to_seconds(" + spread(d.multiplier())
                                + " % (" + days + " * 86400)))"
                        : "(DATE " + literal(d.from()) + " + CAST(" + spread(d.multiplier()) + " % " + days
                                + " AS INTEGER))";
            }
            case HostOperation.Formula.Alternate a -> "(" + turn(a.every(), 2) + " = 0)";
        };
    }

    /** A turn column's index: the row number's digit of radix {@code k} worth {@code every}, {@code (i // every) % k}
     *  ({@code //} DuckDB's integer division). */
    private static String turn(long every, int k) {
        return every == 1 ? "(i % " + k + ")" : "((i // " + every + ") % " + k + ")";
    }

    /** A spread column's offset before its range: the row number times its multiplier, modulo the prime. */
    private static String spread(long multiplier) {
        return "((i * " + multiplier + ") % " + HostOperation.Formula.MODULUS + ")";
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
            // a host's own file, its ? bound by the host's runner to what DuckDB reads it by (B3: never written here)
            case HostOperation.Source.HostFile f -> switch (f.format()) {
                // the sniffer takes delimiter, quoting and types; upstream's DataCube asks for a header, as this does
                case CSV -> "read_csv(?, AUTO_DETECT=TRUE, HEADER=TRUE)";
                // a declared table's file: every column as text, each converted to its column's type as it goes in
                case CSV_TEXT -> "read_csv(?, header = true, all_varchar = true)";
                case PARQUET -> "read_parquet(?)";
                // each record's top-level keys its columns; what is nested below them a STRUCT or a LIST, kept
                case JSON -> "read_json(?, auto_detect=true)";
            };
            case HostOperation.Source.Remote r -> {
                // the address as it is, or the session variable a bearer URL is bound to (B3's Q3: measured on
                // duckdb-python 1.5.5 and DuckDB-WASM, the view's text then holds the variable's name alone)
                String at = switch (r.location()) {
                    case HostOperation.Location.Url u -> literal(u.url());
                    case HostOperation.Location.Variable v -> "getvariable(" + literal(HostOperation.variable(v.held())) + ")";
                };
                yield switch (r.format()) {
                    case CSV -> "read_csv_auto(" + at + ")";
                    case PARQUET -> "read_parquet(" + at + ")";
                    case ICEBERG -> "iceberg_scan(" + at + ")";
                };
            }
        };
    }

    /** A secret for S3, scoped to a source's own path, its values every one a bound {@code ?} in
     *  {@link HostOperation.Secret#binds()}'s order -- each field's, then the scope -- so no value is in the statement
     *  (B3, 2026-11-11: until then each a literal, the planned SQL holding the secret); none when it names no field.
     *  TEMPORARY, as DuckDB's default already was: the database's session holds it, never a file. */
    private static List<String> secret(HostOperation.Secret s) {
        if (s.fields().isEmpty()) {
            return List.of();
        }
        List<String> parts = new ArrayList<>();
        parts.add("TYPE S3");
        for (String field : s.fields()) {
            if (!CREDENTIALS.contains(field)) {
                throw new IllegalArgumentException("a secret's credential is one of " + new java.util.TreeSet<>(CREDENTIALS)
                        + ", not " + field);
            }
            parts.add(field + " ?");
        }
        parts.add(HostOperation.Secret.SCOPE + " ?");
        return List.of("CREATE OR REPLACE TEMPORARY SECRET " + quoted(s.name()) + " (" + String.join(", ", parts) + ")");
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
    private static String databaseIs(@com.legend.base.Nullable String database) {
        return database == null ? "database_name = current_database()" : "database_name = " + literal(database);
    }

    /** A relation's name: a model's declared one as its seed spelled it, any other quoted; its database, schema too. */
    private static String name(HostOperation.Name n) {
        String last = n.declared() ? physicalName(n.name()) : quoted(n.name());
        String schema = n.schema() == null ? null : n.declared() ? physicalName(n.schema()) : quoted(n.schema());
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
