// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.sql.dialect;

import java.util.List;
import java.util.Map;

/**
 * A NAMED OPERATION A HOST RUNS ON ITS OWN DATABASE, outside any query (docs/ONE_PLATFORM_PROGRAM_2026_10_12.md D5, the
 * dialect program's decision D5): what the tab and Python's host used to spell as SQL themselves -- a file read into a
 * table, a remote file mounted, a copy made, rows counted, an extension, a secret, a catalog question. A host names the
 * operation and its data; the database's {@link HostOperations} writes the statements (names quoted, values written as
 * literals), and a database without one refuses it by name. Data only: no SQL text here, but a query a planner wrote
 * ({@link Source.Query}) and a select list a planner wrote ({@link MakeRelation#selectList}).
 */
public sealed interface HostOperation {

    /**
     * A relation's name, with its database and schema when given. {@code declared}: a model's own (a Database's table),
     * spelled as the model's database holds it ({@link SqlDialect#physicalName}, as its seed made it); otherwise a
     * person's or a host's name (a file's table, a frame's), always quoted, whatever it holds.
     */
    record Name(@com.legend.base.Nullable String database, @com.legend.base.Nullable String schema, String name,
            boolean declared) {
        public Name {
            java.util.Objects.requireNonNull(name, "name");
            // a model's default schema is no schema: its tables are made with none (as their seed makes them)
            if (declared && "default".equals(schema)) {
                schema = null;
            }
        }

        /** A person's or a host's name in the database's own schema. */
        public static Name of(String name) {
            return new Name(null, null, name, false);
        }

        /** A model's declared table: its schema ({@code null} or {@code default}: none) and its name. */
        public static Name declared(@com.legend.base.Nullable String schema, String table) {
            return new Name(null, schema, table, true);
        }
    }

    /** Where a relation's rows come from. */
    sealed interface Source {
        /** A relation of the database. */
        record Relation(Name name) implements Source {
        }

        /** A file the database reads, by a registered name or a path ({@code at}), in a format. */
        record File(String at, FileFormat format) implements Source {
        }

        /** A file the database reads from a remote address, in a format. */
        record Remote(String url, RemoteFormat format) implements Source {
        }

        /** A query a planner wrote, for this database. */
        record Query(String sql) implements Source {
        }
    }

    /** How a file is read: sniffed CSV with a header; CSV with a header, every column as text (a declared table's file,
     *  each value converted as it goes in); Parquet; JSON, each record's keys its columns. */
    enum FileFormat { CSV, CSV_TEXT, PARQUET, JSON }

    /** How a remote file is read. */
    enum RemoteFormat { CSV, PARQUET, ICEBERG }

    /** A relation's kind. */
    enum Kind { TABLE, VIEW }

    /** A column replaced as a copy is made (Snap's conversions): its name, and the planner's expression for it. */
    record Conversion(String column, String sql) {
    }

    /**
     * A relation made from a source: {@code replace} an existing one of its name or not; its columns the planner's copy
     * select list ({@code selectList}), or every column with {@code replacing}'s replaced, or every column.
     */
    record MakeRelation(Kind kind, Name target, boolean replace, @com.legend.base.Nullable String selectList,
            List<Conversion> replacing, Source from) implements HostOperation {
        public MakeRelation {
            replacing = List.copyOf(replacing);
        }
    }

    /** One column put in: the declared column's name, and the source's column it is read from (a file's header). */
    record Column(String target, String from) {
    }

    /** Rows put into a table from a source, each target column from the source's column of its name. */
    record Insert(Name target, List<Column> columns, Source from) implements HostOperation {
        public Insert {
            columns = List.copyOf(columns);
        }
    }

    /**
     * Rows put into a keyed table from a table of the database (a live table's upsert, docs/DUCKDB_LIVE_2026_10_25.md):
     * each the latest row of its {@code key} -- the target's columns, its primary key's -- so a row of the key the table
     * holds is replaced, and of the source's rows of one key its last is put in. Each target column from the source's
     * column {@link Column#from}, the key's columns among them.
     */
    record Upsert(Name target, List<Column> columns, List<String> key, Source.Relation from) implements HostOperation {
        public Upsert {
            columns = List.copyOf(columns);
            key = List.copyOf(key);
            if (key.isEmpty()) {
                throw new IllegalArgumentException("an upsert names its table's key");
            }
            for (String k : key) {
                if (columns.stream().noneMatch(c -> c.target().equals(k))) {
                    throw new IllegalArgumentException("an upsert puts in its key's column " + k);
                }
            }
        }
    }

    /**
     * A stream's rows past its bound deleted (a live table's history, docs/DUCKDB_LIVE_2026_10_25.md): those older, by
     * {@code column}, than its latest {@code rows}, or than its latest value less {@code millis} (a time column).
     */
    record Bound(Name target, String column, Keep keep) implements HostOperation {
        /** What a stream keeps of its history. */
        public sealed interface Keep {
            /** Its latest rows: a whole number, at least one. */
            record Rows(long rows) implements Keep {
                public Rows {
                    if (rows < 1) {
                        throw new IllegalArgumentException("a stream keeps at least one row, not " + rows);
                    }
                }
            }

            /** Its rows within this many milliseconds of its latest. */
            record Within(long millis) implements Keep {
                public Within {
                    if (millis < 0) {
                        throw new IllegalArgumentException("a stream keeps a span of no less than nothing, not " + millis + " ms");
                    }
                }
            }
        }
    }

    /** A relation dropped, when it is there. */
    record Drop(Kind kind, Name target) implements HostOperation {
    }

    /** A schema made, when it is not there. */
    record MakeSchema(@com.legend.base.Nullable String database, String name) implements HostOperation {
    }

    /** A schema dropped, when it is there. */
    record DropSchema(@com.legend.base.Nullable String database, String name) implements HostOperation {
    }

    /** How many rows a source has: one row, one column {@code n}. */
    record Count(Source from) implements HostOperation {
    }

    /** A secret a remote source's reads use, named, scoped to a path: its credentials by their kind's names. */
    record Secret(String name, String scope, Map<String, String> credentials) implements HostOperation {
        public Secret {
            credentials = java.util.Collections.unmodifiableMap(new java.util.LinkedHashMap<>(credentials));
        }
    }

    /** Extensions installed and loaded, in order. */
    record Extensions(List<String> names) implements HostOperation {
        public Extensions {
            names = List.copyOf(names);
        }
    }

    /** Where the database installs extensions from, on demand, by their names (a site's folder: the tab's). */
    record ExtensionRepository(String url) implements HostOperation {
    }

    /** Where the database's extensions already are, none installed on demand (a host that ships them: Python's). */
    record ExtensionDirectory(String folder) implements HostOperation {
    }

    /** The folder a file's relative path is read from. */
    record FileSearchPath(String folder) implements HostOperation {
    }

    /** A transaction's step. */
    record Transaction(Step step) implements HostOperation {
        public enum Step { BEGIN, COMMIT, ROLLBACK }
    }

    /** How many relations of {@code name}, whatever its case, the database's own schemas and its temporary ones hold:
     *  one row, one column {@code n}. */
    record NameTaken(String name) implements HostOperation {
    }

    /** How many tables and views of {@code name} there are (its database the current one when none; its schema and name
     *  matched whatever their case): one row, one column {@code n}. */
    record RelationExists(Name name) implements HostOperation {
    }

    /** How many tables a schema holds (its database the current one when none): one row, one column {@code n}. */
    record TablesIn(@com.legend.base.Nullable String database, String schema) implements HostOperation {
    }

    /** How many schemas of the name, whatever its case, the database has: one row, one column {@code n}. */
    record SchemaExists(@com.legend.base.Nullable String database, String schema) implements HostOperation {
    }

    /** The database's own name: one row, one column {@code name}. */
    record CurrentDatabase() implements HostOperation {
    }

    /** The platform the database names its extensions by: one row, one column {@code platform}. */
    record Platform() implements HostOperation {
    }

    /** An Iceberg table's manifest lists, one row each, column {@code path}. */
    record IcebergManifestLists(String url) implements HostOperation {
    }

    /** The manifests those lists name, one row each, column {@code path}. */
    record IcebergManifests(List<String> lists) implements HostOperation {
        public IcebergManifests {
            lists = List.copyOf(lists);
        }
    }

    /** The data files an Iceberg table's metadata names, one row each, column {@code path}. */
    record IcebergDataFiles(String url) implements HostOperation {
    }
}
