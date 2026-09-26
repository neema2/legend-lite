// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.exec;

import com.legend.model.DatabaseDefinition;
import com.legend.model.RelationalDataType;

/**
 * DDL rendering from the COMPILED store model — the K-native
 * {@code dropAndCreateTableInDb} boundary (the real engine's
 * {@code toDDL.pure} walks the Database metamodel; legend-lite renders
 * from {@link DatabaseDefinition}). Lives in the EXEC (K-phase) package —
 * the SQL layer ({@code com.legend.sql}) stays standalone and never sees
 * the store model. Model-derived DDL is spelled correctly for its
 * TARGET the first time (F7.4, audit S4): the type switch carries both
 * flavors, and the {@link RawSqlBoundary} translator serves
 * HAND-WRITTEN corpus text only — text whose origin really is another
 * dialect.
 *
 * <p>Constraints: the {@code dropAndCreateTableInDb} native emits the
 * declared {@code PRIMARY KEY(...)} and {@code NULL}/{@code NOT NULL}
 * exactly like the engine (its {@code applyConstraints} defaults true —
 * batch 71, measured across the corpus: zero seed failures, so no
 * fixture re-seeds a keyed table it created through the native; the one
 * failure was declared-quoted key names, now spelled per flavor). The
 * HARNESS's ambient CSV seed ({@code CsvSeed}) still creates its tables
 * without constraints: milestoned test data holds several versions of
 * one id, and that seeding has no engine counterpart to match.
 */
public final class Ddl {

    private Ddl() {
    }

    /** The render TARGETS of the ONE generator (ratified E4 design:
     * engine-exact text is a FLAVOR of the single speller, never a
     * second one). {@code H2_EXEC}/{@code DUCK_EXEC} are the EXECUTION
     * forms — full-name quoting, no constraints (the deliberate DuckDB
     * re-seed divergence in this file's header); {@code ENGINE_TEXT} is
     * the engine's {@code translateCreateTableStatementDefault}
     * (extensionDefaults.pure:609-620) — reserved-word column quoting,
     * engine type spellings (INT), NULL / NOT NULL nullability,
     * trailing {@code , PRIMARY KEY(...)} with RAW pk names. */
    /** {@code Drop table if exists s.T;} — the engine's
     * dropTableStatement spelling, identical across every flavor. */
    public static com.legend.sql.SqlDdl.DropTable dropTable(@com.legend.base.Nullable String schema,
            String table) {
        return new com.legend.sql.SqlDdl.DropTable(schema, table);
    }

    /** The store model's column type as the SQL layer's DECLARED type —
     *  the one crossing from store model to SQL IR (the SQL layer stands
     *  alone; {@code Ddl} is the exec-side translator). */
    public static com.legend.sql.SqlDdl.ColumnType columnType(RelationalDataType t) {
        return switch (t) {
            case RelationalDataType.BigInt ignored -> plain(com.legend.sql.SqlDdl.ColumnType.Kind.BIGINT);
            case RelationalDataType.SmallInt ignored -> plain(com.legend.sql.SqlDdl.ColumnType.Kind.SMALLINT);
            case RelationalDataType.TinyInt ignored -> plain(com.legend.sql.SqlDdl.ColumnType.Kind.TINYINT);
            case RelationalDataType.Integer_ ignored -> plain(com.legend.sql.SqlDdl.ColumnType.Kind.INTEGER);
            case RelationalDataType.Float_ ignored -> plain(com.legend.sql.SqlDdl.ColumnType.Kind.FLOAT);
            case RelationalDataType.Double_ ignored -> plain(com.legend.sql.SqlDdl.ColumnType.Kind.DOUBLE);
            case RelationalDataType.Real ignored -> plain(com.legend.sql.SqlDdl.ColumnType.Kind.REAL);
            case RelationalDataType.Bit ignored -> plain(com.legend.sql.SqlDdl.ColumnType.Kind.BIT);
            case RelationalDataType.Timestamp ignored -> plain(com.legend.sql.SqlDdl.ColumnType.Kind.TIMESTAMP);
            case RelationalDataType.Date_ ignored -> plain(com.legend.sql.SqlDdl.ColumnType.Kind.DATE);
            case RelationalDataType.SemiStructured ignored -> plain(com.legend.sql.SqlDdl.ColumnType.Kind.JSON);
            case RelationalDataType.Other ignored -> plain(com.legend.sql.SqlDdl.ColumnType.Kind.OTHER);
            case RelationalDataType.Distinct ignored -> plain(com.legend.sql.SqlDdl.ColumnType.Kind.DISTINCT);
            case RelationalDataType.Array ignored -> plain(com.legend.sql.SqlDdl.ColumnType.Kind.ARRAY);
            case RelationalDataType.Object_ ignored -> plain(com.legend.sql.SqlDdl.ColumnType.Kind.OBJECT);
            case RelationalDataType.Varchar v -> new com.legend.sql.SqlDdl.ColumnType.Sized("VARCHAR", v.size());
            case RelationalDataType.Char_ c -> new com.legend.sql.SqlDdl.ColumnType.Sized("CHAR", c.size());
            case RelationalDataType.Binary b -> new com.legend.sql.SqlDdl.ColumnType.Sized("BINARY", b.size());
            case RelationalDataType.Varbinary v -> new com.legend.sql.SqlDdl.ColumnType.Sized("VARBINARY", v.size());
            case RelationalDataType.Decimal d -> new com.legend.sql.SqlDdl.ColumnType.Scaled("DECIMAL", d.precision(), d.scale());
            case RelationalDataType.Numeric n -> new com.legend.sql.SqlDdl.ColumnType.Scaled("NUMERIC", n.precision(), n.scale());
        };
    }

    private static com.legend.sql.SqlDdl.ColumnType plain(com.legend.sql.SqlDdl.ColumnType.Kind k) {
        return new com.legend.sql.SqlDdl.ColumnType.Plain(k);
    }

    /** {@code constraints}: emit the engine's {@code NULL}/{@code NOT NULL}
     * and trailing {@code PRIMARY KEY(...)} in an EXECUTION flavor too —
     * the {@code dropAndCreateTableInDb} native (engine parity: its
     * {@code applyConstraints} defaults true, so the physical table the
     * engine's test creates CARRIES its declared key and the live catalog
     * answers {@code fetchDbPrimaryKeysMetaData}); the ambient seed stays
     * unconstrained (this file's header). */
    /** THE store's declared shape as a DDL node: every column with its
     *  declared type, nullability and key membership (the engine's
     *  applyConstraints defaults true — the physical table CARRIES its
     *  declared key and the live catalog answers fetchDbPrimaryKeysMetaData);
     *  the dialect renders it ({@link com.legend.sql.dialect.SqlDialect#render(com.legend.sql.SqlDdl)}). */
    public static com.legend.sql.SqlDdl.CreateTable createTable(
            DatabaseDefinition.TableDefinition def, @com.legend.base.Nullable String schema) {
        java.util.List<com.legend.sql.SqlDdl.Column> cols = new java.util.ArrayList<>();
        for (DatabaseDefinition.ColumnDefinition col : def.columns()) {
            cols.add(new com.legend.sql.SqlDdl.Column(col.name(), col.quoted(), columnType(col.dataType()),
                    col.notNull(), col.primaryKey()));
        }
        return new com.legend.sql.SqlDdl.CreateTable(schema, def.name(), cols);
    }


    /** The ENGINE's setUpDataSQLs TEXT (toDDL.pure:186-195 +
     * loadCsvDataToDbTable): schema drop/create pairs, every table's
     * drop/create text, then one {@code insert} per CSV row. Faithful
     * quirks: cells are NOT trimmed; a cell whose FIRST char is a quote
     * unquotes, leading-space-then-quote keeps the cell verbatim; block
     * separators are lines of dashes (the CsvSeed corpus form). */
    public static java.util.List<String> setUpDataSqlsText(String data,
            DatabaseDefinition db, com.legend.sql.dialect.SqlDialect engineText) {
        return setUpDataSqlsText(data, db, f -> java.util.Optional.empty(), engineText);
    }

    /** Include-closure form (cluster 60 — the engine's {@code
     * allSchemas()} recurses {@code includes} FIRST, groups schemas by
     * name and de-duplicates tables; setUpDataSQLs walks that closure,
     * so a db of includes yields every included table's DDL). The
     * lookup resolves an include FQN to its definition; an unresolvable
     * include contributes nothing (parity with the engine's cast walk
     * over a compiled model, where it cannot happen). */
    public static java.util.List<String> setUpDataSqlsText(String data,
            DatabaseDefinition db,
            java.util.function.Function<String,
                    java.util.Optional<DatabaseDefinition>> lookup,
            com.legend.sql.dialect.SqlDialect engineText) {
        // include-first, group-by-name, table-dedup-by-name (first wins)
        java.util.LinkedHashMap<String,
                java.util.LinkedHashMap<String,
                        DatabaseDefinition.TableDefinition>> named =
                new java.util.LinkedHashMap<>();
        java.util.LinkedHashMap<String,
                DatabaseDefinition.TableDefinition> defaults =
                new java.util.LinkedHashMap<>();
        collectClosure(db, lookup, new java.util.LinkedHashSet<>(), named,
                defaults);
        java.util.List<String> out = new java.util.ArrayList<>();
        for (var sc : named.entrySet()) {
            out.add(engineText.render(new com.legend.sql.SqlDdl.DropSchema(sc.getKey())));
            out.add(engineText.render(new com.legend.sql.SqlDdl.CreateSchema(sc.getKey())));
        }
        out.add(engineText.render(new com.legend.sql.SqlDdl.DropSchema("default")));
        out.add(engineText.render(new com.legend.sql.SqlDdl.CreateSchema("default")));
        for (var sc : named.entrySet()) {
            for (var t : sc.getValue().values()) {
                out.add(engineText.render(dropTable(sc.getKey(), t.name())));
                out.add(engineText.render(createTable(t, sc.getKey())));
            }
        }
        // the parser FLATTENS named-schema tables into the top-level list
        // too — the DDL surface lists each table once, under its schema
        java.util.Set<String> inSchemas = new java.util.HashSet<>();
        for (var sc : named.values()) {
            for (var t : sc.values()) {
                inSchemas.add(t.name());
            }
        }
        for (var t : defaults.values()) {
            if (!inSchemas.contains(t.name())) {
                out.add(engineText.render(dropTable("default", t.name())));
                out.add(engineText.render(createTable(t, "default")));
            }
        }
        String[] lines = data.split("\n", -1);
        int i = 0;
        while (i < lines.length) {
            while (i < lines.length && (lines[i].isBlank()
                    || lines[i].strip().matches("-+"))) {
                i++;
            }
            if (i + 2 >= lines.length) {
                break;
            }
            String schema = lines[i].strip();
            String table = lines[i + 1].strip();
            java.util.List<String> header = csvCells(lines[i + 2]);
            DatabaseDefinition.TableDefinition def =
                    findTable(db, schema, table);
            i += 3;
            while (i < lines.length && !lines[i].isBlank()
                    && !lines[i].strip().matches("-+")) {
                out.add(insertText(schema, table, def, header,
                        csvCells(lines[i])));
                i++;
            }
        }
        return out;
    }

    /** The RECORDS form (engine setUpDataSQLs(records:List<String>[*])):
     *  pre-split cells, no CSV re-parsing — schema/table statements from
     *  the string form with EMPTY data, inserts appended per record
     *  block (blank single-cell records separate blocks). */
    public static java.util.List<String> setUpDataSqlsTextFromRecords(
            java.util.List<java.util.List<String>> records,
            DatabaseDefinition db, com.legend.sql.dialect.SqlDialect engineText) {
        return setUpDataSqlsTextFromRecords(records, db,
                f -> java.util.Optional.empty(), engineText);
    }

    public static java.util.List<String> setUpDataSqlsTextFromRecords(
            java.util.List<java.util.List<String>> records,
            DatabaseDefinition db,
            java.util.function.Function<String,
                    java.util.Optional<DatabaseDefinition>> lookup,
            com.legend.sql.dialect.SqlDialect engineText) {
        java.util.List<String> out = setUpDataSqlsText("", db, lookup, engineText);
        int i = 0;
        while (i < records.size()) {
            while (i < records.size() && blankRecord(records.get(i))) {
                i++;
            }
            if (i + 2 >= records.size()) {
                break;
            }
            String schema = records.get(i).get(0).strip();
            String table = records.get(i + 1).get(0).strip();
            java.util.List<String> header = records.get(i + 2);
            DatabaseDefinition.TableDefinition def =
                    findTable(db, schema, table);
            i += 3;
            while (i < records.size() && !blankRecord(records.get(i))) {
                out.add(insertText(schema, table, def, header,
                        records.get(i)));
                i++;
            }
        }
        return out;
    }

    private static boolean blankRecord(java.util.List<String> r) {
        return r.isEmpty() || (r.size() == 1 && r.get(0).isBlank());
    }

    private static DatabaseDefinition.@com.legend.base.Nullable TableDefinition
            findTable(DatabaseDefinition db, String schema, String table) {
        if (!"default".equals(schema)) {
            for (var sc : db.schemas()) {
                if (sc.name().equals(schema)) {
                    for (var t : sc.tables()) {
                        if (t.name().equalsIgnoreCase(table)) {
                            return t;
                        }
                    }
                }
            }
        }
        for (var t : db.tables()) {
            if (t.name().equalsIgnoreCase(table)) {
                return t;
            }
        }
        return null;
    }

    /** Quote-aware split; a cell whose FIRST character is the quote
     *  unquotes (CSV semantics), leading whitespace before the quote
     *  keeps the cell verbatim, quotes and all (engine parseCSV parity). */
    private static java.util.List<String> csvCells(String line) {
        java.util.List<String> cells = new java.util.ArrayList<>();
        StringBuilder cur = new StringBuilder();
        boolean inQuotes = false;
        for (int k = 0; k < line.length(); k++) {
            char ch = line.charAt(k);
            if (ch == '"') {
                inQuotes = !inQuotes;
                cur.append(ch);
            } else if (ch == ',' && !inQuotes) {
                cells.add(cur.toString());
                cur.setLength(0);
            } else {
                cur.append(ch);
            }
        }
        cells.add(cur.toString());
        for (int k = 0; k < cells.size(); k++) {
            String c = cells.get(k);
            if (c.length() >= 2 && c.charAt(0) == '"' && c.endsWith("\"")) {
                cells.set(k, c.substring(1, c.length() - 1));
            }
        }
        return cells;
    }

    private static String insertText(String schema, String table,
            DatabaseDefinition.@com.legend.base.Nullable TableDefinition def,
            java.util.List<String> header, java.util.List<String> cells) {
        java.util.List<String> colNames = new java.util.ArrayList<>();
        java.util.List<String> values = new java.util.ArrayList<>();
        for (int c = 0; c < header.size() && c < cells.size(); c++) {
            String h = header.get(c).strip();
            DatabaseDefinition.ColumnDefinition col = null;
            if (def != null) {
                for (var cd : def.columns()) {
                    if (cd.name().equalsIgnoreCase(h)) {
                        col = cd;
                        break;
                    }
                }
            }
            // SAME identifier rule as the create (batch A): a reserved
            // or space-bearing column quotes in BOTH or the pair breaks
            // on a case-sensitive session
            colNames.add(col != null && col.quoted()
                    ? '"' + col.name() + '"'
                    : com.legend.sql.dialect.DdlSpelling.h2ExecIdentifier(col != null ? col.name() : h));
            String cell = cells.get(c);
            boolean numeric = col != null && isNumericType(col.dataType());
            values.add(numeric ? cell.strip()
                    : "'" + cell.replace("'", "''") + "'");
        }
        String qualified = "default".equals(schema) ? table
                : schema + "." + table;
        return "insert into " + qualified + " ("
                + String.join(",", colNames) + ") values ("
                + String.join(",", values) + ");";
    }

    private static boolean isNumericType(RelationalDataType t) {
        return t instanceof RelationalDataType.Integer_
                || t instanceof RelationalDataType.BigInt
                || t instanceof RelationalDataType.SmallInt
                || t instanceof RelationalDataType.TinyInt
                || t instanceof RelationalDataType.Float_
                || t instanceof RelationalDataType.Double_
                || t instanceof RelationalDataType.Real
                || t instanceof RelationalDataType.Decimal
                || t instanceof RelationalDataType.Numeric;
    }

    private static void collectClosure(DatabaseDefinition db,
            java.util.function.Function<String,
                    java.util.Optional<DatabaseDefinition>> lookup,
            java.util.Set<String> seen,
            java.util.Map<String, java.util.LinkedHashMap<String,
                    DatabaseDefinition.TableDefinition>> named,
            java.util.Map<String,
                    DatabaseDefinition.TableDefinition> defaults) {
        if (!seen.add(db.qualifiedName())) {
            return;
        }
        for (String inc : db.includes()) {
            lookup.apply(inc).ifPresent(d ->
                    collectClosure(d, lookup, seen, named, defaults));
        }
        for (var sc : db.schemas()) {
            var tables = named.computeIfAbsent(sc.name(),
                    k -> new java.util.LinkedHashMap<>());
            for (var t : sc.tables()) {
                tables.putIfAbsent(t.name(), t);
            }
        }
        for (var t : db.tables()) {
            defaults.putIfAbsent(t.name(), t);
        }
    }

    /** The SYSTEM metamodel seed's DDL (METAMODEL_STORE_HANDOFF.md &sect;5):
     * schema + drop/create through the ONE generator. Its rows are
     * {@link #metamodelRows}. Idempotent per context &mdash; the content is
     * a pure function of the active model context, so overlays simply
     * re-seed. */
    public static java.util.List<String> metamodelSeed(
            DatabaseDefinition.TableDefinition def, String schema,
            com.legend.sql.dialect.SqlDialect dialect) {
        return java.util.List.of(
                dialect.render(new com.legend.sql.SqlDdl.CreateSchema(schema)),
                dialect.render(dropTable(schema, def.name())),
                dialect.render(createTable(def, schema)));
    }

    /** The seed's rows for one store table (all-string cells, pre-sorted by
     * the registry's deterministic-order contract; null an absent optional
     * fact), every column in declared order. */
    public static RowLoad metamodelRows(DatabaseDefinition.TableDefinition def, String schema,
            java.util.List<java.util.List<String>> rows) {
        return new RowLoad(schema, def.name(), java.util.List.of(), def.columns().size(), rows);
    }

    /** The FLAVORED type spelling: the deltas from the H2 base are the
     * ONLY per-target lines — DuckDB where H2's type SEMANTICS differ
     * from its name (FLOAT is an 8-byte double, BIT a boolean — spelled
     * from the TYPE, never recovered from text, F7.4); engine TEXT is
     * {@link #dataTypeToSqlText}, the ONE engine spelling. */

    /** THE engine {@code dataTypeToSqlText} spelling
     * (platform_store_relational/functions.pure:68-96), spelled ONCE —
     * the ENGINE_TEXT DDL flavor and the metamodel walk's
     * dataTypeToSqlText native both read here (ratified E4 design: no
     * type text is spelled twice). Deltas from the EXECUTION base:
     * Integer spells INT; Other spells OTHER (execution walls — a
     * column of type Other cannot be created). */
    public static String dataTypeToSqlText(RelationalDataType t) {
        return com.legend.sql.dialect.DdlSpelling.engineText(columnType(t));
    }

}
