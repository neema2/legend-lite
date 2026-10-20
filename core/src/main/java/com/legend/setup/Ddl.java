// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.setup;

import com.legend.model.DatabaseDefinition;
import com.legend.model.RelationalDataType;

/**
 * DDL rendering from the COMPILED store model — the K-native
 * {@code dropAndCreateTableInDb} boundary (the real engine's
 * {@code toDDL.pure} walks the Database metamodel; legend-lite renders
 * from {@link DatabaseDefinition}). Lives in the SETUP library (the plan
 * side's setup text, moved out of exec 2026-10-08,
 * docs/EXECUTION_PLAN_BOUNDARY_2026_10_05.md §9) — the SQL layer
 * ({@code com.legend.sql}) stays standalone and never sees the store model.
 * Model-derived DDL is spelled correctly for its
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
        return new com.legend.sql.SqlDdl.DropTable(schema, table, com.legend.sql.NameQuoting.BARE);
    }

    /** {@link #dropTable(String, String)} of a declared table: its names, and which the store declared quoted
     *  ({@link #quoting}). */
    public static com.legend.sql.SqlDdl.DropTable dropDeclaredTable(DatabaseDefinition.TableDefinition def,
            @com.legend.base.Nullable String schema) {
        return new com.legend.sql.SqlDdl.DropTable(schema, def.name(), quoting(def, schema));
    }

    /** Which of a declared table's names, in schema {@code schema} (null, or {@code default}, for none), its store
     *  declared quoted ({@code Table "Orders"}, {@code Schema "Sales"}): data the dialect spells (sql.NameQuoting), never
     *  quote characters in the names. */
    public static com.legend.sql.NameQuoting quoting(DatabaseDefinition.TableDefinition def,
            @com.legend.base.Nullable String schema) {
        boolean named = schema != null && !schema.isEmpty() && !"default".equals(schema);
        return new com.legend.sql.NameQuoting(named && def.schemaQuoted(), def.quoted());
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
            cols.add(new com.legend.sql.SqlDdl.Column(col.name(), col.quoted(), com.legend.compiler.element.StoreCompiler.declaredType(col.dataType()),
                    col.notNull(), col.primaryKey()));
        }
        return new com.legend.sql.SqlDdl.CreateTable(schema, def.name(), quoting(def, schema), cols);
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
            // the engine's text spells a schema bare (EngineStyleH2.storedName), quoted or not
            out.add(engineText.render(new com.legend.sql.SqlDdl.DropSchema(sc.getKey(), false)));
            out.add(engineText.render(new com.legend.sql.SqlDdl.CreateSchema(sc.getKey(), false)));
        }
        out.add(engineText.render(new com.legend.sql.SqlDdl.DropSchema("default", false)));
        out.add(engineText.render(new com.legend.sql.SqlDdl.CreateSchema("default", false)));
        for (var sc : named.entrySet()) {
            for (var t : sc.getValue().values()) {
                out.add(engineText.render(dropDeclaredTable(t, sc.getKey())));
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
                out.add(engineText.render(dropDeclaredTable(t, "default")));
                out.add(engineText.render(createTable(t, "default")));
            }
        }
        String[] lines = data.split("\n", -1);
        int i = 0;
        while (i < lines.length) {
            while (i < lines.length && (lines[i].strip().isEmpty()
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
                    findTable(db, lookup, schema, table);
            i += 3;
            while (i < lines.length && !lines[i].strip().isEmpty()
                    && !lines[i].strip().matches("-+")) {
                out.add(insertText(engineText, schema, table, def, header,
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
                    findTable(db, lookup, schema, table);
            i += 3;
            while (i < records.size() && !blankRecord(records.get(i))) {
                out.add(insertText(engineText, schema, table, def, header,
                        records.get(i)));
                i++;
            }
        }
        return out;
    }

    private static boolean blankRecord(java.util.List<String> r) {
        return r.isEmpty() || (r.size() == 1 && r.get(0).strip().isEmpty());
    }

    /** The store's table a CSV block names, found in the database's include closure as the engine's
     *  {@code $db->schema(...)->table(...)} finds it over {@code ^Database(includes=...)} (the
     *  closure the DDL above walks): its schema's, else the flattened top-level list's. Null when
     *  no database of the closure declares it. */
    private static DatabaseDefinition.@com.legend.base.Nullable TableDefinition findTable(DatabaseDefinition db,
            java.util.function.Function<String, java.util.Optional<DatabaseDefinition>> lookup,
            String schema, String table) {
        java.util.Map<String, java.util.LinkedHashMap<String, DatabaseDefinition.TableDefinition>> named =
                new java.util.LinkedHashMap<>();
        java.util.Map<String, DatabaseDefinition.TableDefinition> defaults = new java.util.LinkedHashMap<>();
        collectClosure(db, lookup, new java.util.LinkedHashSet<>(), named, defaults);
        if (!"default".equals(schema) && named.containsKey(schema)) {
            for (var t : named.get(schema).values()) {
                if (t.name().equalsIgnoreCase(table)) {
                    return t;
                }
            }
        }
        for (var t : defaults.values()) {
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

    /** One CSV row as the engine's {@code loadValuesToDbTable} writes it: an IR row
     *  ({@link com.legend.sql.SqlDml.InsertValues}) the engine-text dialect renders, each cell
     *  typed by its store column as the engine's {@code convertValuesToCsv} reads it (the
     *  dialect program's Phase 3, 2026-10-16: until then the text was written here, an empty
     *  cell as {@code ''} or nothing where the engine writes {@code null}). */
    private static String insertText(com.legend.sql.dialect.SqlDialect engineText, String schema, String table,
            DatabaseDefinition.@com.legend.base.Nullable TableDefinition def,
            java.util.List<String> header, java.util.List<String> cells) {
        if (def == null) {
            throw new IllegalStateException("setUpDataSQLs: no table '" + table + "' in schema '" + schema
                    + "' of the store: a row's cells are typed by its columns");
        }
        java.util.List<String> columns = new java.util.ArrayList<>();
        java.util.Set<String> quoted = new java.util.LinkedHashSet<>();
        java.util.List<com.legend.sql.SqlExpr> row = new java.util.ArrayList<>();
        for (int c = 0; c < header.size() && c < cells.size(); c++) {
            String h = header.get(c).strip();
            DatabaseDefinition.ColumnDefinition col = null;
            for (var cd : def.columns()) {
                if (cd.name().equalsIgnoreCase(h)) {
                    col = cd;
                    break;
                }
            }
            if (col == null) {
                throw new IllegalStateException("setUpDataSQLs: no column '" + h + "' in table '" + table + "'");
            }
            columns.add(col.name());
            if (col.quoted()) {
                quoted.add(col.name());
            }
            row.add(cell(cells.get(c), col, table));
        }
        return engineText.render(new com.legend.sql.SqlDml.InsertValues(
                "default".equals(schema) ? null : schema, table, com.legend.sql.NameQuoting.BARE, columns, quoted,
                java.util.List.of(row)));
    }

    /** The engine's spellings of an absent cell. */
    private static final java.util.Set<String> NULL_CELLS = java.util.Set.of("", "---null---");

    /** H2's {@code bitValueFromString}: the spellings of a bit's truth. */
    private static final java.util.Set<String> TRUE_BITS = java.util.Set.of("y", "1", "true");

    private static final java.util.Set<String> FALSE_BITS = java.util.Set.of("n", "0", "false");

    /** A CSV cell as the engine's {@code convertValuesToCsvH2} reads it, by its column's type: empty
     *  (or the engine's {@code ---null---}) is null; text, char and semi-structured are strings, a
     *  date and a timestamp their text; a bit is its truth (H2's {@code bitValueFromString}); a
     *  number is its value, exact. The engine copies any other cell as raw text, which is SQL only
     *  by accident, so such a cell is refused here, naming it (invariant 4). */
    private static com.legend.sql.SqlExpr cell(String text, DatabaseDefinition.ColumnDefinition col, String table) {
        if (NULL_CELLS.contains(text)) {
            return new com.legend.sql.SqlExpr.NullLit();
        }
        RelationalDataType t = col.dataType();
        if (t instanceof RelationalDataType.Varchar || t instanceof RelationalDataType.Char_
                || t instanceof RelationalDataType.SemiStructured) {
            return new com.legend.sql.SqlExpr.StringLit(text);
        }
        if (t instanceof RelationalDataType.Date_) {
            return new com.legend.sql.SqlExpr.DateLit(text);
        }
        if (t instanceof RelationalDataType.Timestamp) {
            return new com.legend.sql.SqlExpr.TimestampLit(text);
        }
        if (t instanceof RelationalDataType.Time) {
            return new com.legend.sql.SqlExpr.TimeLit(text);
        }
        if (t instanceof RelationalDataType.TimestampNs) {
            return new com.legend.sql.SqlExpr.TimestampLit(text);
        }
        if (t instanceof RelationalDataType.Bit) {
            String b = text.strip().toLowerCase(java.util.Locale.ROOT);
            if (TRUE_BITS.contains(b) || FALSE_BITS.contains(b)) {
                return new com.legend.sql.SqlExpr.BoolLit(TRUE_BITS.contains(b));
            }
        } else if (isNumericType(t)) {
            try {
                return new com.legend.sql.SqlExpr.DecimalLit(new java.math.BigDecimal(text.strip()));
            } catch (NumberFormatException e) {
                // not a number: refused below
            }
        }
        throw new IllegalStateException("setUpDataSQLs: the cell '" + text + "' of column '" + col.name() + "' ("
                + t + ") in table '" + table + "' is no value of its type the engine's seed text writes");
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
                dialect.render(new com.legend.sql.SqlDdl.CreateSchema(schema, false)),
                dialect.render(dropTable(schema, def.name())),
                dialect.render(createTable(def, schema)));
    }

    /** The seed's rows for one store table (all-string cells, pre-sorted by
     * the registry's deterministic-order contract; null an absent optional
     * fact), every column in declared order. */
    public static RowLoad metamodelRows(DatabaseDefinition.TableDefinition def, String schema,
            java.util.List<java.util.List<String>> rows) {
        return new RowLoad(schema, def.name(), quoting(def, schema), java.util.List.of(), java.util.Set.of(),
                def.columns().size(), rows);
    }

    /** A store table's name in a SERVER engine's system database (exec.SystemDatabase): {@code schema_table}, one
     *  name in the system session's own namespace, where its temporary tables live (a temporary table takes no
     *  schema on most servers: Postgres keeps it in {@code pg_temp}, Databricks and BigQuery in the session). A query
     *  reads it under this name through its table renames. */
    public static String systemTable(String schema, String table) {
        return schema + "_" + table;
    }

    /** One store table in a SERVER engine's system database, with its rows: a TEMPORARY table, the system session's
     *  own (no other session sees it; it goes when the session closes, a crash included), named {@link #systemTable},
     *  made by the dialect's {@code sessionTable} statements (a temporary table and its INSERT, or what the engine
     *  reads instead). The session is new, so nothing is dropped first and no schema is made. It declares NO key (a
     *  key column stays NOT NULL) and no length (a VARCHAR is the dialect's unbounded text): Databricks refuses a key
     *  on a temporary table, MariaDB and MySQL one past 3,072 bytes (the store's VARCHAR(1024) keys in utf8mb4) and a
     *  row whose declared lengths pass 65,535 bytes, Snowflake and BigQuery enforce no key (all measured 2026-10-11).
     *  The table is read-only, its rows the ones the in-process engines load under their keys and lengths. */
    public static java.util.List<String> metamodelTemporary(DatabaseDefinition.TableDefinition def, String schema,
            java.util.List<java.util.List<String>> rows, com.legend.sql.dialect.SqlDialect dialect) {
        java.util.List<com.legend.sql.SqlDdl.Column> columns = new java.util.ArrayList<>();
        for (com.legend.sql.SqlDdl.Column c : createTable(def, null).columns()) {
            com.legend.sql.SqlDdl.ColumnType type = c.type() instanceof com.legend.sql.SqlDdl.ColumnType.Sized z
                    && z.kind().equalsIgnoreCase("VARCHAR")
                    ? new com.legend.sql.SqlDdl.ColumnType.Plain(com.legend.sql.SqlDdl.ColumnType.Kind.VARCHAR)
                    : c.type();
            columns.add(new com.legend.sql.SqlDdl.Column(c.name(), c.declaredQuoted(), type,
                    c.notNull() || c.primaryKey(), false));
        }
        return dialect.sessionTable(new com.legend.sql.SqlDdl.CreateTable(null, systemTable(schema, def.name()),
                com.legend.sql.NameQuoting.BARE, columns, true), rows.isEmpty() ? null : metamodelTemporaryRows(def, schema, rows).values());
    }

    /** The rows of {@link #metamodelTemporary}'s table, as {@link #metamodelRows}, each cell of a column that is not
     *  text CAST to the column's type in the database: a server may take no text into it ({@link RowLoad#casts}). */
    private static RowLoad metamodelTemporaryRows(DatabaseDefinition.TableDefinition def, String schema,
            java.util.List<java.util.List<String>> rows) {
        java.util.List<com.legend.sql.@com.legend.base.Nullable SqlType> casts = new java.util.ArrayList<>();
        for (DatabaseDefinition.ColumnDefinition col : def.columns()) {
            casts.add(switch (col.dataType()) {
                case RelationalDataType.Varchar ignored -> null;
                case RelationalDataType.Integer_ ignored -> com.legend.sql.SqlType.Scalar.INTEGER;
                case RelationalDataType.Bit ignored -> com.legend.sql.SqlType.Scalar.BOOLEAN;
                default -> throw new IllegalStateException("system table " + def.name() + "." + col.name()
                        + ": no cast for its type " + col.dataType() + " (the store declares VARCHAR, INTEGER and"
                        + " BIT)");
            });
        }
        return new RowLoad(null, systemTable(schema, def.name()), com.legend.sql.NameQuoting.BARE, java.util.List.of(),
                java.util.Set.of(),
                def.columns().size(), rows, casts);
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
        return com.legend.sql.dialect.DdlSpelling.engineText(com.legend.compiler.element.StoreCompiler.declaredType(t));
    }

}
