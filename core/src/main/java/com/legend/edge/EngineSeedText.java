// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.edge;

import com.legend.model.DatabaseDefinition;
import com.legend.model.RelationalDataType;
import com.legend.setup.Ddl;

/**
 * LEGEND-ENGINE'S setUpDataSQLs TEXT (J32 of the edge audit, 2026-11-18; moved unchanged from {@code setup.Ddl}): a
 * test connection's CSV as the statements legend-engine's plan prints for it -- the schemas, every table's DDL through
 * the product's one generator ({@link Ddl#dropDeclaredTable}, {@link Ddl#createTable}), and one insert per CSV row,
 * each rendered by the engine-text dialect the caller hands ({@code PureText.engineText}). Its callers are the edge's
 * plan printers ({@link EnginePlans}); the product seeds a store through {@code setup.CsvSeed} and plans no such
 * text. Composes and prints; runs nothing, and a cell is typed by its column, never computed.
 */
public final class EngineSeedText {

    private EngineSeedText() {
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
            // the schema's DDL as the dialect writes it (A2, 2026-11-19: legend-engine's printer spelled it bare)
            out.add(engineText.render(new com.legend.sql.SqlDdl.DropSchema(sc.getKey(), false)));
            out.add(engineText.render(new com.legend.sql.SqlDdl.CreateSchema(sc.getKey(), false)));
        }
        out.add(engineText.render(new com.legend.sql.SqlDdl.DropSchema("default", false)));
        out.add(engineText.render(new com.legend.sql.SqlDdl.CreateSchema("default", false)));
        for (var sc : named.entrySet()) {
            for (var t : sc.getValue().values()) {
                out.add(engineText.render(Ddl.dropDeclaredTable(t, sc.getKey())));
                out.add(engineText.render(Ddl.createTable(t, sc.getKey())));
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
                out.add(engineText.render(Ddl.dropDeclaredTable(t, "default")));
                out.add(engineText.render(Ddl.createTable(t, "default")));
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
}
