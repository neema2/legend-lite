// SPDX-License-Identifier: Apache-2.0

package com.legend.exec;

import com.legend.compiler.element.ModelContext;

import java.util.ArrayList;
import java.util.List;

/**
 * The engine's {@code setUpDataSQLsV2} semantics: CSV seed blocks
 * ({@code schema\ntable\nheader\nrows...}, blocks joined by {@code \n-\n})
 * become DDL + INSERT statements derived from the PARSED store's column
 * types ({@link ModelContext#findTable}). Returns SQL text — execution
 * stays with the caller's executeInDb loop (the corpus's own
 * {@code setupTestData} body maps the strings through executeInDb;
 * audit 19d B5 moved this synthesis out of the harness so that body runs
 * through the platform).
 */
public final class CsvSeed {

    private CsvSeed() {
    }

    /** @param dialect the session's dialect — the table is created with its
     *  DECLARED column types, spelled by the dialect
     *  ({@link Ddl#createTable}); a seed typed from the Pure property
     *  types instead (the shape before 2026-09-16) turned every
     *  {@code DECIMAL(18,4)} into {@code DECIMAL(38, 9)}, and H2's
     *  decimal arithmetic then answered with the wrong scale (940 stress
     *  rows: {@code notional / riskScore} = 3571428.571, not the declared
     *  type's 3571428.5714285714). */
    public static List<String> sqls(String csvBlocks, @com.legend.base.Nullable String dbFqn,
            ModelContext ctx, com.legend.sql.dialect.SqlDialect dialect) {
        List<String> out = new ArrayList<>();
        // block separators: a line of dashes — '-' (the Alloy '\n-\n'
        // form) or '-----' (the testDataGeneration CSV form)
        StringBuilder block = new StringBuilder();
        for (String line : csvBlocks.split("\n", -1)) {
            if (line.strip().matches("-+")) {
                blockSqls(block.toString(), dbFqn, ctx, dialect, out);
                block.setLength(0);
            } else {
                if (block.length() > 0) {
                    block.append('\n');
                }
                block.append(line);
            }
        }
        blockSqls(block.toString(), dbFqn, ctx, dialect, out);
        return out;
    }

    private static void blockSqls(String csv, @com.legend.base.Nullable String dbFqn, ModelContext ctx,
            com.legend.sql.dialect.SqlDialect dialect, List<String> out) {
        String[] lines = csv.split("\n");
        while (lines.length > 0 && lines[0].isBlank()) {
            lines = java.util.Arrays.copyOfRange(lines, 1, lines.length);
        }
        if (lines.length < 3) {
            return;
        }
        String schema = lines[0].strip();
        String table = lines[1].strip();
        boolean defaultSchema = "default".equals(schema);
        String qualified = defaultSchema ? ident(table)
                : ident(schema) + "." + ident(table);
        String[] cols = cells(lines[2]);
        var def = dbFqn == null
                ? java.util.Optional.<com.legend.model.DatabaseDefinition.TableDefinition>empty()
                : ctx.findTableDefinition(dbFqn, defaultSchema ? table : schema + "." + table);
        if (def.isPresent()) {
            // schema-qualified CSV tables need their schema first (the
            // inline-CSV lane's creation half, FULL_RESIDUE_CENSUS §9a:
            // the engine's own lane creates model-derived tables on a
            // fresh database; TEST_SCHEMA.PEOPLE has no authored setup).
            // IF NOT EXISTS: idempotent on both engines and on mirrors
            // that already carry the schema.
            if (!defaultSchema) {
                out.add(dialect.render(new com.legend.sql.SqlDdl.CreateSchema(schema)));
            }
            // DROP-then-CREATE, never CREATE OR REPLACE: H2 (2.1.214, the
            // engine's own target) has no OR REPLACE for tables — this was
            // the recorded root cause of ~39 'Table already exists' H2
            // replay declines (H2_BACKEND.md §12 step 2); DuckDB accepts
            // the two-statement form identically
            out.add(dialect.render(Ddl.dropTable(defaultSchema ? null : schema, table)));
            // THE ONE DDL PRODUCER: the store's declared column types,
            // spelled for the target (the engine creates what the store
            // declares — its setUpDataSQLs reads the metamodel's types)
            out.add(dialect.render(Ddl.createTable(def.get(), defaultSchema ? null : schema)));
        } else {
            out.add("DELETE FROM " + qualified);
        }
        // F7.5: ONE multi-row INSERT per block — the statement count is
        // the seed cost (task #14: per-statement parse+plan+JNI), and
        // both H2 (mirror replay) and DuckDB accept multi-row VALUES
        List<String[]> rows = new ArrayList<>();
        for (int i = 3; i < lines.length; i++) {
            if (!lines[i].isBlank()) {
                rows.add(cells(lines[i]));
            }
        }
        String sql = insertStatement(qualified, cols, rows);
        if (sql != null) {
            out.add(sql);
        }
    }

    /** Words either target dialect reserves — quoted here as the query
     *  renderers quote them ({@code AnsiSqlRenderer.ident}, {@code H2.execPart}),
     *  so the seeded table and the query spell one name. */
    private static final java.util.Set<String> RESERVED;

    static {
        java.util.Set<String> all = new java.util.HashSet<>(
                com.legend.sql.dialect.Lexicon.DUCKDB.reservedWords());
        all.addAll(com.legend.sql.dialect.Lexicon.H2.reservedWords());
        RESERVED = java.util.Set.copyOf(all);
    }

    /** An identifier as DDL spells it: bare when plain and unreserved,
     *  double-quoted otherwise (a quoted store declaration is its own
     *  spelling already). */
    static String ident(String name) {
        if (name.length() > 1 && name.charAt(0) == '"' && name.endsWith("\"")) {
            return name;
        }
        if (name.matches("[A-Za-z_][A-Za-z0-9_$]*")
                && !RESERVED.contains(name.toLowerCase(java.util.Locale.ROOT))) {
            return name;
        }
        return '"' + name + '"';
    }

    /** One CSV line's cells. A bare line splits on commas; a cell wrapped
     *  in double quotes keeps its commas and reads {@code ""} as one
     *  quote (RFC 4180, the engine's relational CSV reader) — the two forms
     *  agree on every line that carries no quote. */
    static String[] cells(String line) {
        if (line.indexOf('"') < 0) {
            return line.split(",", -1);
        }
        List<String> out = new ArrayList<>();
        StringBuilder cell = new StringBuilder();
        boolean quoted = false;
        for (int i = 0; i < line.length(); i++) {
            char c = line.charAt(i);
            if (quoted) {
                if (c == '"') {
                    if (i + 1 < line.length() && line.charAt(i + 1) == '"') {
                        cell.append('"');
                        i++;
                    } else {
                        quoted = false;
                    }
                } else {
                    cell.append(c);
                }
            } else if (c == '"') {
                quoted = true;
            } else if (c == ',') {
                out.add(cell.toString());
                cell.setLength(0);
            } else {
                cell.append(c);
            }
        }
        out.add(cell.toString());
        return out.toArray(String[]::new);
    }

    /** ONE multi-row INSERT of CSV cells — the seed spelling, shared with
     * the loadCsvToDbTable arm (batch 85): every value rides as a QUOTED
     * literal and the DATABASE casts it to the column's type (F7.2); an
     * empty or {@code ---null---} cell is NULL. Null when no rows. */
    public static @com.legend.base.Nullable String insertStatement(String qualified,
            String[] cols, List<String[]> rows) {
        StringBuilder sql = null;
        for (String[] vals : rows) {
            if (sql == null) {
                sql = new StringBuilder("INSERT INTO ")
                        .append(qualified).append(" (");
                for (int c = 0; c < cols.length; c++) {
                    if (c > 0) {
                        sql.append(", ");
                    }
                    sql.append(ident(cols[c].strip()));
                }
                sql.append(") VALUES ");
            } else {
                sql.append(", ");
            }
            sql.append('(');
            for (int c = 0; c < cols.length; c++) {
                String tok = c < vals.length ? vals[c].strip() : "";
                if (c > 0) {
                    sql.append(", ");
                }
                if (tok.isEmpty() || tok.equals("---null---")) {
                    sql.append("NULL");
                } else {
                    sql.append("'").append(tok.replace("'", "''"))
                            .append("'");
                }
            }
            sql.append(')');
        }
        return sql == null ? null : sql.toString();
    }

    /** The from() node's {@code testDataSetupCsv} FACTS as seed SQL — the
     * executor's half against the store (the compiler only records the
     * block and its database). */
    public static List<String> setupSqls(
            com.legend.compiler.spec.typed.TypedFrom fr,
            com.legend.compiler.element.ModelContext ctx, com.legend.sql.dialect.SqlDialect dialect) {
        List<String> out = new java.util.ArrayList<>();
        for (var c : fr.csvSetups()) {
            String db = c.dbFqn() != null && ctx.findDatabase(c.dbFqn()).isPresent()
                    ? c.dbFqn() : null;
            out.addAll(sqls(c.csv(), db, ctx, dialect));
        }
        return out;
    }
}
