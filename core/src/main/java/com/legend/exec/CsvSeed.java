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
        return steps(csvBlocks, dbFqn, ctx, dialect).stream().map(st -> st.text(dialect)).toList();
    }

    /** One step of a seed, in order: a statement (the dialect's DDL text), or
     *  rows to load ({@link Executor#load} &mdash; an engine's bulk API when
     *  it has one; its text is the one multi-row insert). */
    public sealed interface Step permits Step.Sql, Step.Rows {
        record Sql(String text) implements Step {
        }

        record Rows(RowLoad load) implements Step {
        }

        /** The step as SQL text, rendered by {@code dialect}. */
        default String text(com.legend.sql.dialect.SqlDialect dialect) {
            return switch (this) {
                case Sql q -> q.text();
                case Rows r -> dialect.render(r.load().values());
            };
        }
    }

    /** The seed as {@link Step}s &mdash; {@link #sqls}' statements, the rows
     *  still rows. */
    public static List<Step> steps(String csvBlocks, @com.legend.base.Nullable String dbFqn,
            ModelContext ctx, com.legend.sql.dialect.SqlDialect dialect) {
        List<Step> out = new ArrayList<>();
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
            com.legend.sql.dialect.SqlDialect dialect, List<Step> out) {
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
                out.add(new Step.Sql(dialect.render(new com.legend.sql.SqlDdl.CreateSchema(schema))));
            }
            // DROP-then-CREATE, never CREATE OR REPLACE: H2 (2.1.214, the
            // engine's own target) has no OR REPLACE for tables — this was
            // the recorded root cause of ~39 'Table already exists' H2
            // replay declines (H2_BACKEND.md §12 step 2); DuckDB accepts
            // the two-statement form identically
            out.add(new Step.Sql(dialect.render(Ddl.dropTable(defaultSchema ? null : schema, table))));
            // THE ONE DDL PRODUCER: the store's declared column types,
            // spelled for the target (the engine creates what the store
            // declares — its setUpDataSQLs reads the metamodel's types)
            out.add(new Step.Sql(dialect.render(Ddl.createTable(def.get(), defaultSchema ? null : schema))));
        } else {
            out.add(new Step.Sql(dialect.render(new com.legend.sql.SqlDml.DeleteAll(
                    defaultSchema ? null : ident(schema), ident(table)))));
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
        RowLoad load = rowLoad(defaultSchema ? null : ident(schema), ident(table), cols, rows);
        if (load != null) {
            out.add(new Step.Rows(load));
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

    /** CSV cells as rows for {@code [schema.]table} (both spelled as SQL names
     * them) &mdash; the seed's rows, shared with the loadCsvToDbTable arm
     * (batch 85): every value rides as TEXT and the DATABASE casts it to the
     * column's type (F7.2); an empty or {@code ---null---} cell, or one past
     * the row's end, is NULL. Null when no rows. */
    public static @com.legend.base.Nullable RowLoad rowLoad(@com.legend.base.Nullable String schema,
            String table, String[] cols, List<String[]> rows) {
        if (rows.isEmpty()) {
            return null;
        }
        List<String> names = new ArrayList<>(cols.length);
        for (String c : cols) {
            names.add(ident(c.strip()));
        }
        List<List<String>> out = new ArrayList<>(rows.size());
        for (String[] vals : rows) {
            List<String> row = new ArrayList<>(cols.length);
            for (int c = 0; c < cols.length; c++) {
                String tok = c < vals.length ? vals[c].strip() : "";
                row.add(tok.isEmpty() || tok.equals("---null---") ? null : tok);
            }
            out.add(row);
        }
        return new RowLoad(schema, table, names, cols.length, out);
    }

    /** The from() node's {@code testDataSetupCsv} FACTS as seed SQL — the
     * executor's half against the store (the compiler only records the
     * block and its database). */
    public static List<Step> setupSteps(
            com.legend.compiler.spec.typed.TypedFrom fr,
            com.legend.compiler.element.ModelContext ctx, com.legend.sql.dialect.SqlDialect dialect) {
        List<Step> out = new java.util.ArrayList<>();
        for (var c : fr.csvSetups()) {
            String db = c.dbFqn() != null && ctx.findDatabase(c.dbFqn()).isPresent()
                    ? c.dbFqn() : null;
            out.addAll(steps(c.csv(), db, ctx, dialect));
        }
        return out;
    }

    /** The test data an ELEMENT runtime's connections declare — every
     *  {@code LocalH2 { testDataSetupSqls; testDataSetupCSV }} bound under
     *  it, as the SQL the platform establishes the session with (the CSV
     *  typed from the bound store's parsed tables, {@link CsvSeed}).
     *  Before 2026-09-16 only the Pure-INSTANCE runtime form seeded; a
     *  declared connection's data was parsed and carried but never run. */
    public static List<Step> declaredSteps(String runtimeFqn,
            ModelContext ctx, com.legend.sql.dialect.SqlDialect dialect) {
        List<Step> out = new ArrayList<>();
        java.util.Optional<com.legend.model.RuntimeDefinition> rt = ctx.findRuntime(runtimeFqn);
        if (rt.isEmpty()) {
            return out;
        }
        for (var binding : rt.get().connectionBindings().entrySet()) {
            String store = binding.getKey();
            for (String connFqn : binding.getValue()) {
                ctx.findConnection(connFqn).ifPresent(cd -> {
                    if (cd.specification()
                            instanceof com.legend.model.ConnectionSpecification.LocalH2 h2) {
                        if (h2.testDataSetupSqls() != null) {
                            for (String sql : h2.testDataSetupSqls()) {
                                out.add(new Step.Sql(sql));
                            }
                        }
                        if (h2.testDataSetupCsv() != null) {
                            String db = ctx.findDatabase(store).isPresent() ? store : null;
                            out.addAll(steps(h2.testDataSetupCsv(), db, ctx, dialect));
                        }
                    }
                });
            }
        }
        return out;
    }
}
