// SPDX-License-Identifier: Apache-2.0

package com.legend.setup;

import com.legend.compiler.element.ModelContext;

import java.util.ArrayList;
import java.util.List;

/**
 * The engine's {@code setUpDataSQLsV2} semantics: CSV seed blocks
 * ({@code schema\ntable\nheader\nrows...}, blocks joined by {@code \n-\n})
 * become DDL + INSERT statements derived from the PARSED store's column
 * types ({@link ModelContext#findTable}). Returns SQL text and rows — execution
 * stays with the caller's executeInDb loop (the corpus's own
 * {@code setupTestData} body maps the strings through executeInDb;
 * audit 19d B5 moved this synthesis out of the harness so that body runs
 * through the platform) or with {@code exec.PlanRunner.setUp}, which runs the steps
 * on a session. Plan side since 2026-10-08 (docs/EXECUTION_PLAN_BOUNDARY_2026_10_05.md §9): text only, no
 * connection, so that the planner and the browser planner can write the same setup the server runs.
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
     *  rows to load ({@code exec.Statements#load}, the session's bulk load &mdash; an engine's
     *  bulk API when it has one; its text is the one multi-row insert). */
    public sealed interface Step permits Step.Sql, Step.Rows {
        /** A statement: written for the database {@code writtenFor} (its connection's declared type, by name;
         *  decision D2), or by the session's own dialect ({@code writtenFor} null). */
        record Sql(String text, @com.legend.base.Nullable String writtenFor) implements Step {

            /** The statement as the session runs it: the engine's H2 text translated, any other as written. */
            public String runOn(com.legend.sql.dialect.SqlDialect dialect) {
                return dialect.raw(text, writtenFor);
            }
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

    /** A step as the plan runs it, beside the text a referee's ledger records for it: a statement of a SQL step
     *  (the text as its author wrote it, before the dialect's translation), a rows step's INSERT text. */
    public record Planned(com.legend.executionplan.ExecutionPlan.SetupStep step, String text) {
    }

    /**
     * {@code steps} as the plan's setup steps, final for {@code dialect}'s database: each statement of a SQL step
     * (written in H2's spelling, as legend-engine's test data is) adapted to the database, and each table's rows
     * either for the database's bulk loader ({@link com.legend.sql.dialect.SqlDialect#loadsRowsInBulk}) with the
     * statements that stage them, or as the one INSERT the database runs. The one owner of that choice for a plan's
     * target and for the body driver's seeds (the doors, 2026-10-29; until then {@code PlanMaker.setup} and
     * {@code exec.SetupRunner} each made it).
     */
    public static List<Planned> planned(List<Step> steps, com.legend.sql.dialect.SqlDialect dialect) {
        boolean bulk = dialect.loadsRowsInBulk();
        List<Planned> out = new ArrayList<>(steps.size());
        for (Step step : steps) {
            switch (step) {
                case Step.Sql blob -> {
                    for (String statement : com.legend.sql.RawSql.splitStatements(blob.text())) {
                        out.add(new Planned(new com.legend.executionplan.ExecutionPlan.SetupStep.Statement(
                                new Step.Sql(statement, blob.writtenFor()).runOn(dialect)), statement));
                    }
                }
                case Step.Rows rows -> {
                    RowLoad load = rows.load();
                    if (!load.rows().isEmpty()) {
                        String insert = dialect.render(load.values());
                        out.add(new Planned(bulk ? load.staged(dialect)
                                : new com.legend.executionplan.ExecutionPlan.SetupStep.Statement(insert), insert));
                    }
                }
            }
        }
        return out;
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
        while (lines.length > 0 && lines[0].strip().isEmpty()) {
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
                out.add(new Step.Sql(dialect.render(new com.legend.sql.SqlDdl.CreateSchema(schema,
                        def.get().schemaQuoted())), null));
            }
            // DROP-then-CREATE, never CREATE OR REPLACE: H2 (2.1.214, the
            // engine's own target) has no OR REPLACE for tables — this was
            // the recorded root cause of ~39 'Table already exists' H2
            // replay declines (H2_BACKEND.md §12 step 2); DuckDB accepts
            // the two-statement form identically
            out.add(new Step.Sql(dialect.render(Ddl.dropDeclaredTable(def.get(), defaultSchema ? null : schema)), null));
            // THE ONE DDL PRODUCER: the store's declared column types,
            // spelled for the target (the engine creates what the store
            // declares — its setUpDataSQLs reads the metamodel's types)
            out.add(new Step.Sql(dialect.render(Ddl.createTable(def.get(), defaultSchema ? null : schema)), null));
        } else {
            // no declaration: the CSV's names are its text, spelled as bare names
            out.add(new Step.Sql(dialect.render(new com.legend.sql.SqlDml.DeleteAll(
                    defaultSchema ? null : schema, table, com.legend.sql.NameQuoting.BARE)), null));
        }
        // F7.5: ONE multi-row INSERT per block — the statement count is
        // the seed cost (task #14: per-statement parse+plan+JNI), and
        // both H2 (mirror replay) and DuckDB accept multi-row VALUES
        List<String[]> rows = new ArrayList<>();
        for (int i = 3; i < lines.length; i++) {
            if (!lines[i].strip().isEmpty()) {
                rows.add(cells(lines[i]));
            }
        }
        // a declared table's names as its store declared them (quoted ones keep their case); with no declaration,
        // the CSV's names spelled as bare names
        RowLoad load = rowLoad(defaultSchema ? null : schema, table,
                def.map(d -> Ddl.quoting(d, defaultSchema ? null : schema)).orElse(com.legend.sql.NameQuoting.BARE),
                cols, def.map(CsvSeed::quotedColumns).orElse(java.util.Set.of()), rows);
        if (load != null) {
            out.add(new Step.Rows(load));
        }
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

    /** The names of {@code def}'s columns its store declared quoted ({@code "Amt"}). */
    public static java.util.Set<String> quotedColumns(com.legend.model.DatabaseDefinition.TableDefinition def) {
        java.util.Set<String> out = new java.util.LinkedHashSet<>();
        for (var c : def.columns()) {
            if (c.quoted()) {
                out.add(c.name());
            }
        }
        return out;
    }

    /** CSV cells as rows for {@code [schema.]table} &mdash; the table's and the columns' names as their text, with
     * which the store declared quoted ({@code quoting}, {@code quotedColumns}): the dialect spells each name once, as
     * its queries reference it ({@link com.legend.sql.dialect.SqlDialect#storedName}) &mdash; the seed's rows, shared
     * with the loadCsvToDbTable arm
     * (batch 85): every value rides as TEXT and the DATABASE casts it to the
     * column's type (F7.2); an empty or {@code ---null---} cell, or one past
     * the row's end, is NULL. Null when no rows. */
    public static @com.legend.base.Nullable RowLoad rowLoad(@com.legend.base.Nullable String schema, String table,
            com.legend.sql.NameQuoting quoting, String[] cols, java.util.Set<String> quotedColumns,
            List<String[]> rows) {
        if (rows.isEmpty()) {
            return null;
        }
        List<String> names = new ArrayList<>(cols.length);
        java.util.Set<String> quoted = new java.util.LinkedHashSet<>();
        for (String c : cols) {
            String name = c.strip();
            names.add(name);
            if (quotedColumns.contains(name)) {
                quoted.add(name);
            }
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
        return new RowLoad(schema, table, quoting, names, quoted, cols.length, out);
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
                    com.legend.model.ConnectionSpecification.TestData data = cd.specification().testData();
                    if (data != null) {
                        if (data.sqls() != null) {
                            for (String sql : data.sqls()) {
                                // written in the database the connection declares (D2)
                                out.add(new Step.Sql(sql, cd.databaseType().name()));
                            }
                        }
                        if (data.csv() != null) {
                            String db = ctx.findDatabase(store).isPresent() ? store : null;
                            out.addAll(steps(data.csv(), db, ctx, dialect));
                        }
                    }
                });
            }
        }
        return out;
    }

}
