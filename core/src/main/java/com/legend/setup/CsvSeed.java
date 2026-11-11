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
 * through the platform) or as a plan's load ({@code planner.LoadPlanner}; exec runs it, {@code PlanRunner.seed}), which runs the steps
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
        List<String> out = new ArrayList<>();
        for (Step st : steps(csvBlocks, dbFqn, ctx, dialect)) {
            out.addAll(st.texts(dialect));
        }
        return out;
    }

    /** One step of a seed, in order: a statement its author wrote ({@link Sql}), a statement the platform writes
     *  ({@link Schema}: a table's or schema's DDL; {@link Clear}: an undeclared table emptied), or a table's rows
     *  ({@link Rows}: inserts of the dialect's typed literals, a content blob in a plan). */
    public sealed interface Step permits Step.Sql, Step.Schema, Step.Clear, Step.Rows {
        /** A statement: written for the database {@code writtenFor} (its connection's declared type, by name;
         *  decision D2), or by the session's own dialect ({@code writtenFor} null). */
        record Sql(String text, @com.legend.base.Nullable String writtenFor) implements Step {

            /** The statement as the session runs it: the engine's H2 text translated, any other as written. */
            public String runOn(com.legend.sql.dialect.SqlDialect dialect) {
                return dialect.raw(text, writtenFor);
            }
        }

        /** DDL the platform writes from the store's declaration: spelled by each database's own dialect. */
        record Schema(com.legend.sql.SqlDdl ddl) implements Step {
        }

        /** A table no store declares, emptied before its rows: spelled by each database's own dialect. */
        record Clear(com.legend.sql.SqlDml.DeleteAll clear) implements Step {
        }

        record Rows(RowLoad load) implements Step {
        }

        /** The step as SQL statements, written by {@code dialect}: an author's statement as written. */
        default List<String> texts(com.legend.sql.dialect.SqlDialect dialect) {
            return switch (this) {
                case Sql q -> List.of(q.text());
                case Schema d -> List.of(dialect.render(d.ddl()));
                case Clear c -> List.of(dialect.render(c.clear()));
                case Rows r -> r.load().statements(dialect);
            };
        }
    }

    /** A seed as the plan runs it ({@code steps}), beside THE REFEREE'S COPY ({@code referee}): the same load written
     *  in the replay referee's own dialect, one step for each of the plan's (docs/DATA_DELIVERY_DESIGN_2026_11_10.md
     *  §1.1, A2: "the planner renders the same rows a second time in H2's dialect and carries that copy as the edge's
     *  data"; B2b, 2026-11-11: a load, its rows a content blob by digest, as the plan's are -- until then a list of
     *  texts) -- the platform's DDL and the rows' inserts the referee's, an author's statement as its author wrote it. */
    public record Planned(LoadSteps steps, java.util.function.Supplier<LoadSteps> referee) {
        public Planned {
            java.util.Objects.requireNonNull(referee, "referee");
        }

        /** The referee's copy, written when an edge first asks for it, once (the A checkpoint's tenets audit, 8,
         *  2026-11-10: demand-driven, as {@code EffectPlanner}'s copy of a file's load is; until then every test plan
         *  wrote every seed's copy in H2's dialect whether or not a judgment could reach the referee). */
        public LoadSteps refereeLoad() {
            return referee.get();
        }
    }

    /** {@code make}'s value, made on its first {@link #get} and kept. */
    private static final class Once implements java.util.function.Supplier<LoadSteps> {
        private final java.util.function.Supplier<LoadSteps> make;
        private volatile @com.legend.base.Nullable LoadSteps made;

        Once(java.util.function.Supplier<LoadSteps> make) {
            this.make = make;
        }

        @Override
        public synchronized LoadSteps get() {
            LoadSteps m = made;
            if (m == null) {
                m = make.get();
                made = m;
            }
            return m;
        }
    }

    /**
     * {@code steps} as a plan's load steps, final for {@code dialect}'s database: each statement of a SQL step
     * (written in H2's spelling, as legend-engine's test data is) adapted to the database, the platform's DDL in the
     * database's spelling, and each table's rows as one content blob of inserts of the dialect's typed literals
     * ({@link LoadSteps#rows}). The one owner of that form for a plan's target and for the body driver's seeds (the
     * doors, 2026-10-29; until then {@code PlanMaker.setup} and {@code exec.SetupRunner} each made it).
     */
    public static LoadSteps planned(List<Step> steps, com.legend.sql.dialect.SqlDialect dialect) {
        LoadSteps.Builder out = new LoadSteps.Builder();
        for (Step step : steps) {
            switch (step) {
                case Step.Sql blob -> {
                    for (String statement : com.legend.sql.RawSql.splitStatements(blob.text())) {
                        out.statement(new Step.Sql(statement, blob.writtenFor()).runOn(dialect));
                    }
                }
                case Step.Schema d -> out.statement(dialect.render(d.ddl()));
                case Step.Clear c -> out.statement(dialect.render(c.clear()));
                case Step.Rows rows -> out.add(LoadSteps.rows(rows.load(), dialect));
            }
        }
        return out.build();
    }

    /** {@link #planned}, with the referee's copy of every step written by {@code referee} (the replay referee's
     *  dialect, its caller's to name: this library reaches no database's registry): ONE step of the copy per load step,
     *  in the steps' order, as the edge records the copy's steps by the places of the plan's that ran
     *  ({@code CompatEdge.seeded}) -- a table's rows one blob of the referee's inserts, however many it holds. */
    public static Planned planned(List<Step> steps, com.legend.sql.dialect.SqlDialect dialect,
            com.legend.sql.dialect.SqlDialect referee) {
        LoadSteps planned = planned(steps, dialect);
        List<Step> kept = List.copyOf(steps);
        return new Planned(planned, new Once(() -> {
            LoadSteps.Builder copy = new LoadSteps.Builder();
            for (Step step : kept) {
                switch (step) {
                    // an author's statements as written: the referee runs the engine's H2 text
                    case Step.Sql blob -> com.legend.sql.RawSql.splitStatements(blob.text()).forEach(copy::statement);
                    case Step.Rows rows -> copy.add(LoadSteps.blob(rows.load().refereeStatements(referee),
                            "the data of the table " + rows.load().where()));
                    case Step.Schema d -> step.texts(referee).forEach(copy::statement);
                    case Step.Clear c -> step.texts(referee).forEach(copy::statement);
                }
            }
            LoadSteps built = copy.build();
            if (planned.steps().size() != built.steps().size()) {
                throw new IllegalStateException("a seed's " + planned.steps().size() + " step(s) and its referee's "
                        + built.steps().size() + " step(s): the edge records the copy's steps by the places of the"
                        + " plan's that ran");
            }
            return built;
        }));
    }

    /** The seed as {@link Step}s &mdash; {@link #sqls}' statements, the rows
     *  still rows. */
    public static List<Step> steps(String csvBlocks, @com.legend.base.Nullable String dbFqn,
            ModelContext ctx, com.legend.sql.dialect.SqlDialect dialect) {
        List<Step> out = new ArrayList<>();
        // block separators: a line of dashes — '-' (the Alloy '\n-\n'
        // form) or '-----' (the testDataGeneration CSV form)
        StringBuilder block = new StringBuilder();
        for (String line : records(csvBlocks, true)) {
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
        String[] lines = records(csv, false).toArray(String[]::new);
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
                out.add(new Step.Schema(new com.legend.sql.SqlDdl.CreateSchema(schema, def.get().schemaQuoted())));
            }
            // DROP-then-CREATE, never CREATE OR REPLACE: H2 (2.1.214, the
            // engine's own target) has no OR REPLACE for tables — this was
            // the recorded root cause of ~39 'Table already exists' H2
            // replay declines (H2_BACKEND.md §12 step 2); DuckDB accepts
            // the two-statement form identically
            out.add(new Step.Schema(Ddl.dropDeclaredTable(def.get(), defaultSchema ? null : schema)));
            // THE ONE DDL PRODUCER: the store's declared column types,
            // spelled for the target (the engine creates what the store
            // declares — its setUpDataSQLs reads the metamodel's types)
            out.add(new Step.Schema(Ddl.createTable(def.get(), defaultSchema ? null : schema)));
        } else {
            // no declaration: the CSV's names are its text, spelled as bare names
            out.add(new Step.Clear(new com.legend.sql.SqlDml.DeleteAll(defaultSchema ? null : schema, table,
                    com.legend.sql.NameQuoting.BARE)));
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
                cols, def.map(CsvSeed::quotedColumns).orElse(java.util.Set.of()), rows,
                def.map(d -> types(d, schema, cols)).orElse(List.of()));
        if (load != null) {
            out.add(new Step.Rows(load));
        }
    }

    /**
     * The RECORDS of a test-data text, quote-aware (the user's ruling 3, 2026-11-10, B_BRIEF §9: "faithful to the real
     * data, line breaks included"; register S191; built by B2d.2, 2026-11-11): split on line breaks outside a quoted
     * cell, so a quoted cell keeps its line breaks -- the generator writes such a cell (RFC 4180) and the text reads
     * back. A quote opens or closes a quoted run wherever it stands, as {@link #cells} reads it (a doubled quote inside
     * one closes and reopens it); a text with no quote splits on every line break, as it always did. {@code trailing}:
     * whether a trailing empty record is kept ({@code split("\n", -1)}) or dropped ({@code split("\n")}).
     */
    static List<String> records(String text, boolean trailing) {
        List<String> out = new ArrayList<>();
        if (text.indexOf('"') < 0) {
            out.addAll(java.util.Arrays.asList(trailing ? text.split("\n", -1) : text.split("\n")));
            return out;
        }
        boolean quoted = false;
        int start = 0;
        for (int i = 0; i < text.length(); i++) {
            char c = text.charAt(i);
            if (c == '"') {
                quoted = !quoted;
            } else if (c == '\n' && !quoted) {
                out.add(text.substring(start, i));
                start = i + 1;
            }
        }
        out.add(text.substring(start));
        if (!trailing) {
            while (!out.isEmpty() && out.get(out.size() - 1).isEmpty()) {
                out.remove(out.size() - 1);
            }
        }
        return out;
    }

    /** One table's block of a test-data text, as lite's reader reads it: its schema, its table, its header's names
     *  and its rows, each cell as written (its quotes read, RFC 4180), an empty or {@code ---null---} cell null --
     *  the engine's absent cell ({@code Ddl}'s {@code NULL_CELLS}). */
    public record Block(String schema, String table, List<String> header, List<List<String>> rows) {
        public Block {
            header = List.copyOf(header);
            rows = List.copyOf(rows);
        }
    }

    /** {@code csvBlocks}'s blocks, read as {@link #steps} reads them (quote-aware: {@link #records}): blocks separated
     *  by a line of dashes, each its schema, table and header lines, then its non-blank rows; a block of fewer than
     *  three lines is none. */
    public static List<Block> blocks(String csvBlocks) {
        List<Block> out = new ArrayList<>();
        List<String> block = new ArrayList<>();
        List<String> all = new ArrayList<>(records(csvBlocks, true));
        all.add("-");
        for (String line : all) {
            if (!line.strip().matches("-+")) {
                block.add(line);
                continue;
            }
            int first = 0;
            while (first < block.size() && block.get(first).strip().isEmpty()) {
                first++;
            }
            List<String> lines = block.subList(first, block.size());
            if (lines.size() >= 3) {
                List<String> header = new ArrayList<>();
                for (String h : cells(lines.get(2))) {
                    header.add(h.strip());
                }
                List<List<String>> rows = new ArrayList<>();
                for (String r : lines.subList(3, lines.size())) {
                    if (r.strip().isEmpty()) {
                        continue;
                    }
                    List<String> row = new ArrayList<>();
                    for (String cell : cells(r)) {
                        row.add(cell.isEmpty() || cell.equals("---null---") ? null : cell);
                    }
                    rows.add(java.util.Collections.unmodifiableList(row));
                }
                out.add(new Block(lines.get(0).strip(), lines.get(1).strip(), header, rows));
            }
            block = new ArrayList<>();
        }
        return out;
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
        return rowLoad(schema, table, quoting, cols, quotedColumns, rows, List.of());
    }

    /** {@link #rowLoad}, each column typed by {@code types} (empty: none declared). */
    public static @com.legend.base.Nullable RowLoad rowLoad(@com.legend.base.Nullable String schema, String table,
            com.legend.sql.NameQuoting quoting, String[] cols, java.util.Set<String> quotedColumns,
            List<String[]> rows, List<com.legend.sql.SqlDdl.ColumnType> types) {
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
        return new RowLoad(schema, table, quoting, names, quoted, cols.length, out, types);
    }

    /** The declared type of each of the CSV's columns {@code cols} in {@code def}, in the CSV's order: a column the
     *  store declared quoted matched by its exact name, any other by its name in any case (the database folds it). A
     *  column the table does not declare is refused by name: its rows would be a guess. */
    static List<com.legend.sql.SqlDdl.ColumnType> types(com.legend.model.DatabaseDefinition.TableDefinition def,
            String schema, String[] cols) {
        List<com.legend.sql.SqlDdl.ColumnType> out = new ArrayList<>(cols.length);
        for (String c : cols) {
            String name = c.strip();
            com.legend.model.DatabaseDefinition.ColumnDefinition found = null;
            for (var col : def.columns()) {
                if (col.quoted() ? col.name().equals(name) : col.name().equalsIgnoreCase(name)) {
                    found = col;
                    break;
                }
            }
            if (found == null) {
                throw new com.legend.error.DataError("the data for the table " + ("default".equals(schema) ? ""
                        : schema + ".") + def.name() + " names the column " + name + ", which the table does not"
                        + " declare");
            }
            out.add(com.legend.compiler.element.StoreCompiler.declaredType(found.dataType()));
        }
        return out;
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
