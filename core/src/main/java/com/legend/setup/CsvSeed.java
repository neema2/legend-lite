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
        return steps(csvBlocks, dbFqn == null ? List.of() : List.of(dbFqn), ctx, dialect);
    }

    /** {@link #steps}, each block's schema and table looked up in {@code dbFqns} -- every database a connection
     *  serves, in order, as upstream's {@code setUpDataSQLs(csv, [db1, db2])} looks them up in a database that includes
     *  them all (the B2d.3 audit's M3) -- or, with none, the CSV's names as their text. */
    public static List<Step> steps(String csvBlocks, List<String> dbFqns, ModelContext ctx,
            com.legend.sql.dialect.SqlDialect dialect) {
        List<Step> out = new ArrayList<>();
        // block separators: a line of dashes — '-' (the Alloy '\n-\n'
        // form) or '-----' (the testDataGeneration CSV form)
        StringBuilder block = new StringBuilder();
        for (String line : records(csvBlocks, true)) {
            if (line.strip().matches("-+")) {
                blockSqls(block.toString(), dbFqns, ctx, dialect, out);
                block.setLength(0);
            } else {
                if (block.length() > 0) {
                    block.append('\n');
                }
                block.append(line);
            }
        }
        blockSqls(block.toString(), dbFqns, ctx, dialect, out);
        return out;
    }

    private static void blockSqls(String csv, List<String> dbFqns, ModelContext ctx,
            com.legend.sql.dialect.SqlDialect dialect, List<Step> out) {
        String[] lines = records(csv, false).toArray(String[]::new);
        while (lines.length > 0 && lines[0].isEmpty()) {
            lines = java.util.Arrays.copyOfRange(lines, 1, lines.length);
        }
        if (lines.length < 3) {
            return;
        }
        // the schema and table lines read as written, as upstream reads them (the user's G7 ruling, register S302)
        String schema = lines[0];
        String table = lines[1];
        boolean defaultSchema = "default".equals(schema);
        String[] cols = cells(lines[2]);
        // the table where its schema declares it, the default schema's by "default." too: a bare name would find a
        // named schema's table of that name, which upstream's schema('default')->table(T) does not (the B2d.3 audit's
        // L1); the first database that declares it, as upstream's lookup over a database including them all
        var def = java.util.Optional.<com.legend.model.DatabaseDefinition.TableDefinition>empty();
        for (String db : dbFqns) {
            // the schema and table apart: a name holding a dot is itself (stream S, 2026-11-14; "s.t" and "x" found
            // the schema s's table "t.x" while the dotted spelling was split at its first dot)
            def = ctx.findTableDefinition(db, schema, table);
            if (def.isPresent()) {
                break;
            }
        }
        String dbNames = String.join(", ", dbFqns);
        if (!dbFqns.isEmpty()) {
            declared(ctx, dbFqns, schema, table, defaultSchema, def.isPresent());
        }
        if (def.isPresent()) {
            // schema-qualified CSV tables need their schema first (the
            // inline-CSV lane's creation half, FULL_RESIDUE_CENSUS §9a:
            // the engine's own lane creates model-derived tables on a
            // fresh database; TEST_SCHEMA.PEOPLE has no authored setup).
            // IF NOT EXISTS: idempotent on both engines and on mirrors
            // that already carry the schema.
            out.addAll(declaredTable(defaultSchema ? null : schema, def.get()));
        } else {
            // no database named: the CSV's names are its text, spelled as bare names (with one named, a schema or table
            // it does not declare is refused above, as upstream's loader refuses it)
            out.add(new Step.Clear(new com.legend.sql.SqlDml.DeleteAll(defaultSchema ? null : schema, table,
                    com.legend.sql.NameQuoting.BARE)));
        }
        // F7.5: ONE multi-row INSERT per block — the statement count is
        // the seed cost (task #14: per-statement parse+plan+JNI), and
        // both H2 (mirror replay) and DuckDB accept multi-row VALUES
        List<String[]> rows = new ArrayList<>();
        for (int i = 3; i < lines.length; i++) {
            if (!lines[i].isEmpty()) {
                rows.add(cells(lines[i]));
            }
        }
        // a declared table's columns as the header names them, matched as upstream's loader matches them, written by
        // their declared names (quoted ones keep their case); with no declaration, the CSV's names spelled as bare names
        List<com.legend.model.DatabaseDefinition.ColumnDefinition> named = def.isPresent()
                ? columnsOf(def.get(), table, dbNames, cols) : null;
        String[] names = named == null ? cols
                : named.stream().map(com.legend.model.DatabaseDefinition.ColumnDefinition::name).toArray(String[]::new);
        // the rows into the table the DDL above made: the definition's own name when one is declared
        RowLoad load = rowLoad(defaultSchema ? null : schema, def.map(d -> d.name()).orElse(table),
                def.map(d -> Ddl.quoting(d, defaultSchema ? null : schema)).orElse(com.legend.sql.NameQuoting.BARE),
                names, def.map(CsvSeed::quotedColumns).orElse(java.util.Set.of()), rows,
                named == null ? List.of() : types(named));
        if (load != null) {
            out.add(new Step.Rows(load));
        }
    }

    /** A declared table made empty, as a seed makes it before its rows: its schema (when named), then the table dropped
     *  and created as its store declares it. TYPED -- the table's definition, its names its own -- so a caller with no
     *  test-data text (a host's file into a declared table, {@code LoadPlanner.hostFile}) passes no name through CSV
     *  text: until 2026-11-14 that caller joined the declared column names into a header line this reader split again,
     *  and a name holding a comma, a line break or a leading quote was read as another name (stream S's fileLoad pin,
     *  {@code DeclaredDdlNamesTest}). {@code schema}: null for the default schema. */
    public static List<Step> declaredTable(@com.legend.base.Nullable String schema,
            com.legend.model.DatabaseDefinition.TableDefinition def) {
        List<Step> out = new ArrayList<>();
        if (schema != null) {
            out.add(new Step.Schema(new com.legend.sql.SqlDdl.CreateSchema(schema, def.schemaQuoted())));
        }
        // DROP-then-CREATE, never CREATE OR REPLACE: H2 (2.1.214, the
        // engine's own target) has no OR REPLACE for tables — this was
        // the recorded root cause of ~39 'Table already exists' H2
        // replay declines (H2_BACKEND.md §12 step 2); DuckDB accepts
        // the two-statement form identically
        out.add(new Step.Schema(Ddl.dropDeclaredTable(def, schema)));
        // THE ONE DDL PRODUCER: the store's declared column types,
        // spelled for the target (the engine creates what the store
        // declares — its setUpDataSQLs reads the metamodel's types)
        out.add(new Step.Schema(Ddl.createTable(def, schema)));
        return out;
    }

    /** A block's schema and table, looked up as upstream's loader looks them up in {@code dbFqns} and their includes
     *  ({@code loadCsvDataToDbTable}, databaseHelperFunctions.pure: a database including them all, its names joined by
     *  {@code ", "}): a schema none declares, or a table its schema does not declare, refused with upstream's words. */
    private static void declared(ModelContext ctx, List<String> dbFqns, String schema, String table,
            boolean defaultSchema, boolean tableDeclared) {
        String dbNames = String.join(", ", dbFqns);
        if (!defaultSchema && dbFqns.stream().noneMatch(db -> schemaDeclared(ctx, db, schema,
                new java.util.HashSet<>()))) {
            throw new com.legend.error.AssertFailed("There should be exactly 1 schema with name " + schema
                    + " found in the provided runtime database(s) [" + dbNames + "] at the given connection.");
        }
        if (!tableDeclared) {
            throw new com.legend.error.AssertFailed("Table \"" + table + "\" not found in Schema \"" + schema
                    + "\" in Database(s) " + dbNames);
        }
    }

    private static boolean schemaDeclared(ModelContext ctx, String dbFqn, String name, java.util.Set<String> seen) {
        if (!seen.add(dbFqn)) {
            return false;
        }
        com.legend.model.DatabaseDefinition db = ctx.findDatabase(dbFqn).orElse(null);
        if (db == null) {
            return false;
        }
        for (com.legend.model.DatabaseDefinition.SchemaDefinition sc : db.schemas()) {
            if (sc.name().equals(name)) {
                return true;
            }
        }
        for (String include : db.includes()) {
            if (schemaDeclared(ctx, include, name, seen)) {
                return true;
            }
        }
        return false;
    }

    /**
     * The RECORDS of a test-data text (the user's ruling 3, 2026-11-10, B_BRIEF §9: "faithful to the real data, line
     * breaks included", read "with the same quote-aware record reader the file loads use"; register S191; built by
     * B2d.2, 2026-11-11): the file loader's rules (the CSV format's default, legend-pure's {@code CSVFormat.EXCEL}; the
     * reader {@code base.CsvCells} held them until 00e67e38) -- a cell is QUOTED only where it STARTS with a double
     * quote: its text runs to the closing quote, a doubled quote one quote, its line breaks kept; after the closing
     * quote only whitespace may come before the comma or the record's end; any other cell is its text exactly, a quote
     * inside it a character. A record ends at a line break outside a quoted cell. A text that ends inside a quoted cell,
     * or has text between a closing quote and the next comma, is refused by name ({@link Malformed}), never repaired.
     * {@code trailing}: whether a trailing empty record is kept ({@code split("\n", -1)}) or dropped
     * ({@code split("\n")}); a text with no quote splits on every line break, as it always did.
     */
    static List<String> records(String given, boolean trailing) {
        // a CRLF line ending is a line break (the user's G7 ruling, 2026-11-12, "Def want windows files accepted";
        // register S302): its '\r' belongs to the line ending, never to a value or a name -- upstream splits on '\n'
        // alone, so a CRLF text fails at its first block's schema lookup there
        String text = given.replace("\r\n", "\n");
        List<String> out = new ArrayList<>();
        if (text.indexOf('"') < 0) {
            out.addAll(java.util.Arrays.asList(trailing ? text.split("\n", -1) : text.split("\n")));
            return out;
        }
        int start = 0;
        int i = 0;
        int line = 1;
        boolean cellStart = true;
        while (i < text.length()) {
            char c = text.charAt(i);
            if (cellStart && c == '"') {
                int opened = line;
                int close = closingQuote(text, i + 1);
                if (close < 0) {
                    throw new Malformed("the test data ends inside the quoted cell that starts on line " + opened);
                }
                for (int k = i + 1; k < close; k++) {
                    if (text.charAt(k) == '\n') {
                        line++;
                    }
                }
                i = afterClosingQuote(text, close + 1, line);
                cellStart = false;
                continue;
            }
            if (c == '\n') {
                out.add(text.substring(start, i));
                start = i + 1;
                line++;
                cellStart = true;
            } else {
                cellStart = c == ',';
            }
            i++;
        }
        out.add(text.substring(start));
        if (!trailing) {
            while (!out.isEmpty() && out.get(out.size() - 1).isEmpty()) {
                out.remove(out.size() - 1);
            }
        }
        return out;
    }

    /** The index of the quote that closes a quoted cell whose text starts at {@code from} (a doubled quote is one
     *  quote of the text), or -1 where the text ends inside the cell. */
    private static int closingQuote(String text, int from) {
        for (int k = from; k < text.length(); k++) {
            if (text.charAt(k) == '"') {
                if (k + 1 < text.length() && text.charAt(k + 1) == '"') {
                    k++;
                    continue;
                }
                return k;
            }
        }
        return -1;
    }

    /** The index of the comma, line break or end that follows a closing quote at {@code from - 1}, whitespace between
     *  them dropped; anything else between them refused by name. */
    private static int afterClosingQuote(String text, int from, int line) {
        int k = from;
        while (k < text.length() && text.charAt(k) != ',' && text.charAt(k) != '\n') {
            if (!Character.isWhitespace(text.charAt(k))) {
                throw new Malformed("the test data's line " + line + " has '" + text.charAt(k) + "' between a quoted"
                        + " cell's closing quote and the next comma");
            }
            k++;
        }
        return k;
    }

    /** A test-data text that is not a CSV text by the file loader's rules (a malformed quoted cell): refused by name,
     *  never repaired. */
    public static final class Malformed extends IllegalArgumentException {
        private static final long serialVersionUID = 1L;

        Malformed(String message) {
            super(message);
        }
    }

    /** One table's block of a test-data text, as lite's reader reads it: its schema, its table, its header's cells
     *  and its rows, each cell as written (its quotes read by {@link #records}' rules), nothing normalized: a reader that
     *  compares or loads the cells applies its own rule to them. */
    public record Block(String schema, String table, List<String> header, List<List<String>> rows) {
        public Block {
            header = List.copyOf(header);
            rows = List.copyOf(rows);
        }
    }

    /** {@code csvBlocks}'s blocks, read as {@link #steps} reads them ({@link #records}): blocks separated by a line of
     *  dashes, each its schema, table and header lines, then its non-blank rows; a block of fewer than three lines is
     *  none. */
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
            while (first < block.size() && block.get(first).isEmpty()) {
                first++;
            }
            List<String> lines = block.subList(first, block.size());
            if (lines.size() >= 3) {
                List<List<String>> rows = new ArrayList<>();
                for (String r : lines.subList(3, lines.size())) {
                    if (!r.isEmpty()) {
                        rows.add(List.of(cells(r)));
                    }
                }
                // the schema and table lines as written (register S302)
                out.add(new Block(lines.get(0), lines.get(1), List.of(cells(lines.get(2))), rows));
            }
            block = new ArrayList<>();
        }
        return out;
    }

    /** One record's cells by the rules of {@link #records}: a cell that starts with a double quote is quoted (its
     *  commas and line breaks kept, a doubled quote one quote, whitespace after its closing quote dropped), any other
     *  cell its text exactly; a line with no quote splits on every comma, as it always did. */
    static String[] cells(String line) {
        if (line.indexOf('"') < 0) {
            return line.split(",", -1);
        }
        List<String> out = new ArrayList<>();
        int i = 0;
        while (true) {
            if (i < line.length() && line.charAt(i) == '"') {
                int close = closingQuote(line, i + 1);
                if (close < 0) {
                    throw new Malformed("the test data ends inside a quoted cell of the record '" + line + "'");
                }
                out.add(line.substring(i + 1, close).replace("\"\"", "\""));
                i = afterClosingQuote(line, close + 1, 1);
            } else {
                int comma = line.indexOf(',', i);
                int end = comma < 0 ? line.length() : comma;
                out.add(line.substring(i, end));
                i = end;
            }
            if (i >= line.length()) {
                return out.toArray(String[]::new);
            }
            i++;
        }
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
                // a cell as written: its spaces kept, as upstream loads it (register S302); an empty or ---null---
                // cell NULL (convertValuesToCsv)
                String tok = c < vals.length ? vals[c] : "";
                row.add(tok.isEmpty() || tok.equals("---null---") ? null : tok);
            }
            out.add(row);
        }
        return new RowLoad(schema, table, quoting, names, quoted, cols.length, out, types);
    }

    /** The column of {@code def} each of the CSV's header cells {@code cols} names, in the CSV's order, matched as
     *  upstream's loader matches it ({@code loadCsvDataToDbTable}: the header trimmed, both names' matching quotes
     *  stripped, any case, the first column that matches): a header no column matches refused with upstream's words.
     *  The header is the user's text naming a declared column, which upstream resolves so, so lite does (the
     *  orchestrator's go, 2026-11-12: exactness, no ruling needed). */
    static List<com.legend.model.DatabaseDefinition.ColumnDefinition> columnsOf(
            com.legend.model.DatabaseDefinition.TableDefinition def, String table, String dbFqn, String[] cols) {
        List<com.legend.model.DatabaseDefinition.ColumnDefinition> out = new ArrayList<>(cols.length);
        for (String c : cols) {
            String name = stripMatchingQuotes(c.strip());
            com.legend.model.DatabaseDefinition.ColumnDefinition found = null;
            for (var col : def.columns()) {
                if (stripMatchingQuotes(col.name()).equalsIgnoreCase(name)) {
                    found = col;
                    break;
                }
            }
            if (found == null) {
                throw new com.legend.error.AssertFailed("Column \"" + c + "\" not found in Table \"" + table
                        + "\" in Database(s) " + dbFqn);
            }
            out.add(found);
        }
        return out;
    }

    /** {@code name} without its enclosing double quotes, where it has both (upstream's {@code stripMatchingQuotes}). */
    private static String stripMatchingQuotes(String name) {
        return name.length() > 1 && name.startsWith("\"") && name.endsWith("\"")
                ? name.substring(1, name.length() - 1) : name;
    }

    /** The declared type of each of {@code columns}, in order. */
    static List<com.legend.sql.SqlDdl.ColumnType> types(
            List<com.legend.model.DatabaseDefinition.ColumnDefinition> columns) {
        List<com.legend.sql.SqlDdl.ColumnType> out = new ArrayList<>(columns.size());
        for (var col : columns) {
            out.add(com.legend.compiler.element.StoreCompiler.declaredType(col.dataType()));
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
        // each connection ONCE, against every store it serves (the B2d.3 audit's M3: upstream's
        // processRuntimeTestConnections, storeContract.pure ~119-141, loads each distinct connection's test data once
        // with setUpDataSQLs(csv, its databases) -- until then lite loaded it per store binding, so a CSV holding the
        // tables of two databases one connection serves was refused against each alone, and its SQL ran twice)
        java.util.Map<String, List<String>> storesOf = new java.util.LinkedHashMap<>();
        for (var binding : rt.get().connectionBindings().entrySet()) {
            for (String connFqn : binding.getValue()) {
                storesOf.computeIfAbsent(connFqn, k -> new ArrayList<>()).add(binding.getKey());
            }
        }
        for (var served : storesOf.entrySet()) {
            ctx.findConnection(served.getKey()).ifPresent(cd -> {
                com.legend.model.ConnectionSpecification.TestData data = cd.specification().testData();
                if (data != null) {
                    if (data.sqls() != null) {
                        for (String sql : data.sqls()) {
                            // written in the database the connection declares (D2)
                            out.add(new Step.Sql(sql, cd.databaseType().name()));
                        }
                    }
                    if (data.csv() != null) {
                        List<String> dbs = served.getValue().stream().filter(s -> ctx.findDatabase(s).isPresent())
                                .distinct().toList();
                        out.addAll(steps(data.csv(), dbs, ctx, dialect));
                    }
                }
            });
        }
        return out;
    }

}
