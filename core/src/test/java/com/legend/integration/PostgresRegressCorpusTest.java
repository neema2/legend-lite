// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.integration;

import com.legend.testing.Lean;

import com.legend.protocol.sql.SqlAst;
import com.legend.protocol.sql.SqlLexer;
import com.legend.protocol.sql.SqlParser;
import com.legend.testcases.Psql;
import com.legend.testcases.SqlParity;
import com.legend.testing.EmbeddedPostgres;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.junit.jupiter.api.Test;

/**
 * The {@code #SQL{...}#} island over the PostgreSQL regression suite (the corpus program's first corpus, 2026-10-17:
 * the yardstick Postgres-compatible projects publish): its read-only statements, judged live against Postgres 16. Named
 * for Postgres, whose suite and database it runs (the dialect program's D11, 2026-10-18; it was PgRegressCorpusTest).
 *
 * <p>The suite under {@code -Dsql.corpus.regress} (a {@code src/test/regress} directory: {@code parallel_schedule},
 * {@code sql/}, {@code data/}) is replayed as {@code pg_regress} runs it, serially: {@code test_setup} first, then each
 * file of the schedule in order, each in its own session of one database, by {@link Psql}. Each read-only statement
 * ({@code SELECT}, {@code WITH}, {@code VALUES}, {@code TABLE}; not {@code SELECT ... INTO}, not a data-modifying
 * {@code WITH}) is judged where the script reaches it: Postgres's answer is the script's own; Postgres runs it a second
 * time (a different answer is NONDETERMINISTIC, judged no further; under a DateStyle the script set, the reference is
 * Postgres's answer under its default, ISO, MDY, as the island reads a date's text); then it runs as an island over a
 * Legend model of the tables it names, read from the catalog at that moment, on the same session, and the parity
 * harness judges the rows ({@link SqlParity#outcome}). The second run and the island run are rolled back (a savepoint
 * inside the script's transaction, else a transaction of their own) under a {@code statement_timeout}, so the script
 * runs on as written.
 *
 * <p>Statuses: SqlParity's (PASS, FAIL, BUG, REFUSED, ERROR, BOTH_ERROR), NAMES (the rows are Postgres's, an output
 * column's name is not), KNOWN (a wrong answer the {@link #KNOWN} ledger names, with why), REGISTERED (a difference
 * docs/SEMANTICS_REGISTER.md rules: S49's jsonb text of a json value), NONDETERMINISTIC, CATALOG (the island failed on
 * a statement that reads a system catalog, which no Legend model has), UNMODELED (the island failed on a statement that
 * reads a column of a type no Legend column models), and SKIP (the script's transaction was already aborted). FAIL and
 * BUG are wrong answers. The report -- every status counted, per file and in all, each failing status's messages
 * normalized and ranked -- is written to {@code -Dsql.corpus.report}. The suite is not this repository's: the lane runs
 * where the property names it (core/BUILD.bazel's {@code pg_regress_corpus_test}).
 */
class PostgresRegressCorpusTest {

    private record Judged(String file, int line, String status, long millis, String detail, String sql) {
    }

    private record Col(String name, String pgType, @com.legend.base.Nullable String legendType) {
    }

    @Test
    void regressionSuite() throws Exception {
        String root = System.getProperty("sql.corpus.regress");
        if (root == null) {
            throw new IllegalStateException("no -Dsql.corpus.regress: the lane runs where it names the suite"
                    + " (core/BUILD.bazel's pg_regress_corpus_test says how)");
        }
        Path dir = Path.of(root);
        String only = System.getProperty("sql.corpus.files");
        int timeoutSeconds = Integer.parseInt(System.getProperty("sql.corpus.timeout.s", "20"));
        EmbeddedPostgres pg = EmbeddedPostgres.shared();
        try (Connection admin = DriverManager.getConnection(pg.jdbcUrl("postgres"));
                Statement s = admin.createStatement()) {
            s.execute("DROP DATABASE IF EXISTS regression WITH (FORCE)");
            s.execute("CREATE DATABASE regression TEMPLATE template0 ENCODING 'UTF8' LC_COLLATE 'C' LC_CTYPE 'C'");
        }
        Map<String, String> vars = new HashMap<>();
        for (String v : List.of("abs_srcdir", "abs_builddir")) {
            vars.put(v, dir.toAbsolutePath().toString());
        }
        vars.put("libdir", "/nonexistent");
        vars.put("dlsuffix", ".so");
        vars.put("env:PG_ABS_SRCDIR", dir.toAbsolutePath().toString());
        vars.put("env:PG_ABS_BUILDDIR", dir.toAbsolutePath().toString());
        vars.put("env:PG_LIBDIR", "/nonexistent");
        vars.put("env:PG_DLSUFFIX", ".so");
        List<String> files = schedule(dir.resolve("parallel_schedule"));
        List<Judged> judged = new ArrayList<>();
        for (String file : files) {
            if (only != null && !file.equals("test_setup") && !Set.of(only.split(",")).contains(file)) {
                continue;
            }
            Path sql = dir.resolve("sql").resolve(file + ".sql");
            if (!Files.exists(sql)) {
                continue;
            }
            // decoded leniently: the suite's encoding tests are written in theirs (and skip themselves on UTF8)
            List<Psql.Item> items = Psql.parse(new String(Files.readAllBytes(sql), StandardCharsets.UTF_8));
            Map<String, String> fileVars = new HashMap<>(vars);
            Connection conn = DriverManager.getConnection(pg.jdbcUrl("regression"));
            List<Connection> opened = new ArrayList<>(List.of(conn));
            Psql.Runner runner = new Psql.Runner(conn, fileVars, () -> {
                try {
                    Connection c = DriverManager.getConnection(pg.jdbcUrl("regression"));
                    opened.add(c);
                    return c;
                } catch (SQLException e) {
                    throw new IllegalStateException(e);
                }
            }, timeoutSeconds);
            try {
                runner.run(items, new Psql.Listener() {
                    @Override
                    public boolean judges(String text) {
                        return readOnly(text);
                    }

                    @Override
                    public void sent(String text, Psql.Sql item, SqlParity.Answer answer) {
                        if (!readOnly(text)) {
                            return;
                        }
                        try {
                            judged.add(judge(file, item.line(), text, answer, runner.connection(),
                                    timeoutSeconds, pg.port()));
                        } catch (RuntimeException | StackOverflowError e) {
                            judged.add(new Judged(file, item.line(), "HARNESS", 0, e.getClass().getSimpleName()
                                    + ": " + e.getMessage(), text));
                        }
                    }
                });
            } finally {
                for (Connection c : opened) {
                    c.close();
                }
            }
            System.out.println("[pg-regress] " + file + ": " + counts(judged, file));
        }
        String report = summary(judged);
        System.out.println(report);
        String out = System.getProperty("sql.corpus.report");
        if (out != null) {
            Path o = Path.of(out);
            Files.createDirectories(o);
            Files.writeString(o.resolve("pg-regress.md"), report, StandardCharsets.UTF_8);
            StringBuilder tsv = new StringBuilder("file\tline\tstatus\tms\tdetail\tsql\n");
            for (Judged j : judged) {
                tsv.append(j.file()).append('\t').append(j.line()).append('\t').append(j.status()).append('\t')
                        .append(j.millis()).append('\t').append(clean(j.detail(), 3000)).append('\t')
                        .append(clean(j.sql(), 600)).append('\n');
            }
            Files.writeString(o.resolve("pg-regress.tsv"), tsv, StandardCharsets.UTF_8);
        }
    }

    /** The schedule's tests, in order ({@code test: a b c} lines; a group runs serially here). */
    static List<String> schedule(Path file) throws java.io.IOException {
        List<String> tests = new ArrayList<>();
        for (String l : Files.readAllLines(file, StandardCharsets.UTF_8)) {
            String t = l.strip();
            if (t.startsWith("test:")) {
                for (String name : t.substring(5).strip().split("\\s+")) {
                    if (!name.isEmpty()) {
                        tests.add(name);
                    }
                }
            }
        }
        return tests;
    }

    // ------------------------------------------------------------------ which statements are judged

    private static final Pattern FIRST_WORD = Pattern.compile("(?is)^[\\s(]*(select|with|values|table)\\b");

    private static final Set<String> FIRST = Set.of("select", "with", "values", "table");

    private static final Set<String> WRITES = Set.of("insert", "delete", "merge");

    /** What a query writes outside its transaction, so the judge's runs of it would change what the script reads next:
     *  a sequence, an advisory lock, a setting, a large object, a notification (the independent review, 2026-10-17). */
    private static final Pattern OUTSIDE = Pattern.compile("(?i)\\b(nextval|setval|pg_(try_)?advisory\\w*|set_config"
            + "|lo_\\w+|pg_notify|pg_sleep\\w*)\\s*\\(");

    /** A write a statement the island's lexer cannot read may hold. */
    private static final Pattern WRITE_WORD = Pattern.compile("(?i)\\b(insert|delete|merge|update|into)\\b");

    /** A statement that reads only: it starts as a query does, and writes nothing ({@code INSERT}, {@code DELETE},
     *  {@code MERGE}, an {@code UPDATE} that is not {@code FOR [NO KEY] UPDATE}, a top-level {@code INTO}). */
    static boolean readOnly(String sql) {
        if (OUTSIDE.matcher(sql).find()) {
            return false;
        }
        List<SqlLexer.Token> tokens;
        try {
            tokens = SqlLexer.tokenize(sql);
        } catch (RuntimeException e) {
            // a statement the island's lexer cannot read (SqlSyntaxException and the like): its first word decides,
            // and no word of a write anywhere
            return FIRST_WORD.matcher(sql).lookingAt() && !WRITE_WORD.matcher(sql).find();
        }
        int k = 0;
        while (k < tokens.size() && tokens.get(k).kind() == SqlLexer.Kind.LPAREN) {
            k++;
        }
        if (k >= tokens.size() || tokens.get(k).kind() != SqlLexer.Kind.IDENT
                || !FIRST.contains(tokens.get(k).text().toLowerCase(Locale.ROOT))) {
            return false;
        }
        int depth = 0;
        String prev = "";
        for (SqlLexer.Token t : tokens) {
            if (t.kind() == SqlLexer.Kind.LPAREN) {
                depth++;
            } else if (t.kind() == SqlLexer.Kind.RPAREN) {
                depth--;
            }
            String w = t.kind() == SqlLexer.Kind.IDENT ? t.text().toLowerCase(Locale.ROOT) : "";
            if (WRITES.contains(w) || w.equals("update") && !prev.equals("for") && !prev.equals("key")
                    || w.equals("into") && depth == 0) {
                return false;
            }
            prev = w;
        }
        return true;
    }

    // ------------------------------------------------------------------ the judging

    private static Judged judge(String file, int line, String sql, SqlParity.Answer answer, Connection conn,
            int timeoutSeconds, int port) {
        long start = System.nanoTime();
        if (answer instanceof SqlParity.Rejected r && r.sqlState().equals("25P02")) {
            return new Judged(file, line, "SKIP", 0, "the script's transaction is aborted", sql);
        }
        if (answer instanceof SqlParity.Rejected r && r.sqlState().equals(Psql.UNREADABLE)) {
            return new Judged(file, line, "HARNESS", 0, "Postgres's answer is unreadable: " + r.message(), sql);
        }
        boolean opened = false;
        boolean savepoint = false;
        try {
            boolean defaults = DEFAULT_DATESTYLE.equals(setting(conn, "DateStyle"))
                    && "on".equals(setting(conn, "standard_conforming_strings"));
            SqlParity.Answer reference = answer;
            savepoint = open(conn, timeoutSeconds);
            opened = true;
            SqlParity.Answer second = send(conn, sql);
            reset(conn, savepoint, timeoutSeconds);
            if (!SqlParity.agrees(answer, second)) {
                // under the script's own DateStyle a date's text reads otherwise, and with its
                // standard_conforming_strings off a string's backslashes: the island's spec is Postgres's answer
                // under the defaults, which the judge runs (twice: it must be one answer)
                SqlParity.Answer third = defaults ? second : send(conn, sql);
                reset(conn, savepoint, timeoutSeconds);
                if (defaults && answer instanceof SqlParity.Rows || !defaults && !SqlParity.agrees(second, third)) {
                    return new Judged(file, line, "NONDETERMINISTIC", ms(start), "Postgres answered it twice"
                            + " differently", sql);
                }
                if (!defaults) {
                    reference = second;
                }
            }
            Map<String, List<Col>> tables = tables(conn, sql);
            reset(conn, savepoint, timeoutSeconds);
            String model = model(tables, port);
            Set<String> names = tables.keySet();
            SqlParity.Case c = new SqlParity.Case("pg-regress/" + file, "L" + line, sql, null);
            String[] island = new String[1];
            SqlParity.Grid[] lite = new SqlParity.Grid[1];
            SqlParity.Outcome o = SqlParity.outcome(c, reference, names, text -> {
                island[0] = text;
                var result = Lean.run(model, text, SqlParity.RUNTIME, conn);
                lite[0] = SqlParity.grid(result);
                return result;
            });
            String status = o.status().name();
            String detail = o.detail();
            if ((status.equals("FAIL") || status.equals("BUG")) && island[0] != null) {
                detail += " [lite's SQL: " + plannedSql(model, island[0]) + "]"; // a wrong answer's triage
            }
            // NAMES and REGISTERED by the whole grids, not SqlParity's details (it stops at ten, and an ORDER BY's
            // key check waits on the rest: the independent review, 2026-10-17)
            if (status.equals("FAIL") && reference instanceof SqlParity.Rows rows && lite[0] != null
                    && !topLevelOrderBy(sql) && SqlParity.compare(false, null, rows.grid(), lite[0]).isEmpty()) {
                // SqlParity reads any ORDER BY in the text as the result's order; one inside a subquery orders no
                // result (2026-10-17, select_parallel's right join of a sorted subquery)
                status = "PASS";
                detail = "";
            }
            if (status.equals("FAIL") && reference instanceof SqlParity.Rows rows && lite[0] != null) {
                SqlParity.Grid pg = rows.grid();
                if (pg.columns().size() == lite[0].columns().size()
                        && SqlParity.compare(c, pg, new SqlParity.Grid(pg.columns(), lite[0].rows())).isEmpty()) {
                    status = "NAMES"; // the rows are Postgres's; only an output column's name differs
                } else if (SqlParity.compare(c, SqlParity.jsonTextAsJson(pg), SqlParity.jsonTextAsJson(lite[0]))
                        .isEmpty()) {
                    status = "REGISTERED";
                    detail = "S49: a json value's object or array is written as jsonb writes it (Postgres's json"
                            + " keeps the text as written)";
                }
            }
            Known known = KNOWN.get(sql.strip().replaceAll("\\s+", " "));
            if (known != null && status.equals(known.status()) && o.detail().contains(known.mark())) {
                status = "KNOWN";
                detail = known.reason();
            }
            if ((status.equals("ERROR") || status.equals("REFUSED")) && CATALOG.matcher(sql).find()) {
                status = "CATALOG"; // a system catalog: no Legend model has one
            } else if ((status.equals("ERROR") || status.equals("REFUSED")) && readsUnmodeled(sql, tables)
                    || status.equals("FAIL") && o.detail().startsWith("Column count mismatch") && starOverUnmodeled(sql,
                    tables)) {
                status = "UNMODELED";
            }
            return new Judged(file, line, status, ms(start), detail, sql);
        } catch (SQLException | RuntimeException e) {
            return new Judged(file, line, "HARNESS", ms(start), e.getClass().getSimpleName() + ": "
                    + e.getMessage(), sql);
        } finally {
            if (opened) {
                // whatever the judge met, its savepoint (or transaction) and its settings go: the script runs on as
                // written (the independent review, 2026-10-17: a failure left them open around the rest of the file)
                try {
                    close(conn, savepoint);
                } catch (SQLException e) {
                    throw new IllegalStateException("the judge could not undo its own statements: " + e.getMessage(),
                            e);
                }
            }
        }
    }

    /** Whether {@code sql}'s own result is ordered: an ORDER BY of the statement itself, not of a subquery inside it
     *  (true where the island's parser cannot read it, as SqlParity reads the text). */
    private static boolean topLevelOrderBy(String sql) {
        SqlAst.Statement s;
        try {
            s = SqlParser.parse(sql);
        } catch (RuntimeException unreadable) {
            return true;
        }
        while (s instanceof SqlAst.With w) {
            s = w.body();
        }
        return switch (s) {
            case SqlAst.Select q -> !q.orderBy().isEmpty();
            case SqlAst.SetOperation o -> !o.orderBy().isEmpty();
            case SqlAst.With w -> true;
            case SqlAst.Write w -> false;
        };
    }

    /** The SQL legend-lite runs for {@code island} over {@code model}, on one line. */
    private static String plannedSql(String model, String island) {
        try {
            return com.legend.Compiler.query(com.legend.Compiler.compileModel(model), island).plan(SqlParity.RUNTIME)
                    .sql().replaceAll("\\s+", " ");
        } catch (RuntimeException e) {
            return "(no plan: " + e.getMessage() + ")";
        }
    }

    /** A statement whose answer SQL leaves to the plan, or whose difference is named and not fixed here: the status
     *  it has, a part of its detail that marks it, and why. Another failure of the statement stays its own. */
    private record Known(String status, String mark, String reason) {
    }

    /** The {@link Known} statements, each judged KNOWN, not FAIL or BUG (2026-10-17, the first full run). A statement
     *  is its text as the script sends it, each run of white space one space. */
    private static final Map<String, Known> KNOWN = Map.of(
            "select unique1, unique2, generate_series(1,10) from tenk1 order by tenthous limit 7",
            new Known("FAIL", "column 'generate_series'", "LIMIT cuts through rows whose ORDER BY key ties (each"
                    + " tenk1 row's ten series rows share its tenthous): which seven are kept is the plan's choice in"
                    + " SQL"),
            "SELECT last_value(ten) OVER (PARTITION BY four), ten, four FROM (SELECT * FROM tenk1 WHERE unique2 < 10"
                    + " ORDER BY four, ten)s ORDER BY four, ten",
            new Known("FAIL", "column 'last_value'", "last_value over a partition with no window ORDER BY: SQL leaves"
                    + " the order of the partition's rows, so the last of them, to the plan"),
            "SELECT CASE WHEN i > 100 THEN 1/0 ELSE 0 END FROM case_tbl",
            new Known("BUG", "division by zero", "Postgres folds a branch's constant 1/0 when it plans the statement,"
                    + " though no row reaches the branch (its documented caveat; the suite's own comment); the island"
                    + " raises for a branch a row reaches"),
            "SELECT a, generate_series(1,2) FROM (VALUES(1),(2),(3)) r(a) LIMIT 2 OFFSET 2",
            new Known("FAIL", "column 'a'", "LIMIT and OFFSET with no ORDER BY: which rows they keep is the plan's"
                    + " choice in SQL"),
            "select t1.f1 from t1 left join t2 using (f1) group by f1",
            new Known("BUG", "must appear in the GROUP BY clause", "USING over an int and a bigint column: Postgres's"
                    + " merged f1 is the bigint cast, which GROUP BY f1 does not make t1.f1; the island's integers"
                    + " carry no width, so the merged column is t1.f1"));

    private static long ms(long start) {
        return (System.nanoTime() - start) / 1_000_000;
    }

    /** A rollback point for what the judge runs: a savepoint in the script's transaction (true), else a transaction
     *  of its own (false); either with the judge's own statement timeout. */
    private static boolean open(Connection conn, int timeoutSeconds) throws SQLException {
        boolean savepoint;
        try (Statement s = conn.createStatement()) {
            s.execute("SAVEPOINT corpus_judge");
            savepoint = true;
        } catch (SQLException e) {
            if (!"25P01".equals(e.getSQLState())) {
                throw e;
            }
            savepoint = false;
            try (Statement s = conn.createStatement()) {
                s.execute("BEGIN");
            }
        }
        settings(conn, timeoutSeconds);
        return savepoint;
    }

    /** The judge's settings, until the rollback: its statement timeout, and Postgres's defaults where the island reads
     *  text as they do: DateStyle, for a date's text (docs/LEGEND_SQL_STUDY_2026_10_11.md §9.4, DecodeDateTime as ISO,
     *  MDY), and standard_conforming_strings, for a string's backslashes (2026-10-17: strings.sql turns it off, where
     *  Postgres refuses U&'...', and the dialect writes its strings with it on). */
    private static void settings(Connection conn, int seconds) throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute("SET LOCAL statement_timeout = '" + seconds + "s'");
            s.execute("SET LOCAL datestyle TO '" + DEFAULT_DATESTYLE + "'");
            s.execute("SET LOCAL standard_conforming_strings TO on");
        }
    }

    private static final String DEFAULT_DATESTYLE = "ISO, MDY";

    private static String setting(Connection conn, String name) throws SQLException {
        try (var ps = conn.prepareStatement("SELECT current_setting(?)")) {
            ps.setString(1, name);
            try (ResultSet rs = ps.executeQuery()) {
                rs.next();
                return rs.getString(1);
            }
        }
    }

    /** Undoes what ran since {@link #open}, and opens again. */
    private static void reset(Connection conn, boolean savepoint, int timeoutSeconds) throws SQLException {
        try (Statement s = conn.createStatement()) {
            s.execute(savepoint ? "ROLLBACK TO SAVEPOINT corpus_judge" : "ROLLBACK");
            if (!savepoint) {
                s.execute("BEGIN");
            }
        }
        settings(conn, timeoutSeconds);
    }

    private static void close(Connection conn, boolean savepoint) throws SQLException {
        try (Statement s = conn.createStatement()) {
            if (savepoint) {
                s.execute("ROLLBACK TO SAVEPOINT corpus_judge");
                s.execute("RELEASE SAVEPOINT corpus_judge");
            } else {
                s.execute("ROLLBACK");
            }
        }
    }

    private static SqlParity.Answer send(Connection conn, String sql) {
        try (Statement st = conn.createStatement()) {
            if (st.execute(sql)) {
                try (ResultSet rs = st.getResultSet()) {
                    return new SqlParity.Rows(SqlParity.grid(rs));
                }
            }
            return new SqlParity.Rows(new SqlParity.Grid(List.of(), List.of()));
        } catch (SQLException e) {
            String message = e.getMessage() == null ? "" : e.getMessage().split("\n")[0];
            return new SqlParity.Rejected(e.getSQLState() == null ? "" : e.getSQLState(), message);
        }
    }

    // ------------------------------------------------------------------ the model

    private static final Pattern SIMPLE = Pattern.compile("[a-z_][a-z0-9_]*");

    /** The relations on the search path that {@code sql} names, each with its columns, read from the catalog. */
    private static Map<String, List<Col>> tables(Connection conn, String sql) throws SQLException {
        Set<String> words = new java.util.HashSet<>();
        Matcher m = Pattern.compile("[A-Za-z_][A-Za-z0-9_]*").matcher(sql);
        while (m.find()) {
            words.add(m.group().toLowerCase(Locale.ROOT));
        }
        Map<String, List<Col>> tables = new LinkedHashMap<>();
        String q = "SELECT c.relname, a.attname, format_type(a.atttypid, a.atttypmod), n.nspname"
                + " FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace"
                + " JOIN pg_attribute a ON a.attrelid = c.oid"
                + " WHERE c.relkind IN ('r', 'v', 'm', 'p', 'f') AND a.attnum > 0 AND NOT a.attisdropped"
                + " AND n.nspname = ANY (current_schemas(true)) AND n.nspname <> 'pg_catalog'"
                + " ORDER BY array_position(current_schemas(true), n.nspname), c.relname, a.attnum";
        // the relation each name reads: the first on the search path (a temporary table hides the table it is named
        // after). By schema and name: by name alone, a temporary char_tbl and public's read as one relation of both
        // their columns (2026-10-17, 41 statements)
        Map<String, String> owner = new java.util.HashMap<>();
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery(q)) {
            while (rs.next()) {
                String rel = rs.getString(1);
                String schema = rs.getString(4);
                if (!words.contains(rel) || !SIMPLE.matcher(rel).matches()
                        || !owner.computeIfAbsent(rel, k -> schema).equals(schema)) {
                    continue;
                }
                tables.computeIfAbsent(rel, k -> new ArrayList<>()).add(col(rs));
            }
        }
        // a relation named by its schema (testschema.amv), as "schema.name": any schema but public (read as the
        // search path's, above) and the system's (2026-10-17, the regression suite's 20)
        String named = "SELECT c.relname, a.attname, format_type(a.atttypid, a.atttypmod), n.nspname"
                + " FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace"
                + " JOIN pg_attribute a ON a.attrelid = c.oid"
                + " WHERE c.relkind IN ('r', 'v', 'm', 'p', 'f') AND a.attnum > 0 AND NOT a.attisdropped"
                + " AND n.nspname NOT IN ('public', 'pg_catalog', 'information_schema')"
                + " AND n.nspname NOT LIKE 'pg\\_to%' AND n.nspname NOT LIKE 'pg\\_temp%'"
                + " ORDER BY n.nspname, c.relname, a.attnum";
        String lower = sql.toLowerCase(Locale.ROOT);
        try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery(named)) {
            while (rs.next()) {
                String rel = rs.getString(1);
                String schema = rs.getString(4);
                if (!words.contains(rel) || !words.contains(schema) || !SIMPLE.matcher(rel).matches()
                        || !SIMPLE.matcher(schema).matches() || !Pattern.compile("\\b" + schema + "\\s*\\.\\s*"
                        + rel + "\\b").matcher(lower).find()) {
                    continue;
                }
                tables.computeIfAbsent(schema + "." + rel, k -> new ArrayList<>()).add(col(rs));
            }
        }
        return tables;
    }

    /** The column a catalog row ({@code relname, attname, format_type, nspname}) describes. */
    private static Col col(ResultSet rs) throws SQLException {
        String col = rs.getString(2);
        String type = rs.getString(3);
        return new Col(col, type, SIMPLE.matcher(col).matches() ? legendType(type) : null);
    }

    private static final Pattern SIZED = Pattern.compile("(character varying|character|numeric)\\((\\d+)(?:,(\\d+))?\\)");

    /** The Legend column type a Postgres type is, or null when none models it. */
    static @com.legend.base.Nullable String legendType(String pgType) {
        Matcher m = SIZED.matcher(pgType);
        if (m.matches()) {
            return switch (m.group(1)) {
                case "character varying" -> "VARCHAR(" + m.group(2) + ")";
                case "character" -> "CHAR(" + m.group(2) + ")";
                default -> "NUMERIC(" + m.group(2) + "," + (m.group(3) == null ? "0" : m.group(3)) + ")";
            };
        }
        if (pgType.startsWith("timestamp") && pgType.endsWith("without time zone")) {
            return "TIMESTAMP";
        }
        return switch (pgType) {
            case "smallint" -> "SMALLINT";
            case "integer" -> "INTEGER";
            case "bigint" -> "BIGINT";
            case "real" -> "REAL";
            case "double precision" -> "DOUBLE";
            // a Legend VARCHAR is declared with its size: text's is Postgres's longest varchar
            case "text", "character varying" -> "VARCHAR(10485760)";
            // name: 63 bytes, compared as text (the database's collation is C)
            case "name" -> "VARCHAR(63)";
            case "boolean" -> "BIT";
            case "date" -> "DATE";
            case "json", "jsonb" -> "SEMISTRUCTURED";
            default -> null;
        };
    }

    /** The model of {@code tables}: their modeled columns, on the embedded Postgres the island runs on (through
     *  the caller's own connection, the judged session). */
    private static String model(Map<String, List<Col>> tables, int port) {
        StringBuilder m = new StringBuilder("###Relational\nDatabase " + SqlParity.DATABASE + "\n(\n");
        Map<String, StringBuilder> schemas = new LinkedHashMap<>();
        for (Map.Entry<String, List<Col>> t : tables.entrySet()) {
            List<Col> cols = t.getValue().stream().filter(c -> c.legendType() != null).toList();
            if (cols.isEmpty()) {
                continue;
            }
            int dot = t.getKey().indexOf('.');
            StringBuilder into = dot < 0 ? m : schemas.computeIfAbsent(t.getKey().substring(0, dot),
                    k -> new StringBuilder());
            into.append("    Table ").append(t.getKey().substring(dot + 1)).append("\n    (\n");
            for (int i = 0; i < cols.size(); i++) {
                into.append("        ").append(cols.get(i).name()).append(' ').append(cols.get(i).legendType())
                        .append(i + 1 < cols.size() ? ",\n" : "\n");
            }
            into.append("    )\n");
        }
        schemas.forEach((schema, body) -> m.append("    Schema ").append(schema).append("\n    (\n").append(body)
                .append("    )\n"));
        if (tables.values().stream().allMatch(cols -> cols.stream().allMatch(c -> c.legendType() == null))) {
            m.append("    Table corpus_none\n    (\n        x INTEGER\n    )\n");
        }
        m.append(")\n###Connection\nRelationalDatabaseConnection e2e::Conn\n{\n    type: Postgres;\n"
                + "    specification: Static { host: '127.0.0.1'; port: " + port + "; name: 'regression'; };\n"
                + "    auth: Test;\n}\n###Runtime\nRuntime ").append(SqlParity.RUNTIME)
                .append("\n{\n    mappings: [];\n    connections: [ ").append(SqlParity.DATABASE)
                .append(": [ environment: e2e::Conn ] ];\n}\n");
        return m.toString();
    }

    /** {@code sql} names a column of a type no Legend column models, or a relation all of whose columns are such. */
    private static boolean readsUnmodeled(String sql, Map<String, List<Col>> tables) {
        String lower = sql.toLowerCase(Locale.ROOT);
        for (List<Col> cols : tables.values()) {
            for (Col c : cols) {
                if (c.legendType() == null && Pattern.compile("\\b" + Pattern.quote(c.name().toLowerCase(Locale.ROOT))
                        + "\\b").matcher(lower).find()) {
                    return true;
                }
            }
            if (cols.stream().allMatch(c -> c.legendType() == null)) {
                return true;
            }
        }
        return false;
    }

    /** A system catalog read in a FROM list: {@code pg_*} (or {@code pg_catalog.*}) or {@code information_schema.*}. */
    private static final Pattern CATALOG = Pattern.compile(
            "(?i)\\b(from|join|,)\\s*(pg_catalog\\.)?(pg_[a-z_]+|information_schema\\.)");

    /** {@code sql} selects {@code *} and a relation it names has a column no Legend column models (the island's
     *  {@code *} has fewer columns than Postgres's). */
    private static boolean starOverUnmodeled(String sql, Map<String, List<Col>> tables) {
        return SELECT_STAR.matcher(sql).find() && tables.values().stream().anyMatch(cols -> cols.stream().anyMatch(c ->
                c.legendType() == null));
    }

    /** A select list's {@code *} or {@code t.*}: after SELECT [DISTINCT] or a comma, before a comma or FROM (not
     *  {@code count(*)}, not {@code a * b}). */
    private static final Pattern SELECT_STAR = Pattern.compile(
            "(?is)(?:\\bselect\\s+(?:distinct\\s+)?|,)\\s*(?:\\w+\\s*\\.\\s*)?\\*\\s*(?:,|\\bfrom\\b|$)");

    // ------------------------------------------------------------------ the report

    private static final List<String> ORDER = List.of("PASS", "NAMES", "FAIL", "BUG", "KNOWN", "REGISTERED", "REFUSED",
            "ERROR", "UNMODELED", "CATALOG", "BOTH_ERROR", "NONDETERMINISTIC", "SKIP", "HARNESS");

    private static String counts(List<Judged> judged, String file) {
        Map<String, Integer> n = new TreeMap<>();
        for (Judged j : judged) {
            if (j.file().equals(file)) {
                n.merge(j.status(), 1, Integer::sum);
            }
        }
        return n.toString();
    }

    private static String summary(List<Judged> judged) {
        Map<String, Integer> total = new LinkedHashMap<>();
        ORDER.forEach(s -> total.put(s, 0));
        for (Judged j : judged) {
            total.merge(j.status(), 1, Integer::sum);
        }
        StringBuilder r = new StringBuilder("# #SQL{} over the PostgreSQL regression suite (read-only statements)\n\n");
        r.append("Judged: ").append(judged.size()).append("\n\n| status | n |\n|---|---|\n");
        total.forEach((s, n) -> r.append("| ").append(s).append(" | ").append(n).append(" |\n"));
        for (String status : List.of("FAIL", "BUG", "KNOWN", "REGISTERED", "NAMES", "ERROR", "REFUSED", "UNMODELED",
                "HARNESS")) {
            Map<String, Integer> reasons = new HashMap<>();
            Map<String, String> example = new HashMap<>();
            for (Judged j : judged) {
                if (j.status().equals(status)) {
                    String key = status.equals("KNOWN") || status.equals("REGISTERED") ? j.detail()
                            : normalized(j.detail());
                    reasons.merge(key, 1, Integer::sum);
                    example.putIfAbsent(key, j.file() + ":" + j.line());
                }
            }
            if (reasons.isEmpty()) {
                continue;
            }
            r.append("\n## ").append(status).append(" (").append(reasons.values().stream().mapToInt(Integer::intValue)
                    .sum()).append(")\n\n| n | reason | e.g. |\n|---|---|---|\n");
            reasons.entrySet().stream().sorted((a, b) -> b.getValue() - a.getValue()).limit(60)
                    .forEach(e -> r.append("| ").append(e.getValue()).append(" | ").append(clean(e.getKey(), 200)
                            .replace("|", "\\|")).append(" | ").append(example.get(e.getKey())).append(" |\n"));
        }
        return r.toString();
    }

    /** A message with its particulars (quoted text, numbers) masked, so messages of one kind count together. */
    static String normalized(String detail) {
        int sql = detail.indexOf(" [lite's SQL: ");
        return (sql < 0 ? detail : detail.substring(0, sql)).replaceAll("'[^']*'", "'…'")
                .replaceAll("\"[^\"]*\"", "\"…\"").replaceAll("\\d+", "N").replaceAll("\\s+", " ").strip();
    }

    private static String clean(String s, int max) {
        String t = s.replace('\t', ' ').replace('\n', ' ').replace('\r', ' ');
        return t.length() > max ? t.substring(0, max) + "…" : t;
    }
}
