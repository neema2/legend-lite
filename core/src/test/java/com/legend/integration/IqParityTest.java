// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.integration;

import com.legend.testing.Lean;

import com.legend.protocol.sql.iq.IqProfile;
import com.legend.testbackend.TestBackend;
import com.legend.testbackend.TestBackends;
import com.legend.testcases.IqAnswers;
import com.legend.testcases.IqParity;
import com.legend.testcases.SqlParity;
import com.legend.testing.Runfile;
import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The {@code #IQ{...}#} island's parity cases on DuckDB ({@link IqParity}): every case of
 * {@code core/src/test/resources/iq-parity/lite} under each of its profiles, as an island over the schema's tables in
 * DuckDB, judged against SQL Anywhere 17's snapped answer (or the case's provisional decision), and held to the
 * ledger's {@code duckdb} column. The answers file is checked against the cases first: UNSNAPPED, STALE and GONE fail
 * the lane, as does a case pinning an engine its island does not run in, a case SQL Anywhere judges whose
 * translation the IQ engine's rules change (engine neutrality, the study's §6.3), a judged case whose WHERE puts
 * an operation that can raise beside AND or OR (its answer depends on IQ's plan: the study's D-E2), and a case whose
 * answer may be one plan's choice (a row limit's ties, a LIST's order: {@link IqParity#determinism}).
 *
 * <p>Its own target ({@code //core:iq_parity_test}). Run with {@code --jvm_flag=-Diq.parity.update=<the ledger's path
 * in the workspace>} to write the ledger's column instead.
 *
 * <p>It also pins the showcase's SQL (docs/IQ_SQL_SHOWCASE.md): {@link #theShowcasePlansItsPinnedSql}.
 */
class IqParityTest {

    /** The platform's own engine: the island's session, its tables and the connection its model declares. */
    private static final TestBackend PLATFORM = TestBackends.platform();

    /** The showcase's query: {@code lite/showcase.yaml}'s main case, judged by SQL Anywhere above like every case. */
    private static final String SHOWCASE = "lite/showcase/showcase_main@iq16-default";

    /** The connections the showcase's SQL is pinned for, each a file of {@code iq-parity/showcase/}: Snowflake's is
     *  rendered only (no Snowflake account is reached; planning never connects), DuckDB's is the SQL the lane runs. */
    private static final String[][] SHOWCASE_LANES = {
        {"snowflake", "type: Snowflake;\n    specification: Snowflake { name: 'IQ_SHOWCASE'; account: 'acct';"
                + " warehouse: 'wh'; region: 'us-east-1'; };\n    auth: SnowflakePublic { publicUserName: 'u';"
                + " privateKeyVaultReference: 'vault/k'; passPhraseVaultReference: 'vault/pp'; };"},
        {"duckdb", "type: DuckDB;\n    specification: DuckDB { };\n    auth: Test;"},
    };

    @Test
    void everyIqCaseHasItsLedgerStatusOnThePlatform() throws Exception {
        Path schemaFile = Runfile.property("iq.parity.schema");
        List<IqParity.Run> runs = IqParity.runs(IqParity.cases(Runfile.property("iq.parity.lite").getParent()));
        List<SqlParity.Table> tables = IqParity.schema(schemaFile);
        Set<String> names = IqParity.tableNames(tables);
        Map<String, IqAnswers.Snap> answers = IqAnswers.read(Runfile.property("iq.parity.answers"));
        List<String> problems = new ArrayList<>(IqAnswers.guards(runs, answers, Files.readAllBytes(schemaFile)));
        String model = IqParity.model(tables, PLATFORM.connectionBody());
        Map<String, SqlParity.Outcome> outcomes = new LinkedHashMap<>();
        try (Connection session = PLATFORM.open()) {
            IqParity.seed(PLATFORM, session, tables);
            for (IqParity.Run r : runs) {
                String mismatch = IqParity.engineMismatch(r, names);
                if (mismatch != null) {
                    problems.add(mismatch);
                }
                String neutrality = IqParity.engineNeutrality(r, tables, names);
                if (neutrality != null) {
                    problems.add(neutrality);
                }
                String order = IqParity.evaluationOrder(r);
                if (order != null) {
                    problems.add(order);
                }
                SqlParity.Outcome outcome = IqParity.outcome(r, answers.get(r.key()), tables, names,
                        island -> Lean.run(model, island, IqParity.RUNTIME, session));
                outcomes.put(r.key(), outcome);
                // IQ island, 2026-10-14 (review 7, the user's ruling on B1): no case pins one plan's choice
                String nondeterministic = IqParity.determinism(r, tables, outcome.status());
                if (nondeterministic != null) {
                    problems.add(nondeterministic);
                }
            }
        }
        String update = System.getProperty("iq.parity.update");
        problems.addAll(IqParity.ratchet(runs, outcomes, Runfile.property("iq.parity.ledger"), "duckdb",
                update == null ? null : Path.of(update)));
        if (update == null) {
            // IQ island, landing 4 (WP1), 2026-10-14: every FAIL, BUG or ERROR is a listed iq17 divergence, and the
            // list only shrinks
            problems.addAll(IqParity.divergences(runs, outcomes, Runfile.property("iq.parity.divergences"), "duckdb"));
        }
        assertTrue(problems.isEmpty(), problems.size() + " problems:\n" + String.join("\n", problems));
    }

    /**
     * IQ island, 2026-10-14: the showcase (docs/IQ_SQL_SHOWCASE.md). Its island -- {@link #SHOWCASE}, with the
     * variable it binds let before it, as the lane runs it -- compiled with a Snowflake connection and with a DuckDB
     * one plans the SQL pinned in {@code iq-parity/showcase/snowflake.sql} and {@code duckdb.sql}, byte for byte. Run
     * with {@code --jvm_flag=-Diq.showcase.update=<core/src/test/resources/iq-parity/showcase in the workspace>} to
     * rewrite them.
     */
    @Test
    void theShowcasePlansItsPinnedSql() throws Exception {
        List<SqlParity.Table> tables = IqParity.schema(Runfile.property("iq.parity.schema"));
        IqParity.Run main = IqParity.runs(IqParity.cases(Runfile.property("iq.parity.lite").getParent())).stream()
                .filter(r -> r.key().equals(SHOWCASE)).findFirst()
                .orElseThrow(() -> new AssertionError("no case " + SHOWCASE));
        String island = IqParity.island(main, IqParity.tableNames(tables));
        Path pinned = Runfile.property("iq.parity.showcase").getParent();
        String update = System.getProperty("iq.showcase.update");
        List<String> problems = new ArrayList<>();
        for (String[] lane : SHOWCASE_LANES) {
            String sql = com.legend.Compiler.query(com.legend.Compiler.compileModel(IqParity.model(tables, lane[1])),
                    island).plan(IqParity.RUNTIME).sql() + "\n";
            String file = lane[0] + ".sql";
            if (update != null) {
                Files.writeString(Path.of(update).resolve(file), sql, java.nio.charset.StandardCharsets.UTF_8);
                continue;
            }
            String expected = Files.readString(pinned.resolve(file), java.nio.charset.StandardCharsets.UTF_8);
            if (!expected.equals(sql)) {
                List<String> was = expected.lines().toList();
                List<String> now = sql.lines().toList();
                int line = 0;
                while (line < Math.min(was.size(), now.size()) && was.get(line).equals(now.get(line))) {
                    line++;
                }
                problems.add(file + " differs from line " + (line + 1) + ":\n  pinned: "
                        + (line < was.size() ? was.get(line) : "<end>") + "\n  now:    "
                        + (line < now.size() ? now.get(line) : "<end>"));
            }
        }
        assertTrue(problems.isEmpty(), "the showcase's SQL moved (if on purpose, rewrite it with"
                + " -Diq.showcase.update=<core/src/test/resources/iq-parity/showcase in the workspace> and update"
                + " docs/IQ_SQL_SHOWCASE.md):\n" + String.join("\n", problems));
    }

    @Test
    void theAnswersCannotFallBehindTheCases() {
        IqParity.Case c = new IqParity.Case("lite/x.yaml", "a", "SELECT 1", List.of("iq16-default", "iq16-tds"), null,
                null, null, null);
        byte[] schema = "tables:\n".getBytes(java.nio.charset.StandardCharsets.UTF_8);
        Map<String, IqAnswers.Snap> answers = new LinkedHashMap<>();
        answers.put("lite/x/a@iq16-default", new IqAnswers.Snap("lite/x/a", "iq16-default",
                IqAnswers.sqlSha("SELECT 2", "iq16-default", null, null, schema), List.of(), List.of(), List.of(), null));
        answers.put("lite/x/b@iq16-default", new IqAnswers.Snap("lite/x/b", "iq16-default",
                IqAnswers.sqlSha("SELECT 1", "iq16-default", null, null, schema), List.of(), List.of(), List.of(), null));
        List<String> problems = IqAnswers.guards(IqParity.runs(List.of(c)), answers, schema);
        // IQ island, landing 4 (WP1): each iq16-* run's iq17-* twin is a run too, answered by the live IQ
        assertEquals(5, problems.size(), String.join("\n", problems));
        assertTrue(problems.get(0).startsWith("STALE: lite/x/a@iq16-default"), problems.get(0));
        assertTrue(problems.get(1).startsWith("UNSNAPPED: lite/x/a@iq16-tds"), problems.get(1));
        assertTrue(problems.get(2).startsWith("UNSNAPPED: lite/x/a@iq17-default"), problems.get(2));
        assertTrue(problems.get(2).contains("--oracle iq"), problems.get(2));
        assertTrue(problems.get(3).startsWith("UNSNAPPED: lite/x/a@iq17-tds"), problems.get(3));
        assertTrue(problems.get(4).startsWith("GONE: lite/x/b@iq16-default"), problems.get(4));
    }

    /** IQ island, landing 4 (WP1), 2026-10-14: a case runs under the iq17-* twin of each iq16-* profile it names (the
     *  live IQ judges it there), never twice; an iq17-* run's answer must be the live IQ's, and one the live IQ could
     *  not judge is a SKIP that says why. */
    @Test
    void eachIq16RunHasAnIq17TwinJudgedByTheLiveIq() throws Exception {
        IqParity.Case c = new IqParity.Case("lite/x.yaml", "a", "SELECT 1 AS x", List.of("iq16-tds", "sqla17-default",
                "iq16-default", "iq17-default"), null, null, "D-S1", "[[\"2\"]]");
        List<String> keys = IqParity.runs(List.of(c)).stream().map(IqParity.Run::key).toList();
        assertEquals(List.of("lite/x/a@iq16-tds", "lite/x/a@sqla17-default", "lite/x/a@iq16-default",
                "lite/x/a@iq17-default", "lite/x/a@iq17-tds"), keys);
        assertTrue(IqParity.liveIq(IqProfile.IQ17_ODBC) && !IqParity.liveIq(IqProfile.IQ16_ODBC));
        byte[] schema = "tables:\n".getBytes(java.nio.charset.StandardCharsets.UTF_8);
        IqParity.Run twin = IqParity.runs(List.of(c)).get(4);
        // stamped without the decision and its rows: real IQ judges it, by its own answer
        String sha = IqAnswers.sqlSha(c.sql(), "iq17-tds", null, null, schema);
        assertEquals(sha, IqAnswers.sqlSha(twin, schema));
        IqAnswers.Snap sa = new IqAnswers.Snap("lite/x/a", "iq17-tds", sha, List.of("x"), List.of("integer"),
                List.of(List.of("1")), null);
        IqAnswers.Snap iq = new IqAnswers.Snap("lite/x/a", "iq17-tds", sha, List.of("x"), List.of("smallint"),
                List.of(List.of("1")), null, List.of(0), "iq", null);
        List<String> problems = IqAnswers.guards(List.of(twin), Map.of(twin.key(), sa), schema);
        assertTrue(problems.size() == 1 && problems.get(0).startsWith("ORACLE: lite/x/a@iq17-tds"), problems.toString());
        assertEquals(List.of(), IqAnswers.guards(List.of(twin), Map.of(twin.key(), iq), schema));
        assertTrue(IqAnswers.expected(twin, iq) instanceof SqlParity.Rows rows && rows.grid().rows().equals(
                List.of(List.of(java.math.BigInteger.ONE))), "the live IQ's rows, not the decision's");
        IqAnswers.Snap notJudged = new IqAnswers.Snap("lite/x/a", "iq17-tds", sha, List.of(), List.of(), null, null,
                List.of(), "iq", "the database property CaseSensitive = 'Off': this server's is 'On'");
        SqlParity.Outcome skipped = IqParity.outcome(twin, notJudged, List.of(), Set.of(), island -> {
            throw new AssertionError("a run the live IQ does not judge is not run");
        });
        assertEquals(SqlParity.Status.SKIP, skipped.status());
        assertTrue(skipped.detail().contains("CaseSensitive"), skipped.detail());
        Path answers = Files.createTempFile("iq-answers", ".jsonl");
        Files.writeString(answers, "{\"meta\":{}}\n{\"id\":\"lite/x/a\",\"profile\":\"iq17-tds\",\"oracle\":\"iq\","
                + "\"sqlSha\":\"" + sha + "\",\"not_judged\":\"why\"}\n");
        IqAnswers.Snap read = IqAnswers.read(answers).get("lite/x/a@iq17-tds");
        assertEquals("iq", read.judge());
        assertEquals("why", read.notJudged());
        Files.delete(answers);
    }

    /** IQ island, landing 4 (WP1), 2026-10-14: the known-divergence allowlist. A FAIL, BUG or ERROR must be listed
     *  (an iq17-* run only: a run SQL Anywhere judges never diverges), and a listed run must still diverge in each lane
     *  it lists: one that passes now, or is gone, or a cause no run names, must leave the list. */
    @Test
    void theKnownDivergencesAreListedAndOnlyShrink() throws Exception {
        IqParity.Case c = new IqParity.Case("lite/x.yaml", "a", "SELECT 1 AS x", List.of("iq16-default"), null, null,
                null, null);
        List<IqParity.Run> runs = IqParity.runs(List.of(c));
        Path file = Files.createTempFile("iq17-divergences", ".tsv");
        Files.writeString(file, "# a test's\ncause\twidening\tD-N3: IQ's engine widens\nrun\tlite/x/a@iq17-default"
                + "\twidening\tduckdb,postgres\n");
        Map<String, SqlParity.Outcome> outcomes = new LinkedHashMap<>();
        outcomes.put("lite/x/a@iq16-default", new SqlParity.Outcome(SqlParity.Status.PASS, ""));
        outcomes.put("lite/x/a@iq17-default", new SqlParity.Outcome(SqlParity.Status.ERROR, "an overflow"));
        assertEquals(List.of(), IqParity.divergences(runs, outcomes, file, "duckdb"));
        // the listed run passes now: it must leave the list, and its cause with it
        outcomes.put("lite/x/a@iq17-default", new SqlParity.Outcome(SqlParity.Status.PASS, ""));
        List<String> problems = IqParity.divergences(runs, outcomes, file, "postgres");
        assertTrue(problems.size() == 1 && problems.get(0).startsWith("ALLOWLIST SHRINKS: lite/x/a@iq17-default"),
                problems.toString());
        // a FAIL of a run SQL Anywhere judges is never allowed
        outcomes.put("lite/x/a@iq16-default", new SqlParity.Outcome(SqlParity.Status.FAIL, "rows"));
        outcomes.put("lite/x/a@iq17-default", new SqlParity.Outcome(SqlParity.Status.FAIL, "rows"));
        problems = IqParity.divergences(runs, outcomes, file, "duckdb");
        assertTrue(problems.size() == 1 && problems.get(0).startsWith("UNLISTED DIVERGENCE: lite/x/a@iq16-default"),
                problems.toString());
        Files.writeString(file, "cause\tw\tx\nrun\tlite/x/a@iq16-default\tw\tduckdb\n");
        assertThrows(IllegalStateException.class, () -> IqParity.divergences(runs, outcomes, file, "duckdb"));
        Files.writeString(file, "run\tlite/x/a@iq17-default\tw\tduckdb\n");
        assertThrows(IllegalStateException.class, () -> IqParity.divergences(runs, outcomes, file, "duckdb"));
        Files.writeString(file, "cause\tw\tx\ncause\tv\ty\nrun\tlite/x/a@iq17-default\tw\tduckdb\n");
        problems = IqParity.divergences(runs, outcomes, file, "duckdb");
        assertTrue(problems.size() == 2 && problems.get(1).startsWith("ALLOWLIST SHRINKS: the cause v"),
                problems.toString());
        Files.delete(file);
    }

    @Test
    void theIslandsProfilesAreTheRefereesProfiles() throws Exception {
        String first = Files.readAllLines(Runfile.property("iq.parity.answers")).get(0);
        com.legend.json.Json.Obj meta = (com.legend.json.Json.Obj) ((com.legend.json.Json.Obj)
                com.legend.json.Json.parse(first)).get("meta");
        for (IqProfile p : IqProfile.values()) {
            // IQ island, landing 4 (WP1): the iq17-* profiles are the live IQ's, in the meta line's live_iq part
            boolean live = IqParity.liveIq(p);
            com.legend.json.Json.Obj source = live ? (com.legend.json.Json.Obj) meta.get("live_iq") : meta;
            com.legend.json.Json.Obj profiles = (com.legend.json.Json.Obj) source.get("profiles");
            com.legend.json.Json.Obj databases = (com.legend.json.Json.Obj) source.get("databases");
            com.legend.json.Json.Obj profile = (com.legend.json.Json.Obj) profiles.get(p.spelling());
            String db = ((com.legend.json.Json.Str) profile.get("db")).value();
            com.legend.json.Json.Obj database = (com.legend.json.Json.Obj) databases.get(db);
            com.legend.json.Json.Obj props = (com.legend.json.Json.Obj) database.get("properties");
            com.legend.json.Json.Obj pub = (com.legend.json.Json.Obj) database.get("public");
            com.legend.json.Json.Obj options = (com.legend.json.Json.Obj) profile.get("options");
            String where = p.spelling() + ": ";
            var s = p.settings();
            assertEquals(live || db.equals("lite_iq"), s.iqDatabase(), where + "database");
            assertEquals(text(props, "Collation", ""), s.value(com.legend.protocol.sql.iq.IqOption.COLLATION),
                    where + "collation");
            assertEquals(text(props, "CaseSensitive", "").equals("On"), s.caseSensitive(), where + "case");
            assertEquals(text(props, "BlankPadding", "").equals("On"), s.blankPadding(), where + "blank padding");
            assertEquals(text(props, "CharSet", "").startsWith("ISO_8859-1"), s.latin1(), where + "charset");
            assertEquals(Integer.parseInt(text(pub, "precision", "30")), s.precision(), where + "precision");
            assertEquals(Integer.parseInt(text(pub, "scale", "6")), s.scale(), where + "scale");
            assertEquals(!text(options, "ansinull", "On").equalsIgnoreCase("Off"), s.ansinull(), where + "ansinull");
            assertEquals(!text(options, "quoted_identifier", "On").equalsIgnoreCase("Off"), s.quotedIdentifier(),
                    where + "quoted_identifier");
            assertEquals(!text(options, "escape_character", "On").equalsIgnoreCase("Off"), s.escapeCharacter(),
                    where + "escape_character");
            assertTrue(text(options, "date_order", "YMD").equalsIgnoreCase(s.dateOrder()), where + "date_order");
        }
    }

    private static String text(com.legend.json.Json.Obj o, String key, String otherwise) {
        return o.getOr(key, null) instanceof com.legend.json.Json.Str s ? s.value() : otherwise;
    }

    /** IQ island, 2026-10-14 (the options framework): a case's settings are given to IqSettings.resolve (no island
     *  header: the lanes translate such a case's SQL under them), and are part of what its answer is snapped for. */
    @Test
    void aCasesSettingsAreResolvedAndStampItsAnswer() {
        byte[] schema = "tables:\n".getBytes(java.nio.charset.StandardCharsets.UTF_8);
        IqParity.Case c = new IqParity.Case("lite/x.yaml", "a", "SELECT CAST('01/02/03' AS DATE) AS d",
                List.of("iq16-default", "sqla17-tds"), null, null, null, null, null,
                "database.CaseSensitive=Off, query.date_order='DMY'", null);
        List<IqParity.Run> runs = IqParity.runs(List.of(c));
        assertEquals("#IQ{SELECT CAST('01/02/03' AS DATE) AS d}#", IqParity.island(runs.get(0), Set.of()));
        assertFalse(runs.get(0).settings().caseSensitive());
        assertEquals("DMY", runs.get(1).settings().dateOrder());
        String query = IqParity.query(runs.get(0), List.of(), Set.of());
        assertTrue(query.contains("2003-02-01"), query);
        assertEquals(2, IqParity.settings("database.precision = 40 , query.date_format='DD''MM'").size());
        assertEquals("DD'MM", IqParity.settings("query.date_format='DD''MM'").get(0).value());
        assertThrows(IllegalStateException.class, () -> IqParity.settings("date_order=DMY"));
        assertThrows(IllegalStateException.class, () -> IqParity.settings("session.date_order=DMY"));
        Map<String, IqAnswers.Snap> answers = new LinkedHashMap<>();
        answers.put("lite/x/a@iq16-default", new IqAnswers.Snap("lite/x/a", "iq16-default",
                IqAnswers.sqlSha(c.sql(), "iq16-default", null, null, null, "query.date_order='MDY'", schema),
                List.of(), List.of(), List.of(), null));
        answers.put("lite/x/a@sqla17-tds", new IqAnswers.Snap("lite/x/a", "sqla17-tds",
                IqAnswers.sqlSha(c.sql(), "sqla17-tds", null, null, null, c.settings(), schema), List.of(),
                List.of(), List.of(), null));
        List<String> problems = IqAnswers.guards(runs, answers, schema);
        // IQ island, landing 4 (WP1): and its iq16-default run's iq17-default twin, the live IQ's to answer
        assertEquals(2, problems.size(), String.join("\n", problems));
        assertTrue(problems.get(0).startsWith("STALE: lite/x/a@iq16-default"), problems.get(0));
        assertTrue(problems.get(1).startsWith("UNSNAPPED: lite/x/a@iq17-default"), problems.get(1));
        // a case without settings keeps its stamp
        assertEquals(IqAnswers.sqlSha("SELECT 1", "iq16-default", null, null, schema),
                IqAnswers.sqlSha("SELECT 1", "iq16-default", null, null, null, null, schema));
    }

    @Test
    void editingADecidedCasesRowsMakesItsAnswerStale() {
        byte[] schema = "tables:\n".getBytes(java.nio.charset.StandardCharsets.UTF_8);
        IqParity.Case decided = new IqParity.Case("lite/x.yaml", "a", "SELECT 1", List.of("iq16-default"), null, null,
                "D-S1", "[[\"1\"]]");
        Map<String, IqAnswers.Snap> answers = new LinkedHashMap<>();
        answers.put("lite/x/a@iq16-default", new IqAnswers.Snap("lite/x/a", "iq16-default",
                IqAnswers.sqlSha("SELECT 1", "iq16-default", "D-S1", "[[\"2\"]]", schema), List.of(), List.of(),
                List.of(), null));
        // IQ island, landing 4 (WP1): its iq17-default twin's answer, the live IQ's, is stamped without the decision:
        // editing the decided rows leaves it as it is
        answers.put("lite/x/a@iq17-default", new IqAnswers.Snap("lite/x/a", "iq17-default",
                IqAnswers.sqlSha("SELECT 1", "iq17-default", null, null, schema), List.of("1"), List.of("smallint"),
                List.of(List.of("1")), null, List.of(0), "iq", null));
        List<String> problems = IqAnswers.guards(IqParity.runs(List.of(decided)), answers, schema);
        assertEquals(1, problems.size(), String.join("\n", problems));
        assertTrue(problems.get(0).startsWith("STALE: lite/x/a@iq16-default"), problems.get(0));
    }

    @Test
    void theJudgeComparesExactlyBySqlAnywheresType() {
        // integers exactly, never through a double
        assertFalse(IqParity.cellEquals(new java.math.BigInteger("9223372036854775807"), 9223372036854775806L,
                "bigint"));
        assertTrue(IqParity.cellEquals(new java.math.BigInteger("9223372036854775807"), Long.MAX_VALUE, "bigint"));
        assertFalse(IqParity.cellEquals(java.math.BigInteger.ONE, new java.math.BigDecimal("1.0"), "integer"));
        // numerics by value, scale aside, never against a double
        assertTrue(IqParity.cellEquals(new java.math.BigDecimal(".50"), new java.math.BigDecimal("0.5"), "numeric"));
        assertFalse(IqParity.cellEquals(new java.math.BigDecimal("0.1"), 0.1d, "numeric"));
        // doubles within a few units in the last place, no absolute slack
        assertFalse(IqParity.cellEquals(1e-300, 0.0d, "double"));
        assertTrue(IqParity.cellEquals(0.30000000000000004, 0.30000000000000004, "double"));
        // text exactly
        assertFalse(IqParity.cellEquals("ab", "ab   ", "char"));
        assertFalse(IqParity.cellEquals("1", 1L, "varchar"));
    }

    @Test
    void aCaseSqlAnywhereJudgesMustTranslateAlikeUnderBothEnginesRules() {
        List<SqlParity.Table> tables = List.of(new SqlParity.Table("t", List.of(new SqlParity.Column("id", "integer"),
                new SqlParity.Column("ch", "char(5)")), List.of()));
        Set<String> names = Set.of("t");
        // IQ island, 2026-10-15 (landing 4): an iq16 run whose live twin judges the IQ engine's rules needs no decision
        IqParity.Case twinned = new IqParity.Case("lite/x.yaml", "a", "SELECT ch FROM t", List.of("iq16-default"),
                null, null, null, null);
        assertEquals(null, IqParity.engineNeutrality(IqParity.runs(List.of(twinned)).get(0), tables, names));
        // one whose settings the live IQ may not take (a database property) still must name its decision: the IQ
        // engine pads ch (D-S1), SQL Anywhere's does not
        IqParity.Case padded = new IqParity.Case("lite/x.yaml", "a", "SELECT ch FROM t", List.of("iq16-default"),
                null, null, null, null, null, "database.BlankPadding='On'", null);
        assertTrue(IqParity.engineNeutrality(IqParity.runs(List.of(padded)).get(0), tables, names) != null);
        IqParity.Case decided = new IqParity.Case("lite/x.yaml", "a", "SELECT ch FROM t", List.of("iq16-default"),
                null, null, "D-S1", "[]");
        assertEquals(null, IqParity.engineNeutrality(IqParity.runs(List.of(decided)).get(0), tables, names));
        // ch unread: nothing differs
        IqParity.Case neutral = new IqParity.Case("lite/x.yaml", "a", "SELECT id FROM t", List.of("iq16-default"),
                null, null, null, null);
        assertEquals(null, IqParity.engineNeutrality(IqParity.runs(List.of(neutral)).get(0), tables, names));
    }

    @Test
    void anEnginePinToIqNeedsADecision() {
        List<SqlParity.Table> tables = List.of(new SqlParity.Table("t", List.of(new SqlParity.Column("id", "integer")),
                List.of()));
        IqParity.Case pinned = new IqParity.Case("lite/x.yaml", "a", "SELECT id FROM t", List.of("iq16-default"),
                "iq", null, null, null);
        String problem = IqParity.engineNeutrality(IqParity.runs(List.of(pinned)).get(0), tables, Set.of("t"));
        assertTrue(problem != null && problem.contains("pins engine=iq"), problem);
        IqParity.Case decided = new IqParity.Case("lite/x.yaml", "a", "SELECT id FROM t", List.of("iq16-default"),
                "iq", null, "D-S1", "[]");
        assertEquals(null, IqParity.engineNeutrality(IqParity.runs(List.of(decided)).get(0), tables, Set.of("t")));
    }

    @Test
    void anOperationThatCanRaiseBesideAndOrInWhereIsNotJudged() {
        java.util.function.BiFunction<String, String, String> order = (sql, skip) -> IqParity.evaluationOrder(
                IqParity.runs(List.of(new IqParity.Case("lite/x.yaml", "a", sql, List.of("iq16-default"), null, skip,
                        null, null))).get(0));
        assertTrue(order.apply("SELECT id FROM t WHERE id > 1 AND it + 1 > 0", null) != null);
        assertTrue(order.apply("SELECT id FROM t WHERE NOT (sm = 0 OR 100 / sm > 1)", null) != null);
        assertTrue(order.apply("SELECT id FROM t WHERE id = 1 OR NULLIF(sm, it) > 0", null) != null);
        // skipped as the decision says, or with nothing beside the operation: judged as usual
        assertEquals(null, order.apply("SELECT id FROM t WHERE id > 1 AND it + 1 > 0", IqParity.PLAN_DEPENDENT));
        assertEquals(null, order.apply("SELECT id FROM t WHERE it + 1 > 0", null));
        assertEquals(null, order.apply("SELECT id FROM t WHERE sm <> 0 AND sm != 1", null));
        // arithmetic of constants cannot raise per row
        assertEquals(null, order.apply("SELECT id FROM t WHERE id > 1 AND id = 2 + 3", null));
        // AND inside a CASE's condition is lazy in IQ too
        assertEquals(null, order.apply("SELECT id FROM t WHERE CASE WHEN id > 1 AND id = 2 THEN 1 END = 1", null));
        // IQ island, 2026-10-14 (review 3's M4): HAVING is a search condition too, and a SUM or an AVG can raise
        assertTrue(order.apply("SELECT k FROM g GROUP BY k HAVING MAX(v) <> 2147483647 AND MAX(v) + 1 > 0", null)
                != null);
        assertTrue(order.apply("SELECT k FROM g GROUP BY k HAVING COUNT(*) > 2 AND SUM(v) > 0", null) != null);
        assertEquals(null, order.apply("SELECT k FROM g GROUP BY k HAVING COUNT(*) > 2 AND MAX(v) > 0", null));
    }

    /**
     * IQ island, 2026-10-14 (review 7, the user's ruling on B1): which rows a TOP keeps where its ORDER BY keys tie at
     * the cut, and a LIST's order without its own ORDER BY, are the plan's choice, so no committed case may depend on
     * them -- the determinism lint ({@link IqParity#determinism}); a case that tests a refusal or an error may opt out
     * with its reason, and must then not PASS.
     */
    @Test
    void aCaseWhoseAnswerIsThePlansChoiceIsNotCommitted() throws Exception {
        List<SqlParity.Table> tables = List.of(
                new SqlParity.Table("t", List.of(new SqlParity.Column("id", "integer"), new SqlParity.Column("sm",
                        "smallint")), List.of(List.of(1L, 5L), List.of(2L, 5L))),
                new SqlParity.Table("u", List.of(new SqlParity.Column("id", "integer"), new SqlParity.Column("t_id",
                        "integer")), List.of(List.of(1L, 1L))),
                new SqlParity.Table("g", List.of(new SqlParity.Column("id", "integer"), new SqlParity.Column("k",
                        "smallint"), new SqlParity.Column("v", "integer")), List.of(List.of(1L, 1L, 1L))));
        java.util.function.BiFunction<String, String, String> lint = (sql, why) -> IqParity.determinism(
                IqParity.runs(List.of(new IqParity.Case("lite/x.yaml", "a", sql, List.of("sqla17-default"), null,
                        null, null, null, null, null, why))).get(0), tables, SqlParity.Status.REFUSED);
        // a row limit without ORDER BY, or whose ORDER BY reaches no unique column of each FROM item
        assertTrue(lint.apply("SELECT TOP 2 id FROM t", null).contains("has no ORDER BY"));
        assertTrue(lint.apply("SELECT TOP ALL START AT 2 id FROM t", null).contains("has no ORDER BY"));
        assertTrue(lint.apply("SELECT TOP 1 id FROM t ORDER BY sm", null).contains("no unique column of t"));
        assertTrue(lint.apply("SELECT FIRST id FROM t ORDER BY 1 + 0", null) != null);
        assertTrue(lint.apply("SELECT id FROM t WHERE id IN (SELECT TOP 1 t_id FROM u ORDER BY t_id)", null)
                != null);
        assertTrue(lint.apply("SELECT d.id FROM (SELECT TOP 1 id FROM t ORDER BY sm DESC) d", null) != null);
        assertTrue(lint.apply("SELECT TOP 1 t.id FROM t, u ORDER BY t.id", null).contains("no unique column of u"));
        assertTrue(lint.apply("SELECT TOP 1 k FROM g GROUP BY k, v ORDER BY k", null) != null);
        assertTrue(lint.apply("SELECT TOP 1 id FROM t UNION ALL SELECT id FROM u ORDER BY 1", null) != null);
        // a LIST without its own ORDER BY, of no GROUP BY key
        assertTrue(lint.apply("SELECT LIST(id) FROM g", null).contains("LIST"));
        assertTrue(lint.apply("SELECT k, LIST(DISTINCT v) FROM g GROUP BY k ORDER BY 1", null).contains("LIST"));
        assertTrue(lint.apply("SELECT (SELECT LIST(v) FROM g WHERE g.k = t.id) FROM t ORDER BY id", null) != null);
        // the key reached, by name, qualified, by position, by an alias, last or before a later key; a key per item
        for (String sql : List.of("SELECT TOP 2 id FROM t ORDER BY sm, id", "SELECT TOP 2 id FROM t ORDER BY t.id DESC",
                "SELECT TOP 2 x.id FROM t AS x ORDER BY x.sm, x.id", "SELECT TOP 2 id, sm FROM t ORDER BY 2, 1",
                "SELECT TOP 2 id AS i FROM t ORDER BY sm, i", "SELECT TOP 2 START AT 2 id FROM DBA.t ORDER BY id, sm",
                "SELECT TOP 1 t.id FROM t, u ORDER BY u.id, t.id",
                "SELECT TOP 1 t.id FROM t JOIN u ON u.id = t.id ORDER BY t.id",
                "SELECT TOP 2 row_num FROM sa_rowgenerator(1, 5) ORDER BY row_num DESC",
                "SELECT TOP 1 d.id FROM (SELECT id, sm FROM t) d ORDER BY d.sm, d.id",
                "SELECT TOP 1 d.k FROM (SELECT k, COUNT(*) AS n FROM g GROUP BY k) d ORDER BY d.n, d.k",
                "SELECT TOP 1 k, COUNT(*) FROM g GROUP BY k ORDER BY 2 DESC, 1",
                "SELECT DISTINCT TOP 1 sm FROM t ORDER BY sm",
                // at most one row, or EXISTS (whether a row is kept)
                "SELECT TOP 1 1", "SELECT TOP 1 COUNT(*) FROM t", "SELECT FIRST dummy_col FROM DUMMY",
                "SELECT id FROM t WHERE EXISTS (SELECT TOP 1 1 FROM u WHERE u.t_id = t.id) ORDER BY id",
                // a LIST ordered, of a GROUP BY key, of a constant; a statement the reader rejects keeps no rows
                "SELECT LIST(id ORDER BY id) FROM g", "SELECT k, LIST(k) FROM g GROUP BY k ORDER BY 1",
                "SELECT k, LIST(DISTINCT 1) FROM g GROUP BY k ORDER BY 1", "SELECT TOP 2 WITH TIES id FROM t")) {
            assertEquals(null, lint.apply(sql, null), sql);
        }
        // IQ island, 2026-10-14 (the combination with the options framework): a run that begins with SET statements
        // is linted on its SELECT, read under the settings they make (quoted_identifier Off: ORDER BY "id" orders by
        // a string); an error in them keeps no rows; a case's settings key reads alike
        assertTrue(lint.apply("SET TEMPORARY OPTION date_order = 'DMY'; SELECT TOP 2 id FROM t", null)
                .contains("has no ORDER BY"));
        assertEquals(null, lint.apply("SET TEMPORARY OPTION date_order = 'DMY'; SELECT TOP 2 id FROM t ORDER BY id",
                null));
        assertEquals(null, lint.apply("SELECT TOP 2 id FROM t ORDER BY \"id\"", null));
        assertTrue(lint.apply("SET TEMPORARY OPTION quoted_identifier = 'Off'; SELECT TOP 2 id FROM t ORDER BY \"id\"",
                null).contains("no unique column of t"));
        assertEquals(null, lint.apply("SET TEMPORARY OPTION no_such_option = 1; SELECT TOP 2 id FROM t", null));
        assertTrue(IqParity.determinism(IqParity.runs(List.of(new IqParity.Case("lite/x.yaml", "a",
                "SELECT TOP 2 id FROM t", List.of("sqla17-default"), null, null, null, null, null,
                "query.date_order='DMY'", null))).get(0), tables, SqlParity.Status.REFUSED)
                .contains("has no ORDER BY"));
        // a case that tests a refusal or an error opts out with its reason: it must not PASS, nor opt out of nothing
        assertEquals(null, lint.apply("SELECT TOP 2 id FROM t", "tests the refusal of a TOP without ORDER BY"));
        assertTrue(IqParity.determinism(IqParity.runs(List.of(new IqParity.Case("lite/x.yaml", "a",
                "SELECT TOP 2 id FROM t", List.of("iq16-default"), null, null, null, null, null, null, "a refusal")))
                .get(0), tables, SqlParity.Status.PASS).contains("PASSes"));
        assertTrue(lint.apply("SELECT TOP 2 id FROM t ORDER BY id", "a refusal").contains("nothing to opt out of"));
        // a skipped case is not run
        assertEquals(null, IqParity.determinism(IqParity.runs(List.of(new IqParity.Case("lite/x.yaml", "a",
                "SELECT TOP 2 id FROM t", List.of("iq16-default"), null, "a skip", null, null))).get(0), tables,
                SqlParity.Status.SKIP));
        // the key is the schema's: a table whose id repeats has none
        List<SqlParity.Table> twice = List.of(new SqlParity.Table("t", List.of(new SqlParity.Column("id", "integer")),
                List.of(List.of(1L), List.of(1L))));
        assertThrows(IllegalStateException.class, () -> IqParity.determinism(IqParity.runs(List.of(
                new IqParity.Case("lite/x.yaml", "a", "SELECT TOP 1 id FROM t ORDER BY id", List.of("iq16-default"),
                null, null, null, null))).get(0), twice, SqlParity.Status.PASS));
        // every committed case: the lanes check each run with its status; here, each case is deterministic or says
        // why not
        List<IqParity.Run> runs = IqParity.runs(IqParity.cases(Runfile.property("iq.parity.lite").getParent()));
        List<SqlParity.Table> schema = IqParity.schema(Runfile.property("iq.parity.schema"));
        List<String> problems = new ArrayList<>();
        for (IqParity.Run r : runs) {
            String p = IqParity.determinism(r, schema, SqlParity.Status.REFUSED);
            if (p != null) {
                problems.add(p);
            }
        }
        assertTrue(problems.isEmpty(), String.join("\n", problems));
    }

    @Test
    void aTieAmongRoundedSortKeysIsNoTie() {
        // IQ island, 2026-10-14 (review 4's B2): .9990 and .9990 are shown alike, but SQL Anywhere sorts 997/998
        // before 998/999 -- the tie rule may not hide lite's other order where a select item is (or reads) a quotient
        // or an AVG
        java.util.function.BiFunction<String, String, List<String>> judge = (sql, type) -> {
            IqParity.Run run = IqParity.runs(List.of(new IqParity.Case("lite/x.yaml", "a", sql, List.of("iq16-default"),
                    null, null, null, null))).get(0);
            IqAnswers.Snap snap = new IqAnswers.Snap("lite/x/a", "iq16-default", "", List.of("id", "q"),
                    List.of("integer", type), List.of(List.of("2", "1"), List.of("1", "1"), List.of("3", "3")), null,
                    List.of(0, 0));
            SqlParity.Grid lite = new SqlParity.Grid(List.of("id", "q"), List.of(List.of(1L, 1L), List.of(2L, 1L),
                    List.of(3L, 3L)));
            SqlParity.Grid sa = ((SqlParity.Rows) IqAnswers.expected(run, snap)).grid();
            return IqParity.compare(run, sa, snap, lite, List.of("Integer", "Integer"));
        };
        assertFalse(judge.apply("SELECT id, v / w AS q FROM d ORDER BY q", "numeric").isEmpty());
        assertFalse(judge.apply("SELECT id, AVG(v) AS q FROM d GROUP BY id ORDER BY q", "numeric").isEmpty());
        assertFalse(judge.apply("SELECT id, q FROM (SELECT id, v / w AS q FROM d) e ORDER BY q", "numeric").isEmpty());
        // ties of values shown as they are sorted stay the plan's order
        assertEquals(List.of(), judge.apply("SELECT id, k AS q FROM d ORDER BY q", "integer"));
    }

    @Test
    void aBothErrorWhoseIqCodeIsNotSqlAnywheresFails() {
        SqlParity.Rejected sa = new SqlParity.Rejected("", "SQLCODE -158, SQLSTATE 22003: Value 32768 out of range");
        RuntimeException wrong = new RuntimeException("#IQ{...}# at line 1: no column 'x' (IQ: SQLCODE -143,"
                + " SQLSTATE 52003)");
        assertEquals(SqlParity.Status.FAIL, IqParity.bothError(wrong, sa).status());
        RuntimeException right = new RuntimeException("#IQ{...}#: out of range for its type, SMALLINT (IQ: SQLCODE"
                + " -158, SQLSTATE 22003)");
        assertEquals(SqlParity.Status.BOTH_ERROR, IqParity.bothError(right, sa).status());
        // an error without IQ's code (a refusal, the database's own) is judged by being one
        assertEquals(SqlParity.Status.BOTH_ERROR, IqParity.bothError(new RuntimeException("Cannot divide 1 by zero"),
                sa).status());
    }

    @Test
    void aNumericCellCarriesSqlAnywheresScale() {
        SqlParity.Grid lite = new SqlParity.Grid(List.of("v"), List.of(List.of(new java.math.BigDecimal("1.50"))));
        assertTrue(IqParity.scale("numeric", 1, lite, 0) != null);
        assertEquals(null, IqParity.scale("numeric", 2, lite, 0));
        SqlParity.Grid whole = new SqlParity.Grid(List.of("v"), List.of(List.of(15L)));
        assertEquals(null, IqParity.scale("numeric", 0, whole, 0));
        assertEquals(null, IqParity.scale("integer", 0, lite, 0));
    }

    @Test
    void aTableNamedInFromIsItsLegendSource_asWritten() {
        Set<String> tables = Set.of("t", "u");
        assertEquals("SELECT [t].id FROM tb('e2e::DB.t') AS [t] WHERE id = 1",
                IqParity.rewrite("SELECT [t].id FROM [t] WHERE id = 1", tables, IqProfile.IQ16_DEFAULT));
        assertEquals("SELECT a.id FROM tb('e2e::DB.t') a, tb('e2e::DB.u') AS U",
                IqParity.rewrite("SELECT a.id FROM t a, U", tables, IqProfile.IQ16_DEFAULT));
        // under a TDS login "t" is a string, no table
        assertEquals("SELECT 1 FROM \"t\"", IqParity.rewrite("SELECT 1 FROM \"t\"", tables, IqProfile.IQ16_TDS));
    }

    /** IQ island, 2026-10-14 (M1e): a case's params are its host variables' values -- each a let before the island,
     *  each a SQL Anywhere variable of its type for the referee (tools/iq-oracle/snap.py reads the same spelling; review
     *  5's M5: a real variable, no longer a constant written into the SQL), {@code null:<type>} a NULL -- and part of its
     *  answer's sqlSha, so editing them makes the answer STALE; an owner-qualified name is left as written. */
    @Test
    void aCaseBindsItsParamsAsLetsBeforeItsIsland() {
        List<IqParity.Param> params = IqParity.params("lo = 3; name = 'x'; amt = 0.50D; day = %2026-03-04;"
                + " stamp = %2026-03-04T05:06:07.5; rate = -1.5; n = null:Integer; neg = -0.50D");
        assertEquals(List.of("Integer", "String", "Decimal(38,2)", "StrictDate", "DateTime", "Float", "Integer",
                "Decimal"), params.stream().map(IqParity.Param::type).toList());
        assertEquals(List.of("BIGINT", "LONG VARCHAR", "NUMERIC(38,2)", "DATE", "TIMESTAMP", "DOUBLE", "BIGINT",
                "NUMERIC(38,2)"), params.stream().map(IqParity.Param::saType).toList());
        assertEquals(java.util.Arrays.asList("3", "'x'", "0.50", "CAST('2026-03-04' AS DATE)",
                "CAST('2026-03-04 05:06:07.5' AS TIMESTAMP)", "CAST(-1.5 AS DOUBLE)", null, "-0.50"),
                params.stream().map(IqParity.Param::saValue).toList());
        assertEquals("[]->cast(@Integer)", params.get(6).pure());
        // the two readers would read a backslash otherwise; names are SQL Anywhere's, distinct without case
        assertThrows(IllegalStateException.class, () -> IqParity.params("s = 'a\\x41b'"));
        assertThrows(IllegalStateException.class, () -> IqParity.params("lo = 1; LO = 2"));
        IqParity.Case c = new IqParity.Case("lite/x.yaml", "a", "SELECT id FROM t, DBA.u WHERE id > @lo",
                List.of("iq16-default"), null, null, null, null, "lo = 3");
        assertEquals("|let lo = 3; #IQ{SELECT id FROM tb('e2e::DB.t') AS t, DBA.u WHERE id > @lo}#;",
                IqParity.island(IqParity.runs(List.of(c)).get(0), Set.of("t", "u")));
        byte[] schema = "tables:\n".getBytes(java.nio.charset.StandardCharsets.UTF_8);
        Map<String, IqAnswers.Snap> answers = new LinkedHashMap<>();
        answers.put("lite/x/a@iq16-default", new IqAnswers.Snap("lite/x/a", "iq16-default",
                IqAnswers.sqlSha(c.sql(), "iq16-default", null, null, "lo = 4", schema), List.of(), List.of(),
                List.of(), null));
        // IQ island, landing 4 (WP1): its iq17-default twin's answer, the live IQ's, stamped with the params too
        answers.put("lite/x/a@iq17-default", new IqAnswers.Snap("lite/x/a", "iq17-default",
                IqAnswers.sqlSha(c.sql(), "iq17-default", null, null, "lo = 3", schema), List.of(), List.of(),
                null, "SQLCODE -141", List.of(), "iq", null));
        List<String> problems = IqAnswers.guards(IqParity.runs(List.of(c)), answers, schema);
        assertEquals(1, problems.size(), String.join("\n", problems));
        assertTrue(problems.get(0).startsWith("STALE: lite/x/a@iq16-default"), problems.get(0));
    }
}
