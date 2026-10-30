// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.integration;

import com.legend.testing.Lean;

import com.legend.testbackend.TestBackend;
import com.legend.testbackend.TestBackends;
import com.legend.testcases.CypherAnswers;
import com.legend.testcases.CypherGraph;
import com.legend.testcases.CypherParity;
import com.legend.testcases.SqlParity;
import com.legend.testing.Runfile;
import org.junit.jupiter.api.Test;

import java.nio.file.Path;
import java.sql.Connection;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The Cypher parity corpus on DuckDB (docs/CYPHER_STUDY_2026_10_15.md §8.4): every TCK scenario and lite case
 * ({@code core/src/test/resources/cypher-parity}) as a {@code #Cypher{...}#} island over the model its graph
 * implies, judged against Neo4j's snapped answer, each case's status pinned in the ledger's {@code duckdb} column.
 * The answers are checked against the cases first (UNSNAPPED, STALE, GONE, VERSION fail the test). Run with
 * {@code --jvm_flag=-Dcypher.parity.update=<core/src/test/resources/cypher-parity/ledger.tsv in the workspace>} to
 * rewrite this lane's column.
 */
class CypherParityTest {

    /** The platform's own engine: the island's session, its tables and the connection its model declares. */
    private static final TestBackend PLATFORM = TestBackends.platform();

    /** The showcase's queries (docs/CYPHER_SHOWCASE.md): {@code lite/basics.yaml}'s {@code showcase_main} (pinned at
     *  the showcase directory's root), {@code showcase_easy} and {@code showcase_hard} (each in its own directory),
     *  judged by Neo4j above like every case; the model, the Pure and the SQL for two connections pinned below. */
    private static final String[][] SHOWCASES = {
        {"lite/basics/showcase_main", ""},
        {"lite/basics/showcase_easy", "easy"},
        {"lite/basics/showcase_hard", "hard"},
    };

    /** The connections the showcase's SQL is pinned for, each a file of {@code cypher-parity/showcase/}: Snowflake's
     *  is rendered only (planning never connects), DuckDB's is the SQL the lane runs. */
    private static final String[][] SHOWCASE_LANES = {
        {"snowflake", "type: Snowflake;\n    specification: Snowflake { name: 'CYPHER_SHOWCASE'; account: 'acct';"
                + " warehouse: 'wh'; region: 'us-east-1'; };\n    auth: SnowflakePublic { publicUserName: 'u';"
                + " privateKeyVaultReference: 'vault/k'; passPhraseVaultReference: 'vault/pp'; };"},
        {"duckdb", "type: DuckDB;\n    specification: DuckDB { };\n    auth: Test;"},
    };

    /**
     * The showcase (docs/CYPHER_SHOWCASE.md): its island, compiled against the model its graph implies with a
     * Snowflake connection and with a DuckDB one, plans the SQL pinned in {@code cypher-parity/showcase/snowflake.sql}
     * and {@code duckdb.sql}, byte for byte. Run with {@code -Dcypher.showcase.update=<core/src/test/resources/
     * cypher-parity/showcase in the workspace>} to rewrite them.
     */
    @Test
    void theShowcasePlansItsPinnedSql() throws Exception {
        List<CypherParity.Case> cases = CypherParity.cases(Runfile.property("cypher.parity.tck").getParent(),
                Runfile.property("cypher.parity.lite").getParent());
        CypherAnswers.File answers = CypherAnswers.read(Runfile.property("cypher.parity.answers"));
        java.nio.file.Path pinned = Runfile.property("cypher.parity.showcase").getParent();
        String update = System.getProperty("cypher.showcase.update");
        List<String> problems = new ArrayList<>();
        for (String[] showcase : SHOWCASES) {
            String key = showcase[0];
            CypherParity.Case main = cases.stream().filter(c -> c.key().equals(key)).findFirst()
                    .orElseThrow(() -> new AssertionError("no case " + key));
            CypherAnswers.Answer answer = answers.answers().get(key);
            if (answer == null || answer.graph() == null) {
                throw new AssertionError("no snapped answer with a graph for " + key);
            }
            CypherGraph.Inferred inferred = CypherGraph.infer(answer.graph());
            // the model the graph implies and the Pure the island desugars to, pinned beside the SQL (the doc's
            // §6 to §9)
            java.util.Map<String, String> texts = new java.util.LinkedHashMap<>();
            texts.put("model.pure", inferred.model());
            com.legend.compiler.element.ModelContext ctx = com.legend.Compiler.compileModel(CypherGraph.model(
                    inferred, SHOWCASE_LANES[1][1]));
            texts.put("pure.txt", com.legend.protocol.PureComposer.valueSpecification(
                    com.legend.compiler.spec.CypherIslands.translate(new com.legend.protocol.spec.SqlIsland(
                            main.query(), null, com.legend.protocol.spec.SqlIsland.CYPHER, null), ctx),
                    com.legend.protocol.PureComposer.Style.STANDARD) + "\n");
            for (String[] lane : SHOWCASE_LANES) {
                texts.put(lane[0] + ".sql", com.legend.Compiler.query(com.legend.Compiler.compileModel(
                        CypherGraph.model(inferred, lane[1])), main.island()).plan(CypherGraph.RUNTIME).sql()
                        + "\n");
            }
            for (java.util.Map.Entry<String, String> e : texts.entrySet()) {
                String sql = e.getValue();
                String file = showcase[1].isEmpty() ? e.getKey() : showcase[1] + "/" + e.getKey();
                if (update != null) {
                    java.nio.file.Files.writeString(Path.of(update).resolve(file), sql,
                            java.nio.charset.StandardCharsets.UTF_8);
                    continue;
                }
                String expected = java.nio.file.Files.readString(pinned.resolve(file),
                        java.nio.charset.StandardCharsets.UTF_8);
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
        }
        assertTrue(problems.isEmpty(), "the showcase's SQL moved (if on purpose, rewrite it with"
                + " -Dcypher.showcase.update=<core/src/test/resources/cypher-parity/showcase in the workspace> and"
                + " update docs/CYPHER_SHOWCASE.md):\n" + String.join("\n", problems));
    }

    @Test
    void theTranslatorKnowsCyphersFunctionsByName() throws Exception {
        // (the study's §12.84) a name Neo4j does not define is its error, Unknown function, and one it does is refused
        // by name where the translator does not implement it: the names are the oracle's snap of SHOW FUNCTIONS
        List<String> snapped = new ArrayList<>();
        for (String line : java.nio.file.Files.readAllLines(Runfile.property("cypher.parity.functions"))) {
            if (!line.isBlank() && !line.startsWith("#")) {
                snapped.add(line.strip());
            }
        }
        assertEquals(new java.util.TreeSet<>(snapped), new java.util.TreeSet<>(
                com.legend.protocol.cypher.CypherFunctions.names()), "CypherFunctions is not the snapped list"
                + " (cypher-parity/cypher-functions.txt, tools/neo4j-oracle/snap.sh --functions)");
    }

    @Test
    void theJudgesDiffIsTheTcksSideEffects() throws Exception {
        // (the writes design's §6) a write is judged on its side effects by Neo4j's diff of its two graphs, the
        // graph before and the graph after (the oracle snaps both); that diff, computed as the lanes compute lite's,
        // is the TCK's own table on every scenario that states one ("no side effects" an empty one)
        List<CypherParity.Case> cases = CypherParity.cases(Runfile.property("cypher.parity.tck").getParent(),
                Runfile.property("cypher.parity.lite").getParent());
        CypherAnswers.File answers = CypherAnswers.read(Runfile.property("cypher.parity.answers"));
        List<String> problems = new ArrayList<>();
        int tables = 0;
        for (CypherParity.Case c : cases) {
            CypherAnswers.Answer a = answers.answers().get(c.key());
            if (c.sideEffects() == null || a == null || a.graph() == null || a.error() != null) {
                continue;
            }
            CypherParity.Read before = CypherParity.Read.of(a.graph());
            Map<String, Integer> diff = CypherParity.sideEffects(before,
                    a.after() == null ? before : CypherParity.Read.of(a.after()));
            tables += c.sideEffects().isEmpty() ? 0 : 1;
            if (!diff.equals(c.sideEffects())) {
                problems.add(c.key() + ": Neo4j's diff " + diff + ", the TCK's " + c.sideEffects());
            }
        }
        assertTrue(problems.isEmpty(), String.join("\n", problems));
        // the scenarios with a table of counts Neo4j answers (2026-10-18, the TCK as vendored): a fall means the
        // reader stopped seeing them
        assertEquals(248, tables, "the TCK's side-effect tables judged");
    }

    @Test
    void everyCaseHasItsLedgerStatusOnThePlatform() throws Exception {
        List<CypherParity.Case> cases = CypherParity.cases(Runfile.property("cypher.parity.tck").getParent(),
                Runfile.property("cypher.parity.lite").getParent());
        // a development run over some of the cases (-Dcypher.parity.only=<a regex found in a case's key>): each
        // case's outcome printed, the ledger not read
        String only = System.getProperty("cypher.parity.only");
        if (only != null) {
            java.util.regex.Pattern p = java.util.regex.Pattern.compile(only);
            cases = cases.stream().filter(c -> p.matcher(c.key()).find()).toList();
        }
        CypherAnswers.File answers = CypherAnswers.read(Runfile.property("cypher.parity.answers"));
        Map<String, CypherParity.Decision> decisions = CypherParity.decisions(
                Runfile.property("cypher.parity.decisions"), "duckdb");
        List<String> problems = new ArrayList<>(CypherAnswers.guards(cases, answers, CypherParity.PINNED_SERVER));
        problems.addAll(CypherParity.decisionGuards(cases, Runfile.property("cypher.parity.decisions")));
        Map<String, SqlParity.Outcome> outcomes;
        try (Connection session = PLATFORM.open()) {
            outcomes = CypherParity.run(cases, answers.answers(), decisions, new CypherParity.Lane() {
                @Override
                public String connection() {
                    return PLATFORM.connectionBody();
                }

                @Override
                public void seed(List<SqlParity.Table> tables) throws Exception {
                    SqlParity.seed(PLATFORM, session, tables);
                }

                @Override
                public Lean.Result run(String model, String island) throws Exception {
                    return Lean.run(model, island, CypherGraph.RUNTIME, session);
                }

                @Override
                public void begin() throws Exception {
                    session.setAutoCommit(false);
                }

                @Override
                public void rollback() throws Exception {
                    session.rollback();
                    session.setAutoCommit(true);
                }
            }, problems);
        }
        if (only != null) {
            outcomes.forEach((k, o) -> System.out.println(o.status() + "\t" + k + "\t" + o.detail()));
            return;
        }
        CypherParity.census(outcomes, "duckdb");
        System.out.println("the write cases by what a graph over a domain model needs: "
                + CypherParity.writeGroups(cases, answers.answers()));
        String update = System.getProperty("cypher.parity.update");
        problems.addAll(CypherParity.ratchet(cases, outcomes, Runfile.property("cypher.parity.ledger"), "duckdb",
                update == null ? null : Path.of(update)));
        assertTrue(problems.isEmpty(), problems.size() + " problems:\n" + String.join("\n", problems.subList(0,
                Math.min(problems.size(), 200))));
    }
}
