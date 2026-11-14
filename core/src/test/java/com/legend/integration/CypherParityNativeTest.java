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

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.sql.Connection;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The Cypher parity corpus on the native store (docs/WRITES_DESIGN_2026_10_18.md §3 B, leg 3): each graph held by
 * the native store's fixed tables ({@code nodes}, {@code node_props}, {@code rels}, {@code rel_props}) and read
 * through views that are the model's tables, the model, its mapping and the island the domain lane's own
 * ({@link CypherGraph.Realization#NATIVE}). On the platform's engine; each case's status pinned in
 * {@code native-ledger.tsv} (run with {@code --jvm_flag=-Dcypher.parity.native.update=<core/src/test/resources/
 * cypher-parity/native-ledger.tsv in the workspace>} to rewrite it), and each case the native store answers otherwise
 * than the domain lane ({@code ledger.tsv}'s {@code duckdb} column) listed.
 */
class CypherParityNativeTest {

    private static final TestBackend PLATFORM = TestBackends.platform();

    @Test
    void theCorpusReadsTheNativeStoreAsItReadsTheDomainTables() throws Exception {
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
        List<String> problems = new ArrayList<>();
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

                @Override
                public void execute(String sql) throws Exception {
                    try (Statement s = session.createStatement()) {
                        s.execute(sql);
                    }
                }

                @Override
                public List<List<Object>> query(String sql) throws Exception {
                    List<List<Object>> rows = new ArrayList<>();
                    try (Statement s = session.createStatement(); java.sql.ResultSet rs = s.executeQuery(sql)) {
                        int n = rs.getMetaData().getColumnCount();
                        while (rs.next()) {
                            List<Object> row = new ArrayList<>();
                            for (int i = 1; i <= n; i++) {
                                row.add(rs.getObject(i));
                            }
                            rows.add(row);
                        }
                    }
                    return rows;
                }
            }, problems, CypherGraph.Realization.NATIVE);
        }
        if (only != null) {
            outcomes.forEach((k, o) -> System.out.println(o.status() + "\t" + k + "\t" + o.detail()));
            return;
        }
        CypherParity.census(outcomes, "native");
        Map<String, String> domain = new LinkedHashMap<>();
        for (String line : Files.readAllLines(Runfile.property("cypher.parity.ledger"), StandardCharsets.UTF_8)) {
            String[] parts = line.split("\t", -1);
            if (line.startsWith("#") || parts.length < 2 || parts[0].equals("case")) {
                continue;
            }
            domain.put(parts[0], parts[1]);
        }
        Map<String, Integer> moves = new TreeMap<>();
        List<String> differ = new ArrayList<>();
        for (Map.Entry<String, SqlParity.Outcome> e : outcomes.entrySet()) {
            String was = domain.get(e.getKey());
            String now = e.getValue().status().name();
            if (was != null && !was.equals(now)) {
                moves.merge(was + " -> " + now, 1, Integer::sum);
                differ.add(e.getKey() + "\t" + was + " -> " + now + "\t" + e.getValue().detail());
            }
        }
        Map<String, Integer> counts = new TreeMap<>();
        outcomes.values().forEach(o -> counts.merge(o.status().name(), 1, Integer::sum));
        System.out.println("the native store: " + counts + "; moved from the domain lane: " + moves);
        differ.stream().limit(300).forEach(System.out::println);
        String update = System.getProperty("cypher.parity.native.update");
        problems.addAll(CypherParity.nativeRatchet(cases, outcomes, Runfile.property("cypher.parity.native.ledger"),
                update == null ? null : java.nio.file.Path.of(update)));
        assertTrue(problems.isEmpty(), problems.size() + " problems:\n" + String.join("\n", problems.subList(0,
                Math.min(problems.size(), 200))));
    }
}
