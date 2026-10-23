// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.integration;

import com.legend.server.QueryService;
import com.legend.testbackend.TestBackend;
import com.legend.testbackend.TestBackends;
import com.legend.testcases.IqFuzz;
import com.legend.testcases.IqParity;
import com.legend.testcases.SqlParity;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The fuzzer's differential between DuckDB and another of lite's targets (the IQ scale program,
 * docs/IQ_SCALE_PROGRAM_2026_10_22.md P2.2): the queries ({@link IqFuzz}), DuckDB's answers in process, the pairs'
 * verdicts and their report. No IQ answer is needed: the two targets must agree. A finding is a query the two answer
 * differently (DIFF), one DuckDB answers and the other fails (OTHER_ERROR), or one where only DuckDB raises
 * (ONLY_DUCKDB_RAISES: IQ's error emulated in one target alone); a refusal by name on the other target alone
 * (OTHER_REFUSED) is counted and ranked, not a finding. {@link IqFuzzSnowflakeTest} and {@link IqFuzzPostgresTest}.
 */
final class IqFuzzDifferential {

    private static final TestBackend PLATFORM = TestBackends.platform();

    private IqFuzzDifferential() {
    }

    /** One query of the run: its seed, its form and its SQL. */
    record Query(long seed, int q, String form, String sql) {
    }

    /** One target's answer: its kind ({@link IqFuzzTest#classify}'s, or ROWS) and its rows or its reason. */
    record Answer(String kind, SqlParity.@com.legend.base.Nullable Grid grid, String reason) {
    }

    /** Three queries a shape: the shape, the shape where a predicate holds, and a richer form of it. */
    static List<Query> queries(long seed, int count, int depth, List<SqlParity.Table> tables) {
        List<Query> queries = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            long s = seed * 1_000_003L + i;
            IqFuzz gen = new IqFuzz(s, tables, depth);
            IqFuzz.Shape shape = gen.shape();
            String p = gen.predicate(shape.scope());
            queries.add(new Query(s, 0, "shape", shape.sql()));
            queries.add(new Query(s, 1, "where", shape.where("(" + p + ")")));
            queries.add(new Query(s, 4, "complex", gen.complex()));
        }
        return queries;
    }

    /** Each query's answer on DuckDB, in process. */
    static Map<Query, Answer> onDuckDb(List<Query> queries, String profile, List<SqlParity.Table> tables,
            Set<String> names) throws Exception {
        Map<Query, Answer> duck = new LinkedHashMap<>();
        try (Connection session = PLATFORM.open()) {
            IqParity.seed(PLATFORM, session, tables);
            String model = IqParity.model(tables, PLATFORM.connectionBody());
            QueryService service = new QueryService();
            for (Query q : queries) {
                duck.put(q, answer(service, session, model, q, profile, tables, names));
            }
        }
        return duck;
    }

    static Answer answer(QueryService service, Connection session, String model, Query q, String profile,
            List<SqlParity.Table> tables, Set<String> names) {
        try {
            var result = service.execute(model, IqFuzzTest.island(q.sql(), profile, tables, names, q.seed(), q.q()),
                    IqParity.RUNTIME, session);
            return new Answer("ROWS", SqlParity.grid(result), "");
        } catch (Exception | StackOverflowError e) {
            IqFuzzTest.Outcome o = IqFuzzTest.classify(e);
            return new Answer(o.kind(), null, o.reason());
        }
    }

    /** Each query's pair of answers, judged; the counts to standard out, the findings and the other target's
     *  refusals to the test's outputs ({@code iq-fuzz-<lane>.tsv}, {@code iq-fuzz-<lane>-refusals.tsv}); a finding
     *  fails the test. */
    static void report(String lane, long seed, int count, int depth, String profile, Map<Query, Answer> duck,
            Map<Query, Answer> other) throws java.io.IOException {
        Map<String, Integer> pairs = new TreeMap<>();
        Map<String, Integer> refusals = new TreeMap<>();
        StringBuilder findings = new StringBuilder("seed\tform\tverdict\tsql\tduckdb\t" + lane + "\n");
        int found = 0;
        for (var e : duck.entrySet()) {
            Query q = e.getKey();
            Answer d = e.getValue();
            Answer o = other.get(q);
            String verdict = verdict(q, d, o);
            pairs.merge(verdict, 1, Integer::sum);
            if (verdict.equals("OTHER_REFUSED")) {
                refusals.merge(o.reason().replaceAll("\\d+", "N"), 1, Integer::sum);
            }
            if (verdict.equals("DIFF") || verdict.equals("OTHER_ERROR") || verdict.equals("ONLY_DUCKDB_RAISES")) {
                found++;
                String what = verdict.equals("DIFF") ? String.join("; ", SqlParity.compare(false, null, d.grid(),
                        o.grid())) : o.kind() + " " + o.reason();
                findings.append(q.seed()).append('\t').append(q.form()).append('\t').append(verdict).append('\t')
                        .append(q.sql()).append('\t').append(d.kind().equals("ROWS") ? d.grid().rows().size()
                                + " rows" : d.kind() + " " + d.reason()).append('\t').append(oneLine(what))
                        .append('\n');
            }
        }
        StringBuilder ranked = new StringBuilder("count\treason\n");
        refusals.entrySet().stream().sorted((a, b) -> b.getValue() - a.getValue())
                .forEach(r -> ranked.append(r.getValue()).append('\t').append(r.getKey()).append('\n'));
        System.out.println("iq-fuzz-" + lane + " seed " + seed + ", " + count + " shapes, depth " + depth + ", "
                + profile + ": " + pairs + "; " + found + " findings");
        String outputs = System.getenv("TEST_UNDECLARED_OUTPUTS_DIR");
        if (outputs != null) {
            Files.writeString(Path.of(outputs, "iq-fuzz-" + lane + ".tsv"), findings.toString(),
                    StandardCharsets.UTF_8);
            Files.writeString(Path.of(outputs, "iq-fuzz-" + lane + "-refusals.tsv"), ranked.toString(),
                    StandardCharsets.UTF_8);
        }
        assertTrue(found == 0, found + " findings (the test's outputs, iq-fuzz-" + lane + ".tsv):\n"
                + findings.substring(0, Math.min(findings.length(), 20_000)));
    }

    /** The pair's verdict: REFUSED (DuckDB refused; never sent), MATCH, DIFF, OTHER_REFUSED, OTHER_ERROR,
     *  ONLY_DUCKDB_RAISES or BOTH_RAISE; and two that are no finding:
     *  <ul>
     *    <li>UNORDERED: a query reading a row's neighbour, number or physical frame (LAG, ROW_NUMBER, ROWS BETWEEN)
     *        over an order with ties, whose answers may rightly differ: answered by both, its rows are not
     *        compared;</li>
     *    <li>RAISE_ORDER: an error one target raises for a row the other never computes -- the island's error by
     *        name (its sentinel's text) or Postgres's float8 overflow (the listed pg-double-overflow) -- as SQL leaves
     *        the order of evaluation to the database (Postgres computes a windowed derived table's columns for every
     *        row before the filter outside it, DuckDB for the rows the filter keeps).</li>
     *  </ul> */
    private static String verdict(Query q, Answer d, @com.legend.base.Nullable Answer o) {
        if (o == null) {
            return "REFUSED";
        }
        boolean dRows = d.kind().equals("ROWS");
        boolean oRows = o.kind().equals("ROWS");
        if (dRows && oRows) {
            if (q.sql().contains("LAG(") || q.sql().contains("ROW_NUMBER(") || q.sql().contains("ROWS BETWEEN")) {
                return "UNORDERED";
            }
            return SqlParity.compare(false, null, d.grid(), o.grid()).isEmpty() ? "MATCH" : "DIFF";
        }
        if (dRows) {
            return o.kind().equals("REFUSED") ? "OTHER_REFUSED" : raised(o) ? "RAISE_ORDER" : "OTHER_ERROR";
        }
        return oRows ? raised(d) ? "RAISE_ORDER" : "ONLY_DUCKDB_RAISES" : "BOTH_RAISE";
    }

    /** Whether {@code a} is an error the island raises by name in the database, or Postgres's float8 overflow. */
    private static boolean raised(Answer a) {
        String r = a.reason();
        return r.contains("Cannot cast a collection of size") || r.contains("Cannot divide")
                || r.contains("More than one row returned") || r.contains("value out of range: overflow");
    }

    private static String oneLine(String s) {
        String flat = s.replaceAll("\\s+", " ");
        return flat.length() > 600 ? flat.substring(0, 600) + "..." : flat;
    }
}
