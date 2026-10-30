// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.integration;

import com.legend.testing.Lean;

import com.legend.testbackend.TestBackend;
import com.legend.testbackend.TestBackends;
import com.legend.testcases.SqlParity;
import com.legend.testing.Runfile;
import org.junit.jupiter.api.Test;

import java.nio.file.Path;
import java.sql.Connection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Upstream's Postgres parity corpus on DuckDB ({@link SqlParity}): every case of legend-engine's
 * {@code parity-tests} as a {@code #SQL{...}#} island over the corpus's tables in DuckDB, judged against Postgres 16's
 * snapped answers by upstream's rules, and held to the ledger's {@code duckdb} column. An island on DuckDB must
 * answer what Postgres answers; a case that does not is pinned FAIL in the ledger until it does.
 *
 * <p>Its own target ({@code //core:sql_parity_test}): it reads the pinned legend-engine tree. Run with
 * {@code --jvm_flag=-Dsql.parity.update=<the ledger's path in the workspace>} to write the ledger's column instead.
 */
class SqlParityTest {

    @Test
    void everyUpstreamCaseHasItsLedgerStatusOnThePlatform() throws Exception {
        Path dir = Runfile.property("sql.parity.schema").getParent();
        List<SqlParity.Case> cases = new java.util.ArrayList<>(SqlParity.cases(dir));
        cases.addAll(SqlParity.cases(Runfile.property("sql.parity.lite").getParent(), "lite/"));
        List<SqlParity.Table> tables = SqlParity.schema(dir);
        Set<String> names = SqlParity.tableNames(tables);
        Map<String, SqlParity.Answer> answers = SqlParity.readAnswers(Runfile.property("sql.parity.answers"));
        TestBackend platform = TestBackends.platform();
        String model = SqlParity.model(tables, platform.connectionBody());
        Map<String, SqlParity.Outcome> outcomes = new LinkedHashMap<>();
        try (Connection session = platform.open()) {
            SqlParity.seed(platform, session, tables);
            SqlParity.Legend legend = new SqlParity.Legend() {
                @Override
                public Lean.Result run(String island) throws Exception {
                    return Lean.run(model, island, SqlParity.RUNTIME, session);
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
            };
            for (SqlParity.Case c : cases) {
                outcomes.put(SqlParity.key(c), SqlParity.outcome(c, answers, names, legend));
            }
        }
        String update = System.getProperty("sql.parity.update");
        List<String> problems = SqlParity.ratchet(cases, outcomes, Runfile.property("sql.parity.ledger"), platform.name(),
                update == null ? null : Path.of(update));
        assertTrue(problems.isEmpty(), problems.size() + " cases differ from the ledger:\n"
                + String.join("\n", problems));
    }

    @Test
    void aTextCellComparesAsJsonOnlyWhereTheSnappedAnswerWroteItAsJsonb() {
        // the independent review of PR neema2/legend-lite#32: jsonb's spelling (", ", ": ", keys shorter first) proves
        // the text is a jsonb value's; any other text is compared as text
        assertTrue(SqlParity.cellEquals("{\"b\": [1, 2], \"aa\": true}", "{\"aa\":true,\"b\":[1,2]}"));
        assertFalse(SqlParity.cellEquals("[1,2]", "[1, 2]"));
        assertFalse(SqlParity.cellEquals("{\"aa\": true, \"b\": 1}", "{\"aa\":true,\"b\":1}"));
    }

    @Test
    void aSnappedAnswerAgreesAcrossPlatformsInItsLastDigitsOnly() {
        // cbrt(-0.1) from Postgres on Linux and on Windows: their C libraries differ in the last place
        // (2026-10-12, PR neema2/legend-lite#28's CI)
        assertTrue(SqlParity.agrees(oneCell(-0.46415888336127786), oneCell(-0.464158883361278)));
        assertFalse(SqlParity.agrees(oneCell(-0.46415888336127786), oneCell(-0.4641588833)));
        assertFalse(SqlParity.agrees(oneCell(1.0), oneCell(null)));
        assertFalse(SqlParity.agrees(oneCell(1.0), oneCell("1.0")));
    }

    @Test
    void aTableInsideAParenthesizedJoinIsRewrittenToItsSource() {
        // 2026-10-17, the regression suite: FROM (x LEFT JOIN y ...) left x and y as bare names (15 statements)
        Set<String> tables = Set.of("x", "y");
        assertEquals("select * from (tb('e2e::DB.x') AS x left join tb('e2e::DB.y') AS y on (x1 = y1)) left join"
                + " tb('e2e::DB.x') xx(xx1,xx2) on (x1 = xx1)", SqlParity.rewrite("select * from (x left join y on"
                + " (x1 = y1)) left join x xx(xx1,xx2) on (x1 = xx1)", tables));
        assertEquals("select * from ((tb('e2e::DB.x') AS x join tb('e2e::DB.y') AS y on true))",
                SqlParity.rewrite("select * from ((x join y on true))", tables));
        // TABLE's name, unaliased; a name's star (its descendants) goes with it
        assertEquals("select 1 union all TABLE tb('e2e::DB.x')", SqlParity.rewrite("select 1 union all TABLE x",
                tables));
        assertEquals("select * from tb('e2e::DB.x') xx, tb('e2e::DB.y') AS y where xx.x1 = 1",
                SqlParity.rewrite("select * from x* xx, y* where xx.x1 = 1", tables));
        // another schema's table, where the tables name it so; public's is the search path's
        assertEquals("select * from tb('e2e::DB.s.x') AS x, tb('e2e::DB.y') AS y, s.y",
                SqlParity.rewrite("select * from s.x, public.y, s.y", Set.of("s.x", "y")));
        // a subquery's parenthesis, a function's and an ON's are no join's
        assertEquals("select * from (select x from tb('e2e::DB.y') AS y) s, tb('e2e::DB.x') AS x"
                + " where f(x) and (y in (1))", SqlParity.rewrite("select * from (select x from y) s, x"
                + " where f(x) and (y in (1))", tables));
    }

    private static SqlParity.Answer oneCell(Object value) {
        return new SqlParity.Rows(new SqlParity.Grid(List.of("result"), List.of(Collections.singletonList(value))));
    }
}
