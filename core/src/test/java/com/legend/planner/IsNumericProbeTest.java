// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.planner;

import com.legend.Compiler;
import com.legend.model.ConnectionDefinition.DatabaseType;
import com.legend.testbackend.TestBackends;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * ISNUMERIC IS ONE NUMERIC RULE ON EVERY DATABASE (census J20, the user's ruling of 2026-11-17 23:05): the product's
 * {@code meta::legend::lite::isNumeric} answers whether the whole text is a decimal number (an optional sign, digits
 * with an optional fraction or a fraction alone, an optional exponent; no space), NULL on NULL -- the same answers on
 * DuckDB, H2, SQLite and Postgres. legend-engine's H2 {@code lower(s) = upper(s)} (which holds for {@code ' 1 '} and
 * {@code ''} and fails {@code '1e3'}) is the edge's alone. Each database's plan is the product's, its statement sent
 * as planned.
 */
class IsNumericProbeTest {

    /** The probed texts, then the NULL cell. */
    static final List<String> TEXTS = List.of("1", "-1.5", "1e3", " 1 ", "abc", "");

    /** The rule's answers, in order: the texts', then NULL's. */
    static final List<Boolean> RULE = Arrays.asList(true, true, true, false, false, false, null);

    static String model(DatabaseType type) {
        return """
                ###Relational
                Database s::DB ( Table T ( ID INTEGER PRIMARY KEY, S VARCHAR(10) ) )
                ###Connection
                RelationalDatabaseConnection s::Conn
                {
                    store: s::DB;
                    type: %s;
                    specification: LocalH2 { testDataSetupCSV: 'default\\nT\\nID,S\\n1,\\n'; };
                    auth: DefaultH2;
                }
                ###Runtime
                Runtime s::RT { mappings: [ ]; connections: [ s::DB: [ c1: s::Conn ] ]; }
                """.formatted(type.name());
    }

    static String query() {
        StringBuilder cols = new StringBuilder();
        for (int i = 0; i < TEXTS.size(); i++) {
            cols.append("c").append(i).append(":r|meta::legend::lite::isNumeric('").append(TEXTS.get(i))
                    .append("'), ");
        }
        cols.append("cnull:r|meta::legend::lite::isNumeric($r.S)");
        return "|#>{s::DB.T}#->select(~[ID, S])->extend(~[" + cols + "])";
    }

    @Test
    void theRuleAnswersAlikeOnEveryDatabase() throws Exception {
        List<String> wrong = new ArrayList<>();
        for (DatabaseType type : List.of(DatabaseType.DuckDB, DatabaseType.H2, DatabaseType.SQLite,
                DatabaseType.Postgres)) {
            var plan = Compiler.query(Compiler.compileModel(model(type)), query()).executionPlan("s::RT");
            String sql = plan.answerSql().statement();
            List<Boolean> got = new ArrayList<>();
            try (Connection c = TestBackends.of(type).open(); Statement st = c.createStatement()) {
                st.execute("CREATE TABLE T (ID INTEGER PRIMARY KEY, S VARCHAR(10))");
                st.execute("INSERT INTO T (ID, S) VALUES (1, NULL)");
                try (ResultSet rs = st.executeQuery(sql)) {
                    rs.next();
                    for (int i = 0; i <= TEXTS.size(); i++) {
                        Object v = rs.getObject(3 + i);
                        got.add(v == null ? null : v instanceof Boolean b ? b : ((Number) v).intValue() != 0);
                    }
                } finally {
                    st.execute("DROP TABLE T");
                }
            }
            System.out.println("[isNumeric probe] " + type + ": " + got + " by " + sql);
            if (!RULE.equals(got)) {
                wrong.add(type + ": " + got + " by " + sql);
            }
        }
        assertEquals(List.of(), wrong, "the rule " + RULE + " over " + TEXTS + " and NULL");
    }
}
