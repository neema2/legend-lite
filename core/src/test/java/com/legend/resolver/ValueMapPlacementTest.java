// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.resolver;

import com.legend.Compiler;
import com.legend.compiler.NameResolver;
import com.legend.compiler.spec.SpecCompiler;
import com.legend.compiler.spec.typed.TypedSpec;
import com.legend.lowering.Lowerer;
import com.legend.sql.SqlQuery;
import com.legend.sql.dialect.DuckDb;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * §4AD P1 (NAV_ROUTING_PLACEMENT_ADDENDUM_4AD §6): VALUE-position
 * filtered navigations take the ROW-DROPPING placement — bare fanned
 * LEFT join, qualifier predicate hoisted to the frame's top WHERE
 * (the measured batch-0 cell; engine witnesses
 * testQualifierWithOperation / testTwoQualifiersWithOperation).
 * These are the R4 distinguishing witnesses: under the old in-target
 * (row-preserving) placement every no-match parent survived as a NULL
 * row and pure's null-skipping plus MINTED phantom values — each test
 * here FAILS on that emission and PASSES on this one.
 */
class ValueMapPlacementTest {

    private static final String MODEL = """
            Class m::Org { name: String[1]; nick: String[0..1];
              nick2: String[0..1]; children: m::Org[*]; }
            ###Relational
            Database s::DB (
              Table ORG (ID INTEGER PRIMARY KEY, PARENT_ID INTEGER,
                NAME VARCHAR(50), NICK VARCHAR(50), NICK2 VARCHAR(50))
              Join OrgChildren (ORG.ID = {target}.PARENT_ID)
            )
            ###Mapping
            Mapping m::M (
              *m::Org: Relational { ~mainTable [s::DB] ORG
                name: [s::DB] ORG.NAME,
                nick: [s::DB] ORG.NICK,
                nick2: [s::DB] ORG.NICK2,
                children: [s::DB] @OrgChildren }
            )
            ###Runtime
            Runtime m::RT { mappings: [m::M]; }
            """;

    private static Connection conn;

    @BeforeAll
    static void setUp() throws SQLException {
        conn = DriverManager.getConnection("jdbc:duckdb:");
        try (Statement st = conn.createStatement()) {
            st.execute("CREATE TABLE ORG (ID INTEGER, PARENT_ID INTEGER,"
                    + " NAME VARCHAR, NICK VARCHAR, NICK2 VARCHAR)");
            // Alpha has children Beta (nick pair differs) and Gamma
            // (nick pair both NULL); Beta and Gamma are childless.
            st.execute("INSERT INTO ORG VALUES"
                    + " (1, NULL, 'Alpha', NULL, NULL),"
                    + " (2, 1, 'Beta', 'b', 'x'),"
                    + " (3, 1, 'Gamma', NULL, NULL)");
        }
    }

    @AfterAll
    static void tearDown() throws SQLException {
        conn.close();
    }

    private static String sqlOf(String query) {
        var ctx = Compiler.compileModel(MODEL);
        SpecCompiler specs = new SpecCompiler(ctx);
        List<TypedSpec> body = specs.typeQueryBody(
                NameResolver.resolveQuery(com.legend.testing.Own.spec(query)));
        List<TypedSpec> resolved = new StoreResolver(ctx, specs).resolve(body, null);
        SqlQuery plan = new Lowerer(com.legend.lowering.PlatformRegistrations.catalogTable()).lower(resolved);
        return new DuckDb().render(plan);
    }

    private List<String> exec(String sql) throws SQLException {
        List<String> rows = new ArrayList<>();
        try (Statement st = conn.createStatement();
                ResultSet rs = st.executeQuery(sql)) {
            int n = rs.getMetaData().getColumnCount();
            while (rs.next()) {
                StringBuilder b = new StringBuilder();
                for (int i = 1; i <= n; i++) {
                    if (i > 1) {
                        b.append("|");
                    }
                    b.append(rs.getObject(i));
                }
                rows.add(b.toString());
            }
        }
        return rows;
    }

    private static int count(String sql, String kw) {
        int c = 0;
        for (int i = sql.indexOf(kw); i >= 0; i = sql.indexOf(kw, i + kw.length())) {
            c++;
        }
        return c;
    }

    @Test
    @DisplayName("value read + null-skipping plus: pred drops rows, no phantom mints")
    void valuePredDropsRows() throws SQLException {
        String sql = sqlOf("m::Org.all()->map(o|"
                + "$o.children->filter(c|$c.name == 'Beta')->toOne().name + 'T')"
                + "->from(m::M, m::RT)");
        // Alpha's Beta child survives; Beta/Gamma (childless) DROP at
        // the WHERE — under in-target parking they survived as NULL
        // rows and concat minted a phantom 'T' each (['BetaT','T','T']).
        assertEquals(List.of("BetaT"), exec(sql), sql);
        // INNER renders as bare JOIN (SqlSource.Kind)
        assertEquals(0, count(sql, "LEFT OUTER JOIN"), sql);
        assertEquals(1, count(sql, "JOIN"), sql);
    }

    @Test
    @DisplayName("multi-occurrence, different preds: join copies fork, preds AND one WHERE")
    void multiOccurrenceForksCopies() throws SQLException {
        String sql = sqlOf("m::Org.all()->map(o|"
                + "$o.children->filter(c|$c.name == 'Beta')->toOne().name"
                + " + $o.children->filter(c|$c.name == 'Gamma')->toOne().name)"
                + "->from(m::M, m::RT)");
        // the measured multi-occurrence cell (engine golden
        // testTwoQualifiersWithOperation): per-occurrence join copies,
        // ALL predicates conjoined in the ONE top WHERE
        assertEquals(List.of("BetaGamma"), exec(sql), sql);
        assertEquals(0, count(sql, "LEFT OUTER JOIN"), sql);
        assertEquals(2, count(sql, "JOIN"), sql);
        assertTrue(sql.contains("'Beta'") && sql.contains("'Gamma'"), sql);
    }

    @Test
    @DisplayName("multi-occurrence, EQUAL preds: one shared join identity")
    void equalPredsShareOneJoin() throws SQLException {
        String sql = sqlOf("m::Org.all()->map(o|"
                + "$o.children->filter(c|$c.name == 'Beta')->toOne().name"
                + " + $o.children->filter(c|$c.name == 'Beta')->toOne().name)"
                + "->from(m::M, m::RT)");
        assertEquals(List.of("BetaBeta"), exec(sql), sql);
        assertEquals(0, count(sql, "LEFT OUTER JOIN"), sql);
        assertEquals(1, count(sql, "JOIN"), sql);
    }

    @Test
    @DisplayName("FILTER position, null-safe cmp: qual-pred groups with cmp (engine cell)")
    void filterPositionGroupsQualPredWithCmp() throws SQLException {
        // §4AD P2 R4 witness. Engine form (testQualifierQueryWithOr
        // cell): BARE fanned join, (qual-pred AND cmp) grouped in the
        // WHERE — a parent with NO matching child fails the group
        // (qual-pred is false on the pad row). The in-target form
        // wrongly PASSES it: the pad row's NULL nick pair satisfies a
        // null-safe cmp when the qual-pred no longer guards the group.
        String sql = sqlOf("m::Org.all()->filter(o|"
                + "$o.children->filter(c|$c.name == 'Beta')->toOne().nick"
                + " == $o.children->filter(c|$c.name == 'Beta')->toOne().nick2)"
                + "->map(o|$o.name)->from(m::M, m::RT)");
        // Alpha's Beta child: 'b' vs 'x' -> false. Beta/Gamma:
        // childless -> qual-pred false on the pad -> fail. Engine: [].
        assertEquals(List.of(), exec(sql), sql);
    }

    @Test
    @DisplayName("FILTER position, OR disjuncts: a missing occurrence must not kill the row")
    void filterPositionOrKeepsRowSurvival() throws SQLException {
        // regression pin: filter-position joins stay LEFT (INNER would
        // wrongly drop parents failing occurrence 1 even when
        // occurrence 2's disjunct holds), and the fan is the IN-TARGET
        // (engine ON-form) count — the engine emits pred-in-ON for this
        // toOne-pierced comparison family
        // (filterFunctionExpressionWithConditionOnRightTableOrExpression
        // golden: ON (root.ID = FIRMID and LASTNAME = ...)), so each
        // occurrence fans MATCHED children only: one surviving row.
        String sql = sqlOf("m::Org.all()->filter(o|"
                + "$o.children->filter(c|$c.name == 'NoSuch')->toOne().name == 'X'"
                + " || $o.children->filter(c|$c.name == 'Gamma')->toOne().name == 'Gamma')"
                + "->map(o|$o.name)->from(m::M, m::RT)");
        assertEquals(List.of("Alpha"), exec(sql), sql);
    }

    @Test
    @DisplayName("injected conjunct keeps the filter-position equality rule (double-NULL)")
    void doubleNullConjunctRuleParity() throws SQLException {
        // risk-6 pin (addendum §6): the hoisted predicate lowers under
        // the SAME NullSemantics filter arm as any user filter — the
        // engine's own position-blind rule (nullSafeEqualsOperation
        // case 5: both operands nullable). Gamma's both-NULL nick pair
        // therefore MATCHES; Beta's ('b','x') does not.
        String sql = sqlOf("m::Org.all()->map(o|"
                + "$o.children->filter(c|$c.nick == $c.nick2)->toOne().name + 'T')"
                + "->from(m::M, m::RT)");
        assertEquals(List.of("GammaT"), exec(sql), sql);
    }

    @Test
    @DisplayName("BARE many read reduced by the body: every parent keeps a value (LEFT)")
    void bareManyReduceKeepsParents() throws SQLException {
        // batch 70 (user-ratified rule): no toOne narrows the read, and
        // joinStrings over an empty String[*] is '' — an org with no
        // matching child contributes '' + 'T' = 'T', one value per org.
        // The head joins LEFT with its predicate in-target (the engine's
        // BuildCorrelatedSubQuery shape); the toOne-narrowed pins above
        // keep INNER (pure has no answer for an empty toOne). (Real pure's
        // `String[*] + String[1]` — plus(strings:String[*]) — is the same
        // reduction; our typer's string plus is binary [1] today, a spec
        // gap noted in the handoff.)
        String sql = sqlOf("m::Org.all()->map(o|"
                + "$o.children->filter(c|$c.name == 'Beta').name->joinStrings(',') + 'T')"
                + "->from(m::M, m::RT)");
        assertEquals(List.of("BetaT", "T", "T"), exec(sql), sql);
        assertEquals(1, count(sql, "LEFT OUTER JOIN"), sql);
    }
}
