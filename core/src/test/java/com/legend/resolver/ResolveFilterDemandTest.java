// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.resolver;

import com.legend.Compiler;
import com.legend.compiler.NameResolver;
import com.legend.compiler.spec.SpecCompiler;
import com.legend.compiler.spec.typed.TypedSpec;
import com.legend.lowering.Lowerer;
import com.legend.parser.SpecParser;
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
 * Leg 1 (RELATIONAL_FEATURE_MAP §2.3/§15) — row-set-defining filter demand:
 * a mapping {@code ~filter} that reads THROUGH a join is never demand-gated
 * away (engine: the class-mapping filter applies DURING getAll; filter-read
 * joins are exempt from join cancellation). Previously the H3-pending wall.
 */
class ResolveFilterDemandTest {

    private static final String MODEL = """
            Class m::Person { name: String[1]; }
            Class m::Org { name: String[1]; parent: m::Org[0..1]; children: m::Org[*]; }
            Class m::SOrg { name: String[1]; parent: m::SOrg[0..1]; }
            ###Relational
            Database s::DB (
              Table TP (ID INTEGER, FIRM_ID INTEGER, NAME VARCHAR(50))
              Table TF (ID INTEGER, IS_ACTIVE INTEGER)
              Table ORG (ID INTEGER PRIMARY KEY, PARENT_ID INTEGER, NAME VARCHAR(50))
              Table OTHER (ORG_ID INTEGER PRIMARY KEY, FILTER_VAL INTEGER)
              Join PF (TP.FIRM_ID = TF.ID)
              Join OrgParent (ORG.PARENT_ID = {target}.ID)
              Join OrgChildren (ORG.ID = {target}.PARENT_ID)
              Join OrgOther (ORG.ID = OTHER.ORG_ID)
              Table SORG (ID INTEGER PRIMARY KEY, PARENT_ID INTEGER, NAME VARCHAR(50), FVAL INTEGER)
              Join SOrgParent (SORG.PARENT_ID = {target}.ID)
              Filter ActiveFirms ( [s::DB] @PF | TF.IS_ACTIVE = 1 )
              Filter OtherFilter ( OTHER.FILTER_VAL <= 4 )
              Filter SOrgFilter ( SORG.FVAL <= 4 )
            )
            ###Mapping
            Mapping m::M (
              *m::Person: Relational { ~filter [s::DB] ActiveFirms
                ~mainTable [s::DB] TP
                name: TP.NAME }
              *m::Org: Relational {
                ~filter [s::DB] @OrgOther | [s::DB] OtherFilter
                name: [s::DB] ORG.NAME,
                parent: [s::DB] @OrgParent,
                children: [s::DB] @OrgChildren }
              *m::SOrg: Relational {
                ~filter [s::DB] SOrgFilter
                ~mainTable [s::DB] SORG
                name: [s::DB] SORG.NAME,
                parent: [s::DB] @SOrgParent }
            )
            ###Runtime
            Runtime m::RT { mappings: [m::M]; }
            """;

    private static Connection conn;

    @BeforeAll
    static void setUp() throws SQLException {
        conn = DriverManager.getConnection("jdbc:duckdb:");
        try (Statement st = conn.createStatement()) {
            st.execute("CREATE TABLE TP (ID INTEGER, FIRM_ID INTEGER, NAME VARCHAR)");
            st.execute("INSERT INTO TP VALUES (1, 1, 'Ann'), (2, 2, 'Bob'),"
                    + " (3, NULL, 'Cat')");
            st.execute("CREATE TABLE TF (ID INTEGER, IS_ACTIVE INTEGER)");
            st.execute("INSERT INTO TF VALUES (1, 1), (2, 0)");
            st.execute("CREATE TABLE ORG (ID INTEGER, PARENT_ID INTEGER, NAME VARCHAR)");
            st.execute("INSERT INTO ORG VALUES (1, NULL, 'Alpha'), (2, 1, 'Beta'),"
                    + " (3, 1, 'Gamma')");
            st.execute("CREATE TABLE OTHER (ORG_ID INTEGER, FILTER_VAL INTEGER)");
            st.execute("INSERT INTO OTHER VALUES (1, 3), (2, 9)");
            st.execute("CREATE TABLE SORG (ID INTEGER, PARENT_ID INTEGER,"
                    + " NAME VARCHAR, FVAL INTEGER)");
            st.execute("INSERT INTO SORG VALUES (1, NULL, 'SAlpha', 1),"
                    + " (2, 1, 'SBeta', 2), (3, 1, 'SGamma', 9),"
                    + " (4, 3, 'SDelta', 3)");
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
        try (Statement st = conn.createStatement(); ResultSet rs = st.executeQuery(sql)) {
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
    @DisplayName("bare getAll with join-mediated ~filter (corpus testFilterMappingWithJoin shape)")
    void bareGetAllJoinMediatedFilter() throws SQLException {
        String sql = sqlOf("m::Person.all()->from(m::M, m::RT)");
        assertEquals(1, count(sql, "LEFT OUTER JOIN"),
                "class-result route demands the filter-read join too:\n" + sql);
        assertTrue(sql.contains("IS_ACTIVE"), sql);
    }

    @Test
    @DisplayName("~filter [db]@Join|Filter spelling + class-typed PMs (corpus orgTestMappingWithJoin)")
    void joinPathPrefixedFilterSpelling() throws SQLException {
        String sql = sqlOf("m::Org.all()"
                + "->project([o|$o.name], ['name'])->from(m::M, m::RT)");
        assertEquals(1, count(sql, "LEFT OUTER JOIN"),
                "the @Join|Filter spelling's join materializes once:\n" + sql);
        assertTrue(sql.contains("FILTER_VAL"), sql);
        // Alpha (other row 3<=4) kept; Beta (other row 9>4) filtered;
        // Gamma (no OTHER row, NULL<=4 false) filtered.
        assertEquals(List.of("Alpha"), exec(sql), sql);
    }

    @Test
    @DisplayName("nav into ~filter'd class: hop filter folds into ON (overlapp shape)")
    void navIntoFilteredClassFoldsIntoOn() throws SQLException {
        // Engine golden (testFilterMappingWithProjectionOverlapp): the
        // TARGET's mapping ~filter rides the navigation join's ON — rows
        // with a filtered-out (or absent) parent survive with NULLs; the
        // ROOT keeps its own filter in WHERE. Org here has a plain
        // single-table filter via OrgSelfFilter to isolate the strategy.
        // The QUERY asks for its order (a positional assertion must
        // demand ordering — never inherit anyone's scan-order habit).
        String sql = sqlOf("m::SOrg.all()->project("
                + "[o|$o.name, o|$o.parent.name], ['name','p_name'])"
                + "->sort(ascending(~name))->from(m::M, m::RT)");
        List<String> rows = exec(sql);
        // SAlpha: no parent -> null. SBeta: parent SAlpha (in extent).
        // SDelta: parent SGamma is FILTERED OUT -> null, row SURVIVES
        // (the ON-fold contract; a WHERE-placed hop filter would drop it).
        assertEquals(List.of("SAlpha|null", "SBeta|SAlpha", "SDelta|null"),
                rows, sql);
    }

    @Test
    @DisplayName("join-mediated mapping ~filter demands its slot (no wall, one join)")
    void joinMediatedMappingFilter() throws SQLException {
        String sql = sqlOf("m::Person.all()"
                + "->project([p|$p.name], ['name'])->from(m::M, m::RT)");
        assertEquals(1, count(sql, "LEFT OUTER JOIN"),
                "the filter-read join materializes exactly once:\n" + sql);
        assertTrue(sql.contains("IS_ACTIVE"),
                "the ~filter predicate survives into SQL:\n" + sql);
        // Ann's firm is active; Bob's is inactive; Cat has no firm (NULL
        // never satisfies = 1) — the mapping filter defines the row set.
        assertEquals(List.of("Ann"), exec(sql), sql);
    }
}
