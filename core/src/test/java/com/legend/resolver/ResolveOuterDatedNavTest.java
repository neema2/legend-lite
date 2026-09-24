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

/**
 * Leg 2 (map §3): OUTER-ROW milestoning dates — the date argument of a
 * milestoned property reads the OUTER row ({@code $o.product($o.orderDate)}).
 * Engine contract (testBusinessDateMilestoning:568): the temporal window
 * composes against the outer row's column — never stamped inside the target
 * frame where {@code $o} is out of scope.
 */
class ResolveOuterDatedNavTest {

    private static final String MODEL = """
            Class n::Order { id: Integer[1]; orderDate: StrictDate[0..1]; product: n::Product[*]; orderDetails: n::Detail[*]; }
            Class n::Detail { settlementDate: StrictDate[1]; }
            Class <<temporal.businesstemporal>> n::Product { name: String[1]; kind: String[1]; }
            ###Relational
            Database n::DB (
              Table OrderT (ID INTEGER PRIMARY KEY, PID INTEGER, orderDate DATE)
              Table DetailT (OID INTEGER PRIMARY KEY, settlementDate DATE)
              Join OD (OrderT.ID = DetailT.OID)
              Table ProdT (
                milestoning( business(BUS_FROM=from_z, BUS_THRU=thru_z) )
                ID INTEGER PRIMARY KEY, name VARCHAR(64), kind VARCHAR(16),
                from_z DATE, thru_z DATE )
              Join OP (OrderT.PID = ProdT.ID)
            )
            ###Mapping
            Mapping n::M (
              *n::Order : Relational { ~mainTable [n::DB] OrderT
                id: OrderT.ID, orderDate: OrderT.orderDate,
                product: [n::DB]@OP,
                orderDetails: [n::DB]@OD }
              *n::Detail : Relational { ~mainTable [n::DB] DetailT
                settlementDate: DetailT.settlementDate }
              *n::Product : Relational { ~mainTable [n::DB] ProdT
                name: ProdT.name, kind: ProdT.kind }
            )
            ###Runtime
            Runtime n::RT { mappings: [n::M]; }
            """;

    private static Connection conn;

    @BeforeAll
    static void setUp() throws SQLException {
        conn = DriverManager.getConnection("jdbc:duckdb:");
        try (Statement st = conn.createStatement()) {
            st.execute("CREATE TABLE OrderT (ID INTEGER, PID INTEGER, orderDate DATE)");
            st.execute("INSERT INTO OrderT VALUES (1, 10, DATE '2015-03-01'),"
                    + " (2, 10, DATE '2015-08-01')");
            st.execute("CREATE TABLE DetailT (OID INTEGER, settlementDate DATE)");
            st.execute("INSERT INTO DetailT VALUES (1, DATE '2015-03-01'),"
                    + " (2, DATE '2015-08-01')");
            st.execute("CREATE TABLE ProdT (ID INTEGER, name VARCHAR,"
                    + " kind VARCHAR, from_z DATE, thru_z DATE)");
            // one product, two versions: STOCK until 2015-07-01, then EQUITY
            st.execute("INSERT INTO ProdT VALUES"
                    + " (10, 'P', 'STOCK', DATE '2015-01-01', DATE '2015-07-01'),"
                    + " (10, 'P', 'EQUITY', DATE '2015-07-01', DATE '9999-12-31')");
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

    @Test
    @DisplayName("filter-position: $o.product($o.orderDate).kind — window against outer column")
    void outerDatedFilterPosition() throws SQLException {
        String sql = sqlOf("n::Order.all()"
                + "->filter(o|$o.product($o.orderDate->toOne()).kind == 'STOCK')"
                + "->project([o|$o.id], ['id'])->from(n::M, n::RT)");
        // order 1 (2015-03-01) sees the STOCK version; order 2 (2015-08-01)
        // sees EQUITY — the window must evaluate PER ROW off orderDate.
        assertEquals(List.of("1"), exec(sql + " ORDER BY 1"), sql);
    }

    @Test
    @DisplayName("project-position: $o.product($o.orderDate).kind projects per-row version")
    void outerDatedProjectPosition() throws SQLException {
        String sql = sqlOf("n::Order.all()"
                + "->project([o|$o.id, o|$o.product($o.orderDate->toOne()).kind],"
                + " ['id','kind'])->from(n::M, n::RT)");
        // to-many auto-map: one row per (order, in-window version) — each
        // order has exactly one version in-window at its own date.
        assertEquals(List.of("1|STOCK", "2|EQUITY"), exec(sql + " ORDER BY 1"), sql);
    }

    @Test
    @DisplayName("form 2: $o.product($o.orderDetails.settlementDate).kind — one nav join only")
    void outerNavDateNoDoubleFan() throws SQLException {
        String sql = sqlOf("n::Order.all()"
                + "->project([o|$o.id, o|$o.product("
                + "$o.orderDetails.settlementDate->toOne()).kind],"
                + " ['id','kind'])->from(n::M, n::RT)");
        // each order: ONE detail row, ONE in-window version — any duplicate
        // means the detail nav materialized twice (the [STOCK, STOCK] bug).
        assertEquals(List.of("1|STOCK", "2|EQUITY"), exec(sql + " ORDER BY 1"), sql);
    }

    @Test
    @DisplayName("two dates on one chain: filter($o.orderDate) + project($o.orderDetails.settlementDate)")
    void twoDatesOneChain() throws SQLException {
        // Engine golden testBusinessDateMilestoning:577 — the corpus
        // WithProject shape: first identity windowed in its join ON,
        // second identity (product#d0) reads through ITS OWN dated
        // materialization. Order 1 is the only STOCK-at-orderDate order;
        // its detail-dated read is also STOCK.
        String sql = sqlOf("n::Order.all()"
                + "->filter(o|$o.product($o.orderDate->toOne()).kind == 'STOCK')"
                + "->project([o|$o.product("
                + "$o.orderDetails.settlementDate->toOne()).kind],"
                + " ['kind'])->from(n::M, n::RT)");
        assertEquals(List.of("STOCK"), exec(sql), sql);
    }
}
