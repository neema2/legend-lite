package com.legend.integration;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.legend.testbackend.TestBackends;

import java.io.StringWriter;
import java.sql.Connection;
import java.sql.Statement;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

/**
 * Two shapes the stress corpus's financial model reached through GraphQL (2026-10-21) and no smaller model had:
 * <ul>
 *   <li>an EXISTS under an exists over a navigation ROUTED to a set of another mapping ({@code book[positions_Book]}
 *       in the agencylending mapping, the set in the positions mapping): the routed row carries the class's
 *       properties and the route's key only, and the inner hop's association joins on the row's own key column --
 *       walled 'UnfoldableRef: DESK_ID' on 1,415 GraphQL twins of the stress services;</li>
 *   <li>a graph fetch of a DERIVED property whose body reads properties mapped through joins, the properties
 *       themselves not selected ({@code projectlink::PricedTrade.backConverted}): the leaf demanded no join slot --
 *       walled 'references column PricedTrade_Trade_NOTIONAL'.</li>
 * </ul>
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class RoutedNavigationIntegrationTest {

    private static final String MODEL = """
            Class m::A { id: String[1]; b: m::B[0..1]; }
            Class m::B { id: String[1]; name: String[1]; }
            Class m::C { id: String[1]; name: String[1]; }
            Association m::B_C { bs: m::B[*]; c: m::C[0..1]; }
            Class m::Priced
            {
              id: String[1];
              amount: Float[1];
              rate: Float[1];
              converted() { $this.amount * $this.rate }: Float[1];
              roundTrip() { $this.converted / $this.rate }: Float[1];
            }
            ###Relational
            Database m::Db (
              Table A (ID VARCHAR(10) PRIMARY KEY, B_ID VARCHAR(10))
              Table B (ID VARCHAR(10) PRIMARY KEY, NAME VARCHAR(10), C_ID VARCHAR(10))
              Table C (ID VARCHAR(10) PRIMARY KEY, NAME VARCHAR(10))
              Table P (ID VARCHAR(10) PRIMARY KEY, AMOUNT_ID VARCHAR(10), RATE_ID VARCHAR(10))
              Table AMT (ID VARCHAR(10) PRIMARY KEY, VALUE DOUBLE)
              Table RT (ID VARCHAR(10) PRIMARY KEY, VALUE DOUBLE)
              Join A_B(A.B_ID = B.ID)
              Join B_C(B.C_ID = C.ID)
              Join P_Amt(P.AMOUNT_ID = AMT.ID)
              Join P_Rt(P.RATE_ID = RT.ID)
            )
            ###Mapping
            Mapping m::MA (
              m::A: Relational { ~mainTable [m::Db] A  id: [m::Db] A.ID, b[m_B]: [m::Db] @A_B }
            )
            Mapping m::MB (
              m::B: Relational { ~mainTable [m::Db] B  id: [m::Db] B.ID, name: [m::Db] B.NAME }
              m::C: Relational { ~mainTable [m::Db] C  id: [m::Db] C.ID, name: [m::Db] C.NAME }
            )
            Mapping m::M (
              include m::MA
              include m::MB
              m::B_C: Relational { AssociationMapping ( bs: [m::Db] @B_C, c: [m::Db] @B_C ) }
              m::Priced: Relational
              {
                ~mainTable [m::Db] P
                id: [m::Db] P.ID,
                amount: [m::Db] @P_Amt | [m::Db] AMT.VALUE,
                rate: [m::Db] @P_Rt | [m::Db] RT.VALUE
              }
            )
            ###Connection
            RelationalDatabaseConnection m::Conn { type: DuckDB; specification: DuckDB { }; auth: Test; }
            ###Runtime
            Runtime m::RT { mappings: [ m::M ]; connections: [ m::Db: [ c: m::Conn ] ]; }
            """;

    private Connection connection;

    @BeforeAll
    void seed() throws Exception {
        connection = TestBackends.platform().open();
        try (Statement s = connection.createStatement()) {
            s.execute("CREATE TABLE A (ID VARCHAR(10) PRIMARY KEY, B_ID VARCHAR(10))");
            s.execute("CREATE TABLE B (ID VARCHAR(10) PRIMARY KEY, NAME VARCHAR(10), C_ID VARCHAR(10))");
            s.execute("CREATE TABLE C (ID VARCHAR(10) PRIMARY KEY, NAME VARCHAR(10))");
            s.execute("INSERT INTO C VALUES ('c1', 'red'), ('c2', 'blue')");
            s.execute("INSERT INTO B VALUES ('b1', 'one', 'c1'), ('b2', 'two', 'c2'), ('b3', 'three', NULL)");
            // a1 -> red, a2 -> blue, a3 -> no C, a4 -> no B, a5 -> a dangling B
            s.execute("INSERT INTO A VALUES ('a1', 'b1'), ('a2', 'b2'), ('a3', 'b3'), ('a4', NULL), ('a5', 'bx')");
            s.execute("CREATE TABLE P (ID VARCHAR(10) PRIMARY KEY, AMOUNT_ID VARCHAR(10), RATE_ID VARCHAR(10))");
            s.execute("CREATE TABLE AMT (ID VARCHAR(10) PRIMARY KEY, VALUE DOUBLE)");
            s.execute("CREATE TABLE RT (ID VARCHAR(10) PRIMARY KEY, VALUE DOUBLE)");
            s.execute("INSERT INTO AMT VALUES ('x1', 100.0), ('x2', 40.0)");
            s.execute("INSERT INTO RT VALUES ('r1', 2.0), ('r2', 0.5)");
            s.execute("INSERT INTO P VALUES ('p1', 'x1', 'r1'), ('p2', 'x2', 'r2')");
        }
    }

    @AfterAll
    void close() throws Exception {
        connection.close();
    }

    private String run(String query) throws Exception {
        StringWriter w = new StringWriter();
        com.legend.Execution.executeWire(MODEL, query, "m::RT", connection,
                com.legend.lowering.WireRender.Format.JSON, w);
        return w.toString();
    }

    @Test
    void anExistsUnderAnExistsOverARoutedNavigationCorrelatesOnTheRoutedRowsKey() throws Exception {
        assertEquals("[{\"id\":\"a1\"}]", run("|m::A.all()"
                + "->filter(x|$x.b->exists(e1|$e1.c->exists(e2|$e2.name->toOne() == 'red')))"
                + "->project(~[id:x|$x.id])->sort(~id->ascending())"));
        assertEquals("[{\"id\":\"a1\"},{\"id\":\"a2\"}]", run("|m::A.all()"
                + "->filter(x|$x.b->exists(e1|$e1.c->exists(e2|$e2.name->toOne() > ' ')))"
                + "->project(~[id:x|$x.id])->sort(~id->ascending())"));
        // the inner exists negated: a B without a C qualifies, no B at all does not
        assertEquals("[{\"id\":\"a3\"}]", run("|m::A.all()"
                + "->filter(x|$x.b->exists(e1|!$e1.c->exists(e2|$e2.name->toOne() > ' ')))"
                + "->project(~[id:x|$x.id])->sort(~id->ascending())"));
    }

    @Test
    void aDerivedGraphLeafJoinsThePropertiesItsBodyReads() throws Exception {
        String tree = "#{m::Priced{id,roundTrip}}#";
        assertEquals("[{\"id\":\"p1\",\"roundTrip()\":100.0},{\"id\":\"p2\",\"roundTrip()\":40.0}]",
                run("|m::Priced.all()->sortBy(x|$x.id)->graphFetch(" + tree + ")->serialize(" + tree + ")"));
        String one = "#{m::Priced{id,converted}}#";
        assertEquals("[{\"id\":\"p2\",\"converted()\":20.0}]",
                run("|m::Priced.all()->filter(x|$x.id == 'p2')->graphFetch(" + one + ")->serialize(" + one + ")"));
    }
}
