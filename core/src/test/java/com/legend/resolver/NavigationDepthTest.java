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
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Navigation DEPTH (the metamodel-as-relations leg, 2026-09-02): object-
 * space chains of three and four hops mixing association ends, Join-PM
 * class slots and an inheritance-mapped target, with filters, first()
 * and nested emptiness between the hops. Every verdict is the ROWS the
 * database returns for the resolved SQL &mdash; the shapes here are the
 * ones the engine's own navigation functions ({@code classMappingById}:
 * visibility &rarr; visible &rarr; classMappings &rarr; filter &rarr;
 * first &rarr; mainTableAlias) compose, written on an ordinary user model
 * so the mechanism is pinned as a user feature.
 *
 * <p>Mechanisms pinned: nav tails flow through the association branch of
 * flattenSource (provenance for later hops); the materializer walks limit/
 * sort/join wrappers above navigate slots (a first() below a slot flatten);
 * dotted emptiness registers inside nested scopes exactly as at the root;
 * a chained condition after a filtered association hop reads the composed
 * prefix; an association end whose target is inheritance-mapped keeps its
 * binding under a filter.
 */
@DisplayName("Resolver navigation depth: 3-4 hop chains through associations, slots and inheritance")
class NavigationDepthTest {

    static final String MODEL = """
            Class n::A { name: String[1]; }
            Class n::L { tag: String[1]; }
            Class n::B { id: String[1]; }
            Class n::B1 extends n::B { extra: String[1]; }
            Class n::C { name: String[1]; d: n::D[1]; }
            Class n::D { name: String[1]; }
            Class n::R { id: String[1]; c: n::C[1]; }
            Association n::AL { a: n::A[1]; links: n::L[*]; }
            Association n::LB { l: n::L[*]; bs: n::B[*]; }
            Association n::LR { lr: n::L[*]; rs: n::R[*]; }
            ###Relational
            Database n::DB (
              Table TA (ID VARCHAR(10) PRIMARY KEY, NAME VARCHAR(10))
              Table TL (ID VARCHAR(10) PRIMARY KEY, AID VARCHAR(10), TAG VARCHAR(10))
              Table TB (ID VARCHAR(10) PRIMARY KEY, LID VARCHAR(10), CID VARCHAR(10), EXTRA VARCHAR(10))
              Table TC (ID VARCHAR(10) PRIMARY KEY, NAME VARCHAR(10), DID VARCHAR(10))
              Table TD (ID VARCHAR(10) PRIMARY KEY, NAME VARCHAR(10))
              Join AL (TA.ID = TL.AID)
              Join LB (TL.ID = TB.LID)
              Join BC (TB.CID = TC.ID)
              Join CD (TC.DID = TD.ID)
            )
            ###Mapping
            Mapping n::M (
              *n::A: Relational { ~primaryKey([n::DB]TA.ID) ~mainTable [n::DB] TA name: [n::DB]TA.NAME }
              *n::L[l]: Relational { ~primaryKey([n::DB]TL.ID) ~mainTable [n::DB] TL tag: [n::DB]TL.TAG }
              *n::R: Relational { ~primaryKey([n::DB]TB.ID) ~mainTable [n::DB] TB id: [n::DB]TB.ID, c: [n::DB]@BC }
              *n::B: Operation { meta::pure::router::operations::inheritance_OperationSetImplementation_1__SetImplementation_MANY_() }
              n::B1[b1]: Relational { ~primaryKey([n::DB]TB.ID) ~mainTable [n::DB] TB id: [n::DB]TB.ID, extra: [n::DB]TB.EXTRA }
              *n::C: Relational { ~primaryKey([n::DB]TC.ID) ~mainTable [n::DB] TC name: [n::DB]TC.NAME, d: [n::DB]@CD }
              *n::D: Relational { ~primaryKey([n::DB]TD.ID) ~mainTable [n::DB] TD name: [n::DB]TD.NAME }
              n::AL: Relational { AssociationMapping ( a: [n::DB]@AL, links: [n::DB]@AL ) }
              n::LR: Relational { AssociationMapping ( lr: [n::DB]@LB, rs: [n::DB]@LB ) }
              n::LB: Relational { AssociationMapping ( l[b1, l]: [n::DB]@LB, bs[l, b1]: [n::DB]@LB ) }
            )
            ###Runtime
            Runtime n::RT { mappings: [n::M]; }
            """;

    private static Connection conn;

    @BeforeAll
    static void setUp() throws SQLException {
        conn = DriverManager.getConnection("jdbc:duckdb:");
        try (Statement st = conn.createStatement()) {
            st.execute("CREATE TABLE TA(ID VARCHAR, NAME VARCHAR)");
            st.execute("INSERT INTO TA VALUES ('a1','x'),('a2','y')");
            st.execute("CREATE TABLE TL(ID VARCHAR, AID VARCHAR, TAG VARCHAR)");
            st.execute("INSERT INTO TL VALUES ('l1','a1','t1'),('l2','a1','t2'),('l3','a2','t3')");
            st.execute("CREATE TABLE TB(ID VARCHAR, LID VARCHAR, CID VARCHAR, EXTRA VARCHAR)");
            st.execute("INSERT INTO TB VALUES ('b1','l1','c1','e1'),('b2','l2','c2','e2'),('b3','l3','c1','e3')");
            st.execute("CREATE TABLE TC(ID VARCHAR, NAME VARCHAR, DID VARCHAR)");
            st.execute("INSERT INTO TC VALUES ('c1','C-one','d1'),('c2','C-two','d2')");
            st.execute("CREATE TABLE TD(ID VARCHAR, NAME VARCHAR)");
            st.execute("INSERT INTO TD VALUES ('d1','D-one'),('d2','D-two')");
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

    /** The first column of every row, sorted. */
    private static List<String> rows(String chain) throws SQLException {
        List<String> out = new ArrayList<>();
        try (Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery(sqlOf("|" + chain + "->from(n::M, n::RT)"))) {
            while (rs.next()) {
                out.add(rs.getString(1));
            }
        }
        Collections.sort(out);
        return out;
    }

    @Test
    @DisplayName("three and four flatten hops: association, association, slot, slot")
    void chainsOfSlotsAfterAssociationHops() throws SQLException {
        assertEquals(List.of("C-one", "C-two"),
                rows("n::A.all()->filter(a|$a.name == 'x').links.rs.c.name"));
        assertEquals(List.of("D-one", "D-two"),
                rows("n::A.all()->filter(a|$a.name == 'x').links.rs.c.d.name"));
        assertEquals(List.of("x", "x"),
                rows("n::A.all()->filter(a|$a.name == 'x').links.rs.lr.a.name"),
                "an association hop after two association hops");
    }

    @Test
    @DisplayName("ops between hops: filter and first() on the mid target, slots after")
    void opsBetweenHops() throws SQLException {
        assertEquals(List.of("C-one"),
                rows("n::A.all()->filter(a|$a.name == 'x').links.rs->filter(r|$r.id == 'b1').c.name"));
        assertEquals(List.of("C-one"),
                rows("n::A.all()->filter(a|$a.name == 'x').links.rs->filter(r|$r.id == 'b1')->first().c.name"));
        assertEquals(List.of("D-one"),
                rows("n::A.all()->filter(a|$a.name == 'x').links.rs->filter(r|$r.id == 'b1')->first().c.d.name"));
        assertEquals(List.of("x"),
                rows("n::A.all()->filter(a|$a.name == 'x').links.rs->filter(r|$r.id == 'b1')->first().lr.a.name"),
                "first() on the mid target, then an association chain");
        assertEquals(List.of("b1"),
                rows("n::A.all()->filter(a|$a.name == 'x').links.rs->filter(r|$r.c.d.name == 'D-one').id"),
                "a two-slot path inside the mid filter");
    }

    @Test
    @DisplayName("nested predicates navigate two levels: dotted reads and dotted emptiness")
    void nestedPredicateDepth() throws SQLException {
        assertEquals(List.of("b1", "b2"), rows("n::R.all()->filter(r|$r.lr.a.name == 'x').id"));
        assertEquals(List.of("b1", "b2"),
                rows("n::R.all()->filter(r|$r.lr->exists(l|$l.a.name == 'x')).id"));
        assertEquals(List.of("b1", "b2"),
                rows("n::R.all()->filter(r|$r.lr.a->exists(a|$a.name == 'x')).id"));
        assertEquals(List.of("b1", "b3"), rows("n::R.all()->filter(r|$r.c.d.name == 'D-one').id"));
        assertEquals(List.of("C-one", "C-two"),
                rows("n::A.all()->filter(a|$a.name == 'x').links.rs"
                        + "->filter(r|$r.lr.a->exists(a|$a.name == 'x')).c.name"),
                "dotted emptiness inside a mid-chain filter (a nested scope)");
    }

    @Test
    @DisplayName("a to-many navigation after first()/sortBy joins ABOVE the limit (the limit counts source rows)")
    void toManyAfterRowCountOps() throws SQLException {
        assertEquals(List.of("t1", "t2"),
                rows("n::A.all()->filter(a|$a.name == 'x')->first().links.tag"),
                "both links of the first A, not the first fanned row");
        assertEquals(List.of("t1", "t2"),
                rows("n::A.all()->sortBy(a|$a.name)->first().links.tag"),
                "a sort below the hop orders the rows the limit counts");
        assertEquals(List.of("b1", "b2"),
                rows("n::A.all()->filter(a|$a.name == 'x')->first().links.rs.id"));
        assertEquals(List.of("t2"),
                rows("n::A.all()->filter(a|$a.name == 'x')->first().links->filter(l|$l.tag == 't2').tag"));
        assertEquals(List.of("t1"),
                rows("n::A.all()->filter(a|$a.name == 'x')->first().links->first().tag"));
    }

    @Test
    @DisplayName("association ends whose target is inheritance-mapped keep their binding under filters")
    void inheritanceMappedAssociationEnds() throws SQLException {
        assertEquals(List.of("b1", "b2"), rows("n::A.all()->filter(a|$a.name == 'x').links.bs.id"));
        assertEquals(List.of("b1"),
                rows("n::A.all()->filter(a|$a.name == 'x').links.bs->filter(b|$b.id == 'b1').id"));
        assertEquals(List.of("b1", "b2"), rows("n::B.all()->filter(b|$b.l.a.name == 'x').id"));
        assertEquals(List.of("b1", "b2"),
                rows("n::B.all()->filter(b|$b.l->exists(l|$l.a.name == 'x')).id"));
        assertEquals(List.of("b1", "b2"),
                rows("n::B1.all()->filter(b|$b.l->exists(l|$l.a.name == 'x')).id"));
        assertEquals(List.of("b1", "b2"),
                rows("n::B1.all()->filter(b|$b.l.a->exists(a|$a.name == 'x')).id"));
        assertEquals(List.of("b1", "b2"),
                rows("n::B.all()->filter(b|$b.l.a->exists(a|$a.name == 'x')).id"));
        assertEquals(List.of("b1", "b2"),
                rows("n::A.all()->filter(a|$a.name == 'x').links.bs"
                        + "->filter(b|$b.l.a->exists(a|$a.name == 'x')).id"),
                "dotted emptiness on the inheritance-mapped target of a chain");
    }
}
