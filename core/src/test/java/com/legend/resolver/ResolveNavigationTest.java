package com.legend.resolver;

import com.legend.Compiler;
import com.legend.compiler.NameResolver;
import com.legend.compiler.spec.SpecCompiler;
import com.legend.compiler.spec.typed.TypedSpec;
import com.legend.lowering.Lowerer;
import com.legend.parser.SpecParser;
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
 * M-H3b — association navigation to-one + embedded per-leaf dispatch
 * (docs/PHASE_H2_H3_RESOLVER_PLAN.md fixtures 11/12/14/16/18): navigation
 * joins are demand-gated LEFT joins against the target class's own
 * pipeline, deduped by head across the WHOLE op chain; embedded reads are
 * parent-alias columns, never joins; un-navigated associations cost
 * nothing.
 */
class ResolveNavigationTest {

    private static final String MODEL = """
            Class m::Person { name: String[1]; addr: m::Addr[1]; }
            Class m::Addr { city: String[1]; }
            Class m::Firm { legal: String[1]; }
            Association m::Emp { employer: m::Firm[1]; staff: m::Person[*]; }
            Association m::Mgr { boss: m::Person[1]; reports: m::Person[*]; }
            Database s::DB (
              Table P (NAME VARCHAR(50), CITY VARCHAR(50), FID INTEGER, BOSS INTEGER, ID INTEGER)
              Table F (ID INTEGER, LEGAL VARCHAR(50))
              Join PF (P.FID = F.ID)
              Join PB (P.BOSS = {target}.ID)
            )
            Mapping m::M (
              *m::Person: Relational { ~mainTable [s::DB] P
                name: P.NAME,
                addr ( city: P.CITY ) }
              *m::Firm: Relational { ~mainTable [s::DB] F legal: F.LEGAL }
              m::Emp: Relational { AssociationMapping ( employer: [s::DB] @PF ) }
              m::Mgr: Relational { AssociationMapping ( boss: [s::DB] @PB ) }
            )
            Runtime m::RT { mappings: [m::M]; }
            """;

    private static Connection conn;

    @BeforeAll
    static void setUp() throws SQLException {
        conn = DriverManager.getConnection("jdbc:duckdb:");
        try (Statement st = conn.createStatement()) {
            st.execute("CREATE TABLE P (NAME VARCHAR, CITY VARCHAR, FID INTEGER,"
                    + " BOSS INTEGER, ID INTEGER)");
            st.execute("INSERT INTO P VALUES ('Ann', 'NYC', 1, 2, 1),"
                    + " ('Bob', 'SF', NULL, NULL, 2), ('Cat', 'LA', 1, NULL, 3)");
            st.execute("CREATE TABLE F (ID INTEGER, LEGAL VARCHAR)");
            st.execute("INSERT INTO F VALUES (1, 'ACME')");
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
                NameResolver.resolveQuery(SpecParser.parse(query)));
        List<TypedSpec> resolved = new StoreResolver(ctx, specs).resolve(body, null);
        return new DuckDb().render(new Lowerer().lower(resolved));
    }

    /** Driver-style resolution (runtime as API argument — the corpus's
     * no-from() shape; scalar-collection roots cannot spell from()). */
    private static String sqlOfDriver(String query) {
        var ctx = Compiler.compileModel(MODEL);
        SpecCompiler specs = new SpecCompiler(ctx);
        List<TypedSpec> body = specs.typeQueryBody(
                NameResolver.resolveQuery(SpecParser.parse(query)));
        List<TypedSpec> resolved = new StoreResolver(ctx, specs)
                .resolve(body, "m::RT");
        return new DuckDb().render(new Lowerer().lower(resolved));
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
        for (int i = sql.indexOf(kw); i >= 0; i = sql.indexOf(kw, i + kw.length())) c++;
        return c;
    }

    @Test
    @DisplayName("11: two properties through one association — ONE join")
    void twoPropsOneAssociation() throws SQLException {
        String sql = sqlOf("m::Person.all()->project(~[name: p|$p.name,"
                + " legal: p|$p.employer.legal])->from(m::RT)");
        assertEquals(1, count(sql, "LEFT OUTER JOIN"), sql);
        assertEquals(1, count(sql, "SELECT"), sql);
        assertEquals(List.of("Ann|ACME", "Bob|null", "Cat|ACME"),
                exec(sql).stream().sorted().toList(),
                "LEFT semantics: Bob's NULL FID keeps his row");
    }

    @Test
    @DisplayName("12: same navigation in filter AND project — ONE join (whole-chain dedup)")
    void filterAndProjectShareTheJoin() throws SQLException {
        String sql = sqlOf("m::Person.all()->filter(p|$p.employer.legal == 'ACME')"
                + "->project(~[legal: p|$p.employer.legal])->from(m::RT)");
        assertEquals(1, count(sql, "LEFT OUTER JOIN"),
                "the beats-V1 pin — one registry across the whole chain:\n" + sql);
        assertEquals(1, count(sql, "SELECT"), sql);
        // TO-ONE filter navigation = flat JOIN semantics (audit R4): both
        // ACME employees qualify — one row each, no dedup of the parent set.
        assertEquals(List.of("ACME", "ACME"), exec(sql));
    }

    @Test
    @DisplayName("14: self-association — distinct aliases, right orientation")
    void selfAssociation() throws SQLException {
        String sql = sqlOf("m::Person.all()->project(~[name: p|$p.name,"
                + " bossName: p|$p.boss.name])->from(m::RT)");
        assertEquals(1, count(sql, "LEFT OUTER JOIN"), sql);
        assertTrue(sql.contains("P AS t0") && sql.contains("P AS t1"),
                "same table twice, two aliases: " + sql);
        assertEquals(List.of("Ann|Bob", "Bob|null", "Cat|null"),
                exec(sql).stream().sorted().toList(),
                "Ann's boss is Bob (orientation pin — a swapped condition"
                        + " would join BOSS to the wrong side)");
    }

    @Test
    @DisplayName("16: embedded navigation — parent-alias column, ZERO joins")
    void embeddedIsJoinless() throws SQLException {
        String sql = sqlOf("m::Person.all()->project(~[city: p|$p.addr.city])"
                + "->from(m::RT)");
        assertEquals(0, count(sql, "JOIN"), sql);
        assertEquals(1, count(sql, "SELECT"), sql);
        assertTrue(sql.contains("t0.CITY AS city"), sql);
        assertEquals(List.of("LA", "NYC", "SF"), exec(sql).stream().sorted().toList());
    }

    @Test
    @DisplayName("18: association declared but NOT navigated — ZERO joins")
    void unNavigatedAssociationCostsNothing() throws SQLException {
        String sql = sqlOf("m::Person.all()->project(~[name: p|$p.name])"
                + "->from(m::RT)");
        assertEquals(0, count(sql, "JOIN"), sql);
        assertEquals(List.of("Ann", "Bob", "Cat"), exec(sql).stream().sorted().toList());
    }

    @Test
    @DisplayName("14b: self-association property2 (to-many) projection EXPLODES with correct orientation")
    void selfAssociationOtherEnd() throws SQLException {
        String sql = sqlOf("m::Person.all()->project(~[name: p|$p.name,"
                + " r: p|$p.reports.name])->from(m::RT)");
        assertEquals(1, count(sql, "LEFT OUTER JOIN"), sql);
        // Bob's report is Ann (Ann.BOSS=2=Bob); Ann has none — explosion +
        // orientation pinned by DATA (property2 = reversed condition).
        assertEquals(List.of("Ann|null", "Bob|Ann", "Cat|null"),
                exec(sql).stream().sorted().toList());
    }

    @Test
    @DisplayName("two DIFFERENT associations in one chain — two joins, one SELECT")
    void twoAssociationsOneChain() throws SQLException {
        String sql = sqlOf("m::Person.all()->project(~[legal: p|$p.employer.legal,"
                + " bossName: p|$p.boss.name])->from(m::RT)");
        assertEquals(2, count(sql, "LEFT OUTER JOIN"), sql);
        assertEquals(1, count(sql, "SELECT"), sql);
        assertEquals(List.of("ACME|Bob", "ACME|null", "null|null"),
                exec(sql).stream().sorted().toList());
    }

    @Test
    @DisplayName("toOne()-wrapped navigation is transparent to the path (audit R3)")
    void toOneWrappedNavigation() throws SQLException {
        String sql = sqlOf("m::Person.all()"
                + "->project(~[legal: p|$p.employer->toOne().legal])->from(m::RT)");
        assertEquals(1, count(sql, "LEFT OUTER JOIN"), sql);
        assertEquals(List.of("ACME", "ACME", "null"),
                exec(sql).stream().sorted().toList());
    }

    @Test
    @DisplayName("bare association head AS A VALUE gets the honest unsupported story")
    void bareAssocHeadHonestError() {
        // ($p.employer->isEmpty() now RESOLVES — fixture 26; the remaining
        // unsupported bare shape is the whole-instance VALUE use.)
        var ctx = Compiler.compileModel(MODEL);
        SpecCompiler specs = new SpecCompiler(ctx);
        var body = specs.typeQueryBody(NameResolver.resolveQuery(SpecParser.parse(
                "m::Person.all()->project(~[e: p|$p.employer])->from(m::RT)")));
        var e = org.junit.jupiter.api.Assertions.assertThrows(
                com.legend.error.NotImplementedException.class,
                () -> new StoreResolver(ctx, specs).resolve(body, null));
        assertTrue(e.getMessage().contains("navigation head"), e.getMessage());
    }

    @Test
    @DisplayName("W4: assoc leaf mapped through the TARGET's own join slot NESTS the join")
    void targetSlotLeafNestsTheJoin() {
        String model = """
            Class m::P { name: String[1]; }
            Class m::E { orgName: String[1]; }
            Association m::PE { emp: m::E[1]; owner: m::P[1]; }
            Database s::DB (
              Table P (NAME VARCHAR(50), EID INTEGER)
              Table E (ID INTEGER, OID INTEGER)
              Table O (ID INTEGER, ONAME VARCHAR(50))
              Join PE (P.EID = E.ID)
              Join EO (E.OID = O.ID) )
            Mapping m::M (
              *m::P: Relational { ~mainTable [s::DB] P name: P.NAME }
              *m::E: Relational { ~mainTable [s::DB] E orgName: @EO | O.ONAME }
              m::PE: Relational { AssociationMapping ( emp: [s::DB] @PE ) } )
            Runtime m::RT { mappings: [m::M]; }
            """;
        var ctx = Compiler.compileModel(model);
        SpecCompiler specs = new SpecCompiler(ctx);
        var body = specs.typeQueryBody(NameResolver.resolveQuery(SpecParser.parse(
                "m::P.all()->project(~[o: p|$p.emp.orgName])->from(m::RT)")));
        // The demanded leaf pulls the TARGET's own @EO slot into its
        // pipeline: assoc hop + nested slot = two LEFT JOINs, and the leaf
        // reads the doubly-prefixed flat column.
        var resolved = new StoreResolver(ctx, specs).resolve(body, null);
        String sql = new DuckDb().render(new Lowerer().lower(
                resolved.get(resolved.size() - 1)));
        assertEquals(2, sql.split("LEFT OUTER JOIN", -1).length - 1,
                "assoc hop + nested slot join: " + sql);
    }

    // ---- part 3: class-typed Join PM (navigate-step) navigation ----

    private static final String A7_MODEL = """
            Class m::P { name: String[1]; firm: m::F[1]; }
            Class m::F { legal: String[1]; }
            Database s::DB (
              Table P (NAME VARCHAR(50), FID INTEGER)
              Table F (ID INTEGER, LEGAL VARCHAR(50))
              Join PF (P.FID = F.ID) )
            Mapping m::M (
              *m::P: Relational { ~mainTable [s::DB] P name: P.NAME, firm: [s::DB] @PF }
              *m::F: Relational { ~mainTable [s::DB] F legal: F.LEGAL } )
            Runtime m::RT { mappings: [m::M]; }
            """;

    private static String sqlOfA7(String query) {
        var ctx = Compiler.compileModel(A7_MODEL);
        SpecCompiler specs = new SpecCompiler(ctx);
        var body = specs.typeQueryBody(NameResolver.resolveQuery(SpecParser.parse(query)));
        return new DuckDb().render(new Lowerer().lower(
                new StoreResolver(ctx, specs).resolve(body, null)));
    }

    @Test
    @DisplayName("25a: class-typed Join PM navigated — ONE LEFT JOIN against the target pipeline")
    void classTypedJoinPmNavigates() throws SQLException {
        String sql = sqlOfA7("m::P.all()->project(~[name: p|$p.name,"
                + " legal: p|$p.firm.legal])->from(m::RT)");
        assertEquals(1, count(sql, "LEFT OUTER JOIN"), sql);
        assertEquals(1, count(sql, "SELECT"), sql);
        assertEquals(List.of("Ann|ACME", "Bob|null", "Cat|ACME"),
                exec(sql).stream().sorted().toList());
    }

    @Test
    @DisplayName("25b: class-typed Join PM NOT navigated — the step strips, ZERO joins")
    void classTypedJoinPmElides() throws SQLException {
        String sql = sqlOfA7("m::P.all()->project(~[name: p|$p.name])->from(m::RT)");
        assertEquals(0, count(sql, "JOIN"),
                "un-navigated class slot must cancel (was the A7 lowerer wall):\n" + sql);
        assertEquals(List.of("Ann", "Bob", "Cat"), exec(sql).stream().sorted().toList());
    }

    @Test
    @DisplayName("23: multiple nav paths in ONE computed expression")
    void multiplePathsOneExpression() throws SQLException {
        String sql = sqlOf("m::Person.all()->project(~[combo:"
                + " p|$p.employer.legal + '/' + $p.boss.name])->from(m::RT)");
        assertEquals(2, count(sql, "LEFT OUTER JOIN"), sql);
        assertEquals(1, count(sql, "SELECT"), sql);
        assertEquals(List.of("ACME/Bob", "null", "null"),
                exec(sql).stream().sorted().toList(),
                "NULL string concat propagates per SQL semantics");
    }

    @Test
    @DisplayName("audit: bare class-typed heads (nav slot / embedded) get the H4 story")
    void bareClassTypedHeadsHonest() {
        var ctx = Compiler.compileModel(A7_MODEL);
        SpecCompiler specs = new SpecCompiler(ctx);
        var body = specs.typeQueryBody(NameResolver.resolveQuery(SpecParser.parse(
                "m::P.all()->project(~[f: p|$p.firm])->from(m::RT)")));
        var e = org.junit.jupiter.api.Assertions.assertThrows(
                com.legend.error.NotImplementedException.class,
                () -> new StoreResolver(ctx, specs).resolve(body, null));
        assertTrue(e.getMessage().contains("H4"), e.getMessage());

        var ctx2 = Compiler.compileModel(MODEL);
        SpecCompiler specs2 = new SpecCompiler(ctx2);
        var body2 = specs2.typeQueryBody(NameResolver.resolveQuery(SpecParser.parse(
                "m::Person.all()->project(~[a: p|$p.addr])->from(m::RT)")));
        var e2 = org.junit.jupiter.api.Assertions.assertThrows(
                com.legend.error.NotImplementedException.class,
                () -> new StoreResolver(ctx2, specs2).resolve(body2, null));
        assertTrue(e2.getMessage().contains("H4"), e2.getMessage());
    }

    // ---- M-H3c: to-many navigation in filter position = correlated EXISTS ----

    @Test
    @DisplayName("15: to-many exists(pred) — correlated EXISTS, no top-level join")
    void toManyExists() throws SQLException {
        String sql = sqlOf("m::Firm.all()->filter(f|$f.staff->exists(s|$s.name == 'Ann'))"
                + "->project(~[legal: f|$f.legal])->from(m::RT)");
        assertEquals(0, count(sql, "LEFT OUTER JOIN"), sql);
        assertEquals(1, count(sql, "EXISTS"), sql);
        assertEquals(2, count(sql, "SELECT"), "outer + the correlated subquery: " + sql);
        assertEquals(List.of("ACME"), exec(sql));
    }

    @Test
    @DisplayName("15b: to-many isEmpty — NOT EXISTS, executed")
    void toManyIsEmpty() throws SQLException {
        String sql = sqlOf("m::Firm.all()->filter(f|$f.staff->isEmpty())"
                + "->project(~[legal: f|$f.legal])->from(m::RT)");
        assertEquals(1, count(sql, "NOT EXISTS"), sql);
        assertEquals(List.of(), exec(sql), "ACME has staff (Ann): no empty firms");
    }

    @Test
    @DisplayName("26: to-one-OPTIONAL class-typed isEmpty — NOT EXISTS (any-multiplicity rule)")
    void toOneOptionalIsEmpty() throws SQLException {
        String sql = sqlOf("m::Person.all()->filter(p|$p.employer->isEmpty())"
                + "->project(~[name: p|$p.name])->from(m::RT)");
        assertEquals(1, count(sql, "NOT EXISTS"), sql);
        assertEquals(0, count(sql, "LEFT OUTER JOIN"), sql);
        assertEquals(List.of("Bob"), exec(sql), "Bob's NULL FID matches no firm");
    }

    @Test
    @DisplayName("implicit to-many crossing in a filter — LEFT JOIN row semantics (engine testIn golden)")
    void filterCrossingJoinsWithRowSemantics() throws SQLException {
        // AUDIT 9: the engine's golden for an implicit crossing is a BARE
        // LEFT JOIN with the predicate in WHERE — result rows DUPLICATE the
        // parent per matching child (the distinct-subselect semi-join is
        // reserved for EXPLICIT exists/isEmpty calls).
        String sql = sqlOf("m::Firm.all()->filter(f|$f.staff.name == 'Ann')"
                + "->project(~[legal: f|$f.legal])->from(m::RT)");
        assertEquals(0, count(sql, "EXISTS"), sql);
        assertEquals(1, count(sql, "LEFT OUTER JOIN"), sql);
        assertEquals(List.of("ACME"), exec(sql), "one row per matching child");
    }

    @Test
    @DisplayName("projection-position to-many — LEFT JOIN with ROW EXPLOSION")
    void projectionToManyExplodes() throws SQLException {
        String sql = sqlOf("m::Firm.all()->project(~[legal: f|$f.legal,"
                + " who: f|$f.staff.name])->from(m::RT)");
        assertEquals(1, count(sql, "LEFT OUTER JOIN"), sql);
        // ACME has two staff (Ann, Cat): two exploded rows.
        assertEquals(List.of("ACME|Ann", "ACME|Cat"),
                exec(sql).stream().sorted().toList());
    }

    @Test
    @DisplayName("mixed position: filter and projection SHARE the join (merge-by-identity); WHERE filters the exploded rows")
    void mixedPositionSameHead() throws SQLException {
        // Engine merge-by-identity (§A.1): the filter thread and the
        // projection thread reference the same association — ONE join; the
        // filter lands in WHERE over the exploded rows, so only matching
        // child rows survive.
        String sql = sqlOf("m::Firm.all()->filter(f|$f.staff.name == 'Ann')"
                + "->project(~[legal: f|$f.legal, who: f|$f.staff.name])->from(m::RT)");
        assertEquals(0, count(sql, "EXISTS"), sql);
        assertEquals(1, count(sql, "LEFT OUTER JOIN"), sql);
        assertEquals(List.of("ACME|Ann"), exec(sql).stream().sorted().toList());
    }

    @Test
    @DisplayName("P02: filter-ONLY to-one navigation — flat LEFT JOIN, never EXISTS (audit R4)")
    void filterOnlyToOneNavIsFlatJoin() throws SQLException {
        String sql = sqlOf("m::Person.all()->filter(p|$p.employer.legal == 'ACME')"
                + "->project(~[name: p|$p.name])->from(m::RT)");
        assertEquals(1, count(sql, "LEFT OUTER JOIN"),
                "to-one head must rebuild its join even though ExistsSub"
                        + " material exists for explicit emptiness calls:\n" + sql);
        assertEquals(0, count(sql, "EXISTS"), sql);
        assertEquals(1, count(sql, "SELECT"), sql);
        assertEquals(List.of("Ann", "Cat"), exec(sql).stream().sorted().toList());
    }

    @Test
    @DisplayName("P03: NOT over a to-one filter navigation — SQL NULL semantics (engine parity)")
    void notOverToOneNavKeepsNullSemantics() throws SQLException {
        String sql = sqlOf("m::Person.all()->filter(p|!($p.employer.legal == 'ACME'))"
                + "->project(~[name: p|$p.name])->from(m::RT)");
        assertEquals(0, count(sql, "EXISTS"), sql);
        // Ann/Cat are ACME (excluded); Bob has NO employer — pure
        // semantics: equal([], 'ACME') is false, so not(...) is TRUE and
        // Bob is ADMITTED. The engine emits exactly this via its
        // processNotEqual null arm (dbExtension.pure: L <> R OR L is
        // null); the earlier pin baked in SQL three-valued <> and
        // silently DROPPED the null row (task #62).
        assertEquals(1, count(sql, "IS NULL"), sql);
        assertEquals(List.of("Bob"), exec(sql));
    }

    @Test
    @DisplayName("AUTO-MAP: class-typed hops in the CHAIN fold over the root (Firm.all().staff.name)")
    void autoMapHopChainFolds() throws SQLException {
        String sql = sqlOfDriver("m::Firm.all().staff.name");
        assertEquals(1, count(sql, "LEFT OUTER JOIN"), sql);
        // ACME's staff Ann/Cat via the PF join (Bob has no firm — absent;
        // the LEFT JOIN keeps employee-less firms as null rows, none here)
        assertEquals(List.of("Ann", "Cat"), exec(sql).stream().sorted().toList());
    }

    @Test
    @DisplayName("AUTO-MAP: ->map(e|scalar) over a hop chain composes into the same fold")
    void autoMapHopMapComposes() throws SQLException {
        String sql = sqlOfDriver("m::Firm.all().staff->map(p|$p.name)");
        assertEquals(1, count(sql, "LEFT OUTER JOIN"), sql);
        assertEquals(List.of("Ann", "Cat"), exec(sql).stream().sorted().toList());
    }

    @Test
    @DisplayName("AUTO-MAP: scalar filter over the exploded column becomes a relation filter")
    void autoMapScalarFilterOverExplodedColumn() throws SQLException {
        String sql = sqlOfDriver("m::Firm.all().staff.name->filter(n|$n == 'Cat')");
        assertEquals(1, count(sql, "LEFT OUTER JOIN"), sql);
        assertEquals(List.of("Cat"), exec(sql));
    }

    @Test
    @DisplayName("AUTO-MAP: a toOne-headed class chain in root position evaluates (eager-let shape)")
    void toOneHeadedRootEvaluates() throws SQLException {
        // the eager run of `let row = ...->toOne()` — the chain resolver owns
        // the head natives; scalar reads off the binding then evaluate per
        // statement (toOne = the documented pass-through stand-in: a
        // multi-row source surfaces at the value compare, never silently)
        var r = Compiler.execute(MODEL,
                "{| let row = m::Person.all()->filter(p|$p.name == 'Ann')->toOne();\n"
                        + "$row.addr.city;}", "m::RT", conn);
        var t = (com.legend.exec.ExecutionResult.Tabular) r;
        assertEquals(1, t.rows().size());
        assertEquals("NYC", t.rows().get(0).values().get(0));
    }

    @Test
    @DisplayName("P01: scalar isEmpty ACROSS a to-one crossing — IS NULL via the join, not EXISTS")
    void scalarIsEmptyAcrossToOneCrossing() throws SQLException {
        String sql = sqlOf("m::Person.all()->filter(p|$p.employer.legal->isEmpty())"
                + "->project(~[name: p|$p.name])->from(m::RT)");
        assertEquals(1, count(sql, "LEFT OUTER JOIN"), sql);
        assertEquals(0, count(sql, "EXISTS"), sql);
        assertEquals(1, count(sql, "IS NULL"), sql);
        assertEquals(List.of("Bob"), exec(sql),
                "no-employer row: joined LEGAL is NULL, isEmpty true");
    }

    @Test
    @DisplayName("P04: OUTER var inside an explicit exists predicate — correlated, no overflow")
    void outerVarInsideExistsPredicate() throws SQLException {
        String sql = sqlOf("m::Firm.all()->filter(f|$f.staff->exists(s|$s.name != $f.legal))"
                + "->project(~[legal: f|$f.legal])->from(m::RT)");
        assertEquals(1, count(sql, "EXISTS"), sql);
        assertEquals(List.of("ACME"), exec(sql),
                "staff names differ from the firm's LEGAL: predicate true");
    }

    @Test
    @DisplayName("emptiness leaf over a to-many crossing — row-level IS NULL through the join")
    void emptinessLeafOverToManyCrossingIsRowLevel() throws SQLException {
        String sql = sqlOf("m::Firm.all()->filter(f|$f.staff.name->isEmpty())"
                + "->project(~[legal: f|$f.legal])->from(m::RT)");
        assertEquals(1, count(sql, "LEFT OUTER JOIN"), sql);
        assertEquals(0, count(sql, "EXISTS"), sql);
        // both ACME staffers have names; no null rows survive
        assertEquals(List.of(), exec(sql));
    }

    @Test
    @DisplayName("21: class-source groupBy with an association-path KEY — GROUP BY the joined column")
    void classSourceGroupByAssociationKey() throws SQLException {
        String sql = sqlOf("m::Person.all()->groupBy(~[legal : p|$p.employer.legal],"
                + " ~cnt : x|$x : y|$y->count())->from(m::RT)");
        assertEquals(1, count(sql, "LEFT OUTER JOIN"), sql);
        assertEquals(1, count(sql, "SELECT"), sql);
        assertEquals(1, count(sql, "GROUP BY"), sql);
        assertEquals(List.of("ACME|2", "null|1"), exec(sql).stream().sorted().toList(),
                "LEFT semantics: Bob's no-employer row groups under NULL");
    }

    @Test
    @DisplayName("21b: filter + class-source groupBy — both lambdas through the one funnel")
    void classSourceGroupByAfterFilter() throws SQLException {
        String sql = sqlOf("m::Person.all()->filter(p|$p.name != 'Bob')"
                + "->groupBy(~[legal : p|$p.employer.legal],"
                + " ~cnt : x|$x : y|$y->count())->from(m::RT)");
        assertEquals(1, count(sql, "LEFT OUTER JOIN"), sql);
        assertEquals(List.of("ACME|2"), exec(sql));
    }

    @Test
    @DisplayName("21c: LEGACY 4-arg class-source groupBy (the plangen form) — desugars and resolves")
    void legacyClassSourceGroupBy() throws SQLException {
        String sql = sqlOf("m::Person.all()->groupBy([{p|$p.employer.legal}],"
                + " [agg({x|$x.name}, {y|$y->count()})], ['legal', 'cnt'])->from(m::RT)");
        assertEquals(1, count(sql, "LEFT OUTER JOIN"), sql);
        assertEquals(List.of("ACME|2", "null|1"), exec(sql).stream().sorted().toList());
    }

    @Test
    @DisplayName("relation-space groupBy ABOVE a resolved projection (the corpus shape)")
    void relationGroupByOverResolvedChain() throws SQLException {
        String sql = sqlOf("m::Person.all()->project(~[legal: p|$p.employer.legal])"
                + "->groupBy(~legal, ~cnt : x|$x : y|$y->count())->from(m::RT)");
        assertEquals(1, count(sql, "LEFT OUTER JOIN"), sql);
        assertEquals(List.of("ACME|2", "null|1"), exec(sql).stream().sorted().toList());
    }

    @Test
    @DisplayName("implicit EXISTS on != — the negation stays INSIDE: ∃(¬X), never ¬∃(X)")
    void notEqualCrossingKeepsNegationInsideExists() throws SQLException {
        String sql = sqlOf("m::Firm.all()->filter(f|$f.staff.name != 'Ann')"
                + "->project(~[legal: f|$f.legal])->from(m::RT)");
        // Engine negation isolation over the JOIN (testInNegated golden:
        // `NOT X OR <read> IS NULL` over a bare LEFT JOIN): the negation
        // applies per exploded row, and a NULL crossing read (no child, or
        // a null child value) passes.
        assertEquals(0, count(sql, "EXISTS"), sql);
        assertEquals(1, count(sql, "LEFT OUTER JOIN"), sql);
        assertEquals(1, count(sql, "CASE"), sql);
        // THE DATA PIN: Ann's row fails ¬(name='Ann'), Cat's row passes —
        // ACME survives once, via Cat.
        assertEquals(List.of("ACME"), exec(sql));
    }

    @Test
    @DisplayName("negated emptiness over a crossing — row-level through the join")
    void notOverEmptinessCrossingIsRowLevel() throws SQLException {
        // not(isNotEmpty(crossing)): the emptiness call substitutes through
        // the JOIN (row-level IS NOT NULL) and the not applies per row —
        // both staffers have names, no row survives.
        String sql = sqlOf("m::Firm.all()->filter(f|!($f.staff.name->isNotEmpty()))"
                + "->project(~[legal: f|$f.legal])->from(m::RT)");
        assertEquals(1, count(sql, "LEFT OUTER JOIN"), sql);
        assertEquals(List.of(), exec(sql));
    }

    @Test
    @DisplayName("nested navigation inside an exists predicate RESOLVES (R2: recursive scope demand)")
    void nestedNavInsideExistsResolves() {
        // was a loud NotImplemented until GAP-H R2: the nested scope now
        // carries its own registered materials (ExistsSub.innerRegs), so
        // $s.boss.name inside the exists correlates a LEFT-joined boss
        // onto the exists relation
        var ctx = Compiler.compileModel(MODEL);
        SpecCompiler specs = new SpecCompiler(ctx);
        var body = specs.typeQueryBody(NameResolver.resolveQuery(SpecParser.parse(
                "m::Firm.all()->filter(f|$f.staff->exists(s|$s.boss.name == 'Bob'))"
                        + "->project(~[legal: f|$f.legal])->from(m::RT)")));
        var resolved = new StoreResolver(ctx, specs).resolve(body, null);
        org.junit.jupiter.api.Assertions.assertNotNull(resolved);
    }

    // ---- multi-hop chains (A5): one join per hop, chained by path prefix ----

    @Test
    @DisplayName("3-hop across two associations: boss.employer.legal — 2 chained LEFT joins")
    void threeHopAcrossTwoAssociations() throws SQLException {
        String sql = sqlOf("m::Person.all()->project(~[n: p|$p.name,"
                + " bn: p|$p.boss.name, bl: p|$p.boss.employer.legal])->from(m::RT)");
        // boss.name and boss.employer.legal SHARE the boss join (chain dedup);
        // the employer hop chains off it — exactly two LEFT joins.
        assertEquals(2, count(sql, "LEFT OUTER JOIN"), sql);
        assertEquals(List.of("Ann|Bob|null", "Bob|null|null", "Cat|null|null"),
                exec(sql + "\nORDER BY n"),
                "hop 1 proves the shared join; NULL bosses stay NULL through hop 2");
    }

    @Test
    @DisplayName("3-hop SELF-association: boss.boss.name — two joins, distinct prefixes")
    void threeHopSelfAssociation() throws SQLException {
        String sql = sqlOf("m::Person.all()->project(~[n: p|$p.name,"
                + " bb: p|$p.boss.boss.name])->from(m::RT)");
        assertEquals(2, count(sql, "LEFT OUTER JOIN"), sql);
        assertEquals(List.of("Ann|null", "Bob|null", "Cat|null"),
                exec(sql + "\nORDER BY n"),
                "Ann's boss Bob has no boss — NULL rides the whole chain");
    }
}
