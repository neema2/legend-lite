package com.legend.resolver;

import com.legend.testbackend.TestBackends;
import com.legend.Compiler;
import com.legend.compiler.NameResolver;
import com.legend.compiler.spec.SpecCompiler;
import com.legend.compiler.spec.typed.TypedSpec;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * RUN-TIME BRANCH CHOICE ON THE ROW'S TYPE COLUMN (metamodel program
 * step 2, 2026-09-02) over an ORDINARY user inheritance mapping: the
 * three pure type-dispatch forms on an instance variable —
 * {@code match} with value arms, {@code instanceOf}, {@code cast(@Sub)}
 * — decide per ROW, on the union row's membership witness, and lower
 * to SQL (CASE / IS NOT NULL) through the one router. A user feature:
 * nothing here knows about tests, the metamodel or the harness.
 *
 * <p>Rows are the verdict. Pure raises where no arm accepts the value
 * or the cast does not hold; so does the SQL.
 */
class RuntimeTypeDispatchTest {

    private static final String INHERITANCE_OP =
            "meta::pure::router::operations::inheritance_OperationSetImplementation_1__SetImplementation_MANY_";

    private static final String MODEL = """
            Class rt::Vehicle { id: Integer[1]; name: String[1]; }
            Class rt::Car extends rt::Vehicle { engineType: String[1]; }
            Class rt::Bicycle extends rt::Vehicle { gears: Integer[1]; }
            ###Relational
            Database rt::DB (
              Table CAR (ID INTEGER PRIMARY KEY, NAME VARCHAR(64), ENGINE VARCHAR(20))
              Table BIKE (ID INTEGER PRIMARY KEY, NAME VARCHAR(64), GEARS INTEGER)
            )
            ###Mapping
            Mapping rt::M (
              *rt::Vehicle : Operation { %s() }
              rt::Car : Relational { ~mainTable [rt::DB] CAR
                id: CAR.ID, name: CAR.NAME, engineType: CAR.ENGINE }
              rt::Bicycle : Relational { ~mainTable [rt::DB] BIKE
                id: BIKE.ID, name: BIKE.NAME, gears: BIKE.GEARS }
            )
            ###Runtime
            Runtime rt::RT { mappings: [rt::M]; }
            """.formatted(INHERITANCE_OP);

    private static Connection conn;

    @BeforeAll
    static void setUp() throws SQLException {
        conn = TestBackends.platform().open();
        try (Statement st = conn.createStatement()) {
            st.execute("CREATE TABLE CAR (ID INTEGER, NAME VARCHAR, ENGINE VARCHAR)");
            st.execute("INSERT INTO CAR VALUES (1, 'Sedan', 'V8'), (3, 'Hatch', 'electric')");
            st.execute("CREATE TABLE BIKE (ID INTEGER, NAME VARCHAR, GEARS INTEGER)");
            st.execute("INSERT INTO BIKE VALUES (2, 'Roadster', 21)");
        }
    }

    @AfterAll
    static void tearDown() throws SQLException {
        conn.close();
    }

    /** The kind of the site {@code e} raised, read through {@code statement}'s sites as exec reads it. */
    private static com.legend.sql.RaiseKind raisedKind(SQLException e,
            com.legend.sql.dialect.RenderedStatement statement) {
        var raised = (com.legend.exec.RaisedErrors.Raised) com.legend.exec.RaisedErrors.unwrapped(e,
                com.legend.planner.PlanMaker.sites(statement.sites(), com.legend.database.PlanStyle.PRODUCT));
        return raised.kind();
    }

    private static String sqlOf(String query) {
        return ResolvedSql.of(MODEL, query);
    }

    private List<String> exec(String sql) throws SQLException {
        List<String> rows = new ArrayList<>();
        try (Statement st = conn.createStatement(); ResultSet rs = st.executeQuery(sql)) {
            int n = rs.getMetaData().getColumnCount();
            while (rs.next()) {
                StringBuilder sb = new StringBuilder();
                for (int i = 1; i <= n; i++) {
                    if (i > 1) {
                        sb.append('|');
                    }
                    sb.append(rs.getString(i));
                }
                rows.add(sb.toString());
            }
        }
        rows.sort(String::compareTo);
        return rows;
    }

    private static final String FROM = "->from(rt::M, rt::RT)";

    @Test
    @DisplayName("match: value arms dispatch per row on the type column")
    void matchValueArms() throws SQLException {
        String sql = sqlOf("|rt::Vehicle.all()->project([v|$v.id, v|$v->match(["
                + "c:rt::Car[1]|$c.engineType,"
                + " b:rt::Bicycle[1]|'gears:' + $b.gears->toString()])],"
                + " ['id', 'power'])" + FROM);
        assertTrue(sql.contains("CASE WHEN"), "a CASE, not a host dispatch:\n" + sql);
        assertEquals(List.of("1|V8", "2|gears:21", "3|electric"), exec(sql), sql);
    }

    @Test
    @DisplayName("match: a catch-all arm at the input's class is the ELSE")
    void matchCatchAllArm() throws SQLException {
        String sql = sqlOf("|rt::Vehicle.all()->project([v|$v.id, v|$v->match(["
                + "c:rt::Car[1]|$c.engineType, v:rt::Vehicle[1]|$v.name])],"
                + " ['id', 'label'])" + FROM);
        assertEquals(List.of("1|V8", "2|Roadster", "3|electric"), exec(sql), sql);
    }

    @Test
    @DisplayName("match: no arm accepts the row's run-time type — raises like pure")
    void matchNoArmRaises() {
        var statement = ResolvedSql.statement(MODEL, "|rt::Vehicle.all()->project([v|$v->match(["
                + "c:rt::Car[1]|$c.engineType])], ['e'])" + FROM);
        SQLException e = assertThrows(SQLException.class, () -> exec(statement.sql()));
        // the product's raise is its kind (W7, 2026-11-16), read through the statement's sites as exec reads it (phase
        // D's Phase 2: the database raises the site's token); legend-pure's "Match failure" sentence is the edge's
        assertEquals(com.legend.sql.RaiseKind.MATCH_FAILURE, raisedKind(e, statement));
    }

    @Test
    @DisplayName("instanceOf in a filter: the row's type column decides")
    void instanceOfFilter() throws SQLException {
        String sql = sqlOf("|rt::Vehicle.all()->filter(v|$v->instanceOf(rt::Car))"
                + "->project([v|$v.name], ['n'])" + FROM);
        assertEquals(List.of("Hatch", "Sedan"), exec(sql), sql);
    }

    @Test
    @DisplayName("instanceOf as a value: one boolean per row; the input's own class is true")
    void instanceOfValue() throws SQLException {
        String sql = sqlOf("|rt::Vehicle.all()->project([v|$v.id,"
                + " v|$v->instanceOf(rt::Bicycle), v|$v->instanceOf(rt::Vehicle)],"
                + " ['id', 'bike', 'vehicle'])" + FROM);
        assertEquals(List.of("1|false|true", "2|true|true", "3|false|true"),
                exec(sql), sql);
    }

    @Test
    @DisplayName("cast(@Sub).prop reads the narrowed row where the type holds")
    void castNarrowedRead() throws SQLException {
        String sql = sqlOf("|rt::Vehicle.all()->filter(v|$v->instanceOf(rt::Car))"
                + "->project([v|$v->cast(@rt::Car).engineType], ['e'])" + FROM);
        assertEquals(List.of("V8", "electric"), exec(sql), sql);
    }

    @Test
    @DisplayName("cast(@Sub) over a row of another type raises like pure")
    void castMismatchRaises() {
        String sql = sqlOf("|rt::Vehicle.all()"
                + "->project([v|$v->cast(@rt::Car).engineType], ['e'])" + FROM);
        SQLException e = assertThrows(SQLException.class, () -> exec(sql));
        // the product's raise: its kind and where (W5 of docs/records/d-plan-2026-11-16.md); legend-pure's "Cast
        // exception: ... cannot be cast to ..." is the edge's (edge.PureMessages)
        // (phase D's Phase 2, 2026-11-16: the database raises the site's bare token; its kind and where are the plan's)
        assertTrue(e.getMessage().contains(com.legend.sql.RaiseKind.TOKEN), e.getMessage());
    }

    @Test
    @DisplayName("a subtype the row carries no columns for stays LOUD, never a guessed boolean")
    void unmappedSubtypeIsLoud() {
        String model = MODEL.replace("Class rt::Bicycle extends",
                "Class rt::Truck extends rt::Vehicle { axles: Integer[1]; }\n"
                + "Class rt::Bicycle extends");
        var ctx = Compiler.compileModel(model);
        SpecCompiler specs = new SpecCompiler(ctx);
        List<TypedSpec> body = specs.typeQueryBody(
                NameResolver.resolveQuery(com.legend.testing.Own.spec(
                        "|rt::Vehicle.all()->filter(v|$v->instanceOf(rt::Truck))"
                        + "->project([v|$v.name], ['n'])" + FROM)));
        RuntimeException e = assertThrows(RuntimeException.class,
                () -> new StoreResolver(ctx, specs).resolve(body, null));
        assertTrue(e.getMessage().contains("rt::Truck"), e.getMessage());
    }
}
