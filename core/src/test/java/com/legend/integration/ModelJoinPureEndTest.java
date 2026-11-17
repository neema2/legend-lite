package com.legend.integration;

import com.legend.testing.LaneFloor;
import com.legend.testing.Lean;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.SQLException;

import static org.junit.jupiter.api.Assertions.*;

/**
 * A ModelJoin association between two Pure (M2M) sets ({@code {firm: Firm[1], employee: Person[1] |
 * $firm.legalName == $employee.firstName}}) takes the property-space route an XStore with a Pure-set end takes: the
 * lambda's parameters renamed onto the predicate's rows, the sets pinned by id. Until 2026-11-14 it was refused ("the
 * property-space route covers XStore only so far"; the Studio roundtrip's DSL_Mapping-model-join-mapping).
 *
 * <p>legend-engine COMPILES this mapping but cannot run it: its graph fetch fails ("ModelJoinPropertyMapping cannot be
 * cast to PurePropertyMapping", probed 2026-11-14). The expected answers are the engine's for the same association
 * written as an XStore over the same sets and data, which it runs; lite answers both forms alike.
 */
@DisplayName("a ModelJoin with Pure-set ends")
@org.junit.jupiter.api.extension.ExtendWith(LaneFloor.class)
class ModelJoinPureEndTest {

    private static final String SETS = """
            ###Pure
            Class s::Firm { legalName: String[1]; }
            Class s::Person { firstName: String[1]; }
            Class t::Firm { legalName: String[1]; }
            Class t::Person { firstName: String[1]; }
            Association t::FirmPerson { firm: t::Firm[0..1]; employees: t::Person[*]; }
            ###Mapping
            Mapping t::M
            (
              *t::Firm[firm]: Pure { ~src s::Firm legalName: $src.legalName }
              *t::Person[person]: Pure { ~src s::Person firstName: $src.firstName }
              %s
            )
            ###Relational
            Database store::TestDb ( )
            ###Connection
            RelationalDatabaseConnection store::TestConnection { type: DuckDB; specification: DuckDB { }; auth: Test; }
            ###Runtime
            Runtime t::R
            {
              mappings: [ t::M ];
              connections: [ store::TestDb: [ env: store::TestConnection ], ModelStore: [
                f: #{ JsonModelConnection { class: s::Firm; url: 'data:application/json,[{"legalName":"A"},{"legalName":"B"}]'; } }#,
                p: #{ JsonModelConnection { class: s::Person; url: 'data:application/json,[{"firstName":"A"},{"firstName":"A"},{"firstName":"C"}]'; } }#
              ] ];
            }
            """;

    private static final String MODEL_JOIN = SETS.formatted("""
            t::FirmPerson: ModelJoin
              {
                {firm: t::Firm[1], employee: t::Person[1]|$firm.legalName == $employee.firstName}
              }""");

    private static final String XSTORE = SETS.formatted("""
            t::FirmPerson: XStore
              {
                firm[person, firm]: $this.firstName == $that.legalName,
                employees[firm, person]: $this.legalName == $that.firstName
              }""");

    private Connection connection;

    @BeforeEach
    void setUp() throws SQLException {
        connection = LaneFloor.backend().open();
    }

    @AfterEach
    void tearDown() throws SQLException {
        if (connection != null) {
            connection.close();
        }
    }

    private String fetch(String model, String query) {
        return Lean.run(model, query + "->from(t::M, t::R)", null, connection).json();
    }

    @Test
    @DisplayName("firm to employees: legend-engine's answer for the XStore twin")
    void firmToEmployees() {
        String q = "t::Firm.all()->graphFetch(#{t::Firm{legalName,employees{firstName}}}#)"
                + "->serialize(#{t::Firm{legalName,employees{firstName}}}#)";
        String engine = "[{\"legalName\":\"A\",\"employees\":[{\"firstName\":\"A\"},{\"firstName\":\"A\"}]},"
                + "{\"legalName\":\"B\",\"employees\":[]}]";
        assertEquals(engine, fetch(MODEL_JOIN, q));
        assertEquals(engine, fetch(XSTORE, q));
    }

    @Test
    @DisplayName("employee to firm: legend-engine's answer for the XStore twin")
    void employeeToFirm() {
        String q = "t::Person.all()->graphFetch(#{t::Person{firstName,firm{legalName}}}#)"
                + "->serialize(#{t::Person{firstName,firm{legalName}}}#)";
        String engine = "[{\"firstName\":\"A\",\"firm\":{\"legalName\":\"A\"}},{\"firstName\":\"A\",\"firm\":"
                + "{\"legalName\":\"A\"}},{\"firstName\":\"C\",\"firm\":null}]";
        assertEquals(engine, fetch(MODEL_JOIN, q));
        assertEquals(engine, fetch(XSTORE, q));
    }

    @Test
    @DisplayName("an inner lambda's parameter shadows an end's name: the inner read is the lambda's own")
    void anInnerLambdaShadowsAnEnd() {
        // `employee` inside map(...) is the lambda's own parameter (the name), not the association's end: the join
        // condition is the plain one (no list function: every lane's database reads it)
        String shadowed = MODEL_JOIN.replace("$firm.legalName == $employee.firstName}",
                "$firm.legalName == $employee.firstName->map(employee | $employee)->toOne()}");
        assertNotEquals(MODEL_JOIN, shadowed);
        String q = "t::Firm.all()->graphFetch(#{t::Firm{legalName,employees{firstName}}}#)"
                + "->serialize(#{t::Firm{legalName,employees{firstName}}}#)";
        assertEquals(fetch(MODEL_JOIN, q), fetch(shadowed, q));
    }
}
