package com.legend.integration;

import com.legend.testing.LaneFloor;
import com.legend.testing.Lean;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * An M2M pass-through: a class-typed property mapped to a transform of its own type, its class mapped nowhere in the
 * mapping ({@code employees: $src.employees}, legend-studio's roundtrip case DSL_Snowflake-m2m-udf-function-activator).
 * legend-engine 4.145.0 compiles it ({@code ClassMappingThirdPassBuilder}'s {@code allowComplexPropertyMappingPassThrough}:
 * no set for the class, the property's target id is the class's default id, and the property's type is among the
 * transform's generalizations; any other id, or a transform of another type, is "Can't find class mapping '<id>'") and
 * runs it: the source objects are the target's values, served as they are ({@code testComplexTypeAutoMapping.pure}
 * {@code testComplexTypePassThrough*}, AlloyOnly). Lite compiles it (the binding's type is checked by the constructor's
 * own subsumption) and serves the rest of the class; a read through the pass-through is refused by name, a lite gap
 * (register S419).
 */
@DisplayName("M2M class-typed pass-through")
@org.junit.jupiter.api.extension.ExtendWith(LaneFloor.class)
class M2MPassThroughTest {

    /** The roundtrip case's model, verbatim. */
    private static final String STUDIO_CASE = """
            ###Relational
            Database store::TestDB
            (
              Table FirmTable
              (
                id INTEGER PRIMARY KEY,
                legal_name VARCHAR(200)
              )
              Table PersonTable
              (
                id INTEGER PRIMARY KEY,
                firm_id INTEGER,
                firstName VARCHAR(200),
                lastName VARCHAR(200)
              )
              Table HobbyTable
              (
                id INTEGER PRIMARY KEY,
                name VARCHAR(200)
              )
              Table AddressTable
              (
                id INTEGER PRIMARY KEY,
                firm_id INTEGER,
                pincode INTEGER,
                streetName VARCHAR(200)
              )

              Join FirmPerson(PersonTable.firm_id = FirmTable.id)
              Join FirmAddress(AddressTable.firm_id = FirmTable.id)
              Join HobbyPerson(PersonTable.id = HobbyTable.id)
            )


            ###Snowflake
            SnowflakeM2MUdf test::udf::MyUDF
            {
               udfName : 'M2MUDF';
               function : test::query::getFirmDetailsWithInput(String[1]):String[1];
               ownership : Deployment { identifier: '1234'};
               deploymentSchema : 'PARTH_UDF_SCHEMA';
               deploymentStage : 'PARTH_UDF_SCHEMA.PARTH_DEMO';
               description : 'A super nice M2M Udf!';
               activationConfiguration : FactsetTest::connections::factsetTestConnectionDev;
            }


            ###Pure
            Class test::model::Firms
            {
              firms: test::model::Firm[1..*];
            }

            Class test::model::Firm
            {
              legalName: String[1];
              employees: test::model::Person[*];
            }

            Class test::model::Person
            {
              firstName: String[1];
              lastName: String[1];
            }

            function test::query::getFirmDetailsWithInput(input: String[1]): String[1]
            {
              test::model::Firms.all()->graphFetch(#{test::model::Firms{firms{legalName,employees{firstName,lastName}}}}#)->serialize(#{test::model::Firms{firms{legalName,employees{firstName,lastName}}}}#)->from(test::mapping::firmMapping, test::runtime::testRuntimeWithInput)
            }


            ###Mapping
            Mapping test::mapping::firmMapping
            (
              *test::model::Firms: Pure
              {
                ~src test::model::Firms
                firms: $src.firms
              }
              *test::model::Firm: Pure
              {
                ~src test::model::Firm
                legalName: $src.legalName,
                employees: $src.employees
              }
            )


            ###Connection
            JsonModelConnection test::connection::testConnectionWithInput
            {
              class: test::model::Firms;
              url: 'data:application/json,${input}';
            }

            RelationalDatabaseConnection FactsetTest::connections::factsetTestConnectionDev
            {
              store: store::TestDB;
              type: Snowflake;
              specification: Snowflake
              {
                name: 'test';
                account: 'test';
                warehouse: 'test';
                region: 'test';
                cloudType: 'test';
              };
              auth: SnowflakePublic
              {
                publicUserName: 'test';
                privateKeyVaultReference: 'test';
                passPhraseVaultReference: 'test';
              };
            }


            ###Runtime
            Runtime test::runtime::testRuntimeWithInput
            {
              mappings:
              [
                test::mapping::firmMapping
              ];
              connections:
              [
                ModelStore:
                [
                  connection_1: test::connection::testConnectionWithInput
                ]
              ];
            }
            """;

    /** {@code line}: the pass-through's property mapping line. */
    private static String model(String line) {
        return """
                ###Pure
                Class s::Party { name: String[1]; }
                Class s::Holder { id: String[1]; party: s::Party[1]; }
                Class s::Trade { id: String[1]; party: s::Party[1]; }
                ###Mapping
                Mapping t::M
                (
                  *s::Trade: Pure { ~src s::Holder id: $src.id, %s }
                )
                ###Relational
                Database store::TestDb ( )
                ###Connection
                RelationalDatabaseConnection store::TestConnection { type: DuckDB; specification: DuckDB { }; auth: Test; }
                ###Runtime
                Runtime t::R
                {
                  mappings: [ t::M ];
                  connections: [ store::TestDb: [ env: store::TestConnection ], ModelStore: [ c: #{ JsonModelConnection { class: s::Holder; url: 'data:application/json,[{"id":"a","party":{"name":"P"}}]'; } }# ] ];
                }
                """.formatted(line);
    }

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

    @Test
    @DisplayName("the roundtrip case compiles, as on legend-engine")
    void theStudioCaseCompiles() {
        assertEquals(List.of(), com.legend.Compiler.compileErrors(STUDIO_CASE));
    }

    @Test
    @DisplayName("the class's other properties are served: legend-engine's answer")
    void theOtherPropertiesAreServed() {
        assertEquals("[{\"id\":\"a\"}]", Lean.run(model("party: $src.party"),
                "s::Trade.all()->graphFetch(#{s::Trade{id}}#)->serialize(#{s::Trade{id}}#)->from(t::M, t::R)",
                null, connection).json());
    }

    @Test
    @DisplayName("a read through the pass-through is refused by name, a lite gap (legend-engine serves the source's objects)")
    void aReadThroughItIsRefused() {
        for (String query : List.of(
                "s::Trade.all()->graphFetch(#{s::Trade{id,party{name}}}#)"
                        + "->serialize(#{s::Trade{id,party{name}}}#)->from(t::M, t::R)",
                "s::Trade.all()->project([x|$x.id, x|$x.party.name], ['id', 'name'])->from(t::M, t::R)")) {
            Throwable t = assertThrows(com.legend.error.NotImplementedException.class,
                    () -> Lean.run(model("party: $src.party"), query, null, connection));
            assertTrue(t.getMessage().contains("'party'"), t.getMessage());
        }
    }

    @Test
    @DisplayName("the class's default set id is the pass-through's own; any other id is legend-engine's error")
    void theTargetIdIsChecked() {
        assertEquals(List.of(), com.legend.Compiler.compileErrors(model("party[s_Party]: $src.party")));
        List<String> errors = com.legend.Compiler.compileErrors(model("party[other]: $src.party"));
        assertEquals(1, errors.size(), errors.toString());
        assertTrue(errors.get(0).contains("Can't find class mapping 'other'"), errors.toString());
        // an id another class's set answers is the engine's routed check, not that error: lite refuses it as a route
        // it does not make (the set-routed refusal), never in the engine's words
        errors = com.legend.Compiler.compileErrors(model("party[s_Trade]: $src.party"));
        assertEquals(1, errors.size(), errors.toString());
        assertFalse(errors.get(0).contains("Can't find class mapping"), errors.toString());
    }

    @Test
    @DisplayName("a transform of another type is refused at compile with legend-engine's error")
    void aTransformOfAnotherTypeIsRefused() {
        // a $src property path's type is read at build: the engine's text, naming the class's default id
        List<String> errors = com.legend.Compiler.compileErrors(model("party: $src.id"));
        assertEquals(1, errors.size(), errors.toString());
        assertTrue(errors.get(0).contains("Can't find class mapping 's_Party'"), errors.toString());
        // any other transform: the constructor's subsumption, in lite's words
        errors = com.legend.Compiler.compileErrors(model("party: $src.id->toOne()"));
        assertEquals(1, errors.size(), errors.toString());
        assertTrue(errors.get(0).contains("party"), errors.toString());
    }
}
