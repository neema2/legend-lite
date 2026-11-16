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
 * A JSON source's TO-MANY property reads its array element by element ({@code toMany(get(..), @T)}, at least one where
 * the property says so). Until 2026-11-14 the source's mapping read it with {@code to}, as ONE value: a {@code [1..*]}
 * property was refused at model build as {@code [0..1]} (the Studio roundtrip's r1 repro) and a {@code [*]} one read
 * an array as a single value. The expected answers are legend-engine's, probed on the same model, data and query
 * through the M2M mapping (2026-11-14; the engine refuses a graph fetch of the JSON source class itself). A to-many
 * CLASS-typed property, a nested array of objects, does not fail the build: a query through the class is refused by name.
 */
@DisplayName("a JSON source's to-many properties")
@org.junit.jupiter.api.extension.ExtendWith(LaneFloor.class)
class JsonSourceToManyTest {

    private static final String MODEL = """
            ###Pure
            Class t::A { n: String[1]; xs: String[1..*]; ys: Integer[*]; }
            Class t::B { n: String[1]; xs: String[1..*]; ys: Integer[*]; first: String[1]; total: Integer[0..1]; }
            ###Mapping
            Mapping t::M
            (
              *t::B: Pure { ~src t::A n: $src.n, xs: $src.xs, ys: $src.ys, first: $src.xs->at(0), total: $src.ys->sum() }
            )
            ###Relational
            Database store::TestDb ( )
            ###Connection
            RelationalDatabaseConnection store::TestConnection { type: DuckDB; specification: DuckDB { }; auth: Test; }
            ###Runtime
            Runtime t::R
            {
              mappings: [ t::M ];
              connections: [ store::TestDb: [ env: store::TestConnection ], ModelStore: [ c: #{ JsonModelConnection { class: t::A; url: 'data:application/json,[{"n":"a","xs":["p","q"],"ys":[1,2,3]},{"n":"b","xs":["r"],"ys":[]}]'; } }# ] ];
            }
            """;

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
    @DisplayName("arrays read whole, through the M2M mapping: legend-engine's answer")
    void arraysReadWhole() {
        assertEquals("[{\"n\":\"a\",\"xs\":[\"p\",\"q\"],\"ys\":[1,2,3]},{\"n\":\"b\",\"xs\":[\"r\"],\"ys\":[]}]",
                Lean.run(MODEL, "t::B.all()->graphFetch(#{t::B{n,xs,ys}}#)->serialize(#{t::B{n,xs,ys}}#)"
                        + "->from(t::M, t::R)", null, connection).json());
    }

    @Test
    @DisplayName("a function of the array, through the M2M mapping: legend-engine's answer")
    void functionsOfTheArray() {
        assertEquals("[{\"n\":\"a\",\"first\":\"p\",\"total\":6},{\"n\":\"b\",\"first\":\"r\",\"total\":0}]",
                Lean.run(MODEL, "t::B.all()->graphFetch(#{t::B{n,first,total}}#)->serialize(#{t::B{n,first,total}}#)"
                        + "->from(t::M, t::R)", null, connection).json());
    }

    @Test
    @DisplayName("a nested array of objects: the model builds, a query through the class is refused by name")
    void aNestedArrayOfObjectsIsRefusedAtUse() {
        // a lite gap, not an engine refusal (legend-engine accepts the model): the class is poisoned
        // (MappingNormalizer's line between the two), the build does not fail. The class and not the key: a read
        // through the property answered null when only the key was deferred (JsonSourceFrame reads the declaration)
        String model = """
                ###Pure
                Class t::P { first: String[1]; }
                Class t::F { name: String[1]; employees: t::P[1..*]; }
                Class t::G { name: String[1]; firsts: String[*]; }
                ###Mapping
                Mapping t::M ( *t::G: Pure { ~src t::F name: $src.name, firsts: $src.employees.first } )
                ###Relational
                Database store::TestDb ( )
                ###Connection
                RelationalDatabaseConnection store::TestConnection { type: DuckDB; specification: DuckDB { }; auth: Test; }
                ###Runtime
                Runtime t::R
                {
                  mappings: [ t::M ];
                  connections: [ store::TestDb: [ env: store::TestConnection ], ModelStore: [ c: #{ JsonModelConnection { class: t::F; url: 'data:application/json,[{"name":"a","employees":[{"first":"x"}]},{"name":"b","employees":[{"first":"y"}]}]'; } }# ] ];
                }
                """;
        com.legend.Compiler.compileModel(model);
        for (String tree : java.util.List.of("#{t::G{name}}#", "#{t::G{name,firsts}}#")) {
            Throwable t = assertThrows(Throwable.class, () -> Lean.run(model,
                    "t::G.all()->graphFetch(" + tree + ")->serialize(" + tree + ")->from(t::M, t::R)", null,
                    connection), tree);
            StringBuilder all = new StringBuilder();
            for (Throwable c = t; c != null; c = c.getCause() == c ? null : c.getCause()) {
                all.append(c.getMessage()).append(" | ");
            }
            assertTrue(all.toString().contains("is a to-many class-typed property"), tree + ": " + all);
        }
    }
}
