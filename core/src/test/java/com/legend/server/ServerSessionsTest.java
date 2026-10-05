// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.server;

import com.legend.exec.ExecutionResult;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The server opens the session the COMPILER decided (C3b, docs/PLAN_EXECUTION_SPLIT_AND_DATABASE_OWNER_2026_10_03.md):
 * the runtime's declared connection, or the platform's DuckDB for a runtime that binds no database — never a
 * connection of its own choosing (it took the runtime's first binding, folded unknown specifications into an
 * in-memory database, and could not run a runtime binding only model data: "Connection not found").
 */
class ServerSessionsTest {

    private static final String PEOPLE = """
            ###Pure
            Class model::RawPerson
            {
                firstName: String[1];
                lastName:  String[1];
            }

            Class model::Person
            {
                fullName: String[1];
            }

            ###Mapping
            Mapping model::PersonM2M
            (
                model::Person: Pure
                {
                    ~src model::RawPerson
                    fullName: $src.firstName + ' ' + $src.lastName
                }
            )
            """;

    @Test
    void aRuntimeBindingOnlyModelDataRunsOnThePlatformDuckDb() throws Exception {
        String model = PEOPLE + """
                ###Runtime
                Runtime test::JsonOnly
                {
                    mappings: [ model::PersonM2M ];
                    connections:
                    [
                        ModelStore:
                        [
                            json: #{
                                JsonModelConnection
                                {
                                    class: model::RawPerson;
                                    url: 'data:application/json,[{"firstName":"Ada","lastName":"Lovelace"},{"firstName":"Alan","lastName":"Turing"}]';
                                }
                            }#
                        ]
                    ];
                }
                """;
        ExecutionResult result = new QueryService().execute(model,
                "model::Person.all()->project(~[fullName:x|$x.fullName])", "test::JsonOnly");
        ExecutionResult.Tabular rows = assertInstanceOf(ExecutionResult.Tabular.class, result);
        assertEquals(2, rows.rowCount(), String.valueOf(result));
    }

    @Test
    void aSpecificationNotBuiltForItsDatabaseIsRefusedByName() {
        String model = PEOPLE + """
                ###Relational
                Database store::DB ( Table T (ID INTEGER) )
                ###Connection
                RelationalDatabaseConnection store::Conn
                {
                    type: DuckDB;
                    specification: Static { name: 'db'; host: 'localhost'; port: 5432; };
                    auth: Test;
                }
                ###Runtime
                Runtime test::RT { mappings: [ ]; connections: [ store::DB: [ environment: store::Conn ] ]; }
                """;
        var refused = assertThrows(com.legend.error.NotImplementedException.class,
                () -> new QueryService().execute(model, "1 + 1", "test::RT"));
        assertTrue(refused.getMessage().contains("StaticDatasource specification is not implemented"),
                refused.getMessage());
    }

    @Test
    void twoDifferentConnectionsInOneRuntimeAreRefusedByName() {
        String model = PEOPLE + """
                ###Relational
                Database store::A ( Table T (ID INTEGER) )
                Database store::B ( Table U (ID INTEGER) )
                ###Connection
                RelationalDatabaseConnection store::ConnA { type: DuckDB; specification: DuckDB { }; auth: Test; }
                RelationalDatabaseConnection store::ConnB { type: DuckDB; specification: DuckDB { path: 'b.duckdb'; }; auth: Test; }
                ###Runtime
                Runtime test::RT
                {
                    mappings: [ ];
                    connections: [ store::A: [ a: store::ConnA ], store::B: [ b: store::ConnB ] ];
                }
                """;
        var refused = assertThrows(com.legend.error.NotImplementedException.class,
                () -> new QueryService().execute(model, "1 + 1", "test::RT"));
        assertTrue(refused.getMessage().contains("different connections"), refused.getMessage());
    }
}
