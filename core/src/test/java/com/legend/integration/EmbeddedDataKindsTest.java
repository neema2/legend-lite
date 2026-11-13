// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.integration;

import com.legend.Compiler;
import com.legend.model.ServiceDefinition;
import com.legend.protocol.Protocol;
import com.legend.setup.ProjectData;
import com.legend.test.ServiceTestRunner;
import com.legend.testing.LaneFloor;
import com.legend.testing.Own;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Which kinds of a model's data have a loading path (B2e, 2026-11-11, census M17; docs/DATA_DELIVERY_DESIGN_2026_11_10.md
 * §9): a relational store's CSV rows alone. Every reader switches over the kinds without a default; the project's
 * database holds a relational store's tables and names every other kind as no table of its; a service test whose data
 * is another kind is refused as not implemented yet, by name, where it was a skip.
 */
@org.junit.jupiter.api.extension.ExtendWith(LaneFloor.class)
class EmbeddedDataKindsTest {

    /** The kinds the protocol has: a new one fails here until each reader classifies it (com.legend.protocol.
     *  EmbeddedDataKinds, ProjectData.tables, ServiceTestRunner.addProvision). */
    @Test
    void theKindsAreTheSixEachReaderClassifies() {
        Set<String> kinds = Arrays.stream(Protocol.PEmbeddedDataValue.class.getPermittedSubclasses())
                .map(Class::getSimpleName).collect(Collectors.toCollection(TreeSet::new));
        assertEquals(new TreeSet<>(List.of("PDataReference", "PExternalFormatData", "PModelStoreData",
                "PRelationData", "PRelationalCsvData", "PServiceStoreData")), kinds);
    }

    private static final String PROJECT = """
            ###Relational
            Database x::DB ( Table T (ID INTEGER PRIMARY KEY, NAME VARCHAR(20)) )
            ###Data
            Data x::Rows
            {
              Relational
              #{
                default.T:
                  'ID,NAME\\n1,a\\n';
              }#
            }
            Data x::Document
            {
              ExternalFormat
              #{
                contentType: 'application/json';
                data: '{"ID":2}';
              }#
            }
            Data x::Again
            {
              Reference
              #{
                x::Rows
              }#
            }
            """;

    /** The project's database fills a relational element's tables, once; a document and a reference are no table of
     *  its (the reference's element is filled itself). */
    @Test
    void theProjectsDatabaseHoldsARelationalStoresRowsAlone() {
        var ctx = Compiler.compileModel(PROJECT);
        assertEquals("default\nT\nID,NAME\n1,a\n\n-\n", ProjectData.rowsKey(ctx));
        assertEquals(null, ProjectData.refusal(ctx));
        assertTrue(ProjectData.fillSteps(ctx, com.legend.testbackend.TestBackends.platform().dialect()).size() > 0);
    }

    private static final String SERVICE = """
            ###Pure
            Class local::P { name: String[1]; }
            ###Relational
            Database local::DB ( Table T (ID INTEGER PRIMARY KEY, NAME VARCHAR(32)) )
            ###Mapping
            Mapping local::M ( *local::P: Relational { ~mainTable [local::DB] T
                name: [local::DB] T.NAME } )
            ###Connection
            RelationalDatabaseConnection local::Conn
            { type: DuckDB; specification: DuckDB { }; auth: Test; }
            ###Runtime
            Runtime local::RT
            { mappings: [ local::M ]; connections: [ local::DB: [ c1: local::Conn ] ]; }
            ###Service
            Service local::S
            {
              pattern: '/s';
              documentation: '';
              execution: Single
              {
                query: |local::P.all()->project(~[name: x | $x.name]);
                mapping: local::M;
                runtime: local::RT;
              }
              testSuites:
              [
                suite:
                {
                  data:
                  [
                    connections:
                    [
                      c1:
                        ExternalFormat
                        #{
                          contentType: 'application/json';
                          data: '[{"ID":1,"NAME":"a"}]';
                        }#
                    ]
                  ]
                  tests:
                  [
                    reads:
                    {
                      serializationFormat: PURE_TDSOBJECT;
                      asserts:
                      [
                        rows:
                          EqualToJson
                          #{
                            expected:
                              ExternalFormat
                              #{
                                contentType: 'application/json';
                                data: '[{"name":"a"}]';
                              }#;
                          }#
                      ]
                    }
                  ]
                }
              ]
            }
            """;

    /** A service test whose data is a document for a relational store: refused as not implemented yet, naming the kind
     *  and the store (until B2e a skip, "not provisioned by this runner"). */
    @Test
    void aServiceTestsDataOfAnotherKindIsNotImplementedYet_byName() {
        var ctx = Compiler.compileModel(SERVICE);
        ServiceDefinition svc = Own.model(SERVICE).elements().stream()
                .filter(e -> e instanceof ServiceDefinition)
                .map(e -> (ServiceDefinition) e).findFirst().orElseThrow();
        try (ServiceTestRunner runner = new ServiceTestRunner(ctx, () -> LaneFloor.backend().openSession(),
                ServiceTestRunner.Sessions.FRESH_PER_TEST, LaneFloor.backend().type())) {
            List<ServiceTestRunner.Result> results = runner.run(svc);
            assertEquals(1, results.size(), results.toString());
            ServiceTestRunner.Result r = results.get(0);
            assertEquals(ServiceTestRunner.Status.FAIL, r.status(), r.reason());
            assertTrue(r.reason().contains("ExternalFormat (a document of application/json) data is not implemented yet")
                    && r.reason().contains("local::DB"), r.reason());
        }
    }
}
