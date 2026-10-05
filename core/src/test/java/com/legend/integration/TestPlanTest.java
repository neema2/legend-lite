// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.integration;

import com.legend.Compiler;
import com.legend.model.ParsedModel;
import com.legend.model.ServiceDefinition;
import com.legend.testable.TestPlan;
import com.legend.testing.Own;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

/**
 * Studio plan A4: a service's tests PLANNED without a database (com.legend.testable.TestPlan) -- what the browser's
 * runner is handed: each test's provisioned tables (a connection's inline CSV, a ###Data reference resolved), its
 * runtime, its serialization format, and its assertions (an EqualToJson's expected JSON; a kind not judged named,
 * never dropped).
 */
class TestPlanTest {

    private static final String MODEL = """
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
            ###Data
            Data local::Rows
            {
              Relational
              #{
                default.T:
                  'ID,NAME\\n2,Bo\\n';
              }#
            }
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
                inline:
                {
                  data:
                  [
                    connections:
                    [
                      c1:
                        Relational
                        #{
                          default.T:
                            'ID,NAME\\n1,Al\\n';
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
                                data: '[{"name":"Al"}]';
                              }#;
                          }#,
                        text:
                          EqualToJson
                          #{
                            expected:
                              ExternalFormat
                              #{
                                contentType: 'text/plain';
                                data: 'Al';
                              }#;
                          }#
                      ]
                    }
                  ]
                },
                referenced:
                {
                  data:
                  [
                    connections:
                    [
                      c1:
                        Reference
                        #{
                          local::Rows
                        }#
                    ]
                  ]
                  tests:
                  [
                    reads:
                    {
                      asserts:
                      [
                        rows:
                          EqualToJson
                          #{
                            expected:
                              ExternalFormat
                              #{
                                contentType: 'application/json';
                                data: '[{"name":"Bo"}]';
                              }#;
                          }#
                      ]
                    }
                  ]
                }
              ]
            }
            """;

    @Test
    void eachTestCarriesItsTablesRuntimeFormatAndAssertions() {
        ParsedModel parsed = Own.model(MODEL);
        ServiceDefinition svc = parsed.elements().stream()
                .filter(e -> e instanceof ServiceDefinition s && s.qualifiedName().equals("local::S"))
                .map(e -> (ServiceDefinition) e).findFirst().orElseThrow();
        List<TestPlan.Planned> plan = TestPlan.of(Compiler.buildModel(parsed), svc);
        assertEquals(2, plan.size());

        TestPlan.Planned inline = plan.get(0);
        assertEquals("inline", inline.suiteId());
        assertEquals("reads", inline.testId());
        assertNull(inline.skipped());
        assertEquals("local::RT", inline.runtime());
        assertEquals("PURE_TDSOBJECT", inline.format());
        assertEquals(List.of(new TestPlan.Table("local::DB", "default", "T", "ID,NAME\n1,Al\n")), inline.tables());
        assertEquals("[{\"name\":\"Al\"}]", inline.assertions().get(0).expectedJson());
        assertEquals("assertion 'text': content type 'text/plain' is not judged by this runner", inline.assertions().get(1).skipped());

        // a ###Data reference is resolved to its rows; no format named is the engine's DEFAULT
        TestPlan.Planned referenced = plan.get(1);
        assertEquals(List.of(new TestPlan.Table("local::DB", "default", "T", "ID,NAME\n2,Bo\n")), referenced.tables());
        assertEquals("DEFAULT", referenced.format());
    }
}
