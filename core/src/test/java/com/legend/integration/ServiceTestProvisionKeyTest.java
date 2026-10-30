// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.integration;

import com.legend.testing.LaneFloor;

import com.legend.Compiler;
import com.legend.model.ConnectionDefinition;
import com.legend.model.ServiceDefinition;
import com.legend.test.ServiceTestRunner;
import com.legend.testing.Own;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Review #15, fixed by rebuild W0.6 push 8: ServiceTestRunner keyed a provisioned CSV by its 32-bit String.hashCode,
 * so two services whose test data differed but collided ("Aa" vs "BB") shared one test runtime and the second ran on
 * the first's rows; the runtime's NAME was a hash of the same key, so under the SHARED policy they also shared a
 * session. Provisions are keyed by value now and runtimes are named by ordinal.
 */
@org.junit.jupiter.api.extension.ExtendWith(LaneFloor.class)
class ServiceTestProvisionKeyTest {

    private static final String CSV_A = "ID,NAME\n1,Aa\n";
    private static final String CSV_B = "ID,NAME\n1,BB\n";

    private static String service(String name, String pattern, String suite, String test, String value) {
        return """
                Service local::%s
                {
                  pattern: '%s';
                  documentation: '';
                  execution: Single
                  {
                    query: |local::P.all()->project(~[name: x | $x.name]);
                    mapping: local::M;
                    runtime: local::RT;
                  }
                  testSuites:
                  [
                    %s:
                    {
                      data:
                      [
                        connections:
                        [
                          c1:
                            Relational
                            #{
                              default.T:
                                'ID,NAME\\n1,%s\\n';
                            }#
                        ]
                      ]
                      tests:
                      [
                        %s:
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
                                    data: '[{"name":"%s"}]';
                                  }#;
                              }#
                          ]
                        }
                      ]
                    }
                  ]
                }
                """.formatted(name, pattern, suite, value, test, value);
    }

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
            ###Service
            """ + service("ServiceA", "/a", "suiteA", "readsA", "Aa")
            + service("ServiceB", "/b", "suiteB", "readsB", "BB");

    private static ServiceTestRunner runner(com.legend.compiler.element.ModelContext ctx) {
        return runner(ctx, ServiceTestRunner.Sessions.FRESH_PER_TEST);
    }

    private static ServiceTestRunner runner(com.legend.compiler.element.ModelContext ctx,
            ServiceTestRunner.Sessions policy) {
        return new ServiceTestRunner(ctx, () -> LaneFloor.backend().open(),
                policy, LaneFloor.backend().type());
    }

    private static ServiceDefinition service(String fqn) {
        return Own.model(MODEL).elements().stream()
                .filter(e -> e instanceof ServiceDefinition s && s.qualifiedName().equals(fqn))
                .map(e -> (ServiceDefinition) e)
                .findFirst()
                .orElseThrow(() -> new IllegalStateException("no service " + fqn));
    }

    private static ServiceTestRunner.Result only(List<ServiceTestRunner.Result> results) {
        if (results.size() != 1) {
            throw new IllegalStateException("expected one test result, got " + results);
        }
        return results.get(0);
    }

    @Test
    void collidingCsvHashesDoNotShareATestRuntime() {
        if (CSV_A.equals(CSV_B) || CSV_A.hashCode() != CSV_B.hashCode()) {
            throw new IllegalStateException("the probe needs two different CSVs with one hashCode");
        }
        var ctx = Compiler.compileModel(MODEL);
        ServiceDefinition a = service("local::ServiceA");
        ServiceDefinition b = service("local::ServiceB");
        // control: B's suite passes on a runner that has provisioned nothing else
        try (ServiceTestRunner solo = runner(ctx)) {
            ServiceTestRunner.Result alone = only(solo.run(b));
            if (!alone.pass()) {
                throw new IllegalStateException("control: B alone failed: " + alone.reason());
            }
        }
        try (ServiceTestRunner shared = runner(ctx)) {
            ServiceTestRunner.Result ra = only(shared.run(a));
            if (!ra.pass()) {
                throw new IllegalStateException("control: A failed: " + ra.reason());
            }
            ServiceTestRunner.Result rb = only(shared.run(b));
            // was FAIL: B read A's rows
            assertEquals(ServiceTestRunner.Status.PASS, rb.status(), rb.reason());
            // A again on the same runner: its own runtime is still its own
            assertEquals(ServiceTestRunner.Status.PASS, only(shared.run(a)).status());
        }
    }

    @Test
    void collidingCsvHashesDoNotShareASessionUnderSharedPolicy() {
        var ctx = Compiler.compileModel(MODEL);
        try (ServiceTestRunner shared = runner(ctx, ServiceTestRunner.Sessions.SHARED)) {
            assertEquals(ServiceTestRunner.Status.PASS, only(shared.run(service("local::ServiceA"))).status());
            ServiceTestRunner.Result rb = only(shared.run(service("local::ServiceB")));
            assertEquals(ServiceTestRunner.Status.PASS, rb.status(), rb.reason());
            // two provisionings, two sessions (the runtime name used to be a hash of the key, so one)
            assertEquals(2, shared.sessions().size());
        }
    }

    @Test
    void identicalCsvSharesOneRuntimeAndSession() {
        // two services with the SAME data: sharing is preserved
        String twin = MODEL.replace(service("ServiceB", "/b", "suiteB", "readsB", "BB"),
                service("ServiceB", "/b", "suiteB", "readsB", "Aa"));
        var ctx = Compiler.compileModel(twin);
        ServiceDefinition a = Own.model(twin).elements().stream()
                .filter(e -> e instanceof ServiceDefinition s && s.qualifiedName().equals("local::ServiceA"))
                .map(e -> (ServiceDefinition) e).findFirst().orElseThrow();
        ServiceDefinition b = Own.model(twin).elements().stream()
                .filter(e -> e instanceof ServiceDefinition s && s.qualifiedName().equals("local::ServiceB"))
                .map(e -> (ServiceDefinition) e).findFirst().orElseThrow();
        try (ServiceTestRunner shared = runner(ctx, ServiceTestRunner.Sessions.SHARED)) {
            assertEquals(ServiceTestRunner.Status.PASS, only(shared.run(a)).status());
            assertEquals(ServiceTestRunner.Status.PASS, only(shared.run(b)).status());
            assertEquals(1, shared.sessions().size());
        }
    }
}
