// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.compiler;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.legend.testbackend.TestBackends;

import java.io.StringWriter;
import java.sql.Connection;
import java.sql.Statement;

import org.junit.jupiter.api.Test;

/**
 * A CLASS IN THE ROOT PACKAGE ({@code Class Thing}, no {@code ::}) with a derived property compiles and runs. Its
 * synthesized body function ({@code Thing$prop$shout}) carries no {@code ::}, and the function lookup took every such
 * name for a bare reference: the model failed to compile ("binds to unknown function"), and since the GraphQL
 * companion gives every root class a {@code __typename()} member, so did every root-package class at all -- the
 * PCT's injected {@code Class String {}} among them (found 2026-10-20 by the GraphQL hardening work's PCT run).
 */
class RootPackageClassTest {

    private static final String MODEL = """
            Class Thing
            {
              id: Integer[1];
              name: String[1];
              shout() { $this.name->toUpper() }: String[1];
            }

            ###Relational
            Database store::Db ( Table THING (ID INTEGER PRIMARY KEY, NAME VARCHAR(20)) )

            ###Mapping
            Mapping store::Map
            (
              Thing: Relational
              {
                ~mainTable [store::Db] THING
                id: [store::Db] THING.ID,
                name: [store::Db] THING.NAME
              }
            )

            ###Connection
            RelationalDatabaseConnection store::Conn { type: DuckDB; specification: DuckDB { }; auth: Test; }

            ###Runtime
            Runtime store::Runtime { mappings: [ store::Map ]; connections: [ store::Db: [ environment: store::Conn ] ]; }
            """;

    @Test
    void aRootPackageClassWithADerivedPropertyCompiles() {
        com.legend.Compiler.compileModel(MODEL);
        com.legend.Compiler.compileModel("Class String {\n}\nClass a::Firm {\n  legalName: String[1];\n}\n");
    }

    @Test
    void itsDerivedPropertyRuns() throws Exception {
        try (Connection c = TestBackends.platform().open()) {
            try (Statement s = c.createStatement()) {
                s.execute("CREATE TABLE THING (ID INTEGER PRIMARY KEY, NAME VARCHAR(20))");
                s.execute("INSERT INTO THING VALUES (1, 'ab'), (2, 'cd')");
            }
            StringWriter w = new StringWriter();
            com.legend.Execution.executeWire(MODEL, "|Thing.all()->project(~[id: x|$x.id, s: x|$x.shout()])->sort(~id->ascending())",
                    "store::Runtime", c, com.legend.lowering.WireRender.Format.JSON, w);
            assertEquals("[{\"id\":1,\"s\":\"AB\"},{\"id\":2,\"s\":\"CD\"}]", w.toString());
        }
    }
}
