// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.planner;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.legend.error.NotImplementedException;
import java.util.LinkedHashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;

/**
 * A SNAPSHOT'S COPY IS MADE IN ONE TRANSACTION (the final audit's 1(b), 2026-11-12; {@link SnapCopy}): its drop and its
 * making are one, so a database whose host opens no transaction -- its host operations declaring none -- refuses the
 * copy by name, through the hosts' boundary, before anything is planned for it.
 */
class SnapCopyTransactionTest {

    @Test
    void aDatabaseWhoseHostOpensNoTransactionRefusesACopyByName() {
        Map<String, Object> r = new LinkedHashMap<>();
        r.put("databaseType", "Postgres");
        r.put("model", "###Relational\nDatabase x::DB ( Table people ( name VARCHAR(10) ) )\n###Connection\n"
                + "RelationalDatabaseConnection x::Conn { type: Postgres; specification: LocalH2 { }; auth: Test; }\n"
                + "###Runtime\nRuntime x::RT { mappings: []; connections: [ x::DB: [ c1: x::Conn ] ]; }\n");
        r.put("runtime", "x::RT");
        r.put("lambda", com.legend.parser.SpecParser.lambdaJson("|#>{x::DB.people}#", false));
        r.put("table", "t");
        assertEquals("a snapshot's copy is made in one transaction, and Postgres declares none",
                assertThrows(NotImplementedException.class,
                        () -> planner.Boundary.snapCopy(com.legend.json.Json.toCompact(r))).getMessage());
    }
}
