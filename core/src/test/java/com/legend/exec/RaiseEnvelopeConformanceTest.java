// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.exec;

import com.legend.json.Json;
import com.legend.testing.Runfile;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * A PRODUCT RAISE'S ENVELOPE, ONE FIXTURE (#104's M2(b), the narrow check's item 7, 2026-11-15): Java's runner reads each
 * case of engine-client/test/fixtures/raise-envelopes.json -- the words it says ({@link RaisedErrors#dataError}) and the
 * kind and where it types -- as the tab's (raised.ts) and Python's (_raised.py) readers do, so the three cannot drift.
 */
class RaiseEnvelopeConformanceTest {

    @Test
    void javasRunnerReadsEachEnvelopeAsTheHostsDo() throws Exception {
        Json.Obj fixture = (Json.Obj) Json.parse(Files.readString(Runfile.property("raise.fixture"),
                StandardCharsets.UTF_8));
        List<Json.Node> cases = fixture.getArr("cases").items();
        assertTrue(cases.size() > 5, "the fixture holds too few cases to prove anything: " + cases.size());
        for (Json.Node node : cases) {
            Json.Obj c = (Json.Obj) node;
            String name = c.getString("name");
            java.sql.SQLException failed = new java.sql.SQLException(c.getString("message"));
            assertEquals(c.getString("words"), RaisedErrors.dataError(failed).getMessage(), name);
            java.sql.SQLException read = RaisedErrors.unwrapped(failed);
            String kind = read instanceof RaisedErrors.Raised r && r.kind() != null ? r.kind().name() : null;
            String where = read instanceof RaisedErrors.Raised r ? r.where() : null;
            assertEquals(c.getStringOr("kind", null), kind, name + ": its kind");
            assertEquals(c.getStringOr("where", null), where, name + ": its where");
        }
    }
}
