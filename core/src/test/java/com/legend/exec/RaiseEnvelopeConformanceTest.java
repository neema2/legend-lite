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
        // the statement's site table (PlanJson v59's sites: facts, no words; stage B's B1, 2026-11-20): each reader
        // words a site itself, the one catalog's sentence of its kind over its operands (stage B's BE, 2026-11-21)
        List<com.legend.executionplan.ExecutionPlan.Site> sites = new java.util.ArrayList<>();
        for (Json.Node n : fixture.getArr("sites").items()) {
            Json.Obj so = (Json.Obj) n;
            assertTrue(so.getOr("message", null) == null, "a v59 site carries no words: site " + so.getInt("n"));
            List<com.legend.executionplan.Operand> operands = new java.util.ArrayList<>();
            Json.Node os = so.getOr("operands", null);
            if (os != null) {
                for (Json.Node on : ((Json.Arr) os).items()) {
                    operands.add(operand((Json.Obj) on));
                }
            }
            sites.add(new com.legend.executionplan.ExecutionPlan.Site(so.getInt("n"),
                    com.legend.sql.RaiseKind.valueOf(so.getString("kind")), so.getStringOr("where", null), operands));
        }
        List<Json.Node> cases = fixture.getArr("cases").items();
        assertTrue(cases.size() > 5, "the fixture holds too few cases to prove anything: " + cases.size());
        for (Json.Node node : cases) {
            Json.Obj c = (Json.Obj) node;
            String name = c.getString("name");
            java.sql.SQLException failed = new java.sql.SQLException(c.getString("message"));
            if (c.getBoolOr("refused", false)) {
                var refused = org.junit.jupiter.api.Assertions.assertThrows(IllegalStateException.class,
                        () -> RaisedErrors.unwrapped(failed, sites), name);
                assertTrue(refused.getMessage().contains("does not declare"), refused.getMessage());
                continue;
            }
            assertEquals(c.getString("words"), RaisedErrors.dataErrorOf(RaisedErrors.unwrapped(failed, sites))
                    .getMessage(), name);
            java.sql.SQLException read = RaisedErrors.unwrapped(failed, sites);
            com.legend.error.ErrorKind told = RaisedErrors.dataErrorOf(read).kind();
            String kind = told == null ? null : told.name();
            String where = read instanceof RaisedErrors.Raised r ? r.where() : null;
            assertEquals(c.getStringOr("kind", null), kind, name + ": its kind");
            assertEquals(c.getStringOr("where", null), where, name + ": its where");
        }
    }

    /** A fixture site's operand, one key and its value (PlanJson v59's: the fixture's are texts, Integers and
     *  computed values). */
    private static com.legend.executionplan.Operand operand(Json.Obj o) {
        if (o.getOr("string", null) != null) {
            return new com.legend.executionplan.Operand.Text(o.getString("string"));
        }
        if (o.getOr("integer", null) != null) {
            return new com.legend.executionplan.Operand.Int(o.getLong("integer"));
        }
        if (o.getOr("computed", null) != null) {
            return com.legend.executionplan.Operand.Computed.IT;
        }
        throw new IllegalArgumentException("a fixture operand this test does not read: " + o);
    }
}
