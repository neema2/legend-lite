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
        // the statement's site table (PlanJson's sites): each message error.ErrorKind's sentence and its where, the one
        // source (phase D's Phase 2, raises as SITES, 2026-11-16); a site's upstream words are no component of it
        // (K10b, 2026-11-17: the edge's records), so a fixture's are not read
        List<com.legend.executionplan.ExecutionPlan.Site> sites = new java.util.ArrayList<>();
        for (Json.Node n : fixture.getArr("sites").items()) {
            Json.Obj so = (Json.Obj) n;
            var site = new com.legend.executionplan.ExecutionPlan.Site(so.getInt("n"),
                    com.legend.sql.RaiseKind.valueOf(so.getString("kind")), so.getStringOr("where", null),
                    so.getString("message"));
            assertEquals(com.legend.executionplan.ExecutionPlan.Site.sentence(site.kind(), site.where()),
                    site.message(), "the fixture's site " + site.n() + " is ErrorKind's sentence and its where");
            sites.add(site);
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
}
