package com.legend.server;

import com.legend.queries.FileRecords;
import com.legend.queries.SavedQueries;

import com.legend.json.Json;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The query store's rules, as legend-engine's {@code QueryStoreManager} and versioned DAO keep
 * them (ported from the engine's source; its Mongo is not runnable here).
 */
class SavedQueriesTest {

    @TempDir
    Path dir;

    private final AtomicLong clock = new AtomicLong(1_000);

    private SavedQueries.Answer call(String method, String rest, String query, String body, String user) {
        clock.addAndGet(10);
        // legend-engine's query API, its refusals in its words as the edge mounts it (D1.8, W8 of
        // docs/records/d-plan-2026-11-16.md: the product's own path speaks lite's, theProductsPathSpeaksLitesWords)
        return SavedQueries.answer(new SavedQueries(new FileRecords(dir), clock::get), method, rest, query, body, user,
                com.legend.edge.EdgeServer::storeWords);
    }

    @Test
    void theProductsPathSpeaksLitesWords() {
        SavedQueries.Answer gone = SavedQueries.answer(new SavedQueries(new FileRecords(dir), clock::get), "GET", "/q9",
                null, "", "a");
        assertEquals(404, gone.status());
        assertEquals("no saved query has the ID 'q9'", obj(gone).getString("message"));
        // J9 of the edge audit (2026-11-17): every refusal a kind, lite's sentence on the product's path (legend-engine's
        // the edge's, StoreWords, which the rest of this class reads)
        java.util.function.BiFunction<String, String, SavedQueries.Answer> lite = (method, body) -> SavedQueries.answer(
                new SavedQueries(new FileRecords(dir), clock::get), method, "", null, body, "a");
        SavedQueries.Answer noName = lite.apply("POST", query("q2", ""));
        assertEquals(400, noName.status());
        assertEquals("field_missing", obj(noName).getString("kind"));
        assertEquals("the name is missing or empty", obj(noName).getString("message"));
        SavedQueries.Answer malformed = lite.apply("POST", "[1]");
        // a body the store cannot read: its own kind (PR #118 fix round, 2026-11-18; lite's route answers it 400 request)
        assertEquals("unreadable", obj(malformed).getString("kind"));
        assertTrue(obj(malformed).getString("message").startsWith("the request could not be read: "),
                obj(malformed).getString("message"));
        SavedQueries.Answer unconfigured = SavedQueries.answer(null, "GET", "/q1", null, "", "a");
        assertEquals(500, unconfigured.status());
        assertEquals("{\"kind\":\"store_not_configured\",\"message\":\"the query store is not configured: start the"
                + " server with --query-store DIR\"}", unconfigured.json());
    }

    private static String query(String id, String name) {
        return "{\"id\":\"" + id + "\",\"name\":\"" + name + "\",\"groupId\":\"demo\",\"artifactId\":\"trading\","
                + "\"versionId\":\"0.0.0\",\"content\":\"|demo::trading::Trade.all()\","
                + "\"executionContext\":{\"_type\":\"explicitExecutionContext\",\"mapping\":\"demo::trading::TradingMapping\","
                + "\"runtime\":\"demo::trading::H2Runtime\"},"
                + "\"taggedValues\":[{\"tag\":{\"profile\":\"meta::pure::profiles::query\",\"value\":\"class\"},"
                + "\"value\":\"demo::trading::Trade\"}]}";
    }

    private static Json.Obj obj(SavedQueries.Answer a) {
        return Json.parseObject(a.json());
    }

    @Test
    void createThenGet_ownerVersionAndTimes_andGetMarksItOpened() {
        SavedQueries.Answer created = call("POST", "", null, query("q1", "Trades"), "alice");
        assertEquals(200, created.status(), created.json());
        Json.Obj c = obj(created);
        assertEquals("alice", c.getString("owner"));
        assertEquals(1, c.getInt("version"));
        long createdAt = c.getLong("createdAt");

        Json.Obj got = obj(call("GET", "/q1", null, "", "bob"));
        assertEquals("Trades", got.getString("name"));
        assertEquals(createdAt, got.getLong("createdAt"));
        assertTrue(got.getLong("lastOpenAt") > createdAt);
        assertEquals(1, got.getInt("version"), "opening is not a new version");
        // every field of the engine's Query, in its order
        assertEquals(List.of("id", "name", "description", "groupId", "artifactId", "versionId", "originalVersionId",
                "executionContext", "content", "lastUpdatedAt", "createdAt", "lastOpenAt", "deletedAt", "validUntil",
                "version", "taggedValues", "stereotypes", "defaultParameterValues", "owner", "gridConfig"),
                List.copyOf(got.fields().keySet()));
    }

    @Test
    void theEnginesValidation_andAnIdCreatedOnce() {
        assertEquals(400, call("POST", "", null, query("q1", "Trades"), "a").status() == 200
                ? call("POST", "", null, query("q1", "Again"), "a").status() : -1);
        SavedQueries.Answer noName = call("POST", "", null, query("q2", ""), "a");
        assertEquals(400, noName.status());
        assertEquals("Query name is missing or empty", obj(noName).getString("message"));
        SavedQueries.Answer badArtifact = call("POST", "", null, query("q3", "x").replace("\"trading\"", "\"Trading\""), "a");
        assertEquals(400, badArtifact.status());
        assertEquals("Query project artifact ID is invalid", obj(badArtifact).getString("message"));
    }

    @Test
    void onlyTheOwnerUpdates_eachUpdateIsAVersion_theOldOneHistory() {
        call("POST", "", null, query("q1", "Trades"), "alice");
        SavedQueries.Answer byBob = call("PUT", "/q1", null, query("q1", "Mine now"), "bob");
        assertEquals(403, byBob.status());
        assertEquals("Only owner can update the query", obj(byBob).getString("message"));

        Json.Obj v2 = obj(call("PUT", "/q1", null, query("q1", "Trades v2"), "alice"));
        assertEquals(2, v2.getInt("version"));
        assertEquals("alice", v2.getString("owner"));
        Json.Arr history = (Json.Arr) Json.parse(call("GET", "/q1/history", null, "", "alice").json());
        assertEquals(1, history.items().size());
        assertEquals("Trades", ((Json.Obj) history.items().get(0)).getString("name"));
        Json.Arr one = (Json.Arr) Json.parse(call("GET", "/q1/history", "version=2", "", "alice").json());
        assertEquals("Trades v2", ((Json.Obj) one.items().get(0)).getString("name"));
        assertEquals(404, call("GET", "/q1/history", "version=9", "", "alice").status());

        SavedQueries.Answer renamedId = call("PUT", "/q1", null, query("other", "x"), "alice");
        assertEquals("Updating query ID is not supported", obj(renamedId).getString("message"));
    }

    @Test
    void patchSetsOnlyWhatItCarries() {
        call("POST", "", null, query("q1", "Trades"), "alice");
        Json.Obj patched = obj(call("PUT", "/q1/patchQuery", null, "{\"versionId\":\"1.2.3\"}", "alice"));
        assertEquals("1.2.3", patched.getString("versionId"));
        assertEquals("Trades", patched.getString("name"));
        assertEquals(2, patched.getInt("version"));
    }

    @Test
    void deleteIsTheOwnersAndKeepsTheHistory() {
        call("POST", "", null, query("q1", "Trades"), "alice");
        assertEquals(403, call("DELETE", "/q1", null, "", "bob").status());
        assertEquals(204, call("DELETE", "/q1", null, "", "alice").status());
        SavedQueries.Answer gone = call("GET", "/q1", null, "", "alice");
        assertEquals(404, gone.status());
        assertEquals("Can't find query with ID 'q1'", obj(gone).getString("message"));
        assertEquals(1, ((Json.Arr) Json.parse(call("GET", "/q1/history", null, "", "alice").json())).items().size());
    }

    @Test
    void search_byNameOrId_mineOnly_taggedValues_sortAndLimit_withoutContent() {
        call("POST", "", null, query("q1", "Equity trades"), "alice");
        call("POST", "", null, query("q2", "Rates trades"), "bob");
        call("POST", "", null, query("q3", "Firms").replace("demo::trading::Trade\"}", "demo::trading::Firm\"}"), "alice");

        Json.Arr byName = (Json.Arr) Json.parse(call("POST", "/search", null,
                "{\"searchTermSpecification\":{\"searchTerm\":\"TRADES\"}}", "bob").json());
        assertEquals(2, byName.items().size());
        assertEquals("q2", ((Json.Obj) byName.items().get(0)).getString("id"), "the caller's own first");
        assertTrue(((Json.Obj) byName.items().get(0)).get("content") instanceof Json.Null, "a search carries no content");

        Json.Arr mine = (Json.Arr) Json.parse(call("POST", "/search", null,
                "{\"showCurrentUserQueriesOnly\":true}", "alice").json());
        assertEquals(2, mine.items().size());

        Json.Arr tagged = (Json.Arr) Json.parse(call("POST", "/search", null,
                "{\"taggedValues\":[{\"tag\":{\"profile\":\"meta::pure::profiles::query\",\"value\":\"class\"},"
                        + "\"value\":\"demo::trading::Firm\"}]}", "alice").json());
        assertEquals(1, tagged.items().size());
        assertEquals("q3", ((Json.Obj) tagged.items().get(0)).getString("id"));

        Json.Arr newest = (Json.Arr) Json.parse(call("POST", "/search", null,
                "{\"sortByOption\":\"SORT_BY_CREATE\",\"limit\":1}", "carol").json());
        assertEquals("q3", ((Json.Obj) newest.items().get(0)).getString("id"));
        assertEquals(400, call("POST", "/search", null, "{\"limit\":0}", "carol").status());
    }

    @Test
    void batch_answersEachOrNamesTheMissing() {
        call("POST", "", null, query("q1", "One"), "a");
        call("POST", "", null, query("q2", "Two"), "a");
        assertEquals(2, ((Json.Arr) Json.parse(call("GET", "/batch", "queryIds=q1&queryIds=q2", "", "a").json())).items().size());
        SavedQueries.Answer missing = call("GET", "/batch", "queryIds=q1&queryIds=nope", "", "a");
        assertEquals(500, missing.status());
        assertEquals("Can't find queries for the following ID(s):\nnope", obj(missing).getString("message"));
    }

    @Test
    void withoutAStoreEveryCallIsRefused_andTheCubeStoreIsNotServed() {
        SavedQueries.Answer a = SavedQueries.answer(null, "GET", "/q1", null, "", "a", com.legend.edge.EdgeServer::storeWords);
        assertEquals(500, a.status());
        assertEquals("Query store has not been configured properly (legend-lite: start the server with --query-store"
                + " DIR)", obj(a).getString("message"));
        assertEquals(404, call("GET", "/dataCube/x", null, "", "a").status());
    }
}
