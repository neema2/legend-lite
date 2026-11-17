// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.deephaven;

import com.legend.json.Json;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** A Deephaven plan's steps (docs/DEEPHAVEN_RUN_SIDE_2026_11_16.md §4): what a runtime sends is the planner's bytes, only
 *  the listed places filled. */
class DeephavenStepsTest {

    /** A read of a store's table, a filter and a sort: the read declared (DH16), the sort the result. */
    private static final String BATCH = "{\"ops\": ["
            + "{\"view\": {\"sourceId\": {\"ticket\": {\"ticket\": \"cy90cmFkZXM=\"}}, \"columnSpecs\": [\"t0__id = id\","
            + " \"t0__desk = desk\"]}}, "
            + "{\"unstructuredFilter\": {\"sourceId\": {\"batchOffset\": 0}, \"filters\": [\"t0__desk == `FX`\"]}}, "
            + "{\"sort\": {\"sourceId\": {\"batchOffset\": 1}, \"sorts\": [{\"columnName\": \"t0__id\", \"direction\":"
            + " \"ASCENDING\"}]}}], "
            + "\"declared\": [{\"op\": 0, \"table\": \"trades\", \"columns\": [{\"name\": \"t0__id\", \"column\": \"id\","
            + " \"type\": \"int\", \"declared\": \"INTEGER\"}, {\"name\": \"t0__desk\", \"column\": \"desk\", \"type\":"
            + " \"java.lang.String\", \"declared\": \"VARCHAR(16)\"}]}]}";

    /** The dialect's batch a Deephaven plan's one step was written from (its readable form). */
    static String readable(String plan) {
        return step(plan).getString("readable");
    }

    private static Json.Obj step(String plan) {
        Json.Obj doc = Json.parseObject(plan);
        assertEquals(DeephavenSteps.FORMAT, doc.getString("format"));
        List<Json.Node> steps = doc.getArr("steps").items();
        assertEquals(1, steps.size());
        return (Json.Obj) steps.get(0);
    }

    /** {@code bytes} with each place of {@code at} filled with the id beside it, little-endian, as a runtime fills it. */
    private static byte[] filled(byte[] bytes, int[] at, int[] ids) {
        byte[] out = bytes.clone();
        for (int i = 0; i < at.length; i++) {
            for (int b = 0; b < 4; b++) {
                out[at[i] + b] = (byte) (ids[i] >>> (8 * b));
            }
        }
        return out;
    }

    @Test
    void aStepFilledWithItsIdsIsTheBatchEncodedWithThoseIds() {
        Json.Obj step = step(DeephavenSteps.of(BATCH));
        List<Json.Node> exports = step.getArr("exports").items();
        // the store's read (checked against its declaration) and the result, in that order
        assertEquals(2, exports.size());
        Json.Obj read = (Json.Obj) exports.get(0);
        Json.Obj result = (Json.Obj) exports.get(1);
        assertEquals(0, read.getInt("op"));
        assertEquals("trades", read.getObj("declared").getString("table"));
        assertEquals(2, result.getInt("op"));
        byte[] bytes = Base64.getDecoder().decode(step.getString("request"));
        byte[] sent = filled(bytes, new int[] {read.getInt("at"), result.getInt("at")}, new int[] {7, 8});
        // the same operations encoded directly, the read exported as 7 and the result as 8
        Json.Obj batch = Json.parseObject(BATCH);
        List<Json.Node> ops = new ArrayList<>(batch.getArr("ops").items());
        ops.set(0, DeephavenWire.exported((Json.Obj) ops.get(0), 7));
        ops.set(2, DeephavenWire.exported((Json.Obj) ops.get(2), 8));
        LinkedHashMap<String, Json.Node> fields = new LinkedHashMap<>();
        fields.put("ops", new Json.Arr(ops));
        assertArrayEquals(ProtoWire.write("BatchTableRequest", new Json.Obj(fields), DeephavenWire::refuseParameters),
                sent);
        // the readable form is the dialect's batch, as it wrote it
        assertEquals(BATCH, step.getString("readable"));
    }

    @Test
    void theStepsIdentityIsItsBytesWithTheirPlacesZeroed_theSameForTheSameBatch() {
        Json.Obj a = step(DeephavenSteps.of(BATCH));
        Json.Obj b = step(DeephavenSteps.of(BATCH));
        assertEquals(a.getString("identity"), b.getString("identity"));
        assertEquals(64, a.getString("identity").length());
        Json.Obj other = step(DeephavenSteps.of(BATCH.replace("`FX`", "`EQ`")));
        assertNotEquals(a.getString("identity"), other.getString("identity"));
        assertTrue(a.getBool("kept"));
    }

    @Test
    void theSessionsRequestsFilledAreTheWiresOwn() {
        Json.Obj requests = Json.parseObject(DeephavenSteps.of(BATCH)).getObj("requests");
        Json.Obj release = requests.getObj("release");
        assertArrayEquals(DeephavenWire.releaseExport(42), filled(Base64.getDecoder().decode(release.getString("request")),
                new int[] {release.getInt("export")}, new int[] {42}));
        Json.Obj fetch = requests.getObj("fetch");
        assertArrayEquals(DeephavenWire.doGet(42), filled(Base64.getDecoder().decode(fetch.getString("ticket")),
                new int[] {fetch.getInt("export")}, new int[] {42}));
        Json.Obj publish = requests.getObj("publish");
        byte[] p = filled(Base64.getDecoder().decode(publish.getString("request")), new int[] {publish.getInt("export")},
                new int[] {42});
        String name = "0123456789abcdefghijklmn";
        assertEquals(DeephavenSteps.NAME_LENGTH, name.length());
        System.arraycopy(name.getBytes(java.nio.charset.StandardCharsets.US_ASCII), 0, p, publish.getInt("name"),
                name.length());
        assertArrayEquals(DeephavenWire.publish(42, DeephavenSteps.NAME_PREFIX + name), p);
    }

    @Test
    void aBatchThatIsNotOneIsRefusedByName() {
        assertThrows(IllegalArgumentException.class, () -> DeephavenSteps.of("{\"ops\": []}"));
        assertThrows(IllegalArgumentException.class, () -> DeephavenSteps.of("{\"ops\": [], \"other\": 1}"));
    }
}
