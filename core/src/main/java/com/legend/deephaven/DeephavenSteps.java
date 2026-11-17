// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.deephaven;

import com.legend.json.Json;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * A DEEPHAVEN PLAN'S STEPS, AS THE SERVER READS THEM (docs/DEEPHAVEN_RUN_SIDE_2026_11_16.md §4): the planner's side of
 * the Deephaven road. The dialect writes a query's table operations as their JSON form ({@code DeephavenBatch}); this
 * writes them as the bytes the server reads -- the protocol's binary form, by the one message table
 * ({@link DeephavenMessages}) -- with the places listed where a runtime writes the numbers only its session knows: the
 * id each exported table is held under, a four-byte number in a five-byte ticket ({@link DeephavenWire#exportTicket}),
 * so filling one changes no length the encoder wrote. A runtime fills those places, sends the bytes as they are, checks
 * the answer against the declared tables (DH16) and reads the rows: it encodes nothing and plans nothing.
 *
 * <p>The document a step plan is ({@link #of}), JSON:
 * <ul>
 *   <li>{@code steps}: each {@code {"identity", "kept", "request", "exports", "operations", "readable"}} -- the step's
 *       {@code BatchTableRequest} in base64; {@code exports}, each table it exports, in order, with the place its id is
 *       written ({@code at}), the operation it is ({@code op}), and, for a store's table read, that table as its store
 *       declares it ({@code declared}: the check of DH16); the last is the step's result. {@code identity} is SHA-256
 *       of the bytes with every id place zeroed: the name a session holds the step's table by. {@code readable} is the
 *       dialect's JSON of the same operations, its text as the dialect wrote it, for a person and an error message; no
 *       runtime reads it.</li>
 *   <li>{@code requests}: the session's own requests, once a plan, each its bytes in base64 and its places:
 *       {@code release} (an export let go: {@code export}), {@code fetch} (the Flight ticket a row read names:
 *       {@code export}), {@code publish} (an export published under a shared name for the live reader: {@code export},
 *       and {@code name}, the place of {@link #NAME_LENGTH} characters the runtime writes a fresh name in).</li>
 * </ul>
 */
public final class DeephavenSteps {

    private DeephavenSteps() {
    }

    /** The format's name, the document's first key: a runtime refuses a document of another. */
    public static final String FORMAT = "legend-deephaven-steps";

    /** The characters of a published name a runtime writes ({@code [0-9a-z]}), after {@link #NAME_PREFIX}. */
    public static final int NAME_LENGTH = 24;

    /** What every published name begins with. */
    public static final String NAME_PREFIX = "legend-";

    /** The high byte of every placeholder id: an id place is the only five-byte run {@code e, k, 0, 0, PLACEHOLDER}. */
    private static final int PLACEHOLDER = 0x6c;

    /** The plan's text for a Deephaven query: {@code batch}, the dialect's operations, as one kept step. */
    public static String of(String batch) {
        Json.Obj request = Json.parseObject(batch);
        if (!request.has("ops") || !java.util.Set.of("ops", "declared").containsAll(request.fields().keySet())) {
            throw new IllegalArgumentException("a Deephaven batch is its operations and the tables they read, not "
                    + request.fields().keySet());
        }
        List<Json.Obj> ops = new ArrayList<>();
        for (Json.Node n : request.getArr("ops").items()) {
            ops.add((Json.Obj) n);
        }
        if (ops.isEmpty()) {
            throw new IllegalArgumentException("a Deephaven plan with no operations");
        }
        Map<Integer, Json.Obj> declared = new LinkedHashMap<>();
        Json.Arr tables = request.getArrOr("declared", null);
        for (Json.Node n : tables == null ? List.<Json.Node>of() : tables.items()) {
            Json.Obj d = (Json.Obj) n;
            int op = d.getInt("op");
            if (op < 0 || op >= ops.size() || declared.put(op, d) != null) {
                throw new IllegalArgumentException("a Deephaven batch declares the table of operation " + op
                        + ", which it has not, or twice");
            }
        }
        // each export: every operation that reads a store's table (its answer checked against the declaration), and the
        // last, the step's result -- each exported under a placeholder id of its own
        List<Integer> exported = new ArrayList<>(declared.keySet());
        int last = ops.size() - 1;
        if (!exported.contains(last)) {
            exported.add(last);
        }
        List<Json.Node> written = new ArrayList<>(ops);
        for (int i = 0; i < exported.size(); i++) {
            written.set(exported.get(i), DeephavenWire.exported(ops.get(exported.get(i)), placeholder(i)));
        }
        LinkedHashMap<String, Json.Node> fields = new LinkedHashMap<>();
        fields.put("ops", new Json.Arr(written));
        byte[] bytes = ProtoWire.write("BatchTableRequest", new Json.Obj(fields), DeephavenWire::refuseParameters);
        byte[] zeroed = bytes.clone();
        List<Object> exports = new ArrayList<>();
        for (int i = 0; i < exported.size(); i++) {
            int at = place(bytes, placeholder(i), "export " + i + " of the step");
            java.util.Arrays.fill(zeroed, at, at + 4, (byte) 0);
            Map<String, Object> e = new LinkedHashMap<>();
            e.put("at", at);
            e.put("op", exported.get(i));
            Json.Obj d = declared.get(exported.get(i));
            if (d != null) {
                e.put("declared", d);
            }
            exports.add(e);
        }
        Map<String, Object> step = new LinkedHashMap<>();
        step.put("identity", com.legend.cache.Hash.of(zeroed).hex());
        step.put("kept", true);
        step.put("request", Base64.getEncoder().encodeToString(bytes));
        step.put("exports", exports);
        step.put("operations", ops.size());
        step.put("readable", batch);
        Map<String, Object> plan = new LinkedHashMap<>();
        plan.put("format", FORMAT);
        plan.put("steps", List.of(step));
        plan.put("requests", requests());
        return Json.toCompact(plan);
    }

    /** The session's own requests, each its bytes and the places a runtime writes in. */
    private static Map<String, Object> requests() {
        Map<String, Object> out = new LinkedHashMap<>();
        byte[] release = DeephavenWire.releaseExport(placeholder(0));
        out.put("release", Map.of("request", Base64.getEncoder().encodeToString(release),
                "export", place(release, placeholder(0), "the release's export")));
        byte[] fetch = DeephavenWire.doGet(placeholder(0));
        out.put("fetch", Map.of("ticket", Base64.getEncoder().encodeToString(fetch),
                "export", place(fetch, placeholder(0), "the fetch's export")));
        String name = NAME_PREFIX + "z".repeat(NAME_LENGTH);
        byte[] publish = DeephavenWire.publish(placeholder(0), name);
        Map<String, Object> p = new LinkedHashMap<>();
        p.put("request", Base64.getEncoder().encodeToString(publish));
        p.put("export", place(publish, placeholder(0), "the publish's export"));
        p.put("name", nameAt(publish, "z".repeat(NAME_LENGTH)));
        out.put("publish", p);
        return out;
    }

    /** The placeholder id of the {@code i}-th id place: positive, its high byte {@link #PLACEHOLDER}. */
    private static int placeholder(int i) {
        if (i < 0 || i > 0xffff) {
            throw new IllegalArgumentException("a Deephaven step exports at most 65536 tables, not " + (i + 1));
        }
        return PLACEHOLDER << 24 | i;
    }

    /** Where {@code id}'s four bytes stand in {@code bytes}, after its ticket's {@code e}: found exactly once, or
     *  refused by name (a place that could be mistaken is no place). */
    private static int place(byte[] bytes, int id, String what) {
        byte[] ticket = DeephavenWire.exportTicket(id);
        int found = -1;
        for (int i = 0; i + ticket.length <= bytes.length; i++) {
            if (java.util.Arrays.equals(bytes, i, i + ticket.length, ticket, 0, ticket.length)) {
                if (found >= 0) {
                    throw new IllegalStateException(what + "'s place is not one: its ticket stands twice in the bytes");
                }
                found = i + 1;
            }
        }
        if (found < 0) {
            throw new IllegalStateException(what + "'s ticket is not in the bytes written");
        }
        return found;
    }

    /** Where the published name's placeholder characters stand, found exactly once. */
    private static int nameAt(byte[] bytes, String placeholder) {
        byte[] p = placeholder.getBytes(StandardCharsets.US_ASCII);
        int found = -1;
        for (int i = 0; i + p.length <= bytes.length; i++) {
            if (java.util.Arrays.equals(bytes, i, i + p.length, p, 0, p.length)) {
                if (found >= 0) {
                    throw new IllegalStateException("the publish's name place is not one");
                }
                found = i;
            }
        }
        if (found < 0) {
            throw new IllegalStateException("the publish's name is not in the bytes written");
        }
        return found;
    }
}
