// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.deephaven;

import com.legend.json.Json;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.List;

/**
 * What a sender sends a Deephaven server, and reads back (docs/DEEPHAVEN_PLAN_2026_10_22.md DH11, DH21, DH22): a
 * plan's batch, made at sending time with what exists only then (the result's id in the sending session), the
 * request that publishes a result under a shared name, the request that releases a ticket, and the outcome of each
 * operation the server created. The browser tab calls it through the WebAssembly planner; a server-side sender
 * (L3s) the same way, so there is one encoder. It sends nothing: how the bytes travel is the sender's.
 */
public final class DeephavenWire {

    private DeephavenWire() {
    }

    /** The ticket bytes a plan's parameters table is written as (the dialect's own marker): a sender uploads the
     *  values and replaces it, which no sender does yet. */
    private static final byte[] PARAMETERS = "legend/parameters".getBytes(StandardCharsets.UTF_8);

    /** {@code plan}, a Deephaven plan's JSON (its batch of operations), as the request that runs it, its last
     *  operation's result exported as {@code exportId} in the sending session. A plan that already exports a result,
     *  or reads parameters (whose upload is not built yet), is refused by name. */
    public static byte[] batch(String plan, int exportId) {
        Json.Obj request = Json.parseObject(plan);
        List<Json.Node> ops = new ArrayList<>(request.getArr("ops").items());
        if (ops.isEmpty()) {
            throw new IllegalArgumentException("a Deephaven plan with no operations");
        }
        Json.Obj last = (Json.Obj) ops.get(ops.size() - 1);
        if (last.fields().size() != 1) {
            throw new IllegalArgumentException("a Deephaven operation is one request, not " + last.fields().keySet());
        }
        String op = last.fields().keySet().iterator().next();
        Json.Obj body = last.getObj(op);
        if (body.has("resultId")) {
            throw new IllegalArgumentException("a Deephaven plan names no result of its own: its sender does");
        }
        LinkedHashMap<String, Json.Node> exported = new LinkedHashMap<>(body.fields());
        exported.put("resultId", ticket(exportTicket(exportId)));
        LinkedHashMap<String, Json.Node> wrapped = new LinkedHashMap<>();
        wrapped.put(op, new Json.Obj(exported));
        ops.set(ops.size() - 1, new Json.Obj(wrapped));
        LinkedHashMap<String, Json.Node> fields = new LinkedHashMap<>(request.fields());
        fields.put("ops", new Json.Arr(ops));
        return ProtoWire.write("BatchTableRequest", new Json.Obj(fields), DeephavenWire::refuseParameters);
    }

    /** The request that publishes the export {@code exportId} under {@code name}, a shared ticket another session (the
     *  JS library's, in the tab) opens. */
    public static byte[] publish(int exportId, String name) {
        LinkedHashMap<String, Json.Node> fields = new LinkedHashMap<>();
        fields.put("sourceId", ticket(exportTicket(exportId)));
        fields.put("resultId", ticket(sharedTicket(name)));
        return ProtoWire.write("PublishRequest", new Json.Obj(fields), b -> b);
    }

    /** The request that releases the export {@code exportId}. */
    public static byte[] releaseExport(int exportId) {
        return release(exportTicket(exportId));
    }

    /** The request that releases the shared ticket {@code name}. */
    public static byte[] releaseShared(String name) {
        return release(sharedTicket(name));
    }

    private static byte[] release(byte[] ticket) {
        LinkedHashMap<String, Json.Node> fields = new LinkedHashMap<>();
        fields.put("id", ticket(ticket));
        return ProtoWire.write("ReleaseRequest", new Json.Obj(fields), b -> b);
    }

    /** What the server answered for one operation of a batch: whether it created the table, and if not, why. */
    public record Created(boolean success, String errorInfo) {
    }

    /** {@code response}, one message of a batch's answer. */
    public static Created created(byte[] response) {
        Json.Obj r = ProtoWire.read("ExportedTableCreationResponse", response);
        // a field the server left out is its type's default (protobuf's own rule, proto3 having no presence)
        String error = r.getStringOr("errorInfo", null);
        return new Created(r.getBoolOr("success", false), error == null ? "" : error);
    }

    /** An export's ticket: {@code e} and the id, a 32-bit little-endian number the sending session chose. */
    static byte[] exportTicket(int exportId) {
        if (exportId <= 0) {
            throw new IllegalArgumentException("a Deephaven export id is positive, not " + exportId);
        }
        return new byte[] {'e', (byte) exportId, (byte) (exportId >>> 8), (byte) (exportId >>> 16),
                (byte) (exportId >>> 24)};
    }

    /** A shared ticket: {@code h/} and the name. */
    static byte[] sharedTicket(String name) {
        if (!name.matches("[A-Za-z0-9_-]{1,64}")) {
            throw new IllegalArgumentException("a shared Deephaven name is 1 to 64 letters, digits, _ and -, not '"
                    + name + "'");
        }
        return ("h/" + name).getBytes(StandardCharsets.UTF_8);
    }

    private static Json.Obj ticket(byte[] bytes) {
        LinkedHashMap<String, Json.Node> t = new LinkedHashMap<>();
        t.put("ticket", new Json.Str(Base64.getEncoder().encodeToString(bytes)));
        return new Json.Obj(t);
    }

    private static byte[] refuseParameters(byte[] ticket) {
        if (Arrays.equals(ticket, PARAMETERS)) {
            throw new UnsupportedOperationException("a Deephaven plan with parameters is not sent yet: its values"
                    + " are uploaded as a one-row table first, which no sender builds yet"
                    + " (docs/DEEPHAVEN_PLAN_2026_10_22.md DH5)");
        }
        return ticket;
    }
}
