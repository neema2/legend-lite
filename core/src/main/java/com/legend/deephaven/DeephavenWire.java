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
import java.util.Map;

/**
 * What a sender sends a Deephaven server, and reads back (docs/DEEPHAVEN_PLAN_2026_10_22.md DH11, DH21, DH22): a
 * plan's batch, made at sending time with what exists only then (the result's id in the sending session), the
 * request that publishes a result under a shared name, the request that releases it, and the outcome of each operation
 * the server created. The browser tab calls it through the WebAssembly planner; a server-side sender
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
        if (last.getObj(last.fields().keySet().iterator().next()).has("resultId")) {
            throw new IllegalArgumentException("a Deephaven plan names no result of its own: its sender does");
        }
        ops.set(ops.size() - 1, exported(last, exportId));
        LinkedHashMap<String, Json.Node> fields = new LinkedHashMap<>(request.fields());
        fields.put("ops", new Json.Arr(ops));
        return ProtoWire.write("BatchTableRequest", new Json.Obj(fields), DeephavenWire::refuseParameters);
    }

    /**
     * The KEY of each of a plan's operations, in the plan's order (docs/DEEPHAVEN_PLAN_2026_10_22.md DH23): a name for
     * the table the operation makes, the same in every plan that makes that table -- SHA-256 of the operation as the
     * plan writes it, with each operation it reads named by THAT one's key rather than by its place in the batch. The
     * target writes a source alike wherever it stands, so the queries a grid sends over one source share their keys up
     * to where they part, and a session that holds the table one of them made can read it rather than make it again
     * ({@link #send}). A table read by its ticket (a server's own name, {@code s/position}) is named by that ticket.
     */
    public static List<String> keys(String plan) {
        return parse(plan).keys();
    }

    /** One table a {@link #send} exports: its key, and its id in the sending session. */
    public record Export(String key, int exportId) {
    }

    /**
     * What a {@link #send} sends: its request (empty when the session holds the result already: nothing is sent),
     * the tables it exports, in order, and the id of the export that is the plan's result.
     */
    public record Send(byte[] request, List<Export> exports, int result) {
    }

    /**
     * {@code plan} as the request that makes what its session does not hold (DH23): {@code held} is the session's
     * tables by {@link #keys key}; an operation whose key it holds is not sent, and the operations that read it read
     * its export by ticket; every operation sent is exported under its own id, {@code nextExportId} up, so a later plan
     * can read it too. Only what the result needs is sent: an operation held cuts the walk there. The same table twice
     * in one plan is sent once. A plan that names a result of its own, or reads parameters, is refused by name as
     * {@link #batch} refuses it.
     */
    public static Send send(String plan, Map<String, Integer> held, int nextExportId) {
        Parsed p = parse(plan);
        int last = p.ops().size() - 1;
        Integer resultHeld = held.get(p.keys().get(last));
        if (resultHeld != null) {
            return new Send(new byte[0], List.of(), resultHeld);
        }
        // the first operation of each key, and which of them the result needs: an operation held cuts the walk
        int[] firstOf = new int[last + 1];
        Map<String, Integer> seen = new LinkedHashMap<>();
        for (int i = 0; i <= last; i++) {
            Integer earlier = seen.putIfAbsent(p.keys().get(i), i);
            firstOf[i] = earlier == null ? i : earlier;
        }
        java.util.BitSet needed = new java.util.BitSet();
        needed.set(firstOf[last]);
        for (int i = last; i >= 0; i--) {
            if (!needed.get(i)) {
                continue;
            }
            int at = i;
            ProtoWire.references("BatchTableRequest.Operation", p.ops().get(i), ref -> {
                if (ref.has("batchOffset")) {
                    int j = firstOf[offset(ref, at)];
                    if (!held.containsKey(p.keys().get(j))) {
                        needed.set(j);
                    }
                }
                return ref;
            });
        }
        // each needed operation exported, in the plan's order; its references to held tables by their tickets
        int[] sentAt = new int[last + 1];
        java.util.Arrays.fill(sentAt, -1);
        List<Export> exports = new ArrayList<>();
        List<Json.Node> ops = new ArrayList<>();
        int id = nextExportId;
        for (int i = needed.nextSetBit(0); i >= 0; i = needed.nextSetBit(i + 1)) {
            int at = i;
            Json.Obj op = ProtoWire.references("BatchTableRequest.Operation", p.ops().get(i), ref -> {
                if (!ref.has("batchOffset")) {
                    return ref;
                }
                int j = firstOf[offset(ref, at)];
                Integer exportId = held.get(p.keys().get(j));
                LinkedHashMap<String, Json.Node> to = new LinkedHashMap<>();
                if (exportId != null) {
                    to.put("ticket", ticket(exportTicket(exportId)));
                } else if (sentAt[j] >= 0) {
                    to.put("batchOffset", Json.Num.ofLong(sentAt[j]));
                } else {
                    throw new IllegalStateException("Deephaven operation " + at + " reads operation " + j
                            + ", neither held nor sent");
                }
                return new Json.Obj(to);
            });
            sentAt[i] = ops.size();
            ops.add(exported(op, id));
            exports.add(new Export(p.keys().get(i), id));
            id++;
        }
        LinkedHashMap<String, Json.Node> request = new LinkedHashMap<>();
        request.put("ops", new Json.Arr(ops));
        byte[] bytes = ProtoWire.write("BatchTableRequest", new Json.Obj(request), DeephavenWire::refuseParameters);
        return new Send(bytes, List.copyOf(exports), exports.get(exports.size() - 1).exportId());
    }

    /** The ticket Arrow Flight's {@code DoGet} reads the export {@code exportId}'s rows by (DH23): Flight's
     *  {@code Ticket} is Deephaven's, one field of bytes numbered 1, so it is written by the same row. */
    public static byte[] doGet(int exportId) {
        LinkedHashMap<String, Json.Node> fields = new LinkedHashMap<>();
        fields.put("ticket", new Json.Str(Base64.getEncoder().encodeToString(exportTicket(exportId))));
        return ProtoWire.write("Ticket", new Json.Obj(fields), b -> b);
    }

    /** What the server says of one of a session's exports on its update stream (DH23): the export, its size, and why
     *  it failed (empty while it has not). */
    public record Updated(int exportId, long size, String failure) {
    }

    /** {@code message}, one message of the session's {@code ExportedTableUpdates} stream. */
    public static Updated updated(byte[] message) {
        Json.Obj r = ProtoWire.read("ExportedTableUpdateMessage", message);
        Json.Obj export = r.getObjOr("exportId", null);
        byte[] ticket = export == null ? new byte[0] : Base64.getDecoder().decode(export.getStringOr("ticket", ""));
        if (ticket.length != 5 || ticket[0] != 'e') {
            throw new IllegalArgumentException("a Deephaven update names no export of the session's: "
                    + Arrays.toString(ticket));
        }
        int exportId = (ticket[1] & 0xFF) | (ticket[2] & 0xFF) << 8 | (ticket[3] & 0xFF) << 16 | (ticket[4] & 0xFF) << 24;
        String failure = r.getStringOr("updateFailureMessage", null);
        // a field the server left out is its type's default (proto3)
        return new Updated(exportId, r.getLongOr("size", 0), failure == null ? "" : failure);
    }

    /** A plan's operations and each one's key. */
    private record Parsed(List<Json.Obj> ops, List<String> keys) {
    }

    private static Parsed parse(String plan) {
        Json.Obj request = Json.parseObject(plan);
        if (!request.fields().keySet().equals(java.util.Set.of("ops"))) {
            throw new IllegalArgumentException("a Deephaven plan is its operations, not " + request.fields().keySet());
        }
        List<Json.Obj> ops = new ArrayList<>();
        for (Json.Node n : request.getArr("ops").items()) {
            if (!(n instanceof Json.Obj op) || op.fields().size() != 1) {
                throw new IllegalArgumentException("a Deephaven operation is one request, not " + n);
            }
            if (op.getObj(op.fields().keySet().iterator().next()).has("resultId")) {
                throw new IllegalArgumentException("a Deephaven plan names no result of its own: its sender does");
            }
            ops.add(op);
        }
        if (ops.isEmpty()) {
            throw new IllegalArgumentException("a Deephaven plan with no operations");
        }
        List<String> keys = new ArrayList<>();
        for (int i = 0; i < ops.size(); i++) {
            int at = i;
            Json.Obj named = ProtoWire.references("BatchTableRequest.Operation", ops.get(i), ref -> {
                if (!ref.has("batchOffset")) {
                    return ref;
                }
                LinkedHashMap<String, Json.Node> key = new LinkedHashMap<>();
                key.put("key", new Json.Str(keys.get(offset(ref, at))));
                return new Json.Obj(key);
            });
            keys.add(com.legend.cache.Hash.ofUtf8(Json.toCompact(named)).hex());
        }
        return new Parsed(List.copyOf(ops), List.copyOf(keys));
    }

    /** The place a reference of the operation at {@code at} reads: an earlier operation of the batch. */
    private static int offset(Json.Obj ref, int at) {
        if (!(ref.get("batchOffset") instanceof Json.Num n) || !n.isInteger()) {
            throw new IllegalArgumentException("Deephaven operation " + at + " reads by a batch offset that is not a"
                    + " number: " + ref);
        }
        long j = n.longValue();
        if (j < 0 || j >= at) {
            throw new IllegalArgumentException("Deephaven operation " + at + " reads operation " + j
                    + ", which is not before it");
        }
        return (int) j;
    }

    /** {@code op} with its result exported as {@code exportId}. */
    private static Json.Obj exported(Json.Obj op, int exportId) {
        String name = op.fields().keySet().iterator().next();
        LinkedHashMap<String, Json.Node> body = new LinkedHashMap<>(op.getObj(name).fields());
        body.put("resultId", ticket(exportTicket(exportId)));
        LinkedHashMap<String, Json.Node> wrapped = new LinkedHashMap<>();
        wrapped.put(name, new Json.Obj(body));
        return new Json.Obj(wrapped);
    }

    /** The request that publishes the export {@code exportId} under {@code name}, a shared ticket another session (the
     *  JS library's, in the tab) opens. The name lives as long as the export: releasing the export ends it, and the
     *  server refuses to release the name itself (measured on Deephaven 42.7, 2026-10-23). */
    public static byte[] publish(int exportId, String name) {
        LinkedHashMap<String, Json.Node> fields = new LinkedHashMap<>();
        fields.put("sourceId", ticket(exportTicket(exportId)));
        fields.put("resultId", ticket(sharedTicket(name)));
        return ProtoWire.write("PublishRequest", new Json.Obj(fields), b -> b);
    }

    /** The request that releases the export {@code exportId}, and with it any name it was published under. */
    public static byte[] releaseExport(int exportId) {
        LinkedHashMap<String, Json.Node> fields = new LinkedHashMap<>();
        fields.put("id", ticket(exportTicket(exportId)));
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
