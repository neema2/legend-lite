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
 *   <li>{@code steps}: each {@code {"identity", "kept", "request", "exports", "operations", "reads"?, "readable"}}, in
 *       the order they are made -- each kept let's table (PlanMaker.keptLet), then the query's result. The step's
 *       {@code BatchTableRequest} in base64; {@code exports}, each table it exports, in order, with the place its id is
 *       written ({@code at}), the operation it is ({@code op}), and, for a store's table read, that table as its store
 *       declares it ({@code declared}: the check of DH16); the last is the step's result. {@code reads}: each earlier
 *       step whose table it reads ({@code step}) and the place that step's export id is written ({@code at}).
 *       {@code identity} is SHA-256 of the bytes with every place zeroed, followed by the identity of each step it reads:
 *       the name a session holds the step's table by, the same for the same table whatever reads it. {@code readable}
 *       is the dialect's JSON of the same operations, as the dialect wrote them, for a person and an error message; no
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

    /** The high byte of every read's placeholder id: a read's place is the only five-byte run {@code e, j, 0, 0,
     *  READ}, distinct from every export's. */
    private static final int READ = 0x6d;

    /**
     * The plan's text for a Deephaven query: {@code batch}, the dialect's operations, as its steps. Each kept expression
     * the dialect marked ({@code kept}: a kept let's table, PlanMaker.keptLet) is a step of its own, in order; the rest
     * is the last step, the query's result. A step reads an earlier one's table by a place of its own ({@code reads}: the
     * step, and where the runtime writes that step's export id), never by its operations; its identity is its bytes
     * with every place zeroed and each step it reads named by that step's identity -- so a kept step is the same bytes,
     * and the same identity, whatever step reads it.
     */
    public static String of(String batch) {
        Json.Obj request = Json.parseObject(batch);
        if (!request.has("ops") || !java.util.Set.of("ops", "declared", "kept").containsAll(request.fields().keySet())) {
            throw new IllegalArgumentException("a Deephaven batch is its operations, the tables they read and the"
                    + " expressions it keeps, not " + request.fields().keySet());
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
        // each operation's step: a kept range's, in order, or the last step's
        int[] stepOf = new int[ops.size()];
        List<int[]> ranges = keptRanges(request.getArrOr("kept", null), ops.size());
        java.util.Arrays.fill(stepOf, ranges.size());
        for (int k = 0; k < ranges.size(); k++) {
            for (int op = ranges.get(k)[0]; op <= ranges.get(k)[1]; op++) {
                stepOf[op] = k;
            }
        }
        // each step's operations, in order, and its result: a kept range's last, or the batch's last
        List<List<Integer>> members = new ArrayList<>();
        for (int k = 0; k <= ranges.size(); k++) {
            members.add(new ArrayList<>());
        }
        for (int op = 0; op < ops.size(); op++) {
            members.get(stepOf[op]).add(op);
        }
        if (members.get(ranges.size()).isEmpty()) {
            throw new IllegalArgumentException("a Deephaven batch whose last operation is a kept expression's: a query"
                    + " reads what it keeps");
        }
        List<Object> steps = new ArrayList<>();
        List<String> identities = new ArrayList<>();
        for (int k = 0; k <= ranges.size(); k++) {
            int result = k < ranges.size() ? ranges.get(k)[1] : ops.size() - 1;
            Map<String, Object> step = step(k, members.get(k), result, ops, declared, stepOf, ranges, identities);
            if (ranges.isEmpty()) {
                // one step: its readable form the dialect's batch as it wrote it
                step.put("readable", batch);
            }
            identities.add((String) step.get("identity"));
            steps.add(step);
        }
        Map<String, Object> plan = new LinkedHashMap<>();
        plan.put("format", FORMAT);
        plan.put("steps", steps);
        plan.put("requests", requests());
        return Json.toCompact(plan);
    }

    /** The batch's kept ranges, each {@code [first, last]}, in order, apart, inside the batch. */
    private static List<int[]> keptRanges(Json.@com.legend.base.Nullable Arr kept, int size) {
        List<int[]> out = new ArrayList<>();
        int after = 0;
        for (Json.Node n : kept == null ? List.<Json.Node>of() : kept.items()) {
            List<Json.Node> pair = ((Json.Arr) n).items();
            if (pair.size() != 2) {
                throw new IllegalArgumentException("a Deephaven batch's kept range is its first and last operation");
            }
            int first = (int) ((Json.Num) pair.get(0)).longValue();
            int last = (int) ((Json.Num) pair.get(1)).longValue();
            if (first < after || last < first || last >= size) {
                throw new IllegalArgumentException("a Deephaven batch keeps operations " + first + " to " + last
                        + ", which are not a range after the last one kept, inside its " + size + " operations");
            }
            out.add(new int[] {first, last});
            after = last + 1;
        }
        return out;
    }

    /** Step {@code k}: its operations {@code members}, renumbered from 0; a read of an earlier step's result a place of
     *  its own; its exports (each declared store read, and {@code result}); its bytes, identity and readable form. */
    private static Map<String, Object> step(int k, List<Integer> members, int result, List<Json.Obj> ops,
            Map<Integer, Json.Obj> declared, int[] stepOf, List<int[]> ranges, List<String> identities) {
        // each operation's place in this step (the step's own operations alone are read)
        int[] local = new int[ops.size()];
        java.util.Arrays.fill(local, -1);
        for (int i = 0; i < members.size(); i++) {
            local[members.get(i)] = i;
        }
        List<Integer> reads = new ArrayList<>();
        List<Json.Node> written = new ArrayList<>();
        for (int op : members) {
            written.add(referenced(ops.get(op), n -> {
                if (stepOf[n] == k) {
                    return offsetRef(local[n]);
                }
                int s = stepOf[n];
                if (s > k || n != ranges.get(s)[1]) {
                    throw new IllegalArgumentException("operation " + n + " is read outside its step, and is no kept"
                            + " step's table");
                }
                int j = reads.indexOf(s);
                if (j < 0) {
                    j = reads.size();
                    reads.add(s);
                }
                return ticketRef(READ << 24 | j);
            }));
        }
        // each export: every operation that reads a store's table (its answer checked against the declaration), and the
        // step's result -- each exported under a placeholder id of its own
        List<Integer> exported = new ArrayList<>();
        for (int op : members) {
            if (declared.containsKey(op)) {
                exported.add(op);
            }
        }
        if (!exported.contains(result)) {
            exported.add(result);
        }
        for (int i = 0; i < exported.size(); i++) {
            int at = local[exported.get(i)];
            written.set(at, DeephavenWire.exported((Json.Obj) written.get(at), placeholder(i)));
        }
        LinkedHashMap<String, Json.Node> fields = new LinkedHashMap<>();
        fields.put("ops", new Json.Arr(written));
        byte[] bytes = ProtoWire.write("BatchTableRequest", new Json.Obj(fields), DeephavenWire::refuseParameters);
        byte[] zeroed = bytes.clone();
        List<Object> exports = new ArrayList<>();
        for (int i = 0; i < exported.size(); i++) {
            int at = place(bytes, placeholder(i), "export " + i + " of step " + k);
            java.util.Arrays.fill(zeroed, at, at + 4, (byte) 0);
            Map<String, Object> e = new LinkedHashMap<>();
            e.put("at", at);
            e.put("op", local[exported.get(i)]);
            Json.Obj d = declared.get(exported.get(i));
            if (d != null) {
                LinkedHashMap<String, Json.Node> renumbered = new LinkedHashMap<>(d.fields());
                renumbered.put("op", Json.Num.ofLong(local[exported.get(i)]));
                e.put("declared", new Json.Obj(renumbered));
            }
            exports.add(e);
        }
        List<Object> readPlaces = new ArrayList<>();
        StringBuilder named = new StringBuilder();
        for (int j = 0; j < reads.size(); j++) {
            int at = place(bytes, READ << 24 | j, "read " + j + " of step " + k);
            java.util.Arrays.fill(zeroed, at, at + 4, (byte) 0);
            readPlaces.add(Map.of("step", reads.get(j), "at", at));
            named.append(identities.get(reads.get(j)));
        }
        byte[] identified = zeroed;
        if (named.length() > 0) {
            byte[] tail = named.toString().getBytes(StandardCharsets.US_ASCII);
            identified = java.util.Arrays.copyOf(zeroed, zeroed.length + tail.length);
            System.arraycopy(tail, 0, identified, zeroed.length, tail.length);
        }
        Map<String, Object> step = new LinkedHashMap<>();
        step.put("identity", com.legend.cache.Hash.of(identified).hex());
        step.put("kept", true);
        step.put("request", Base64.getEncoder().encodeToString(bytes));
        step.put("exports", exports);
        step.put("operations", members.size());
        if (!readPlaces.isEmpty()) {
            step.put("reads", readPlaces);
        }
        // its operations as the dialect wrote them, each reference as the batch numbered it (a person's and an error
        // message's: no runtime reads it)
        List<Json.Node> readable = new ArrayList<>();
        for (int op : members) {
            readable.add(ops.get(op));
        }
        LinkedHashMap<String, Json.Node> readableFields = new LinkedHashMap<>();
        readableFields.put("ops", new Json.Arr(readable));
        step.put("readable", Json.toCompact(new Json.Obj(readableFields)));
        return step;
    }

    /** {@code op} with every reference to another operation ({@code {"batchOffset": n}}) replaced by {@code to(n)}. */
    private static Json.Obj referenced(Json.Obj op, java.util.function.IntFunction<Json.Obj> to) {
        return (Json.Obj) referencedNode(op, to);
    }

    private static Json.Node referencedNode(Json.Node n, java.util.function.IntFunction<Json.Obj> to) {
        return switch (n) {
            case Json.Obj o when o.fields().size() == 1 && o.has("batchOffset") -> to.apply(o.getInt("batchOffset"));
            case Json.Obj o -> {
                LinkedHashMap<String, Json.Node> out = new LinkedHashMap<>();
                o.fields().forEach((k, v) -> out.put(k, referencedNode(v, to)));
                yield new Json.Obj(out);
            }
            case Json.Arr a -> {
                List<Json.Node> out = new ArrayList<>();
                for (Json.Node item : a.items()) {
                    out.add(referencedNode(item, to));
                }
                yield new Json.Arr(out);
            }
            default -> n;
        };
    }

    private static Json.Obj offsetRef(int op) {
        LinkedHashMap<String, Json.Node> f = new LinkedHashMap<>();
        f.put("batchOffset", Json.Num.ofLong(op));
        return new Json.Obj(f);
    }

    /** A reference to the table the session holds under export {@code id}: its ticket. */
    private static Json.Obj ticketRef(int id) {
        LinkedHashMap<String, Json.Node> f = new LinkedHashMap<>();
        f.put("ticket", DeephavenWire.ticket(DeephavenWire.exportTicket(id)));
        return new Json.Obj(f);
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
