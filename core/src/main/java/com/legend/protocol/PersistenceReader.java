// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.protocol;

import com.legend.json.Json;
import com.legend.protocol.Protocol.PPersistenceEntry;
import com.legend.protocol.Protocol.PPersistenceNode;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Persistence and persistence contexts read back -- the mirror of {@link TailEmitter}'s
 * {@code persistence}, {@code persistenceNode}, {@code persistenceTest} and {@code persistenceContext}
 * rules. The sub-DSL is one generic node walk on both sides: a node's {@code _type} is its kind through
 * the {@link TailEmitter#PERSISTENCE_TYPES} table (read in reverse for the node's slot and context), and
 * its fields are its entries, each entry's kind told by its JSON shape (a string, boolean or number is a
 * scalar; a typed object a node; a {@code path} string a pointer, a {@code path} array a path value; an
 * array of strings, of path values or of nodes a list).
 *
 * <p>What the wire does not carry reads back as the parser's absent value: annotations (never written),
 * an unspelled notifier, output targets or tests ({@code null}), an unspelled target ({@code __empty__}).
 * Defaults the emitter injects (a flat target's {@code noDeduplicationStrategy} and empty
 * {@code partitionFields}) read back as the spelled entries they print as -- the same bytes.
 */
final class PersistenceReader {

    private PersistenceReader() {
    }

    static Protocol.Element persistence(Wire w) {
        Wire nf = w.obj("notifier");
        List<PPersistenceNode> notifyees = nf.list("notifyees", n -> node(n, "notifyee", false, false));
        SourceInfo nfSpan = nf.done(nf.span());
        Protocol.PPersistenceNotifier notifier = notifyees.isEmpty() && nfSpan == null ? null
                : new Protocol.PPersistenceNotifier(notifyees, nfSpan);
        Json.Node persister = w.opt("persister");
        String service = null;
        SourceInfo serviceSpan = null;
        Json.Node sv = w.opt("service");
        if (sv != null) {
            Protocol.PPointer p = typedPointer(sv, "SERVICE");
            service = p.path();
            serviceSpan = p.sourceInformation();
        }
        // left out: none, as the engine's printer reads its null
        List<Protocol.PServiceOutputTarget> targets = w.listOrEmpty("serviceOutputTargets",
                PersistenceReader::outputTarget);
        Wire trigger = w.obj("trigger");
        String triggerKind = trigger.done(kindOf("trigger", trigger.type(), false, false));
        return new Protocol.PPersistence(w.str("package"), w.str("name"), List.of(), List.of(),
                w.optStr("documentation"), triggerKind, service, serviceSpan,
                persister == null ? null : node(persister, "persister", false, false), notifier,
                targets.isEmpty() ? null : targets, w.optList("tests", PersistenceReader::test), w.span());
    }

    private static Protocol.PPointer typedPointer(Json.Node node, String type) {
        Protocol.PPointer p = DomainReader.pointer(node);
        if (!type.equals(p.type())) {
            throw Wire.refuse("a persistence pointer typed '" + p.type() + "' where the wire writes " + type);
        }
        return p;
    }

    /** {@code ServiceOutput -> Target}: an unspelled ({@code { }}) target omits its slot. */
    private static Protocol.PServiceOutputTarget outputTarget(Json.Node node) {
        Wire t = Wire.of(node, "service output target");
        Json.Node target = t.opt("persistenceTarget");
        return t.done(new Protocol.PServiceOutputTarget(node(t.take("serviceOutput"), "serviceOutput", false, false),
                target == null ? new PPersistenceNode("__empty__", List.of(), null)
                        : node(target, "persistenceTarget", false, true), t.span()));
    }

    // ---------------------------------------------------------------------
    // The generic node walk
    // ---------------------------------------------------------------------

    /**
     * One sub-DSL node in {@code slot}: {@code gf} inside a graphFetch service output, {@code tgt} inside a
     * persistence target -- both decide the {@code _type}s and key spellings, as on the way out.
     */
    static PPersistenceNode node(Json.Node json, String slot, boolean gfIn, boolean tgt) {
        Wire w = Wire.of(json, "persistence node '" + slot + "'");
        String wireType = w.type();
        boolean gf = gfIn;
        String kind;
        com.legend.protocol.spec.ValueSpecification headPath = null;
        if (wireType == null) {
            kind = "__part__";
        } else if ("serviceOutput".equals(slot) && "graphFetchServiceOutput".equals(wireType)
                && w.json().fields().get("path") instanceof Json.Obj) {
            // a PATH-HEADED node (#/Class/prop# {...}): its head rides the "path" slot
            kind = "#path";
            gf = true;
            headPath = SpecIslandReader.pathValue(w.take("path"));
        } else {
            kind = kindOf(slot, wireType, gf, tgt);
        }
        boolean gfPartition = gf && "fieldBasedForGraphFetch".equals(wireType);
        SourceInfo span = w.span();
        List<PPersistenceEntry> entries = new ArrayList<>();
        for (Map.Entry<String, Json.Node> f : w.json().fields().entrySet()) {
            String key = f.getKey();
            if (key.equals("_type") || key.equals("sourceInformation") || (headPath != null && key.equals("path"))) {
                continue;
            }
            entries.add(entry(key, w.take(key), gf, tgt, gfPartition));
        }
        return w.done(new PPersistenceNode(kind, headPath, entries, span));
    }

    /** One entry, its kind told by its JSON shape, its key spelled back as the grammar spells it. */
    private static PPersistenceEntry entry(String wireKey, Json.Node v, boolean gf, boolean tgt,
            boolean gfPartition) {
        if (v instanceof Json.Str s) {
            return new PPersistenceEntry.Scalar(scalarKey(wireKey, tgt, gfPartition), s.value(), true);
        }
        if (v instanceof Json.Bool b) {
            return new PPersistenceEntry.Scalar(scalarKey(wireKey, tgt, gfPartition), b.value() ? "true" : "false",
                    false);
        }
        if (v instanceof Json.Num n && n.isInteger() && n.longValue() >= 0) {
            return new PPersistenceEntry.Scalar(scalarKey(wireKey, tgt, gfPartition), Long.toString(n.longValue()),
                    false);
        }
        if (v instanceof Json.Obj o) {
            if (o.has("_type")) {
                String key = nodeKey(wireKey, tgt);
                return new PPersistenceEntry.Node(key, node(v, key, gf, tgt));
            }
            if (o.fields().get("path") instanceof Json.Arr) {
                return new PPersistenceEntry.PathValue(pathKey(wireKey), SpecIslandReader.pathValue(v));
            }
            return pointer(wireKey, v);
        }
        if (v instanceof Json.Arr a) {
            return list(wireKey, a.items(), gf, tgt, gfPartition);
        }
        throw Wire.refuse("no reader rule for persistence entry '" + wireKey + "': " + Wire.abbreviate(v));
    }

    /** {@code database} wires a STORE pointer; {@code binding} a typeless one. */
    private static PPersistenceEntry pointer(String key, Json.Node v) {
        Wire p = Wire.of(v, "persistence pointer '" + key + "'");
        String path = p.str("path");
        SourceInfo span = p.span();
        String type = p.optStr("type");
        if ("binding".equals(key) ? type != null : !"STORE".equals(type)) {
            throw Wire.refuse("a persistence pointer '" + key + "' typed '" + type + "'");
        }
        return p.done(new PPersistenceEntry.Pointer(key, path, span));
    }

    private static PPersistenceEntry list(String wireKey, List<Json.Node> items, boolean gf, boolean tgt,
            boolean gfPartition) {
        if (items.isEmpty() || items.get(0) instanceof Json.Str) {
            List<String> values = new ArrayList<>();
            for (Json.Node i : items) {
                if (!(i instanceof Json.Str s)) {
                    throw Wire.refuse("a mixed persistence list '" + wireKey + "'");
                }
                values.add(s.value());
            }
            return new PPersistenceEntry.Strings(scalarKey(wireKey, tgt, gfPartition), values);
        }
        if (items.get(0) instanceof Json.Obj o && o.fields().get("path") instanceof Json.Arr) {
            return new PPersistenceEntry.PathList(wireKey, StoreReader.each(items, SpecIslandReader::pathValue));
        }
        return new PPersistenceEntry.NodeList(wireKey, StoreReader.each(items, n -> node(n, wireKey, gf, tgt)));
    }

    // ---------------------------------------------------------------------
    // Spellings, in reverse
    // ---------------------------------------------------------------------

    /** The V2 target's scalar renames and the graphFetch partition list, back to the grammar's keys. */
    private static String scalarKey(String wireKey, boolean tgt, boolean gfPartition) {
        if (gfPartition && "partitionFieldPaths".equals(wireKey)) {
            return "partitionFields";
        }
        if (!tgt) {
            return wireKey;
        }
        String k = TARGET_SCALARS.get(wireKey);
        return k == null ? wireKey : k;
    }

    /** {@code auditingDateTimeName} is the one wire key of two grammar keys: the auditing node's is read. */
    private static final Map<String, String> TARGET_SCALARS = Map.of(
            "auditingDateTimeName", "dateTimeName",
            "timeIn", "dateTimeIn",
            "timeOut", "dateTimeOut",
            "timeStart", "dateTimeStart",
            "timeEnd", "dateTimeEnd");

    private static String nodeKey(String wireKey, boolean tgt) {
        return tgt && "sourceTimeFields".equals(wireKey) ? "sourceFields" : wireKey;
    }

    private static String pathKey(String wireKey) {
        if ("deleteFieldPath".equals(wireKey)) {
            return "deleteField";
        }
        return "versionFieldPath".equals(wireKey) ? "versionField" : wireKey;
    }

    /** The kind whose {@code _type} in {@code slot} (and this context) is {@code wireType}. */
    private static String kindOf(String slot, @com.legend.base.Nullable String wireType, boolean gf, boolean tgt) {
        String found = null;
        for (String k : TailEmitter.PERSISTENCE_TYPES.keySet()) {
            int colon = k.indexOf(':');
            String s = k.substring(0, colon);
            int at = s.indexOf('@');
            if (!(at < 0 ? s : s.substring(0, at)).equals(slot)) {
                continue;
            }
            String kind = k.substring(colon + 1);
            if (wireType != null && wireType.equals(forward(slot, kind, gf, tgt))) {
                if (found != null && !found.equals(kind)) {
                    throw Wire.refuse("persistence " + slot + " _type '" + wireType + "' is two kinds: " + found
                            + ", " + kind);
                }
                found = kind;
            }
        }
        if (found == null) {
            throw Wire.refuse("no reader rule for persistence " + slot + " _type '" + wireType + "'");
        }
        return found;
    }

    /** The emitter's lookup order: the target spelling, then the graphFetch one, then the plain one. */
    private static @com.legend.base.Nullable String forward(String slot, String kind, boolean gf, boolean tgt) {
        Map<String, String> t = TailEmitter.PERSISTENCE_TYPES;
        String w = tgt ? t.get(slot + "@tgt:" + kind) : null;
        if (w == null && gf) {
            w = t.get(slot + "@gf:" + kind);
        }
        return w == null ? t.get(slot + ":" + kind) : w;
    }

    // ---------------------------------------------------------------------
    // Tests
    // ---------------------------------------------------------------------

    private static Protocol.PPersistenceTest test(Json.Node node) {
        Wire t = Wire.of(node, "persistence test");
        t.constant("_type", "test");
        Json.Node gfp = t.opt("graphFetchPath");
        // left out: no batches (the engine's printer prints no block for its null)
        List<Json.Node> batches = t.optArr("testBatches");
        List<Protocol.PPersistenceTestBatch> out = null;
        if (batches != null) {
            out = new ArrayList<>();
            for (int i = 0; i < batches.size(); i++) {
                out.add(batch(batches.get(i), i));
            }
        }
        // left out it is true, the engine's Boolean field's start; written null it is none (not printed)
        Json.Node fromOutput = t.opt("isTestDataFromServiceOutput");
        Boolean isFromOutput;
        if (fromOutput == null) {
            isFromOutput = Boolean.TRUE;
        } else if (fromOutput instanceof Json.Null) {
            isFromOutput = null;
        } else if (fromOutput instanceof Json.Bool written) {
            isFromOutput = written.value();
        } else {
            throw Wire.refuse("persistence test.isTestDataFromServiceOutput is not a boolean: "
                    + Wire.abbreviate(fromOutput));
        }
        return t.done(new Protocol.PPersistenceTest(t.str("id"), out, isFromOutput,
                gfp == null ? null : SpecIslandReader.pathValue(gfp), t.span()));
    }

    /**
     * One batch: its {@code batchId} is its index, written by the emitter. Its test data, the data's connection and
     * its assertions may each be left out, as the engine's printer reads each null.
     */
    private static Protocol.PPersistenceTestBatch batch(Json.Node node, int index) {
        Wire b = Wire.of(node, "persistence test batch");
        if (b.lng("batchId") != index) {
            throw Wire.refuse("a persistence test batch numbered " + b.lng("batchId") + " at position " + index);
        }
        PPersistenceNode connectionData = null;
        SourceInfo connectionSpan = null;
        SourceInfo dataSpan = null;
        Wire data = b.optObj("testData");
        if (data != null) {
            Wire conn = data.optObj("connection");
            if (conn != null) {
                connectionData = node(conn.take("data"), "connectionData", false, false);
                connectionSpan = conn.done(conn.span());
            }
            dataSpan = data.done(data.span());
        }
        return b.done(new Protocol.PPersistenceTestBatch(b.str("id"), connectionData, connectionSpan, dataSpan,
                b.optList("assertions", PersistenceReader::assertion), b.span(), data != null));
    }

    /** {@code id: Kind #{...}#}: the entries ride in record order between {@code _type} and {@code id}. */
    private static Protocol.PPersistenceAssert assertion(Json.Node node) {
        Wire a = Wire.of(node, "persistence assertion");
        String kind = kindOf("assertion", a.type(), false, false);
        List<PPersistenceEntry> entries = new ArrayList<>();
        for (Map.Entry<String, Json.Node> f : a.json().fields().entrySet()) {
            String key = f.getKey();
            if (key.equals("_type") || key.equals("id") || key.equals("sourceInformation")) {
                continue;
            }
            Json.Node v = a.take(key);
            if (v instanceof Json.Str s) {
                entries.add(new PPersistenceEntry.Scalar(key, s.value(), true));
            } else if (v instanceof Json.Obj) {
                entries.add(new PPersistenceEntry.Node(key, node(v, "connectionData", false, false)));
            } else {
                throw Wire.refuse("no reader rule for persistence assertion entry '" + key + "'");
            }
        }
        return a.done(new Protocol.PPersistenceAssert(a.str("id"), new PPersistenceNode(kind, entries, null),
                a.span()));
    }

    // ---------------------------------------------------------------------
    // Persistence contexts
    // ---------------------------------------------------------------------

    static Protocol.Element persistenceContext(Wire w) {
        Protocol.PPointer persistence = typedPointer(w.take("persistence"), "PERSISTENCE");
        Json.Node sink = w.opt("sinkConnection");
        return new Protocol.PPersistenceContext(w.str("package"), w.str("name"), List.of(), List.of(),
                persistence.path(), persistence.sourceInformation(), platform(w.take("platform")),
                w.list("serviceParameters", PersistenceReader::serviceParameter),
                sink == null ? null : ConnectionReader.connectionValue(sink), w.span());
    }

    /**
     * The platform, always on the wire: {@code {_type:"default"}} unspelled; a bare kind its lower-first
     * {@code _type} with the statement's span; a node with entries the generic walk.
     */
    private static @com.legend.base.Nullable PPersistenceNode platform(Json.Node node) {
        Json.Obj o = Wire.of(node, "platform").json();
        boolean bare = true;
        for (String k : o.fields().keySet()) {
            if (!k.equals("_type") && !k.equals("sourceInformation")) {
                bare = false;
            }
        }
        if (!bare) {
            return node(node, "platform", false, false);
        }
        Wire p = Wire.of(node, "platform");
        String type = p.type();
        SourceInfo span = p.span();
        if (type == null || type.isEmpty()) {
            throw Wire.refuse("a platform without its _type");
        }
        if ("default".equals(type) && span == null) {
            p.done(type);
            return null;
        }
        return p.done(new PPersistenceNode(Character.toUpperCase(type.charAt(0)) + type.substring(1), List.of(),
                span));
    }

    private static Protocol.PCtxParam serviceParameter(Json.Node node) {
        Wire p = Wire.of(node, "persistence context service parameter");
        Wire v = p.obj("value");
        String type = v.type();
        Protocol.PCtxParamValue value;
        if ("primitiveTypeValue".equals(type)) {
            value = new Protocol.PCtxParamValue.Primitive(ProtocolReader.valueSpec(v.take("primitiveType")));
        } else if ("connectionValue".equals(type)) {
            Protocol.PConnectionValue c = ConnectionReader.connectionValue(v.take("connection"));
            value = c instanceof Protocol.PConnectionPointer cp
                    ? new Protocol.PCtxParamValue.ConnectionPtr(cp.connection(), cp.sourceInformation())
                    : new Protocol.PCtxParamValue.ConnectionVal(c);
        } else {
            throw Wire.refuse("no reader rule for a context service parameter value _type '" + type + "'");
        }
        v.done(value);
        return p.done(new Protocol.PCtxParam(p.str("name"), value, p.span()));
    }
}
