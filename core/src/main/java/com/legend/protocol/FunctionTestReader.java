// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.protocol;

import com.legend.json.Json;
import com.legend.protocol.Protocol.PTestPayload;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

/**
 * A function's {@code functionTestSuite}s read back -- the mirror of {@link ProtocolEmitter}'s
 * {@code testSuite}, {@code assertion}, {@code testPayload} and {@code relationElement} rules. An
 * unnamed suite is written {@code "default"} and reads back unnamed; a test's single assertion is
 * always {@code "default"}.
 */
final class FunctionTestReader {

    private FunctionTestReader() {
    }

    static Protocol.PTestSuite testSuite(Json.Node node) {
        Wire s = Wire.of(node, "function test suite");
        s.constant("_type", "functionTestSuite");
        String id = s.str("id");
        return s.done(new Protocol.PTestSuite("default".equals(id) ? null : id, s.span(),
                s.list("testData", FunctionTestReader::testData), s.list("tests", FunctionTestReader::test)));
    }

    /**
     * A function test's data. Older JSON names the store in {@code store} ({@code StoreProviderPointer}: a bare path,
     * a STORE, or the pointer object), which the engine reads as the pointer and writes back as
     * {@code packageableElementPointer} ({@code Function.FunctionDeserializer}); it reads the newer key first, so
     * both at once is refused.
     */
    private static Protocol.PTestData testData(Json.Node node) {
        Wire d = Wire.of(node, "function test data");
        Json.Node older = d.opt("store");
        Json.Node current = d.opt("packageableElementPointer");
        if (older != null && current != null) {
            throw Wire.refuse("function test data with both 'packageableElementPointer' and the older 'store': the"
                    + " engine reads the first and drops the other");
        }
        // neither: refused, naming the current key
        Json.Node pointer = current != null ? current : older != null ? older : d.take("packageableElementPointer");
        if (current == null && pointer instanceof Json.Str path) {
            return d.done(new Protocol.PTestData(path.value(), null, payload(d.take("data")), "STORE", d.span()));
        }
        Wire ptr = Wire.of(pointer, "store pointer");
        String path = ptr.str("path");
        SourceInfo storeSpan = ptr.span();
        String type = ptr.done(ptr.optStr("type"));
        return d.done(new Protocol.PTestData(path, storeSpan, payload(d.take("data")), type, d.span()));
    }

    private static Protocol.PFunctionTest test(Json.Node node) {
        Wire t = Wire.of(node, "function test");
        t.constant("_type", "functionTest");
        List<Json.Node> assertions = t.arr("assertions");
        if (assertions.size() != 1) {
            throw Wire.refuse("a function test with " + assertions.size() + " assertions (the grammar has one)");
        }
        List<Protocol.PTestParam> params = t.listOrEmpty("parameters", FunctionTestReader::param);
        if (t.has("parameters") && params.isEmpty()) {
            throw Wire.refuse("a function test with an empty parameters array (the wire omits it)");
        }
        return t.done(new Protocol.PFunctionTest(t.str("id"), t.span(), params, assertion(assertions.get(0))));
    }

    private static Protocol.PTestParam param(Json.Node node) {
        Wire p = Wire.of(node, "function test parameter");
        return p.done(new Protocol.PTestParam(p.optStr("name"), ProtocolReader.valueSpec(p.take("value")),
                p.span()));
    }

    private static Protocol.PAssertion assertion(Json.Node node) {
        Wire a = Wire.of(node, "function test assertion");
        String type = a.type();
        a.constant("id", "default");
        SourceInfo span = a.span();
        Json.Node expected = a.take("expected");
        Protocol.PAssertion out;
        if ("equalTo".equals(type)) {
            out = new Protocol.PAssertion.EqualTo(ProtocolReader.valueSpec(expected), span);
        } else if ("equalToJson".equals(type)) {
            if (!(payload(expected) instanceof PTestPayload.ExternalFormat ef)) {
                throw Wire.refuse("an equalToJson whose expected value is not externalFormat");
            }
            out = new Protocol.PAssertion.EqualToJson(ef, span);
        } else if ("equalToRelation".equals(type)) {
            out = new Protocol.PAssertion.EqualToRelation(relationElement(expected), span);
        } else {
            throw Wire.refuse("no reader rule for function test assertion _type '" + type + "'");
        }
        return a.done(out);
    }

    /** The reader rule for each test-data payload {@code _type}. */
    private static final Map<String, Function<Wire, PTestPayload>> PAYLOADS = Map.of(
            "externalFormat", w -> new PTestPayload.ExternalFormat(w.str("contentType"), w.str("data"), w.span()),
            "reference", FunctionTestReader::reference,
            "relationAccessor", w -> new PTestPayload.RelationElements(
                    w.list("relationElements", FunctionTestReader::relationElement), w.span()),
            "modelStore", w -> new PTestPayload.ModelStoreData(w.list("modelData", FunctionTestReader::modelEmbedded),
                    w.span()),
            "relationalCSVData", w -> new PTestPayload.RelationalCsv(w.list("tables", FunctionTestReader::csvTable),
                    w.span()));

    static PTestPayload payload(Json.Node node) {
        Wire w = Wire.of(node, "test data");
        return w.done(Wire.rule(PAYLOADS, w.type(), "test data").apply(w));
    }

    /**
     * A data-element reference: the pointer and the payload share one span; {@code DATA} is the default type. Older
     * JSON writes the bare path ({@code PackageableElementPointer}'s string creator): that DATA pointer, no span.
     */
    private static PTestPayload reference(Wire w) {
        Json.Node written = w.take("dataElement");
        if (written instanceof Json.Str bare) {
            if (w.span() != null) {
                throw Wire.refuse("a reference to " + bare.value() + " with a span and a bare path");
            }
            return new PTestPayload.Reference(bare.value(), null, null);
        }
        Wire de = Wire.of(written, "test data.dataElement");
        String path = de.str("path");
        SourceInfo inner = de.span();
        String type = de.done(de.str("type"));
        SourceInfo outer = w.span();
        if (!java.util.Objects.equals(inner, outer)) {
            throw Wire.refuse("a reference whose pointer span " + inner + " is not its own " + outer);
        }
        return new PTestPayload.Reference(path, "DATA".equals(type) ? null : type, outer);
    }

    private static PTestPayload.ModelEmbedded modelEmbedded(Json.Node node) {
        Wire m = Wire.of(node, "model embedded data");
        m.constant("_type", "modelEmbeddedData");
        if (!(payload(m.take("data")) instanceof PTestPayload.ExternalFormat ef)) {
            throw Wire.refuse("model embedded data that is not externalFormat");
        }
        return m.done(new PTestPayload.ModelEmbedded(m.str("model"), ef, m.span()));
    }

    private static PTestPayload.CsvTable csvTable(Json.Node node) {
        Wire t = Wire.of(node, "csv table");
        return t.done(new PTestPayload.CsvTable(t.str("schema"), t.str("table"), t.str("values"), t.span()));
    }

    /** The bare columns/paths/rows shape -- every cell a string. */
    static PTestPayload.RelationElement relationElement(Json.Node node) {
        Wire r = Wire.of(node, "relation element");
        List<List<String>> rows = new ArrayList<>();
        for (Json.Node row : r.arr("rows")) {
            Wire rw = Wire.of(row, "relation row");
            rows.add(rw.done(rw.strings("values")));
        }
        return r.done(new PTestPayload.RelationElement(r.strings("columns"), r.strings("paths"), rows, r.span()));
    }
}
