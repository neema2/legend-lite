// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.protocol;

import com.legend.json.Json;
import com.legend.protocol.spec.AppliedFunction;
import com.legend.protocol.spec.EnumValue;
import com.legend.protocol.spec.NewInstance;
import com.legend.protocol.spec.PackageableElementPtr;
import com.legend.protocol.spec.PureCollection;
import com.legend.protocol.spec.ValueSpecification;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

/**
 * Embedded data read back -- the mirror of {@link MappingEmitter#embeddedDataValue} (external format,
 * model store, relation accessor, relational CSV, data reference, service-store stubs) and of
 * {@link ProtocolEmitter}'s data-walker rules for ModelStore instances, and the {@code ###Data} element
 * built on them ({@code dataElement}, its resolvers).
 */
final class EmbeddedDataReader {

    private EmbeddedDataReader() {
    }

    /** {@code _type:"dataElement"}: an optional value and optional resolvers (written only when non-empty). */
    static Protocol.Element dataElement(Wire w) {
        Json.Node data = w.opt("data");
        List<Protocol.PDataResolver> resolvers = StoreReader.nonEmpty(w, "dataResolvers",
                EmbeddedDataReader::resolver);
        return new Protocol.PDataElement(w.str("package"), w.str("name"),
                new Protocol.PDataBody(data == null ? null : value(data), resolvers),
                w.list("stereotypes", DomainReader::stereotype), w.list("taggedValues", DomainReader::taggedValue),
                w.span());
    }

    /** {@code store::S: <value>;} is a {@code baseDataResolver}; a bare {@code fqn;} a {@code referenceDataResolver}. */
    private static Protocol.PDataResolver resolver(Json.Node node) {
        Wire r = Wire.of(node, "data resolver");
        String type = r.type();
        Protocol.PEmbeddedDataValue data;
        if ("baseDataResolver".equals(type)) {
            data = value(r.take("data"));
        } else if ("referenceDataResolver".equals(type)) {
            data = null;
        } else {
            throw Wire.refuse("no reader rule for data resolver _type '" + type + "'");
        }
        return r.done(new Protocol.PDataResolver(elementRef(r.take("elementPointer")), data, r.span()));
    }

    /** A typeless element pointer: path and span only. */
    static Protocol.PElementRef elementRef(Json.Node node) {
        Wire p = Wire.of(node, "element pointer");
        return p.done(new Protocol.PElementRef(p.str("path"), p.span()));
    }

    /** The reader rule for each embedded-data {@code _type}. */
    private static final Map<String, Function<Wire, Protocol.PEmbeddedDataValue>> VALUES = Map.of(
            "externalFormat", EmbeddedDataReader::externalFormatBody,
            "modelStore", w -> new Protocol.PModelStoreData(w.list("modelData", EmbeddedDataReader::modelData),
                    w.span()),
            "relationAccessor", w -> new Protocol.PRelationData(
                    w.list("relationElements", EmbeddedDataReader::relationElement), w.span()),
            "relationalCSVData", w -> new Protocol.PRelationalCsvData(w.list("tables", EmbeddedDataReader::csvTable),
                    w.span()),
            "reference", w -> new Protocol.PDataReference(DomainReader.pointer(w.take("dataElement")), w.span()),
            "serviceStore", w -> new Protocol.PServiceStoreData(
                    w.list("serviceStubMappings", EmbeddedDataReader::stub), w.span()));

    static Protocol.PEmbeddedDataValue value(Json.Node node) {
        Wire w = Wire.of(node, "embedded data");
        return w.done(Wire.rule(VALUES, w.type(), "embedded data").apply(w));
    }

    /** {@code ExternalFormat #{ contentType; data; }#} -- also an assertion's expected value. */
    static Protocol.PExternalFormatData externalFormat(Json.Node node) {
        Wire w = Wire.of(node, "external format data");
        w.constant("_type", "externalFormat");
        return w.done(externalFormatBody(w));
    }

    private static Protocol.PExternalFormatData externalFormatBody(Wire w) {
        return new Protocol.PExternalFormatData(w.str("contentType"), w.str("data"), w.span());
    }

    /** One ModelStore entry: an embedded value, or a {@code [ ^X(...) ]} instance collection. */
    private static Protocol.PModelData modelData(Json.Node node) {
        Wire m = Wire.of(node, "model data");
        String type = m.type();
        if ("modelEmbeddedData".equals(type)) {
            return m.done(new Protocol.PModelEmbeddedData(m.str("model"), value(m.take("data")), m.span()));
        }
        if ("modelInstanceData".equals(type)) {
            return m.done(new Protocol.PModelInstanceData(m.str("model"), instances(m.take("instances")), m.span()));
        }
        throw Wire.refuse("no reader rule for model data _type '" + type + "'");
    }

    private static Protocol.PRelationalCsvTable csvTable(Json.Node node) {
        Wire t = Wire.of(node, "relational CSV table");
        return t.done(new Protocol.PRelationalCsvTable(t.str("schema"), t.str("table"), t.str("values"), t.span()));
    }

    /** The bare columns/paths/rows shape -- every cell a string. */
    static Protocol.PRelationElement relationElement(Json.Node node) {
        Protocol.PTestPayload.RelationElement r = FunctionTestReader.relationElement(node);
        return new Protocol.PRelationElement(r.columns(), r.paths(), r.rows(), r.sourceInformation());
    }

    /**
     * One named test assertion (mapping, service and data-quality tests alike): {@code equalToJson} over
     * external format, {@code equalToRelation} over a relation element, {@code equalTo} over a value.
     */
    static Protocol.PTestAssertion assertion(Json.Node node) {
        Wire a = Wire.of(node, "test assertion");
        String type = a.type();
        Json.Node expected = a.take("expected");
        Protocol.PAssertionValue value;
        if ("equalToJson".equals(type)) {
            value = externalFormat(expected);
        } else if ("equalToRelation".equals(type)) {
            value = relationElement(expected);
        } else if ("equalTo".equals(type)) {
            value = new Protocol.PEqualToValue(ProtocolReader.valueSpec(expected));
        } else {
            throw Wire.refuse("no reader rule for test assertion _type '" + type + "'");
        }
        return a.done(new Protocol.PTestAssertion(a.str("id"), value, a.span()));
    }

    // ---------------------------------------------------------------------
    // Service-store stubs
    // ---------------------------------------------------------------------

    private static Protocol.PServiceStub stub(Json.Node node) {
        Wire s = Wire.of(node, "service stub");
        Wire req = s.obj("requestPattern");
        List<Protocol.PStringValuePattern> body = req.optList("bodyPatterns", EmbeddedDataReader::pattern);
        List<Protocol.PStubParam> headers = stubParams(req, "headerParams");
        List<Protocol.PStubParam> query = stubParams(req, "queryParams");
        String method = req.str("method");
        String url = req.optStr("url");
        String urlPath = req.optStr("urlPath");
        SourceInfo requestSpan = req.done(req.span());
        Wire resp = s.obj("responseDefinition");
        Protocol.PExternalFormatData bodyData = externalFormat(resp.take("body"));
        SourceInfo responseSpan = resp.done(resp.span());
        return s.done(new Protocol.PServiceStub(method, url, urlPath, query, headers, body, requestSpan, bodyData,
                responseSpan, s.span()));
    }

    /** A stub's parameter map, written sorted by name (the engine's map order): kept in that order. */
    private static @com.legend.base.Nullable List<Protocol.PStubParam> stubParams(Wire req, String key) {
        Wire m = req.optObj(key);
        if (m == null) {
            return null;
        }
        List<Protocol.PStubParam> out = new ArrayList<>();
        for (Map.Entry<String, Json.Node> e : m.json().fields().entrySet()) {
            out.add(new Protocol.PStubParam(e.getKey(), pattern(m.take(e.getKey()))));
        }
        return m.done(out);
    }

    private static Protocol.PStringValuePattern pattern(Json.Node node) {
        Wire p = Wire.of(node, "string value pattern");
        String type = p.type();
        if (type == null) {
            throw Wire.refuse("a string value pattern without its _type");
        }
        return p.done(new Protocol.PStringValuePattern(type, p.str("expectedValue")));
    }

    // ---------------------------------------------------------------------
    // ModelStore instances (the engine's DATA walker shapes)
    // ---------------------------------------------------------------------

    /**
     * A ModelStore {@code instances} value: the data walker's span-less collection of instances, or one
     * value. A spanned collection is the ###Pure collection, read as any value.
     */
    static ValueSpecification instances(Json.Node node) {
        if (node instanceof Json.Obj o && "collection".equals(o.getStringOr("_type", null))
                && !o.has("sourceInformation")) {
            Wire c = Wire.of(node, "instances collection");
            c.type();
            List<ValueSpecification> values = c.list("values", EmbeddedDataReader::instance);
            ProtocolReader.multiplicityOfSize(c.take("multiplicity"), values.size(), "instances collection");
            return c.done(new PureCollection(values));
        }
        return instance(node);
    }

    /**
     * One instance: {@code func new} over [a span-less class pointer, the literal {@code "dummy"}, a
     * span-less collection of {@code keyExpression}s each wrapping its value in a span-less collection],
     * read back to the parser's {@code new(X, NewInstance)}; anything else is a data leaf.
     */
    private static ValueSpecification instance(Json.Node node) {
        if (!(node instanceof Json.Obj o) || !"func".equals(o.getStringOr("_type", null))
                || !AppliedFunction.NEW.equals(o.getStringOr("function", null))) {
            return leaf(node);
        }
        Wire f = Wire.of(node, "data instance");
        f.type();
        f.str("function");
        List<Json.Node> params = f.arr("parameters");
        f.done(params);
        if (params.size() != 3) {
            throw Wire.refuse("a data instance with " + params.size() + " parameters");
        }
        Wire cls = Wire.of(params.get(0), "data instance class");
        cls.constant("_type", "packageableElementPtr");
        String className = cls.done(cls.str("fullPath"));
        SpecIslandReader.emptyName(params.get(1), "dummy");
        Wire keys = Wire.of(params.get(2), "data instance keys");
        keys.constant("_type", "collection");
        List<NewInstance.KeyBinding> bindings = keys.list("values", EmbeddedDataReader::keyBinding);
        ProtocolReader.multiplicityOfSize(keys.take("multiplicity"), bindings.size(), "data instance keys");
        keys.done(bindings);
        return new AppliedFunction(AppliedFunction.NEW, List.of(new PackageableElementPtr(className),
                new NewInstance(className, List.of(), List.of(), bindings)));
    }

    /** A key's value rides a span-less collection: one element is the value itself. */
    private static NewInstance.KeyBinding keyBinding(Json.Node node) {
        Wire k = Wire.of(node, "data instance key");
        k.constant("_type", "keyExpression");
        k.constant("add", false);
        Wire key = k.obj("key");
        key.constant("_type", "string");
        String name = key.done(key.str("value"));
        ValueSpecification v = instances(k.take("expression"));
        if (v instanceof PureCollection c && c.pos() == null && c.values().size() == 1) {
            v = c.values().get(0);
        }
        return k.done(new NewInstance.KeyBinding(name, new com.legend.protocol.spec.KeyExpression(v, false, false)));
    }

    /**
     * A data leaf: the data walker's real {@code enumValue} node spans the enumeration path through the
     * value (split back into the two spans the record holds); every other leaf is an ordinary value (a
     * negative number arrives folded, as the walker writes it).
     */
    private static ValueSpecification leaf(Json.Node node) {
        if (node instanceof Json.Obj o && "enumValue".equals(o.getStringOr("_type", null))) {
            Wire w = Wire.of(node, "data enum value");
            w.type();
            String fullPath = w.str("fullPath");
            String value = w.str("value");
            SourceInfo at = w.span();
            return w.done(at == null ? new EnumValue(fullPath, value, null, null, false)
                    : new EnumValue(fullPath, value,
                            new SourceInfo(at.sourceId(), at.startLine(), at.startColumn(), at.startLine(),
                                    at.startColumn() + fullPath.length() - 1),
                            new SourceInfo(at.sourceId(), at.endLine(), at.endColumn() - value.length() + 1,
                                    at.endLine(), at.endColumn()), false));
        }
        return ProtocolReader.valueSpec(node);
    }
}
