// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.protocol;

import com.legend.json.Json;
import com.legend.protocol.Protocol.PServiceTestSuite;
import com.legend.protocol.spec.EnumValue;
import com.legend.protocol.spec.PureCollection;
import com.legend.protocol.spec.ValueSpecification;

import java.util.List;

/**
 * A service's {@code serviceTestSuite}s read back -- the mirror of {@link TailEmitter}'s
 * {@code serviceTestSuite} and {@code paramValue} rules: connection test data and the compact form's
 * resolver entries (whose element pointer carries no type), tests with their keys, parameters and
 * assertions.
 */
final class ServiceTestReader {

    private ServiceTestReader() {
    }

    static PServiceTestSuite testSuite(Json.Node node) {
        Wire s = Wire.of(node, "service test suite");
        s.constant("_type", "serviceTestSuite");
        PServiceTestSuite.PSuiteData data = null;
        Wire d = s.optObj("testData");
        if (d != null) {
            List<PServiceTestSuite.PSuiteConnData> conns = StoreReader.nonEmpty(d, "connectionsTestData",
                    ServiceTestReader::connectionData);
            List<PServiceTestSuite.PResolverData> resolvers = StoreReader.nonEmpty(d, "serviceTestData",
                    ServiceTestReader::resolver);
            data = d.done(new PServiceTestSuite.PSuiteData(conns, d.has("serviceTestData") ? resolvers : null,
                    d.span()));
        }
        return s.done(new PServiceTestSuite(s.str("id"), s.optStr("doc"), data,
                s.list("tests", ServiceTestReader::test), s.span()));
    }

    private static PServiceTestSuite.PSuiteConnData connectionData(Json.Node node) {
        Wire c = Wire.of(node, "connection test data");
        return c.done(new PServiceTestSuite.PSuiteConnData(c.str("id"), EmbeddedDataReader.value(c.take("data")),
                c.span()));
    }

    /** A compact-form entry: {@code path;} is a reference resolver, {@code path: Kind #{...}#;} a base one. */
    private static PServiceTestSuite.PResolverData resolver(Json.Node node) {
        Wire r = Wire.of(node, "service test data resolver");
        String type = r.type();
        Protocol.PEmbeddedDataValue data;
        if ("baseDataResolver".equals(type)) {
            data = EmbeddedDataReader.value(r.take("data"));
        } else if ("referenceDataResolver".equals(type)) {
            data = null;
        } else {
            throw Wire.refuse("no reader rule for service test data _type '" + type + "'");
        }
        Protocol.PElementRef ptr = EmbeddedDataReader.elementRef(r.take("elementPointer"));
        return r.done(new PServiceTestSuite.PResolverData(data, ptr.path(), ptr.sourceInformation(), r.span()));
    }

    private static PServiceTestSuite.PSuiteTest test(Json.Node node) {
        Wire t = Wire.of(node, "service test");
        t.constant("_type", "serviceTest");
        return t.done(new PServiceTestSuite.PSuiteTest(t.str("id"), t.optStr("doc"), t.optStr("serializationFormat"),
                t.strings("keys"), t.optList("parameters", ServiceTestReader::parameter),
                t.list("assertions", EmbeddedDataReader::assertion), t.span()));
    }

    private static PServiceTestSuite.PSuiteParam parameter(Json.Node node) {
        Wire p = Wire.of(node, "service test parameter");
        return p.done(new PServiceTestSuite.PSuiteParam(p.str("name"), paramValue(p.take("value"))));
    }

    /**
     * A test parameter's value: the TEST walker writes an enum as a real {@code enumValue} node spanning
     * the whole dotted text (split back into the enumeration's and the value's spans), and a collection
     * holding one keeps its own span with its elements rewritten the same way; anything else is a value.
     */
    private static ValueSpecification paramValue(Json.Node node) {
        if (node instanceof Json.Obj o) {
            String type = o.getStringOr("_type", null);
            if ("enumValue".equals(type)) {
                return enumNode(node);
            }
            if ("collection".equals(type) && hasEnumNode(o)) {
                Wire c = Wire.of(node, "service test parameter collection");
                c.type();
                List<ValueSpecification> values = c.list("values", ServiceTestReader::paramValue);
                ProtocolReader.multiplicityOfSize(c.take("multiplicity"), values.size(), "parameter collection");
                return c.done(new PureCollection(values, c.span()));
            }
        }
        return ProtocolReader.valueSpec(node);
    }

    private static boolean hasEnumNode(Json.Obj collection) {
        Json.Arr values = collection.getArrOr("values", null);
        if (values == null) {
            return false;
        }
        for (Json.Node v : values.items()) {
            if (v instanceof Json.Obj e && "enumValue".equals(e.getStringOr("_type", null))) {
                return true;
            }
        }
        return false;
    }

    private static ValueSpecification enumNode(Json.Node node) {
        Wire w = Wire.of(node, "service test enum parameter");
        w.type();
        String fullPath = w.str("fullPath");
        String value = w.str("value");
        SourceInfo at = w.span();
        if (at == null) {
            return w.done(new EnumValue(fullPath, value, null, null, false));
        }
        return w.done(new EnumValue(fullPath, value,
                new SourceInfo(at.sourceId(), at.startLine(), at.startColumn(), at.startLine(),
                        at.startColumn() + fullPath.length() - 1),
                new SourceInfo(at.sourceId(), at.endLine(), at.endColumn() - value.length() + 1, at.endLine(),
                        at.endColumn()), false));
    }
}
