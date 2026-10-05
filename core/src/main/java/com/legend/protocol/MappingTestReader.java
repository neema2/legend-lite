// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.protocol;

import com.legend.json.Json;

import java.util.List;

/**
 * A mapping's tests read back -- the mirror of {@link MappingEmitter#testSuite} (the
 * {@code mappingTestSuite}s, their tests and store test data) and of the legacy {@code tests} array.
 *
 * <p>Store test data has four record forms with three wire shapes: a {@code relationAccessor} is the
 * relation-elements form, a {@code reference} whose span is its pointer's the data-element form, a
 * {@code modelStore} the model-data form, and anything else an embedded value (which writes the same
 * bytes as the form that would otherwise take it).
 */
final class MappingTestReader {

    private MappingTestReader() {
    }

    static Protocol.PMappingTestSuite suite(Json.Node node) {
        Wire w = Wire.of(node, "mapping test suite");
        w.constant("_type", "mappingTestSuite");
        return w.done(new Protocol.PMappingTestSuite(w.str("id"), w.optStr("doc"),
                ProtocolReader.valueSpec(w.take("func")), w.list("tests", MappingTestReader::test), w.span()));
    }

    private static Protocol.PMappingTest test(Json.Node node) {
        Wire w = Wire.of(node, "mapping test");
        w.constant("_type", "mappingTest");
        return w.done(new Protocol.PMappingTest(w.str("id"), w.optStr("doc"),
                w.list("storeTestData", MappingTestReader::storeTestData),
                w.list("assertions", EmbeddedDataReader::assertion), w.span()));
    }

    private static Protocol.PStoreTestData storeTestData(Json.Node node) {
        Wire w = Wire.of(node, "store test data");
        Protocol.PPointer store = DomainReader.pointer(w.take("store"));
        SourceInfo span = w.span();
        Json.Node data = w.take("data");
        String type = data instanceof Json.Obj o ? o.getStringOr("_type", null) : null;
        Protocol.PStoreTestData out;
        if ("relationAccessor".equals(type)) {
            Wire d = Wire.of(data, "relation accessor");
            d.type();
            List<Protocol.PRelationElement> rels = d.list("relationElements", EmbeddedDataReader::relationElement);
            SourceInfo accessorSpan = d.span();
            d.done(rels);
            out = new Protocol.PStoreTestData(store, null, null, null, rels, accessorSpan, null, span);
        } else {
            Protocol.PEmbeddedDataValue v = EmbeddedDataReader.value(data);
            if (v instanceof Protocol.PModelStoreData ms) {
                out = new Protocol.PStoreTestData(store, ms.modelData(), ms.sourceInformation(), null, null, null,
                        null, span);
            } else if (v instanceof Protocol.PDataReference ref
                    && java.util.Objects.equals(ref.sourceInformation(), ref.dataElement().sourceInformation())) {
                out = new Protocol.PStoreTestData(store, null, null, ref.dataElement(), null, null, null, span);
            } else {
                out = new Protocol.PStoreTestData(store, null, null, null, null, null, v, span);
            }
        }
        return w.done(out);
    }

    // ---------------------------------------------------------------------
    // Legacy tests
    // ---------------------------------------------------------------------

    static Protocol.PLegacyMappingTest legacyTest(Json.Node node) {
        Wire w = Wire.of(node, "legacy mapping test");
        Wire a = w.obj("assert");
        a.constant("_type", "expectedOutputMappingTestAssert");
        String expected = a.str("expectedOutput");
        SourceInfo assertSpan = a.done(a.span());
        return w.done(new Protocol.PLegacyMappingTest(w.str("name"), ProtocolReader.valueSpec(w.take("query")),
                w.list("inputData", MappingTestReader::inputData), expected, assertSpan, w.span()));
    }

    /** {@code <Object, JSON, cls, 'data'>} or {@code <Relational, CSV, db, 'data'>}. */
    private static Protocol.PLegacyInputData inputData(Json.Node node) {
        Wire w = Wire.of(node, "legacy input data");
        String type = w.type();
        boolean relational;
        String target;
        if ("relational".equals(type)) {
            relational = true;
            target = w.str("database");
        } else if ("object".equals(type)) {
            relational = false;
            target = w.str("sourceClass");
        } else {
            throw Wire.refuse("no reader rule for legacy input data _type '" + type + "'");
        }
        return w.done(new Protocol.PLegacyInputData(relational, target, w.str("inputType"), w.str("data"),
                w.span()));
    }
}
