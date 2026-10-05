// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.protocol;

import com.legend.json.Json;

import java.util.List;

/**
 * The DataQuality trio read back -- the mirror of {@link TailEmitter}'s {@code dataQualityValidation}
 * (context, validation tree, filter), {@code dataqualityRelationValidation},
 * {@code dataQualityRelationComparison} and their owner-prefixed test suites.
 */
final class DataQualityReader {

    private DataQualityReader() {
    }

    private static final String MAPPING_AND_RUNTIME = "mappingAndRuntimeDataQualityExecutionContext";
    private static final String DATA_SPACE = "dataSpaceDataQualityExecutionContext";

    static Protocol.Element validation(Wire w) {
        Wire ctx = w.obj("context");
        String ctxType = ctx.type();
        String kind;
        String path;
        SourceInfo pathSpan;
        String second;
        SourceInfo secondSpan;
        if (MAPPING_AND_RUNTIME.equals(ctxType)) {
            Protocol.PPointer mapping = typedPointer(ctx.take("mapping"), "MAPPING");
            Protocol.PPointer runtime = typedPointer(ctx.take("runtime"), "RUNTIME");
            kind = "fromMappingAndRuntime";
            path = mapping.path();
            pathSpan = mapping.sourceInformation();
            second = runtime.path();
            secondSpan = runtime.sourceInformation();
        } else if (DATA_SPACE.equals(ctxType)) {
            Protocol.PPointer ds = typedPointer(ctx.take("dataSpace"), "DATASPACE");
            kind = "fromDataSpace";
            path = ds.path();
            pathSpan = ds.sourceInformation();
            second = ctx.str("context");
            secondSpan = null;
        } else {
            throw Wire.refuse("no reader rule for DataQuality context _type '" + ctxType + "'");
        }
        ctx.done(kind);
        Json.Node filter = w.opt("filter");
        return new Protocol.PDataQualityValidation(w.str("package"), w.str("name"),
                w.list("stereotypes", DomainReader::stereotype), w.list("taggedValues", DomainReader::taggedValue), kind,
                path, pathSpan, second, secondSpan, tree(w.take("dataQualityRootGraphFetchTree"), true),
                filter == null ? null : ProtocolReader.valueSpec(filter), w.span());
    }

    private static Protocol.PPointer typedPointer(Json.Node node, String type) {
        Protocol.PPointer p = DomainReader.pointer(node);
        if (!type.equals(p.type())) {
            throw Wire.refuse("a DataQuality context pointer typed " + p.type() + ", expected " + type);
        }
        return p;
    }

    /** A validation-tree node: the root names its class, a property node its property (no parameters). */
    private static Protocol.PDqTreeNode tree(Json.Node node, boolean root) {
        Wire n = Wire.of(node, "DataQuality tree");
        n.constant("_type", root ? "dataQualityRootGraphFetchTree" : "dataQualityPropertyGraphFetchTree");
        n.emptyArray("subTypeTrees");
        String className = root ? n.str("class") : null;
        String property = null;
        if (!root) {
            n.emptyArray("parameters");
            property = n.str("property");
        }
        return n.done(new Protocol.PDqTreeNode(className, property, n.strings("constraints"),
                n.list("subTrees", t -> tree(t, false)), n.optStr("subType"), n.span()));
    }

    static Protocol.Element relationValidation(Wire w) {
        return new Protocol.PDataQualityRelationValidation(w.str("package"), w.str("name"),
                w.list("stereotypes", DomainReader::stereotype), w.list("taggedValues", DomainReader::taggedValue),
                ProtocolReader.valueSpec(w.take("query")), w.list("validations", n -> {
                    Wire c = Wire.of(n, "relation validation");
                    return c.done(new Protocol.PDqRelationCheck(c.str("name"), c.optStr("description"),
                            ProtocolReader.valueSpec(c.take("assertion")), c.optStr("type")));
                }), testSuites(w, "dataQualityRelationValidation"), w.span());
    }

    /** The comparison's strategy: {@code md5Hash} on the wire is the grammar's {@code MD5Hash}. */
    static Protocol.Element relationComparison(Wire w) {
        Wire s = w.obj("strategy");
        s.constant("_type", "md5Hash");
        Protocol.PReconStrategy strategy = s.done(new Protocol.PReconStrategy("MD5Hash", s.optStr("sourceHashColumn"),
                s.optStr("targetHashColumn"), s.optBool("aggregatedHash")));
        Json.Node expected = w.opt("expectedMatch");
        return new Protocol.PDataQualityRelationComparison(w.str("package"), w.str("name"),
                ProtocolReader.valueSpec(w.take("source")), ProtocolReader.valueSpec(w.take("target")),
                w.strings("keys"), w.strings("columnsToCompare"),
                expected == null ? null : Wire.asDouble(expected, "expectedMatch"), strategy,
                testSuites(w, "dataQualityRelationComparison"), w.span());
    }

    /** The Testable block: suites and tests carry the OWNER's type name as their {@code _type} prefix. */
    private static @com.legend.base.Nullable List<Protocol.PDqTestSuite> testSuites(Wire w, String owner) {
        return w.optList("testSuites", n -> {
            Wire s = Wire.of(n, owner + " test suite");
            s.constant("_type", owner + "TestSuite");
            Protocol.PDqTestData data = null;
            Wire d = s.optObj("testData");
            if (d != null) {
                data = d.done(new Protocol.PDqTestData(d.list("testData", DataQualityReader::storeData), d.span()));
            }
            return s.done(new Protocol.PDqTestSuite(s.str("id"), data, s.list("tests", t -> {
                Wire test = Wire.of(t, owner + " test");
                test.constant("_type", owner + "Test");
                return test.done(new Protocol.PDqTest(test.str("id"),
                        test.list("assertions", EmbeddedDataReader::assertion), test.span()));
            }), s.span()));
        });
    }

    /** One {@code store: EmbeddedData} entry: a STORE pointer and the data. */
    private static Protocol.PDqStoreData storeData(Json.Node node) {
        Wire d = Wire.of(node, "DataQuality test data");
        Protocol.PPointer store = typedPointer(d.take("packageableElementPointer"), "STORE");
        return d.done(new Protocol.PDqStoreData(store.path(), store.sourceInformation(),
                EmbeddedDataReader.value(d.take("data")), d.span()));
    }
}
