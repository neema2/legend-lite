// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.protocol;

import com.legend.json.Json;
import com.legend.protocol.spec.AppliedFunction;
import com.legend.protocol.spec.PureCollection;
import com.legend.protocol.spec.ValueSpecification;

import java.util.List;

/**
 * Services and execution environments read back -- the mirror of {@link TailEmitter}'s {@code service},
 * {@code serviceExecution}, {@code serviceFunc}, {@code serviceRuntime}, {@code legacyAssert},
 * {@code executionEnvironment} and {@code singleExecutionParameters} rules; the test suites read in
 * {@link ServiceTestReader}.
 *
 * <p>What the wire does not carry reads back as the parser's absent value: {@code autoActivateUpdates}
 * written {@code false} (unspelled), an empty
 * {@code postValidations} (unspelled). A query the grammar wrote bare rides the wire as a span-less
 * lambda and reads back as that lambda (the same bytes).
 */
final class ServiceReader {

    private ServiceReader() {
    }

    /** Older JSON leaves out what the engine's {@code Service} starts with: no annotations, owners or post
     *  validations, and {@code autoActivateUpdates} true. */
    static Protocol.Element service(Wire w) {
        Boolean written = w.optBool("autoActivateUpdates");
        boolean auto = written == null || written;
        String ownershipKind = null;
        String ownershipId = null;
        List<String> users = null;
        Wire own = w.optObj("ownership");
        if (own != null) {
            String type = own.type();
            if ("userListOwnership".equals(type)) {
                ownershipKind = "UserList";
                users = own.strings("users");
            } else if ("deploymentOwnership".equals(type)) {
                ownershipKind = "DID";
                ownershipId = own.str("identifier");
            } else {
                throw Wire.refuse("no reader rule for service ownership _type '" + type + "'");
            }
            own.done(own);
        }
        List<Protocol.PPostValidation> post = w.listOrEmpty("postValidations", ServiceReader::postValidation);
        List<String> owners = w.optStrings("owners");
        return new Protocol.PService(w.str("package"), w.str("name"), DomainReader.stereotypes(w),
                DomainReader.taggedValues(w), w.optStr("pattern"), w.optStr("title"),
                owners == null ? List.of() : owners, ownershipKind, ownershipId, users, w.optStr("mcpServer"),
                w.optStr("documentation"), auto ? Boolean.TRUE : null, execution(w.take("execution")),
                legacyTest(w.opt("test")), w.optList("testSuites", ServiceTestReader::testSuite),
                post.isEmpty() ? null : post, w.span());
    }

    private static Protocol.PPostValidation postValidation(Json.Node node) {
        Wire p = Wire.of(node, "post validation");
        return p.done(new Protocol.PPostValidation(p.str("description"),
                p.list("parameters", ProtocolReader::valueSpec), p.list("assertions", n -> {
                    Wire a = Wire.of(n, "post validation assertion");
                    return a.done(new Protocol.PPostValidationAssertion(a.str("id"),
                            ProtocolReader.valueSpec(a.take("assertion")), a.span()));
                }), p.span()));
    }

    // ---------------------------------------------------------------------
    // Executions
    // ---------------------------------------------------------------------

    private static Protocol.PServiceExecution execution(Json.Node node) {
        Wire e = Wire.of(node, "service execution");
        String type = e.type();
        if ("pureSingleExecution".equals(type)) {
            ValueSpecification query = ProtocolReader.lambdaNode(e.take("func"));
            Keyed k = keyed(e);
            return e.done(new Protocol.PSingleExecution(query, k.mapping, k.mappingSpan, k.runtime, k.runtimeSpan,
                    k.embedded, e.span()));
        }
        if ("pureMultiExecution".equals(type)) {
            return e.done(new Protocol.PMultiExecution(ProtocolReader.lambdaNode(e.take("func")),
                    e.optStr("executionKey"), e.optList("executionParameters", ServiceReader::multiParameter),
                    e.span()));
        }
        throw Wire.refuse("no reader rule for service execution _type '" + type + "'");
    }

    private static Protocol.PKeyedExecution multiParameter(Json.Node node) {
        Wire p = Wire.of(node, "multi execution parameter");
        Keyed k = keyed(p);
        return p.done(new Protocol.PKeyedExecution(p.str("key"), k.mapping, k.mappingSpan, k.runtime, k.runtimeSpan,
                k.embedded, null, p.span()));
    }

    /** The mapping and runtime slots every execution shape writes. */
    private record Keyed(@com.legend.base.Nullable String mapping, @com.legend.base.Nullable SourceInfo mappingSpan,
            @com.legend.base.Nullable String runtime, @com.legend.base.Nullable SourceInfo runtimeSpan,
            Protocol.@com.legend.base.Nullable PEmbeddedRuntime embedded) {
    }

    private static Keyed keyed(Wire w) {
        String mapping = w.optStr("mapping");
        SourceInfo mappingSpan = w.span("mappingSourceInformation");
        String runtime = null;
        SourceInfo runtimeSpan = null;
        Protocol.PEmbeddedRuntime embedded = null;
        Wire r = w.optObj("runtime");
        if (r != null) {
            String type = r.type();
            if ("runtimePointer".equals(type)) {
                runtime = r.str("runtime");
                runtimeSpan = r.span();
            } else {
                embedded = embeddedRuntime(r, type);
            }
            r.done(r);
        }
        return new Keyed(mapping, mappingSpan, runtime, runtimeSpan, embedded);
    }

    /** An execution's runtime written in full: today's {@code engineRuntime}, or an older {@code legacyRuntime}. */
    private static Protocol.PEmbeddedRuntime embeddedRuntime(Wire r, @com.legend.base.Nullable String type) {
        if ("engineRuntime".equals(type)) {
            ConnectionReader.Arrays a = ConnectionReader.arrays(r);
            return new Protocol.PEmbeddedRuntime(a.mappings(), a.connections(), a.connectionStores(), r.span());
        }
        if ("legacyRuntime".equals(type) || type == null) {
            // no _type: the engine's default Runtime subtype is the legacy one (CorePureProtocolExtension)
            return legacyRuntime(r);
        }
        throw Wire.refuse("no reader rule for an execution runtime _type '" + type + "'");
    }

    /**
     * An older {@code legacyRuntime}: the runtime legend-engine itself turns it into
     * ({@code LegacyRuntime.toEngineRuntime}, which its service grammar composer and test runner use): its mappings,
     * and its connections grouped under each one's store, in order, identified {@code connection_1}, {@code _2}, ...
     */
    private static Protocol.PEmbeddedRuntime legacyRuntime(Wire r) {
        List<Protocol.PPointer> mappings = r.listOrEmpty("mappings", n -> DomainReader.pointer(n, "MAPPING"));
        java.util.LinkedHashMap<String, List<Protocol.PIdentifiedConnection>> byStore = new java.util.LinkedHashMap<>();
        int n = 1;
        for (Json.Node c : r.arrOrEmpty("connections")) {
            Protocol.PConnectionValue value = ConnectionReader.connectionValue(c);
            String store = element(value);
            byStore.computeIfAbsent(store, k -> new java.util.ArrayList<>())
                    .add(new Protocol.PIdentifiedConnection("connection_" + n++, value, null));
        }
        List<Protocol.PStoreConnections> connections = new java.util.ArrayList<>();
        byStore.forEach((store, cs) -> connections.add(new Protocol.PStoreConnections(
                new Protocol.PPointer("STORE", store, null), cs, null)));
        return new Protocol.PEmbeddedRuntime(mappings, connections, List.of(), r.span());
    }

    /** The store a legacy runtime's connection names; a pointer names none, so no store can group it. */
    private static String element(Protocol.PConnectionValue value) {
        String element = switch (value) {
            case Protocol.PConnectionPointer p -> null;
            case Protocol.PJsonModelConnection c -> c.element();
            case Protocol.PXmlModelConnection c -> c.element();
            case Protocol.PModelChainConnection c -> c.element();
            case Protocol.PRelationalDatabaseConnection c -> c.element();
            case Protocol.PServiceStoreConnection c -> c.element();
            case Protocol.PDeephavenConnection c -> c.element();
            case Protocol.PMongoDbConnection c -> c.element();
            case Protocol.PElasticsearchConnection c -> c.element();
        };
        if (element == null) {
            throw Wire.refuse("a legacyRuntime connection that names no store (element): it has none to be grouped under");
        }
        return element;
    }

    // ---------------------------------------------------------------------
    // Execution environments
    // ---------------------------------------------------------------------

    static Protocol.Element executionEnvironment(Wire w) {
        return new Protocol.PExecutionEnvironment(w.str("package"), w.str("name"),
                w.list("executionParameters", ServiceReader::executionParameters), w.span());
    }

    private static Protocol.PExecutionParameters executionParameters(Json.Node node) {
        Wire p = Wire.of(node, "execution parameters");
        String type = p.type();
        if ("singleExecutionParameters".equals(type)) {
            return p.done(singleParameters(p));
        }
        if ("multiExecutionParameters".equals(type)) {
            return p.done(new Protocol.PMultiKeyedExecution(p.str("masterKey"),
                    p.list("singleExecutionParameters", n -> {
                        Wire s = Wire.of(n, "single execution parameters");
                        s.constant("_type", "singleExecutionParameters");
                        return s.done(singleParameters(s));
                    })));
        }
        throw Wire.refuse("no reader rule for execution parameters _type '" + type + "'");
    }

    private static Protocol.PKeyedExecution singleParameters(Wire p) {
        Keyed k = keyed(p);
        Protocol.PRuntimeComponents rc = null;
        Wire c = p.optObj("runtimeComponents");
        if (c != null) {
            Protocol.PPointer binding = DomainReader.pointer(c.take("binding"));
            Protocol.PPointer clazz = DomainReader.pointer(c.take("clazz"));
            Wire rt = c.obj("runtime");
            rt.constant("_type", "runtimePointer");
            String runtime = rt.str("runtime");
            SourceInfo span = rt.done(rt.span());
            rc = c.done(new Protocol.PRuntimeComponents(binding, clazz, runtime, span));
        }
        return new Protocol.PKeyedExecution(p.str("key"), k.mapping, k.mappingSpan, k.runtime, k.runtimeSpan,
                k.embedded, rc, p.span());
    }

    // ---------------------------------------------------------------------
    // Legacy tests
    // ---------------------------------------------------------------------

    private static Protocol.@com.legend.base.Nullable PLegacyServiceTest legacyTest(
            @com.legend.base.Nullable Json.Node node) {
        if (node == null) {
            return null;
        }
        Wire t = Wire.of(node, "legacy service test");
        String type = t.type();
        if ("singleExecutionTest".equals(type)) {
            return t.done(new Protocol.PLegacyServiceTest("Single", t.str("data"),
                    t.list("asserts", ServiceReader::legacyAssert), List.of(), t.span()));
        }
        if ("multiExecutionTest".equals(type)) {
            return t.done(new Protocol.PLegacyServiceTest("Multi", null, List.of(),
                    t.list("tests", ServiceReader::keyedTest), t.span()));
        }
        throw Wire.refuse("no reader rule for legacy service test _type '" + type + "'");
    }

    private static Protocol.PLegacyServiceTest.PKeyedLegacyTest keyedTest(Json.Node node) {
        Wire k = Wire.of(node, "keyed legacy test");
        return k.done(new Protocol.PLegacyServiceTest.PKeyedLegacyTest(k.str("key"), k.str("data"),
                k.list("asserts", ServiceReader::legacyAssert), k.span()));
    }

    /** {@code {assert: lambda, parametersValues: [...]}}: a {@code list([...])} parameter rides a
     *  {@code listInstance} class instance whose outer and inner spans are the call's. */
    private static Protocol.PLegacyServiceTest.PLegacyAssert legacyAssert(Json.Node node) {
        Wire a = Wire.of(node, "legacy assert");
        return a.done(new Protocol.PLegacyServiceTest.PLegacyAssert(
                a.listOrEmpty("parametersValues", ServiceReader::legacyParameter), ProtocolReader.lambdaNode(a.take("assert")),
                a.span()));
    }

    private static ValueSpecification legacyParameter(Json.Node node) {
        if (node instanceof Json.Obj o && "classInstance".equals(o.getStringOr("_type", null))
                && "listInstance".equals(o.getStringOr("type", null))) {
            Wire c = Wire.of(node, "list instance");
            c.type();
            c.str("type");
            SourceInfo outer = c.span();
            Wire v = c.obj("value");
            SourceInfo inner = v.span();
            StoreReader.sameSpan(outer, inner, "list instance");
            List<ValueSpecification> values = v.done(v.list("values", ProtocolReader::valueSpec));
            return c.done(AppliedFunction.list(new PureCollection(values), outer));
        }
        return ProtocolReader.valueSpec(node);
    }
}
