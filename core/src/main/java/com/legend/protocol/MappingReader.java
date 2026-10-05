// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.protocol;

import com.legend.json.Json;
import com.legend.protocol.spec.LambdaFunction;
import com.legend.protocol.spec.PackageableElementPtr;

import java.util.List;
import java.util.Map;
import java.util.function.Function;

/**
 * The {@code ###Mapping} element read back -- the mirror of {@link MappingEmitter#mapping}: the envelope,
 * association mappings, includes, enumeration mappings, the legacy {@code tests} and the
 * {@code testSuites} ({@link MappingTestReader}); class mappings read in {@link ClassMappingReader}.
 *
 * <p>Not on the wire, so absent on read: the mapping's {@code testSuites} source text, and the
 * substitutions of an include with more than one {@code src->tgt} pair (the engine records neither path
 * then); a single pair is its own substitution.
 */
final class MappingReader {

    private MappingReader() {
    }

    static Protocol.Element mapping(Wire w) {
        List<Protocol.PMappingTestSuite> suites = StoreReader.nonEmpty(w, "testSuites", MappingTestReader::suite);
        return new Protocol.PMapping(w.str("package"), w.str("name"),
                w.list("associationMappings", MappingReader::associationMapping),
                w.list("classMappings", ClassMappingReader::classMapping),
                w.list("enumerationMappings", MappingReader::enumerationMapping),
                w.list("includedMappings", MappingReader::include), suites,
                w.list("tests", MappingTestReader::legacyTest), null, w.span());
    }

    // ---------------------------------------------------------------------
    // Association mappings
    // ---------------------------------------------------------------------

    /** The reader rule for each association-mapping {@code _type}. */
    private static final Map<String, Function<Wire, Protocol.PAssociationMapping>> ASSOCIATIONS = Map.of(
            "functionAssociation", MappingReader::functionAssociation,
            "relational", w -> new Protocol.PRelAssociationMapping(DomainReader.pointer(w.take("association")),
                    w.optStr("id"), w.list("propertyMappings", MappingReader::relAssocProperty), w.strings("stores"),
                    w.span()),
            "xStore", w -> {
                w.emptyArray("stores");
                return new Protocol.PXStoreAssociationMapping(DomainReader.pointer(w.take("association")),
                        w.optStr("id"), w.list("propertyMappings", MappingReader::xStoreProperty), w.span());
            },
            "modelJoin", w -> {
                w.emptyArray("stores");
                return new Protocol.PModelJoinAssociationMapping(DomainReader.pointer(w.take("association")),
                        w.optStr("id"), ProtocolReader.valueSpec(w.take("joinCondition")), w.span());
            });

    private static Protocol.PAssociationMapping associationMapping(Json.Node node) {
        Wire w = Wire.of(node, "association mapping");
        return w.done(Wire.rule(ASSOCIATIONS, w.type(), "association mapping").apply(w));
    }

    private static Protocol.PAssociationMapping functionAssociation(Wire w) {
        Json.Node body = w.opt("bodyLambda");
        Json.Node fn = w.opt("function");
        LambdaFunction lambda = body == null ? null : ProtocolReader.lambdaNode(body);
        PackageableElementPtr ptr = null;
        if (fn != null) {
            if (!(ProtocolReader.valueSpec(fn) instanceof PackageableElementPtr p)) {
                throw Wire.refuse("a function association whose function is not an element pointer");
            }
            ptr = p;
        }
        if ((ptr == null) == (lambda == null)) {
            throw Wire.refuse("a function association is EITHER a function or a body lambda");
        }
        return new Protocol.PFunctionAssociationMapping(DomainReader.pointer(w.take("association")), ptr, lambda,
                w.span());
    }

    /** A relational association side: the property carries no class on this wire. */
    private static Protocol.PRelAssocPropertyMapping relAssocProperty(Json.Node node) {
        Wire w = Wire.of(node, "relational association property mapping");
        w.constant("_type", "relationalPropertyMapping");
        Wire p = w.obj("property");
        String property = p.str("property");
        SourceInfo propSpan = p.done(p.span());
        return w.done(new Protocol.PRelAssocPropertyMapping(property, propSpan,
                StoreReader.relOp(w.take("relationalOperation")), w.optStr("source"), w.optStr("target"),
                w.span()));
    }

    /** An xStore side: its cross expression is a span-less parameterless lambda. */
    private static Protocol.PXStorePropertyMapping xStoreProperty(Json.Node node) {
        Wire w = Wire.of(node, "xStore property mapping");
        w.constant("_type", "xStorePropertyMapping");
        Wire p = w.obj("property");
        String owner = p.str("class");
        String property = p.str("property");
        SourceInfo propSpan = p.done(p.span());
        return w.done(new Protocol.PXStorePropertyMapping(owner, property, propSpan,
                ClassMappingReader.bareLambda(w.take("crossExpression"), "xStore cross expression"),
                w.str("source"), w.str("target"), w.span()));
    }

    // ---------------------------------------------------------------------
    // Includes and enumeration mappings
    // ---------------------------------------------------------------------

    private static Protocol.PMappingInclude include(Json.Node node) {
        Wire w = Wire.of(node, "mapping include");
        String type = w.type();
        if ("mappingIncludeDataSpace".equals(type)) {
            return w.done(new Protocol.PMappingInclude(null, w.str("includedDataSpace"), null, null, List.of(),
                    w.span()));
        }
        if (!"mappingIncludeMapping".equals(type)) {
            throw Wire.refuse("no reader rule for mapping include _type '" + type + "'");
        }
        String src = w.optStr("sourceDatabasePath");
        String tgt = w.optStr("targetDatabasePath");
        List<Protocol.PStoreSubstitution> subs = src != null && tgt != null
                ? List.of(new Protocol.PStoreSubstitution(src, tgt)) : List.of();
        return w.done(new Protocol.PMappingInclude(w.str("includedMapping"), null, src, tgt, subs, w.span()));
    }

    private static Protocol.PEnumerationMapping enumerationMapping(Json.Node node) {
        Wire w = Wire.of(node, "enumeration mapping");
        return w.done(new Protocol.PEnumerationMapping(w.optStr("id"), DomainReader.pointer(w.take("enumeration")),
                w.list("enumValueMappings", MappingReader::enumValueMapping), w.span()));
    }

    private static Protocol.PEnumValueMapping enumValueMapping(Json.Node node) {
        Wire w = Wire.of(node, "enum value mapping");
        return w.done(new Protocol.PEnumValueMapping(w.str("enumValue"),
                w.list("sourceValues", MappingReader::sourceValue)));
    }

    /** A source value: a string, an integer (a long, as the parser holds it), or an enum value reference. */
    private static Protocol.PEnumSourceValue sourceValue(Json.Node node) {
        Wire w = Wire.of(node, "enum source value");
        String type = w.type();
        Protocol.PEnumSourceValue out;
        if ("stringSourceValue".equals(type)) {
            out = new Protocol.PEnumSourceValue(null, w.str("value"));
        } else if ("integerSourceValue".equals(type)) {
            out = new Protocol.PEnumSourceValue(null, w.lng("value"));
        } else if ("enumSourceValue".equals(type)) {
            out = new Protocol.PEnumSourceValue(w.str("enumeration"), w.str("value"));
        } else {
            throw Wire.refuse("no reader rule for enum source value _type '" + type + "'");
        }
        return w.done(out);
    }
}
