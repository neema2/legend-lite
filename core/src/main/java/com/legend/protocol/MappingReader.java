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

    /** A list older JSON leaves out is empty where the engine's {@code Mapping} starts it empty (all but
     *  {@code classMappings}). */
    static Protocol.Element mapping(Wire w) {
        List<Protocol.PMappingTestSuite> suites = StoreReader.nonEmpty(w, "testSuites", MappingTestReader::suite);
        return new Protocol.PMapping(w.str("package"), w.str("name"),
                w.listOrEmpty("associationMappings", MappingReader::associationMapping),
                w.list("classMappings", ClassMappingReader::classMapping),
                w.listOrEmpty("enumerationMappings", MappingReader::enumerationMapping),
                w.listOrEmpty("includedMappings", MappingReader::include), suites,
                w.listOrEmpty("tests", MappingTestReader::legacyTest), null, w.span());
    }

    /** An association mapping's stores: none when older JSON leaves them out, as the engine's AssociationMapping. */
    private static List<String> stores(Wire w) {
        List<String> stores = w.optStrings("stores");
        return stores == null ? List.of() : stores;
    }

    /** An association mapping's association: the bare path in older JSON. */
    private static Protocol.PPointer association(Wire w) {
        return DomainReader.pointer(w.take("association"), "ASSOCIATION");
    }

    // ---------------------------------------------------------------------
    // Association mappings
    // ---------------------------------------------------------------------

    /** The reader rule for each association-mapping {@code _type}. */
    private static final Map<String, Function<Wire, Protocol.PAssociationMapping>> ASSOCIATIONS = Map.of(
            "functionAssociation", MappingReader::functionAssociation,
            "relational", w -> new Protocol.PRelAssociationMapping(association(w),
                    w.optStr("id"), w.list("propertyMappings", MappingReader::relAssocProperty), stores(w),
                    w.span()),
            "xStore", w -> {
                w.emptyArray("stores");
                return new Protocol.PXStoreAssociationMapping(association(w),
                        w.optStr("id"), w.list("propertyMappings", MappingReader::xStoreProperty), w.span());
            },
            "modelJoin", w -> {
                w.emptyArray("stores");
                return new Protocol.PModelJoinAssociationMapping(association(w),
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
        return new Protocol.PFunctionAssociationMapping(association(w), ptr, lambda,
                w.span());
    }

    /**
     * A relational association side: the property carries no class on the grammar's wire; older JSON writes the
     * property's owner class, which the engine's compiler resolves the property on -- kept.
     */
    private static Protocol.PRelAssocPropertyMapping relAssocProperty(Json.Node node) {
        Wire w = Wire.of(node, "relational association property mapping");
        w.constant("_type", "relationalPropertyMapping");
        Wire p = w.obj("property");
        String owner = p.optStr("class");
        String property = p.str("property");
        SourceInfo propSpan = p.done(p.span());
        return w.done(new Protocol.PRelAssocPropertyMapping(property, propSpan,
                StoreReader.relOp(w.take("relationalOperation")), w.optStr("source"), w.optStr("target"),
                w.span(), owner));
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

    /**
     * An include. Older JSON leaves the {@code _type} out (the engine's {@code MappingInclude} defaults it to a
     * mapping include) and names the included mapping by package and name ({@code MappingIncludeMapping}'s
     * deprecated {@code includedMappingPackage}/{@code includedMappingName}, joined by its getter); the engine
     * discards a {@code createdFromExplicitType}, so it is refused.
     */
    private static Protocol.PMappingInclude include(Json.Node node) {
        Wire w = Wire.of(node, "mapping include");
        String type = w.type();
        if ("mappingIncludeDataSpace".equals(type)) {
            return w.done(new Protocol.PMappingInclude(null, w.str("includedDataSpace"), null, null, List.of(),
                    w.span()));
        }
        if (type != null && !"mappingIncludeMapping".equals(type)) {
            throw Wire.refuse("no reader rule for mapping include _type '" + type + "'");
        }
        String src = w.optStr("sourceDatabasePath");
        String tgt = w.optStr("targetDatabasePath");
        List<Protocol.PStoreSubstitution> subs = src != null && tgt != null
                ? List.of(new Protocol.PStoreSubstitution(src, tgt)) : List.of();
        return w.done(new Protocol.PMappingInclude(includedMapping(w), null, src, tgt, subs, w.span()));
    }

    private static String includedMapping(Wire w) {
        String path = w.optStr("includedMapping");
        String pkg = w.optStr("includedMappingPackage");
        String name = w.optStr("includedMappingName");
        if (pkg == null && name == null) {
            return path != null ? path : w.str("includedMapping");
        }
        if (path != null || pkg == null || name == null) {
            throw Wire.refuse("a mapping include with " + (path != null ? "both its path and the older package and"
                    + " name: the engine reads the path and drops the others" : "half of the older package and name"));
        }
        return pkg + "::" + name;
    }

    /**
     * An enumeration mapping. Older JSON writes the enumeration as a bare path, and (protocol 1.10) a
     * {@code sourceType} that says how to read the source values ({@code EnumerationMapping}'s deprecated,
     * read-only field).
     */
    private static Protocol.PEnumerationMapping enumerationMapping(Json.Node node) {
        Wire w = Wire.of(node, "enumeration mapping");
        String id = w.optStr("id");
        Protocol.PPointer enumeration = DomainReader.pointer(w.take("enumeration"), "ENUMERATION");
        String sourceType = w.optStr("sourceType");
        List<Protocol.PEnumValueMapping> values = w.list("enumValueMappings", n -> enumValueMapping(n, sourceType));
        if (sourceType != null && w.json().getOr("enumValueMappings", null) instanceof Json.Arr evms
                && evms.items().stream().noneMatch(e -> e instanceof Json.Obj o
                        && o.getOr("sourceValues", null) instanceof Json.Arr sv
                        && EnumSourceValues.readsSourceType(sv.items()))) {
            // no value mapping reads it: the engine drops it (decision C refuses that)
            throw Wire.refuse("an enumeration mapping's sourceType '" + sourceType + "' that none of its values"
                    + " reads: the engine drops it");
        }
        return w.done(new Protocol.PEnumerationMapping(id, enumeration, values, w.span()));
    }

    private static Protocol.PEnumValueMapping enumValueMapping(Json.Node node,
            @com.legend.base.Nullable String sourceType) {
        Wire w = Wire.of(node, "enum value mapping");
        return w.done(new Protocol.PEnumValueMapping(w.str("enumValue"),
                EnumSourceValues.read(w.arr("sourceValues"), sourceType)));
    }

    /** A source value: a string, an integer (a long, as the parser holds it), or an enum value reference. */
    static Protocol.PEnumSourceValue sourceValue(Json.Node node) {
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
