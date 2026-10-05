// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.protocol;

import com.legend.json.Json;

import java.util.List;
import java.util.Map;
import java.util.function.Function;

/**
 * Property mappings read back -- the mirror of {@link MappingEmitter#relPropertyMapping} (relational,
 * embedded, inline-embedded, otherwise-embedded) and of the pure and aggregation-aware property mapping
 * arms. The emitter writes several values twice (an embedded mapping's id as its class mapping's id and
 * its target, its span on both levels; an otherwise mapping's property on both levels): each pair must
 * agree, or there is no record that writes it.
 */
final class PropertyMappingReader {

    private PropertyMappingReader() {
    }

    /** {@code {[class,] property, sourceInformation}}: the property a mapping line names. */
    private record PropertyRef(@com.legend.base.Nullable String ownerClass, String property,
            @com.legend.base.Nullable SourceInfo span) {
    }

    private static PropertyRef property(Json.Node node) {
        Wire p = Wire.of(node, "mapped property");
        return p.done(new PropertyRef(p.optStr("class"), p.str("property"), p.span()));
    }

    /** {@code +prop: Type[m]} -- a mapping-local property. */
    static Protocol.@com.legend.base.Nullable PLocalProp localProperty(Wire w) {
        Wire lp = w.optObj("localMappingProperty");
        if (lp == null) {
            return null;
        }
        Wire m = lp.obj("multiplicity");
        long lower = m.lng("lowerBound");
        Long upper = m.done(m.optLong("upperBound"));
        return lp.done(new Protocol.PLocalProp(lp.str("type"), lower, upper, lp.span()));
    }

    /** {@code "bindingTransformer":{"binding":fqn}}, or none. */
    static @com.legend.base.Nullable String bindingTransformer(Wire w) {
        Wire b = w.optObj("bindingTransformer");
        return b == null ? null : b.done(b.str("binding"));
    }

    // ---------------------------------------------------------------------
    // Relational
    // ---------------------------------------------------------------------

    /** The reader rule for each relational property-mapping {@code _type}. */
    private static final Map<String, Function<Wire, Protocol.PPropertyMapping>> RELATIONAL = Map.of(
            "relationalPropertyMapping", PropertyMappingReader::relationalLine,
            "embeddedPropertyMapping", PropertyMappingReader::embedded,
            "inlineEmbeddedPropertyMapping", PropertyMappingReader::inlineEmbedded,
            "otherwiseEmbeddedPropertyMapping", PropertyMappingReader::otherwiseEmbedded);

    static Protocol.PPropertyMapping relational(Json.Node node) {
        Wire w = Wire.of(node, "relational property mapping");
        return w.done(Wire.rule(RELATIONAL, w.type(), "relational property mapping").apply(w));
    }

    private static Protocol.PPropertyMapping relationalLine(Wire w) {
        PropertyRef p = property(w.take("property"));
        return new Protocol.PRelPropertyMapping(p.ownerClass(), bindingTransformer(w), p.property(), p.span(),
                w.optStr("enumMappingId"), localProperty(w), StoreReader.relOp(w.take("relationalOperation")),
                w.optStr("source"), w.optStr("target"), w.span());
    }

    /** The {@code _type:"embedded"} class mapping an embedded line nests: id, primary key, lines, span. */
    private record Embedded(@com.legend.base.Nullable String id, List<Protocol.PRelOp> primaryKey,
            List<Protocol.PPropertyMapping> lines, @com.legend.base.Nullable SourceInfo span) {
    }

    private static Embedded embeddedClassMapping(Json.Node node) {
        Wire c = Wire.of(node, "embedded class mapping");
        c.constant("_type", "embedded");
        c.constant("root", false);
        return c.done(new Embedded(c.optStr("id"), c.list("primaryKey", StoreReader::relOp),
                c.list("propertyMappings", PropertyMappingReader::relational), c.span()));
    }

    /** {@code prop[k] ( lines )}: one id (class mapping id, line id, target) and one span on both levels. */
    private static Protocol.PPropertyMapping embedded(Wire w) {
        Embedded cm = embeddedClassMapping(w.take("classMapping"));
        String id = w.optStr("id");
        String target = w.optStr("target");
        SourceInfo span = w.span();
        if (!java.util.Objects.equals(cm.id(), id) || !java.util.Objects.equals(id, target)) {
            throw Wire.refuse("an embedded property mapping whose ids differ: " + cm.id() + ", " + id + ", "
                    + target);
        }
        StoreReader.sameSpan(cm.span(), span, "embedded property mapping");
        PropertyRef p = property(w.take("property"));
        return new Protocol.PEmbeddedPropertyMapping(p.ownerClass(), p.property(), p.span(), id, cm.primaryKey(),
                cm.lines(), span);
    }

    private static Protocol.PPropertyMapping inlineEmbedded(Wire w) {
        PropertyRef p = property(w.take("property"));
        return new Protocol.PInlineEmbeddedPropertyMapping(p.ownerClass(), p.property(), p.span(), w.optStr("id"),
                w.str("setImplementationId"), w.span());
    }

    /**
     * {@code prop ( .. ) Otherwise ( [tgt]:<op> )}: the otherwise line repeats the property and carries
     * its operation's span; its target is the {@code [tgt]} id.
     */
    private static Protocol.PPropertyMapping otherwiseEmbedded(Wire w) {
        Embedded cm = embeddedClassMapping(w.take("classMapping"));
        PropertyRef p = property(w.take("property"));
        Wire o = w.obj("otherwisePropertyMapping");
        o.constant("_type", "relationalPropertyMapping");
        PropertyRef op = property(o.take("property"));
        if (!op.equals(p)) {
            throw Wire.refuse("an otherwise mapping whose two property references differ");
        }
        Protocol.PRelOp otherwise = StoreReader.relOp(o.take("relationalOperation"));
        StoreReader.sameSpan(otherwise.sourceInformation(), o.span(), "otherwise property mapping");
        String target = o.done(o.str("target"));
        return new Protocol.POtherwiseEmbeddedPropertyMapping(p.ownerClass(), p.property(), p.span(), cm.id(),
                cm.primaryKey(), cm.lines(), otherwise, target, cm.span(), w.span());
    }

    // ---------------------------------------------------------------------
    // Pure and aggregation aware
    // ---------------------------------------------------------------------

    /** A pure property mapping: the transform is a span-less parameterless lambda. */
    static Protocol.PPurePropertyMapping pure(Json.Node node) {
        Wire w = Wire.of(node, "pure property mapping");
        w.constant("_type", "purePropertyMapping");
        PropertyRef p = property(w.take("property"));
        return w.done(new Protocol.PPurePropertyMapping(p.ownerClass(), p.property(), p.span(),
                w.optStr("enumMappingId"), w.bool("explodeProperty"), localProperty(w),
                ClassMappingReader.bareLambda(w.take("transform"), "pure transform"), w.optStr("source"),
                w.optStr("target"), w.span()));
    }

    /**
     * An aggregation-aware mapping's copy of its pure main mapping's lines: class, property, source and
     * target only (the transform is not on this wire).
     */
    static Protocol.PPurePropertyMapping aggregationAware(Json.Node node) {
        Wire w = Wire.of(node, "aggregation-aware property mapping");
        w.constant("_type", "AggregationAwarePropertyMapping");
        PropertyRef p = property(w.take("property"));
        if (p.ownerClass() == null) {
            throw Wire.refuse("an aggregation-aware property mapping without its class");
        }
        return w.done(new Protocol.PPurePropertyMapping(p.ownerClass(), p.property(), p.span(), null, false, null,
                List.of(), w.optStr("source"), w.optStr("target"), w.span()));
    }
}
