// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.protocol;

import com.legend.json.Json;
import com.legend.protocol.Protocol.PStereotype;
import com.legend.protocol.Protocol.PTaggedValue;
import com.legend.protocol.spec.ValueSpecification;

import java.util.List;

/**
 * The domain elements read back -- the mirror of {@link ProtocolEmitter}'s {@code class},
 * {@code Enumeration}, {@code association}, {@code profile}, {@code function} and {@code measure}
 * rules, and of the annotations, properties, constraints and type pointers every family shares.
 *
 * <p>What the wire does not carry comes back as the record's absent value, as the parser produces it
 * for text without the clause: a class's type parameters and type variables (the emitter writes
 * neither), a function's pre-constraints (always written empty), and a realization's function
 * reference (the wire carries only the body: a {@link Realization.Inline}).
 */
final class DomainReader {

    private DomainReader() {
    }

    static Protocol.Element pclass(Wire w) {
        List<ConstraintDefinition> constraints = w.list("constraints", DomainReader::constraint);
        w.emptyArray("originalMilestonedProperties");
        return new Protocol.PClass(w.str("package"), w.str("name"), List.of(), List.of(),
                w.list("superTypes", DomainReader::superType), w.list("properties", DomainReader::property),
                w.list("qualifiedProperties", DomainReader::qualifiedProperty), constraints,
                w.list("stereotypes", DomainReader::stereotype), w.list("taggedValues", DomainReader::taggedValue),
                false, w.span());
    }

    static Protocol.Element association(Wire w) {
        w.emptyArray("originalMilestonedProperties");
        return new Protocol.PAssociation(w.str("package"), w.str("name"),
                w.list("properties", DomainReader::property),
                w.list("qualifiedProperties", DomainReader::qualifiedProperty),
                w.list("stereotypes", DomainReader::stereotype), w.list("taggedValues", DomainReader::taggedValue),
                w.span());
    }

    static Protocol.Element profile(Wire w) {
        return new Protocol.PProfile(w.str("package"), w.str("name"),
                w.list("stereotypes", DomainReader::profileEntry), w.list("tags", DomainReader::profileEntry),
                w.span());
    }

    private static Protocol.PProfileEntry profileEntry(Json.Node node) {
        Wire e = Wire.of(node, "profile entry");
        return e.done(new Protocol.PProfileEntry(e.str("value"), e.span()));
    }

    static Protocol.Element enumeration(Wire w) {
        return new Protocol.PEnumeration(w.str("package"), w.str("name"), w.list("values", DomainReader::enumValue),
                w.list("stereotypes", DomainReader::stereotype), w.list("taggedValues", DomainReader::taggedValue),
                w.span());
    }

    private static Protocol.PEnumValue enumValue(Json.Node node) {
        Wire v = Wire.of(node, "enum value");
        return v.done(new Protocol.PEnumValue(v.str("value"), v.list("stereotypes", DomainReader::stereotype),
                v.list("taggedValues", DomainReader::taggedValue), v.span()));
    }

    /**
     * {@code _type:"function"}: the wire name is SIGNATURE-MANGLED ({@link Protocol.PFunction#mangledName});
     * the declared name is what remains once the signature's mangling is taken off the end.
     */
    static Protocol.Element function(Wire w) {
        List<ParameterDefinition> params = w.list("parameters", DomainReader::parameter);
        TypeExpression returnType = ProtocolReader.genericType(w.take("returnGenericType"));
        Multiplicity returnMult = ProtocolReader.multiplicity(w.take("returnMultiplicity"));
        w.emptyArray("preConstraints");
        w.emptyArray("postConstraints");
        String pkg = w.str("package");
        String wireName = w.str("name");
        List<ValueSpecification> body = w.list("body", ProtocolReader::valueSpec);
        List<Protocol.PTestSuite> tests = w.list("tests", FunctionTestReader::testSuite);
        List<PStereotype> stereotypes = w.list("stereotypes", DomainReader::stereotype);
        List<PTaggedValue> taggedValues = w.list("taggedValues", DomainReader::taggedValue);
        String mangling = new Protocol.PFunction(pkg, "", List.of(), List.of(), params, returnType, returnMult,
                body, List.of(), tests, stereotypes, taggedValues, null).mangledName();
        if (!wireName.endsWith(mangling) || wireName.length() == mangling.length()) {
            throw Wire.refuse("function name '" + wireName + "' does not end in its signature's mangling '"
                    + mangling + "'");
        }
        return new Protocol.PFunction(pkg, wireName.substring(0, wireName.length() - mangling.length()), List.of(),
                List.of(), params, returnType, returnMult, body, List.of(), tests, stereotypes, taggedValues,
                w.span());
    }

    /** {@code _type:"measure"}: units with a span-less arrow-lambda conversion. */
    static Protocol.Element measure(Wire w) {
        Json.Node canonical = w.opt("canonicalUnit");
        return new Protocol.PMeasure(w.str("package"), w.str("name"), canonical == null ? null : unit(canonical),
                w.list("nonCanonicalUnits", DomainReader::unit), w.span());
    }

    private static Protocol.PUnit unit(Json.Node node) {
        Wire u = Wire.of(node, "unit");
        String param = null;
        ValueSpecification body = null;
        Wire fn = u.optObj("conversionFunction");
        if (fn != null) {
            fn.constant("_type", "lambda");
            List<ValueSpecification> statements = fn.list("body", ProtocolReader::valueSpec);
            List<Json.Node> params = fn.arr("parameters");
            fn.done(params);
            if (statements.size() != 1 || params.size() != 1) {
                throw Wire.refuse("a unit conversion is one parameter and one statement");
            }
            Wire p = Wire.of(params.get(0), "unit conversion parameter");
            p.constant("_type", "var");
            param = p.done(p.str("name"));
            body = statements.get(0);
        }
        return u.done(new Protocol.PUnit(u.str("name"), u.str("measure"), param, body, u.span()));
    }

    // ---------------------------------------------------------------------
    // Members
    // ---------------------------------------------------------------------

    /** A simple property; the default value's outer span covers the whole expression. */
    static Protocol.PProperty property(Json.Node node) {
        Wire p = Wire.of(node, "property");
        Protocol.PDefaultValue dv = null;
        Wire d = p.optObj("defaultValue");
        if (d != null) {
            dv = d.done(new Protocol.PDefaultValue(ProtocolReader.valueSpec(d.take("value")), d.span()));
        }
        return p.done(new Protocol.PProperty(p.str("name"), ProtocolReader.genericType(p.take("genericType")),
                ProtocolReader.multiplicity(p.take("multiplicity")), p.list("stereotypes", DomainReader::stereotype),
                p.list("taggedValues", DomainReader::taggedValue), p.span(), dv, p.optStr("aggregation")));
    }

    /** A qualified (derived) property: the body is the bare statement list, the parameters typed vars. */
    static DerivedPropertyDefinition qualifiedProperty(Json.Node node) {
        Wire q = Wire.of(node, "qualified property");
        return q.done(new DerivedPropertyDefinition(q.str("name"), q.list("parameters", DomainReader::parameter),
                new Realization.Inline(q.list("body", ProtocolReader::valueSpec)),
                ProtocolReader.genericType(q.take("returnGenericType")),
                ProtocolReader.multiplicity(q.take("returnMultiplicity")),
                q.list("stereotypes", DomainReader::stereotype), q.list("taggedValues", DomainReader::taggedValue),
                q.span()));
    }

    /** A typed parameter: {@code {"_type":"var","genericType":..,"multiplicity":..,"name":..,"sourceInformation":..}}. */
    static ParameterDefinition parameter(Json.Node node) {
        Wire v = Wire.of(node, "parameter");
        v.constant("_type", "var");
        return v.done(new ParameterDefinition(v.str("name"), ProtocolReader.genericType(v.take("genericType")),
                ProtocolReader.multiplicity(v.take("multiplicity")), v.span()));
    }

    /**
     * A class constraint: the predicate (and the {@code ~message}) wrapped in the engine's lambda whose
     * synthesised {@code $this} parameter carries {@code [1]} and no span.
     */
    static ConstraintDefinition constraint(Json.Node node) {
        Wire c = Wire.of(node, "constraint");
        List<ValueSpecification> body = thisLambda(c.take("functionDefinition"));
        Json.Node msg = c.opt("messageFunction");
        ValueSpecification message = null;
        if (msg != null) {
            List<ValueSpecification> m = thisLambda(msg);
            if (m.size() != 1) {
                throw Wire.refuse("a constraint message of " + m.size() + " statements");
            }
            message = m.get(0);
        }
        return c.done(new ConstraintDefinition(c.str("name"), new Realization.Inline(body), message,
                c.optStr("enforcementLevel"), c.optStr("externalId"), c.optStr("owner"), c.span()));
    }

    private static List<ValueSpecification> thisLambda(Json.Node node) {
        Wire l = Wire.of(node, "constraint lambda");
        l.constant("_type", "lambda");
        List<ValueSpecification> body = l.list("body", ProtocolReader::valueSpec);
        List<Json.Node> params = l.arr("parameters");
        if (params.size() != 1) {
            throw Wire.refuse("a constraint lambda with " + params.size() + " parameters");
        }
        Wire p = Wire.of(params.get(0), "constraint $this");
        p.constant("_type", "var");
        p.constant("name", "this");
        Multiplicity m = ProtocolReader.multiplicity(p.take("multiplicity"));
        if (!m.equals(new Multiplicity.Concrete(1, 1))) {
            throw Wire.refuse("a constraint $this of multiplicity " + m);
        }
        p.done(m);
        return l.done(body);
    }

    /** {@code {"path":..,"sourceInformation":..,"type":"CLASS"}}: the type arguments are not on the wire. */
    private static Protocol.PSuperType superType(Json.Node node) {
        Wire s = Wire.of(node, "super type");
        s.constant("type", "CLASS");
        SourceInfo span = s.span();
        return s.done(new Protocol.PSuperType(new TypeExpression.NameRef(s.str("path"), span), span));
    }

    // ---------------------------------------------------------------------
    // Annotations and pointers (every family)
    // ---------------------------------------------------------------------

    static PStereotype stereotype(Json.Node node) {
        Wire s = Wire.of(node, "stereotype");
        return s.done(new PStereotype(s.str("profile"), s.str("value"), s.span("profileSourceInformation"),
                s.span()));
    }

    /** A tagged value: a bare string, or the {@code multiLine} string object a {@code '''} block writes. */
    static PTaggedValue taggedValue(Json.Node node) {
        Wire t = Wire.of(node, "tagged value");
        Wire tag = t.obj("tag");
        Protocol.PTag ptag = tag.done(new Protocol.PTag(tag.str("profile"), tag.str("value"),
                tag.span("profileSourceInformation"), tag.span()));
        Json.Node v = t.take("value");
        if (v instanceof Json.Obj) {
            Wire s = Wire.of(v, "tagged value string");
            s.constant("_type", "string");
            s.constant("multiLine", true);
            return t.done(new PTaggedValue(ptag, s.done(s.str("value")), true, t.span()));
        }
        return t.done(new PTaggedValue(ptag, t.asStr(v, "value"), false, t.span()));
    }

    /** A typed packageable-element pointer {@code {"path":..,"sourceInformation":..,"type":..}}. */
    static Protocol.PPointer pointer(Json.Node node) {
        Wire p = Wire.of(node, "pointer");
        return p.done(new Protocol.PPointer(p.str("type"), p.str("path"), p.span()));
    }
}
