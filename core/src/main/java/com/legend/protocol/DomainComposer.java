// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.protocol;

import com.legend.protocol.spec.ValueSpecification;
import com.legend.protocol.spec.Variable;

import java.util.ArrayList;
import java.util.List;

import static com.legend.protocol.Composing.TAB;
import static com.legend.protocol.Composing.convertIdentifier;
import static com.legend.protocol.Composing.convertString;
import static com.legend.protocol.Composing.genericType;
import static com.legend.protocol.Composing.multiplicity;
import static com.legend.protocol.Composing.tab;
import static com.legend.protocol.Composing.valueSpecification;

/**
 * The domain's elements as upstream prints them: {@code DEPRECATED_PureGrammarComposerCore}'s
 * profile, enumeration, measure, class, association and function, with
 * {@code HelperDomainGrammarComposer}'s annotations, documentation blocks, properties, derived
 * properties and constraints -- over the records ({@link Protocol.PProfile}, {@link Protocol.PEnumeration},
 * {@link Protocol.PMeasure}, {@link Protocol.PClass}, {@link Protocol.PAssociation}, {@link Protocol.PFunction}; the
 * protocol program's leg 2, step 3).
 */
final class DomainComposer {

    private static final String ANY = "meta::pure::metamodel::type::Any";

    /** The element {@code _type}s this composer prints. */
    static final List<String> TYPES = List.of("profile", "Enumeration", "measure", "class", "association", "function");

    private DomainComposer() {
    }

    static String element(Protocol.Element e, PureComposer.Style style) {
        return switch (e) {
            case Protocol.PProfile p -> profile(p);
            case Protocol.PEnumeration en -> enumeration(en);
            case Protocol.PMeasure m -> measure(m, style);
            case Protocol.PClass c -> klass(c, style);
            case Protocol.PAssociation a -> association(a, style);
            case Protocol.PFunction f -> function(f, style);
            default -> throw Composing.refused("the domain composer has no rule for a " + e.getClass().getSimpleName());
        };
    }

    // ---------------------------------------------------------------------
    // Elements
    // ---------------------------------------------------------------------

    private static String profile(Protocol.PProfile profile) {
        StringBuilder b = new StringBuilder("Profile ").append(Composing.elementPath(profile.pkg(), profile.name()))
                .append("\n{\n");
        if (!profile.stereotypes().isEmpty()) {
            b.append(TAB).append("stereotypes: [").append(profileValues(profile.stereotypes())).append("];\n");
        }
        if (!profile.tags().isEmpty()) {
            b.append(TAB).append("tags: [").append(profileValues(profile.tags())).append("];\n");
        }
        return b.append("}").toString();
    }

    private static String profileValues(List<Protocol.PProfileEntry> entries) {
        List<String> out = new ArrayList<>();
        for (Protocol.PProfileEntry e : entries) {
            out.add(convertIdentifier(e.value()));
        }
        return String.join(", ", out);
    }

    private static String enumeration(Protocol.PEnumeration e) {
        List<String> values = new ArrayList<>();
        for (Protocol.PEnumValue v : e.values()) {
            values.add(TAB + declarationPrefix("", TAB, v.stereotypes(), v.taggedValues()) + convertIdentifier(v.value()));
        }
        return declarationPrefix("Enum", "", e.stereotypes(), e.taggedValues()) + Composing.elementPath(e.pkg(), e.name())
                + "\n{\n" + String.join(",\n", values) + (values.isEmpty() ? "" : "\n") + "}";
    }

    private static String measure(Protocol.PMeasure measure, PureComposer.Style style) {
        StringBuilder b = new StringBuilder("Measure ").append(Composing.elementPath(measure.pkg(), measure.name()))
                .append("\n{\n");
        Protocol.PUnit canonical = measure.canonicalUnit();
        if (canonical != null) {
            b.append(TAB).append(canonical.body() != null ? "*" : "").append(unit(canonical, style)).append("\n");
        }
        List<String> others = new ArrayList<>();
        for (Protocol.PUnit u : measure.nonCanonicalUnits()) {
            others.add(TAB + unit(u, style));
        }
        if (!others.isEmpty()) {
            b.append(String.join("\n", others)).append("\n");
        }
        return b.append("}").toString();
    }

    /** {@code renderUnit} and {@code renderUnitLambda}: a conversion is one parameter and one statement. */
    private static String unit(Protocol.PUnit unit, PureComposer.Style style) {
        ValueSpecification body = unit.body();
        if (body == null) {
            return convertIdentifier(unit.name()) + ";";
        }
        return convertIdentifier(unit.name()) + ": " + unit.paramName() + " -> " + valueSpecification(body, style) + ";";
    }

    private static String klass(Protocol.PClass c, PureComposer.Style style) {
        StringBuilder b = new StringBuilder(declarationPrefix("Class", "", c.stereotypes(), c.taggedValues()))
                .append(Composing.elementPath(c.pkg(), c.name()));
        List<String> superTypes = new ArrayList<>();
        for (Protocol.PSuperType st : c.superTypes()) {
            if (!(st.type() instanceof TypeExpression.NameRef ref)) {
                throw Composing.refused("a super type that is not a class name: " + st.type());
            }
            if (!ANY.equals(ref.name())) {
                superTypes.add(ref.name());
            }
        }
        if (!superTypes.isEmpty()) {
            b.append(" extends ").append(String.join(", ", superTypes));
        }
        b.append("\n");
        List<ConstraintDefinition> constraints = c.constraints();
        if (!constraints.isEmpty()) {
            List<String> cs = new ArrayList<>();
            for (int i = 0; i < constraints.size(); i++) {
                cs.add(TAB + constraint(constraints.get(i), i, style));
            }
            b.append("[\n").append(String.join(",\n", cs)).append("\n]\n");
        }
        b.append("{\n");
        List<String> properties = properties(c.properties(), style);
        if (!properties.isEmpty()) {
            b.append(String.join("\n", properties)).append("\n");
        }
        List<String> derived = derivedProperties(c.derivedProperties(), style);
        if (!derived.isEmpty()) {
            b.append(String.join("\n", derived)).append("\n");
        }
        return b.append("}").toString();
    }

    private static String association(Protocol.PAssociation a, PureComposer.Style style) {
        List<String> properties = properties(a.properties(), style);
        List<String> derived = derivedProperties(a.derivedProperties(), style);
        return declarationPrefix("Association", "", a.stereotypes(), a.taggedValues())
                + Composing.elementPath(a.pkg(), a.name()) + "\n{\n"
                + String.join("\n", properties) + (properties.isEmpty() ? "" : "\n")
                + String.join("\n", derived) + (derived.isEmpty() ? "" : "\n")
                + "}";
    }

    /** The function under its declared name: the reader has taken the wire name's signature mangling off. */
    private static String function(Protocol.PFunction f, PureComposer.Style style) {
        List<String> params = new ArrayList<>();
        for (ParameterDefinition p : f.parameters()) {
            params.add(parameter(p));
        }
        List<String> body = new ArrayList<>();
        // upstream: withIndentation(getTabSize(1))
        for (String statement : PureComposer.statements(f.body(), style, Composing.indented("", 2, style))) {
            body.add("  " + statement);
        }
        return declarationPrefix("function", "", f.stereotypes(), f.taggedValues())
                + Composing.convertPath(f.qualifiedName())
                + "(" + String.join(", ", params) + ")"
                + ": " + genericType(f.returnType()) + "[" + multiplicity(f.returnMultiplicity()) + "]\n"
                + "{\n" + String.join(";\n", body) + (f.body().size() > 1 ? ";" : "") + "\n}"
                + FunctionTestComposer.testSuites(f, style);
    }

    /** A declared parameter, printed as the typed variable it is on the wire. */
    private static String parameter(ParameterDefinition p) {
        return PureComposer.signatureParameter(new Variable(p.name(), p.type(), p.multiplicity()));
    }

    // ---------------------------------------------------------------------
    // Members
    // ---------------------------------------------------------------------

    private static List<String> properties(List<Protocol.PProperty> properties, PureComposer.Style style) {
        List<String> out = new ArrayList<>();
        for (Protocol.PProperty p : properties) {
            out.add(TAB + property(p, style) + ";");
        }
        return out;
    }

    private static List<String> derivedProperties(List<DerivedPropertyDefinition> properties, PureComposer.Style style) {
        List<String> out = new ArrayList<>();
        for (DerivedPropertyDefinition p : properties) {
            out.add(TAB + derivedProperty(p, style) + ";");
        }
        return out;
    }

    /** {@code renderProperty}. */
    private static String property(Protocol.PProperty p, PureComposer.Style style) {
        Protocol.PDefaultValue defaultValue = p.defaultValue();
        String value = "";
        if (defaultValue != null) {
            if (defaultValue.value() == null) {
                throw Composing.refused("property '" + p.name() + "' has a default value with no expression");
            }
            value = " = " + valueSpecification(defaultValue.value(), style);
        }
        return declarationPrefix("", TAB, p.stereotypes(), p.taggedValues()) + aggregation(p.aggregation())
                + convertIdentifier(p.name()) + ": " + genericType(p.type()) + "[" + multiplicity(p.multiplicity()) + "]"
                + value;
    }

    private static String aggregation(@com.legend.base.Nullable String kind) {
        if (kind == null) {
            return "";
        }
        return switch (kind) {
            case "NONE" -> "(none) ";
            case "SHARED" -> "(shared) ";
            case "COMPOSITE" -> "(composite) ";
            default -> throw Composing.refused("unknown aggregation kind '" + kind + "'");
        };
    }

    /** {@code renderDerivedProperty}: its parameters less any {@code this}. */
    private static String derivedProperty(DerivedPropertyDefinition qp, PureComposer.Style style) {
        List<String> params = new ArrayList<>();
        for (ParameterDefinition p : qp.parameters()) {
            if (!"this".equals(p.name())) {
                params.add(parameter(p));
            }
        }
        List<String> body = PureComposer.statements(inline(qp.realization(), "derived property '" + qp.name() + "'"),
                style, "");
        String bodyText = body.size() <= 1
                ? String.join("\n", body)
                : "\n" + tab(2) + String.join(";\n" + tab(2), body) + ";\n" + TAB;
        return declarationPrefix("", TAB, qp.stereotypes(), qp.taggedValues()) + convertIdentifier(qp.name())
                + "(" + String.join(", ", params) + ") {" + bodyText + "}: "
                + genericType(qp.type()) + "[" + multiplicity(qp.multiplicity()) + "]";
    }

    /** An inline body: the wire carries only the body, so a function-ref binding has nothing to print. */
    private static List<ValueSpecification> inline(Realization r, String what) {
        if (!(r instanceof Realization.Inline inl)) {
            throw Composing.refused(what + " bound to a function, not written inline (the wire carries a body only)");
        }
        return inl.body();
    }

    /** {@code renderConstraint}: {@code index} is the constraint's position among its class's. */
    private static String constraint(ConstraintDefinition c, int index, PureComposer.Style style) {
        String name = c.name();
        String function = Composing.lambdaBodyText(inline(c.realization(), "constraint '" + name + "'"), style, "");
        String enforcement = c.enforcementLevel();
        String externalId = c.externalId();
        ValueSpecification message = c.message();
        if (enforcement == null && externalId == null && message == null) {
            return (String.valueOf(index).equals(name) ? "" : convertIdentifier(name) + ": ") + function;
        }
        StringBuilder b = new StringBuilder(name).append('\n').append(TAB).append("(").append('\n');
        if (c.owner() != null) {
            b.append(tab(2)).append("~owner: ").append(c.owner()).append('\n');
        }
        if (externalId != null) {
            b.append(tab(2)).append("~externalId: ").append(convertString(externalId, true)).append('\n');
        }
        b.append(tab(2)).append("~function: ").append(function).append('\n');
        if (enforcement != null) {
            b.append(tab(2)).append("~enforcementLevel: ").append(enforcement).append('\n');
        }
        if (message != null) {
            b.append(tab(2)).append("~message: ").append(Composing.lambdaBodyText(List.of(message), style, "")).append('\n');
        }
        return b.append(TAB).append(")").toString();
    }

    // ---------------------------------------------------------------------
    // Annotations and documentation
    // ---------------------------------------------------------------------

    /**
     * {@code renderDeclarationPrefix}: the documentation block when the element has one to promote,
     * the keyword, then the annotations less the promoted value.
     */
    static String declarationPrefix(String keyword, String indent, List<Protocol.PStereotype> stereotypes,
            List<Protocol.PTaggedValue> taggedValues) {
        return documentationOnly(taggedValues, indent)
                + (keyword.isEmpty() ? "" : keyword + " ")
                + annotations(stereotypes, withoutDocumentation(taggedValues));
    }

    /** {@code renderDocumentation}: the block for the one doc tagged value to promote, or {@code ""}. */
    static String documentationOnly(List<Protocol.PTaggedValue> taggedValues, String indent) {
        Protocol.PTaggedValue documentation = documentation(taggedValues);
        return documentation == null ? "" : documentationBlock(documentation.value(), indent);
    }

    /** {@code withoutDocumentation}: the tagged values less the one promoted to a block. */
    static List<Protocol.PTaggedValue> withoutDocumentation(List<Protocol.PTaggedValue> taggedValues) {
        Protocol.PTaggedValue documentation = documentation(taggedValues);
        List<Protocol.PTaggedValue> rest = new ArrayList<>();
        for (Protocol.PTaggedValue tv : taggedValues) {
            if (tv != documentation) {
                rest.add(tv);
            }
        }
        return rest;
    }

    /** {@code renderAnnotations}. */
    static String annotations(List<Protocol.PStereotype> stereotypes, List<Protocol.PTaggedValue> taggedValues) {
        StringBuilder b = new StringBuilder();
        if (!stereotypes.isEmpty()) {
            List<String> s = new ArrayList<>();
            for (Protocol.PStereotype st : stereotypes) {
                s.add(stereotype(st));
            }
            b.append("<<").append(String.join(", ", s)).append(">> ");
        }
        if (!taggedValues.isEmpty()) {
            List<String> t = new ArrayList<>();
            for (Protocol.PTaggedValue tv : taggedValues) {
                t.add(taggedValue(tv));
            }
            b.append("{").append(String.join(", ", t)).append("} ");
        }
        return b.toString();
    }

    /** {@code renderStereotypePointer}. */
    static String stereotype(Protocol.PStereotype st) {
        return Composing.convertPath(st.profile()) + "." + convertIdentifier(st.value());
    }

    /** {@code renderTaggedValue}. */
    static String taggedValue(Protocol.PTaggedValue tv) {
        return Composing.convertPath(tv.tag().profile()) + "." + convertIdentifier(tv.tag().value())
                + " = " + convertString(tv.value(), true);
    }

    /** {@code extractDocumentation}: the one doc tagged value authored multi-line, or null. */
    private static @com.legend.base.Nullable Protocol.PTaggedValue documentation(List<Protocol.PTaggedValue> taggedValues) {
        Protocol.PTaggedValue found = null;
        for (Protocol.PTaggedValue tv : taggedValues) {
            Protocol.PTag tag = tv.tag();
            if (Documentation.TAG.equals(tag.value())
                    && (Documentation.TAG.equals(tag.profile()) || Documentation.PROFILE.equals(tag.profile()))) {
                if (found != null) {
                    return null;
                }
                found = tv;
            }
        }
        return found != null && found.multiLine() && renderableAsDocumentation(found.value()) ? found : null;
    }

    /** {@code isRenderableAsDocumentation}. */
    private static boolean renderableAsDocumentation(String value) {
        if (value.isEmpty() || value.contains("'''") || value.indexOf('\r') >= 0 || value.startsWith("\n") || value.endsWith("\n")) {
            return false;
        }
        for (String line : Composing.lines(value)) {
            if (line.startsWith("###") || (!line.isEmpty() && Character.isWhitespace(line.charAt(line.length() - 1)))) {
                return false;
            }
        }
        return true;
    }

    /** {@code renderDocumentation}'s block. */
    private static String documentationBlock(String value, String indent) {
        StringBuilder b = new StringBuilder("'''\n");
        for (String line : Composing.lines(value)) {
            b.append(line.isEmpty() ? "" : indent + line).append('\n');
        }
        return b.append(indent).append("'''\n").append(indent).toString();
    }
}
