// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.protocol;

import com.legend.json.Json;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import static com.legend.protocol.Composing.TAB;
import static com.legend.protocol.Composing.convertIdentifier;
import static com.legend.protocol.Composing.convertString;
import static com.legend.protocol.Composing.elementPath;
import static com.legend.protocol.Composing.genericType;
import static com.legend.protocol.Composing.items;
import static com.legend.protocol.Composing.multiplicity;
import static com.legend.protocol.Composing.objOr;
import static com.legend.protocol.Composing.objs;
import static com.legend.protocol.Composing.str;
import static com.legend.protocol.Composing.tab;
import static com.legend.protocol.Composing.valueSpecification;

/**
 * The domain's elements as upstream prints them: {@code DEPRECATED_PureGrammarComposerCore}'s
 * profile, enumeration, measure, class, association and function, with
 * {@code HelperDomainGrammarComposer}'s annotations, documentation blocks, properties, derived
 * properties and constraints.
 */
final class DomainComposer {

    private static final String ANY = "meta::pure::metamodel::type::Any";

    private static final Map<String, Function<Json.Obj, String>> PRINTERS = Map.of(
            "profile", DomainComposer::profile,
            "Enumeration", DomainComposer::enumeration,
            "measure", DomainComposer::measure,
            "class", DomainComposer::klass,
            "association", DomainComposer::association,
            "function", DomainComposer::function);

    /** The element {@code _type}s this composer prints. */
    static final List<String> TYPES = List.copyOf(PRINTERS.keySet());

    private DomainComposer() {
    }

    static String element(Json.Obj e) {
        Function<Json.Obj, String> p = PRINTERS.get(Composing.type(e));
        if (p == null) {
            throw Composing.refused("the domain composer has no rule for _type '" + Composing.type(e) + "'");
        }
        return p.apply(e);
    }

    // ---------------------------------------------------------------------
    // Elements
    // ---------------------------------------------------------------------

    private static String profile(Json.Obj profile) {
        StringBuilder b = new StringBuilder("Profile ").append(elementPath(profile)).append("\n{\n");
        List<String> stereotypes = profileValues(profile, "stereotypes");
        if (!stereotypes.isEmpty()) {
            b.append(TAB).append("stereotypes: [").append(String.join(", ", stereotypes)).append("];\n");
        }
        List<String> tags = profileValues(profile, "tags");
        if (!tags.isEmpty()) {
            b.append(TAB).append("tags: [").append(String.join(", ", tags)).append("];\n");
        }
        return b.append("}").toString();
    }

    /** A profile's stereotypes or tags: each {@code {"value":...}} (or a bare string on an older wire). */
    private static List<String> profileValues(Json.Obj profile, String key) {
        List<String> out = new ArrayList<>();
        for (Json.Node n : items(profile, key)) {
            String v = n instanceof Json.Str s ? s.value() : Composing.obj(n, key).getString("value");
            out.add(convertIdentifier(v));
        }
        return out;
    }

    private static String enumeration(Json.Obj e) {
        List<String> values = new ArrayList<>();
        for (Json.Obj v : objs(e, "values")) {
            values.add(TAB + declarationPrefix("", TAB, v) + convertIdentifier(v.getString("value")));
        }
        return declarationPrefix("Enum", "", e) + elementPath(e) + "\n{\n"
                + String.join(",\n", values) + (values.isEmpty() ? "" : "\n") + "}";
    }

    private static String measure(Json.Obj measure) {
        StringBuilder b = new StringBuilder("Measure ").append(elementPath(measure)).append("\n{\n");
        Json.Obj canonical = objOr(measure, "canonicalUnit");
        if (canonical != null) {
            b.append(TAB).append(objOr(canonical, "conversionFunction") != null ? "*" : "").append(unit(canonical)).append("\n");
        }
        List<String> others = new ArrayList<>();
        for (Json.Obj u : objs(measure, "nonCanonicalUnits")) {
            others.add(TAB + unit(u));
        }
        if (!others.isEmpty()) {
            b.append(String.join("\n", others)).append("\n");
        }
        return b.append("}").toString();
    }

    /** {@code renderUnit} and {@code renderUnitLambda}. */
    private static String unit(Json.Obj unit) {
        Json.Obj conversion = objOr(unit, "conversionFunction");
        if (conversion == null) {
            return convertIdentifier(unit.getString("name")) + ";";
        }
        List<String> params = new ArrayList<>();
        for (Json.Obj p : objs(conversion, "parameters")) {
            params.add(p.getString("name"));
        }
        List<String> body = new ArrayList<>();
        for (Json.Node b : items(conversion, "body")) {
            body.add(valueSpecification(b));
        }
        return convertIdentifier(unit.getString("name")) + ": " + String.join(",", params) + " -> " + String.join(";", body) + ";";
    }

    private static String klass(Json.Obj c) {
        StringBuilder b = new StringBuilder(declarationPrefix("Class", "", c)).append(elementPath(c));
        List<String> superTypes = new ArrayList<>();
        for (Json.Node st : items(c, "superTypes")) {
            String path = st instanceof Json.Str s ? s.value() : Composing.obj(st, "super type").getString("path");
            if (!ANY.equals(path)) {
                superTypes.add(path);
            }
        }
        if (!superTypes.isEmpty()) {
            b.append(" extends ").append(String.join(", ", superTypes));
        }
        b.append("\n");
        List<Json.Obj> constraints = objs(c, "constraints");
        if (!constraints.isEmpty()) {
            List<String> cs = new ArrayList<>();
            for (int i = 0; i < constraints.size(); i++) {
                cs.add(TAB + constraint(constraints.get(i), i));
            }
            b.append("[\n").append(String.join(",\n", cs)).append("\n]\n");
        }
        b.append("{\n");
        List<String> properties = properties(c);
        if (!properties.isEmpty()) {
            b.append(String.join("\n", properties)).append("\n");
        }
        List<String> derived = derivedProperties(c);
        if (!derived.isEmpty()) {
            b.append(String.join("\n", derived)).append("\n");
        }
        return b.append("}").toString();
    }

    private static String association(Json.Obj a) {
        List<String> properties = properties(a);
        List<String> derived = derivedProperties(a);
        return declarationPrefix("Association", "", a) + elementPath(a) + "\n{\n"
                + String.join("\n", properties) + (properties.isEmpty() ? "" : "\n")
                + String.join("\n", derived) + (derived.isEmpty() ? "" : "\n")
                + "}";
    }

    private static String function(Json.Obj f) {
        List<String> params = new ArrayList<>();
        for (Json.Node p : items(f, "parameters")) {
            params.add(PureComposer.signatureParameter(p));
        }
        List<Json.Node> bodies = items(f, "body");
        List<String> body = new ArrayList<>();
        for (Json.Node b : bodies) {
            body.add("  " + valueSpecification(b));
        }
        return declarationPrefix("function", "", f) + Composing.convertPath(FunctionNames.functionName(f))
                + "(" + String.join(", ", params) + ")"
                + ": " + genericType(f.getObj("returnGenericType")) + "[" + multiplicity(f.getObj("returnMultiplicity")) + "]\n"
                + "{\n" + String.join(";\n", body) + (bodies.size() > 1 ? ";" : "") + "\n}"
                + FunctionTestComposer.testSuites(f);
    }

    // ---------------------------------------------------------------------
    // Members
    // ---------------------------------------------------------------------

    private static List<String> properties(Json.Obj owner) {
        List<String> out = new ArrayList<>();
        for (Json.Obj p : objs(owner, "properties")) {
            out.add(TAB + property(p) + ";");
        }
        return out;
    }

    private static List<String> derivedProperties(Json.Obj owner) {
        List<String> out = new ArrayList<>();
        for (Json.Obj p : objs(owner, "qualifiedProperties")) {
            out.add(TAB + derivedProperty(p) + ";");
        }
        return out;
    }

    /** {@code renderProperty}. */
    private static String property(Json.Obj p) {
        Json.Obj defaultValue = objOr(p, "defaultValue");
        return declarationPrefix("", TAB, p) + aggregation(str(p, "aggregation")) + convertIdentifier(p.getString("name"))
                + ": " + genericType(p.getObj("genericType")) + "[" + multiplicity(p.getObj("multiplicity")) + "]"
                + (defaultValue != null ? " = " + valueSpecification(defaultValue.get("value")) : "");
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

    /** {@code renderDerivedProperty}. */
    private static String derivedProperty(Json.Obj qp) {
        List<String> params = new ArrayList<>();
        for (Json.Obj p : objs(qp, "parameters")) {
            if (!"this".equals(p.getString("name"))) {
                params.add(PureComposer.signatureParameter(p));
            }
        }
        List<String> body = new ArrayList<>();
        for (Json.Node b : items(qp, "body")) {
            body.add(valueSpecification(b));
        }
        String bodyText = body.size() <= 1
                ? String.join("\n", body)
                : "\n" + tab(2) + String.join(";\n" + tab(2), body) + ";\n" + TAB;
        return declarationPrefix("", TAB, qp) + convertIdentifier(qp.getString("name"))
                + "(" + String.join(", ", params) + ") {" + bodyText + "}: "
                + genericType(qp.getObj("returnGenericType")) + "[" + multiplicity(qp.getObj("returnMultiplicity")) + "]";
    }

    /** {@code renderConstraint}: {@code index} is the constraint's position among its class's. */
    private static String constraint(Json.Obj c, int index) {
        String name = c.getString("name");
        String function = Composing.lambdaBodyText(c.getObj("functionDefinition"), "");
        String enforcement = str(c, "enforcementLevel");
        String externalId = str(c, "externalId");
        Json.Obj message = objOr(c, "messageFunction");
        if (enforcement == null && externalId == null && message == null) {
            return (String.valueOf(index).equals(name) ? "" : convertIdentifier(name) + ": ") + function;
        }
        StringBuilder b = new StringBuilder(name).append('\n').append(TAB).append("(").append('\n');
        String owner = str(c, "owner");
        if (owner != null) {
            b.append(tab(2)).append("~owner: ").append(owner).append('\n');
        }
        if (externalId != null) {
            b.append(tab(2)).append("~externalId: ").append(convertString(externalId, true)).append('\n');
        }
        b.append(tab(2)).append("~function: ").append(function).append('\n');
        if (enforcement != null) {
            b.append(tab(2)).append("~enforcementLevel: ").append(enforcement).append('\n');
        }
        if (message != null) {
            b.append(tab(2)).append("~message: ").append(Composing.lambdaBodyText(message, "")).append('\n');
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
    static String declarationPrefix(String keyword, String indent, Json.Obj annotated) {
        List<Json.Obj> taggedValues = objs(annotated, "taggedValues");
        Json.Obj documentation = documentation(taggedValues);
        List<Json.Obj> rest = new ArrayList<>();
        for (Json.Obj tv : taggedValues) {
            if (tv != documentation) {
                rest.add(tv);
            }
        }
        return (documentation == null ? "" : documentationBlock(taggedValueText(documentation), indent))
                + (keyword.isEmpty() ? "" : keyword + " ")
                + annotations(objs(annotated, "stereotypes"), rest);
    }

    /** {@code renderAnnotations}. */
    static String annotations(List<Json.Obj> stereotypes, List<Json.Obj> taggedValues) {
        StringBuilder b = new StringBuilder();
        if (!stereotypes.isEmpty()) {
            List<String> s = new ArrayList<>();
            for (Json.Obj st : stereotypes) {
                s.add(stereotype(st));
            }
            b.append("<<").append(String.join(", ", s)).append(">> ");
        }
        if (!taggedValues.isEmpty()) {
            List<String> t = new ArrayList<>();
            for (Json.Obj tv : taggedValues) {
                t.add(taggedValue(tv));
            }
            b.append("{").append(String.join(", ", t)).append("} ");
        }
        return b.toString();
    }

    /** {@code renderStereotypePointer}. */
    static String stereotype(Json.Obj st) {
        return Composing.convertPath(st.getString("profile")) + "." + convertIdentifier(st.getString("value"));
    }

    /** {@code renderTaggedValue}. */
    static String taggedValue(Json.Obj tv) {
        Json.Obj tag = tv.getObj("tag");
        return Composing.convertPath(tag.getString("profile")) + "." + convertIdentifier(tag.getString("value"))
                + " = " + convertString(taggedValueText(tv), true);
    }

    /** A tagged value's text: a bare string, or the multi-line form's {@code value}. */
    private static String taggedValueText(Json.Obj tv) {
        Json.Node v = tv.get("value");
        return v instanceof Json.Str s ? s.value() : Composing.obj(v, "tagged value").getString("value");
    }

    private static boolean multiLine(Json.Obj tv) {
        return tv.get("value") instanceof Json.Obj o && o.getBoolOr("multiLine", false);
    }

    /** {@code extractDocumentation}: the one doc tagged value authored multi-line, or null. */
    private static @com.legend.base.Nullable Json.Obj documentation(List<Json.Obj> taggedValues) {
        Json.Obj found = null;
        for (Json.Obj tv : taggedValues) {
            Json.Obj tag = objOr(tv, "tag");
            if (tag != null && Documentation.TAG.equals(str(tag, "value"))
                    && (Documentation.TAG.equals(str(tag, "profile")) || Documentation.PROFILE.equals(str(tag, "profile")))) {
                if (found != null) {
                    return null;
                }
                found = tv;
            }
        }
        return found != null && multiLine(found) && renderableAsDocumentation(taggedValueText(found)) ? found : null;
    }

    /** {@code isRenderableAsDocumentation}. */
    private static boolean renderableAsDocumentation(String value) {
        if (value.isEmpty() || value.contains("'''") || value.indexOf('\r') >= 0 || value.startsWith("\n") || value.endsWith("\n")) {
            return false;
        }
        for (String line : value.split("\n", -1)) {
            if (line.startsWith("###") || (!line.isEmpty() && Character.isWhitespace(line.charAt(line.length() - 1)))) {
                return false;
            }
        }
        return true;
    }

    /** {@code renderDocumentation}'s block. */
    private static String documentationBlock(String value, String indent) {
        StringBuilder b = new StringBuilder("'''\n");
        for (String line : value.split("\n", -1)) {
            b.append(line.isEmpty() ? "" : indent + line).append('\n');
        }
        return b.append(indent).append("'''\n").append(indent).toString();
    }
}
