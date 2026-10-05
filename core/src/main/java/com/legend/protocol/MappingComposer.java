// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.protocol;

import com.legend.json.Json;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static com.legend.protocol.Composing.TAB;
import static com.legend.protocol.Composing.convertIdentifier;
import static com.legend.protocol.Composing.convertString;
import static com.legend.protocol.Composing.elementPath;
import static com.legend.protocol.Composing.items;
import static com.legend.protocol.Composing.objOr;
import static com.legend.protocol.Composing.objs;
import static com.legend.protocol.Composing.str;
import static com.legend.protocol.Composing.tab;

/**
 * A {@code ###Mapping} mapping as upstream prints it ({@code DEPRECATED_PureGrammarComposerCore.visit(Mapping)}
 * and {@code HelperMappingGrammarComposer}): includes, class mappings (Pure, operation, aggregation-aware,
 * relation function, and the stores' through their composers), association and enumeration mappings,
 * legacy tests and test suites.
 *
 * <p>Upstream's printer keeps a mutable "base tab level" for a class mapping nested in an aggregation-aware
 * one; here it is the {@code level} argument.
 */
final class MappingComposer {

    /** A class mapping's body printer, at a base tab level. */
    interface Body {
        String print(Json.Obj classMapping, int level);
    }

    /** A property mapping's printer, at a base tab level. */
    interface PropertyPrinter {
        String print(Json.Obj propertyMapping, int level);
    }

    private static final Map<String, Body> CLASS_MAPPINGS = Map.of(
            "pureInstance", MappingComposer::pureInstance,
            "operation", (cm, level) -> operation(cm),
            "mergeOperation", (cm, level) -> operation(cm),
            "aggregationAware", (cm, level) -> aggregationAware(cm),
            "relation", MappingComposer::relationFunction,
            "relational", (cm, level) -> RelationalMappingComposer.classMapping(cm));

    private static final Map<String, PropertyPrinter> PROPERTY_MAPPINGS = Map.of(
            "purePropertyMapping", (pm, level) -> purePropertyMapping(pm),
            "relationFunctionPropertyMapping", (pm, level) -> relationFunctionPropertyMapping(pm),
            "relationFunctionEmbeddedPropertyMapping", MappingComposer::relationFunctionEmbedded);

    private MappingComposer() {
    }

    static String mapping(Json.Obj mapping) {
        StringBuilder b = new StringBuilder("Mapping ").append(elementPath(mapping)).append("\n(\n");
        boolean empty = true;
        List<String> includes = new ArrayList<>();
        for (Json.Obj include : objs(mapping, "includedMappings")) {
            includes.add(TAB + include(include));
        }
        if (!includes.isEmpty()) {
            empty = false;
            b.append(String.join("\n", includes)).append("\n");
        }
        List<String> classMappings = new ArrayList<>();
        for (Json.Obj cm : objs(mapping, "classMappings")) {
            String ext = str(cm, "extendsClassMappingId");
            classMappings.add(TAB + (cm.getBoolOr("root", false) ? "*" : "") + cm.getString("class") + mappingId(str(cm, "id"))
                    + (ext != null ? " extends " + mappingId(ext) : "") + classMappingBody(cm, 1));
        }
        if (!classMappings.isEmpty()) {
            b.append(empty ? "" : "\n");
            empty = false;
            b.append(String.join("\n", classMappings)).append("\n");
        }
        List<String> associations = new ArrayList<>();
        for (Json.Obj am : objs(mapping, "associationMappings")) {
            associations.add(TAB + associationMapping(am));
        }
        if (!associations.isEmpty()) {
            b.append(empty ? "" : "\n");
            empty = false;
            b.append(String.join("\n", associations)).append("\n");
        }
        List<String> enumerations = new ArrayList<>();
        for (Json.Obj em : objs(mapping, "enumerationMappings")) {
            enumerations.add(TAB + enumerationMapping(em));
        }
        if (!enumerations.isEmpty()) {
            b.append(empty ? "" : "\n");
            empty = false;
            b.append(String.join("\n", enumerations)).append("\n");
        }
        List<Json.Obj> tests = objs(mapping, "tests");
        if (!tests.isEmpty()) {
            b.append(empty ? "" : "\n");
            List<String> ts = new ArrayList<>();
            for (Json.Obj t : tests) {
                ts.add(TAB + MappingTestComposer.legacyTest(t));
            }
            b.append(TAB).append("MappingTests\n").append(TAB).append("[\n").append(String.join(",\n", ts)).append("\n")
                    .append(TAB).append("]\n");
        }
        List<Json.Obj> suites = objs(mapping, "testSuites");
        if (!suites.isEmpty()) {
            b.append(empty ? "" : "\n");
            List<String> ss = new ArrayList<>();
            for (Json.Obj s : suites) {
                ss.add(TAB + MappingTestComposer.testSuite(s));
            }
            b.append(TAB).append("testSuites:\n").append(TAB).append("[\n").append(String.join(",\n", ss)).append("\n")
                    .append(TAB).append("]\n");
        }
        return b.append(")").toString();
    }

    private static String include(Json.Obj include) {
        String type = Composing.type(include);
        if ("mappingIncludeMapping".equals(type)) {
            String source = str(include, "sourceDatabasePath");
            String target = str(include, "targetDatabasePath");
            return "include mapping " + include.getString("includedMapping")
                    + (source != null && target != null ? "[" + Composing.convertPath(source) + "->" + Composing.convertPath(target) + "]" : "");
        }
        if ("mappingIncludeDataSpace".equals(type)) {
            return "include dataspace " + include.getString("includedDataSpace");
        }
        throw Composing.refused("no composer rule for a mapping include of _type '" + type + "'");
    }

    static String mappingId(@com.legend.base.Nullable String id) {
        return id != null ? "[" + id + "]" : "";
    }

    /** A class mapping's body, from its {@code :} on. */
    static String classMappingBody(Json.Obj cm, int level) {
        Body body = CLASS_MAPPINGS.get(Composing.type(cm));
        if (body == null) {
            throw Composing.refused("no composer rule for a class mapping of _type '" + Composing.type(cm) + "'");
        }
        return body.print(cm, level);
    }

    private static String propertyMapping(Json.Obj pm, int level) {
        PropertyPrinter p = PROPERTY_MAPPINGS.get(Composing.type(pm));
        if (p == null) {
            throw Composing.refused("no composer rule for a property mapping of _type '" + Composing.type(pm) + "'");
        }
        return p.print(pm, level);
    }

    private static String pureInstance(Json.Obj cm, int level) {
        Json.Obj filter = objOr(cm, "filter");
        String pureFilter = filter == null ? "" : tab(2) + "~filter " + Composing.lambdaBodyText(filter, "") + "\n";
        String src = str(cm, "srcClass");
        List<String> pms = new ArrayList<>();
        for (Json.Obj pm : objs(cm, "propertyMappings")) {
            pms.add(tab(level + 1) + propertyMapping(pm, level));
        }
        return ": Pure\n" + tab(level) + "{\n"
                + (src == null ? "" : tab(level + 1) + "~src " + src + "\n") + pureFilter
                + String.join(",\n", pms) + (pms.isEmpty() ? "" : "\n")
                + tab(level) + "}";
    }

    /** {@code renderPossibleLocalMappingProperty}. */
    static String localMappingProperty(Json.Obj pm) {
        Json.Obj local = objOr(pm, "localMappingProperty");
        String property = convertIdentifier(pm.getObj("property").getString("property"));
        return local == null ? property
                : "+" + property + ": " + local.getString("type") + "[" + Composing.multiplicity(local.getObj("multiplicity")) + "]";
    }

    private static String purePropertyMapping(Json.Obj pm) {
        String target = str(pm, "target");
        String enumMapping = str(pm, "enumMappingId");
        return localMappingProperty(pm)
                + (pm.getBoolOr("explodeProperty", false) ? "*" : "")
                + (target == null || target.isEmpty() ? "" : "[" + convertIdentifier(target) + "]")
                + (enumMapping == null ? "" : ": EnumerationMapping " + enumMapping)
                + ": " + Composing.lambdaBodyText(pm.getObj("transform"), "");
    }

    private static String operation(Json.Obj cm) {
        String op = str(cm, "operation");
        if (op == null) {
            throw Composing.refused("an operation class mapping with no operation (upstream cannot name its function)");
        }
        String function = MappingOperation.valueOf(op).function;
        String params = String.join(",", cm.getStringArrayOr("parameters", List.of()));
        String call = "mergeOperation".equals(Composing.type(cm))
                ? function + "([" + params + "]," + Composing.valueSpecification(items(cm.getObj("validationFunction"), "body").get(0)) + ")"
                : function + "(" + params + ")";
        return ": Operation\n" + TAB + "{\n" + tab(2) + call + "\n" + TAB + "}";
    }

    private static String aggregationAware(Json.Obj cm) {
        Json.Obj main = objOr(cm, "mainSetImplementation");
        String mainMapping = main == null ? "" : "~mainMapping" + classMappingBody(main, 2);
        List<String> views = new ArrayList<>();
        for (Json.Obj agg : objs(cm, "aggregateSetImplementations")) {
            views.add(tab(3) + aggregateSetImplementation(agg));
        }
        return ": AggregationAware \n" + TAB + "{\n"
                + tab(2) + "Views: [\n" + String.join(",\n", views) + tab(2) + "],\n"
                + tab(2) + mainMapping + "\n" + TAB + "}";
    }

    /** {@code renderAggregateSetImplementationContainer}. */
    private static String aggregateSetImplementation(Json.Obj agg) {
        Json.Obj set = objOr(agg, "setImplementation");
        String aggregateMapping = set == null ? "" : "~aggregateMapping" + classMappingBody(set, 4);
        Json.Obj spec = agg.getObj("aggregateSpecification");
        List<String> groupBy = new ArrayList<>();
        for (Json.Obj g : objs(spec, "groupByFunctions")) {
            groupBy.add(tab(6) + firstBody(g.getObj("groupByFn")));
        }
        List<String> values = new ArrayList<>();
        for (Json.Obj v : objs(spec, "aggregateValues")) {
            values.add(tab(6) + "( ~mapFn:" + firstBody(v.getObj("mapFn")) + " , ~aggregateFn: " + firstBody(v.getObj("aggregateFn")) + " )");
        }
        return "(\n"
                + tab(4) + "~modelOperation: {\n"
                + tab(5) + "~canAggregate " + (spec.getBoolOr("canAggregate", false) ? "true" : "false") + ",\n"
                + tab(5) + "~groupByFunctions (\n" + String.join(",\n", groupBy) + "\n" + tab(5) + "),\n"
                + tab(5) + "~aggregateValues (\n" + String.join(",\n", values) + "\n" + tab(5) + ")\n"
                + tab(4) + "},\n"
                + tab(4) + aggregateMapping + "\n" + tab(3) + ")\n";
    }

    private static String firstBody(Json.Obj lambda) {
        return Composing.valueSpecification(items(lambda, "body").get(0));
    }

    private static String relationFunction(Json.Obj cm, int level) {
        List<String> primaryKey = cm.getStringArrayOr("primaryKey", List.of());
        String pk = "";
        if (!primaryKey.isEmpty()) {
            List<String> ks = new ArrayList<>();
            for (String k : primaryKey) {
                ks.add(convertIdentifier(k));
            }
            pk = tab(level + 1) + "~primaryKey: " + (ks.size() == 1 ? ks.get(0) : "[" + String.join(", ", ks) + "]") + "\n";
        }
        Json.Obj function = objOr(cm, "relationFunction");
        Json.Obj source = objOr(cm, "sourceLambda");
        String sourceLine = function != null ? tab(level + 1) + "~func " + function.getString("path") + "\n"
                : source != null ? tab(level + 1) + "~src " + relationLambdaBody(source) + "\n" : "";
        List<String> pms = new ArrayList<>();
        for (Json.Obj pm : objs(cm, "propertyMappings")) {
            pms.add(tab(level + 1) + propertyMapping(pm, level));
        }
        return ": Relation\n" + tab(level) + "{\n" + sourceLine + pk
                + String.join(",\n", pms) + (pms.isEmpty() ? "" : "\n") + tab(level) + "}";
    }

    /** {@code renderRelationLambdaBody}: the body alone when it is one expression with no parameters. */
    private static String relationLambdaBody(Json.Obj lambda) {
        List<Json.Node> body = items(lambda, "body");
        if (body.isEmpty()) {
            return "";
        }
        if (body.size() == 1 && items(lambda, "parameters").isEmpty()) {
            return Composing.valueSpecification(body.get(0));
        }
        return Composing.valueSpecification(lambda);
    }

    private static String relationFunctionPropertyMapping(Json.Obj pm) {
        Json.Obj valueFn = objOr(pm, "valueFn");
        String rhs = valueFn != null ? relationLambdaBody(valueFn) : convertIdentifier(pm.getString("column"));
        Json.Obj binding = objOr(pm, "bindingTransformer");
        String enumMapping = str(pm, "enumMappingId");
        return localMappingProperty(pm)
                + (binding != null ? ": Binding " + binding.getString("binding") + " " : "")
                + (enumMapping != null ? ": EnumerationMapping " + enumMapping + " " : "")
                + ": " + rhs;
    }

    private static String relationFunctionEmbedded(Json.Obj pm, int level) {
        String property = pm.getObj("property").getString("property");
        List<Json.Obj> nested = objs(pm, "propertyMappings");
        String id = str(pm, "id");
        if (id != null && nested.isEmpty()) {
            return property + " () Inline [" + id + "]";
        }
        List<String> pms = new ArrayList<>();
        for (Json.Obj n : nested) {
            pms.add(tab(level + 2) + propertyMapping(n, level));
        }
        return property + "\n" + tab(level + 1) + "(\n" + String.join(",\n", pms) + (pms.isEmpty() ? "" : "\n") + tab(level + 1) + ")";
    }

    // ---------------------------------------------------------------------
    // Association and enumeration mappings
    // ---------------------------------------------------------------------

    private static String associationMapping(Json.Obj am) {
        String type = Composing.type(am);
        String association = DatabaseComposer.pointerPath(am.get("association"));
        if ("xStore".equals(type)) {
            List<String> pms = new ArrayList<>();
            for (Json.Obj pm : objs(am, "propertyMappings")) {
                String source = str(pm, "source");
                pms.add(tab(2) + convertIdentifier(pm.getObj("property").getString("property"))
                        + (source == null || source.isEmpty() ? "" : "[" + convertIdentifier(source) + ", " + convertIdentifier(pm.getString("target")) + "]")
                        + ": " + Composing.valueSpecification(items(pm.getObj("crossExpression"), "body").get(0)));
            }
            return association + mappingId(str(am, "id")) + ": XStore\n" + TAB + "{\n"
                    + String.join(",\n", pms) + (pms.isEmpty() ? "" : "\n") + TAB + "}";
        }
        if ("modelJoin".equals(type)) {
            return association + mappingId(str(am, "id")) + ": ModelJoin\n" + TAB + "{\n"
                    + tab(2) + Composing.valueSpecification(am.get("joinCondition")) + "\n" + TAB + "}";
        }
        if ("relational".equals(type)) {
            return RelationalMappingComposer.associationMapping(am, association);
        }
        throw Composing.refused("no composer rule for an association mapping of _type '" + type + "'");
    }

    private static String enumerationMapping(Json.Obj em) {
        String id = str(em, "id");
        List<String> values = new ArrayList<>();
        for (Json.Obj v : objs(em, "enumValueMappings")) {
            values.add(tab(2) + enumValueMapping(v));
        }
        return Composing.convertPath(DatabaseComposer.pointerPath(em.get("enumeration"))) + ": EnumerationMapping"
                + (id != null ? " " + convertIdentifier(id) : "") + "\n" + TAB + "{\n"
                + (values.isEmpty() ? "" : String.join(",\n", values) + "\n")
                + TAB + "}";
    }

    private static String enumValueMapping(Json.Obj v) {
        List<Json.Node> sources = items(v, "sourceValues");
        if (sources.isEmpty()) {
            return "";
        }
        List<String> out = new ArrayList<>();
        for (Json.Node s : sources) {
            out.add(sourceValue(s));
        }
        return convertIdentifier(v.getString("enumValue")) + ": [" + String.join(", ", out) + "]";
    }

    private static String sourceValue(Json.Node n) {
        if (n instanceof Json.Str s) {
            return convertString(s.value(), true);
        }
        Json.Obj s = Composing.obj(n, "source value");
        String type = Composing.type(s);
        if ("stringSourceValue".equals(type)) {
            return convertString(s.getString("value"), true);
        }
        if ("integerSourceValue".equals(type)) {
            return Long.toString(((Json.Num) s.get("value")).longValue());
        }
        if ("enumSourceValue".equals(type)) {
            return Composing.convertPath(s.getString("enumeration")) + "." + convertIdentifier(s.getString("value"));
        }
        throw Composing.refused("no composer rule for an enumeration source value of _type '" + type + "'");
    }
}
