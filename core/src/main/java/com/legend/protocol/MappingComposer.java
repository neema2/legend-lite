// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.protocol;

import com.legend.protocol.spec.AppliedFunction;
import com.legend.protocol.spec.ValueSpecification;

import java.util.ArrayList;
import java.util.List;

import static com.legend.protocol.Composing.TAB;
import static com.legend.protocol.Composing.convertIdentifier;
import static com.legend.protocol.Composing.convertString;
import static com.legend.protocol.Composing.tab;

/**
 * A {@code ###Mapping} mapping as upstream prints it ({@code DEPRECATED_PureGrammarComposerCore.visit(Mapping)}
 * and {@code HelperMappingGrammarComposer}): includes, class mappings (Pure, operation, aggregation-aware,
 * relation function, and the stores' through their composers), association and enumeration mappings,
 * legacy tests and test suites -- over the records ({@link Protocol.PMapping}; the protocol program's leg 2, step 3).
 *
 * <p>Upstream's printer keeps a mutable "base tab level" for a class mapping nested in an aggregation-aware
 * one; here it is the {@code level} argument.
 */
final class MappingComposer {

    private MappingComposer() {
    }

    static String mapping(Protocol.PMapping mapping, PureComposer.Style style) {
        StringBuilder b = new StringBuilder("Mapping ").append(Composing.elementPath(mapping.pkg(), mapping.name()))
                .append("\n(\n");
        boolean empty = true;
        List<String> includes = new ArrayList<>();
        for (Protocol.PMappingInclude include : mapping.includedMappings()) {
            includes.add(TAB + include(include));
        }
        if (!includes.isEmpty()) {
            empty = false;
            b.append(String.join("\n", includes)).append("\n");
        }
        List<String> classMappings = new ArrayList<>();
        for (Protocol.PClassMapping cm : mapping.classMappings()) {
            classMappings.add(TAB + classMapping(cm, style));
        }
        if (!classMappings.isEmpty()) {
            b.append(empty ? "" : "\n");
            empty = false;
            b.append(String.join("\n", classMappings)).append("\n");
        }
        List<String> associations = new ArrayList<>();
        for (Protocol.PAssociationMapping am : mapping.associationMappings()) {
            associations.add(TAB + associationMapping(am, style));
        }
        if (!associations.isEmpty()) {
            b.append(empty ? "" : "\n");
            empty = false;
            b.append(String.join("\n", associations)).append("\n");
        }
        List<String> enumerations = new ArrayList<>();
        for (Protocol.PEnumerationMapping em : mapping.enumerationMappings()) {
            enumerations.add(TAB + enumerationMapping(em));
        }
        if (!enumerations.isEmpty()) {
            b.append(empty ? "" : "\n");
            empty = false;
            b.append(String.join("\n", enumerations)).append("\n");
        }
        if (!mapping.tests().isEmpty()) {
            b.append(empty ? "" : "\n");
            List<String> ts = new ArrayList<>();
            for (Protocol.PLegacyMappingTest t : mapping.tests()) {
                ts.add(TAB + MappingTestComposer.legacyTest(t, style));
            }
            b.append(TAB).append("MappingTests\n").append(TAB).append("[\n").append(String.join(",\n", ts)).append("\n")
                    .append(TAB).append("]\n");
        }
        if (!mapping.testSuites().isEmpty()) {
            b.append(empty ? "" : "\n");
            List<String> ss = new ArrayList<>();
            for (Protocol.PMappingTestSuite s : mapping.testSuites()) {
                ss.add(TAB + MappingTestComposer.testSuite(s, style));
            }
            b.append(TAB).append("testSuites:\n").append(TAB).append("[\n").append(String.join(",\n", ss)).append("\n")
                    .append(TAB).append("]\n");
        }
        return b.append(")").toString();
    }

    private static String include(Protocol.PMappingInclude include) {
        if (include.includedDataSpace() != null) {
            return "include dataspace " + include.includedDataSpace();
        }
        String source = include.sourceDatabasePath();
        String target = include.targetDatabasePath();
        return "include mapping " + include.includedMapping()
                + (source != null && target != null ? "[" + Composing.convertPath(source) + "->" + Composing.convertPath(target) + "]" : "");
    }

    static String mappingId(@com.legend.base.Nullable String id) {
        return id != null ? "[" + id + "]" : "";
    }

    /** What every class mapping's header says: {@code *Class[id] extends [other]}. */
    private record Head(String className, @com.legend.base.Nullable String id, boolean root,
            @com.legend.base.Nullable String extendsId) {
    }

    private static Head head(Protocol.PClassMapping cm) {
        return switch (cm) {
            case Protocol.PClassMappingRel r -> new Head(r.className(), r.id(), r.root(), r.extendsClassMappingId());
            case Protocol.PClassMappingPure p -> new Head(p.className(), p.id(), p.root(), p.extendsClassMappingId());
            case Protocol.PClassMappingOperation o -> new Head(o.className(), o.id(), o.root(), o.extendsClassMappingId());
            case Protocol.PClassMappingMergeOperation m -> new Head(m.className(), m.id(), m.root(), m.extendsClassMappingId());
            case Protocol.PClassMappingRelation r -> new Head(r.className(), r.id(), r.root(), r.extendsClassMappingId());
            case Protocol.PClassMappingAggregationAware a ->
                    new Head(a.className(), a.id(), a.root(), a.extendsClassMappingId());
            case Protocol.PClassMappingFunction f -> new Head(f.className(), f.id(), f.root(), f.extendsClassMappingId());
            case Protocol.PServiceStoreClassMapping s -> new Head(s.className(), s.id(), s.root(), s.extendsClassMappingId());
            case Protocol.PClassMappingMongoDb m -> new Head(m.className(), m.id(), m.root(), m.extendsClassMappingId());
        };
    }

    private static String classMapping(Protocol.PClassMapping cm, PureComposer.Style style) {
        Head h = head(cm);
        return (h.root() ? "*" : "") + h.className() + mappingId(h.id())
                + (h.extendsId() != null ? " extends " + mappingId(h.extendsId()) : "") + classMappingBody(cm, 1, style);
    }

    /** A class mapping's body, from its {@code :} on. */
    private static String classMappingBody(Protocol.PClassMapping cm, int level, PureComposer.Style style) {
        return switch (cm) {
            case Protocol.PClassMappingPure p -> pureInstance(p, level, style);
            case Protocol.PClassMappingOperation o -> operation(o.operation(), o.parameters(), null, style);
            case Protocol.PClassMappingMergeOperation m -> operation("MERGE", m.parameters(), m.validationLambda(), style);
            case Protocol.PClassMappingAggregationAware a -> aggregationAware(a, style);
            case Protocol.PClassMappingRelation r -> relationFunction(r, level, style);
            case Protocol.PClassMappingRel r -> RelationalMappingComposer.classMapping(r);
            case Protocol.PServiceStoreClassMapping s -> ServiceStoreComposer.classMapping(s);
            case Protocol.PClassMappingMongoDb m -> MongoComposer.classMapping(m);
            case Protocol.PClassMappingFunction f ->
                    throw Composing.refused("no composer rule for a class mapping of _type 'functionInstance'");
        };
    }

    private static String pureInstance(Protocol.PClassMappingPure cm, int level, PureComposer.Style style) {
        String pureFilter = cm.filter() == null ? ""
                : tab(2) + "~filter " + Composing.lambdaBodyText(cm.filter(), style, "") + "\n";
        List<String> pms = new ArrayList<>();
        for (Protocol.PPurePropertyMapping pm : cm.propertyMappings()) {
            pms.add(tab(level + 1) + purePropertyMapping(pm, style));
        }
        return ": Pure\n" + tab(level) + "{\n"
                + (cm.srcClass() == null ? "" : tab(level + 1) + "~src " + cm.srcClass() + "\n") + pureFilter
                + String.join(",\n", pms) + (pms.isEmpty() ? "" : "\n")
                + tab(level) + "}";
    }

    /** {@code renderPossibleLocalMappingProperty}. */
    private static String localMappingProperty(String property, Protocol.@com.legend.base.Nullable PLocalProp local) {
        return local == null ? convertIdentifier(property)
                : "+" + convertIdentifier(property) + ": " + local.type() + "["
                        + Composing.multiplicity(local.lowerBound(), local.upperBound()) + "]";
    }

    private static String purePropertyMapping(Protocol.PPurePropertyMapping pm, PureComposer.Style style) {
        String target = pm.target();
        String enumMapping = pm.enumMappingId();
        return localMappingProperty(pm.property(), pm.localMappingProperty())
                + (pm.explodeProperty() ? "*" : "")
                + (target == null || target.isEmpty() ? "" : "[" + convertIdentifier(target) + "]")
                + (enumMapping == null ? "" : ": EnumerationMapping " + enumMapping)
                + ": " + Composing.lambdaBodyText(pm.transform(), style, "");
    }

    /** An operation, or a merge with its validation lambda: the router function called with the set ids. */
    private static String operation(@com.legend.base.Nullable String op, List<String> parameters,
            @com.legend.base.Nullable ValueSpecification validation, PureComposer.Style style) {
        if (op == null) {
            throw Composing.refused("an operation class mapping with no operation (upstream cannot name its function)");
        }
        String function = MappingOperation.valueOf(op).function;
        String params = String.join(",", parameters);
        String call = validation != null
                ? function + "([" + params + "]," + Composing.valueSpecification(validation, style) + ")"
                : function + "(" + params + ")";
        return ": Operation\n" + TAB + "{\n" + tab(2) + call + "\n" + TAB + "}";
    }

    private static String aggregationAware(Protocol.PClassMappingAggregationAware cm, PureComposer.Style style) {
        String mainMapping = "~mainMapping" + classMappingBody(cm.mainSetImplementation(), 2, style);
        List<String> views = new ArrayList<>();
        for (Protocol.PAggregateSetImplementation agg : cm.aggregateSetImplementations()) {
            views.add(tab(3) + aggregateSetImplementation(agg, style));
        }
        return ": AggregationAware \n" + TAB + "{\n"
                + tab(2) + "Views: [\n" + String.join(",\n", views) + tab(2) + "],\n"
                + tab(2) + mainMapping + "\n" + TAB + "}";
    }

    /** {@code renderAggregateSetImplementationContainer}. */
    private static String aggregateSetImplementation(Protocol.PAggregateSetImplementation agg, PureComposer.Style style) {
        String aggregateMapping = "~aggregateMapping" + classMappingBody(agg.setImplementation(), 4, style);
        List<String> groupBy = new ArrayList<>();
        for (ValueSpecification g : agg.groupByFunctions()) {
            groupBy.add(tab(6) + Composing.valueSpecification(g, style));
        }
        List<String> values = new ArrayList<>();
        for (Protocol.PAggregateValue v : agg.aggregateValues()) {
            values.add(tab(6) + "( ~mapFn:" + Composing.valueSpecification(v.mapFn(), style) + " , ~aggregateFn: "
                    + Composing.valueSpecification(v.aggregateFn(), style) + " )");
        }
        return "(\n"
                + tab(4) + "~modelOperation: {\n"
                + tab(5) + "~canAggregate " + (agg.canAggregate() ? "true" : "false") + ",\n"
                + tab(5) + "~groupByFunctions (\n" + String.join(",\n", groupBy) + "\n" + tab(5) + "),\n"
                + tab(5) + "~aggregateValues (\n" + String.join(",\n", values) + "\n" + tab(5) + ")\n"
                + tab(4) + "},\n"
                + tab(4) + aggregateMapping + "\n" + tab(3) + ")\n";
    }

    private static String relationFunction(Protocol.PClassMappingRelation cm, int level, PureComposer.Style style) {
        String pk = "";
        if (!cm.primaryKey().isEmpty()) {
            List<String> ks = new ArrayList<>();
            for (String k : cm.primaryKey()) {
                ks.add(convertIdentifier(k));
            }
            pk = tab(level + 1) + "~primaryKey: " + (ks.size() == 1 ? ks.get(0) : "[" + String.join(", ", ks) + "]") + "\n";
        }
        Protocol.PRelationSrcLambda source = cm.sourceLambda();
        String sourceLine = cm.relationFunction() != null ? tab(level + 1) + "~func " + cm.relationFunction() + "\n"
                : source != null ? tab(level + 1) + "~src " + sourceExpression(source, style) + "\n" : "";
        List<String> pms = new ArrayList<>();
        for (Protocol.PRelationFnPropertyMapping pm : cm.propertyMappings()) {
            pms.add(tab(level + 1) + relationPropertyMapping(pm, level, style));
        }
        return ": Relation\n" + tab(level) + "{\n" + sourceLine + pk
                + String.join(",\n", pms) + (pms.isEmpty() ? "" : "\n") + tab(level) + "}";
    }

    /** {@code ~src}'s one expression: the bare {@code fn()} the reader keeps by name, or any other expression. */
    private static String sourceExpression(Protocol.PRelationSrcLambda source, PureComposer.Style style) {
        ValueSpecification expr = source.function() != null ? new AppliedFunction(source.function(), List.of())
                : source.expr();
        if (expr == null) {
            throw Composing.refused("a relation ~src with neither a function nor an expression");
        }
        return Composing.valueSpecification(expr, style);
    }

    /** A column binding, a nested embedded block, or an inline-embedded set. */
    private static String relationPropertyMapping(Protocol.PRelationFnPropertyMapping pm, int level, PureComposer.Style style) {
        if (pm.inlineSetId() != null) {
            return pm.property() + " () Inline [" + pm.inlineSetId() + "]";
        }
        List<Protocol.PRelationFnPropertyMapping> nested = pm.nested();
        if (nested != null) {
            List<String> pms = new ArrayList<>();
            for (Protocol.PRelationFnPropertyMapping n : nested) {
                pms.add(tab(level + 2) + relationPropertyMapping(n, level, style));
            }
            return pm.property() + "\n" + tab(level + 1) + "(\n" + String.join(",\n", pms) + (pms.isEmpty() ? "" : "\n")
                    + tab(level + 1) + ")";
        }
        String rhs;
        if (pm.expr() != null) {
            rhs = Composing.valueSpecification(pm.expr(), style);
        } else if (pm.column() != null) {
            rhs = convertIdentifier(pm.column());
        } else {
            throw Composing.refused("a relation property mapping with neither a column nor a value function");
        }
        return localMappingProperty(pm.property(), pm.localMappingProperty())
                + (pm.bindingTransformer() != null ? ": Binding " + pm.bindingTransformer() + " " : "")
                + (pm.enumMappingId() != null ? ": EnumerationMapping " + pm.enumMappingId() + " " : "")
                + ": " + rhs;
    }

    // ---------------------------------------------------------------------
    // Association and enumeration mappings
    // ---------------------------------------------------------------------

    private static String associationMapping(Protocol.PAssociationMapping am, PureComposer.Style style) {
        return switch (am) {
            case Protocol.PXStoreAssociationMapping x -> {
                List<String> pms = new ArrayList<>();
                for (Protocol.PXStorePropertyMapping pm : x.propertyMappings()) {
                    String source = pm.source();
                    if (pm.crossExpression().isEmpty()) {
                        throw Composing.refused("an xStore property mapping with no cross expression");
                    }
                    pms.add(tab(2) + convertIdentifier(pm.property())
                            + (source == null || source.isEmpty() ? "" : "[" + convertIdentifier(source) + ", "
                                    + convertIdentifier(pm.target()) + "]")
                            + ": " + Composing.valueSpecification(pm.crossExpression().get(0), style));
                }
                yield x.association().path() + mappingId(x.id()) + ": XStore\n" + TAB + "{\n"
                        + String.join(",\n", pms) + (pms.isEmpty() ? "" : "\n") + TAB + "}";
            }
            case Protocol.PModelJoinAssociationMapping m -> m.association().path() + mappingId(m.id()) + ": ModelJoin\n"
                    + TAB + "{\n" + tab(2) + Composing.valueSpecification(m.joinCondition(), style) + "\n" + TAB + "}";
            case Protocol.PRelAssociationMapping r -> RelationalMappingComposer.associationMapping(r, r.association().path());
            case Protocol.PFunctionAssociationMapping f ->
                    throw Composing.refused("no composer rule for an association mapping of _type 'functionAssociation'");
        };
    }

    private static String enumerationMapping(Protocol.PEnumerationMapping em) {
        List<String> values = new ArrayList<>();
        for (Protocol.PEnumValueMapping v : em.enumValueMappings()) {
            values.add(tab(2) + enumValueMapping(v));
        }
        return Composing.convertPath(em.enumeration().path()) + ": EnumerationMapping"
                + (em.id() != null ? " " + convertIdentifier(em.id()) : "") + "\n" + TAB + "{\n"
                + (values.isEmpty() ? "" : String.join(",\n", values) + "\n")
                + TAB + "}";
    }

    private static String enumValueMapping(Protocol.PEnumValueMapping v) {
        if (v.sourceValues().isEmpty()) {
            return "";
        }
        List<String> out = new ArrayList<>();
        for (Protocol.PEnumSourceValue s : v.sourceValues()) {
            out.add(sourceValue(s));
        }
        return convertIdentifier(v.enumValue()) + ": [" + String.join(", ", out) + "]";
    }

    /** A string, an integer, or an enumeration's value. */
    private static String sourceValue(Protocol.PEnumSourceValue s) {
        if (s.enumeration() != null) {
            return Composing.convertPath(s.enumeration()) + "." + convertIdentifier(String.valueOf(s.value()));
        }
        return switch (s.value()) {
            case String str -> convertString(str, true);
            case Long n -> Long.toString(n);
            default -> throw Composing.refused("an enumeration source value upstream cannot print: " + s.value());
        };
    }
}
