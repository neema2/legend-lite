package com.legend.nlq;

import com.legend.model.AssociationDefinition;
import com.legend.model.ClassDefinition;
import com.legend.model.ParsedModel;
import com.legend.model.TaggedValue;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * nlq's view of a parsed core model — the class/association maps and
 * tagged-value lookups the index and schema extractor walk. Mirrors the
 * DiagramService pattern: work straight off {@link ParsedModel} records
 * (the engine module's PureModelBuilder is gone).
 */
public final class NlqModel {

    private NlqModel() {
    }

    public static ParsedModel parse(String pureSource) {
        return com.legend.Compiler.parseModel(pureSource);
    }

    /** All classes keyed by qualified name, in declaration order. */
    public static Map<String, ClassDefinition> allClasses(ParsedModel model) {
        Map<String, ClassDefinition> out = new LinkedHashMap<>();
        for (var el : model.elements()) {
            if (el instanceof ClassDefinition c) {
                out.put(c.qualifiedName(), c);
            }
        }
        return out;
    }

    /** All enums keyed by qualified name, in declaration order. */
    public static Map<String, com.legend.model.EnumDefinition> allEnums(ParsedModel model) {
        Map<String, com.legend.model.EnumDefinition> out = new LinkedHashMap<>();
        for (var el : model.elements()) {
            if (el instanceof com.legend.model.EnumDefinition e) {
                out.put(e.qualifiedName(), e);
            }
        }
        return out;
    }

    /** All associations keyed by qualified name, in declaration order. */
    public static Map<String, AssociationDefinition> allAssociations(ParsedModel model) {
        Map<String, AssociationDefinition> out = new LinkedHashMap<>();
        for (var el : model.elements()) {
            if (el instanceof AssociationDefinition a) {
                out.put(a.qualifiedName(), a);
            }
        }
        return out;
    }

    /** The class's own plus inherited properties (superclass walk). */
    public static List<ClassDefinition.PropertyDefinition> allProperties(
            ClassDefinition cd, Map<String, ClassDefinition> classes) {
        var out = new java.util.ArrayList<>(cd.properties());
        for (var superRef : cd.superClasses()) {
            String superFqn = resolveClassFqn(typeName(superRef),
                    cd.qualifiedName(), classes);
            ClassDefinition superDef = classes.get(superFqn);
            if (superDef != null && superDef != cd) {
                out.addAll(allProperties(superDef, classes));
            }
        }
        return out;
    }

    public static @com.legend.base.Nullable String tag(List<TaggedValue> taggedValues,
            String profileName, String tagName) {
        for (TaggedValue tv : taggedValues) {
            if (tv.tagName().equals(tagName)
                    && (tv.profileName().equals(profileName)
                            || simpleName(tv.profileName())
                                    .equals(simpleName(profileName)))) {
                return tv.value();
            }
        }
        return null;
    }

    /** A type reference as written → the FQN of a class in this model:
     *  exact FQN wins; else the referencing element's package; else a
     *  simple-name match (imports are wildcard in practice — the
     *  DiagramService resolution rule). */
    public static String resolveClassFqn(String written, String fromFqn,
            Map<String, ClassDefinition> classes) {
        if (classes.containsKey(written)) {
            return written;
        }
        String pkg = packageOf(fromFqn);
        if (!pkg.isEmpty() && classes.containsKey(pkg + "::" + written)) {
            return pkg + "::" + written;
        }
        for (String fqn : classes.keySet()) {
            if (simpleName(fqn).equals(written)) {
                return fqn;
            }
        }
        return written;
    }

    public static String typeName(com.legend.protocol.TypeExpression t) {
        return switch (t) {
            case com.legend.protocol.TypeExpression.NameRef n -> n.name();
            case com.legend.protocol.TypeExpression.Generic g -> g.name();
            default -> t.toString();
        };
    }

    /** Pure multiplicity print form: [1], [0..1], [*], [2..5]. */
    public static String multText(com.legend.protocol.Multiplicity m) {
        if (!(m instanceof com.legend.protocol.Multiplicity.Concrete c)) {
            return "[" + m + "]";
        }
        Integer up = c.upperBound();
        if (up == null) {
            return c.lowerBound() == 0 ? "[*]" : "[" + c.lowerBound() + "..*]";
        }
        return c.lowerBound() == up ? "[" + up + "]"
                : "[" + c.lowerBound() + ".." + up + "]";
    }

    public static String simpleName(String fqn) {
        int cut = fqn.lastIndexOf("::");
        return cut < 0 ? fqn : fqn.substring(cut + 2);
    }

    public static String packageOf(String fqn) {
        int cut = fqn.lastIndexOf("::");
        return cut < 0 ? "" : fqn.substring(0, cut);
    }
}
