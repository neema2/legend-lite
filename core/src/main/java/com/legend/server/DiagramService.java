package com.legend.server;

import com.legend.model.AssociationDefinition;
import com.legend.model.ClassDefinition;
import com.legend.model.ParsedModel;
import com.legend.model.TaggedValue;

import java.util.ArrayList;
import java.util.List;

/**
 * Extracts diagram data (classes, associations, generalisations) from Pure
 * source.
 *
 * <p>Rerouted onto the CORE parser ({@code com.legend.parser.ElementParser})
 * as part of the engine-lite deletion: the parse output enumerates
 * {@link ClassDefinition}/{@link AssociationDefinition} directly, carries the
 * same stereotype/tagged-value/property shapes, and pre-seeds NO builtin
 * classes — so the old {@code BuiltinClassRegistry} phantom-node filter is
 * unnecessary by construction.
 */
public final class DiagramService {

    // ── Result records ──

    public record DiagramData(
            List<ClassInfo> classes,
            List<AssociationInfo> associations,
            List<GeneralisationInfo> generalisations) {
    }

    public record ClassInfo(
            String id,
            String name,
            String packagePath,
            String stereotype,
            String description,
            String businessDomain,
            List<PropertyInfo> properties) {
    }

    public record PropertyInfo(String name, String type, String multiplicity) {
    }

    public record AssociationInfo(
            String name,
            String source,
            String target,
            String sourceProperty,
            String targetProperty,
            String sourceMult,
            String targetMult) {
    }

    public record GeneralisationInfo(String child, String parent) {
    }

    // ── Core logic ──

    /**
     * Parse Pure source and extract diagram data.
     *
     * @param pureSource Complete Pure source text
     * @return Structured diagram data
     */
    public DiagramData extract(String pureSource) {
        ParsedModel model = com.legend.Compiler.parseModel(pureSource);

        List<ClassInfo> classes = new ArrayList<>();
        List<AssociationInfo> associations = new ArrayList<>();
        List<GeneralisationInfo> generalisations = new ArrayList<>();

        for (var el : model.elements()) {
            if (!(el instanceof ClassDefinition cd)) {
                continue;
            }
            String stereotype = cd.stereotypes().isEmpty() ? ""
                    : cd.stereotypes().get(0).stereotypeName();
            String desc = getTag(cd, "description");
            String domain = getTag(cd, "businessDomain");

            List<PropertyInfo> props = new ArrayList<>();
            for (ClassDefinition.PropertyDefinition p : cd.properties()) {
                props.add(new PropertyInfo(p.name(),
                        simpleName(typeName(p.type())),
                        p.multiplicity().toString()));
            }

            classes.add(new ClassInfo(
                    cd.qualifiedName(),
                    simpleName(cd.qualifiedName()),
                    packageOf(cd.qualifiedName()),
                    stereotype,
                    desc != null ? desc : "",
                    domain != null ? domain : "",
                    props));

            for (var sup : cd.superClasses()) {
                generalisations.add(new GeneralisationInfo(
                        cd.qualifiedName(),
                        resolve(typeName(sup), model, cd.qualifiedName())));
            }
        }

        for (var el : model.elements()) {
            if (!(el instanceof AssociationDefinition ad)) {
                continue;
            }
            var p1 = ad.property1();
            var p2 = ad.property2();
            associations.add(new AssociationInfo(
                    simpleName(ad.qualifiedName()),
                    resolve(typeName(p2.targetClass()), model,
                            ad.qualifiedName()),
                    resolve(typeName(p1.targetClass()), model,
                            ad.qualifiedName()),
                    p2.propertyName(),
                    p1.propertyName(),
                    p2.multiplicity().toString(),
                    p1.multiplicity().toString()));
        }

        return new DiagramData(classes, associations, generalisations);
    }

    /** Serialise diagram data to the compact JSON the HTTP endpoint
     *  returns (shape unchanged by the core reroute). */
    public String toJson(DiagramData data) {
        Json.Writer w =
                Json.compactWriter();
        w.beginObject();

        w.name("classes").beginArray();
        for (ClassInfo c : data.classes()) {
            w.beginObject()
                .field("id", c.id())
                .field("name", c.name())
                .field("package", c.packagePath())
                .field("stereotype", c.stereotype())
                .field("description", c.description())
                .field("businessDomain", c.businessDomain());
            w.name("properties").beginArray();
            for (PropertyInfo p : c.properties()) {
                w.beginObject()
                    .field("name", p.name())
                    .field("type", p.type())
                    .field("multiplicity", p.multiplicity())
                    .endObject();
            }
            w.endArray().endObject();
        }
        w.endArray();

        w.name("associations").beginArray();
        for (AssociationInfo a : data.associations()) {
            w.beginObject()
                .field("name", a.name())
                .field("source", a.source())
                .field("target", a.target())
                .field("sourceProperty", a.sourceProperty())
                .field("targetProperty", a.targetProperty())
                .field("sourceMult", a.sourceMult())
                .field("targetMult", a.targetMult())
                .endObject();
        }
        w.endArray();

        w.name("generalisations").beginArray();
        for (GeneralisationInfo g : data.generalisations()) {
            w.beginObject()
                .field("child", g.child())
                .field("parent", g.parent())
                .endObject();
        }
        w.endArray();

        w.endObject();
        return w.toString();
    }

    // ── Helpers ──

    /** The WRITTEN name of a type reference (diagram labels want names,
     *  not record dumps). */
    private static String typeName(com.legend.protocol.TypeExpression t) {
        return switch (t) {
            case com.legend.protocol.TypeExpression.NameRef n -> n.name();
            case com.legend.protocol.TypeExpression.Generic g -> g.name();
            default -> t.toString();
        };
    }

    private static String simpleName(String fqn) {
        int cut = fqn.lastIndexOf("::");
        return cut < 0 ? fqn : fqn.substring(cut + 2);
    }

    private static String packageOf(String fqn) {
        int cut = fqn.lastIndexOf("::");
        return cut < 0 ? "" : fqn.substring(0, cut);
    }

    /** A type reference as written → the FQN of a class in this model, so
     *  diagram edges land on class node ids: exact FQN wins; else try the
     *  referencing element's package, then any model class whose simple
     *  name matches (imports are wildcard in practice). */
    private static String resolve(String written, ParsedModel model,
            String fromFqn) {
        String name = written;
        java.util.Set<String> fqns = new java.util.HashSet<>();
        for (var el : model.elements()) {
            if (el instanceof ClassDefinition c) {
                fqns.add(c.qualifiedName());
            }
        }
        if (fqns.contains(name)) {
            return name;
        }
        String samePkg = packageOf(fromFqn).isEmpty() ? name
                : packageOf(fromFqn) + "::" + name;
        if (fqns.contains(samePkg)) {
            return samePkg;
        }
        for (String fqn : fqns) {
            if (simpleName(fqn).equals(name)) {
                return fqn;
            }
        }
        return name;
    }

    private static @com.legend.base.Nullable String getTag(ClassDefinition cd, String tagName) {
        for (TaggedValue tv : cd.taggedValues()) {
            if (tagName.equals(tv.tagName())
                    && ("NlqProfile".equals(tv.profileName())
                            || "nlq::NlqProfile".equals(tv.profileName()))) {
                return tv.value();
            }
        }
        return null;
    }
}
