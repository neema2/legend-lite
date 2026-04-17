package com.gs.legend.server;

import com.gs.legend.model.PureModelBuilder;
import com.gs.legend.model.SymbolTable;
import com.gs.legend.model.m3.Association;
import com.gs.legend.model.m3.Property;
import com.gs.legend.model.m3.PureClass;
import com.gs.legend.util.Json;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Extracts diagram data (classes, associations, generalisations) from Pure source.
 * 
 * Uses PureModelBuilder to parse, then produces structured records
 * suitable for JSON serialisation and graph rendering.
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
        PureModelBuilder model = new PureModelBuilder();
        model.addSource(pureSource);

        Map<String, PureClass> allClasses = model.getAllClasses();
        Map<String, Association> allAssocs = model.getAllAssociations();

        List<ClassInfo> classes = new ArrayList<>();
        List<AssociationInfo> associations = new ArrayList<>();
        List<GeneralisationInfo> generalisations = new ArrayList<>();

        for (var entry : allClasses.entrySet()) {
            PureClass pc = entry.getValue();

            String stereotype = "";
            if (!pc.stereotypes().isEmpty()) {
                stereotype = pc.stereotypes().get(0).stereotypeName();
            }

            String desc = getTag(pc, "description");
            String domain = getTag(pc, "businessDomain");

            List<PropertyInfo> props = new ArrayList<>();
            for (Property prop : pc.properties()) {
                // Preserve pre-Phase-A behavior: diagram shows simple type names.
                // typeFqn is fully qualified; extract the simple name for display.
                props.add(new PropertyInfo(
                        prop.name(),
                        SymbolTable.extractSimpleName(prop.typeFqn()),
                        prop.multiplicity().toString()));
            }

            classes.add(new ClassInfo(
                    pc.qualifiedName(),
                    pc.name(),
                    pc.packagePath(),
                    stereotype,
                    desc != null ? desc : "",
                    domain != null ? domain : "",
                    props));
        }

        for (var entry : allAssocs.entrySet()) {
            Association assoc = entry.getValue();
            associations.add(new AssociationInfo(
                    assoc.name(),
                    assoc.property2().targetClass(),
                    assoc.property1().targetClass(),
                    assoc.property2().propertyName(),
                    assoc.property1().propertyName(),
                    assoc.property2().multiplicity().toString(),
                    assoc.property1().multiplicity().toString()));
        }

        for (var entry : allClasses.entrySet()) {
            PureClass pc = entry.getValue();
            for (String superFqn : pc.superClassFqns()) {
                generalisations.add(new GeneralisationInfo(
                        pc.qualifiedName(),
                        superFqn));
            }
        }

        return new DiagramData(classes, associations, generalisations);
    }

    /**
     * Serialize DiagramData to a compact JSON string.
     *
     * <p>Uses {@link Json.Writer} so escaping is RFC 8259 compliant. The
     * previous hand-rolled {@code esc} helper missed {@code \t}, {@code \b},
     * {@code \f}, and {@code \}u00xx control-char escapes.
     */
    public String toJson(DiagramData data) {
        Json.Writer w = Json.compactWriter();
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

    private static String getTag(PureClass pc, String tagName) {
        String val = pc.getTagValue("NlqProfile", tagName);
        if (val == null) val = pc.getTagValue("nlq::NlqProfile", tagName);
        return val;
    }

}
