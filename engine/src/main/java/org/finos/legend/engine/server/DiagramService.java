package org.finos.legend.engine.server;

import org.finos.legend.pure.dsl.definition.PureModelBuilder;
import org.finos.legend.pure.m3.Association;
import org.finos.legend.pure.m3.PureClass;
import org.finos.legend.pure.m3.Property;

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
                props.add(new PropertyInfo(
                        prop.name(),
                        prop.genericType().typeName(),
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
            for (PureClass superClass : pc.superClasses()) {
                generalisations.add(new GeneralisationInfo(
                        pc.qualifiedName(),
                        superClass.qualifiedName()));
            }
        }

        return new DiagramData(classes, associations, generalisations);
    }

    /**
     * Serialize DiagramData to JSON string.
     */
    public String toJson(DiagramData data) {
        StringBuilder sb = new StringBuilder();
        sb.append("{\"classes\":[");

        for (int i = 0; i < data.classes().size(); i++) {
            if (i > 0) sb.append(",");
            ClassInfo c = data.classes().get(i);
            sb.append("{\"id\":\"").append(esc(c.id()))
              .append("\",\"name\":\"").append(esc(c.name()))
              .append("\",\"package\":\"").append(esc(c.packagePath()))
              .append("\",\"stereotype\":\"").append(esc(c.stereotype()))
              .append("\",\"description\":\"").append(esc(c.description()))
              .append("\",\"businessDomain\":\"").append(esc(c.businessDomain()))
              .append("\",\"properties\":[");

            for (int j = 0; j < c.properties().size(); j++) {
                if (j > 0) sb.append(",");
                PropertyInfo p = c.properties().get(j);
                sb.append("{\"name\":\"").append(esc(p.name()))
                  .append("\",\"type\":\"").append(esc(p.type()))
                  .append("\",\"multiplicity\":\"").append(esc(p.multiplicity()))
                  .append("\"}");
            }
            sb.append("]}");
        }

        sb.append("],\"associations\":[");
        for (int i = 0; i < data.associations().size(); i++) {
            if (i > 0) sb.append(",");
            AssociationInfo a = data.associations().get(i);
            sb.append("{\"name\":\"").append(esc(a.name()))
              .append("\",\"source\":\"").append(esc(a.source()))
              .append("\",\"target\":\"").append(esc(a.target()))
              .append("\",\"sourceProperty\":\"").append(esc(a.sourceProperty()))
              .append("\",\"targetProperty\":\"").append(esc(a.targetProperty()))
              .append("\",\"sourceMult\":\"").append(esc(a.sourceMult()))
              .append("\",\"targetMult\":\"").append(esc(a.targetMult()))
              .append("\"}");
        }

        sb.append("],\"generalisations\":[");
        for (int i = 0; i < data.generalisations().size(); i++) {
            if (i > 0) sb.append(",");
            GeneralisationInfo g = data.generalisations().get(i);
            sb.append("{\"child\":\"").append(esc(g.child()))
              .append("\",\"parent\":\"").append(esc(g.parent()))
              .append("\"}");
        }

        sb.append("]}");
        return sb.toString();
    }

    // ── Helpers ──

    private static String getTag(PureClass pc, String tagName) {
        String val = pc.getTagValue("NlqProfile", tagName);
        if (val == null) val = pc.getTagValue("nlq::NlqProfile", tagName);
        return val;
    }

    private static String esc(String s) {
        if (s == null) return "";
        return s.replace("\\", "\\\\")
                .replace("\"", "\\\"")
                .replace("\n", "\\n")
                .replace("\r", "\\r");
    }
}
