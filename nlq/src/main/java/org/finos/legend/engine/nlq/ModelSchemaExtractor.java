package org.finos.legend.engine.nlq;

import org.finos.legend.pure.dsl.definition.PureModelBuilder;
import org.finos.legend.pure.m3.Association;
import org.finos.legend.pure.m3.Property;
import org.finos.legend.pure.m3.PureClass;

import java.util.*;

/**
 * Extracts a focused model schema from the top-K retrieved classes,
 * formatted as compact text optimized for LLM context windows.
 *
 * Target: ~2-4K tokens regardless of total model size.
 */
public class ModelSchemaExtractor {

    /**
     * Builds a focused schema string from a set of relevant class names.
     * Includes full property details for the primary classes and reduced
     * detail (name + description only) for 1-hop associated classes.
     *
     * @param classNames   The set of relevant class qualified or simple names
     * @param modelBuilder The model builder with the full model
     * @return A compact text schema suitable for LLM context
     */
    public static String extractSchema(Set<String> classNames, PureModelBuilder modelBuilder) {
        Map<String, PureClass> allClasses = modelBuilder.getAllClasses();
        Map<String, Association> allAssociations = modelBuilder.getAllAssociations();

        // Resolve class names to PureClass objects
        Map<String, PureClass> primaryClasses = new LinkedHashMap<>();
        for (String name : classNames) {
            PureClass pc = allClasses.get(name);
            if (pc == null) {
                // Try lookup by simple name
                for (PureClass candidate : allClasses.values()) {
                    if (candidate.name().equals(name)) {
                        pc = candidate;
                        break;
                    }
                }
            }
            if (pc != null) {
                primaryClasses.put(pc.qualifiedName(), pc);
            }
        }

        // Find associations between primary classes
        List<Association> relevantAssociations = new ArrayList<>();
        Set<String> primaryNames = new HashSet<>();
        for (PureClass pc : primaryClasses.values()) {
            primaryNames.add(pc.qualifiedName());
            primaryNames.add(pc.name());
        }

        for (Association assoc : allAssociations.values()) {
            String t1 = assoc.property1().targetClass();
            String t2 = assoc.property2().targetClass();
            if (matchesAny(t1, primaryNames) || matchesAny(t2, primaryNames)) {
                relevantAssociations.add(assoc);
            }
        }

        // Find 1-hop neighbors (reduced detail)
        Set<String> neighborNames = new HashSet<>();
        for (Association assoc : relevantAssociations) {
            String t1 = assoc.property1().targetClass();
            String t2 = assoc.property2().targetClass();
            if (!matchesAny(t1, primaryNames)) neighborNames.add(t1);
            if (!matchesAny(t2, primaryNames)) neighborNames.add(t2);
        }

        Map<String, PureClass> neighborClasses = new LinkedHashMap<>();
        for (String name : neighborNames) {
            PureClass pc = allClasses.get(name);
            if (pc == null) {
                for (PureClass candidate : allClasses.values()) {
                    if (candidate.name().equals(name)) {
                        pc = candidate;
                        break;
                    }
                }
            }
            if (pc != null && !primaryClasses.containsKey(pc.qualifiedName())) {
                neighborClasses.put(pc.qualifiedName(), pc);
            }
        }

        // Build the schema text
        StringBuilder sb = new StringBuilder();

        // Primary classes with full detail
        sb.append("=== Classes (full detail) ===\n\n");
        for (PureClass pc : primaryClasses.values()) {
            appendClassFull(sb, pc);
        }

        // Neighbor classes with reduced detail
        if (!neighborClasses.isEmpty()) {
            sb.append("=== Related Classes (summary) ===\n\n");
            for (PureClass pc : neighborClasses.values()) {
                appendClassSummary(sb, pc);
            }
        }

        // Associations
        if (!relevantAssociations.isEmpty()) {
            sb.append("=== Associations ===\n\n");
            for (Association assoc : relevantAssociations) {
                appendAssociation(sb, assoc);
            }
        }

        return sb.toString();
    }

    // ==================== Formatting ====================

    private static void appendClassFull(StringBuilder sb, PureClass pc) {
        sb.append("Class ").append(pc.qualifiedName());

        // Stereotypes
        if (!pc.stereotypes().isEmpty()) {
            sb.append(" <<");
            for (int i = 0; i < pc.stereotypes().size(); i++) {
                if (i > 0) sb.append(", ");
                sb.append(pc.stereotypes().get(i).toReference());
            }
            sb.append(">>");
        }
        sb.append("\n");

        // Class-level description
        String desc = pc.getTagValue("nlq::NlqProfile", "description");
        if (desc == null) desc = pc.getTagValue("doc", "doc");
        if (desc != null) {
            sb.append("  Description: ").append(desc).append("\n");
        }

        // Synonyms
        String syn = pc.getTagValue("nlq::NlqProfile", "synonyms");
        if (syn != null) {
            sb.append("  Synonyms: ").append(syn).append("\n");
        }

        // Business domain
        String domain = pc.getTagValue("nlq::NlqProfile", "businessDomain");
        if (domain != null) {
            sb.append("  Domain: ").append(domain).append("\n");
        }

        // When to use (routing hint)
        String whenToUse = pc.getTagValue("nlq::NlqProfile", "whenToUse");
        if (whenToUse != null) {
            sb.append("  When to use: ").append(whenToUse).append("\n");
        }

        // Properties
        sb.append("  Properties:\n");
        for (Property prop : pc.allProperties()) {
            sb.append("    - ").append(prop.name())
              .append(": ").append(prop.genericType().typeName())
              .append(prop.multiplicity());

            String propDesc = prop.getTagValue("nlq::NlqProfile", "description");
            if (propDesc != null) {
                sb.append("  // ").append(propDesc);
            }

            String unit = prop.getTagValue("nlq::NlqProfile", "unit");
            if (unit != null) {
                sb.append(" [").append(unit).append("]");
            }

            String sample = prop.getTagValue("nlq::NlqProfile", "sampleValues");
            if (sample != null) {
                sb.append(" e.g. ").append(sample);
            }

            sb.append("\n");
        }
        sb.append("\n");
    }

    private static void appendClassSummary(StringBuilder sb, PureClass pc) {
        sb.append("Class ").append(pc.qualifiedName());

        String desc = pc.getTagValue("nlq::NlqProfile", "description");
        if (desc == null) desc = pc.getTagValue("doc", "doc");
        if (desc != null) {
            sb.append(" — ").append(desc);
        }

        sb.append(" (").append(pc.allProperties().size()).append(" properties)\n");
    }

    private static void appendAssociation(StringBuilder sb, Association assoc) {
        sb.append(assoc.property1().targetClass())
          .append(".").append(assoc.property2().propertyName())
          .append(" → ")
          .append(assoc.property2().targetClass())
          .append("[").append(assoc.property2().multiplicity()).append("]");

        sb.append("  |  ");

        sb.append(assoc.property2().targetClass())
          .append(".").append(assoc.property1().propertyName())
          .append(" → ")
          .append(assoc.property1().targetClass())
          .append("[").append(assoc.property1().multiplicity()).append("]");

        sb.append("\n");
    }

    /**
     * Extracts routing hints from model metadata for the semantic router.
     * Builds a compact string of disambiguation hints from whenToUse and exampleQuestions tags.
     * Returns empty string if no hints are found (model-agnostic).
     */
    public static String extractRoutingHints(Set<String> classNames, PureModelBuilder modelBuilder) {
        Map<String, PureClass> allClasses = modelBuilder.getAllClasses();
        StringBuilder sb = new StringBuilder();

        for (String name : classNames) {
            PureClass pc = allClasses.get(name);
            if (pc == null) {
                for (PureClass candidate : allClasses.values()) {
                    if (candidate.name().equals(name)) {
                        pc = candidate;
                        break;
                    }
                }
            }
            if (pc == null) continue;

            String whenToUse = pc.getTagValue("nlq::NlqProfile", "whenToUse");
            String examples = pc.getTagValue("nlq::NlqProfile", "exampleQuestions");

            if (whenToUse != null || examples != null) {
                sb.append(pc.name()).append(": ");
                if (whenToUse != null) {
                    sb.append(whenToUse);
                }
                if (examples != null) {
                    if (whenToUse != null) sb.append(" ");
                    sb.append("(examples: ").append(examples.replace("|", ", ")).append(")");
                }
                sb.append("\n");
            }
        }

        return sb.toString();
    }

    private static boolean matchesAny(String name, Set<String> names) {
        if (names.contains(name)) return true;
        // Try simple name extraction
        if (name.contains("::")) {
            String simple = name.substring(name.lastIndexOf("::") + 2);
            return names.contains(simple);
        }
        return false;
    }
}
