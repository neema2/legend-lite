package org.finos.legend.pure.dsl.definition;

import java.util.List;
import java.util.Objects;

/**
 * Represents a Pure Profile definition.
 * 
 * Profiles are containers for stereotypes and tags that can be applied
 * to model elements (classes, properties) as metadata annotations.
 * 
 * Pure syntax:
 * 
 * <pre>
 * Profile doc::Documentation
 * {
 *     stereotypes: [legacy, deprecated, experimental];
 *     tags: [author, since, description];
 * }
 * </pre>
 * 
 * @param qualifiedName The fully qualified profile name (e.g.,
 *                      "doc::Documentation")
 * @param stereotypes   List of stereotype names defined in this profile
 * @param tags          List of tag names defined in this profile
 */
public record ProfileDefinition(
        String qualifiedName,
        List<String> stereotypes,
        List<String> tags) implements PureDefinition {

    public ProfileDefinition {
        Objects.requireNonNull(qualifiedName, "Profile name cannot be null");
        stereotypes = stereotypes == null ? List.of() : List.copyOf(stereotypes);
        tags = tags == null ? List.of() : List.copyOf(tags);
    }

    /**
     * @return The simple profile name (without package)
     */
    public String simpleName() {
        int idx = qualifiedName.lastIndexOf("::");
        return idx >= 0 ? qualifiedName.substring(idx + 2) : qualifiedName;
    }

    /**
     * @return The package path (without profile name)
     */
    public String packagePath() {
        int idx = qualifiedName.lastIndexOf("::");
        return idx >= 0 ? qualifiedName.substring(0, idx) : "";
    }
}
