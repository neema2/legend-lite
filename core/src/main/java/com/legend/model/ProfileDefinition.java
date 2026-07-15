package com.legend.model;

import java.util.List;
import java.util.Objects;

/**
 * A parsed Pure {@code Profile} declaration.
 *
 * <p>Profiles are containers for stereotypes and tags that can be applied
 * to model elements as metadata.
 *
 * <p>Pure syntax:
 * <pre>
 *   Profile doc::Documentation
 *   {
 *     stereotypes: [legacy, deprecated, experimental];
 *     tags: [author, since, description];
 *   }
 * </pre>
 *
 * <p>Mirrors engine's {@code com.gs.legend.model.def.ProfileDefinition}.
 *
 * @param qualifiedName fully qualified profile name (e.g. {@code "doc::Documentation"})
 * @param stereotypes   stereotype names declared in this profile
 * @param tags          tag names declared in this profile
 */
public record ProfileDefinition(
        String qualifiedName,
        List<String> stereotypes,
        List<String> tags) implements PackageableElement {

    public ProfileDefinition {
        Objects.requireNonNull(qualifiedName, "Profile name cannot be null");
        stereotypes = stereotypes == null ? List.of() : List.copyOf(stereotypes);
        tags = tags == null ? List.of() : List.copyOf(tags);
    }
}
