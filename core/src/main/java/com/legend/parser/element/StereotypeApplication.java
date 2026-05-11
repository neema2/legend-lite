package com.legend.parser.element;

import java.util.Objects;

/**
 * A stereotype reference applied to a model element.
 *
 * <p>Pure syntax: {@code <<profile::Name.stereotypeName>>}
 *
 * <p>Mirrors engine's {@code com.gs.legend.model.def.StereotypeApplication}
 * verbatim.
 *
 * @param profileName    the fully qualified profile name (e.g. {@code "doc::Documentation"})
 * @param stereotypeName the stereotype name within the profile (e.g. {@code "deprecated"})
 */
public record StereotypeApplication(String profileName, String stereotypeName) {
    public StereotypeApplication {
        Objects.requireNonNull(profileName, "Profile name cannot be null");
        Objects.requireNonNull(stereotypeName, "Stereotype name cannot be null");
    }
}
