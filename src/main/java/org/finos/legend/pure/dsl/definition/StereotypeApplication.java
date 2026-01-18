package org.finos.legend.pure.dsl.definition;

import java.util.Objects;

/**
 * Represents a stereotype application to a model element.
 * 
 * Pure syntax: <<profile::Name.stereotypeName>>
 * 
 * Example: <<doc::Documentation.deprecated>>
 * 
 * @param profileName    The fully qualified profile name (e.g.,
 *                       "doc::Documentation")
 * @param stereotypeName The stereotype name within the profile (e.g.,
 *                       "deprecated")
 */
public record StereotypeApplication(
        String profileName,
        String stereotypeName) {
    public StereotypeApplication {
        Objects.requireNonNull(profileName, "Profile name cannot be null");
        Objects.requireNonNull(stereotypeName, "Stereotype name cannot be null");
    }

    /**
     * Factory method to parse from "profile::Name.stereotype" format.
     */
    public static StereotypeApplication parse(String fullReference) {
        int dotIdx = fullReference.lastIndexOf('.');
        if (dotIdx < 0) {
            throw new IllegalArgumentException("Invalid stereotype reference: " + fullReference);
        }
        return new StereotypeApplication(
                fullReference.substring(0, dotIdx),
                fullReference.substring(dotIdx + 1));
    }

    /**
     * @return The full reference string "profile::Name.stereotype"
     */
    public String toReference() {
        return profileName + "." + stereotypeName;
    }
}
