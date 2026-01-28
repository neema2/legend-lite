package org.finos.legend.pure.dsl.definition;

import java.util.Objects;

/**
 * Represents a tagged value application to a model element.
 * 
 * Pure syntax: <<profile::Name.tagName: 'value'>>
 * 
 * Example: <<doc::Documentation.author: 'John Smith'>>
 * 
 * @param profileName The fully qualified profile name (e.g.,
 *                    "doc::Documentation")
 * @param tagName     The tag name within the profile (e.g., "author")
 * @param value       The tag value (e.g., "John Smith")
 */
public record TaggedValue(
        String profileName,
        String tagName,
        String value) {
    public TaggedValue {
        Objects.requireNonNull(profileName, "Profile name cannot be null");
        Objects.requireNonNull(tagName, "Tag name cannot be null");
        Objects.requireNonNull(value, "Tag value cannot be null");
    }

    /**
     * Factory method to create a tagged value.
     */
    public static TaggedValue of(String profileName, String tagName, String value) {
        return new TaggedValue(profileName, tagName, value);
    }

    /**
     * @return The full reference string "profile::Name.tagName"
     */
    public String toReference() {
        return profileName + "." + tagName;
    }
}
