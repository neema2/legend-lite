package com.legend.parser.element;

import java.util.Objects;

/**
 * A tagged value application on a model element.
 *
 * <p>Pure syntax: {@code <<profile::Name.tagName = 'value'>>}
 *
 * <p>Mirrors engine's {@code com.gs.legend.model.def.TaggedValue}
 * verbatim.
 *
 * @param profileName the fully qualified profile name (e.g. {@code "doc::Documentation"})
 * @param tagName     the tag name within the profile (e.g. {@code "author"})
 * @param value       the tag value (string, quotes stripped by parser)
 */
public record TaggedValue(String profileName, String tagName, String value) {
    public TaggedValue {
        Objects.requireNonNull(profileName, "Profile name cannot be null");
        Objects.requireNonNull(tagName, "Tag name cannot be null");
        Objects.requireNonNull(value, "Tag value cannot be null");
    }
}
