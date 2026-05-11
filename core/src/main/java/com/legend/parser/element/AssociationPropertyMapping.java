package com.legend.parser.element;

import java.util.Objects;

/**
 * One property binding inside an {@link AssociationMapping}.
 *
 * <p>Composition over duplication: an association property mapping is a
 * class-mapping-style {@link PropertyMapping} body plus an optional
 * {@code [sourceSetId, targetSetId]} pair disambiguating which class
 * mappings the association's two ends connect when ambiguity exists.
 *
 * <p>Surface syntax (engine):
 * <pre>
 *   propName: [db::DB] @SomeJoin                             // unbracketed (typical)
 *   propName[srcSetId, dstSetId]: [db::DB] @SomeJoin         // bracketed disambiguation
 * </pre>
 *
 * <p>{@code sourceSetId} and {@code targetSetId} are <strong>both</strong>
 * non-null or <strong>both</strong> null &mdash; they only appear together
 * in source. The compact constructor enforces this invariant.
 *
 * @param sourceSetId optional class-mapping set ID for the source end of
 *                    the association, or {@code null} when the user did
 *                    not write the disambiguation brackets
 * @param targetSetId optional class-mapping set ID for the target end
 * @param body        the underlying property mapping body (reuses the
 *                    class-mapping {@link PropertyMapping} sealed type;
 *                    {@code body.propertyName()} carries the property name)
 */
public record AssociationPropertyMapping(
        String sourceSetId,
        String targetSetId,
        PropertyMapping body) {

    public AssociationPropertyMapping {
        Objects.requireNonNull(body, "Body cannot be null");
        if ((sourceSetId == null) != (targetSetId == null)) {
            throw new IllegalArgumentException(
                    "sourceSetId and targetSetId must both be set or both be null");
        }
    }

    /** Convenience: the property name from the underlying body. */
    public String propertyName() {
        return body.propertyName();
    }
}
