package com.legend.parser.element;

import java.util.List;
import java.util.Objects;

/**
 * A relational association mapping inside a {@link MappingDefinition} body.
 *
 * <p>Surface syntax:
 * <pre>
 *   path::To::SomeAssociation: Relational {
 *     AssociationMapping (
 *       propName1: [db::DB] @Join1,
 *       propName2: [db::DB] @Join2
 *     )
 *   }
 * </pre>
 *
 * <p>Sealed because B.4c only supports the {@code Relational} flavor.
 * Future variants (M2M associations, etc.) would add new permits.
 *
 * <p>An association mapping is structurally distinct from a class mapping:
 * <ul>
 *   <li>No {@code *} root marker (associations are inherently shared).</li>
 *   <li>No {@code ~mainTable} &mdash; the association is realized through
 *       joins, not by anchoring to a table.</li>
 *   <li>No {@code ~filter}/{@code ~distinct}/{@code ~groupBy}/{@code ~primaryKey}.</li>
 *   <li>Property mappings may optionally carry {@code [sourceSetId, targetSetId]}
 *       brackets disambiguating which class mappings the two ends connect.</li>
 * </ul>
 *
 * <p>Bare identifiers are NOT allowed in association property bodies:
 * there is no main table to scope them. Engine parity: the parser leaves
 * {@code currentMappingScope = null} while parsing an association body, so
 * the same "Missing table or alias" error fires for bare identifiers.
 */
public sealed interface AssociationMapping permits AssociationMapping.Relational {

    /** Fully qualified path of the association being mapped. */
    String associationName();

    /** Per-property bindings; never {@code null}, may be empty. */
    List<AssociationPropertyMapping> propertyMappings();

    /**
     * Relational flavor: bodies typically navigate joins.
     */
    record Relational(
            String associationName,
            List<AssociationPropertyMapping> propertyMappings)
            implements AssociationMapping {

        public Relational {
            Objects.requireNonNull(associationName, "Association name cannot be null");
            propertyMappings = propertyMappings != null
                    ? List.copyOf(propertyMappings)
                    : List.of();
        }
    }
}
