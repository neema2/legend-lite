// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.model;

import com.legend.protocol.Realization;

import java.util.List;
import java.util.Objects;

/**
 * The <strong>clean-sheet</strong> mapping surface tree &mdash; Door 1/3's
 * B&rarr;E artifact, mirroring {@link LegacyMappingDefinition}'s role for
 * the legacy door: the protocol/grammar door produces it, {@code
 * NameResolver} resolves it, {@code MappingNormalizer} is its sole
 * post-resolution consumer and lifts it into the compiled
 * {@link MappingDefinition}. It never appears in a {@code NormalizedModel}.
 *
 * <p>Phase invariants live in THIS type, not in the compiled artifact:
 * inline expression bodies ({@link Realization.Inline}) and function-ref
 * bindings are both legal here; sources are UNSTAMPED (the stamp is born
 * at the Phase-E lift, after name resolution). The compiled
 * {@link MappingDefinition.ClassBinding} carries neither a
 * {@link Realization} union nor an absent source &mdash; eradicating the
 * former {@code Undeclared} placeholder (census 2026-08-30: separate
 * phase types instead of in-band pre-phase markers).
 *
 * @param qualifiedName        fully-qualified mapping name
 * @param includes             included mappings, with optional store substitutions
 * @param classBindings        per-class bindings (ref or inline)
 * @param associationBindings  per-association predicate bindings (ref or inline)
 * @param enumerationMappings  per-enumeration mappings
 * @param testSuitesSource     raw {@code testSuites: [...]} text, or {@code null}
 */
public record CleanSheetMappingDefinition(
        String qualifiedName,
        List<MappingInclude> includes,
        List<ClassBinding> classBindings,
        List<AssociationBinding> associationBindings,
        List<EnumerationMapping> enumerationMappings,
        @com.legend.base.Nullable String testSuitesSource)
        implements PackageableElement {

    public CleanSheetMappingDefinition {
        Objects.requireNonNull(qualifiedName, "Qualified name cannot be null");
        includes = includes == null ? List.of() : List.copyOf(includes);
        classBindings = classBindings == null ? List.of() : List.copyOf(classBindings);
        associationBindings = associationBindings == null
                ? List.of() : List.copyOf(associationBindings);
        enumerationMappings = enumerationMappings == null
                ? List.of() : List.copyOf(enumerationMappings);
    }

    /** Class-binding kind tag. Both kinds realize {@code Class[*]}; the tag
     * is a property of the binding relationship, not derivable from the
     * function (MAPPING_CLEAN_SHEET.md §1). Pre-E only &mdash; the compiled
     * binding's VARIANT is the kind. */
    public enum Kind { RELATIONAL, PURE }

    /** A pre-lift class binding: the body may still be inline. */
    public record ClassBinding(
            String classFqn,
            Kind kind,
            @com.legend.base.Nullable String setId,
            @com.legend.base.Nullable String extendsSetId,
            boolean root,
            Realization realization,
            List<String> primaryKeyColumns) {
        public ClassBinding {
            Objects.requireNonNull(classFqn, "classFqn");
            Objects.requireNonNull(kind, "kind");
            Objects.requireNonNull(realization, "realization");
            primaryKeyColumns = primaryKeyColumns == null ? List.of()
                    : List.copyOf(primaryKeyColumns);
        }
    }

    /** A pre-lift association binding: the predicate may still be inline. */
    public record AssociationBinding(String associationFqn, Realization realization) {
        public AssociationBinding {
            Objects.requireNonNull(associationFqn, "associationFqn");
            Objects.requireNonNull(realization, "realization");
        }
    }
}
