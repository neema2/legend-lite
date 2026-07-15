package com.legend.model;

import com.legend.model.spec.ValueSpecification;

import java.util.List;
import java.util.Objects;

/**
 * The canonical {@code Mapping} element &mdash; a <strong>binding table</strong>
 * (docs/CLEAN_SHEET_INVERSION.md §2.1). Structure only: each binding pairs a
 * class / association with a realizing function <em>by FQN</em>; no
 * {@code ValueSpecification} and no DSL body lives here. This is the form every
 * phase after E sees and the form the clean-sheet surface parses to directly
 * (Door 1); the legacy DSL ({@link LegacyMappingDefinition}) is rewritten into
 * it by {@code MappingNormalizer}.
 *
 * <p>Bodies live in ordinary {@link FunctionDefinition}s in the model's element
 * list, lifted by Phase E and named per {@code SynthFqn} (the
 * {@code <mapping>$class$<classFqn>} / {@code <mapping>$assoc$<assocFqn>}
 * scheme). A {@link ClassBinding#functionFqn()} is exactly the lifted
 * function's FQN, so dispatch (§6) is: binding table &rarr; FQN &rarr; the one
 * {@code findFunction} index. Nothing reconstructs or parses these strings.
 *
 * @param qualifiedName        fully-qualified mapping name
 * @param includes             included mappings, with optional store substitutions
 * @param classBindings        per-class realizing-function bindings
 * @param associationBindings  per-association predicate-function bindings
 * @param enumerationMappings  per-enumeration mappings (inline static tables &mdash;
 *                             data, not expressions, so they stay structural)
 * @param testSuitesSource     raw {@code testSuites: [...]} text, or {@code null}
 */
public record MappingDefinition(
        String qualifiedName,
        List<MappingInclude> includes,
        List<ClassBinding> classBindings,
        List<AssociationBinding> associationBindings,
        List<EnumerationMapping> enumerationMappings,
        String testSuitesSource)
        implements PackageableElement {

    public MappingDefinition {
        Objects.requireNonNull(qualifiedName, "Qualified name cannot be null");
        includes = includes == null ? List.of() : List.copyOf(includes);
        classBindings = classBindings == null ? List.of() : List.copyOf(classBindings);
        associationBindings = associationBindings == null ? List.of() : List.copyOf(associationBindings);
        enumerationMappings = enumerationMappings == null ? List.of() : List.copyOf(enumerationMappings);
    }

    /** Class-mapping kind tag. Both kinds realize {@code Class[*]}; the tag is a
     * property of the binding relationship, not derivable from the function
     * (MAPPING_CLEAN_SHEET.md §1). */
    public enum Kind { RELATIONAL, PURE }

    /**
     * A class binding: structure only; the body is a {@link Realization}
     * (function ref or &mdash; B&rarr;E only &mdash; an inline expression).
     *
     * @param classFqn      the mapped class
     * @param kind          {@code RELATIONAL} | {@code PURE}
     * @param setId         explicit set identifier, or {@code null} for the default set
     * @param extendsSetId  parent set id for {@code extends [parentId]}, or {@code null}
     * @param root          {@code true} when marked root ({@code *})
     * @param realization   how the binding is realized ({@link Realization.Ref} /
     *                      {@link Realization.Inline})
     */
    public record ClassBinding(
            String classFqn,
            Kind kind,
            String setId,
            String extendsSetId,
            boolean root,
            Realization realization) {
        public ClassBinding {
            Objects.requireNonNull(classFqn, "classFqn");
            Objects.requireNonNull(kind, "kind");
            Objects.requireNonNull(realization, "realization");
        }

        /** Convenience: a function-ref binding (Door 1 / post-lift). */
        public ClassBinding(String classFqn, Kind kind, String setId,
                            String extendsSetId, boolean root, String functionFqn) {
            this(classFqn, kind, setId, extendsSetId, root, new Realization.Ref(functionFqn));
        }

        /**
         * The realizing function's FQN. Valid only when the realization is a
         * {@link Realization.Ref} &mdash; always true after Phase E, since the
         * normalizer lifts every {@link Realization.Inline}. Throws on an
         * unlifted inline binding (a phase-ordering bug, surfaced loudly).
         */
        public String functionFqn() {
            if (realization instanceof Realization.Ref r) return r.functionFqn();
            throw new IllegalStateException(
                    "class binding for '" + classFqn + "' is an unlifted inline body");
        }
    }

    /**
     * An association binding: the association realized by a predicate. The body
     * is a {@link Realization} (a predicate-function ref, or &mdash; B&rarr;E
     * only &mdash; an inline {@code (Source[1], Target[1]) -> Boolean[1]} lambda).
     *
     * @param associationFqn the mapped association
     * @param realization    how the predicate is realized
     */
    public record AssociationBinding(String associationFqn, Realization realization) {
        public AssociationBinding {
            Objects.requireNonNull(associationFqn, "associationFqn");
            Objects.requireNonNull(realization, "realization");
        }

        /** Convenience: a function-ref predicate binding (Door 1 / post-lift). */
        public AssociationBinding(String associationFqn, String predicateFunctionFqn) {
            this(associationFqn, new Realization.Ref(predicateFunctionFqn));
        }

        /** The predicate function's FQN. Valid only post-lift (see {@link ClassBinding#functionFqn()}). */
        public String predicateFunctionFqn() {
            if (realization instanceof Realization.Ref r) return r.functionFqn();
            throw new IllegalStateException(
                    "association binding for '" + associationFqn + "' is an unlifted inline body");
        }
    }
}
