package com.legend.compiler.element;

import java.util.List;
import java.util.Objects;

/**
 * A compiled Pure {@code Class} (Phase F) &mdash; the typed counterpart of
 * {@link com.legend.model.ClassDefinition}. Pure data; superclasses
 * are <strong>FQN strings</strong> (never live refs), so structural walks go
 * through {@code ModelContext.findClass} and cross-project lazy loading works
 * transparently (doc §5; AGENTS.md invariant 5).
 *
 * <p>Holds <em>locally declared</em> members only. Inherited and
 * association-injected navigation properties are resolved at lookup time via
 * {@code ModelContext.findProperty}, never embedded here (doc §5 discipline 3).
 * Both stored and derived properties live in {@link #properties()} as the
 * polymorphic {@link Property}; a derived property keeps its signature here and
 * references its externalized body function by FQN.
 *
 * @param qualifiedName   fully qualified class name
 * @param typeParameters  generic type parameter names ({@code <T, U>}); empty if none
 * @param superClassFqns  direct superclass FQNs; empty if none
 * @param properties      locally declared properties (stored + derived), in source order
 * @param constraints     class constraints (metadata; bodies externalized, §1.4)
 * @param isNative        {@code true} for bootstrap native classes (from {@code builtin/Pure})
 */
public record TypedClass(
        String qualifiedName,
        List<String> typeParameters,
        List<String> superClassFqns,
        List<Property> properties,
        List<TypedConstraint> constraints,
        boolean isNative) implements TypedNominal {

    public TypedClass {
        Objects.requireNonNull(qualifiedName, "qualifiedName");
        typeParameters = typeParameters == null ? List.of() : List.copyOf(typeParameters);
        superClassFqns = superClassFqns == null ? List.of() : List.copyOf(superClassFqns);
        properties = properties == null ? List.of() : List.copyOf(properties);
        constraints = constraints == null ? List.of() : List.copyOf(constraints);
    }
}
