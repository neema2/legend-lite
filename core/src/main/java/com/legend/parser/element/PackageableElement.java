package com.legend.parser.element;

/**
 * Sealed root for parsed top-level Pure declarations.
 *
 * <p>Every variant is a record that captures the structure the parser saw,
 * with simple-name type references (resolved later by {@code NameResolver})
 * and no semantic types attached.
 *
 * <p>The {@code permits} clause grows as each element kind lands in
 * sub-slices B.1 through B.4. Today only {@link ClassDefinition} is
 * implemented; other kinds will be added incrementally.
 *
 * <p>Mirrors engine's
 * {@code com.gs.legend.model.def.PackageableElement}, with one
 * deliberate omission: <strong>no {@code simpleName()} or
 * {@code packagePath()} default methods.</strong> Those exist on engine's
 * version and invite an attractive nuisance &mdash; a caller writes
 * {@code modelContext.findClass(element.simpleName())}, the lookup hits
 * the wrong class, and the bug surfaces only when two elements share a
 * simple name across packages. Keys are {@link #qualifiedName()}, full
 * stop. Display callers can compute a simple name inline if they need
 * one (one substring), and we'll add a typed display helper if it
 * becomes a real pattern.
 */
public sealed interface PackageableElement
        permits ClassDefinition,
                AssociationDefinition,
                EnumDefinition,
                ProfileDefinition,
                FunctionDefinition,
                NativeFunctionDefinition,
                ServiceDefinition,
                RuntimeDefinition,
                ConnectionDefinition,
                DatabaseDefinition,
                MappingDefinition {

    /** Fully qualified name, e.g. {@code "model::Person"}. The only identity. */
    String qualifiedName();
}
