package com.gs.legend.compiled;

/**
 * Compiled-state type reference: the kind of the referenced type plus its
 * fully-qualified name.
 *
 * <p>Used by {@link CompiledProperty}, {@link CompiledDerivedProperty},
 * {@link CompiledParameter}, {@link CompiledAssociationEnd}, and
 * {@link CompiledFunction.returnTypeRef} to encode "what type is this?" as
 * a pure-data fingerprint (no live {@code GenericType} refs, no cycles).
 *
 * <p>Primitives keep {@code fqn} as the Pure primitive name (e.g.
 * {@code "Integer"}, {@code "String"}, {@code "Date"}).
 */
public record TypeRef(Kind kind, String fqn) {

    public enum Kind { PRIMITIVE, CLASS, ENUMERATION }

    public static TypeRef primitive(String pureName) {
        return new TypeRef(Kind.PRIMITIVE, pureName);
    }

    public static TypeRef classRef(String fqn) {
        return new TypeRef(Kind.CLASS, fqn);
    }

    public static TypeRef enumRef(String fqn) {
        return new TypeRef(Kind.ENUMERATION, fqn);
    }
}
