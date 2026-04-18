package com.gs.legend.model.m3;

/**
 * Sealed interface representing a user-defined declared type in the Pure model —
 * a named declaration that shows up in the symbol table.
 *
 * <p>Analogous to Java's {@code javax.lang.model.element.TypeElement}: a
 * {@code TypeDecl} is a declaration ({@link PureClass} or {@link PureEnumType}),
 * not a type expression. Type expressions (primitives, parameterized types,
 * relations, functions, etc.) live on the orthogonal {@link Type} sealed hierarchy.
 *
 * <p>Primitives are intentionally <em>not</em> declarations — they're built-in
 * type expressions modeled by {@link Type.Primitive}.
 *
 * <p>Renamed from {@code m3.Type} in Phase B chunk 2.5a to free the {@code Type}
 * name for the unified type-expression hierarchy; {@code PrimitiveType} dropped
 * from the permits clause in 2.5b.3 when the redundant enum was deleted.
 */
public sealed interface TypeDecl permits PureClass, PureEnumType {

    /**
     * @return The simple name of this declared type (e.g., "Person" for
     *         {@code model::Person}).
     */
    String typeName();
}
