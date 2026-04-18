package com.gs.legend.model.m3;

/**
 * Sealed interface representing a declared type in the Pure model — the entity
 * that shows up in the symbol table as a named declaration.
 *
 * <p>Analogous to Java's {@code javax.lang.model.element.TypeElement}: a
 * {@code TypeDecl} is a declaration ({@link PrimitiveType}, {@link PureClass},
 * {@link PureEnumType}), not a type expression. Type expressions (primitive
 * references, parameterized types, relations, functions, etc.) are represented
 * by the forthcoming {@code m3.Type} sealed hierarchy, which is orthogonal.
 *
 * <p>Renamed from {@code m3.Type} in Phase B chunk 2.5a to free the {@code Type}
 * name for the unified type-expression hierarchy.
 */
public sealed interface TypeDecl permits PrimitiveType, PureClass, PureEnumType {

    /**
     * @return The name of this declared type (simple name for classes/enums,
     *         Pure-level name for primitives).
     */
    String typeName();
}
