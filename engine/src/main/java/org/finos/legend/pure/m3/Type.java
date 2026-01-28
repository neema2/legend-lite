package org.finos.legend.pure.m3;

/**
 * Sealed interface representing all types in the Pure type system.
 * This is the root of the type hierarchy.
 * 
 * Permitted implementations:
 * - PrimitiveType: Built-in scalar types (String, Integer, Boolean, Date)
 * - PureClass: User-defined composite types with properties
 */
public sealed interface Type permits PrimitiveType, PureClass {
    
    /**
     * @return The name of this type
     */
    String typeName();
}
