package com.gs.legend.compiler.typed;

import com.gs.legend.compiler.ExpressionType;

/**
 * Reference to a packageable element (function, class, enum, runtime, store, …)
 * by its fully-qualified name.
 *
 * <p>Produced by {@code TypeChecker.resolvePackageableElement} when a
 * {@code PackageableElementPtr} appears at an expression position and isn't
 * structurally rewritten (e.g., to an {@code AppliedFunction}) by its enclosing
 * checker. The {@code info().type()} carries the resolved kind:
 * {@link com.gs.legend.model.m3.Type.FunctionReference} for functions,
 * {@link com.gs.legend.model.m3.Type.ClassType} for classes and opaque named
 * references, and {@link com.gs.legend.model.m3.Type.EnumType} for enums.
 *
 * @param fullPath Fully-qualified name as it appeared in source.
 * @param info     Resolved type + multiplicity (always {@code [1]}).
 */
public record TypedPackageableRef(
        String fullPath,
        ExpressionType info
) implements TypedSpec {}
