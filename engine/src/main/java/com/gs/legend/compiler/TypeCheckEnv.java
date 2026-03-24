package com.gs.legend.compiler;

import com.gs.legend.ast.ValueSpecification;
import com.gs.legend.model.mapping.ClassMapping;

import java.util.List;
import java.util.Map;

/**
 * Callback interface for individual type-checkers.
 *
 * <p>
 * Individual function checkers (FilterChecker, SortChecker, etc.) receive a
 * {@code TypeCheckEnv} to compile sub-expressions and access shared services.
 * TypeChecker implements this interface.
 */
public interface TypeCheckEnv {

    /** Compile a value specification and return its type info. */
    TypeInfo compileExpr(ValueSpecification vs, TypeChecker.CompilationContext ctx);

    /**
     * Resolve association targets in lambda bodies for multi-hop property paths.
     * Returns a map of association property name → pre-resolved target.
     */
    Map<String, TypeInfo.AssociationTarget> resolveAssociations(
            List<ValueSpecification> body, ClassMapping mapping);

    /** Access model context for class hierarchy resolution (e.g., LCA in concatenate). */
    com.gs.legend.model.ModelContext modelContext();

    /** Look up the TypeInfo for a previously compiled AST node (by identity). */
    TypeInfo lookupCompiled(ValueSpecification vs);
}
