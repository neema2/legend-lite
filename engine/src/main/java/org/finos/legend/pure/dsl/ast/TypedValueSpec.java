package org.finos.legend.pure.dsl.ast;

import org.finos.legend.engine.plan.RelationType;
import org.finos.legend.engine.store.RelationalMapping;

/**
 * Result of compiling a ValueSpecification through the CleanCompiler.
 *
 * <p>
 * Carries the original AST tree annotated with resolved types, plus the
 * optional {@link RelationalMapping} for property→column resolution in
 * downstream operations (e.g., project, filter on class-based queries).
 *
 * @param ast        The ValueSpecification AST
 * @param resultType The output RelationType (columns + types)
 * @param mapping    The mapping context (nullable, for class-based queries)
 */
public record TypedValueSpec(
        ValueSpecification ast,
        RelationType resultType,
        RelationalMapping mapping) {

    /** Two-arg constructor (no mapping). */
    public TypedValueSpec(ValueSpecification ast, RelationType resultType) {
        this(ast, resultType, null);
    }

    /** Creates a TypedValueSpec with empty result type (for scalars). */
    public static TypedValueSpec scalar(ValueSpecification ast) {
        return new TypedValueSpec(ast, RelationType.empty(), null);
    }
}
