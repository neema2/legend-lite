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
 * @param sourceKind What kind of data source backs this query
 */
public record TypedValueSpec(
        ValueSpecification ast,
        RelationType resultType,
        RelationalMapping mapping,
        SourceKind sourceKind) {

    /**
     * The kind of data source that originates a query.
     * Set by CleanCompiler at the source (getAll, struct literal, etc.)
     * and propagated through all downstream operations (filter, sort, project...).
     * Used by SqlCompiler as the single routing signal.
     */
    public enum SourceKind {
        /** Person.all() — class-based query with mapping */
        CLASS_ALL,
        /** ^Person(name='John') — struct literal, compiles to VALUES */
        CLASS_INSTANCE,
        /** #TDS val,str\n-- 1,'a' — inline tabular data */
        TDS_LITERAL,
        /** #>{db.TABLE} — direct relation/table accessor */
        RELATION,
        /** {|42}, {|'hello'}, {|today()} — standalone scalar expression */
        SCALAR
    }
}
