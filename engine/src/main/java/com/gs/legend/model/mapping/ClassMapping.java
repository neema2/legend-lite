package com.gs.legend.model.mapping;
import com.gs.legend.model.store.*;
import com.gs.legend.ast.ValueSpecification;
import com.gs.legend.plan.GenericType;
import com.gs.legend.model.m3.PureClass;

/**
 * Unified mapping interface for all class-to-store mappings.
 *
 * <p>All property resolution is expression-based: a simple column reference
 * is just the trivial expression. This unifies relational (column, expression,
 * enum) and M2M (computed expression) mappings under one interface.
 *
 * <p>Joins (association navigation) are handled separately via
 * {@code TypeInfo.AssociationTarget} in the compiler sidecar — they produce
 * relations, not scalar values.
 */
public sealed interface ClassMapping permits RelationalMapping, PureClassMapping {

    /**
     * The target Pure class this mapping resolves.
     */
    PureClass targetClass();

    /**
     * The source table to SELECT FROM.
     * For relational: the mapped table.
     * For M2M: the source class's table (resolved through the source mapping chain).
     */
    Table sourceTable();

    /**
     * Resolves a property to its value expression.
     *
     * <p>For relational column: {@code AppliedProperty("COLUMN_NAME", [Variable("src")])}
     * <p>For relational expression: pre-parsed variant access expression
     * <p>For relational enum: pre-built CASE WHEN expression
     * <p>For M2M: pre-parsed M2M expression ({@code $src.firstName + ' ' + $src.lastName})
     *
     * @param propertyName The Pure property name
     * @return The expression that computes this property's value
     * @throws IllegalArgumentException if property not found
     */
    ValueSpecification expressionForProperty(String propertyName);

    /**
     * Gets the type of a property.
     *
     * @param propertyName The Pure property name
     * @return The GenericType for this property
     */
    GenericType typeForProperty(String propertyName);

    /**
     * Whether this mapping has an expression for the given property.
     */
    boolean hasProperty(String propertyName);
}
