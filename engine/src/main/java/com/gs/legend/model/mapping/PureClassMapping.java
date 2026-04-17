package com.gs.legend.model.mapping;

import com.gs.legend.ast.ValueSpecification;
import com.gs.legend.model.m3.PureClass;
import com.gs.legend.model.store.Table;
import com.gs.legend.plan.GenericType;

import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * Represents a Model-to-Model (Pure) class mapping using clean AST nodes.
 * 
 * Pure syntax:
 * <pre>
 * TargetClass: Pure
 * {
 *     ~src SourceClass
 *     ~filter $src.isActive == true
 *     
 *     targetProp1: $src.sourceProp1,
 *     targetProp2: $src.first + ' ' + $src.last
 * }
 * </pre>
 * 
 * Property expressions are pre-parsed by ValueSpecificationBuilder into ValueSpecification
 * AST nodes. The compiler type-checks these; PlanGenerator emits SQL.
 * 
 * @param targetClassName  The target class being mapped to
 * @param sourceClassName  The source class being mapped from (~src)
 * @param propertyExpressions  Map of property name → pre-parsed AST expression
 * @param filter  Optional filter expression (~filter), pre-parsed
 * @param targetClass  Resolved target PureClass (may be null if not yet resolved)
 * @param sourceMapping  Resolved source ClassMapping (may be null if not yet resolved)
 */
public record PureClassMapping(
        String targetClassName,
        String sourceClassName,
        Map<String, ValueSpecification> propertyExpressions,
        ValueSpecification filter,
        PureClass targetClass,
        ClassMapping sourceMapping
) implements ClassMapping {
    public PureClassMapping {
        Objects.requireNonNull(targetClassName, "Target class name cannot be null");
        Objects.requireNonNull(sourceClassName, "Source class name cannot be null");
        Objects.requireNonNull(propertyExpressions, "Property expressions cannot be null");
        propertyExpressions = Map.copyOf(propertyExpressions);
    }

    /**
     * Creates a PureClassMapping without filter (and unresolved).
     */
    public static PureClassMapping of(
            String targetClassName,
            String sourceClassName,
            Map<String, ValueSpecification> propertyExpressions) {
        return new PureClassMapping(targetClassName, sourceClassName, propertyExpressions,
                null, null, null);
    }

    /**
     * Creates a resolved copy with target PureClass and source ClassMapping.
     */
    public PureClassMapping withResolved(PureClass resolvedTarget, ClassMapping resolvedSource) {
        return new PureClassMapping(targetClassName, sourceClassName, propertyExpressions,
                filter, resolvedTarget, resolvedSource);
    }

    /**
     * @return The optional filter expression
     */
    public Optional<ValueSpecification> optionalFilter() {
        return Optional.ofNullable(filter);
    }

    // ========== ClassMapping interface ==========

    @Override
    public Table sourceTable() {
        if (sourceMapping != null) {
            return sourceMapping.sourceTable();
        }
        throw new IllegalStateException(
                "PureClassMapping for '" + targetClassName + "' has unresolved source mapping. "
                + "Source class '" + sourceClassName + "' must be resolved via MappingRegistry.");
    }

    @Override
    public ValueSpecification expressionForProperty(String propertyName) {
        var expr = propertyExpressions.get(propertyName);
        if (expr != null) {
            return expr;
        }
        throw new IllegalArgumentException(
                "Property '" + propertyName + "' not found in M2M mapping for " + targetClassName);
    }

    @Override
    public GenericType typeForProperty(String propertyName) {
        if (targetClass != null) {
            var propOpt = targetClass.findProperty(propertyName);
            if (propOpt.isPresent()) {
                return GenericType.fromTypeRef(propOpt.get().typeRef());
            }
        }
        // Default to String if we can't resolve
        return GenericType.Primitive.STRING;
    }

    @Override
    public boolean hasProperty(String propertyName) {
        return propertyExpressions.containsKey(propertyName);
    }
}
