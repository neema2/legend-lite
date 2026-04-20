package com.gs.legend.model.mapping;

import com.gs.legend.ast.ValueSpecification;
import com.gs.legend.model.store.Table;
import com.gs.legend.model.m3.Type;

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
 * <p>The target class is referenced by FQN ({@code targetClassName}) only — resolved lazily
 * via {@link com.gs.legend.model.ModelContext#findClass} at use time (e.g., in
 * {@link #typeForProperty}). AGENTS.md §5: cross-project mapping targets must not force
 * their containing project to load eagerly.
 *
 * @param targetClassName  The target class being mapped to (FQN)
 * @param sourceClassName  The source class being mapped from (~src)
 * @param propertyExpressions  Map of property name → pre-parsed AST expression
 * @param filter  Optional filter expression (~filter), pre-parsed
 * @param sourceMapping  Resolved source ClassMapping (may be null if not yet resolved)
 */
public record PureClassMapping(
        String targetClassName,
        String sourceClassName,
        Map<String, ValueSpecification> propertyExpressions,
        ValueSpecification filter,
        ClassMapping sourceMapping
) implements ClassMapping {
    public PureClassMapping {
        Objects.requireNonNull(targetClassName, "Target class name cannot be null");
        Objects.requireNonNull(sourceClassName, "Source class name cannot be null");
        Objects.requireNonNull(propertyExpressions, "Property expressions cannot be null");
        propertyExpressions = Map.copyOf(propertyExpressions);
    }

    /**
     * Creates a resolved copy with the source ClassMapping filled in. The target class is
     * already tracked by FQN and resolved lazily — no separate "resolve" step needed.
     */
    public PureClassMapping withResolvedSource(ClassMapping resolvedSource) {
        return new PureClassMapping(targetClassName, sourceClassName, propertyExpressions,
                filter, resolvedSource);
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
    public Type typeForProperty(String propertyName, com.gs.legend.model.ModelContext ctx) {
        // No fallback — if the target class or property isn't resolvable, the model is
        // incomplete and downstream SQL generation would produce wrong types. AGENTS.md
        // #4 / #8 — fail loudly, don't silently default.
        var targetClass = ctx.findClass(targetClassName)
                .orElseThrow(() -> new IllegalStateException(
                        "PureClassMapping for '" + targetClassName + "': target class not in "
                        + "model context — cannot determine type for property '" + propertyName + "'"));
        return targetClass.findProperty(propertyName, ctx)
                .orElseThrow(() -> new IllegalArgumentException(
                        "Property '" + propertyName + "' not found on class '"
                        + targetClass.qualifiedName() + "'"))
                .type();
    }

    @Override
    public boolean hasProperty(String propertyName) {
        return propertyExpressions.containsKey(propertyName);
    }
}
