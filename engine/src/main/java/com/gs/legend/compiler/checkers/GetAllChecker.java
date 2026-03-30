package com.gs.legend.compiler.checkers;

import com.gs.legend.ast.AppliedFunction;
import com.gs.legend.ast.PackageableElementPtr;
import com.gs.legend.ast.ValueSpecification;
import com.gs.legend.compiler.*;
import com.gs.legend.model.mapping.ClassMapping;
import com.gs.legend.model.mapping.MappingRegistry;
import com.gs.legend.model.mapping.PureClassMapping;
import com.gs.legend.model.mapping.RelationalMapping;
import com.gs.legend.model.m3.PureClass;
import com.gs.legend.plan.GenericType;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Checker for {@code getAll()}.
 *
 * <p>Resolves class → mapping → type:
 * <ul>
 *   <li>RelationalMapping → ClassType[*]</li>
 *   <li>PureClassMapping → M2M chain resolution + virtual schema</li>
 * </ul>
 */
public class GetAllChecker extends AbstractChecker {

    public GetAllChecker(TypeCheckEnv env) {
        super(env);
    }

    @Override
    public TypeInfo check(AppliedFunction af, TypeInfo source,
                          TypeChecker.CompilationContext ctx) {
        List<ValueSpecification> params = af.parameters();
        resolveOverload("getAll", params, null);

        // Extract class name from PackageableElementPtr
        if (!(params.get(0) instanceof PackageableElementPtr(String fullPath))) {
            throw new PureCompileException(
                    "getAll(): first argument must be a class reference, got "
                            + params.get(0).getClass().getSimpleName());
        }
        String className = TypeInfo.simpleName(fullPath);

        // Single lookup via findAnyMapping — covers both relational and M2M
        ClassMapping mapping = registry().findAnyMapping(className)
                .orElseThrow(() -> new PureCompileException(
                        "getAll(): no mapping found for class '" + className + "'"));

        if (mapping instanceof PureClassMapping pcm) {
            return compileM2MGetAll(pcm);
        }

        // Relational: return ClassType[*] — schema resolved later by project()
        return TypeInfo.builder()
                .mapping(mapping)
                .expressionType(ExpressionType.many(new GenericType.ClassType(fullPath)))
                .build();
    }

    // ==================== M2M Resolution ====================

    /**
     * Resolves M2M getAll: source chain → virtual schema → type-checks property expressions.
     */
    private TypeInfo compileM2MGetAll(PureClassMapping pureMapping) {
        ClassMapping srcMapping = resolveSource(pureMapping);

        PureClass targetClass = findClass(pureMapping.targetClassName())
                .orElseThrow(() -> new PureCompileException(
                        "M2M target class '" + pureMapping.targetClassName() + "' not found"));
        var resolvedMapping = pureMapping.withResolved(targetClass, srcMapping);

        // Build virtual schema from target class property types
        Map<String, GenericType> virtualColumns = new LinkedHashMap<>();
        for (String propName : pureMapping.propertyExpressions().keySet()) {
            virtualColumns.put(propName, resolvePropertyType(targetClass, propName));
        }

        // Add join-referenced properties to virtual schema
        for (String joinProp : pureMapping.joinReferences().keySet()) {
            virtualColumns.putIfAbsent(joinProp, resolvePropertyType(targetClass, joinProp));
        }

        // Type-check M2M property expressions and filter against source context
        TypeChecker.CompilationContext srcCtx = buildSourceContext(srcMapping);
        for (var entry : pureMapping.propertyExpressions().entrySet()) {
            env.compileExpr(entry.getValue(), srcCtx);
        }
        // Stamp filter expression so PlanGenerator can read $src.property nodes
        pureMapping.optionalFilter().ifPresent(f -> env.compileExpr(f, srcCtx));

        return TypeInfo.builder()
                .mapping(resolvedMapping)
                .expressionType(ExpressionType.many(
                        new GenericType.Relation(GenericType.Relation.Schema.withoutPivot(virtualColumns))))
                .build();
    }

    /**
     * Resolves the source mapping for an M2M class.
     * If the source is itself M2M, recursively resolves the chain first.
     */
    private ClassMapping resolveSource(PureClassMapping pureMapping) {
        String sourceClassName = pureMapping.sourceClassName();
        ClassMapping srcMapping = registry().findAnyMapping(sourceClassName)
                .orElseThrow(() -> new PureCompileException(
                        "M2M source class '" + sourceClassName + "' has no mapping"));

        if (srcMapping instanceof PureClassMapping srcPcm) {
            resolveM2MChain(srcPcm);
            srcMapping = registry().findAnyMapping(sourceClassName).orElseThrow();
        }
        return srcMapping;
    }

    /**
     * Recursively resolves M2M chain, linking each PureClassMapping to its source.
     * Chain must terminate at a RelationalMapping.
     */
    private void resolveM2MChain(PureClassMapping pcm) {
        String srcClassName = pcm.sourceClassName();
        ClassMapping srcMapping = registry().findAnyMapping(srcClassName)
                .orElseThrow(() -> new PureCompileException(
                        "M2M chain: source class '" + srcClassName + "' has no mapping"));

        if (srcMapping instanceof PureClassMapping innerPcm) {
            resolveM2MChain(innerPcm);
            srcMapping = registry().findAnyMapping(srcClassName).orElseThrow();
        }

        // Type-check intermediate property expressions and filter
        TypeChecker.CompilationContext srcCtx = buildSourceContext(srcMapping);
        for (var entry : pcm.propertyExpressions().entrySet()) {
            env.compileExpr(entry.getValue(), srcCtx);
        }
        pcm.optionalFilter().ifPresent(f -> env.compileExpr(f, srcCtx));

        PureClass targetClass = findClass(pcm.targetClassName())
                .orElseThrow(() -> new PureCompileException(
                        "M2M chain: target class '" + pcm.targetClassName() + "' not found"));
        registry().updatePureClassMapping(
                pcm.targetClassName(), pcm.withResolved(targetClass, srcMapping));
    }

    // ==================== Context Building ====================

    /**
     * Builds CompilationContext with $src bound to source schema + mapping.
     */
    private TypeChecker.CompilationContext buildSourceContext(ClassMapping srcMapping) {
        Map<String, GenericType> srcColumns = new LinkedHashMap<>();

        if (srcMapping instanceof RelationalMapping srcRm) {
            for (var pm : srcRm.propertyMappings()) {
                srcColumns.put(pm.propertyName(), srcRm.pureTypeForProperty(pm.propertyName()));
            }
        } else if (srcMapping instanceof PureClassMapping srcPcm) {
            PureClass srcClass = findClass(srcPcm.targetClassName())
                    .orElseThrow(() -> new PureCompileException(
                            "M2M source class '" + srcPcm.targetClassName() + "' not found"));
            for (String propName : srcPcm.propertyExpressions().keySet()) {
                srcColumns.put(propName, resolvePropertyType(srcClass, propName));
            }
        } else {
            throw new PureCompileException(
                    "getAll: unsupported source mapping type: "
                            + srcMapping.getClass().getSimpleName());
        }

        return new TypeChecker.CompilationContext()
                .withRelationType("src", GenericType.Relation.Schema.withoutPivot(srcColumns));
    }

    // ==================== Type Resolution ====================

    /**
     * Resolves a property's Pure type from its owning class.
     * Uses PureClass.findProperty which already searches superclasses.
     */
    private static GenericType resolvePropertyType(PureClass pureClass, String propertyName) {
        return pureClass.findProperty(propertyName)
                .map(prop -> GenericType.fromTypeName(prop.genericType().typeName()))
                .orElseThrow(() -> new PureCompileException(
                        "Property '" + propertyName + "' not found on class '"
                                + pureClass.qualifiedName() + "'"));
    }

    // ==================== Helpers ====================

    private MappingRegistry registry() {
        return env.modelContext().getMappingRegistry();
    }
}
