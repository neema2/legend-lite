package com.gs.legend.parser;

import com.gs.legend.ast.*;
import com.gs.legend.model.def.*;
import com.gs.legend.model.def.ClassDefinition.*;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * Name resolution pass — resolves simple names to FQN using imports.
 *
 * <p>Sits between parsing and model building in the compiler pipeline:
 * <pre>
 * Parser → NameResolver → PureModelBuilder / TypeChecker
 * </pre>
 *
 * <p>Resolution rule (matches legend-engine CompileContext.resolve):
 * <ul>
 *   <li>Contains {@code ::} → already FQN, return as-is</li>
 *   <li>Simple name → try each wildcard import ({@code pkg::name}), check against {@code knownFqns}</li>
 *   <li>0 matches → return as-is (may be a primitive like String, Integer)</li>
 *   <li>1 match → return it</li>
 *   <li>&gt;1 matches → ambiguity error</li>
 * </ul>
 */
public final class NameResolver {

    private NameResolver() {}

    /**
     * Resolves all simple name references in model definitions to FQN.
     *
     * <p>Walks each definition record and resolves name fields:
     * <ul>
     *   <li>{@link ClassDefinition}: superclass names, property types, derived property types</li>
     *   <li>{@link AssociationDefinition}: target class names on each end</li>
     *   <li>{@link MappingDefinition}: className and sourceClassName in class mappings</li>
     * </ul>
     *
     * <p>Definition types with only FQN qualifiedName (Database, Runtime, Service, etc.)
     * pass through unchanged — their internal references use element names that are
     * already scoped (e.g., table names, join names).
     *
     * @param definitions The raw definitions from the parser
     * @param imports     The import scope extracted from the same source
     * @param knownFqns   All registered FQN element names
     * @return New list with resolved definitions (originals unchanged if already FQN)
     */
    public static List<PackageableElement> resolveDefinitions(
            List<PackageableElement> definitions, ImportScope imports, Set<String> knownFqns) {

        List<PackageableElement> resolved = new ArrayList<>(definitions.size());
        for (PackageableElement def : definitions) {
            resolved.add(resolveDefinition(def, imports, knownFqns));
        }
        return resolved;
    }

    private static PackageableElement resolveDefinition(
            PackageableElement def, ImportScope imports, Set<String> knownFqns) {
        return switch (def) {
            case ClassDefinition classDef -> resolveClass(classDef, imports, knownFqns);
            case AssociationDefinition assocDef -> resolveAssociation(assocDef, imports, knownFqns);
            case MappingDefinition mappingDef -> resolveMapping(mappingDef, imports, knownFqns);
            case FunctionDefinition funcDef -> resolveFunction(funcDef, imports, knownFqns);
            default -> def; // Database, Runtime, Service, etc. — no simple name fields
        };
    }

    // ==================== ClassDefinition ====================

    private static ClassDefinition resolveClass(
            ClassDefinition classDef, ImportScope imports, Set<String> knownFqns) {

        // Resolve superclass names
        List<String> resolvedSuperClasses = classDef.superClasses().stream()
                .map(s -> imports.resolve(s, knownFqns))
                .toList();

        // Resolve property types
        List<PropertyDefinition> resolvedProps = classDef.properties().stream()
                .map(p -> resolveProperty(p, imports, knownFqns))
                .toList();

        // Resolve derived property types
        List<DerivedPropertyDefinition> resolvedDerived = classDef.derivedProperties().stream()
                .map(d -> resolveDerivedProperty(d, imports, knownFqns))
                .toList();

        // Only create new record if something changed
        if (resolvedSuperClasses.equals(classDef.superClasses())
                && resolvedProps.equals(classDef.properties())
                && resolvedDerived.equals(classDef.derivedProperties())) {
            return classDef;
        }

        return new ClassDefinition(
                classDef.qualifiedName(),
                resolvedSuperClasses,
                resolvedProps,
                resolvedDerived,
                classDef.constraints(),
                classDef.stereotypes(),
                classDef.taggedValues());
    }

    private static PropertyDefinition resolveProperty(
            PropertyDefinition prop, ImportScope imports, Set<String> knownFqns) {
        String resolvedType = imports.resolve(prop.type(), knownFqns);
        if (resolvedType.equals(prop.type())) return prop;
        return new PropertyDefinition(
                prop.name(), resolvedType, prop.lowerBound(), prop.upperBound(),
                prop.stereotypes(), prop.taggedValues());
    }

    private static DerivedPropertyDefinition resolveDerivedProperty(
            DerivedPropertyDefinition dp, ImportScope imports, Set<String> knownFqns) {
        String resolvedType = imports.resolve(dp.type(), knownFqns);
        List<ParameterDefinition> resolvedParams = dp.parameters().stream()
                .map(p -> resolveParameter(p, imports, knownFqns))
                .toList();
        if (resolvedType.equals(dp.type()) && resolvedParams.equals(dp.parameters())) return dp;
        return new DerivedPropertyDefinition(
                dp.name(), resolvedParams, dp.expression(), resolvedType,
                dp.lowerBound(), dp.upperBound());
    }

    private static ParameterDefinition resolveParameter(
            ParameterDefinition param, ImportScope imports, Set<String> knownFqns) {
        String resolvedType = imports.resolve(param.type(), knownFqns);
        if (resolvedType.equals(param.type())) return param;
        return new ParameterDefinition(param.name(), resolvedType, param.lowerBound(), param.upperBound());
    }

    // ==================== FunctionDefinition ====================

    private static FunctionDefinition resolveFunction(
            FunctionDefinition funcDef, ImportScope imports, Set<String> knownFqns) {

        String resolvedReturnType = imports.resolve(funcDef.returnType(), knownFqns);
        List<FunctionDefinition.ParameterDefinition> resolvedParams = funcDef.parameters().stream()
                .map(p -> resolveFuncParam(p, imports, knownFqns))
                .toList();

        if (resolvedReturnType.equals(funcDef.returnType())
                && resolvedParams.equals(funcDef.parameters())) {
            return funcDef;
        }

        return new FunctionDefinition(funcDef.qualifiedName(), resolvedParams,
                resolvedReturnType, funcDef.returnLowerBound(), funcDef.returnUpperBound(),
                funcDef.body(), funcDef.stereotypes(), funcDef.taggedValues());
    }

    private static FunctionDefinition.ParameterDefinition resolveFuncParam(
            FunctionDefinition.ParameterDefinition param, ImportScope imports, Set<String> knownFqns) {
        String resolvedType = imports.resolve(param.type(), knownFqns);
        if (resolvedType.equals(param.type())) return param;
        return new FunctionDefinition.ParameterDefinition(
                param.name(), resolvedType, param.lowerBound(), param.upperBound(), param.functionType());
    }

    // ==================== AssociationDefinition ====================

    private static AssociationDefinition resolveAssociation(
            AssociationDefinition assocDef, ImportScope imports, Set<String> knownFqns) {

        var end1 = assocDef.property1();
        var end2 = assocDef.property2();

        String resolved1 = imports.resolve(end1.targetClass(), knownFqns);
        String resolved2 = imports.resolve(end2.targetClass(), knownFqns);

        if (resolved1.equals(end1.targetClass()) && resolved2.equals(end2.targetClass())) {
            return assocDef;
        }

        return new AssociationDefinition(
                assocDef.qualifiedName(),
                new AssociationDefinition.AssociationEndDefinition(
                        end1.propertyName(), resolved1, end1.lowerBound(), end1.upperBound()),
                new AssociationDefinition.AssociationEndDefinition(
                        end2.propertyName(), resolved2, end2.lowerBound(), end2.upperBound()));
    }

    // ==================== MappingDefinition ====================

    private static MappingDefinition resolveMapping(
            MappingDefinition mappingDef, ImportScope imports, Set<String> knownFqns) {

        List<MappingDefinition.ClassMappingDefinition> resolvedMappings = mappingDef.classMappings().stream()
                .map(cm -> resolveClassMapping(cm, imports, knownFqns))
                .toList();

        List<AssociationMappingDefinition> resolvedAssocMappings =
                mappingDef.associationMappings().stream()
                        .map(am -> resolveAssociationMapping(am, imports, knownFqns))
                        .toList();

        if (resolvedMappings.equals(mappingDef.classMappings())
                && resolvedAssocMappings.equals(mappingDef.associationMappings())) {
            return mappingDef;
        }

        return new MappingDefinition(
                mappingDef.qualifiedName(),
                mappingDef.includes(),
                resolvedMappings,
                resolvedAssocMappings,
                mappingDef.enumerationMappings(),
                mappingDef.testSuites());
    }

    private static MappingDefinition.ClassMappingDefinition resolveClassMapping(
            MappingDefinition.ClassMappingDefinition cm, ImportScope imports, Set<String> knownFqns) {

        String resolvedClassName = imports.resolve(cm.className(), knownFqns);
        String resolvedSourceClassName = cm.sourceClassName() != null
                ? imports.resolve(cm.sourceClassName(), knownFqns) : null;

        if (resolvedClassName.equals(cm.className())
                && java.util.Objects.equals(resolvedSourceClassName, cm.sourceClassName())) {
            return cm;
        }

        return new MappingDefinition.ClassMappingDefinition(
                resolvedClassName, cm.mappingType(), cm.setId(), cm.isRoot(), cm.extendsSetId(),
                cm.mainTable(), cm.filter(), cm.distinct(), cm.groupBy(), cm.primaryKey(),
                cm.propertyMappings(), resolvedSourceClassName, cm.filterExpression(),
                cm.m2mPropertyExpressions());
    }

    private static AssociationMappingDefinition resolveAssociationMapping(
            AssociationMappingDefinition am, ImportScope imports, Set<String> knownFqns) {

        String resolvedAssocName = imports.resolve(am.associationName(), knownFqns);
        if (resolvedAssocName.equals(am.associationName())) return am;

        return new AssociationMappingDefinition(
                resolvedAssocName, am.mappingType(), am.properties());
    }

    // ==================== Query AST Resolution ====================

    /**
     * Resolves all class name references in a query AST to FQN.
     *
     * <p>Walks the AST tree and resolves:
     * <ul>
     *   <li>{@link PackageableElementPtr#fullPath()} — e.g., {@code Employee} in {@code Employee.all()}</li>
     *   <li>Desugared graphFetch root class — e.g., {@code Employee} in {@code #{Employee{...}}#}</li>
     *   <li>{@link GenericTypeInstance#fullPath()} — e.g., {@code Employee} in {@code @Employee}</li>
     * </ul>
     *
     * @param ast       The raw query AST from the parser
     * @param imports   The import scope from the model source
     * @param knownFqns All registered FQN element names
     * @return New AST with resolved names (original returned if nothing changed)
     */
    public static ValueSpecification resolveQuery(
            ValueSpecification ast, ImportScope imports, Set<String> knownFqns) {
        return resolveVs(ast, imports, knownFqns);
    }

    private static ValueSpecification resolveVs(
            ValueSpecification vs, ImportScope imports, Set<String> knownFqns) {
        return switch (vs) {
            case PackageableElementPtr ptr -> {
                String resolved = imports.resolve(ptr.fullPath(), knownFqns);
                yield resolved.equals(ptr.fullPath()) ? ptr : new PackageableElementPtr(resolved);
            }
            case GenericTypeInstance gti -> {
                String resolved = imports.resolve(gti.fullPath(), knownFqns);
                yield resolved.equals(gti.fullPath()) ? gti : new GenericTypeInstance(resolved, gti.resolvedType());
            }
            case AppliedFunction af -> {
                String resolvedFunc = imports.resolve(af.function(), knownFqns);
                List<ValueSpecification> resolvedParams = resolveList(af.parameters(), imports, knownFqns);
                yield (resolvedFunc.equals(af.function()) && resolvedParams == af.parameters()) ? af
                        : new AppliedFunction(resolvedFunc, resolvedParams, af.hasReceiver(),
                                af.sourceText(), af.argTexts());
            }
            case LambdaFunction lf -> {
                List<ValueSpecification> resolvedBody = resolveList(lf.body(), imports, knownFqns);
                yield resolvedBody == lf.body() ? lf : new LambdaFunction(lf.parameters(), resolvedBody);
            }
            case PureCollection coll -> {
                List<ValueSpecification> resolvedValues = resolveList(coll.values(), imports, knownFqns);
                yield resolvedValues == coll.values() ? coll : new PureCollection(resolvedValues);
            }
            case ClassInstance ci -> {
                Object resolvedValue = resolveClassInstanceValue(ci.value(), imports, knownFqns);
                yield resolvedValue == ci.value() ? ci : new ClassInstance(ci.type(), resolvedValue);
            }
            case AppliedProperty ap -> {
                List<ValueSpecification> resolvedParams = resolveList(ap.parameters(), imports, knownFqns);
                yield resolvedParams == ap.parameters() ? ap
                        : new AppliedProperty(ap.property(), resolvedParams);
            }
            case EnumValue ev -> {
                String resolved = imports.resolve(ev.fullPath(), knownFqns);
                yield resolved.equals(ev.fullPath()) ? ev : new EnumValue(resolved, ev.value());
            }
            // Leaf nodes — no names to resolve
            default -> vs;
        };
    }

    private static List<ValueSpecification> resolveList(
            List<ValueSpecification> list, ImportScope imports, Set<String> knownFqns) {
        boolean changed = false;
        List<ValueSpecification> result = new ArrayList<>(list.size());
        for (ValueSpecification vs : list) {
            ValueSpecification resolved = resolveVs(vs, imports, knownFqns);
            if (resolved != vs) changed = true;
            result.add(resolved);
        }
        return changed ? List.copyOf(result) : list;
    }

    private static Object resolveClassInstanceValue(
            Object value, ImportScope imports, Set<String> knownFqns) {
        // ClassInstance values (ColSpecArray, ColSpec, etc.) contain no class names to resolve.
        return value;
    }
}
