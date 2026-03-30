package com.gs.legend.compiler.checkers;

import com.gs.legend.antlr.ValueSpecificationBuilder;
import com.gs.legend.ast.*;
import com.gs.legend.compiler.*;
import com.gs.legend.model.mapping.RelationalMapping;
import com.gs.legend.model.m3.PureClass;
import com.gs.legend.plan.GenericType;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Checker for {@code new(PE(className), ClassInstance("instance", InstanceData))}.
 *
 * <p>Validates the class exists in model context, checks property names,
 * compiles property value expressions, builds identity mapping + associations,
 * and returns TypeInfo with ClassType.
 *
 * <p>Parser emits: {@code new(PackageableElementPtr(className), ClassInstance("instance", InstanceData))}
 */
public class NewChecker extends AbstractChecker {

    public NewChecker(TypeCheckEnv env) {
        super(env);
    }

    @Override
    public TypeInfo check(AppliedFunction af, TypeInfo source,
                          TypeChecker.CompilationContext ctx) {
        // Extract InstanceData from param[1]: new(PE(className), ClassInstance("instance", data))
        var ci = (ClassInstance) af.parameters().get(1);
        var data = (ValueSpecificationBuilder.InstanceData) ci.value();

        PureClass pureClass = resolveClass(data);

        // Validate properties and compile value expressions
        for (var entry : data.properties().entrySet()) {
            String propName = entry.getKey();
            var propOpt = pureClass.findProperty(propName);
            if (propOpt.isEmpty()) {
                throw new PureCompileException(
                        "Struct literal: property '" + propName + "' not found in class '"
                                + data.className() + "'");
            }
            // Compile the property value expression so variables etc. are in the side table
            env.compileExpr(entry.getValue(), ctx);
        }

        // Build identity mapping — scalar primitives get identity PropertyMappings
        var identityMapping = RelationalMapping.identity(pureClass);

        // Synthesize associations for to-many class-typed properties
        // These get AssociationTarget(null, null, true) — no Join signals UNNEST
        var associations = new LinkedHashMap<String, TypeInfo.AssociationTarget>();
        var modelCtx = env.modelContext();
        for (var prop : pureClass.allProperties()) {
            if (prop.isCollection() && prop.genericType() instanceof PureClass elementClass) {
                // Resolve FULL class from modelContext — property genericType may be a
                // forward-reference stub with no properties
                var resolvedClass = modelCtx != null
                        ? modelCtx.findClass(elementClass.qualifiedName()).orElse(elementClass)
                        : elementClass;
                // Build identity mapping for element class so compileProject can resolve leaf properties
                var targetMapping = RelationalMapping.identity(resolvedClass);
                associations.put(prop.name(), new TypeInfo.AssociationTarget(targetMapping, null, true));
            }
        }

        return TypeInfo.builder()
                .mapping(identityMapping)
                .associations(associations.isEmpty() ? Map.of() : Map.copyOf(associations))
                .expressionType(ExpressionType.one(new GenericType.ClassType(data.className())))
                .build();
    }

    private PureClass resolveClass(ValueSpecificationBuilder.InstanceData data) {
        // Built-in Pure standard library types (no model context needed)
        String simpleName = data.className().contains("::")
                ? data.className().substring(data.className().lastIndexOf("::") + 2)
                : data.className();

        if ("Pair".equals(simpleName) && data.typeArguments().size() == 2) {
            var firstType = GenericType.fromTypeName(data.typeArguments().get(0));
            var secondType = GenericType.fromTypeName(data.typeArguments().get(1));
            return new PureClass(
                    data.className().contains("::")
                            ? data.className().substring(0, data.className().lastIndexOf("::"))
                            : "",
                    "Pair", java.util.List.of(
                            new com.gs.legend.model.m3.Property("first",
                                    com.gs.legend.model.m3.PrimitiveType.fromName(firstType.typeName()),
                                    new com.gs.legend.model.m3.Multiplicity(1, 1)),
                            new com.gs.legend.model.m3.Property("second",
                                    com.gs.legend.model.m3.PrimitiveType.fromName(secondType.typeName()),
                                    new com.gs.legend.model.m3.Multiplicity(1, 1))));
        }

        // Fall back to model context for user-defined classes
        var modelCtx = env.modelContext();
        if (modelCtx != null) {
            var found = modelCtx.findClass(data.className()).orElse(null);
            if (found != null) return found;
        }

        throw new PureCompileException(
                "Struct literal: class '" + data.className() + "' not found in model context");
    }
}
