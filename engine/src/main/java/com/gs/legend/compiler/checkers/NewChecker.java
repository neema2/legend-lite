package com.gs.legend.compiler.checkers;

import com.gs.legend.antlr.ValueSpecificationBuilder;
import com.gs.legend.ast.*;
import com.gs.legend.compiler.*;
import com.gs.legend.model.SymbolTable;
import com.gs.legend.model.m3.PureClass;
import com.gs.legend.plan.GenericType;



/**
 * Checker for {@code new(PE(className), ClassInstance("instance", InstanceData))}.
 *
 * <p>Validates the class exists in model context, checks property names,
 * compiles property value expressions, and returns TypeInfo with ClassType.
 * Stamps {@code instanceLiteral=true} so MappingResolver can create identity mappings.
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
            var propOpt = pureClass.findProperty(propName, env.modelContext());
            if (propOpt.isEmpty()) {
                throw new PureCompileException(
                        "Struct literal: property '" + propName + "' not found in class '"
                                + data.className() + "'");
            }
            // Compile the property value expression so variables etc. are in the side table
            env.compileExpr(entry.getValue(), ctx);
        }

        return TypeInfo.builder()
                .instanceLiteral(true)
                .expressionType(ExpressionType.one(new GenericType.ClassType(
                        findClass(data.className()).map(c -> c.qualifiedName()).orElse(data.className()))))
                .build();
    }

    private PureClass resolveClass(ValueSpecificationBuilder.InstanceData data) {
        // Built-in Pure standard library types (no model context needed)
        String simpleName = SymbolTable.extractSimpleName(data.className());

        if ("Pair".equals(simpleName) && data.typeArguments().size() == 2) {
            var firstType = GenericType.fromTypeName(data.typeArguments().get(0));
            var secondType = GenericType.fromTypeName(data.typeArguments().get(1));
            return new PureClass(
                    SymbolTable.extractPackagePath(data.className()),
                    "Pair", java.util.List.of(
                            new com.gs.legend.model.m3.Property("first",
                                    new com.gs.legend.model.m3.TypeRef.PrimitiveRef(firstType.typeName()),
                                    new com.gs.legend.model.m3.Multiplicity(1, 1)),
                            new com.gs.legend.model.m3.Property("second",
                                    new com.gs.legend.model.m3.TypeRef.PrimitiveRef(secondType.typeName()),
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
