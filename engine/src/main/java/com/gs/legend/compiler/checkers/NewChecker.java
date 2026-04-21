package com.gs.legend.compiler.checkers;

import com.gs.legend.ast.*;
import com.gs.legend.compiler.*;
import com.gs.legend.compiler.typed.TypedCollection;
import com.gs.legend.compiler.typed.TypedNewInstance;
import com.gs.legend.compiler.typed.TypedSpec;
import com.gs.legend.model.m3.PureClass;
import com.gs.legend.model.m3.Type;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;


/**
 * Checker for {@code new(PE(className), NewInstance)}.
 *
 * <p>Validates the class exists in model context, checks property names,
 * compiles property value expressions, and returns TypeInfo with ClassType.
 * Stamps {@code instanceLiteral=true} so MappingResolver can create identity mappings.
 *
 * <p>Parser emits: {@code new(PackageableElementPtr(className), NewInstance(...))}
 */
public class NewChecker extends AbstractChecker {

    public NewChecker(TypeCheckEnv env) {
        super(env);
    }

    @Override
    public TypedNewInstance check(AppliedFunction af, TypedSpec source,
                          TypeChecker.CompilationContext ctx) {
        // Parser shape: new(PackageableElementPtr(className), NewInstance(props...)).
        var data = (NewInstance) af.parameters().get(1);
        PureClass pureClass = resolveClass(data);

        // Validate each property name against the class and compile its value
        // expression to a typed child. Preserves insertion order (LinkedHashMap).
        // To-many fixup: if the property is declared [*] but the user wrote a
        // single-value literal (not a PureCollection), wrap the typed value in
        // a singleton TypedCollection with many-multiplicity so downstream
        // consumers see the intended list shape.
        Map<String, TypedSpec> values = new LinkedHashMap<>();
        for (var entry : data.properties().entrySet()) {
            String propName = entry.getKey();
            var propOpt = pureClass.findProperty(propName, env.modelContext());
            if (propOpt.isEmpty()) {
                throw new PureCompileException(
                        "Struct literal: property '" + propName + "' not found in class '"
                                + data.className() + "'");
            }
            TypedSpec compiled = env.compileExpr(entry.getValue(), ctx);
            if (propOpt.get().isCollection()
                    && !(entry.getValue() instanceof PureCollection)) {
                Type propType = propOpt.get().type();
                compiled = new TypedCollection(List.of(compiled), ExpressionType.many(propType));
            }
            values.put(propName, compiled);
        }

        String resolvedFqn = findClass(data.className())
                .map(c -> c.qualifiedName()).orElse(data.className());
        return new TypedNewInstance(resolvedFqn, values,
                ExpressionType.one(new Type.ClassType(resolvedFqn)));
    }

    private PureClass resolveClass(NewInstance data) {
        // Phase 2.5e: platform classes (Pair, ...) are seeded into every PureModelBuilder
        // via BuiltinClassRegistry and auto-imported via BUILTIN_IMPORTS — so a simple-name
        // lookup hits them just like a user-declared class. No hand-rolled synthesis
        // required. The returned PureClass carries TypeVar property types (first: U, second: V
        // for Pair), which is fine because the caller only uses findProperty(name) for
        // name-based validation; monomorphization of the type args is handled elsewhere
        // in the type-checker pipeline.
        var modelCtx = env.modelContext();
        if (modelCtx != null) {
            var found = modelCtx.findClass(data.className()).orElse(null);
            if (found != null) return found;
        }

        throw new PureCompileException(
                "Struct literal: class '" + data.className() + "' not found in model context");
    }
}
