package com.gs.legend.compiler.checkers;

import com.gs.legend.ast.*;
import com.gs.legend.compiler.*;
import com.gs.legend.plan.GenericType;

import java.util.List;

/**
 * Checker for {@code match(input, [branches], extraParams...)} — static type dispatch.
 *
 * <p>Finds matching branch by type + multiplicity, compiles matched body with
 * params bound as let bindings, and stores as inlinedBody for PlanGenerator.
 */
public class MatchChecker extends AbstractChecker {

    public MatchChecker(TypeCheckEnv env) {
        super(env);
    }

    public TypeInfo check(AppliedFunction af, TypeInfo source,
                          TypeChecker.CompilationContext ctx) {
        List<ValueSpecification> params = af.parameters();

        // Compile all params first (failures swallowed — branch matching is lenient)
        for (var p : params) {
            try {
                env.compileExpr(p, ctx);
            } catch (PureCompileException ignored) {
            }
        }
        if (params.size() < 2)
            throw new PureCompileException("match requires at least 2 parameters: value, branches");

        // Determine input type name
        String inputTypeName = inferTypeName(params.get(0));
        if (inputTypeName == null)
            throw new PureCompileException("match: cannot infer input type");

        // Extract branches from Collection (params[1])
        List<LambdaFunction> branches;
        if (params.get(1) instanceof PureCollection(List<ValueSpecification> values)) {
            branches = values.stream()
                    .filter(v -> v instanceof LambdaFunction)
                    .map(v -> (LambdaFunction) v)
                    .toList();
        } else if (params.get(1) instanceof LambdaFunction lf) {
            branches = List.of(lf);
        } else {
            throw new PureCompileException("match: second parameter must be a lambda or collection of lambdas");
        }

        // Determine if input is a collection (affects multiplicity matching)
        boolean inputIsMany = (params.get(0) instanceof PureCollection(List<ValueSpecification> values)
                && values.size() != 1)
                || (env.lookupCompiled(params.get(0)) != null && env.lookupCompiled(params.get(0)).isMany());

        // Find matching branch by type + multiplicity
        for (var branch : branches) {
            if (branch.parameters().isEmpty())
                continue;
            Variable branchParam = branch.parameters().get(0);
            if (branchParam.typeName() == null)
                continue;
            String branchType = TypeInfo.simpleName(branchParam.typeName());
            if (branchType.equals(inputTypeName)
                    || branchType.equals("Any")
                    || inputTypeName.equals("Any")) {
                // Check multiplicity compatibility
                String mult = branchParam.multiplicity();
                boolean branchAcceptsMany = mult == null || mult.equals("*")
                        || mult.equals("0") || mult.contains("..");
                if (inputIsMany && !branchAcceptsMany)
                    continue;
                // Match found — compile the body with params bound as let bindings
                TypeChecker.CompilationContext matchCtx = ctx;
                matchCtx = matchCtx.withLetBinding(branchParam.name(), params.get(0));
                // If there's an extra param (params[2]), bind it similarly
                if (branch.parameters().size() > 1 && params.size() > 2) {
                    Variable extraParam = branch.parameters().get(1);
                    matchCtx = matchCtx.withLetBinding(extraParam.name(), params.get(2));
                }
                if (!branch.body().isEmpty()) {
                    ValueSpecification body = branch.body().get(0);
                    TypeInfo bodyInfo = env.compileExpr(body, matchCtx);
                    return TypeInfo.from(bodyInfo).inlinedBody(body).build();
                }
            }
        }
        throw new PureCompileException("Unresolved type for function: " + TypeInfo.simpleName(af.function()));
    }

    /** Infers the simple type name from an AST node. */
    private String inferTypeName(ValueSpecification vs) {
        if (vs instanceof CString)       return "String";
        if (vs instanceof CInteger)      return "Integer";
        if (vs instanceof CFloat)        return "Float";
        if (vs instanceof CDecimal)      return "Decimal";
        if (vs instanceof CBoolean)      return "Boolean";
        if (vs instanceof CStrictDate)   return "StrictDate";
        if (vs instanceof CDateTime)     return "DateTime";
        if (vs instanceof PureCollection(List<ValueSpecification> values)) {
            if (!values.isEmpty()) {
                return inferTypeName(values.get(0));
            }
        }
        // Check side table
        TypeInfo info = env.lookupCompiled(vs);
        if (info != null && info.type() != null) {
            GenericType st = info.type();
            if (st.isList() && st.elementType() != null)
                st = st.elementType();
            if (st == GenericType.Primitive.STRING)   return "String";
            if (st == GenericType.Primitive.INTEGER)  return "Integer";
            if (st == GenericType.Primitive.FLOAT)    return "Float";
            if (st == GenericType.Primitive.BOOLEAN)  return "Boolean";
        }
        return null;
    }
}
