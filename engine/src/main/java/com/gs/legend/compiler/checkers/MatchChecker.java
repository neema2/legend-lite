package com.gs.legend.compiler.checkers;

import com.gs.legend.ast.*;
import com.gs.legend.compiler.*;
import com.gs.legend.model.m3.Multiplicity;
import com.gs.legend.model.m3.Type;

import java.util.List;

/**
 * Checker for {@code match(input, [branches], extraParams...)} — static type dispatch.
 *
 * <p>Finds the first matching branch by type + multiplicity, compiles the matched body
 * with params bound as let bindings, and stores as {@code inlinedBody} for PlanGenerator.
 *
 * <p>Return type comes from the compiled branch body, not the signature ({@code Any[*]}).
 */
public class MatchChecker extends AbstractChecker {

    public MatchChecker(TypeCheckEnv env) {
        super(env);
    }

    public TypeInfo check(AppliedFunction af, TypeInfo source,
                          TypeChecker.CompilationContext ctx) {
        List<ValueSpecification> params = af.parameters();
        if (params.size() < 2)
            throw new PureCompileException("match requires at least 2 parameters: value, branches");

        // 1. Compile input value — let errors propagate (no silent swallowing)
        TypeInfo inputInfo = env.compileExpr(params.get(0), ctx);

        // 2. Get input type from compiled result
        Type inputType = resolveElementType(inputInfo);
        if (inputType == null)
            throw new PureCompileException("match: cannot infer input type");

        // 3. Extract branches from Collection or single lambda (params[1])
        List<LambdaFunction> branches = extractBranches(params.get(1));

        // 4. Determine if input is a collection (affects multiplicity matching)
        boolean inputIsMany = inputInfo.isMany();

        // 5. Find first matching branch by type + multiplicity, compile its body
        for (var branch : branches) {
            if (branch.parameters().isEmpty()) continue;
            Variable branchParam = branch.parameters().get(0);
            if (branchParam.typeName() == null) continue;

            if (!typeMatches(branchParam.typeName(), inputType)) continue;

            // Check multiplicity: if input is many, branch must accept many.
            // branchParam.multiplicity() is already structured (parser produced it) — no re-parse.
            if (inputIsMany) {
                Multiplicity branchMult = branchParam.multiplicity();
                if (branchMult != null && !branchMult.isMany() && !branchMult.equals(Multiplicity.ZERO_ONE)
                        && !branchMult.equals(new Multiplicity.Bounded(0, 0))) {
                    continue;
                }
            }

            // Match found — compile body with branch params bound as let bindings
            TypeChecker.CompilationContext matchCtx = ctx
                    .withLetBinding(branchParam.name(), params.get(0));

            // Bind extra param (params[2]) if branch expects it
            if (branch.parameters().size() > 1 && params.size() > 2) {
                matchCtx = matchCtx.withLetBinding(
                        branch.parameters().get(1).name(), params.get(2));
            }

            if (!branch.body().isEmpty()) {
                ValueSpecification body = branch.body().get(0);
                TypeInfo bodyInfo = env.compileExpr(body, matchCtx);
                return TypeInfo.from(bodyInfo).inlinedBody(body).build();
            }
        }

        throw new PureCompileException(
                "match: no branch matches input type '" + inputType.typeName() + "'");
    }

    // ==================== Helpers ====================

    /**
     * Returns the element type from a compiled TypeInfo.
     */
    private static Type resolveElementType(TypeInfo info) {
        if (info == null || info.type() == null) return null;
        return info.type();
    }

    /**
     * Type-matches using the {@link Type} hierarchy: exact match, subtype in the primitive
     * lattice (Integer ⊆ Number), or ANY wildcard.
     */
    private boolean typeMatches(String branchTypeName, Type inputType) {
        Type branchType = Type.resolve(branchTypeName, env.modelContext());
        if (branchType instanceof Type.Primitive branchPrim) {
            if (branchPrim == Type.Primitive.ANY) return true;
            if (inputType instanceof Type.Primitive inputPrim) {
                return inputPrim == branchPrim || inputPrim.isSubtypeOf(branchPrim);
            }
            return false;
        }
        // Non-primitive branch (class, enum) — nominal name equality.
        return branchType.typeName().equals(inputType.typeName());
    }

    /** Extracts branch lambdas from a PureCollection or a single LambdaFunction. */
    private static List<LambdaFunction> extractBranches(ValueSpecification branchParam) {
        if (branchParam instanceof PureCollection(List<ValueSpecification> values)) {
            return values.stream()
                    .filter(v -> v instanceof LambdaFunction)
                    .map(v -> (LambdaFunction) v)
                    .toList();
        }
        if (branchParam instanceof LambdaFunction lf) {
            return List.of(lf);
        }
        throw new PureCompileException("match: second parameter must be a lambda or collection of lambdas");
    }
}
