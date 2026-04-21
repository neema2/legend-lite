package com.gs.legend.compiler.checkers;

import com.gs.legend.ast.*;
import com.gs.legend.compiler.*;
import com.gs.legend.compiler.typed.TypedSpec;
import com.gs.legend.model.m3.Multiplicity;
import com.gs.legend.model.m3.Primitive;
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

    public TypedSpec check(AppliedFunction af, TypedSpec source,
                          TypeChecker.CompilationContext ctx) {
        List<ValueSpecification> params = af.parameters();
        if (params.size() < 2) {
            throw new PureCompileException(
                    "match requires at least 2 parameters: value, branches");
        }

        // Compile input value — errors propagate (no silent swallowing).
        TypedSpec inputTyped = env.compileExpr(params.get(0), ctx);
        Type inputType = inputTyped.type();
        if (inputType == null) {
            throw new PureCompileException("match: cannot infer input type");
        }
        boolean inputIsMany = inputTyped.isMany();

        // Extract branches from a PureCollection or a single lambda (params[1]).
        List<LambdaFunction> branches = extractBranches(params.get(1));

        // Static dispatch: pick the first branch whose param type + multiplicity
        // matches the input, then return its compiled body directly. {@code match}
        // is resolved at compile time here, so the typed HIR is the body itself —
        // there is no residual runtime dispatch to preserve.
        for (var branch : branches) {
            if (branch.parameters().isEmpty()) continue;
            Variable branchParam = branch.parameters().get(0);
            if (branchParam.typeName() == null) continue;
            if (!typeMatches(branchParam.typeName(), inputType)) continue;

            if (inputIsMany) {
                Multiplicity branchMult = branchParam.multiplicity();
                if (branchMult != null && !branchMult.isMany()
                        && !branchMult.equals(Multiplicity.ZERO_ONE)
                        && !branchMult.equals(new Multiplicity.Bounded(0, 0))) {
                    continue;
                }
            }

            // Bind the branch's params as let bindings so references to them
            // resolve to the input (and optional extra arg) when compiling the body.
            TypeChecker.CompilationContext matchCtx = ctx
                    .withLetBinding(branchParam.name(), params.get(0));
            if (branch.parameters().size() > 1 && params.size() > 2) {
                matchCtx = matchCtx.withLetBinding(
                        branch.parameters().get(1).name(), params.get(2));
            }

            if (!branch.body().isEmpty()) {
                return env.compileExpr(branch.body().get(0), matchCtx);
            }
        }

        throw new PureCompileException(
                "match: no branch matches input type '" + inputType.typeName() + "'");
    }

    // ==================== Helpers ====================

    /**
     * Type-matches using the {@link Type} hierarchy: exact match, subtype in the primitive
     * lattice (Integer ⊆ Number), class subclass chain (Employee ⊆ Person), or ANY wildcard.
     */
    private boolean typeMatches(String branchTypeName, Type inputType) {
        Type branchType = Type.resolve(branchTypeName, env.modelContext());
        // Primitive branch — polymorphic isSubtypeOf covers ANY-acceptance, primitive lattice,
        // and rejection of non-primitive inputs uniformly.
        if (branchType instanceof Primitive branchPrim) {
            return inputType.isSubtypeOf(branchPrim);
        }
        // Class branch — walk the superclass chain via ModelContext so Employee matches a
        // Person branch (and cross-project lazy-loaded classes resolve correctly).
        if (branchType instanceof Type.ClassType branchCt && inputType instanceof Type.ClassType inputCt) {
            return env.modelContext().isClassSubtype(inputCt.qualifiedName(), branchCt.qualifiedName());
        }
        // Enum (or other nominal type) — nominal equality; enums have no hierarchy.
        return branchType.equals(inputType);
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
