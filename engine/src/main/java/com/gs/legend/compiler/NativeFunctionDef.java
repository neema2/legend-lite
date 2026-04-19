package com.gs.legend.compiler;

import com.gs.legend.model.m3.Multiplicity;

import java.util.List;

/**
 * Structured representation of a native function definition parsed from
 * a Pure signature string.
 *
 * <p>
 * Produced by parsing strings like:
 * <pre>
 * native function filter&lt;T&gt;(rel:Relation&lt;T&gt;[1], f:Function&lt;{T[1]-&gt;Boolean[1]}&gt;[1]):Relation&lt;T&gt;[1];
 * </pre>
 *
 * @param name         Simple function name (e.g., "filter", "abs", "toLower")
 * @param typeParams   Type parameter names (e.g., ["T"] or ["T","V","K","R"])
 * @param multParams   Multiplicity parameter names (e.g., ["m"] from sort&lt;T|m&gt;)
 * @param params       Typed parameters with multiplicities
 * @param returnType   Return type
 * @param returnMult   Return multiplicity
 * @param rawSignature The original Pure signature string (for LSP hover, debugging)
 */
public record NativeFunctionDef(
        String name,
        List<String> typeParams,
        List<String> multParams,
        List<PType.Param> params,
        PType returnType,
        Multiplicity returnMult,
        String rawSignature
) {

    /**
     * Number of declared parameters.
     */
    public int arity() {
        return params.size();
    }

    /**
     * Whether this def matches a call-site with the given number of arguments.
     * Exact match for fixed params; if the last param has [*] multiplicity,
     * any arity >= (paramCount - 1) matches (the [*] param absorbs 0..N individual args).
     */
    public boolean matchesArity(int callArity) {
        if (params.size() == callArity) return true;
        if (!params.isEmpty()) {
            var lastParam = params.get(params.size() - 1);
            if (lastParam.mult() == Multiplicity.MANY || lastParam.mult() == Multiplicity.ONE_MANY) {
                return callArity >= params.size() - 1;
            }
        }
        return false;
    }

    /**
     * Whether this function uses type variables (generic).
     */
    public boolean isGeneric() {
        return !typeParams.isEmpty();
    }

    /**
     * Whether this function uses multiplicity variables.
     */
    public boolean hasMultiplicityVars() {
        return !multParams.isEmpty();
    }

    @Override
    public String toString() {
        return rawSignature;
    }
}
