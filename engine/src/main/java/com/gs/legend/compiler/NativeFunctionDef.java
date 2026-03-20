package com.gs.legend.compiler;

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
        Mult returnMult,
        String rawSignature
) {

    /**
     * Number of required parameters.
     * Used for fast arity checks before full overload resolution.
     */
    public int arity() {
        return params.size();
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
