package com.legend.compiler.element;

import com.legend.compiler.element.type.Multiplicity;
import com.legend.compiler.element.type.Type;
import com.legend.protocol.spec.ValueSpecification;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * A compiled function (Phase F) &mdash; the typed counterpart of
 * {@link com.legend.model.Function} (both
 * {@link com.legend.model.FunctionDefinition} and
 * {@link com.legend.model.NativeFunctionDefinition}). It is the
 * <strong>single body substrate</strong>: every Pure body site (derived
 * properties, constraints, service queries, mapping transforms) is externalized
 * (Phase E) into one of these, so no other {@code Typed*} record carries a body
 * (doc §1.1, §1.2).
 *
 * <p>Carries the type-checked <em>signature</em> (own {@link Type} /
 * {@link Multiplicity}, FQN-only) plus the <strong>un-type-checked parsed
 * body</strong> &mdash; the input to Phase G, never type-checked at F
 * (invariant 7). There is <strong>no</strong> compiled-body field, ever; G's
 * output lives in a separate side table, leaving this record identical whether
 * or not G has run.
 *
 * <p><b>Body optionality is origin-determined.</b> {@code body} is
 * <em>present</em> for functions compiled from source, and <em>absent</em> for
 * natives (signature-only) and for functions loaded from a {@code .legend}
 * dependency (you hold the dependency's compiled body separately, never its
 * AST). The body is a {@code List} of statements (a Pure body is a statement
 * sequence); {@link Optional} distinguishes "no body" (native/dependency) from a
 * present-but-empty {@code {}} body.
 *
 * <p>Overloads are <strong>not</strong> disambiguated here: several
 * {@code TypedFunction}s may share one FQN and are returned together as the
 * overload list by {@code ModelContext.findFunction(fqn)}; the call site
 * disambiguates by arity + argument types.
 *
 * @param qualifiedName          fully qualified function name (shared across overloads)
 * @param typeParameters         generic type parameter names
 * @param multiplicityParameters multiplicity parameter names
 * @param parameters             classified parameters in source order
 * @param returnType             classified return type
 * @param returnMultiplicity     classified return multiplicity
 * @param body                   parsed, un-type-checked body statements; empty for
 *                               natives and {@code .legend} dependencies
 * @param isNative               {@code true} for {@code native function} declarations
 */
public record TypedFunction(
        String qualifiedName,
        List<String> typeParameters,
        List<String> multiplicityParameters,
        List<TypedParameter> parameters,
        Type returnType,
        Multiplicity returnMultiplicity,
        Optional<List<ValueSpecification>> body,
        boolean isNative,
        com.legend.model.@com.legend.base.Nullable Function definition,
        com.legend.model.@com.legend.base.Nullable FunctionId id) implements TypedElement {

    /** The declaration's identity — upstream's own signature id, held whole
     *  ({@link com.legend.model.FunctionId}): what every rule table is keyed
     *  by and every callee compare uses (execution plan step 2, 2026-09-26). */
    public com.legend.model.FunctionId id() {
        if (id == null) {
            throw new IllegalStateException("TypedFunction '" + qualifiedName
                    + "' has no source definition (test-convenience ctor) —"
                    + " it cannot be dispatched by the lowering");
        }
        return id;
    }

    /**
     * Test convenience: a signature with no source definition. Real
     * compilation always threads the parser definition &mdash; it is the
     * lowering's dispatch identity (a resolved native call dispatches on
     * WHICH overload Phase G chose, never on the function's name).
     */
    public TypedFunction(String qualifiedName, List<String> typeParameters,
                         List<String> multiplicityParameters, List<TypedParameter> parameters,
                         Type returnType, Multiplicity returnMultiplicity,
                         Optional<List<ValueSpecification>> body, boolean isNative) {
        this(qualifiedName, typeParameters, multiplicityParameters, parameters,
                returnType, returnMultiplicity, body, isNative, null, null);
    }

    public TypedFunction(String qualifiedName,
                         @com.legend.base.Nullable List<String> typeParameters,
                         @com.legend.base.Nullable List<String> multiplicityParameters,
                         @com.legend.base.Nullable List<TypedParameter> parameters,
                         Type returnType, Multiplicity returnMultiplicity,
                         @com.legend.base.Nullable Optional<List<ValueSpecification>> body,
                         boolean isNative,
                         com.legend.model.@com.legend.base.Nullable Function definition) {
        this(qualifiedName, typeParameters, multiplicityParameters, parameters, returnType, returnMultiplicity,
                body, isNative, definition, definition == null ? null : com.legend.model.FunctionId.of(definition));
    }

    /** The canonical form: the identity is computed ONCE, when the function is
     *  compiled, and rides every callee compare and rule lookup after. */
    public TypedFunction(String qualifiedName,
                         @com.legend.base.Nullable List<String> typeParameters,
                         @com.legend.base.Nullable List<String> multiplicityParameters,
                         @com.legend.base.Nullable List<TypedParameter> parameters,
                         Type returnType, Multiplicity returnMultiplicity,
                         @com.legend.base.Nullable Optional<List<ValueSpecification>> body,
                         boolean isNative,
                         com.legend.model.@com.legend.base.Nullable Function definition,
                         com.legend.model.@com.legend.base.Nullable FunctionId id) {
        Objects.requireNonNull(qualifiedName, "qualifiedName");
        Objects.requireNonNull(returnType, "returnType");
        Objects.requireNonNull(returnMultiplicity, "returnMultiplicity");
        this.qualifiedName = qualifiedName;
        this.typeParameters = typeParameters == null
                ? List.of() : List.copyOf(typeParameters);
        this.multiplicityParameters = multiplicityParameters == null
                ? List.of() : List.copyOf(multiplicityParameters);
        this.parameters = parameters == null
                ? List.of() : List.copyOf(parameters);
        this.returnType = returnType;
        this.returnMultiplicity = returnMultiplicity;
        this.body = body == null ? Optional.empty() : body.map(List::copyOf);
        this.isNative = isNative;
        this.definition = definition;
        this.id = id;
    }
}
