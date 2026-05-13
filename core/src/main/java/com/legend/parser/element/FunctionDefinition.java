package com.legend.parser.element;

import com.legend.parser.spec.ValueSpecification;

import java.util.List;
import java.util.Objects;

/**
 * A parsed Pure top-level <em>concrete</em> function declaration.
 *
 * <p>Pure syntax:
 * <pre>
 *   function my::pkg::name&lt;T,V|m,n&gt;(p1: T1[m1], p2: T2[m2]): R[m] { stmt; stmt; expr }
 * </pre>
 *
 * <p>Concrete functions have a body; native functions ({@link NativeFunctionDefinition})
 * do not. Both share signature shape (type/multiplicity parameters, params,
 * return type/multiplicity, stereotypes, tagged values), captured by the
 * sealed {@link Function} marker which both implement.
 *
 * <h2>Deliberate divergence from engine's {@code FunctionDefinition}</h2>
 *
 * <p>Engine's record carries four <em>compiler-cache</em> fields populated
 * during {@code PureModelBuilder} (Phase F): {@code List<ValueSpecification>
 * resolvedBody}, {@code Type parsedReturnType}, and on each
 * {@code ParameterDefinition}: {@code Type.FunctionType functionType},
 * {@code Type parsedType}. The {@code withResolvedBody(...)} helper mutates
 * the parser-output record by stuffing the compiled body AST back in,
 * tangling parser-output with compiler-output and forcing Phase F to
 * force-parse every function body unconditionally. That is the textbook
 * <strong>F-must-not-trigger-G violation</strong> AGENTS.md invariant 1
 * forbids.
 *
 * <p>{@code core/} resolves this by parsing function bodies <strong>at
 * parse time</strong>, eagerly, inside {@code ElementParser}. The result
 * is a {@code List<ValueSpecification>} (a sequence of statements), held
 * directly on this record. There is no separate {@code resolvedBody}
 * field, no {@code withResolvedBody(...)} helper, no compiler-stage
 * re-parsing pass; downstream layers read {@link #body()} and just get
 * the AST. Element-level laziness (parse only the elements a query
 * actually touches) is provided by the IDE-layer orchestrator in
 * {@code com.legend.ide}, not the batch compiler.
 *
 * @param qualifiedName           fully qualified function name (e.g. {@code "my::utils::greet"})
 * @param typeParameters          declared generic type parameter names (e.g. {@code <T,V>})
 * @param multiplicityParameters  declared multiplicity parameter names (e.g. {@code <|m,n>})
 * @param parameters              parameter declarations in source order
 * @param returnType              declared return type as a structured AST
 * @param returnMultiplicity      return multiplicity ({@link Multiplicity.Concrete} or {@link Multiplicity.Parameter})
 * @param body                    parsed function body as a sequence of statements;
 *                                non-null but may be empty for a {@code {}} body
 * @param stereotypes             applied stereotypes
 * @param taggedValues            applied tagged values
 */
public record FunctionDefinition(
        String qualifiedName,
        List<String> typeParameters,
        List<String> multiplicityParameters,
        List<ParameterDefinition> parameters,
        TypeExpression returnType,
        Multiplicity returnMultiplicity,
        List<ValueSpecification> body,
        List<StereotypeApplication> stereotypes,
        List<TaggedValue> taggedValues) implements PackageableElement, Function {

    public FunctionDefinition {
        Objects.requireNonNull(qualifiedName, "Qualified name cannot be null");
        Objects.requireNonNull(returnType, "Return type cannot be null");
        Objects.requireNonNull(returnMultiplicity, "Return multiplicity cannot be null");
        Objects.requireNonNull(body, "Function body cannot be null");
        typeParameters = typeParameters == null ? List.of() : List.copyOf(typeParameters);
        multiplicityParameters = multiplicityParameters == null ? List.of() : List.copyOf(multiplicityParameters);
        parameters = parameters == null ? List.of() : List.copyOf(parameters);
        body = List.copyOf(body);
        stereotypes = stereotypes == null ? List.of() : List.copyOf(stereotypes);
        taggedValues = taggedValues == null ? List.of() : List.copyOf(taggedValues);
    }

    /**
     * A function parameter declaration. Parameter type is a structured
     * {@link TypeExpression}; pre-NameResolver leaves may carry simple
     * unresolved names, post-resolution every leaf is FQN.
     */
    public record ParameterDefinition(
            String name,
            TypeExpression type,
            Multiplicity multiplicity) {
        public ParameterDefinition {
            Objects.requireNonNull(name, "Parameter name cannot be null");
            Objects.requireNonNull(type, "Parameter type cannot be null");
            Objects.requireNonNull(multiplicity, "Parameter multiplicity cannot be null");
        }
    }
}
