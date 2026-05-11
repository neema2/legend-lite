package com.legend.parser.element;

import java.util.List;
import java.util.Objects;

/**
 * A parsed Pure top-level function declaration.
 *
 * <p>Pure syntax:
 * <pre>
 *   function my::pkg::name(p1: T1[m1], p2: T2[m2]): R[m] { body }
 * </pre>
 *
 * <h2>Deliberate divergence from engine's {@code FunctionDefinition}</h2>
 *
 * <p>Engine's record carries four <em>compiler-cache</em> fields populated
 * during {@code PureModelBuilder} (Phase F): {@code List<ValueSpecification>
 * resolvedBody}, {@code Type parsedReturnType}, and on each
 * {@code ParameterDefinition}: {@code Type.FunctionType functionType},
 * {@code Type parsedType}. The {@code withResolvedBody(...)} helper mutates
 * the parser-output record with the compiled body AST.
 *
 * <p>That bridges parser-output and compiler-output through a single record,
 * which forces Phase F to parse expression bodies eagerly to populate the
 * cache &mdash; the textbook
 * <strong>F-must-not-trigger-G violation</strong>
 * (see AGENTS.md invariant 1).
 *
 * <p>{@code core/} keeps the parser record <strong>pure parser data</strong>.
 * The compile-side output of a function will be a separate
 * {@code compiler.element.TypedFunction} record built in Phase F. Function
 * body parsing is deferred to Phase G (see decision D-1 in core's README).
 *
 * @param qualifiedName    fully qualified function name (e.g. {@code "my::utils::greet"})
 * @param parameters       parameter declarations in source order
 * @param returnType       return type as written (simple or qualified, unresolved)
 * @param returnLowerBound lower multiplicity bound
 * @param returnUpperBound upper multiplicity bound ({@code null} = unbounded)
 * @param body             function body as raw source text (parsed lazily in Phase G)
 * @param stereotypes      applied stereotypes
 * @param taggedValues     applied tagged values
 */
public record FunctionDefinition(
        String qualifiedName,
        List<ParameterDefinition> parameters,
        String returnType,
        int returnLowerBound,
        Integer returnUpperBound,
        String body,
        List<StereotypeApplication> stereotypes,
        List<TaggedValue> taggedValues) implements PackageableElement {

    public FunctionDefinition {
        Objects.requireNonNull(qualifiedName, "Qualified name cannot be null");
        Objects.requireNonNull(returnType, "Return type cannot be null");
        Objects.requireNonNull(body, "Function body cannot be null");
        parameters = parameters == null ? List.of() : List.copyOf(parameters);
        stereotypes = stereotypes == null ? List.of() : List.copyOf(stereotypes);
        taggedValues = taggedValues == null ? List.of() : List.copyOf(taggedValues);
    }

    /**
     * A function parameter declaration.
     *
     * <p>Engine's {@code FunctionDefinition.ParameterDefinition} carries two
     * compiler-cache fields ({@code Type.FunctionType functionType},
     * {@code Type parsedType}) used by engine's type checker. Core/'s parser
     * record stays pure data; type structure is rebuilt by {@code SpecCompiler}
     * in Phase G from {@code type} (the raw type string).
     */
    public record ParameterDefinition(
            String name,
            String type,
            int lowerBound,
            Integer upperBound) {
        public ParameterDefinition {
            Objects.requireNonNull(name, "Parameter name cannot be null");
            Objects.requireNonNull(type, "Parameter type cannot be null");
        }
    }
}
