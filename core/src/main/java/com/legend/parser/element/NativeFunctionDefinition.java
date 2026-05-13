package com.legend.parser.element;

import com.legend.parser.TypeExpression;

import com.legend.parser.Multiplicity;

import java.util.List;
import java.util.Objects;

/**
 * A parsed Pure top-level <em>native</em> function declaration &mdash; signature only,
 * no body.
 *
 * <p>Pure syntax:
 * <pre>
 *   native function &lt;&lt;stereo&gt;&gt; {tag=val}
 *       my::pkg::name&lt;T,V|m,n&gt;(p1: T1[m1], p2: T2[m2]): R[m];
 * </pre>
 *
 * <p>Mirrors Pure's M3 grammar:
 * <pre>
 *   nativeFunction: NATIVE FUNCTION stereotypes? taggedValues? qualifiedName
 *                   typeAndMultiplicityParameters? functionTypeSignature END_LINE
 * </pre>
 * which differs from {@code functionDefinition} only by the {@code NATIVE}
 * keyword and the trailing {@code ;} instead of a body block. Both rules share
 * the same {@code functionTypeSignature} non-terminal, so this record exposes
 * the same signature-shape fields as {@link FunctionDefinition} and the two
 * share the sealed {@link Function} marker.
 *
 * <p>Native functions back built-in primitives, platform functions, and the
 * relation algebra DSL (filter, project, extend, etc.). Their typing rules
 * are driven by the declared signature plus per-function checkers in the
 * compiler (`compiler/checkers/`); this record only captures the parser-level
 * declaration.
 *
 * @param qualifiedName           fully qualified function name
 * @param typeParameters          declared generic type parameter names
 * @param multiplicityParameters  declared multiplicity parameter names
 * @param parameters              parameter declarations in source order
 * @param returnType              declared return type as a structured AST
 * @param returnMultiplicity      return multiplicity ({@link Multiplicity.Concrete} or {@link Multiplicity.Parameter})
 * @param stereotypes             applied stereotypes
 * @param taggedValues            applied tagged values
 */
public record NativeFunctionDefinition(
        String qualifiedName,
        List<String> typeParameters,
        List<String> multiplicityParameters,
        List<FunctionDefinition.ParameterDefinition> parameters,
        TypeExpression returnType,
        Multiplicity returnMultiplicity,
        List<StereotypeApplication> stereotypes,
        List<TaggedValue> taggedValues) implements PackageableElement, Function {

    public NativeFunctionDefinition {
        Objects.requireNonNull(qualifiedName, "Qualified name cannot be null");
        Objects.requireNonNull(returnType, "Return type cannot be null");
        Objects.requireNonNull(returnMultiplicity, "Return multiplicity cannot be null");
        typeParameters = typeParameters == null ? List.of() : List.copyOf(typeParameters);
        multiplicityParameters = multiplicityParameters == null ? List.of() : List.copyOf(multiplicityParameters);
        parameters = parameters == null ? List.of() : List.copyOf(parameters);
        stereotypes = stereotypes == null ? List.of() : List.copyOf(stereotypes);
        taggedValues = taggedValues == null ? List.of() : List.copyOf(taggedValues);
    }
}
