package com.legend.protocol.spec;

import java.util.List;
import java.util.Objects;

/**
 * Anonymous function value. Source forms:
 * <ul>
 *   <li>Braced multi-param: {@code {p, q | body}} (body may be a
 *       multi-statement code block separated by {@code ;}).</li>
 *   <li>Braced zero-param: {@code {| body}}.</li>
 *   <li>Shorthand single-param: {@code x | body} (one body
 *       statement, no surrounding braces). Common inside arrow
 *       calls: {@code $xs->filter(p | $p.age > 21)}.</li>
 *   <li>Pipe zero-param: {@code | body} (single body statement).</li>
 * </ul>
 *
 * <p>Parameters are {@link Variable} records so they can carry an
 * optional type name and multiplicity (typed-lambda support lands in
 * C.5; C.4 always produces untyped {@link Variable}s with
 * {@code typeName == null} and {@code multiplicity == null}). The
 * body is a {@code List<ValueSpecification>}, mirroring engine's
 * shape and matching {@link com.legend.parser.SpecParser#parseCodeBlock}
 * &mdash; multi-statement bodies introduced by the braced form are
 * a sequence of {@link LetExpression}s and final-value expressions.
 *
 * <p>Mirrors engine's {@code LambdaFunction} record shape: same
 * field names, same semantics. No divergence needed; engine's record
 * is already minimal parser data.
 *
 * @param parameters  declared parameters in source order; empty for
 *                    zero-param forms
 * @param body        body statements in source order; the value of
 *                    the lambda is the value of the last statement
 */
public record LambdaFunction(
        List<Variable> parameters,
        List<ValueSpecification> body,
        @com.legend.base.Nullable com.legend.protocol.SourceInfo pos) implements ValueSpecification {

    public LambdaFunction {
        Objects.requireNonNull(parameters, "parameters");
        Objects.requireNonNull(body, "body");
        parameters = List.copyOf(parameters);
        body = List.copyOf(body);
    }

    /** Position-free form for synthesis and tests. An INLINE lambda carries a span on the
     *  wire (verified via ProbeWireShapes): from the parameter token (single untyped param)
     *  or the pipe (typed param) to the body's end. The constraint wrapper lambda the
     *  emitter synthesises carries none. */
    public LambdaFunction(List<Variable> parameters, List<ValueSpecification> body) {
        this(parameters, body, null);
    }

    /** Position is excluded from equality — see {@code ValueSpecEqualityTest}. */
    @Override
    public boolean equals(Object o) {
        return o instanceof LambdaFunction other
                && parameters.equals(other.parameters())
                && body.equals(other.body());
    }

    @Override
    public int hashCode() {
        return Objects.hash(parameters, body);
    }
}
