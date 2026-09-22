package com.legend.protocol.spec;

import java.util.Objects;

/**
 * A {@code #TDS{ ... }#} literal. Like {@link PathLiteral}, the parse
 * product keeps BOTH representations: the wire fields (engine emits a
 * {@code classInstance} of type {@code TDS} whose value is
 * {@code {"tdsString": <inner text, untrimmed>}} — ZTailProbe
 * "tds-accessor") and the desugared {@code tds(...)} application the
 * compiler consumes. {@code NameResolver} dissolves the node into
 * {@link #desugared()} on first touch.
 */
public record TdsLiteral(String tdsString, AppliedFunction desugared,
        @com.legend.base.Nullable com.legend.protocol.SourceInfo pos)
        implements ValueSpecification {

    public TdsLiteral {
        Objects.requireNonNull(tdsString, "tdsString");
        Objects.requireNonNull(desugared, "desugared");
    }
}
