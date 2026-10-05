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

    /**
     * The literal and its desugaring -- an application of the lite-internal {@code tds} native,
     * spelled by its EXACT FQN (the bare name {@code tds} is not user-resolvable, so this is the only
     * route to the native) over the {@code "TDS"} discriminator and the literal's verbatim text. ONE
     * owner of the spelling for the parser (text) and the model reader (JSON, whose literal text is
     * the brace form around {@code tdsString}).
     */
    public static TdsLiteral of(String tdsString, String literalText,
            @com.legend.base.Nullable com.legend.protocol.SourceInfo pos) {
        return new TdsLiteral(tdsString, new AppliedFunction("meta::legend::lite::tds",
                java.util.List.of(new CString("TDS"), new CString(literalText))), pos);
    }
}
