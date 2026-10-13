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

    /** Position is excluded from equality, as on every value specification ({@code ValueSpecEqualityTest}): a
     *  let's value is written with the let's span, so it reads back with that one (leg 8 step 2). */
    @Override
    public boolean equals(Object o) {
        return o instanceof TdsLiteral other && tdsString.equals(other.tdsString())
                && desugared.equals(other.desugared());
    }

    @Override
    public int hashCode() {
        return Objects.hash(tdsString, desugared);
    }

    public TdsLiteral {
        Objects.requireNonNull(tdsString, "tdsString");
        Objects.requireNonNull(desugared, "desugared");
    }

    /**
     * The literal and its desugaring -- an application of the lite-internal {@code tds} native,
     * spelled by its EXACT FQN (the bare name {@code tds} is not user-resolvable, so this is the only
     * route to the native) over the {@code "TDS"} discriminator and the table: {@code tdsString},
     * the text between the literal's delimiters, in either spelling ({@code #TDS{...}#}, the
     * engine's; {@code #TDS ... #}, legend-pure's). ONE owner for the parser (text) and the model
     * reader (JSON), so the compiler reads a table and never a delimiter (2026-10-13: it stripped
     * {@code #TDS} and {@code #} only, and read the engine's braces as the header and a row).
     */
    public static TdsLiteral of(String tdsString,
            @com.legend.base.Nullable com.legend.protocol.SourceInfo pos) {
        return new TdsLiteral(tdsString, new AppliedFunction("meta::legend::lite::tds",
                java.util.List.of(new CString("TDS"), new CString(tdsString))), pos);
    }
}
