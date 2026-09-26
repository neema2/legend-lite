package com.legend.protocol.spec;

import java.util.Objects;

/**
 * String literal. Example Pure source: {@code 'hello world'}.
 *
 * <p>The {@code value} is the unquoted, unescaped source text &mdash; the
 * surrounding single quotes are stripped and backslash escapes
 * ({@code \\}, {@code \'}, {@code \n}, {@code \t}, {@code \r}) are
 * resolved by the parser so consumers see the logical string content.
 */
public record CString(String value, @com.legend.base.Nullable com.legend.protocol.SourceInfo pos,
                      boolean multiLine)
        implements ValueSpecification {

    /** Position-free convenience constructor — keeps hand-built test expectations compiling. */
    public CString(String value) {
        this(value, null, false);
    }

    /** Ordinary single-quoted literal at a position. {@code multiLine} marks
     *  a {@code '''...'''} literal (4.138 wire: {@code "multiLine":true};
     *  ZMissedRowsProbe) — like {@code pos} it is EXCLUDED from equality:
     *  the logical string content is the semantic identity. */
    public CString(String value, @com.legend.base.Nullable com.legend.protocol.SourceInfo pos) {
        this(value, pos, false);
    }

    /**
     * <b>Position is excluded from equality on purpose.</b> These records are compared
     * structurally by the compiler and by 111 hand-built test assertions of the form
     * {@code assertEquals(new CString(...), spec)}; including a span would break every one and
     * would make two structurally identical expressions unequal for no semantic reason.
     * {@code ValueSpecEqualityTest} guards this — do not "fix" it.
     */
    @Override
    public boolean equals(Object o) {
        return o instanceof CString other && java.util.Objects.equals(value, other.value());
    }

    @Override
    public int hashCode() {
        return java.util.Objects.hashCode(value);
    }

    public CString {
        Objects.requireNonNull(value, "value");
    }
}
