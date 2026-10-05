package com.legend.protocol;

/**
 * A source span, in legend-engine's coordinate convention.
 *
 * <p><b>1-based</b> lines, <b>1-based</b> start column, and an <b>inclusive</b> end column
 * ({@code charPositionInLine + text.length()}, deliberately no {@code +1}). Reproduced exactly
 * because protocol {@code sourceInformation} must be byte-identical.
 *
 * <p>Lives in {@code com.legend.protocol} — the bottom parse-product layer — because every
 * record that carries a span (protocol elements, the value-spec AST) lives at or above this
 * layer. {@code protocol} depends on nothing but {@code values} and the JDK; {@code model}
 * sits above it (ArchitectureTest invariants 6j/7b).
 */
public record SourceInfo(String sourceId, int startLine, int startColumn,
                         int endLine, int endColumn) {

    /**
     * A span the PARSER built, which it always has: the records type their spans {@code @Nullable}
     * because a model READ from JSON without source information carries none ({@code ModelReader}),
     * and the parser's own derivations go through here.
     */
    public static SourceInfo parsed(@com.legend.base.Nullable SourceInfo span) {
        return java.util.Objects.requireNonNull(span, "a parsed node always carries its span");
    }
}
