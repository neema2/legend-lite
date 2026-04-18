package com.gs.legend.compiled;

/**
 * Source location of a compiled element — file URI plus 1-based line/column
 * spans. Carried on {@link CompiledElement} so compiler errors and IDE
 * diagnostics can point back at the originating source.
 *
 * <p>Stub shape for Phase 1a. A full source-location sidecar is tracked as a
 * separate workstream; until it lands, callers populate this with {@code null}
 * or with whatever partial info the parser makes available. Consumers must
 * tolerate null fields.
 *
 * @param fileUri   URI of the source file, or {@code null} if unknown.
 * @param startLine 1-based inclusive line of the element start, or 0 if unknown.
 * @param startCol  1-based inclusive column of the element start, or 0 if unknown.
 * @param endLine   1-based inclusive line of the element end, or 0 if unknown.
 * @param endCol    1-based inclusive column of the element end, or 0 if unknown.
 */
public record SourceLocation(
        String fileUri,
        int startLine,
        int startCol,
        int endLine,
        int endCol) {

    /** Placeholder used until a real source-location sidecar lands. */
    public static final SourceLocation UNKNOWN = new SourceLocation(null, 0, 0, 0, 0);
}
