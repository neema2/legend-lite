package com.legend.error;

/**
 * Base of every USER-FACING compilation failure — the answer to "is this
 * your model/query, our bug, or an unimplemented feature?" being
 * indistinguishable by type (AUDIT_2026_07 §11: 115 IllegalStateExceptions
 * spanning all three meanings; the pipeline stage-failure test had to grep
 * class-name strings).
 *
 * <p>The taxonomy:
 * <ul>
 *   <li>{@code LegendCompileException} subtypes — the USER's input is at
 *       fault; {@link #phase()} names the pipeline stage that rejected it.</li>
 *   <li>{@link NotImplementedException} — the input may be fine; the
 *       feature isn't built. Greppable backlog.</li>
 *   <li>{@code IllegalStateException} — reserved for genuine internal
 *       invariant violations (our bugs).</li>
 *   <li>{@code IllegalArgumentException} — record-constructor preconditions.</li>
 * </ul>
 *
 * <p>This package is a LEAF (imports nothing) so every layer may depend on
 * it under the package-acyclicity rule (ArchitectureTest Invariant 4).
 */
public abstract class LegendCompileException extends RuntimeException {

    /** The pipeline stage that rejected the input. */
    public enum Phase { PARSE, RESOLVE, NORMALIZE, MODEL, TYPE, LOWER, EXECUTE }

    private final Phase phase;
    private final String element;

    protected LegendCompileException(Phase phase, String message) {
        this(phase, message, (String) null);
    }

    protected LegendCompileException(Phase phase, String message, String element) {
        super(message);
        this.phase = java.util.Objects.requireNonNull(phase, "phase");
        this.element = element;
    }

    protected LegendCompileException(Phase phase, String message, Throwable cause) {
        super(message, cause);
        this.phase = java.util.Objects.requireNonNull(phase, "phase");
        this.element = null;
    }

    public Phase phase() {
        return phase;
    }

    /**
     * FQN of the model element the failure is about, when the throw site
     * knows it — the driver decorates the message with the element's
     * {@code [line:col]} from the parse-time side index ({@code ParsedModel
     * .elementOffsets}). Null when unknown.
     */
    public String element() {
        return element;
    }

    /** Render a char offset in {@code source} as {@code [line:col]} (1-based). */
    public static String position(String source, int offset) {
        int line = 1;
        int col = 1;
        int end = Math.min(offset, source.length());
        for (int i = 0; i < end; i++) {
            if (source.charAt(i) == '\n') {
                line++;
                col = 1;
            } else {
                col++;
            }
        }
        return "[" + line + ":" + col + "]";
    }
}
