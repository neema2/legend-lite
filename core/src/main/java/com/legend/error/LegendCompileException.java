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
    /** The pipeline stages. {@code RENDER} (F4.1) is a PLAN-CONSTRUCTION
     *  phase: when a text form is requested the plan wraps with a render
     *  projection and the DATABASE produces the final text. */
    public enum Phase { PARSE, RESOLVE, NORMALIZE, MODEL, TYPE, MAPPING, LOWER, RENDER, EXECUTE }

    private final Phase phase;
    private final @com.legend.base.Nullable String element;
    /** The error's kind, where its throw site names one (D1.6): its message is lite's sentence of the kind. */
    private final @com.legend.base.Nullable CompileKind kind;
    private final java.util.List<String> operands;
    /** The element's {@code [line:col]} the driver put before the sentence, if any. */
    private final @com.legend.base.Nullable String position;

    /** An error of {@code kind} over {@code operands}, its message lite's sentence ({@link CompileKind#sentence}),
     *  after {@code position} where the driver placed it. */
    protected LegendCompileException(Phase phase, CompileKind kind, java.util.List<String> operands,
            @com.legend.base.Nullable String element, @com.legend.base.Nullable String position) {
        super((position == null ? "" : position + " ") + kind.sentence(operands));
        this.phase = java.util.Objects.requireNonNull(phase, "phase");
        this.element = element;
        this.kind = kind;
        this.operands = java.util.List.copyOf(operands);
        this.position = position;
    }

    protected LegendCompileException(Phase phase, @com.legend.base.Nullable String message) {
        this(phase, message, (String) null);
    }

    protected LegendCompileException(Phase phase, @com.legend.base.Nullable String message,
            @com.legend.base.Nullable String element) {
        super(message);
        this.phase = java.util.Objects.requireNonNull(phase, "phase");
        this.element = element;
        this.kind = null;
        this.operands = java.util.List.of();
        this.position = null;
    }

    /** An error with no kind, its message its site's own text, after {@code position} where the driver or the parser
     *  placed it -- the position kept as data, {@link #position()}, beside the same message (lite's API v1 spec §3's
     *  {@code where}, 2026-11-17). */
    protected LegendCompileException(Phase phase, @com.legend.base.Nullable String message,
            @com.legend.base.Nullable String element, @com.legend.base.Nullable String position) {
        super(position == null ? message : position + " " + message);
        this.phase = java.util.Objects.requireNonNull(phase, "phase");
        this.element = element;
        this.kind = null;
        this.operands = java.util.List.of();
        this.position = position;
    }

    protected LegendCompileException(Phase phase, @com.legend.base.Nullable String message,
            Throwable cause) {
        super(message, cause);
        this.phase = java.util.Objects.requireNonNull(phase, "phase");
        this.element = null;
        this.kind = null;
        this.operands = java.util.List.of();
        this.position = null;
    }

    public Phase phase() {
        return phase;
    }

    /**
     * FQN of the model element the failure is about, when the throw site
     * knows it — the driver decorates the message with the element's
     * {@code [line:col]} ({@code ParsedModel.position}: its offset into the
     * text, or its span when the model came as records). Null when unknown.
     */
    public @com.legend.base.Nullable String element() {
        return element;
    }

    /** The error's kind, where its throw site names one (D1.6), else null: its message then is the site's own text. */
    public @com.legend.base.Nullable CompileKind kind() {
        return kind;
    }

    /** The kind's operands, in its sentence's order (empty where there is no kind). */
    public java.util.List<String> operands() {
        return operands;
    }

    /** The {@code [line:col]} the driver or the parser placed before the message, or null. */
    public @com.legend.base.Nullable String position() {
        return position;
    }
}
