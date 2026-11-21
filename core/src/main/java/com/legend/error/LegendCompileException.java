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
public abstract class LegendCompileException extends RuntimeException implements LegendError {

    /** The pipeline stage that rejected the input. */
    /** The pipeline stages. {@code RENDER} (F4.1) is a PLAN-CONSTRUCTION
     *  phase: when a text form is requested the plan wraps with a render
     *  projection and the DATABASE produces the final text. */
    public enum Phase { PARSE, RESOLVE, NORMALIZE, MODEL, TYPE, MAPPING, LOWER, RENDER, EXECUTE }

    private final Phase phase;
    private final @com.legend.base.Nullable String element;
    /** The error as data, where its site states one (stage B's BE, the one error model, 2026-11-21): its code, its
     *  values, its position, its frames; its message is the record's sentence in lite's words. Null where the site
     *  states its own text (a site not yet moved to a code). */
    private final @com.legend.base.Nullable ErrorRecord record;
    /** The element's {@code [line:col]} the driver put before the sentence, if any. */
    private final @com.legend.base.Nullable String position;

    /** An error of {@code kind} over {@code operands}, its message lite's sentence ({@link CompileKind#sentence}),
     *  after {@code position} where the driver placed it. */
    protected LegendCompileException(Phase phase, CompileKind kind, java.util.List<String> operands,
            @com.legend.base.Nullable String element, @com.legend.base.Nullable String position) {
        this(phase, new ErrorRecord(kind, operands, position, java.util.List.of()), element);
    }

    /** The error {@code record} states, its message the record's sentence in lite's words. */
    protected LegendCompileException(Phase phase, ErrorRecord record, @com.legend.base.Nullable String element) {
        super(record.sentence());
        this.phase = java.util.Objects.requireNonNull(phase, "phase");
        this.element = element;
        this.record = record;
        this.position = record.position();
    }

    /** {@code wrapped} inside one more {@code frame} (stage B's BE, 2026-11-21: until then the wrapper's stated texts):
     *  its message the frame's words around the wrapped error's, its cause {@code wrapped}, its record the wrapped
     *  error's, framed. */
    protected LegendCompileException(Phase phase, LegendCompileException wrapped, ErrorRecord.Frame frame) {
        this(phase, wrapped, frame, wrapped.record() == null ? null : wrapped.record().framed(frame));
    }

    private LegendCompileException(Phase phase, LegendCompileException wrapped, ErrorRecord.Frame frame,
            @com.legend.base.Nullable ErrorRecord framed) {
        // a record's message is its sentence (the position outermost); an error with none, the frame around its text
        super(framed != null ? framed.sentence() : frame.kind().wrap(frame.values(),
                String.valueOf(wrapped.getMessage())), wrapped);
        this.phase = java.util.Objects.requireNonNull(phase, "phase");
        this.element = null;
        this.record = framed;
        this.position = framed == null ? null : framed.position();
    }

    protected LegendCompileException(Phase phase, @com.legend.base.Nullable String message) {
        this(phase, message, (String) null);
    }

    protected LegendCompileException(Phase phase, @com.legend.base.Nullable String message,
            @com.legend.base.Nullable String element) {
        super(message);
        this.phase = java.util.Objects.requireNonNull(phase, "phase");
        this.element = element;
        this.record = null;
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
        this.record = null;
        this.position = position;
    }

    protected LegendCompileException(Phase phase, @com.legend.base.Nullable String message,
            Throwable cause) {
        super(message, cause);
        this.phase = java.util.Objects.requireNonNull(phase, "phase");
        this.element = null;
        this.record = null;
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

    @Override
    public @com.legend.base.Nullable ErrorRecord record() {
        return record;
    }

    /** The error's compile kind, where its record's code is one and it passed through no frame, else null. */
    public @com.legend.base.Nullable CompileKind kind() {
        return record != null && record.frames().isEmpty() && record.code() instanceof CompileKind k ? k : null;
    }

    /** The kind's operands, in its sentence's order (empty where {@link #kind()} is null). */
    public java.util.List<String> operands() {
        ErrorRecord r = record;
        return r == null || kind() == null ? java.util.List.of() : r.values();
    }

    /** The {@code [line:col]} the driver or the parser placed before the message, or null. */
    public @com.legend.base.Nullable String position() {
        return position;
    }
}
