package com.legend.base;

/**
 * THE WALL between planning and running (the deep audit of 2026-11-05; the owner: "closed off at runtime any plan vs
 * exec stuff so they can never cross even through the driver"; made total 2026-11-06, the owner: "this IS the invariant
 * we need to ship"). Three scopes on a thread, and four rules, each refused by name ({@link Refused}) at the one place
 * every crossing must pass:
 *
 * <ol>
 *   <li><b>A run plans nothing.</b> While a plan runs ({@link #enter}), the planning seams -- the SQL renderer's one
 *   writer, the lowerer, the spec compiler -- refuse what they are about to do ({@link #planning}).</li>
 *   <li><b>Planning touches no database.</b> While a body is planned ({@link #plans}), no run starts and no session
 *   opens ({@link #opening}: the session owner's doors), and no statement is sent (rule 3).</li>
 *   <li><b>Every statement is a run's.</b> A session's statements -- a send, a load, a metadata read, a session's
 *   check -- are taken inside a plan's run alone ({@link #statement}: the session's one door to them).</li>
 *   <li><b>A run starts no run.</b> A run started inside a run is refused ({@link #enter}): every statement a run
 *   sends is a step of its plan (a caller's message and a split rung's statement are planned sides and verdicts of
 *   the program, P1.2, 2026-11-07). No exception: the engine-text appeal's runs inside a body's run were deleted on
 *   2026-11-06, and a held appeal is decided after the body's run, its rows read as plans of their own.</li>
 *   <li><b>A judgment plans nothing.</b> While a held appeal is decided after the body's run ({@link #judging}), the
 *   planning seams refuse as they do inside a run, and no body is planned ({@link #plans}).</li>
 * </ol>
 *
 * <p>Nothing turns the wall off: there is no report mode (the measurement of 2026-11-06 listed every crossing over
 * core, PCT and both corpus lanes; each was fixed or named a residue). The session's openers and statements are the
 * JDBC census's seam ({@code JdbcSurfaceCensusTest}): no other class in core's main sources reaches a driver, so the
 * rules hold for every road. {@code RunScopeTest} tries each crossing and is refused.
 *
 * <p>JDK-only, as base is: the one place both the runner and the planning libraries can see -- and within what the
 * WebAssembly planner's class library has (TeaVM: no {@code StackWalker}, no {@code ConcurrentHashMap.newKeySet}; the
 * 2026-11-05 CI run failed every lane on {@code //wasm:planner} until the callers were read off a throwable's trace).
 */
public final class RunScope implements AutoCloseable {

    private static final ThreadLocal<int[]> DEPTH = ThreadLocal.withInitial(() -> new int[1]);
    /** The running execution's id (C1's second half, the user's ruling of 2026-11-10, "Agree with all": "the execution
     *  id lives in lite's own trace"; built 2026-11-12, the C checkpoint's code audit SHOULD-FIX 4): the id the DOOR that
     *  started the run minted and passed in ({@link #enter(java.util.UUID)}; the user's ruling of 2026-11-12, "Do the exec
     *  id as part of C": exec makes no value of its own, so nothing here or in exec mints one -- until then this scope
     *  minted one as a run entered), as legend-engine's {@code ExecutionState} holds its {@code execID} once per
     *  execution ({@code ExecutionState.java:149}) and tags its relational span with it
     *  ({@code RelationalExecutionNodeExecutor.java:467}); this scope is lite's span of a run. */
    private static final ThreadLocal<Execution> EXECUTION = ThreadLocal.withInitial(Execution::new);
    private static final ThreadLocal<Slot> PLANNING = ThreadLocal.withInitial(Slot::new);
    private static final RunScope SCOPE = new RunScope();

    private RunScope() {
    }

    /** A thread's named scope: what it plans; null for none. */
    private static final class Slot {
        @Nullable String name;
    }

    /** A thread's running execution: its id, as its door minted it; null where none runs. */
    private static final class Execution {
        java.util.@Nullable UUID id;
    }

    /**
     * A plan starts running on this thread as the execution {@code execution} -- the id its door minted and passed in
     * (an execution is one plan run: a product query's, a test body's, a fixture's); close it when the run ends. Refused
     * as {@link #enter()} is.
     */
    public static RunScope enter(java.util.UUID execution) {
        java.util.Objects.requireNonNull(execution, "execution: the run's door mints it");
        RunScope scope = enter();
        EXECUTION.get().id = execution;
        return scope;
    }

    /**
     * A plan starts running on this thread, as no execution its activity reports (a run of one statement: a write's,
     * an effect's, a probe's, a load's -- none reads an execution's id); close it when the run ends. Refused while this
     * thread plans (rule 2), and inside a run (rule 4).
     */
    public static RunScope enter() {
        String planning = PLANNING.get().name;
        if (planning != null) {
            throw new Refused("the wall: a plan run while " + planning + " is planned (planning touches no database)"
                    + " <- " + callers(12));
        }
        if (DEPTH.get()[0] > 0) {
            throw new Refused("the wall: a run started inside a run (every statement a run sends is a step of its"
                    + " plan) <- " + callers(12));
        }
        DEPTH.get()[0]++;
        EXECUTION.get().id = null;
        return SCOPE;
    }

    @Override
    public void close() {
        DEPTH.get()[0]--;
        EXECUTION.get().id = null;
    }

    /** The id of the execution running on this thread: what EVERY stamp its statements read binds (one id per plan
     *  run, a product query's and a test body's alike: the user's ruling of 2026-11-12, the simple execution id), what
     *  the run names to its listener, what the SQL dump prints -- its door's. Refused by name where none runs, or the
     *  run is no execution ({@link #enter()}): an id read there is a defect, never a fresh one. */
    public static java.util.UUID execution() {
        java.util.UUID id = EXECUTION.get().id;
        if (id == null) {
            throw new IllegalStateException("no execution runs on this thread: an execution's id is its run's");
        }
        return id;
    }

    /** The id of the execution running on this thread, or null where none runs. */
    public static java.util.@Nullable UUID currentExecution() {
        return EXECUTION.get().id;
    }

    /** The wall's refusal, by type: an {@link Error}, so no {@code catch (RuntimeException)} or {@code catch (Exception)}
     *  -- a decline, a fallback, a "not this shape" -- can turn a crossing into an answer (the final audit,
     *  2026-11-06: the compatibility re-run arm's catch had made one a text mismatch). A crossing is a bug of lite's,
     *  never a verdict. */
    public static final class Refused extends Error {
        private static final long serialVersionUID = 1L;

        Refused(String message) {
            super(message);
        }
    }

    /** Whether a plan is running on this thread. */
    public static boolean running() {
        return DEPTH.get()[0] > 0;
    }

    /** Rule 1: a planning seam is about to do {@code what}, refused while a plan runs, and while a judgment made after
     *  a run is decided ({@link #judging}). */
    public static void planning(String what) {
        if (DEPTH.get()[0] > 0 || JUDGING.get()[0] > 0) {
            throw new Refused("the runtime wall: " + what + " while " + busy() + " (" + what + " <- " + callers(24)
                    + ")");
        }
    }

    private static final ThreadLocal<int[]> JUDGING = ThreadLocal.withInitial(() -> new int[1]);

    /** What refuses planning on this thread: a run, or a judgment decided after one, or a run's caller called back. */
    private static String busy() {
        return DEPTH.get()[0] > 0 ? "a plan runs"
                : "a judgment is decided after its run, or a run's caller is called back for it";
    }

    /** A judgment decided AFTER a run (a held compatibility appeal, P1, 2026-11-07): it plans nothing -- the planning
     *  seams refuse as inside a run -- and reads only plans planned ahead, each a run of its own. Close it when the
     *  judgment ends. */
    public static Judging judging() {
        JUDGING.get()[0]++;
        return () -> JUDGING.get()[0]--;
    }

    /** A run's caller called back around it (the full audit's must-fix, 2026-11-09: the wall's blind spot at exec's
     *  entry): the sink a caller makes from the plan, the session it opens for the plan's target, the answer's sink --
     *  each called before the plan's run scope opens ({@link #enter}) -- plan nothing; planning is refused as inside a
     *  run. Exec's runs hold it from their entry to their end. Close it when the run ends. */
    public static Judging callingBack() {
        JUDGING.get()[0]++;
        return () -> JUDGING.get()[0]--;
    }

    /** A judgment's scope ({@link #judging}). */
    public interface Judging extends AutoCloseable {
        @Override
        void close();
    }

    /** Rule 2: this thread plans {@code what} until the scope closes (an inner scope keeps the outer's name). Refused
     *  inside a run: a run plans nothing (rule 1). */
    public static Planning plans(String what) {
        if (DEPTH.get()[0] > 0 || JUDGING.get()[0] > 0) {
            throw new Refused("the runtime wall: planning " + what + " while " + busy() + " <- " + callers(24));
        }
        Slot slot = PLANNING.get();
        String previous = slot.name;
        slot.name = previous != null ? previous : what;
        return new Planning(previous);
    }

    /** Whether this thread is planning ({@link #plans}). */
    public static boolean isPlanning() {
        return PLANNING.get().name != null;
    }

    /** The name of what this thread is planning, or null. */
    public static @Nullable String planningWhat() {
        return PLANNING.get().name;
    }

    /** A planning scope; closing restores the one before it. */
    public static final class Planning implements AutoCloseable {
        private final @Nullable String previous;

        private Planning(@Nullable String previous) {
            this.previous = previous;
        }

        @Override
        public void close() {
            PLANNING.get().name = previous;
        }
    }

    /** Rule 2: a session's owner is about to {@code act} (open a session, bracket a transaction): refused while this
     *  thread plans. */
    public static void opening(String act) {
        String planning = PLANNING.get().name;
        if (planning != null) {
            throw new Refused("the wall: " + act + " while " + planning + " is planned (planning touches no database)"
                    + " <- " + callers(12));
        }
    }

    /** Rule 3: a session's statements are about to {@code act} (send, load, read metadata, check the session): refused
     *  outside a plan's run. */
    public static void statement(String act) {
        if (DEPTH.get()[0] == 0) {
            String planning = PLANNING.get().name;
            throw new Refused("the wall: " + act + " outside a plan's run"
                    + (planning == null ? "" : " (while " + planning + " is planned)") + " <- " + callers(12));
        }
    }

    /** The nearest callers outside the wall's own seams, for a refusal's message. */
    private static String callers(int depth) {
        StringBuilder out = new StringBuilder();
        int named = 0;
        for (StackTraceElement f : new Throwable().getStackTrace()) {
            String n = f.getClassName().substring(f.getClassName().lastIndexOf('.') + 1) + "." + f.getMethodName();
            if (n.startsWith("RunScope.") || n.startsWith("AnsiSqlRenderer.") || n.startsWith("Lowerer.")
                    || n.startsWith("SpecCompiler.")) {
                continue;
            }
            if (named++ == depth) {
                break;
            }
            out.append(out.length() == 0 ? "" : " <- ").append(n);
        }
        return out.toString();
    }
}
