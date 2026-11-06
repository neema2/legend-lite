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
 *   sends is a step of its plan, read by the runner (a caller's message, a split rung's row: the program's reader).
 *   The one exception is a NAMED RESIDUE ({@link #residue}): a crossing the platform still carries, closed unless the
 *   lane opens its door ({@code -Dlegend.wall.residue=<name>,...}), deleted by name with the code that needs it.</li>
 * </ol>
 *
 * <p>Nothing turns the wall off: there is no report mode (the measurement of 2026-11-06 listed every crossing over
 * core, PCT and both corpus lanes; each was fixed or named a residue). The session's openers and statements are the
 * JDBC census's seam ({@code JdbcSurfaceCensusTest}): no other class in core's main sources reaches a driver, so the
 * rules hold for every road. {@code WallTest} tries each crossing and is refused.
 *
 * <p>JDK-only, as base is: the one place both the runner and the planning libraries can see -- and within what the
 * WebAssembly planner's class library has (TeaVM: no {@code StackWalker}, no {@code ConcurrentHashMap.newKeySet}; the
 * 2026-11-05 CI run failed every lane on {@code //wasm:planner} until the callers were read off a throwable's trace).
 */
public final class RunScope implements AutoCloseable {

    private static final ThreadLocal<int[]> DEPTH = ThreadLocal.withInitial(() -> new int[1]);
    private static final ThreadLocal<Slot> PLANNING = ThreadLocal.withInitial(Slot::new);
    private static final ThreadLocal<Slot> RESIDUE = ThreadLocal.withInitial(Slot::new);
    private static final RunScope SCOPE = new RunScope();

    private RunScope() {
    }

    /** A thread's named scope: what it plans, or the residue in force; null for none. */
    private static final class Slot {
        @Nullable String name;
    }

    /**
     * A plan starts running on this thread; close it when the run ends. Refused while this thread plans (rule 2), and
     * inside a run (rule 4) unless a residue whose door is open is in force.
     */
    public static RunScope enter() {
        String planning = PLANNING.get().name;
        if (planning != null) {
            throw new Refused("the wall: a plan run while " + planning + " is planned (planning touches no database)"
                    + " <- " + callers(12));
        }
        if (DEPTH.get()[0] > 0) {
            String residue = RESIDUE.get().name;
            if (residue == null || !open(residue)) {
                throw new Refused("the wall: a run started inside a run (every statement a run sends is a step of its"
                        + " plan)" + (residue == null ? "" : "; the residue '" + residue + "' is closed on this lane")
                        + " <- " + callers(12));
            }
        }
        DEPTH.get()[0]++;
        return SCOPE;
    }

    @Override
    public void close() {
        DEPTH.get()[0]--;
    }

    /** The wall's refusal, by type: a catch that turns a failure into an answer (a decline, a fallback) rethrows it. */
    public static final class Refused extends IllegalStateException {
        private static final long serialVersionUID = 1L;

        Refused(String message) {
            super(message);
        }
    }

    /** Whether a plan is running on this thread. */
    public static boolean running() {
        return DEPTH.get()[0] > 0;
    }

    /** Rule 1: a planning seam is about to do {@code what}, refused while a plan runs. */
    public static void planning(String what) {
        if (DEPTH.get()[0] > 0) {
            throw new Refused("the runtime wall: " + what + " while a plan runs (" + what + " <- " + callers(24) + ")");
        }
    }

    /** Rule 2: this thread plans {@code what} until the scope closes (an inner scope keeps the outer's name). Refused
     *  inside a run: a run plans nothing (rule 1). */
    public static Planning plans(String what) {
        if (DEPTH.get()[0] > 0) {
            throw new Refused("the runtime wall: planning " + what + " while a plan runs <- " + callers(24));
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

    /**
     * Rule 4's exception: the NAMED RESIDUE {@code name} is in force until the scope closes -- a run it starts inside
     * a run is admitted where the lane opens its door ({@code -Dlegend.wall.residue} lists it), refused by name
     * elsewhere. A residue is a crossing the platform still carries, named here so it cannot hide and deleted with the
     * code that needs it.
     */
    public static Residue residue(String name) {
        Slot slot = RESIDUE.get();
        String previous = slot.name;
        slot.name = name;
        return new Residue(previous);
    }

    /** A residue scope; closing restores the one before it. */
    public static final class Residue implements AutoCloseable {
        private final @Nullable String previous;

        private Residue(@Nullable String previous) {
            this.previous = previous;
        }

        @Override
        public void close() {
            RESIDUE.get().name = previous;
        }
    }

    /** Whether the lane opens the door of residue {@code name}. */
    private static boolean open(String name) {
        String doors = System.getProperty("legend.wall.residue", "");
        for (String d : doors.split(",")) {
            if (d.strip().equals(name)) {
                return true;
            }
        }
        return false;
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
