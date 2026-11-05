package com.legend.base;

/**
 * THE RUNTIME WALL (the deep audit of 2026-11-05; the owner: "closed off at runtime any plan vs exec stuff so they can
 * never cross even through the driver"). While a plan runs on a thread, nothing on that thread may type, lower, render
 * or compose a statement the plan does not carry: every statement a run sends is planned before it starts.
 *
 * <p>The plan runner enters the scope for each run ({@link #enter}); the planning seams -- the SQL renderer's one
 * writer, the lowerer, the spec compiler -- say what they are about to do ({@link #planning}), and it is refused by
 * name ({@link Refused}). {@code -Dlegend.runwall=report} prints each site once instead ({@code [run-wall]}), a
 * diagnostic; nothing turns the wall off. Strict since 2026-11-05, once the measurement over core and PCT found every
 * site and each was planned ahead (the batch's split rung, ruled fallbacks and caller's messages). No exemption (the
 * owner, 2026-11-05: "yes to zero exemptions please"): compatibility mode's row comparison after a text mismatch plans
 * its rows with the assert and has its stores provided before the program runs.
 *
 * <p>JDK-only, as base is: the one place both the runner and the planning libraries can see.
 */
public final class RunScope implements AutoCloseable {

    private static final ThreadLocal<int[]> DEPTH = ThreadLocal.withInitial(() -> new int[1]);
    private static final java.util.Set<String> REPORTED = java.util.concurrent.ConcurrentHashMap.newKeySet();
    private static final RunScope SCOPE = new RunScope();

    private RunScope() {
    }

    /** A plan starts running on this thread; close it when the run ends (nested runs nest). */
    public static RunScope enter() {
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

    /** A planning seam is about to do {@code what}: refused or reported while a plan runs, per the mode. */
    public static void planning(String what) {
        if (DEPTH.get()[0] == 0) {
            return;
        }
        String site = what + " <- " + callers() + ("deep".equals(System.getProperty("legend.runwall.detail"))
                ? " [test " + System.getProperty("legend.test.current") + "]" : "");
        if (!"report".equals(System.getProperty("legend.runwall"))) {
            throw new Refused("the runtime wall: " + what + " while a plan runs (" + what + " <- " + callers(24) + ")");
        }
        if (REPORTED.add(site)) {
            System.err.println("[run-wall] " + site);
        }
    }

    /** The nearest callers outside the planning libraries, for the report. */
    private static String callers() {
        return callers("deep".equals(System.getProperty("legend.runwall.detail")) ? 14 : 4);
    }

    private static String callers(int depth) {
        return StackWalker.getInstance().walk(frames -> frames
                .map(f -> f.getClassName().substring(f.getClassName().lastIndexOf('.') + 1) + "." + f.getMethodName())
                .filter(n -> !n.startsWith("RunScope.") && !n.startsWith("AnsiSqlRenderer.")
                        && !n.startsWith("Lowerer.") && !n.startsWith("SpecCompiler."))
                .limit(depth)
                .collect(java.util.stream.Collectors.joining(" <- ")));
    }
}
