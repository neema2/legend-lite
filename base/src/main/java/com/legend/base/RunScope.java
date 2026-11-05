package com.legend.base;

/**
 * THE RUNTIME WALL (the deep audit of 2026-11-05; the owner: "closed off at runtime any plan vs exec stuff so they can
 * never cross even through the driver"). While a plan runs on a thread, nothing on that thread may type, lower, render
 * or compose a statement the plan does not carry: every statement a run sends is planned before it starts.
 *
 * <p>The plan runner enters the scope for each run ({@link #enter}); the planning seams -- the SQL renderer's one
 * writer, the lowerer, the spec compiler -- say what they are about to do ({@link #planning}). The mode is
 * {@code -Dlegend.runwall}: {@code report} prints each site once ({@code [run-wall]}), {@code strict} refuses it by
 * name, and with no property nothing is checked, until every site is planned ahead and the default becomes strict.
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

    /** Whether a plan is running on this thread. */
    public static boolean running() {
        return DEPTH.get()[0] > 0;
    }

    /** A planning seam is about to do {@code what}: refused or reported while a plan runs, per the mode. */
    public static void planning(String what) {
        if (DEPTH.get()[0] == 0) {
            return;
        }
        String mode = System.getProperty("legend.runwall");
        if (mode == null || mode.equals("off")) {
            return;
        }
        String site = what + " <- " + callers();
        if (mode.equals("strict")) {
            throw new IllegalStateException("the runtime wall: " + what + " while a plan runs (" + site + ")");
        }
        if (REPORTED.add(site)) {
            System.err.println("[run-wall] " + site);
        }
    }

    /** The nearest callers outside the planning libraries, for the report. */
    private static String callers() {
        return StackWalker.getInstance().walk(frames -> frames
                .map(f -> f.getClassName().substring(f.getClassName().lastIndexOf('.') + 1) + "." + f.getMethodName())
                .filter(n -> !n.startsWith("RunScope.") && !n.startsWith("AnsiSqlRenderer.")
                        && !n.startsWith("Lowerer.") && !n.startsWith("SpecCompiler."))
                .limit(4)
                .collect(java.util.stream.Collectors.joining(" <- ")));
    }
}
