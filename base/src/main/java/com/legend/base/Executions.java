package com.legend.base;

/**
 * A RUN'S EXECUTIONS, as its door minted them (the execution id done as upstream, the user's ruling of 2026-11-12: "this
 * needs to go in C that was the whole point of C"): {@code run}, the run's own id -- what the run names to its listener,
 * a test body's own -- and {@code declared}, one id for each execution the plan declares, by its stamp's name.
 *
 * <p>Upstream mints one id per execution: a plan's run has ONE ({@code ExecutionState.java:149}, every SQL node's
 * {@code ${execID}} filled with it, {@code RelationalExecutor.java:332}), and a Pure test body's {@code execute()} calls
 * each get their own ({@code relationalMappingExecution.pure:475-492}, a GUID per query run). So a product plan is
 * {@link #one}: every stamp it carries is its one execution's; a test body's plan declares one execution per
 * {@code execute()} ({@code TestPlan.executions}) and is {@link #of}: each stamp its own id, a stamp it does not declare
 * refused by name. Data the door makes; exec binds, never makes one ({@code ArchitectureTest.execMakesNoValueOfItsOwn}).
 */
public record Executions(java.util.UUID run, java.util.@Nullable Map<String, java.util.UUID> declared) {

    public Executions {
        java.util.Objects.requireNonNull(run, "run: the run's door mints it");
        if (declared != null) {
            declared = java.util.Collections.unmodifiableMap(new java.util.LinkedHashMap<>(declared));
        }
    }

    /** A run that is ONE execution (a product plan's, a fixture's): every stamp it binds is {@code run}. */
    public static Executions one(java.util.UUID run) {
        return new Executions(run, null);
    }

    /** A run whose plan declares {@code declared}, each its own execution (a test body's {@code execute()}s). */
    public static Executions of(java.util.UUID run, java.util.Map<String, java.util.UUID> declared) {
        return new Executions(run, declared);
    }

    /** The id of the execution the stamp {@code stamp} names: the run's one, or the one its plan declared under that
     *  name -- refused by name where the plan declared executions and not this one (never a fresh id, never the run's
     *  in its place). */
    public java.util.UUID of(String stamp) {
        if (declared == null) {
            return run;
        }
        java.util.UUID id = declared.get(stamp);
        if (id == null) {
            throw new IllegalStateException("the plan reads the execution '" + stamp + "', which it does not declare"
                    + " (it declares " + declared.keySet() + "): an execution's id is its door's, one per declared"
                    + " execution");
        }
        return id;
    }
}
