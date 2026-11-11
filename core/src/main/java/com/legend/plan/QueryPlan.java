package com.legend.plan;

import com.legend.compiler.element.type.ExprType;

import java.util.Objects;

/**
 * THE TEXT PLAN: the query's execution plan as one answering statement, WITHOUT executing -- its text, the root's typed
 * contract, and the execution-shape classification -- rendered from the execution plan (B3, 2026-11-11,
 * docs/TOTAL_WALL_START_HERE_2026_11_11.md §5.2; until then the query was lowered again for its text, and a live source
 * refused), so the text is the statement that answers, with what runs it: {@link #plan}'s target (its session
 * statements and the loads that establish it), its slots (a value the run binds: a parameter, a {@code file:} source's
 * path) and, for a pivot whose columns the data names, its keys statement and per-key regions. A host that runs the
 * text as it is runs only a plan {@link #textAlone}; any other is its plan's runner's to run.
 *
 * @param plan     the query's execution plan, whose answer this is: a typed result, or a keyed one
 * @param rootType the query root's {@link ExprType} (schema + multiplicity)
 * @param shape    how a result of {@code rootType} is consumed
 */
public record QueryPlan(com.legend.executionplan.ExecutionPlan plan, ExprType rootType, ResultShape shape) {
    public QueryPlan {
        Objects.requireNonNull(plan, "plan");
        Objects.requireNonNull(rootType, "rootType");
        Objects.requireNonNull(shape, "shape");
        if (!plan.answers() || !plan.sendsTheAnswer()
                || plan.root() instanceof com.legend.executionplan.ExecutionPlan.Sequence) {
            throw new IllegalArgumentException("a text plan is a plan answered by one statement: a typed or keyed result");
        }
    }

    /** The answering statement (a keyed result's body, its per-key regions written once): its text, its slots and its
     *  target. */
    public com.legend.executionplan.ExecutionPlan.Sql statement() {
        return plan.answerSql();
    }

    /** The answering statement's text, in the runtime's dialect: what the database is sent -- for a keyed result its
     *  body as the plan holds it, each per-key region written once, its lead and separator the runner's, so a text
     *  that runs only through its plan ({@link #textAlone} false). */
    public String sql() {
        return statement().statement();
    }

    /** The statement's target: its database, its session statements, and the loads that establish it. */
    public com.legend.executionplan.ExecutionPlan.Target target() {
        return statement().target();
    }

    /** The result's columns with their three type facts (the plan's: {@code PlanColumns}); none for a graph fetch,
     *  whose objects are the wire's, or a keyed result, whose per-key columns its keys name when it runs. */
    public java.util.List<com.legend.executionplan.ExecutionPlan.Column> columns() {
        java.util.List<com.legend.executionplan.ExecutionPlan.Column> columns = plan.answerColumns();
        return shape == ResultShape.GRAPH || columns == null ? java.util.List.of() : columns;
    }

    /**
     * What of this plan neither its text nor its target's declared data says, or null where nothing: a pivot whose keys
     * are read first, a statement binding a {@code file:} source's path or a {@code data:} payload, or a target whose
     * loads make a {@code data:} source's session table and drop it after. A host that runs a plan's TEXT itself (the
     * tab's DuckDB, Python's) refuses such a plan by name, never sending it to fail on a table no load made
     * (docs/DATA_DELIVERY_DESIGN_2026_11_10.md §3 rule 5) -- its runner takes each over as it learns to run it (B3:
     * the hosts run a plan's loads; until then a target's test data and a project's Data are the host's own tables, as
     * they were); upstream's plan (generatePlan) carries lite's execution plan beside its text for it, where for a
     * connection's declared test data its connection already says it, as upstream's does.
     */
    public @com.legend.base.Nullable String beyondItsText() {
        if (target().database() instanceof com.legend.executionplan.ExecutionPlan.Database.System) {
            // the process's catalog, never the runtime's connection (the B checkpoint's tenets audit, LOW 8, 2026-11-11:
            // its text, in the catalog's dialect, was answered under the runtime's connection as if it ran there)
            return "its statement reads the system metamodel store, on the process's catalog, not the runtime's"
                    + " connection";
        }
        if (plan.root() instanceof com.legend.executionplan.ExecutionPlan.Keyed) {
            return "a pivot whose columns the data names reads its keys first, then its body per key";
        }
        for (com.legend.executionplan.ExecutionPlan.Slot slot : statement().slots()) {
            if (slot.binding() instanceof com.legend.executionplan.ExecutionPlan.Binding.Source) {
                return "its statement binds a file: source's path";
            }
            if (slot.binding() instanceof com.legend.executionplan.ExecutionPlan.Binding.Payload) {
                return "its statement binds a data: source's payload";
            }
        }
        // (a target's data: session table, its AT_RELEASE load, went with the JSON pass, 2026-11-11: a payload is its
        // statement's bound parameter, above, or its own rows in the text)
        return null;
    }

    /** Whether the text runs as it is: the plan is its one statement, which binds no value and whose target loads
     *  nothing first (no parameter, no {@code file:} source's path, no test data, no {@code data:} table). */
    public boolean textAlone() {
        return plan.root() instanceof com.legend.executionplan.ExecutionPlan.TdsResult && plan.parameters().isEmpty()
                && statement().slots().isEmpty() && statement().stamp() == null && target().loads().isEmpty();
    }
}
