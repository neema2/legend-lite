// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.planner;

import com.legend.Compiler;
import com.legend.TypedQuery;
import com.legend.compiler.element.ModelContext;
import com.legend.executionplan.ExecutionPlan;
import com.legend.sql.dialect.SqlDialect;

import java.util.ArrayList;
import java.util.List;

/**
 * A TABLE OUTPUT, AS ONE PLAN (the final audit's item 2, 2026-11-12, the user's ruling (b); B3's contract): a DataCube
 * box's rows written into a table of its host's own database, made before it runs -- where the Output makes the table
 * ({@link Mode#CREATE}, {@link Mode#OVERWRITE}), the planned {@code CREATE TABLE ... AS <the rows' statement>}, the
 * database typing its columns as its own create of the rows always did (the orchestrator's ruling (e), 2026-11-12), its
 * answer the table's rows counted; where the table is there, the write the planner plans over one model of the box's
 * tables and the target ({@code write}, {@code delete} then {@code write}, {@code merge} by its keys: the store writes)
 * -- one writing {@code Sequence} on the target's database,
 * its answer the write's count. Where the Output makes its table, the create carries the data
 * ({@link #make}): no column is declared. Until then the host made the table with that create as a host operation's
 * query, read its catalog, and asked for the write's plan apart.
 */
public final class OutputPlan {

    private OutputPlan() {
    }

    /** How an Output writes its table. */
    public enum Mode {
        /** A new table, made for the rows: the database refuses one of its name. */
        CREATE("create"),
        /** The table made anew for the rows, an old one of its name dropped first. */
        OVERWRITE("overwrite"),
        /** The rows added to the table there. */
        APPEND("append"),
        /** Every row of the table there deleted, then the rows written: one change. */
        DELETE_APPEND("deleteAppend"),
        /** The rows merged into the table there by their keys. */
        MERGE("merge");

        private final String spelled;

        Mode(String spelled) {
            this.spelled = spelled;
        }

        /** The mode a request spells, refused by name where it spells none. */
        public static Mode named(String spelled) {
            for (Mode m : values()) {
                if (m.spelled.equals(spelled)) {
                    return m;
                }
            }
            throw new IllegalArgumentException("an output's mode is create, overwrite, append, deleteAppend or merge,"
                    + " not " + spelled);
        }

        /** Whether the Output makes its table. */
        public boolean makes() {
            return this == CREATE || this == OVERWRITE;
        }

        /** As a request spells it. */
        public String spelled() {
            return spelled;
        }
    }

    /**
     * An Output that MAKES its table ({@link Mode#CREATE}, {@link Mode#OVERWRITE}), as one plan over {@code ctx} (the
     * box's tables): {@code rows} (a lambda's protocol JSON: the box's one query) planned as the product plans its one
     * statement, its target the plan's; then {@code CREATE TABLE <table> AS <the rows' statement>}, written from the
     * rows' own tree ({@link PlanMaker#copyInto}), its slots and content the rows' -- the create carries the data, and
     * the DATABASE types the columns, as its own create of the rows always did (the orchestrator's ruling (e) and
     * proposal, 2026-11-12: no column is declared or mapped, so none is refused or narrowed); the old one of its name
     * dropped first where the Output overwrites; its answer the made table's rows counted ({@link PlanMaker#countOf}).
     */
    public static ExecutionPlan make(ModelContext ctx, String runtime, String rows, Mode mode, String table,
            SqlDialect dialect) {
        if (!mode.makes()) {
            throw new IllegalStateException("an output that writes a table there is planned as its write: " + mode);
        }
        try (var planning = com.legend.base.RunScope.plans("a table output")) {
            TypedQuery query = Compiler.query(ctx, com.legend.protocol.ProtocolReader.lambda(rows));
            ExecutionPlan read = query.executionPlan(runtime);
            if (!(read.root() instanceof ExecutionPlan.TdsResult answer)) {
                throw new com.legend.error.NotImplementedException("the rows written into " + table + " are not one"
                        + " statement: " + read.root().getClass().getSimpleName());
            }
            ExecutionPlan.Target target = answer.sql().target();
            List<ExecutionPlan.Node> steps = new ArrayList<>();
            if (mode == Mode.OVERWRITE) {
                // a host's own table: its name delimited, as the making names it
                steps.add(new ExecutionPlan.Effect(ExecutionPlan.Sql.of(dialect.render(new com.legend.sql.SqlDdl
                        .DropTable(null, table, new com.legend.sql.NameQuoting(false, true))), target)));
            }
            steps.add(new ExecutionPlan.Effect(PlanMaker.copyInto(query, runtime, dialect, target, null, table)));
            steps.add(PlanMaker.countOf(table, dialect, target));
            return ExecutionPlan.writing(read.parameters(), new ExecutionPlan.Sequence(steps));
        }
    }

    /**
     * An Output that writes a table there ({@link Mode#APPEND}, {@link Mode#DELETE_APPEND}, {@link Mode#MERGE}), as one
     * plan over {@code ctx} (the box's tables and the target, read off its catalog before the run): {@code program} (a
     * lambda's protocol JSON: the box's rows written into the target as {@code mode} writes -- {@code write}, {@code
     * delete} then {@code write}, or {@code merge} by its keys -- composed by the hosts' boundary) planned through
     * {@code runtime}, the store writes' plan; its answer the write's count.
     */
    public static ExecutionPlan write(ModelContext ctx, String runtime, String program, Mode mode) {
        if (mode.makes()) {
            throw new IllegalStateException("an output that makes its table is planned as its making: " + mode);
        }
        try (var planning = com.legend.base.RunScope.plans("a table output")) {
            ExecutionPlan written = Compiler.query(ctx, com.legend.protocol.ProtocolReader.lambda(program))
                    .executionPlan(runtime);
            List<ExecutionPlan.Node> steps = written.root() instanceof ExecutionPlan.Sequence s ? s.steps()
                    : List.of(written.root());
            return ExecutionPlan.writing(written.parameters(), new ExecutionPlan.Sequence(steps));
        }
    }
}
