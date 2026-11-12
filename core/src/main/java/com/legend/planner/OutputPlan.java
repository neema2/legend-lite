// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.planner;

import com.legend.Compiler;
import com.legend.TypedQuery;
import com.legend.compiler.element.ModelContext;
import com.legend.compiler.element.type.Type;
import com.legend.executionplan.ExecutionPlan;
import com.legend.plan.UpstreamRelationType;
import com.legend.sql.dialect.CatalogModel;
import com.legend.sql.dialect.SqlDialect;

import java.util.ArrayList;
import java.util.List;

/**
 * A TABLE OUTPUT, AS ONE PLAN (the final audit's item 2, 2026-11-12, the user's ruling (b); B3's contract): a DataCube
 * box's rows written into a table of its host's own database, made before it runs -- where the Output makes the table
 * ({@link Mode#CREATE}, {@link Mode#OVERWRITE}), its DDL from its rows' relation type, each column's Pure type mapped by
 * the database ({@link SqlDialect#createdColumn}) and the table declared in the model as any of its tables; then the
 * write the planner plans over one model of the box's tables and the target ({@code write}, {@code delete} then
 * {@code write}, {@code merge} by its keys: the store writes) -- one writing {@code Sequence} on the target's database,
 * its answer the write's count. Until then the host made the table with the database's own CREATE of
 * {@code rows LIMIT 0} -- a query handed to a host operation -- read its catalog, and asked for the write's plan apart.
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

    /** A created table's columns, as its database's catalog would answer them: {@code rows}' relation type, each
     *  column's Pure type mapped by {@code dialect} (a Decimal with its precision and scale). */
    public static List<CatalogModel.Column> createdColumns(TypedQuery rows, SqlDialect dialect) {
        List<CatalogModel.Column> out = new ArrayList<>();
        for (Type.Column c : UpstreamRelationType.columns(rows.resultType())) {
            Integer precision = c.type() instanceof Type.PrecisionDecimal d ? d.precision() : null;
            Integer scale = c.type() instanceof Type.PrecisionDecimal d ? d.scale() : null;
            out.add(dialect.createdColumn(c.name(), UpstreamRelationType.typePath(c.type()), precision, scale));
        }
        return out;
    }

    /**
     * The Output as one plan over {@code ctx} (the box's tables and the target, {@code database}'s {@code table}):
     * {@code program} (a lambda's protocol JSON: the box's rows written into the target as {@code mode} writes --
     * {@code write}, {@code delete} then {@code write}, or {@code merge} by its keys -- composed by the hosts' boundary)
     * planned through {@code runtime}; where the Output makes the table, its
     * DDL (the declared table's columns and types, every name delimited, written by the dialect) before the write, on the
     * write's own target.
     */
    public static ExecutionPlan plan(ModelContext ctx, String runtime, String program, Mode mode, String database,
            String table, SqlDialect dialect) {
        try (var planning = com.legend.base.RunScope.plans("a table output")) {
            ExecutionPlan written = Compiler.query(ctx, com.legend.protocol.ProtocolReader.lambda(program))
                    .executionPlan(runtime);
            List<ExecutionPlan.Node> steps = new ArrayList<>();
            List<ExecutionPlan.Node> writes = written.root() instanceof ExecutionPlan.Sequence s ? s.steps()
                    : List.of(written.root());
            if (mode.makes()) {
                com.legend.model.DatabaseDefinition.TableDefinition def = ctx.findTableDefinition(database, table)
                        .orElseThrow(() -> new IllegalStateException("the output's table " + table + " is not "
                                + database + "'s, which its model declares it in"));
                ExecutionPlan.Target target = firstTarget(writes);
                // a host's own table: every name delimited, as the rows name it (a seed's bare name is spelled as its
                // database folds one, which would name the made table's columns otherwise than the write reads them)
                com.legend.sql.NameQuoting delimited = new com.legend.sql.NameQuoting(false, true);
                if (mode == Mode.OVERWRITE) {
                    steps.add(new ExecutionPlan.Effect(ExecutionPlan.Sql.of(dialect.render(
                            new com.legend.sql.SqlDdl.DropTable(null, def.name(), delimited)), target)));
                }
                List<com.legend.sql.SqlDdl.Column> columns = new ArrayList<>();
                for (com.legend.model.DatabaseDefinition.ColumnDefinition col : def.columns()) {
                    columns.add(new com.legend.sql.SqlDdl.Column(col.name(), true,
                            com.legend.compiler.element.StoreCompiler.declaredType(col.dataType()), false, false));
                }
                steps.add(new ExecutionPlan.Effect(ExecutionPlan.Sql.of(dialect.render(
                        new com.legend.sql.SqlDdl.CreateTable(null, def.name(), delimited, columns)), target)));
            }
            steps.addAll(writes);
            return ExecutionPlan.writing(written.parameters(), new ExecutionPlan.Sequence(steps));
        }
    }

    /** The target of the first statement of {@code steps}: the write's, where its table is made. */
    private static ExecutionPlan.Target firstTarget(List<ExecutionPlan.Node> steps) {
        for (ExecutionPlan.Node step : steps) {
            ExecutionPlan.Sql sql = switch (step) {
                case ExecutionPlan.Update u -> u.sql();
                case ExecutionPlan.Effect e -> e.sql();
                case ExecutionPlan.TdsResult t -> t.sql();
                case ExecutionPlan.Check c -> c.sql();
                default -> null;
            };
            if (sql != null) {
                return sql.target();
            }
        }
        throw new IllegalStateException("an output's write sends no statement");
    }
}
