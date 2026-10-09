// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend;

import com.legend.compiler.element.type.ExprType;
import com.legend.compiler.element.type.Type;
import com.legend.database.Databases;
import com.legend.executionplan.ExecutionPlan;
import com.legend.lowering.WireRender;
import com.legend.model.ConnectionDefinition;
import com.legend.setup.CsvSeed;
import com.legend.setup.RowLoad;
import com.legend.sql.SqlQuery;
import com.legend.sql.dialect.RenderedStatement;
import com.legend.sql.dialect.SqlDialect;

import java.util.ArrayList;
import java.util.List;

/**
 * THE PLANNER MAKES LITE PLANS (docs/EXECUTION_PLAN_BOUNDARY_2026_10_05.md §9, step 2's landing 2): a typed query planned
 * for its runtime as an {@link ExecutionPlan} that a model-free runner executes. Its one node is a text the DATABASE
 * writes — the wire statement ({@link WireRender}), or a graph fetch's JSON — so the runner passes the text on. Every
 * statement is final for its target: written in the dialect of the target's database, with the server versions that
 * spelling is for; and the target is whole — the database (a declared connection, or the platform's own engine), the
 * statements each connection runs first, and the setup that establishes it, written at plan time. The statements are
 * the ones {@link Execution}'s wire and streaming paths render for the same query, so a plan answers as they do.
 */
final class PlanMaker {

    private PlanMaker() {
    }

    /** {@code query}'s plan on {@code runtime}, its text in the form {@code output} asks for; each declared parameter
     *  a value the statement binds where it is used. */
    static ExecutionPlan plan(TypedQuery query, @com.legend.base.Nullable String runtime, TypedQuery.Output output) {
        List<QueryParameters.Declared> declared = query.parameters();
        List<com.legend.sql.SqlExpr.PlanParam> slots = new ArrayList<>(declared.size());
        for (QueryParameters.Declared p : declared) {
            // one value, an optional one's absence, or a list as one array (Declared.slot)
            slots.add(p.slot());
        }
        Compiler.LoweredQuery l = query.lower(runtime, output == TypedQuery.Output.STREAMED_JSON, slots);
        // the runtime decided: a null one is refused there, by name
        com.legend.database.Target decided = Compiler.executesOn(l.ctx(), runtime);
        SqlDialect dialect = Databases.dialect(decided.type());
        ExecutionPlan.Target target = target(decided, java.util.Objects.requireNonNull(runtime), l, dialect);
        ExprType root = l.root().info();
        // an enumeration parameter compared with a mapped column: through that place's value table
        SqlQuery lowered = com.legend.sql.EnumValueTables.apply(l.plan());
        ExecutionPlan.Node node = switch (com.legend.plan.ResultShape.of(l.root())) {
            case GRAPH -> switch (output) {
                case CSV -> throw new com.legend.error.NotImplementedException("graph results have no CSV wire");
                case JSON -> text(ExecutionPlan.Format.JSON, objects(root), lowered, dialect, target);
                case STREAMED_JSON -> text(ExecutionPlan.Format.JSON_PER_ROW, objects(root), lowered, dialect, target);
            };
            case TABULAR -> switch (output) {
                case CSV -> wire(WireRender.Format.CSV, lowered, root, dialect, target);
                case JSON -> wire(WireRender.Format.JSON, lowered, root, dialect, target);
                case STREAMED_JSON -> text(ExecutionPlan.Format.JSON_PER_ROW, relation(WireRender.schema(root)),
                        WireRender.rows(lowered), dialect, target);
            };
            // a value's text is the one-column relation `value`, whole: no row to stream
            case SCALAR, COLLECTION -> switch (output) {
                case CSV -> wire(WireRender.Format.CSV, lowered, root, dialect, target);
                case JSON, STREAMED_JSON -> wire(WireRender.Format.JSON, lowered, root, dialect, target);
            };
        };
        return new ExecutionPlan(declared.stream().map(p -> p.declaration(l.ctx())).toList(), node);
    }

    /** The whole result as one text of {@code format}: the lowered query, of {@code root}'s type, wrapped by the wire. */
    private static ExecutionPlan.TextResult wire(WireRender.Format format, SqlQuery lowered, ExprType root,
            SqlDialect dialect, ExecutionPlan.Target target) {
        Type.RelationType schema = WireRender.schema(root);
        ExecutionPlan.Format text = switch (format) {
            case CSV -> ExecutionPlan.Format.CSV;
            case JSON -> ExecutionPlan.Format.JSON;
        };
        return text(text, relation(schema), WireRender.wrap(lowered, schema, format), dialect, target);
    }

    private static ExecutionPlan.TextResult text(ExecutionPlan.Format format, ExecutionPlan.ResultType type,
            SqlQuery statement, SqlDialect dialect, ExecutionPlan.Target target) {
        RenderedStatement rendered = dialect.renderStatement(statement);
        List<ExecutionPlan.Slot> slots = rendered.binds().stream()
                .map(b -> new ExecutionPlan.Slot(b.parameter(), b.arrayElementSqlType())).toList();
        return new ExecutionPlan.TextResult(format, type, new ExecutionPlan.Sql(rendered.sql(), slots, target,
                statement));
    }

    private static ExecutionPlan.Relation relation(Type.RelationType schema) {
        return new ExecutionPlan.Relation(schema.columns().stream()
                .map(c -> new ExecutionPlan.Column(c.name(), c.type().typeName())).toList());
    }

    private static ExecutionPlan.Value objects(ExprType root) {
        var m = root.multiplicity().requireBounded("a graph fetch's result");
        return new ExecutionPlan.Value(root.type().typeName(), new ExecutionPlan.Multiplicity(m.lower(), m.upper()));
    }

    /** Where the plan runs: the database {@code decided}, the server versions and session statements of its dialect,
     *  and the setup its runtime's connections declare, every statement final for that database. */
    private static ExecutionPlan.Target target(com.legend.database.Target decided, String runtime,
            Compiler.LoweredQuery l, SqlDialect dialect) {
        ExecutionPlan.Database database = switch (decided) {
            case com.legend.database.Target.Platform p -> new ExecutionPlan.Database.Platform(p.type());
            case com.legend.database.Target.Declared d -> new ExecutionPlan.Database.Declared(oneDefinition(d));
        };
        return new ExecutionPlan.Target(database, Databases.servers(decided.type()), dialect.sessionSetup(),
                setup(CsvSeed.declaredSteps(runtime, l.ctx(), dialect), decided.type(), dialect));
    }

    /** The one definition a declared target opens: its connections may differ in name, not in database —
     *  a query runs on one session. */
    private static ConnectionDefinition oneDefinition(com.legend.database.Target.Declared d) {
        ConnectionDefinition first = d.connections().get(0);
        for (ConnectionDefinition c : d.connections()) {
            if (c.databaseType() != first.databaseType() || !c.specification().equals(first.specification())
                    || !c.authentication().equals(first.authentication())) {
                throw new com.legend.error.NotImplementedException("the runtime binds "
                        + d.connections().stream().map(ConnectionDefinition::qualifiedName).toList()
                        + ", different connections: a query runs on one session");
            }
        }
        return first;
    }

    /**
     * A target's setup as the plan's steps, final for {@code type}: each statement of a connection's SQL (written in
     * H2's spelling, as legend-engine's test data is) adapted to the database, and each table's rows either for the
     * database's bulk loader ({@link Databases#loadsRowsInBulk}) with the statements that stage them, or as the one
     * INSERT the database runs. A table of no rows is no step.
     */
    private static List<ExecutionPlan.SetupStep> setup(List<CsvSeed.Step> steps, ConnectionDefinition.DatabaseType type,
            SqlDialect dialect) {
        boolean bulk = Databases.loadsRowsInBulk(type);
        List<ExecutionPlan.SetupStep> out = new ArrayList<>(steps.size());
        for (CsvSeed.Step step : steps) {
            switch (step) {
                case CsvSeed.Step.Sql blob -> {
                    for (String statement : com.legend.sql.RawSql.splitStatements(blob.text())) {
                        out.add(new ExecutionPlan.SetupStep.Statement(CsvSeed.adaptRaw(statement, dialect)));
                    }
                }
                case CsvSeed.Step.Rows rows -> {
                    RowLoad load = rows.load();
                    if (!load.rows().isEmpty()) {
                        out.add(bulk ? bulkRows(load, dialect)
                                : new ExecutionPlan.SetupStep.Statement(dialect.render(load.values())));
                    }
                }
            }
        }
        return out;
    }

    private static ExecutionPlan.SetupStep bulkRows(RowLoad load, SqlDialect dialect) {
        RowLoad.Staging staging = load.staging(dialect);
        return new ExecutionPlan.SetupStep.Rows(staging.table(), staging.create(), staging.copy(), staging.drop(),
                load.rows());
    }
}
