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
        if (query.writes()) {
            return new ExecutionPlan(declared.stream().map(p -> p.declaration(query.context())).toList(),
                    program(query, runtime, output, slots));
        }
        Compiler.LoweredQuery l = query.lower(runtime, output == TypedQuery.Output.STREAMED_JSON, slots);
        // the runtime decided: a null one is refused there, by name
        com.legend.database.Target decided = Compiler.executesOn(l.ctx(), runtime);
        SqlDialect dialect = Databases.dialect(decided.type());
        ExecutionPlan.Target target = target(decided, java.util.Objects.requireNonNull(runtime), l.ctx(), dialect);
        // an enumeration parameter compared with a mapped column: through that place's value table
        SqlQuery lowered = com.legend.sql.EnumValueTables.apply(l.plan());
        return new ExecutionPlan(declared.stream().map(p -> p.declaration(l.ctx())).toList(),
                result(l.root(), lowered, output, dialect, target));
    }

    /**
     * A program that changes a store, planned as the steps of one {@link ExecutionPlan.Sequence}
     * (docs/WRITES_DESIGN_2026_10_18.md §8, leg 7): each statement at its turn with the lets before it, as the
     * execution's body compiler takes them, every one inlined, resolved and lowered as a query is
     * ({@link TypedQuery#resolve}), each declared parameter a value its statement binds. A staged let
     * ({@code let s = rows->stage()}) creates its temporary table, which the statements after read, and is dropped
     * after the result; another let rides the lets of the statements after; a write into a store table is an update;
     * any other statement before the last a check (its rows dropped: a Cypher write's guard raises). The last
     * statement's value is the plan's answer: a query's text in the form {@code output} asks for, or a write's count,
     * which the database writes as a value bound in a query of its own.
     */
    private static ExecutionPlan.Node program(TypedQuery query, @com.legend.base.Nullable String runtime,
            TypedQuery.Output output, List<com.legend.sql.SqlExpr.PlanParam> slots) {
        com.legend.compiler.element.ModelContext ctx = query.context();
        // the runtime decided: a null one is refused there, by name
        com.legend.database.Target decided = Compiler.executesOn(ctx, runtime);
        SqlDialect dialect = Databases.dialect(decided.type());
        ExecutionPlan.Target target = target(decided, java.util.Objects.requireNonNull(runtime), ctx, dialect);
        boolean streaming = output == TypedQuery.Output.STREAMED_JSON;
        List<com.legend.compiler.spec.typed.TypedSpec> body = query.body();
        com.legend.resolver.StagedRows.refuseLetsTheWritesChange(body, query.specs());
        List<com.legend.compiler.spec.typed.TypedSpec> lets = new ArrayList<>();
        List<ExecutionPlan.Node> steps = new ArrayList<>();
        List<String> staged = new ArrayList<>();
        for (int i = 0; i < body.size(); i++) {
            com.legend.compiler.spec.typed.TypedSpec stmt = body.get(i);
            boolean last = i == body.size() - 1;
            if (stmt instanceof com.legend.compiler.spec.typed.TypedLet let && !last) {
                com.legend.compiler.spec.typed.TypedSpec rows = com.legend.resolver.StagedRows.rowsOf(let);
                if (rows == null) {
                    lets.add(let);
                    continue;
                }
                // STAGED ROWS: computed at this statement's turn into a temporary table of the session, which the
                // statements after read by name (the writes between read the rows as they were)
                String table = com.legend.resolver.StagedRows.table(let.name());
                TypedQuery.Resolved r = query.resolve(with(lets, rows), runtime);
                com.legend.lowering.Lowerer lowerer = query.lowerer(r.temporal(), false, slots);
                SqlQuery q = com.legend.sql.EnumValueTables.apply(lowerer.namedColumns(lowerer.lower(r.body())));
                steps.add(new ExecutionPlan.Update(sql(dialect.renderStatement(com.legend.sql.SqlDml.CreateTempTableAs
                        .staged(table, q, com.legend.resolver.StagedRows.columns(r.root().info()))), target, null),
                        null));
                staged.add(table);
                lets.add(com.legend.resolver.StagedRows.boundTo(let.name(), table, rows.info(),
                        com.legend.resolver.StagedRows.storeOf(r.body(), ctx, runtime), let.info()));
                continue;
            }
            TypedQuery.Resolved r = query.resolve(with(lets, stmt), runtime);
            com.legend.compiler.spec.typed.TypedSpec root = com.legend.compiler.spec.typed.Lets.bare(r.root());
            while (root instanceof com.legend.compiler.spec.typed.TypedFrom f) {
                root = f.source();
            }
            if (com.legend.compiler.spec.StatementEffects.isStoreWrite(root)) {
                List<com.legend.compiler.spec.typed.TypedSpec> write = new ArrayList<>(r.body().subList(0,
                        r.body().size() - 1));
                write.add(root);
                com.legend.sql.SqlDml dml = query.lowerer(r.temporal(), false, slots).lowerStoreWrite(write);
                steps.add(new ExecutionPlan.Update(sql(dialect.renderStatement(dml), target, null),
                        last ? ExecutionPlan.COUNT : null));
                if (last) {
                    steps.add(count(query, root, output, dialect, target));
                }
                continue;
            }
            SqlQuery q = com.legend.sql.EnumValueTables.apply(query.lowerer(r.temporal(), last && streaming, slots)
                    .lower(r.body()));
            steps.add(last ? result(r.root(), q, output, dialect, target)
                    : new ExecutionPlan.Check(sql(dialect.renderStatement(q), target, q)));
        }
        for (String table : dialect.dropsStagedTablesWithTheirTransaction() ? List.<String>of() : staged) {
            // (a failed program's transaction takes its tables back with its writes)
            steps.add(new ExecutionPlan.Update(new ExecutionPlan.Sql(dialect.render(new com.legend.sql.SqlDdl.DropTable(
                    null, table, com.legend.sql.NameQuoting.BARE)), List.of(), target, null), null));
        }
        return new ExecutionPlan.Sequence(steps);
    }

    /** {@code statement} after {@code lets}. */
    private static List<com.legend.compiler.spec.typed.TypedSpec> with(
            List<com.legend.compiler.spec.typed.TypedSpec> lets, com.legend.compiler.spec.typed.TypedSpec statement) {
        List<com.legend.compiler.spec.typed.TypedSpec> out = new ArrayList<>(lets);
        out.add(statement);
        return out;
    }

    /** The answer of a program whose last statement is the write {@code write}: its count, which the step before
     *  binds ({@link ExecutionPlan#COUNT}), as the database writes an {@code Integer[1]} value's text (a value's text
     *  is the one-column relation {@code value}, whole: no row to stream). */
    private static ExecutionPlan.Node count(TypedQuery query, com.legend.compiler.spec.typed.TypedSpec write,
            TypedQuery.Output output, SqlDialect dialect, ExecutionPlan.Target target) {
        ExprType type = new ExprType(write.info().type(), com.legend.compiler.element.type.Multiplicity.Bounded.ONE);
        QueryParameters.Declared count = new QueryParameters.Declared(ExecutionPlan.COUNT, type.type(),
                type.multiplicity());
        SqlQuery q = query.lowerer(false, false, List.of(count.slot())).lowerPlanParam(ExecutionPlan.COUNT, type);
        return switch (output) {
            case CSV -> wire(WireRender.Format.CSV, q, type, dialect, target);
            case JSON, STREAMED_JSON -> wire(WireRender.Format.JSON, q, type, dialect, target);
        };
    }

    /** The plan's answer to {@code root}, lowered to {@code lowered}: the text the database writes in the form
     *  {@code output} asks for. */
    private static ExecutionPlan.Node result(com.legend.compiler.spec.typed.TypedSpec rootSpec, SqlQuery lowered,
            TypedQuery.Output output, SqlDialect dialect, ExecutionPlan.Target target) {
        ExprType root = rootSpec.info();
        return switch (com.legend.plan.ResultShape.of(rootSpec)) {
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
    }

    /** A rendered statement as the plan's, on {@code target}. */
    private static ExecutionPlan.Sql sql(RenderedStatement rendered, ExecutionPlan.Target target,
            @com.legend.base.Nullable SqlQuery tree) {
        List<ExecutionPlan.Slot> slots = rendered.binds().stream()
                .map(b -> new ExecutionPlan.Slot(b.parameter(), binding(b.binding()))).toList();
        return new ExecutionPlan.Sql(rendered.sql(), slots, target, tree);
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
        return new ExecutionPlan.TextResult(format, type, sql(dialect.renderStatement(statement), target, statement));
    }

    /** A placeholder's binding, as the dialect decided it, in the plan's own terms. */
    private static ExecutionPlan.Binding binding(RenderedStatement.Binding b) {
        return switch (b) {
            case RenderedStatement.Binding.One one -> {
                RenderedStatement.TypeHole hole = one.hole();
                yield new ExecutionPlan.Binding.One(one.nullType(), hole == null ? null
                        : new ExecutionPlan.TypeHole(hole.at(), spellings(hole.types()), kind(hole.absent())));
            }
            case RenderedStatement.Binding.Array array -> new ExecutionPlan.Binding.Array(array.elementSqlType());
        };
    }

    private static java.util.Map<ExecutionPlan.ValueKind, ExecutionPlan.TypeSpelling> spellings(
            java.util.Map<com.legend.sql.ValueKind, RenderedStatement.TypeSpelling> types) {
        java.util.Map<ExecutionPlan.ValueKind, ExecutionPlan.TypeSpelling> out =
                new java.util.EnumMap<>(ExecutionPlan.ValueKind.class);
        types.forEach((k, t) -> out.put(kind(k), new ExecutionPlan.TypeSpelling(t.name(), switch (t.digits()) {
            case NONE -> ExecutionPlan.Digits.NONE;
            case PRECISION -> ExecutionPlan.Digits.PRECISION;
            case PRECISION_AND_SCALE -> ExecutionPlan.Digits.PRECISION_AND_SCALE;
        }, t.fraction())));
        return out;
    }

    private static ExecutionPlan.ValueKind kind(com.legend.sql.ValueKind k) {
        return switch (k) {
            case INTEGER -> ExecutionPlan.ValueKind.INTEGER;
            case DECIMAL -> ExecutionPlan.ValueKind.DECIMAL;
            case FLOATING -> ExecutionPlan.ValueKind.FLOATING;
            case DATE -> ExecutionPlan.ValueKind.DATE;
            case DATE_TIME -> ExecutionPlan.ValueKind.DATE_TIME;
            case DATE_TIME_NANOS -> ExecutionPlan.ValueKind.DATE_TIME_NANOS;
        };
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
            com.legend.compiler.element.ModelContext ctx, SqlDialect dialect) {
        ExecutionPlan.Database database = switch (decided) {
            case com.legend.database.Target.Platform p -> new ExecutionPlan.Database.Platform(p.type());
            case com.legend.database.Target.Declared d -> new ExecutionPlan.Database.Declared(oneDefinition(d));
        };
        List<String> versions = dialect.serverVersions();
        return new ExecutionPlan.Target(database, versions == null ? new ExecutionPlan.Servers.Every()
                : new ExecutionPlan.Servers.Versions(versions), dialect.sessionSetup(),
                setup(CsvSeed.declaredSteps(runtime, ctx, dialect), dialect));
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
     * database's bulk loader ({@link SqlDialect#loadsRowsInBulk}) with the statements that stage them, or as the one
     * INSERT the database runs. A table of no rows is no step.
     */
    private static List<ExecutionPlan.SetupStep> setup(List<CsvSeed.Step> steps, SqlDialect dialect) {
        boolean bulk = dialect.loadsRowsInBulk();
        List<ExecutionPlan.SetupStep> out = new ArrayList<>(steps.size());
        for (CsvSeed.Step step : steps) {
            switch (step) {
                case CsvSeed.Step.Sql blob -> {
                    for (String statement : com.legend.sql.RawSql.splitStatements(blob.text())) {
                        out.add(new ExecutionPlan.SetupStep.Statement(
                                new CsvSeed.Step.Sql(statement, blob.writtenFor()).runOn(dialect)));
                    }
                }
                case CsvSeed.Step.Rows rows -> {
                    RowLoad load = rows.load();
                    if (!load.rows().isEmpty()) {
                        out.add(bulk ? load.staged(dialect)
                                : new ExecutionPlan.SetupStep.Statement(dialect.render(load.values())));
                    }
                }
            }
        }
        return out;
    }
}
