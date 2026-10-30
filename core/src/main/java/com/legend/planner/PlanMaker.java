// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.planner;

import com.legend.Compiler;
import com.legend.TypedQuery;

import com.legend.compiler.element.type.ExprType;
import com.legend.compiler.element.type.Type;
import com.legend.database.Databases;
import com.legend.executionplan.ExecutionPlan;
import com.legend.lowering.TypedRows;
import com.legend.model.ConnectionDefinition;
import com.legend.setup.CsvSeed;
import com.legend.sql.SqlQuery;
import com.legend.sql.dialect.RenderedStatement;
import com.legend.sql.dialect.SqlDialect;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * THE PLANNER MAKES LITE PLANS (docs/EXECUTION_PLAN_BOUNDARY_2026_10_05.md §9, step 2's landing 2): a typed query planned
 * for its runtime as an {@link ExecutionPlan} that a model-free runner executes. Its answer is TYPED ROWS
 * (docs/RESULT_BOUNDARY_PROGRAM_2026_10_26.md, the end state of 2026-10-27: a relation's rows, a value's elements, a
 * graph fetch's objects), read by the plan's column facts and spelled by a writer at the edge: no plan asks its
 * database to write a text (the last, Pure's TDS text, went with the program's phase 4). Every statement is final
 * for its target: written in the dialect of the target's database, with the server versions that spelling is for; and
 * the target is whole — the database (a declared connection, or the platform's own engine), the statements each
 * connection runs first, and the setup that establishes it, written at plan time.
 *
 * <p>Public since step C (D73, 2026-10-30): the planner's machinery is its own package, so the root's door (TypedQuery.plan) and the driver (the body driver's and the judges' plans) reach it across a package line; the product API is the door.
 */
public final class PlanMaker {

    private PlanMaker() {
    }

    /** {@code query}'s TYPED plan on {@code runtime}, lowered under the feature flags {@code features}, each declared
     *  parameter a value the statement binds where it is used, for a session whose server is
     *  {@code serverVersion} (the caller's session fact, as the legacy seam read it off the connection,
     *  {@code SqlDialect.forServer}): where the database's spelling depends on the version (H2: 2.1 and 2.2 the
     *  engine-parity dialect, 2.3 and later the modern one), the plan is written in that version's dialect and names
     *  that version as what it is for, so its runner accepts the session; null plans for the dialect's declared
     *  target. The plan path program's step 9, 2026-10-19: the H2 PCT lane runs the modern H2, and a plan for the
     *  declared target was refused by its runner on every case. */
    public static ExecutionPlan plan(TypedQuery query, @com.legend.base.Nullable String runtime,
            java.util.Set<com.legend.platform.Feature> features, @com.legend.base.Nullable String serverVersion) {
        List<QueryParameters.Declared> declared = query.parameters();
        List<com.legend.sql.SqlExpr.PlanParam> slots = new ArrayList<>(declared.size());
        for (QueryParameters.Declared p : declared) {
            // one value, an optional one's absence, or a list as one array (Declared.slot)
            slots.add(p.slot());
        }
        if (query.writes()) {
            // a program that changes a store: main's write plans (docs/WRITES_DESIGN_2026_10_18.md §8, leg 7; merged
            // 2026-10-26), a sequence of steps on one session
            return new ExecutionPlan(declared.stream().map(p -> p.declaration(query.context())).toList(),
                    program(query, runtime, slots, features, serverVersion));
        }
        // a typed plan answers a graph fetch's objects one per row: the streaming graph root
        Compiler.LoweredQuery l = query.lower(runtime, true, slots, features);
        // the runtime decided: a null one is refused there, by name
        com.legend.database.Target decided = Compiler.executesOn(l.ctx(), runtime);
        SqlDialect dialect = Databases.dialect(decided.type());
        ExecutionPlan.Servers servers = serversFor(decided.type(), serverVersion);
        if (serverVersion != null && servers instanceof ExecutionPlan.Servers.Versions) {
            dialect = dialect.forServer(serverVersion);
        }
        Metamodel metamodel = metamodel(l, dialect);
        ExecutionPlan.Target target = target(decided, java.util.Objects.requireNonNull(runtime), l.ctx(), dialect, servers,
                metamodel);
        ExprType root = l.root().info();
        // an enumeration parameter compared with a mapped column: through that place's value table; a system
        // metamodel table on a server read by its session name (Metamodel.renames)
        SqlQuery lowered = metamodel.read(com.legend.sql.EnumValueTables.apply(l.plan()));
        com.legend.sql.SqlSource.Pivot dynamic = com.legend.lowering.KeyedPivot.dynamic(lowered);
        // a relation root's schema with any row-struct column flattened (TypedRows.flat; its dynamic columns kept)
        Type.RelationType rowSchema = Type.schemaView(root.type()) == null ? null : TypedRows.schema(root);
        if (dynamic != null && rowSchema != null && !rowSchema.dynamicColumns().isEmpty()
                && com.legend.plan.ResultShape.of(l.root()) == com.legend.plan.ResultShape.TABULAR) {
            // a pivot no cast names: its columns are the data's, so its plan reads its keys first (the probe plan,
            // 2026-10-20, KeyedPivot); every database
            return new ExecutionPlan(declared.stream().map(p -> p.declaration(l.ctx())).toList(),
                    keyed(lowered, dynamic, rowSchema, dialect, target));
        }
        return new ExecutionPlan(declared.stream().map(p -> p.declaration(l.ctx())).toList(),
                typed(l.root(), lowered, dialect, target));
    }

    /**
     * A program that changes a store, planned as the steps of one {@link ExecutionPlan.Sequence}
     * (docs/WRITES_DESIGN_2026_10_18.md §8, leg 7): each statement at its turn with the lets before it, as the
     * execution's body compiler takes them, every one inlined, resolved and lowered as a query is
     * ({@link TypedQuery#resolve}), each declared parameter a value its statement binds. A staged let
     * ({@code let s = rows->stage()}) creates its temporary table, which the statements after read, and is dropped
     * after the result; another let rides the lets of the statements after; a write into a store table is an update;
     * any other statement before the last a check (its rows dropped: a Cypher write's guard raises). The last
     * statement's value is the plan's answer: a query's typed rows, or a write's count, which the database answers as a
     * value bound in a query of its own.
     */
    private static ExecutionPlan.Node program(TypedQuery query, @com.legend.base.Nullable String runtime,
            List<com.legend.sql.SqlExpr.PlanParam> slots,
            java.util.Set<com.legend.platform.Feature> features, @com.legend.base.Nullable String serverVersion) {
        com.legend.compiler.element.ModelContext ctx = query.context();
        // the runtime decided: a null one is refused there, by name
        com.legend.database.Target decided = Compiler.executesOn(ctx, runtime);
        SqlDialect dialect = Databases.dialect(decided.type());
        ExecutionPlan.Servers servers = serversFor(decided.type(), serverVersion);
        if (serverVersion != null && servers instanceof ExecutionPlan.Servers.Versions) {
            dialect = dialect.forServer(serverVersion);
        }
        // a program reads no system metamodel table (Class.all() inside a write program is not designed)
        ExecutionPlan.Target target = target(decided, java.util.Objects.requireNonNull(runtime), ctx, dialect, servers,
                Metamodel.NONE);
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
                com.legend.lowering.Lowerer lowerer = query.lowerer(r.temporal(), false, slots, features);
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
                com.legend.sql.SqlDml dml = query.lowerer(r.temporal(), false, slots, features).lowerStoreWrite(write);
                steps.add(new ExecutionPlan.Update(sql(dialect.renderStatement(dml), target, null),
                        last ? ExecutionPlan.COUNT : null));
                if (last) {
                    steps.add(count(query, root, dialect, target, features));
                }
                continue;
            }
            SqlQuery q = com.legend.sql.EnumValueTables.apply(query.lowerer(r.temporal(), last, slots, features)
                    .lower(r.body()));
            steps.add(last ? typed(r.root(), q, dialect, target)
                    : new ExecutionPlan.Check(sql(dialect.renderStatement(q), target, q)));
        }
        for (String table : dialect.dropsStagedTablesWithTheirTransaction() ? List.<String>of() : staged) {
            // (a failed program's transaction takes its tables back with its writes)
            steps.add(new ExecutionPlan.Update(new ExecutionPlan.Sql(dialect.render(new com.legend.sql.SqlDdl.DropTable(
                    null, table, com.legend.sql.NameQuoting.BARE)), List.of(), target, null, List.of()), null));
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
     *  binds ({@link ExecutionPlan#COUNT}), as an {@code Integer[1]} value: one row, one column {@code value}. */
    private static ExecutionPlan.Node count(TypedQuery query, com.legend.compiler.spec.typed.TypedSpec write,
            SqlDialect dialect, ExecutionPlan.Target target, java.util.Set<com.legend.platform.Feature> features) {
        ExprType type = new ExprType(write.info().type(), com.legend.compiler.element.type.Multiplicity.Bounded.ONE);
        QueryParameters.Declared count = new QueryParameters.Declared(ExecutionPlan.COUNT, type.type(),
                type.multiplicity());
        SqlQuery q = query.lowerer(false, false, List.of(count.slot()), features).lowerPlanParam(ExecutionPlan.COUNT, type);
        return new ExecutionPlan.TdsResult(ExecutionPlan.TdsResult.Shape.VALUE,
                com.legend.plan.PlanColumns.of(TypedRows.schema(type), q, ExecutionPlan.TdsResult.Shape.VALUE),
                sql(q, dialect, target));
    }

    /**
     * The plan's TYPED answer to {@code rootSpec}, lowered to {@code lowered} (the result boundary program's step 1,
     * docs/RESULT_BOUNDARY_PROGRAM_2026_10_26.md §5): the statement as the query's own, its rows read by the session
     * from the driver by the plan's columns and their three facts, no output text in it. A relation answers its rows;
     * a value its elements in the one column {@code value}; a graph fetch its objects, one per row, each the object's
     * JSON as the database built it (the streaming graph root). A sort key that is no output rides as a hidden column
     * ({@link TypedRows#statement}).
     */
    private static ExecutionPlan.TdsResult typed(com.legend.compiler.spec.typed.TypedSpec rootSpec, SqlQuery lowered,
            SqlDialect dialect, ExecutionPlan.Target target) {
        ExprType root = rootSpec.info();
        return switch (com.legend.plan.ResultShape.of(rootSpec)) {
            case GRAPH -> {
                List<com.legend.sql.OutputCol> outs = lowered.outputs();
                if (outs.size() != 1) {
                    throw new IllegalStateException("a graph fetch's statement answers " + outs.size() + " columns "
                            + outs.stream().map(com.legend.sql.OutputCol::name).toList()
                            + ": the streaming graph root answers one object per row");
                }
                yield new ExecutionPlan.TdsResult(ExecutionPlan.TdsResult.Shape.OBJECTS, List.of(
                        new ExecutionPlan.Column(outs.get(0).name(), root.type().typeName(),
                                com.legend.plan.PlanColumns.multiplicity(root.multiplicity()),
                                com.legend.executionplan.ColumnFacts.slot(outs.get(0).type()), null,
                                com.legend.executionplan.ColumnFacts.precision(root.type().typeName(),
                                        outs.get(0).type(), ExecutionPlan.TdsResult.Shape.OBJECTS))),
                        sql(lowered, dialect, target));
            }
            case TABULAR -> new ExecutionPlan.TdsResult(ExecutionPlan.TdsResult.Shape.RELATION,
                    com.legend.plan.PlanColumns.of(TypedRows.schema(root), lowered,
                            ExecutionPlan.TdsResult.Shape.RELATION),
                    sql(TypedRows.statement(lowered), dialect, target));
            case SCALAR, COLLECTION -> new ExecutionPlan.TdsResult(ExecutionPlan.TdsResult.Shape.VALUE,
                    com.legend.plan.PlanColumns.of(TypedRows.schema(root), lowered,
                            ExecutionPlan.TdsResult.Shape.VALUE),
                    sql(TypedRows.statement(lowered), dialect, target));
        };
    }

    /** A rendered statement as the plan's, on {@code target}. */
    private static ExecutionPlan.Sql sql(RenderedStatement rendered, ExecutionPlan.Target target,
            @com.legend.base.Nullable SqlQuery tree) {
        List<ExecutionPlan.Slot> slots = rendered.binds().stream()
                .map(b -> new ExecutionPlan.Slot(b.parameter(), binding(b.binding()))).toList();
        List<ExecutionPlan.Region> regions = rendered.regions().stream()
                .map(r -> new ExecutionPlan.Region(r.start(), r.end(), r.lead(), r.separator(), r.empty(),
                        r.firstBind(), r.bindCount(), r.counters())).toList();
        return new ExecutionPlan.Sql(rendered.sql(), slots, target, tree, regions);
    }

    /**
     * A dynamic pivot's probe plan ({@link com.legend.lowering.KeyedPivot}, 2026-10-20): the keys probe, the body with
     * its per-key regions, the outputs with their three facts (a fixed column's from the typed schema and the lowered
     * query, a per-key one's from its aggregate's template and slot), the database's snapshot and column limit; its
     * answer typed rows.
     */
    public static ExecutionPlan.Keyed keyed(SqlQuery lowered, com.legend.sql.SqlSource.Pivot pivot, Type.RelationType schema,
            SqlDialect dialect, ExecutionPlan.Target target) {
        // the database's snapshot first: a database with no probe plan (MongoDB) refuses by its own name before any
        // statement is written
        SqlDialect.KeysSnapshot keysSnapshot = dialect.keysSnapshot();
        com.legend.lowering.KeyedPivot.Parts parts = com.legend.lowering.KeyedPivot.of(lowered, pivot, schema);
        java.util.Map<String, ExecutionPlan.Column> facts = new java.util.HashMap<>();
        for (ExecutionPlan.Column c : com.legend.plan.PlanColumns.of(schema, lowered,
                ExecutionPlan.TdsResult.Shape.RELATION)) {
            facts.put(c.name(), c);
        }
        // each statement's cells by the names it writes them under (the body's outputs in the outputs' order)
        List<com.legend.sql.OutputCol> bodyFields = parts.body().outputs();
        if (bodyFields.size() != parts.outputs().size()) {
            throw new IllegalStateException("a keyed body answers " + bodyFields.size() + " columns for "
                    + parts.outputs().size() + " outputs");
        }
        List<ExecutionPlan.Keyed.Output> outputs = new ArrayList<>();
        for (int i = 0; i < parts.outputs().size(); i++) {
            String field = bodyFields.get(i).name();
            outputs.add(switch (parts.outputs().get(i)) {
                case com.legend.lowering.KeyedPivot.Fixed f -> new ExecutionPlan.Keyed.Fixed(java.util.Objects
                        .requireNonNull(facts.get(f.column().name()), () -> "the fixed column '" + f.column().name()
                                + "' has no facts in the typed schema " + facts.keySet()), field);
                case com.legend.lowering.KeyedPivot.PerKey k -> new ExecutionPlan.Keyed.PerKey(
                        new ExecutionPlan.Column(k.template().name(), k.template().type().typeName(),
                                com.legend.plan.PlanColumns.multiplicity(k.template().multiplicity()),
                                com.legend.executionplan.ColumnFacts.slot(k.slot()), null,
                                com.legend.executionplan.ColumnFacts.precision(k.template().type().typeName(),
                                        k.slot(), ExecutionPlan.TdsResult.Shape.RELATION)),
                        k.nameCell(), field);
            });
        }
        List<String> keyFields = parts.keys().outputs().stream().map(com.legend.sql.OutputCol::name).toList();
        ExecutionPlan.Keyed.Snapshot snapshot = switch (keysSnapshot) {
            case TRANSACTION -> ExecutionPlan.Keyed.Snapshot.TRANSACTION;
            case REPEATABLE_READ -> ExecutionPlan.Keyed.Snapshot.REPEATABLE_READ;
            case SERIALIZABLE -> ExecutionPlan.Keyed.Snapshot.SERIALIZABLE;
            case NONE -> ExecutionPlan.Keyed.Snapshot.NONE;
        };
        return new ExecutionPlan.Keyed(sql(parts.keys(), dialect, target), keyFields, outputs, parts.perKeyAt(),
                sql(parts.body(), dialect, target), snapshot, dialect.resultColumnLimit());
    }

    /** {@code statement} final for {@code target}: rendered in {@code dialect}, each placeholder's binding as the
     *  dialect decided it, the typed tree kept (the judges' plans take it too, JudgePlans, step 6). */
    public static ExecutionPlan.Sql sql(SqlQuery statement, SqlDialect dialect, ExecutionPlan.Target target) {
        return sql(dialect.renderStatement(statement), target, statement);
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
            case RenderedStatement.Binding.Key key -> new ExecutionPlan.Binding.Key(key.cell());
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

    /** The servers a plan written for a session of {@code serverVersion} names: where {@code type}'s dialect is written
     *  for given versions ({@link SqlDialect#serverVersions}: H2's 2.1 and 2.2 the engine-parity dialect, 2.3 and later
     *  the modern one), that version's {@code major.minor} alone ({@link Databases#majorMinor}), so the plan's runner
     *  accepts the session the plan was written for (the dialect is {@code SqlDialect.forServer}'s); else the
     *  dialect's declared servers. A null version is the declared target (the plan path program's step 9, 2026-10-19:
     *  the H2 PCT lane and channel B's H2 judge run the modern H2, and a plan for the declared target was refused on
     *  every case). Here, not in {@code Databases}, since main's dialect program Phase 5 (2026-10-16): the database
     *  owner does not reach the plan's types. */
    public static ExecutionPlan.Servers serversFor(ConnectionDefinition.DatabaseType type,
            @com.legend.base.Nullable String serverVersion) {
        List<String> versions = Databases.dialect(type).serverVersions();
        if (versions == null) {
            return new ExecutionPlan.Servers.Every();
        }
        return new ExecutionPlan.Servers.Versions(
                serverVersion == null ? versions : List.of(Databases.majorMinor(serverVersion)));
    }

    /** Where the plan runs: the database {@code decided}, the server versions and session statements of its dialect,
     *  and the setup its runtime's connections declare, every statement final for that database. */
    private static ExecutionPlan.Target target(com.legend.database.Target decided, String runtime,
            com.legend.compiler.element.ModelContext ctx, SqlDialect dialect, ExecutionPlan.Servers servers,
            Metamodel metamodel) {
        ExecutionPlan.Database database = switch (decided) {
            case com.legend.database.Target.Platform p -> new ExecutionPlan.Database.Platform(p.type());
            case com.legend.database.Target.Declared d -> new ExecutionPlan.Database.Declared(oneDefinition(d));
        };
        List<ExecutionPlan.SetupStep> steps = new ArrayList<>(
                setup(CsvSeed.declaredSteps(runtime, ctx, dialect), dialect));
        steps.addAll(metamodel.setup());
        List<String> session = new ArrayList<>(dialect.sessionSetup());
        session.addAll(metamodel.session());
        return new ExecutionPlan.Target(database, servers, session, steps);
    }

    /**
     * The system metamodel store's tables a plan carries, in the form its database takes: {@code setup}, the target's
     * setup steps (an engine in this process); {@code session}, statements of the session's own, run on every session
     * the plan opens (a server engine); {@code renames}, the query's references to read a session table by its name;
     * {@code inline}, on a database with no temporary table, each table referenced as the query's own rows (the
     * relation for an alias).
     */
    record Metamodel(List<ExecutionPlan.SetupStep> setup, List<String> session, Map<String, String> renames,
            Map<String, java.util.function.Function<String, com.legend.sql.SqlSource>> inline) {
        static final Metamodel NONE = new Metamodel(List.of(), List.of(), Map.of(), Map.of());

        /** {@code q} reading the tables as they are held: a session table by its name, an inline table as its rows. */
        SqlQuery read(SqlQuery q) {
            SqlQuery renamed = com.legend.lowering.SqlPostProcessors.apply(q, renames);
            if (inline.isEmpty()) {
                return renamed;
            }
            return new com.legend.sql.SqlRewriter() {
                @Override
                protected com.legend.sql.SqlSource source(com.legend.sql.SqlSource s) {
                    if (s instanceof com.legend.sql.SqlSource.Table t && inline.containsKey(t.name())) {
                        return inline.get(t.name()).apply(t.alias());
                    }
                    return s;
                }
            }.rewrite(renamed);
        }
    }

    /**
     * The system metamodel store's tables a query reads ({@code Class.all()}: the typed tree's references to
     * {@link com.legend.builtin.SystemMetamodel#STORE_FQN}), carried by the plan as the legacy route seeded them on
     * the graph's own system database ({@code SystemDatabase}; the plan path program's step 9): each referenced
     * table with the graph's facts ({@link MetamodelSeeds}, written at plan time), so a model-free runner answers the
     * query on the plan's session. On an engine in this process (2026-10-18) the tables are the target's setup, in
     * their schema, as the private instance held them. On a SERVER engine (2026-10-19; the user's design of
     * 2026-10-11 for the legacy route) they are the session's own TEMPORARY tables, made by the session's statements
     * on every session the plan opens and gone with it (no other session sees them, nothing permanent is made in the
     * user's database, so a plan with them runs on a database reached by a URL), each named
     * {@link com.legend.setup.Ddl#systemTable} and read by that name through the query's renames; on a server with no
     * temporary table (Trino, MongoDB; 2026-10-20) each is the query's own rows, inline, each cell its column's typed
     * literal. A query reading no such table adds nothing; a table of no rows is created empty (inline: a relation of
     * its columns with no row).
     */
    private static Metamodel metamodel(Compiler.LoweredQuery l, SqlDialect dialect) {
        java.util.Set<String> tables = new java.util.TreeSet<>();
        collectSystemTables(l.root(), tables);
        if (tables.isEmpty()) {
            return Metamodel.NONE;
        }
        com.legend.model.DatabaseDefinition store = l.ctx()
                .findDatabase(com.legend.builtin.SystemMetamodel.STORE_FQN)
                .orElseThrow(() -> new IllegalStateException("the system metamodel store is not in the model"));
        boolean inProcess = dialect.inProcess();
        List<ExecutionPlan.SetupStep> setup = new ArrayList<>();
        List<String> session = new ArrayList<>();
        Map<String, String> renames = new java.util.LinkedHashMap<>();
        Map<String, java.util.function.Function<String, com.legend.sql.SqlSource>> inline =
                new java.util.LinkedHashMap<>();
        for (com.legend.model.DatabaseDefinition.SchemaDefinition schema : store.schemas()) {
            for (com.legend.model.DatabaseDefinition.TableDefinition def : schema.tables()) {
                // a reference names its table schema-qualified (metamodel.classes)
                if (!tables.contains(schema.name() + "." + def.name()) && !tables.contains(def.name())) {
                    continue;
                }
                List<List<String>> rows = MetamodelSeeds.rows(def.name(), l.ctx());
                if (inProcess) {
                    setup.addAll(com.legend.setup.Ddl.metamodelSteps(def, schema.name(), rows, dialect));
                } else if (!dialect.temporaryTables()) {
                    inline.put(schema.name() + "." + def.name(), inlineRows(def, rows));
                } else {
                    session.addAll(com.legend.setup.Ddl.metamodelTemporary(def, schema.name(), rows, dialect));
                    renames.put(schema.name() + "." + def.name(),
                            com.legend.setup.Ddl.systemTable(schema.name(), def.name()));
                }
            }
        }
        return new Metamodel(List.copyOf(setup), List.copyOf(session),
                java.util.Collections.unmodifiableMap(new java.util.LinkedHashMap<>(renames)),
                java.util.Collections.unmodifiableMap(new java.util.LinkedHashMap<>(inline)));
    }

    /**
     * A system table's rows as a query's own relation, for an alias: each cell its column's typed literal (the graph's
     * facts, plan-time constants: the store declares VARCHAR, INTEGER and BIT), a NULL cast to the column's type; a
     * table of no rows a select of typed NULLs that holds no row.
     */
    private static java.util.function.Function<String, com.legend.sql.SqlSource> inlineRows(
            com.legend.model.DatabaseDefinition.TableDefinition def, List<List<String>> rows) {
        List<String> columns = new ArrayList<>();
        List<com.legend.sql.SqlType> types = new ArrayList<>();
        List<com.legend.sql.OutputCol> outputs = new ArrayList<>();
        for (com.legend.model.DatabaseDefinition.ColumnDefinition col : def.columns()) {
            com.legend.sql.SqlType type = switch (col.dataType()) {
                case com.legend.model.RelationalDataType.Varchar ignored -> com.legend.sql.SqlType.Scalar.VARCHAR;
                case com.legend.model.RelationalDataType.Integer_ ignored -> com.legend.sql.SqlType.Scalar.INTEGER;
                case com.legend.model.RelationalDataType.Bit ignored -> com.legend.sql.SqlType.Scalar.BOOLEAN;
                default -> throw new IllegalStateException("system table " + def.name() + "." + col.name()
                        + ": no literal for its type " + col.dataType() + " (the store declares VARCHAR, INTEGER and"
                        + " BIT)");
            };
            columns.add(col.name());
            types.add(type);
            outputs.add(new com.legend.sql.OutputCol(col.name(), type, !col.notNull() && !col.primaryKey()));
        }
        List<List<com.legend.sql.SqlExpr>> cells = new ArrayList<>();
        for (List<String> row : rows.isEmpty() ? List.of(java.util.Collections.<String>nCopies(columns.size(), null))
                : rows) {
            List<com.legend.sql.SqlExpr> out = new ArrayList<>();
            for (int c = 0; c < row.size(); c++) {
                out.add(literal(def.name() + "." + columns.get(c), row.get(c), types.get(c)));
            }
            cells.add(out);
        }
        List<List<com.legend.sql.SqlExpr>> values = List.copyOf(cells);
        if (!rows.isEmpty()) {
            return alias -> new com.legend.sql.SqlSource.Values(values, columns, alias, outputs);
        }
        return alias -> new com.legend.sql.SqlSource.Subselect(com.legend.sql.SqlSelect.starOf(
                new com.legend.sql.SqlSource.Values(values, columns, alias + "_none", outputs))
                .withWhere(new com.legend.sql.SqlExpr.BoolLit(false)), alias, null);
    }

    /** A system table's cell as its column's typed literal; a NULL cast to the type. */
    private static com.legend.sql.SqlExpr literal(String column, @com.legend.base.Nullable String text,
            com.legend.sql.SqlType type) {
        if (text == null) {
            return new com.legend.sql.SqlExpr.Cast(new com.legend.sql.SqlExpr.NullLit(), type);
        }
        if (type == com.legend.sql.SqlType.Scalar.VARCHAR) {
            return new com.legend.sql.SqlExpr.StringLit(text);
        }
        if (type == com.legend.sql.SqlType.Scalar.INTEGER) {
            return new com.legend.sql.SqlExpr.Cast(new com.legend.sql.SqlExpr.IntLit(Long.parseLong(text)), type);
        }
        return switch (text) {
            case "true" -> new com.legend.sql.SqlExpr.BoolLit(true);
            case "false" -> new com.legend.sql.SqlExpr.BoolLit(false);
            default -> throw new IllegalStateException("system table column " + column + ": '" + text + "' is no BIT"
                    + " fact (true or false)");
        };
    }

    private static void collectSystemTables(com.legend.compiler.spec.typed.TypedSpec n, java.util.Set<String> out) {
        if (n instanceof com.legend.compiler.spec.typed.TypedTableReference tr
                && tr.store().equals(com.legend.builtin.SystemMetamodel.STORE_FQN)) {
            out.add(tr.table());
        }
        for (com.legend.compiler.spec.typed.TypedSpec c : n.children()) {
            collectSystemTables(c, out);
        }
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
        // the one owner of the steps' form (the doors, 2026-10-29): the body driver's seeds take the same
        return CsvSeed.planned(steps, dialect).stream().map(CsvSeed.Planned::step).toList();
    }
}
