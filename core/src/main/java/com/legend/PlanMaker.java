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
import java.util.Map;

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

    /** {@code query}'s plan on {@code runtime}, its text in the form {@code output} asks for, lowered under the
     *  feature flags {@code features}; each declared parameter a value the statement binds where it is used. */
    static ExecutionPlan plan(TypedQuery query, @com.legend.base.Nullable String runtime, TypedQuery.Output output,
            java.util.Set<com.legend.platform.Feature> features) {
        return plan(query, runtime, output, features, null);
    }

    /** {@link #plan(TypedQuery, String, TypedQuery.Output, java.util.Set)} for a session whose server is
     *  {@code serverVersion} (the caller's session fact, as the legacy seam read it off the connection,
     *  {@code SqlDialect.forServer}): where the database's spelling depends on the version (H2: 2.1 and 2.2 the
     *  engine-parity dialect, 2.3 and later the modern one), the plan is written in that version's dialect and names
     *  that version as what it is for, so its runner accepts the session; null plans for the dialect's declared
     *  target. The plan path program's step 9, 2026-10-19: the H2 PCT lane runs the modern H2, and a plan for the
     *  declared target was refused by its runner on every case. */
    static ExecutionPlan plan(TypedQuery query, @com.legend.base.Nullable String runtime, TypedQuery.Output output,
            java.util.Set<com.legend.platform.Feature> features, @com.legend.base.Nullable String serverVersion) {
        List<QueryParameters.Declared> declared = query.parameters();
        List<com.legend.sql.SqlExpr.PlanParam> slots = new ArrayList<>(declared.size());
        for (QueryParameters.Declared p : declared) {
            // one value, an optional one's absence, or a list as one array (Declared.slot)
            slots.add(p.slot());
        }
        Compiler.LoweredQuery l = query.lower(runtime, output == TypedQuery.Output.STREAMED_JSON, slots, features);
        // the runtime decided: a null one is refused there, by name
        com.legend.database.Target decided = Compiler.executesOn(l.ctx(), runtime);
        SqlDialect dialect = Databases.dialect(decided.type());
        ExecutionPlan.Servers servers = serversFor(decided.type(), serverVersion);
        if (serverVersion != null && servers instanceof ExecutionPlan.Servers.Versions) {
            dialect = dialect.forServer(serverVersion);
        }
        Metamodel metamodel = metamodel(l, dialect);
        ExecutionPlan.Target target = target(decided, java.util.Objects.requireNonNull(runtime), l, dialect, servers,
                metamodel);
        ExprType root = l.root().info();
        // an enumeration parameter compared with a mapped column: through that place's value table; a system
        // metamodel table on a server read by its session name (Metamodel.renames)
        SqlQuery lowered = metamodel.read(com.legend.sql.EnumValueTables.apply(l.plan()));
        com.legend.sql.SqlSource.Pivot dynamic = com.legend.lowering.KeyedPivot.dynamic(lowered);
        Type.RelationType rowSchema = Type.schemaView(root.type());
        if (dynamic != null && rowSchema != null && !rowSchema.dynamicColumns().isEmpty()
                && com.legend.plan.ResultShape.of(l.root()) == com.legend.plan.ResultShape.TABULAR) {
            // a pivot no cast names: its columns are the data's, so its plan reads its keys first (the probe plan,
            // 2026-10-20, KeyedPivot); every output, every database
            com.legend.lowering.KeyedPivot.Out out = switch (output) {
                case CSV -> com.legend.lowering.KeyedPivot.Out.CSV;
                case JSON, STREAMED_JSON -> com.legend.lowering.KeyedPivot.Out.JSON;
                case TDS_TEXT -> com.legend.lowering.KeyedPivot.Out.TDS_TEXT;
            };
            return new ExecutionPlan(declared.stream().map(p -> p.declaration(l.ctx())).toList(),
                    keyed(lowered, dynamic, rowSchema, out, dialect, target, l.ctx(), WireRender.Spelling.PURE));
        }
        ExecutionPlan.Node node = switch (com.legend.plan.ResultShape.of(l.root())) {
            case GRAPH -> switch (output) {
                case CSV -> throw new com.legend.error.NotImplementedException("graph results have no CSV wire");
                case TDS_TEXT -> throw new com.legend.error.NotImplementedException("graph results have no TDS text");
                case JSON -> text(ExecutionPlan.Format.JSON, objects(root), lowered, dialect, target);
                case STREAMED_JSON -> text(ExecutionPlan.Format.JSON_PER_ROW, objects(root), lowered, dialect, target);
            };
            case TABULAR -> switch (output) {
                case CSV -> wire(WireRender.Format.CSV, lowered, root, dialect, target);
                case JSON -> wire(WireRender.Format.JSON, lowered, root, dialect, target);
                case STREAMED_JSON -> text(ExecutionPlan.Format.JSON_PER_ROW, relation(WireRender.schema(root), lowered),
                        WireRender.rows(lowered, WireRender.schema(root)), dialect, target);
                case TDS_TEXT -> tdsText(lowered, root, dialect, l.ctx(), target);
            };
            // a value's text is the one-column relation `value`, whole: no row to stream
            case SCALAR, COLLECTION -> switch (output) {
                case CSV -> wire(WireRender.Format.CSV, lowered, root, dialect, target);
                case JSON, STREAMED_JSON -> wire(WireRender.Format.JSON, lowered, root, dialect, target);
                case TDS_TEXT -> tdsText(lowered, root, dialect, l.ctx(), target);
            };
        };
        return new ExecutionPlan(declared.stream().map(p -> p.declaration(l.ctx())).toList(), node);
    }

    /**
     * The whole result as Pure's TDS text (the MongoDB line's homework 2, 2026-10-17: the harness's form on the plan
     * path, docs/MONGODB_PLAN_2026_10_16.md §6): a relation's own columns, typed by the query; a value's the one-column
     * relation {@code value}, its column optional only when the value is. A pivot's concrete columns exist only once
     * the database is asked (the legacy executor's probe, ColumnProbe), so a plan with one is refused by name.
     */
    private static ExecutionPlan.TextResult tdsText(SqlQuery lowered, ExprType root, SqlDialect dialect,
            com.legend.compiler.element.ModelContext ctx,
            ExecutionPlan.Target target) {
        // a dynamic pivot's columns are named by the database in the statement itself (TdsTextWrap, step 4)
        Type.RelationType own = Type.schemaView(root.type());
        Type.RelationType schema;
        if (own != null) {
            schema = own;
        } else {
            var m = root.multiplicity();
            boolean optional = m instanceof com.legend.compiler.element.type.Multiplicity.Bounded b && b.lower() == 0
                    && Integer.valueOf(1).equals(b.upper());
            schema = new Type.RelationType(List.of(new Type.Column("value", root.type(), optional
                    ? com.legend.compiler.element.type.Multiplicity.Bounded.ZERO_ONE
                    : com.legend.compiler.element.type.Multiplicity.Bounded.ONE)));
        }
        SqlQuery wrapped = com.legend.lowering.TdsTextWrap.wrap(lowered, schema, null, sqlType -> {
            throw new IllegalStateException("TDS text: an output column outside the query's typed schema (SQL type "
                    + sqlType + "): the lowering's typed slot is missing");
        }, own == null,   // a value's strings quoted whole: the reader types an unquoted cell by its text
                layouts(ctx));
        return text(ExecutionPlan.Format.TDS_TEXT, relation(schema, lowered), wrapped, dialect, target);
    }

    /**
     * A dynamic pivot's probe plan ({@link com.legend.lowering.KeyedPivot}, 2026-10-20): the keys probe, the body with
     * its per-key regions, the outputs with their three facts (a fixed column's from the typed schema and the lowered
     * query, a per-key one's from its aggregate's template and slot), the database's snapshot and column limit.
     * {@code out} {@code ROWS} answers typed rows; a text output its format, the runner writing its punctuation.
     */
    static ExecutionPlan.Keyed keyed(SqlQuery lowered, com.legend.sql.SqlSource.Pivot pivot, Type.RelationType schema,
            com.legend.lowering.KeyedPivot.Out out, SqlDialect dialect, ExecutionPlan.Target target,
            com.legend.compiler.element.ModelContext ctx, WireRender.Spelling spelling) {
        // the database's snapshot first: a database with no probe plan (MongoDB) refuses by its own name before any
        // statement is written
        SqlDialect.KeysSnapshot keysSnapshot = dialect.keysSnapshot();
        com.legend.lowering.KeyedPivot.Parts parts = com.legend.lowering.KeyedPivot.of(lowered, pivot, schema, out,
                false, layouts(ctx), spelling);
        java.util.Map<String, ExecutionPlan.Column> facts = new java.util.HashMap<>();
        for (ExecutionPlan.Column c : com.legend.plan.PlanColumns.of(schema, lowered)) {
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
                                + "' has no facts in the typed schema " + facts.keySet()), f.name(), field);
                case com.legend.lowering.KeyedPivot.PerKey k -> new ExecutionPlan.Keyed.PerKey(
                        new ExecutionPlan.Column(k.template().name(), k.template().type().typeName(),
                                com.legend.plan.PlanColumns.multiplicity(k.template().multiplicity()),
                                com.legend.executionplan.ColumnFacts.slot(k.slot()), null),
                        k.nameCell(), field);
            });
        }
        List<String> keyFields = parts.keys().outputs().stream().map(com.legend.sql.OutputCol::name).toList();
        ExecutionPlan.Format format = switch (out) {
            case ROWS -> null;
            case TDS_TEXT -> ExecutionPlan.Format.TDS_TEXT;
            case CSV -> ExecutionPlan.Format.CSV;
            case JSON -> ExecutionPlan.Format.JSON;
        };
        ExecutionPlan.Keyed.Snapshot snapshot = switch (keysSnapshot) {
            case TRANSACTION -> ExecutionPlan.Keyed.Snapshot.TRANSACTION;
            case REPEATABLE_READ -> ExecutionPlan.Keyed.Snapshot.REPEATABLE_READ;
            case SERIALIZABLE -> ExecutionPlan.Keyed.Snapshot.SERIALIZABLE;
            case NONE -> ExecutionPlan.Keyed.Snapshot.NONE;
        };
        return new ExecutionPlan.Keyed(sql(parts.keys(), dialect, target), keyFields, format, outputs,
                sql(parts.body(), dialect, target), snapshot, dialect.resultColumnLimit());
    }

    /**
     * The probe plan of {@code l} for the legacy wire's callers ({@code Execution}'s wire and streaming paths, until
     * the plan path program's step 7 switches the query service onto plans): null when the query holds no pivot that no
     * cast names; else its keyed plan in {@code dialect} (the session's, version-refined) for the session's server
     * {@code serverVersion}, answered as {@code out} in {@code spelling}. The caller runs it on its own session, whose
     * data it has set up (2026-10-20: the row-as-JSON wires it replaces are deleted).
     */
    public static @com.legend.base.Nullable ExecutionPlan keyedWire(Compiler.LoweredQuery l,
            @com.legend.base.Nullable String runtime, SqlDialect dialect,
            @com.legend.base.Nullable String serverVersion, com.legend.lowering.KeyedPivot.Out out,
            WireRender.Spelling spelling) {
        com.legend.sql.SqlSource.Pivot dynamic = com.legend.lowering.KeyedPivot.dynamic(l.plan());
        Type.RelationType rowSchema = Type.schemaView(l.root().info().type());
        if (dynamic == null || rowSchema == null || rowSchema.dynamicColumns().isEmpty()) {
            return null;
        }
        com.legend.database.Target decided = Compiler.executesOn(l.ctx(), runtime);
        Metamodel metamodel = metamodel(l, dialect);
        ExecutionPlan.Target target = target(decided, java.util.Objects.requireNonNull(runtime), l, dialect,
                serversFor(decided.type(), serverVersion), metamodel);
        SqlQuery lowered = metamodel.read(com.legend.sql.EnumValueTables.apply(l.plan()));
        return new ExecutionPlan(List.of(), keyed(lowered, java.util.Objects.requireNonNull(
                com.legend.lowering.KeyedPivot.dynamic(lowered)), rowSchema, out, dialect, target, l.ctx(), spelling));
    }

    /** The whole result as one text of {@code format}: the lowered query, of {@code root}'s type, wrapped by the wire. */
    private static ExecutionPlan.TextResult wire(WireRender.Format format, SqlQuery lowered, ExprType root,
            SqlDialect dialect, ExecutionPlan.Target target) {
        Type.RelationType schema = WireRender.schema(root);
        ExecutionPlan.Format text = switch (format) {
            case CSV -> ExecutionPlan.Format.CSV;
            case JSON -> ExecutionPlan.Format.JSON;
        };
        return text(text, relation(schema, lowered), WireRender.wrap(lowered, schema, format), dialect, target);
    }

    /** The model's facts the text carrier spells a class instance by: the classes' layouts and their subclasses. */
    private static com.legend.lowering.LiteralSpelling.Layouts layouts(com.legend.compiler.element.ModelContext ctx) {
        return com.legend.lowering.LiteralSpelling.Layouts.of(
                t -> com.legend.compiler.element.ClassLayouts.layoutOf(ctx, t), ctx::subtree);
    }

    static ExecutionPlan.TextResult text(ExecutionPlan.Format format, ExecutionPlan.ResultType type,
            SqlQuery statement, SqlDialect dialect, ExecutionPlan.Target target) {
        return new ExecutionPlan.TextResult(format, type, sql(statement, dialect, target));
    }

    /** {@code statement} final for {@code target}: rendered in {@code dialect}, each placeholder's binding as the
     *  dialect decided it, the typed tree kept (the judges' plans take it too, JudgePlans, step 6). */
    static ExecutionPlan.Sql sql(SqlQuery statement, SqlDialect dialect, ExecutionPlan.Target target) {
        RenderedStatement rendered = dialect.renderStatement(statement);
        List<ExecutionPlan.Slot> slots = rendered.binds().stream()
                .map(b -> new ExecutionPlan.Slot(b.parameter(), binding(b.binding()))).toList();
        List<ExecutionPlan.Region> regions = rendered.regions().stream()
                .map(r -> new ExecutionPlan.Region(r.start(), r.end(), r.lead(), r.separator(), r.empty(),
                        r.firstBind(), r.bindCount(), r.counters())).toList();
        return new ExecutionPlan.Sql(rendered.sql(), slots, target, statement, regions);
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

    /** The relation's columns with their three type facts, read from the lowered query (the plan path program's §3). */
    private static ExecutionPlan.Relation relation(Type.RelationType schema, SqlQuery lowered) {
        return new ExecutionPlan.Relation(com.legend.plan.PlanColumns.of(schema, lowered));
    }

    static ExecutionPlan.Value objects(ExprType root) {
        var m = root.multiplicity().requireBounded("a graph fetch's result");
        return new ExecutionPlan.Value(root.type().typeName(), new ExecutionPlan.Multiplicity(m.lower(), m.upper()));
    }

    /** The servers a plan written for a session of {@code serverVersion} names: where {@code type}'s dialect is written
     *  for given versions ({@link SqlDialect#serverVersions}: H2's 2.1 and 2.2 the engine-parity dialect, 2.3 and later
     *  the modern one), that version's {@code major.minor} alone ({@link Databases#majorMinor}), so the plan's runner
     *  accepts the session the plan was written for (the dialect is {@code SqlDialect.forServer}'s); else the
     *  dialect's declared servers. A null version is the declared target (the plan path program's step 9, 2026-10-19:
     *  the H2 PCT lane and channel B's H2 judge run the modern H2, and a plan for the declared target was refused on
     *  every case). Here, not in {@code Databases}, since main's dialect program Phase 5 (2026-10-16): the database
     *  owner does not reach the plan's types. */
    static ExecutionPlan.Servers serversFor(ConnectionDefinition.DatabaseType type,
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
            Compiler.LoweredQuery l, SqlDialect dialect, ExecutionPlan.Servers servers, Metamodel metamodel) {
        ExecutionPlan.Database database = switch (decided) {
            case com.legend.database.Target.Platform p -> new ExecutionPlan.Database.Platform(p.type());
            case com.legend.database.Target.Declared d -> new ExecutionPlan.Database.Declared(oneDefinition(d));
        };
        List<ExecutionPlan.SetupStep> steps = new ArrayList<>(
                setup(CsvSeed.declaredSteps(runtime, l.ctx(), dialect), dialect));
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
     * the graph's own system database ({@code exec.SystemDatabase}; the plan path program's step 9): each referenced
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
        boolean bulk = dialect.loadsRowsInBulk();
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
                    for (String statement : com.legend.setup.Ddl.metamodelSeed(def, schema.name(), dialect)) {
                        setup.add(new ExecutionPlan.SetupStep.Statement(statement));
                    }
                    if (!rows.isEmpty()) {
                        RowLoad load = com.legend.setup.Ddl.metamodelRows(def, schema.name(), rows);
                        setup.add(bulk ? load.staged(dialect)
                                : new ExecutionPlan.SetupStep.Statement(dialect.render(load.values())));
                    }
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
