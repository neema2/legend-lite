// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.plan;

import com.legend.compiler.element.ModelContext;
import com.legend.error.NotImplementedException;
import com.legend.lineage.ScanRelations;
import com.legend.model.DatabaseDefinition;
import com.legend.model.RelationalDataType;
import com.legend.sql.SqlExpr;
import com.legend.sql.SqlQuery;
import com.legend.sql.SqlSelect;
import com.legend.sql.SqlSource;

/**
 * The engine's {@code planToString} text for SINGLE-RELATIONAL plans
 * (#47 pilot): the literal plan printout the executionPlan corpus pins —
 * a text envelope around the ENGINE-STYLE SQL the toSQLString pipeline
 * already renders literally, plus the set-implementation identity and
 * the result columns read STRUCTURALLY from the SQL IR (alias + the
 * store column's engine type spelling). Plan text compares LITERALLY —
 * the toSQLString doctrine. Anything beyond the single-node vocabulary
 * (unions, computed projections, multi-node sequences) is a named wall.
 */
public final class PlanText {

    private PlanText() {
    }

    public static String single(ModelContext ctx, String rootClassFqn,
            String mappingFqn, SqlQuery plan, String sql,
            java.util.List<com.legend.compiler.spec.typed.TypedSpec> body) {
        return single(ctx, rootClassFqn, mappingFqn, plan, sql, body,
                "TestDatabaseConnection(type = \"H2\")");
    }

    /** {@code connectionName}: the runtime connection's full plan
     * spelling — class simple name + declared DatabaseType (an inline
     * {@code ^DatabaseConnection(type=DatabaseType.DB2)} prints
     * {@code DatabaseConnection(type = "DB2")}). */
    public static String single(ModelContext ctx, String rootClassFqn,
            String mappingFqn, SqlQuery plan, String sql,
            java.util.List<com.legend.compiler.spec.typed.TypedSpec> body,
            @com.legend.Nullable String connectionName) {
        return single(ctx, rootClassFqn, mappingFqn, plan, sql, body,
                connectionName, java.util.List.of());
    }

    /** {@code chainMappings}: ModelChainConnection mappings (M2M2R) —
     * the root set's physical identity may chase through them. */
    public static String single(ModelContext ctx, String rootClassFqn,
            String mappingFqn, SqlQuery plan, String sql,
            java.util.List<com.legend.compiler.spec.typed.TypedSpec> body,
            @com.legend.Nullable String connectionName,
            java.util.List<String> chainMappings) {
        return single(ctx, rootClassFqn, mappingFqn, plan, sql, body,
                connectionName, chainMappings, plan, false);
    }

    /** {@code colsPlan}: the plan resultColumns types against — the
     * cross-store splice passes the placeholder-bearing IR here (engine:
     * the placeholder relation IS in the query when resultColumns is
     * computed) while typeBlock/tdsTuples keep the pre-splice plan. */
    public static String single(ModelContext ctx, String rootClassFqn,
            String mappingFqn, SqlQuery plan, String sql,
            java.util.List<com.legend.compiler.spec.typed.TypedSpec> body,
            @com.legend.Nullable String connectionName,
            java.util.List<String> chainMappings, SqlQuery colsPlan, boolean pushDownEnums) {
        String[] impl = ScanRelations.rootImpl(ctx, mappingFqn,
                rootClassFqn, chainMappings);
        com.legend.compiler.element.type.Type.RelationType rrt =
                body.isEmpty() ? null
                        : com.legend.compiler.element.type.Type.relationSchema(
                                body.get(body.size() - 1).info().type());
        String cols;
        if (rrt == null && !body.isEmpty()
                && body.get(body.size() - 1).info().type()
                        instanceof com.legend.compiler.element.type
                                .Type.Primitive) {
            // scalar projection: the ONE select expression, empty doc —
            // spelled from the emitted sql (the engine prints the raw
            // select item text)
            String item = sql.startsWith("select ")
                    ? sql.substring("select ".length(),
                            sql.indexOf(" from ")) : sql;
            cols = "(" + item + ", \"\")";
        } else {
            cols = resultColumns(ctx, storeDbs(ctx, mappingFqn, body,
                    chainMappings, impl[2]), colsPlan, rrt);
        }
        return "Relational\n(\n"
                + typeBlock(ctx, rootClassFqn, impl, plan, body, mappingFqn, pushDownEnums)
                + "  resultColumns = [" + cols + "]\n"
                + "  sql = " + sql + "\n"
                + "  connection = " + connectionName + "\n"
                + ")\n";
    }

    /** The class extent a plan body is rooted at (its first getAll), or
     * null for a relation-rooted body. */
    public static @com.legend.Nullable String rootGetAllClass(
            java.util.List<com.legend.compiler.spec.typed.TypedSpec> body) {
        var ga = firstOf(body, com.legend.compiler.spec.typed.TypedGetAll.class);
        return ga == null ? null : ga.classFqn();
    }

    /** The table reference a relation-rooted plan body is rooted at. */
    public static com.legend.compiler.spec.typed.@com.legend.Nullable TypedTableReference
            rootTableReference(java.util.List<com.legend.compiler.spec.typed.TypedSpec> body) {
        return firstOf(body, com.legend.compiler.spec.typed.TypedTableReference.class);
    }

    private static <T extends com.legend.compiler.spec.typed.TypedSpec> @com.legend.Nullable T firstOf(
            java.util.List<com.legend.compiler.spec.typed.TypedSpec> body, Class<T> kind) {
        java.util.ArrayDeque<com.legend.compiler.spec.typed.TypedSpec> work =
                new java.util.ArrayDeque<>(body);
        while (!work.isEmpty()) {
            var t = work.poll();
            if (kind.isInstance(t)) {
                return kind.cast(t);
            }
            work.addAll(t.children());
        }
        return null;
    }

    /** A RELATION-ROOTED single node (a table accessor / tableToTDS
     * query has no class root): the TDS tuples resolve physically through
     * the root table's database; an ACCESSOR root spells its columns as
     * the engine's precisePrimitives with their default relational types
     * ({@link PreciseTypes}), a tableToTDS root as base pure types. */
    public static String singleRelationRoot(ModelContext ctx, String dbFqn,
            boolean accessor, SqlQuery plan, String sql,
            java.util.List<com.legend.compiler.spec.typed.TypedSpec> body,
            @com.legend.Nullable String connectionName) {
        com.legend.compiler.element.type.Type.RelationType rt =
                com.legend.compiler.element.type.Type.relationSchema(
                        body.get(body.size() - 1).info().type());
        if (rt == null) {
            throw new NotImplementedException(
                    "plan: relation-rooted node with a non-relation terminal pending");
        }
        String tuples = tdsTuples(ctx, java.util.List.of(dbFqn), plan, rt,
                docsOf(body.get(body.size() - 1)), null, false, false);
        if (accessor) {
            StringBuilder sb = new StringBuilder();
            for (String t : tuples.split("\\), \\(")) {
                // (name, PureType, DB, "doc") — re-spell the middle pair
                String u = t.startsWith("(") ? t.substring(1) : t;
                u = u.endsWith(")") ? u.substring(0, u.length() - 1) : u;
                String[] parts = u.split(", ", 4);
                String pure = PreciseTypes.pureType(
                        physicalType(ctx, java.util.List.of(dbFqn), plan, parts[0]));
                sb.append(sb.length() > 0 ? ", " : "").append('(')
                        .append(parts[0]).append(", ").append(pure).append(", ")
                        .append(PreciseTypes.defaultSpelling(pure)).append(", ")
                        .append(parts[3]).append(')');
            }
            tuples = sb.toString();
        }
        return "Relational\n(\n"
                + "  type = TDS[" + tuples + "]\n"
                + "  resultColumns = [" + resultColumns(ctx, java.util.List.of(dbFqn), plan, rt) + "]\n"
                + "  sql = " + sql + "\n"
                + "  connection = " + connectionName + "\n"
                + ")\n";
    }

    /** The STORE databases a plan body reads — the root's first, then
     * every other root class's (a cross-store TDS join's from-tree names
     * the tables of two stores; the engine types each physical column by
     * ITS table's store — tdsTwoJoinThreeDB, batch 112). */
    static java.util.List<String> storeDbs(ModelContext ctx, @com.legend.Nullable String mappingFqn,
            java.util.List<com.legend.compiler.spec.typed.TypedSpec> body,
            java.util.List<String> chainMappings, String primary) {
        java.util.LinkedHashSet<String> dbs = new java.util.LinkedHashSet<>();
        dbs.add(primary);
        if (mappingFqn == null) {
            return java.util.List.copyOf(dbs);
        }
        java.util.ArrayDeque<com.legend.compiler.spec.typed.TypedSpec> work =
                new java.util.ArrayDeque<>(body);
        while (!work.isEmpty()) {
            var t = work.poll();
            if (t instanceof com.legend.compiler.spec.typed.TypedGetAll ga) {
                try {
                    String[] impl = ScanRelations.rootImpl(ctx, mappingFqn,
                            ga.classFqn(), chainMappings);
                    if (impl.length > 2 && impl[2] != null) {
                        dbs.add(impl[2]);
                    }
                } catch (NotImplementedException notMapped) {
                    // rootImpl's "no class mapping": a class without a
                    // relational impl under this mapping contributes no
                    // store — its reads type elsewhere
                }
            }
            work.addAll(t.children());
        }
        return java.util.List.copyOf(dbs);
    }

    /** The table's definition in the first of {@code dbs} that declares it. */
    private static java.util.Optional<DatabaseDefinition.TableDefinition> tableIn(
            ModelContext ctx, java.util.List<String> dbs, String table) {
        for (String db : dbs) {
            var td = ctx.findTableDefinition(db, table);
            if (td.isPresent()) {
                return td;
            }
        }
        return java.util.Optional.empty();
    }

    /** The physical DDL type of the top select's column {@code name}. */
    private static RelationalDataType physicalType(ModelContext ctx,
            java.util.List<String> dbs, SqlQuery plan, String name) {
        SqlSelect s = (SqlSelect) plan;
        String[] pc = null;
        for (SqlSelect.Projection p : s.projections()) {
            if (strip(p.alias() == null ? "" : p.alias()).equals(strip(name))
                    && p.expr() instanceof SqlExpr.Column c) {
                pc = resolvePhysical(s.from(), c.table(), strip(c.name()));
            }
        }
        if (pc == null) {
            pc = resolveStarColumn(ctx, dbs, s.from(), strip(name));
        }
        final String[] found = pc;
        var td = tableIn(ctx, dbs, found[0]).orElseThrow();
        return td.columns().stream()
                .filter(x -> x.name().equalsIgnoreCase(found[1]))
                .findFirst().orElseThrow().dataType();
    }

    /** The node's {@code type = ...} block (2-space indent, trailing
     * newline): TDS tuple form (no resultSizeRange), Class impls form,
     * or a bare primitive. */
    public static String typeBlock(ModelContext ctx, String rootClassFqn,
            String[] impl, SqlQuery plan,
            java.util.List<com.legend.compiler.spec.typed.TypedSpec> body) {
        return typeBlock(ctx, rootClassFqn, impl, plan, body, null, false);
    }

    /** {@code pushDownEnums}: PUSH_DOWN_ENUM_TRANSFORM on the plan's context —
     *  enum-typed TDS tuples then carry no enumeration-mapping id. */
    public static String typeBlock(ModelContext ctx, String rootClassFqn,
            String[] impl, SqlQuery plan,
            java.util.List<com.legend.compiler.spec.typed.TypedSpec> body,
            @com.legend.Nullable String mappingFqn, boolean pushDownEnums) {
        com.legend.compiler.spec.typed.TypedSpec last =
                body.get(body.size() - 1);
        if (com.legend.compiler.element.type.Type.relationSchema(last.info().type())
                instanceof com.legend.compiler.element.type.Type.RelationType rt) {
            // TDS plans: per-column (name, PureType, DBTYPE, "doc")
            // tuples and NO resultSizeRange line; the engine quotes the
            // column name exactly when a documentation string rides it
            return "  type = TDS[" + tdsTuples(ctx,
                    storeDbs(ctx, mappingFqn, body, java.util.List.of(), impl[2]),
                    plan, rt, docsOf(last), mappingFqn, impl.length > 4, pushDownEnums) + "]\n";
        }
        String size = "*";
        if (last.info().multiplicity()
                instanceof com.legend.compiler.element.type.Multiplicity
                        .Bounded bm && bm.upper() != null) {
            size = bm.lower() == bm.upper()
                    ? String.valueOf(bm.lower())
                    : bm.lower() + ".." + bm.upper();
        }
        // a SCALAR terminal (class extent ->map to a primitive) types the
        // node as the primitive, not the class envelope
        // (testMapWithOpenVariable's golden)
        if (last.info().type()
                instanceof com.legend.compiler.element.type.Type.Primitive) {
            return "  type = " + pureTypeName(last.info().type())
                    + "\n  resultSizeRange = " + size + "\n";
        }
        return "  type = Class[impls=(" + rootClassFqn + " | "
                + impl[0] + "." + impl[1] + ")]\n"
                + "         as " + rootClassFqn + "\n"
                + "  resultSizeRange = " + size + "\n";
    }

    /** Every line of {@code block} (newline-terminated) shifted right by
     * {@code pad}. */
    public static String indent(String block, String pad) {
        StringBuilder sb = new StringBuilder();
        for (String line : block.split("\n")) {
            sb.append(pad).append(line).append('\n');
        }
        return sb.toString();
    }

    /** The multi-node envelope: type/size lines from the TERMINAL,
     * children (validation node, allocations, terminal Relational) in
     * declaration order at 4-space indent. */
    public static String sequence(String typeBlock,
            java.util.List<String> children) {
        return headedBlock("Sequence", typeBlock, children, "  )\n");
    }

    /** {@code RelationalBlockExecutionNode} — the temp-table IN
     * protocol's envelope (the engine's processInOperation.pure); the
     * same layout as {@link #sequence} under a different head. */
    public static String relationalBlock(String typeBlock,
            java.util.List<String> children) {
        // engine storeContract planNodeToString: the children block's
        // closer carries a TRAILING space (childrenToString + ' \n')
        return headedBlock("RelationalBlockExecutionNode", typeBlock,
                children, "  ) \n");
    }

    private static String headedBlock(String head, String typeBlock,
            java.util.List<String> children, String childrenCloser) {
        StringBuilder sb = new StringBuilder(head).append("\n(\n")
                .append(typeBlock).append("  (\n");
        for (String c : children) {
            sb.append(indent(c, "    "));
        }
        return sb.append(childrenCloser).append(")\n").toString();
    }

    /** {@code CreateAndPopulateTempTable} (processInOperation): the
     * temp-table population step — Void-typed, its column metadata the
     * engine's ColumnForStoringInCollection with the db-specific type. */
    public static String createAndPopulateTempTable(
            java.util.List<String> inputVarNames, String tempTableName,
            String columnType, String connName) {
        return "CreateAndPopulateTempTable\n(\n"
                + "  type = Void\n"
                + "  inputVarNames = ["
                + String.join(", ", inputVarNames) + "]\n"
                + "  tempTableName = " + tempTableName + "\n"
                // engine spelling: a space before the closing parenthesis
                + "  tempTableColumns = [(ColumnForStoringInCollection, "
                + columnType + " )]\n"
                + "  connection = " + connName + "\n)\n";
    }

    /** {@code FreeMarkerConditionalExecutionNode} (processInOperation):
     * the size-threshold conditional — String-typed, the freemarker
     * boolean prints as {@code condition}. */
    public static String freeMarkerConditional(String condition,
            String trueBlock, String falseBlock) {
        return "FreeMarkerConditionalExecutionNode\n(\n"
                + "  type = String\n"
                + "  condition = " + condition + "\n"
                // engine planNodeToString: the block's parenthesis sits at
                // the label's indent, its node two deeper
                + "  trueBlock = \n  (\n"
                + indent(trueBlock, "    ")
                + "  )\n"
                + "  falseBlock = \n  (\n"
                + indent(falseBlock, "    ")
                + "  )\n)\n";
    }

    /** {@code PureExp} — a NON-RELATIONAL let value carried as a plan
     * expression (the engine's PureExpressionPlatformExecutionNode):
     * type/sizeRange, the free plan variables it requires, and the PURE
     * SOURCE of the expression. */
    public static String pureExp(String typeName,
            @com.legend.Nullable String sizeRange, String requiresSpell,
            String exprSource) {
        return "PureExp\n(\n"
                + "  type = " + typeName + "\n"
                + "  resultSizeRange = " + sizeRange + "\n"
                + "  requires = [" + requiresSpell + "]\n"
                + "  expression = " + exprSource + "\n)\n";
    }

    /** {@code Constant} WITHOUT a resultSizeRange line — the temp-table
     * protocol's value lists (processInOperation spells only
     * type/values). */
    public static String constantBare(String typeName, String valueText) {
        return "Constant\n(\n"
                + "  type = " + typeName + "\n"
                + "  values=[" + valueText + "]\n)\n";
    }

    /** {@code FunctionParametersValidationNode} — parameterized plan
     * lambdas validate their arguments first. */
    public static String functionParametersNode(String paramsSpell) {
        return "FunctionParametersValidationNode\n(\n"
                + "  functionParameters = [" + paramsSpell + "]\n)\n";
    }

    /** {@code Allocation} — a let binding materialized as a named node;
     * {@code typeAndSize} is the pre-built 2-indent type block (scalar
     * {@code type/resultSizeRange} pair or the Class impls form),
     * {@code inner} the value's own plan node text. */
    public static String allocation(String name, String typeAndSize,
            String inner) {
        return "Allocation\n(\n"
                + typeAndSize
                + "  name = " + name + "\n"
                + "  value = \n"
                + "    (\n"
                + indent(inner, "      ")
                + "    )\n)\n";
    }

    /** The scalar {@code type/resultSizeRange} pair at 2-space indent. */
    public static String scalarTypeBlock(String typeName,
            @com.legend.Nullable String sizeRange) {
        return "  type = " + typeName + "\n"
                + "  resultSizeRange = " + sizeRange + "\n";
    }

    /** A SCALAR-projection Relational node (an Allocation's query value
     * — {@code ->toOne().lastName} bodies): bare primitive type line,
     * resultColumns spelled as the RAW column expression, and the SQL
     * rendered WITHOUT projection aliases (the engine's scalar select
     * form). Rendering stays in the root layer — the caller supplies the
     * alias-less SQL text and the post-render alias spelling. */
    public static String scalarRelational(ModelContext ctx, String dbFqn,
            SqlSelect plan, String typeName, @com.legend.Nullable String sizeRange, String sql,
            java.util.function.UnaryOperator<String> aliasSpell) {
        StringBuilder rc = new StringBuilder();
        for (SqlSelect.Projection p : plan.projections()) {
            if (rc.length() > 0) {
                rc.append(", ");
            }
            if (!(p.expr() instanceof SqlExpr.Column c)) {
                throw new NotImplementedException("plan: computed scalar"
                        + " projection spelling pending");
            }
            String table = tableOf(plan.from(), c.table());
            var td = ctx.findTableDefinition(dbFqn, table).orElseThrow(
                    () -> new NotImplementedException("plan: table '"
                            + table + "' not in '" + dbFqn + "'"));
            var cd = td.columns().stream()
                    .filter(x -> x.name().equalsIgnoreCase(strip(c.name())))
                    .findFirst().orElseThrow();
            rc.append("(\"").append(aliasSpell.apply(java.util.Objects.requireNonNull(
                    c.table(), "resultColumns need a table-qualified column")))
                    .append("\".").append(c.name()).append(", ")
                    .append(spell(cd.dataType())).append(')');
        }
        return "Relational\n(\n"
                + "  type = " + typeName + "\n"
                + "  resultSizeRange = " + sizeRange + "\n"
                + "  resultColumns = [" + rc + "]\n"
                + "  sql = " + sql + "\n"
                + "  connection = TestDatabaseConnection(type = \"H2\")\n"
                + ")\n";
    }

    /** {@code Constant} — a literal-valued Allocation body (the engine
     * spells {@code values=[...]} without spaces). */
    public static String constant(String typeName, String valueText) {
        return "Constant\n(\n"
                + "  type = " + typeName + "\n"
                + "  resultSizeRange = 1\n"
                + "  values=[" + valueText + "]\n)\n";
    }

    /** Engine pure-type spelling for plan type lines ({@code String},
     * {@code Integer}, ...). */
    public static String pureTypeName(
            com.legend.compiler.element.type.Type t) {
        return pureName(t);
    }

    private static java.util.Map<String, String> docsOf(
            com.legend.compiler.spec.typed.TypedSpec last) {
        java.util.Map<String, String> docs = new java.util.LinkedHashMap<>();
        java.util.ArrayDeque<com.legend.compiler.spec.typed.TypedSpec> work =
                new java.util.ArrayDeque<>();
        work.add(last);
        while (!work.isEmpty()) {
            com.legend.compiler.spec.typed.TypedSpec t = work.poll();
            if (t instanceof com.legend.compiler.spec.typed.TypedProject tp) {
                for (var fc : tp.columns()) {
                    if (fc.documentation() != null) {
                        docs.put(strip(fc.name()), fc.documentation());
                    }
                }
                return docs;
            }
            work.addAll(t.children());
        }
        return docs;
    }

    private static String tdsTuples(ModelContext ctx, java.util.List<String> dbs,
            SqlQuery plan,
            com.legend.compiler.element.type.Type.RelationType rt,
            java.util.Map<String, String> docs, @com.legend.Nullable String mappingFqn) {
        return tdsTuples(ctx, dbs, plan, rt, docs, mappingFqn, false, false);
    }

    /** {@code m2m}: the root followed an M2M (~src) chase — tuple DB
     * types spell PURE defaults, never the physical columns (the M2M
     * layer erases them; m2m2rShowcase golden name VARCHAR(8192)). */
    private static String tdsTuples(ModelContext ctx, java.util.List<String> dbs,
            SqlQuery plan,
            com.legend.compiler.element.type.Type.RelationType rt,
            java.util.Map<String, String> docs, @com.legend.Nullable String mappingFqn,
            boolean m2m, boolean pushDownEnums) {
        if (!(plan instanceof SqlSelect s)) {
            throw new NotImplementedException(
                    "plan: non-select TDS top query pending");
        }
        StringBuilder sb = new StringBuilder();
        var cols = rt.columns();
        // STAR pass-through top select: no positional projection list —
        // every column resolves BY NAME through the from tree (the
        // tdsJoin plans' top query is SELECT * over the join)
        boolean starTop = s.projections().isEmpty()
                || s.projections().stream()
                        .anyMatch(x -> x.expr() instanceof SqlExpr.Star);
        for (int i = 0; i < cols.size(); i++) {
            if (sb.length() > 0) {
                sb.append(", ");
            }
            String name = strip(cols.get(i).name());
            String doc = docs.getOrDefault(name, "");
            SqlSelect.Projection p = starTop ? null : s.projections().get(i);
            String db;
            String[] phys = null;
            if (p == null && !m2m) {
                String[] pc = resolveStarColumn(ctx, dbs, s.from(), name);
                phys = pc;
                var td = tableIn(ctx, dbs, pc[0]).orElseThrow();
                db = spell(td.columns().stream()
                        .filter(x -> x.name().equalsIgnoreCase(pc[1]))
                        .findFirst().orElseThrow().dataType());
            } else if (m2m) {
                db = pureDbSpelling(cols.get(i).type());
                if (db == null) {
                    throw new NotImplementedException("plan: M2M TDS"
                            + " column '" + name + "' type spelling pending");
                }
            } else if (java.util.Objects.requireNonNull(p).expr()
                    instanceof SqlExpr.Column c) {
                String[] pc = resolvePhysical(s.from(), c.table(),
                        strip(c.name()));
                phys = pc;
                var td = tableIn(ctx, dbs, pc[0]).orElseThrow();
                db = spell(td.columns().stream()
                        .filter(x -> x.name().equalsIgnoreCase(pc[1]))
                        .findFirst().orElseThrow().dataType());
            } else {
                // COMPUTED TDS column: when every CASE branch reads the
                // SAME column, the engine resolves that column's physical
                // type (tdsWithEnumReturn: if over \$p.synonyms.type ->
                // VARCHAR(10)); otherwise the PURE type's engine
                // equivalent (aggregate Number -> FLOAT, String -> 8192)
                SqlExpr.Column uni = uniformCaseColumn(p.expr());
                if (uni != null) {
                    String[] pc = resolvePhysical(s.from(), uni.table(),
                            strip(uni.name()));
                    phys = pc;
                    var td = tableIn(ctx, dbs, pc[0]).orElseThrow();
                    db = spell(td.columns().stream()
                            .filter(x -> x.name().equalsIgnoreCase(pc[1]))
                            .findFirst().orElseThrow().dataType());
                } else {
                    db = pureDbSpelling(cols.get(i).type());
                }
                if (db == null) {
                    throw new NotImplementedException("plan: computed TDS"
                            + " column '" + name + "' type spelling pending");
                }
            }
            sb.append('(')
                    .append(doc.isEmpty() ? name : "\"" + name + "\"")
                    .append(", ").append(pureName(cols.get(i).type()))
                    .append(", ").append(db)
                    .append(", \"").append(doc).append("\"");
            // ENUM columns append their ENUMERATION-MAPPING id (the
            // engine's 5-element tuple: (type, <enumFqn>, VARCHAR(20),
            // "", Foo)) — the host-side decode; under PUSH_DOWN_ENUM_TRANSFORM
            // the decode is the CASE in the SQL and the engine's tuple carries
            // no id (relationalMappingExecution.pure: enumMappingId = [])
            if (cols.get(i).type()
                    instanceof com.legend.compiler.element.type.Type
                            .EnumType et2 && mappingFqn != null
                    && !pushDownEnums) {
                String emid = enumMappingIdFor(ctx, mappingFqn,
                        et2.fqn(), phys);
                if (emid != null) {
                    sb.append(", ").append(emid);
                }
            }
            sb.append(')');
        }
        return sb.toString();
    }

    /** The mapping's ENUMERATION-MAPPING for an enum FQN — read off the
     * NORMALIZED artifact, whose list is already include-flattened at
     * Phase E (exact match first, simple-name second — parsed mappings
     * may hold either spelling), or null. */
    public static com.legend.model.@com.legend.Nullable EnumerationMapping enumMappingOf(
            ModelContext ctx, String mappingFqn, String enumFqn) {
        return enumMappingOf(ctx, mappingFqn, enumFqn, new java.util.HashSet<>());
    }

    /** The mapping's own enumeration mappings first, then its INCLUDED
     * mappings' (real pure: an included mapping's enumeration mappings
     * are visible through the include — the enum-decoded rows leg over
     * a mapping that includes the store mapping). */
    private static com.legend.model.@com.legend.Nullable EnumerationMapping enumMappingOf(
            ModelContext ctx, String mappingFqn, String enumFqn,
            java.util.Set<String> seen) {
        if (!seen.add(mappingFqn)) {
            return null;
        }
        var md = ctx.findMapping(mappingFqn).orElse(null);
        if (md == null) {
            return null;
        }
        var ems = md.enumerationMappings();
        String simple = enumFqn.substring(enumFqn.lastIndexOf(':') + 1);
        for (var em : ems) {
            if (em.enumName().equals(enumFqn)) {
                return em;
            }
        }
        for (var em : ems) {
            if (em.enumName().equals(simple)
                    || em.enumName().endsWith("::" + simple)) {
                return em;
            }
        }
        for (var inc : md.includes()) {
            var em = enumMappingOf(ctx, inc.mappingPath(), enumFqn, seen);
            if (em != null) {
                return em;
            }
        }
        return null;
    }

    /** enumMappingOf's id, disambiguated: a mapping may declare SEVERAL
     * enumeration mappings over one enum — the PROPERTY MAPPING that
     * reads the column declares which one ({@code prop:
     * EnumerationMapping synonym: T.COL}). Falls back to
     * first-declared. */
    private static @com.legend.Nullable String enumMappingIdFor(ModelContext ctx,
            String mappingFqn, String enumFqn, String @com.legend.Nullable [] phys) {
        var md = ctx.findMapping(mappingFqn).orElse(null);
        if (md == null) {
            return null;
        }
        String simple = enumFqn.substring(enumFqn.lastIndexOf(':') + 1);
        var candidates = md.enumerationMappings().stream()
                .filter(em -> em.enumName().equals(enumFqn)
                        || em.enumName().equals(simple)
                        || em.enumName().endsWith("::" + simple))
                .toList();
        if (candidates.size() > 1 && phys != null) {
            var ids = candidates.stream()
                    .map(com.legend.model.EnumerationMapping::mappingId)
                    .collect(java.util.stream.Collectors.toSet());
            for (var cb : md.classBindings()) {
                if (!(cb instanceof com.legend.model.MappingDefinition
                                .ClassBinding.Relational rb)
                        || !(rb.source() instanceof com.legend.model
                                .MappingDefinition.RelationalSource.Table t)) {
                    continue;
                }
                for (var ec : t.enumColumns()) {
                    if (ids.contains(ec.enumMappingId())
                            && bareTable(ec.table())
                                    .equalsIgnoreCase(bareTable(phys[0]))
                            && ec.column().equalsIgnoreCase(phys[1])) {
                        return ec.enumMappingId();
                    }
                }
            }
        }
        return candidates.isEmpty() ? null
                : candidates.get(0).mappingId();
    }

    /** A table name without its schema prefix ({@code default.T -> T}). */
    private static String bareTable(String t) {
        return t.substring(t.lastIndexOf('.') + 1);
    }

    /** The engine's dynamic freemarker enum-map FUNCTION NAME —
     * {@code enumMap_<mapping fqn underscored>_<enum-mapping id>}
     * (relationalMappingExecution enum templates), or null when the
     * mapping carries no enumeration mapping for the enum. */
    public static @com.legend.Nullable String enumMapFnOf(ModelContext ctx, String mappingFqn,
            String enumFqn) {
        var em = enumMappingOf(ctx, mappingFqn, enumFqn);
        String id = em == null ? null : em.mappingId();
        return id == null ? null
                : "enumMap_" + mappingFqn.replace("::", "_") + "_" + id;
    }

    private static String pureName(
            com.legend.compiler.element.type.Type t) {
        if (t == com.legend.compiler.element.type.Type.Primitive.STRING) {
            return "String";
        }
        if (t == com.legend.compiler.element.type.Type.Primitive.INTEGER) {
            return "Integer";
        }
        if (t == com.legend.compiler.element.type.Type.Primitive.FLOAT) {
            return "Float";
        }
        if (t == com.legend.compiler.element.type.Type.Primitive.BOOLEAN) {
            return "Boolean";
        }
        if (t == com.legend.compiler.element.type.Type.Primitive.STRICT_DATE) {
            return "StrictDate";
        }
        if (t == com.legend.compiler.element.type.Type.Primitive.DATE_TIME) {
            return "DateTime";
        }
        if (t == com.legend.compiler.element.type.Type.Primitive.DATE) {
            return "Date";
        }
        if (t == com.legend.compiler.element.type.Type.Primitive.DECIMAL) {
            return "Decimal";
        }
        if (t == com.legend.compiler.element.type.Type.Primitive.NUMBER) {
            return "Number";
        }
        // enum-typed columns/parameters spell the enumeration FQN
        if (t instanceof com.legend.compiler.element.type.Type.EnumType et) {
            return et.fqn();
        }
        throw new NotImplementedException("plan: pure type name for " + t);
    }

    /** {@code plan}'s top select with its from-tree's LEFTMOST subselect
     * replaced by a {@code (${var})} placeholder, re-rendered with
     * {@code renderer} — null when the top shape is not
     * join-of-subselects (the cross-store split's SQL-text channel). */
    public static @com.legend.Nullable String spliceLeftVar(
            com.legend.sql.SqlQuery plan, String var,
            java.util.function.Function<com.legend.sql.SqlQuery,
                    String> render) {
        // render is PASSED AS A FUNCTION from the root layer — the
        // plan layer stays dialect-blind (the InProtocol convention;
        // the AnsiSqlRenderer parameter here was the second breach the
        // 2026-08-18 dialect-blind rule caught)
        SqlSelect spliced = spliceLeftVarQuery(plan, var);
        return spliced == null ? null : render.apply(spliced);
    }

    /** The colsPlan for a maybe-spliced plan: the placeholder-bearing
     * IR when the splice applies, else the plan itself. */
    public static com.legend.sql.SqlQuery colsPlanFor(
            com.legend.sql.SqlQuery plan, @com.legend.Nullable String var) {
        if (var == null) {
            return plan;
        }
        SqlSelect s = spliceLeftVarQuery(plan, var);
        return s == null ? plan : s;
    }

    /** The spliced IR itself (the cross-store colsPlan). */
    public static @com.legend.Nullable SqlSelect spliceLeftVarQuery(
            com.legend.sql.SqlQuery plan, String var) {
        if (!(plan instanceof SqlSelect top)
                || !(top.from() instanceof SqlSource.Join jn)) {
            return null;
        }
        SqlSource left = jn.left();
        while (left instanceof SqlSource.Join lj) {
            left = lj.left();
        }
        if (!(left instanceof SqlSource.Subselect ls)) {
            return null;
        }
        SqlSource swapped = swapLeftmost(jn,
                new SqlSource.VarSetPlaceholder(var, ls.alias(),
                        ls.outputs()));
        return new SqlSelect(top.projections(),
                top.distinct(), swapped, top.where(), top.groupBy(),
                top.having(), top.qualify(), top.orderBy(), top.limit(),
                top.offset(), top.outputs());
    }

    private static SqlSource swapLeftmost(SqlSource src, SqlSource repl) {
        return src instanceof SqlSource.Join j
                ? new SqlSource.Join(swapLeftmost(j.left(), repl),
                        j.right(), j.kind(), j.on())
                : repl;
    }

    private static String strip(String name) {
        return name.length() > 1 && name.startsWith("\"")
                && name.endsWith("\"")
                ? name.substring(1, name.length() - 1) : name;
    }

    private static String resultColumns(ModelContext ctx, java.util.List<String> dbs,
            SqlQuery plan,
            com.legend.compiler.element.type.Type
                    .@com.legend.Nullable RelationType rt) {
        if (!(plan instanceof SqlSelect s)) {
            throw new NotImplementedException(
                    "plan: non-select top query (union) pending");
        }
        // STAR pass-through top (the tdsJoin plans): no positional
        // projections — every TDS column resolves BY NAME through the
        // from tree, the same physical typing the type = TDS[...] line
        // spells (resolveStarColumn)
        boolean starTop = s.projections().isEmpty()
                || s.projections().stream()
                        .anyMatch(x -> x.expr() instanceof SqlExpr.Star);
        if (starTop) {
            if (rt == null) {
                throw new NotImplementedException(
                        "plan: star-top resultColumns need the TDS type");
            }
            StringBuilder star = new StringBuilder();
            for (var col : rt.columns()) {
                if (star.length() > 0) {
                    star.append(", ");
                }
                String name = strip(col.name());
                String[] pc;
                try {
                    pc = resolveStarColumn(ctx, dbs, s.from(), name);
                } catch (NotImplementedException e) {
                    // a column resolvable through NO physical branch of a
                    // placeholder-bearing tree is VAR-SOURCED — the
                    // engine types every VarSetPlaceHolder column INT
                    // (cluster 20 follow-up: the 3-db chain's placeholder
                    // carries empty outputs)
                    if (!containsVarSet(s.from())) {
                        throw e;
                    }
                    pc = new String[]{VAR_SET_SENTINEL, name};
                }
                final String[] pcf = pc;
                String spelled = VAR_SET_SENTINEL.equals(pcf[0]) ? "INT"
                        : spell(tableIn(ctx, dbs, pcf[0])
                                .orElseThrow().columns().stream()
                                .filter(x -> x.name().equalsIgnoreCase(pcf[1]))
                                .findFirst().orElseThrow().dataType());
                star.append("(\"").append(name).append("\", ")
                        .append(spelled).append(')');
            }
            return star.toString();
        }
        StringBuilder sb = new StringBuilder();
        for (SqlSelect.Projection p : s.projections()) {
            if (sb.length() > 0) {
                sb.append(", ");
            }
            if (!(p.expr() instanceof SqlExpr.Column c)) {
                // COMPUTED projection (aggregate, expression): the engine
                // spells an EMPTY QUOTED type (inferRelationalType has no
                // physical column) — golden ("Income Function", "")
                sb.append("(\"").append(strip(java.util.Objects
                    .requireNonNull(p.outputName(),
                            "plan TDS projection without an output name")))
                        .append("\", \"\")");
                continue;
            }
            String[] phys = resolvePhysical(s.from(), c.table(),
                    strip(c.name()));
            String table = phys[0];
            var td = tableIn(ctx, dbs, table).orElseThrow(
                    () -> new NotImplementedException("plan: table '"
                            + table + "' not in " + dbs));
            DatabaseDefinition.ColumnDefinition cd = td.columns().stream()
                    .filter(x -> x.name().equalsIgnoreCase(phys[1]))
                    .findFirst().orElseThrow(
                            () -> new NotImplementedException("plan:"
                                    + " column '" + c.name() + "' not on '"
                                    + table + "'"));
            sb.append("(\"").append(strip(java.util.Objects
                    .requireNonNull(p.outputName(),
                            "plan TDS projection without an output name")))
                    .append("\", ").append(spell(cd.dataType())).append(')');
        }
        return sb.toString();
    }

    /** The engine dataType a computed column's PURE type infers to
     * (executionPlan goldens: aggregate Number/Float -> FLOAT); null =
     * no known spelling (stays a named wall). */
    private static @com.legend.Nullable String pureDbSpelling(
            com.legend.compiler.element.type.Type t) {
        if (t == com.legend.compiler.element.type.Type.Primitive.NUMBER
                || t == com.legend.compiler.element.type.Type.Primitive.FLOAT) {
            return "FLOAT";
        }
        if (t == com.legend.compiler.element.type.Type.Primitive.INTEGER) {
            return "INT";
        }
        if (t == com.legend.compiler.element.type.Type.Primitive.BOOLEAN) {
            // m2m2rShowcase golden: (prop1, Boolean, BIT, "")
            return "BIT";
        }
        if (t == com.legend.compiler.element.type.Type.Primitive.STRING) {
            // computed strings carry the engine's default width
            // (executionPlan golden: (name, String, VARCHAR(8192), ""))
            return "VARCHAR(8192)";
        }
        return null;
    }

    /** The ONE column every CASE branch (thens + else, nested) reads,
     * or null when branches differ or carry non-column leaves. */
    private static SqlExpr.@com.legend.Nullable Column uniformCaseColumn(SqlExpr e) {
        if (!(e instanceof SqlExpr.Case)) {
            return null;
        }
        java.util.List<SqlExpr> leaves = new java.util.ArrayList<>();
        collectCaseLeaves(e, leaves);
        SqlExpr.Column col = null;
        for (SqlExpr l : leaves) {
            // an enum-decode leaf resolves to its SOURCE column (the
            // engine plans keep enum columns RAW)
            SqlExpr.Column c = l instanceof SqlExpr.Column lc ? lc
                    : com.legend.sql.DecodeShapes.sourceColumn(l).orElse(null);
            if (c == null) {
                return null;
            }
            if (col == null) {
                col = c;
            } else if (!col.equals(c)) {
                return null;
            }
        }
        return col;
    }

    private static void collectCaseLeaves(SqlExpr e,
            java.util.List<SqlExpr> out) {
        if (e instanceof SqlExpr.Case c
                && com.legend.sql.DecodeShapes.flattenDecode(e).isEmpty()) {
            c.whens().forEach(w -> collectCaseLeaves(w.then(), out));
            if (c.otherwise() != null) {
                collectCaseLeaves(c.otherwise(), out);
            }
            return;
        }
        out.add(e);
    }

    /** The physical table behind a FROM-tree alias. */
    private static String tableOf(SqlSource src, @com.legend.Nullable String alias) {
        return resolvePhysical(src, alias, null)[0];
    }

    /** {@code [physTable, physColumn]} behind an alias.column pair —
     * looks THROUGH subselects (a VIEW's pnl resolves to the underlying
     * table's column; the engine types resultColumns by the physical
     * store column). {@code col} null = table identity only. */
    /** Star-top column resolution BY NAME: the first FROM-tree table
     * whose store definition carries {@code col} wins (from-tree order —
     * the join emission projects left-to-right). Loud when no table
     * claims it. */
    private static String[] resolveStarColumn(ModelContext ctx, java.util.List<String> dbs,
            SqlSource src, String col) {
        switch (src) {
            case SqlSource.Table t -> {
                var td = tableIn(ctx, dbs, t.name());
                if (td.isPresent() && td.get().columns().stream()
                        .anyMatch(c -> c.name().equalsIgnoreCase(col))) {
                    return new String[]{t.name(), col};
                }
            }
            case SqlSource.Join j -> {
                try {
                    return resolveStarColumn(ctx, dbs, j.left(), col);
                } catch (NotImplementedException e) {
                    return resolveStarColumn(ctx, dbs, j.right(), col);
                }
            }
            case SqlSource.VarSetPlaceholder vp -> {
                // engine pureToSQLQuery.pure:583 hard-types EVERY
                // VarSetPlaceHolder column ^Integer() — a tdsVar-sourced
                // column prints INT regardless of its physical origin
                // (ledger cluster 20)
                if (vp.outputs().stream().anyMatch(o ->
                        col.equalsIgnoreCase(strip(o.name())))) {
                    return new String[]{VAR_SET_SENTINEL, col};
                }
            }
            case SqlSource.Subselect ss -> {
                // a PROJECTED subselect (the tdsJoin shape: star top over
                // joined projection subselects): the named projection
                // resolves THROUGH to its physical column (the engine
                // types resultColumns by the physical store column); a
                // star wrap or non-column projection descends by name
                if (ss.inner() instanceof SqlSelect is) {
                    for (SqlSelect.Projection p2 : is.projections()) {
                        if (p2.outputName() != null
                                && col.equalsIgnoreCase(strip(p2.outputName()))
                                && p2.expr() instanceof SqlExpr.Column c2) {
                            String[] phys = resolvePhysical(is.from(),
                                    c2.table(), strip(c2.name()));
                            // a FOREIGN-db physical table (the cross-db
                            // splice put another allocation's subtree in
                            // this from-tree) declines — the search
                            // continues by name (cluster 20 follow-up:
                            // tdsTwoJoinThreeDB's 3-db chain)
                            if (VAR_SET_SENTINEL.equals(phys[0])
                                    || tableIn(ctx, dbs, phys[0]).isPresent()) {
                                return phys;
                            }
                            break;
                        }
                    }
                    return resolveStarColumn(ctx, dbs, is.from(), col);
                }
            }
            default -> {
                // values under a star top: fall through loud
            }
        }
        throw new NotImplementedException("plan: star-top TDS column '"
                + col + "' resolves through no FROM-tree table");
    }

    /** Private star-top sentinel: a VarSetPlaceholder-sourced column has
     * no physical table — resultColumns spells the engine's hard INT.
     * Never a legal table name, never reachable from tdsTuples. */
    private static final String VAR_SET_SENTINEL = "\u0000varset";

    private static boolean containsVarSet(SqlSource src) {
        return switch (src) {
            case SqlSource.VarSetPlaceholder ignored -> true;
            case SqlSource.Join j ->
                    containsVarSet(j.left()) || containsVarSet(j.right());
            case SqlSource.Subselect ss ->
                    ss.inner() instanceof SqlSelect is
                            && containsVarSet(is.from());
            default -> false;
        };
    }

    private static String[] resolvePhysical(SqlSource src, @com.legend.Nullable String alias,
            @com.legend.Nullable String col) {
        switch (src) {
            case SqlSource.VarSetPlaceholder vp -> {
                if (vp.alias().equals(alias)) {
                    return new String[]{VAR_SET_SENTINEL, col};
                }
            }
            case SqlSource.Table t -> {
                if (t.alias().equals(alias)) {
                    return new String[]{t.name(), col};
                }
            }
            case SqlSource.Join j -> {
                try {
                    return resolvePhysical(j.left(), alias, col);
                } catch (NotImplementedException e) {
                    return resolvePhysical(j.right(), alias, col);
                }
            }
            case SqlSource.Subselect sub -> {
                if (sub.alias().equals(alias)
                        && sub.inner() instanceof SqlSelect is) {
                    if (col == null) {
                        throw new NotImplementedException("plan: alias '"
                                + alias + "' is a subselect — column"
                                + " required to resolve through it");
                    }
                    for (SqlSelect.Projection p2 : is.projections()) {
                        if (p2.outputName() != null
                                && col.equals(strip(p2.outputName()))
                                && p2.expr() instanceof SqlExpr.Column c2) {
                            return resolvePhysical(is.from(), c2.table(),
                                    strip(c2.name()));
                        }
                    }
                    // a STAR pass-through subselect (a table accessor's
                    // filter/limit stage): the column resolves BY NAME
                    // through the inner from tree
                    boolean star = is.projections().isEmpty()
                            || is.projections().stream()
                                    .anyMatch(x -> x.expr() instanceof SqlExpr.Star);
                    if (star && is.from() instanceof SqlSource.Table it) {
                        return new String[]{it.name(), col};
                    }
                    if (star && is.from() instanceof SqlSource.Subselect isub) {
                        return resolvePhysical(isub, isub.alias(), col);
                    }
                    throw new NotImplementedException("plan: column '" + col
                            + "' not a plain projection of subselect '"
                            + alias + "'");
                }
            }
            default -> { }
        }
        throw new NotImplementedException(
                "plan: alias '" + alias + "' not resolvable to a table"
                + " (" + src.getClass().getSimpleName() + ")");
    }

    /** The engine's resultColumns type spelling (dataTypeToSqlText):
     * INT (not INTEGER), sized VARCHAR/CHAR, etc. */
    public static String spell(RelationalDataType t) {
        return switch (t) {
            case RelationalDataType.Integer_ ignored -> "INT";
            case RelationalDataType.BigInt ignored -> "BIGINT";
            case RelationalDataType.SmallInt ignored -> "SMALLINT";
            case RelationalDataType.TinyInt ignored -> "TINYINT";
            case RelationalDataType.Varchar v -> "VARCHAR(" + v.size() + ")";
            case RelationalDataType.Char_ c -> "CHAR(" + c.size() + ")";
            case RelationalDataType.Double_ ignored -> "DOUBLE";
            case RelationalDataType.Float_ ignored -> "FLOAT";
            case RelationalDataType.Real ignored -> "REAL";
            case RelationalDataType.Decimal d ->
                    "DECIMAL(" + d.precision() + "," + d.scale() + ")";
            case RelationalDataType.Numeric n ->
                    "NUMERIC(" + n.precision() + "," + n.scale() + ")";
            case RelationalDataType.Timestamp ignored -> "TIMESTAMP";
            case RelationalDataType.Date_ ignored -> "DATE";
            case RelationalDataType.Bit ignored -> "BIT";
            // Pending spellings — EXPLICIT so a new variant is a compile
            // error here, not a runtime surprise (T3.1).
            case RelationalDataType.Binary ignored -> throw new NotImplementedException(
                    "plan: type spelling for " + t + " pending");
            case RelationalDataType.Varbinary ignored -> throw new NotImplementedException(
                    "plan: type spelling for " + t + " pending");
            case RelationalDataType.Distinct ignored -> throw new NotImplementedException(
                    "plan: type spelling for " + t + " pending");
            case RelationalDataType.Other ignored -> throw new NotImplementedException(
                    "plan: type spelling for " + t + " pending");
            case RelationalDataType.SemiStructured ignored -> throw new NotImplementedException(
                    "plan: type spelling for " + t + " pending");
            case RelationalDataType.Array ignored -> throw new NotImplementedException(
                    "plan: type spelling for " + t + " pending");
            case RelationalDataType.Object_ ignored -> throw new NotImplementedException(
                    "plan: type spelling for " + t + " pending");
        };
    }
}
