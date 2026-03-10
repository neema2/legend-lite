package org.finos.legend.pure.dsl.ast;

import org.finos.legend.engine.plan.GenericType;
import org.finos.legend.engine.plan.RelationType;
import org.finos.legend.engine.plan.SQLExecutionNode;
import org.finos.legend.engine.plan.SingleExecutionPlan;
import org.finos.legend.engine.store.Join;
import org.finos.legend.engine.store.RelationalMapping;
import org.finos.legend.engine.transpiler.SQLDialect;

import org.finos.legend.pure.dsl.PureCompileException;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Compiles Pure AST to structured SQL via {@link SqlBuilder}.
 *
 * <p>
 * Replaces {@link PlanGenerator}: instead of building SQL strings by
 * concatenation, builds a structured {@link SqlBuilder} (SQL AST) that
 * gets serialized later via {@link SqlBuilder#toSql(SQLDialect)}.
 *
 * <p>
 * Aligned with legend-engine's {@code toSQLQuery()} — walks the Pure
 * expression tree once, building a mutable SQL object. Type information
 * is looked up from the compilation side table when needed.
 *
 * <p>
 * Design principle: <strong>minimize subquery wrapping</strong>. Only wrap
 * in subquery when structurally necessary (e.g., ORDER BY on a GROUP BY,
 * or window function on a filtered source).
 */
public class PlanGenerator {

    private final CompilationUnit unit;
    private final SQLDialect dialect;
    private int tableAliasCounter = 0;

    public PlanGenerator(CompilationUnit unit, SQLDialect dialect) {
        this.unit = unit;
        this.dialect = dialect;
    }

    public SingleExecutionPlan generate() {
        ValueSpecification vs = unit.root();
        TypeInfo info = unit.types().get(vs);

        // If root was inlined (user function), process the expanded body
        if (info != null && info.inlinedBody() != null) {
            vs = info.inlinedBody();
            info = unit.types().get(vs);
        }

        SqlBuilder builder;
        if (info.isScalar()) {
            builder = generateScalarQuery(vs);
        } else {
            builder = generateRelation(vs);
        }

        String sql = builder.toSql(dialect);
        var sqlNode = new SQLExecutionNode(
                sql,
                info != null ? info.relationType() : null,
                null // connectionRef resolved by QueryService at execution time
        );
        return new SingleExecutionPlan(sqlNode);
    }

    /**
     * Compiles a standalone scalar query → SELECT scalarExpr AS "value".
     * Used for queries like |42, |'hello', |%2024-01-15->adjust(...), etc.
     */
    private SqlBuilder generateScalarQuery(ValueSpecification ast) {
        ValueSpecification body = ast;
        if (ast instanceof LambdaFunction lf && !lf.body().isEmpty()) {
            body = lf.body().getLast();
        }
        SqlExpr scalar = generateScalar(body, null, mappingFor(ast));
        return new SqlBuilder().addSelect(scalar, dialect.quoteIdentifier("value"));
    }

    // ========== Relation Compilation ==========

    private SqlBuilder generateRelation(ValueSpecification vs) {
        return switch (vs) {
            case AppliedFunction af -> generateFunction(af);
            case LambdaFunction lf -> {
                if (lf.body().isEmpty())
                    throw new PureCompileException("PlanGenerator: empty lambda body");

                yield generateRelation(lf.body().getLast());
            }
            case

                    ClassInstance ci ->

                generateClassInstance(ci);
            default -> throw new PureCompileException(
                    "PlanGenerator: cannot compile: " + vs.getClass().getSimpleName());
        };

    }

    private SqlBuilder generateClassInstance(ClassInstance ci) {
        return switch (ci.type()) {
            case "relation" -> {
                // #>{DB.TABLE}# → SELECT * FROM "TABLE" AS "t0"
                String tableRef = (String) ci.value();
                String tableName = tableRef.contains(".")
                        ? tableRef.substring(tableRef.lastIndexOf('.') + 1)
                        : tableRef;
                String alias = nextTableAlias();
                yield new SqlBuilder()
                        .selectStar()
                        .from(dialect.quoteIdentifier(tableName), dialect.quoteIdentifier(alias));
            }
            case "tdsLiteral" -> {
                // CleanCompiler stores parsed TdsLiteral; nested ASTs may still have raw String
                org.finos.legend.pure.dsl.TdsLiteral tds = ci.value() instanceof org.finos.legend.pure.dsl.TdsLiteral t
                        ? t
                        : org.finos.legend.pure.dsl.TdsLiteral.parse((String) ci.value());

                yield generateTdsLiteral(tds);
            }
            case "instance" ->

            {
                var data = (CleanAstBuilder.InstanceData) ci.value();

                yield generateStructLiteral(data);
            }
            default -> throw new PureCompileException("PlanGenerator: unsupported ClassInstance type: " + ci.type());

        };
    }

    /**
     * Compiles a struct literal ^ClassName(field=val, ...) to SQL VALUES.
     * Renders the instance as a dialect-specific struct inside VALUES.
     */
    private SqlBuilder generateStructLiteral(CleanAstBuilder.InstanceData data) {
        SqlExpr structExpr = renderStructValue(new ClassInstance("instance", data));

        // Use lowercased simple class name as alias (e.g., "test::TypeForProjectTest" →
        // "typeForProjectTest")
        String className = data.className();
        String simpleName = className.contains("::") ? className.substring(className.lastIndexOf("::") + 2) : className;
        String alias = Character.toLowerCase(simpleName.charAt(0)) + simpleName.substring(1);

        return new SqlBuilder()
                .selectStar()
                .fromValues(
                        java.util.List.of(java.util.List.of(structExpr)),
                        "t",
                        java.util.List.of(alias));
    }

    /**
     * Recursively renders a ValueSpecification to a SqlExpr struct/value literal.
     * All syntax goes through dialect — no DuckDB-specific code here.
     */
    private SqlExpr renderStructValue(ValueSpecification vs) {
        return switch (vs) {
            case CString cs -> new SqlExpr.StringLiteral(cs.value());
            case CInteger ci -> new SqlExpr.Literal(String.valueOf(ci.value()));
            case CFloat cf -> new SqlExpr.Literal(String.valueOf(cf.value()));
            case CBoolean cb -> new SqlExpr.BoolLiteral(cb.value());
            case CDateTime cdt -> new SqlExpr.TimestampLiteral(cdt.value());
            case CStrictDate csd -> new SqlExpr.DateLiteral(csd.value());
            case Collection coll -> {
                java.util.List<SqlExpr> elements = coll.values().stream()
                        .map(this::renderStructValue)
                        .toList();
                yield new SqlExpr.ArrayLiteral(elements);
            }
            case ClassInstance ci when "instance".equals(ci.type()) -> {
                var data = (CleanAstBuilder.InstanceData) ci.value();
                var fields = new java.util.LinkedHashMap<String, SqlExpr>();
                for (var entry : data.properties().entrySet()) {
                    fields.put(entry.getKey(), renderStructValue(entry.getValue()));
                }
                yield new SqlExpr.StructLiteral(fields);
            }
            default -> throw new PureCompileException(
                    "PlanGenerator: cannot render struct value from " + vs.getClass().getSimpleName());
        };
    }

    /**
     * Compiles a parsed TdsLiteral to VALUES SQL.
     * Receives structured data from CleanCompiler — no string parsing here.
     */
    private SqlBuilder generateTdsLiteral(org.finos.legend.pure.dsl.TdsLiteral tds) {
        List<String> quotedCols = tds.columns().stream()
                .map(c -> dialect.quoteIdentifier(c.name())).toList();

        List<List<SqlExpr>> rows = tds.rows().stream()
                .map(row -> row.stream().map(this::formatTdsValue).toList())
                .toList();

        return new SqlBuilder()
                .selectStar()
                .fromValues(rows, "_tds", quotedCols);
    }

    /** Formats a typed TDS cell value as SqlExpr. */
    private SqlExpr formatTdsValue(Object val) {
        if (val == null)
            return new SqlExpr.NullLiteral();
        if (val instanceof Boolean b)
            return new SqlExpr.BoolLiteral(b);
        if (val instanceof Number)
            return new SqlExpr.Literal(val.toString());
        return new SqlExpr.StringLiteral(val.toString());
    }

    private SqlBuilder generateFunction(AppliedFunction af) {
        String funcName = simpleName(af.function());
        return switch (funcName) {
            case "getAll" -> generateGetAll(af);
            case "table", "tableReference" -> generateTableAccess(af);
            case "filter" -> generateFilter(af);
            case "project" -> generateProject(af);
            case "sort" -> generateSort(af);
            case "limit", "take" -> generateLimit(af);
            case "drop" -> generateDrop(af);
            case "slice" -> generateSlice(af);
            case "first" -> generateFirst(af);
            case "distinct" -> generateDistinct(af);
            case "select" -> generateSelect(af);
            case "rename" -> generateRename(af);
            case "concatenate", "union" -> generateConcatenate(af);
            case "groupBy" -> generateGroupBy(af);
            case "aggregate" -> generateAggregate(af);
            case "extend" -> generateExtend(af);
            case "join" -> generateJoin(af);
            case "from" -> generateFrom(af);
            case "flatten", "toString", "toVariant" -> {
                // pass through to source
                yield generateRelation(af.parameters().get(0));
            }
            case "sortBy" -> {
                // Old-style sortBy — pass through source for now
                yield generateRelation(af.parameters().get(0));
            }
            case "pivot" -> {
                // pivot not yet SQL-supported — compile source for now
                yield generateRelation(af.parameters().get(0));
            }
            case "eq" -> {
                // eq on a relation — pass through first arg
                yield generateRelation(af.parameters().get(0));
            }
            case "asOfJoin" -> {
                // asOfJoin not yet SQL-supported — compile source for now
                yield generateRelation(af.parameters().get(0));
            }
            case "greaterThan", "lessThan", "greaterThanEqual", "lessThanEqual" -> {
                // comparison on relation — pass through source
                yield generateRelation(af.parameters().get(0));
            }
            case "cast" -> {
                // cast on relation — pass through source
                yield generateRelation(af.parameters().get(0));
            }
            case "write" -> {
                // write passes through the source relation
                yield generateRelation(af.parameters().get(0));
            }
            case "size" -> {
                // size() on a relation → COUNT(*)
                var source = generateRelation(af.parameters().get(0));
                yield source;
            }
            default -> throw new PureCompileException("PlanGenerator: unsupported function '" + funcName + "'");

        };
    }

    // ========== getAll / table ==========

    private SqlBuilder generateGetAll(AppliedFunction af) {
        TypeInfo info = unit.typeInfoFor(af);
        String tableName = info != null ? info.tableName() : null;
        if (tableName == null) {
            tableName = extractClassName(af.parameters().get(0));
        }
        String alias = nextTableAlias();
        return new SqlBuilder()
                .selectStar()
                .from(dialect.quoteIdentifier(tableName), dialect.quoteIdentifier(alias));
    }

    private SqlBuilder generateTableAccess(AppliedFunction af) {
        TypeInfo info = unit.typeInfoFor(af);
        String tableName = info != null ? info.tableName() : null;
        if (tableName == null) {
            String content = extractString(af.parameters().get(0));
            tableName = extractTableName(content);
        }
        return new SqlBuilder()
                .selectStar()
                .from(dialect.quoteIdentifier(tableName), dialect.quoteIdentifier("t0"));
    }

    // ========== filter ==========

    private SqlBuilder generateFilter(AppliedFunction af) {
        var mapping = mappingFor(af);
        List<ValueSpecification> params = af.parameters();
        SqlBuilder source = generateRelation(params.get(0));
        LambdaFunction lambda = (LambdaFunction) params.get(1);
        String paramName = lambda.parameters().isEmpty() ? "x" : lambda.parameters().get(0).name();

        // Minimize wrapping: if source has a direct FROM table (not a subquery),
        // we can inline the WHERE clause regardless of whether it's SELECT * or
        // explicit columns
        if (!source.hasWhere() && !source.hasGroupBy()
                && !source.hasOrderBy() && source.getFromSubquery() == null
                && source.getFromTable() != null) {
            // Use table-alias-prefixed scalar so all column refs get t0. prefix
            if (source.getFromAlias() == null)
                throw new PureCompileException("PlanGenerator: source has no FROM alias for filter inlining");
            String tableAlias = unquote(source.getFromAlias());
            SqlExpr whereClause = generateScalar(lambda.body().get(0), paramName, mapping, tableAlias);
            source.addWhere(whereClause);
            return source;
        }
        // Subquery wrapping — columns resolve by name, no prefix needed
        SqlExpr whereClause = generateScalar(lambda.body().get(0), paramName, mapping);
        return new SqlBuilder()
                .selectStar()
                .fromSubquery(source, "filter_src")
                .addWhere(whereClause);
    }

    // ========== project ==========

    private SqlBuilder generateProject(AppliedFunction af) {
        List<ValueSpecification> params = af.parameters();

        // Sidecar tells us if source is struct-based
        if (unit.types().get(params.get(0)).isStructSource()) {
            return generateStructProject(af);
        }

        var mapping = mappingFor(af);

        // Step 1: Compile the source (getAll, filter, etc.)
        SqlBuilder source = generateRelation(params.get(0));

        // Step 2: Determine table alias from source builder
        String tableAlias;
        if (source.getFromAlias() == null) {
            // TDS literal / VALUES source — wrap as subquery
            String subAlias = nextTableAlias();
            source = new SqlBuilder().selectStar().fromSubquery(source,
                    dialect.quoteIdentifier(subAlias));
            tableAlias = subAlias;
        } else {
            tableAlias = unquote(source.getFromAlias());
        }

        // Step 3: Can we inline columns into source, or do we need a subquery?
        boolean canInline = source.isSelectStar()
                && !source.hasGroupBy()
                && source.getFromSubquery() == null;

        SqlBuilder builder;
        if (canInline) {
            // Replace SELECT * with explicit columns
            source.clearSelect();
            builder = source;
        } else {
            // Wrap source as subquery
            tableAlias = "proj_src";
            builder = new SqlBuilder().fromSubquery(source, tableAlias);
        }

        // Track association joins: assocProperty → cached info
        record AssocJoinInfo(String alias, RelationalMapping mapping, Join join) {
        }
        java.util.Map<String, AssocJoinInfo> assocJoins = new java.util.LinkedHashMap<>();

        // Step 4: Build SELECT columns from pre-resolved projections
        TypeInfo info = unit.types().get(af);
        for (var proj : info.projections()) {
            if (proj.isAssociation()) {
                // Association navigation: e.g., propertyPath=[\"items\", \"productName\"]
                String assocProp = proj.associationProperty();
                String leafProp = proj.property();

                AssocJoinInfo joinInfo = assocJoins.get(assocProp);
                String targetAlias;
                RelationalMapping targetMapping;

                if (joinInfo == null) {
                    var assocTarget = resolveAssociationFromSidecar(af, assocProp);
                    targetMapping = assocTarget.targetMapping();
                    targetAlias = "j" + (assocJoins.size() + 1);
                    assocJoins.put(assocProp, new AssocJoinInfo(targetAlias, targetMapping, assocTarget.join()));
                } else {
                    targetAlias = joinInfo.alias();
                    targetMapping = joinInfo.mapping();
                }

                SqlExpr colExpr = resolveColumnExpr(leafProp, targetMapping, targetAlias);
                builder.addSelect(colExpr, dialect.quoteIdentifier(proj.alias()));
            } else if (proj.computedExpr() != null) {
                // Computed expression: e.g., $e.eventDate->monthNumber()
                // Compile the lambda body as a scalar expression
                SqlExpr computed = generateScalar(proj.computedExpr(), null, mapping, tableAlias);
                builder.addSelect(computed, dialect.quoteIdentifier(proj.alias()));
            } else {
                // Simple single-hop property
                SqlExpr colExpr = resolveColumnExpr(proj.property(), mapping, tableAlias);
                builder.addSelect(colExpr, dialect.quoteIdentifier(proj.alias()));
            }
        }

        // Step 5: Add LEFT OUTER JOINs for association projections
        for (var entry : assocJoins.entrySet()) {
            AssocJoinInfo joinInfo = entry.getValue();
            Join join = joinInfo.join();
            if (join != null && mapping != null) {
                String targetTableName = joinInfo.mapping().table().name();
                String srcTableName = mapping != null ? mapping.table().name() : null;
                String leftCol = srcTableName != null ? join.getColumnForTable(srcTableName) : null;
                String rightCol = join.getColumnForTable(targetTableName);
                SqlExpr onCondition = new SqlExpr.Binary(
                        new SqlExpr.Column(tableAlias, leftCol), "=",
                        new SqlExpr.Column(joinInfo.alias(), rightCol));
                builder.addJoin(SqlBuilder.JoinType.LEFT,
                        dialect.quoteIdentifier(targetTableName),
                        dialect.quoteIdentifier(joinInfo.alias()), onCondition);
            }
        }

        return builder;
    }

    /**
     * Compiles project() when the source is a struct literal.
     * Uses currentResultType (from CleanCompiler) to distinguish
     * array properties (need UNNEST) from scalar properties.
     *
     * Example output:
     * SELECT struct_src.alias.name AS "one", u0_elem.val AS "two"
     * FROM (SELECT * FROM (VALUES ({...})) AS t(alias)) AS struct_src
     * LEFT JOIN LATERAL (SELECT UNNEST(struct_src.alias.addresses)) AS u0(u0_elem)
     * ON TRUE
     */
    private SqlBuilder generateStructProject(AppliedFunction af) {
        ClassInstance structCi = (ClassInstance) af.parameters().get(0);
        CleanAstBuilder.InstanceData structData = (CleanAstBuilder.InstanceData) structCi.value();
        RelationType structType = unit.types().get(structCi).relationType();

        // Build the struct source
        SqlBuilder structSource = generateStructLiteral(structData);

        // Extract class alias (lowercased simple name)
        String className = structData.className();
        String simpleName = className.contains("::") ? className.substring(className.lastIndexOf("::") + 2) : className;
        String structAlias = Character.toLowerCase(simpleName.charAt(0)) + simpleName.substring(1);
        String srcAlias = "struct_src";

        // Track UNNEST joins needed for array properties
        int unnestCounter = 0;

        // Maps: array property name → (joinAlias, elemAlias)
        var arrayAliases = new java.util.LinkedHashMap<String, String[]>();

        SqlBuilder builder = new SqlBuilder();

        // Use pre-resolved projections from sidecar
        TypeInfo info = unit.types().get(af);
        for (var proj : info.projections()) {
            List<String> propPath = proj.propertyPath();
            String alias = proj.alias();

            if (propPath.size() == 1) {
                // Scalar property: struct_src.alias.propName
                SqlExpr colExpr = new SqlExpr.FieldAccess(
                        new SqlExpr.FieldAccess(new SqlExpr.ColumnRef(srcAlias), structAlias), propPath.get(0));
                builder.addSelect(colExpr, dialect.quoteIdentifier(alias));
            } else if (propPath.size() >= 2) {
                // Array property path: e.g., [addresses, val]
                // First segment is the array field, rest is element access
                String arrayProp = propPath.get(0);

                // Look up type from CleanCompiler's side table
                GenericType propType = structType != null
                        ? structType.getColumnType(arrayProp)
                        : null;

                if (propType != null && propType.isList()) {
                    // Array property — need UNNEST
                    String[] existingAliases = arrayAliases.get(arrayProp);
                    String elemAlias;

                    if (existingAliases == null) {
                        // First time seeing this array — register for UNNEST join
                        String joinAlias = "u" + unnestCounter;
                        elemAlias = "u" + unnestCounter + "_elem";
                        unnestCounter++;
                        arrayAliases.put(arrayProp, new String[] { joinAlias, elemAlias });
                    } else {
                        elemAlias = existingAliases[1];
                    }

                    // Access sub-property on the unnested element
                    String leafProp = propPath.get(propPath.size() - 1);
                    SqlExpr colExpr = new SqlExpr.Column(elemAlias, leafProp);
                    builder.addSelect(colExpr, dialect.quoteIdentifier(alias));
                } else {
                    // Nested scalar: struct_src.alias.prop1.prop2
                    SqlExpr base = new SqlExpr.FieldAccess(new SqlExpr.ColumnRef(srcAlias), structAlias);
                    for (String seg : propPath) {
                        base = new SqlExpr.FieldAccess(base, seg);
                    }
                    builder.addSelect(base, dialect.quoteIdentifier(alias));
                }
            }
        }

        // FROM subquery wrapping the struct VALUES
        builder.fromSubquery(structSource, srcAlias);

        // Append UNNEST joins as structured LEFT_LATERAL JoinClauses
        for (var entry : arrayAliases.entrySet()) {
            String arrayProp = entry.getKey();
            String[] als = entry.getValue();
            String joinAlias = als[0];
            String elemAlias = als[1];

            // Build subquery: SELECT UNNEST(path)
            SqlBuilder unnestSubquery = new SqlBuilder()
                    .addSelect(new SqlExpr.Unnest(
                            new SqlExpr.FieldAccess(
                                    new SqlExpr.FieldAccess(new SqlExpr.ColumnRef(srcAlias), structAlias),
                                    arrayProp)),
                            null);

            // Compose alias as "u0(u0_elem)" for the compound alias syntax
            String compoundAlias = joinAlias + "(" + elemAlias + ")";

            builder.addJoin(new SqlBuilder.JoinClause(
                    SqlBuilder.JoinType.LEFT_LATERAL, null, compoundAlias,
                    unnestSubquery, new SqlExpr.BoolLiteral(true), null));
        }

        return builder;
    }

    /** Recursively extracts property chain from nested AppliedProperty. */
    private List<String> extractPropertyChain(ValueSpecification vs) {
        if (vs instanceof AppliedProperty ap) {
            if (!ap.parameters().isEmpty() && ap.parameters().get(0) instanceof AppliedProperty) {
                List<String> parent = extractPropertyChain(ap.parameters().get(0));
                var result = new ArrayList<>(parent);
                result.add(ap.property());
                return result;
            }
            return List.of(ap.property());
        }
        throw new PureCompileException(
                "PlanGenerator: cannot extract property chain from " + vs.getClass().getSimpleName());
    }

    /**
     * Resolves a property to its column expression, handling expression mappings.
     */
    private SqlExpr resolveColumnExpr(String propertyName, RelationalMapping mapping, String alias) {
        if (mapping == null) {
            // No mapping (TDS literal, bare relation) — use property name as column
            // directly
            return alias != null ? new SqlExpr.Column(alias, propertyName) : new SqlExpr.ColumnRef(propertyName);
        }
        if (propertyName == null)
            throw new PureCompileException("PlanGenerator: resolveColumnExpr requires non-null property name");
        var pmOpt = mapping.getPropertyMapping(propertyName);
        if (pmOpt.isPresent()) {
            var pm = pmOpt.get();
            var exprAccess = pm.expressionAccess();
            if (exprAccess.isPresent()) {
                var ea = exprAccess.get();
                SqlExpr base = new SqlExpr.Column(alias, pm.columnName());
                SqlExpr jsonAccess = new SqlExpr.JsonAccess(base, ea.jsonKey());
                return ea.castType() != null
                        ? new SqlExpr.Cast(jsonAccess, ea.castType())
                        : jsonAccess;
            }
            return new SqlExpr.Column(alias, pm.columnName());
        }
        // No mapping for this property — use property name as column (unmapped/direct)
        return new SqlExpr.Column(alias, propertyName);
    }

    /**
     * Looks up a pre-resolved association target from the sidecar.
     * The CleanCompiler pre-resolves all associations during compilation.
     *
     * @param contextNode The AST node whose TypeInfo contains the associations
     * @param assocProp   The association property name to look up
     * @return The pre-resolved association target
     */
    private TypeInfo.AssociationTarget resolveAssociationFromSidecar(ValueSpecification contextNode,
            String assocProp) {
        TypeInfo info = unit.types().get(contextNode);
        if (info.hasAssociations()) {
            var target = info.associations().get(assocProp);
            if (target != null) {
                return target;
            }
        }
        throw new PureCompileException(
                "PlanGenerator: association '" + assocProp + "' not pre-resolved in sidecar"
                        + " — ensure CleanCompiler resolves associations for this node");
    }

    /**
     * Searches all nodes in the sidecar for a pre-resolved association.
     * Used by compileScalar/buildComparison which don't have the parent node handy.
     */
    private TypeInfo.AssociationTarget findAssociationInSidecar(String assocProp) {
        for (var info : unit.types().values()) {
            if (info.hasAssociations()) {
                var target = info.associations().get(assocProp);
                if (target != null) {
                    return target;
                }
            }
        }
        return null;
    }

    /**
     * Resolves the RelationalMapping for a relation expression by finding the
     * getAll class.
     */

    // ========== sort ==========

    private SqlBuilder generateSort(AppliedFunction af) {
        SqlBuilder source = generateRelation(af.parameters().get(0));

        // Read pre-resolved sort specs from sidecar
        TypeInfo info = unit.types().get(af);
        List<TypeInfo.SortSpec> sortSpecs = info.sortSpecs();

        // Minimize wrapping: if source has no ORDER BY, add directly
        SqlBuilder target = source;
        if (source.hasOrderBy()) {
            target = new SqlBuilder()
                    .selectStar()
                    .fromSubquery(source, "sort_src");
        }

        for (var spec : sortSpecs) {
            SqlBuilder.SortDirection dir = spec.direction() == TypeInfo.SortDirection.ASC
                    ? SqlBuilder.SortDirection.ASC
                    : SqlBuilder.SortDirection.DESC;
            target.addOrderBy(new SqlExpr.ColumnRef(spec.column()), dir, SqlBuilder.NullsPosition.DEFAULT);
        }

        return target;
    }

    // ========== limit / drop / slice / first ==========

    private SqlBuilder generateLimit(AppliedFunction af) {
        SqlBuilder source = generateRelation(af.parameters().get(0));
        long count = extractIntValue(af.parameters().get(1));

        // Minimize wrapping: if source has no limit, just add it
        if (!source.hasLimit()) {
            source.limit((int) count);
            return source;
        }
        return new SqlBuilder()
                .selectStar()
                .fromSubquery(source, "limit_src")
                .limit((int) count);
    }

    private SqlBuilder generateDrop(AppliedFunction af) {
        SqlBuilder source = generateRelation(af.parameters().get(0));
        long count = extractIntValue(af.parameters().get(1));

        if (!source.hasOffset()) {
            source.offset((int) count);
            return source;
        }
        return new SqlBuilder()
                .selectStar()
                .fromSubquery(source, "limit_src")
                .offset((int) count);
    }

    private SqlBuilder generateSlice(AppliedFunction af) {
        SqlBuilder source = generateRelation(af.parameters().get(0));
        long start = extractIntValue(af.parameters().get(1));
        long end = extractIntValue(af.parameters().get(2));

        if (!source.hasLimit() && !source.hasOffset()) {
            source.limit((int) (end - start));
            if (start > 0)
                source.offset((int) start);
            return source;
        }
        SqlBuilder wrapper = new SqlBuilder()
                .selectStar()
                .fromSubquery(source, "limit_src")
                .limit((int) (end - start));
        if (start > 0)
            wrapper.offset((int) start);
        return wrapper;
    }

    private SqlBuilder generateFirst(AppliedFunction af) {
        SqlBuilder source = generateRelation(af.parameters().get(0));
        if (!source.hasLimit()) {
            source.limit(1);
            return source;
        }
        return new SqlBuilder()
                .selectStar()
                .fromSubquery(source, "limit_src")
                .limit(1);
    }

    // ========== distinct ==========

    private SqlBuilder generateDistinct(AppliedFunction af) {
        List<ValueSpecification> params = af.parameters();
        SqlBuilder source = generateRelation(params.get(0));
        // Inline DISTINCT into source when no conflicting clauses
        if (!source.hasGroupBy() && !source.hasOrderBy() && !source.hasLimit()) {
            source.distinct();
            return source;
        }
        return new SqlBuilder()
                .selectStar()
                .distinct()
                .fromSubquery(source, "distinct_src");
    }

    // ========== select ==========

    private SqlBuilder generateSelect(AppliedFunction af) {
        List<ValueSpecification> params = af.parameters();
        SqlBuilder source = generateRelation(params.get(0));

        SqlBuilder builder = new SqlBuilder()
                .fromSubquery(source, "subq");

        TypeInfo info = unit.types().get(af);
        for (var cs : info.columnSpecs()) {
            builder.addSelect(new SqlExpr.ColumnRef(cs.columnName()), dialect.quoteIdentifier(cs.columnName()));
        }
        return builder;
    }

    // ========== rename ==========

    private SqlBuilder generateRename(AppliedFunction af) {
        SqlBuilder source = generateRelation(af.parameters().get(0));

        TypeInfo info = unit.types().get(af);
        var renameSpec = info.columnSpecs().get(0);
        String oldName = renameSpec.columnName();
        String newName = renameSpec.alias();

        // Build SELECT using DuckDB EXCLUDE + rename pattern
        SqlBuilder builder = new SqlBuilder().fromSubquery(source, "rename_src");
        builder.selectStar();
        builder.addStarExcept(dialect.quoteIdentifier(oldName));
        builder.addSelect(new SqlExpr.ColumnRef(oldName), dialect.quoteIdentifier(newName));
        return builder;
    }

    // ========== concatenate / union ==========

    private SqlBuilder generateConcatenate(AppliedFunction af) {
        SqlBuilder left = generateRelation(af.parameters().get(0));
        SqlBuilder right = generateRelation(af.parameters().get(1));
        left.unionWrapped(right, true);
        return left;
    }

    // ========== groupBy ==========

    private SqlBuilder generateGroupBy(AppliedFunction af) {
        List<ValueSpecification> params = af.parameters();
        SqlBuilder source = generateRelation(params.get(0));

        SqlBuilder builder = new SqlBuilder()
                .fromSubquery(source, "groupby_src");

        TypeInfo info = unit.types().get(af);
        for (var cs : info.columnSpecs()) {
            if (!cs.isAggregate()) {
                // Group column
                SqlExpr colRef = new SqlExpr.ColumnRef(cs.columnName());
                builder.addSelect(colRef, null);
                builder.addGroupBy(colRef);
            } else {
                // Aggregate column
                String aggFunc = mapPureFuncToSql(cs.aggFunction());
                if (aggFunc == null)
                    aggFunc = "SUM";
                List<SqlExpr> args = new ArrayList<>();
                args.add(new SqlExpr.ColumnRef(cs.columnName()));
                // Add extra args (column refs or literals for multi-arg aggregates)
                for (String extra : cs.extraArgs()) {
                    try {
                        // Try as numeric literal
                        Double.parseDouble(extra);
                        args.add(new SqlExpr.Literal(extra));
                    } catch (NumberFormatException e) {
                        // Treat as column ref
                        args.add(new SqlExpr.ColumnRef(extra));
                    }
                }
                SqlExpr expr = new SqlExpr.FunctionCall(aggFunc, args);
                builder.addSelect(expr, dialect.quoteIdentifier(cs.alias()));
            }
        }

        return builder;
    }

    // ========== aggregate ==========

    private SqlBuilder generateAggregate(AppliedFunction af) {
        // Same as groupBy but with no group columns
        return generateGroupBy(af);
    }

    // ========== extend (window functions) ==========

    private SqlBuilder generateExtend(AppliedFunction af) {
        List<ValueSpecification> params = af.parameters();
        SqlBuilder source = generateRelation(params.get(0));

        // Read pre-resolved window specs from sidecar
        TypeInfo info = unit.types().get(af);
        if (info != null && !info.windowSpecs().isEmpty()) {
            SqlBuilder b = new SqlBuilder().selectStar().fromSubquery(source, "window_src");

            for (var ws : info.windowSpecs()) {
                String quotedAlias = dialect.quoteIdentifier(ws.alias());

                // Build partition/order SqlExprs from sidecar specs
                List<SqlExpr> partitionCols = ws.partitionBy().stream()
                        .map(c -> (SqlExpr) new SqlExpr.ColumnRef(c))
                        .toList();
                List<SqlExpr> orderParts = ws.orderBy().stream()
                        .map(s -> {
                            String dir = s.direction() == TypeInfo.SortDirection.ASC ? "ASC" : "DESC";
                            String nullOrder = "DESC".equals(dir) ? "NULLS FIRST" : "NULLS LAST";
                            return (SqlExpr) new SqlExpr.Literal(
                                    dialect.quoteIdentifier(s.column()) + " " + dir + " " + nullOrder);
                        }).toList();
                String frameClause = ws.frame() != null ? formatFrameSpec(ws.frame()) : null;
                SqlExpr.WindowSpec windowSpec = new SqlExpr.WindowSpec(partitionCols, orderParts, frameClause);

                // Map Pure function name to SQL
                String sqlFunc = mapPureFuncToSql(ws.pureFunctionName());

                // Build the window function SqlExpr
                SqlExpr windowFunc = buildWindowFunc(ws, sqlFunc);

                if (ws.isWrapped()) {
                    String sqlWrapper = mapPureFuncToSql(ws.wrapperFuncName());
                    SqlExpr windowedInner = new SqlExpr.WindowFunction(windowFunc, windowSpec);
                    List<SqlExpr> wrapperArgs = new java.util.ArrayList<>();
                    wrapperArgs.add(windowedInner);
                    for (String arg : ws.wrapperArgs()) {
                        wrapperArgs.add(new SqlExpr.Literal(arg));
                    }
                    SqlExpr wrappedExpr = new SqlExpr.FunctionCall(sqlWrapper, wrapperArgs);
                    b.addWindowColumn(wrappedExpr, null, quotedAlias);
                } else {
                    b.addWindowColumn(windowFunc, windowSpec, quotedAlias);
                }
            }
            return b;
        }

        // Simple extend (computed column) — placeholder
        return new SqlBuilder()
                .selectStar()
                .fromSubquery(source, "extend_src");
    }

    /** Builds the SqlExpr for a window function call from its spec. */
    private SqlExpr buildWindowFunc(TypeInfo.WindowFunctionSpec ws, String sqlFunc) {
        if (ws.isNtile()) {
            return new SqlExpr.FunctionCall("NTILE",
                    List.of(new SqlExpr.Literal(String.valueOf(ws.ntileArg()))));
        }
        if (ws.hasSourceColumn()) {
            List<SqlExpr> args = new ArrayList<>();
            if ("*".equals(ws.sourceColumn())) {
                args.add(new SqlExpr.Literal("*"));
            } else {
                args.add(new SqlExpr.ColumnRef(ws.sourceColumn()));
            }
            for (String extra : ws.extraArgs()) {
                if (extra.startsWith("'")) {
                    args.add(new SqlExpr.Literal(extra));
                } else {
                    try {
                        Double.parseDouble(extra);
                        args.add(new SqlExpr.Literal(extra));
                    } catch (NumberFormatException e) {
                        args.add(new SqlExpr.ColumnRef(extra));
                    }
                }
            }
            return new SqlExpr.FunctionCall(sqlFunc, args);
        }
        return new SqlExpr.FunctionCall(sqlFunc, List.of());
    }

    /** Maps a Pure function name to SQL function name. */
    private String mapPureFuncToSql(String pureFuncName) {
        return switch (pureFuncName) {
            // Aggregates
            case "plus", "sum" -> "SUM";
            case "average", "avg", "mean" -> "AVG";
            case "count", "size" -> "COUNT";
            case "min" -> "MIN";
            case "max" -> "MAX";
            case "stdDev", "stddev" -> "STDDEV";
            case "stdDevSample" -> "STDDEV_SAMP";
            case "stdDevPopulation" -> "STDDEV_POP";
            case "variance" -> "VARIANCE";
            case "varianceSample" -> "VAR_SAMP";
            case "variancePopulation" -> "VAR_POP";
            case "covarSample" -> "COVAR_SAMP";
            case "covarPopulation" -> "COVAR_POP";
            case "median" -> "MEDIAN";
            case "percentileCont" -> "QUANTILE_CONT";
            case "percentile", "percentileDisc" -> "QUANTILE_DISC";
            case "joinStrings" -> "STRING_AGG";
            case "mode" -> "MODE";
            case "corr" -> "CORR";
            // Ranking
            case "rowNumber" -> "ROW_NUMBER";
            case "rank" -> "RANK";
            case "denseRank" -> "DENSE_RANK";
            case "percentRank" -> "PERCENT_RANK";
            case "cumulativeDistribution" -> "CUME_DIST";
            // Value functions
            case "first", "firstValue" -> "FIRST_VALUE";
            case "last", "lastValue" -> "LAST_VALUE";
            case "lag" -> "LAG";
            case "lead" -> "LEAD";
            case "ntile" -> "NTILE";
            case "nthValue", "nth" -> "NTH_VALUE";
            // Math wrappers
            case "round" -> "ROUND";
            case "abs" -> "ABS";
            case "ceil" -> "CEIL";
            case "floor" -> "FLOOR";
            case "truncate" -> "TRUNCATE";
            default -> pureFuncName.toUpperCase();
        };
    }

    /** Formats a structured FrameSpec into SQL frame clause text. */
    private String formatFrameSpec(TypeInfo.FrameSpec frame) {
        String type = frame.frameType().toUpperCase();
        return type + " BETWEEN " + formatFrameBound(frame.start(), true)
                + " AND " + formatFrameBound(frame.end(), false);
    }

    /** Formats a FrameBound into SQL text. */
    private String formatFrameBound(TypeInfo.FrameBound bound, boolean isStart) {
        return switch (bound.type()) {
            case UNBOUNDED -> isStart ? "UNBOUNDED PRECEDING" : "UNBOUNDED FOLLOWING";
            case CURRENT_ROW -> "CURRENT ROW";
            case OFFSET -> {
                long v = bound.offset();
                if (v < 0)
                    yield Math.abs(v) + " PRECEDING";
                if (v > 0)
                    yield v + " FOLLOWING";
                yield "CURRENT ROW";
            }
        };
    }

    // ========== join ==========

    private SqlBuilder generateJoin(AppliedFunction af) {
        List<ValueSpecification> params = af.parameters();
        // join(left, right, joinType, condition)
        if (params.size() < 3) {
            throw new PureCompileException("PlanGenerator: join() requires left, right, and condition");
        }

        SqlBuilder left = generateRelation(params.get(0));
        SqlBuilder right = generateRelation(params.get(1));

        // Read pre-resolved join type from sidecar
        TypeInfo info = unit.types().get(af);
        SqlBuilder.JoinType joinType = switch (info.joinType()) {
            case "LEFT", "LEFT_OUTER" -> SqlBuilder.JoinType.LEFT;
            case "RIGHT", "RIGHT_OUTER" -> SqlBuilder.JoinType.RIGHT;
            case "FULL", "FULL_OUTER" -> SqlBuilder.JoinType.FULL;
            case "CROSS" -> SqlBuilder.JoinType.CROSS;
            default -> SqlBuilder.JoinType.INNER;
        };

        // Determine condition index
        int conditionIdx = params.size() >= 4 ? 3 : 2;
        if (conditionIdx == 2 && params.get(2) instanceof EnumValue) {
            conditionIdx = -1; // cross join, no condition
        }

        // Build ON condition from lambda
        SqlExpr onCondition = null;
        if (conditionIdx >= 0 && conditionIdx < params.size()) {
            var condSpec = params.get(conditionIdx);
            if (condSpec instanceof LambdaFunction lf) {
                String leftParam = lf.parameters().size() > 0 ? lf.parameters().get(0).name() : "l";
                String rightParam = lf.parameters().size() > 1 ? lf.parameters().get(1).name() : "r";
                onCondition = generateJoinCondition(lf.body().get(0), leftParam, rightParam,
                        left, right);
            }
        }

        // Wrap left in a base builder, add right as a JOIN subquery
        SqlBuilder result = new SqlBuilder()
                .selectStar()
                .fromSubquery(left, "\"left_src\"");

        result.addJoin(new SqlBuilder.JoinClause(
                joinType,
                null, // no table name — using subquery
                "\"right_src\"", // right alias
                right, // right as subquery
                onCondition,
                null)); // no USING

        return result;
    }

    /**
     * Compiles a join condition lambda where $l refers to left (t0) and $r refers
     * to right (t1).
     */
    private SqlExpr generateJoinCondition(ValueSpecification vs, String leftParam, String rightParam,
            SqlBuilder left, SqlBuilder right) {
        return switch (vs) {
            case AppliedProperty ap -> {
                // Property access — determine if from left or right
                String owner = "";
                if (ap.parameters() != null && !ap.parameters().isEmpty()
                        && ap.parameters().get(0) instanceof Variable v) {
                    owner = v.name();
                }
                String colName = ap.property();

                yield resolveJoinColumn(colName, owner, leftParam, rightParam, left, right);
            }
            case

                    AppliedFunction af -> {
                String funcName = simpleName(af.function());
                var params = af.parameters();
                yield switch (funcName) {
                    case "equal" -> new SqlExpr.Binary(
                            generateJoinCondition(params.get(0), leftParam, rightParam, left, right), "=",
                            generateJoinCondition(params.get(1), leftParam, rightParam, left, right));
                    case "greaterThan" -> new SqlExpr.Binary(
                            generateJoinCondition(params.get(0), leftParam, rightParam, left, right), ">",
                            generateJoinCondition(params.get(1), leftParam, rightParam, left, right));
                    case "lessThan" -> new SqlExpr.Binary(
                            generateJoinCondition(params.get(0), leftParam, rightParam, left, right), "<",
                            generateJoinCondition(params.get(1), leftParam, rightParam, left, right));
                    case "and" -> new SqlExpr.And(List.of(
                            generateJoinCondition(params.get(0), leftParam, rightParam, left, right),
                            generateJoinCondition(params.get(1), leftParam, rightParam, left, right)));
                    case "or" -> new SqlExpr.Or(List.of(
                            generateJoinCondition(params.get(0), leftParam, rightParam, left, right),
                            generateJoinCondition(params.get(1), leftParam, rightParam, left, right)));
                    case "not" -> new SqlExpr.Not(
                            generateJoinCondition(params.get(0), leftParam, rightParam, left, right));
                    default -> generateScalarFunction(af, leftParam, null, null);
                };
            }
            case
                    Variable v -> {
                yield new SqlExpr.ColumnRef("");
            }
            case
                    CString s ->
                new SqlExpr.StringLiteral(s.value());
            case
                    CInteger i ->
                new SqlExpr.Literal(String.valueOf(i.value()));

            default -> generateScalar(vs, leftParam, null);
        };

    }

    /**
     * Resolves a column reference in a join condition.
     * Maps property name to the correct alias (t0 for left, t1 for right).
     */
    private SqlExpr resolveJoinColumn(String colName, String owner, String leftParam, String rightParam,
            SqlBuilder left, SqlBuilder right) {
        if (owner.equals(leftParam)) {
            return new SqlExpr.Column("left_src", colName);
        } else if (owner.equals(rightParam)) {
            return new SqlExpr.Column("right_src", colName);
        }
        return new SqlExpr.ColumnRef(colName);
    }

    private String unquote(String s) {
        if (s != null && s.length() >= 2 && s.startsWith("\"") && s.endsWith("\"")) {
            return s.substring(1, s.length() - 1);
        }
        return s;
    }

    // ========== from ==========

    private SqlBuilder generateFrom(AppliedFunction af) {
        // from() is a runtime binding — pass through source
        return generateRelation(af.parameters().get(0));
    }

    // ========== Scalar Expression Compilation ==========

    /**
     * Compiles a scalar expression to SqlExpr. Delegates to 4-arg with null
     * tableAlias.
     */
    SqlExpr generateScalar(ValueSpecification vs, String rowParam, RelationalMapping mapping) {
        return generateScalar(vs, rowParam, mapping, null);
    }

    /**
     * Canonical 4-arg version: compiles scalar with optional table alias prefix.
     */
    SqlExpr generateScalar(ValueSpecification vs, String rowParam, RelationalMapping mapping, String tableAlias) {
        return switch (vs) {
            case AppliedProperty ap -> {
                // Check for association path: $p.addresses.street
                if (tableAlias != null && !ap.parameters().isEmpty()
                        && ap.parameters().get(0) instanceof AppliedProperty) {
                    List<String> path = extractPropertyChain(ap);
                    if (path.size() > 1) {
                        String assocProp = path.get(0);
                        String leafProp = path.getLast();
                        // Look up pre-resolved association from sidecar
                        // Walk up to find the nearest parent node with associations
                        TypeInfo.AssociationTarget assocTarget = findAssociationInSidecar(assocProp);
                        if (assocTarget != null) {
                            String targetCol = leafProp;
                            var pmOpt = assocTarget.targetMapping().getPropertyMapping(leafProp);
                            if (pmOpt.isPresent())
                                targetCol = pmOpt.get().columnName();
                            yield new SqlExpr.AssociationRef(assocProp, targetCol);
                        }
                    }
                }
                String colName = ap.property();
                if (mapping != null) {
                    var columnOpt = mapping.getColumnForProperty(colName);
                    if (columnOpt.isPresent())
                        colName = columnOpt.get();
                }
                yield tableAlias != null ? new SqlExpr.Column(tableAlias, colName) : new SqlExpr.ColumnRef(colName);
            }
            case AppliedFunction af -> {
                // Check for inlined user function — process expanded body
                TypeInfo afInfo = unit.types().get(af);
                if (afInfo != null && afInfo.inlinedBody() != null) {
                    TypeInfo bodyInfo = unit.types().get(afInfo.inlinedBody());
                    if (bodyInfo != null && !bodyInfo.isScalar()) {
                        throw new PureCompileException(
                                "Inlined function '" + af.function() + "' returns a relation in scalar context");
                    }
                    yield generateScalar(afInfo.inlinedBody(), rowParam, mapping, tableAlias);
                }
                yield generateScalarFunction(af, rowParam, mapping, tableAlias);
            }
            case CInteger i -> new SqlExpr.Literal(String.valueOf(i.value()));
            case CFloat f -> new SqlExpr.Literal(String.valueOf(f.value()));
            case CDecimal d -> new SqlExpr.Literal(String.valueOf(d.value()));
            case CString s -> new SqlExpr.StringLiteral(s.value());
            case CBoolean b -> new SqlExpr.BoolLiteral(b.value());
            case CDateTime dt -> new SqlExpr.TimestampLiteral(dt.value());
            case CStrictDate sd -> new SqlExpr.DateLiteral(sd.value());
            case CStrictTime st -> new SqlExpr.TimeLiteral(st.value());
            case CLatestDate ld -> new SqlExpr.CurrentDate();
            case Variable v -> {
                if (v.name().equals(rowParam))
                    yield new SqlExpr.ColumnRef("");
                yield new SqlExpr.ColumnRef(v.name());
            }
            case ClassInstance ci -> {
                if (ci.value() instanceof ColSpec cs)
                    yield new SqlExpr.ColumnRef(cs.name());
                // In-memory struct literal: ^Type(prop=val, ...) — render value struct
                if ("instance".equals(ci.type())) {
                    yield renderStructValue(ci);
                }
                throw new PureCompileException(
                        "PlanGenerator: unsupported ClassInstance in scalar: " + ci.type());
            }
            case EnumValue ev -> new SqlExpr.StringLiteral(ev.value());
            case Collection coll -> {
                var exprs = coll.values().stream()
                        .map(v -> generateScalar(v, rowParam, mapping, tableAlias))
                        .collect(Collectors.toList());
                yield new SqlExpr.ArrayLiteral(exprs);
            }
            case LambdaFunction lf -> {
                // Nested lambda in scalar context (predicate arg to find/exists)
                if (!lf.body().isEmpty()) {
                    yield generateScalar(lf.body().get(0), rowParam, mapping, tableAlias);
                }
                throw new PureCompileException(
                        "PlanGenerator: empty lambda body in scalar context");
            }
            case PackageableElementPtr ptr -> {
                // Function/class reference in scalar context — render as string
                yield new SqlExpr.StringLiteral(ptr.fullPath());
            }
            default -> throw new PureCompileException(
                    "PlanGenerator: unsupported scalar: " + vs.getClass().getSimpleName());
        };
    }

    /**
     * Unified scalar function compilation. Handles all Pure functions → SqlExpr.
     * When tableAlias is non-null, property accesses are prefixed with the alias
     * and comparisons may produce EXISTS subqueries for association paths.
     */
    private SqlExpr generateScalarFunction(AppliedFunction af, String rowParam, RelationalMapping mapping,
            String tableAlias) {
        String funcName = simpleName(af.function());
        List<ValueSpecification> params = af.parameters();

        // Helper: recursively compile with same context
        java.util.function.Function<ValueSpecification, SqlExpr> c = v -> generateScalar(v, rowParam, mapping,
                tableAlias);

        // Helper: check if first param is a list via TypeInfo side table
        boolean firstArgIsList = !params.isEmpty() && isListArg(params.get(0));

        return switch (funcName) {
            // --- Comparison (may produce EXISTS for association paths when tableAlias set)
            // ---
            case "equal", "greaterThan", "greaterThanEqual",
                    "lessThan", "lessThanEqual", "notEqual" -> {
                String op = switch (funcName) {
                    case "equal" -> "=";
                    case "greaterThan" -> ">";
                    case "greaterThanEqual" -> ">=";
                    case "lessThan" -> "<";
                    case "lessThanEqual" -> "<=";
                    case "notEqual" -> "<>";
                    default -> "=";
                };
                // Partial date comparisons: render as string comparison
                if (hasPartialDate(params)) {
                    SqlExpr left = renderForDateComparison(params.get(0), c);
                    SqlExpr right = renderForDateComparison(params.get(1), c);
                    yield new SqlExpr.Binary(left, op, right);
                }
                yield buildComparison(params, op, c, mapping, tableAlias);
            }

            // --- Logical ---
            case "and" -> {
                if (params.size() == 1 && firstArgIsList) {
                    yield new SqlExpr.FunctionCall("listBoolAnd", List.of(c.apply(params.get(0))));
                }
                if (params.size() == 1) {
                    yield c.apply(params.get(0));
                }
                yield new SqlExpr.And(List.of(c.apply(params.get(0)), c.apply(params.get(1))));
            }
            case "or" -> {
                if (params.size() == 1 && firstArgIsList) {
                    yield new SqlExpr.FunctionCall("listBoolOr", List.of(c.apply(params.get(0))));
                }
                if (params.size() == 1) {
                    yield c.apply(params.get(0));
                }
                yield new SqlExpr.Or(List.of(c.apply(params.get(0)), c.apply(params.get(1))));
            }
            case "not" -> new SqlExpr.Not(c.apply(params.get(0)));

            // --- Arithmetic ---
            case "plus" -> {
                if (params.size() == 1)
                    yield c.apply(params.get(0)); // unary +
                // String concat: use || operator
                if (params.get(0) instanceof CString || params.get(1) instanceof CString) {
                    yield new SqlExpr.Binary(c.apply(params.get(0)), "||", c.apply(params.get(1)));
                }
                yield new SqlExpr.Binary(c.apply(params.get(0)), "+", c.apply(params.get(1)));
            }
            case "minus" -> {
                if (params.size() == 1) {
                    // Unary minus: (-1 * x) to match old pipeline
                    yield new SqlExpr.Binary(new SqlExpr.Literal("-1"), "*", c.apply(params.get(0)));
                }
                yield new SqlExpr.Binary(c.apply(params.get(0)), "-", c.apply(params.get(1)));
            }
            case "times" -> new SqlExpr.Binary(c.apply(params.get(0)), "*", c.apply(params.get(1)));
            case "divide" -> {
                if (params.size() > 2) {
                    // divide(a, b, scale) → ROUND_EVEN((a / b), scale)
                    yield new SqlExpr.FunctionCall("roundHalfEven",
                            List.of(new SqlExpr.Binary(c.apply(params.get(0)), "/", c.apply(params.get(1))),
                                    c.apply(params.get(2))));
                }
                yield new SqlExpr.Binary(c.apply(params.get(0)), "/", c.apply(params.get(1)));
            }
            case "rem" -> new SqlExpr.FunctionCall("MOD", List.of(c.apply(params.get(0)), c.apply(params.get(1))));

            // --- String ---
            case "contains" -> {
                if (firstArgIsList) {
                    yield new SqlExpr.ListContains(c.apply(params.get(0)), c.apply(params.get(1)));
                }
                // String contains: STRPOS(str, substr) > 0
                yield new SqlExpr.Binary(
                        new SqlExpr.FunctionCall("STRPOS", List.of(c.apply(params.get(0)), c.apply(params.get(1)))),
                        ">", new SqlExpr.Literal("0"));
            }
            case "startsWith" -> new SqlExpr.StartsWith(c.apply(params.get(0)), c.apply(params.get(1)));
            case "endsWith" -> new SqlExpr.EndsWith(c.apply(params.get(0)), c.apply(params.get(1)));
            case "toLower" -> new SqlExpr.FunctionCall("LOWER", List.of(c.apply(params.get(0))));
            case "toUpper" -> new SqlExpr.FunctionCall("UPPER", List.of(c.apply(params.get(0))));
            case "length" -> new SqlExpr.FunctionCall("LENGTH", List.of(c.apply(params.get(0))));
            case "trim" -> new SqlExpr.FunctionCall("TRIM", List.of(c.apply(params.get(0))));
            case "toString" -> {
                // DateTime/StrictDate toString: return the literal string directly
                if (params.get(0) instanceof CDateTime dt) {
                    yield new SqlExpr.StringLiteral(dt.value());
                } else if (params.get(0) instanceof CStrictDate sd) {
                    yield new SqlExpr.StringLiteral(sd.value());
                } else if (params.get(0) instanceof PackageableElementPtr ptr) {
                    // Class toString: return simplified name
                    String full = ptr.fullPath();
                    int idx = full.lastIndexOf("::");
                    yield new SqlExpr.StringLiteral(idx >= 0 ? full.substring(idx + 2) : full);
                }
                yield new SqlExpr.Cast(c.apply(params.get(0)), "String");
            }
            case "substring" -> {
                // Pure is 0-based, SQL is 1-based
                SqlExpr offset = new SqlExpr.Binary(c.apply(params.get(1)), "+", new SqlExpr.Literal("1"));
                if (params.size() > 2) {
                    yield new SqlExpr.FunctionCall("SUBSTRING",
                            List.of(c.apply(params.get(0)), offset, c.apply(params.get(2))));
                }
                yield new SqlExpr.FunctionCall("SUBSTRING",
                        List.of(c.apply(params.get(0)), offset));
            }
            case "indexOf" -> {
                if (firstArgIsList) {
                    yield new SqlExpr.FunctionCall("indexOf",
                            List.of(c.apply(params.get(0)), c.apply(params.get(1))));
                }
                // String indexOf: (INSTR(str, substr) - 1)
                yield new SqlExpr.Binary(
                        new SqlExpr.FunctionCall("INSTR", List.of(c.apply(params.get(0)), c.apply(params.get(1)))),
                        "-", new SqlExpr.Literal("1"));
            }
            case "replace" -> new SqlExpr.FunctionCall("REPLACE",
                    List.of(c.apply(params.get(0)), c.apply(params.get(1)), c.apply(params.get(2))));

            // --- Null checks ---
            case "isEmpty" -> {
                if (firstArgIsList) {
                    yield new SqlExpr.Binary(
                            new SqlExpr.FunctionCall("LEN", List.of(c.apply(params.get(0)))),
                            "=", new SqlExpr.Literal("0"));
                }
                yield new SqlExpr.IsNull(c.apply(params.get(0)));
            }
            case "isNotEmpty" -> {
                if (firstArgIsList) {
                    yield new SqlExpr.Binary(
                            new SqlExpr.FunctionCall("LEN", List.of(c.apply(params.get(0)))),
                            ">", new SqlExpr.Literal("0"));
                }
                yield new SqlExpr.IsNotNull(c.apply(params.get(0)));
            }

            // --- Math ---
            case "abs" -> new SqlExpr.FunctionCall("ABS", List.of(c.apply(params.get(0))));
            case "ceiling", "ceil" -> new SqlExpr.Cast(
                    new SqlExpr.FunctionCall("CEIL", List.of(c.apply(params.get(0)))), "Integer");
            case "floor" -> new SqlExpr.Cast(
                    new SqlExpr.FunctionCall("FLOOR", List.of(c.apply(params.get(0)))), "Integer");
            case "round" -> {
                if (params.size() > 1) {
                    yield new SqlExpr.FunctionCall("ROUND",
                            List.of(c.apply(params.get(0)), c.apply(params.get(1))));
                }
                yield new SqlExpr.Cast(
                        new SqlExpr.FunctionCall("roundHalfEven",
                                List.of(c.apply(params.get(0)), new SqlExpr.Literal("0"))),
                        "Integer");
            }
            case "sqrt" -> new SqlExpr.FunctionCall("SQRT", List.of(c.apply(params.get(0))));
            case "pow", "power" -> new SqlExpr.FunctionCall("POW",
                    List.of(c.apply(params.get(0)), c.apply(params.get(1))));
            case "log" -> new SqlExpr.FunctionCall(params.size() > 1 ? "LOG" : "LN",
                    params.stream().map(c).collect(Collectors.toList()));
            case "exp" -> new SqlExpr.FunctionCall("EXP", List.of(c.apply(params.get(0))));
            case "roundHalfEven" -> {
                if (params.size() > 1) {
                    yield new SqlExpr.FunctionCall("roundHalfEven",
                            List.of(c.apply(params.get(0)), c.apply(params.get(1))));
                }
                yield new SqlExpr.FunctionCall("roundHalfEven", List.of(c.apply(params.get(0))));
            }

            // --- Cast ---
            case "toInteger", "parseInteger" -> new SqlExpr.Cast(c.apply(params.get(0)), "Integer");
            case "toFloat", "parseFloat" -> new SqlExpr.Cast(c.apply(params.get(0)), "Float");
            case "toDecimal", "parseDecimal" -> new SqlExpr.Cast(
                    new SqlExpr.FunctionCall("REGEXP_REPLACE",
                            List.of(c.apply(params.get(0)), new SqlExpr.StringLiteral("[dD]$"),
                                    new SqlExpr.StringLiteral(""))),
                    "Decimal");

            // --- If/Case ---
            case "if" -> {
                if (params.size() >= 3) {
                    // Standard if(cond, then, else)
                    SqlExpr cond = c.apply(params.get(0));
                    LambdaFunction thenLambda = (LambdaFunction) params.get(1);
                    LambdaFunction elseLambda = (LambdaFunction) params.get(2);
                    SqlExpr thenVal = generateScalar(thenLambda.body().get(0), rowParam, mapping, tableAlias);
                    SqlExpr elseVal = generateScalar(elseLambda.body().get(0), rowParam, mapping, tableAlias);
                    yield new SqlExpr.CaseWhen(cond, thenVal, elseVal);
                }
                // Multi-if: [pair(cond, val), ...] -> if(default) — 2 params
                SqlExpr source = c.apply(params.get(0));
                if (params.size() >= 2) {
                    SqlExpr defaultVal = c.apply(params.get(1));
                    yield new SqlExpr.FunctionCall("IF", List.of(source, defaultVal));
                }
                yield source;
            }

            // --- In ---
            case "in" -> {
                SqlExpr left = c.apply(params.get(0));
                if (params.get(1) instanceof Collection coll) {
                    var vals = coll.values().stream().map(c).collect(Collectors.toList());
                    yield new SqlExpr.In(left, vals);
                }
                yield new SqlExpr.In(left, List.of(c.apply(params.get(1))));
            }

            // --- Coalesce ---
            case "coalesce" -> {
                // Map empty list [] to NULL for COALESCE semantics
                java.util.List<SqlExpr> coalArgs = new java.util.ArrayList<>();
                for (var p : params) {
                    if (p instanceof Collection coll && coll.values().isEmpty()) {
                        coalArgs.add(new SqlExpr.Literal("NULL"));
                    } else {
                        coalArgs.add(c.apply(p));
                    }
                }
                yield new SqlExpr.FunctionCall("COALESCE", coalArgs);
            }

            // --- Date adjust ---
            case "adjust" -> {
                if (params.size() < 2) {
                    throw new PureCompileException(
                            "PlanGenerator: adjust() requires at least 2 parameters, got " + params.size());
                }
                SqlExpr dateExpr = c.apply(params.get(0));
                SqlExpr amount = c.apply(params.get(1));
                String unit = "DAY";
                if (params.size() > 2 && params.get(2) instanceof EnumValue ev) {
                    unit = switch (ev.value()) {
                        case "DAYS" -> "DAY";
                        case "HOURS" -> "HOUR";
                        case "MINUTES" -> "MINUTE";
                        case "SECONDS" -> "SECOND";
                        case "MILLISECONDS" -> "MILLISECOND";
                        case "MICROSECONDS" -> "MICROSECOND";
                        case "MONTHS" -> "MONTH";
                        case "YEARS" -> "YEAR";
                        case "WEEKS" -> "WEEK";
                        default -> ev.value();
                    };
                }
                SqlExpr adjusted = new SqlExpr.DateAdd(dateExpr, amount, unit);
                // Check input date precision for proper output wrapping
                if (params.get(0) instanceof CStrictDate sd) {
                    String raw = sd.value();
                    if (raw.matches("\\d{4}")) {
                        // Year-only: wrap in STRFTIME('%Y', CAST(adjusted AS DATE))
                        yield new SqlExpr.FunctionCall("STRFTIME", List.of(
                                new SqlExpr.Cast(adjusted, "Date"),
                                new SqlExpr.StringLiteral("%Y")));
                    } else if (raw.matches("\\d{4}-\\d{2}")) {
                        // Year-month: wrap in STRFTIME('%Y-%m', CAST(adjusted AS DATE))
                        yield new SqlExpr.FunctionCall("STRFTIME", List.of(
                                new SqlExpr.Cast(adjusted, "Date"),
                                new SqlExpr.StringLiteral("%Y-%m")));
                    }
                    // Full date: keep CAST to Date
                    yield new SqlExpr.Cast(adjusted, "Date");
                }
                // CDateTime (timestamp): no CAST wrapping
                if (params.get(0) instanceof CDateTime) {
                    yield adjusted;
                }
                // Column ref or other non-literal: keep CAST to Date
                yield new SqlExpr.Cast(adjusted, "Date");
            }

            // --- Trig ---
            case "sin" -> new SqlExpr.FunctionCall("SIN", List.of(c.apply(params.get(0))));
            case "cos" -> new SqlExpr.FunctionCall("COS", List.of(c.apply(params.get(0))));
            case "tan" -> new SqlExpr.FunctionCall("TAN", List.of(c.apply(params.get(0))));
            case "asin" -> new SqlExpr.FunctionCall("ASIN", List.of(c.apply(params.get(0))));
            case "acos" -> new SqlExpr.FunctionCall("ACOS", List.of(c.apply(params.get(0))));
            case "atan" -> new SqlExpr.FunctionCall("ATAN", List.of(c.apply(params.get(0))));
            case "atan2" -> new SqlExpr.FunctionCall("ATAN2",
                    List.of(c.apply(params.get(0)), c.apply(params.get(1))));
            case "toRadians" -> new SqlExpr.FunctionCall("RADIANS", List.of(c.apply(params.get(0))));
            case "toDegrees" -> new SqlExpr.FunctionCall("DEGREES", List.of(c.apply(params.get(0))));
            case "pi" -> new SqlExpr.FunctionCall("PI", List.of());

            // --- More math ---
            case "mod" -> new SqlExpr.Binary(c.apply(params.get(0)), "%", c.apply(params.get(1)));
            case "log10" -> new SqlExpr.FunctionCall("LOG10", List.of(c.apply(params.get(0))));
            case "sign" -> new SqlExpr.Cast(
                    new SqlExpr.FunctionCall("SIGN", List.of(c.apply(params.get(0)))), "Integer");

            // --- More string ---
            case "reverseString" -> new SqlExpr.FunctionCall("reverseString", List.of(c.apply(params.get(0))));
            case "repeatString" -> new SqlExpr.FunctionCall("REPEAT",
                    List.of(c.apply(params.get(0)), c.apply(params.get(1))));
            case "splitPart" -> {
                SqlExpr str = c.apply(params.get(0));
                SqlExpr delim = c.apply(params.get(1));
                // Pure 0-based index -> SQL 1-based: offset + 1
                SqlExpr idx = new SqlExpr.Binary(c.apply(params.get(2)), "+", new SqlExpr.Literal("1"));
                yield new SqlExpr.CaseWhen(
                        new SqlExpr.Binary(delim, "=", new SqlExpr.StringLiteral("")),
                        str,
                        new SqlExpr.FunctionCall("splitPart", List.of(str, delim, idx)));
            }
            case "joinStrings" -> {
                if (params.size() > 1) {
                    yield new SqlExpr.FunctionCall("arrayToString",
                            params.stream().map(c).collect(Collectors.toList()));
                }
                yield new SqlExpr.FunctionCall("CONCAT", List.of(c.apply(params.get(0))));
            }
            case "find" -> new SqlExpr.StrPosition(c.apply(params.get(1)), c.apply(params.get(0)));
            case "levenshteinDistance" -> new SqlExpr.FunctionCall("levenshteinDistance",
                    List.of(c.apply(params.get(0)), c.apply(params.get(1))));
            case "ascii" -> new SqlExpr.FunctionCall("ASCII", List.of(c.apply(params.get(0))));
            case "char" -> new SqlExpr.FunctionCall("CHR", List.of(c.apply(params.get(0))));
            case "hash" -> {
                // Check for HashType enum (SHA256, MD5, etc.)
                if (params.size() > 1 && params.get(1) instanceof PackageableElementPtr ptr
                        && ptr.fullPath().contains("SHA256")) {
                    yield new SqlExpr.FunctionCall("SHA256", List.of(c.apply(params.get(0))));
                }
                yield new SqlExpr.FunctionCall("hash", List.of(c.apply(params.get(0))));
            }
            case "lpad" -> {
                SqlExpr str = c.apply(params.get(0));
                SqlExpr len = c.apply(params.get(1));
                SqlExpr fill = params.size() > 2 ? c.apply(params.get(2)) : new SqlExpr.StringLiteral(" ");
                SqlExpr lenCast = new SqlExpr.Cast(len, "Integer");
                yield new SqlExpr.CaseWhen(
                        new SqlExpr.Binary(new SqlExpr.FunctionCall("LENGTH", List.of(str)), ">=", len),
                        new SqlExpr.FunctionCall("LEFT", List.of(str, len)),
                        new SqlExpr.FunctionCall("LPAD", List.of(str, lenCast, fill)));
            }
            case "rpad" -> {
                SqlExpr str = c.apply(params.get(0));
                SqlExpr len = c.apply(params.get(1));
                SqlExpr fill = params.size() > 2 ? c.apply(params.get(2)) : new SqlExpr.StringLiteral(" ");
                SqlExpr lenCast = new SqlExpr.Cast(len, "Integer");
                yield new SqlExpr.CaseWhen(
                        new SqlExpr.Binary(new SqlExpr.FunctionCall("LENGTH", List.of(str)), ">=", len),
                        new SqlExpr.FunctionCall("LEFT", List.of(str, len)),
                        new SqlExpr.FunctionCall("RPAD", List.of(str, lenCast, fill)));
            }
            case "encodeBase64" -> new SqlExpr.FunctionCall("encodeBase64",
                    List.of(c.apply(params.get(0))));
            case "decodeBase64" -> new SqlExpr.FunctionCall("decodeBase64",
                    List.of(c.apply(params.get(0))));
            case "parseDate" -> new SqlExpr.Cast(c.apply(params.get(0)), "TimestampTZ");
            case "parseBoolean" -> new SqlExpr.Cast(c.apply(params.get(0)), "Boolean");

            // --- Date/time extraction: context-aware ---
            // Literal dates → function form (YEAR(x)), column refs → EXTRACT form
            case "year" -> {
                var arg = params.get(0);
                boolean isLiteral = unit.typeInfoFor(arg) != null && unit.typeInfoFor(arg).isDateType();
                yield new SqlExpr.FunctionCall(
                        isLiteral ? "year" : "extractYear", List.of(c.apply(arg)));
            }
            case "monthNumber", "month" -> {
                var arg = params.get(0);
                boolean isLiteral = unit.typeInfoFor(arg) != null && unit.typeInfoFor(arg).isDateType();
                yield new SqlExpr.FunctionCall(
                        isLiteral ? "month" : "extractMonth", List.of(c.apply(arg)));
            }
            case "dayOfMonth", "day" -> {
                var arg = params.get(0);
                boolean isLiteral = unit.typeInfoFor(arg) != null && unit.typeInfoFor(arg).isDateType();
                yield new SqlExpr.FunctionCall(
                        isLiteral ? "dayOfMonth" : "extractDay", List.of(c.apply(arg)));
            }
            case "hour" -> {
                var arg = params.get(0);
                boolean isLiteral = unit.typeInfoFor(arg) != null && unit.typeInfoFor(arg).isDateType();
                yield new SqlExpr.FunctionCall(
                        isLiteral ? "hour" : "extractHour", List.of(c.apply(arg)));
            }
            case "minute" -> new SqlExpr.FunctionCall("minute", List.of(c.apply(params.get(0))));
            case "second" -> new SqlExpr.FunctionCall("second", List.of(c.apply(params.get(0))));
            case "quarter" -> new SqlExpr.FunctionCall("quarter", List.of(c.apply(params.get(0))));
            case "quarterNumber" -> new SqlExpr.FunctionCall("quarterNumber",
                    List.of(c.apply(params.get(0))));
            case "dayOfWeek" -> new SqlExpr.FunctionCall("dayOfWeek",
                    List.of(c.apply(params.get(0))));
            case "dayOfWeekNumber" -> new SqlExpr.FunctionCall("dayOfWeekNumber",
                    List.of(c.apply(params.get(0))));
            case "dayOfYear" -> new SqlExpr.FunctionCall("dayOfYear",
                    List.of(c.apply(params.get(0))));
            case "weekOfYear" -> new SqlExpr.FunctionCall("weekOfYear",
                    List.of(c.apply(params.get(0))));

            // --- Date truncation ---
            case "firstDayOfMonth" -> new SqlExpr.FunctionCall("firstDayOfMonth",
                    List.of(c.apply(params.get(0))));
            case "firstDayOfYear" -> new SqlExpr.FunctionCall("firstDayOfYear",
                    List.of(c.apply(params.get(0))));
            case "firstDayOfQuarter" -> new SqlExpr.FunctionCall("firstDayOfQuarter",
                    List.of(c.apply(params.get(0))));
            case "firstHourOfDay" -> new SqlExpr.FunctionCall("firstHourOfDay",
                    List.of(c.apply(params.get(0))));
            case "firstMillisecondOfSecond" -> new SqlExpr.FunctionCall("firstMillisecondOfSecond",
                    List.of(c.apply(params.get(0))));

            // --- Epoch conversion ---
            case "fromEpochValue" -> new SqlExpr.FunctionCall("fromEpochValue",
                    List.of(c.apply(params.get(0))));
            case "toEpochValue" -> new SqlExpr.FunctionCall("toEpochValue",
                    List.of(c.apply(params.get(0))));
            case "datePart" -> {
                // datePart on year/year-month uses STRFTIME; on full date/timestamp uses
                // DATE_TRUNC
                if (params.get(0) instanceof CStrictDate sd) {
                    String raw = sd.value();
                    if (raw.matches("\\d{4}")) {
                        yield new SqlExpr.FunctionCall("STRFTIME",
                                List.of(c.apply(params.get(0)), new SqlExpr.StringLiteral("%Y")));
                    } else if (raw.matches("\\d{4}-\\d{2}")) {
                        yield new SqlExpr.FunctionCall("STRFTIME",
                                List.of(c.apply(params.get(0)), new SqlExpr.StringLiteral("%Y-%m")));
                    }
                }
                yield new SqlExpr.FunctionCall("DATE_TRUNC",
                        List.of(new SqlExpr.StringLiteral("day"), c.apply(params.get(0))));
            }
            case "dateDiff" -> {
                SqlExpr start = c.apply(params.get(0));
                SqlExpr end = c.apply(params.get(1));
                String dunit = "DAY";
                if (params.size() > 2 && params.get(2) instanceof EnumValue ev) {
                    dunit = switch (ev.value()) {
                        case "DAYS" -> "DAY";
                        case "HOURS" -> "HOUR";
                        case "MINUTES" -> "MINUTE";
                        case "SECONDS" -> "SECOND";
                        case "MONTHS" -> "MONTH";
                        case "YEARS" -> "YEAR";
                        case "WEEKS" -> "WEEK";
                        default -> ev.value();
                    };
                }
                if ("WEEK".equals(dunit)) {
                    // WEEKS: (DATE_DIFF('day', start, end) + CAST(EXTRACT(DOW FROM start) AS INTEGER)) // 7
                    // DOW returns 0-6 (Sunday=0), matching Pure's week boundary counting
                    SqlExpr dayDiff = new SqlExpr.FunctionCall("dateDiff",
                            List.of(new SqlExpr.Literal("'day'"), start, end));
                    SqlExpr dow = new SqlExpr.FunctionCall("extractDow", List.of(start));
                    // Use RawCast to produce "CAST(... AS INTEGER)" bypassing dialect type mapping
                    SqlExpr castDow = new SqlExpr.RawCast(dow, "INTEGER");
                    SqlExpr sum = new SqlExpr.Binary(dayDiff, "+", castDow);
                    // Produce: (sum) // 7 — no outer parens around the whole expression
                    yield new SqlExpr.IntegerDivide(sum, new SqlExpr.Literal("7"));
                }
                yield new SqlExpr.FunctionCall("dateDiff",
                        List.of(new SqlExpr.Literal("'" + dunit + "'"), start, end));
            }
            case "date" -> {
                SqlExpr year = c.apply(params.get(0));
                if (params.size() == 1) {
                    // Year only: STRFTIME(MAKE_DATE(year,1,1), '%Y')
                    yield new SqlExpr.FunctionCall("STRFTIME", List.of(
                            new SqlExpr.FunctionCall("makeDate", List.of(year,
                                    new SqlExpr.Literal("1"), new SqlExpr.Literal("1"))),
                            new SqlExpr.StringLiteral("%Y")));
                } else if (params.size() == 2) {
                    // Year-month: STRFTIME(MAKE_DATE(year,month,1), '%Y-%m')
                    yield new SqlExpr.FunctionCall("STRFTIME", List.of(
                            new SqlExpr.FunctionCall("makeDate", List.of(year,
                                    c.apply(params.get(1)), new SqlExpr.Literal("1"))),
                            new SqlExpr.StringLiteral("%Y-%m")));
                } else if (params.size() == 3) {
                    // Full date: MAKE_DATE(year,month,day)
                    yield new SqlExpr.FunctionCall("makeDate",
                            List.of(year, c.apply(params.get(1)), c.apply(params.get(2))));
                } else if (params.size() == 4) {
                    // To hour: STRFTIME(MAKE_TIMESTAMP(y,m,d,h,0,0), '%Y-%m-%dT%H')
                    yield new SqlExpr.FunctionCall("STRFTIME", List.of(
                            new SqlExpr.FunctionCall("MAKE_TIMESTAMP", List.of(
                                    year, c.apply(params.get(1)), c.apply(params.get(2)),
                                    c.apply(params.get(3)), new SqlExpr.Literal("0"), new SqlExpr.Literal("0"))),
                            new SqlExpr.StringLiteral("%Y-%m-%dT%H")));
                } else if (params.size() == 5) {
                    // To minute: STRFTIME(MAKE_TIMESTAMP(y,m,d,h,min,0), '%Y-%m-%dT%H:%M')
                    yield new SqlExpr.FunctionCall("STRFTIME", List.of(
                            new SqlExpr.FunctionCall("MAKE_TIMESTAMP", List.of(
                                    year, c.apply(params.get(1)), c.apply(params.get(2)),
                                    c.apply(params.get(3)), c.apply(params.get(4)), new SqlExpr.Literal("0"))),
                            new SqlExpr.StringLiteral("%Y-%m-%dT%H:%M")));
                } else {
                    // To second: REGEXP_REPLACE(STRFTIME(MAKE_TIMESTAMP(y,m,d,h,min,sec), ...),
                    // '0{1,5}$', '')
                    SqlExpr makeTs = new SqlExpr.FunctionCall("MAKE_TIMESTAMP", List.of(
                            year, c.apply(params.get(1)), c.apply(params.get(2)),
                            c.apply(params.get(3)), c.apply(params.get(4)), c.apply(params.get(5))));
                    yield new SqlExpr.FunctionCall("REGEXP_REPLACE", List.of(
                            new SqlExpr.FunctionCall("STRFTIME", List.of(
                                    makeTs, new SqlExpr.StringLiteral("%Y-%m-%dT%H:%M:%S.%f"))),
                            new SqlExpr.StringLiteral("0{1,5}$"),
                            new SqlExpr.StringLiteral("")));
                }
            }
            case "timeBucket" -> {
                // timeBucket(date, quantity, DurationUnit)
                // Context-aware: literal dates use TO_DAYS+origin+CAST, column refs use INTERVAL
                var dateParam = params.get(0);
                SqlExpr dateExpr = c.apply(dateParam);
                SqlExpr quantityExpr = c.apply(params.get(1));
                String tbUnit = "days";
                if (params.size() > 2 && params.get(2) instanceof EnumValue ev) {
                    tbUnit = ev.value().toLowerCase();
                }
                TypeInfo dateTypeInfo = unit.typeInfoFor(dateParam);
                if (dateTypeInfo != null && dateTypeInfo.isDateType()) {
                    // Literal date path: TO_DAYS/TO_WEEKS + origin + CAST
                    // Pass type info so dialect can render correctly
                    String castType = (dateTypeInfo.scalarType() != null
                            && dateTypeInfo.scalarType() == GenericType.Primitive.DATE_TIME)
                            ? "TIMESTAMP_NS" : "DATE";
                    yield new SqlExpr.FunctionCall("timeBucketScalar",
                            List.of(quantityExpr, new SqlExpr.StringLiteral(tbUnit),
                                    dateExpr, new SqlExpr.StringLiteral(castType)));
                }
                // Column ref path: INTERVAL syntax
                yield new SqlExpr.FunctionCall("timeBucket",
                        List.of(quantityExpr, new SqlExpr.StringLiteral(tbUnit), dateExpr));
            }
            case "hasHour" -> {
                // Check date precision: hasHour is true for DateTime, false for StrictDate
                if (params.get(0) instanceof CDateTime) {
                    yield new SqlExpr.Literal("TRUE");
                } else if (params.get(0) instanceof CStrictDate) {
                    yield new SqlExpr.Literal("FALSE");
                }
                yield new SqlExpr.FunctionCall("HOUR", List.of(c.apply(params.get(0))));
            }
            case "hasMinute" -> {
                // Check date precision: hasMinute is true only for DateTime with minute
                // component
                if (params.get(0) instanceof CDateTime dt) {
                    String raw = dt.value();
                    // Dates like %2015-04-15T17 have hour but no minute
                    boolean hasMin = raw.matches(".*T\\d{2}:\\d{2}.*");
                    yield new SqlExpr.Literal(hasMin ? "TRUE" : "FALSE");
                } else if (params.get(0) instanceof CStrictDate) {
                    yield new SqlExpr.Literal("FALSE");
                }
                yield new SqlExpr.FunctionCall("MINUTE", List.of(c.apply(params.get(0))));
            }
            case "hasMonth" -> {
                // Pure dates: year-only has no month, everything else has month
                if (params.get(0) instanceof CStrictDate || params.get(0) instanceof CDateTime) {
                    yield new SqlExpr.Literal("TRUE");
                }
                yield new SqlExpr.FunctionCall("MONTH", List.of(c.apply(params.get(0))));
            }

            // --- Date constants ---
            case "now" -> new SqlExpr.Literal("CURRENT_TIMESTAMP");
            case "today" -> new SqlExpr.Literal("CURRENT_DATE");

            // --- Date comparisons: wrap both sides in DATE_TRUNC('day', ...) ---
            case "isOnDay", "isAfterDay", "isBeforeDay", "isOnOrAfterDay", "isOnOrBeforeDay" -> {
                String dateOp = switch (funcName) {
                    case "isOnDay" -> "=";
                    case "isAfterDay" -> ">";
                    case "isBeforeDay" -> "<";
                    case "isOnOrAfterDay" -> ">=";
                    case "isOnOrBeforeDay" -> "<=";
                    default -> "=";
                };
                SqlExpr left = new SqlExpr.FunctionCall("dateTruncDay", List.of(c.apply(params.get(0))));
                SqlExpr right = new SqlExpr.FunctionCall("dateTruncDay", List.of(c.apply(params.get(1))));
                yield new SqlExpr.Grouped(new SqlExpr.Binary(left, dateOp, right));
            }

            // --- Bitwise ---
            case "bitAnd" -> new SqlExpr.Binary(
                    new SqlExpr.Cast(c.apply(params.get(0)), "Integer"), "&",
                    new SqlExpr.Cast(c.apply(params.get(1)), "Integer"));
            case "bitOr" -> new SqlExpr.Binary(
                    new SqlExpr.Cast(c.apply(params.get(0)), "Integer"), "|",
                    new SqlExpr.Cast(c.apply(params.get(1)), "Integer"));
            case "bitXor" -> new SqlExpr.FunctionCall("bitXor",
                    List.of(new SqlExpr.Cast(c.apply(params.get(0)), "Integer"),
                            new SqlExpr.Cast(c.apply(params.get(1)), "Integer")));
            case "bitNot" -> new SqlExpr.Unary("~",
                    new SqlExpr.Cast(c.apply(params.get(0)), "Integer"));
            case "bitShiftLeft" -> new SqlExpr.Binary(
                    new SqlExpr.Cast(c.apply(params.get(0)), "Integer"), "<<",
                    c.apply(params.get(1)));
            case "bitShiftRight" -> new SqlExpr.Binary(
                    new SqlExpr.Cast(c.apply(params.get(0)), "Integer"), ">>",
                    c.apply(params.get(1)));
            case "xor" -> {
                // Boolean xor: (A AND NOT B) OR (NOT A AND B)
                SqlExpr a = c.apply(params.get(0));
                SqlExpr b = c.apply(params.get(1));
                yield new SqlExpr.Or(List.of(
                        new SqlExpr.And(List.of(a, new SqlExpr.Not(b))),
                        new SqlExpr.And(List.of(new SqlExpr.Not(a), b))));
            }

            // --- Collection/list ---
            case "size", "count" -> new SqlExpr.FunctionCall("COUNT",
                    params.stream().map(c).collect(Collectors.toList()));
            case "at" -> new SqlExpr.FunctionCall("listExtract",
                    List.of(c.apply(params.get(0)),
                            new SqlExpr.Binary(c.apply(params.get(1)), "+",
                                    new SqlExpr.Literal("1"))));
            case "head", "first" -> new SqlExpr.FunctionCall("listExtract",
                    List.of(c.apply(params.get(0)), new SqlExpr.Literal("1")));
            case "last" -> new SqlExpr.FunctionCall("listExtract",
                    List.of(c.apply(params.get(0)),
                            new SqlExpr.FunctionCall("listLength",
                                    List.of(c.apply(params.get(0))))));
            case "tail" -> new SqlExpr.FunctionCall("listSlice",
                    List.of(c.apply(params.get(0)), new SqlExpr.Literal("2"),
                            new SqlExpr.FunctionCall("listLength",
                                    List.of(c.apply(params.get(0))))));
            case "init" -> new SqlExpr.FunctionCall("listSlice",
                    List.of(c.apply(params.get(0)), new SqlExpr.Literal("1"),
                            new SqlExpr.Binary(
                                    new SqlExpr.FunctionCall("listLength",
                                            List.of(c.apply(params.get(0)))),
                                    "-", new SqlExpr.Literal("1"))));
            case "add" -> {
                if (params.size() > 2) {
                    yield new SqlExpr.FunctionCall("listAppend",
                            List.of(c.apply(params.get(0)), c.apply(params.get(2))));
                }
                yield new SqlExpr.FunctionCall("listAppend",
                        List.of(c.apply(params.get(0)), c.apply(params.get(1))));
            }
            case "concatenate" -> new SqlExpr.FunctionCall("listConcat",
                    List.of(c.apply(params.get(0)), c.apply(params.get(1))));
            case "take" -> new SqlExpr.FunctionCall("listSlice",
                    List.of(c.apply(params.get(0)), new SqlExpr.Literal("1"),
                            c.apply(params.get(1))));
            case "drop" -> new SqlExpr.FunctionCall("listSlice",
                    List.of(c.apply(params.get(0)),
                            new SqlExpr.Binary(c.apply(params.get(1)), "+",
                                    new SqlExpr.Literal("1")),
                            new SqlExpr.FunctionCall("listLength",
                                    List.of(c.apply(params.get(0))))));
            case "slice" -> new SqlExpr.FunctionCall("listSlice",
                    List.of(c.apply(params.get(0)),
                            new SqlExpr.Binary(c.apply(params.get(1)), "+",
                                    new SqlExpr.Literal("1")),
                            c.apply(params.get(2))));

            // --- Aggregates (list-context aware) ---
            case "sum" -> {
                if (firstArgIsList)
                    yield new SqlExpr.FunctionCall("listSum", List.of(c.apply(params.get(0))));
                yield c.apply(params.get(0)); // scalar identity
            }
            case "average", "mean" -> {
                if (firstArgIsList)
                    yield new SqlExpr.FunctionCall("listAvg", List.of(c.apply(params.get(0))));
                yield new SqlExpr.Cast(c.apply(params.get(0)), "Double"); // scalar cast
            }
            case "min" -> {
                if (params.size() > 1)
                    yield new SqlExpr.FunctionCall("LEAST",
                            params.stream().map(c).collect(Collectors.toList()));
                if (firstArgIsList)
                    yield new SqlExpr.FunctionCall("listMin", List.of(c.apply(params.get(0))));
                yield c.apply(params.get(0)); // scalar identity
            }
            case "max" -> {
                if (params.size() > 1)
                    yield new SqlExpr.FunctionCall("GREATEST",
                            params.stream().map(c).collect(Collectors.toList()));
                if (firstArgIsList)
                    yield new SqlExpr.FunctionCall("listMax", List.of(c.apply(params.get(0))));
                yield c.apply(params.get(0)); // scalar identity
            }
            case "greatest" -> new SqlExpr.FunctionCall(
                    firstArgIsList ? "listMax" : "GREATEST",
                    params.stream().map(c).collect(Collectors.toList()));
            case "least" -> new SqlExpr.FunctionCall(
                    firstArgIsList ? "listMin" : "LEAST",
                    params.stream().map(c).collect(Collectors.toList()));
            case "median" -> new SqlExpr.FunctionCall(
                    firstArgIsList ? "listMedian" : "median",
                    params.stream().map(c).collect(Collectors.toList()));
            case "mode" -> new SqlExpr.FunctionCall(
                    firstArgIsList ? "listMode" : "mode",
                    params.stream().map(c).collect(Collectors.toList()));

            // --- Statistical aggregates (list-context aware) ---
            case "stdDev", "stdDevSample" -> new SqlExpr.FunctionCall(
                    firstArgIsList ? "listStdDevSample" : "stdDevSample",
                    params.stream().map(c).collect(Collectors.toList()));
            case "stdDevPopulation" -> new SqlExpr.FunctionCall(
                    firstArgIsList ? "listStdDevPopulation" : "stdDevPopulation",
                    params.stream().map(c).collect(Collectors.toList()));
            case "variance", "varianceSample" -> new SqlExpr.FunctionCall(
                    firstArgIsList ? "listVarianceSample" : "varianceSample",
                    params.stream().map(c).collect(Collectors.toList()));
            case "variancePopulation" -> new SqlExpr.FunctionCall(
                    firstArgIsList ? "listVariancePopulation" : "variancePopulation",
                    params.stream().map(c).collect(Collectors.toList()));
            case "corr" -> new SqlExpr.FunctionCall(
                    firstArgIsList ? "listCorr" : "corr",
                    List.of(c.apply(params.get(0)), c.apply(params.get(1))));
            case "covarSample" -> new SqlExpr.FunctionCall(
                    firstArgIsList ? "listCovarSample" : "covarSample",
                    List.of(c.apply(params.get(0)), c.apply(params.get(1))));
            case "covarPopulation" -> new SqlExpr.FunctionCall(
                    firstArgIsList ? "listCovarPopulation" : "covarPopulation",
                    List.of(c.apply(params.get(0)), c.apply(params.get(1))));
            case "percentile", "percentileCont" -> new SqlExpr.FunctionCall(
                    firstArgIsList ? "listPercentileCont" : "percentileCont",
                    params.stream().map(c).collect(Collectors.toList()));
            case "percentileDisc" -> new SqlExpr.FunctionCall(
                    firstArgIsList ? "listPercentileDisc" : "percentileDisc",
                    params.stream().map(c).collect(Collectors.toList()));

            // --- Analytical helpers ---
            case "maxBy" -> new SqlExpr.FunctionCall("maxBy",
                    List.of(c.apply(params.get(0)), c.apply(params.get(1))));
            case "minBy" -> new SqlExpr.FunctionCall("minBy",
                    List.of(c.apply(params.get(0)), c.apply(params.get(1))));

            // --- Misc ---
            case "generateGuid" -> new SqlExpr.FunctionCall("generateGuid", List.of());
            case "between" -> new SqlExpr.And(List.of(
                    new SqlExpr.Binary(c.apply(params.get(0)), ">=", c.apply(params.get(1))),
                    new SqlExpr.Binary(c.apply(params.get(0)), "<=", c.apply(params.get(2)))));
            case "eq" -> buildComparison(params, "=", c, mapping, tableAlias);
            case "type" -> new SqlExpr.FunctionCall("typeOf", List.of(c.apply(params.get(0))));

            // --- Pass-through for non-SQL functions ---
            case "toOne", "toMany", "eval", "forAll", "exists",
                    "list", "pair", "map", "fold", "match", "zip",
                    "range", "cast", "toVariant", "letWithParam",
                    "filter", "groupBy", "select", "write",
                    "compare", "sort", "comparator" -> {
                // These are Pure-level functions that should pass through the first arg
                if (!params.isEmpty()) {
                    yield c.apply(params.get(0));
                }
                throw new PureCompileException(
                        "PlanGenerator: pass-through function '" + funcName + "' has no parameters");
            }

            // --- Let binding ---
            case "letAsLastStatement", "letFunction" -> {
                // Let bindings: compile the value expression
                // Zero-param calls to letAsLastStatement are test-defined functions, not let
                // bindings
                if (params.size() >= 2) {
                    yield c.apply(params.get(1));
                }
                if (!params.isEmpty()) {
                    yield c.apply(params.get(0));
                }
                throw new PureCompileException(
                        "PlanGenerator: unsupported scalar function '" + funcName + "'");
            }

            // --- Block (multi-statement let bindings) ---
            case "block" -> {
                // |let x = 42; $x; → block wraps sequence, last expr is result
                if (!params.isEmpty()) {
                    yield c.apply(params.getLast());
                }
                throw new PureCompileException(
                        "PlanGenerator: block has no statements");
            }

            // --- String format ---
            case "format" -> {
                // Flatten Collection args (Pure passes format args as a list)
                // Emit semantic FunctionCall — dialect handles %t/%f/%r conversion
                SqlExpr fmtString = c.apply(params.get(0));
                java.util.List<SqlExpr> fmtArgs = new java.util.ArrayList<>();
                fmtArgs.add(fmtString);
                for (int fi = 1; fi < params.size(); fi++) {
                    var fp = params.get(fi);
                    if (fp instanceof Collection coll) {
                        for (var elem : coll.values()) {
                            fmtArgs.add(c.apply(elem));
                        }
                    } else {
                        fmtArgs.add(c.apply(fp));
                    }
                }
                yield new SqlExpr.FunctionCall("format", fmtArgs);
            }

            default -> throw new PureCompileException(
                    "PlanGenerator: unsupported scalar function '" + funcName + "'");
        };
    }

    /**
     * /** Checks if any param is a partial date (year-only or year-month
     * CStrictDate).
     */
    /** Checks if any param is a CStrictDate (partial OR full) for date-to-date equality. */
    private boolean hasPartialDate(List<ValueSpecification> params) {
        for (var p : params) {
            if (p instanceof CStrictDate) return true;
        }
        return false;
    }

    /**
     * Renders a param for date precision comparison: partial dates become string
     * literals.
     */
    private SqlExpr renderForDateComparison(ValueSpecification vs,
            java.util.function.Function<ValueSpecification, SqlExpr> c) {
        if (vs instanceof CStrictDate sd) {
            return new SqlExpr.StringLiteral(sd.value());
        }
        return c.apply(vs);
    }

    /**
     * Builds a comparison, generating EXISTS subquery for association paths.
     */
    private SqlExpr buildComparison(List<ValueSpecification> params, String op,
            java.util.function.Function<ValueSpecification, SqlExpr> c,
            RelationalMapping mapping, String tableAlias) {
        SqlExpr left = c.apply(params.get(0));
        SqlExpr right = c.apply(params.get(1));

        if (left instanceof SqlExpr.AssociationRef ar
                && mapping != null) {
            String assocProp = ar.assocProp();
            String targetCol = ar.targetCol();

            TypeInfo.AssociationTarget assocTarget = findAssociationInSidecar(assocProp);
            if (assocTarget != null) {
                String subAlias = "sub" + (tableAliasCounter++);
                String targetTableName = assocTarget.targetMapping().table().name();
                Join join = assocTarget.join();
                String srcTableName = mapping != null ? mapping.table().name() : null;

                if (join != null && srcTableName != null) {
                    String leftJoinCol = join.getColumnForTable(srcTableName);
                    String rightJoinCol = join.getColumnForTable(targetTableName);

                    SqlExpr correlation = new SqlExpr.Binary(
                            new SqlExpr.Column(subAlias, rightJoinCol),
                            "=",
                            new SqlExpr.Column(tableAlias, leftJoinCol));
                    SqlExpr filter = new SqlExpr.Binary(
                            new SqlExpr.Column(subAlias, targetCol),
                            op,
                            right);

                    SqlBuilder subquery = new SqlBuilder()
                            .addSelect(new SqlExpr.Literal("1"), null)
                            .from(dialect.quoteIdentifier(targetTableName), dialect.quoteIdentifier(subAlias))
                            .addWhere(new SqlExpr.And(List.of(correlation, filter)));
                    return new SqlExpr.Exists(subquery);
                }
            }
        }

        return new SqlExpr.Binary(left, op, right);
    }

    /** Checks if an AST node represents a list value via TypeInfo side table. */
    private boolean isListArg(ValueSpecification vs) {
        TypeInfo info = unit.types().get(vs);
        return info != null && info.isList();
    }

    // ========== Utility Methods ==========

    private String nextTableAlias() {
        return "t" + tableAliasCounter++;
    }

    /** Looks up the mapping for an AST node from the sidecar. */
    private RelationalMapping mappingFor(ValueSpecification vs) {
        return unit.types().get(vs).mapping();
    }

    // binaryOp DELETED — was thin wrapper, inlined into generateScalarFunction

    private long extractIntValue(ValueSpecification vs) {
        if (vs instanceof CInteger i)
            return i.value().longValue();
        if (vs instanceof CFloat f)
            return (long) f.value();
        throw new PureCompileException("Expected integer, got: " + vs.getClass().getSimpleName());
    }

    private String extractString(ValueSpecification vs) {
        if (vs instanceof CString s)
            return s.value();
        throw new PureCompileException("Expected string, got: " + vs.getClass().getSimpleName());
    }

    private String extractTableName(String content) {
        int dotIdx = content.lastIndexOf('.');
        return dotIdx > 0 ? content.substring(dotIdx + 1) : content;
    }

    private String extractClassName(ValueSpecification vs) {
        if (vs instanceof PackageableElementPtr pep) {
            String fqn = pep.fullPath();
            int idx = fqn.lastIndexOf("::");
            return idx > 0 ? fqn.substring(idx + 2) : fqn;
        }
        throw new PureCompileException(
                "PlanGenerator: expected PackageableElementPtr but got " + vs.getClass().getSimpleName());
    }

    private static String simpleName(String qualifiedName) {
        return TypeInfo.simpleName(qualifiedName);
    }
}
