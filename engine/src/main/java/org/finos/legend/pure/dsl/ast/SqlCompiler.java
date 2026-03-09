package org.finos.legend.pure.dsl.ast;

import org.finos.legend.engine.plan.GenericType;
import org.finos.legend.engine.plan.RelationType;
import org.finos.legend.engine.store.Join;
import org.finos.legend.engine.store.RelationalMapping;
import org.finos.legend.engine.transpiler.SQLDialect;
import org.finos.legend.pure.dsl.ModelContext;
import org.finos.legend.pure.dsl.PureCompileException;

import java.util.ArrayList;
import java.util.IdentityHashMap;
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
public class SqlCompiler {

    private final IdentityHashMap<ValueSpecification, TypeInfo> types;
    private final SQLDialect dialect;
    private final ModelContext modelContext;
    private int tableAliasCounter = 0;

    public SqlCompiler(IdentityHashMap<ValueSpecification, TypeInfo> types, SQLDialect dialect,
            ModelContext modelContext) {
        this.types = types;
        this.dialect = dialect;
        this.modelContext = modelContext;
    }

    private String nextTableAlias() {
        return "t" + tableAliasCounter++;
    }

    public SqlBuilder compile(TypedValueSpec tvs) {
        return switch (tvs.sourceKind()) {
            case SCALAR -> compileScalarQuery(tvs.ast(), tvs.mapping());
            case CLASS_INSTANCE -> {
                // Struct literal: walk AST to find the ClassInstance node
                ClassInstance ci = (ClassInstance) unwrapSource(tvs.ast());
                yield compileStructProject(
                        (AppliedFunction) tvs.ast(), ci);
            }
            case CLASS_ALL, TDS_LITERAL, RELATION ->
                compileRelation(tvs.ast(), tvs.mapping());
        };
    }

    /** Unwraps lambda/function chain to find the source ValueSpecification. */
    private ValueSpecification unwrapSource(ValueSpecification vs) {
        if (vs instanceof LambdaFunction lf && !lf.body().isEmpty())
            return unwrapSource(lf.body().getLast());
        if (vs instanceof AppliedFunction af && !af.parameters().isEmpty())
            return unwrapSource(af.parameters().get(0));
        return vs;
    }

    /**
     * Compiles a standalone scalar query → SELECT scalarExpr AS "value".
     * Used for queries like |42, |'hello', |%2024-01-15->adjust(...), etc.
     */
    private SqlBuilder compileScalarQuery(ValueSpecification ast, RelationalMapping mapping) {
        ValueSpecification body = ast;
        if (ast instanceof LambdaFunction lf && !lf.body().isEmpty()) {
            body = lf.body().getLast();
        }
        SqlExpr scalar = compileScalar(body, null, mapping);
        return new SqlBuilder().addSelect(scalar, dialect.quoteIdentifier("value"));
    }

    // ========== Relation Compilation ==========

    private SqlBuilder compileRelation(ValueSpecification vs, RelationalMapping mapping) {
        return switch (vs) {
            case AppliedFunction af -> compileFunction(af, mapping);
            case LambdaFunction lf -> {
                if (lf.body().isEmpty())
                    throw new PureCompileException("SqlCompiler: empty lambda body");

                yield compileRelation(lf.body().getLast(), mapping);
            }
            case

                    ClassInstance ci ->

                compileClassInstance(ci, mapping);
            default -> throw new PureCompileException(
                    "SqlCompiler: cannot compile: " + vs.getClass().getSimpleName());
        };

    }

    private SqlBuilder compileClassInstance(ClassInstance ci, RelationalMapping mapping) {
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

                yield compileTdsLiteral(tds);
            }
            case "instance" ->

            {
                var data = (CleanAstBuilder.InstanceData) ci.value();

                yield compileStructLiteral(data);
            }
            default -> throw new PureCompileException("SqlCompiler: unsupported ClassInstance type: " + ci.type());

        };
    }

    /**
     * Compiles a struct literal ^ClassName(field=val, ...) to SQL VALUES.
     * Renders the instance as a dialect-specific struct inside VALUES.
     */
    private SqlBuilder compileStructLiteral(CleanAstBuilder.InstanceData data) {
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
                    "SqlCompiler: cannot render struct value from " + vs.getClass().getSimpleName());
        };
    }

    /**
     * Compiles a parsed TdsLiteral to VALUES SQL.
     * Receives structured data from CleanCompiler — no string parsing here.
     */
    private SqlBuilder compileTdsLiteral(org.finos.legend.pure.dsl.TdsLiteral tds) {
        List<String> quotedCols = tds.columns().stream()
                .map(c -> dialect.quoteIdentifier(c.name())).toList();

        List<List<SqlExpr>> rows = tds.rows().stream()
                .map(row -> row.stream().map(this::formatTdsValue).toList())
                .toList();

        return new SqlBuilder()
                .selectStar()
                .fromValues(rows, dialect.quoteIdentifier("_tds"), quotedCols);
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

    private SqlBuilder compileFunction(AppliedFunction af, RelationalMapping mapping) {
        String funcName = simpleName(af.function());
        return switch (funcName) {
            case "getAll" -> compileGetAll(af, mapping);
            case "table", "tableReference" -> compileTableAccess(af, mapping);
            case "filter" -> compileFilter(af, mapping);
            case "project" -> compileProject(af, mapping);
            case "sort" -> compileSort(af, mapping);
            case "limit", "take" -> compileLimit(af, mapping);
            case "drop" -> compileDrop(af, mapping);
            case "slice" -> compileSlice(af, mapping);
            case "first" -> compileFirst(af, mapping);
            case "distinct" -> compileDistinct(af, mapping);
            case "select" -> compileSelect(af, mapping);
            case "rename" -> compileRename(af, mapping);
            case "concatenate", "union" -> compileConcatenate(af, mapping);
            case "groupBy" -> compileGroupBy(af, mapping);
            case "aggregate" -> compileAggregate(af, mapping);
            case "extend" -> compileExtend(af, mapping);
            case "join" -> compileJoin(af, mapping);
            case "from" -> compileFrom(af, mapping);
            case "flatten" -> {
                // flatten passes through to source — unnests in relational context
                yield compileRelation(af.parameters().get(0), mapping);
            }
            case "pivot" ->

            {
                // pivot not yet SQL-supported — compile source for now
                yield compileRelation(af.parameters().get(0), mapping);
            }
            default -> throw new PureCompileException("SqlCompiler: unsupported function '" + funcName + "'");

        };
    }

    // ========== getAll / table ==========

    private SqlBuilder compileGetAll(AppliedFunction af, RelationalMapping mapping) {
        String tableName;
        if (mapping != null) {
            tableName = mapping.table().name();
        } else {
            tableName = extractClassName(af.parameters().get(0));
        }
        String alias = nextTableAlias();
        return new SqlBuilder()
                .selectStar()
                .from(dialect.quoteIdentifier(tableName), dialect.quoteIdentifier(alias));
    }

    private SqlBuilder compileTableAccess(AppliedFunction af, RelationalMapping mapping) {
        String content = extractString(af.parameters().get(0));
        String tableName = extractTableName(content);
        if (mapping != null) {
            tableName = mapping.table().name();
        }
        return new SqlBuilder()
                .selectStar()
                .from(dialect.quoteIdentifier(tableName), dialect.quoteIdentifier("t0"));
    }

    // ========== filter ==========

    private SqlBuilder compileFilter(AppliedFunction af, RelationalMapping mapping) {
        List<ValueSpecification> params = af.parameters();
        SqlBuilder source = compileRelation(params.get(0), mapping);
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
                throw new PureCompileException("SqlCompiler: source has no FROM alias for filter inlining");
            String tableAlias = unquote(source.getFromAlias());
            SqlExpr whereClause = compileScalar(lambda.body().get(0), paramName, mapping, tableAlias);
            source.addWhere(whereClause);
            return source;
        }
        // Subquery wrapping — columns resolve by name, no prefix needed
        SqlExpr whereClause = compileScalar(lambda.body().get(0), paramName, mapping);
        return new SqlBuilder()
                .selectStar()
                .fromSubquery(source, "filter_src")
                .addWhere(whereClause);
    }

    // ========== project ==========

    private SqlBuilder compileProject(AppliedFunction af, RelationalMapping mapping) {
        List<ValueSpecification> params = af.parameters();

        // Step 1: Compile the source (getAll, filter, etc.)
        SqlBuilder source = compileRelation(params.get(0), mapping);

        // Step 2: Extract projection lambdas and aliases
        List<ValueSpecification> lambdaSpecs;
        List<String> aliases;
        if (params.get(1) instanceof Collection coll) {
            lambdaSpecs = coll.values();
            aliases = params.size() > 2 ? extractStringList(params.get(2)) : List.of();
        } else {
            lambdaSpecs = params.subList(1, params.size());
            aliases = List.of();
        }

        // Step 3: Determine table alias from source builder
        if (source.getFromAlias() == null)
            throw new PureCompileException("SqlCompiler: source has no FROM alias for project");
        String tableAlias = unquote(source.getFromAlias());

        // Step 4: Can we inline columns into source, or do we need a subquery?
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

        // Step 5: Build SELECT columns
        for (int i = 0; i < lambdaSpecs.size(); i++) {
            List<String> propPath = extractPropertyPath(lambdaSpecs.get(i));
            String alias = i < aliases.size() ? aliases.get(i) : propPath.getLast();

            if (propPath.size() > 1) {
                if (modelContext == null)
                    throw new PureCompileException(
                            "SqlCompiler: association navigation requires modelContext for property '" + propPath.get(0)
                                    + "'");
                // Association navigation: e.g., ["items", "productName"]
                String assocProp = propPath.get(0);
                String leafProp = propPath.getLast();

                AssocJoinInfo joinInfo = assocJoins.get(assocProp);
                String targetAlias;
                RelationalMapping targetMapping;

                if (joinInfo == null) {
                    var assocResult = resolveAssociationTarget(assocProp, mapping);
                    targetMapping = assocResult.mapping();
                    targetAlias = "j" + (assocJoins.size() + 1);
                    assocJoins.put(assocProp, new AssocJoinInfo(targetAlias, targetMapping, assocResult.join()));
                } else {
                    targetAlias = joinInfo.alias();
                    targetMapping = joinInfo.mapping();
                }

                SqlExpr colExpr = resolveColumnExpr(leafProp, targetMapping, targetAlias);
                builder.addSelect(colExpr, dialect.quoteIdentifier(alias));
            } else {
                // Simple single-hop property
                String propertyName = propPath.getLast();
                SqlExpr colExpr = resolveColumnExpr(propertyName, mapping, tableAlias);
                builder.addSelect(colExpr, dialect.quoteIdentifier(alias));
            }
        }

        // Step 6: Add LEFT OUTER JOINs for association projections
        for (var entry : assocJoins.entrySet()) {
            AssocJoinInfo info = entry.getValue();
            Join join = info.join();
            if (join != null && mapping != null) {
                String targetTableName = info.mapping().table().name();
                String leftCol = join.getColumnForTable(mapping.table().name());
                String rightCol = join.getColumnForTable(targetTableName);
                SqlExpr onCondition = new SqlExpr.Binary(
                        new SqlExpr.Column(tableAlias, leftCol), "=",
                        new SqlExpr.Column(info.alias(), rightCol));
                builder.addJoin(SqlBuilder.JoinType.LEFT,
                        dialect.quoteIdentifier(targetTableName),
                        dialect.quoteIdentifier(info.alias()), onCondition);
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
    private SqlBuilder compileStructProject(AppliedFunction af, ClassInstance structCi) {
        CleanAstBuilder.InstanceData structData = (CleanAstBuilder.InstanceData) structCi.value();
        List<ValueSpecification> params = af.parameters();

        // Look up struct type info from CleanCompiler's side table
        TypeInfo structTypeInfo = types.get(structCi);
        RelationType structType = structTypeInfo != null ? structTypeInfo.relationType() : null;

        // Build the struct source
        SqlBuilder structSource = compileStructLiteral(structData);

        // Extract class alias (lowercased simple name)
        String className = structData.className();
        String simpleName = className.contains("::") ? className.substring(className.lastIndexOf("::") + 2) : className;
        String structAlias = Character.toLowerCase(simpleName.charAt(0)) + simpleName.substring(1);
        String srcAlias = "struct_src";

        // Extract projection ColSpecs
        List<ValueSpecification> lambdaSpecs;
        List<String> aliases;
        if (params.get(1) instanceof ClassInstance ci && "colSpecArray".equals(ci.type())) {
            ColSpecArray csa = (ColSpecArray) ci.value();
            lambdaSpecs = new ArrayList<>();
            aliases = new ArrayList<>();
            for (ColSpec spec : csa.colSpecs()) {
                if (spec.function1() != null) {
                    lambdaSpecs.add(spec.function1());
                } else {
                    lambdaSpecs.add(new LambdaFunction(List.of(new Variable("x")),
                            new AppliedProperty(spec.name(), List.of(new Variable("x")))));
                }
                aliases.add(spec.name());
            }
        } else if (params.get(1) instanceof Collection coll) {
            lambdaSpecs = coll.values();
            aliases = params.size() > 2 ? extractStringList(params.get(2)) : List.of();
        } else {
            lambdaSpecs = params.subList(1, params.size());
            aliases = List.of();
        }

        // Track UNNEST joins needed for array properties
        int unnestCounter = 0;

        // Maps: array property name → (joinAlias, elemAlias)
        var arrayAliases = new java.util.LinkedHashMap<String, String[]>();

        SqlBuilder builder = new SqlBuilder();

        for (int i = 0; i < lambdaSpecs.size(); i++) {
            List<String> propPath = extractPropertyPath(lambdaSpecs.get(i));
            String alias = i < aliases.size() ? aliases.get(i) : propPath.getLast();

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

    /**
     * Extracts the full property path from a lambda, e.g., ["addresses", "street"].
     */
    private List<String> extractPropertyPath(ValueSpecification vs) {
        if (vs instanceof LambdaFunction lf && !lf.body().isEmpty()) {
            return extractPropertyChain(lf.body().get(0));
        }
        // ColSpec like ~three:x|$x.values.val → treat as column name
        if (vs instanceof ClassInstance ci && ci.value() instanceof ColSpec cs) {
            return List.of(cs.name());
        }
        throw new PureCompileException(
                "SqlCompiler: cannot extract property path from " + vs.getClass().getSimpleName());
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
                "SqlCompiler: cannot extract property chain from " + vs.getClass().getSimpleName());
    }

    /**
     * Resolves a property to its column expression, handling expression mappings.
     */
    private SqlExpr resolveColumnExpr(String propertyName, RelationalMapping mapping, String alias) {
        if (mapping == null)
            throw new PureCompileException(
                    "SqlCompiler: resolveColumnExpr requires mapping for property '" + propertyName + "'");
        if (propertyName == null)
            throw new PureCompileException("SqlCompiler: resolveColumnExpr requires non-null property name");
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
     * Resolves an association property to its target RelationalMapping and Join.
     * Uses ModelContext.findAssociationByProperty for proper resolution.
     *
     * @return target mapping and join, or throws if not found
     */
    private AssociationResult resolveAssociationTarget(String assocProp,
            RelationalMapping sourceMapping) {
        if (modelContext == null || sourceMapping == null) {
            throw new PureCompileException(
                    "SqlCompiler: cannot resolve association '" + assocProp + "' — no model context");
        }

        String sourceClassName = sourceMapping.pureClass().name();
        var assocNav = modelContext.findAssociationByProperty(sourceClassName, assocProp);
        if (assocNav.isEmpty()) {
            throw new PureCompileException(
                    "SqlCompiler: no association found for property '" + assocProp
                            + "' on class '" + sourceClassName + "'");
        }

        String targetClassName = assocNav.get().targetClassName();
        var targetMapping = modelContext.findMapping(targetClassName);
        if (targetMapping.isEmpty()) {
            throw new PureCompileException(
                    "SqlCompiler: no mapping found for association target class '" + targetClassName + "'");
        }

        return new AssociationResult(targetMapping.get(), assocNav.get().join());
    }

    /**
     * Result of resolving an association: the target mapping and the join to use.
     */
    private record AssociationResult(RelationalMapping mapping, Join join) {
    }

    /**
     * Resolves the RelationalMapping for a relation expression by finding the
     * getAll class.
     */

    // ========== sort ==========

    private SqlBuilder compileSort(AppliedFunction af, RelationalMapping mapping) {
        List<ValueSpecification> params = af.parameters();
        SqlBuilder source = compileRelation(params.get(0), mapping);

        // Parse sort specs
        List<SqlBuilder.OrderByColumn> sortCols;
        if (params.size() >= 2 && params.get(1) instanceof CString colStr) {
            // Legacy: sort('colName', 'asc'/'desc')
            String dir = "ASC";
            if (params.size() >= 3 && params.get(2) instanceof CString dirStr) {
                dir = dirStr.value().toUpperCase();
            }
            sortCols = List.of(new SqlBuilder.OrderByColumn(
                    new SqlExpr.ColumnRef(colStr.value()),
                    "ASC".equals(dir) ? SqlBuilder.SortDirection.ASC : SqlBuilder.SortDirection.DESC,
                    SqlBuilder.NullsPosition.DEFAULT));
        } else {
            sortCols = compileSortSpecs(params.get(1), mapping);
        }

        // Minimize wrapping: if source has no ORDER BY, add directly
        if (!source.hasOrderBy()) {
            for (var sc : sortCols) {
                source.addOrderBy(sc.expression(), sc.direction(), sc.nulls());
            }
            return source;
        }

        // Source already has ORDER BY — wrap in subquery
        SqlBuilder wrapper = new SqlBuilder()
                .selectStar()
                .fromSubquery(source, "sort_src");
        for (var sc : sortCols) {
            wrapper.addOrderBy(sc.expression(), sc.direction(), sc.nulls());
        }
        return wrapper;
    }

    private List<SqlBuilder.OrderByColumn> compileSortSpecs(ValueSpecification vs, RelationalMapping mapping) {
        List<SqlBuilder.OrderByColumn> result = new ArrayList<>();
        List<ValueSpecification> specs = (vs instanceof Collection coll) ? coll.values() : List.of(vs);
        for (var spec : specs) {
            result.add(compileSingleSortSpec(spec, mapping));
        }
        return result;
    }

    private SqlBuilder.OrderByColumn compileSingleSortSpec(ValueSpecification vs, RelationalMapping mapping) {
        if (vs instanceof ClassInstance ci && "sortInfo".equals(ci.type())) {
            if (ci.value() instanceof ColSpec cs) {
                return new SqlBuilder.OrderByColumn(
                        new SqlExpr.ColumnRef(cs.name()),
                        SqlBuilder.SortDirection.ASC,
                        SqlBuilder.NullsPosition.DEFAULT);
            }
        }
        if (vs instanceof AppliedFunction af) {
            String funcName = simpleName(af.function());
            if ("asc".equals(funcName) || "ascending".equals(funcName)
                    || "desc".equals(funcName) || "descending".equals(funcName)) {
                String col = extractColumnNameFromParam(af.parameters().get(0));
                SqlBuilder.SortDirection dir = ("asc".equals(funcName) || "ascending".equals(funcName))
                        ? SqlBuilder.SortDirection.ASC
                        : SqlBuilder.SortDirection.DESC;
                return new SqlBuilder.OrderByColumn(
                        new SqlExpr.ColumnRef(col), dir, SqlBuilder.NullsPosition.DEFAULT);
            }
        }
        if (vs instanceof CString s) {
            return new SqlBuilder.OrderByColumn(
                    new SqlExpr.ColumnRef(s.value()),
                    SqlBuilder.SortDirection.ASC,
                    SqlBuilder.NullsPosition.DEFAULT);
        }
        throw new PureCompileException("SqlCompiler: unsupported sort spec: " + vs);
    }

    // ========== limit / drop / slice / first ==========

    private SqlBuilder compileLimit(AppliedFunction af, RelationalMapping mapping) {
        SqlBuilder source = compileRelation(af.parameters().get(0), mapping);
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

    private SqlBuilder compileDrop(AppliedFunction af, RelationalMapping mapping) {
        SqlBuilder source = compileRelation(af.parameters().get(0), mapping);
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

    private SqlBuilder compileSlice(AppliedFunction af, RelationalMapping mapping) {
        SqlBuilder source = compileRelation(af.parameters().get(0), mapping);
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

    private SqlBuilder compileFirst(AppliedFunction af, RelationalMapping mapping) {
        SqlBuilder source = compileRelation(af.parameters().get(0), mapping);
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

    private SqlBuilder compileDistinct(AppliedFunction af, RelationalMapping mapping) {
        List<ValueSpecification> params = af.parameters();
        SqlBuilder source = compileRelation(params.get(0), mapping);
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

    private SqlBuilder compileSelect(AppliedFunction af, RelationalMapping mapping) {
        List<ValueSpecification> params = af.parameters();
        SqlBuilder source = compileRelation(params.get(0), mapping);

        SqlBuilder builder = new SqlBuilder()
                .fromSubquery(source, "subq");

        for (int i = 1; i < params.size(); i++) {
            for (String col : extractColumnNames(params.get(i))) {
                builder.addSelect(new SqlExpr.ColumnRef(col), dialect.quoteIdentifier(col));
            }
        }
        return builder;
    }

    // ========== rename ==========

    private SqlBuilder compileRename(AppliedFunction af, RelationalMapping mapping) {
        SqlBuilder source = compileRelation(af.parameters().get(0), mapping);
        List<ValueSpecification> params = af.parameters();

        String oldName = extractColumnNameFromParam(params.get(1));
        String newName = extractColumnNameFromParam(params.get(2));

        // Build SELECT using DuckDB EXCLUDE + rename pattern
        SqlBuilder builder = new SqlBuilder().fromSubquery(source, "rename_src");
        builder.selectStar();
        builder.addStarExcept(dialect.quoteIdentifier(oldName));
        builder.addSelect(new SqlExpr.ColumnRef(oldName), dialect.quoteIdentifier(newName));
        return builder;
    }

    // ========== concatenate / union ==========

    private SqlBuilder compileConcatenate(AppliedFunction af, RelationalMapping mapping) {
        SqlBuilder left = compileRelation(af.parameters().get(0), mapping);
        SqlBuilder right = compileRelation(af.parameters().get(1), mapping);
        left.unionWrapped(right, true);
        return left;
    }

    // ========== groupBy ==========

    private SqlBuilder compileGroupBy(AppliedFunction af, RelationalMapping mapping) {
        List<ValueSpecification> params = af.parameters();
        SqlBuilder source = compileRelation(params.get(0), mapping);

        SqlBuilder builder = new SqlBuilder()
                .fromSubquery(source, "groupby_src");

        if (params.size() >= 3) {
            // Group columns
            List<ValueSpecification> groupSpecs = extractList(params.get(1));
            for (var spec : groupSpecs) {
                String colName = extractPropertyOrColumnName(spec);
                if (colName != null) {
                    SqlExpr colRef = new SqlExpr.ColumnRef(colName);
                    builder.addSelect(colRef, null);
                    builder.addGroupBy(colRef);
                }
            }

            // Aggregate specs
            List<ValueSpecification> aggSpecs = extractList(params.get(2));
            List<String> aliases = params.size() > 3 ? extractStringList(params.get(3)) : List.of();
            List<String> groupColNames = groupSpecs.stream()
                    .map(this::extractPropertyOrColumnName)
                    .toList();

            for (int i = 0; i < aggSpecs.size(); i++) {
                var aggSpec = aggSpecs.get(i);
                String sourceCol;
                String aggFunc = "SUM";
                String alias;

                if (aggSpec instanceof ClassInstance ci && ci.value() instanceof ColSpec cs) {
                    alias = cs.name();
                    sourceCol = cs.function1() != null
                            ? extractPropertyNameFromLambda(cs.function1())
                            : cs.name();
                    if (cs.function2() != null) {
                        aggFunc = mapAggregateFunction(simpleName(
                                ((AppliedFunction) cs.function2().body().get(0)).function()));
                    }
                } else {
                    sourceCol = extractPropertyOrColumnName(aggSpec);
                    int aliasIndex = groupColNames.size() + i;
                    alias = aliasIndex < aliases.size() ? aliases.get(aliasIndex) : sourceCol + "_agg";
                }

                SqlExpr expr = new SqlExpr.FunctionCall(aggFunc, List.of(new SqlExpr.ColumnRef(sourceCol)));
                builder.addSelect(expr, dialect.quoteIdentifier(alias));
            }
        }

        return builder;
    }

    // ========== aggregate ==========

    private SqlBuilder compileAggregate(AppliedFunction af, RelationalMapping mapping) {
        // Same as groupBy but with no group columns
        return compileGroupBy(af, mapping);
    }

    // ========== extend (window functions) ==========

    private SqlBuilder compileExtend(AppliedFunction af, RelationalMapping mapping) {
        List<ValueSpecification> params = af.parameters();
        SqlBuilder source = compileRelation(params.get(0), mapping);

        // Look for window function pattern: extend(source, over(...), ~colSpec)
        AppliedFunction overSpec = null;
        ColSpec windowColSpec = null;

        for (int i = 1; i < params.size(); i++) {
            var p = params.get(i);
            if (p instanceof AppliedFunction paf && "over".equals(simpleName(paf.function()))) {
                overSpec = paf;
            } else if (p instanceof ClassInstance ci && ci.value() instanceof ColSpec cs) {
                windowColSpec = cs;
            }
        }

        if (overSpec != null && windowColSpec != null) {
            SqlExpr windowFunc = extractWindowFunction(windowColSpec);
            SqlExpr.WindowSpec windowSpec = generateOverClause(overSpec);
            String alias = windowColSpec.name();

            if (windowFunc instanceof SqlExpr.WrappedWindowFunction wwf) {
                SqlExpr windowedInner = new SqlExpr.WindowFunction(wwf.innerWindowFunc(), windowSpec);
                List<SqlExpr> wrapperArgs = new java.util.ArrayList<>();
                wrapperArgs.add(windowedInner);
                wrapperArgs.addAll(wwf.extraArgs());
                SqlExpr wrappedExpr = new SqlExpr.FunctionCall(wwf.wrapperFunc(), wrapperArgs);
                SqlBuilder b = new SqlBuilder().selectStar().fromSubquery(source, "window_src");
                b.addWindowColumn(wrappedExpr, null, dialect.quoteIdentifier(alias));
                return b;
            }

            return new SqlBuilder()
                    .selectStar()
                    .addWindowColumn(windowFunc, windowSpec, dialect.quoteIdentifier(alias))
                    .fromSubquery(source, "window_src");
        }

        // Aggregate extend without OVER — e.g.,
        // extend(~totalSalary:x|$x.salary:y|$y->plus())
        // Generates: SUM("salary") OVER ()
        if (windowColSpec != null && windowColSpec.function2() != null) {
            SqlExpr windowFunc = extractWindowFunction(windowColSpec);
            String alias = windowColSpec.name();
            return new SqlBuilder()
                    .selectStar()
                    .addWindowColumn(windowFunc,
                            new SqlExpr.WindowSpec(List.of(), List.of(), null),
                            dialect.quoteIdentifier(alias))
                    .fromSubquery(source, "window_src");
        }

        // Simple extend (computed column) — placeholder
        return new SqlBuilder()
                .selectStar()
                .fromSubquery(source, "extend_src");
    }

    // ========== join ==========

    private SqlBuilder compileJoin(AppliedFunction af, RelationalMapping mapping) {
        List<ValueSpecification> params = af.parameters();
        // join(left, right, joinType, condition)
        if (params.size() < 3) {
            throw new PureCompileException("SqlCompiler: join() requires left, right, and condition");
        }

        SqlBuilder left = compileRelation(params.get(0), mapping);
        // TODO(Phase3): eliminate mapping param — each side reads from sidecar
        SqlBuilder right = compileRelation(params.get(1), mapping);

        // Determine join type
        SqlBuilder.JoinType joinType = SqlBuilder.JoinType.INNER;
        int conditionIdx = 2; // default: join(left, right, condition)
        if (params.size() >= 4) {
            // join(left, right, JoinType, condition)
            joinType = extractJoinType(params.get(2));
            conditionIdx = 3;
        } else if (params.get(2) instanceof EnumValue) {
            // join(left, right, JoinType) — no condition ??
            joinType = extractJoinType(params.get(2));
            conditionIdx = -1; // cross join, no condition
        }

        // Build ON condition from lambda
        SqlExpr onCondition = null;
        if (conditionIdx >= 0 && conditionIdx < params.size()) {
            var condSpec = params.get(conditionIdx);
            if (condSpec instanceof LambdaFunction lf) {
                // Lambda params: {l, r | condition}
                String leftParam = lf.parameters().size() > 0 ? lf.parameters().get(0).name() : "l";
                String rightParam = lf.parameters().size() > 1 ? lf.parameters().get(1).name() : "r";
                onCondition = compileJoinCondition(lf.body().get(0), leftParam, rightParam,
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

    private SqlBuilder.JoinType extractJoinType(ValueSpecification vs) {
        String typeName = switch (vs) {
            case EnumValue ev -> ev.value();
            case CString cs -> cs.value();
            case AppliedProperty ap -> ap.property();
            default -> "INNER"; // unknown VS type — default to INNER join
        };

        return switch (typeName.toUpperCase().replace(" ", "_")) {
            case "INNER" -> SqlBuilder.JoinType.INNER;
            case "LEFT", "LEFT_OUTER" -> SqlBuilder.JoinType.LEFT;
            case "RIGHT", "RIGHT_OUTER" -> SqlBuilder.JoinType.RIGHT;
            case "FULL", "FULL_OUTER" -> SqlBuilder.JoinType.FULL;
            case "CROSS" -> SqlBuilder.JoinType.CROSS;
            default -> SqlBuilder.JoinType.INNER;
        };
    }

    /**
     * Compiles a join condition lambda where $l refers to left (t0) and $r refers
     * to right (t1).
     */
    private SqlExpr compileJoinCondition(ValueSpecification vs, String leftParam, String rightParam,
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
                            compileJoinCondition(params.get(0), leftParam, rightParam, left, right), "=",
                            compileJoinCondition(params.get(1), leftParam, rightParam, left, right));
                    case "greaterThan" -> new SqlExpr.Binary(
                            compileJoinCondition(params.get(0), leftParam, rightParam, left, right), ">",
                            compileJoinCondition(params.get(1), leftParam, rightParam, left, right));
                    case "lessThan" -> new SqlExpr.Binary(
                            compileJoinCondition(params.get(0), leftParam, rightParam, left, right), "<",
                            compileJoinCondition(params.get(1), leftParam, rightParam, left, right));
                    case "and" -> new SqlExpr.And(List.of(
                            compileJoinCondition(params.get(0), leftParam, rightParam, left, right),
                            compileJoinCondition(params.get(1), leftParam, rightParam, left, right)));
                    case "or" -> new SqlExpr.Or(List.of(
                            compileJoinCondition(params.get(0), leftParam, rightParam, left, right),
                            compileJoinCondition(params.get(1), leftParam, rightParam, left, right)));
                    case "not" -> new SqlExpr.Not(
                            compileJoinCondition(params.get(0), leftParam, rightParam, left, right));
                    default -> compileScalarFunction(af, leftParam, null, null);
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

            default -> compileScalar(vs, leftParam, null);
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

    private SqlBuilder compileFrom(AppliedFunction af, RelationalMapping mapping) {
        // from() is a runtime binding — pass through source
        return compileRelation(af.parameters().get(0), mapping);
    }

    // ========== Scalar Expression Compilation ==========

    /**
     * Compiles a scalar expression to SqlExpr. Delegates to 4-arg with null
     * tableAlias.
     */
    SqlExpr compileScalar(ValueSpecification vs, String rowParam, RelationalMapping mapping) {
        return compileScalar(vs, rowParam, mapping, null);
    }

    /**
     * Canonical 4-arg version: compiles scalar with optional table alias prefix.
     */
    SqlExpr compileScalar(ValueSpecification vs, String rowParam, RelationalMapping mapping, String tableAlias) {
        return switch (vs) {
            case AppliedProperty ap -> {
                // Check for association path: $p.addresses.street
                if (tableAlias != null && !ap.parameters().isEmpty()
                        && ap.parameters().get(0) instanceof AppliedProperty) {
                    List<String> path = extractPropertyChain(ap);
                    if (path.size() > 1 && modelContext != null) {
                        String assocProp = path.get(0);
                        String leafProp = path.getLast();
                        var assocResult = resolveAssociationTarget(assocProp, mapping);
                        String targetCol = leafProp;
                        var pmOpt = assocResult.mapping().getPropertyMapping(leafProp);
                        if (pmOpt.isPresent())
                            targetCol = pmOpt.get().columnName();
                        yield new SqlExpr.AssociationRef(assocProp, targetCol);
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
            case AppliedFunction af -> compileScalarFunction(af, rowParam, mapping, tableAlias);
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
                throw new PureCompileException("SqlCompiler: unsupported ClassInstance: " + ci.type());
            }
            case EnumValue ev -> new SqlExpr.StringLiteral(ev.value());
            case Collection coll -> {
                var exprs = coll.values().stream()
                        .map(v -> compileScalar(v, rowParam, mapping, tableAlias))
                        .collect(Collectors.toList());
                yield new SqlExpr.FunctionCall("", exprs);
            }
            default -> throw new PureCompileException(
                    "SqlCompiler: unsupported scalar: " + vs.getClass().getSimpleName());
        };
    }

    /**
     * Unified scalar function compilation. Handles all Pure functions → SqlExpr.
     * When tableAlias is non-null, property accesses are prefixed with the alias
     * and comparisons may produce EXISTS subqueries for association paths.
     */
    private SqlExpr compileScalarFunction(AppliedFunction af, String rowParam, RelationalMapping mapping,
            String tableAlias) {
        String funcName = simpleName(af.function());
        List<ValueSpecification> params = af.parameters();

        // Helper: recursively compile with same context
        java.util.function.Function<ValueSpecification, SqlExpr> c = v -> compileScalar(v, rowParam, mapping,
                tableAlias);

        return switch (funcName) {
            // --- Comparison (may produce EXISTS for association paths when tableAlias set)
            // ---
            case "equal" -> buildComparison(params, "=", c, mapping, tableAlias);
            case "greaterThan" -> buildComparison(params, ">", c, mapping, tableAlias);
            case "greaterThanEqual" -> buildComparison(params, ">=", c, mapping, tableAlias);
            case "lessThan" -> buildComparison(params, "<", c, mapping, tableAlias);
            case "lessThanEqual" -> buildComparison(params, "<=", c, mapping, tableAlias);
            case "notEqual" -> buildComparison(params, "<>", c, mapping, tableAlias);

            // --- Logical ---
            case "and" -> new SqlExpr.And(List.of(c.apply(params.get(0)), c.apply(params.get(1))));
            case "or" -> new SqlExpr.Or(List.of(c.apply(params.get(0)), c.apply(params.get(1))));
            case "not" -> new SqlExpr.Not(c.apply(params.get(0)));

            // --- Arithmetic ---
            case "plus" -> params.size() == 1
                    ? c.apply(params.get(0)) // unary +
                    : new SqlExpr.Binary(c.apply(params.get(0)), "+", c.apply(params.get(1)));
            case "minus" -> params.size() == 1
                    ? new SqlExpr.Unary("-", c.apply(params.get(0))) // unary -
                    : new SqlExpr.Binary(c.apply(params.get(0)), "-", c.apply(params.get(1)));
            case "times" -> new SqlExpr.Binary(c.apply(params.get(0)), "*", c.apply(params.get(1)));
            case "divide" -> new SqlExpr.Binary(c.apply(params.get(0)), "/", c.apply(params.get(1)));
            case "rem" -> new SqlExpr.Binary(c.apply(params.get(0)), "%", c.apply(params.get(1)));

            // --- String ---
            case "contains" -> new SqlExpr.ListContains(c.apply(params.get(0)), c.apply(params.get(1)));
            case "startsWith" -> new SqlExpr.StartsWith(c.apply(params.get(0)), c.apply(params.get(1)));
            case "endsWith" -> new SqlExpr.EndsWith(c.apply(params.get(0)), c.apply(params.get(1)));
            case "toLower" -> new SqlExpr.FunctionCall("LOWER", List.of(c.apply(params.get(0))));
            case "toUpper" -> new SqlExpr.FunctionCall("UPPER", List.of(c.apply(params.get(0))));
            case "length" -> new SqlExpr.FunctionCall("LENGTH", List.of(c.apply(params.get(0))));
            case "trim" -> new SqlExpr.FunctionCall("TRIM", List.of(c.apply(params.get(0))));
            case "toString" -> new SqlExpr.Cast(c.apply(params.get(0)), "String");
            case "substring" -> {
                if (params.size() > 2) {
                    yield new SqlExpr.FunctionCall("SUBSTRING",
                            List.of(c.apply(params.get(0)), c.apply(params.get(1)), c.apply(params.get(2))));
                }
                yield new SqlExpr.FunctionCall("SUBSTRING",
                        List.of(c.apply(params.get(0)), c.apply(params.get(1))));
            }
            case "indexOf" -> new SqlExpr.StrPosition(c.apply(params.get(1)), c.apply(params.get(0)));
            case "replace" -> new SqlExpr.FunctionCall("REPLACE",
                    List.of(c.apply(params.get(0)), c.apply(params.get(1)), c.apply(params.get(2))));

            // --- Null checks ---
            case "isEmpty" -> new SqlExpr.IsNull(c.apply(params.get(0)));
            case "isNotEmpty" -> new SqlExpr.IsNotNull(c.apply(params.get(0)));

            // --- Math ---
            case "abs" -> new SqlExpr.FunctionCall("ABS", List.of(c.apply(params.get(0))));
            case "ceiling", "ceil" -> new SqlExpr.FunctionCall("CEIL", List.of(c.apply(params.get(0))));
            case "floor" -> new SqlExpr.FunctionCall("FLOOR", List.of(c.apply(params.get(0))));
            case "round" -> {
                if (params.size() > 1) {
                    yield new SqlExpr.FunctionCall("ROUND",
                            List.of(c.apply(params.get(0)), c.apply(params.get(1))));
                }
                yield new SqlExpr.FunctionCall("ROUND", List.of(c.apply(params.get(0))));
            }
            case "sqrt" -> new SqlExpr.FunctionCall("SQRT", List.of(c.apply(params.get(0))));
            case "pow", "power" -> new SqlExpr.FunctionCall("POWER",
                    List.of(c.apply(params.get(0)), c.apply(params.get(1))));
            case "log" -> new SqlExpr.FunctionCall("LOG", List.of(c.apply(params.get(0))));
            case "exp" -> new SqlExpr.FunctionCall("EXP", List.of(c.apply(params.get(0))));

            // --- Cast ---
            case "toInteger", "parseInteger" -> new SqlExpr.Cast(c.apply(params.get(0)), "Integer");
            case "toFloat", "parseFloat" -> new SqlExpr.Cast(c.apply(params.get(0)), "Float");
            case "toDecimal", "parseDecimal" -> new SqlExpr.Cast(c.apply(params.get(0)), "Decimal");

            // --- If/Case ---
            case "if" -> {
                SqlExpr cond = c.apply(params.get(0));
                LambdaFunction thenLambda = (LambdaFunction) params.get(1);
                LambdaFunction elseLambda = (LambdaFunction) params.get(2);
                SqlExpr thenVal = compileScalar(thenLambda.body().get(0), rowParam, mapping, tableAlias);
                SqlExpr elseVal = compileScalar(elseLambda.body().get(0), rowParam, mapping, tableAlias);
                yield new SqlExpr.CaseWhen(cond, thenVal, elseVal);
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
            case "coalesce" -> new SqlExpr.FunctionCall("COALESCE",
                    params.stream().map(c).collect(Collectors.toList()));

            // --- Date adjust ---
            case "adjust" -> {
                if (params.size() < 2) {
                    throw new PureCompileException(
                            "SqlCompiler: adjust() requires at least 2 parameters, got " + params.size());
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
                yield new SqlExpr.DateAdd(dateExpr, amount, unit);
            }

            default -> throw new PureCompileException(
                    "SqlCompiler: unsupported scalar function '" + funcName + "'");
        };
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
                && modelContext != null && mapping != null) {
            String assocProp = ar.assocProp();
            String targetCol = ar.targetCol();

            var assocResult = resolveAssociationTarget(assocProp, mapping);
            String subAlias = "sub" + (tableAliasCounter++);
            String targetTableName = assocResult.mapping().table().name();
            Join join = assocResult.join();

            if (join != null) {
                String leftJoinCol = join.getColumnForTable(mapping.table().name());
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

        return new SqlExpr.Binary(left, op, right);
    }

    // compileScalarFunction (3-arg) DELETED — unified into 4-arg version above

    // ========== Window Function Helpers ==========

    private SqlExpr extractWindowFunction(ColSpec cs) {
        // Pattern 1: Aggregate window with function2 = aggregate lambda
        if (cs.function2() != null) {
            String column = extractPropertyNameFromLambda(cs.function1());
            String aggFunc = extractAggFuncFromLambda(cs.function2());
            if (column != null && aggFunc != null) {
                return new SqlExpr.FunctionCall(aggFunc, List.of(new SqlExpr.ColumnRef(column)));
            }
        }

        // Pattern 2: Ranking/value function in function1
        if (cs.function1() != null) {
            var body = cs.function1().body();
            if (!body.isEmpty() && body.get(0) instanceof AppliedFunction af) {
                String funcName = simpleName(af.function());

                // Check for aggregate body WITH property access:
                if (body.get(0) instanceof AppliedProperty ap) {
                    String propName = ap.property();
                    if (!ap.parameters().isEmpty() && ap.parameters().get(0) instanceof AppliedFunction innerAf) {
                        String innerFunc = simpleName(innerAf.function());
                        String sqlAgg = mapAggregateFunction(innerFunc);
                        if (sqlAgg != null) {
                            return new SqlExpr.FunctionCall(sqlAgg, List.of(new SqlExpr.ColumnRef(propName)));
                        }
                    }
                }

                // Check for post-processor wrapping a window function:
                // e.g., round(cumulativeDistribution($w,$r), 2) → ROUND(CUME_DIST() OVER(...),
                // 2)
                // We return FunctionCall(ROUND, [WindowFunction(CUME_DIST(), overSpec), arg2,
                // ...])
                // The caller provides the overSpec; we use a placeholder WindowSpec here
                if (isPostProcessor(funcName) && !af.parameters().isEmpty()
                        && af.parameters().get(0) instanceof AppliedFunction innerAf) {
                    String innerFuncName = simpleName(innerAf.function());
                    SqlExpr innerWindowFunc = mapWindowFuncName(innerFuncName);
                    if (innerWindowFunc != null) {
                        List<SqlExpr> wrapperArgs = new java.util.ArrayList<>();
                        // First arg is the inner window function — caller will wrap with OVER
                        wrapperArgs.add(innerWindowFunc);
                        for (int i = 1; i < af.parameters().size(); i++) {
                            wrapperArgs.add(compileScalar(af.parameters().get(i), "", null));
                        }
                        // Mark as a wrapped window function by returning a FunctionCall
                        // whose first arg is the inner window function (a FunctionCall with no args)
                        return new SqlExpr.WrappedWindowFunction(
                                funcName.toUpperCase(), innerWindowFunc,
                                wrapperArgs.subList(1, wrapperArgs.size()));
                    }
                }

                SqlExpr winFunc = mapWindowFuncName(funcName);
                if (winFunc != null)
                    return winFunc;

                // Aggregate/value functions with arguments
                SqlExpr arg = extractWindowFuncArgExpr(af);
                return switch (funcName) {
                    case "first", "firstValue" -> new SqlExpr.FunctionCall("FIRST_VALUE", List.of(arg));
                    case "last", "lastValue" -> new SqlExpr.FunctionCall("LAST_VALUE", List.of(arg));
                    case "ntile" -> {
                        if (af.parameters().size() > 1) {
                            yield new SqlExpr.FunctionCall("NTILE",
                                    List.of(new SqlExpr.Literal(
                                            String.valueOf(extractIntValue(af.parameters().get(1))))));
                        }
                        yield new SqlExpr.FunctionCall("NTILE", List.of(new SqlExpr.Literal("1")));
                    }
                    case "sum" -> new SqlExpr.FunctionCall("SUM", List.of(arg));
                    case "average", "avg" -> new SqlExpr.FunctionCall("AVG", List.of(arg));
                    case "count" -> new SqlExpr.FunctionCall("COUNT", List.of(arg));
                    case "min" -> new SqlExpr.FunctionCall("MIN", List.of(arg));
                    case "max" -> new SqlExpr.FunctionCall("MAX", List.of(arg));
                    case "lag" -> new SqlExpr.FunctionCall("LAG", List.of(arg));
                    case "lead" -> new SqlExpr.FunctionCall("LEAD", List.of(arg));
                    default -> new SqlExpr.FunctionCall(funcName.toUpperCase(), List.of());
                };
            }

            // Property access pattern: {p,w,r|$p->avg($w,$r).salary}
            if (!body.isEmpty() && body.get(0) instanceof AppliedProperty ap) {
                String propName = ap.property();
                if (!ap.parameters().isEmpty() && ap.parameters().get(0) instanceof AppliedFunction innerAf) {
                    String innerFunc = simpleName(innerAf.function());
                    String sqlAgg = mapAggregateFunction(innerFunc);
                    if (sqlAgg != null) {
                        return new SqlExpr.FunctionCall(sqlAgg, List.of(new SqlExpr.ColumnRef(propName)));
                    }
                    return switch (innerFunc) {
                        case "first", "firstValue" ->
                            new SqlExpr.FunctionCall("FIRST_VALUE", List.of(new SqlExpr.ColumnRef(propName)));
                        case "last", "lastValue" ->
                            new SqlExpr.FunctionCall("LAST_VALUE", List.of(new SqlExpr.ColumnRef(propName)));
                        case "lag" -> new SqlExpr.FunctionCall("LAG", List.of(new SqlExpr.ColumnRef(propName)));
                        case "lead" -> new SqlExpr.FunctionCall("LEAD", List.of(new SqlExpr.ColumnRef(propName)));
                        default -> throw new PureCompileException(
                                "SqlCompiler: unsupported window function '" + innerFunc + "'");
                    };
                }
            }
        }
        throw new PureCompileException("SqlCompiler: cannot extract window function from lambda");
    }

    /**
     * Maps Pure window function name to SQL FunctionCall (zero-arg ranking
     * functions).
     */
    private SqlExpr mapWindowFuncName(String funcName) {
        return switch (funcName) {
            case "rowNumber" -> new SqlExpr.FunctionCall("ROW_NUMBER", List.of());
            case "rank" -> new SqlExpr.FunctionCall("RANK", List.of());
            case "denseRank" -> new SqlExpr.FunctionCall("DENSE_RANK", List.of());
            case "cumulativeDistribution" -> new SqlExpr.FunctionCall("CUME_DIST", List.of());
            case "percentRank" -> new SqlExpr.FunctionCall("PERCENT_RANK", List.of());
            default -> throw new PureCompileException(
                    "SqlCompiler: unsupported window function '" + funcName + "'");
        };
    }

    private SqlExpr extractWindowFuncArgExpr(AppliedFunction af) {
        if (af.parameters().size() > 1) {
            return new SqlExpr.ColumnRef(extractColumnNameFromParam(af.parameters().get(1)));
        }
        return new SqlExpr.Star();
    }

    /**
     * Maps Pure aggregate function name to SQL aggregate function.
     * Returns null for unmapped functions — callers should handle this gracefully.
     */
    private String mapAggregateFunction(String funcName) {
        return switch (funcName) {
            case "plus", "sum" -> "SUM";
            case "average", "avg" -> "AVG";
            case "count", "size" -> "COUNT";
            case "min" -> "MIN";
            case "max" -> "MAX";
            case "stdDev", "stddev" -> "STDDEV";
            case "variance" -> "VARIANCE";
            default -> {
                System.err.println("[SqlCompiler] unmapped aggregate function: " + funcName);
                yield null;
            }
        };
    }

    /** Extracts SQL aggregate function name from a lambda like {y|$y->plus()}. */
    private String extractAggFuncFromLambda(LambdaFunction lf) {
        if (lf == null || lf.body().isEmpty())
            return null;
        var body = lf.body().get(0);
        if (body instanceof AppliedFunction af) {
            return mapAggregateFunction(simpleName(af.function()));
        }
        return null;
    }

    private boolean isPostProcessor(String funcName) {
        return switch (funcName) {
            case "round", "floor", "ceil", "ceiling", "trunc", "truncate" -> true;
            default -> false;
        };
    }

    private SqlExpr.WindowSpec generateOverClause(AppliedFunction overSpec) {
        List<SqlExpr> partitionCols = new ArrayList<>();
        List<SqlExpr> orderParts = new ArrayList<>();
        String frameClause = null;

        for (var p : overSpec.parameters()) {
            if (p instanceof ClassInstance ci && ci.value() instanceof ColSpec cs) {
                partitionCols.add(new SqlExpr.ColumnRef(cs.name()));
            } else if (p instanceof AppliedFunction paf) {
                String funcName = simpleName(paf.function());
                if ("asc".equals(funcName) || "ascending".equals(funcName)
                        || "desc".equals(funcName) || "descending".equals(funcName)) {
                    String col = extractColumnNameFromParam(paf.parameters().get(0));
                    String dir = ("asc".equals(funcName) || "ascending".equals(funcName)) ? "ASC" : "DESC";
                    String nullOrder = "DESC".equals(dir) ? "NULLS FIRST" : "NULLS LAST";
                    // OrderBy entry: rendered as "col" ASC NULLS LAST
                    orderParts.add(new SqlExpr.Literal(
                            dialect.quoteIdentifier(col) + " " + dir + " " + nullOrder));
                } else if ("rows".equals(funcName)) {
                    frameClause = "ROWS BETWEEN " + formatFrameBound(paf.parameters(), true)
                            + " AND " + formatFrameBound(paf.parameters(), false);
                } else if ("range".equals(funcName) || "_range".equals(funcName)) {
                    frameClause = "RANGE BETWEEN " + formatFrameBound(paf.parameters(), true)
                            + " AND " + formatFrameBound(paf.parameters(), false);
                }
            }
        }

        return new SqlExpr.WindowSpec(partitionCols, orderParts, frameClause);
    }

    /** Formats a frame bound from the rows()/range() parameters. */
    private String formatFrameBound(List<ValueSpecification> params, boolean isStart) {
        int idx = isStart ? 0 : 1;
        if (idx >= params.size())
            return isStart ? "UNBOUNDED PRECEDING" : "CURRENT ROW";
        var param = params.get(idx);
        if (param instanceof AppliedFunction af && "unbounded".equals(simpleName(af.function()))) {
            return isStart ? "UNBOUNDED PRECEDING" : "UNBOUNDED FOLLOWING";
        }
        // Handle negative numbers: AppliedFunction("minus", [CInteger(n)]) → n
        // PRECEDING
        if (param instanceof AppliedFunction af && "minus".equals(simpleName(af.function()))) {
            if (!af.parameters().isEmpty()) {
                try {
                    long v = extractIntValue(af.parameters().get(af.parameters().size() - 1));
                    return v + " PRECEDING";
                } catch (PureCompileException e) {
                    /* fall through */ }
            }
        }
        // Try extracting as integer value (handles CInteger, CFloat, etc.)
        try {
            long v = extractIntValue(param);
            if (v == 0)
                return "CURRENT ROW";
            if (v < 0)
                return Math.abs(v) + " PRECEDING";
            return v + " FOLLOWING";
        } catch (PureCompileException e) {
            // Not a simple integer, fall through
        }
        return isStart ? "CURRENT ROW" : "CURRENT ROW";
    }

    // ========== Aggregate Helpers ==========

    // extractAggFunction DELETED — replaced by mapAggregateFunction + simpleName
    // extraction at callsite

    private String extractPropertyOrColumnName(ValueSpecification vs) {
        if (vs instanceof LambdaFunction lf && !lf.body().isEmpty()) {
            ValueSpecification body = lf.body().get(0);
            if (body instanceof AppliedProperty ap)
                return ap.property();
        }
        if (vs instanceof ClassInstance ci && ci.value() instanceof ColSpec cs) {
            return cs.name();
        }
        if (vs instanceof AppliedFunction af) {
            if (!af.parameters().isEmpty()) {
                return extractPropertyOrColumnName(af.parameters().get(0));
            }
        }
        return null;
    }

    // ========== Utility Methods ==========

    // binaryOp DELETED — was thin wrapper, inlined into compileScalarFunction

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

    private String extractColumnNameFromParam(ValueSpecification vs) {
        if (vs instanceof ClassInstance ci && ci.value() instanceof ColSpec cs)
            return cs.name();
        if (vs instanceof CString s)
            return s.value();
        if (vs instanceof Variable v)
            return v.name();
        throw new PureCompileException("Expected column name, got: " + vs.getClass().getSimpleName());
    }

    private List<String> extractColumnNames(ValueSpecification vs) {
        if (vs instanceof ClassInstance ci && ci.value() instanceof ColSpecArray csa) {
            return csa.colSpecs().stream().map(ColSpec::name).toList();
        }
        if (vs instanceof ClassInstance ci && ci.value() instanceof ColSpec cs) {
            return List.of(cs.name());
        }
        return List.of(extractColumnNameFromParam(vs));
    }

    private List<ValueSpecification> extractList(ValueSpecification vs) {
        if (vs instanceof Collection coll)
            return coll.values();
        return List.of(vs);
    }

    private List<String> extractStringList(ValueSpecification vs) {
        if (vs instanceof Collection coll) {
            return coll.values().stream()
                    .filter(v -> v instanceof CString)
                    .map(v -> ((CString) v).value())
                    .toList();
        }
        if (vs instanceof CString s)
            return List.of(s.value());
        return List.of();
    }

    private String extractTableName(String content) {
        int dotIdx = content.lastIndexOf('.');
        return dotIdx > 0 ? content.substring(dotIdx + 1) : content;
    }

    private String extractPropertyNameFromLambda(ValueSpecification vs) {
        if (vs instanceof LambdaFunction lf && !lf.body().isEmpty()) {
            ValueSpecification body = lf.body().get(0);
            if (body instanceof AppliedProperty ap)
                return ap.property();
        }
        return null;
    }

    private String extractClassName(ValueSpecification vs) {
        if (vs instanceof PackageableElementPtr pep) {
            String fqn = pep.fullPath();
            int idx = fqn.lastIndexOf("::");
            return idx > 0 ? fqn.substring(idx + 2) : fqn;
        }
        throw new PureCompileException(
                "SqlCompiler: expected PackageableElementPtr but got " + vs.getClass().getSimpleName());
    }

    private static String simpleName(String qualifiedName) {
        return CleanCompiler.simpleName(qualifiedName);
    }
}
