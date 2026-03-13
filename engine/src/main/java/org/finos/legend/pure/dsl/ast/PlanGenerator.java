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
        // Compiler-driven inlining: follow inlinedBody pointers (let bindings, blocks, user functions)
        TypeInfo info = unit.types().get(vs);
        if (info != null && info.inlinedBody() != null) {
            return generateRelation(info.inlinedBody());
        }
        return switch (vs) {
            case AppliedFunction af -> generateFunction(af);
            case LambdaFunction lf -> {
                if (lf.body().isEmpty())
                    throw new PureCompileException("PlanGenerator: empty lambda body");

                yield generateRelation(lf.body().getLast());
            }
            case ClassInstance ci -> generateClassInstance(ci);
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
            case CInteger ci -> {
                // Check compiler type annotation for precision
                TypeInfo tiInt = unit.typeInfoFor(ci);
                if (tiInt != null && tiInt.scalarType() == GenericType.Primitive.INT128) {
                    yield new SqlExpr.Cast(new SqlExpr.NumericLiteral(ci.value()), "Int128");
                }
                yield new SqlExpr.NumericLiteral(ci.value());
            }
            case CFloat cf -> {
                yield new SqlExpr.DecimalLiteral(java.math.BigDecimal.valueOf(cf.value()));
            }
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

        var columns = tds.columns();
        List<List<SqlExpr>> rows = tds.rows().stream()
                .map(row -> {
                    List<SqlExpr> cells = new java.util.ArrayList<>();
                    for (int i = 0; i < row.size(); i++) {
                        SqlExpr cell = formatTdsValue(row.get(i));
                        // Variant columns need dialect-specific variant literal wrapping
                        if (i < columns.size() && columns.get(i).isVariant()) {
                            cell = new SqlExpr.VariantLiteral(cell);
                        }
                        cells.add(cell);
                    }
                    return cells;
                })
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
            return new SqlExpr.NumericLiteral((Number) val);
        return new SqlExpr.StringLiteral(val.toString());
    }

    private SqlBuilder generateFunction(AppliedFunction af) {
        String funcName = simpleName(af.function());
        return switch (funcName) {
            case "getAll" -> generateGetAll(af);
            case "table", "tableReference" -> generateTableAccess(af);
            case "filter" -> generateFilter(af);
            case "project" -> generateProject(af);
            case "sort", "sortBy" -> generateSort(af);
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
            case "pivot" -> generatePivot(af);
            case "asOfJoin" -> generateAsOfJoin(af);
            case "flatten" -> generateFlatten(af);
            // --- Pass-through: source-preserving relational functions ---
            case "toString", "toVariant",
                 "eq", "cast", "write", "size",
                 "greaterThan", "lessThan", "greaterThanEqual", "lessThanEqual" ->
                generateRelation(af.parameters().get(0));
            default -> throw new PureCompileException("PlanGenerator: unsupported function '" + funcName + "'");
        };
    }

    // ========== getAll / table ==========

    private SqlBuilder generateGetAll(AppliedFunction af) {
        TypeInfo info = unit.typeInfoFor(af);
        String tableName = info != null ? info.tableName() : null;
        if (tableName == null) {
            throw new PureCompileException("getAll: tableName not resolved in sidecar for " + af.function());
        }
        String alias = nextTableAlias();
        return new SqlBuilder()
                .selectStar()
                .from(dialect.quoteIdentifier(tableName), dialect.quoteIdentifier(alias));
    }

    private SqlBuilder generateTableAccess(AppliedFunction af) {
        TypeInfo info = unit.typeInfoFor(af);
        String tableName = info != null ? info.tableName() : null;
        // table('myTable') — tableName comes from the string parameter, not from a mapping
        if (tableName == null && !af.parameters().isEmpty()
                && af.parameters().get(0) instanceof CString s) {
            String content = s.value();
            int dotIdx = content.lastIndexOf('.');
            tableName = dotIdx > 0 ? content.substring(dotIdx + 1) : content;
        }
        if (tableName == null) {
            throw new PureCompileException("tableAccess: tableName not resolved for " + af.function());
        }
        return new SqlBuilder()
                .selectStar()
                .from(dialect.quoteIdentifier(tableName), dialect.quoteIdentifier("t0"));
    }

    // ========== flatten ==========

    /**
     * Generates UNNEST for flatten(~col).
     * Reads the column name from compiler's TypeInfo (columnSpecs).
     * Produces: SELECT * EXCLUDE("col"), UNNEST(CAST("col" AS JSON[])) AS "col" FROM (source) AS t
     */
    private SqlBuilder generateFlatten(AppliedFunction af) {
        List<ValueSpecification> params = af.parameters();
        SqlBuilder source = generateRelation(params.get(0));

        // Read column name from compiler's TypeInfo — not raw AST
        TypeInfo info = unit.typeInfoFor(af);
        if (info == null || info.columnSpecs().isEmpty()) {
            // No flatten column resolved — pass through
            return source;
        }
        String colName = info.columnSpecs().get(0).columnName();
        String quotedCol = dialect.quoteIdentifier(colName);

        // Build UNNEST layer: SELECT * EXCLUDE("col"), UNNEST(CAST("col" AS JSON[])) AS "col"
        //                      FROM (source) AS t
        SqlBuilder unnestLayer = new SqlBuilder()
                .selectStar()
                .addStarExcept(quotedCol)
                .addSelect(
                    new SqlExpr.Unnest(new SqlExpr.VariantArrayCast(
                        new SqlExpr.ColumnRef(colName), "JSON")),
                    quotedCol)
                .fromSubquery(source, "t");

        // Wrap in opaque subquery so downstream extend creates a separate layer
        return new SqlBuilder()
                .selectStar()
                .fromSubquery(unnestLayer, "window_src");
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
        String filterAlias = source.hasGroupBy() ? "grp" : "ext";
        return new SqlBuilder()
                .selectStar()
                .fromSubquery(source, filterAlias)
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
        // We can inline as long as source is a simple SELECT * (no GROUP BY, no existing WHERE, etc.)
        boolean canInline = source.isSelectStar()
                && !source.hasGroupBy();

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
                SqlExpr variantAccess = new SqlExpr.VariantTextExtract(base, ea.jsonKey());
                return ea.castType() != null
                        ? new SqlExpr.Cast(variantAccess, ea.castType())
                        : variantAccess;
            }
            // Enum mapping: generate CASE WHEN translation
            if (pm.hasEnumMapping()) {
                SqlExpr colExpr = alias != null
                        ? new SqlExpr.Column(alias, pm.columnName())
                        : new SqlExpr.ColumnRef(pm.columnName());
                var branches = new java.util.ArrayList<SqlExpr.SearchedCase.WhenBranch>();
                for (var entry : pm.enumMapping().entrySet()) {
                    String enumValue = entry.getKey();
                    java.util.List<Object> sourceValues = entry.getValue();
                    java.util.List<SqlExpr> conditions = sourceValues.stream()
                            .map(sv -> (SqlExpr) new SqlExpr.Binary(colExpr, "=",
                                    new SqlExpr.StringLiteral(sv.toString())))
                            .toList();
                    SqlExpr condition = conditions.get(0);
                    for (int i = 1; i < conditions.size(); i++) {
                        condition = new SqlExpr.Grouped(
                                new SqlExpr.Binary(condition, "OR", conditions.get(i)));
                    }
                    branches.add(new SqlExpr.SearchedCase.WhenBranch(
                            condition, new SqlExpr.StringLiteral(enumValue)));
                }
                return new SqlExpr.SearchedCase(branches, new SqlExpr.NullLiteral());
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

        // Check if distinct specifies particular columns
        TypeInfo info = unit.types().get(af);
        if (info != null && !info.columnSpecs().isEmpty()) {
            // Project specific columns: SELECT DISTINCT "col1", "col2" FROM (source) AS distinct_src
            SqlBuilder builder = new SqlBuilder()
                    .distinct()
                    .fromSubquery(source, "distinct_src");
            for (var cs : info.columnSpecs()) {
                builder.addSelect(new SqlExpr.ColumnRef(cs.columnName()), null);
            }
            return builder;
        }

        // No explicit columns — inline DISTINCT into source when possible
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

        TypeInfo info = unit.types().get(af);
        // select() with no column specs = select all, pass through source directly
        if (info.columnSpecs().isEmpty()) {
            return source;
        }
        // If selecting all columns from source, also pass through
        TypeInfo sourceInfo = unit.types().get(params.get(0));
        if (sourceInfo != null && sourceInfo.relationType() != null
                && info.columnSpecs().size() == sourceInfo.relationType().columns().size()) {
            boolean allMatch = info.columnSpecs().stream()
                    .allMatch(cs -> sourceInfo.relationType().columns().containsKey(cs.columnName()));
            if (allMatch) {
                return source;
            }
        }

        // If source is a simple star-select (no GROUP/ORDER/LIMIT/window),
        // inline column projections directly instead of wrapping in a subquery.
        // WHERE is safe — SELECT cols FROM table WHERE cond is valid SQL.
        if (source.isSelectStar() && !source.hasSelectColumns()
                && !source.hasGroupBy()
                && !source.hasOrderBy() && !source.hasLimit()
                && !source.hasWindowColumns()) {
            source.clearSelect();
            for (var cs : info.columnSpecs()) {
                source.addSelect(new SqlExpr.ColumnRef(cs.columnName()), dialect.quoteIdentifier(cs.columnName()));
            }
            return source;
        }

        SqlBuilder builder = new SqlBuilder()
                .fromSubquery(source, "subq");

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

        // Build SELECT using EXCLUDE + rename pattern (EXCLUDE rendered by SqlBuilder via dialect)
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
                    throw new PureCompileException("PlanGenerator: unknown aggregate function '" + cs.aggFunction() + "'");
                SqlExpr expr = buildAggExpr(aggFunc, cs);
                builder.addSelect(expr, dialect.quoteIdentifier(cs.alias()));
            }
        }

        return builder;
    }

    /**
     * Builds the SQL expression for an aggregate column.
     * Handles compound patterns like wavg and hashCode that need
     * multi-function SQL expansion instead of a simple function call.
     */
    private SqlExpr buildAggExpr(String aggFunc, TypeInfo.ColumnSpec cs) {
        SqlExpr col = new SqlExpr.ColumnRef(cs.columnName());

        // wavg: SUM(col * weight) / SUM(weight)
        if ("wavg".equals(aggFunc)) {
            if (cs.extraArgs().isEmpty()) {
                throw new PureCompileException("wavg requires a weight column");
            }
            SqlExpr weight = new SqlExpr.ColumnRef(cs.extraArgs().get(0));
            SqlExpr product = new SqlExpr.Binary(col, "*", weight);
            SqlExpr sumProduct = new SqlExpr.FunctionCall("SUM", List.of(product));
            SqlExpr sumWeight = new SqlExpr.FunctionCall("SUM", List.of(weight));
            return new SqlExpr.Binary(sumProduct, "/", sumWeight);
        }

        // hashCode: HASH(LIST(col))
        if ("hashCode".equals(aggFunc)) {
            SqlExpr listExpr = new SqlExpr.FunctionCall("LIST", List.of(col));
            return new SqlExpr.FunctionCall("HASH", List.of(listExpr));
        }

        // Standard: FUNC(col, extraArgs...)
        List<SqlExpr> args = new ArrayList<>();
        args.add(col);
        for (String extra : cs.extraArgs()) {
            args.add(parseExtraArg(extra));
        }
        return new SqlExpr.FunctionCall(aggFunc, args);
    }

    private SqlBuilder generateAggregate(AppliedFunction af) {
        List<ValueSpecification> params = af.parameters();
        SqlBuilder source = generateRelation(params.get(0));

        SqlBuilder builder = new SqlBuilder()
                .fromSubquery(source, "agg_src");

        TypeInfo info = unit.types().get(af);
        for (var cs : info.columnSpecs()) {
            String aggFunc = mapPureFuncToSql(cs.aggFunction());
            if (aggFunc == null)
                throw new PureCompileException("PlanGenerator: unknown aggregate function '" + cs.aggFunction() + "'");
            SqlExpr expr = buildAggExpr(aggFunc, cs);
            builder.addSelect(expr, dialect.quoteIdentifier(cs.alias()));
        }

        return builder;
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
                            return (SqlExpr) new SqlExpr.OrderByTerm(
                                    new SqlExpr.ColumnRef(s.column()), dir, nullOrder);
                        }).toList();
                String frameClause = ws.frame() != null ? formatFrameSpec(ws.frame()) : null;
                SqlExpr.WindowSpec windowSpec = new SqlExpr.WindowSpec(partitionCols, orderParts, frameClause);

                // Map Pure function name to SQL (fall back to uppercase for non-aggregate window fns)
                String sqlFunc = mapPureFuncToSql(ws.pureFunctionName());
                if (sqlFunc == null) sqlFunc = ws.pureFunctionName().toUpperCase();

                // Build the window function SqlExpr
                SqlExpr windowFunc = buildWindowFunc(ws, sqlFunc);

                if (ws.isWrapped()) {
                    String sqlWrapper = mapPureFuncToSql(ws.wrapperFuncName());
                    if (sqlWrapper == null) sqlWrapper = ws.wrapperFuncName().toUpperCase();
                    SqlExpr windowedInner = new SqlExpr.WindowFunction(windowFunc, windowSpec);
                    List<SqlExpr> wrapperArgs = new java.util.ArrayList<>();
                    wrapperArgs.add(windowedInner);
                    for (String arg : ws.wrapperArgs()) {
                        wrapperArgs.add(parseExtraArg(arg));
                    }
                    SqlExpr wrappedExpr = new SqlExpr.FunctionCall(sqlWrapper, wrapperArgs);
                    b.addWindowColumn(wrappedExpr, null, quotedAlias);
                } else {
                    b.addWindowColumn(windowFunc, windowSpec, quotedAlias);
                }
            }
            return b;
        }

        // Simple extend (computed column): extract ColSpec lambda(s) and compile as scalar
        List<ColSpec> colSpecs = new java.util.ArrayList<>();
        for (int i = 1; i < params.size(); i++) {
            if (params.get(i) instanceof ClassInstance ci) {
                if (ci.value() instanceof ColSpec cs) {
                    colSpecs.add(cs);
                } else if (ci.value() instanceof ColSpecArray csa) {
                    colSpecs.addAll(csa.colSpecs());
                }
            }
        }
        for (ColSpec cs : colSpecs) {
            if (cs.function1() != null) {
                String lambdaParam = cs.function1().parameters().isEmpty() ? null
                        : cs.function1().parameters().get(0).name();
                SqlExpr computed = generateScalar(
                        cs.function1().body().get(0), lambdaParam, null, null);
                source.addSelect(computed, dialect.quoteIdentifier(cs.name()));
            }
        }
        return source;
    }

    /** Builds the SqlExpr for a window function call from its spec. */
    private SqlExpr buildWindowFunc(TypeInfo.WindowFunctionSpec ws, String sqlFunc) {
        if (ws.isNtile()) {
            return new SqlExpr.FunctionCall("NTILE",
                    List.of(new SqlExpr.NumericLiteral(ws.ntileArg())));
        }
        if (ws.hasSourceColumn()) {
            List<SqlExpr> args = new ArrayList<>();
            if ("*".equals(ws.sourceColumn())) {
                args.add(new SqlExpr.Star());
            } else {
                args.add(new SqlExpr.ColumnRef(ws.sourceColumn()));
            }
            for (String extra : ws.extraArgs()) {
                args.add(parseExtraArg(extra));
            }
            return new SqlExpr.FunctionCall(sqlFunc, args);
        }
        return new SqlExpr.FunctionCall(sqlFunc, List.of());
    }

    /** Maps a Pure function name to a semantic function name for SQL generation.
     *  Dialect layer takes care of rendering semantic names to SQL syntax. */
    private String mapPureFuncToSql(String pureFuncName) {
        return switch (pureFuncName) {
            // Aggregates
            case "plus", "sum" -> "sum";
            case "average", "avg", "mean" -> "avg";
            case "count", "size" -> "count";
            case "min" -> "min";
            case "max" -> "max";
            case "stdDev", "stddev" -> "stdDev";
            case "stdDevSample" -> "stdDevSample";
            case "stdDevPopulation" -> "stdDevPopulation";
            case "variance" -> "variance";
            case "varianceSample" -> "varianceSample";
            case "variancePopulation" -> "variancePopulation";
            case "covarSample" -> "covarSample";
            case "covarPopulation" -> "covarPopulation";
            case "median" -> "median";
            case "percentileCont" -> "percentileCont";
            case "percentile", "percentileDisc" -> "percentileDisc";
            case "joinStrings" -> "joinStrings";
            case "mode" -> "mode";
            case "corr" -> "corr";
            case "maxBy" -> "maxBy";
            case "minBy" -> "minBy";
            case "wavg" -> "wavg";
            case "hashCode" -> "hashCode";
            // Ranking
            case "rowNumber" -> "rowNumber";
            case "rank" -> "rank";
            case "denseRank" -> "denseRank";
            case "percentRank" -> "percentRank";
            case "cumulativeDistribution" -> "cumulativeDistribution";
            // Value functions
            case "first", "firstValue" -> "firstValue";
            case "last", "lastValue" -> "lastValue";
            case "lag" -> "lag";
            case "lead" -> "lead";
            case "ntile" -> "ntile";
            case "nthValue", "nth" -> "nthValue";
            // Math wrappers
            case "round" -> "round";
            case "abs" -> "abs";
            case "ceil" -> "ceil";
            case "floor" -> "floor";
            case "truncate" -> "truncate";
            default -> null;
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
                double v = bound.offset();
                if (v < 0)
                    yield formatOffset(Math.abs(v)) + " PRECEDING";
                if (v > 0)
                    yield formatOffset(v) + " FOLLOWING";
                yield "CURRENT ROW";
            }
        };
    }

    /** Formats a frame offset: integer if whole, decimal otherwise. */
    private static String formatOffset(double v) {
        if (v == Math.floor(v) && !Double.isInfinite(v))
            return String.valueOf((long) v);
        return java.math.BigDecimal.valueOf(v).stripTrailingZeros().toPlainString();
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

        // Build ON condition from lambda — properties pre-tagged by CleanCompiler
        SqlExpr onCondition = null;
        if (conditionIdx >= 0 && conditionIdx < params.size()) {
            var condSpec = params.get(conditionIdx);
            if (condSpec instanceof LambdaFunction lf) {
                onCondition = generateScalar(lf.body().get(0), null, null);
            }
        }
        String leftAlias = dialect.quoteIdentifier("left_src");
        String rightAlias = dialect.quoteIdentifier("right_src");
        SqlBuilder result = new SqlBuilder()
                .selectStar()
                .fromSubquery(left, leftAlias);

        result.addJoin(new SqlBuilder.JoinClause(
                joinType,
                null, // no table name — using subquery
                rightAlias,
                right, // right as subquery
                onCondition,
                null)); // no USING

        return result;
    }

    private SqlBuilder generateAsOfJoin(AppliedFunction af) {
        List<ValueSpecification> params = af.parameters();
        // asOfJoin(left, right, matchCondition [, keyCondition])
        if (params.size() < 3) {
            throw new PureCompileException("PlanGenerator: asOfJoin() requires left, right, and match condition");
        }

        SqlBuilder left = generateRelation(params.get(0));
        SqlBuilder right = generateRelation(params.get(1));

        // Match condition: {t, q | $t.time > $q.time}
        SqlExpr matchCondition = null;
        if (params.get(2) instanceof LambdaFunction lf && !lf.body().isEmpty()) {
            matchCondition = generateScalar(lf.body().get(0), null, null);
        }

        // Optional key condition: {t, q | $t.symbol == $q.symbol}
        SqlExpr keyCondition = null;
        if (params.size() >= 4 && params.get(3) instanceof LambdaFunction lf && !lf.body().isEmpty()) {
            keyCondition = generateScalar(lf.body().get(0), null, null);
        }

        // Build ON clause: key AND match (key first if present)
        SqlExpr onCondition;
        if (keyCondition != null && matchCondition != null) {
            onCondition = new SqlExpr.And(List.of(keyCondition, matchCondition));
        } else if (matchCondition != null) {
            onCondition = matchCondition;
        } else {
            throw new PureCompileException("asOfJoin: match condition required");
        }

        String leftAlias = dialect.quoteIdentifier("left_src");
        String rightAlias = dialect.quoteIdentifier("right_src");
        SqlBuilder result = new SqlBuilder()
                .selectStar()
                .fromSubquery(left, leftAlias);

        result.addJoin(new SqlBuilder.JoinClause(
                SqlBuilder.JoinType.ASOF_LEFT,
                null,
                rightAlias,
                right,
                onCondition,
                null));

        return result;
    }

    // (generateJoinCondition and resolveJoinColumn — DELETED: replaced by sidecar-based columnAlias)

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

    // ========== pivot ==========
    /**
     * Generates PIVOT SQL from TypeInfo sidecar (all metadata extracted by CleanCompiler).
     * SQL: PIVOT (source) ON pivotCol USING AGG(valueCol) AS "_|__alias"
     */
    private SqlBuilder generatePivot(AppliedFunction af) {
        SqlBuilder source = generateRelation(af.parameters().get(0));

        TypeInfo info = unit.typeInfoFor(af);
        if (info == null || info.pivotSpec() == null) {
            throw new PureCompileException("pivot(): missing PivotSpec in TypeInfo sidecar");
        }
        TypeInfo.PivotSpec spec = info.pivotSpec();

        // Convert compiler PivotAggSpecs to SqlBuilder PivotAggregates
        List<SqlBuilder.PivotAggregate> aggregates = new java.util.ArrayList<>();
        for (var agg : spec.aggregates()) {
            String valueExprSql = null;
            if (agg.valueExpr() != null) {
                // Complex expression — compile to SQL with lambda param as rowParam
                // so property accesses render as column refs, not struct field access
                SqlExpr expr = generateScalar(agg.valueExpr(), agg.lambdaParam(), null, null);
                valueExprSql = expr.toSql(dialect);
            }
            aggregates.add(new SqlBuilder.PivotAggregate(
                    agg.aggFunction(), agg.valueColumn(), valueExprSql, agg.alias()));
        }
        // Build pivot on a fresh builder — source becomes fromSubquery so
        // renderPivot serializes the full source chain (WHERE, SELECT, etc.)
        SqlBuilder pivotBuilder = new SqlBuilder().fromSubquery(source, null);
        pivotBuilder.pivot(new SqlBuilder.PivotClause(spec.pivotColumns(), aggregates));
        return pivotBuilder;
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
        // Compiler-driven inlining: follow inlinedBody pointers (let bindings, blocks, user functions)
        TypeInfo vsInfo = unit.types().get(vs);
        if (vsInfo != null && vsInfo.inlinedBody() != null) {
            return generateScalar(vsInfo.inlinedBody(), rowParam, mapping, tableAlias);
        }
        return switch (vs) {
            case AppliedProperty ap -> {
                // Join condition: CleanCompiler tagged this with columnAlias ("left_src" or "right_src")
                TypeInfo apInfo = unit.types().get(ap);
                if (apInfo != null && apInfo.columnAlias() != null) {
                    yield new SqlExpr.Column(apInfo.columnAlias(), ap.property());
                }
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
                    // Check for enum mapping first
                    var pmOpt = mapping.getPropertyMapping(colName);
                    if (pmOpt.isPresent() && pmOpt.get().hasEnumMapping()) {
                        var pm = pmOpt.get();
                        String dbCol = pm.columnName();
                        SqlExpr colExpr = tableAlias != null
                                ? new SqlExpr.Column(tableAlias, dbCol)
                                : new SqlExpr.ColumnRef(dbCol);
                        // Build CASE WHEN branches from enum value mappings
                        var branches = new java.util.ArrayList<SqlExpr.SearchedCase.WhenBranch>();
                        for (var entry : pm.enumMapping().entrySet()) {
                            String enumValue = entry.getKey();
                            List<Object> sourceValues = entry.getValue();
                            // Build OR condition for multiple source values
                            List<SqlExpr> conditions = sourceValues.stream()
                                    .map(sv -> (SqlExpr) new SqlExpr.Binary(colExpr, "=",
                                            new SqlExpr.StringLiteral(sv.toString())))
                                    .toList();
                            SqlExpr condition = conditions.size() == 1
                                    ? conditions.get(0)
                                    : new SqlExpr.Or(conditions);
                            branches.add(new SqlExpr.SearchedCase.WhenBranch(
                                    condition, new SqlExpr.StringLiteral(enumValue)));
                        }
                        yield new SqlExpr.SearchedCase(branches, new SqlExpr.NullLiteral());
                    }
                    var columnOpt = mapping.getColumnForProperty(colName);
                    if (columnOpt.isPresent())
                        colName = columnOpt.get();
                }
                // Struct field access in lambda: $f.legalName → f.legalName
                // Only when owner is NOT the relational row param (relational row accesses → column ref)
                if (tableAlias == null && !ap.parameters().isEmpty()
                        && ap.parameters().get(0) instanceof Variable owner
                        && (rowParam == null || !owner.name().equals(rowParam))) {
                    yield new SqlExpr.FieldAccess(new SqlExpr.Identifier(owner.name()), colName);
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
            case CInteger i -> {
                // Check compiler type annotation for precision
                TypeInfo tiInt = unit.typeInfoFor(i);
                if (tiInt != null && tiInt.scalarType() == GenericType.Primitive.INT128) {
                    yield new SqlExpr.Cast(new SqlExpr.NumericLiteral(i.value()), "Int128");
                }
                yield new SqlExpr.NumericLiteral(i.value());
            }
            case CFloat f -> {
                yield new SqlExpr.DecimalLiteral(java.math.BigDecimal.valueOf(f.value()));
            }
            case CDecimal d -> {
                // Check compiler type annotation for precision
                TypeInfo tiDec = unit.typeInfoFor(d);
                if (tiDec != null && tiDec.scalarType() instanceof GenericType.PrecisionDecimal pd) {
                    yield new SqlExpr.Cast(
                            new SqlExpr.DecimalLiteral(d.value()),
                            "Decimal(" + pd.precision() + "," + pd.scale() + ")");
                }
                yield new SqlExpr.DecimalLiteral(d.value());
            }
            case CString s -> new SqlExpr.StringLiteral(s.value());
            case CBoolean b -> new SqlExpr.BoolLiteral(b.value());
            case CDateTime dt -> new SqlExpr.TimestampLiteral(dt.value());
            case CStrictDate sd -> new SqlExpr.DateLiteral(sd.value());
            case CStrictTime st -> new SqlExpr.TimeLiteral(st.value());
            case CLatestDate ld -> new SqlExpr.CurrentDate();
            case Variable v -> {
                if (v.name().equals(rowParam))
                    yield new SqlExpr.ColumnRef("");
                // Lambda param → raw unquoted identifier (from compiler side-table)
                TypeInfo vti = unit.types().get(v);
                if (vti != null && vti.lambdaParam())
                    yield new SqlExpr.Identifier(v.name());
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
                // Mixed-type lists need TO_JSON wrapping for DuckDB variant handling
                TypeInfo collInfo = unit.typeInfoFor(coll);
                if (collInfo != null && collInfo.isMixedList()
                        && coll.values().stream().noneMatch(v -> v instanceof ClassInstance)) {
                    exprs = exprs.stream()
                            .map(e -> (SqlExpr) new SqlExpr.FunctionCall("toJson", List.of(e)))
                            .collect(Collectors.toList());
                }
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
                // Check literal strings, or typed lambda params (from compiler side-table)
                boolean isStringConcat = params.get(0) instanceof CString || params.get(1) instanceof CString;
                if (!isStringConcat) {
                    // Check TypeInfo for string type (e.g., lambda param x: String[1])
                    for (var p : params) {
                        TypeInfo pti = unit.types().get(p);
                        if (pti != null && pti.scalarType() == GenericType.Primitive.STRING) {
                            isStringConcat = true;
                            break;
                        }
                    }
                }
                if (isStringConcat) {
                    yield new SqlExpr.Binary(c.apply(params.get(0)), "||", c.apply(params.get(1)));
                }
                // Numeric addition: only valid when compiler confirms numeric types
                boolean isNumeric = false;
                for (var p : params) {
                    TypeInfo pti = unit.types().get(p);
                    if (pti != null && pti.scalarType() != null && pti.scalarType().isNumeric()) {
                        isNumeric = true;
                        break;
                    }
                    // Literal numeric types are always arithmetic
                    if (p instanceof CInteger || p instanceof CFloat || p instanceof CDecimal) {
                        isNumeric = true;
                        break;
                    }
                }
                if (!isNumeric) {
                    throw new PureCompileException(
                            "plus(): cannot determine type — compiler must tag operands. "
                            + "Params: " + params.stream()
                                .map(p -> p.getClass().getSimpleName() + ":" + unit.types().get(p))
                                .toList());
                }
                yield new SqlExpr.Binary(c.apply(params.get(0)), "+", c.apply(params.get(1)));
            }
            case "minus" -> {
                if (params.size() == 1) {
                    // Unary minus: (-1 * x) to match old pipeline
                    yield new SqlExpr.Binary(new SqlExpr.NumericLiteral(-1), "*", c.apply(params.get(0)));
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
                    // Check if list has mixed types (List<ANY>) — needs TO_JSON wrapping
                    TypeInfo listInfo = unit.typeInfoFor(params.get(0));
                    if (listInfo != null && listInfo.isMixedList()
                            && params.get(0) instanceof Collection coll
                            && coll.values().stream().noneMatch(v -> v instanceof ClassInstance)) {
                        // Mixed-type list: wrap elements in toJson for comparable representation
                        // "toJson" is a semantic name — dialect maps it (DuckDB: TO_JSON)
                        var wrappedElems = coll.values().stream()
                                .map(v -> (SqlExpr) new SqlExpr.FunctionCall("toJson",
                                        List.of(c.apply(v))))
                                .collect(Collectors.toList());
                        SqlExpr wrappedList = new SqlExpr.ArrayLiteral(wrappedElems);
                        SqlExpr wrappedSearch = new SqlExpr.FunctionCall("toJson",
                                List.of(c.apply(params.get(1))));
                        yield new SqlExpr.ListContains(wrappedList, wrappedSearch);
                    }
                    yield new SqlExpr.ListContains(c.apply(params.get(0)), c.apply(params.get(1)));
                }
                // String contains: strPos(str, substr) > 0
                yield new SqlExpr.Binary(
                        new SqlExpr.FunctionCall("strPos", List.of(c.apply(params.get(0)), c.apply(params.get(1)))),
                        ">", new SqlExpr.NumericLiteral(0));
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
                SqlExpr start = c.apply(params.get(1));
                SqlExpr offset = new SqlExpr.Binary(start, "+", new SqlExpr.NumericLiteral(1));
                if (params.size() > 2) {
                    // Pure substring(str, start, end) → SQL SUBSTRING(str, start+1, end-start)
                    SqlExpr end = c.apply(params.get(2));
                    SqlExpr length = new SqlExpr.Binary(end, "-", start);
                    yield new SqlExpr.FunctionCall("SUBSTRING",
                            List.of(c.apply(params.get(0)), offset, length));
                }
                yield new SqlExpr.FunctionCall("SUBSTRING",
                        List.of(c.apply(params.get(0)), offset));
            }
            case "indexOf" -> {
                if (firstArgIsList) {
                    // Pure is 0-based, LIST_POSITION is 1-based → subtract 1
                    yield new SqlExpr.Binary(
                            new SqlExpr.FunctionCall("indexOf",
                                    List.of(c.apply(params.get(0)), c.apply(params.get(1)))),
                            "-", new SqlExpr.NumericLiteral(1));
                }
                // String indexOf with optional fromIndex
                if (params.size() > 2) {
                    // indexOf(str, search, fromIndex) →
                    // ((fromIndex + INSTR(SUBSTRING(str, fromIndex+1), search)) - 1)
                    SqlExpr str = c.apply(params.get(0));
                    SqlExpr search = c.apply(params.get(1));
                    SqlExpr fromIdx = c.apply(params.get(2));
                    yield new SqlExpr.FunctionCall("indexOfFrom",
                            List.of(str, search, fromIdx));
                }
                // Simple string indexOf: (instr(str, substr) - 1)
                yield new SqlExpr.Binary(
                        new SqlExpr.FunctionCall("instr", List.of(c.apply(params.get(0)), c.apply(params.get(1)))),
                        "-", new SqlExpr.NumericLiteral(1));
            }
            case "replace" -> new SqlExpr.FunctionCall("REPLACE",
                    List.of(c.apply(params.get(0)), c.apply(params.get(1)), c.apply(params.get(2))));

            // --- Null checks ---
            case "isEmpty" -> {
                if (firstArgIsList) {
                    yield new SqlExpr.Binary(
                            new SqlExpr.FunctionCall("listLength", List.of(c.apply(params.get(0)))),
                            "=", new SqlExpr.NumericLiteral(0));
                }
                yield new SqlExpr.IsNull(c.apply(params.get(0)));
            }
            case "isNotEmpty" -> {
                if (firstArgIsList) {
                    yield new SqlExpr.Binary(
                            new SqlExpr.FunctionCall("listLength", List.of(c.apply(params.get(0)))),
                            ">", new SqlExpr.NumericLiteral(0));
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
                    // Pure round() uses banker's rounding (round half to even)
                    yield new SqlExpr.FunctionCall("roundHalfEven",
                            List.of(c.apply(params.get(0)), c.apply(params.get(1))));
                }
                yield new SqlExpr.Cast(
                        new SqlExpr.FunctionCall("roundHalfEven",
                                List.of(c.apply(params.get(0)), new SqlExpr.NumericLiteral(0))),
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
            case "toInteger", "parseInteger", "toFloat", "parseFloat", "toDecimal" -> {
                // Read target type from compiler's TypeInfo — compiler is the source of truth
                TypeInfo castInfo = unit.types().get(af);
                String castType = (castInfo != null && castInfo.scalarType() != null)
                        ? castInfo.scalarType().typeName() : switch (funcName) {
                            case "toInteger", "parseInteger" -> "Integer";
                            case "toFloat", "parseFloat" -> "Float";
                            default -> "Decimal";
                        };
                yield new SqlExpr.Cast(c.apply(params.get(0)), castType);
            }
            case "parseDecimal" -> {
                // parseDecimal: strip d/D suffix then CAST — read type from TypeInfo
                TypeInfo castInfo = unit.types().get(af);
                String castType = (castInfo != null && castInfo.scalarType() != null)
                        ? castInfo.scalarType().typeName() : "Decimal";
                yield new SqlExpr.Cast(
                        new SqlExpr.FunctionCall("regexpReplace",
                                List.of(c.apply(params.get(0)), new SqlExpr.StringLiteral("[dD]$"),
                                        new SqlExpr.StringLiteral(""))),
                        castType);
            }

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
                // Multi-if: [pair(cond, val), ...] -> if(default) — compile to CASE WHEN
                // params[0] is Collection of pairs, params[1] is else lambda
                if (params.size() >= 2 && params.get(0) instanceof Collection pairsColl) {
                    List<SqlExpr.SearchedCase.WhenBranch> branches = new ArrayList<>();
                    for (var pairExpr : pairsColl.values()) {
                        if (pairExpr instanceof AppliedFunction pairAf
                                && pairAf.parameters().size() >= 2) {
                            // pair(|condition, |value) — both are lambdas
                            var condParam = pairAf.parameters().get(0);
                            var valParam = pairAf.parameters().get(1);
                            SqlExpr condExpr;
                            if (condParam instanceof LambdaFunction condLf && !condLf.body().isEmpty()) {
                                condExpr = generateScalar(condLf.body().get(0), rowParam, mapping, tableAlias);
                            } else {
                                condExpr = c.apply(condParam);
                            }
                            SqlExpr valExpr;
                            if (valParam instanceof LambdaFunction valLf && !valLf.body().isEmpty()) {
                                valExpr = generateScalar(valLf.body().get(0), rowParam, mapping, tableAlias);
                            } else {
                                valExpr = c.apply(valParam);
                            }
                            branches.add(new SqlExpr.SearchedCase.WhenBranch(condExpr, valExpr));
                        }
                    }
                    // else value from params[1] (lambda)
                    SqlExpr elseExpr;
                    if (params.get(1) instanceof LambdaFunction elseLf && !elseLf.body().isEmpty()) {
                        elseExpr = generateScalar(elseLf.body().get(0), rowParam, mapping, tableAlias);
                    } else {
                        elseExpr = c.apply(params.get(1));
                    }
                    yield new SqlExpr.SearchedCase(branches, elseExpr);
                }
                yield c.apply(params.get(0));
            }

            // --- In ---
            case "in" -> {
                SqlExpr left = c.apply(params.get(0));
                if (params.get(1) instanceof Collection coll) {
                    // Check if this is a mixed-type list — needs TO_JSON wrapping
                    TypeInfo listInfo = unit.typeInfoFor(params.get(1));
                    if (listInfo != null && listInfo.isMixedList()
                            && coll.values().stream().noneMatch(v -> v instanceof ClassInstance)) {
                        var wrappedElems = coll.values().stream()
                                .map(v -> (SqlExpr) new SqlExpr.FunctionCall("toJson",
                                        List.of(c.apply(v))))
                                .collect(Collectors.toList());
                        SqlExpr wrappedList = new SqlExpr.ArrayLiteral(wrappedElems);
                        SqlExpr wrappedSearch = new SqlExpr.FunctionCall("toJson", List.of(left));
                        yield new SqlExpr.ListContains(wrappedList, wrappedSearch);
                    }
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
                        coalArgs.add(new SqlExpr.NullLiteral());
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
                        // Year-only: wrap in strftime('%Y', CAST(adjusted AS DATE))
                        yield new SqlExpr.FunctionCall("strftime", List.of(
                                new SqlExpr.Cast(adjusted, "Date"),
                                new SqlExpr.StringLiteral("%Y")));
                    } else if (raw.matches("\\d{4}-\\d{2}")) {
                        // Year-month: wrap in strftime('%Y-%m', CAST(adjusted AS DATE))
                        yield new SqlExpr.FunctionCall("strftime", List.of(
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
            case "mod" -> {
                // Pure mod always returns non-negative: MOD((MOD(x, n) + n), n)
                SqlExpr x = c.apply(params.get(0));
                SqlExpr n = c.apply(params.get(1));
                SqlExpr innerMod = new SqlExpr.FunctionCall("MOD", List.of(x, n));
                SqlExpr adjusted = new SqlExpr.Binary(innerMod, "+", n);
                yield new SqlExpr.FunctionCall("MOD", List.of(adjusted, n));
            }
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
                SqlExpr idx = new SqlExpr.Binary(c.apply(params.get(2)), "+", new SqlExpr.NumericLiteral(1));
                // Empty input -> NULL
                if (params.get(0) instanceof Collection col && col.values().isEmpty()) {
                    yield new SqlExpr.NullLiteral();
                }
                yield new SqlExpr.CaseWhen(
                        new SqlExpr.Binary(delim, "=", new SqlExpr.StringLiteral("")),
                        str,
                        new SqlExpr.FunctionCall("splitPart", List.of(str, delim, idx)));
            }
            case "joinStrings" -> {
                if (params.size() == 4) {
                    // joinStrings(list, prefix, separator, suffix)
                    yield new SqlExpr.FunctionCall("joinStringsWithPrefixSuffix",
                            List.of(c.apply(params.get(0)), c.apply(params.get(1)),
                                    c.apply(params.get(2)), c.apply(params.get(3))));
                }
                if (params.size() > 1) {
                    SqlExpr listArg = c.apply(params.get(0));
                    // If first arg is not a list, wrap in list brackets
                    if (!firstArgIsList) {
                        listArg = new SqlExpr.FunctionCall("wrapList", List.of(listArg));
                    }
                    yield new SqlExpr.FunctionCall("arrayToString",
                            List.of(listArg, c.apply(params.get(1))));
                }
                yield new SqlExpr.FunctionCall("CONCAT", List.of(c.apply(params.get(0))));
            }
            case "find" -> {
                if (firstArgIsList && params.size() > 1 && params.get(1) instanceof LambdaFunction lf) {
                    // Collection find: LIST_EXTRACT(list_filter(list, (s) -> predicate), 1)
                    String paramName = lf.parameters().isEmpty() ? "s" : lf.parameters().get(0).name();
                    SqlExpr body = !lf.body().isEmpty()
                            ? c.apply(lf.body().get(0)) : new SqlExpr.NullLiteral();
                    SqlExpr lambda = new SqlExpr.LambdaExpr(List.of(paramName), body);
                    yield new SqlExpr.FunctionCall("listFind",
                            List.of(c.apply(params.get(0)), lambda));
                }
                yield new SqlExpr.StrPosition(c.apply(params.get(1)), c.apply(params.get(0)));
            }
            case "levenshteinDistance" -> new SqlExpr.FunctionCall("levenshteinDistance",
                    List.of(c.apply(params.get(0)), c.apply(params.get(1))));
            case "ascii" -> new SqlExpr.FunctionCall("ASCII", List.of(c.apply(params.get(0))));
            case "char" -> new SqlExpr.FunctionCall("CHR", List.of(c.apply(params.get(0))));
            case "hash" -> {
                // Dispatch on HashType enum: MD5, SHA256, etc.
                if (params.size() > 1 && params.get(1) instanceof EnumValue ev) {
                    String hashAlgo = ev.value().toUpperCase();
                    yield new SqlExpr.FunctionCall(hashAlgo, List.of(c.apply(params.get(0))));
                }
                if (params.size() > 1 && params.get(1) instanceof PackageableElementPtr ptr) {
                    String path = ptr.fullPath();
                    if (path.contains("SHA256"))
                        yield new SqlExpr.FunctionCall("SHA256", List.of(c.apply(params.get(0))));
                    if (path.contains("MD5"))
                        yield new SqlExpr.FunctionCall("MD5", List.of(c.apply(params.get(0))));
                }
                yield new SqlExpr.FunctionCall("hash", List.of(c.apply(params.get(0))));
            }
            case "lpad" -> {
                SqlExpr str = c.apply(params.get(0));
                SqlExpr len = c.apply(params.get(1));
                SqlExpr fill = params.size() > 2 ? c.apply(params.get(2)) : new SqlExpr.StringLiteral(" ");
                yield new SqlExpr.FunctionCall("lpadSafe",
                        List.of(str, len, fill));
            }
            case "rpad" -> {
                SqlExpr str = c.apply(params.get(0));
                SqlExpr len = c.apply(params.get(1));
                SqlExpr fill = params.size() > 2 ? c.apply(params.get(2)) : new SqlExpr.StringLiteral(" ");
                yield new SqlExpr.FunctionCall("rpadSafe",
                        List.of(str, len, fill));
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
                        yield new SqlExpr.FunctionCall("strftime",
                                List.of(c.apply(params.get(0)), new SqlExpr.StringLiteral("%Y")));
                    } else if (raw.matches("\\d{4}-\\d{2}")) {
                        yield new SqlExpr.FunctionCall("strftime",
                                List.of(c.apply(params.get(0)), new SqlExpr.StringLiteral("%Y-%m")));
                    }
                }
                yield new SqlExpr.FunctionCall("dateTruncDay",
                        List.of(c.apply(params.get(0))));
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
                yield new SqlExpr.FunctionCall("dateDiff",
                        List.of(new SqlExpr.IntervalLiteral(dunit), start, end));
            }
            case "date" -> {
                SqlExpr year = c.apply(params.get(0));
                if (params.size() == 1) {
                    // Year only: strftime(makeDate(year,1,1), '%Y')
                    yield new SqlExpr.FunctionCall("strftime", List.of(
                            new SqlExpr.FunctionCall("makeDate", List.of(year,
                                    new SqlExpr.NumericLiteral(1), new SqlExpr.NumericLiteral(1))),
                            new SqlExpr.StringLiteral("%Y")));
                } else if (params.size() == 2) {
                    // Year-month: strftime(makeDate(year,month,1), '%Y-%m')
                    yield new SqlExpr.FunctionCall("strftime", List.of(
                            new SqlExpr.FunctionCall("makeDate", List.of(year,
                                    c.apply(params.get(1)), new SqlExpr.NumericLiteral(1))),
                            new SqlExpr.StringLiteral("%Y-%m")));
                } else if (params.size() == 3) {
                    // Full date: makeDate(year,month,day)
                    yield new SqlExpr.FunctionCall("makeDate",
                            List.of(year, c.apply(params.get(1)), c.apply(params.get(2))));
                } else if (params.size() == 4) {
                    // To hour: strftime(makeTimestamp(y,m,d,h,0,0), '%Y-%m-%dT%H')
                    yield new SqlExpr.FunctionCall("strftime", List.of(
                            new SqlExpr.FunctionCall("makeTimestamp", List.of(
                                    year, c.apply(params.get(1)), c.apply(params.get(2)),
                                    c.apply(params.get(3)), new SqlExpr.NumericLiteral(0), new SqlExpr.NumericLiteral(0))),
                            new SqlExpr.StringLiteral("%Y-%m-%dT%H")));
                } else if (params.size() == 5) {
                    // To minute: strftime(makeTimestamp(y,m,d,h,min,0), '%Y-%m-%dT%H:%M')
                    yield new SqlExpr.FunctionCall("strftime", List.of(
                            new SqlExpr.FunctionCall("makeTimestamp", List.of(
                                    year, c.apply(params.get(1)), c.apply(params.get(2)),
                                    c.apply(params.get(3)), c.apply(params.get(4)), new SqlExpr.NumericLiteral(0))),
                            new SqlExpr.StringLiteral("%Y-%m-%dT%H:%M")));
                } else {
                    // To second: regexpReplace(strftime(makeTimestamp(y,m,d,h,min,sec), ...), '0{1,5}$', '')
                    SqlExpr makeTs = new SqlExpr.FunctionCall("makeTimestamp", List.of(
                            year, c.apply(params.get(1)), c.apply(params.get(2)),
                            c.apply(params.get(3)), c.apply(params.get(4)), c.apply(params.get(5))));
                    yield new SqlExpr.FunctionCall("regexpReplace", List.of(
                            new SqlExpr.FunctionCall("strftime", List.of(
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
                            ? "TimestampNS" : "Date";
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
                    yield new SqlExpr.BoolLiteral(true);
                } else if (params.get(0) instanceof CStrictDate) {
                    yield new SqlExpr.BoolLiteral(false);
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
                    yield new SqlExpr.BoolLiteral(hasMin);
                } else if (params.get(0) instanceof CStrictDate) {
                    yield new SqlExpr.BoolLiteral(false);
                }
                yield new SqlExpr.FunctionCall("MINUTE", List.of(c.apply(params.get(0))));
            }
            case "hasMonth" -> {
                // Pure dates: year-only has no month, everything else has month
                if (params.get(0) instanceof CStrictDate || params.get(0) instanceof CDateTime) {
                    yield new SqlExpr.BoolLiteral(true);
                }
                yield new SqlExpr.FunctionCall("MONTH", List.of(c.apply(params.get(0))));
            }

            // --- Date constants (SQL keywords, not functions — no parens) ---
            case "now" -> new SqlExpr.CurrentTimestamp();
            case "today" -> new SqlExpr.CurrentDate();

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
                    c.apply(params.get(0)), "&",
                    c.apply(params.get(1)));
            case "bitOr" -> new SqlExpr.Binary(
                    c.apply(params.get(0)), "|",
                    c.apply(params.get(1)));
            case "bitXor" -> new SqlExpr.FunctionCall("bitXor",
                    List.of(c.apply(params.get(0)),
                            c.apply(params.get(1))));
            case "bitNot" -> new SqlExpr.Unary("~",
                    c.apply(params.get(0)));
            case "bitShiftLeft" -> new SqlExpr.Binary(
                    new SqlExpr.Cast(c.apply(params.get(0)), "Integer"), "<<",
                    c.apply(params.get(1)));
            case "bitShiftRight" -> {
                // Old pipeline: CASE WHEN shift > 62 THEN CAST(1 AS BIGINT) << shift
                //               ELSE CAST(val AS BIGINT) >> shift END
                SqlExpr val = new SqlExpr.Cast(c.apply(params.get(0)), "Integer");
                SqlExpr shift = c.apply(params.get(1));
                yield new SqlExpr.FunctionCall("bitShiftRightSafe",
                        List.of(val, shift));
            }
            case "xor" -> {
                // Boolean xor: (A AND NOT B) OR (NOT A AND B)
                SqlExpr a = c.apply(params.get(0));
                SqlExpr b = c.apply(params.get(1));
                yield new SqlExpr.Or(List.of(
                        new SqlExpr.And(List.of(a, new SqlExpr.Not(b))),
                        new SqlExpr.And(List.of(new SqlExpr.Not(a), b))));
            }

            // --- Collection/list ---
            case "size", "count" -> {
                // Dispatch based on source type
                if (!params.isEmpty()) {
                    TypeInfo sourceInfo = unit.types().get(params.get(0));
                    if (sourceInfo != null && sourceInfo.relationType() != null) {
                        // Relational size: (SELECT COUNT('*') FROM (subquery) AS subq)
                        SqlBuilder source = generateRelation(params.get(0));
                        SqlBuilder countQuery = new SqlBuilder()
                                .addSelect(new SqlExpr.FunctionCall("COUNT",
                                        List.of(new SqlExpr.StringLiteral("*"))), null)
                                .fromSubquery(source, "subq");
                        yield new SqlExpr.Subquery(countQuery);
                    }
                    // List/array size: use LEN (not aggregate COUNT)
                    if (sourceInfo != null && sourceInfo.scalarType() != null
                            && sourceInfo.scalarType().isList()) {
                        yield new SqlExpr.FunctionCall("listLength",
                                List.of(c.apply(params.get(0))));
                    }
                    // Lambda param size (fold accumulator etc.) — also use LEN
                    if (sourceInfo != null && sourceInfo.lambdaParam()
                            && sourceInfo.relationType() == null) {
                        yield new SqlExpr.FunctionCall("listLength",
                                List.of(c.apply(params.get(0))));
                    }
                }
                // No fallback — compiler must type the source. Fix CleanCompiler if this fires.
                throw new PureCompileException(
                        "PlanGenerator: size() source has no TypeInfo. Compiler bug — fix CleanCompiler.");
            }
            case "at" -> {
                // at(0) on a scalar (e.g., struct field p.lastName) is identity — skip LIST_EXTRACT
                if (!firstArgIsList) {
                    yield c.apply(params.get(0));
                }
                yield new SqlExpr.FunctionCall("listExtract",
                    List.of(c.apply(params.get(0)),
                            new SqlExpr.Binary(c.apply(params.get(1)), "+",
                                    new SqlExpr.NumericLiteral(1))));
            }
            case "head", "first" -> new SqlExpr.FunctionCall("listExtract",
                    List.of(c.apply(params.get(0)), new SqlExpr.NumericLiteral(1)));
            case "last" -> {
                SqlExpr arg = c.apply(params.get(0));
                yield new SqlExpr.FunctionCall("listExtract",
                        List.of(arg,
                                new SqlExpr.FunctionCall("listLength", List.of(arg))));
            }
            case "tail" -> {
                SqlExpr listArg = firstArgIsList ? c.apply(params.get(0))
                        : new SqlExpr.FunctionCall("wrapList", List.of(c.apply(params.get(0))));
                yield new SqlExpr.FunctionCall("listSlice",
                        List.of(listArg, new SqlExpr.NumericLiteral(2),
                                new SqlExpr.FunctionCall("listLength", List.of(listArg))));
            }
            case "init" -> {
                SqlExpr listArg = firstArgIsList ? c.apply(params.get(0))
                        : new SqlExpr.FunctionCall("wrapList", List.of(c.apply(params.get(0))));
                yield new SqlExpr.FunctionCall("listSlice",
                        List.of(listArg, new SqlExpr.NumericLiteral(1),
                                new SqlExpr.Binary(
                                        new SqlExpr.FunctionCall("listLength", List.of(listArg)),
                                        "-", new SqlExpr.NumericLiteral(1))));
            }
            case "add" -> {
                if (params.size() > 2) {
                    // add(list, offset, value) — splice: insert value at offset position
                    SqlExpr list = c.apply(params.get(0));
                    SqlExpr offset = c.apply(params.get(1));
                    SqlExpr value = c.apply(params.get(2));
                    // LIST_CONCAT(LIST_CONCAT(LIST_SLICE(list, 1, offset), LIST_VALUE(value)),
                    //             LIST_SLICE(list, offset+1, LENGTH(list)))
                    SqlExpr leftSlice = new SqlExpr.FunctionCall("listSlice",
                            List.of(list, new SqlExpr.NumericLiteral(1), offset));
                    SqlExpr wrapped = new SqlExpr.FunctionCall("LIST_VALUE", List.of(value));
                    SqlExpr leftConcat = new SqlExpr.FunctionCall("listConcat",
                            List.of(leftSlice, wrapped));
                    SqlExpr rightStart = new SqlExpr.Binary(offset, "+", new SqlExpr.NumericLiteral(1));
                    SqlExpr listLen = new SqlExpr.FunctionCall("LENGTH", List.of(list));
                    SqlExpr rightSlice = new SqlExpr.FunctionCall("listSlice",
                            List.of(list, rightStart, listLen));
                    yield new SqlExpr.FunctionCall("listConcat",
                            List.of(leftConcat, rightSlice));
                }
                yield new SqlExpr.FunctionCall("listAppend",
                        List.of(c.apply(params.get(0)), c.apply(params.get(1))));
            }
            case "concatenate" -> {
                // LIST_CONCAT requires list arguments — wrap scalars
                SqlExpr left = c.apply(params.get(0));
                SqlExpr right = c.apply(params.get(1));
                if (!firstArgIsList) {
                    left = new SqlExpr.FunctionCall("wrapList", List.of(left));
                }
                // Check if second arg is a scalar (not a list/collection)
                if (!(params.get(1) instanceof org.finos.legend.pure.dsl.ast.Collection)) {
                    TypeInfo rightInfo = unit.types().get(params.get(1));
                    boolean rightIsList = params.get(1) instanceof org.finos.legend.pure.dsl.ast.Collection
                            || (rightInfo != null && rightInfo.scalarType() != null && rightInfo.scalarType().isList());
                    if (!rightIsList) {
                        right = new SqlExpr.FunctionCall("wrapList", List.of(right));
                    }
                }

                // Cross-list mixed-type: [1,2,3].concatenate(['a','b'])
                // Each list is homogeneous but they have different element types → need TO_JSON
                // But skip when either side is empty collection (fold+add desugar with Nil/Any —
                // empty list is polymorphically compatible with any type)
                TypeInfo leftInfo = unit.typeInfoFor(params.get(0));
                TypeInfo rightInfo2 = unit.typeInfoFor(params.get(1));
                boolean leftEmpty = params.get(0) instanceof org.finos.legend.pure.dsl.ast.Collection lc
                        && lc.values().isEmpty();
                boolean rightEmpty = params.get(1) instanceof org.finos.legend.pure.dsl.ast.Collection rc
                        && rc.values().isEmpty();
                if (!leftEmpty && !rightEmpty
                        && leftInfo != null && rightInfo2 != null
                        && leftInfo.scalarType() != null && rightInfo2.scalarType() != null
                        && !leftInfo.scalarType().equals(rightInfo2.scalarType())) {
                    // Re-render with TO_JSON wrapping
                    left = wrapCollectionInToJson(params.get(0), c);
                    right = wrapCollectionInToJson(params.get(1), c);
                }

                yield new SqlExpr.FunctionCall("listConcat", List.of(left, right));
            }
            case "take" -> new SqlExpr.FunctionCall("listSlice",
                    List.of(c.apply(params.get(0)), new SqlExpr.NumericLiteral(1),
                            c.apply(params.get(1))));
            case "drop" -> new SqlExpr.FunctionCall("listSlice",
                    List.of(c.apply(params.get(0)),
                            new SqlExpr.FunctionCall("greatest", List.of(
                                    new SqlExpr.Binary(c.apply(params.get(1)), "+",
                                            new SqlExpr.NumericLiteral(1)),
                                    new SqlExpr.NumericLiteral(1))),
                            new SqlExpr.FunctionCall("listLength",
                                    List.of(c.apply(params.get(0))))));
            case "slice" -> {
                SqlExpr list = c.apply(params.get(0));
                SqlExpr start = c.apply(params.get(1));
                SqlExpr end = c.apply(params.get(2));
                // Pure slice is 0-based, DuckDB LIST_SLICE is 1-based
                // Pre-compute for literal starts; negative→clamp to 1
                if (params.get(1) instanceof CInteger ci) {
                    long startVal = ci.value().longValue();
                    long offset = Math.max(startVal + 1, 1);
                    yield new SqlExpr.FunctionCall("listSlice",
                            List.of(list, new SqlExpr.NumericLiteral(offset), end));
                }
                // Dynamic: max(start + 1, 1)
                yield new SqlExpr.FunctionCall("listSlice",
                        List.of(list,
                                new SqlExpr.Binary(start, "+", new SqlExpr.NumericLiteral(1)),
                                end));
            }

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
            case "greatest" -> {
                if (firstArgIsList) yield new SqlExpr.FunctionCall("listMax",
                        params.stream().map(c).collect(Collectors.toList()));
                // Single scalar: greatest(x) = x (identity)
                if (params.size() == 1) yield c.apply(params.get(0));
                yield new SqlExpr.FunctionCall("GREATEST",
                        params.stream().map(c).collect(Collectors.toList()));
            }
            case "least" -> {
                if (firstArgIsList) yield new SqlExpr.FunctionCall("listMin",
                        params.stream().map(c).collect(Collectors.toList()));
                // Single scalar: least(x) = x (identity)
                if (params.size() == 1) yield c.apply(params.get(0));
                yield new SqlExpr.FunctionCall("LEAST",
                        params.stream().map(c).collect(Collectors.toList()));
            }
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
            case "variance", "varianceSample" -> {
                // variance(list, isSample) — isSample=true→var_samp, isSample=false→var_pop
                if (params.size() > 1 && params.get(1) instanceof CBoolean cb && !cb.value()) {
                    yield new SqlExpr.FunctionCall(
                            firstArgIsList ? "listVariancePopulation" : "variancePopulation",
                            List.of(c.apply(params.get(0))));
                }
                yield new SqlExpr.FunctionCall(
                        firstArgIsList ? "listVarianceSample" : "varianceSample",
                        List.of(c.apply(params.get(0))));
            }
            case "variancePopulation" -> new SqlExpr.FunctionCall(
                    firstArgIsList ? "listVariancePopulation" : "variancePopulation",
                    List.of(c.apply(params.get(0))));
            case "corr" -> {
                // Bivariate: always needs paired sequences via UNNEST
                SqlExpr arg0 = c.apply(params.get(0));
                if (!firstArgIsList) arg0 = new SqlExpr.ArrayLiteral(List.of(arg0));
                yield new SqlExpr.FunctionCall("listCorr", List.of(arg0, c.apply(params.get(1))));
            }
            case "covarSample" -> {
                SqlExpr arg0 = c.apply(params.get(0));
                if (!firstArgIsList) arg0 = new SqlExpr.ArrayLiteral(List.of(arg0));
                yield new SqlExpr.FunctionCall("listCovarSample", List.of(arg0, c.apply(params.get(1))));
            }
            case "covarPopulation" -> {
                SqlExpr arg0 = c.apply(params.get(0));
                if (!firstArgIsList) arg0 = new SqlExpr.ArrayLiteral(List.of(arg0));
                yield new SqlExpr.FunctionCall("listCovarPopulation", List.of(arg0, c.apply(params.get(1))));
            }
            case "percentile", "percentileCont" -> {
                SqlExpr list = c.apply(params.get(0));
                SqlExpr p = c.apply(params.get(1));
                // percentile(value, ascending, continuous)
                // param[2]: ascending — when false, invert p to (1 - p)
                boolean ascending = true;
                boolean continuous = true;
                if (params.size() > 2 && params.get(2) instanceof CBoolean cb) {
                    ascending = cb.value();
                }
                // param[3]: continuous — when false, use percentileDisc
                if (params.size() > 3 && params.get(3) instanceof CBoolean cb) {
                    continuous = cb.value();
                }
                if (!ascending) {
                    p = new SqlExpr.Binary(new SqlExpr.NumericLiteral(1), "-", p);
                }
                String pctFunc = continuous
                        ? (firstArgIsList ? "listPercentileCont" : "percentileCont")
                        : (firstArgIsList ? "listPercentileDisc" : "percentileDisc");
                yield new SqlExpr.FunctionCall(pctFunc, List.of(list, p));
            }
            case "percentileDisc" -> {
                SqlExpr list = c.apply(params.get(0));
                SqlExpr p = c.apply(params.get(1));
                // param[2]: ascending — when false, invert p
                if (params.size() > 2 && params.get(2) instanceof CBoolean cb && !cb.value()) {
                    p = new SqlExpr.Binary(new SqlExpr.NumericLiteral(1), "-", p);
                }
                yield new SqlExpr.FunctionCall(
                        firstArgIsList ? "listPercentileDisc" : "percentileDisc",
                        List.of(list, p));
            }

            // --- Analytical helpers: minBy/maxBy → UNNEST subquery decomposition ---
            case "maxBy", "minBy" -> {
                String aggFunc = funcName.equals("maxBy") ? "maxBy" : "minBy";
                SqlExpr list1 = c.apply(params.get(0));
                SqlExpr list2 = c.apply(params.get(1));

                if (params.size() >= 3) {
                    // TopK form: (SELECT LIST(sub.a) FROM (SELECT a FROM
                    //   (SELECT UNNEST(l1) AS a, UNNEST(l2) AS b,
                    //    UNNEST(generate_series(0, len(l1)-1)) AS rn)
                    //   ORDER BY b ASC/DESC, rn ASC LIMIT k) sub)
                    SqlExpr limit = c.apply(params.get(2));
                    String orderDir = funcName.equals("minBy") ? "ASC" : "DESC";
                    // Build as raw SQL since SqlBuilder doesn't model nested subqueries easily
                    yield new SqlExpr.FunctionCall("listMinMaxByTopK",
                            List.of(list1, list2, limit, new SqlExpr.Identifier(orderDir)));
                } else {
                    // Simple form: (SELECT ARG_MIN/MAX(a, b) FROM (SELECT UNNEST(l1) AS a, UNNEST(l2) AS b))
                    yield new SqlExpr.FunctionCall("listMinMaxBy",
                            List.of(list1, list2, new SqlExpr.Identifier(aggFunc)));
                }
            }

            // --- Variant access (compiler resolves access pattern) ---
            case "get" -> {
                SqlExpr source = c.apply(params.get(0));
                TypeInfo info = unit.types().get(af);
                String targetSqlType = info != null ? info.variantScalarSqlType(dialect) : null;
                TypeInfo.VariantAccess access = info != null ? info.variantAccess() : null;

                if (access instanceof TypeInfo.VariantAccess.IndexAccess ia) {
                    SqlExpr indexExpr = new SqlExpr.VariantIndex(source, ia.index());
                    yield targetSqlType != null
                            ? new SqlExpr.VariantScalarCast(indexExpr, targetSqlType) : indexExpr;
                }
                if (access instanceof TypeInfo.VariantAccess.FieldAccess fa) {
                    if (targetSqlType != null) {
                        yield new SqlExpr.VariantScalarCast(
                                new SqlExpr.VariantTextAccess(source, fa.key()), targetSqlType);
                    }
                    yield new SqlExpr.VariantAccess(source, fa.key());
                }
                // Compiler should always provide access pattern
                throw new IllegalStateException("get() missing VariantAccess annotation from compiler");
            }

            // --- List/array operations ---
            case "reverse" -> new SqlExpr.FunctionCall("listReverse",
                    List.of(c.apply(params.get(0))));

            // --- Misc ---
            case "generateGuid" -> new SqlExpr.FunctionCall("generateGuid", List.of());
            case "between" -> new SqlExpr.And(List.of(
                    new SqlExpr.Binary(c.apply(params.get(0)), ">=", c.apply(params.get(1))),
                    new SqlExpr.Binary(c.apply(params.get(0)), "<=", c.apply(params.get(2)))));
            case "eq" -> buildComparison(params, "=", c, mapping, tableAlias);
            case "type" -> new SqlExpr.FunctionCall("typeOf", List.of(c.apply(params.get(0))));

            // --- forAll / exists ---
            case "forAll" -> {
                // forAll(list, {e|pred}) → COALESCE(listBoolAnd(listTransform(list, e -> pred)), TRUE)
                if (params.size() >= 2 && params.get(1) instanceof LambdaFunction lf) {
                    SqlExpr list = c.apply(params.get(0));
                    String elemParam = lf.parameters().get(0).name();
                    SqlExpr predBody = !lf.body().isEmpty()
                            ? c.apply(lf.body().get(0)) : new SqlExpr.BoolLiteral(true);
                    SqlExpr lambda = new SqlExpr.LambdaExpr(List.of(elemParam), predBody);
                    SqlExpr transformed = new SqlExpr.FunctionCall("listTransform", List.of(list, lambda));
                    SqlExpr boolAnd = new SqlExpr.FunctionCall("listBoolAnd", List.of(transformed));
                    yield new SqlExpr.FunctionCall("COALESCE", List.of(boolAnd, new SqlExpr.BoolLiteral(true)));
                }
                if (!params.isEmpty()) yield c.apply(params.get(0));
                throw new PureCompileException("forAll: requires list and predicate lambda");
            }
            case "exists" -> {
                // exists(list, {f|pred}) → listLength(listFilter(list, f -> pred)) > 0
                if (params.size() >= 2 && params.get(1) instanceof LambdaFunction lf) {
                    SqlExpr list = c.apply(params.get(0));
                    String elemParam = lf.parameters().get(0).name();
                    SqlExpr predBody = !lf.body().isEmpty()
                            ? c.apply(lf.body().get(0)) : new SqlExpr.BoolLiteral(true);
                    SqlExpr lambda = new SqlExpr.LambdaExpr(List.of(elemParam), predBody);
                    SqlExpr filtered = new SqlExpr.FunctionCall("listFilter", List.of(list, lambda));
                    SqlExpr len = new SqlExpr.FunctionCall("listLength", List.of(filtered));
                    yield new SqlExpr.Binary(len, ">", new SqlExpr.NumericLiteral(0));
                }
                if (!params.isEmpty()) yield c.apply(params.get(0));
                throw new PureCompileException("exists: requires list and predicate lambda");
            }

            // --- Scalar list operations (used in variant array chains) ---
            case "filter" -> {
                // filter(list, {t|pred}) → list_filter(list, t -> pred)
                if (params.size() >= 2 && params.get(1) instanceof LambdaFunction lf) {
                    SqlExpr list = c.apply(params.get(0));
                    String elemParam = lf.parameters().get(0).name();
                    SqlExpr predBody = !lf.body().isEmpty()
                            ? c.apply(lf.body().get(0)) : new SqlExpr.BoolLiteral(true);
                    SqlExpr lambda = new SqlExpr.LambdaExpr(List.of(elemParam), predBody);
                    yield new SqlExpr.FunctionCall("listFilter", List.of(list, lambda));
                }
                // Relational filter pass-through (no lambda — handled in relational path)
                if (!params.isEmpty()) yield c.apply(params.get(0));
                throw new PureCompileException("filter: requires list and predicate lambda");
            }

            // --- Variant conversion functions ---
            case "toMany" -> {
                SqlExpr source = c.apply(params.get(0));
                TypeInfo info = unit.types().get(af);
                String elemType = info != null ? info.variantArrayElementSqlType(dialect) : null;
                yield elemType != null ? new SqlExpr.VariantArrayCast(source, elemType) : source;
            }
            case "toVariant" -> {
                yield new SqlExpr.ToVariant(c.apply(params.get(0)));
            }
            case "to" -> {
                SqlExpr source = c.apply(params.get(0));
                TypeInfo info = unit.types().get(af);
                String sqlType = info != null ? info.variantScalarSqlType(dialect) : null;
                yield sqlType != null ? new SqlExpr.VariantScalarCast(source, sqlType) : source;
            }

            // --- Pass-through for non-SQL functions ---
            case "toOne", "eval",
                    "list", "pair", "match",
                    "letWithParam",
                    "groupBy", "select", "write",
                    "compare", "comparator" -> {
                // These are Pure-level functions that should pass through the first arg
                if (!params.isEmpty()) {
                    yield c.apply(params.get(0));
                }
                throw new PureCompileException(
                        "PlanGenerator: pass-through function '" + funcName + "' has no parameters");
            }
            case "cast" -> {
                if (!params.isEmpty()) {
                    SqlExpr source = c.apply(params.get(0));
                    // Check compiler type info for typed list cast: cast(@Integer) on a list
                    TypeInfo castInfo = unit.typeInfoFor(af);
                    if (castInfo != null && castInfo.scalarType() != null
                            && castInfo.scalarType().isList()) {
                        GenericType elemType = castInfo.scalarType().elementType();
                        if (elemType != null && elemType != GenericType.Primitive.ANY) {
                            String sqlType = dialect.sqlTypeName(elemType.typeName());
                            yield new SqlExpr.VariantArrayCast(source, sqlType);
                        }
                    }
                    yield source;
                }
                throw new PureCompileException(
                        "PlanGenerator: pass-through function 'cast' has no parameters");
            }
            case "range" -> {
                // range(n) → RANGE(n) — produces a list of integers [0..n-1]
                yield new SqlExpr.FunctionCall("RANGE",
                        List.of(c.apply(params.get(0))));
            }
            case "map" -> {
                // map(list, {x|body}) → list_transform(list, x -> body)
                if (params.size() >= 2 && params.get(1) instanceof LambdaFunction mapLf) {
                    SqlExpr list = c.apply(params.get(0));
                    // If source is a variant (JSON), wrap in CAST(AS JSON[]) for array iteration
                    TypeInfo sourceInfo = unit.types().get(params.get(0));
                    if (sourceInfo != null && sourceInfo.scalarType() == GenericType.Primitive.JSON) {
                        list = new SqlExpr.VariantArrayCast(list, dialect.sqlTypeName("JSON"));
                    }
                    String elemParam = mapLf.parameters().isEmpty() ? "x"
                            : mapLf.parameters().get(0).name();
                    SqlExpr body = !mapLf.body().isEmpty()
                            ? c.apply(mapLf.body().get(0)) : new SqlExpr.NullLiteral();
                    SqlExpr lambda = new SqlExpr.LambdaExpr(List.of(elemParam), body);
                    yield new SqlExpr.FunctionCall("listTransform", List.of(list, lambda));
                }
                // Non-lambda map: pass through source
                if (!params.isEmpty()) {
                    yield c.apply(params.get(0));
                }
                throw new PureCompileException("map: no parameters");
            }
            case "fold" -> {
                // fold(source, {elem,acc|body}, init) → list_reduce(source, ((acc,elem)->body), init)
                // Note: fold+add is handled by compiler desugar (inlinedBody → concatenate)
                if (params.size() >= 3
                        && params.get(1) instanceof LambdaFunction foldLf) {
                    SqlExpr list = c.apply(params.get(0));
                    // Wrap single values in list for list_reduce
                    if (!firstArgIsList) {
                        list = new SqlExpr.FunctionCall("wrapList", List.of(list));
                    }
                    SqlExpr init = c.apply(params.get(2));
                    // Extract lambda params: x=element (1st), y=accumulator (2nd)
                    String elemParam = foldLf.parameters().isEmpty() ? "x"
                            : foldLf.parameters().get(0).name();
                    String accParam = foldLf.parameters().size() < 2 ? "y"
                            : foldLf.parameters().get(1).name();
                    // Compile body
                    SqlExpr body = !foldLf.body().isEmpty()
                            ? c.apply(foldLf.body().get(0)) : new SqlExpr.NullLiteral();
                    // Emit LambdaExpr: ((acc, elem) -> body)
                    SqlExpr lambda = new SqlExpr.LambdaExpr(
                            List.of(accParam, elemParam), body);
                    yield new SqlExpr.FunctionCall("listReduce",
                            List.of(list, lambda, init));
                }
                // Non-fold (< 3 params): pass through
                if (!params.isEmpty()) {
                    yield c.apply(params.get(0));
                }
                throw new PureCompileException("fold: no parameters");
            }
            case "zip" -> {
                // zip(list1, list2) → list of pairs
                if (params.size() >= 2) {
                    yield new SqlExpr.FunctionCall("listZip",
                            List.of(c.apply(params.get(0)), c.apply(params.get(1))));
                }
                if (!params.isEmpty()) {
                    yield c.apply(params.get(0));
                }
                throw new PureCompileException("zip: no parameters");
            }
            case "sort" -> {
                if (firstArgIsList) {
                    // Read pre-resolved sort direction from sidecar (CleanCompiler detected compare lambda)
                    TypeInfo sortInfo = unit.types().get(af);
                    String direction = (sortInfo != null && sortInfo.hasSortSpecs()
                            && sortInfo.sortSpecs().get(0).direction() == TypeInfo.SortDirection.DESC)
                            ? "DESC" : "ASC";
                    // Check for key function: sort(keyFn, compareFn)
                    if (params.size() > 2
                            || (params.size() == 2 && params.get(1) instanceof LambdaFunction kf
                                    && kf.parameters().size() == 1)) {
                        // sort with key function → listSort with key
                        LambdaFunction keyLf = (LambdaFunction) params.get(1);
                        String keyParam = keyLf.parameters().get(0).name();
                        SqlExpr keyBody = !keyLf.body().isEmpty()
                                ? c.apply(keyLf.body().get(0)) : new SqlExpr.NullLiteral();
                        // Pass: list, direction, paramName, keyBody
                        yield new SqlExpr.FunctionCall("listSortWithKey",
                                List.of(c.apply(params.get(0)),
                                        new SqlExpr.StringLiteral(direction),
                                        new SqlExpr.Identifier(keyParam),
                                        keyBody));
                    }
                    if ("DESC".equals(direction)) {
                        yield new SqlExpr.FunctionCall("listSort",
                                List.of(c.apply(params.get(0)),
                                        new SqlExpr.StringLiteral(direction)));
                    }
                    yield new SqlExpr.FunctionCall("listSort",
                            List.of(c.apply(params.get(0))));
                }
                // Relation sort — pass through (handled by generateSort)
                if (!params.isEmpty()) {
                    yield c.apply(params.get(0));
                }
                throw new PureCompileException("sort: no parameters");
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

    /** Checks if any param is an actual partial date (year-only or year-month, not full YYYY-MM-DD). */
    private boolean hasPartialDate(List<ValueSpecification> params) {
        for (var p : params) {
            if (p instanceof CStrictDate sd && isPartialDate(sd.value())) return true;
        }
        return false;
    }

    /** True if the date string is partial (year-only or year-month, not full YYYY-MM-DD). */
    private static boolean isPartialDate(String dateValue) {
        // Full date: YYYY-MM-DD (exactly 2 dashes)
        // Partial: YYYY (0 dashes) or YYYY-MM (1 dash)
        long dashes = dateValue.chars().filter(ch -> ch == '-').count();
        return dashes < 2;
    }

    /**
     * Renders a param for precision-aware date comparison.
     * Called ONLY when hasPartialDate() is true (some side is partial).
     * In that context, ALL CStrictDate values must become string literals
     * so the comparison is string-vs-string (preserving Pure date precision).
     * Full-date-vs-full-date comparisons skip this path entirely.
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
                            .addSelect(new SqlExpr.NumericLiteral(1), null)
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


    private static String simpleName(String qualifiedName) {
        return TypeInfo.simpleName(qualifiedName);
    }

    /**
     * Parses an extra arg string (from window function sidecar) into a type-safe SqlExpr.
     * Numeric strings → NumericLiteral, quoted strings → StringLiteral, else → ColumnRef.
     */
    private static SqlExpr parseExtraArg(String s) {
        if (s == null || s.isEmpty()) return new SqlExpr.ColumnRef(s);
        // Quoted string literal: 'value'
        if (s.startsWith("'") && s.endsWith("'")) {
            return new SqlExpr.StringLiteral(s.substring(1, s.length() - 1));
        }
        // Numeric literal
        char first = s.charAt(0);
        if ((first >= '0' && first <= '9') || first == '-' || first == '.') {
            try {
                if (s.contains(".")) {
                    return new SqlExpr.DecimalLiteral(new java.math.BigDecimal(s));
                }
                return new SqlExpr.NumericLiteral(Long.parseLong(s));
            } catch (NumberFormatException ignored) {
                // Fall through to column ref
            }
        }
        return new SqlExpr.ColumnRef(s);
    }

    /**
     * Wraps a collection's elements in toJson() for mixed-type list operations.
     * Used by concatenate when two lists have different element types.
     */
    private SqlExpr wrapCollectionInToJson(ValueSpecification vs,
                                            java.util.function.Function<ValueSpecification, SqlExpr> c) {
        if (vs instanceof Collection coll) {
            var wrapped = coll.values().stream()
                    .map(v -> (SqlExpr) new SqlExpr.FunctionCall("toJson", List.of(c.apply(v))))
                    .collect(java.util.stream.Collectors.toList());
            return new SqlExpr.ArrayLiteral(wrapped);
        }
        // Non-collection: wrap the entire expression in toJson
        return new SqlExpr.FunctionCall("toJson", List.of(c.apply(vs)));
    }
}
