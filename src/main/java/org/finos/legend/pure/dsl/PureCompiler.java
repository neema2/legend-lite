package org.finos.legend.pure.dsl;

import org.finos.legend.engine.plan.*;
import org.finos.legend.engine.store.*;
import org.finos.legend.pure.dsl.ModelContext.AssociationNavigation;
import org.finos.legend.pure.dsl.definition.PureModelBuilder;
import org.finos.legend.pure.dsl.m2m.M2MClassMapping;
import org.finos.legend.pure.dsl.m2m.M2MCompiler;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Compiles Pure AST expressions into RelationNode execution plans.
 * 
 * This is the bridge between the Pure language and the SQL transpiler.
 * 
 * Key features:
 * - Compiles ClassAllExpression, FilterExpression, ProjectExpression
 * - Handles association navigation in filters (generates EXISTS for to-many)
 * - Preserves JOINs for explicit relational/DataFrame queries
 */
public final class PureCompiler {

    private final MappingRegistry mappingRegistry;
    private final ModelContext modelContext;
    private int aliasCounter = 0;

    /**
     * Creates a compiler with just a mapping registry (legacy compatibility).
     */
    public PureCompiler(MappingRegistry mappingRegistry) {
        this(mappingRegistry, null);
    }

    /**
     * Creates a compiler with full model context for association navigation.
     */
    public PureCompiler(MappingRegistry mappingRegistry, ModelContext modelContext) {
        this.mappingRegistry = Objects.requireNonNull(mappingRegistry, "Mapping registry cannot be null");
        this.modelContext = modelContext;
    }

    /**
     * Validates Pure model source code (definitions only, no query execution).
     * 
     * This parses Classes, Mappings, Connections, Runtimes, etc. and validates
     * their structure without trying to compile them into SQL execution plans.
     * 
     * @param pureSource The Pure source code to validate
     * @throws RuntimeException if validation fails with error details
     */
    public void validate(String pureSource) {
        // Use PureModelBuilder to parse and validate Pure definitions
        try {
            new PureModelBuilder().addSource(pureSource);
        } catch (Exception e) {
            throw new RuntimeException("Validation failed: " + e.getMessage(), e);
        }
    }

    /**
     * Compiles a Pure query string into a RelationNode execution plan.
     * 
     * @param pureQuery The Pure query string
     * @return The compiled RelationNode
     */
    public RelationNode compile(String pureQuery) {
        PureExpression ast = PureParser.parse(pureQuery);
        return compileExpression(ast, null);
    }

    /**
     * Compiles a Pure expression AST into a RelationNode.
     * 
     * @param expr    The Pure expression
     * @param context Compilation context (for lambda parameter resolution)
     * @return The compiled RelationNode
     */
    public RelationNode compileExpression(PureExpression expr, CompilationContext context) {
        return switch (expr) {
            case ClassAllExpression classAll -> compileClassAll(classAll);
            case ClassFilterExpression classFilter -> compileClassFilter(classFilter, context);
            case RelationFilterExpression relationFilter -> compileRelationFilter(relationFilter, context);
            case ProjectExpression project -> compileProject(project, context);
            case GroupByExpression groupBy -> compileGroupBy(groupBy, context);
            case JoinExpression join -> compileJoin(join, context);
            case FlattenExpression flatten -> compileFlatten(flatten, context);
            case ClassSortByExpression classSortBy -> compileClassSortBy(classSortBy, context);
            case RelationSortExpression relationSort -> compileRelationSort(relationSort, context);
            case SortExpression sort -> compileSort(sort, context);
            case SortByExpression sortBy -> compileSortBy(sortBy, context);
            case ClassLimitExpression classLimit -> compileClassLimit(classLimit, context);
            case RelationLimitExpression relationLimit -> compileRelationLimit(relationLimit, context);
            case LimitExpression limit -> compileLimit(limit, context);
            case FirstExpression first -> compileFirst(first, context);
            case SliceExpression slice -> compileSlice(slice, context);
            case DropExpression drop -> compileDrop(drop, context);
            case RelationLiteral literal -> compileRelationLiteral(literal);
            case RelationSelectExpression select -> compileRelationSelect(select, context);
            case RelationExtendExpression extend -> compileRelationExtend(extend, context);
            case ExtendExpression extend -> compileExtend(extend, context);
            case FromExpression from -> compileFrom(from, context);
            case SerializeExpression serialize -> compileSerialize(serialize, context);
            case DistinctExpression distinct -> compileDistinct(distinct, context);
            case RenameExpression rename -> compileRename(rename, context);
            case ConcatenateExpression concatenate -> compileConcatenate(concatenate, context);
            default -> throw new PureCompileException("Cannot compile expression to RelationNode: " + expr);
        };
    }

    // ========================================================================
    // MUTATION COMPILATION (save, update, delete)
    // ========================================================================

    /**
     * Compiles a Pure mutation expression into a MutationNode.
     * 
     * Supports:
     * - SaveExpression: ^Person(...)->save() → InsertNode
     * - UpdateExpression: Person.all()->filter(...)->update({...}) → UpdateNode
     * - DeleteExpression: Person.all()->filter(...)->delete() → DeleteNode
     */
    public MutationNode compileMutation(PureExpression expr) {
        return switch (expr) {
            case SaveExpression save -> compileSave(save);
            case UpdateExpression update -> compileUpdate(update);
            case DeleteExpression delete -> compileDelete(delete);
            default -> throw new PureCompileException("Not a mutation expression: " + expr.getClass().getSimpleName());
        };
    }

    private MutationNode compileSave(SaveExpression save) {
        PureExpression source = save.instance();

        if (source instanceof InstanceExpression instance) {
            return compileInstanceInsert(instance);
        }

        throw new PureCompileException("Cannot save expression: " + source.getClass().getSimpleName());
    }

    private InsertNode compileInstanceInsert(InstanceExpression instance) {
        String className = instance.className();
        Table table = inferTableFromClassName(className);

        List<String> columns = new ArrayList<>();
        List<Expression> values = new ArrayList<>();

        for (var entry : instance.properties().entrySet()) {
            columns.add(toColumnName(entry.getKey()));
            values.add(literalToExpression(entry.getValue()));
        }

        return new InsertNode(table, columns, values);
    }

    private MutationNode compileUpdate(UpdateExpression update) {
        PureExpression source = update.source();
        String className = extractClassNameFromSource(source);
        Table table = inferTableFromClassName(className);

        // Compile WHERE clause from filter
        Expression whereClause = compileWhereFromSource(source);

        // Compile SET clause from lambda
        Map<String, Expression> setClause = compileSetClause(update.updateLambda(), className);

        return new UpdateNode(table, setClause, whereClause);
    }

    private MutationNode compileDelete(DeleteExpression delete) {
        PureExpression source = delete.source();
        String className = extractClassNameFromSource(source);
        Table table = inferTableFromClassName(className);

        // Compile WHERE clause from filter
        Expression whereClause = compileWhereFromSource(source);

        return new DeleteNode(table, whereClause);
    }

    private String extractClassNameFromSource(PureExpression source) {
        return switch (source) {
            case ClassAllExpression cae -> cae.className();
            case ClassFilterExpression cfe -> extractClassNameFromSource(cfe.source());
            default ->
                throw new PureCompileException("Cannot extract class name from: " + source.getClass().getSimpleName());
        };
    }

    private Expression compileWhereFromSource(PureExpression source) {
        if (source instanceof ClassFilterExpression cfe) {
            // Get mapping for property name resolution
            RelationalMapping mapping = mappingRegistry.findByClassName(extractClassNameFromSource(cfe.source()))
                    .orElse(null);
            String className = extractClassNameFromSource(cfe.source());

            CompilationContext ctx = new CompilationContext(
                    cfe.lambda().parameter(),
                    "t0", // Default alias
                    mapping,
                    className,
                    true);
            return compileToSqlExpression(cfe.lambda().body(), ctx);
        }
        return null; // No filter
    }

    private Map<String, Expression> compileSetClause(LambdaExpression lambda, String className) {
        Map<String, Expression> setClause = new HashMap<>();
        PureExpression body = lambda.body();

        // Handle comparison as assignment: {p | $p.age == 31}
        if (body instanceof ComparisonExpr cmp && cmp.operator() == ComparisonExpr.Operator.EQUALS) {
            if (cmp.left() instanceof PropertyAccessExpression pae) {
                String columnName = toColumnName(pae.propertyName());
                Expression value = compileLiteral((LiteralExpr) cmp.right());
                setClause.put(columnName, value);
            }
        }

        if (setClause.isEmpty()) {
            throw new PureCompileException("No valid assignments in update lambda");
        }

        return setClause;
    }

    private Table inferTableFromClassName(String className) {
        // Check if we have a mapping
        var mapping = mappingRegistry.findByClassName(className);
        if (mapping.isPresent()) {
            return mapping.get().table();
        }
        // Fall back to convention: Person -> T_PERSON
        String tableName = "T_" + toUpperSnakeCase(className);
        return new Table(tableName, List.of());
    }

    private String toColumnName(String propertyName) {
        return toUpperSnakeCase(propertyName);
    }

    private String toUpperSnakeCase(String camelCase) {
        StringBuilder result = new StringBuilder();
        for (int i = 0; i < camelCase.length(); i++) {
            char c = camelCase.charAt(i);
            if (Character.isUpperCase(c) && i > 0) {
                result.append('_');
            }
            result.append(Character.toUpperCase(c));
        }
        return result.toString();
    }

    private Expression literalToExpression(Object value) {
        if (value == null)
            return Literal.nullValue();
        if (value instanceof String s)
            return Literal.string(s);
        if (value instanceof Long l)
            return Literal.integer(l);
        if (value instanceof Integer i)
            return Literal.integer(i);
        if (value instanceof Double d)
            return new Literal(d, Literal.LiteralType.DOUBLE);
        if (value instanceof Number n)
            return Literal.integer(n.longValue());
        if (value instanceof Boolean b)
            return Literal.bool(b);
        return Literal.string(value.toString());
    }

    private RelationNode compileClassAll(ClassAllExpression classAll) {
        RelationalMapping mapping = mappingRegistry.findByClassName(classAll.className())
                .orElseThrow(() -> new PureCompileException("No mapping found for class: " + classAll.className()));

        String alias = "t" + aliasCounter++;
        return new TableNode(mapping.table(), alias);
    }

    /**
     * Compiles a filter on a ClassExpression (e.g., Person.all()->filter(...))
     */
    private RelationNode compileClassFilter(ClassFilterExpression filter, CompilationContext outerContext) {
        RelationNode source = compileExpression(filter.source(), outerContext);

        // Get the table alias from the source
        String tableAlias = getTableAlias(source);

        // Get the mapping and class for the source
        RelationalMapping mapping = getMappingFromSource(filter.source());
        String className = getClassNameFromSource(filter.source());

        // Create context for the lambda
        CompilationContext lambdaContext = new CompilationContext(
                filter.lambda().parameter(),
                tableAlias,
                mapping,
                className,
                true // We're in a filter context
        );

        // Compile the filter condition
        Expression condition = compileToSqlExpression(filter.lambda().body(), lambdaContext);

        return new FilterNode(source, condition);
    }

    /**
     * Compiles a filter on a RelationExpression (e.g.,
     * ...->project(...)->filter(...))
     * Uses column aliases from the source Relation, not class property mappings.
     */
    private RelationNode compileRelationFilter(RelationFilterExpression filter, CompilationContext outerContext) {
        RelationNode source = compileExpression(filter.source(), outerContext);

        // For Relation API filters (chained after extend, etc.), use empty alias
        // Column references should be unqualified since they reference the subquery
        // result
        String tableAlias = "";

        // Create a context where property names ARE column names (no mapping)
        CompilationContext lambdaContext = new CompilationContext(
                filter.lambda().parameter(),
                tableAlias,
                null, // No class mapping - we're working with Relation columns
                null,
                true);

        // Compile the filter condition
        Expression condition = compileToSqlExpression(filter.lambda().body(), lambdaContext);

        return new FilterNode(source, condition);
    }

    private RelationNode compileProject(ProjectExpression project, CompilationContext outerContext) {
        RelationNode source = compileExpression(project.source(), outerContext);

        // Get the table alias from the source
        String baseTableAlias = getTableAlias(source);

        // Get the mapping and class name for the source
        RelationalMapping baseMapping = getMappingFromSource(project.source());
        String baseClassName = getClassNameFromSource(project.source());

        List<Projection> projections = new ArrayList<>();

        // Track joins we need to add for association projections
        // Key: association property name, Value: join info (alias and mapping)
        java.util.Map<String, JoinInfo> joinInfos = new java.util.HashMap<>();

        for (int i = 0; i < project.projections().size(); i++) {
            LambdaExpression lambda = project.projections().get(i);

            // Analyze if this projection navigates through an association
            CompilationContext projContext = new CompilationContext(
                    lambda.parameter(),
                    baseTableAlias,
                    baseMapping,
                    baseClassName,
                    false);

            AssociationPath path = analyzePropertyPath(lambda.body(), projContext);

            // Determine alias for this projection
            String alias;
            if (i < project.aliases().size()) {
                alias = project.aliases().get(i);
            } else {
                alias = extractFinalPropertyName(lambda.body());
            }

            if (path != null && path.hasToManyNavigation() && modelContext != null) {
                // Association navigation - collect join info and create projection
                Projection proj = compileAssociationProjection(path, alias, joinInfos, baseTableAlias, baseMapping);
                projections.add(proj);
            } else {
                // Property access - check if expression-based or column-based
                String propertyName = extractPropertyName(lambda.body());
                var propertyMapping = baseMapping.getPropertyMapping(propertyName);

                if (propertyMapping.isPresent() && propertyMapping.get().hasExpression()) {
                    // Expression-based mapping: compile the expression string
                    String expressionStr = propertyMapping.get().expressionString();
                    Expression expr = compileExpressionMapping(expressionStr, baseTableAlias);
                    projections.add(new Projection(expr, alias));
                } else {
                    String columnName = baseMapping.getColumnForProperty(propertyName)
                            .orElseThrow(
                                    () -> new PureCompileException("No column mapping for property: " + propertyName));
                    projections.add(Projection.column(baseTableAlias, columnName, alias));
                }
            }
        }

        // Build the final source by adding JOINs on top of the existing source
        // This preserves any filters that were applied to source!
        RelationNode finalSource = source;
        for (JoinInfo ji : joinInfos.values()) {
            // Create the join ON TOP of the current finalSource
            TableNode targetTable = new TableNode(ji.targetMapping().table(), ji.alias());
            Expression joinCondition = ComparisonExpression.equals(
                    ColumnReference.of(baseTableAlias, ji.leftColumn()),
                    ColumnReference.of(ji.alias(), ji.rightColumn()));
            finalSource = new JoinNode(finalSource, targetTable, joinCondition, JoinNode.JoinType.LEFT_OUTER);
        }

        return new ProjectNode(finalSource, projections);
    }

    /**
     * Compiles a groupBy expression into a GroupByNode.
     * 
     * Example Pure: ->groupBy({p | $p.department}, {p | $p.salary}, 'totalSalary')
     * 
     * IMPORTANT: groupBy only works on Relation type (from project() or Relation
     * literal).
     * It cannot be called directly on Class.all() - use project() first to convert
     * Class to Relation.
     * 
     * @param groupBy      The groupBy expression
     * @param outerContext Compilation context
     * @return The compiled GroupByNode
     */
    private RelationNode compileGroupBy(GroupByExpression groupBy, CompilationContext outerContext) {
        // Type safety is now enforced at compile-time by the Java type system.
        // GroupByExpression.source() is RelationExpression, which can only be:
        // - ProjectExpression
        // - RelationFilterExpression
        // - GroupByExpression
        // NOT ClassAllExpression or ClassFilterExpression!

        RelationNode source = compileExpression(groupBy.source(), outerContext);

        // For ProjectExpression source, the lambda property names reference the
        // projected column aliases
        // For example: project([...], ['dept', 'sal'])->groupBy([{r | $r.dept}], ...)
        // Here 'dept' is the column alias from project, not a class property

        // Extract grouping column names from lambdas
        // The property name in the lambda IS the column name (from project aliases)
        List<String> groupingColumns = new ArrayList<>();
        for (LambdaExpression lambda : groupBy.groupByColumns()) {
            String columnName = extractPropertyName(lambda.body());
            groupingColumns.add(columnName);
        }

        // Extract aggregations from lambdas
        List<GroupByNode.AggregateProjection> aggregations = new ArrayList<>();
        for (int i = 0; i < groupBy.aggregations().size(); i++) {
            LambdaExpression aggLambda = groupBy.aggregations().get(i);
            String columnName = extractPropertyName(aggLambda.body());

            // Get alias from the aliases list or generate one
            String alias;
            if (i < groupBy.aliases().size()) {
                alias = groupBy.aliases().get(i);
            } else {
                alias = columnName + "_agg";
            }

            // Default to SUM for now - in full implementation, parse the aggregate function
            // from lambda
            AggregateExpression.AggregateFunction aggFunc = AggregateExpression.AggregateFunction.SUM;

            aggregations.add(new GroupByNode.AggregateProjection(alias, columnName, aggFunc));
        }

        return new GroupByNode(source, groupingColumns, aggregations);
    }

    /**
     * Compiles a join expression into a JoinNode.
     * 
     * Example Pure: ->join(otherRelation, JoinType.LEFT_OUTER, {l, r | $l.id ==
     * $r.personId})
     * 
     * @param join    The join expression
     * @param context Compilation context
     * @return The compiled JoinNode
     */
    private RelationNode compileJoin(JoinExpression join, CompilationContext context) {
        // Compile left and right relations
        RelationNode left = compileExpression(join.left(), context);
        RelationNode right = compileExpression(join.right(), context);

        // Use consistent aliases that match SQLGenerator.generateJoinSource
        // When the source is a ProjectNode, SQLGenerator wraps it in a subquery with
        // these aliases
        String leftAlias = "left_src";
        String rightAlias = "right_src";

        // Get the lambda parameter names to map to left/right aliases
        // Lambda format: {l, r | $l.col == $r.col}
        // Unfortunately our LambdaExpression only supports single parameter
        // So we need to extract column references differently
        LambdaExpression condLambda = join.condition();
        String leftParam = condLambda.parameter();

        // Compile the join condition, mapping parameter names to aliases
        Expression condition = compileJoinConditionWithParams(condLambda.body(),
                leftParam, leftAlias, rightAlias);

        // Map JoinExpression.JoinType to JoinNode.JoinType
        JoinNode.JoinType joinType = switch (join.joinType()) {
            case INNER -> JoinNode.JoinType.INNER;
            case LEFT_OUTER -> JoinNode.JoinType.LEFT_OUTER;
            case RIGHT_OUTER -> JoinNode.JoinType.RIGHT_OUTER;
            case FULL_OUTER -> JoinNode.JoinType.FULL_OUTER;
        };

        return new JoinNode(left, right, condition, joinType);
    }

    /**
     * Compiles a join condition expression with parameter name mapping.
     * First property access goes to leftAlias, second goes to rightAlias.
     */
    private Expression compileJoinConditionWithParams(PureExpression expr,
            String leftParam, String leftAlias, String rightAlias) {
        // Track which side we're on based on position in comparison
        return compileJoinConditionInternal(expr, leftAlias, rightAlias, new int[] { 0 });
    }

    private Expression compileJoinConditionInternal(PureExpression expr,
            String leftAlias, String rightAlias, int[] accessCount) {
        return switch (expr) {
            case ComparisonExpr cmp -> {
                // Reset counter and compile each side
                accessCount[0] = 0;
                Expression leftExpr = compileJoinConditionInternal(cmp.left(), leftAlias, rightAlias, accessCount);
                Expression rightExpr = compileJoinConditionInternal(cmp.right(), leftAlias, rightAlias, accessCount);
                yield switch (cmp.operator()) {
                    case EQUALS -> ComparisonExpression.equals(leftExpr, rightExpr);
                    case NOT_EQUALS -> new ComparisonExpression(leftExpr,
                            ComparisonExpression.ComparisonOperator.NOT_EQUALS, rightExpr);
                    case LESS_THAN -> ComparisonExpression.lessThan(leftExpr, rightExpr);
                    case LESS_THAN_OR_EQUALS -> new ComparisonExpression(leftExpr,
                            ComparisonExpression.ComparisonOperator.LESS_THAN_OR_EQUALS, rightExpr);
                    case GREATER_THAN -> ComparisonExpression.greaterThan(leftExpr, rightExpr);
                    case GREATER_THAN_OR_EQUALS -> new ComparisonExpression(leftExpr,
                            ComparisonExpression.ComparisonOperator.GREATER_THAN_OR_EQUALS, rightExpr);
                };
            }
            case PropertyAccessExpression prop -> {
                // First property access uses left alias, second uses right
                String columnName = prop.propertyName();
                String alias = (accessCount[0] == 0) ? leftAlias : rightAlias;
                accessCount[0]++;
                yield ColumnReference.of(alias, columnName);
            }
            case LiteralExpr lit -> compileLiteral(lit);
            default -> throw new PureCompileException(
                    "Cannot compile join condition expression: " + expr.getClass().getSimpleName());
        };
    }

    /**
     * Compiles flatten(~col) to a lateral join that unnests a JSON array column.
     * Creates a new column with the unnested elements.
     */
    private RelationNode compileFlatten(FlattenExpression flatten, CompilationContext context) {
        RelationNode source = compileExpression(flatten.source(), context);
        String columnName = flatten.columnName();

        // Create a lateral join that unnests the array column
        // Use unqualified column reference since the source SQL will handle
        // qualification
        ColumnReference arrayCol = ColumnReference.of(columnName);

        // Return a LateralJoinNode that represents the lateral join with unnest
        return new LateralJoinNode(source, arrayCol, columnName);
    }

    /**
     * Compiles distinct() expression.
     * - distinct() - all columns
     * - distinct(~[col1, col2]) - specific columns
     */
    private RelationNode compileDistinct(DistinctExpression distinct, CompilationContext context) {
        RelationNode source = compileExpression(distinct.source(), context);
        return new DistinctNode(source, distinct.columns());
    }

    /**
     * Compiles rename(~oldCol, ~newCol) expression.
     * Renames a column in the relation.
     */
    private RelationNode compileRename(RenameExpression rename, CompilationContext context) {
        RelationNode source = compileExpression(rename.source(), context);
        return new RenameNode(source, rename.oldColumnName(), rename.newColumnName());
    }

    /**
     * Compiles concatenate(other) expression.
     * SQL: UNION ALL
     */
    private RelationNode compileConcatenate(ConcatenateExpression concatenate, CompilationContext context) {
        RelationNode left = compileExpression(concatenate.left(), context);
        RelationNode right = compileExpression(concatenate.right(), context);
        return new ConcatenateNode(left, right);
    }

    /**
     * Compiles sortBy() on a ClassExpression.
     * Uses lambda to extract property name for ORDER BY.
     */
    private RelationNode compileClassSortBy(ClassSortByExpression sortBy, CompilationContext context) {
        RelationNode source = compileExpression(sortBy.source(), context);

        RelationalMapping mapping = getMappingFromSource(sortBy.source());

        // Extract property name from lambda
        String propertyName = extractPropertyName(sortBy.lambda().body());
        String columnName = mapping.getColumnForProperty(propertyName)
                .orElseThrow(() -> new PureCompileException("No column mapping for property: " + propertyName));

        // Create SortColumn
        SortNode.SortDirection direction = sortBy.ascending()
                ? SortNode.SortDirection.ASC
                : SortNode.SortDirection.DESC;
        SortNode.SortColumn sortColumn = new SortNode.SortColumn(columnName, direction);

        return new SortNode(source, List.of(sortColumn));
    }

    /**
     * Compiles sort() on a RelationExpression.
     * Uses column name directly (from project aliases).
     */
    private RelationNode compileRelationSort(RelationSortExpression sort, CompilationContext context) {
        RelationNode source = compileExpression(sort.source(), context);

        // Column name is already the relation column (from project alias)
        SortNode.SortDirection direction = sort.ascending()
                ? SortNode.SortDirection.ASC
                : SortNode.SortDirection.DESC;
        SortNode.SortColumn sortColumn = new SortNode.SortColumn(sort.column(), direction);

        return new SortNode(source, List.of(sortColumn));
    }

    /**
     * Compiles limit/take/drop/slice on a ClassExpression.
     */
    private RelationNode compileClassLimit(ClassLimitExpression limit, CompilationContext context) {
        RelationNode source = compileExpression(limit.source(), context);
        return new LimitNode(source, limit.limit(), limit.offset());
    }

    /**
     * Compiles limit/take/drop/slice on a RelationExpression.
     */
    private RelationNode compileRelationLimit(RelationLimitExpression limit, CompilationContext context) {
        RelationNode source = compileExpression(limit.source(), context);
        return new LimitNode(source, limit.limit(), limit.offset());
    }

    /**
     * Compiles a generic SortExpression (from new AST builder).
     * Extracts column names from the sort specifications.
     */
    private RelationNode compileSort(SortExpression sort, CompilationContext context) {
        RelationNode source = compileExpression(sort.source(), context);

        List<SortNode.SortColumn> sortColumns = new ArrayList<>();
        SortNode.SortDirection nextDirection = SortNode.SortDirection.ASC;

        for (PureExpression colExpr : sort.sortColumns()) {
            // Check if this is a direction modifier
            if (colExpr instanceof LiteralExpr lit && lit.value() instanceof String s) {
                if ("DESC".equalsIgnoreCase(s)) {
                    // Apply to the previous column if exists
                    if (!sortColumns.isEmpty()) {
                        SortNode.SortColumn last = sortColumns.removeLast();
                        sortColumns.add(new SortNode.SortColumn(last.column(), SortNode.SortDirection.DESC));
                    } else {
                        nextDirection = SortNode.SortDirection.DESC;
                    }
                    continue;
                } else if ("ASC".equalsIgnoreCase(s)) {
                    continue; // Skip ASC literals, ASC is default
                }
            }

            String colName = extractColumnName(colExpr);
            SortNode.SortDirection direction = extractSortDirection(colExpr);
            if (direction == SortNode.SortDirection.ASC) {
                direction = nextDirection; // Use pending direction
            }
            sortColumns.add(new SortNode.SortColumn(colName, direction));
            nextDirection = SortNode.SortDirection.ASC; // Reset
        }

        return new SortNode(source, sortColumns);
    }

    /**
     * Compiles a SortByExpression (from new AST builder).
     * Similar to SortExpression but uses lambda-style column specs.
     */
    private RelationNode compileSortBy(SortByExpression sortBy, CompilationContext context) {
        RelationNode source = compileExpression(sortBy.source(), context);

        List<SortNode.SortColumn> sortColumns = new ArrayList<>();
        for (PureExpression colExpr : sortBy.sortColumns()) {
            String colName = extractColumnName(colExpr);
            SortNode.SortDirection direction = extractSortDirection(colExpr);
            sortColumns.add(new SortNode.SortColumn(colName, direction));
        }

        return new SortNode(source, sortColumns);
    }

    /**
     * Compiles a generic LimitExpression (from new AST builder).
     */
    private RelationNode compileLimit(LimitExpression limit, CompilationContext context) {
        RelationNode source = compileExpression(limit.source(), context);
        return new LimitNode(source, limit.count(), 0);
    }

    /**
     * Compiles first() expression: source->first()
     * Returns first element (or null if empty). Translates to LIMIT 1.
     */
    private RelationNode compileFirst(FirstExpression first, CompilationContext context) {
        RelationNode source = compileExpression(first.source(), context);
        return new LimitNode(source, 1, 0);
    }

    /**
     * Compiles a SliceExpression: source->slice(start, end)
     * Equivalent to LIMIT (end - start) OFFSET start
     */
    private RelationNode compileSlice(SliceExpression slice, CompilationContext context) {
        RelationNode source = compileExpression(slice.source(), context);
        int limit = slice.end() - slice.start();
        int offset = slice.start();
        return new LimitNode(source, limit, offset);
    }

    /**
     * Compiles a DropExpression: source->drop(n)
     * Equivalent to OFFSET n (skip first n rows)
     */
    private RelationNode compileDrop(DropExpression drop, CompilationContext context) {
        RelationNode source = compileExpression(drop.source(), context);
        // Drop is just an offset - we use a very large limit to get "all remaining
        // rows"
        // In practice, we should use an OffsetNode, but for now LimitNode with offset
        // works
        return new LimitNode(source, Integer.MAX_VALUE, drop.count());
    }

    /**
     * Extracts column name from various expression types.
     */
    private String extractColumnName(PureExpression expr) {
        return switch (expr) {
            case LiteralExpr lit -> String.valueOf(lit.value());
            case ColumnSpec col -> col.name();
            case LambdaExpression lambda -> extractPropertyName(lambda.body());
            case PropertyAccessExpression prop -> prop.propertyName();
            case MethodCall method -> extractColumnName(method.source());
            default -> throw new PureCompileException("Cannot extract column name from: " + expr);
        };
    }

    /**
     * Extracts sort direction from expression (defaults to ASC).
     * Handles MethodCall with .desc() or .asc() suffix.
     */
    private SortNode.SortDirection extractSortDirection(PureExpression expr) {
        if (expr instanceof MethodCall method) {
            if ("desc".equalsIgnoreCase(method.methodName())) {
                return SortNode.SortDirection.DESC;
            }
        }
        // Check if column spec has "DESC" somewhere
        if (expr instanceof LiteralExpr lit && lit.value() instanceof String s) {
            if ("DESC".equalsIgnoreCase(s)) {
                return SortNode.SortDirection.DESC;
            }
        }
        return SortNode.SortDirection.ASC;
    }

    /**
     * Compiles a generic ExtendExpression (from new AST builder).
     * 
     * Handles window functions in the format:
     * extend(~rowNum : row_number()->over(~department, ~salary->desc()))
     * 
     * The AST structure is:
     * ExtendExpression
     * .source = ProjectExpression
     * .columns = [MethodCall[source=ColumnSpec[name=rowNum], methodName=over,
     * arguments=[...]]]
     */
    private RelationNode compileExtend(ExtendExpression extend, CompilationContext context) {
        RelationNode source = compileExpression(extend.source(), context);

        List<ExtendNode.WindowProjection> projections = new ArrayList<>();

        // Check if first arg is an over() function call (legend-engine style)
        List<PureExpression> columns = extend.columns();
        FunctionCall overFunctionCall = null;

        if (!columns.isEmpty() && columns.get(0) instanceof FunctionCall fc && "over".equals(fc.functionName())) {
            // Legend-engine style: extend(over(...), ~col:{...})
            overFunctionCall = fc;
            columns = columns.subList(1, columns.size()); // Skip the over() for column processing
        }

        for (PureExpression colExpr : columns) {
            if (colExpr instanceof MethodCall overCall && "over".equals(overCall.methodName())) {
                // Old style: MethodCall with over()
                ExtendNode.WindowProjection projection = compileWindowProjection(overCall);
                projections.add(projection);
            } else if (colExpr instanceof ColumnSpec colSpec && overFunctionCall != null) {
                // Legend-engine style: over() + ColumnSpec with aggregation
                ExtendNode.WindowProjection projection = compileWindowProjectionFromFunctionCall(overFunctionCall,
                        colSpec);
                projections.add(projection);
            } else if (colExpr instanceof ColumnSpec colSpec && colSpec.lambda() instanceof MethodCall mc
                    && "over".equals(mc.methodName())) {
                // Handle: ~prevSalary : lag(~salary, 1)->over(...) where lambda is the over()
                // call
                ExtendNode.WindowProjection projection = compileWindowProjectionWithAlias(colSpec.name(), mc);
                projections.add(projection);
            } else if (colExpr instanceof ColumnSpec colSpec && colSpec.extraFunction() instanceof MethodCall mc
                    && "over".equals(mc.methodName())) {
                // Handle: ~prevSalary : ... extraFunction contains over()
                ExtendNode.WindowProjection projection = compileWindowProjectionWithAlias(colSpec.name(), mc);
                projections.add(projection);
            } else {
                throw new PureCompileException(
                        "extend() currently only supports window functions with over(). Got: " + colExpr);
            }
        }

        return new ExtendNode(source, projections.stream().map(p -> (ExtendNode.ExtendProjection) p).toList());
    }

    /**
     * Compiles a window function MethodCall with over() into a WindowProjection.
     * 
     * Input format: MethodCall[source=ColumnSpec[name=rowNum], methodName=over,
     * arguments=[...]]
     * where the source ColumnSpec's name is the new column name, and if it has a
     * lambda,
     * the lambda contains the window function call.
     */
    private ExtendNode.WindowProjection compileWindowProjection(MethodCall overCall) {
        // The source of the over() call contains the column spec with window function
        // info
        PureExpression source = overCall.source();

        String newColumnName;
        String functionName;
        String aggregateColumn = null;
        Integer offset = null; // For LAG/LEAD

        if (source instanceof ColumnSpec colSpec) {
            newColumnName = colSpec.name();
            // The column name IS the function name (e.g., "rowNum" -> "row_number")
            functionName = mapColumnNameToWindowFunction(colSpec.name());
        } else if (source instanceof MethodCall funcCall) {
            // Nested method call like sum(~val)->over(...) or lag(~col, 1)->over(...)
            functionName = funcCall.methodName();

            // Extract column and offset for LAG/LEAD
            if ("lag".equals(functionName) || "lead".equals(functionName)) {
                if (!funcCall.arguments().isEmpty()) {
                    PureExpression firstArg = funcCall.arguments().get(0);
                    aggregateColumn = extractColumnName(firstArg);
                    // Second arg is the offset
                    if (funcCall.arguments().size() > 1) {
                        PureExpression offsetArg = funcCall.arguments().get(1);
                        if (offsetArg instanceof IntegerLiteral lit) {
                            offset = lit.value().intValue();
                        }
                    }
                }
                newColumnName = aggregateColumn != null ? aggregateColumn : "lag_lead_col";
            } else {
                newColumnName = extractColumnName(funcCall.source());
                aggregateColumn = newColumnName;
            }
        } else {
            throw new PureCompileException("Unknown window function source: " + source);
        }

        // Parse the over() arguments: partition columns and order columns
        List<String> partitionColumns = new ArrayList<>();
        List<WindowExpression.SortSpec> orderBy = new ArrayList<>();

        for (PureExpression arg : overCall.arguments()) {
            if (arg instanceof ColumnSpec cs) {
                // Partition column
                partitionColumns.add(cs.name());
            } else if (arg instanceof MethodCall mc) {
                if ("desc".equals(mc.methodName()) || "asc".equals(mc.methodName())) {
                    // Sort column with direction
                    String colName = extractColumnName(mc.source());
                    WindowExpression.SortDirection dir = "desc".equals(mc.methodName())
                            ? WindowExpression.SortDirection.DESC
                            : WindowExpression.SortDirection.ASC;
                    orderBy.add(new WindowExpression.SortSpec(colName, dir));
                } else {
                    // Unknown method call, treat as partition
                    partitionColumns.add(extractColumnName(mc));
                }
            }
        }

        // Create the WindowExpression
        WindowExpression.WindowFunction windowFunc = mapWindowFunction(functionName);
        WindowExpression windowExpr;

        if (offset != null) {
            // LAG/LEAD with offset
            windowExpr = WindowExpression.lagLead(windowFunc, aggregateColumn, offset, partitionColumns, orderBy);
        } else if (aggregateColumn != null) {
            // Aggregate window function
            windowExpr = WindowExpression.aggregate(windowFunc, aggregateColumn, partitionColumns, orderBy);
        } else {
            // Ranking function
            windowExpr = WindowExpression.ranking(windowFunc, partitionColumns, orderBy, null);
        }

        return new ExtendNode.WindowProjection(newColumnName, windowExpr);
    }

    /**
     * Compiles a window function with an explicit column alias.
     * Used for patterns like: ~prevSalary : lag(~salary, 1)->over(...)
     */
    private ExtendNode.WindowProjection compileWindowProjectionWithAlias(String alias, MethodCall overCall) {
        // The source of the over() call contains the function call (e.g., lag(~col,
        // offset))
        PureExpression source = overCall.source();

        String functionName;
        String aggregateColumn = null;
        Integer offset = null;

        if (source instanceof FunctionCall funcCall) {
            functionName = funcCall.functionName();
            // Extract column and offset for value window functions
            if (!funcCall.arguments().isEmpty()) {
                PureExpression firstArg = funcCall.arguments().get(0);
                if ("ntile".equals(functionName)) {
                    // NTILE takes bucket count as first arg
                    if (firstArg instanceof IntegerLiteral lit) {
                        offset = lit.value().intValue();
                    }
                } else {
                    // Value functions: lag, lead, first_value, last_value, nth_value
                    aggregateColumn = extractColumnName(firstArg);
                    if (funcCall.arguments().size() > 1 && funcCall.arguments().get(1) instanceof IntegerLiteral lit) {
                        offset = lit.value().intValue();
                    }
                }
            }
        } else if (source instanceof MethodCall mc) {
            functionName = mc.methodName();
            if (!mc.arguments().isEmpty()) {
                PureExpression firstArg = mc.arguments().get(0);
                if ("ntile".equals(functionName)) {
                    if (firstArg instanceof IntegerLiteral lit) {
                        offset = lit.value().intValue();
                    }
                } else {
                    aggregateColumn = extractColumnName(firstArg);
                    if (mc.arguments().size() > 1 && mc.arguments().get(1) instanceof IntegerLiteral lit) {
                        offset = lit.value().intValue();
                    }
                }
            }
        } else {
            throw new PureCompileException("Unknown window function source: " + source);
        }

        // Parse the over() arguments: partition columns, order columns, and frame
        List<String> partitionColumns = new ArrayList<>();
        List<WindowExpression.SortSpec> orderBy = new ArrayList<>();
        WindowExpression.FrameSpec frame = null;

        for (PureExpression arg : overCall.arguments()) {
            if (arg instanceof ColumnSpec cs) {
                partitionColumns.add(cs.name());
            } else if (arg instanceof MethodCall mc) {
                if ("desc".equals(mc.methodName()) || "asc".equals(mc.methodName())) {
                    String colName = extractColumnName(mc.source());
                    WindowExpression.SortDirection dir = "desc".equals(mc.methodName())
                            ? WindowExpression.SortDirection.DESC
                            : WindowExpression.SortDirection.ASC;
                    orderBy.add(new WindowExpression.SortSpec(colName, dir));
                } else {
                    partitionColumns.add(extractColumnName(mc));
                }
            } else if (arg instanceof FunctionCall fc) {
                // Handle rows() or range() frame specifications
                if ("rows".equals(fc.functionName())) {
                    frame = parseRowsFrame(fc);
                } else if ("range".equals(fc.functionName())) {
                    frame = parseRangeFrame(fc);
                }
            }
        }

        WindowExpression.WindowFunction windowFunc = mapWindowFunction(functionName);
        WindowExpression windowExpr;

        // NTILE uses offset for bucket count, not for row offset
        if (windowFunc == WindowExpression.WindowFunction.NTILE && offset != null) {
            windowExpr = WindowExpression.ntile(offset, partitionColumns, orderBy);
        } else if (offset != null && aggregateColumn != null) {
            // LAG/LEAD with offset
            windowExpr = WindowExpression.lagLead(windowFunc, aggregateColumn, offset, partitionColumns, orderBy);
        } else if (aggregateColumn != null) {
            // Value functions like FIRST_VALUE, LAST_VALUE, SUM, etc.
            windowExpr = WindowExpression.aggregate(windowFunc, aggregateColumn, partitionColumns, orderBy, frame);
        } else {
            // Ranking functions: ROW_NUMBER, RANK, DENSE_RANK
            windowExpr = WindowExpression.ranking(windowFunc, partitionColumns, orderBy, frame);
        }

        return new ExtendNode.WindowProjection(alias, windowExpr);
    }

    /**
     * Compiles window projection from legend-engine style:
     * extend(over(~partition, ~order->ascending(), rows(unbounded(), 0)),
     * ~newCol:{p,w,r|$r.col}:y|$y->plus())
     */
    private ExtendNode.WindowProjection compileWindowProjectionFromFunctionCall(FunctionCall overCall,
            ColumnSpec colSpec) {
        // Extract partition, order, and frame from over() arguments
        List<String> partitionColumns = new ArrayList<>();
        List<WindowExpression.SortSpec> orderBy = new ArrayList<>();
        WindowExpression.FrameSpec frame = null;

        for (PureExpression arg : overCall.arguments()) {
            if (arg instanceof ColumnSpec cs) {
                // Partition column
                partitionColumns.add(cs.name());
            } else if (arg instanceof MethodCall mc) {
                String methodName = mc.methodName();
                if ("ascending".equals(methodName) || "descending".equals(methodName)) {
                    String colName = extractColumnName(mc.source());
                    WindowExpression.SortDirection dir = "descending".equals(methodName)
                            ? WindowExpression.SortDirection.DESC
                            : WindowExpression.SortDirection.ASC;
                    orderBy.add(new WindowExpression.SortSpec(colName, dir));
                } else {
                    partitionColumns.add(extractColumnName(mc));
                }
            } else if (arg instanceof FunctionCall fc) {
                // Handle rows() or range() frame specifications
                if ("rows".equals(fc.functionName())) {
                    frame = parseRowsFrame(fc);
                } else if ("range".equals(fc.functionName())) {
                    frame = parseRangeFrame(fc);
                }
            }
        }

        // Get new column name
        String newColumnName = colSpec.name();

        // Extract window function information from the lambda body
        // Pattern 1: {p,w,r|$p->lag($r).salary} - MethodCall with PropertyAccess
        // Pattern 2: {p,w,r|$p->ntile($r,2)} - MethodCall with numeric argument
        // Pattern 3: {p,w,r|$p->rank($w,$r)} - MethodCall with window and relation args
        // Pattern 4: {p,w,r|$r.salary} - Simple property access for aggregates
        String functionName = "sum"; // Default
        String aggregateColumn = null;
        Integer offset = null;

        if (colSpec.lambda() instanceof LambdaExpression lambda) {
            PureExpression body = lambda.body();

            // Check if body is a property access on a method call: $p->lag($r).salary
            if (body instanceof PropertyAccessExpression pa) {
                if (pa.source() instanceof MethodCall mc) {
                    functionName = mc.methodName();
                    aggregateColumn = pa.propertyName();
                } else if (pa.source() instanceof FirstExpression) {
                    // Special AST node for first() window function
                    functionName = "first_value";
                    aggregateColumn = pa.propertyName();
                } else {
                    // Fall back to just getting the property name for simple access
                    aggregateColumn = pa.propertyName();
                }
            }
            // Check if body is a direct method call: $p->ntile($r,2) or $p->rank($w,$r)
            else if (body instanceof MethodCall mc) {
                functionName = mc.methodName();
                // Extract numeric argument (for ntile)
                for (PureExpression arg : mc.arguments()) {
                    if (arg instanceof LiteralExpr lit && lit.type() == LiteralExpr.LiteralType.INTEGER) {
                        offset = ((Number) lit.value()).intValue();
                    } else if (arg instanceof IntegerLiteral lit) {
                        offset = lit.value().intValue();
                    }
                }
            }
            // Simple property access: $r.salary (for aggregate functions)
            else if (body instanceof PropertyAccessExpression pa) {
                aggregateColumn = pa.propertyName();
            }
        }

        // Determine function from extraFunction (e.g., y|$y->sum() = sum)
        if (colSpec.extraFunction() instanceof LambdaExpression extraLambda) {
            if (extraLambda.body() instanceof MethodCall mc) {
                functionName = mapAggregateMethodToFunction(mc.methodName());
            }
        }

        // Create the WindowExpression
        WindowExpression.WindowFunction windowFunc = mapWindowFunction(functionName);
        WindowExpression windowExpr;

        if (offset != null) {
            // NTILE with bucket count
            windowExpr = WindowExpression.ntile(offset, partitionColumns, orderBy);
        } else if (aggregateColumn != null && isValueFunction(functionName)) {
            // LAG, LEAD, FIRST_VALUE, LAST_VALUE - value functions accessing a column
            windowExpr = WindowExpression.lagLead(windowFunc, aggregateColumn, 1, partitionColumns, orderBy, frame);
        } else if (aggregateColumn != null) {
            // Aggregate window function
            windowExpr = WindowExpression.aggregate(windowFunc, aggregateColumn, partitionColumns, orderBy, frame);
        } else {
            // Ranking function (RANK, DENSE_RANK, ROW_NUMBER, PERCENT_RANK, etc.)
            windowExpr = WindowExpression.ranking(windowFunc, partitionColumns, orderBy, frame);
        }

        return new ExtendNode.WindowProjection(newColumnName, windowExpr);
    }

    /**
     * Checks if a function name is a value function (returns row values, not
     * rankings).
     */
    private boolean isValueFunction(String functionName) {
        return switch (functionName.toLowerCase()) {
            case "lag", "lead", "first", "last", "first_value", "last_value", "nth", "nth_value" -> true;
            default -> false;
        };
    }

    /**
     * Parses a rows() frame specification: rows(start, end)
     * Where start/end can be: unbounded(), 0 (current row), negative (preceding),
     * positive (following)
     */
    private WindowExpression.FrameSpec parseRowsFrame(FunctionCall rowsCall) {
        List<PureExpression> args = rowsCall.arguments();
        if (args.size() < 2) {
            throw new PureCompileException("rows() requires start and end bounds");
        }

        WindowExpression.FrameBound start = parseFrameBound(args.get(0));
        WindowExpression.FrameBound end = parseFrameBound(args.get(1));

        return WindowExpression.FrameSpec.rows(start, end);
    }

    /**
     * Parses a range() frame specification.
     */
    private WindowExpression.FrameSpec parseRangeFrame(FunctionCall rangeCall) {
        List<PureExpression> args = rangeCall.arguments();
        if (args.size() < 2) {
            throw new PureCompileException("range() requires start and end bounds");
        }

        WindowExpression.FrameBound start = parseFrameBound(args.get(0));
        WindowExpression.FrameBound end = parseFrameBound(args.get(1));

        return WindowExpression.FrameSpec.range(start, end);
    }

    /**
     * Parses a frame bound: unbounded(), 0 (current row), or integer
     * (preceding/following)
     */
    private WindowExpression.FrameBound parseFrameBound(PureExpression expr) {
        if (expr instanceof FunctionCall fc && "unbounded".equals(fc.functionName())) {
            return WindowExpression.FrameBound.unbounded();
        } else if (expr instanceof LiteralExpr lit && lit.value() instanceof Number num) {
            int value = num.intValue();
            if (value == 0) {
                return WindowExpression.FrameBound.currentRow();
            } else if (value < 0) {
                return WindowExpression.FrameBound.preceding(-value); // Convert to positive offset
            } else {
                return WindowExpression.FrameBound.following(value);
            }
        } else if (expr instanceof UnaryExpression unary && "-".equals(unary.operator())) {
            // Handle negative literals like -1, -2
            if (unary.operand() instanceof LiteralExpr lit && lit.value() instanceof Number num) {
                int value = num.intValue();
                return WindowExpression.FrameBound.preceding(value); // Already positive, represents "X PRECEDING"
            }
        }
        throw new PureCompileException("Cannot parse frame bound: " + expr);
    }

    /**
     * Maps Pure aggregate method names to SQL function names.
     */
    private String mapAggregateMethodToFunction(String methodName) {
        return switch (methodName.toLowerCase()) {
            case "plus", "sum" -> "sum";
            case "average", "avg" -> "avg";
            case "min" -> "min";
            case "max" -> "max";
            case "count" -> "count";
            case "joinstrings" -> "string_agg";
            default -> methodName;
        };
    }

    /**
     * Maps column names like "rowNum", "denseRank" to window function names.
     */
    private String mapColumnNameToWindowFunction(String columnName) {
        // Convert camelCase to snake_case style function names
        return switch (columnName.toLowerCase()) {
            case "rownum", "rownumber" -> "row_number";
            case "denserank" -> "dense_rank";
            case "salaryrank", "rank" -> "rank";
            case "runningtotal", "sum" -> "sum";
            default -> columnName.toLowerCase().replaceAll("([a-z])([A-Z])", "$1_$2").toLowerCase();
        };
    }

    // ==================== Relation Literal Operations ====================

    /**
     * Compiles a Relation literal: #>{store::DatabaseRef.TABLE_NAME}
     * 
     * This creates a TableScanNode directly from the database table reference.
     */
    private RelationNode compileRelationLiteral(RelationLiteral literal) {
        // Extract simple database name from qualified reference
        // e.g., "store::PersonDatabase" -> "PersonDatabase"
        String dbRef = literal.databaseRef();
        String simpleDbName = dbRef.contains("::")
                ? dbRef.substring(dbRef.lastIndexOf("::") + 2)
                : dbRef;

        // Build table key: simpleDbName.tableName (e.g., "PersonDatabase.T_PERSON")
        String tableKey = simpleDbName + "." + literal.tableName();

        // Look up via model context if available
        if (modelContext != null) {
            var tableOpt = modelContext.findTable(tableKey);
            if (tableOpt.isPresent()) {
                return new TableNode(tableOpt.get(), "t0");
            }
            // Fallback: try just table name
            tableOpt = modelContext.findTable(literal.tableName());
            if (tableOpt.isPresent()) {
                return new TableNode(tableOpt.get(), "t0");
            }
        }

        // Fallback to mapping registry lookup
        RelationalMapping mapping = mappingRegistry.findByTableName(tableKey)
                .orElseThrow(() -> new PureCompileException(
                        "Table not found: " + literal.tableName() + " in " + literal.databaseRef()));

        return new TableNode(mapping.table(), "t0");
    }

    /**
     * Compiles a select expression: relation->select(~col1, ~col2)
     * 
     * This projects specific columns from the source Relation.
     */
    private RelationNode compileRelationSelect(RelationSelectExpression select, CompilationContext context) {
        RelationNode source = compileExpression(select.source(), context);

        // Build projection for each selected column - use empty alias for Relation API
        // Column references should be unqualified to work correctly in chained
        // operations
        List<Projection> projections = select.columns().stream()
                .map(colName -> Projection.column("", colName, colName))
                .toList();

        // Wrap the source in a ProjectNode
        return new ProjectNode(source, projections);
    }

    /**
     * Compiles an extend expression: relation->extend(~newCol : ...)
     * 
     * Supports both:
     * 1. Simple calculated columns: extend(~newCol : x | $x.col1 + $x.col2)
     * 2. Window functions: extend(~rowNum : row_number()->over(~department))
     * 3. Window with frame: extend(~sum : sum(~val)->over(~dept, ~date,
     * rows(unbounded(), 0)))
     */
    private RelationNode compileRelationExtend(RelationExtendExpression extend, CompilationContext context) {
        RelationNode source = compileExpression(extend.source(), context);

        if (extend.isWindowFunction()) {
            // Compile window function
            RelationExtendExpression.WindowFunctionSpec spec = extend.windowSpec();

            // Map function name to WindowFunction enum
            WindowExpression.WindowFunction windowFunc = mapWindowFunction(spec.functionName());

            // Map sort specs
            List<WindowExpression.SortSpec> orderBy = spec.orderColumns().stream()
                    .map(s -> new WindowExpression.SortSpec(
                            s.column(),
                            s.direction() == RelationExtendExpression.SortDirection.DESC
                                    ? WindowExpression.SortDirection.DESC
                                    : WindowExpression.SortDirection.ASC))
                    .toList();

            // Map frame spec if present
            WindowExpression.FrameSpec frameSpec = null;
            if (spec.hasFrame()) {
                frameSpec = mapFrameSpec(spec.frame());
            }

            WindowExpression windowExpr;
            if (spec.aggregateColumn() != null) {
                windowExpr = WindowExpression.aggregate(
                        windowFunc,
                        spec.aggregateColumn(),
                        spec.partitionColumns(),
                        orderBy,
                        frameSpec);
            } else {
                windowExpr = WindowExpression.ranking(
                        windowFunc,
                        spec.partitionColumns(),
                        orderBy,
                        frameSpec);
            }

            ExtendNode.WindowProjection projection = new ExtendNode.WindowProjection(extend.newColumnName(),
                    windowExpr);

            return new ExtendNode(source, List.of(projection));
        } else {
            // Simple calculated column: ~newCol: x | $x.col1 + $x.col2
            // or JSON access: ~page: x | $x.PAYLOAD->get('page')
            String tableAlias = getTableAlias(source);

            // Collect extended columns from the source expression (from flatten and extend
            // ops)
            java.util.Set<String> extendedColumns = collectExtendedColumns(extend.source());

            CompilationContext lambdaContext = new CompilationContext(
                    extend.expression().parameter(),
                    tableAlias,
                    null, // No class mapping - working with Relation columns
                    null,
                    false,
                    new java.util.HashMap<>(),
                    extendedColumns);

            Expression calculatedExpr = compileToSqlExpression(extend.expression().body(), lambdaContext);

            // Create SimpleProjection for the calculated column
            ExtendNode.SimpleProjection simpleProj = new ExtendNode.SimpleProjection(
                    extend.newColumnName(), calculatedExpr);

            return new ExtendNode(source, List.of(simpleProj));
        }
    }

    /**
     * Collects all extended column names from the source expression chain.
     * This includes columns from extend and flatten operations.
     */
    private java.util.Set<String> collectExtendedColumns(PureExpression source) {
        java.util.Set<String> columns = new java.util.HashSet<>();
        collectExtendedColumnsRecursive(source, columns);
        return columns;
    }

    private void collectExtendedColumnsRecursive(PureExpression source, java.util.Set<String> columns) {
        switch (source) {
            case FlattenExpression flatten -> {
                // Flatten adds its output column
                columns.add(flatten.columnName());
                collectExtendedColumnsRecursive(flatten.source(), columns);
            }
            case RelationExtendExpression extend -> {
                // Extend adds its new column
                columns.add(extend.newColumnName());
                collectExtendedColumnsRecursive(extend.source(), columns);
            }
            case RelationFilterExpression filter -> collectExtendedColumnsRecursive(filter.source(), columns);
            case RelationSelectExpression select -> collectExtendedColumnsRecursive(select.source(), columns);
            case RelationSortExpression sort -> collectExtendedColumnsRecursive(sort.source(), columns);
            case RelationLimitExpression limit -> collectExtendedColumnsRecursive(limit.source(), columns);
            default -> {
                /* Base case - no more extended columns to collect */ }
        }
    }

    /**
     * Maps AST FrameSpec to IR FrameSpec.
     */
    private WindowExpression.FrameSpec mapFrameSpec(RelationExtendExpression.FrameSpec astFrame) {
        WindowExpression.FrameBound start = mapFrameBound(astFrame.start());
        WindowExpression.FrameBound end = mapFrameBound(astFrame.end());

        return switch (astFrame.type()) {
            case ROWS -> WindowExpression.FrameSpec.rows(start, end);
            case RANGE -> WindowExpression.FrameSpec.range(start, end);
        };
    }

    /**
     * Maps AST FrameBound to IR FrameBound.
     */
    private WindowExpression.FrameBound mapFrameBound(RelationExtendExpression.FrameBound astBound) {
        return switch (astBound.type()) {
            case UNBOUNDED -> WindowExpression.FrameBound.unbounded();
            case CURRENT_ROW -> WindowExpression.FrameBound.currentRow();
            case PRECEDING -> WindowExpression.FrameBound.preceding(astBound.offset());
            case FOLLOWING -> WindowExpression.FrameBound.following(astBound.offset());
        };
    }

    /**
     * Maps a Pure function name to the corresponding WindowFunction enum.
     */
    private WindowExpression.WindowFunction mapWindowFunction(String functionName) {
        return switch (functionName.toLowerCase()) {
            case "row_number" -> WindowExpression.WindowFunction.ROW_NUMBER;
            case "rank" -> WindowExpression.WindowFunction.RANK;
            case "dense_rank" -> WindowExpression.WindowFunction.DENSE_RANK;
            case "ntile" -> WindowExpression.WindowFunction.NTILE;
            case "lag" -> WindowExpression.WindowFunction.LAG;
            case "lead" -> WindowExpression.WindowFunction.LEAD;
            case "first", "first_value" -> WindowExpression.WindowFunction.FIRST_VALUE;
            case "last", "last_value" -> WindowExpression.WindowFunction.LAST_VALUE;
            case "sum" -> WindowExpression.WindowFunction.SUM;
            case "avg" -> WindowExpression.WindowFunction.AVG;
            case "min" -> WindowExpression.WindowFunction.MIN;
            case "max" -> WindowExpression.WindowFunction.MAX;
            case "count" -> WindowExpression.WindowFunction.COUNT;
            default -> throw new PureCompileException("Unknown window function: " + functionName);
        };
    }

    /**
     * Compiles a from expression: query->from(runtime)
     * 
     * The from() binds a Relation query to a runtime for execution.
     * It compiles the source query and wraps it with runtime binding information.
     */
    private RelationNode compileFrom(FromExpression from, CompilationContext context) {
        // Compile the source Relation expression
        RelationNode source = compileExpression(from.source(), context);

        // Wrap in a FromNode that carries the runtime reference
        return new FromNode(source, from.runtimeRef());
    }

    // ==================== M2M graphFetch/serialize Operations ====================

    /**
     * Compiles a serialize expression from a graphFetch chain.
     * 
     * This is the terminal compile point for M2M queries:
     * Person.all()->graphFetch(#{...}#)->serialize(#{...}#)
     * 
     * The compilation:
     * 1. Gets the target class from the graphFetch tree
     * 2. Looks up the M2M mapping for that target class
     * 3. Uses M2MCompiler to generate the ProjectNode with transforms
     * 4. Applies any filters that were on the source before graphFetch
     */
    private RelationNode compileSerialize(SerializeExpression serialize, CompilationContext context) {
        GraphFetchExpression graphFetch = serialize.source();
        GraphFetchTree fetchTree = graphFetch.fetchTree();

        // Get the target class name (what we're transforming to)
        String targetClassName = fetchTree.rootClass();

        // Look up the M2M mapping for the target class
        M2MClassMapping m2mMapping = mappingRegistry.findM2MMapping(targetClassName)
                .orElseThrow(() -> new PureCompileException(
                        "No M2M mapping found for class: " + targetClassName +
                                ". Register it with MappingRegistry.registerM2M()"));

        // Get the source class and its relational mapping
        String sourceClassName = m2mMapping.sourceClassName();
        RelationalMapping sourceMapping = mappingRegistry.findByClassName(sourceClassName)
                .orElseThrow(() -> new PureCompileException(
                        "No relational mapping found for M2M source class: " + sourceClassName));

        // Generate table alias
        String rootAlias = "t" + aliasCounter++;

        // Track JOINs needed for associations (for deep fetch)
        List<M2MJoinInfo> associationJoins = new ArrayList<>();

        // Create M2MCompiler with resolver for association references
        M2MCompiler m2mCompiler = new M2MCompiler(sourceMapping, rootAlias)
                .withAssociationResolver((assocRef, propertyName) -> {
                    // Resolve the association to a nested JsonObjectExpression
                    return resolveAssociation(assocRef, propertyName, sourceMapping,
                            rootAlias, fetchTree, associationJoins);
                });

        RelationNode m2mProjection = m2mCompiler.compile(m2mMapping);

        // Check if there's a filter on the source (e.g.,
        // Person.all()->filter(...)->graphFetch(...))
        // If so, we need to wrap the filter around the M2M projection
        ClassExpression sourceExpr = graphFetch.source();
        if (sourceExpr instanceof ClassFilterExpression filterExpr) {
            // Compile the filter condition
            CompilationContext lambdaContext = new CompilationContext(
                    filterExpr.lambda().parameter(),
                    rootAlias,
                    sourceMapping,
                    sourceClassName,
                    true);

            Expression filterCondition = compileToSqlExpression(filterExpr.lambda().body(), lambdaContext);

            // Apply filter to the base table (not the projection)
            // The M2M projection's source is a TableNode, we need to wrap it with filter
            if (m2mProjection instanceof ProjectNode projectNode) {
                RelationNode filteredSource = new FilterNode(projectNode.source(), filterCondition);
                m2mProjection = new ProjectNode(filteredSource, projectNode.projections());
            }
        }

        // Apply JOINs for any associations
        if (!associationJoins.isEmpty() && m2mProjection instanceof ProjectNode projectNode) {
            RelationNode joinedSource = projectNode.source();
            for (M2MJoinInfo joinInfo : associationJoins) {
                joinedSource = new JoinNode(
                        joinedSource,
                        joinInfo.rightTable(),
                        joinInfo.condition(),
                        JoinNode.JoinType.LEFT_OUTER);
            }
            m2mProjection = new ProjectNode(joinedSource, projectNode.projections());
        }

        return m2mProjection;
    }

    /**
     * Resolves an M2M association reference to a nested JsonObjectExpression.
     * For 1-to-1: adds JOIN and returns JsonObjectExpression
     * For 1-to-many: returns SubqueryExpression with JsonArrayExpression
     */
    private Expression resolveAssociation(
            org.finos.legend.pure.dsl.m2m.AssociationRef assocRef,
            String propertyName,
            RelationalMapping sourceMapping,
            String sourceAlias,
            GraphFetchTree fetchTree,
            List<M2MJoinInfo> associationJoins) {

        // Look up the Join definition
        org.finos.legend.engine.store.Join join = mappingRegistry.findJoin(assocRef.joinName())
                .orElseThrow(() -> new PureCompileException(
                        "No join found: " + assocRef.joinName()));

        // Determine target table from join (the table that's NOT the source)
        String sourceTableName = sourceMapping.table().name();
        String targetTableName = join.getOtherTable(sourceTableName);

        // Find the relational mapping for the target table
        RelationalMapping targetMapping = mappingRegistry.findByTableName(targetTableName)
                .orElseThrow(() -> new PureCompileException(
                        "No relational mapping found for table: " + targetTableName));

        // Find the nested M2M mapping
        // The property type from the class definition tells us the target class
        // For now, derive from graphFetch tree or use naming convention
        String nestedClassName = getNestedClassFromFetchTree(fetchTree, propertyName);

        // Try to find the M2M mapping - first with the name as-is, then try singular
        // form
        M2MClassMapping nestedM2M = mappingRegistry.findM2MMapping(nestedClassName)
                .or(() -> {
                    // Try singular form (e.g., "Addresses" -> "Address")
                    if (nestedClassName.endsWith("es")) {
                        return mappingRegistry.findM2MMapping(
                                nestedClassName.substring(0, nestedClassName.length() - 2));
                    } else if (nestedClassName.endsWith("s")) {
                        return mappingRegistry.findM2MMapping(
                                nestedClassName.substring(0, nestedClassName.length() - 1));
                    }
                    return java.util.Optional.empty();
                })
                .orElseThrow(() -> new PureCompileException(
                        "No M2M mapping found for nested class: " + nestedClassName +
                                " (property: " + propertyName + ")"));

        // Generate alias for joined/subquery table
        String targetAlias = "t" + aliasCounter++;

        // Build correlation condition (same for both 1-to-1 and 1-to-many)
        String leftCol, rightCol;
        if (join.leftTable().equals(sourceTableName)) {
            leftCol = join.leftColumn();
            rightCol = join.rightColumn();
        } else {
            leftCol = join.rightColumn();
            rightCol = join.leftColumn();
        }

        Expression correlationCondition = new ComparisonExpression(
                ColumnReference.of(targetAlias, rightCol),
                ComparisonExpression.ComparisonOperator.EQUALS,
                ColumnReference.of(sourceAlias, leftCol));

        // Build nested projections for the JsonObjectExpression
        List<Projection> nestedProjections = new ArrayList<>();
        GraphFetchTree nestedFetchTree = getNestedFetchTree(fetchTree, propertyName);

        for (org.finos.legend.pure.dsl.m2m.M2MPropertyMapping pm : nestedM2M.propertyMappings()) {
            // Check if this property is requested in the nested fetch tree
            if (nestedFetchTree == null || wantsProperty(nestedFetchTree, pm.propertyName())) {
                // Compile the nested property expression
                M2MCompiler nestedCompiler = new M2MCompiler(targetMapping, targetAlias);
                Expression expr = nestedCompiler.compileExpression(pm.expression());
                nestedProjections.add(new Projection(expr, pm.propertyName()));
            }
        }

        // Detect if this is a 1-to-many by checking if property name is plural
        boolean isToMany = isCollectionProperty(propertyName);

        if (isToMany) {
            // 1-to-many: use correlated subquery with json_group_array()
            // (SELECT json_group_array(json_object(...)) FROM target WHERE correlation)
            TableNode targetTable = new TableNode(targetMapping.table(), targetAlias);
            FilterNode filteredTarget = new FilterNode(targetTable, correlationCondition);

            JsonObjectExpression jsonObj = new JsonObjectExpression(nestedProjections);
            JsonArrayExpression jsonArray = new JsonArrayExpression(jsonObj);

            return new SubqueryExpression(filteredTarget, jsonArray);
        } else {
            // 1-to-1: use LEFT JOIN with json_object()
            // Track the JOIN (will be added to FROM clause)
            TableNode targetTable = new TableNode(targetMapping.table(), targetAlias);

            // For JOIN, the condition references source.leftCol = target.rightCol
            Expression joinCondition = new ComparisonExpression(
                    ColumnReference.of(sourceAlias, leftCol),
                    ComparisonExpression.ComparisonOperator.EQUALS,
                    ColumnReference.of(targetAlias, rightCol));

            associationJoins.add(new M2MJoinInfo(targetTable, joinCondition, sourceAlias, targetAlias));

            return new JsonObjectExpression(nestedProjections);
        }
    }

    /**
     * Gets the nested class name from the graphFetch tree for a property.
     */
    private String getNestedClassFromFetchTree(GraphFetchTree tree, String propertyName) {
        if (tree == null) {
            // Fallback: capitalize property name
            return capitalize(propertyName);
        }

        for (GraphFetchTree.PropertyFetch pf : tree.properties()) {
            if (pf.name().equals(propertyName) && pf.subTree() != null) {
                String rootClass = pf.subTree().rootClass();
                if (rootClass != null && !rootClass.isEmpty()) {
                    // The parser sets rootClass to property name, which may be lowercase.
                    // Capitalize to get the class name (e.g., "address" -> "Address")
                    return capitalize(rootClass);
                }
            }
        }

        // Fallback
        return capitalize(propertyName);
    }

    /**
     * Capitalizes the first letter of a string.
     */
    private String capitalize(String s) {
        if (s == null || s.isEmpty())
            return s;
        return s.substring(0, 1).toUpperCase() + s.substring(1);
    }

    /**
     * Detects if a property name indicates a collection (1-to-many).
     * Uses simple heuristic: plural property names end with 's'.
     * 
     * Examples:
     * - "address" -> false (1-to-1)
     * - "addresses" -> true (1-to-many)
     * - "person" -> false
     * - "people" -> false (irregular plural, not detected)
     */
    private boolean isCollectionProperty(String propertyName) {
        if (propertyName == null || propertyName.length() < 2) {
            return false;
        }
        // Simple heuristic: ends with 's' but not 'ss' (like "address")
        // This handles "addresses" (collection) vs "address" (single)
        return propertyName.endsWith("es") ||
                (propertyName.endsWith("s") && !propertyName.endsWith("ss"));
    }

    /**
     * Gets the nested fetch tree for a property.
     */
    private GraphFetchTree getNestedFetchTree(GraphFetchTree tree, String propertyName) {
        if (tree == null)
            return null;

        for (GraphFetchTree.PropertyFetch pf : tree.properties()) {
            if (pf.name().equals(propertyName)) {
                return pf.subTree();
            }
        }
        return null;
    }

    /**
     * Checks if a property is wanted in the fetch tree.
     */
    private boolean wantsProperty(GraphFetchTree tree, String propertyName) {
        if (tree == null)
            return true;
        return tree.propertyNames().contains(propertyName);
    }

    /**
     * Record for tracking JOIN information during association resolution.
     */
    private record M2MJoinInfo(
            TableNode rightTable,
            Expression condition,
            String leftAlias,
            String rightAlias) {
    }

    /**
     * Compiles a projection that navigates through an association.
     * 
     * For: {p | $p.addresses.street}
     * Collects join info and returns the projection referencing the joined table.
     * 
     * The actual JOIN is added later in compileProject(), preserving any
     * filters that were applied to the source.
     */
    private Projection compileAssociationProjection(
            AssociationPath path,
            String projectionAlias,
            java.util.Map<String, JoinInfo> joinInfos,
            String baseTableAlias,
            RelationalMapping baseMapping) {

        List<NavigationSegment> segments = path.segments();

        // Find the association navigation segment
        NavigationSegment assocSegment = null;
        for (NavigationSegment seg : segments) {
            if (seg.isAssociationNavigation()) {
                assocSegment = seg;
                break;
            }
        }

        if (assocSegment == null) {
            throw new PureCompileException("No association found in path");
        }

        String assocPropertyName = assocSegment.propertyName();
        AssociationNavigation nav = assocSegment.navigation();
        RelationalMapping targetMapping = assocSegment.targetMapping();

        if (targetMapping == null || nav.join() == null) {
            throw new PureCompileException("No mapping or join for association: " + nav.association().name());
        }

        // Check if we already have join info for this association
        JoinInfo joinInfo = joinInfos.get(assocPropertyName);
        String targetAlias;

        if (joinInfo == null) {
            // Need to record join info (actual join built later)
            targetAlias = "j" + aliasCounter++;

            Join join = nav.join();
            String leftColumn = join.getColumnForTable(baseMapping.table().name());
            String rightColumn = join.getColumnForTable(targetMapping.table().name());

            joinInfo = new JoinInfo(targetAlias, targetMapping, leftColumn, rightColumn);
            joinInfos.put(assocPropertyName, joinInfo);
        } else {
            targetAlias = joinInfo.alias();
            targetMapping = joinInfo.targetMapping();
        }

        // Get the final property (e.g., "street" from $p.addresses.street)
        NavigationSegment finalSegment = segments.getLast();
        String finalProperty = finalSegment.propertyName();

        // Check if the property has an expression mapping (e.g., ->get('key', @Type))
        PropertyMapping propertyMapping = targetMapping.getPropertyMapping(finalProperty)
                .orElseThrow(() -> new PureCompileException("No property mapping for: " + finalProperty));

        if (propertyMapping.expressionString() != null) {
            // Expression mapping (JSON extraction) - use compileExpressionMapping
            Expression expr = compileExpressionMapping(propertyMapping.expressionString(), targetAlias);
            return new Projection(expr, projectionAlias);
        } else {
            // Simple column mapping
            String columnName = propertyMapping.columnName();
            return Projection.column(targetAlias, columnName, projectionAlias);
        }
    }

    /**
     * Extracts the final property name from a potentially chained property access.
     * For $p.addresses.street, returns "street".
     */
    private String extractFinalPropertyName(PureExpression expr) {
        if (expr instanceof PropertyAccessExpression propAccess) {
            return propAccess.propertyName();
        }
        throw new PureCompileException("Expected property access, got: " + expr);
    }

    /**
     * Information needed to build a JOIN for association projection.
     * The actual JoinNode is built later to preserve the source (with filters).
     */
    private record JoinInfo(
            String alias,
            RelationalMapping targetMapping,
            String leftColumn,
            String rightColumn) {
    }

    /**
     * Compiles a Pure expression to a SQL expression.
     * 
     * This method handles association navigation. When we encounter a property
     * access like $p.addresses.street where 'addresses' is a to-many association,
     * we generate an EXISTS subquery instead of a simple column reference.
     */
    private Expression compileToSqlExpression(PureExpression expr, CompilationContext context) {
        return switch (expr) {
            case ComparisonExpr comp -> compileComparison(comp, context);
            case LogicalExpr logical -> compileLogical(logical, context);
            case PropertyAccessExpression propAccess -> compilePropertyAccess(propAccess, context);
            case LiteralExpr literal -> compileLiteral(literal);
            case BinaryExpression binary -> compileBinaryExpression(binary, context);
            case MethodCall methodCall -> compileMethodCall(methodCall, context);
            case VariableExpr var -> {
                // Check if this is a lambda parameter (e.g., `i` in map(i | $i->get('price')))
                if (context.lambdaParameters() != null && context.isLambdaParameter(var.name())) {
                    // Lambda parameters become unqualified column references
                    yield ColumnReference.of(var.name());
                }
                throw new PureCompileException("Unexpected variable in expression context: " + var);
            }
            case CastExpression cast -> compileCastExpression(cast, context);
            default -> throw new PureCompileException("Cannot compile to SQL expression: " + expr);
        };
    }

    /**
     * Compiles cast(@Type) to a SqlFunctionCall with appropriate return type.
     */
    private Expression compileCastExpression(CastExpression cast, CompilationContext context) {
        Expression source = compileToSqlExpression(cast.source(), context);
        SqlType targetType = mapTypeName(cast.targetType());
        return SqlFunctionCall.of("cast", source, targetType);
    }

    /**
     * Maps Pure type names to SqlType.
     */
    private SqlType mapTypeName(String typeName) {
        return switch (typeName.toLowerCase()) {
            case "integer", "int" -> SqlType.INTEGER;
            case "float", "double", "number" -> SqlType.DOUBLE;
            case "string", "varchar" -> SqlType.VARCHAR;
            case "boolean", "bool" -> SqlType.BOOLEAN;
            default -> SqlType.UNKNOWN;
        };
    }

    /**
     * Compiles a MethodCall (like ->get(), ->fromJson(), ->toJson(), ->map(),
     * ->fold()) to a SQL
     * function call.
     */
    private Expression compileMethodCall(MethodCall methodCall, CompilationContext context) {
        String methodName = methodCall.methodName();

        // Handle collection functions that take lambdas
        return switch (methodName) {
            case "map" -> compileMapCall(methodCall, context);
            case "filter" -> compileFilterCollectionCall(methodCall, context);
            case "fold" -> compileFoldCall(methodCall, context);
            case "flatten" -> compileFlattenCall(methodCall, context);
            case "get" -> compileGetCall(methodCall, context);
            default -> compileSimpleMethodCall(methodCall, context);
        };
    }

    /**
     * Compiles get('key') or get('key', @Type).
     * get('key') returns JSON (for arrays/objects).
     * get('key', @Type) returns typed scalar value.
     */
    private Expression compileGetCall(MethodCall methodCall, CompilationContext context) {
        Expression source = compileToSqlExpression(methodCall.source(), context);
        List<PureExpression> args = methodCall.arguments();

        if (args.isEmpty()) {
            throw new PureCompileException("get() requires at least a key argument");
        }

        // First argument is the key
        Expression keyArg = compileToSqlExpression(args.get(0), context);

        // Check for optional type argument (@Type)
        SqlType returnType = SqlType.UNKNOWN;
        if (args.size() >= 2 && args.get(1) instanceof TypeReference typeRef) {
            returnType = mapTypeName(typeRef.typeName());
        }

        return new SqlFunctionCall("get", source, List.of(keyArg), returnType);
    }

    /**
     * Compiles a simple method call (get, fromJson, toJson, etc.)
     */
    private Expression compileSimpleMethodCall(MethodCall methodCall, CompilationContext context) {
        // Compile the source expression (e.g., $_.PAYLOAD)
        Expression source = compileToSqlExpression(methodCall.source(), context);

        // Compile additional arguments (e.g., the key name in get('page'))
        List<Expression> additionalArgs = new java.util.ArrayList<>();
        for (PureExpression arg : methodCall.arguments()) {
            additionalArgs.add(compileToSqlExpression(arg, context));
        }

        // Return SqlFunctionCall which will be handled by SQLGenerator
        return new SqlFunctionCall(methodCall.methodName(), source, additionalArgs, SqlType.UNKNOWN);
    }

    /**
     * Compiles map(x | expr) to list_transform(arr, lambda x: expr)
     */
    private Expression compileMapCall(MethodCall methodCall, CompilationContext context) {
        Expression source = compileToSqlExpression(methodCall.source(), context);

        if (methodCall.arguments().isEmpty() || !(methodCall.arguments().get(0) instanceof LambdaExpression lambda)) {
            throw new PureCompileException("map() requires a lambda argument");
        }

        // Compile lambda body with the lambda parameter in context
        String lambdaParam = lambda.parameter();
        CompilationContext newContext = context.withLambdaParameter(lambdaParam, "");
        Expression lambdaBody = compileToSqlExpression(lambda.body(), newContext);

        return SqlCollectionCall.map(source, lambdaParam, lambdaBody);
    }

    /**
     * Compiles filter(x | condition) on collections to list_filter
     */
    private Expression compileFilterCollectionCall(MethodCall methodCall, CompilationContext context) {
        Expression source = compileToSqlExpression(methodCall.source(), context);

        if (methodCall.arguments().isEmpty() || !(methodCall.arguments().get(0) instanceof LambdaExpression lambda)) {
            throw new PureCompileException("filter() on collection requires a lambda argument");
        }

        String lambdaParam = lambda.parameter();
        Expression lambdaBody = compileToSqlExpression(lambda.body(),
                context.withLambdaParameter(lambdaParam, ""));

        return SqlCollectionCall.filter(source, lambdaParam, lambdaBody);
    }

    /**
     * Compiles fold({acc, x | expr}, init) to list_reduce(arr, lambda acc, x: expr,
     * init)
     */
    private Expression compileFoldCall(MethodCall methodCall, CompilationContext context) {
        Expression source = compileToSqlExpression(methodCall.source(), context);

        if (methodCall.arguments().size() < 2) {
            throw new PureCompileException("fold() requires lambda and initial value arguments");
        }

        PureExpression lambdaArg = methodCall.arguments().get(0);
        if (!(lambdaArg instanceof LambdaExpression lambda)) {
            throw new PureCompileException("fold() first argument must be a lambda");
        }

        // Fold lambda has two parameters: accumulator and current element
        // Parse as "{acc, x | expr}" format
        String lambdaParams = lambda.parameter(); // This contains "acc, x" format
        String[] params = lambdaParams.split(",");
        if (params.length < 2) {
            throw new PureCompileException("fold() lambda requires two parameters (accumulator, element)");
        }
        String accParam = params[0].trim();
        String elemParam = params[1].trim();

        Expression lambdaBody = compileToSqlExpression(lambda.body(),
                context.withFoldParameters(accParam, elemParam));

        Expression initialValue = compileToSqlExpression(methodCall.arguments().get(1), context);

        return SqlCollectionCall.fold(source, accParam, elemParam, lambdaBody, initialValue);
    }

    /**
     * Compiles flatten() to flatten(arr)
     */
    private Expression compileFlattenCall(MethodCall methodCall, CompilationContext context) {
        Expression source = compileToSqlExpression(methodCall.source(), context);
        return SqlCollectionCall.flatten(source);
    }

    private Expression compileBinaryExpression(BinaryExpression binary, CompilationContext context) {
        Expression left = compileToSqlExpression(binary.left(), context);
        Expression right = compileToSqlExpression(binary.right(), context);
        String op = binary.operator();

        // Map to comparison operators
        return switch (op) {
            case "==", "=" -> new ComparisonExpression(left, ComparisonExpression.ComparisonOperator.EQUALS, right);
            case "!=" -> new ComparisonExpression(left, ComparisonExpression.ComparisonOperator.NOT_EQUALS, right);
            case "<" -> new ComparisonExpression(left, ComparisonExpression.ComparisonOperator.LESS_THAN, right);
            case "<=" ->
                new ComparisonExpression(left, ComparisonExpression.ComparisonOperator.LESS_THAN_OR_EQUALS, right);
            case ">" -> new ComparisonExpression(left, ComparisonExpression.ComparisonOperator.GREATER_THAN, right);
            case ">=" ->
                new ComparisonExpression(left, ComparisonExpression.ComparisonOperator.GREATER_THAN_OR_EQUALS, right);
            // Arithmetic operators - wrap in ArithmeticExpression
            case "+" -> ArithmeticExpression.add(left, right);
            case "-" -> ArithmeticExpression.subtract(left, right);
            case "*" -> ArithmeticExpression.multiply(left, right);
            case "/" -> ArithmeticExpression.divide(left, right);
            default -> throw new PureCompileException("Unknown binary operator: " + op);
        };
    }

    private Expression compileComparison(ComparisonExpr comp, CompilationContext context) {
        // Check if left side involves association navigation through to-many
        AssociationPath leftPath = analyzePropertyPath(comp.left(), context);

        if (leftPath != null && leftPath.hasToManyNavigation() && context.inFilterContext()) {
            // Generate EXISTS for to-many navigation
            return compileToManyComparison(leftPath, comp.operator(), comp.right(), context);
        }

        // Standard comparison
        Expression left = compileToSqlExpression(comp.left(), context);
        Expression right = compileToSqlExpression(comp.right(), context);

        ComparisonExpression.ComparisonOperator op = switch (comp.operator()) {
            case EQUALS -> ComparisonExpression.ComparisonOperator.EQUALS;
            case NOT_EQUALS -> ComparisonExpression.ComparisonOperator.NOT_EQUALS;
            case LESS_THAN -> ComparisonExpression.ComparisonOperator.LESS_THAN;
            case LESS_THAN_OR_EQUALS -> ComparisonExpression.ComparisonOperator.LESS_THAN_OR_EQUALS;
            case GREATER_THAN -> ComparisonExpression.ComparisonOperator.GREATER_THAN;
            case GREATER_THAN_OR_EQUALS -> ComparisonExpression.ComparisonOperator.GREATER_THAN_OR_EQUALS;
        };

        return new ComparisonExpression(left, op, right);
    }

    /**
     * Analyzes a property access path to detect association navigation.
     * 
     * For $p.addresses.street, this returns:
     * - baseVariable: "p"
     * - segments: ["addresses", "street"]
     * - toManyNavigations: [AssociationNavigation for addresses]
     */
    private AssociationPath analyzePropertyPath(PureExpression expr, CompilationContext context) {
        if (!(expr instanceof PropertyAccessExpression)) {
            return null;
        }

        if (modelContext == null) {
            return null; // No model context, can't analyze associations
        }

        // Collect the property chain
        List<String> segments = new ArrayList<>();
        PureExpression current = expr;

        while (current instanceof PropertyAccessExpression propAccess) {
            segments.addFirst(propAccess.propertyName()); // Add to front
            current = propAccess.source();
        }

        if (!(current instanceof VariableExpr var)) {
            return null;
        }

        // Verify the variable matches the lambda parameter
        if (!var.name().equals(context.lambdaParameter())) {
            return null;
        }

        // Analyze each segment for association navigation
        String currentClassName = context.className();
        RelationalMapping currentMapping = context.mapping();
        List<NavigationSegment> navigationSegments = new ArrayList<>();
        boolean hasToMany = false;

        for (int i = 0; i < segments.size(); i++) {
            String propName = segments.get(i);

            // Check if this is an association navigation
            var assocNav = modelContext.findAssociationByProperty(currentClassName, propName);

            if (assocNav.isPresent()) {
                var nav = assocNav.get();
                if (nav.isToMany()) {
                    hasToMany = true;
                }

                // Get the target class and mapping
                String targetClassName = nav.targetClassName();
                RelationalMapping targetMapping = modelContext.findMapping(targetClassName).orElse(null);

                navigationSegments.add(new NavigationSegment(
                        propName,
                        nav,
                        currentMapping,
                        targetMapping));

                currentClassName = targetClassName;
                currentMapping = targetMapping;
            } else {
                // This is a regular property access (final segment)
                navigationSegments.add(new NavigationSegment(
                        propName,
                        null,
                        currentMapping,
                        null));
            }
        }

        return new AssociationPath(var.name(), navigationSegments, hasToMany);
    }

    /**
     * Compiles a comparison that involves to-many navigation into an EXISTS
     * expression.
     * 
     * For: $p.addresses.street == 'Main St'
     * Generates: EXISTS (SELECT 1 FROM T_ADDRESS a WHERE a.PERSON_ID = p.ID AND
     * a.STREET = 'Main St')
     */
    private Expression compileToManyComparison(AssociationPath path, ComparisonExpr.Operator op,
            PureExpression rightExpr, CompilationContext context) {
        // Build the EXISTS subquery
        // We need to navigate through the association chain and build the correlated
        // subquery

        List<NavigationSegment> segments = path.segments();
        if (segments.isEmpty()) {
            throw new PureCompileException("Empty association path");
        }

        // Find the first to-many navigation
        int toManyIndex = -1;
        for (int i = 0; i < segments.size(); i++) {
            if (segments.get(i).isAssociationNavigation() && segments.get(i).navigation().isToMany()) {
                toManyIndex = i;
                break;
            }
        }

        if (toManyIndex < 0) {
            throw new PureCompileException("No to-many navigation found in path");
        }

        NavigationSegment toManySegment = segments.get(toManyIndex);
        AssociationNavigation nav = toManySegment.navigation();
        RelationalMapping targetMapping = toManySegment.targetMapping();

        if (targetMapping == null || nav.join() == null) {
            throw new PureCompileException("No mapping or join found for association: " + nav.association().name());
        }

        // Create the target table node
        String targetAlias = "sub" + aliasCounter++;
        TableNode targetTable = new TableNode(targetMapping.table(), targetAlias);

        // Build the correlation condition (e.g., a.PERSON_ID = p.ID)
        Join join = nav.join();
        String outerColumn = join.getColumnForTable(context.mapping().table().name());
        String innerColumn = join.getColumnForTable(targetMapping.table().name());

        Expression correlation = ComparisonExpression.equals(
                ColumnReference.of(targetAlias, innerColumn),
                ColumnReference.of(context.tableAlias(), outerColumn));

        // Build the filter condition on the target property
        // The final segment should be a property access on the target class
        NavigationSegment finalSegment = segments.getLast();
        String targetProperty = finalSegment.propertyName();
        String targetColumn = targetMapping.getColumnForProperty(targetProperty)
                .orElseThrow(() -> new PureCompileException("No column mapping for property: " + targetProperty));

        Expression left = ColumnReference.of(targetAlias, targetColumn);
        Expression right = compileLiteral((LiteralExpr) rightExpr);

        ComparisonExpression.ComparisonOperator sqlOp = switch (op) {
            case EQUALS -> ComparisonExpression.ComparisonOperator.EQUALS;
            case NOT_EQUALS -> ComparisonExpression.ComparisonOperator.NOT_EQUALS;
            case LESS_THAN -> ComparisonExpression.ComparisonOperator.LESS_THAN;
            case LESS_THAN_OR_EQUALS -> ComparisonExpression.ComparisonOperator.LESS_THAN_OR_EQUALS;
            case GREATER_THAN -> ComparisonExpression.ComparisonOperator.GREATER_THAN;
            case GREATER_THAN_OR_EQUALS -> ComparisonExpression.ComparisonOperator.GREATER_THAN_OR_EQUALS;
        };

        Expression propertyFilter = new ComparisonExpression(left, sqlOp, right);

        // Combine correlation and property filter
        Expression subqueryCondition = LogicalExpression.and(correlation, propertyFilter);

        // Create the subquery
        FilterNode subquery = new FilterNode(targetTable, subqueryCondition);

        // Return EXISTS expression
        return ExistsExpression.exists(subquery);
    }

    private Expression compileLogical(LogicalExpr logical, CompilationContext context) {
        List<Expression> compiledOperands = logical.operands().stream()
                .map(op -> compileToSqlExpression(op, context))
                .toList();

        return switch (logical.operator()) {
            case AND -> LogicalExpression.and(compiledOperands);
            case OR -> new LogicalExpression(LogicalExpression.LogicalOperator.OR, compiledOperands);
            case NOT -> LogicalExpression.not(compiledOperands.getFirst());
        };
    }

    private Expression compilePropertyAccess(PropertyAccessExpression propAccess, CompilationContext context) {
        String propertyName = propAccess.propertyName();

        // Standard property access handling
        String varName = extractVariableName(propAccess.source());

        if (!varName.equals(context.lambdaParameter())) {
            throw new PureCompileException(
                    "Unknown variable: $" + varName + ", expected: $" + context.lambdaParameter());
        }

        // Check if this is an extended column (from extend/flatten) - use unqualified
        // reference
        if (context.isExtendedColumn(propertyName)) {
            return ColumnReference.of(propertyName);
        }

        // If no mapping exists (direct Relation API access), use property name as
        // column name
        if (context.mapping() == null) {
            return ColumnReference.of(context.tableAlias(), propertyName);
        }

        String columnName = context.mapping().getColumnForProperty(propertyName)
                .orElseThrow(() -> new PureCompileException("No column mapping for property: " + propertyName));

        return ColumnReference.of(context.tableAlias(), columnName);
    }

    /**
     * Compiles an expression mapping string to a SQL expression.
     * 
     * Handles expressions like: [DB] T.PAYLOAD->get('price', @Integer)
     * 
     * @param expressionStr The expression string from the mapping
     * @param tableAlias    The table alias to use for column references
     * @return The compiled SQL expression
     */
    private Expression compileExpressionMapping(String expressionStr, String tableAlias) {
        // Pattern: [DB] TABLE.COLUMN->get('key', @Type)
        java.util.regex.Pattern pattern = java.util.regex.Pattern.compile(
                "\\[\\w+\\]\\s+\\w+\\.(\\w+)\\s*->\\s*get\\s*\\(\\s*'([^']+)'\\s*,\\s*@(\\w+)\\s*\\)");
        java.util.regex.Matcher matcher = pattern.matcher(expressionStr);

        if (matcher.find()) {
            String columnName = matcher.group(1);
            String jsonKey = matcher.group(2);
            String typeName = matcher.group(3);

            // Build: get(COLUMN, 'key') with return type
            Expression columnRef = ColumnReference.of(tableAlias, columnName);
            SqlType returnType = mapTypeName(typeName);
            return SqlFunctionCall.of("get", columnRef, returnType, Literal.string(jsonKey));
        }

        throw new PureCompileException("Cannot parse expression mapping: " + expressionStr);
    }

    private Expression compileLiteral(LiteralExpr literal) {
        return switch (literal.type()) {
            case STRING -> Literal.string((String) literal.value());
            case INTEGER -> Literal.integer(((Number) literal.value()).longValue());
            case FLOAT -> new Literal(literal.value(), Literal.LiteralType.DOUBLE);
            case BOOLEAN -> Literal.bool((Boolean) literal.value());
        };
    }

    private String extractPropertyName(PureExpression expr) {
        if (expr instanceof PropertyAccessExpression propAccess) {
            return propAccess.propertyName();
        }
        throw new PureCompileException("Expected property access in projection, got: " + expr);
    }

    private String extractVariableName(PureExpression expr) {
        if (expr instanceof VariableExpr var) {
            return var.name();
        }
        if (expr instanceof PropertyAccessExpression propAccess) {
            return extractVariableName(propAccess.source());
        }
        throw new PureCompileException("Cannot extract variable name from: " + expr);
    }

    private String getTableAlias(RelationNode node) {
        return switch (node) {
            case TableNode table -> table.alias();
            case FilterNode filter -> getTableAlias(filter.source());
            case ProjectNode project -> getTableAlias(project.source());
            case JoinNode join -> getTableAlias(join.left());
            case GroupByNode groupBy -> getTableAlias(groupBy.source());
            case SortNode sort -> getTableAlias(sort.source());
            case LimitNode limit -> getTableAlias(limit.source());
            case org.finos.legend.engine.plan.FromNode from -> getTableAlias(from.source());
            case ExtendNode extend -> getTableAlias(extend.source());
            case LateralJoinNode lateral -> getTableAlias(lateral.source());
            case DistinctNode distinct -> getTableAlias(distinct.source());
            case RenameNode rename -> getTableAlias(rename.source());
            case ConcatenateNode concat -> getTableAlias(concat.left());
        };
    }

    private RelationalMapping getMappingFromSource(PureExpression source) {
        return switch (source) {
            case ClassAllExpression classAll -> mappingRegistry.findByClassName(classAll.className())
                    .orElseThrow(() -> new PureCompileException("No mapping for class: " + classAll.className()));
            case ClassFilterExpression filter -> getMappingFromSource(filter.source());
            case ProjectExpression project -> getMappingFromSource(project.source());
            default -> throw new PureCompileException("Cannot determine mapping from: " + source);
        };
    }

    private String getClassNameFromSource(PureExpression source) {
        return switch (source) {
            case ClassAllExpression classAll -> classAll.className();
            case ClassFilterExpression filter -> getClassNameFromSource(filter.source());
            case ProjectExpression project -> getClassNameFromSource(project.source());
            default -> throw new PureCompileException("Cannot determine class name from: " + source);
        };
    }

    /**
     * Context for compiling within a lambda expression.
     */
    public record CompilationContext(
            String lambdaParameter,
            String tableAlias,
            RelationalMapping mapping,
            String className,
            boolean inFilterContext,
            java.util.Map<String, String> lambdaParameters,
            java.util.Set<String> extendedColumns) {

        public CompilationContext(String lambdaParameter, String tableAlias,
                RelationalMapping mapping, String className, boolean inFilterContext) {
            this(lambdaParameter, tableAlias, mapping, className, inFilterContext,
                    new java.util.HashMap<>(), new java.util.HashSet<>());
        }

        /**
         * Legacy constructor for backwards compatibility.
         */
        public CompilationContext(String lambdaParameter, String tableAlias, RelationalMapping mapping) {
            this(lambdaParameter, tableAlias, mapping, mapping.pureClass().name(), false,
                    new java.util.HashMap<>(), new java.util.HashSet<>());
        }

        /**
         * Creates a new context with a lambda parameter (for map/filter).
         */
        public CompilationContext withLambdaParameter(String paramName, String alias) {
            var newParams = new java.util.HashMap<>(lambdaParameters);
            newParams.put(paramName, alias);
            return new CompilationContext(lambdaParameter, tableAlias, mapping, className,
                    inFilterContext, newParams, extendedColumns);
        }

        /**
         * Creates a new context with fold parameters (accumulator and element).
         */
        public CompilationContext withFoldParameters(String accParam, String elemParam) {
            var newParams = new java.util.HashMap<>(lambdaParameters);
            newParams.put(accParam, "");
            newParams.put(elemParam, "");
            return new CompilationContext(lambdaParameter, tableAlias, mapping, className,
                    inFilterContext, newParams, extendedColumns);
        }

        /**
         * Creates a new context with an extended column (from extend/flatten).
         * Extended columns should use unqualified references.
         */
        public CompilationContext withExtendedColumn(String columnName) {
            var newExtended = new java.util.HashSet<>(extendedColumns);
            newExtended.add(columnName);
            return new CompilationContext(lambdaParameter, tableAlias, mapping, className,
                    inFilterContext, lambdaParameters, newExtended);
        }

        /**
         * Checks if a variable name is a lambda parameter.
         */
        public boolean isLambdaParameter(String name) {
            return lambdaParameters.containsKey(name);
        }

        /**
         * Checks if a column name is an extended column (should be unqualified).
         */
        public boolean isExtendedColumn(String name) {
            return extendedColumns.contains(name);
        }
    }

    /**
     * Represents a navigation path through properties and associations.
     */
    private record AssociationPath(
            String baseVariable,
            List<NavigationSegment> segments,
            boolean hasToManyNavigation) {
    }

    /**
     * Represents a single segment in a navigation path.
     */
    private record NavigationSegment(
            String propertyName,
            AssociationNavigation navigation,
            RelationalMapping sourceMapping,
            RelationalMapping targetMapping) {
        boolean isAssociationNavigation() {
            return navigation != null;
        }
    }
}
