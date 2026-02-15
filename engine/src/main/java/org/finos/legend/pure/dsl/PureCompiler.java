package org.finos.legend.pure.dsl;

import org.finos.legend.engine.plan.*;
import org.finos.legend.engine.plan.GenericType.Primitive;
import org.finos.legend.engine.store.*;
import org.finos.legend.pure.dsl.ModelContext.AssociationNavigation;
import org.finos.legend.pure.dsl.definition.PureModelBuilder;
import org.finos.legend.pure.dsl.m2m.M2MClassMapping;
import org.finos.legend.pure.dsl.m2m.M2MCompiler;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

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
    private final TypeEnvironment typeEnvironment;
    private int aliasCounter = 0;

    /**
     * Creates a compiler with just a mapping registry (legacy compatibility).
     */
    public PureCompiler(MappingRegistry mappingRegistry) {
        this(mappingRegistry, null, TypeEnvironment.empty());
    }

    /**
     * Creates a compiler with full model context for association navigation.
     * 
     * @param mappingRegistry The mapping registry (can be null for STRUCT literal
     *                        compilation)
     * @param modelContext    The model context (optional)
     */
    public PureCompiler(MappingRegistry mappingRegistry, ModelContext modelContext) {
        this(mappingRegistry, modelContext, TypeEnvironment.empty());
    }

    /**
     * Creates a compiler with full model context and type environment.
     * 
     * @param mappingRegistry  The mapping registry (can be null for STRUCT literal compilation)
     * @param modelContext     The model context (optional)
     * @param typeEnvironment  Type metadata for classes, multiplicities, etc.
     */
    public PureCompiler(MappingRegistry mappingRegistry, ModelContext modelContext, TypeEnvironment typeEnvironment) {
        this.mappingRegistry = mappingRegistry; // Allow null for STRUCT literal compilation
        this.modelContext = modelContext;
        this.typeEnvironment = typeEnvironment != null ? typeEnvironment : TypeEnvironment.empty();
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
            case AggregateExpression aggregate -> compileAggregate(aggregate, context);
            case JoinExpression join -> compileJoin(join, context);
            case AsOfJoinExpression asOfJoin -> compileAsOfJoin(asOfJoin, context);
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
            case RelationProjectExpression relationProject -> compileRelationProject(relationProject, context);
            case RelationExtendExpression extend -> compileRelationExtend(extend, context);
            case ExtendExpression extend -> compileExtend(extend, context);
            case FromExpression from -> compileFrom(from, context);
            case SerializeExpression serialize -> compileSerialize(serialize, context);
            case DistinctExpression distinct -> compileDistinct(distinct, context);
            case RenameExpression rename -> compileRename(rename, context);
            case ConcatenateExpression concatenate -> compileConcatenate(concatenate, context);
            case PivotExpression pivot -> compilePivot(pivot, context);
            case RelationWriteExpression write -> compileRelationWrite(write, context);
            // CastExpression to Relation - compile the source expression (cast is type
            // annotation only)
            case CastExpression cast -> compileExpression(cast.source(), context);
            case TdsLiteral tds -> compileTdsLiteral(tds);
            case LambdaExpression lambda -> compileConstant(lambda, context);
            case VariableExpr var -> compileRelationVariable(var, context);
            case BlockExpression block -> compileBlock(block, context);
            case ArrayLiteral array when isInstanceArray(array) -> compileInstanceArray(array);
            case InstanceExpression inst -> compileSingleInstance(inst);
            default -> throw new PureCompileException("Cannot compile expression to RelationNode: " + expr);
        };
    }

    /**
     * Compiles a lambda expression that returns either a scalar or a relation.
     * 
     * For scalar results (e.g., |1+1), creates a ConstantNode with SELECT 1+1.
     * For relation results (e.g., |#TDS...#->filter(...)), compiles the body as a
     * relation.
     *
     * @param lambda  The lambda expression
     * @param context Compilation context (may be null for constant expressions)
     * @return A RelationNode (either ConstantNode for scalars or relation node for
     *         relations)
     */
    private RelationNode compileConstant(LambdaExpression lambda, CompilationContext context) {
        PureExpression body = lambda.body();

        // Check if the body is a relation expression that should go through
        // compileExpression
        if (isRelationExpression(body)) {
            return compileExpression(body, context);
        }

        // Create empty context if null - needed for method calls like map() that use
        // lambdas
        CompilationContext workingContext = context;
        if (workingContext == null) {
            workingContext = new CompilationContext(null, null, null, null, false);
        }

        // Otherwise, treat as scalar expression for SELECT <expr>
        Expression sqlExpr = compileToSqlExpression(body, workingContext);
        return new ConstantNode(sqlExpr);
    }

    /**
     * Compiles a block expression containing let statements and a result.
     * 
     * Let statements are processed first to bind variables to the symbol table,
     * then the result expression is compiled with the enriched context.
     *
     * @param block   The block expression
     * @param context The compilation context (may be null)
     * @return The compiled relation node
     */
    private RelationNode compileBlock(BlockExpression block, CompilationContext context) {
        // Start with input context or create empty one
        CompilationContext workingContext = context;
        if (workingContext == null) {
            workingContext = new CompilationContext(null, null, null, null, false);
        }

        // Process let statements, binding each to the symbol table
        for (LetExpression let : block.letStatements()) {
            PureExpression valueExpr = let.value();
            String varName = let.variableName();

            // Determine the type of the value and bind appropriately
            if (isRelationExpression(valueExpr)) {
                // Relation-valued let: compile to RelationNode and bind
                RelationNode relationNode = compileExpression(valueExpr, workingContext);
                workingContext = workingContext.withRelationBinding(varName, relationNode);
            } else if (valueExpr instanceof LambdaExpression lambdaValue
                    && isRelationExpression(lambdaValue.body())) {
                // Lambda that returns a relation
                RelationNode relationNode = compileExpression(lambdaValue.body(), workingContext);
                workingContext = workingContext.withRelationBinding(varName, relationNode);
            } else {
                // Scalar-valued let: compile to Expression and bind
                Expression scalarExpr = compileToSqlExpression(valueExpr, workingContext);
                workingContext = workingContext.withScalarBinding(varName, scalarExpr);
            }
        }

        // Compile the result expression with the enriched context
        PureExpression result = block.result();

        // For VariableExpr results, check the symbol table to determine type
        if (result instanceof VariableExpr varResult && workingContext.hasSymbol(varResult.name())) {
            SymbolBinding binding = workingContext.lookupSymbol(varResult.name());
            if (binding.kind() == SymbolBinding.BindingKind.SCALAR) {
                // Scalar binding - return as ConstantNode
                return new ConstantNode((Expression) binding.value());
            }
            // RELATION binding - compile via compileExpression
            return compileExpression(result, workingContext);
        }

        // For other expressions, use the standard approach
        if (isRelationExpression(result)) {
            return compileExpression(result, workingContext);
        } else {
            // Scalar result - compile to SQL expression and wrap in ConstantNode
            Expression sqlExpr = compileToSqlExpression(result, workingContext);
            return new ConstantNode(sqlExpr);
        }
    }

    /**
     * Checks if an expression is a relation expression that should be compiled
     * via compileExpression rather than compileToSqlExpression.
     */
    private boolean isRelationExpression(PureExpression expr) {
        // Collection operations on scalar sources (ArrayLiteral, LiteralExpr) are scalar, not relational
        if (expr instanceof DropExpression drop && isScalarSource(drop.source())) return false;
        if (expr instanceof SliceExpression slice && isScalarSource(slice.source())) return false;
        if (expr instanceof FirstExpression first && isScalarSource(first.source())) return false;
        if (expr instanceof LimitExpression limit && isScalarSource(limit.source())) return false;
        if (expr instanceof ConcatenateExpression concat && isScalarSource(concat.left())) return false;
        if (expr instanceof SortExpression sort && isScalarSource(sort.source())) return false;

        return expr instanceof RelationExpression
                || expr instanceof ClassAllExpression
                || expr instanceof ClassFilterExpression
                || expr instanceof ProjectExpression
                || expr instanceof GroupByExpression
                || expr instanceof JoinExpression
                || expr instanceof FlattenExpression
                || expr instanceof SortExpression
                || expr instanceof SortByExpression
                || expr instanceof LimitExpression
                || expr instanceof FirstExpression
                || expr instanceof SliceExpression
                || expr instanceof DropExpression
                || expr instanceof DistinctExpression
                || expr instanceof ConcatenateExpression
                || expr instanceof PivotExpression
                || expr instanceof RelationWriteExpression
                || expr instanceof TdsLiteral
                || expr instanceof ExtendExpression
                || expr instanceof RenameExpression
                || expr instanceof FromExpression
                || expr instanceof SerializeExpression
                || expr instanceof BlockExpression
                || expr instanceof VariableExpr; // Variable could be a let-bound relation
    }

    /**
     * Checks if a source expression is a known scalar type (not a relation).
     */
    private boolean isScalarSource(PureExpression source) {
        return source instanceof ArrayLiteral || source instanceof LiteralExpr;
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

            // Create context with ROW binding for the lambda parameter
            CompilationContext ctx = new CompilationContext(
                    cfe.lambda().parameter(),
                    "t0", // Default alias
                    mapping,
                    className,
                    true).withRowBinding(cfe.lambda().parameter(), "t0");
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

        // Create context for the lambda with ROW binding for the parameter
        CompilationContext lambdaContext = new CompilationContext(
                filter.lambda().parameter(),
                tableAlias,
                mapping,
                className,
                true // We're in a filter context
        ).withRowBinding(filter.lambda().parameter(), tableAlias);

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
        String rowAlias = "";

        // Create a context with ROW binding for the lambda parameter
        // This binds $x in filter(x | $x.col > 5) to the row alias
        CompilationContext lambdaContext = new CompilationContext(
                filter.lambda().parameter(),
                rowAlias,
                null, // No class mapping - we're working with Relation columns
                null,
                true).withRowBinding(filter.lambda().parameter(), rowAlias);

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
            // Create context for this projection with ROW binding for the lambda parameter
            CompilationContext projContext = new CompilationContext(
                    lambda.parameter(),
                    baseTableAlias,
                    baseMapping,
                    baseClassName,
                    false).withRowBinding(lambda.parameter(), baseTableAlias);

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
            } else if (lambda.body() instanceof FunctionCall mc && mc.source() != null && isDateFunction(mc.functionName())) {
                // Date function call: $e.birthDate->year()
                String propertyName = extractPropertyName(mc.source());
                String columnName = baseMapping.getColumnForProperty(propertyName)
                        .orElseThrow(() -> new PureCompileException("No column mapping for property: " + propertyName));
                Expression colRef = ColumnReference.of(baseTableAlias, columnName, baseMapping.pureTypeForProperty(propertyName));
                DateFunctionExpression dateExpr = new DateFunctionExpression(
                        mapDateFunction(mc.functionName()), colRef);
                projections.add(new Projection(dateExpr, alias));
            } else if (lambda.body() instanceof FunctionCall fc && isCurrentDateFunction(fc.functionName())) {
                // Zero-arg date function: now() or today()
                CurrentDateExpression currentDateExpr = new CurrentDateExpression(
                        mapCurrentDateFunction(fc.functionName()));
                projections.add(new Projection(currentDateExpr, alias));
            } else if (lambda.body() instanceof FunctionCall fc && fc.source() == null && "dateDiff".equalsIgnoreCase(fc.functionName())) {
                // dateDiff(d1, d2, DurationUnit.DAYS) -> DATE_DIFF('day', d1, d2)
                if (fc.arguments().size() != 3) {
                    throw new PureCompileException("dateDiff requires 3 arguments: dateDiff(date1, date2, unit)");
                }
                Expression d1 = compileProjectionArg(fc.arguments().get(0), baseTableAlias, baseMapping);
                Expression d2 = compileProjectionArg(fc.arguments().get(1), baseTableAlias, baseMapping);
                DurationUnit unit = parseDurationUnit(fc.arguments().get(2));
                DateDiffExpression dateDiffExpr = new DateDiffExpression(d1, d2, unit);
                projections.add(new Projection(dateDiffExpr, alias));
            } else if (lambda.body() instanceof FunctionCall fc && fc.source() == null && "adjust".equalsIgnoreCase(fc.functionName())) {
                // adjust(date, amount, DurationUnit.DAYS) -> date + INTERVAL 'amount' DAY
                if (fc.arguments().size() != 3) {
                    throw new PureCompileException("adjust requires 3 arguments: adjust(date, amount, unit)");
                }
                Expression dateExpr = compileProjectionArg(fc.arguments().get(0), baseTableAlias, baseMapping);
                Expression amountExpr = compileProjectionArg(fc.arguments().get(1), baseTableAlias, baseMapping);
                DurationUnit unit = parseDurationUnit(fc.arguments().get(2));
                DateAdjustExpression adjustExpr = new DateAdjustExpression(dateExpr, amountExpr, unit);
                projections.add(new Projection(adjustExpr, alias));
            } else if (lambda.body() instanceof FunctionCall mc && mc.source() != null && isDateTruncFunction(mc.functionName())) {
                // Date truncation function: $e.date->firstDayOfMonth()
                Expression argument;
                if (isDateTruncThisFunction(mc.functionName())) {
                    // firstDayOfThisMonth() - use CURRENT_DATE as argument
                    argument = new CurrentDateExpression(CurrentDateExpression.CurrentDateFunction.TODAY);
                } else {
                    // firstDayOfMonth($e.date) - use the source expression
                    String propertyName = extractPropertyName(mc.source());
                    String columnName = baseMapping.getColumnForProperty(propertyName)
                            .orElseThrow(
                                    () -> new PureCompileException("No column mapping for property: " + propertyName));
                    argument = ColumnReference.of(baseTableAlias, columnName, baseMapping.pureTypeForProperty(propertyName));
                }
                DateTruncExpression truncExpr = new DateTruncExpression(
                        mapDateTruncFunction(mc.functionName()), argument);
                projections.add(new Projection(truncExpr, alias));
            } else if (lambda.body() instanceof FunctionCall fc
                    && "fromEpochValue".equalsIgnoreCase(fc.functionName())) {
                // fromEpochValue(seconds) -> TO_TIMESTAMP(seconds)
                if (fc.arguments().isEmpty() || fc.arguments().size() > 2) {
                    throw new PureCompileException(
                            "fromEpochValue requires 1-2 arguments: fromEpochValue(value[, unit])");
                }
                Expression value = compileProjectionArg(fc.arguments().get(0), baseTableAlias, baseMapping);
                DurationUnit unit = fc.arguments().size() > 1
                        ? parseDurationUnit(fc.arguments().get(1))
                        : DurationUnit.SECONDS;
                EpochExpression epochExpr = EpochExpression.fromEpoch(value, unit);
                projections.add(new Projection(epochExpr, alias));
            } else if (lambda.body() instanceof FunctionCall fc && "toEpochValue".equalsIgnoreCase(fc.functionName())) {
                // toEpochValue(date) -> EPOCH(date)
                if (fc.arguments().isEmpty() || fc.arguments().size() > 2) {
                    throw new PureCompileException("toEpochValue requires 1-2 arguments: toEpochValue(date[, unit])");
                }
                Expression value = compileProjectionArg(fc.arguments().get(0), baseTableAlias, baseMapping);
                DurationUnit unit = fc.arguments().size() > 1
                        ? parseDurationUnit(fc.arguments().get(1))
                        : DurationUnit.SECONDS;
                EpochExpression epochExpr = EpochExpression.toEpoch(value, unit);
                projections.add(new Projection(epochExpr, alias));
            } else if (lambda.body() instanceof FunctionCall fc && fc.source() == null && isDateComparisonFunction(fc.functionName())) {
                // isOnDay(d1, d2), isAfterDay, etc.
                if (fc.arguments().size() != 2) {
                    throw new PureCompileException(fc.functionName() + " requires 2 arguments");
                }
                Expression d1 = compileProjectionArg(fc.arguments().get(0), baseTableAlias, baseMapping);
                Expression d2 = compileProjectionArg(fc.arguments().get(1), baseTableAlias, baseMapping);
                DateComparisonExpression dateCompExpr = mapDateComparisonFunction(fc.functionName(), d1, d2);
                projections.add(new Projection(dateCompExpr, alias));
            } else if (lambda.body() instanceof FunctionCall mc && mc.source() != null && isDateComparisonFunction(mc.functionName())) {
                // $e.date1->isOnDay($e.date2)
                Expression d1 = compileProjectionArg(mc.source(), baseTableAlias, baseMapping);
                if (mc.arguments().isEmpty()) {
                    throw new PureCompileException(mc.functionName() + " requires 1 argument");
                }
                Expression d2 = compileProjectionArg(mc.arguments().get(0), baseTableAlias, baseMapping);
                DateComparisonExpression dateCompExpr = mapDateComparisonFunction(mc.functionName(), d1, d2);
                projections.add(new Projection(dateCompExpr, alias));
            } else if (lambda.body() instanceof FunctionCall mc && mc.source() != null && "timeBucket".equalsIgnoreCase(mc.functionName())) {
                // $e.date->timeBucket(5, DurationUnit.DAYS)
                if (mc.arguments().size() != 2) {
                    throw new PureCompileException("timeBucket requires 2 arguments: timeBucket(quantity, unit)");
                }
                Expression dateExpr = compileProjectionArg(mc.source(), baseTableAlias, baseMapping);
                Expression quantity = compileProjectionArg(mc.arguments().get(0), baseTableAlias, baseMapping);
                DurationUnit unit = parseDurationUnit(mc.arguments().get(1));
                TimeBucketExpression timeBucketExpr = TimeBucketExpression.of(dateExpr, quantity, unit);
                projections.add(new Projection(timeBucketExpr, alias));
            } else if (lambda.body() instanceof FunctionCall mc && mc.source() != null && "adjust".equalsIgnoreCase(mc.functionName())) {
                // $e.date->adjust(amount, DurationUnit.DAYS) - method call syntax with 2 args
                if (mc.arguments().size() != 2) {
                    throw new PureCompileException("adjust as method requires 2 arguments: date->adjust(amount, unit)");
                }
                Expression dateExpr = compileProjectionArg(mc.source(), baseTableAlias, baseMapping);
                Expression amountExpr = compileProjectionArg(mc.arguments().get(0), baseTableAlias, baseMapping);
                DurationUnit unit = parseDurationUnit(mc.arguments().get(1));
                DateAdjustExpression adjustExpr = new DateAdjustExpression(dateExpr, amountExpr, unit);
                projections.add(new Projection(adjustExpr, alias));
            } else if (lambda.body() instanceof FunctionCall fc && fc.source() == null && "timeBucket".equalsIgnoreCase(fc.functionName())) {
                // timeBucket(date, quantity, unit)
                if (fc.arguments().size() != 3) {
                    throw new PureCompileException("timeBucket requires 3 arguments: timeBucket(date, quantity, unit)");
                }
                Expression dateExpr = compileProjectionArg(fc.arguments().get(0), baseTableAlias, baseMapping);
                Expression quantity = compileProjectionArg(fc.arguments().get(1), baseTableAlias, baseMapping);
                DurationUnit unit = parseDurationUnit(fc.arguments().get(2));
                TimeBucketExpression timeBucketExpr = TimeBucketExpression.of(dateExpr, quantity, unit);
                projections.add(new Projection(timeBucketExpr, alias));
            } else if (lambda.body() instanceof FunctionCall fc
                    && ("min".equalsIgnoreCase(fc.functionName()) || "max".equalsIgnoreCase(fc.functionName()))) {
                // min(d1, d2) or max(d1, d2)
                if (fc.arguments().size() != 2) {
                    throw new PureCompileException(fc.functionName() + " requires 2 arguments");
                }
                Expression left = compileProjectionArg(fc.arguments().get(0), baseTableAlias, baseMapping);
                Expression right = compileProjectionArg(fc.arguments().get(1), baseTableAlias, baseMapping);
                MinMaxExpression minMaxExpr = "min".equalsIgnoreCase(fc.functionName())
                        ? MinMaxExpression.min(left, right)
                        : MinMaxExpression.max(left, right);
                projections.add(new Projection(minMaxExpr, alias));
            } else {
                // Property access - check if expression-based or column-based
                String propertyName = extractPropertyName(lambda.body());
                var propertyMapping = baseMapping.getPropertyMapping(propertyName);

                if (propertyMapping.isPresent() && propertyMapping.get().hasExpression()) {
                    // Expression-based mapping: compile the expression string
                    String expressionStr = propertyMapping.get().expressionString();
                    Expression expr = compileExpressionMapping(expressionStr, baseTableAlias);
                    projections.add(new Projection(expr, alias));
                } else if (propertyMapping.isPresent() && propertyMapping.get().hasEnumMapping()) {
                    // Enum column mapping: generate CASE expression to translate db values to enum
                    // values
                    String columnName = propertyMapping.get().columnName();
                    Expression caseExpr = buildEnumMappingCaseExpression(
                            baseTableAlias, columnName,
                            propertyMapping.get().enumType(),
                            propertyMapping.get().enumMapping());
                    projections.add(new Projection(caseExpr, alias));
                } else {
                    String columnName = baseMapping.getColumnForProperty(propertyName)
                            .orElseThrow(
                                    () -> new PureCompileException("No column mapping for property: " + propertyName));
                    projections.add(Projection.column(baseTableAlias, columnName, alias, baseMapping.pureTypeForProperty(propertyName)));
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
                    ColumnReference.of(baseTableAlias, ji.leftColumn(), Primitive.ANY),
                    ColumnReference.of(ji.alias(), ji.rightColumn(), Primitive.ANY));
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

            // Extract column name(s) and aggregate function from lambda body
            // Supports:
            // {r | $r.col} (defaults to SUM)
            // {r | $r.col->stdDev()} (explicit single-column)
            // {r | $r.col1->corr($r.col2)} (bi-variate)
            // {r | $r.col->percentileCont(0.5)} (ordered-set aggregate)
            // {r | $r.col->joinStrings(',')} (string aggregation)
            String columnName;
            String secondColumnName = null;
            Double percentileValue = null;
            String separator = null;
            org.finos.legend.engine.plan.AggregateExpression.AggregateFunction aggFunc;

            PureExpression body = aggLambda.body();
            if (body instanceof FunctionCall methodCall) {
                // Pattern: $r.col->stdDev() or $r.col1->corr($r.col2) or
                // $r.col->percentileCont(0.5) or $r.col->joinStrings(',')
                // Also: rowMapper($x.col1, $x.col2)->wavg() (merged from two-lambda pattern)
                aggFunc = mapAggregateFunction(methodCall.functionName());

                // Detect rowMapper pattern: source is rowMapper($x.col1, $x.col2)
                if (methodCall.source() instanceof FunctionCall rowMapperCall
                        && rowMapperCall.functionName().endsWith("rowMapper")) {
                    if (rowMapperCall.source() != null) {
                        // Arrow style: $x.col1->rowMapper($x.col2)
                        columnName = extractPropertyName(rowMapperCall.source());
                        if (!rowMapperCall.arguments().isEmpty()) {
                            secondColumnName = extractPropertyName(rowMapperCall.arguments().get(0));
                        }
                    } else if (rowMapperCall.arguments().size() >= 2) {
                        // Standalone: rowMapper($x.col1, $x.col2)
                        columnName = extractPropertyName(rowMapperCall.arguments().get(0));
                        secondColumnName = extractPropertyName(rowMapperCall.arguments().get(1));
                    } else {
                        columnName = extractPropertyName(methodCall.source());
                    }
                } else {
                    columnName = extractPropertyName(methodCall.source());
                }

                // Check for bi-variate function with second argument (non-rowMapper pattern)
                if (aggFunc.isBivariate() && secondColumnName == null && !methodCall.arguments().isEmpty()) {
                    secondColumnName = extractPropertyName(methodCall.arguments().get(0));
                }

                // Check for percentile function with percentile value argument
                if (isPercentileFunction(aggFunc) && !methodCall.arguments().isEmpty()) {
                    PureExpression arg = methodCall.arguments().get(0);
                    if (arg instanceof DecimalLiteral dl) {
                        percentileValue = dl.value().doubleValue();
                    } else if (arg instanceof IntegerLiteral il) {
                        percentileValue = il.value().doubleValue();
                    } else if (arg instanceof LiteralExpr le) {
                        // Handle LiteralExpr with FLOAT or INTEGER type
                        if (le.type() == LiteralExpr.LiteralType.FLOAT
                                || le.type() == LiteralExpr.LiteralType.INTEGER) {
                            percentileValue = ((Number) le.value()).doubleValue();
                        } else {
                            throw new PureCompileException(
                                    "Percentile function requires a numeric literal argument, got: " + le.type());
                        }
                    } else {
                        throw new PureCompileException(
                                "Percentile function requires a numeric literal argument, got: " + arg);
                    }
                    // percentile(p, ascending, continuous): 3rd arg false -> DISC
                    if (methodCall.arguments().size() >= 3) {
                        PureExpression contArg = methodCall.arguments().get(2);
                        if (contArg instanceof LiteralExpr le2 && Boolean.FALSE.equals(le2.value())) {
                            aggFunc = org.finos.legend.engine.plan.AggregateExpression.AggregateFunction.PERCENTILE_DISC;
                        }
                    }
                    // percentile(p, ascending=false, ...): 2nd arg false -> use (1-p)
                    if (methodCall.arguments().size() >= 2) {
                        PureExpression ascArg = methodCall.arguments().get(1);
                        if (ascArg instanceof LiteralExpr le2 && Boolean.FALSE.equals(le2.value())) {
                            percentileValue = 1.0 - percentileValue;
                        }
                    }
                }

                // Check for joinStrings/STRING_AGG with separator argument
                if (isStringAggFunction(aggFunc) && !methodCall.arguments().isEmpty()) {
                    PureExpression arg = methodCall.arguments().get(0);
                    if (arg instanceof StringLiteral sl) {
                        separator = sl.value();
                    } else if (arg instanceof LiteralExpr le && le.type() == LiteralExpr.LiteralType.STRING) {
                        separator = (String) le.value();
                    } else {
                        throw new PureCompileException("joinStrings requires a string literal separator");
                    }
                }
            } else {
                // Pattern: $r.col - defaults to SUM
                columnName = extractPropertyName(body);
                aggFunc = org.finos.legend.engine.plan.AggregateExpression.AggregateFunction.SUM;
            }

            // Get alias from the aliases list or generate one
            // Note: aliases list contains grouping column names first, then aggregate
            // aliases
            // So we need to offset by the number of grouping columns
            String alias;
            int aliasIndex = groupingColumns.size() + i;
            if (aliasIndex < groupBy.aliases().size()) {
                alias = groupBy.aliases().get(aliasIndex);
            } else {
                alias = columnName + "_agg";
            }

            aggregations.add(
                    new GroupByNode.AggregateProjection(alias, columnName, secondColumnName, aggFunc, percentileValue,
                            separator));
        }

        return new GroupByNode(source, groupingColumns, aggregations);
    }

    /**
     * Compiles an AggregateExpression into a GroupByNode with no grouping columns.
     * 
     * This aggregates the entire relation into a single row.
     * Example: ->aggregate(~idSum : x | $x.id : y | $y->plus())
     * SQL: SELECT SUM(id) AS idSum FROM source
     *
     * @param aggregate    The aggregate expression
     * @param outerContext Compilation context
     * @return A GroupByNode with empty grouping columns
     */
    private RelationNode compileAggregate(AggregateExpression aggregate, CompilationContext outerContext) {
        RelationNode source = compileExpression(aggregate.source(), outerContext);

        // Use AggregateNode (no grouping columns - aggregates entire relation)

        // Extract aggregations: mapFunctions give column, aggFunctions give aggregation
        // type
        List<GroupByNode.AggregateProjection> aggregations = new ArrayList<>();
        for (int i = 0; i < aggregate.mapFunctions().size(); i++) {
            LambdaExpression mapLambda = aggregate.mapFunctions().get(i);
            LambdaExpression aggLambda = aggregate.aggFunctions().get(i);

            // Extract column name from map function body (e.g., x | $x.id -> id)
            // Also detect rowMapper pattern: x | rowMapper($x.col1, $x.col2)
            String columnName;
            String secondColumnName = null;
            PureExpression mapBody = mapLambda.body();
            if (mapBody instanceof FunctionCall rowMapperMc && "rowMapper".equals(rowMapperMc.functionName())) {
                columnName = extractPropertyName(rowMapperMc.source());
                if (!rowMapperMc.arguments().isEmpty()) {
                    secondColumnName = extractPropertyName(rowMapperMc.arguments().get(0));
                }
            } else if (mapBody instanceof FunctionCall rowMapperFc && rowMapperFc.functionName().endsWith("rowMapper")) {
                if (rowMapperFc.arguments().size() >= 2) {
                    columnName = extractPropertyName(rowMapperFc.arguments().get(0));
                    secondColumnName = extractPropertyName(rowMapperFc.arguments().get(1));
                } else {
                    columnName = extractPropertyName(mapBody);
                }
            } else {
                columnName = extractPropertyName(mapBody);
            }
            Double percentileValue = null;
            String separator = null;
            org.finos.legend.engine.plan.AggregateExpression.AggregateFunction aggFunc;

            // Extract aggregation function from agg function body
            PureExpression aggBody = aggLambda.body();
            if (aggBody instanceof FunctionCall methodCall) {
                // Pattern: y | $y->plus() or y | $y->sum()
                aggFunc = mapAggregateFunction(methodCall.functionName());

                // Check for bi-variate function with second argument
                if (aggFunc.isBivariate() && !methodCall.arguments().isEmpty()) {
                    secondColumnName = extractPropertyName(methodCall.arguments().get(0));
                }

                // Check for percentile function with percentile value argument
                if (isPercentileFunction(aggFunc) && !methodCall.arguments().isEmpty()) {
                    PureExpression arg = methodCall.arguments().get(0);
                    if (arg instanceof DecimalLiteral dl) {
                        percentileValue = dl.value().doubleValue();
                    } else if (arg instanceof IntegerLiteral il) {
                        percentileValue = il.value().doubleValue();
                    } else if (arg instanceof LiteralExpr le) {
                        if (le.type() == LiteralExpr.LiteralType.FLOAT
                                || le.type() == LiteralExpr.LiteralType.INTEGER) {
                            percentileValue = ((Number) le.value()).doubleValue();
                        } else {
                            throw new PureCompileException(
                                    "Percentile function requires a numeric literal argument");
                        }
                    }
                    // percentile(p, ascending, continuous): 3rd arg false -> DISC
                    if (methodCall.arguments().size() >= 3) {
                        PureExpression contArg = methodCall.arguments().get(2);
                        if (contArg instanceof LiteralExpr le2 && Boolean.FALSE.equals(le2.value())) {
                            aggFunc = org.finos.legend.engine.plan.AggregateExpression.AggregateFunction.PERCENTILE_DISC;
                        }
                    }
                    // percentile(p, ascending=false, ...): 2nd arg false -> use (1-p)
                    if (methodCall.arguments().size() >= 2) {
                        PureExpression ascArg = methodCall.arguments().get(1);
                        if (ascArg instanceof LiteralExpr le2 && Boolean.FALSE.equals(le2.value())) {
                            percentileValue = 1.0 - percentileValue;
                        }
                    }
                }

                // Check for joinStrings with separator argument
                if (isStringAggFunction(aggFunc) && !methodCall.arguments().isEmpty()) {
                    PureExpression arg = methodCall.arguments().get(0);
                    if (arg instanceof StringLiteral sl) {
                        separator = sl.value();
                    } else if (arg instanceof LiteralExpr le && le.type() == LiteralExpr.LiteralType.STRING) {
                        separator = (String) le.value();
                    } else {
                        throw new PureCompileException("joinStrings requires a string literal separator");
                    }
                }
            } else {
                // No method call - defaults to SUM
                aggFunc = org.finos.legend.engine.plan.AggregateExpression.AggregateFunction.SUM;
            }

            // Get alias from the aliases list
            String alias = i < aggregate.aliases().size() ? aggregate.aliases().get(i) : columnName + "_agg";

            aggregations.add(
                    new GroupByNode.AggregateProjection(alias, columnName, secondColumnName, aggFunc, percentileValue,
                            separator));
        }

        return new AggregateNode(source, aggregations);
    }

    /**
     * Maps a Pure aggregate function name to the AggregateFunction enum.
     */
    private org.finos.legend.engine.plan.AggregateExpression.AggregateFunction mapAggregateFunction(
            String functionName) {
        return switch (functionName.toLowerCase()) {
            case "sum", "plus" -> org.finos.legend.engine.plan.AggregateExpression.AggregateFunction.SUM;
            case "avg", "average" -> org.finos.legend.engine.plan.AggregateExpression.AggregateFunction.AVG;
            case "count", "size" -> org.finos.legend.engine.plan.AggregateExpression.AggregateFunction.COUNT;
            case "min" -> org.finos.legend.engine.plan.AggregateExpression.AggregateFunction.MIN;
            case "max" -> org.finos.legend.engine.plan.AggregateExpression.AggregateFunction.MAX;
            // Statistical functions
            case "stddev" -> org.finos.legend.engine.plan.AggregateExpression.AggregateFunction.STDDEV;
            case "stddevsample", "stdDevSample" ->
                org.finos.legend.engine.plan.AggregateExpression.AggregateFunction.STDDEV_SAMP;
            case "stddevpopulation", "stdDevPopulation" ->
                org.finos.legend.engine.plan.AggregateExpression.AggregateFunction.STDDEV_POP;
            case "variance" -> org.finos.legend.engine.plan.AggregateExpression.AggregateFunction.VARIANCE;
            case "variancesample", "varianceSample" ->
                org.finos.legend.engine.plan.AggregateExpression.AggregateFunction.VAR_SAMP;
            case "variancepopulation", "variancePopulation" ->
                org.finos.legend.engine.plan.AggregateExpression.AggregateFunction.VAR_POP;
            case "median" -> org.finos.legend.engine.plan.AggregateExpression.AggregateFunction.MEDIAN;
            // Correlation and covariance
            case "corr" -> org.finos.legend.engine.plan.AggregateExpression.AggregateFunction.CORR;
            case "covarsample", "covar_samp" ->
                org.finos.legend.engine.plan.AggregateExpression.AggregateFunction.COVAR_SAMP;
            case "covarpopulation", "covar_pop" ->
                org.finos.legend.engine.plan.AggregateExpression.AggregateFunction.COVAR_POP;
            // Percentile functions
            case "percentilecont", "percentile_cont", "percentile" ->
                org.finos.legend.engine.plan.AggregateExpression.AggregateFunction.PERCENTILE_CONT;
            case "percentiledisc", "percentile_disc" ->
                org.finos.legend.engine.plan.AggregateExpression.AggregateFunction.PERCENTILE_DISC;
            // String aggregation
            case "joinstrings" -> org.finos.legend.engine.plan.AggregateExpression.AggregateFunction.STRING_AGG;
            // Mode (most frequent value)
            case "mode" -> org.finos.legend.engine.plan.AggregateExpression.AggregateFunction.MODE;
            // Hash code
            case "hashcode" -> org.finos.legend.engine.plan.AggregateExpression.AggregateFunction.HASH_CODE;
            // Weighted average and arg max/min (rowMapper bi-variate functions)
            case "wavg" -> org.finos.legend.engine.plan.AggregateExpression.AggregateFunction.WAVG;
            case "maxby" -> org.finos.legend.engine.plan.AggregateExpression.AggregateFunction.ARG_MAX;
            case "minby" -> org.finos.legend.engine.plan.AggregateExpression.AggregateFunction.ARG_MIN;
            default -> throw new PureCompileException("Unknown aggregate function: " + functionName);
        };
    }

    /**
     * Checks if the given aggregate function is a percentile function.
     */
    private boolean isPercentileFunction(org.finos.legend.engine.plan.AggregateExpression.AggregateFunction function) {
        return function == org.finos.legend.engine.plan.AggregateExpression.AggregateFunction.PERCENTILE_CONT
                || function == org.finos.legend.engine.plan.AggregateExpression.AggregateFunction.PERCENTILE_DISC;
    }

    /**
     * Checks if the given aggregate function is a string aggregation function.
     */
    private boolean isStringAggFunction(org.finos.legend.engine.plan.AggregateExpression.AggregateFunction function) {
        return function == org.finos.legend.engine.plan.AggregateExpression.AggregateFunction.STRING_AGG;
    }

    /**
     * Checks if the given method name is a date extraction function.
     */
    private boolean isDateFunction(String methodName) {
        return switch (methodName.toLowerCase()) {
            case "year", "month", "monthnumber", "dayofmonth", "day", "hour", "minute", "second",
                    "dayofweek", "dayofweeknumber", "dayofyear", "weekofyear", "quarter", "quarternumber" ->
                true;
            default -> false;
        };
    }

    /**
     * Maps a Pure date function name to the DateFunction enum.
     */
    private DateFunctionExpression.DateFunction mapDateFunction(String functionName) {
        return switch (functionName.toLowerCase()) {
            case "year" -> DateFunctionExpression.DateFunction.YEAR;
            case "month", "monthnumber" -> DateFunctionExpression.DateFunction.MONTH;
            case "dayofmonth", "day" -> DateFunctionExpression.DateFunction.DAY;
            case "hour" -> DateFunctionExpression.DateFunction.HOUR;
            case "minute" -> DateFunctionExpression.DateFunction.MINUTE;
            case "second" -> DateFunctionExpression.DateFunction.SECOND;
            case "dayofweek", "dayofweeknumber" -> DateFunctionExpression.DateFunction.DAY_OF_WEEK;
            case "dayofyear" -> DateFunctionExpression.DateFunction.DAY_OF_YEAR;
            case "weekofyear" -> DateFunctionExpression.DateFunction.WEEK_OF_YEAR;
            case "quarter", "quarternumber" -> DateFunctionExpression.DateFunction.QUARTER;
            default -> throw new PureCompileException("Unknown date function: " + functionName);
        };
    }

    /**
     * Checks if the given method name is a zero-argument current date function.
     */
    private boolean isCurrentDateFunction(String methodName) {
        return switch (methodName.toLowerCase()) {
            case "now", "today" -> true;
            default -> false;
        };
    }

    /**
     * Maps a zero-argument date function to CurrentDateExpression.
     */
    private CurrentDateExpression.CurrentDateFunction mapCurrentDateFunction(String functionName) {
        return switch (functionName.toLowerCase()) {
            case "now" -> CurrentDateExpression.CurrentDateFunction.NOW;
            case "today" -> CurrentDateExpression.CurrentDateFunction.TODAY;
            default -> throw new PureCompileException("Unknown current date function: " + functionName);
        };
    }

    /**
     * Checks if the given method name is a date truncation function (firstDayOf*).
     */
    private boolean isDateTruncFunction(String methodName) {
        return switch (methodName.toLowerCase()) {
            case "firstdayofmonth", "firstdayofyear", "firstdayofquarter", "firstdayofweek",
                    "firstdayofthismonth", "firstdayofthisyear", "firstdayofthisquarter",
                    "datepart", "firsthourofday", "firstminuteofhour", "firstsecondofminute" ->
                true;
            default -> false;
        };
    }

    /**
     * Checks if the date truncation function is a "this" variant (no argument).
     */
    private boolean isDateTruncThisFunction(String methodName) {
        return switch (methodName.toLowerCase()) {
            case "firstdayofthismonth", "firstdayofthisyear", "firstdayofthisquarter" -> true;
            default -> false;
        };
    }

    /**
     * Maps a date truncation function to TruncPart.
     */
    private DateTruncExpression.TruncPart mapDateTruncFunction(String functionName) {
        return switch (functionName.toLowerCase()) {
            case "firstdayofmonth", "firstdayofthismonth" -> DateTruncExpression.TruncPart.MONTH;
            case "firstdayofyear", "firstdayofthisyear" -> DateTruncExpression.TruncPart.YEAR;
            case "firstdayofquarter", "firstdayofthisquarter" -> DateTruncExpression.TruncPart.QUARTER;
            case "firstdayofweek" -> DateTruncExpression.TruncPart.WEEK;
            case "datepart", "firsthourofday" -> DateTruncExpression.TruncPart.DAY;
            case "firstminuteofhour" -> DateTruncExpression.TruncPart.HOUR;
            case "firstsecondofminute" -> DateTruncExpression.TruncPart.MINUTE;
            default -> throw new PureCompileException("Unknown date truncation function: " + functionName);
        };
    }

    /**
     * Compiles a projection argument expression (property access or literal).
     * Used for multi-argument functions like dateDiff and adjust.
     */
    private Expression compileProjectionArg(PureExpression expr, String tableAlias, RelationalMapping mapping) {
        // Property access: $e.propertyName (from ANTLR: PropertyAccessExpression)
        if (expr instanceof PropertyAccessExpression pa) {
            String propertyName = pa.propertyName();
            String columnName = mapping.getColumnForProperty(propertyName)
                    .orElseThrow(() -> new PureCompileException("No column mapping for property: " + propertyName));
            return ColumnReference.of(tableAlias, columnName, mapping.pureTypeForProperty(propertyName));
        }
        // LiteralExpr (integers, strings, floats, etc. from ANTLR parser)
        if (expr instanceof LiteralExpr le) {
            return compileLiteral(le);
        }
        // Integer literal (legacy support)
        if (expr instanceof IntegerLiteral il) {
            return Literal.integer(il.value());
        }
        // String literal (legacy support)
        if (expr instanceof StringLiteral sl) {
            return Literal.string(sl.value());
        }
        throw new PureCompileException("Unsupported projection argument: " + expr.getClass().getSimpleName());
    }

    /**
     * Parses a DurationUnit from a Pure expression.
     * Handles: DurationUnit.DAYS, DAYS, 'day'
     */
    private DurationUnit parseDurationUnit(PureExpression expr) {
        // EnumValueReference from proper parsing: DurationUnit.DAYS ->
        // EnumValueReference(DurationUnit, DAYS)
        if (expr instanceof EnumValueReference enumRef) {
            return DurationUnit.fromPure(enumRef.valueName());
        }
        // Enum reference: DurationUnit.DAYS (parsed as PropertyAccessExpression) -
        // legacy fallback
        if (expr instanceof PropertyAccessExpression pa) {
            return DurationUnit.fromPure(pa.propertyName());
        }
        // Qualified enum: meta::pure::duration::DurationUnit.DAYS (parsed as
        // MethodCall)
        if (expr instanceof FunctionCall mc) {
            return DurationUnit.fromPure(mc.functionName());
        }
        // String literal: 'day' or 'DAYS'
        if (expr instanceof StringLiteral sl) {
            return DurationUnit.fromPure(sl.value());
        }
        // FunctionCall for enum values like DAYS()
        if (expr instanceof FunctionCall fc) {
            return DurationUnit.fromPure(fc.functionName());
        }
        throw new PureCompileException("Expected DurationUnit, got: " + expr.getClass().getSimpleName());
    }

    /**
     * Test helper: Expose parseDurationUnit for testing.
     */
    public DurationUnit testParseDurationUnit(PureExpression expr) {
        return parseDurationUnit(expr);
    }

    /**
     * Test helper: Compile an EnumValueReference to SQL expression.
     */
    public Expression compileEnumToSql(EnumValueReference enumRef) {
        return Literal.string(enumRef.valueName());
    }

    /**
     * Checks if a function name is a date comparison function.
     */
    private boolean isDateComparisonFunction(String functionName) {
        return switch (functionName.toLowerCase()) {
            case "isonday", "isafterday", "isbeforeday", "isonorafterday", "isonorbeforeday" -> true;
            default -> false;
        };
    }

    /**
     * Maps a date comparison function to its corresponding Expression.
     */
    private DateComparisonExpression mapDateComparisonFunction(String functionName, Expression d1, Expression d2) {
        return switch (functionName.toLowerCase()) {
            case "isonday" -> DateComparisonExpression.isOnDay(d1, d2);
            case "isafterday" -> DateComparisonExpression.isAfterDay(d1, d2);
            case "isbeforeday" -> DateComparisonExpression.isBeforeDay(d1, d2);
            case "isonorafterday" -> DateComparisonExpression.isOnOrAfterDay(d1, d2);
            case "isonorbeforeday" -> DateComparisonExpression.isOnOrBeforeDay(d1, d2);
            default -> throw new PureCompileException("Unknown date comparison function: " + functionName);
        };
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
        String leftAlias = "left_src";
        String rightAlias = "right_src";

        // Get the lambda parameter names from multi-param lambda: {l, r | ...}
        LambdaExpression condLambda = join.condition();
        List<String> params = condLambda.parameters();

        // Build context with row bindings for both left and right parameters
        // If context is null (top-level call), create a fresh context
        CompilationContext joinContext = context != null ? context
                : new CompilationContext(null, null, null, null, false);
        if (params.size() >= 1) {
            joinContext = joinContext.withRowBinding(params.get(0), leftAlias);
        }
        if (params.size() >= 2) {
            joinContext = joinContext.withRowBinding(params.get(1), rightAlias);
        }

        // Compile the join condition using the enriched context
        Expression condition = compileToSqlExpression(condLambda.body(), joinContext);

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
     * Compiles an asOfJoin expression into a JoinNode with ASOF_LEFT type.
     * 
     * asOfJoin is a temporal join that finds the closest matching row from the
     * right
     * where the temporal condition is satisfied.
     * 
     * Example Pure: ->asOfJoin(other, {x,y | $x.time > $y.time})
     * DuckDB SQL: left_src ASOF LEFT JOIN right_src ON left_src.time >
     * right_src.time
     * 
     * With optional key match:
     * Example: ->asOfJoin(other, {x,y | $x.time > $y.time}, {x,y | $x.key ==
     * $y.key})
     * DuckDB SQL: left_src ASOF LEFT JOIN right_src ON left_src.key = right_src.key
     * AND left_src.time > right_src.time
     *
     * @param asOfJoin The asOfJoin expression
     * @param context  Compilation context
     * @return The compiled JoinNode with ASOF_LEFT type
     */
    private RelationNode compileAsOfJoin(AsOfJoinExpression asOfJoin, CompilationContext context) {
        // Compile left and right relations
        RelationNode left = compileExpression(asOfJoin.left(), context);
        RelationNode right = compileExpression(asOfJoin.right(), context);

        // Use consistent aliases
        String leftAlias = "left_src";
        String rightAlias = "right_src";

        // Get lambda parameters from match condition
        LambdaExpression matchLambda = asOfJoin.matchCondition();
        List<String> params = matchLambda.parameters();

        // Build context with row bindings for both parameters
        CompilationContext joinContext = context != null ? context
                : new CompilationContext(null, null, null, null, false);
        if (params.size() >= 1) {
            joinContext = joinContext.withRowBinding(params.get(0), leftAlias);
        }
        if (params.size() >= 2) {
            joinContext = joinContext.withRowBinding(params.get(1), rightAlias);
        }

        // Compile the match condition
        Expression matchCondition = compileToSqlExpression(matchLambda.body(), joinContext);

        // If there's a key condition, combine with AND
        Expression finalCondition;
        if (asOfJoin.hasKeyCondition()) {
            // Compile key condition with same bindings
            LambdaExpression keyLambda = asOfJoin.keyCondition();
            Expression keyCondition = compileToSqlExpression(keyLambda.body(), joinContext);

            // DuckDB requires: key condition AND time condition
            // So: ON left.key = right.key AND left.time > right.time
            finalCondition = LogicalExpression.and(keyCondition, matchCondition);
        } else {
            finalCondition = matchCondition;
        }

        return new JoinNode(left, right, finalCondition, JoinNode.JoinType.ASOF_LEFT);
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
        ColumnReference arrayCol = ColumnReference.of(columnName, Primitive.ANY);

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
     * Compiles pivot() to a PivotNode.
     * Pure: relation->pivot(~[pivotCols], ~[aggName : x | $x.col : y | $y->plus()])
     * SQL: PIVOT source ON pivotCol USING SUM(valueCol) AS aggName
     */
    private RelationNode compilePivot(PivotExpression pivot, CompilationContext context) {
        RelationNode source = compileExpression(pivot.source(), context);

        // Convert string pivot columns
        List<String> pivotColumns = pivot.pivotColumns();

        // Convert aggregate specs - preserve column vs expression distinction
        List<PivotNode.AggregateSpec> aggregates = pivot.aggregates().stream()
                .map(agg -> agg.isColumnBased()
                        ? PivotNode.AggregateSpec.column(agg.name(), agg.valueColumn(), agg.aggFunction())
                        : PivotNode.AggregateSpec.expression(agg.name(), agg.valueExpression(), agg.aggFunction()))
                .toList();

        // Convert static values if present
        List<String> staticValues = pivot.staticValues().stream()
                .map(String::valueOf)
                .toList();

        if (pivot.isStatic()) {
            return PivotNode.withValues(source, pivotColumns, staticValues, aggregates);
        } else {
            return PivotNode.dynamic(source, pivotColumns, aggregates);
        }
    }

    /**
     * Compiles write() on a Relation.
     * For PCT testing, this generates SELECT COUNT(*) FROM (source).
     */
    private RelationNode compileRelationWrite(RelationWriteExpression write, CompilationContext context) {
        RelationNode sourceNode = compileExpression(write.source(), context);
        return new WriteNode(sourceNode);
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
            case FunctionCall method -> extractColumnName(method.source());
            default -> throw new PureCompileException("Cannot extract column name from: " + expr);
        };
    }

    /**
     * Extracts sort direction from expression (defaults to ASC).
     * Handles MethodCall with .desc()/.descending() or .asc()/.ascending() suffix.
     */
    private SortNode.SortDirection extractSortDirection(PureExpression expr) {
        if (expr instanceof FunctionCall method) {
            String methodName = method.functionName().toLowerCase();
            if ("desc".equals(methodName) || "descending".equals(methodName)) {
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

        // Also check for MethodCall-style over() as first column (e.g., ~grp->over([]))
        // BUT only if there are additional ColumnSpec columns after it.
        // If the MethodCall with over() is the only column, it should be processed
        // directly.
        FunctionCall overMethodCall = null;
        if (columns.size() > 1 && columns.get(0) instanceof FunctionCall mc && "over".equals(mc.functionName())) {
            // Check if the second column is a ColumnSpec (legend-engine style with separate
            // over + aggregate)
            if (columns.get(1) instanceof ColumnSpec) {
                overMethodCall = mc;
                columns = columns.subList(1, columns.size()); // Skip the over() for column processing
            }
        }

        for (PureExpression colExpr : columns) {
            if (colExpr instanceof FunctionCall overCall && "over".equals(overCall.functionName())) {
                // Old style: MethodCall with over()
                ExtendNode.WindowProjection projection = compileWindowProjection(overCall);
                projections.add(projection);
            } else if (colExpr instanceof ColumnSpec colSpec && overFunctionCall != null) {
                // Legend-engine style: over() + ColumnSpec with aggregation
                ExtendNode.WindowProjection projection = compileWindowProjectionFromFunctionCall(overFunctionCall,
                        colSpec);
                projections.add(projection);
            } else if (colExpr instanceof ColumnSpec colSpec && overMethodCall != null) {
                // Legend-engine style with MethodCall: ~grp->over([]) + ~newCol:{...}
                ExtendNode.WindowProjection projection = compileWindowProjectionFromMethodCall(overMethodCall,
                        colSpec);
                projections.add(projection);
            } else if (colExpr instanceof ColumnSpec colSpec && colSpec.lambda() instanceof FunctionCall mc
                    && "over".equals(mc.functionName())) {
                // Handle: ~prevSalary : lag(~salary, 1)->over(...) where lambda is the over()
                // call
                ExtendNode.WindowProjection projection = compileWindowProjectionWithAlias(colSpec.name(), mc);
                projections.add(projection);
            } else if (colExpr instanceof ColumnSpec colSpec && colSpec.extraFunction() instanceof FunctionCall mc
                    && "over".equals(mc.functionName())) {
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
    private ExtendNode.WindowProjection compileWindowProjection(FunctionCall overCall) {
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
        } else if (source instanceof FunctionCall funcCall) {
            // Standalone function call like percentRank()->over(...) or ntile(4)->over(...)
            functionName = funcCall.functionName();
            newColumnName = functionName; // Use function name as column name if not aliased

            // Extract arguments for functions like ntile(4)
            if (!funcCall.arguments().isEmpty()) {
                PureExpression firstArg = funcCall.arguments().get(0);
                if ("ntile".equals(functionName) && firstArg instanceof IntegerLiteral lit) {
                    offset = lit.value().intValue();
                }
            }
        } else if (source instanceof FunctionCall funcCall) {
            // Nested method call like sum(~val)->over(...) or lag(~col, 1)->over(...)
            functionName = funcCall.functionName();

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
            } else if (arg instanceof FunctionCall mc) {
                if ("desc".equals(mc.functionName()) || "asc".equals(mc.functionName())) {
                    // Sort column with direction
                    String colName = extractColumnName(mc.source());
                    WindowExpression.SortDirection dir = "desc".equals(mc.functionName())
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
    private ExtendNode.WindowProjection compileWindowProjectionWithAlias(String alias, FunctionCall overCall) {
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
        } else if (source instanceof FunctionCall mc) {
            functionName = mc.functionName();
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
            } else if (arg instanceof FunctionCall mc) {
                if ("desc".equals(mc.functionName()) || "asc".equals(mc.functionName())) {
                    String colName = extractColumnName(mc.source());
                    WindowExpression.SortDirection dir = "desc".equals(mc.functionName())
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
            } else if (arg instanceof FunctionCall mc) {
                String methodName = mc.functionName();
                if ("ascending".equals(methodName) || "descending".equals(methodName)
                        || "asc".equals(methodName) || "desc".equals(methodName)) {
                    String colName = extractColumnName(mc.source());
                    WindowExpression.SortDirection dir = ("descending".equals(methodName) || "desc".equals(methodName))
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
        String functionName = null; // Must be explicitly set
        String aggregateColumn = null;
        Integer offset = null;

        if (colSpec.lambda() instanceof LambdaExpression lambda) {
            PureExpression body = lambda.body();

            // Check if body is a property access on a method call: $p->lag($r).salary or
            // $p->nth($w,$r,2).id
            if (body instanceof PropertyAccessExpression pa) {
                if (pa.source() instanceof FunctionCall mc) {
                    functionName = mc.functionName();
                    aggregateColumn = pa.propertyName();
                    // Extract offset for nth() - it's the last integer argument
                    // Pattern: $p->nth($w, $r, 2).id - args are [$w, $r, 2]
                    for (PureExpression arg : mc.arguments()) {
                        if (arg instanceof LiteralExpr lit && lit.type() == LiteralExpr.LiteralType.INTEGER) {
                            offset = ((Number) lit.value()).intValue();
                        } else if (arg instanceof IntegerLiteral lit) {
                            offset = lit.value().intValue();
                        }
                    }
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
            else if (body instanceof FunctionCall mc) {
                functionName = mc.functionName();
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
            if (extraLambda.body() instanceof FunctionCall mc) {
                functionName = mapAggregateMethodToFunction(mc.functionName());
            }
        }

        // Validate that we determined a function
        if (functionName == null) {
            throw new PureParseException("Unable to determine window function from column spec: " + colSpec.name()
                    + ". Specify explicit function (sum, avg, count, rank, etc.)");
        }

        // Create the WindowExpression
        WindowExpression.WindowFunction windowFunc = mapWindowFunction(functionName);
        WindowExpression windowExpr;

        if (offset != null && "ntile".equalsIgnoreCase(functionName)) {
            // NTILE with bucket count
            windowExpr = WindowExpression.ntile(offset, partitionColumns, orderBy);
        } else if (offset != null && aggregateColumn != null
                && ("nth".equalsIgnoreCase(functionName) || "nth_value".equalsIgnoreCase(functionName))) {
            // NTH_VALUE(column, n) - needs both aggregateColumn and offset
            windowExpr = WindowExpression.lagLead(windowFunc, aggregateColumn, offset, partitionColumns, orderBy,
                    frame);
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
     * Compiles a window projection from a MethodCall-style over() specification.
     * 
     * Input format: MethodCall[source=ColumnSpec[name=grp], methodName=over,
     * arguments=[]]
     * where the source ColumnSpec's name is the partition column.
     * 
     * The colSpec contains the new column name and aggregate function in its
     * extraFunction.
     */
    private ExtendNode.WindowProjection compileWindowProjectionFromMethodCall(FunctionCall overCall,
            ColumnSpec colSpec) {
        // Extract partition column from the source of over()
        List<String> partitionColumns = new ArrayList<>();
        List<WindowExpression.SortSpec> orderBy = new ArrayList<>();
        WindowExpression.FrameSpec frame = null;

        if (overCall.source() instanceof ColumnSpec partitionSpec) {
            partitionColumns.add(partitionSpec.name());
        }

        // Process over() arguments for additional partition/order specs
        for (PureExpression arg : overCall.arguments()) {
            if (arg instanceof ColumnSpec cs) {
                partitionColumns.add(cs.name());
            } else if (arg instanceof FunctionCall mc) {
                String methodName = mc.functionName();
                if ("ascending".equals(methodName) || "descending".equals(methodName)
                        || "asc".equals(methodName) || "desc".equals(methodName)) {
                    String colName = extractColumnName(mc.source());
                    WindowExpression.SortDirection dir = ("descending".equals(methodName) || "desc".equals(methodName))
                            ? WindowExpression.SortDirection.DESC
                            : WindowExpression.SortDirection.ASC;
                    orderBy.add(new WindowExpression.SortSpec(colName, dir));
                }
            }
        }

        // Get new column name
        String newColumnName = colSpec.name();

        // Extract function, aggregate column, and offset from lambda
        // Pattern 1: {p,w,r|$r.id} -> simple property access for aggregates
        // Pattern 2: {p,w,r|$p->nth($w,$r,2).id} -> method call with property access
        // for nth()
        String aggregateColumn = null;
        String functionName = null; // Must be explicitly set
        Integer offset = null;

        if (colSpec.lambda() instanceof LambdaExpression lambda) {
            PureExpression body = lambda.body();

            if (body instanceof PropertyAccessExpression pa) {
                aggregateColumn = pa.propertyName();

                // Check if this is a method call with property access: $p->nth($w,$r,2).id
                if (pa.source() instanceof FunctionCall mc) {
                    functionName = mc.functionName();
                    // Extract offset for nth() - it's the last integer argument
                    for (PureExpression arg : mc.arguments()) {
                        if (arg instanceof LiteralExpr lit && lit.type() == LiteralExpr.LiteralType.INTEGER) {
                            offset = ((Number) lit.value()).intValue();
                        } else if (arg instanceof IntegerLiteral lit) {
                            offset = lit.value().intValue();
                        }
                    }
                }
            }
        }

        // Extract window function from extraFunction (e.g., y|$y->size() = count)
        if (colSpec.extraFunction() instanceof LambdaExpression extraLambda) {
            if (extraLambda.body() instanceof FunctionCall mc) {
                functionName = mapAggregateMethodToFunction(mc.functionName());
            } else if (extraLambda.body() instanceof UnaryExpression unary) {
                // Handle unary +$y which means sum
                if ("+".equals(unary.operator())) {
                    functionName = "sum";
                }
            }
        }

        // Validate that we determined a function
        if (functionName == null) {
            throw new PureParseException("Unable to determine window function from column spec: " + colSpec.name()
                    + ". Specify explicit function (sum, avg, count, rank, etc.)");
        }

        // Create the WindowExpression
        WindowExpression.WindowFunction windowFunc = mapWindowFunction(functionName);
        WindowExpression windowExpr;

        if (offset != null && ("nth".equalsIgnoreCase(functionName) || "nth_value".equalsIgnoreCase(functionName))) {
            // NTH_VALUE(column, n)
            windowExpr = WindowExpression.lagLead(windowFunc, aggregateColumn, offset, partitionColumns, orderBy,
                    frame);
        } else if (aggregateColumn != null && isValueFunction(functionName)) {
            // LAG, LEAD, FIRST_VALUE, LAST_VALUE
            windowExpr = WindowExpression.lagLead(windowFunc, aggregateColumn, 1, partitionColumns, orderBy, frame);
        } else if (aggregateColumn != null) {
            // Aggregate window function (SUM, COUNT, etc.)
            windowExpr = WindowExpression.aggregate(windowFunc, aggregateColumn, partitionColumns, orderBy, frame);
        } else {
            // Ranking function
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
     * Parses a frame bound: unbounded(), 0 (current row), or number
     * (preceding/following).
     * Supports decimal bounds for RANGE frames (e.g., 0.5D->_range(2.5)).
     */
    private WindowExpression.FrameBound parseFrameBound(PureExpression expr) {
        if (expr instanceof FunctionCall fc && "unbounded".equals(fc.functionName())) {
            return WindowExpression.FrameBound.unbounded();
        } else if (expr instanceof LiteralExpr lit && lit.value() instanceof Number num) {
            double value = num.doubleValue();
            if (value == 0) {
                return WindowExpression.FrameBound.currentRow();
            } else if (value < 0) {
                return WindowExpression.FrameBound.preceding(-value); // Convert to positive offset
            } else {
                return WindowExpression.FrameBound.following(value);
            }
        } else if (expr instanceof UnaryExpression unary && "-".equals(unary.operator())) {
            // Handle negative literals like -1, -2, -0.5
            if (unary.operand() instanceof LiteralExpr lit && lit.value() instanceof Number num) {
                double value = num.doubleValue();
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
     * Compiles a TDS literal: #TDS col1, col2 val1, val2 #
     *
     * TDS literals are inline tabular data that become a VALUES clause in SQL.
     */
    private RelationNode compileTdsLiteral(TdsLiteral tds) {
        // Convert AST columns to IR columns, preserving types
        List<TdsLiteralNode.TdsColumn> irColumns = tds.columns().stream()
                .map(c -> TdsLiteralNode.TdsColumn.of(c.name(), c.type()))
                .toList();
        return new TdsLiteralNode(irColumns, tds.rows());
    }

    /**
     * Checks if an ArrayLiteral contains InstanceExpressions; (^Class(...)
     * instances).
     */
    private boolean isInstanceArray(ArrayLiteral array) {
        return !array.elements().isEmpty() &&
                array.elements().get(0) instanceof InstanceExpression;
    }

    private boolean isEmptyArrayLiteral(PureExpression expr) {
        return expr instanceof ArrayLiteral arr && arr.elements().isEmpty();
    }

    /**
     * Compiles an array of instance expressions into a StructLiteralNode.
     * 
     * Pure: [^Person(firstName='John', age=30), ^Person(firstName='Jane', age=25)]
     * SQL: SELECT * FROM (VALUES ({firstName: 'John', age: 30}), ({firstName:
     * 'Jane', age: 25})) AS t(person)
     */
    private RelationNode compileInstanceArray(ArrayLiteral array) {
        if (array.elements().isEmpty()) {
            throw new PureCompileException("Instance array cannot be empty");
        }

        // Get class name from first instance
        InstanceExpression first = (InstanceExpression) array.elements().get(0);
        String className = first.className();

        // Convert each InstanceExpression to StructInstance
        List<StructInstance> instances = new ArrayList<>();
        for (PureExpression elem : array.elements()) {
            if (elem instanceof InstanceExpression inst) {
                instances.add(toStructInstance(inst));
            } else {
                throw new PureCompileException("Expected InstanceExpression in array, got: " + elem);
            }
        }

        // Normalize types across instances - if ANY instance has a List for a property,
        // ALL instances should have Lists for that property (for DuckDB type
        // consistency)
        instances = normalizeStructTypes(instances);

        return new StructLiteralNode(className, instances);
    }

    /**
     * Compiles a single InstanceExpression (e.g., ^ClassName(prop=value)) to a
     * relation.
     * This is treated as a single-row relation with struct columns.
     */
    private RelationNode compileSingleInstance(InstanceExpression inst) {
        String className = inst.className();
        StructInstance structInstance = toStructInstance(inst);
        List<StructInstance> instances = List.of(structInstance);

        // Still need to normalize in case properties have empty arrays
        instances = normalizeStructTypes(new ArrayList<>(instances));

        return new StructLiteralNode(className, instances);
    }

    /**
     * Normalizes struct types across all instances to ensure consistent DuckDB
     * types.
     * If any instance has a List for a property, all instances get Lists for that
     * property.
     */
    private List<StructInstance> normalizeStructTypes(List<StructInstance> instances) {
        if (instances.isEmpty())
            return instances;

        // Find properties that have List values in any instance
        Set<String> listProperties = new HashSet<>();
        for (StructInstance inst : instances) {
            for (var entry : inst.fields().entrySet()) {
                if (entry.getValue() instanceof List<?>) {
                    listProperties.add(entry.getKey());
                }
            }
        }

        if (listProperties.isEmpty())
            return instances;

        // Normalize: wrap single values as single-element Lists for list properties
        List<StructInstance> normalized = new ArrayList<>();
        for (StructInstance inst : instances) {
            Map<String, Object> normalizedFields = new LinkedHashMap<>();
            for (var entry : inst.fields().entrySet()) {
                String key = entry.getKey();
                Object value = entry.getValue();

                if (listProperties.contains(key) && !(value instanceof List<?>)) {
                    // Wrap single value as single-element List
                    normalizedFields.put(key, List.of(value));
                } else {
                    normalizedFields.put(key, value);
                }
            }
            normalized.add(new StructInstance(normalizedFields));
        }
        return normalized;
    }

    /**
     * Converts an InstanceExpression to a StructInstance for SQL generation.
     * Handles nested objects and collections recursively.
     */
    private StructInstance toStructInstance(InstanceExpression expr) {
        Map<String, Object> fields = new LinkedHashMap<>();

        for (var entry : expr.properties().entrySet()) {
            String key = entry.getKey();
            Object value = entry.getValue();
            fields.put(key, convertInstanceValue(value));
        }

        return new StructInstance(fields);
    }

    /**
     * Converts a property value from an InstanceExpression to a value suitable for
     * StructInstance.
     */
    private Object convertInstanceValue(Object value) {
        if (value == null) {
            return null;
        }
        if (value instanceof InstanceExpression nested) {
            return toStructInstance(nested);
        }
        if (value instanceof ArrayLiteral arr) {
            List<Object> list = new ArrayList<>();
            for (PureExpression elem : arr.elements()) {
                list.add(convertInstanceValue(elem));
            }
            return list;
        }
        if (value instanceof LiteralExpr lit) {
            return lit.value();
        }
        if (value instanceof String || value instanceof Number || value instanceof Boolean) {
            return value;
        }
        // For other expressions, try to extract the value
        if (value instanceof PureExpression) {
            // This might be a more complex expression - stringify for now
            return value.toString();
        }
        return value;
    }

    /**
     * Compiles an InstanceExpression to a StructLiteralExpression IR node.
     * Recursively compiles property values to Expression nodes.
     *
     * Pure: ^Person(firstName='John', age=30)
     * IR:   StructLiteralExpression("Person", {firstName: Literal("John"), age: Literal(30)})
     */
    private Expression compileInstanceExpression(InstanceExpression inst, CompilationContext context) {
        Map<String, Expression> compiledFields = new LinkedHashMap<>();

        // Look up class in TypeEnvironment for multiplicity-aware compilation
        var pureClass = typeEnvironment.findClass(inst.className());

        for (var entry : inst.properties().entrySet()) {
            String key = entry.getKey();
            Object value = entry.getValue();
            Expression compiled = compileInstancePropertyValue(value, context);

            // If property is collection ([*]) but value is a single struct, wrap in array
            // This ensures DuckDB type consistency: all instances have the same struct shape
            if (pureClass.isPresent()) {
                var prop = pureClass.get().findProperty(key);
                if (prop.isPresent() && prop.get().isCollection() && !(compiled instanceof ListLiteral)) {
                    compiled = ListLiteral.of(List.of(compiled));
                }
            }

            compiledFields.put(key, compiled);
        }

        return new StructLiteralExpression(inst.className(), compiledFields);
    }

    /**
     * Compiles a property value from an InstanceExpression to an IR Expression.
     * Handles literals, nested instances, arrays, enums, and variable references.
     */
    private Expression compileInstancePropertyValue(Object value, CompilationContext context) {
        if (value == null) {
            return Literal.nullValue();
        }
        if (value instanceof LiteralExpr lit) {
            return compileLiteral(lit);
        }
        if (value instanceof InstanceExpression nested) {
            return compileInstanceExpression(nested, context);
        }
        if (value instanceof ArrayLiteral arr) {
            List<Expression> elements = arr.elements().stream()
                    .map(e -> compileInstancePropertyValue(e, context))
                    .toList();
            return ListLiteral.of(elements);
        }
        if (value instanceof EnumValueReference enumRef) {
            return Literal.string(enumRef.valueName());
        }
        if (value instanceof VariableExpr var) {
            return compileToSqlExpression(var, context);
        }
        if (value instanceof PureExpression pureExpr) {
            return compileToSqlExpression(pureExpr, context);
        }
        // Raw Java values (from parser)
        if (value instanceof String s) {
            return Literal.string(s);
        }
        if (value instanceof Long l) {
            return Literal.integer(l);
        }
        if (value instanceof Integer i) {
            return Literal.integer(i);
        }
        if (value instanceof Double d) {
            return new Literal(d, Literal.LiteralType.DOUBLE);
        }
        if (value instanceof Number n) {
            return Literal.integer(n.longValue());
        }
        if (value instanceof Boolean b) {
            return Literal.bool(b);
        }
        return Literal.string(value.toString());
    }

    /**
     * Compiles a variable reference in relation context.
     * 
     * This handles cases like {@code $t->select(~col)} where $t is a lambda
     * parameter referring to the input relation. The variable resolves via:
     * 1. Symbol table RELATION bindings (for let-bound relations)
     * 2. Legacy relationSources map
     * 
     * @param var     The variable expression
     * @param context Compilation context with symbol table and lambda parameter
     *                info
     * @return The resolved RelationNode
     * @throws PureCompileException if the variable cannot be resolved
     */
    private RelationNode compileRelationVariable(VariableExpr var, CompilationContext context) {
        // First check symbol table for RELATION bindings (new approach)
        if (context != null && context.hasSymbol(var.name())) {
            SymbolBinding binding = context.lookupSymbol(var.name());
            if (binding.kind() == SymbolBinding.BindingKind.RELATION) {
                // Return the inlined relation node
                return binding.relationNode();
            }
            // ROW variables shouldn't be used as relations - throw error
            throw new PureCompileException(
                    "Variable '$" + var.name() + "' is a " + binding.kind() + " binding, not a RELATION");
        }

        // Legacy: Check if this variable is bound to a relation source in the context
        if (context != null && context.isRelationSource(var.name())) {
            // Return the actual relation node that this variable refers to
            return context.getRelationSource(var.name());
        }

        // Unbound variable - fail early with clear error
        throw new PureCompileException(
                "Unbound relation variable '$" + var.name() + "'. " +
                        "This variable must be bound via a let statement or lambda parameter.");
    }

    /**
     * Compiles a select expression: relation->select(~col1, ~col2)
     * 
     * This projects specific columns from the source Relation.
     */
    private RelationNode compileRelationSelect(RelationSelectExpression select, CompilationContext context) {
        RelationNode source = compileExpression(select.source(), context);

        // Handle select() with no arguments - equivalent to SELECT * (returns all
        // columns)
        if (select.columns().isEmpty()) {
            return source;
        }

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
     * Compiles a relation project expression:
     * #TDS...#->project(~[col:x|$x.col->transform()])
     * 
     * This handles TDS-to-TDS column projections with optional transformations.
     * Unlike class-based project(), this doesn't require a mapping.
     */
    private RelationNode compileRelationProject(RelationProjectExpression project, CompilationContext context) {
        RelationNode source = compileExpression(project.source(), context);

        List<Projection> projections = new ArrayList<>();

        for (int i = 0; i < project.projections().size(); i++) {
            LambdaExpression lambda = project.projections().get(i);
            String alias = i < project.aliases().size() ? project.aliases().get(i)
                    : extractFinalPropertyName(lambda.body());

            // Compile the lambda body as an expression
            // Use empty table alias for unqualified column references (like RelationSelect)
            CompilationContext projContext = new CompilationContext(
                    lambda.parameter(),
                    "", // Empty alias for unqualified column references
                    null, // No mapping needed for relation-based project
                    null, // No class name
                    false).withRowBinding(lambda.parameter(), "");

            Expression expr = compileToSqlExpression(lambda.body(), projContext);
            projections.add(new Projection(expr, alias));
        }

        return new ProjectNode(source, projections);
    }

    /**
     * Compiles an extend expression: relation->extend(~newCol : ...)
     * 
     * Supports both:
     * 1. Simple calculated columns: extend(~newCol : x | $x.col1 + $x.col2)
     * 2. Window functions: extend(over(~dept), ~rowNum:{p,w,r|$p->rowNumber($r)})
     * 3. Window with frame: extend(over(~dept, ~date, rows(unbounded(), 0)),
     * ~sum:{...})
     */
    private RelationNode compileRelationExtend(RelationExtendExpression extend, CompilationContext context) {
        RelationNode source = compileExpression(extend.source(), context);

        if (extend.isWindowFunction()) {
            // Compile window function using typed spec
            RelationExtendExpression.TypedWindowSpec typedSpec = extend.windowSpec();
            WindowFunctionSpec spec = typedSpec.spec();

            // Map sort specs
            List<WindowExpression.SortSpec> orderBy = typedSpec.orderColumns().stream()
                    .map(s -> new WindowExpression.SortSpec(
                            s.column(),
                            s.direction() == RelationExtendExpression.SortDirection.DESC
                                    ? WindowExpression.SortDirection.DESC
                                    : WindowExpression.SortDirection.ASC))
                    .toList();

            // Map frame spec if present
            WindowExpression.FrameSpec frameSpec = null;
            if (typedSpec.hasFrame()) {
                frameSpec = mapFrameSpec(typedSpec.frame());
            }

            // Type-safe pattern matching on sealed WindowFunctionSpec
            WindowExpression windowExpr = switch (spec) {
                case RankingFunctionSpec ranking -> compileRankingWindowFunction(
                        ranking, typedSpec.partitionColumns(), orderBy, frameSpec);
                case ValueFunctionSpec value -> compileValueWindowFunction(
                        value, typedSpec.partitionColumns(), orderBy, frameSpec);
                case AggregateFunctionSpec aggregate -> compileAggregateWindowFunction(
                        aggregate, typedSpec.partitionColumns(), orderBy, frameSpec);
                case PostProcessedWindowFunctionSpec postProcessed -> {
                    // Compile the inner window function first
                    WindowExpression inner = switch (postProcessed.inner()) {
                        case RankingFunctionSpec ranking -> compileRankingWindowFunction(
                                ranking, typedSpec.partitionColumns(), orderBy, frameSpec);
                        case ValueFunctionSpec value -> compileValueWindowFunction(
                                value, typedSpec.partitionColumns(), orderBy, frameSpec);
                        case AggregateFunctionSpec aggregate -> compileAggregateWindowFunction(
                                aggregate, typedSpec.partitionColumns(), orderBy, frameSpec);
                        case PostProcessedWindowFunctionSpec nested ->
                            throw new PureCompileException("Nested post-processors not supported");
                    };
                    // Wrap with post-processor
                    yield new WindowExpression(
                            inner.function(),
                            inner.aggregateColumn(),
                            inner.secondColumn(),
                            inner.partitionBy(),
                            inner.orderBy(),
                            inner.frame(),
                            inner.offset(),
                            new WindowExpression.PostProcessor(
                                    postProcessed.postProcessorFunction(),
                                    postProcessed.postProcessorArgs()),
                            inner.percentileValue());
                }
            };

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
                    extendedColumns,
                    new java.util.HashMap<>(),
                    new java.util.HashMap<>(),
                    new java.util.HashMap<>(),
                    new java.util.HashMap<>())
                    .withRowBinding(extend.expression().parameter(), tableAlias);

            Expression calculatedExpr = compileToSqlExpression(extend.expression().body(), lambdaContext);

            // Create SimpleProjection for the calculated column
            ExtendNode.SimpleProjection simpleProj = new ExtendNode.SimpleProjection(
                    extend.newColumnName(), calculatedExpr);

            return new ExtendNode(source, List.of(simpleProj));
        }
    }

    /**
     * Compiles a ranking window function (row_number, rank, dense_rank, etc.)
     */
    private WindowExpression compileRankingWindowFunction(
            RankingFunctionSpec ranking,
            List<String> partitionColumns,
            List<WindowExpression.SortSpec> orderBy,
            WindowExpression.FrameSpec frameSpec) {

        // Special handling for NTILE which requires bucket count
        if (ranking.function() == RankingFunctionSpec.RankingFunction.NTILE) {
            int buckets = ranking.ntileBuckets() != null ? ranking.ntileBuckets() : 1;
            return WindowExpression.ntile(buckets, partitionColumns, orderBy);
        }

        WindowExpression.WindowFunction windowFunc = switch (ranking.function()) {
            case ROW_NUMBER -> WindowExpression.WindowFunction.ROW_NUMBER;
            case RANK -> WindowExpression.WindowFunction.RANK;
            case DENSE_RANK -> WindowExpression.WindowFunction.DENSE_RANK;
            case PERCENT_RANK -> WindowExpression.WindowFunction.PERCENT_RANK;
            case CUME_DIST -> WindowExpression.WindowFunction.CUME_DIST;
            case NTILE -> WindowExpression.WindowFunction.NTILE; // Won't reach here
        };

        return WindowExpression.ranking(windowFunc, partitionColumns, orderBy, frameSpec);
    }

    /**
     * Compiles a value window function (lag, lead, first_value, etc.)
     */
    private WindowExpression compileValueWindowFunction(
            ValueFunctionSpec value,
            List<String> partitionColumns,
            List<WindowExpression.SortSpec> orderBy,
            WindowExpression.FrameSpec frameSpec) {

        WindowExpression.WindowFunction windowFunc = switch (value.function()) {
            case LAG -> WindowExpression.WindowFunction.LAG;
            case LEAD -> WindowExpression.WindowFunction.LEAD;
            case FIRST_VALUE -> WindowExpression.WindowFunction.FIRST_VALUE;
            case LAST_VALUE -> WindowExpression.WindowFunction.LAST_VALUE;
            case NTH_VALUE -> WindowExpression.WindowFunction.NTH_VALUE;
        };

        int offset = value.offset() != null ? value.offset() : 1;
        // Use frame-aware overload if frame is present
        if (frameSpec != null) {
            return WindowExpression.lagLead(windowFunc, value.column(), offset, partitionColumns, orderBy, frameSpec);
        }
        return WindowExpression.lagLead(windowFunc, value.column(), offset, partitionColumns, orderBy);
    }

    /**
     * Compiles an aggregate window function (sum, avg, count, etc.)
     */
    private WindowExpression compileAggregateWindowFunction(
            AggregateFunctionSpec aggregate,
            List<String> partitionColumns,
            List<WindowExpression.SortSpec> orderBy,
            WindowExpression.FrameSpec frameSpec) {

        WindowExpression.WindowFunction windowFunc = switch (aggregate.function()) {
            case SUM -> WindowExpression.WindowFunction.SUM;
            case AVG -> WindowExpression.WindowFunction.AVG;
            case COUNT -> WindowExpression.WindowFunction.COUNT;
            case MIN -> WindowExpression.WindowFunction.MIN;
            case MAX -> WindowExpression.WindowFunction.MAX;
            case STDDEV -> WindowExpression.WindowFunction.STDDEV;
            case STDDEV_SAMP -> WindowExpression.WindowFunction.STDDEV_SAMP;
            case STDDEV_POP -> WindowExpression.WindowFunction.STDDEV_POP;
            case VARIANCE -> WindowExpression.WindowFunction.VARIANCE;
            case VAR_SAMP -> WindowExpression.WindowFunction.VAR_SAMP;
            case VAR_POP -> WindowExpression.WindowFunction.VAR_POP;
            case MEDIAN -> WindowExpression.WindowFunction.MEDIAN;
            case MODE -> WindowExpression.WindowFunction.MODE;
            case CORR -> WindowExpression.WindowFunction.CORR;
            case COVAR_SAMP -> WindowExpression.WindowFunction.COVAR_SAMP;
            case COVAR_POP -> WindowExpression.WindowFunction.COVAR_POP;
            case PERCENTILE_CONT -> WindowExpression.WindowFunction.QUANTILE_CONT;
            case PERCENTILE_DISC -> WindowExpression.WindowFunction.QUANTILE_DISC;
            case STRING_AGG -> WindowExpression.WindowFunction.STRING_AGG;
            case WAVG -> WindowExpression.WindowFunction.SUM; // WAVG handled specially below
            case ARG_MAX -> WindowExpression.WindowFunction.MAX; // ARG_MAX handled specially below
            case ARG_MIN -> WindowExpression.WindowFunction.MIN; // ARG_MIN handled specially below
        };

        // For percentile functions, pass the percentile value
        if ((windowFunc == WindowExpression.WindowFunction.QUANTILE_CONT
                || windowFunc == WindowExpression.WindowFunction.QUANTILE_DISC)
                && aggregate.percentileValue() != null) {
            return WindowExpression.percentile(windowFunc, aggregate.column(),
                    aggregate.percentileValue(), partitionColumns, orderBy);
        }

        // For bi-variate functions, pass the second column
        if (aggregate.secondColumn() != null) {
            return WindowExpression.bivariate(windowFunc, aggregate.column(), aggregate.secondColumn(),
                    partitionColumns, orderBy);
        }

        return WindowExpression.aggregate(windowFunc, aggregate.column(), partitionColumns, orderBy, frameSpec);
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
            case "nth", "nth_value" -> WindowExpression.WindowFunction.NTH_VALUE;
            case "sum" -> WindowExpression.WindowFunction.SUM;
            case "avg" -> WindowExpression.WindowFunction.AVG;
            case "min" -> WindowExpression.WindowFunction.MIN;
            case "max" -> WindowExpression.WindowFunction.MAX;
            case "count", "size" -> WindowExpression.WindowFunction.COUNT;
            // Statistical functions
            case "stddev", "stddev_number_1__number_1_" -> WindowExpression.WindowFunction.STDDEV;
            case "stddevsample", "stdDevSample" -> WindowExpression.WindowFunction.STDDEV_SAMP;
            case "stddevpopulation", "stdDevPopulation" -> WindowExpression.WindowFunction.STDDEV_POP;
            case "variance" -> WindowExpression.WindowFunction.VARIANCE;
            case "variancesample", "varianceSample" -> WindowExpression.WindowFunction.VAR_SAMP;
            case "variancepopulation", "variancePopulation" -> WindowExpression.WindowFunction.VAR_POP;
            case "median" -> WindowExpression.WindowFunction.MEDIAN;
            case "corr", "correlation" -> WindowExpression.WindowFunction.CORR;
            case "covarsample", "covar_samp" -> WindowExpression.WindowFunction.COVAR_SAMP;
            case "covarpopulation", "covar_pop" -> WindowExpression.WindowFunction.COVAR_POP;
            // Ranking distribution functions
            case "percentrank", "percent_rank" -> WindowExpression.WindowFunction.PERCENT_RANK;
            case "cumulativedistribution", "cume_dist" -> WindowExpression.WindowFunction.CUME_DIST;
            // Percentile functions
            case "percentilecont", "percentile_cont" -> WindowExpression.WindowFunction.QUANTILE_CONT;
            case "percentiledisc", "percentile_disc" -> WindowExpression.WindowFunction.QUANTILE_DISC;
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
                    true).withRowBinding(filterExpr.lambda().parameter(), rootAlias);

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
                ColumnReference.of(targetAlias, rightCol, Primitive.ANY),
                ComparisonExpression.ComparisonOperator.EQUALS,
                ColumnReference.of(sourceAlias, leftCol, Primitive.ANY));

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
                    ColumnReference.of(sourceAlias, leftCol, Primitive.ANY),
                    ComparisonExpression.ComparisonOperator.EQUALS,
                    ColumnReference.of(targetAlias, rightCol, Primitive.ANY));

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
            return Projection.column(targetAlias, columnName, projectionAlias, targetMapping.pureTypeForProperty(finalProperty));
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
            case EnumValueReference enumRef -> Literal.string(enumRef.valueName()); // DurationUnit.DAYS -> 'DAYS'
            case PropertyAccessExpression propAccess -> compilePropertyAccess(propAccess, context);
            case LiteralExpr literal -> compileLiteral(literal);
            case BinaryExpression binary -> compileBinaryExpression(binary, context);

            // ===== TIER 2: USER-DEFINED FUNCTION INLINING =====
            case UserFunctionCallExpression userFn -> {
                // Look up the function definition
                var fn = PureFunctionRegistry.withBuiltins().getFunction(userFn.functionName());
                if (fn.isEmpty()) {
                    throw new PureCompileException("Unknown user function: " + userFn.functionName());
                }
                PureFunctionRegistry.FunctionEntry entry = fn.get();

                // Start with the function body
                String expandedBody = entry.bodySource();

                if (userFn.sourceText() != null) {
                    // Arrow call: first param = receiver, rest = argumentTexts
                    if (!entry.paramNames().isEmpty()) {
                        expandedBody = expandedBody.replace("$" + entry.paramNames().get(0), userFn.sourceText());
                    }
                    for (int i = 1; i < entry.paramNames().size() && i <= userFn.argumentTexts().size(); i++) {
                        String argText = userFn.argumentTexts().get(i - 1);
                        if (argText != null && !argText.isEmpty()) {
                            expandedBody = expandedBody.replace("$" + entry.paramNames().get(i), argText);
                        }
                    }
                } else {
                    // Standalone call: all params come from argumentTexts
                    for (int i = 0; i < entry.paramNames().size() && i < userFn.argumentTexts().size(); i++) {
                        String argText = userFn.argumentTexts().get(i);
                        if (argText != null && !argText.isEmpty()) {
                            expandedBody = expandedBody.replace("$" + entry.paramNames().get(i), argText);
                        }
                    }
                }

                // Re-parse the expanded expression and compile it
                PureExpression inlined = PureParser.parse(expandedBody);
                yield compileToSqlExpression(inlined, context);
            }

            // ===== LET EXPRESSIONS (from inlined function bodies) =====
            case LetExpression let -> {
                // In SQL context, let x = val just evaluates to val
                yield compileToSqlExpression(let.value(), context);
            }
            case BlockExpression block -> {
                // Process let bindings, then compile the result expression
                CompilationContext workingCtx = context != null ? context
                        : new CompilationContext(null, null, null, null, false);
                for (LetExpression let : block.letStatements()) {
                    Expression scalarExpr = compileToSqlExpression(let.value(), workingCtx);
                    workingCtx = workingCtx.withScalarBinding(let.variableName(), scalarExpr);
                }
                yield compileToSqlExpression(block.result(), workingCtx);
            }

            case VariableExpr var -> {
                // Check if this is a lambda parameter (e.g., `i` in map(i | $i->get('price')))
                if (context != null && context.lambdaParameters() != null && context.isLambdaParameter(var.name())) {
                    // Check for parameter expression override (e.g., fold element unwrapping)
                    Expression override = context.paramOverrides().get(var.name());
                    if (override != null) {
                        yield override;
                    }
                    // Lambda parameters become unqualified column references with element type
                    yield new ColumnReference("", var.name(), context.getLambdaParamType(var.name()));
                }
                // Check symbol table for SCALAR bindings (let-bound values)
                if (context != null && context.hasSymbol(var.name())) {
                    SymbolBinding binding = context.lookupSymbol(var.name());
                    if (binding.kind() == SymbolBinding.BindingKind.SCALAR) {
                        yield (Expression) binding.value();
                    }
                }
                throw new PureCompileException("Unexpected variable in expression context: " + var);
            }
            case CastExpression cast -> compileCastExpression(cast, context);
            case FunctionCall funcCall -> funcCall.source() != null
                    ? compileMethodCall(funcCall, context)
                    : compileFunctionCallToSql(funcCall, context);
            case UnaryExpression unary -> {
                // Handle unary expressions like -5, +3
                Expression operand = compileToSqlExpression(unary.operand(), context);
                yield switch (unary.operator()) {
                    case "-" -> ArithmeticExpression.multiply(Literal.integer(-1), operand);
                    case "+" -> operand;
                    case "!" -> FunctionExpression.of("NOT", operand);
                    default -> throw new PureCompileException("Unknown unary operator: " + unary.operator());
                };
            }
            // ArrayLiteral - compile elements and create SQL list
            case ArrayLiteral array -> {
                // Compile ArrayLiteral to DuckDB list literal: [elem1, elem2, ...]
                List<Expression> sqlElements = array.elements().stream()
                        .map(e -> compileToSqlExpression(e, context))
                        .toList();
                // Detect mixed types: if elements have different SqlTypes, wrap each in to_json()
                // This creates a JSON[] (list of JSON scalars) which preserves per-element type info
                if (sqlElements.size() > 1 && hasMixedTypes(sqlElements)) {
                    List<Expression> jsonElements = sqlElements.stream()
                            .map(e -> (Expression) FunctionExpression.of("to_json", e))
                            .toList();
                    yield ListLiteral.of(jsonElements);
                }
                yield ListLiteral.of(sqlElements);
            }
            // Collection operations on ArrayLiterals -> DuckDB list functions
            case DropExpression drop -> {
                Expression list = compileToSqlExpression(drop.source(), context);
                // list_slice(list, n+1) — DuckDB 1-based, drop first n elements
                yield FunctionExpression.of("list_slice", list,
                        Literal.integer(drop.count() + 1),
                        FunctionExpression.of("len", list));
            }
            case LimitExpression limit -> {
                Expression list = compileToSqlExpression(limit.source(), context);
                // list_slice(list, 1, n) — take first n elements
                int n = Math.max(0, limit.count());
                yield FunctionExpression.of("list_slice", list, Literal.integer(1), Literal.integer(n));
            }
            case SliceExpression slice -> {
                Expression list = compileToSqlExpression(slice.source(), context);
                // list_slice(list, start+1, end) — Pure slice is 0-based, DuckDB is 1-based
                int start = Math.max(0, slice.start());
                yield FunctionExpression.of("list_slice", list,
                        Literal.integer(start + 1), Literal.integer(slice.end()));
            }
            case FirstExpression first -> {
                Expression list = compileToSqlExpression(first.source(), context);
                // list[1] — first element (1-based in DuckDB)
                yield FunctionExpression.of("list_extract", list, Literal.integer(1));
            }
            case ConcatenateExpression concat -> {
                Expression left = compileToSqlExpression(concat.left(), context);
                Expression right = compileToSqlExpression(concat.right(), context);
                // If both sides are already JSON[] (mixed-type), concat directly
                // If one or both are homogeneous but different types, promote both to JSON[]
                boolean leftIsJson = isJsonList(left);
                boolean rightIsJson = isJsonList(right);
                if (leftIsJson || rightIsJson) {
                    // At least one side is already JSON[]; wrap the other if needed
                    if (!leftIsJson) left = wrapListToJson(left);
                    if (!rightIsJson) right = wrapListToJson(right);
                    yield FunctionExpression.of("concatenate", left, right);
                }
                // Check if the two sides have different element types (e.g., INTEGER[] vs VARCHAR[])
                if (needsCrossTypeConcatPromotion(left, right)) {
                    left = wrapListToJson(left);
                    right = wrapListToJson(right);
                }
                yield FunctionExpression.of("concatenate", left, right);
            }
            case SortExpression sort when sort.source() instanceof ArrayLiteral -> {
                Expression list = compileToSqlExpression(sort.source(), context);
                // Check if first sortColumn is a single-param lambda (key function)
                if (!sort.sortColumns().isEmpty()
                        && sort.sortColumns().getFirst() instanceof LambdaExpression keyLambda
                        && !keyLambda.isMultiParam()) {
                    String param = keyLambda.parameters().getFirst();
                    GenericType sortElemType = list.type().elementType();
                    CompilationContext keyCtx = context.withLambdaParameter(param, param, sortElemType);
                    Expression keyBody = compileToSqlExpression(keyLambda.body(), keyCtx);
                    java.util.LinkedHashMap<String, Expression> fields = new java.util.LinkedHashMap<>();
                    fields.put("k", keyBody);
                    fields.put("v", new ColumnReference("", param, sortElemType));
                    Expression structExpr = new StructLiteralExpression("_sort", fields);
                    Expression tagged = CollectionExpression.map(list, param, structExpr);
                    Expression sorted = FunctionExpression.of("sort", tagged, detectSortDirection(sort.sortColumns()));
                    String param2 = "_sv";
                    Expression extractVal = FunctionExpression.of("struct_extract", ColumnReference.of(param2, Primitive.ANY), Literal.string("v"));
                    yield CollectionExpression.map(sorted, param2, extractVal);
                }
                yield FunctionExpression.of("sort", list, detectSortDirection(sort.sortColumns()));
            }
            // Relation expressions that reach here need special handling
            case TdsLiteral tds -> {
                // TdsLiteral should be handled by compileExpression, not compileToSqlExpression
                // This likely means aggregation source is being passed here incorrectly
                throw new PureCompileException(
                        "TdsLiteral should be compiled via compileExpression. Got: " + tds);
            }
            case FilterExpression filter -> {
                // Check if this is a collection filter vs relation filter
                // Collection filter: $list->filter(x|predicate) compiles to list_filter(list, x
                // -> condition)
                // We detect collection filter by checking if source is NOT a relation-returning
                // expression
                // For now, assume it's a collection filter if we reach here through
                // compileToSqlExpression
                Expression source = compileToSqlExpression(filter.source(), context);

                // Compile the lambda predicate for list_filter
                // DuckDB syntax: list_filter(array, x -> condition)
                LambdaExpression lambda = filter.predicate();
                String lambdaParam = lambda.parameters().isEmpty() ? "x" : lambda.parameters().get(0);

                // Create a new context with the lambda parameter bound to itself (for direct
                // use in SQL)
                GenericType filterElemType = source.type().elementType();
                CompilationContext lambdaContext = context.withLambdaParameter(lambdaParam, lambdaParam, filterElemType);
                Expression condition = compileToSqlExpression(lambda.body(), lambdaContext);

                // Build list_filter with lambda expression
                yield new ListFilterExpression(source, lambdaParam, condition);
            }
            case SortExpression sort -> {
                // Check if this is a collection sort (no sort columns) vs relation sort
                // Collection sort: $list->sort() has empty sortColumns and non-relation source
                if (sort.sortColumns().isEmpty()) {
                    Expression source = compileToSqlExpression(sort.source(), context);
                    yield FunctionExpression.of("sort", source);
                }
                // Collection sort with comparator: $list->sort({x,y|$y->compare($x)})
                // or sort with key function: $list->sort(keyFn, comparatorFn)
                if (!isRelationExpression(sort.source())) {
                    Expression source = compileToSqlExpression(sort.source(), context);
                    // Check if first sortColumn is a single-param lambda (key function)
                    LambdaExpression keyLambda = null;
                    if (!sort.sortColumns().isEmpty()
                            && sort.sortColumns().getFirst() instanceof LambdaExpression lambda
                            && !lambda.isMultiParam()) {
                        keyLambda = lambda;
                    }
                    if (keyLambda != null) {
                        // sort(keyFn, comparatorFn): sort by key using struct trick
                        // list_transform(list_sort(list_transform(source, x -> {'k': keyFn(x), 'v': x}), dir), x -> struct_extract(x, 'v'))
                        String param = keyLambda.parameters().getFirst();
                        GenericType sortElemType2 = source.type().elementType();
                        CompilationContext keyCtx = context.withLambdaParameter(param, param, sortElemType2);
                        Expression keyBody = compileToSqlExpression(keyLambda.body(), keyCtx);
                        java.util.LinkedHashMap<String, Expression> fields = new java.util.LinkedHashMap<>();
                        fields.put("k", keyBody);
                        fields.put("v", new ColumnReference("", param, sortElemType2));
                        Expression structExpr = new StructLiteralExpression("_sort", fields);
                        Expression tagged = CollectionExpression.map(source, param, structExpr);
                        Expression sorted = FunctionExpression.of("sort", tagged, detectSortDirection(sort.sortColumns()));
                        String param2 = "_sv";
                        Expression extractVal = FunctionExpression.of("struct_extract", ColumnReference.of(param2, Primitive.ANY), Literal.string("v"));
                        yield CollectionExpression.map(sorted, param2, extractVal);
                    }
                    yield FunctionExpression.of("sort", source, detectSortDirection(sort.sortColumns()));
                }
                // Otherwise it's a relation sort that should be handled elsewhere
                throw new PureCompileException(
                        "SortExpression should be compiled via compileExpression. Got: " + sort);
            }
            case GroupByExpression groupBy -> {
                throw new PureCompileException(
                        "GroupByExpression should be compiled via compileExpression. Got: " + groupBy);
            }
            // TypeReference (@Type syntax) - used in variant conversion functions
            case TypeReference typeRef -> {
                // Convert Pure type name to SQL type literal string
                // This is used by functions like toMany(@Integer) for variant conversion
                String sqlType = mapPureTypeToSqlType(typeRef.typeName());
                yield Literal.string(sqlType);
            }
            // InstanceExpression: ^Class(prop=value) -> StructLiteralExpression IR
            case InstanceExpression inst -> compileInstanceExpression(inst, context);
            default -> throw new PureCompileException("Cannot compile to SQL expression: " + expr);
        };
    }

    /**
     * Maps Pure type names to DuckDB SQL type names.
     * Used for variant conversion functions like toMany(@Integer).
     */
    private String mapPureTypeToSqlType(String pureType) {
        // Extract simple type name if fully qualified
        String simpleName = pureType.contains("::")
                ? pureType.substring(pureType.lastIndexOf("::") + 2)
                : pureType;

        return switch (simpleName) {
            case "Integer" -> "BIGINT";
            case "Float" -> "DOUBLE";
            case "String" -> "VARCHAR";
            case "Boolean" -> "BOOLEAN";
            case "Date", "StrictDate" -> "DATE";
            case "DateTime" -> "TIMESTAMP";
            case "Decimal", "Number" -> "DECIMAL";
            case "Variant" -> "JSON";
            default -> simpleName.toUpperCase();
        };
    }

    /**
     * Compiles Pure function calls to SQL, with special handling for
     * functions like greatest(), least(), in() that take array arguments.
     */
    private Expression compileFunctionCallToSql(FunctionCall funcCall, CompilationContext context) {
        String funcName = funcCall.functionName();
        List<PureExpression> args = funcCall.arguments();

        // Extract simple name from fully qualified name (e.g.,
        // meta::pure::functions::collection::reverse -> reverse)
        String simpleName = funcName.contains("::")
                ? funcName.substring(funcName.lastIndexOf("::") + 2)
                : funcName;

        // Special handling for functions that take arrays
        return switch (simpleName) {
            // if(condition, |thenBody, |elseBody) -> CASE WHEN condition THEN thenBody ELSE elseBody END
            case "if" -> compileIfFunction(args, context);
            // eval(lambda, arg1, arg2, ...) -> inline lambda body with args substituted
            case "eval" -> compileEvalFunction(args, context);
            // forAll(list, predicate) -> list_bool_and(list_transform(list, x -> predicate))
            case "forAll" -> compileForAllFunction(args, context);
            // find(list, predicate) -> list_filter(list, x -> predicate)[1]
            case "find" -> compileFindFunction(args, context);
            // removeDuplicatesBy(list, keyFn) -> custom handling
            case "removeDuplicatesBy" -> compileRemoveDuplicatesByFunction(args, context);
            // greatest([a,b,c]) -> list_max([a, b, c]); scalar -> scalar
            case "greatest" -> {
                if (args.isEmpty()) {
                    throw new PureCompileException("greatest() requires at least one argument");
                }
                if (args.getFirst() instanceof ArrayLiteral) {
                    yield FunctionExpression.of("max", compileToSqlExpression(args.getFirst(), context));
                }
                yield compileToSqlExpression(args.getFirst(), context);
            }
            // least([a,b,c]) -> list_min([a, b, c]); scalar -> scalar
            case "least" -> {
                if (args.isEmpty()) {
                    throw new PureCompileException("least() requires at least one argument");
                }
                if (args.getFirst() instanceof ArrayLiteral) {
                    yield FunctionExpression.of("min", compileToSqlExpression(args.getFirst(), context));
                }
                yield compileToSqlExpression(args.getFirst(), context);
            }
            // Variant conversion: toMany(@Type) -> CAST(json_value AS Type[])
            case "toMany" -> {
                if (args.size() < 2) {
                    throw new PureCompileException("toMany() requires source and type arguments");
                }
                Expression source = compileToSqlExpression(args.get(0), context);
                GenericType pureType = Primitive.STRING; // default
                if (args.get(1) instanceof TypeReference typeRef) {
                    pureType = GenericType.fromTypeName(typeRef.typeName());
                }
                yield new org.finos.legend.engine.plan.CastExpression(source, pureType, true);
            }
            // Variant conversion: toVariant() -> to_json(value)
            case "toVariant" -> {
                if (args.isEmpty()) {
                    throw new PureCompileException("toVariant() requires an argument");
                }
                Expression source = compileToSqlExpression(args.getFirst(), context);
                yield FunctionExpression.of("toJson", source);
            }
            // Variant conversion: to(@Type) -> CAST(json_value AS Type) for scalar
            // extraction
            case "to" -> {
                if (args.size() < 2) {
                    throw new PureCompileException("to() requires source and type arguments");
                }
                Expression source = compileToSqlExpression(args.get(0), context);
                GenericType pureType = Primitive.STRING; // default
                if (args.get(1) instanceof TypeReference typeRef) {
                    pureType = GenericType.fromTypeName(typeRef.typeName());
                }
                yield new org.finos.legend.engine.plan.CastExpression(source, pureType);
            }
            // Collection: add(list, element) or add(list, offset, element)
            case "add" -> {
                if (args.size() < 2) {
                    throw new PureCompileException("add() requires list and element arguments");
                }
                Expression source = compileToSqlExpression(args.get(0), context);
                if (args.size() == 2) {
                    // add(list, element) -> list_append(list, element)
                    Expression element = compileToSqlExpression(args.get(1), context);
                    yield FunctionExpression.of("add", source, element);
                }
                // add(list, offset, element) -> splice via list_concat + list_slice
                // Pure offset is 0-based, DuckDB list_slice is 1-based inclusive
                Expression offset = compileToSqlExpression(args.get(1), context);
                Expression element = compileToSqlExpression(args.get(2), context);
                Expression before = FunctionExpression.of("list_slice", source, Literal.integer(1), offset);
                Expression elemList = FunctionExpression.of("list", element);
                Expression afterStart = ArithmeticExpression.add(offset, Literal.integer(1));
                Expression listLen = FunctionExpression.of("length", source);
                Expression after = FunctionExpression.of("list_slice", source, afterStart, listLen);
                yield FunctionExpression.of("concatenate",
                        FunctionExpression.of("concatenate", before, elemList), after);
            }
            // Collection: reverse() -> list_reverse(list)
            case "reverse" -> {
                if (args.isEmpty()) {
                    throw new PureCompileException("reverse() requires an argument");
                }
                Expression source = compileToSqlExpression(args.getFirst(), context);
                yield FunctionExpression.of("reverse", source);
            }
            // Collection: sort() -> list_sort(list)
            case "sort" -> {
                if (args.isEmpty()) {
                    throw new PureCompileException("sort() requires an argument");
                }
                Expression source = compileToSqlExpression(args.getFirst(), context);
                yield FunctionExpression.of("sort", source);
            }
            case "in" -> compilePureFunctionIn(args, context);
            case "toString" -> {
                if (args.isEmpty()) throw new PureCompileException("toString() requires an argument");
                yield new FunctionExpression("cast",
                        compileToSqlExpression(args.getFirst(), context),
                        java.util.List.of(), Primitive.STRING);
            }
            case "parseInteger" -> {
                if (args.isEmpty()) throw new PureCompileException("parseInteger() requires an argument");
                yield new org.finos.legend.engine.plan.CastExpression(
                        compileToSqlExpression(args.getFirst(), context), Primitive.INTEGER);
            }
            case "parseFloat" -> {
                if (args.isEmpty()) throw new PureCompileException("parseFloat() requires an argument");
                yield new org.finos.legend.engine.plan.CastExpression(
                        compileToSqlExpression(args.getFirst(), context), Primitive.FLOAT);
            }
            case "parseDecimal" -> {
                if (args.isEmpty()) throw new PureCompileException("parseDecimal() requires an argument");
                // Strip d/D suffix, cast to DOUBLE for full numeric precision
                // IR type is DECIMAL so adapter creates Pure Decimal
                Expression src = compileToSqlExpression(args.getFirst(), context);
                Expression stripped = FunctionExpression.of("replace", src, Literal.string("[dD]$"), Literal.string(""));
                yield new org.finos.legend.engine.plan.CastExpression(stripped, Primitive.DECIMAL);
            }
            case "parseBoolean" -> {
                if (args.isEmpty()) throw new PureCompileException("parseBoolean() requires an argument");
                yield new org.finos.legend.engine.plan.CastExpression(
                        compileToSqlExpression(args.getFirst(), context), Primitive.BOOLEAN);
            }
            case "char" -> {
                if (args.isEmpty()) throw new PureCompileException("char() requires an argument");
                yield FunctionExpression.of("char", compileToSqlExpression(args.getFirst(), context));
            }
            case "ascii" -> {
                if (args.isEmpty()) throw new PureCompileException("ascii() requires an argument");
                yield FunctionExpression.of("ASCII", compileToSqlExpression(args.getFirst(), context));
            }
            // List aggregate functions (function-call style)
            case "sum", "plus" -> {
                if (args.isEmpty()) throw new PureCompileException("sum() requires an argument");
                if (args.getFirst() instanceof ArrayLiteral) {
                    yield FunctionExpression.of("sum", compileToSqlExpression(args.getFirst(), context));
                }
                yield compileToSqlExpression(args.getFirst(), context);
            }
            case "average", "mean" -> {
                if (args.isEmpty()) throw new PureCompileException("average() requires an argument");
                if (args.getFirst() instanceof ArrayLiteral) {
                    yield FunctionExpression.of("average", compileToSqlExpression(args.getFirst(), context));
                }
                // average/mean always returns Float, so cast scalar to DOUBLE
                yield new org.finos.legend.engine.plan.CastExpression(
                        compileToSqlExpression(args.getFirst(), context), Primitive.FLOAT);
            }
            case "min" -> {
                if (args.isEmpty()) throw new PureCompileException("min() requires an argument");
                if (args.getFirst() instanceof ArrayLiteral) {
                    yield FunctionExpression.of("min", compileToSqlExpression(args.getFirst(), context));
                }
                if (args.size() >= 2) {
                    // min(a, b) -> LEAST(a, b) for scalar comparison
                    yield FunctionExpression.of("least",
                            compileToSqlExpression(args.get(0), context),
                            compileToSqlExpression(args.get(1), context));
                }
                yield FunctionExpression.of("min", compileToSqlExpression(args.getFirst(), context));
            }
            case "max" -> {
                if (args.isEmpty()) throw new PureCompileException("max() requires an argument");
                if (args.getFirst() instanceof ArrayLiteral) {
                    yield FunctionExpression.of("max", compileToSqlExpression(args.getFirst(), context));
                }
                if (args.size() >= 2) {
                    // max(a, b) -> GREATEST(a, b) for scalar comparison
                    yield FunctionExpression.of("greatest",
                            compileToSqlExpression(args.get(0), context),
                            compileToSqlExpression(args.get(1), context));
                }
                yield FunctionExpression.of("max", compileToSqlExpression(args.getFirst(), context));
            }
            case "mode" -> {
                if (args.isEmpty()) throw new PureCompileException("mode() requires an argument");
                if (args.getFirst() instanceof ArrayLiteral) {
                    yield FunctionExpression.of("list_aggr",
                            compileToSqlExpression(args.getFirst(), context), Literal.of("mode"));
                }
                yield FunctionExpression.of("mode", compileToSqlExpression(args.getFirst(), context));
            }
            case "stdDevSample" -> {
                if (args.isEmpty()) throw new PureCompileException("stdDevSample() requires an argument");
                if (args.getFirst() instanceof ArrayLiteral) {
                    yield FunctionExpression.of("list_aggr",
                            compileToSqlExpression(args.getFirst(), context), Literal.of("stddev_samp"));
                }
                yield FunctionExpression.of("stdDevSample", compileToSqlExpression(args.getFirst(), context));
            }
            case "stdDevPopulation" -> {
                if (args.isEmpty()) throw new PureCompileException("stdDevPopulation() requires an argument");
                if (args.getFirst() instanceof ArrayLiteral) {
                    yield FunctionExpression.of("list_aggr",
                            compileToSqlExpression(args.getFirst(), context), Literal.of("stddev_pop"));
                }
                yield FunctionExpression.of("stdDevPopulation", compileToSqlExpression(args.getFirst(), context));
            }
            case "median" -> {
                if (args.isEmpty()) throw new PureCompileException("median() requires an argument");
                if (args.getFirst() instanceof ArrayLiteral) {
                    yield FunctionExpression.of("list_aggr",
                            compileToSqlExpression(args.getFirst(), context), Literal.of("median"));
                }
                yield FunctionExpression.of("median", compileToSqlExpression(args.getFirst(), context));
            }
            case "varianceSample" -> {
                if (args.isEmpty()) throw new PureCompileException("varianceSample() requires an argument");
                yield FunctionExpression.of("list_aggr",
                        compileToSqlExpression(args.getFirst(), context), Literal.of("var_samp"));
            }
            case "variancePopulation" -> {
                if (args.isEmpty()) throw new PureCompileException("variancePopulation() requires an argument");
                yield FunctionExpression.of("list_aggr",
                        compileToSqlExpression(args.getFirst(), context), Literal.of("var_pop"));
            }
            case "covarSample" -> {
                if (args.size() < 2) throw new PureCompileException("covarSample() requires two arguments");
                yield new org.finos.legend.engine.plan.AggregateExpression(
                        org.finos.legend.engine.plan.AggregateExpression.AggregateFunction.COVAR_SAMP,
                        compileToSqlExpression(args.get(0), context),
                        compileToSqlExpression(args.get(1), context));
            }
            case "covarPopulation" -> {
                if (args.size() < 2) throw new PureCompileException("covarPopulation() requires two arguments");
                yield new org.finos.legend.engine.plan.AggregateExpression(
                        org.finos.legend.engine.plan.AggregateExpression.AggregateFunction.COVAR_POP,
                        compileToSqlExpression(args.get(0), context),
                        compileToSqlExpression(args.get(1), context));
            }
            case "corr" -> {
                if (args.size() < 2) throw new PureCompileException("corr() requires two arguments");
                yield new org.finos.legend.engine.plan.AggregateExpression(
                        org.finos.legend.engine.plan.AggregateExpression.AggregateFunction.CORR,
                        compileToSqlExpression(args.get(0), context),
                        compileToSqlExpression(args.get(1), context));
            }
            // and(vals) -> list_bool_and(vals); or(vals) -> list_bool_or(vals)
            case "and" -> {
                if (args.isEmpty()) throw new PureCompileException("and() requires an argument");
                if (args.getFirst() instanceof ArrayLiteral) {
                    yield FunctionExpression.of("and", compileToSqlExpression(args.getFirst(), context));
                }
                yield compileToSqlExpression(args.getFirst(), context);
            }
            case "or" -> {
                if (args.isEmpty()) throw new PureCompileException("or() requires an argument");
                if (args.getFirst() instanceof ArrayLiteral) {
                    yield FunctionExpression.of("or", compileToSqlExpression(args.getFirst(), context));
                }
                yield compileToSqlExpression(args.getFirst(), context);
            }
            // xor(a, b) -> (a AND NOT b) OR (NOT a AND b) — DuckDB xor() is bitwise, not logical
            case "xor" -> {
                if (args.size() < 2) throw new PureCompileException("xor() requires 2 arguments");
                Expression a = compileToSqlExpression(args.get(0), context);
                Expression b = compileToSqlExpression(args.get(1), context);
                yield LogicalExpression.or(
                        LogicalExpression.and(a, LogicalExpression.not(b)),
                        LogicalExpression.and(LogicalExpression.not(a), b));
            }
            // hash(str, hashType) -> md5(str), sha256(str), etc.
            case "hash" -> {
                if (args.size() < 2) throw new PureCompileException("hash() requires string and hash type arguments");
                Expression src = compileToSqlExpression(args.get(0), context);
                // Second arg is the hash type enum (e.g., HashType.MD5)
                String hashType = "";
                PureExpression hashTypeArg = args.get(1);
                if (hashTypeArg instanceof EnumValueReference enumRef) {
                    hashType = enumRef.valueName().toUpperCase();
                } else if (hashTypeArg instanceof VariableExpr v) {
                    hashType = v.name().toUpperCase();
                } else {
                    hashType = hashTypeArg.toString().toUpperCase();
                }
                // Map Pure hash types to DuckDB functions
                if (hashType.contains("SHA256")) {
                    yield FunctionExpression.of("sha256", src);
                } else if (hashType.contains("SHA1")) {
                    yield FunctionExpression.of("sha1", src);
                }
                // Default (MD5 or unknown): use md5
                yield FunctionExpression.of("md5", src);
            }
            // generateGuid() -> uuid()
            case "generateGuid" -> {
                yield FunctionExpression.of("uuid");
            }
            // pi() -> pi()
            case "pi" -> {
                yield FunctionExpression.of("pi");
            }
            // round(x) or round(x, scale) -> round_even for no-scale, round(x, scale) with scale
            case "round" -> {
                if (args.isEmpty()) throw new PureCompileException("round() requires at least 1 argument");
                Expression src = compileToSqlExpression(args.get(0), context);
                if (args.size() == 1) {
                    // round(x) -> CAST(ROUND_EVEN(x, 0) AS BIGINT)
                    yield new org.finos.legend.engine.plan.CastExpression(
                            FunctionExpression.of("round", src, Literal.integer(0)), Primitive.INTEGER);
                } else {
                    // round(x, scale) -> ROUND_EVEN(x, scale)
                    Expression scale = compileToSqlExpression(args.get(1), context);
                    yield FunctionExpression.of("round", src, scale);
                }
            }
            // mod(a, b) -> mod(mod(a, b) + b, b) (Pure mod is always non-negative)
            case "mod" -> {
                if (args.size() < 2) throw new PureCompileException("mod() requires 2 arguments");
                Expression a = compileToSqlExpression(args.get(0), context);
                Expression b = compileToSqlExpression(args.get(1), context);
                Expression innerMod = FunctionExpression.of("mod", a, b);
                Expression addB = ArithmeticExpression.add(innerMod, b);
                yield FunctionExpression.of("mod", addB, b);
            }
            // between(x, low, high) -> x >= low AND x <= high
            case "between" -> {
                if (args.size() < 3) throw new PureCompileException("between() requires 3 arguments: value, low, high");
                Expression value = compileToSqlExpression(args.get(0), context);
                Expression low = compileToSqlExpression(args.get(1), context);
                Expression high = compileToSqlExpression(args.get(2), context);
                yield LogicalExpression.and(
                        new ComparisonExpression(value, ComparisonExpression.ComparisonOperator.GREATER_THAN_OR_EQUALS, low),
                        new ComparisonExpression(value, ComparisonExpression.ComparisonOperator.LESS_THAN_OR_EQUALS, high));
            }
            // Date functions (function-call syntax)
            case "datePart" -> {
                if (args.isEmpty()) throw new PureCompileException("datePart() requires a date argument");
                Expression src = compileToSqlExpression(args.getFirst(), context);
                yield new DateTruncExpression(DateTruncExpression.TruncPart.DAY, src);
            }
            case "adjust" -> {
                // adjust(date, amount, DurationUnit) -> DateAdjustExpression
                if (args.size() < 3) throw new PureCompileException("adjust() requires 3 arguments: date, amount, unit");
                Expression dateExpr = compileToSqlExpression(args.get(0), context);
                Expression amountExpr = compileToSqlExpression(args.get(1), context);
                DurationUnit unit = parseDurationUnit(args.get(2));
                yield new DateAdjustExpression(dateExpr, amountExpr, unit);
            }
            case "parseDate" -> {
                if (args.isEmpty()) throw new PureCompileException("parseDate() requires a string argument");
                Expression src = compileToSqlExpression(args.getFirst(), context);
                if (args.size() == 1) {
                    // No format: generator emits CAST(... AS TIMESTAMPTZ)
                    yield FunctionExpression.of("parseDate", src);
                }
                // With format: generator emits CAST(STRPTIME(s, fmt) AS DATE)
                yield new FunctionExpression("cast",
                        FunctionExpression.of("parseDate", src,
                                compileToSqlExpression(args.get(1), context)),
                        java.util.List.of(), Primitive.STRICT_DATE);
            }
            case "hasMonth", "hasDay" -> {
                if (args.isEmpty()) throw new PureCompileException(simpleName + "() requires a date argument");
                Expression src = compileToSqlExpression(args.getFirst(), context);
                if (src instanceof Literal lit && (lit.literalType() == Literal.LiteralType.DATE || lit.literalType() == Literal.LiteralType.TIMESTAMP)) {
                    String dateStr = (String) lit.value();
                    String stripped = dateStr.startsWith("%") ? dateStr.substring(1) : dateStr;
                    boolean has = simpleName.equals("hasMonth")
                            ? stripped.length() >= 7 : stripped.length() >= 10;
                    yield Literal.bool(has);
                }
                yield Literal.bool(true);
            }
            case "hasHour", "hasMinute", "hasSecond" -> {
                if (args.isEmpty()) throw new PureCompileException(simpleName + "() requires a date argument");
                Expression src = compileToSqlExpression(args.getFirst(), context);
                if (src instanceof Literal lit && (lit.literalType() == Literal.LiteralType.DATE || lit.literalType() == Literal.LiteralType.TIMESTAMP)) {
                    String dateStr = (String) lit.value();
                    String stripped = dateStr.startsWith("%") ? dateStr.substring(1) : dateStr;
                    boolean hasTime = stripped.contains("T");
                    boolean has = switch (simpleName) {
                        case "hasHour" -> hasTime;
                        case "hasMinute" -> hasTime && stripped.indexOf(':', stripped.indexOf('T') + 1) > 0;
                        case "hasSecond" -> {
                            int fc2 = stripped.indexOf(':');
                            yield hasTime && fc2 > 0 && stripped.indexOf(':', fc2 + 1) > 0;
                        }
                        default -> hasTime;
                    };
                    yield Literal.bool(has);
                }
                yield Literal.bool(true);
            }
            // divide(a, b, scale) -> ROUND(a / b, scale)
            // DuckDB's DIVIDE only takes 2 args; Pure's 3-arg divide rounds to scale
            case "divide" -> {
                if (args.size() == 3) {
                    Expression a = compileToSqlExpression(args.get(0), context);
                    Expression b = compileToSqlExpression(args.get(1), context);
                    Expression scale = compileToSqlExpression(args.get(2), context);
                    yield FunctionExpression.of("round", ArithmeticExpression.divide(a, b), scale);
                }
                // 2-arg divide: normal division
                if (args.size() == 2) {
                    yield ArithmeticExpression.divide(
                            compileToSqlExpression(args.get(0), context),
                            compileToSqlExpression(args.get(1), context));
                }
                throw new PureCompileException("divide() requires 2 or 3 arguments");
            }
            // list(collection) -> no-op; Pure's List<T> wrapper has no SQL equivalent
            case "list" -> {
                if (args.isEmpty()) throw new PureCompileException("list() requires an argument");
                yield compileToSqlExpression(args.getFirst(), context);
            }
            default -> {
                // Standard function call: first arg is target, rest are additional
                List<Expression> sqlArgs = args.stream()
                        .map(arg -> compileToSqlExpression(arg, context))
                        .toList();
                if (sqlArgs.isEmpty()) {
                    throw new PureCompileException(
                            "Function call requires at least one argument: " + funcName);
                }
                Expression target = sqlArgs.getFirst();
                if (sqlArgs.size() == 1) {
                    yield FunctionExpression.of(funcName, target);
                } else {
                    Expression[] additionalArgs = sqlArgs.subList(1, sqlArgs.size()).toArray(new Expression[0]);
                    yield FunctionExpression.of(funcName, target, additionalArgs);
                }
            }
        };
    }

    /**
     * Compiles if(condition, |thenBody, |elseBody) to CASE WHEN ... THEN ... ELSE ... END.
     * Handles both simple if and multi-if with pairs: if([pair(|cond1, |val1), ...], |default).
     */
    private Expression compileIfFunction(List<PureExpression> args, CompilationContext context) {
        if (args.isEmpty()) {
            throw new PureCompileException("if() requires at least one argument");
        }

        // Simple if: if(condition, |thenBody, |elseBody)
        if (args.size() >= 3) {
            Expression condition = compileToSqlExpression(args.get(0), context);
            Expression thenVal = compileLambdaBodyOrExpr(args.get(1), context);
            Expression elseVal = compileLambdaBodyOrExpr(args.get(2), context);
            return CaseExpression.of(condition, thenVal, elseVal);
        }

        // Method-style: condition->if(|thenBody, |elseBody) — condition is first arg
        if (args.size() == 2) {
            // This shouldn't happen for function-call style, but handle gracefully
            Expression condition = compileToSqlExpression(args.get(0), context);
            Expression thenVal = compileLambdaBodyOrExpr(args.get(1), context);
            return CaseExpression.of(condition, thenVal, Literal.of("NULL"));
        }

        throw new PureCompileException("if() requires condition and at least one lambda body");
    }

    /**
     * Compiles value->match([a: Type1[1]|result1, b: Type2[1]|result2, ...], extraParams...)
     * Resolves type at compile time for scalar literals, picks the first matching branch.
     * Binds the matched lambda's first param to the source value, extra params to extra args.
     */
    private Expression compileMatchExpression(FunctionCall methodCall, CompilationContext context) {
        if (methodCall.arguments().isEmpty()) {
            throw new PureCompileException("match() requires at least one argument (array of typed lambdas)");
        }

        PureExpression source = methodCall.source();
        PureExpression firstArg = methodCall.arguments().getFirst();
        List<PureExpression> extraArgs = methodCall.arguments().size() > 1
                ? methodCall.arguments().subList(1, methodCall.arguments().size())
                : List.of();

        // First arg must be an ArrayLiteral of typed lambdas
        if (!(firstArg instanceof ArrayLiteral branches)) {
            throw new PureCompileException("match() first argument must be an array of typed lambdas");
        }

        // Detect the source type and size for compile-time resolution
        String sourceType = detectPureType(source);
        int sourceSize = detectSourceSize(source);

        // Find the first matching branch
        for (PureExpression branchExpr : branches.elements()) {
            if (!(branchExpr instanceof LambdaExpression lambda)) {
                throw new PureCompileException("match() branches must be lambda expressions, got: " + branchExpr);
            }

            LambdaExpression.TypeAnnotation typeAnn = (lambda.parameterTypes() != null && !lambda.parameterTypes().isEmpty())
                    ? lambda.parameterTypes().getFirst() : null;

            if (typeAnn == null || (typeMatches(sourceType, typeAnn.simpleTypeName())
                    && multiplicityMatches(sourceSize, typeAnn.multiplicity()))) {
                // This branch matches — compile its body with param bound to source
                Expression sourceExpr = compileToSqlExpression(source, context);
                CompilationContext branchContext = context.withScalarBinding(lambda.parameter(), sourceExpr);

                // Bind extra params: lambda params after the first are bound to extraArgs
                int extraParamCount = lambda.parameters().size() - 1;
                for (int i = 0; i < extraParamCount && i < extraArgs.size(); i++) {
                    Expression extraExpr = compileToSqlExpression(extraArgs.get(i), branchContext);
                    branchContext = branchContext.withScalarBinding(lambda.parameters().get(i + 1), extraExpr);
                }

                return compileToSqlExpression(lambda.body(), branchContext);
            }
        }

        throw new PureCompileException("match() no branch matches source type: " + sourceType);
    }

    /**
     * Detects the Pure type of a source expression for match() compile-time resolution.
     */
    private String detectPureType(PureExpression expr) {
        if (expr instanceof LiteralExpr lit) {
            return switch (lit.type()) {
                case INTEGER -> "Integer";
                case STRING -> "String";
                case FLOAT -> "Float";
                case DECIMAL -> "Decimal";
                case BOOLEAN -> "Boolean";
                case DATE -> "Date";
                case STRICTTIME -> "StrictTime";
            };
        }
        if (expr instanceof ArrayLiteral array) {
            // Detect element type from first element
            if (array.elements().isEmpty()) return "Any";
            return detectPureType(array.elements().getFirst());
        }
        // For cast expressions: []->cast(@String) → CastExpression with targetType
        if (expr instanceof CastExpression cast) {
            String typeName = cast.targetType();
            int lastSep = typeName.lastIndexOf("::");
            return lastSep >= 0 ? typeName.substring(lastSep + 2) : typeName;
        }
        // For instance expressions: ^ClassName(...) → extract class name
        if (expr instanceof InstanceExpression inst) {
            String className = inst.className();
            int lastSep = className.lastIndexOf("::");
            return lastSep >= 0 ? className.substring(lastSep + 2) : className;
        }
        // For method calls that chain through cast
        if (expr instanceof FunctionCall mc && "cast".equals(mc.functionName())) {
            if (!mc.arguments().isEmpty() && mc.arguments().getFirst() instanceof ClassReference cr) {
                String className = cr.className();
                int lastSep = className.lastIndexOf("::");
                return lastSep >= 0 ? className.substring(lastSep + 2) : className;
            }
        }
        return "Any"; // Unknown type — matches Any branches
    }

    /**
     * Detects the source size (number of elements) for multiplicity matching.
     * Returns 1 for scalars, actual count for arrays, -1 for unknown.
     */
    private int detectSourceSize(PureExpression expr) {
        if (expr instanceof ArrayLiteral array) return array.elements().size();
        if (expr instanceof LiteralExpr) return 1;
        if (expr instanceof InstanceExpression) return 1;
        if (expr instanceof CastExpression cast) return detectSourceSize(cast.source());
        return -1; // unknown
    }

    /**
     * Checks if a detected source type matches a branch's type annotation.
     */
    private boolean typeMatches(String sourceType, String branchType) {
        if ("Any".equals(branchType)) return true;
        if ("Any".equals(sourceType)) return true;
        // Number is a supertype of Integer, Float, Decimal
        if ("Number".equals(branchType) && ("Integer".equals(sourceType) || "Float".equals(sourceType) || "Decimal".equals(sourceType))) return true;
        return sourceType.equals(branchType);
    }

    /**
     * Checks if a source size is compatible with a branch's multiplicity annotation.
     * Multiplicity formats: "1", "0..1", "1..4", "*", "0"
     */
    private boolean multiplicityMatches(int sourceSize, String multiplicity) {
        if (sourceSize < 0) return true; // unknown size, assume match
        if ("*".equals(multiplicity)) return true;
        if (multiplicity.contains("..")) {
            String[] parts = multiplicity.split("\\.\\.");
            int lower = Integer.parseInt(parts[0]);
            int upper = "*".equals(parts[1]) ? Integer.MAX_VALUE : Integer.parseInt(parts[1]);
            return sourceSize >= lower && sourceSize <= upper;
        }
        // Exact multiplicity like "1" or "0"
        int exact = Integer.parseInt(multiplicity);
        return sourceSize == exact;
    }

    /**
     * Compiles eval(lambda, arg1, arg2, ...) by inlining the lambda body
     * with parameters substituted by the provided arguments.
     */
    private Expression compileEvalFunction(List<PureExpression> args, CompilationContext context) {
        if (args.isEmpty()) {
            throw new PureCompileException("eval() requires a lambda argument");
        }

        PureExpression first = args.getFirst();
        if (first instanceof LambdaExpression lambda) {
            // Bind lambda parameters to the provided arguments
            CompilationContext lambdaContext = context;
            List<String> params = lambda.parameters();
            for (int i = 0; i < params.size() && i + 1 < args.size(); i++) {
                Expression argExpr = compileToSqlExpression(args.get(i + 1), lambdaContext);
                lambdaContext = lambdaContext.withScalarBinding(params.get(i), argExpr);
            }
            return compileToSqlExpression(lambda.body(), lambdaContext);
        }

        throw new PureCompileException("eval() first argument must be a lambda, got: " + first);
    }

    /**
     * Compiles forAll(list, predicate) to list_bool_and(list_transform(list, x -> predicate)).
     */
    private Expression compileForAllFunction(List<PureExpression> args, CompilationContext context) {
        if (args.size() < 2) {
            throw new PureCompileException("forAll() requires list and predicate arguments");
        }

        Expression source = compileToSqlExpression(args.get(0), context);
        return compileForAllWithLambda(source, args.get(1), context);
    }

    /**
     * Compiles find(list, predicate) to list_filter(list, x -> predicate)[1].
     */
    private Expression compileFindFunction(List<PureExpression> args, CompilationContext context) {
        if (args.size() < 2) {
            throw new PureCompileException("find() requires list and predicate arguments");
        }

        Expression source = compileToSqlExpression(args.get(0), context);
        return compileFindWithLambda(source, args.get(1), context);
    }

    /**
     * Compiles removeDuplicatesBy(list, keyFn) using DuckDB list operations.
     * Strategy: for each element, keep it only if it's the first occurrence of its key.
     */
    private Expression compileRemoveDuplicatesByFunction(List<PureExpression> args, CompilationContext context) {
        if (args.size() < 2) {
            throw new PureCompileException("removeDuplicatesBy() requires list and key function arguments");
        }

        Expression source = compileToSqlExpression(args.get(0), context);

        // DuckDB doesn't have a direct removeDuplicatesBy, so approximate with list_distinct
        // This removes exact duplicates (correct when keyFn is identity/toString)
        return FunctionExpression.of("removeDuplicates", source);
    }

    /**
     * Helper: compile forAll with a lambda predicate on a list source.
     * list_bool_and(list_transform(source, param -> condition))
     */
    private Expression compileForAllWithLambda(Expression source, PureExpression predicate, CompilationContext context) {
        if (predicate instanceof LambdaExpression lambda) {
            String param = lambda.parameters().isEmpty() ? "x" : lambda.parameters().getFirst();
            GenericType elemType = source.type().elementType();
            CompilationContext lambdaContext = context.withLambdaParameter(param, param, elemType);
            Expression condition = compileToSqlExpression(lambda.body(), lambdaContext);

            // COALESCE(list_bool_and(list_transform(source, param -> condition)), true)
            // COALESCE handles empty list case (vacuous truth: forAll on [] = true)
            Expression transformed = CollectionExpression.map(source, param, condition);
            Expression boolAnd = FunctionExpression.of("and", transformed);
            return FunctionExpression.of("COALESCE", boolAnd, new Literal(true, Literal.LiteralType.BOOLEAN));
        }
        throw new PureCompileException("forAll() predicate must be a lambda, got: " + predicate);
    }

    /**
     * Helper: compile find with a lambda predicate on a list source.
     * list_filter(source, param -> condition)[1]
     */
    private Expression compileFindWithLambda(Expression source, PureExpression predicate, CompilationContext context) {
        if (predicate instanceof LambdaExpression lambda) {
            String param = lambda.parameters().isEmpty() ? "x" : lambda.parameters().getFirst();
            GenericType elemType = source.type().elementType();
            CompilationContext lambdaContext = context.withLambdaParameter(param, param, elemType);
            Expression condition = compileToSqlExpression(lambda.body(), lambdaContext);

            // list_filter(source, param -> condition)
            Expression filtered = new ListFilterExpression(source, param, condition);
            // [1] — first matching element
            return FunctionExpression.of("list_extract", filtered, Literal.integer(1));
        }
        throw new PureCompileException("find() predicate must be a lambda, got: " + predicate);
    }

    /**
     * Helper: compile a lambda body or a plain expression.
     * If the arg is a LambdaExpression with zero params, compile the body.
     * Otherwise compile as a normal expression.
     */
    private Expression compileLambdaBodyOrExpr(PureExpression expr, CompilationContext context) {
        if (expr instanceof LambdaExpression lambda) {
            return compileToSqlExpression(lambda.body(), context);
        }
        return compileToSqlExpression(expr, context);
    }

    /**
     * Compiles in(value, [array]) Pure function to SQL: list_contains([array], value)
     * Uses list_contains with JSON[] wrapping for mixed-type lists.
     */
    private Expression compilePureFunctionIn(List<PureExpression> args, CompilationContext context) {
        if (args.size() < 2) {
            throw new PureCompileException("in() requires at least two arguments: value and array");
        }

        // First arg is the value to check
        Expression operand = compileToSqlExpression(args.getFirst(), context);

        // Second arg is the array — compile it as a list expression
        Expression list = compileToSqlExpression(args.get(1), context);

        // If the list is JSON[] (mixed-type), wrap the search element in to_json() too
        if (isJsonList(list)) {
            operand = FunctionExpression.of("to_json", operand);
        }
        return FunctionExpression.of("list_contains", list, operand);
    }

    /**
     * Compiles cast(@Type) to a FunctionExpression with appropriate return type.
     */
    private Expression compileCastExpression(CastExpression cast, CompilationContext context) {
        Expression source = compileToSqlExpression(cast.source(), context);
        GenericType targetType = mapTypeName(cast.targetType());
        return FunctionExpression.of("cast", source, targetType);
    }

    /**
     * Maps Pure type names to PureType.
     */
    private GenericType mapTypeName(String typeName) {
        return switch (typeName.toLowerCase()) {
            case "integer", "int" -> Primitive.INTEGER;
            case "float", "double", "number" -> Primitive.FLOAT;
            case "string", "varchar" -> Primitive.STRING;
            case "boolean", "bool" -> Primitive.BOOLEAN;
            case "date" -> Primitive.DATE;
            case "strictdate" -> Primitive.STRICT_DATE;
            case "datetime" -> Primitive.DATE_TIME;
            case "decimal" -> Primitive.DECIMAL;
            default -> Primitive.ANY;
        };
    }

    /**
     * Compiles a MethodCall (like ->get(), ->fromJson(), ->toJson(), ->map(),
     * ->fold()) to a SQL
     * function call.
     */
    private Expression compileMethodCall(FunctionCall methodCall, CompilationContext context) {
        String methodName = methodCall.functionName();

        // Handle collection functions that take lambdas
        // Also handle fully-qualified Pure paths like
        // meta::pure::functions::collection::greatest
        String shortMethodName = methodName.contains("::")
                ? methodName.substring(methodName.lastIndexOf("::") + 2)
                : methodName;

        return switch (shortMethodName) {
            case "map" -> compileMapCall(methodCall, context);
            case "filter" -> compileFilterCollectionCall(methodCall, context);
            case "exists" -> compileExistsCall(methodCall, context);
            case "fold" -> compileFoldCall(methodCall, context);
            case "flatten" -> compileFlattenCall(methodCall, context);
            case "get" -> compileGetCall(methodCall, context);
            // add: list->add(element) or list->add(offset, element)
            case "add" -> {
                Expression source = compileToSqlExpression(methodCall.source(), context);
                if (methodCall.arguments().isEmpty()) {
                    throw new PureCompileException("add() requires an element argument");
                }
                if (methodCall.arguments().size() == 1) {
                    Expression element = compileToSqlExpression(methodCall.arguments().getFirst(), context);
                    yield FunctionExpression.of("add", source, element);
                }
                // add(offset, element) -> splice via list_concat + list_slice
                Expression offset = compileToSqlExpression(methodCall.arguments().get(0), context);
                Expression element = compileToSqlExpression(methodCall.arguments().get(1), context);
                Expression before = FunctionExpression.of("list_slice", source, Literal.integer(1), offset);
                Expression elemList = FunctionExpression.of("list", element);
                Expression afterStart = ArithmeticExpression.add(offset, Literal.integer(1));
                Expression listLen = FunctionExpression.of("length", source);
                Expression after = FunctionExpression.of("list_slice", source, afterStart, listLen);
                yield FunctionExpression.of("concatenate",
                        FunctionExpression.of("concatenate", before, elemList), after);
            }
            // Pure multiplicity/meta functions - identity in SQL context
            case "toOne", "deactivate" -> compileToSqlExpression(methodCall.source(), context);
            // if: condition->if(|thenBody, |elseBody) -> CASE WHEN condition THEN then ELSE else END
            // multi-if: [pair(|cond1, |val1), pair(|cond2, |val2)]->if(|default)
            //        -> CASE WHEN cond1 THEN val1 WHEN cond2 THEN val2 ELSE default END
            case "if" -> {
                // Multi-if: source is an array of pair(|condition, |value) calls
                if (methodCall.source() instanceof ArrayLiteral arrayLit
                        && !methodCall.arguments().isEmpty()) {
                    boolean allPairs = arrayLit.elements().stream().allMatch(
                            e -> e instanceof FunctionCall fc && "pair".equals(fc.functionName())
                                    && fc.arguments().size() == 2);
                    if (allPairs) {
                        Expression defaultVal = compileLambdaBodyOrExpr(methodCall.arguments().getFirst(), context);
                        // Build nested CaseExpressions from last to first
                        Expression result = defaultVal;
                        for (int i = arrayLit.elements().size() - 1; i >= 0; i--) {
                            FunctionCall pair = (FunctionCall) arrayLit.elements().get(i);
                            Expression cond = compileLambdaBodyOrExpr(pair.arguments().get(0), context);
                            Expression val = compileLambdaBodyOrExpr(pair.arguments().get(1), context);
                            result = CaseExpression.of(cond, val, result);
                        }
                        yield result;
                    }
                }
                Expression condition = compileToSqlExpression(methodCall.source(), context);
                var ifArgs = methodCall.arguments();
                if (ifArgs.size() >= 2) {
                    Expression thenVal = compileLambdaBodyOrExpr(ifArgs.get(0), context);
                    Expression elseVal = compileLambdaBodyOrExpr(ifArgs.get(1), context);
                    yield CaseExpression.of(condition, thenVal, elseVal);
                }
                throw new PureCompileException("if() requires two lambda arguments (then, else)");
            }
            // match: value->match([a: Type1[1]|result1, b: Type2[1]|result2, ...], extraParams...)
            // Resolves type at compile time for scalar literals, picks matching branch
            case "match" -> {
                yield compileMatchExpression(methodCall, context);
            }
            // forAll: list->forAll(e|condition) -> list_bool_and(list_transform(list, e -> condition))
            case "forAll" -> {
                Expression source = compileToSqlExpression(methodCall.source(), context);
                if (methodCall.arguments().isEmpty()) {
                    throw new PureCompileException("forAll() requires a predicate argument");
                }
                yield compileForAllWithLambda(source, methodCall.arguments().getFirst(), context);
            }
            // find: list->find(s|condition) -> list_filter(list, s -> condition)[1]
            case "find" -> {
                Expression source = compileToSqlExpression(methodCall.source(), context);
                if (methodCall.arguments().isEmpty()) {
                    throw new PureCompileException("find() requires a predicate argument");
                }
                yield compileFindWithLambda(source, methodCall.arguments().getFirst(), context);
            }
            // removeDuplicatesBy: list->removeDuplicatesBy(x|key) -> list_distinct(list)
            case "removeDuplicatesBy" -> {
                Expression source = compileToSqlExpression(methodCall.source(), context);
                // Approximate with list_distinct (exact duplicates only)
                yield FunctionExpression.of("removeDuplicates", source);
            }
            // Relation eval() - evaluates a column spec against a row
            // ~columnName->eval($row) compiles to just the column reference
            case "eval" -> {
                if (methodCall.source() instanceof ColumnSpec cs) {
                    // Column spec evaluated on row becomes simple column reference
                    yield ColumnReference.of(cs.name(), Primitive.ANY);
                }
                if (methodCall.source() instanceof ClassReference cr) {
                    // funcRef->eval(args): extract function name and compile as function call
                    String fullName = cr.className();
                    String afterLastColon = fullName.contains("::")
                            ? fullName.substring(fullName.lastIndexOf("::") + 2)
                            : fullName;
                    // Strip type signature: "acos_Number_1__Float_1_" -> "acos"
                    String funcName = afterLastColon.replaceFirst("_[A-Z].*", "");
                    // Build as FunctionCall and compile through existing path
                    var evalArgs = methodCall.arguments();
                    yield compileFunctionCallToSql(new FunctionCall(funcName, evalArgs), context);
                }
                // Lambda source: lambda->eval(args) -> inline lambda body
                if (methodCall.source() instanceof LambdaExpression lambda) {
                    CompilationContext lambdaContext = context;
                    List<String> params = lambda.parameters();
                    var evalArgs = methodCall.arguments();
                    for (int i = 0; i < params.size() && i < evalArgs.size(); i++) {
                        Expression argExpr = compileToSqlExpression(evalArgs.get(i), lambdaContext);
                        lambdaContext = lambdaContext.withScalarBinding(params.get(i), argExpr);
                    }
                    yield compileToSqlExpression(lambda.body(), lambdaContext);
                }
                throw new PureCompileException("eval() requires a column spec, class ref, or lambda source, got: " + methodCall.source());
            }
            // IN expression: $x->in([a, b, c]) -> list_contains([a, b, c], x)
            case "in" -> compileListContains(methodCall, context);
            case "contains" -> {
                // String contains: STRPOS(s, search) > 0; List contains: list_contains(list, value)
                if (methodCall.source() instanceof LiteralExpr lit && lit.type() == LiteralExpr.LiteralType.STRING) {
                    // String source -> string contains
                    if (methodCall.arguments().isEmpty())
                        throw new PureCompileException("contains() requires an argument");
                    yield new ComparisonExpression(
                            FunctionExpression.of("strpos",
                                    compileToSqlExpression(methodCall.source(), context),
                                    compileToSqlExpression(methodCall.arguments().getFirst(), context)),
                            ComparisonExpression.ComparisonOperator.GREATER_THAN, Literal.integer(0));
                }
                if (methodCall.source() instanceof ArrayLiteral) {
                    // List source -> list_contains
                    yield compileListContainsMethod(methodCall, context);
                }
                // Default: try list_contains (may be a variable reference to a list)
                yield compileListContainsMethod(methodCall, context);
            }
            // greatest/least on arrays: [1,2,3]->greatest() -> list_max([1, 2, 3])
            case "greatest" -> {
                if (methodCall.source() instanceof ArrayLiteral) {
                    yield FunctionExpression.of("max", compileToSqlExpression(methodCall.source(), context));
                }
                yield compileToSqlExpression(methodCall.source(), context);
            }
            case "least" -> {
                if (methodCall.source() instanceof ArrayLiteral) {
                    yield FunctionExpression.of("min", compileToSqlExpression(methodCall.source(), context));
                }
                yield compileToSqlExpression(methodCall.source(), context);
            }
            // Type conversion functions
            case "toString" -> {
                // For date/datetime literals, return the original Pure date string directly
                if (methodCall.source() instanceof LiteralExpr lit && lit.type() == LiteralExpr.LiteralType.DATE) {
                    String dateStr = (String) lit.value();
                    String stripped = dateStr.startsWith("%") ? dateStr.substring(1) : dateStr;
                    yield Literal.string(stripped);
                }
                // ClassReference->toString() returns the short class name
                if (methodCall.source() instanceof ClassReference cr) {
                    String className = cr.className();
                    int lastSep = className.lastIndexOf("::");
                    yield Literal.string(lastSep >= 0 ? className.substring(lastSep + 2) : className);
                }
                yield new FunctionExpression("cast",
                        compileToSqlExpression(methodCall.source(), context),
                        java.util.List.of(), Primitive.STRING);
            }

            // ===== STRING FUNCTIONS =====
            case "format" -> { // 'template %s %d'->format([arg1, arg2]) -> format IR node
                Expression formatStr = compileToSqlExpression(methodCall.source(), context);
                var args = methodCall.arguments();

                // Unpack array argument into individual expressions
                List<Expression> argExprs = new java.util.ArrayList<>();
                if (!args.isEmpty()) {
                    PureExpression argExpr = args.get(0);
                    if (argExpr instanceof ArrayLiteral array) {
                        for (PureExpression elem : array.elements()) {
                            argExprs.add(compileToSqlExpression(elem, context));
                        }
                    } else {
                        argExprs.add(compileToSqlExpression(argExpr, context));
                    }
                }

                // Create format function call — SQLGenerator handles dialect-specific translation
                yield new FunctionExpression("format", formatStr, argExprs, Primitive.STRING);
            }
            case "splitPart" -> { // splitPart(s, sep, idx) -> split_part(s, sep, idx+1)
                // Pure splitPart is 0-based, DuckDB split_part is 1-based
                // Empty delimiter: Pure returns whole string, DuckDB splits into chars
                // Empty list [] source: Pure expects [] (empty collection), return NULL
                var args = methodCall.arguments();
                if (args.size() < 2)
                    throw new PureCompileException("splitPart requires 2 arguments");
                if (methodCall.source() instanceof ArrayLiteral arr && arr.elements().isEmpty()) {
                    yield Literal.nullValue();
                }
                Expression src = compileToSqlExpression(methodCall.source(), context);
                Expression delim = compileToSqlExpression(args.get(0), context);
                Expression idx = compileToSqlExpression(args.get(1), context);
                Expression oneBasedIdx = ArithmeticExpression.add(idx, Literal.integer(1));
                Expression splitResult = FunctionExpression.of("splitPart", src, delim, oneBasedIdx);
                // CASE WHEN delim = '' THEN src ELSE split_part(...) END
                yield CaseExpression.of(
                        ComparisonExpression.equals(delim, Literal.string("")),
                        src,
                        splitResult);
            }
            case "toUpperFirstCharacter" -> { // upper(s[1]) || s[2:]
                Expression src = compileToSqlExpression(methodCall.source(), context);
                // DuckDB: upper(s[1]) || s[2:]
                yield new ConcatExpression(java.util.List.of(
                        FunctionExpression.of("toUpper",
                                new FunctionExpression("substring", src, java.util.List.of(Literal.of(1), Literal.of(1)),
                                        Primitive.STRING)),
                        new FunctionExpression("substring", src, java.util.List.of(Literal.of(2)), Primitive.STRING)));
            }
            case "toLowerFirstCharacter" -> { // lower(s[1]) || s[2:]
                Expression src = compileToSqlExpression(methodCall.source(), context);
                yield new ConcatExpression(java.util.List.of(
                        FunctionExpression.of("toLower",
                                new FunctionExpression("substring", src, java.util.List.of(Literal.of(1), Literal.of(1)),
                                        Primitive.STRING)),
                        new FunctionExpression("substring", src, java.util.List.of(Literal.of(2)), Primitive.STRING)));
            }
            case "indexOf" -> { // indexOf on list or string
                if (methodCall.arguments().isEmpty())
                    throw new PureCompileException("indexOf requires an argument");
                Expression src = compileToSqlExpression(methodCall.source(), context);
                Expression arg = compileToSqlExpression(methodCall.arguments().getFirst(), context);
                if (methodCall.source() instanceof ArrayLiteral) {
                    // List indexOf: list_position(list, elem) returns 1-based, subtract 1 for 0-based
                    yield ArithmeticExpression.subtract(
                            FunctionExpression.of("listIndexOf", src, arg),
                            Literal.integer(1));
                }
                // String indexOf: instr(string, search) returns 1-based, subtract 1 for Pure's 0-based
                // indexOf(str, fromIndex) 2-arg form: use instr with offset
                if (methodCall.arguments().size() >= 2) {
                    Expression fromIndex = compileToSqlExpression(methodCall.arguments().get(1), context);
                    // DuckDB doesn't have instr with offset, so use:
                    // fromIndex + instr(substring(src, fromIndex+1), search) - 1
                    Expression subStr = new FunctionExpression("substring", src,
                            java.util.List.of(ArithmeticExpression.add(fromIndex, Literal.integer(1))),
                            Primitive.STRING);
                    yield ArithmeticExpression.subtract(
                            ArithmeticExpression.add(
                                    fromIndex,
                                    FunctionExpression.of("indexOf", subStr, arg)),
                            Literal.integer(1));
                }
                yield ArithmeticExpression.subtract(
                        FunctionExpression.of("indexOf", src, arg),
                        Literal.integer(1));
            }
            case "substring" -> { // substring(start) or substring(start, end) — Pure is 0-based
                Expression src = compileToSqlExpression(methodCall.source(), context);
                if (methodCall.arguments().isEmpty())
                    throw new PureCompileException("substring requires a start argument");
                Expression start = compileToSqlExpression(methodCall.arguments().getFirst(), context);
                // Convert 0-based Pure start to 1-based DuckDB start
                Expression oneBasedStart = ArithmeticExpression.add(start, Literal.integer(1));
                if (methodCall.arguments().size() >= 2) {
                    // substring(start, end) -> SUBSTRING(str, start+1, end-start)
                    Expression end = compileToSqlExpression(methodCall.arguments().get(1), context);
                    Expression length = ArithmeticExpression.subtract(end, start);
                    yield new FunctionExpression("substring", src,
                            java.util.List.of(oneBasedStart, length), Primitive.STRING);
                }
                // substring(start) -> SUBSTRING(str, start+1)
                yield new FunctionExpression("substring", src,
                        java.util.List.of(oneBasedStart), Primitive.STRING);
            }
            case "lpad" -> { // lpad(s, n [, fill]) -> CASE WHEN len(s) >= n THEN left(s,n) ELSE lpad(s,n,fill) END
                if (methodCall.arguments().isEmpty())
                    throw new PureCompileException("lpad requires a length argument");
                Expression src = compileToSqlExpression(methodCall.source(), context);
                Expression len = compileToSqlExpression(methodCall.arguments().getFirst(), context);
                Expression fill = methodCall.arguments().size() >= 2
                        ? compileToSqlExpression(methodCall.arguments().get(1), context)
                        : Literal.string(" ");
                // CASE WHEN length(s) >= n THEN left(s, n)
                //      WHEN length(fill) = 0 THEN s
                //      ELSE lpad(s, n, fill) END
                yield CaseExpression.of(
                        new ComparisonExpression(
                                FunctionExpression.of("length", src),
                                ComparisonExpression.ComparisonOperator.GREATER_THAN_OR_EQUALS, len),
                        FunctionExpression.of("left", src, len),
                        CaseExpression.of(
                                new ComparisonExpression(
                                        FunctionExpression.of("length", fill),
                                        ComparisonExpression.ComparisonOperator.EQUALS, Literal.integer(0)),
                                src,
                                FunctionExpression.of("lpad", src, len, fill)));
            }
            case "rpad" -> { // rpad(s, n [, fill]) -> CASE WHEN len(s) >= n THEN left(s,n) ELSE rpad(s,n,fill) END
                if (methodCall.arguments().isEmpty())
                    throw new PureCompileException("rpad requires a length argument");
                Expression src = compileToSqlExpression(methodCall.source(), context);
                Expression len = compileToSqlExpression(methodCall.arguments().getFirst(), context);
                Expression fill = methodCall.arguments().size() >= 2
                        ? compileToSqlExpression(methodCall.arguments().get(1), context)
                        : Literal.string(" ");
                // CASE WHEN length(s) >= n THEN left(s, n)
                //      WHEN length(fill) = 0 THEN s
                //      ELSE rpad(s, n, fill) END
                yield CaseExpression.of(
                        new ComparisonExpression(
                                FunctionExpression.of("length", src),
                                ComparisonExpression.ComparisonOperator.GREATER_THAN_OR_EQUALS, len),
                        FunctionExpression.of("left", src, len),
                        CaseExpression.of(
                                new ComparisonExpression(
                                        FunctionExpression.of("length", fill),
                                        ComparisonExpression.ComparisonOperator.EQUALS, Literal.integer(0)),
                                src,
                                FunctionExpression.of("rpad", src, len, fill)));
            }
            case "char" -> // char(n) -> chr(n) (DuckDB uses chr, not char)
                FunctionExpression.of("char", compileToSqlExpression(methodCall.source(), context));
            case "ascii" -> // ascii(s) -> ASCII(s)
                FunctionExpression.of("ASCII", compileToSqlExpression(methodCall.source(), context));
            case "parseInteger" -> // parseInteger(s) -> CAST(s AS BIGINT)
                new org.finos.legend.engine.plan.CastExpression(
                        compileToSqlExpression(methodCall.source(), context), Primitive.INTEGER);
            case "parseFloat" -> // parseFloat(s) -> CAST(s AS DOUBLE)
                new org.finos.legend.engine.plan.CastExpression(
                        compileToSqlExpression(methodCall.source(), context), Primitive.FLOAT);
            case "parseDecimal" -> { // parseDecimal(s) -> CAST(regexp_replace(s, '[dD]$', '') AS DECIMAL)
                Expression src = compileToSqlExpression(methodCall.source(), context);
                Expression stripped = FunctionExpression.of("replace", src, Literal.string("[dD]$"), Literal.string(""));
                yield new org.finos.legend.engine.plan.CastExpression(stripped, Primitive.DECIMAL);
            }
            case "parseBoolean" -> // parseBoolean(s) -> CAST(s AS BOOLEAN)
                new org.finos.legend.engine.plan.CastExpression(
                        compileToSqlExpression(methodCall.source(), context), Primitive.BOOLEAN);
            case "repeatString" -> { // repeatString(s, n) -> repeat(s, n)
                if (methodCall.arguments().isEmpty())
                    throw new PureCompileException("repeatString requires a count argument");
                yield FunctionExpression.of("repeat",
                        compileToSqlExpression(methodCall.source(), context),
                        compileToSqlExpression(methodCall.arguments().getFirst(), context));
            }
            case "startsWith" -> { // startsWith(s, prefix) -> starts_with(s, prefix)
                if (methodCall.arguments().isEmpty())
                    throw new PureCompileException("startsWith requires an argument");
                yield FunctionExpression.of("startsWith",
                        compileToSqlExpression(methodCall.source(), context),
                        compileToSqlExpression(methodCall.arguments().getFirst(), context));
            }
            case "endsWith" -> { // endsWith(s, suffix) -> suffix_search(s, suffix)
                if (methodCall.arguments().isEmpty())
                    throw new PureCompileException("endsWith requires an argument");
                yield FunctionExpression.of("endsWith",
                        compileToSqlExpression(methodCall.source(), context),
                        compileToSqlExpression(methodCall.arguments().getFirst(), context));
            }
            case "joinStrings" -> { // [a,b,c]->joinStrings(sep) or joinStrings(prefix, sep, suffix)
                if (methodCall.arguments().isEmpty())
                    throw new PureCompileException("joinStrings requires a separator argument");
                Expression listExpr = compileToSqlExpression(methodCall.source(), context);
                // Pure treats scalars as single-element collections; wrap for ARRAY_TO_STRING
                if (!(listExpr instanceof ListLiteral) && !isListReturningExpression(listExpr)) {
                    listExpr = ListLiteral.of(List.of(listExpr));
                }
                if (methodCall.arguments().size() >= 3) {
                    // 3-arg form: joinStrings(prefix, separator, suffix)
                    Expression prefix = compileToSqlExpression(methodCall.arguments().get(0), context);
                    Expression separator = compileToSqlExpression(methodCall.arguments().get(1), context);
                    Expression suffix = compileToSqlExpression(methodCall.arguments().get(2), context);
                    // COALESCE handles empty lists where ARRAY_TO_STRING returns NULL
                    Expression joined = FunctionExpression.of("coalesce",
                            FunctionExpression.of("joinStrings", listExpr, separator), Literal.of(""));
                    yield new ConcatExpression(java.util.List.of(prefix, joined, suffix));
                }
                // 1-arg form: joinStrings(separator)
                // COALESCE handles empty lists where ARRAY_TO_STRING returns NULL
                yield FunctionExpression.of("coalesce",
                        FunctionExpression.of("joinStrings", listExpr,
                                compileToSqlExpression(methodCall.arguments().getFirst(), context)),
                        Literal.of(""));
            }
            case "decodeBase64" -> { // decodeBase64(s) -> CAST(from_base64(padded_s) AS VARCHAR)
                // Pad base64 string to multiple of 4 with '=' for DuckDB compatibility
                // Strip existing '=' padding first, then re-pad correctly
                Expression src = compileToSqlExpression(methodCall.source(), context);
                // rtrim(s, '=') removes existing padding
                Expression stripped = FunctionExpression.of("rtrim", src, Literal.string("="));
                // CAST((length(stripped) + 3) / 4 AS INTEGER) * 4
                Expression divResult = new org.finos.legend.engine.plan.CastExpression(
                        ArithmeticExpression.divide(
                                ArithmeticExpression.add(FunctionExpression.of("length", stripped), Literal.integer(3)),
                                Literal.integer(4)),
                        Primitive.INTEGER);
                Expression paddedLen = ArithmeticExpression.multiply(divResult, Literal.integer(4));
                Expression padded = FunctionExpression.of("rpad", stripped, paddedLen, Literal.string("="));
                yield new org.finos.legend.engine.plan.CastExpression(
                        FunctionExpression.of("decodeBase64", padded), Primitive.STRING);
            }
            case "encodeBase64" -> // encodeBase64(s) -> encodeBase64(s), generator handles BLOB cast
                FunctionExpression.of("encodeBase64",
                        compileToSqlExpression(methodCall.source(), context), Primitive.STRING);
            case "toLower" -> // toLower(s) -> LOWER(s)
                FunctionExpression.of("toLower", compileToSqlExpression(methodCall.source(), context));
            case "toUpper" -> // toUpper(s) -> UPPER(s)
                FunctionExpression.of("toUpper", compileToSqlExpression(methodCall.source(), context));

            // ===== MATH FUNCTIONS =====
            case "min" -> { // x->min(y) -> LEAST(x, y), or list->min() -> list_min(list)
                if (methodCall.arguments().isEmpty()) {
                    Expression src = compileToSqlExpression(methodCall.source(), context);
                    // Scalar->min() is identity; only use list_min for arrays
                    if (!(methodCall.source() instanceof ArrayLiteral)) { yield src; }
                    yield FunctionExpression.of("min", src);
                }
                yield FunctionExpression.of("least",
                        compileToSqlExpression(methodCall.source(), context),
                        compileToSqlExpression(methodCall.arguments().getFirst(), context));
            }
            case "max" -> { // x->max(y) -> GREATEST(x, y), or list->max() -> list_max(list)
                if (methodCall.arguments().isEmpty()) {
                    Expression src = compileToSqlExpression(methodCall.source(), context);
                    // Scalar->max() is identity; only use list_max for arrays
                    if (!(methodCall.source() instanceof ArrayLiteral)) { yield src; }
                    yield FunctionExpression.of("max", src);
                }
                yield FunctionExpression.of("greatest",
                        compileToSqlExpression(methodCall.source(), context),
                        compileToSqlExpression(methodCall.arguments().getFirst(), context));
            }
            case "round" -> { // round(x) or x->round(scale)
                Expression src = compileToSqlExpression(methodCall.source(), context);
                if (methodCall.arguments().isEmpty()) {
                    // round() with no scale -> round half-even: CAST(ROUND_EVEN(x, 0) AS BIGINT)
                    yield new org.finos.legend.engine.plan.CastExpression(
                            FunctionExpression.of("round", src, Literal.integer(0)), Primitive.INTEGER);
                } else {
                    // round(scale) -> ROUND_EVEN(x, scale)
                    Expression scale = compileToSqlExpression(methodCall.arguments().getFirst(), context);
                    yield FunctionExpression.of("round", src, scale);
                }
            }
            case "rem" -> { // rem(a, b) -> mod(a, b) (can return negative)
                if (methodCall.arguments().isEmpty())
                    throw new PureCompileException("rem requires an argument");
                yield FunctionExpression.of("mod",
                        compileToSqlExpression(methodCall.source(), context),
                        compileToSqlExpression(methodCall.arguments().getFirst(), context));
            }
            case "mod" -> { // mod(a, b) -> mod(mod(a, b) + b, b) (always non-negative)
                if (methodCall.arguments().isEmpty())
                    throw new PureCompileException("mod requires an argument");
                Expression a = compileToSqlExpression(methodCall.source(), context);
                Expression b = compileToSqlExpression(methodCall.arguments().getFirst(), context);
                Expression innerMod = FunctionExpression.of("mod", a, b);
                Expression addB = ArithmeticExpression.add(innerMod, b);
                yield FunctionExpression.of("mod", addB, b);
            }
            case "compare" -> { // compare(a, b) -> CASE WHEN a < b THEN -1 WHEN a > b THEN 1 ELSE 0 END
                if (methodCall.arguments().isEmpty())
                    throw new PureCompileException("compare requires an argument");
                Expression a = compileToSqlExpression(methodCall.source(), context);
                Expression b = compileToSqlExpression(methodCall.arguments().getFirst(), context);
                // Build nested CASE: CASE WHEN a < b THEN -1 ELSE (CASE WHEN a > b THEN 1 ELSE
                // 0 END) END
                yield CaseExpression.of(
                        ComparisonExpression.lessThan(a, b),
                        Literal.of(-1),
                        CaseExpression.of(
                                ComparisonExpression.greaterThan(a, b),
                                Literal.of(1),
                                Literal.of(0)));
            }
            case "eq", "equal" -> { // eq(a, b) -> a = b
                if (methodCall.arguments().isEmpty())
                    throw new PureCompileException("eq requires an argument");
                yield ComparisonExpression.equals(
                        compileToSqlExpression(methodCall.source(), context),
                        compileToSqlExpression(methodCall.arguments().getFirst(), context));
            }
            case "average" -> { // [list]->average() -> list_avg(list); scalar->average() -> CAST(x AS DOUBLE)
                if (methodCall.source() instanceof ArrayLiteral) {
                    yield FunctionExpression.of("average", compileToSqlExpression(methodCall.source(), context));
                }
                // average() always returns Float, so cast scalar to DOUBLE
                yield new org.finos.legend.engine.plan.CastExpression(
                        compileToSqlExpression(methodCall.source(), context), Primitive.FLOAT);
            }
            case "mode" -> { // [list]->mode() -> list_aggr(list, 'mode'); scalar->mode() -> MODE(x)
                if (methodCall.source() instanceof ArrayLiteral) {
                    yield FunctionExpression.of("list_aggr",
                            compileToSqlExpression(methodCall.source(), context),
                            Literal.of("mode"));
                }
                yield FunctionExpression.of("mode", compileToSqlExpression(methodCall.source(), context));
            }
            case "stdDevSample" -> { // [list]->stdDevSample() -> list_aggr(list, 'stddev_samp')
                if (methodCall.source() instanceof ArrayLiteral) {
                    yield FunctionExpression.of("list_aggr",
                            compileToSqlExpression(methodCall.source(), context),
                            Literal.of("stddev_samp"));
                }
                yield FunctionExpression.of("stdDevSample", compileToSqlExpression(methodCall.source(), context));
            }
            case "stdDevPopulation" -> { // [list]->stdDevPopulation()
                if (methodCall.source() instanceof ArrayLiteral) {
                    yield FunctionExpression.of("list_aggr",
                            compileToSqlExpression(methodCall.source(), context),
                            Literal.of("stddev_pop"));
                }
                yield FunctionExpression.of("stdDevPopulation", compileToSqlExpression(methodCall.source(), context));
            }
            case "median" -> { // [list]->median() -> list_aggr(list, 'median')
                if (methodCall.source() instanceof ArrayLiteral) {
                    yield FunctionExpression.of("list_aggr",
                            compileToSqlExpression(methodCall.source(), context),
                            Literal.of("median"));
                }
                yield FunctionExpression.of("median", compileToSqlExpression(methodCall.source(), context));
            }
            case "varianceSample" -> { // [list]->varianceSample() -> list_aggr(list, 'var_samp')
                if (methodCall.source() instanceof ArrayLiteral) {
                    yield FunctionExpression.of("list_aggr",
                            compileToSqlExpression(methodCall.source(), context),
                            Literal.of("var_samp"));
                }
                yield FunctionExpression.of("varianceSample", compileToSqlExpression(methodCall.source(), context));
            }
            case "variancePopulation" -> { // [list]->variancePopulation() -> list_aggr(list, 'var_pop')
                if (methodCall.source() instanceof ArrayLiteral) {
                    yield FunctionExpression.of("list_aggr",
                            compileToSqlExpression(methodCall.source(), context),
                            Literal.of("var_pop"));
                }
                yield FunctionExpression.of("variancePopulation", compileToSqlExpression(methodCall.source(), context));
            }
            case "variance" -> { // [list]->variance(bool) -> var_samp (true) or var_pop (false)
                String varFunc = "varianceSample";
                String duckdbAggr = "var_samp";
                if (!methodCall.arguments().isEmpty()) {
                    PureExpression arg = methodCall.arguments().getFirst();
                    if (arg instanceof BooleanLiteral bl && Boolean.FALSE.equals(bl.value())) {
                        varFunc = "variancePopulation";
                        duckdbAggr = "var_pop";
                    } else if (arg instanceof LiteralExpr lit && lit.type() == LiteralExpr.LiteralType.BOOLEAN
                            && Boolean.FALSE.equals(lit.value())) {
                        varFunc = "variancePopulation";
                        duckdbAggr = "var_pop";
                    }
                }
                if (methodCall.source() instanceof ArrayLiteral) {
                    yield FunctionExpression.of("list_aggr",
                            compileToSqlExpression(methodCall.source(), context),
                            Literal.of(duckdbAggr));
                }
                yield FunctionExpression.of(varFunc, compileToSqlExpression(methodCall.source(), context));
            }
            case "percentileCont", "percentile" -> { // list->percentileCont(0.5) or list->percentile(0.75, true, false)
                if (methodCall.arguments().isEmpty())
                    throw new PureCompileException("percentileCont requires a percentile value argument");
                Expression src = compileToSqlExpression(methodCall.source(), context);
                Expression pValue = compileToSqlExpression(methodCall.arguments().getFirst(), context);
                // percentile(p, ascending, continuous):
                //   2nd arg false = descending -> use (1-p)
                //   3rd arg false = discrete -> use quantile_disc
                boolean ascending = true;
                String quantileFunc = "quantile_cont";
                if (methodCall.arguments().size() >= 2) {
                    PureExpression ascArg = methodCall.arguments().get(1);
                    if (ascArg instanceof LiteralExpr le && Boolean.FALSE.equals(le.value())) {
                        ascending = false;
                    }
                }
                if (methodCall.arguments().size() >= 3) {
                    PureExpression continuousArg = methodCall.arguments().get(2);
                    if (continuousArg instanceof LiteralExpr le && Boolean.FALSE.equals(le.value())) {
                        quantileFunc = "quantile_disc";
                    }
                }
                if (!ascending) {
                    // descending percentile(p) = ascending percentile(1-p)
                    pValue = ArithmeticExpression.subtract(Literal.of(1), pValue);
                }
                yield FunctionExpression.of("list_aggr", src, Literal.of(quantileFunc), pValue);
            }
            case "percentileDisc" -> { // list->percentileDisc(0.5) -> list_aggr(list, 'quantile_disc', p)
                if (methodCall.arguments().isEmpty())
                    throw new PureCompileException("percentileDisc requires a percentile value argument");
                Expression src = compileToSqlExpression(methodCall.source(), context);
                Expression pValue = compileToSqlExpression(methodCall.arguments().getFirst(), context);
                yield FunctionExpression.of("list_aggr", src, Literal.of("quantile_disc"), pValue);
            }
            case "covarSample" -> { // [list1]->covarSample([list2])
                if (methodCall.arguments().isEmpty())
                    throw new PureCompileException("covarSample requires a second list argument");
                Expression list1 = compileToSqlExpression(methodCall.source(), context);
                Expression list2 = compileToSqlExpression(methodCall.arguments().getFirst(), context);
                yield new org.finos.legend.engine.plan.AggregateExpression(
                        org.finos.legend.engine.plan.AggregateExpression.AggregateFunction.COVAR_SAMP, list1, list2);
            }
            case "covarPopulation" -> { // [list1]->covarPopulation([list2])
                if (methodCall.arguments().isEmpty())
                    throw new PureCompileException("covarPopulation requires a second list argument");
                Expression list1 = compileToSqlExpression(methodCall.source(), context);
                Expression list2 = compileToSqlExpression(methodCall.arguments().getFirst(), context);
                yield new org.finos.legend.engine.plan.AggregateExpression(
                        org.finos.legend.engine.plan.AggregateExpression.AggregateFunction.COVAR_POP, list1, list2);
            }
            case "corr" -> { // [list1]->corr([list2])
                if (methodCall.arguments().isEmpty())
                    throw new PureCompileException("corr requires a second list argument");
                Expression list1 = compileToSqlExpression(methodCall.source(), context);
                Expression list2 = compileToSqlExpression(methodCall.arguments().getFirst(), context);
                yield new org.finos.legend.engine.plan.AggregateExpression(
                        org.finos.legend.engine.plan.AggregateExpression.AggregateFunction.CORR, list1, list2);
            }
            case "toFloat" -> // toFloat(x) -> CAST(x AS DOUBLE)
                new FunctionExpression("cast", compileToSqlExpression(methodCall.source(), context),
                        java.util.List.of(), Primitive.FLOAT);
            case "toDecimal" -> // toDecimal(x) -> CAST(x AS DECIMAL)
                new org.finos.legend.engine.plan.CastExpression(
                        compileToSqlExpression(methodCall.source(), context), Primitive.DECIMAL);

            // ===== LIST FUNCTIONS =====
            case "zip" -> { // zip(l1, l2) -> list_transform(generate_series(1, LEAST(LEN(l1), LEN(l2))), i -> {'first': l1[i], 'second': l2[i]})
                if (methodCall.arguments().isEmpty())
                    throw new PureCompileException("zip requires an argument");
                Expression l1 = compileToSqlExpression(methodCall.source(), context);
                Expression l2 = compileToSqlExpression(methodCall.arguments().getFirst(), context);
                // generate_series(1, LEAST(LEN(l1), LEN(l2)))
                Expression range = FunctionExpression.of("generate_series",
                        Literal.of(1),
                        FunctionExpression.of("LEAST",
                                FunctionExpression.of("LEN", l1),
                                FunctionExpression.of("LEN", l2)));
                // {'first': list_extract(l1, i), 'second': list_extract(l2, i)}
                String param = "_zip_i";
                var fields = new java.util.LinkedHashMap<String, Expression>();
                fields.put("first", FunctionExpression.of("list_extract", l1, new ColumnReference("", param, Primitive.INTEGER)));
                fields.put("second", FunctionExpression.of("list_extract", l2, new ColumnReference("", param, Primitive.INTEGER)));
                Expression structExpr = new StructLiteralExpression("Pair", fields);
                yield CollectionExpression.map(range, param, structExpr);
            }
            case "head" -> // head(list) -> list[1]
                FunctionExpression.of("list_extract",
                        compileToSqlExpression(methodCall.source(), context),
                        Literal.of(1));
            case "first" -> // first(list) -> list[1]
                FunctionExpression.of("list_extract",
                        compileToSqlExpression(methodCall.source(), context),
                        Literal.of(1));
            case "last" -> // last(list) -> list[-1]
                FunctionExpression.of("list_extract",
                        compileToSqlExpression(methodCall.source(), context),
                        Literal.of(-1));
            case "tail" -> { // tail(list) -> list_slice(list, 2, len(list))
                // Pure treats scalars as single-element collections;
                // LIST_SLICE on a string does character slicing, so wrap scalars in [x]
                Expression tailSource = compileToSqlExpression(methodCall.source(), context);
                if (!(tailSource instanceof ListLiteral) && !isListReturningExpression(tailSource)) {
                    tailSource = ListLiteral.of(List.of(tailSource));
                }
                yield FunctionExpression.of("list_slice", tailSource, Literal.of(2),
                        FunctionExpression.of("len", tailSource));
            }
            case "init" -> { // init(list) -> list_slice(list, 1, -2) - all but last
                Expression initSource = compileToSqlExpression(methodCall.source(), context);
                if (!(initSource instanceof ListLiteral) && !isListReturningExpression(initSource)) {
                    initSource = ListLiteral.of(List.of(initSource));
                }
                yield FunctionExpression.of("list_slice", initSource, Literal.of(1), Literal.of(-2));
            }
            case "at" -> { // at(list, idx) -> list_extract(list, idx+1)
                // Pure at() is 0-indexed, DuckDB list_extract is 1-indexed
                if (methodCall.arguments().isEmpty())
                    throw new PureCompileException("at requires an index argument");

                // Optimization: at(0) on a scalar struct property (multiplicity [1]) is a no-op.
                // In fold/map over struct lists, $p.lastName->at(0) should just be p.lastName,
                // not LIST_EXTRACT(p.lastName, 1) which would extract a character from the string.
                if (isScalarStructPropertyAccess(methodCall.source(), context)) {
                    yield compileToSqlExpression(methodCall.source(), context);
                }

                Expression index = compileToSqlExpression(methodCall.arguments().getFirst(), context);
                // Add 1 to convert 0-based Pure index to 1-based DuckDB index
                Expression adjustedIndex = ArithmeticExpression.add(index, Literal.integer(1));
                yield FunctionExpression.of("list_extract",
                        compileToSqlExpression(methodCall.source(), context),
                        adjustedIndex);
            }
            // Boolean collection functions: [list]->and() / [list]->or()
            case "and" -> {
                if (methodCall.source() instanceof ArrayLiteral) {
                    yield FunctionExpression.of("and", compileToSqlExpression(methodCall.source(), context));
                }
                // scalar->and() is just the scalar itself
                yield compileToSqlExpression(methodCall.source(), context);
            }
            case "or" -> {
                if (methodCall.source() instanceof ArrayLiteral) {
                    yield FunctionExpression.of("or", compileToSqlExpression(methodCall.source(), context));
                }
                // scalar->or() is just the scalar itself
                yield compileToSqlExpression(methodCall.source(), context);
            }
            // Aggregate functions on arrays -> DuckDB list aggregate functions
            case "sum" -> {
                if (methodCall.source() instanceof ArrayLiteral) {
                    yield FunctionExpression.of("sum", compileToSqlExpression(methodCall.source(), context));
                }
                yield compileToSqlExpression(methodCall.source(), context);
            }
            case "mean" -> {
                if (methodCall.source() instanceof ArrayLiteral) {
                    yield FunctionExpression.of("average", compileToSqlExpression(methodCall.source(), context));
                }
                // mean() always returns Float, so cast scalar to DOUBLE
                yield new org.finos.legend.engine.plan.CastExpression(
                        compileToSqlExpression(methodCall.source(), context), Primitive.FLOAT);
            }

            case "size" -> {
                // Check if source is a relation expression (TdsLiteral, FilterExpression, etc.)
                // If so, compile as COUNT(*) scalar subquery.
                // But lambda/fold parameters and scalar bindings are lists, not relations.
                boolean isScalarVar = methodCall.source() instanceof VariableExpr v
                        && context != null && (context.isLambdaParameter(v.name()) || context.hasScalarBinding(v.name()));
                if (!isScalarVar && isRelationExpression(methodCall.source())) {
                    RelationNode relationNode = compileExpression(methodCall.source(), context);
                    yield new SubqueryExpression(relationNode, FunctionExpression.of("count", Literal.of("*")));
                }
                // Otherwise, it's a list -> len(list)
                yield FunctionExpression.of("len", compileToSqlExpression(methodCall.source(), context));
            }
            case "length" -> // length(s) -> len(s) or length(s)
                FunctionExpression.of("length", compileToSqlExpression(methodCall.source(), context));
            case "removeDuplicates" -> // removeDuplicates(list) -> list_distinct(list)
                FunctionExpression.of("removeDuplicates", compileToSqlExpression(methodCall.source(), context));
            case "distinct" -> // distinct(list) -> list_distinct(list)
                FunctionExpression.of("removeDuplicates", compileToSqlExpression(methodCall.source(), context));
            case "reverse" -> // reverse(list) -> list_reverse(list)
                FunctionExpression.of("reverse", compileToSqlExpression(methodCall.source(), context));
            case "sort" -> { // sort() is parsed as SortExpression; this handles edge cases
                Expression listExpr = compileToSqlExpression(methodCall.source(), context);
                yield FunctionExpression.of("sort", listExpr);
            }

            // ===== DATE FUNCTIONS =====
            case "datePart" -> { // date->datePart() -> DATE_TRUNC('day', date)
                Expression src = compileToSqlExpression(methodCall.source(), context);
                yield new DateTruncExpression(DateTruncExpression.TruncPart.DAY, src);
            }
            case "date" -> { // date(y, m, d [, h, min, sec]) -> make_date or make_timestamp
                var args = methodCall.arguments();
                Expression year = compileToSqlExpression(methodCall.source(), context);
                Expression month = args.size() >= 1
                        ? compileToSqlExpression(args.get(0), context) : Literal.integer(1);
                Expression day = args.size() >= 2
                        ? compileToSqlExpression(args.get(1), context) : Literal.integer(1);
                if (args.size() >= 3) {
                    // date(y, m, d, h, ...) -> make_timestamp(y, m, d, h, min, sec)
                    // source=y, args=[m, d, h, min?, sec?]
                    Expression hour = compileToSqlExpression(args.get(2), context);
                    Expression minute = args.size() >= 4
                            ? compileToSqlExpression(args.get(3), context) : Literal.integer(0);
                    Expression second = args.size() >= 5
                            ? compileToSqlExpression(args.get(4), context) : Literal.integer(0);
                    Expression ts = FunctionExpression.of("date", year, month, day, hour, minute, second);
                    // Preserve precision: hour-only → %Y-%m-%dT%H, minute → %Y-%m-%dT%H:%M
                    if (args.size() == 3) {
                        yield FunctionExpression.of("strftime", ts, Literal.string("%Y-%m-%dT%H"));
                    } else if (args.size() == 4) {
                        yield FunctionExpression.of("strftime", ts, Literal.string("%Y-%m-%dT%H:%M"));
                    }
                    // Subsecond precision: if seconds arg is float/decimal (e.g., 11.0),
                    // use strftime with %S.%f and trim trailing zeros (keep at least 1 decimal)
                    if (args.size() >= 5) {
                        PureExpression secExpr = args.get(4);
                        boolean hasSubSecond = secExpr instanceof LiteralExpr lit &&
                                (lit.type() == LiteralExpr.LiteralType.FLOAT || lit.type() == LiteralExpr.LiteralType.DECIMAL);
                        if (hasSubSecond) {
                            // REGEXP_REPLACE(STRFTIME(ts, '%Y-%m-%dT%H:%M:%S.%f'), '0{1,5}$', '')
                            // Trims 1-5 trailing zeros from 6-digit microseconds, keeping at least 1 decimal
                            yield FunctionExpression.of("replace",
                                    FunctionExpression.of("strftime", ts, Literal.string("%Y-%m-%dT%H:%M:%S.%f")),
                                    Literal.string("0{1,5}$"), Literal.string(""));
                        }
                    }
                    yield ts;
                }
                // date(y, m, d) or fewer -> make_date(y, m, d)
                if (args.isEmpty()) {
                    // date(y) -> year-only precision
                    yield FunctionExpression.of("strftime", FunctionExpression.of("date", year, Literal.integer(1), Literal.integer(1)), Literal.string("%Y"));
                } else if (args.size() == 1) {
                    // date(y, m) -> year-month precision
                    yield FunctionExpression.of("strftime", FunctionExpression.of("date", year, month, Literal.integer(1)), Literal.string("%Y-%m"));
                }
                yield FunctionExpression.of("date", year, month, day);
            }
            case "dateDiff" -> { // date1->dateDiff(date2, DurationUnit.DAYS) -> DateDiffExpression
                var args = methodCall.arguments();
                if (args.size() < 2) {
                    throw new PureCompileException("dateDiff requires 2 arguments: dateDiff(date2, unit)");
                }
                Expression d1 = compileToSqlExpression(methodCall.source(), context);
                Expression d2 = compileToSqlExpression(args.get(0), context);
                DurationUnit unit = parseDurationUnit(args.get(1));
                yield new DateDiffExpression(d1, d2, unit);
            }
            case "adjust" -> { // date->adjust(amount, DurationUnit.DAYS) -> date + INTERVAL 'amount' DAY
                var args = methodCall.arguments();
                if (args.size() < 2) {
                    throw new PureCompileException("adjust requires 2 arguments: adjust(amount, unit)");
                }
                Expression dateExpr = compileToSqlExpression(methodCall.source(), context);
                Expression amountExpr = compileToSqlExpression(args.get(0), context);
                DurationUnit unit = parseDurationUnit(args.get(1));
                yield new DateAdjustExpression(dateExpr, amountExpr, unit);
            }
            case "parseDate" -> { // parseDate(s) or parseDate(s, fmt)
                Expression src = compileToSqlExpression(methodCall.source(), context);
                if (methodCall.arguments().isEmpty()) {
                    // No format: generator emits CAST(... AS TIMESTAMPTZ)
                    yield FunctionExpression.of("parseDate", src);
                }
                // With format: generator emits CAST(STRPTIME(s, fmt) AS DATE)
                yield new FunctionExpression("cast",
                        FunctionExpression.of("parseDate", src,
                                compileToSqlExpression(methodCall.arguments().getFirst(), context)),
                        java.util.List.of(), Primitive.STRICT_DATE);
            }
            case "timeBucket" -> { // timeBucket(count, unit) -> time_bucket(interval, timestamp, origin)
                // DuckDB time_bucket expects INTERVAL like 'to_days(1)' etc.
                var args = methodCall.arguments();
                if (args.size() < 2)
                    throw new PureCompileException("timeBucket requires count and DurationUnit arguments");
                Expression timestamp = compileToSqlExpression(methodCall.source(), context);
                Expression count = compileToSqlExpression(args.get(0), context);
                Expression unitExpr = compileToSqlExpression(args.get(1), context); // e.g., 'DAYS'
                // Get the unit name from the literal
                String unitName = unitExpr instanceof Literal lit && lit.value() instanceof String s ? s : "DAYS";
                // Validate unit for StrictDate - only YEARS, MONTHS, WEEKS, DAYS allowed
                boolean isStrictDate = timestamp.type() == Primitive.STRICT_DATE;
                if (isStrictDate) {
                    switch (unitName.toUpperCase()) {
                        case "YEARS", "MONTHS", "WEEKS", "DAYS" -> { }
                        default -> throw new PureCompileException(
                                "Unsupported duration unit for StrictDate. Units can only be: [YEARS, DAYS, MONTHS, WEEKS]");
                    }
                }
                // Map DurationUnit to DuckDB interval function
                String intervalFunc = switch (unitName.toUpperCase()) {
                    case "DAYS" -> "to_days";
                    case "HOURS" -> "to_hours";
                    case "MINUTES" -> "to_minutes";
                    case "SECONDS" -> "to_seconds";
                    case "WEEKS" -> "to_weeks";
                    case "MONTHS" -> "to_months";
                    case "YEARS" -> "to_years";
                    default -> "to_days";
                };
                // Use Unix epoch as origin to match Pure's expected bucket boundaries
                // For WEEKS, use ISO Monday epoch 1969-12-29 (Monday before Unix epoch)
                // ref: legend-engine DuckDB constructTimeBucketOffset
                String originDate = "WEEKS".equals(unitName.toUpperCase()) ? "1969-12-29" : "1970-01-01";
                Expression timeBucketExpr = FunctionExpression.of("timeBucket",
                        FunctionExpression.of(intervalFunc, count),
                        timestamp,
                        new org.finos.legend.engine.plan.CastExpression(
                                Literal.of(originDate), Primitive.DATE_TIME));
                // For DateTime inputs, cast to TIMESTAMP_NS to preserve nanosecond precision
                // For StrictDate inputs, cast back to DATE to preserve date-only type
                if (isStrictDate) {
                    yield new org.finos.legend.engine.plan.CastExpression(timeBucketExpr, Primitive.STRICT_DATE);
                }
                yield new org.finos.legend.engine.plan.CastExpression(timeBucketExpr, Primitive.DATE_TIME);
            }

            // ===== MATH FUNCTIONS =====
            case "log" -> // x->log() -> ln(x)
                FunctionExpression.of("ln", compileToSqlExpression(methodCall.source(), context));
            case "log10" -> // x->log10() -> log10(x)
                FunctionExpression.of("log10", compileToSqlExpression(methodCall.source(), context));
            case "exp" -> // x->exp() -> exp(x)
                FunctionExpression.of("exp", compileToSqlExpression(methodCall.source(), context));
            case "pow" -> { // x->pow(y) -> pow(x, y)
                if (methodCall.arguments().isEmpty())
                    throw new PureCompileException("pow requires an exponent argument");
                yield FunctionExpression.of("pow",
                        compileToSqlExpression(methodCall.source(), context),
                        compileToSqlExpression(methodCall.arguments().getFirst(), context));
            }
            case "cbrt" -> // x->cbrt() -> cbrt(x)
                FunctionExpression.of("cbrt", compileToSqlExpression(methodCall.source(), context));
            case "sqrt" -> // x->sqrt() -> sqrt(x)
                FunctionExpression.of("sqrt", compileToSqlExpression(methodCall.source(), context));

            // ===== BITWISE FUNCTIONS =====
            case "bitAnd" -> { // bitAnd(a, b) -> a & b
                if (methodCall.arguments().isEmpty())
                    throw new PureCompileException("bitAnd requires an argument");
                yield FunctionExpression.of("bitand",
                        compileToSqlExpression(methodCall.source(), context),
                        compileToSqlExpression(methodCall.arguments().getFirst(), context));
            }
            case "bitOr" -> { // bitOr(a, b) -> a | b
                if (methodCall.arguments().isEmpty())
                    throw new PureCompileException("bitOr requires an argument");
                yield FunctionExpression.of("bitor",
                        compileToSqlExpression(methodCall.source(), context),
                        compileToSqlExpression(methodCall.arguments().getFirst(), context));
            }
            case "bitXor" -> { // bitXor(a, b) -> a ^ b
                if (methodCall.arguments().isEmpty())
                    throw new PureCompileException("bitXor requires an argument");
                yield FunctionExpression.of("bitxor",
                        compileToSqlExpression(methodCall.source(), context),
                        compileToSqlExpression(methodCall.arguments().getFirst(), context));
            }
            case "bitNot" -> // bitNot(a) -> ~a
                FunctionExpression.of("bitnot", compileToSqlExpression(methodCall.source(), context));

            // ===== DATE INTROSPECTION FUNCTIONS =====
            // Pure dates have precision (year-only, year-month, full date, datetime, etc.)
            // DuckDB always has full timestamps, so we check based on the original literal precision.
            // For full timestamps/dates, all components are present -> true.
            // For partial dates (%2015, %2015-04), only some components exist.
            case "hasDay", "hasMonth" -> {
                // Check if source is a date literal with enough precision
                Expression src = compileToSqlExpression(methodCall.source(), context);
                if (src instanceof Literal lit && (lit.literalType() == Literal.LiteralType.DATE || lit.literalType() == Literal.LiteralType.TIMESTAMP)) {
                    String dateStr = (String) lit.value();
                    String stripped = dateStr.startsWith("%") ? dateStr.substring(1) : dateStr;
                    boolean hasComponent = shortMethodName.equals("hasMonth")
                            ? stripped.length() >= 7  // yyyy-MM
                            : stripped.length() >= 10; // yyyy-MM-dd
                    yield Literal.bool(hasComponent);
                }
                // For non-literal dates (computed), assume full precision
                yield Literal.bool(true);
            }
            case "hasHour", "hasMinute", "hasSecond" -> {
                Expression src = compileToSqlExpression(methodCall.source(), context);
                if (src instanceof Literal lit && (lit.literalType() == Literal.LiteralType.DATE || lit.literalType() == Literal.LiteralType.TIMESTAMP)) {
                    String dateStr = (String) lit.value();
                    String stripped = dateStr.startsWith("%") ? dateStr.substring(1) : dateStr;
                    boolean hasTime = stripped.contains("T");
                    boolean hasComponent = switch (shortMethodName) {
                        case "hasHour" -> hasTime;
                        case "hasMinute" -> hasTime && stripped.indexOf(':', stripped.indexOf('T') + 1) > 0;
                        case "hasSecond" -> {
                            int firstColon = stripped.indexOf(':');
                            yield hasTime && firstColon > 0
                                    && stripped.indexOf(':', firstColon + 1) > 0;
                        }
                        default -> hasTime;
                    };
                    yield Literal.bool(hasComponent);
                }
                yield Literal.bool(true);
            }
            case "hasSubsecond", "hasSubsecondWithAtLeastPrecision" -> {
                Expression src = compileToSqlExpression(methodCall.source(), context);
                if (src instanceof Literal lit && (lit.literalType() == Literal.LiteralType.DATE || lit.literalType() == Literal.LiteralType.TIMESTAMP)) {
                    String dateStr = (String) lit.value();
                    yield Literal.bool(dateStr.contains("."));
                }
                yield Literal.bool(false);
            }

            // ===== VARIANT FUNCTIONS =====
            case "toMany" -> { // $x.payload->toMany(@Integer) -> CAST(payload AS BIGINT[])
                var args = methodCall.arguments();
                GenericType pureType = Primitive.STRING; // default
                if (!args.isEmpty() && args.get(0) instanceof TypeReference typeRef) {
                    pureType = GenericType.fromTypeName(typeRef.typeName());
                }
                yield new org.finos.legend.engine.plan.CastExpression(
                        compileToSqlExpression(methodCall.source(), context),
                        pureType, true);
            }
            case "toVariant" -> // value->toVariant() -> to_json(value)
                FunctionExpression.of("toJson", compileToSqlExpression(methodCall.source(), context));
            case "to" -> { // $x.payload->get(0)->to(@Integer) -> CAST(... AS BIGINT)
                var args = methodCall.arguments();
                GenericType pureType = Primitive.STRING; // default
                if (!args.isEmpty() && args.get(0) instanceof TypeReference typeRef) {
                    pureType = GenericType.fromTypeName(typeRef.typeName());
                }
                yield new org.finos.legend.engine.plan.CastExpression(
                        compileToSqlExpression(methodCall.source(), context),
                        pureType);
            }

            // isEmpty: value->isEmpty() -> value IS NULL (scalars) or len(value) = 0 (lists)
            case "isEmpty" -> {
                Expression src = compileToSqlExpression(methodCall.source(), context);
                if (methodCall.source() instanceof ArrayLiteral) {
                    yield ComparisonExpression.equals(FunctionExpression.of("len", src), Literal.of(0));
                }
                yield new ComparisonExpression(src, ComparisonExpression.ComparisonOperator.IS_NULL, null);
            }
            // isNotEmpty: value->isNotEmpty() -> value IS NOT NULL (scalars) or len(value) > 0 (lists)
            case "isNotEmpty" -> {
                Expression src = compileToSqlExpression(methodCall.source(), context);
                if (methodCall.source() instanceof ArrayLiteral) {
                    yield ComparisonExpression.greaterThan(FunctionExpression.of("len", src), Literal.of(0));
                }
                yield new ComparisonExpression(src, ComparisonExpression.ComparisonOperator.IS_NOT_NULL, null);
            }
            // not: bool->not() -> NOT bool
            case "not" -> {
                Expression src = compileToSqlExpression(methodCall.source(), context);
                yield LogicalExpression.not(src);
            }
            case "between" -> { // x->between(low, high) -> x >= low AND x <= high
                var args = methodCall.arguments();
                if (args.size() < 2)
                    throw new PureCompileException("between requires 2 arguments: between(low, high)");
                Expression value = compileToSqlExpression(methodCall.source(), context);
                Expression low = compileToSqlExpression(args.get(0), context);
                Expression high = compileToSqlExpression(args.get(1), context);
                yield LogicalExpression.and(
                        new ComparisonExpression(value, ComparisonExpression.ComparisonOperator.GREATER_THAN_OR_EQUALS, low),
                        new ComparisonExpression(value, ComparisonExpression.ComparisonOperator.LESS_THAN_OR_EQUALS, high));
            }
            case "xor" -> { // a->xor(b) -> (a AND NOT b) OR (NOT a AND b)
                if (methodCall.arguments().isEmpty())
                    throw new PureCompileException("xor requires an argument");
                Expression a = compileToSqlExpression(methodCall.source(), context);
                Expression b = compileToSqlExpression(methodCall.arguments().getFirst(), context);
                yield LogicalExpression.or(
                        LogicalExpression.and(a, LogicalExpression.not(b)),
                        LogicalExpression.and(LogicalExpression.not(a), b));
            }
            case "hash" -> { // str->hash(HashType.MD5) -> md5(str), or str->hash() -> HASH(str)
                Expression src = compileToSqlExpression(methodCall.source(), context);
                if (methodCall.arguments().isEmpty()) {
                    // No-arg hash() = hashCode, use DuckDB HASH()
                    yield FunctionExpression.of("hash", src);
                }
                PureExpression hashTypeArg = methodCall.arguments().getFirst();
                String hashType = "";
                if (hashTypeArg instanceof EnumValueReference enumRef) {
                    hashType = enumRef.valueName().toUpperCase();
                } else if (hashTypeArg instanceof VariableExpr v) {
                    hashType = v.name().toUpperCase();
                } else {
                    hashType = hashTypeArg.toString().toUpperCase();
                }
                if (hashType.contains("SHA256")) {
                    yield FunctionExpression.of("sha256", src);
                } else if (hashType.contains("SHA1")) {
                    yield FunctionExpression.of("sha1", src);
                }
                yield FunctionExpression.of("md5", src);
            }

            // list(collection) -> no-op; Pure's List<T> wrapper has no SQL equivalent
            case "list" -> compileToSqlExpression(methodCall.source(), context);

            case "coalesce" -> {
                // Pure's [] means "no value" (like SQL NULL), but our compiler emits DuckDB [] (empty array, NOT NULL).
                // COALESCE only skips NULL, so we must convert empty ArrayLiterals to NULL.
                java.util.List<Expression> coalArgs = new java.util.ArrayList<>();
                coalArgs.add(isEmptyArrayLiteral(methodCall.source())
                        ? Literal.ofNull()
                        : compileToSqlExpression(methodCall.source(), context));
                for (PureExpression arg : methodCall.arguments()) {
                    coalArgs.add(isEmptyArrayLiteral(arg)
                            ? Literal.ofNull()
                            : compileToSqlExpression(arg, context));
                }
                Expression first = coalArgs.getFirst();
                Expression[] rest = coalArgs.subList(1, coalArgs.size()).toArray(new Expression[0]);
                yield FunctionExpression.of("COALESCE", first, rest);
            }

            default -> compileSimpleMethodCall(methodCall, context);
        };
    }

    /**
     * Compiles get('key') or get('key', @Type).
     * get('key') returns JSON (for arrays/objects).
     * get('key', @Type) returns typed scalar value.
     */
    private Expression compileGetCall(FunctionCall methodCall, CompilationContext context) {
        Expression source = compileToSqlExpression(methodCall.source(), context);
        List<PureExpression> args = methodCall.arguments();

        if (args.isEmpty()) {
            throw new PureCompileException("get() requires at least a key argument");
        }

        // First argument is the key
        Expression keyArg = compileToSqlExpression(args.get(0), context);

        // Check for optional type argument (@Type)
        // Default to JSON since get() extracts from JSON and returns JSON unless typed
        GenericType returnType = Primitive.JSON;
        if (args.size() >= 2 && args.get(1) instanceof TypeReference typeRef) {
            returnType = mapTypeName(typeRef.typeName());
        }

        return new FunctionExpression("get", source, List.of(keyArg), returnType);
    }

    /**
     * Compiles a simple method call (get, fromJson, toJson, etc.)
     */
    private Expression compileSimpleMethodCall(FunctionCall methodCall, CompilationContext context) {
        // Compile the source expression (e.g., $_.PAYLOAD)
        Expression source = compileToSqlExpression(methodCall.source(), context);

        // Compile additional arguments (e.g., the key name in get('page'))
        List<Expression> additionalArgs = new java.util.ArrayList<>();
        for (PureExpression arg : methodCall.arguments()) {
            additionalArgs.add(compileToSqlExpression(arg, context));
        }

        // Return FunctionExpression which will be handled by SQLGenerator
        return new FunctionExpression(methodCall.functionName(), source, additionalArgs, Primitive.ANY);
    }

    /**
     * Compiles $x->in([a, b, c]) to list_contains([a, b, c], x)
     * Uses DuckDB's native list_contains function.
     */
    private Expression compileListContains(FunctionCall methodCall, CompilationContext context) {
        // The source is the value to check
        Expression valueToCheck = compileToSqlExpression(methodCall.source(), context);

        // The argument is the list
        if (methodCall.arguments().isEmpty()) {
            throw new PureCompileException("in() requires an array argument");
        }

        Expression list = compileToSqlExpression(methodCall.arguments().getFirst(), context);
        // If the list is JSON[] (mixed-type), wrap the search element in to_json() too
        if (isJsonList(list)) {
            valueToCheck = FunctionExpression.of("to_json", valueToCheck);
        }
        // list_contains(list, value)
        return FunctionExpression.of("list_contains", list, valueToCheck);
    }

    /**
     * Compiles [a, b, c]->contains($value) to list_contains([a, b, c], value)
     * Uses DuckDB's native list_contains function.
     */
    private Expression compileListContainsMethod(FunctionCall methodCall, CompilationContext context) {
        // The source is the list
        Expression list = compileToSqlExpression(methodCall.source(), context);

        // The argument is the value to check
        if (methodCall.arguments().isEmpty()) {
            throw new PureCompileException("contains() requires an argument");
        }

        Expression valueToCheck = compileToSqlExpression(methodCall.arguments().getFirst(), context);

        // Type mismatch short-circuit: struct list vs primitive (or vice versa) -> always false
        if (list instanceof ListLiteral ll && !ll.isEmpty()) {
            boolean listIsStruct = ll.elements().getFirst() instanceof StructLiteralExpression;
            boolean valueIsStruct = valueToCheck instanceof StructLiteralExpression;
            if (listIsStruct != valueIsStruct) {
                return Literal.bool(false);
            }
        }

        // If the list is JSON[] (mixed-type), wrap the search element in to_json() too
        if (isJsonList(list)) {
            valueToCheck = FunctionExpression.of("to_json", valueToCheck);
        }
        // list_contains(list, value)
        return FunctionExpression.of("list_contains", list, valueToCheck);
    }

    /**
     * Detects sort direction from comparator lambda in sort() arguments.
     * {x, y | $y->compare($x)} = descending (params reversed)
     * Returns Literal.string("DESC") or Literal.string("ASC").
     */
    private Expression detectSortDirection(List<PureExpression> sortColumns) {
        for (PureExpression arg : sortColumns) {
            if (arg instanceof LambdaExpression lambda && lambda.isMultiParam()
                    && lambda.body() instanceof FunctionCall cmp
                    && (cmp.functionName().equals("compare") || cmp.functionName().endsWith("::compare"))) {
                String firstParam = lambda.parameters().get(0);
                String secondParam = lambda.parameters().get(1);
                if (cmp.source() instanceof VariableExpr v && v.name().equals(secondParam)
                        && !cmp.arguments().isEmpty()
                        && cmp.arguments().getFirst() instanceof VariableExpr v2 && v2.name().equals(firstParam)) {
                    return Literal.string("DESC");
                }
            }
        }
        return Literal.string("ASC");
    }

    /**
     * Checks if a list of compiled SQL expressions contains elements of truly incompatible types.
     * Used to detect heterogeneous Pure lists that need JSON[] wrapping.
     * Numeric types (INTEGER, BIGINT, DOUBLE, DECIMAL) are considered compatible since
     * DuckDB natively promotes them.
     */
    private boolean hasMixedTypes(List<Expression> elements) {
        if (elements.size() <= 1) return false;
        GenericType firstType = elements.getFirst().type();
        for (int i = 1; i < elements.size(); i++) {
            GenericType t = elements.get(i).type();
            // Treat UNKNOWN as compatible with anything (it may resolve at runtime)
            if (t == Primitive.ANY || firstType == Primitive.ANY) continue;
            if (t != firstType && !areNumericCompatible(firstType, t)) return true;
        }
        return false;
    }

    private static boolean isNumericType(GenericType t) {
        return t.isNumeric();
    }

    private static boolean areNumericCompatible(GenericType a, GenericType b) {
        return isNumericType(a) && isNumericType(b);
    }

    /**
     * Checks if a compiled expression represents a JSON[] list (mixed-type list).
     * A ListLiteral is JSON[] if any element is a to_json() call.
     */
    private boolean isJsonList(Expression expr) {
        if (expr instanceof ListLiteral list && !list.isEmpty()) {
            Expression first = list.elements().getFirst();
            return first instanceof FunctionExpression fc && "to_json".equals(fc.functionName());
        }
        return false;
    }

    /**
     * Wraps a list expression so its elements become JSON scalars.
     * For ListLiteral: wraps each element in to_json().
     * For other list expressions: uses list_transform(list, _x -> to_json(_x)).
     */
    private Expression wrapListToJson(Expression listExpr) {
        if (listExpr instanceof ListLiteral lit) {
            List<Expression> jsonElems = lit.elements().stream()
                    .map(e -> (Expression) FunctionExpression.of("to_json", e))
                    .toList();
            return ListLiteral.of(jsonElems);
        }
        // For non-literal list expressions, use list_transform
        GenericType jsonElemType = listExpr.type().elementType();
        return CollectionExpression.map(listExpr, "_json_x",
                FunctionExpression.of("to_json", new ColumnReference("", "_json_x", jsonElemType)));
    }

    /**
     * Checks if two list expressions need JSON promotion for cross-type concatenation.
     * Returns true if both are ListLiterals with different element types.
     */
    private boolean needsCrossTypeConcatPromotion(Expression left, Expression right) {
        if (left instanceof ListLiteral ll && right instanceof ListLiteral rl) {
            if (ll.isEmpty() || rl.isEmpty()) return false;
            GenericType leftType = ll.elements().getFirst().type();
            GenericType rightType = rl.elements().getFirst().type();
            if (leftType == Primitive.ANY || rightType == Primitive.ANY) return false;
            return leftType != rightType && !areNumericCompatible(leftType, rightType);
        }
        return false;
    }

    /**
     * Compiles map(x | expr) to list_transform(arr, lambda x: expr)
     */
    private Expression compileMapCall(FunctionCall methodCall, CompilationContext context) {
        Expression source = compileToSqlExpression(methodCall.source(), context);

        if (methodCall.arguments().isEmpty() || !(methodCall.arguments().get(0) instanceof LambdaExpression lambda)) {
            throw new PureCompileException("map() requires a lambda argument");
        }

        // Compile lambda body with the lambda parameter in context
        // For struct list sources, pass the element class name so at(0) on scalar
        // properties can be optimized to a no-op instead of LIST_EXTRACT
        String lambdaParam = lambda.parameter();
        String elemClassName = extractStructListClassName(methodCall.source());
        GenericType elemType = source.type().elementType();
        CompilationContext newContext = context.withLambdaParameter(lambdaParam,
                elemClassName != null ? elemClassName : "", elemType);
        Expression lambdaBody = compileToSqlExpression(lambda.body(), newContext);

        return CollectionExpression.map(source, lambdaParam, lambdaBody);
    }

    /**
     * Compiles filter(x | condition) on collections to list_filter
     */
    private Expression compileFilterCollectionCall(FunctionCall methodCall, CompilationContext context) {
        Expression source = compileToSqlExpression(methodCall.source(), context);

        if (methodCall.arguments().isEmpty() || !(methodCall.arguments().get(0) instanceof LambdaExpression lambda)) {
            throw new PureCompileException("filter() on collection requires a lambda argument");
        }

        String lambdaParam = lambda.parameter();
        GenericType elemType = source.type().elementType();
        Expression lambdaBody = compileToSqlExpression(lambda.body(),
                context.withLambdaParameter(lambdaParam, "", elemType));

        return CollectionExpression.filter(source, lambdaParam, lambdaBody);
    }

    /**
     * Compiles exists(x | condition) on collections to len(list_filter(arr, x -> condition)) > 0
     * Pure: [1,2,3]->exists(x|$x > 2) = true
     * SQL:  len(list_filter([1,2,3], x -> x > 2)) > 0  
     */
    private Expression compileExistsCall(FunctionCall methodCall, CompilationContext context) {
        Expression source = compileToSqlExpression(methodCall.source(), context);

        if (methodCall.arguments().isEmpty() || !(methodCall.arguments().get(0) instanceof LambdaExpression lambda)) {
            throw new PureCompileException("exists() on collection requires a lambda argument");
        }

        String lambdaParam = lambda.parameter();
        GenericType elemType = source.type().elementType();
        Expression lambdaBody = compileToSqlExpression(lambda.body(),
                context.withLambdaParameter(lambdaParam, "", elemType));

        // exists = len(list_filter(source, param -> body)) > 0
        Expression filtered = CollectionExpression.filter(source, lambdaParam, lambdaBody);
        return new ComparisonExpression(
                FunctionExpression.of("len", filtered),
                ComparisonExpression.ComparisonOperator.GREATER_THAN,
                Literal.integer(0));
    }

    /**
     * Compiles fold({acc, x | expr}, init) to list_reduce(arr, lambda acc, x: expr,
     * init)
     *
     * When the source is a struct list and the initial value is a different type
     * (e.g., string), DuckDB's list_reduce fails with a type mismatch error.
     * In this case, we decompose into: list_reduce(list_transform(source, elem -> f(elem)), (acc, x) -> acc + x, init)
     */
    private Expression compileFoldCall(FunctionCall methodCall, CompilationContext context) {
        Expression source = compileToSqlExpression(methodCall.source(), context);

        if (methodCall.arguments().size() < 2) {
            throw new PureCompileException("fold() requires lambda and initial value arguments");
        }

        PureExpression lambdaArg = methodCall.arguments().get(0);
        if (!(lambdaArg instanceof LambdaExpression lambda)) {
            throw new PureCompileException("fold() first argument must be a lambda");
        }

        // Pure fold lambda: {element, accumulator | body}
        // params[0] = current element, params[1] = accumulator
        List<String> params = lambda.parameters();
        if (params.size() < 2) {
            throw new PureCompileException("fold() lambda requires two parameters (element, accumulator)");
        }
        String pureElemParam = params.get(0);
        String pureAccParam = params.get(1);

        Expression initialValue = compileToSqlExpression(methodCall.arguments().get(1), context);

        // Fix 2: Wrap scalar source in list_value() for fold
        // Pure allows 1->fold(...) where source is a single value, not a list.
        // DuckDB's list_reduce requires a list, so wrap scalar sources.
        if (isScalarFoldSource(methodCall.source(), source)) {
            source = FunctionExpression.of("list", source);
        }

        // Fix 1: Detect "list accumulation" pattern: fold({val, acc | acc->add(val)}, init)
        // When the fold lambda is just appending elements to a list accumulator,
        // compile as list_concat(initial, source) instead of list_reduce.
        if (isFoldListAccumulation(lambda, pureElemParam, pureAccParam)) {
            // Strip cast from empty list initial to avoid type mismatch
            // e.g., CAST([] AS VARCHAR[]) -> [] so DuckDB infers type from source
            Expression init = stripEmptyListCast(initialValue);
            return FunctionExpression.of("concatenate", init, source);
        }

        // Check if source is a struct list that needs decomposition for DuckDB compatibility.
        // DuckDB's list_reduce requires initial value type = list element type.
        // When folding structs into a string, we decompose into list_transform + list_reduce.
        String elemClassName = extractStructListClassName(methodCall.source());
        if (elemClassName != null && !(initialValue instanceof StructLiteralExpression)) {
            Expression lambdaBody = compileToSqlExpression(lambda.body(),
                    context.withFoldParameters(pureAccParam, pureElemParam, elemClassName));

            // Try to decompose the body: strip accumulator from left spine of ADD chain
            Expression elemTransform = stripAccumulatorFromBody(lambdaBody, pureAccParam);
            if (elemTransform != null) {
                // Generate: list_reduce(list_transform(source, elem -> elemTransform), (acc, x) -> acc || x, init)
                Expression transformedList = CollectionExpression.map(source, pureElemParam, elemTransform);
                String freshX = "__x";
                GenericType accType = initialValue.type();
                GenericType freshXType = transformedList.type().elementType();
                Expression reduceBody = ConcatExpression.of(
                        new ColumnReference("", pureAccParam, accType), new ColumnReference("", freshX, freshXType));
                return CollectionExpression.fold(transformedList, pureAccParam, freshX, reduceBody, initialValue);
            }
        }

        // Fix 3: Generalize mixed-type fold decomposition for any list type.
        // When accumulator type differs from list element type, decompose into
        // list_reduce(list_transform(source, elem -> f(elem)), (acc, x) -> acc OP x, init).
        if (elemClassName == null) {
            Expression lambdaBody = compileToSqlExpression(lambda.body(),
                    context.withFoldParameters(pureElemParam, pureAccParam));
            Expression elemTransform = stripAccumulatorFromBody(lambdaBody, pureAccParam);
            if (elemTransform != null) {
                Expression transformedList = CollectionExpression.map(source, pureElemParam, elemTransform);
                String freshX = "__x";
                GenericType accType2 = initialValue.type();
                GenericType freshXType2 = transformedList.type().elementType();
                Expression reduceBody = ArithmeticExpression.add(
                        new ColumnReference("", pureAccParam, accType2), new ColumnReference("", freshX, freshXType2));
                return CollectionExpression.fold(transformedList, pureAccParam, freshX, reduceBody, initialValue);
            }
        }

        // Fix 4: List-accumulator fold — wrap source elements to match accumulator type.
        // When the initial value is a list (e.g., [-1, 0]) but source elements are scalar (e.g., INTEGER),
        // DuckDB's list_reduce fails because INTEGER != INTEGER[].
        // Solution: wrap each source element in a single-element list so both are INTEGER[],
        // then use a param override to unwrap element references during compilation.
        if (initialValue instanceof ListLiteral initList && !initList.isEmpty()) {
            // Wrap source: list_transform(source, __e -> list_value(__e))
            String wrapParam = "__e";
            GenericType wrapElemType = source.type().elementType();
            Expression wrappedSource = CollectionExpression.map(source, wrapParam,
                    FunctionExpression.of("list", new ColumnReference("", wrapParam, wrapElemType)));

            // Compile body with element param overridden to list_extract(param, 1) for unwrapping
            GenericType elemListType = wrappedSource.type().elementType();
            Expression elemUnwrap = FunctionExpression.of("list_extract",
                    new ColumnReference("", pureElemParam, elemListType), Literal.integer(1));
            Expression lambdaBody = compileToSqlExpression(lambda.body(),
                    context.withFoldParameters(pureElemParam, pureAccParam)
                           .withParamOverride(pureElemParam, elemUnwrap));

            return CollectionExpression.fold(wrappedSource, pureAccParam, pureElemParam, lambdaBody, initialValue);
        }

        // Fallback: standard list_reduce approach
        Expression lambdaBody = compileToSqlExpression(lambda.body(),
                context.withFoldParameters(pureElemParam, pureAccParam));

        // DuckDB list_reduce: (accumulator, element) -> body
        // Map Pure's accumulator to DuckDB's first position (accumulator)
        return CollectionExpression.fold(source, pureAccParam, pureElemParam, lambdaBody, initialValue);
    }

    /**
     * Checks if an expression is known to return a list (not a scalar).
     * Used to avoid double-wrapping in tail/init where scalars need [x] but list results don't.
     */
    private boolean isListReturningExpression(Expression expr) {
        if (expr instanceof FunctionExpression func) {
            return switch (func.functionName().toLowerCase()) {
                case "add", "list_prepend", "concatenate", "list_slice",
                     "sort", "reverse", "removeDuplicates", "list_filter",
                     "list_transform", "list", "list_resize", "flatten",
                     "list_reduce" -> true;
                default -> false;
            };
        }
        if (expr instanceof CollectionExpression) return true;
        if (expr instanceof CaseExpression) return true; // CASE branches may return lists
        return false;
    }

    /**
     * Detects if the fold lambda body is a "list accumulation" pattern: acc->add(val).
     * In the AST, this is a MethodCall with source=$acc, method="add", args=[$val].
     */
    private boolean isFoldListAccumulation(LambdaExpression lambda, String elemParam, String accParam) {
        PureExpression body = lambda.body();
        if (!(body instanceof FunctionCall addCall)) return false;
        String shortName = addCall.functionName().contains("::")
                ? addCall.functionName().substring(addCall.functionName().lastIndexOf("::") + 2)
                : addCall.functionName();
        if (!"add".equals(shortName)) return false;
        // Source must be the accumulator variable
        if (!(addCall.source() instanceof VariableExpr accVar) || !accVar.name().equals(accParam)) return false;
        // Single argument must be the element variable
        if (addCall.arguments().size() != 1) return false;
        if (!(addCall.arguments().getFirst() instanceof VariableExpr elemVar) || !elemVar.name().equals(elemParam)) return false;
        return true;
    }

    /**
     * Strips CAST from an empty list expression to avoid type mismatch in list_concat.
     * CAST([] AS VARCHAR[]) -> [] (untyped, DuckDB infers from other operand)
     */
    private Expression stripEmptyListCast(Expression expr) {
        // CastExpression IR node: CAST([] AS TYPE[])
        if (expr instanceof org.finos.legend.engine.plan.CastExpression cast
                && cast.source() instanceof ListLiteral ll && ll.isEmpty()) {
            return ll;
        }
        // FunctionExpression("cast", [], ...) from compileCastExpression
        if (expr instanceof FunctionExpression func
                && "cast".equals(func.functionName())
                && func.target() instanceof ListLiteral ll && ll.isEmpty()) {
            return ll;
        }
        return expr;
    }

    /**
     * Checks if a fold source is a scalar value (not a list) that needs wrapping.
     * Returns true for literals and other non-array expressions that aren't already lists.
     */
    private boolean isScalarFoldSource(PureExpression pureSource, Expression compiledSource) {
        // Array literals are already lists
        if (pureSource instanceof ArrayLiteral) return false;
        // Method calls that return lists (cast, range, etc.) are not scalar
        if (pureSource instanceof FunctionCall) return false;
        if (pureSource instanceof FunctionCall) return false;
        // Literal values (integers, strings) are scalar
        if (pureSource instanceof LiteralExpr) return true;
        // Variable references could be either — assume list for safety
        return false;
    }

    /**
     * Checks if a PureExpression is a property access on a lambda parameter
     * whose class has the property at multiplicity [1] (scalar).
     * Used to optimize at(0) as a no-op for struct field access.
     */
    private boolean isScalarStructPropertyAccess(PureExpression source, CompilationContext context) {
        if (!(source instanceof PropertyAccessExpression propAccess)) return false;
        if (!(propAccess.source() instanceof VariableExpr var)) return false;
        if (context == null || !context.isLambdaParameter(var.name())) return false;

        String className = context.getLambdaParamClass(var.name());
        if (className == null) return false;

        var pureClass = typeEnvironment.findClass(className);
        if (pureClass.isEmpty()) return false;

        var property = pureClass.get().findProperty(propAccess.propertyName());
        return property.isPresent() && property.get().multiplicity().isSingular();
    }

    /**
     * Extracts the class name from a struct list source (ArrayLiteral of InstanceExpressions).
     * Returns null if the source is not a struct list.
     */
    private String extractStructListClassName(PureExpression source) {
        if (!(source instanceof ArrayLiteral array)) return null;
        if (array.elements().isEmpty()) return null;
        if (!(array.elements().getFirst() instanceof InstanceExpression inst)) return null;
        return inst.className();
    }

    /**
     * Strips the accumulator reference from the left spine of an ADD/CONCAT chain.
     * For a body like: CONCAT([CONCAT([CONCAT([CONCAT([acc, '; ']), p.lastName]), ', ']), p.firstName])
     * Returns:         CONCAT([CONCAT([CONCAT(['; ', p.lastName]), ', ']), p.firstName])
     * (i.e., the element-only transform expression)
     *
     * Handles both ArithmeticExpression (numeric fold) and ConcatExpression (string fold).
     * Returns null if the accumulator cannot be found in the left spine.
     */
    private Expression stripAccumulatorFromBody(Expression body, String accParam) {
        // Handle ConcatExpression (string concatenation: a || b)
        if (body instanceof ConcatExpression concat && concat.parts().size() == 2) {
            Expression first = concat.parts().get(0);
            Expression second = concat.parts().get(1);

            // Base case: first part is the accumulator column reference
            if (first instanceof ColumnReference ref
                    && ref.columnName().equals(accParam)
                    && ref.tableAlias().isEmpty()) {
                return second;
            }

            // Recursive case: first part is another ConcatExpression
            if (first instanceof ConcatExpression) {
                Expression stripped = stripAccumulatorFromBody(first, accParam);
                if (stripped != null) {
                    return ConcatExpression.of(stripped, second);
                }
            }
        }

        // Handle ArithmeticExpression (numeric fold: a + b)
        if (body instanceof ArithmeticExpression arith
                && arith.operator() == org.finos.legend.pure.dsl.m2m.BinaryArithmeticExpr.Operator.ADD) {
            Expression left = arith.left();
            Expression right = arith.right();

            // Base case: left is the accumulator column reference
            if (left instanceof ColumnReference ref
                    && ref.columnName().equals(accParam)
                    && ref.tableAlias().isEmpty()) {
                return right;
            }

            // Recursive case: left is another ADD
            if (left instanceof ArithmeticExpression leftArith
                    && leftArith.operator() == org.finos.legend.pure.dsl.m2m.BinaryArithmeticExpr.Operator.ADD) {
                Expression stripped = stripAccumulatorFromBody(left, accParam);
                if (stripped != null) {
                    return ArithmeticExpression.add(stripped, right);
                }
            }
        }

        return null;
    }

    /**
     * Compiles flatten() to flatten(arr)
     */
    private Expression compileFlattenCall(FunctionCall methodCall, CompilationContext context) {
        Expression source = compileToSqlExpression(methodCall.source(), context);
        return CollectionExpression.flatten(source);
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
            // Plus operator - check if string concatenation or arithmetic addition
            case "+" -> {
                if (isStringExpression(binary.left()) || isStringExpression(binary.right())) {
                    // String concatenation: use CONCAT or || operator
                    yield ConcatExpression.of(left, right);
                }
                yield ArithmeticExpression.add(left, right);
            }
            case "-" -> ArithmeticExpression.subtract(left, right);
            case "*" -> ArithmeticExpression.multiply(left, right);
            case "/" -> ArithmeticExpression.divide(left, right);
            default -> throw new PureCompileException("Unknown binary operator: " + op);
        };
    }

    /**
     * Checks if a Pure expression is a string type.
     */
    private boolean isStringExpression(PureExpression expr) {
        // Check literal strings
        if (expr instanceof LiteralExpr lit && lit.type() == LiteralExpr.LiteralType.STRING) {
            return true;
        }
        // Check toString() method call
        if (expr instanceof FunctionCall mc && "toString".equals(mc.functionName())) {
            return true;
        }
        // Check binary expressions where one side is string (propagates string type)
        if (expr instanceof BinaryExpression bin && "+".equals(bin.operator())) {
            return isStringExpression(bin.left()) || isStringExpression(bin.right());
        }
        // Property access on a known string column (heuristic - 'str' in name suggests
        // string)
        if (expr instanceof PropertyAccessExpression prop) {
            String propName = prop.propertyName().toLowerCase();
            if (propName.contains("str") || propName.contains("name") || propName.contains("text")) {
                return true;
            }
        }
        return false;
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
        if (context == null || context.lambdaParameter() == null || !var.name().equals(context.lambdaParameter())) {
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
                ColumnReference.of(targetAlias, innerColumn, Primitive.ANY),
                ColumnReference.of(context.tableAlias(), outerColumn, Primitive.ANY));

        // Build the filter condition on the target property
        // The final segment should be a property access on the target class
        NavigationSegment finalSegment = segments.getLast();
        String targetProperty = finalSegment.propertyName();
        String targetColumn = targetMapping.getColumnForProperty(targetProperty)
                .orElseThrow(() -> new PureCompileException("No column mapping for property: " + targetProperty));

        Expression left = ColumnReference.of(targetAlias, targetColumn, Primitive.ANY);
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

        // Check if this is a chained property access (e.g., employees.firstName)
        // If so, build the full path for STRUCT array UNNEST support
        String fullPath = buildPropertyPath(propAccess);

        // Extract the variable name from the source (e.g., "x" from $x.col)
        // If the source is a complex expression (filter, method call, etc.),
        // fall back to collection property access: list_transform(source, _x -> _x.prop)
        String varName;
        try {
            varName = extractVariableName(propAccess.source());
        } catch (PureCompileException e) {
            // Source is a complex expression — compile it
            Expression compiledSource = compileToSqlExpression(propAccess.source(), context);

            // If source returns a scalar struct (at/head/first/last or InstanceExpression),
            // use struct_extract instead of list_transform which expects a list input
            if (propAccess.source() instanceof FunctionCall mc && isScalarMethod(mc.functionName())) {
                return FunctionExpression.of("struct_extract", compiledSource, Literal.string(fullPath));
            }
            if (compiledSource instanceof StructLiteralExpression) {
                return FunctionExpression.of("struct_extract", compiledSource, Literal.string(fullPath));
            }

            // Source is a collection — wrap in list_transform
            String syntheticParam = "_prop_x";
            Expression body = ColumnReference.of(syntheticParam, fullPath, Primitive.ANY);
            return CollectionExpression.map(compiledSource, syntheticParam, body);
        }

        // First check symbol table for ROW binding (new approach)
        if (context != null && context.hasSymbol(varName)) {
            SymbolBinding binding = context.lookupSymbol(varName);
            if (binding.kind() == SymbolBinding.BindingKind.ROW) {
                String rowAlias = binding.tableAlias();

                // Check if this is an extended column (from extend/flatten) - use unqualified
                // reference
                if (context.isExtendedColumn(propertyName)) {
                    return ColumnReference.of(propertyName, Primitive.ANY);
                }

                // If there's a mapping, use it to translate property -> column
                if (context.mapping() != null) {
                    String columnName = context.mapping().getColumnForProperty(propertyName)
                            .orElseThrow(
                                    () -> new PureCompileException("No column mapping for property: " + propertyName));
                    return ColumnReference.of(rowAlias, columnName, context.mapping().pureTypeForProperty(propertyName));
                }

                // No mapping - use full path (for nested STRUCT access like
                // employees.firstName)
                return ColumnReference.of(rowAlias, fullPath, Primitive.ANY);
            }
            // SCALAR binding: if value is a StructLiteralExpression, extract the field
            if (binding.kind() == SymbolBinding.BindingKind.SCALAR && binding.value() instanceof StructLiteralExpression struct) {
                Expression fieldValue = struct.fields().get(propertyName);
                if (fieldValue != null) {
                    return fieldValue;
                }
                throw new PureCompileException("No property '" + propertyName + "' on struct: " + struct.className());
            }
            throw new PureCompileException(
                    "Cannot access property '" + propertyName + "' on " + binding.kind() + " variable: $" + varName);
        }

        // Check if variable is a lambda parameter (from collection operations like map/filter/fold)
        // For struct elements, property access becomes struct field access: p.lastName
        if (context != null && context.lambdaParameters() != null && context.isLambdaParameter(varName)) {
            return ColumnReference.of(varName, fullPath, Primitive.ANY);
        }

        // Legacy fallback: check lambdaParameter directly
        if (context == null || context.lambdaParameter() == null || !varName.equals(context.lambdaParameter())) {
            throw new PureCompileException(
                    "Unknown variable: $" + varName + ", expected lambda parameter");
        }

        // Check if this is an extended column (from extend/flatten) - use unqualified
        // reference
        if (context.isExtendedColumn(propertyName)) {
            return ColumnReference.of(propertyName, Primitive.ANY);
        }

        // If no mapping exists (direct Relation API access), use full path for STRUCT
        // access
        if (context.mapping() == null) {
            return ColumnReference.of(context.tableAlias(), fullPath, Primitive.ANY);
        }

        String columnName = context.mapping().getColumnForProperty(propertyName)
                .orElseThrow(() -> new PureCompileException("No column mapping for property: " + propertyName));

        return ColumnReference.of(context.tableAlias(), columnName, context.mapping().pureTypeForProperty(propertyName));
    }

    /**
     * Builds the full property path from a PropertyAccessExpression.
     * For $x.employees.firstName, returns "employees.firstName"
     * For $x.legalName, returns "legalName"
     */
    private String buildPropertyPath(PropertyAccessExpression propAccess) {
        PureExpression source = propAccess.source();
        if (source instanceof PropertyAccessExpression parentAccess) {
            // Chained access: parent.child
            return buildPropertyPath(parentAccess) + "." + propAccess.propertyName();
        }
        // Base case: $x.property -> just the property name
        return propAccess.propertyName();
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
            Expression columnRef = new ColumnReference(tableAlias, columnName, Primitive.JSON);
            GenericType returnType = mapTypeName(typeName);
            return FunctionExpression.of("get", columnRef, returnType, Literal.string(jsonKey));
        }

        throw new PureCompileException("Cannot parse expression mapping: " + expressionStr);
    }

    /**
     * Builds a CASE expression to translate database values to enum values.
     * 
     * Example: For enum ProductType with mapping { 'OPTION' -> ['O', 0], 'FUTURE'
     * -> ['F', 1] }
     * Generates: CASE WHEN column='O' OR column=0 THEN 'model::ProductType.OPTION'
     * WHEN column='F' OR column=1 THEN 'model::ProductType.FUTURE'
     * ELSE NULL END
     *
     * @param tableAlias  The table alias for the column
     * @param columnName  The column containing database values
     * @param enumType    The enum type name (e.g., 'ProductType')
     * @param enumMapping Map from enum value name to list of source db values
     */
    private Expression buildEnumMappingCaseExpression(
            String tableAlias, String columnName, String enumType,
            java.util.Map<String, java.util.List<Object>> enumMapping) {

        Expression columnRef = ColumnReference.of(tableAlias, columnName, Primitive.ANY);

        // Build nested CASE WHEN expressions
        // Start from the end with NULL as the final ELSE
        Expression result = Literal.nullValue();

        // Process each enum value mapping
        java.util.List<String> enumValues = new java.util.ArrayList<>(enumMapping.keySet());
        java.util.Collections.reverse(enumValues); // Process in reverse for proper nesting

        for (String enumValue : enumValues) {
            java.util.List<Object> dbValues = enumMapping.get(enumValue);

            // Build OR condition for all db values that map to this enum value
            Expression condition = null;
            for (Object dbValue : dbValues) {
                Expression valueCheck;
                if (dbValue instanceof String s) {
                    valueCheck = ComparisonExpression.equals(columnRef, Literal.string(s));
                } else if (dbValue instanceof Number n) {
                    valueCheck = ComparisonExpression.equals(columnRef, Literal.integer(n.longValue()));
                } else {
                    valueCheck = ComparisonExpression.equals(columnRef, Literal.string(dbValue.toString()));
                }

                if (condition == null) {
                    condition = valueCheck;
                } else {
                    condition = LogicalExpression.or(condition, valueCheck);
                }
            }

            // Create the enum value string (just the value name, not fully qualified)
            Expression thenValue = Literal.string(enumValue);

            // Build CASE WHEN condition THEN enumValue ELSE previousResult END
            result = new CaseExpression(condition, thenValue, result);
        }

        return result;
    }

    private Expression compileLiteral(LiteralExpr literal) {
        return switch (literal.type()) {
            case STRING -> Literal.string((String) literal.value());
            case INTEGER -> {
                // Preserve BigInteger for values exceeding Long range (e.g., 9223372036854775898)
                if (literal.value() instanceof java.math.BigInteger bi) {
                    yield new Literal(bi, Literal.LiteralType.INTEGER);
                }
                yield Literal.integer(((Number) literal.value()).longValue());
            }
            case FLOAT -> {
                // Preserve BigDecimal precision for large float literals
                if (literal.value() instanceof java.math.BigDecimal bd) {
                    yield new Literal(bd, Literal.LiteralType.DECIMAL);
                }
                yield new Literal(literal.value(), Literal.LiteralType.DOUBLE);
            }
            case DECIMAL -> {
                // Preserve BigDecimal scale from parser for correct DuckDB DECIMAL precision
                if (literal.value() instanceof java.math.BigDecimal bd) {
                    yield new Literal(bd, Literal.LiteralType.DECIMAL);
                }
                yield Literal.decimal(((Number) literal.value()).doubleValue());
            }
            case BOOLEAN -> Literal.bool((Boolean) literal.value());
            case DATE -> {
                // Distinguish StrictDate (date-only) from DateTime (has time component 'T')
                String dateStr = (String) literal.value();
                String stripped = dateStr.startsWith("%") ? dateStr.substring(1) : dateStr;
                if (stripped.contains("T")) {
                    yield Literal.timestamp(dateStr);
                }
                yield Literal.date(dateStr);
            }
            case STRICTTIME -> Literal.time((String) literal.value());
        };
    }

    private String extractPropertyName(PureExpression expr) {
        if (expr instanceof PropertyAccessExpression propAccess) {
            return propAccess.propertyName();
        }
        // Handle method calls on property access: $x.prop->toOne(), etc.
        if (expr instanceof FunctionCall mc) {
            return extractPropertyName(mc.source());
        }
        // Handle cast on property access: $x.prop->cast(@Type)
        if (expr instanceof CastExpression cast) {
            return extractPropertyName(cast.source());
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
            case PivotNode pivot -> getTableAlias(pivot.source());
            case TdsLiteralNode tds -> "_tds"; // TDS literals use synthetic alias
            case ConstantNode constant -> "_const"; // Constants have no source table
            default -> throw new PureCompileException("Cannot get table alias for: " + node.getClass().getSimpleName());
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
     * 
     * The symbol table (symbols) provides first-class variable binding support for:
     * - ROW: Lambda row parameters (e.g., $t in filter(t | $t.col > 5))
     * - RELATION: Let-bound relations (e.g., let tds = #TDS...#; $tds->filter(...))
     * - SCALAR: Let-bound scalar values (e.g., let x = 5; $x + 1)
     */
    public record CompilationContext(
            String lambdaParameter,
            String tableAlias,
            RelationalMapping mapping,
            String className,
            boolean inFilterContext,
            java.util.Map<String, String> lambdaParameters,
            java.util.Set<String> extendedColumns,
            java.util.Map<String, RelationNode> relationSources,
            java.util.Map<String, SymbolBinding> symbols,
            java.util.Map<String, Expression> paramOverrides,
            java.util.Map<String, org.finos.legend.engine.plan.GenericType> lambdaParamTypes) {

        public CompilationContext(String lambdaParameter, String tableAlias,
                RelationalMapping mapping, String className, boolean inFilterContext) {
            this(lambdaParameter, tableAlias, mapping, className, inFilterContext,
                    new java.util.HashMap<>(), new java.util.HashSet<>(), new java.util.HashMap<>(),
                    new java.util.HashMap<>(), new java.util.HashMap<>(), new java.util.HashMap<>());
        }

        /**
         * Legacy constructor for backwards compatibility.
         */
        public CompilationContext(String lambdaParameter, String tableAlias, RelationalMapping mapping) {
            this(lambdaParameter, tableAlias, mapping, mapping.pureClass().name(), false,
                    new java.util.HashMap<>(), new java.util.HashSet<>(), new java.util.HashMap<>(),
                    new java.util.HashMap<>(), new java.util.HashMap<>(), new java.util.HashMap<>());
        }

        /**
         * Creates a new context with a typed lambda parameter. Every call site must provide a type.
         */
        public CompilationContext withLambdaParameter(String paramName, String alias, org.finos.legend.engine.plan.GenericType elementType) {
            var newParams = new java.util.HashMap<>(lambdaParameters);
            newParams.put(paramName, alias);
            var newTypes = new java.util.HashMap<>(lambdaParamTypes);
            newTypes.put(paramName, elementType);
            return new CompilationContext(lambdaParameter, tableAlias, mapping, className,
                    inFilterContext, newParams, extendedColumns, relationSources, symbols, paramOverrides, newTypes);
        }

        /**
         * Gets the type of a lambda parameter.
         */
        public org.finos.legend.engine.plan.GenericType getLambdaParamType(String paramName) {
            return lambdaParamTypes.getOrDefault(paramName, org.finos.legend.engine.plan.GenericType.Primitive.ANY);
        }

        /**
         * Creates a new context with a lambda parameter bound to a relation source.
         * Used for relation lambda expressions like {t | $t->select(~col)}.
         */
        public CompilationContext withRelationSource(String paramName, RelationNode source) {
            var newParams = new java.util.HashMap<>(lambdaParameters);
            newParams.put(paramName, "");
            var newSources = new java.util.HashMap<>(relationSources);
            newSources.put(paramName, source);
            return new CompilationContext(lambdaParameter, tableAlias, mapping, className,
                    inFilterContext, newParams, extendedColumns, newSources, symbols, paramOverrides, lambdaParamTypes);
        }

        /**
         * Creates a new context with fold parameters (accumulator and element).
         */
        public CompilationContext withFoldParameters(String accParam, String elemParam) {
            return withFoldParameters(accParam, elemParam, null);
        }

        /**
         * Creates a new context with fold parameters and optional element class name.
         * The class name enables at(0) optimization for scalar struct properties.
         */
        public CompilationContext withFoldParameters(String accParam, String elemParam, String elemClassName) {
            var newParams = new java.util.HashMap<>(lambdaParameters);
            newParams.put(accParam, "");
            newParams.put(elemParam, elemClassName != null ? elemClassName : "");
            return new CompilationContext(lambdaParameter, tableAlias, mapping, className,
                    inFilterContext, newParams, extendedColumns, relationSources, symbols, paramOverrides, lambdaParamTypes);
        }

        /**
         * Gets the class name associated with a lambda parameter (e.g., the element class in fold).
         * Returns null if the parameter has no associated class.
         */
        public String getLambdaParamClass(String paramName) {
            String val = lambdaParameters.get(paramName);
            return (val != null && !val.isEmpty()) ? val : null;
        }

        /**
         * Creates a new context with an extended column (from extend/flatten).
         * Extended columns should use unqualified references.
         */
        public CompilationContext withExtendedColumn(String columnName) {
            var newExtended = new java.util.HashSet<>(extendedColumns);
            newExtended.add(columnName);
            return new CompilationContext(lambdaParameter, tableAlias, mapping, className,
                    inFilterContext, lambdaParameters, newExtended, relationSources, symbols, paramOverrides, lambdaParamTypes);
        }

        /**
         * Creates a new context with a parameter expression override.
         * When a lambda parameter has an override, the override expression is emitted
         * instead of a plain ColumnReference. Used for fold element unwrapping.
         */
        public CompilationContext withParamOverride(String paramName, Expression expr) {
            var newOverrides = new java.util.HashMap<>(paramOverrides);
            newOverrides.put(paramName, expr);
            return new CompilationContext(lambdaParameter, tableAlias, mapping, className,
                    inFilterContext, lambdaParameters, extendedColumns, relationSources, symbols, newOverrides, lambdaParamTypes);
        }

        // ========== Symbol Table Methods (Phase 1 of Variable Binding) ==========

        /**
         * Creates a new context with a ROW binding for lambda row parameters.
         * Example: filter(t | $t.col > 5) binds "t" to table alias "t0"
         */
        public CompilationContext withRowBinding(String paramName, String alias) {
            var newSymbols = new java.util.HashMap<>(symbols);
            newSymbols.put(paramName, SymbolBinding.row(paramName, alias));
            // Also update lambdaParameter for backward compatibility
            return new CompilationContext(paramName, alias, mapping, className,
                    inFilterContext, lambdaParameters, extendedColumns, relationSources, newSymbols, paramOverrides, lambdaParamTypes);
        }

        /**
         * Creates a new context with a RELATION binding for let-bound relations.
         * Example: let tds = #TDS...#; $tds->filter(...) binds "tds" to the relation
         * node
         */
        public CompilationContext withRelationBinding(String paramName, RelationNode relation) {
            var newSymbols = new java.util.HashMap<>(symbols);
            newSymbols.put(paramName, SymbolBinding.relation(paramName, relation));
            return new CompilationContext(lambdaParameter, tableAlias, mapping, className,
                    inFilterContext, lambdaParameters, extendedColumns, relationSources, newSymbols, paramOverrides, lambdaParamTypes);
        }

        /**
         * Creates a new context with a SCALAR binding for let-bound scalars.
         * Example: let x = 5; $x + 1 binds "x" to the literal expression
         */
        public CompilationContext withScalarBinding(String paramName, Expression expr) {
            var newSymbols = new java.util.HashMap<>(symbols);
            newSymbols.put(paramName, SymbolBinding.scalar(paramName, expr));
            return new CompilationContext(lambdaParameter, tableAlias, mapping, className,
                    inFilterContext, lambdaParameters, extendedColumns, relationSources, newSymbols, paramOverrides, lambdaParamTypes);
        }

        /**
         * Looks up a symbol binding by name.
         * 
         * @return The binding, or null if not found
         */
        public SymbolBinding lookupSymbol(String name) {
            return symbols.get(name);
        }

        /**
         * Checks if a symbol is bound in the current context.
         */
        public boolean hasSymbol(String name) {
            return symbols.containsKey(name);
        }

        /**
         * Checks if a variable has a scalar binding (not a relation binding).
         */
        public boolean hasScalarBinding(String name) {
            SymbolBinding binding = symbols.get(name);
            return binding != null && binding.kind() == SymbolBinding.BindingKind.SCALAR;
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

        /**
         * Gets the relation source bound to a lambda parameter, if any.
         */
        public RelationNode getRelationSource(String paramName) {
            return relationSources.get(paramName);
        }

        /**
         * Checks if a variable represents a relation source.
         */
        public boolean isRelationSource(String paramName) {
            return relationSources.containsKey(paramName);
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
    private static final Set<String> SCALAR_METHODS = Set.of("at", "head", "first", "last");

    private static boolean isScalarMethod(String methodName) {
        // Strip qualified package prefix if present (e.g., meta::pure::...::head -> head)
        int lastColon = methodName.lastIndexOf(':');
        String simpleName = lastColon >= 0 ? methodName.substring(lastColon + 1) : methodName;
        return SCALAR_METHODS.contains(simpleName);
    }

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
