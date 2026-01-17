package org.finos.legend.pure.dsl;

import org.finos.legend.engine.plan.*;
import org.finos.legend.engine.store.*;
import org.finos.legend.pure.dsl.ModelContext.AssociationNavigation;

import java.util.ArrayList;
import java.util.List;
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
     * @param expr The Pure expression
     * @param context Compilation context (for lambda parameter resolution)
     * @return The compiled RelationNode
     */
    public RelationNode compileExpression(PureExpression expr, CompilationContext context) {
        return switch (expr) {
            case ClassAllExpression classAll -> compileClassAll(classAll);
            case FilterExpression filter -> compileFilter(filter, context);
            case ProjectExpression project -> compileProject(project, context);
            default -> throw new PureCompileException("Cannot compile expression to RelationNode: " + expr);
        };
    }
    
    private RelationNode compileClassAll(ClassAllExpression classAll) {
        RelationalMapping mapping = mappingRegistry.findByClassName(classAll.className())
                .orElseThrow(() -> new PureCompileException("No mapping found for class: " + classAll.className()));
        
        String alias = "t" + aliasCounter++;
        return new TableNode(mapping.table(), alias);
    }
    
    private RelationNode compileFilter(FilterExpression filter, CompilationContext outerContext) {
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
                true  // We're in a filter context
        );
        
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
                    false
            );
            
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
                // Simple property access
                String propertyName = extractPropertyName(lambda.body());
                String columnName = baseMapping.getColumnForProperty(propertyName)
                        .orElseThrow(() -> new PureCompileException("No column mapping for property: " + propertyName));
                projections.add(Projection.column(baseTableAlias, columnName, alias));
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
                    ColumnReference.of(ji.alias(), ji.rightColumn())
            );
            finalSource = new JoinNode(finalSource, targetTable, joinCondition, JoinNode.JoinType.LEFT_OUTER);
        }
        
        return new ProjectNode(finalSource, projections);
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
        String columnName = targetMapping.getColumnForProperty(finalProperty)
                .orElseThrow(() -> new PureCompileException("No column mapping for: " + finalProperty));
        
        return Projection.column(targetAlias, columnName, projectionAlias);
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
            String rightColumn
    ) {}
    
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
            case VariableExpr var -> throw new PureCompileException("Unexpected variable in expression context: " + var);
            default -> throw new PureCompileException("Cannot compile to SQL expression: " + expr);
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
                        targetMapping
                ));
                
                currentClassName = targetClassName;
                currentMapping = targetMapping;
            } else {
                // This is a regular property access (final segment)
                navigationSegments.add(new NavigationSegment(
                        propName,
                        null,
                        currentMapping,
                        null
                ));
            }
        }
        
        return new AssociationPath(var.name(), navigationSegments, hasToMany);
    }
    
    /**
     * Compiles a comparison that involves to-many navigation into an EXISTS expression.
     * 
     * For: $p.addresses.street == 'Main St'
     * Generates: EXISTS (SELECT 1 FROM T_ADDRESS a WHERE a.PERSON_ID = p.ID AND a.STREET = 'Main St')
     */
    private Expression compileToManyComparison(AssociationPath path, ComparisonExpr.Operator op, 
                                                PureExpression rightExpr, CompilationContext context) {
        // Build the EXISTS subquery
        // We need to navigate through the association chain and build the correlated subquery
        
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
                ColumnReference.of(context.tableAlias(), outerColumn)
        );
        
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
        // For simple property access (not through associations), just map to column
        String varName = extractVariableName(propAccess.source());
        
        if (!varName.equals(context.lambdaParameter())) {
            throw new PureCompileException("Unknown variable: $" + varName + ", expected: $" + context.lambdaParameter());
        }
        
        String propertyName = propAccess.propertyName();
        String columnName = context.mapping().getColumnForProperty(propertyName)
                .orElseThrow(() -> new PureCompileException("No column mapping for property: " + propertyName));
        
        return ColumnReference.of(context.tableAlias(), columnName);
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
        };
    }
    
    private RelationalMapping getMappingFromSource(PureExpression source) {
        return switch (source) {
            case ClassAllExpression classAll -> mappingRegistry.findByClassName(classAll.className())
                    .orElseThrow(() -> new PureCompileException("No mapping for class: " + classAll.className()));
            case FilterExpression filter -> getMappingFromSource(filter.source());
            case ProjectExpression project -> getMappingFromSource(project.source());
            default -> throw new PureCompileException("Cannot determine mapping from: " + source);
        };
    }
    
    private String getClassNameFromSource(PureExpression source) {
        return switch (source) {
            case ClassAllExpression classAll -> classAll.className();
            case FilterExpression filter -> getClassNameFromSource(filter.source());
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
            boolean inFilterContext
    ) {
        /**
         * Legacy constructor for backwards compatibility.
         */
        public CompilationContext(String lambdaParameter, String tableAlias, RelationalMapping mapping) {
            this(lambdaParameter, tableAlias, mapping, mapping.pureClass().name(), false);
        }
    }
    
    /**
     * Represents a navigation path through properties and associations.
     */
    private record AssociationPath(
            String baseVariable,
            List<NavigationSegment> segments,
            boolean hasToManyNavigation
    ) {}
    
    /**
     * Represents a single segment in a navigation path.
     */
    private record NavigationSegment(
            String propertyName,
            AssociationNavigation navigation,
            RelationalMapping sourceMapping,
            RelationalMapping targetMapping
    ) {
        boolean isAssociationNavigation() {
            return navigation != null;
        }
    }
}
