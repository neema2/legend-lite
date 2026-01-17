package org.finos.legend.pure.dsl;

import org.finos.legend.engine.plan.*;
import org.finos.legend.engine.store.*;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Compiles Pure AST expressions into RelationNode execution plans.
 * 
 * This is the bridge between the Pure language and the SQL transpiler.
 */
public final class PureCompiler {
    
    private final MappingRegistry mappingRegistry;
    private int aliasCounter = 0;
    
    public PureCompiler(MappingRegistry mappingRegistry) {
        this.mappingRegistry = Objects.requireNonNull(mappingRegistry, "Mapping registry cannot be null");
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
        
        // Get the mapping for the class
        RelationalMapping mapping = getMappingFromSource(filter.source());
        
        // Create context for the lambda
        CompilationContext lambdaContext = new CompilationContext(
                filter.lambda().parameter(),
                tableAlias,
                mapping
        );
        
        // Compile the filter condition
        Expression condition = compileToSqlExpression(filter.lambda().body(), lambdaContext);
        
        return new FilterNode(source, condition);
    }
    
    private RelationNode compileProject(ProjectExpression project, CompilationContext outerContext) {
        RelationNode source = compileExpression(project.source(), outerContext);
        
        // Get the table alias from the source
        String tableAlias = getTableAlias(source);
        
        // Get the mapping for the class
        RelationalMapping mapping = getMappingFromSource(project.source());
        
        List<Projection> projections = new ArrayList<>();
        
        for (int i = 0; i < project.projections().size(); i++) {
            LambdaExpression lambda = project.projections().get(i);
            
            // Create context for this lambda
            CompilationContext lambdaContext = new CompilationContext(
                    lambda.parameter(),
                    tableAlias,
                    mapping
            );
            
            // The lambda body should be a property access
            String propertyName = extractPropertyName(lambda.body());
            String columnName = mapping.getColumnForProperty(propertyName)
                    .orElseThrow(() -> new PureCompileException("No column mapping for property: " + propertyName));
            
            // Determine alias
            String alias;
            if (i < project.aliases().size()) {
                alias = project.aliases().get(i);
            } else {
                alias = propertyName;
            }
            
            projections.add(Projection.column(tableAlias, columnName, alias));
        }
        
        return new ProjectNode(source, projections);
    }
    
    /**
     * Compiles a Pure expression to a SQL expression.
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
        // Extract the variable name to verify it matches the lambda parameter
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
            case JoinNode join -> getTableAlias(join.left()); // Use left table alias for joins
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
    
    /**
     * Context for compiling within a lambda expression.
     */
    public record CompilationContext(
            String lambdaParameter,
            String tableAlias,
            RelationalMapping mapping
    ) {}
}
