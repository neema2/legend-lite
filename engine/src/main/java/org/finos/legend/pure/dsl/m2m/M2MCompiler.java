package org.finos.legend.pure.dsl.m2m;

import org.finos.legend.engine.plan.*;
import org.finos.legend.engine.store.RelationalMapping;
import org.finos.legend.pure.dsl.PureCompileException;

import java.util.ArrayList;
import java.util.List;

/**
 * Compiles M2M mappings to RelationNode execution plans.
 * 
 * This enables Model-to-Model transforms to execute entirely in the database
 * by compiling Pure M2M expressions to SQL.
 * 
 * Example M2M mapping:
 * 
 * <pre>
 * Person: Pure
 * {
 *     ~src RawPerson
 *     fullName: $src.firstName + ' ' + $src.lastName,
 *     upperName: $src.firstName->toUpper()
 * }
 * </pre>
 * 
 * Compiles to SQL like:
 * 
 * <pre>
 * SELECT 
 *     t0.FIRST_NAME || ' ' || t0.LAST_NAME AS fullName,
 *     UPPER(t0.FIRST_NAME) AS upperName
 * FROM T_RAW_PERSON AS t0
 * </pre>
 */
public final class M2MCompiler {

    /**
     * Functional interface for resolving association references.
     * When an M2M property is mapped via @JoinName, this resolver
     * is called to produce a nested JsonObjectExpression.
     */
    @FunctionalInterface
    public interface AssociationResolver {
        /**
         * Resolves an AssociationRef to an expression.
         * 
         * @param assocRef     The association reference (e.g., @PersonAddress)
         * @param propertyName The property name being mapped (e.g., "address")
         * @return A JsonObjectExpression for the nested object
         */
        Expression resolve(AssociationRef assocRef, String propertyName);
    }

    private final RelationalMapping sourceMapping;
    private final String tableAlias;
    private AssociationResolver associationResolver;

    /**
     * Creates a compiler for an M2M mapping.
     * 
     * @param sourceMapping The relational mapping for the source class
     * @param tableAlias    The alias to use for the source table
     */
    public M2MCompiler(RelationalMapping sourceMapping, String tableAlias) {
        this.sourceMapping = sourceMapping;
        this.tableAlias = tableAlias;
    }

    /**
     * Sets the association resolver for deep fetch compilation.
     * 
     * @param resolver The resolver to use for @JoinName references
     * @return this compiler for chaining
     */
    public M2MCompiler withAssociationResolver(AssociationResolver resolver) {
        this.associationResolver = resolver;
        return this;
    }

    /**
     * Compiles an M2M class mapping to a RelationNode.
     * 
     * @param classMapping The M2M class mapping
     * @return The compiled RelationNode (typically ProjectNode)
     */
    public RelationNode compile(M2MClassMapping classMapping) {
        // Create the base table node
        TableNode tableNode = new TableNode(sourceMapping.table(), tableAlias);
        RelationNode source = tableNode;

        // Apply filter if present
        if (classMapping.filter() != null) {
            Expression filterExpr = compileExpression(classMapping.filter());
            source = new FilterNode(source, filterExpr);
        }

        // Compile projections
        List<Projection> projections = new ArrayList<>();
        for (M2MPropertyMapping pm : classMapping.propertyMappings()) {
            Expression expr;

            // Check if this is an association reference (deep fetch)
            if (pm.expression() instanceof AssociationRef assocRef) {
                if (associationResolver != null) {
                    expr = associationResolver.resolve(assocRef, pm.propertyName());
                } else {
                    throw new PureCompileException(
                            "Association reference @" + assocRef.joinName() +
                                    " requires an AssociationResolver. " +
                                    "Use M2MCompiler.withAssociationResolver() for deep fetch.");
                }
            } else {
                expr = compileExpression(pm.expression());
            }

            projections.add(new Projection(expr, pm.propertyName()));
        }

        return new ProjectNode(source, projections);
    }

    /**
     * Compiles an M2M expression to a SQL expression.
     */
    public Expression compileExpression(M2MExpression expr) {
        return expr.accept(new M2MExpressionVisitor<>() {

            @Override
            public Expression visit(SourcePropertyRef ref) {
                if (!ref.isSimple()) {
                    throw new PureCompileException("Chained property access not yet supported in M2M: " + ref);
                }

                String propertyName = ref.firstProperty();
                String columnName = sourceMapping.getColumnForProperty(propertyName)
                        .orElseThrow(() -> new PureCompileException(
                                "No column mapping for property: " + propertyName));

                return ColumnReference.of(tableAlias, columnName, sourceMapping.pureTypeForProperty(propertyName));
            }

            @Override
            public Expression visit(M2MLiteral literal) {
                return switch (literal.type()) {
                    case STRING -> Literal.string((String) literal.value());
                    case INTEGER -> Literal.integer(((Number) literal.value()).longValue());
                    case FLOAT -> new Literal(literal.value(), Literal.LiteralType.DOUBLE);
                    case BOOLEAN -> Literal.bool((Boolean) literal.value());
                };
            }

            @Override
            public Expression visit(StringConcatExpr concat) {
                List<Expression> compiledParts = concat.parts().stream()
                        .map(M2MCompiler.this::compileExpression)
                        .toList();
                return new ConcatExpression(compiledParts);
            }

            @Override
            public Expression visit(FunctionCallExpr functionCall) {
                Expression target = compileExpression(functionCall.target());
                String funcName = functionCall.functionName().toLowerCase();

                List<Expression> args = functionCall.arguments().stream()
                        .map(M2MCompiler.this::compileExpression)
                        .toList();

                return new FunctionExpression(funcName, target, args, target.type());
            }

            @Override
            public Expression visit(IfExpr ifExpr) {
                Expression condition = compileExpression(ifExpr.condition());
                Expression thenBranch = compileExpression(ifExpr.thenBranch());
                Expression elseBranch = compileExpression(ifExpr.elseBranch());

                return new CaseExpression(condition, thenBranch, elseBranch);
            }

            @Override
            public Expression visit(BinaryArithmeticExpr arithmetic) {
                Expression left = compileExpression(arithmetic.left());
                Expression right = compileExpression(arithmetic.right());

                return new ArithmeticExpression(left, arithmetic.operator(), right);
            }

            @Override
            public Expression visit(M2MComparisonExpr comparison) {
                Expression left = compileExpression(comparison.left());
                Expression right = compileExpression(comparison.right());

                ComparisonExpression.ComparisonOperator op = switch (comparison.operator()) {
                    case EQUALS -> ComparisonExpression.ComparisonOperator.EQUALS;
                    case NOT_EQUALS -> ComparisonExpression.ComparisonOperator.NOT_EQUALS;
                    case LESS_THAN -> ComparisonExpression.ComparisonOperator.LESS_THAN;
                    case LESS_THAN_OR_EQUALS -> ComparisonExpression.ComparisonOperator.LESS_THAN_OR_EQUALS;
                    case GREATER_THAN -> ComparisonExpression.ComparisonOperator.GREATER_THAN;
                    case GREATER_THAN_OR_EQUALS -> ComparisonExpression.ComparisonOperator.GREATER_THAN_OR_EQUALS;
                };

                return new ComparisonExpression(left, op, right);
            }

            @Override
            public Expression visit(AssociationRef associationRef) {
                // Association references are handled at a higher level during deep fetch
                // JSON SQL generation, not during regular property expression compilation.
                // This method should not be called directly.
                throw new PureCompileException(
                        "Association reference @" + associationRef.joinName() +
                                " must be handled during deep fetch compilation, not as a scalar expression");
            }
        });
    }
}
