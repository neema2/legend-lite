package org.finos.legend.pure.dsl;

import java.util.List;

/**
 * Project columns from a Relation with optional transformations.
 * 
 * Syntax: relation->project(~[col1:x|$x.col1, col2:x|$x.col2->toLower()])
 * 
 * This is the relation-based project, distinct from class-based
 * ProjectExpression.
 * Each column spec can include a lambda for transformation.
 * 
 * Example:
 * 
 * <pre>
 * #TDS
 *     id,name
 *     1,George
 * #->project(~[id:x|$x.id, name:x|$x.name->toLower()])
 * </pre>
 */
public record RelationProjectExpression(
        PureExpression source,
        List<LambdaExpression> projections,
        List<String> aliases) implements RelationExpression {
}
