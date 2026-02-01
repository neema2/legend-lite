package org.finos.legend.engine.transpiler.json;

import org.finos.legend.engine.plan.*;
import org.finos.legend.engine.transpiler.DuckDBDialect;
import org.finos.legend.engine.transpiler.SQLGenerator;
import org.finos.legend.pure.dsl.GraphFetchTree;

import java.util.ArrayList;
import java.util.List;

/**
 * Generates JSON-producing SQL for graphFetch queries.
 * 
 * Instead of returning tabular data, generates SQL that returns
 * a single JSON column directly from the database.
 * 
 * Example output for flat properties:
 * 
 * <pre>
 * SELECT json_group_array(
 *     json_object('fullName', t0.FIRST_NAME || ' ' || t0.LAST_NAME, 'age', t0.AGE)
 * ) AS result FROM T_PERSON t0
 * </pre>
 * 
 * Example output for nested 1-to-1:
 * 
 * <pre>
 * SELECT json_group_array(
 *     json_object('fullName', ..., 'address', json_object('city', a.CITY))
 * ) AS result 
 * FROM T_PERSON p LEFT JOIN T_ADDRESS a ON ...
 * </pre>
 * 
 * Example output for 1-to-many:
 * 
 * <pre>
 * SELECT json_group_array(
 *     json_object('fullName', ..., 'addresses', (
 *         SELECT json_group_array(json_object('city', a.CITY))
 *         FROM T_ADDRESS a WHERE a.PERSON_ID = p.ID
 *     ))
 * ) AS result FROM T_PERSON p
 * </pre>
 */
public final class JsonSqlGenerator {

    private final JsonSqlDialect dialect;
    private final SQLGenerator sqlGenerator;

    public JsonSqlGenerator(JsonSqlDialect dialect) {
        this.dialect = dialect;
        this.sqlGenerator = new SQLGenerator(DuckDBDialect.INSTANCE);
    }

    /**
     * Generates JSON-producing SQL from a ProjectNode (flat properties only).
     * 
     * @param projectNode The projection IR node
     * @return SQL that returns a JSON array as a single column
     */
    public String generateJsonSql(ProjectNode projectNode) {
        // Build the json_object() call for each row
        String jsonObjectExpr = buildJsonObject(projectNode.projections());

        // Get the FROM clause from the source
        String fromClause = generateFromClause(projectNode.source());

        // Build WHERE clause if there's a filter
        String whereClause = generateWhereClause(projectNode.source());

        // Wrap in json_group_array for final array result
        StringBuilder sql = new StringBuilder();
        sql.append("SELECT ").append(dialect.jsonArrayAgg(jsonObjectExpr)).append(" AS result");
        sql.append("\n").append(fromClause);
        if (!whereClause.isEmpty()) {
            sql.append("\n").append(whereClause);
        }

        return sql.toString();
    }

    /**
     * Generates JSON-producing SQL from a GraphFetchTree and base ProjectNode.
     * Handles nested properties for deep fetch.
     * 
     * @param tree     The graphFetch tree specification
     * @param baseNode The base projection node (source + M2M transforms)
     * @return SQL that returns nested JSON
     */
    public String generateDeepFetchSql(GraphFetchTree tree, ProjectNode baseNode) {
        // For now, delegate to flat generation
        // TODO: Handle nested properties for 1-to-1 and 1-to-many
        return generateJsonSql(baseNode);
    }

    /**
     * Builds a json_object() expression from projections.
     */
    private String buildJsonObject(List<Projection> projections) {
        List<String> keyValuePairs = new ArrayList<>();

        for (Projection proj : projections) {
            // Key: the alias (property name)
            keyValuePairs.add(dialect.quotedKey(proj.alias()));
            // Value: the SQL expression
            keyValuePairs.add(generateExpression(proj.expression()));
        }

        return dialect.jsonObject(keyValuePairs.toArray(new String[0]));
    }

    /**
     * Generates SQL for an expression.
     */
    private String generateExpression(Expression expr) {
        // Use the standard SQLGenerator to render expressions
        return sqlGenerator.generateExpression(expr);
    }

    /**
     * Generates FROM clause from relation node.
     */
    private String generateFromClause(RelationNode node) {
        return switch (node) {
            case TableNode table ->
                "FROM " + quoteIdentifier(table.table().name()) + " AS " + quoteIdentifier(table.alias());
            case FilterNode filter ->
                generateFromClause(filter.source());
            case JoinNode join ->
                generateJoinFromClause(join);
            default ->
                throw new UnsupportedOperationException("Unsupported source node: " + node.getClass());
        };
    }

    /**
     * Generates JOIN clauses.
     */
    private String generateJoinFromClause(JoinNode join) {
        String leftFrom = generateFromClause(join.left());
        String rightTable = switch (join.right()) {
            case TableNode table ->
                quoteIdentifier(table.table().name()) + " AS " + quoteIdentifier(table.alias());
            default ->
                throw new UnsupportedOperationException("Right side of join must be TableNode");
        };

        String joinType = switch (join.joinType()) {
            case INNER -> "INNER JOIN";
            case LEFT_OUTER -> "LEFT JOIN";
            case RIGHT_OUTER -> "RIGHT JOIN";
            case FULL_OUTER -> "FULL OUTER JOIN";
            case ASOF_LEFT -> "ASOF LEFT JOIN";
        };

        String condition = generateExpression(join.condition());

        return leftFrom + "\n" + joinType + " " + rightTable + " ON " + condition;
    }

    /**
     * Extracts WHERE clause from a filter node if present.
     */
    private String generateWhereClause(RelationNode node) {
        if (node instanceof FilterNode filter) {
            return "WHERE " + generateExpression(filter.condition());
        }
        return "";
    }

    private String quoteIdentifier(String identifier) {
        return "\"" + identifier + "\"";
    }
}
