package org.finos.legend.engine.transpiler;

import org.finos.legend.engine.plan.DeleteNode;
import org.finos.legend.engine.plan.Expression;
import org.finos.legend.engine.plan.InsertNode;
import org.finos.legend.engine.plan.Literal;
import org.finos.legend.engine.plan.MutationNode;
import org.finos.legend.engine.plan.MutationNodeVisitor;
import org.finos.legend.engine.plan.UpdateNode;

import java.util.stream.Collectors;

/**
 * SQL generator for mutation (write) operations.
 * 
 * Generates INSERT, UPDATE, DELETE SQL statements from MutationNode IR.
 */
public class MutationSQLGenerator implements MutationNodeVisitor<String> {

    private static final MutationSQLGenerator INSTANCE = new MutationSQLGenerator();

    /**
     * Generates SQL for a mutation node.
     */
    public static String generate(MutationNode node) {
        return node.accept(INSTANCE);
    }

    @Override
    public String visit(InsertNode insert) {
        String tableName = "\"" + insert.table().name() + "\"";

        String columns = insert.columns().stream()
                .map(col -> "\"" + col + "\"")
                .collect(Collectors.joining(", "));

        String values = insert.values().stream()
                .map(this::expressionToSql)
                .collect(Collectors.joining(", "));

        return "INSERT INTO " + tableName + " (" + columns + ") VALUES (" + values + ")";
    }

    @Override
    public String visit(UpdateNode update) {
        String tableName = "\"" + update.table().name() + "\"";

        String setClause = update.setClause().entrySet().stream()
                .map(e -> "\"" + e.getKey() + "\" = " + expressionToSql(e.getValue()))
                .collect(Collectors.joining(", "));

        StringBuilder sql = new StringBuilder();
        sql.append("UPDATE ").append(tableName);
        sql.append(" SET ").append(setClause);

        if (update.whereClause() != null) {
            sql.append(" WHERE ").append(expressionToSql(update.whereClause()));
        }

        return sql.toString();
    }

    @Override
    public String visit(DeleteNode delete) {
        String tableName = "\"" + delete.table().name() + "\"";

        StringBuilder sql = new StringBuilder();
        sql.append("DELETE FROM ").append(tableName);

        if (delete.whereClause() != null) {
            sql.append(" WHERE ").append(expressionToSql(delete.whereClause()));
        }

        return sql.toString();
    }

    private String expressionToSql(Expression expr) {
        if (expr instanceof Literal lit) {
            return literalToSql(lit);
        }
        // For complex expressions, use SQLGenerator
        SQLGenerator sqlGen = new SQLGenerator(DuckDBDialect.INSTANCE);
        return expr.accept(sqlGen);
    }

    private String literalToSql(Literal lit) {
        if (lit.type() == Literal.LiteralType.NULL) {
            return "NULL";
        }
        if (lit.type() == Literal.LiteralType.STRING) {
            String s = (String) lit.value();
            return "'" + s.replace("'", "''") + "'";
        }
        if (lit.type() == Literal.LiteralType.BOOLEAN) {
            return lit.value().toString().toUpperCase();
        }
        return lit.value().toString();
    }
}
