package org.finos.legend.engine.server;

import org.finos.legend.engine.plan.InsertNode;
import org.finos.legend.engine.plan.MutationNode;
import org.finos.legend.engine.plan.WriteResult;
import org.finos.legend.engine.transpiler.DDLGenerator;
import org.finos.legend.engine.transpiler.MutationSQLGenerator;
import org.finos.legend.pure.dsl.DeleteExpression;
import org.finos.legend.pure.dsl.InstanceExpression;
import org.finos.legend.engine.store.MappingRegistry;
import org.finos.legend.pure.dsl.PureCompiler;
import org.finos.legend.pure.dsl.PureExpression;
import org.finos.legend.pure.dsl.PureParser;
import org.finos.legend.pure.dsl.SaveExpression;
import org.finos.legend.pure.dsl.UpdateExpression;
import org.finos.legend.pure.dsl.definition.ClassDefinition;
import org.finos.legend.pure.dsl.definition.PureDefinition;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Service for executing write (mutation) operations with transaction support.
 * 
 * All operations use implicit transactions:
 * - autoCommit = false
 * - Commit on success
 * - Rollback on failure
 * 
 * Usage (matching QueryService pattern):
 * 
 * <pre>
 * String pureSource = """
 *         Class model::Person {
 *             firstName: String[1];
 *             lastName: String[1];
 *             age: Integer[0..1];
 *         }
 *         """;
 * 
 * WriteService service = new WriteService();
 * WriteResult result = service.execute(
 *         pureSource,
 *         "^Person(firstName='John', lastName='Doe')->save()",
 *         connection);
 * </pre>
 */
public class WriteService {

    /**
     * Compiles Pure source, validates class, auto-creates table, and executes
     * mutation.
     * 
     * This is the main entry point matching QueryService.execute() pattern.
     * 
     * @param pureSource The Pure source containing class definitions
     * @param query      The Pure mutation query (save, update, or delete)
     * @param connection Database connection
     * @return WriteResult with rowsAffected and generated keys
     * @throws WriteException If the operation fails
     */
    public WriteResult execute(String pureSource, String query, Connection connection) {
        // Parse model definitions
        Map<String, ClassDefinition> classRegistry = parseClasses(pureSource);

        // Parse mutation query
        PureExpression expr = PureParser.parse(query);

        // Extract class name based on expression type
        String className = switch (expr) {
            case SaveExpression save -> getClassNameFromSave(save);
            case UpdateExpression update -> getClassNameFromSource(update.source());
            case DeleteExpression delete -> getClassNameFromSource(delete.source());
            default -> throw new WriteException(
                    "Expected mutation expression (save/update/delete), got: " + expr.getClass().getSimpleName());
        };

        // Validate class exists
        ClassDefinition classDef = classRegistry.get(className);
        if (classDef == null) {
            throw new WriteException("Class not found: " + className + ". Available: " + classRegistry.keySet());
        }

        // Auto-create table if not exists
        ensureTable(classDef, connection);

        // Compile to MutationNode IR using PureCompiler
        PureCompiler compiler = new PureCompiler(new MappingRegistry());
        MutationNode mutation = compiler.compileMutation(expr);

        // Generate SQL and execute
        return execute(mutation, connection);
    }

    /**
     * Ensures the table for a class exists, creating it if necessary.
     */
    public void ensureTable(ClassDefinition classDef, Connection connection) {
        String ddl = DDLGenerator.generateCreateTable(classDef);
        try (Statement stmt = connection.createStatement()) {
            stmt.execute(ddl);
        } catch (SQLException e) {
            throw new WriteException("Failed to create table: " + e.getMessage(), e);
        }
    }

    /**
     * Parses class definitions from Pure source.
     */
    private Map<String, ClassDefinition> parseClasses(String pureSource) {
        Map<String, ClassDefinition> classes = new HashMap<>();

        // Use high-level parser API - no grammar-specific types needed
        List<PureDefinition> definitions = org.finos.legend.pure.dsl.PureParser.parseToDefinitions(pureSource);

        for (PureDefinition def : definitions) {
            if (def instanceof ClassDefinition classDef) {
                classes.put(classDef.simpleName(), classDef);
                classes.put(classDef.qualifiedName(), classDef);
            }
        }
        return classes;
    }

    private String getClassNameFromSave(SaveExpression save) {
        if (save.instance() instanceof InstanceExpression instance) {
            return instance.className();
        }
        throw new WriteException("Cannot determine class name from save expression");
    }

    private String getClassNameFromSource(PureExpression source) {
        return switch (source) {
            case org.finos.legend.pure.dsl.ClassAllExpression cae -> cae.className();
            case org.finos.legend.pure.dsl.ClassFilterExpression cfe -> getClassNameFromSource(cfe.source());
            default -> throw new WriteException("Cannot extract class from: " + source.getClass().getSimpleName());
        };
    }

    /**
     * Executes a mutation operation with transaction support.
     */
    public WriteResult execute(MutationNode mutation, Connection connection) {
        String sql = MutationSQLGenerator.generate(mutation);
        return executeWithTransaction(sql, mutation instanceof InsertNode, connection);
    }

    /**
     * Executes raw SQL with transaction support.
     */
    public WriteResult executeWithTransaction(String sql, boolean returnGeneratedKeys, Connection connection) {
        boolean originalAutoCommit = true;

        try {
            originalAutoCommit = connection.getAutoCommit();
            connection.setAutoCommit(false);

            int rowsAffected;
            List<Object> generatedKeys = new ArrayList<>();

            try (Statement stmt = connection.createStatement()) {
                rowsAffected = stmt.executeUpdate(sql);
            }

            connection.commit();
            return WriteResult.success(rowsAffected, generatedKeys);

        } catch (SQLException e) {
            try {
                connection.rollback();
            } catch (SQLException rollbackEx) {
                e.addSuppressed(rollbackEx);
            }
            throw new WriteException("Write operation failed: " + e.getMessage(), e);

        } finally {
            try {
                connection.setAutoCommit(originalAutoCommit);
            } catch (SQLException e) {
                // Best effort
            }
        }
    }

    /**
     * Exception thrown when a write operation fails.
     */
    public static class WriteException extends RuntimeException {
        public WriteException(String message, Throwable cause) {
            super(message, cause);
        }

        public WriteException(String message) {
            super(message);
        }
    }
}
