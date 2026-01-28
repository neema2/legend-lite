package org.finos.legend.engine.plan;

import org.finos.legend.pure.dsl.definition.ConnectionDefinition.DatabaseType;
import org.finos.legend.engine.transpiler.DuckDBDialect;
import org.finos.legend.engine.transpiler.SQLDialect;
import org.finos.legend.engine.transpiler.SQLGenerator;
import org.finos.legend.engine.transpiler.SQLiteDialect;

import java.util.Map;

/**
 * A self-contained execution plan that can be stored and executed later.
 * 
 * Contains:
 * - The logical plan (IR) as source of truth
 * - Pre-generated SQL for specified database targets
 * - Result schema for clients
 * - Reference to the Runtime
 * 
 * This is the artifact that flows from Plan Server to Execution Server.
 */
public record ExecutionPlan(
        String id, // Unique plan ID
        RelationNode ir, // Logical plan (source of truth)
        ResultSchema schema, // Result column metadata
        Map<String, GeneratedSql> sqlByStore, // store → SQL + dbType
        String runtimeRef // Runtime qualified name
) {

    /**
     * Gets the SQL for a specific store.
     * 
     * @param storeRef The store qualified name
     * @return The pre-generated SQL, or null if not found
     */
    public String getSql(String storeRef) {
        GeneratedSql gen = sqlByStore.get(storeRef);
        return gen != null ? gen.sql() : null;
    }

    /**
     * Gets the database type for a specific store.
     */
    public DatabaseType getDatabaseType(String storeRef) {
        GeneratedSql gen = sqlByStore.get(storeRef);
        return gen != null ? gen.databaseType() : null;
    }

    /**
     * Gets the SQL for a store, generating on-demand if not pre-generated.
     * 
     * @param storeRef     The store qualified name
     * @param databaseType The target database type
     * @return The SQL for the target
     */
    public String getSqlOrGenerate(String storeRef, DatabaseType databaseType) {
        GeneratedSql gen = sqlByStore.get(storeRef);
        if (gen != null && gen.databaseType() == databaseType) {
            return gen.sql();
        }
        // Generate on-demand
        SQLDialect dialect = getDialect(databaseType);
        return new SQLGenerator(dialect).generate(ir);
    }

    /**
     * Gets the first/default SQL (for single-store plans).
     */
    public String getDefaultSql() {
        if (sqlByStore.isEmpty()) {
            return null;
        }
        return sqlByStore.values().iterator().next().sql();
    }

    /**
     * Gets the first/default store reference (for single-store plans).
     */
    public String getDefaultStoreRef() {
        if (sqlByStore.isEmpty()) {
            return null;
        }
        return sqlByStore.keySet().iterator().next();
    }

    private static SQLDialect getDialect(DatabaseType databaseType) {
        return switch (databaseType) {
            case DuckDB -> DuckDBDialect.INSTANCE;
            case SQLite -> SQLiteDialect.INSTANCE;
            case H2, Postgres, Snowflake, BigQuery -> DuckDBDialect.INSTANCE;
        };
    }

    /**
     * Pre-generated SQL for a specific database type.
     */
    public record GeneratedSql(
            DatabaseType databaseType,
            String sql) {
    }
}
