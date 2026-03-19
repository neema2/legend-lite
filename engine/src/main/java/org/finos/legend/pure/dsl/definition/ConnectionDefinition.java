package org.finos.legend.pure.dsl.definition;

import org.finos.legend.engine.transpiler.DuckDBDialect;
import org.finos.legend.engine.transpiler.SQLDialect;
import org.finos.legend.engine.transpiler.SQLiteDialect;

/**
 * Represents a relational database connection definition.
 * 
 * Pure syntax:
 * ```
 * ###Connection
 * RelationalDatabaseConnection store::InMemoryDuckDb
 * {
 * store: store::PersonDb;
 * type: DuckDB;
 * specification: InMemory {};
 * auth: NoAuth {};
 * }
 * ```
 */
public record ConnectionDefinition(
        String qualifiedName,
        String storeName,
        DatabaseType databaseType,
        ConnectionSpecification specification,
        AuthenticationSpec authentication) implements PackageableElement {

    /**
     * Supported database types.
     */
    public enum DatabaseType {
        DuckDB,
        SQLite,
        H2,
        Postgres,
        Snowflake,
        BigQuery
    }

    /**
     * Returns the SQL dialect for this connection's database type.
     */
    public SQLDialect dialect() {
        return switch (databaseType) {
            case DuckDB -> DuckDBDialect.INSTANCE;
            case SQLite -> SQLiteDialect.INSTANCE;
            default -> DuckDBDialect.INSTANCE;
        };
    }

    /**
     * Returns the simple name (last part after ::).
     */
    public String simpleName() {
        int idx = qualifiedName.lastIndexOf("::");
        return idx >= 0 ? qualifiedName.substring(idx + 2) : qualifiedName;
    }

    /**
     * Returns the package path (everything before the last ::).
     */
    public String packagePath() {
        int idx = qualifiedName.lastIndexOf("::");
        return idx >= 0 ? qualifiedName.substring(0, idx) : "";
    }
}
