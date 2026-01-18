package org.finos.legend.engine.server;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Map;

/**
 * Functional interface for executing a service request.
 * 
 * Implementations handle the full lifecycle:
 * 1. Parameter binding
 * 2. Query compilation
 * 3. SQL execution
 * 4. Result serialization
 */
@FunctionalInterface
public interface ServiceExecutor {

    /**
     * Executes the service with the given parameters.
     * 
     * @param pathParams  Path parameters extracted from the URL
     * @param queryParams Query string parameters
     * @param connection  Database connection to use
     * @return JSON result as a string
     * @throws Exception if execution fails
     */
    String execute(Map<String, String> pathParams,
            Map<String, String> queryParams,
            Connection connection) throws Exception;
}
