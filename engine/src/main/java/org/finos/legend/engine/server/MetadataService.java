package org.finos.legend.engine.server;

import org.finos.legend.engine.plan.ExecutionPlan;
import org.finos.legend.engine.plan.RelationNode;
import org.finos.legend.engine.plan.ResultSchema;
import org.finos.legend.engine.store.MappingRegistry;
import org.finos.legend.engine.transpiler.DuckDBDialect;
import org.finos.legend.engine.transpiler.SQLDialect;
import org.finos.legend.engine.transpiler.SQLGenerator;
import org.finos.legend.engine.transpiler.SQLiteDialect;
import org.finos.legend.pure.dsl.PureCompiler;
import org.finos.legend.pure.dsl.definition.ConnectionDefinition;
import org.finos.legend.pure.dsl.definition.PureModelBuilder;
import org.finos.legend.pure.dsl.definition.RuntimeDefinition;
import org.finos.legend.pure.dsl.definition.ServiceDefinition;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

/**
 * Stateful metadata service that holds the compiled Pure model.
 * 
 * Use cases:
 * - Deploy model definitions (Classes, Mappings, Connections, Runtimes,
 * Services)
 * - Generate and cache plans for known Services
 * - Serve stored plans to execution servers
 * 
 * Example:
 * 
 * <pre>
 * MetadataService service = new MetadataService();
 * service.compile(classesSource);
 * service.compile(mappingsSource);
 * service.compile(serviceSource);
 * 
 * ExecutionPlan plan = service.generatePlan("app::PersonService");
 * </pre>
 */
public class MetadataService {

    private final PureModelBuilder model = new PureModelBuilder();
    private final Map<String, ExecutionPlan> planCache = new HashMap<>();

    /**
     * Compiles Pure source and registers definitions in the global model.
     * 
     * @param pureSource Pure source code (Classes, Mappings, Services, etc.)
     */
    public void compile(String pureSource) {
        model.addSource(pureSource);
    }

    /**
     * Generates an execution plan for a known Service.
     * 
     * Since Services don't bundle their runtime, a runtime name must be provided.
     * 
     * @param serviceName The qualified name of the Service
     * @param runtimeName The Runtime to use for execution
     * @return The execution plan
     */
    public ExecutionPlan generatePlan(String serviceName, String runtimeName) {
        String cacheKey = serviceName + "::" + runtimeName;

        // Check cache first
        if (planCache.containsKey(cacheKey)) {
            return planCache.get(cacheKey);
        }

        ServiceDefinition service = model.getService(serviceName);
        if (service == null) {
            throw new IllegalArgumentException("Service not found: " + serviceName);
        }

        RuntimeDefinition runtime = model.getRuntime(runtimeName);
        if (runtime == null) {
            throw new IllegalArgumentException("Runtime not found: " + runtimeName);
        }

        ExecutionPlan plan = buildPlanFromQuery(service.functionBody(), runtime);
        planCache.put(cacheKey, plan);
        return plan;
    }

    /**
     * Generates an execution plan for an ad-hoc query using a known Runtime.
     * 
     * @param query       The Pure query (e.g., "Person.all()->filter(...)")
     * @param runtimeName The qualified name of the Runtime to use
     * @return The execution plan
     */
    public ExecutionPlan generateQueryPlan(String query, String runtimeName) {
        RuntimeDefinition runtime = model.getRuntime(runtimeName);
        if (runtime == null) {
            throw new IllegalArgumentException("Runtime not found: " + runtimeName);
        }

        return buildPlanFromQuery(query, runtime);
    }

    private ExecutionPlan buildPlanFromQuery(String query, RuntimeDefinition runtime) {
        // Compile query to IR
        MappingRegistry mappingRegistry = model.getMappingRegistry();
        PureCompiler compiler = new PureCompiler(mappingRegistry, model);
        RelationNode ir = compiler.compile(query);

        // Generate SQL for each store's database type
        Map<String, ExecutionPlan.GeneratedSql> sqlByStore = new HashMap<>();

        for (Map.Entry<String, String> binding : runtime.connectionBindings().entrySet()) {
            String storeRef = binding.getKey();
            String connectionRef = binding.getValue();

            ConnectionDefinition connection = model.getConnection(connectionRef);
            if (connection != null) {
                SQLDialect dialect = getDialect(connection.databaseType());
                String sql = new SQLGenerator(dialect).generate(ir);
                sqlByStore.put(storeRef, new ExecutionPlan.GeneratedSql(connection.databaseType(), sql));
            }
        }

        // Build result schema (simplified - uses column names from projection)
        ResultSchema schema = new ResultSchema(java.util.List.of());

        return new ExecutionPlan(
                UUID.randomUUID().toString(),
                ir,
                schema,
                sqlByStore,
                runtime.qualifiedName());
    }

    private SQLDialect getDialect(ConnectionDefinition.DatabaseType dbType) {
        return switch (dbType) {
            case DuckDB -> DuckDBDialect.INSTANCE;
            case SQLite -> SQLiteDialect.INSTANCE;
            default -> DuckDBDialect.INSTANCE;
        };
    }
}
