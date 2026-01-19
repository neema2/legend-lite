package org.finos.legend.engine.plan;

import org.finos.legend.engine.store.MappingRegistry;
import org.finos.legend.engine.transpiler.DuckDBDialect;
import org.finos.legend.engine.transpiler.SQLDialect;
import org.finos.legend.engine.transpiler.SQLGenerator;
import org.finos.legend.engine.transpiler.SQLiteDialect;
import org.finos.legend.pure.dsl.PureCompiler;
import org.finos.legend.pure.dsl.definition.ConnectionDefinition;
import org.finos.legend.pure.dsl.definition.ConnectionDefinition.DatabaseType;
import org.finos.legend.pure.dsl.definition.PureModelBuilder;
import org.finos.legend.pure.dsl.definition.RuntimeDefinition;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * Fluent builder for creating ExecutionPlan instances.
 * 
 * Uses Runtime as the single source of truth for mappings and connections.
 * Automatically generates SQL for all database types referenced in the Runtime.
 * 
 * Example:
 * 
 * <pre>
 * ExecutionPlan plan = PlanBuilder.fromQuery("Person.all()->filter(...)")
 *         .withRuntime(myRuntime)
 *         .build();
 * </pre>
 */
public class PlanBuilder {

    private final String pureQuery;
    private RuntimeDefinition runtime;
    private PureModelBuilder modelBuilder;
    private Map<String, ConnectionDefinition> connections = new HashMap<>();

    private PlanBuilder(String pureQuery) {
        this.pureQuery = pureQuery;
    }

    /**
     * Creates a PlanBuilder from a Pure query string.
     */
    public static PlanBuilder fromQuery(String pureQuery) {
        return new PlanBuilder(pureQuery);
    }

    /**
     * Sets the Runtime for this plan.
     * The Runtime contains mappings and connection bindings.
     */
    public PlanBuilder withRuntime(RuntimeDefinition runtime) {
        this.runtime = runtime;
        return this;
    }

    /**
     * Sets the model builder containing all Pure definitions.
     */
    public PlanBuilder withModelBuilder(PureModelBuilder modelBuilder) {
        this.modelBuilder = modelBuilder;
        return this;
    }

    /**
     * Adds a connection definition.
     */
    public PlanBuilder withConnection(ConnectionDefinition connection) {
        this.connections.put(connection.qualifiedName(), connection);
        this.connections.put(connection.simpleName(), connection);
        return this;
    }

    /**
     * Adds multiple connection definitions.
     */
    public PlanBuilder withConnections(Map<String, ConnectionDefinition> connections) {
        this.connections.putAll(connections);
        return this;
    }

    /**
     * Builds the ExecutionPlan.
     * 
     * 1. Compiles the Pure query to IR (RelationNode)
     * 2. Extracts result schema from IR
     * 3. Generates SQL for each store's database type
     * 
     * @return The complete ExecutionPlan
     */
    public ExecutionPlan build() {
        if (runtime == null) {
            throw new IllegalStateException("Runtime is required");
        }
        if (modelBuilder == null) {
            throw new IllegalStateException("ModelBuilder is required");
        }

        // 1. Compile Pure query to IR
        MappingRegistry mappingRegistry = modelBuilder.getMappingRegistry();
        PureCompiler compiler = new PureCompiler(mappingRegistry, modelBuilder);
        RelationNode ir = compiler.compile(pureQuery);

        // 2. Extract result schema from IR
        ResultSchema schema = extractSchema(ir);

        // 3. Generate SQL for each store's database type
        Map<String, ExecutionPlan.GeneratedSql> sqlByStore = new HashMap<>();

        for (Map.Entry<String, String> binding : runtime.connectionBindings().entrySet()) {
            String storeRef = binding.getKey();
            String connectionRef = binding.getValue();

            ConnectionDefinition connection = connections.get(connectionRef);
            if (connection != null) {
                SQLDialect dialect = getDialect(connection.databaseType());
                String sql = new SQLGenerator(dialect).generate(ir);
                sqlByStore.put(storeRef, new ExecutionPlan.GeneratedSql(connection.databaseType(), sql));
            }
        }

        // 4. Generate unique ID
        String id = UUID.randomUUID().toString();

        return new ExecutionPlan(id, ir, schema, sqlByStore, runtime.qualifiedName());
    }

    /**
     * Extracts the result schema from the IR.
     * For ProjectNode, uses the projection column names and inferred types.
     */
    private ResultSchema extractSchema(RelationNode ir) {
        List<ResultSchema.Column> columns = new ArrayList<>();

        // Navigate to find ProjectNode
        RelationNode current = ir;
        while (current != null) {
            if (current instanceof ProjectNode projectNode) {
                for (Projection projection : projectNode.projections()) {
                    String name = projection.alias();
                    String pureType = inferType(projection.expression());
                    columns.add(new ResultSchema.Column(name, pureType, 1, 1));
                }
                break;
            }
            // Navigate through wrapped nodes
            current = switch (current) {
                case FilterNode f -> f.source();
                case SortNode s -> s.source();
                case LimitNode l -> l.source();
                case GroupByNode g -> g.source();
                default -> null;
            };
        }

        return new ResultSchema(columns);
    }

    /**
     * Infers the Pure type from an expression.
     */
    private String inferType(Expression expression) {
        return switch (expression) {
            case ColumnReference col -> "String"; // Default to String - could be enhanced with mapping metadata
            case Literal lit -> inferLiteralType(lit.value());
            default -> "String";
        };
    }

    private String inferLiteralType(Object value) {
        if (value instanceof String)
            return "String";
        if (value instanceof Integer || value instanceof Long)
            return "Integer";
        if (value instanceof Double || value instanceof Float)
            return "Float";
        if (value instanceof Boolean)
            return "Boolean";
        return "String";
    }

    private static SQLDialect getDialect(DatabaseType databaseType) {
        return switch (databaseType) {
            case DuckDB -> DuckDBDialect.INSTANCE;
            case SQLite -> SQLiteDialect.INSTANCE;
            case H2, Postgres, Snowflake, BigQuery -> DuckDBDialect.INSTANCE;
        };
    }
}
