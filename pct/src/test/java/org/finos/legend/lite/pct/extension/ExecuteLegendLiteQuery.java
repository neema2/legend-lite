// Copyright 2026 Legend Contributors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package org.finos.legend.lite.pct.extension;

import org.eclipse.collections.api.list.ListIterable;
import org.eclipse.collections.api.map.MutableMap;
import org.eclipse.collections.api.stack.MutableStack;
import org.finos.legend.engine.execution.BufferedResult;
import org.finos.legend.engine.server.QueryService;
import org.finos.legend.pure.m3.compiler.Context;
import org.finos.legend.pure.m3.exception.PureExecutionException;
import org.finos.legend.pure.m3.navigation.Instance;
import org.finos.legend.pure.m3.navigation.M3Properties;
import org.finos.legend.pure.m3.navigation.PrimitiveUtilities;
import org.finos.legend.pure.m3.navigation.ProcessorSupport;
import org.finos.legend.pure.m3.navigation.ValueSpecificationBootstrap;
import org.finos.legend.pure.m4.ModelRepository;
import org.finos.legend.pure.m4.coreinstance.CoreInstance;
import org.finos.legend.pure.runtime.java.interpreted.ExecutionSupport;
import org.finos.legend.pure.runtime.java.interpreted.FunctionExecutionInterpreted;
import org.finos.legend.pure.runtime.java.interpreted.VariableContext;
import org.finos.legend.pure.runtime.java.interpreted.natives.InstantiationContext;
import org.finos.legend.pure.runtime.java.interpreted.natives.NativeFunction;
import org.finos.legend.pure.runtime.java.interpreted.profiler.Profiler;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Stack;

/**
 * Native function that executes Pure expressions through Legend-Lite's
 * QueryService.
 * 
 * This bridges the PCT framework running in the Pure interpreted runtime to
 * Legend-Lite's execution engine. Pure expressions are serialized to grammar
 * text,
 * then executed via QueryService which compiles to SQL and runs against DuckDB.
 * 
 * Signature: executeLegendLiteQuery(pureExpression:String[1]):Any[1]
 */
public class ExecuteLegendLiteQuery extends NativeFunction {

    private static final String PURE_MODEL = """
            Class test::Person {
                name: String[1];
                age: Integer[1];
            }

            ###Mapping
            Mapping test::PersonMapping()

            ###Connection
            RelationalDatabaseConnection test::TestConnection {
                type: DuckDB;
                specification: LocalDuckDB { };
                auth: Test { };
            }

            ###Runtime
            Runtime test::TestRuntime {
                mappings: [ test::PersonMapping ];
                connections: [ test::TestConnection ];
            }
            """;

    private final ModelRepository modelRepository;

    public ExecuteLegendLiteQuery(FunctionExecutionInterpreted functionExecution, ModelRepository modelRepository) {
        this.modelRepository = modelRepository;
    }

    @Override
    public CoreInstance execute(
            ListIterable<? extends CoreInstance> params,
            Stack<MutableMap<String, CoreInstance>> resolvedTypeParameters,
            Stack<MutableMap<String, CoreInstance>> resolvedMultiplicityParameters,
            VariableContext variableContext,
            MutableStack<CoreInstance> functionExpressionCallStack,
            Profiler profiler,
            InstantiationContext instantiationContext,
            ExecutionSupport executionSupport,
            Context context,
            ProcessorSupport processorSupport) throws PureExecutionException {

        // Extract the Pure expression string from params
        String pureExpression = PrimitiveUtilities.getStringValue(
                Instance.getValueForMetaPropertyToOneResolved(params.get(0), M3Properties.values, processorSupport));

        try {
            // Execute through Legend-Lite's QueryService
            QueryService queryService = new QueryService();

            // Create in-memory DuckDB connection
            try (Connection connection = DriverManager.getConnection("jdbc:duckdb:")) {
                BufferedResult result = queryService.execute(PURE_MODEL, pureExpression, "test::TestRuntime",
                        connection);

                // Convert result to a Pure string representation
                String resultString = formatResultAsTds(result);

                // Return the result as a Pure string value
                return ValueSpecificationBootstrap.newStringLiteral(
                        modelRepository, resultString, processorSupport);
            }
        } catch (SQLException e) {
            throw new PureExecutionException(
                    functionExpressionCallStack.peek().getSourceInformation(),
                    "Legend-Lite execution failed: " + e.getMessage(),
                    e);
        }
    }

    /**
     * Formats a BufferedResult as a TDS string for comparison.
     */
    private String formatResultAsTds(BufferedResult result) {
        StringBuilder sb = new StringBuilder("#TDS\n");

        // Header row
        var columns = result.columns();
        sb.append("   ");
        for (int i = 0; i < columns.size(); i++) {
            if (i > 0)
                sb.append(",");
            sb.append(columns.get(i).name());
        }
        sb.append("\n");

        // Data rows
        for (var row : result.rows()) {
            sb.append("   ");
            var values = row.values();
            for (int i = 0; i < values.size(); i++) {
                if (i > 0)
                    sb.append(",");
                Object value = values.get(i);
                sb.append(value == null ? "" : value.toString());
            }
            sb.append("\n");
        }

        sb.append("#");
        return sb.toString();
    }
}
