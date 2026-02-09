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
import org.finos.legend.engine.execution.Column;
import org.finos.legend.engine.execution.Result;
import org.finos.legend.engine.execution.ScalarResult;
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

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.Stack;

import org.finos.legend.pure.m4.coreinstance.primitive.date.DateFunctions;
import org.finos.legend.pure.m4.coreinstance.primitive.date.PureDate;

/**
 * Native function that executes Pure expressions through Legend-Lite's
 * QueryService.
 * 
 * This bridges the PCT framework running in the Pure interpreted runtime to
 * Legend-Lite's execution engine. Pure expressions are serialized to grammar
 * text,
 * then executed via QueryService which compiles to SQL and runs against DuckDB.
 * 
 * Returns a TDS-formatted string for use with stringToTDS():
 * Format: "col1:Type,col2:Type\nval1,val2\nval3,val4"
 * 
 * Signature: executeLegendLiteQuery(pureExpression:String[1]):String[1]
 */
public class ExecuteLegendLiteQuery extends NativeFunction {

    // Minimal model for TDS-based testing - no classes needed for pure relation
    // operations
    private static final String PURE_MODEL = """
                Class model::DoyRecord { eventDate: StrictDate[1]; }
                Database store::DoyDb ( Table T_DOY ( ID INTEGER, EVENT_DATE DATE ) )
                Mapping model::DoyMap ( DoyRecord: Relational { ~mainTable [DoyDb] T_DOY eventDate: [DoyDb] T_DOY.EVENT_DATE } )
                RelationalDatabaseConnection store::TestConn { type: DuckDB; specification: InMemory { }; auth: NoAuth { }; }
                Runtime test::TestRuntime { mappings: [ model::DoyMap ]; connections: [ store::DoyDb: [ environment: store::TestConn ] ]; }
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

        System.out.println("[LegendLite PCT] Executing Pure expression: " + pureExpression);

        try {
            // Create in-memory DuckDB connection
            try (Connection connection = DriverManager.getConnection("jdbc:duckdb:")) {
                Result result;

                // Check for InstanceExpression-based queries (e.g.,
                // [^FirmType(...)...]->project(...))
                InstanceExpressionHandler instanceHandler = new InstanceExpressionHandler();
                if (instanceHandler.requiresInstanceHandling(pureExpression)) {
                    System.out.println("[LegendLite PCT] Detected InstanceExpression pattern, using handler");
                    result = instanceHandler.execute(pureExpression, connection);
                } else {
                    // Execute through Legend-Lite's QueryService for TDS-based queries
                    QueryService queryService = new QueryService();
                    result = queryService.execute(PURE_MODEL, pureExpression, "test::TestRuntime",
                            connection, QueryService.ResultMode.BUFFERED);
                }

                // For scalar results (constant queries), return the primitive value directly
                if (result instanceof ScalarResult scalarResult) {
                    Object value = scalarResult.value();
                    System.out.println("[LegendLite PCT] Scalar result: " + value
                            + " sqlType: " + scalarResult.sqlType()
                            + " pureType: " + scalarResult.pureType());

                    // If the result is a Map (unwrapped DuckDB struct) with a Pure type,
                    // reconstruct the Pure class instance
                    if (value instanceof java.util.Map && scalarResult.pureType() != null) {
                        @SuppressWarnings("unchecked")
                        java.util.Map<String, Object> structMap = (java.util.Map<String, Object>) value;
                        return wrapStructAsClassInstance(structMap, scalarResult.pureType(), processorSupport);
                    }

                    return wrapPrimitiveValue(value, scalarResult.sqlType(), processorSupport);
                }

                // For TDS results, wrap in TDSResult class so Pure can distinguish from scalar
                // strings
                BufferedResult buffered = result.toBuffered();
                String tdsString = formatResultForStringToTds(buffered);

                System.out.println("[LegendLite PCT] Result TDS: " + tdsString.replace("\n", "\\n"));

                // Create a TDSResult instance: ^TDSResult(tdsString = '...')
                return createTDSResult(tdsString, processorSupport);
            }
        } catch (SQLException e) {
            throw new PureExecutionException(
                    functionExpressionCallStack.peek().getSourceInformation(),
                    e.getMessage(),
                    e);
        } catch (Exception e) {
            throw new PureExecutionException(
                    functionExpressionCallStack.peek().getSourceInformation(),
                    e.getMessage(),
                    e);
        }
    }

    /**
     * Formats a BufferedResult as a TDS string for stringToTDS().
     * 
     * Format: col1:Type,col2:Type\nval1,val2\nval3,val4
     * 
     * This matches the format expected by legend-engine's stringToTDS() function.
     */
    private String formatResultForStringToTds(BufferedResult result) {
        StringBuilder sb = new StringBuilder();

        // Column header: name:Type,name:Type
        var columns = result.columns();
        for (int i = 0; i < columns.size(); i++) {
            if (i > 0)
                sb.append(",");
            Column col = columns.get(i);
            String colName = col.name();
            // Quote column names containing '__|__' (pivot columns) with single quotes
            if (colName.contains("__|__")) {
                colName = "'" + colName + "'";
            }
            sb.append(colName).append(":").append(mapToLegendType(col.sqlType()));
        }

        // Data rows: value,value\nvalue,value
        for (var row : result.rows()) {
            sb.append("\n");
            var values = row.values();
            for (int i = 0; i < values.size(); i++) {
                if (i > 0)
                    sb.append(",");
                Object value = values.get(i);
                sb.append(formatValue(value));
            }
        }

        return sb.toString();
    }

    /**
     * Maps Java/SQL types to Pure type names.
     */
    private String mapToLegendType(String sqlType) {
        if (sqlType == null)
            return "String";
        String lower = sqlType.toLowerCase();
        // Handle parameterized types like DECIMAL(18,2) -> Float for TDS columns
        if (lower.startsWith("decimal") || lower.startsWith("numeric"))
            return "Float";
        return switch (lower) {
            case "integer", "int", "bigint", "smallint", "tinyint", "hugeint", "ubigint", "uinteger", "usmallint", "utinyint" -> "Integer";
            case "double", "float", "real" -> "Float";
            case "boolean", "bool" -> "Boolean";
            case "date" -> "StrictDate";
            case "timestamp", "datetime" -> "DateTime";
            // JSON types map to Pure Variant
            case "json", "jsonb" -> "Variant";
            default -> "String";
        };
    }

    /**
     * Formats a value for CSV output, escaping as needed.
     */
    private String formatValue(Object value) {
        if (value == null) {
            return "null";
        }
        String str = value.toString();
        // Escape strings that contain commas, quotes, or newlines
        if (str.contains(",") || str.contains("\"") || str.contains("\n")) {
            return "\"" + str.replace("\"", "\"\"") + "\"";
        }
        return str;
    }

    /**
     * Wraps a Java primitive value into a Pure CoreInstance.
     * Used for scalar results from constant queries.
     */
    private CoreInstance wrapPrimitiveValue(Object value, String sqlType, ProcessorSupport processorSupport) {
        if (value == null) {
            // Return Pure's nil/empty
            return ValueSpecificationBootstrap.wrapValueSpecification(
                    org.eclipse.collections.api.factory.Lists.immutable.empty(), true, processorSupport);
        }
        // Handle lists (from DuckDB arrays unwrapped by Row.java)
        // Format as a string that the Pure adapter's resultToType can parse.
        // Uses type-preserving format: [1, 2, 'a', true, %2014-02-01]
        if (value instanceof java.util.List<?> list) {
            StringBuilder sb = new StringBuilder("[");
            for (int i = 0; i < list.size(); i++) {
                if (i > 0) sb.append(", ");
                Object elem = list.get(i);
                if (elem instanceof String s) {
                    sb.append("'").append(s).append("'");
                } else if (elem == null) {
                    sb.append("[]");
                } else {
                    sb.append(elem);
                }
            }
            sb.append("]");
            return ValueSpecificationBootstrap.newStringLiteral(modelRepository, sb.toString(), processorSupport);
        }
        if (value instanceof Boolean b) {
            return ValueSpecificationBootstrap.newBooleanLiteral(modelRepository, b, processorSupport);
        }
        if (value instanceof Integer i) {
            return ValueSpecificationBootstrap.newIntegerLiteral(modelRepository, i, processorSupport);
        }
        if (value instanceof Long l) {
            return ValueSpecificationBootstrap.newIntegerLiteral(modelRepository, l, processorSupport);
        }
        if (value instanceof BigDecimal bd) {
            // "DECIMAL" = from Decimal literal arithmetic (preserve DuckDB scale as-is)
            // "DECIMAL_CAST" = from toDecimal() CAST (strip trailing zeros from CAST padding)
            if ("DECIMAL".equals(sqlType)) {
                return ValueSpecificationBootstrap.wrapValueSpecification(
                        modelRepository.newDecimalCoreInstance(bd), true, processorSupport);
            }
            if ("DECIMAL_CAST".equals(sqlType)) {
                return ValueSpecificationBootstrap.wrapValueSpecification(
                        modelRepository.newDecimalCoreInstance(bd.stripTrailingZeros()), true, processorSupport);
            }
            return ValueSpecificationBootstrap.newFloatLiteral(modelRepository, bd, processorSupport);
        }
        if (value instanceof Double d) {
            if ("DECIMAL".equals(sqlType) || "DECIMAL_CAST".equals(sqlType)) {
                BigDecimal bd = BigDecimal.valueOf(d);
                if ("DECIMAL_CAST".equals(sqlType)) bd = bd.stripTrailingZeros();
                return ValueSpecificationBootstrap.wrapValueSpecification(
                        modelRepository.newDecimalCoreInstance(bd), true, processorSupport);
            }
            return ValueSpecificationBootstrap.newFloatLiteral(modelRepository, BigDecimal.valueOf(d),
                    processorSupport);
        }
        if (value instanceof Float f) {
            return ValueSpecificationBootstrap.newFloatLiteral(modelRepository, BigDecimal.valueOf(f.doubleValue()),
                    processorSupport);
        }
        if (value instanceof java.math.BigInteger bi) {
            // DuckDB UBIGINT (e.g., HASH()) returns BigInteger via JDBC
            return ValueSpecificationBootstrap.newIntegerLiteral(modelRepository, bi.longValue(), processorSupport);
        }
        if (value instanceof Number n) {
            return ValueSpecificationBootstrap.newFloatLiteral(modelRepository, BigDecimal.valueOf(n.doubleValue()),
                    processorSupport);
        }
        // Handle date types - convert to PureDate and wrap as date literal
        if (value instanceof java.sql.Date sqlDate) {
            LocalDate ld = sqlDate.toLocalDate();
            PureDate pureDate = DateFunctions.newPureDate(ld.getYear(), ld.getMonthValue(), ld.getDayOfMonth());
            return ValueSpecificationBootstrap.newDateLiteral(modelRepository, pureDate, processorSupport);
        }
        if (value instanceof LocalDate ld) {
            PureDate pureDate = DateFunctions.newPureDate(ld.getYear(), ld.getMonthValue(), ld.getDayOfMonth());
            return ValueSpecificationBootstrap.newDateLiteral(modelRepository, pureDate, processorSupport);
        }
        if (value instanceof java.sql.Timestamp ts) {
            LocalDateTime ldt = ts.toLocalDateTime();
            int nanos = ldt.getNano();
            if (nanos > 0) {
                // Format subseconds and strip trailing zeros to match Pure format
                String subsecond = stripTrailingZeros(String.format("%09d", nanos));
                PureDate pureDate = DateFunctions.newPureDate(
                        ldt.getYear(), ldt.getMonthValue(), ldt.getDayOfMonth(),
                        ldt.getHour(), ldt.getMinute(), ldt.getSecond(), subsecond);
                return ValueSpecificationBootstrap.newDateLiteral(modelRepository, pureDate, processorSupport);
            } else if ("TIMESTAMP_NS".equalsIgnoreCase(sqlType)) {
                // TIMESTAMP_NS with zero nanos: preserve nanosecond precision (9 zeros)
                PureDate pureDate = DateFunctions.newPureDate(
                        ldt.getYear(), ldt.getMonthValue(), ldt.getDayOfMonth(),
                        ldt.getHour(), ldt.getMinute(), ldt.getSecond(), "000000000");
                return ValueSpecificationBootstrap.newDateLiteral(modelRepository, pureDate, processorSupport);
            } else {
                PureDate pureDate = DateFunctions.newPureDate(
                        ldt.getYear(), ldt.getMonthValue(), ldt.getDayOfMonth(),
                        ldt.getHour(), ldt.getMinute(), ldt.getSecond());
                return ValueSpecificationBootstrap.newDateLiteral(modelRepository, pureDate, processorSupport);
            }
        }
        if (value instanceof LocalDateTime ldt) {
            int nanos = ldt.getNano();
            if (nanos > 0) {
                String subsecond = stripTrailingZeros(String.format("%09d", nanos));
                PureDate pureDate = DateFunctions.newPureDate(
                        ldt.getYear(), ldt.getMonthValue(), ldt.getDayOfMonth(),
                        ldt.getHour(), ldt.getMinute(), ldt.getSecond(), subsecond);
                return ValueSpecificationBootstrap.newDateLiteral(modelRepository, pureDate, processorSupport);
            } else {
                PureDate pureDate = DateFunctions.newPureDate(
                        ldt.getYear(), ldt.getMonthValue(), ldt.getDayOfMonth(),
                        ldt.getHour(), ldt.getMinute(), ldt.getSecond());
                return ValueSpecificationBootstrap.newDateLiteral(modelRepository, pureDate, processorSupport);
            }
        }
        if (value instanceof LocalTime lt) {
            // StrictTime - just time part, use arbitrary date
            PureDate pureDate = DateFunctions.newPureDate(1, 1, 1, lt.getHour(), lt.getMinute(), lt.getSecond());
            return ValueSpecificationBootstrap.newDateLiteral(modelRepository, pureDate, processorSupport);
        }
        if (value instanceof String s) {
            return ValueSpecificationBootstrap.newStringLiteral(modelRepository, s, processorSupport);
        }
        // Default to string representation
        return ValueSpecificationBootstrap.newStringLiteral(modelRepository, value.toString(), processorSupport);
    }

    /**
     * Creates a TDSResult instance to wrap TDS string results.
     * This allows the Pure adapter to distinguish TDS results from scalar String
     * values.
     */
    private CoreInstance createTDSResult(String tdsString, ProcessorSupport processorSupport) {
        // Get the TDSResult class
        CoreInstance tdsResultClass = processorSupport.package_getByUserPath("meta::legend::lite::pct::TDSResult");
        if (tdsResultClass == null) {
            throw new RuntimeException("TDSResult class not found in Pure model");
        }

        // Create an instance with tdsString property
        CoreInstance instance = modelRepository.newCoreInstance(
                "TDSResult", tdsResultClass, null);

        // Set the tdsString property - use raw string value, not wrapped
        // ValueSpecification
        // The property expects a String primitive, not a ValueSpecification wrapper
        CoreInstance tdsStringValue = modelRepository.newStringCoreInstance(tdsString);
        Instance.addValueToProperty(instance, "tdsString", tdsStringValue, processorSupport);

        // Wrap in value specification for return
        return ValueSpecificationBootstrap.wrapValueSpecification(instance, true, processorSupport);
    }

    /**
     * Wraps a DuckDB struct (Map) as a Pure class instance.
     * Uses the pureType to look up the class, creates an instance,
     * and sets each property from the map values.
     *
     * @param structMap     The struct fields as key-value pairs
     * @param pureTypeName  The fully qualified Pure class name (e.g., "meta::...::CO_Person")
     * @param processorSupport The processor support for class lookup
     * @return A wrapped Pure class instance
     */
    private CoreInstance wrapStructAsClassInstance(
            java.util.Map<String, Object> structMap,
            String pureTypeName,
            ProcessorSupport processorSupport) {

        // Look up the Pure class by path
        CoreInstance classInstance = processorSupport.package_getByUserPath(pureTypeName);
        if (classInstance == null) {
            throw new RuntimeException("Pure class not found: " + pureTypeName);
        }

        // Create an instance of the class
        CoreInstance instance = modelRepository.newCoreInstance(
                pureTypeName.substring(pureTypeName.lastIndexOf(':') + 1),
                classInstance, null);

        // Set each property from the map
        for (java.util.Map.Entry<String, Object> entry : structMap.entrySet()) {
            String propName = entry.getKey();
            Object propValue = entry.getValue();
            if (propValue == null) continue;

            CoreInstance valueInstance;
            if (propValue instanceof String s) {
                valueInstance = modelRepository.newStringCoreInstance(s);
            } else if (propValue instanceof Integer i) {
                valueInstance = modelRepository.newIntegerCoreInstance(i);
            } else if (propValue instanceof Long l) {
                valueInstance = modelRepository.newIntegerCoreInstance(l);
            } else if (propValue instanceof Boolean b) {
                valueInstance = modelRepository.newBooleanCoreInstance(b);
            } else if (propValue instanceof Double d) {
                valueInstance = modelRepository.newFloatCoreInstance(BigDecimal.valueOf(d));
            } else if (propValue instanceof BigDecimal bd) {
                valueInstance = modelRepository.newFloatCoreInstance(bd);
            } else if (propValue instanceof java.util.Map) {
                // Nested struct — recursively wrap (would need nested pureType, skip for now)
                valueInstance = modelRepository.newStringCoreInstance(propValue.toString());
            } else {
                valueInstance = modelRepository.newStringCoreInstance(propValue.toString());
            }

            Instance.addValueToProperty(instance, propName, valueInstance, processorSupport);
        }

        return ValueSpecificationBootstrap.wrapValueSpecification(instance, true, processorSupport);
    }

    /**
     * Strips trailing zeros from a subsecond string.
     * Pure normalizes subseconds without trailing zeros (e.g., "338001" not
     * "338001000").
     */
    private String stripTrailingZeros(String subsecond) {
        int end = subsecond.length();
        while (end > 1 && subsecond.charAt(end - 1) == '0') {
            end--;
        }
        return subsecond.substring(0, end);
    }
}
