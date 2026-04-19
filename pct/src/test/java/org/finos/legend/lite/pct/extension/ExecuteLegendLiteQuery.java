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
import com.gs.legend.exec.ExecutionResult;
import com.gs.legend.exec.ExecutionResult.ScalarResult;
import com.gs.legend.exec.ExecutionResult.CollectionResult;
import com.gs.legend.exec.ExecutionResult.TabularResult;
import com.gs.legend.exec.ExecutionResult.GraphResult;
import com.gs.legend.exec.Column;
import com.gs.legend.model.m3.Type;
import com.gs.legend.server.QueryService;

import org.finos.legend.pure.m3.compiler.Context;
import org.finos.legend.pure.m3.exception.PureExecutionException;
import org.finos.legend.pure.m3.navigation.Instance;
import org.finos.legend.pure.m3.navigation.M3Properties;
import org.finos.legend.pure.m3.navigation.PrimitiveUtilities;
import org.finos.legend.pure.m3.navigation.ProcessorSupport;
import org.finos.legend.pure.m3.navigation.ValueSpecificationBootstrap;
import org.finos.legend.pure.m4.ModelRepository;
import org.finos.legend.pure.m4.coreinstance.CoreInstance;
import org.finos.legend.pure.m4.coreinstance.primitive.date.DateFunctions;
import org.finos.legend.pure.m4.coreinstance.primitive.date.PureDate;
import org.finos.legend.pure.runtime.java.interpreted.ExecutionSupport;
import org.finos.legend.pure.runtime.java.interpreted.FunctionExecutionInterpreted;
import org.finos.legend.pure.runtime.java.interpreted.VariableContext;
import org.finos.legend.pure.runtime.java.interpreted.natives.InstantiationContext;
import org.finos.legend.pure.runtime.java.interpreted.natives.NativeFunction;
import org.finos.legend.pure.runtime.java.interpreted.profiler.Profiler;

import com.gs.legend.model.m3.Multiplicity;
import com.gs.legend.model.m3.Property;
import com.gs.legend.model.m3.PureClass;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Connection;
import java.sql.DriverManager;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Stack;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Native function that bridges PCT tests to Legend-Lite's QueryService.
 *
 * Pure expressions are re-escaped, executed via QueryService (compile → SQL → DuckDB),
 * and the typed ExecutionResult is converted back to Pure CoreInstances.
 *
 * All type information flows from Type on ExecutionResult — no SQL type inspection.
 */
public class ExecuteLegendLiteQuery extends NativeFunction {

    private static final String PURE_MODEL = """
                Class model::DoyRecord { eventDate: StrictDate[1]; }
                Database store::DoyDb ( Table T_DOY ( ID INTEGER, EVENT_DATE DATE ) )
                Mapping model::DoyMap ( DoyRecord: Relational { ~mainTable [DoyDb] T_DOY eventDate: [DoyDb] T_DOY.EVENT_DATE } )
                RelationalDatabaseConnection store::TestConn { type: DuckDB; specification: InMemory { }; auth: NoAuth { }; }
                Runtime test::TestRuntime { mappings: [ model::DoyMap ]; connections: [ store::DoyDb: [ environment: store::TestConn ] ]; }

                function meta::pure::functions::relation::tests::composition::testVariantColumn_functionComposition_filterValues(val: Integer[*]):Boolean[1]
                {
                    $val->filter(y | $y->mod(2) == 0)->size() == 2
                }

                function meta::pure::functions::lang::tests::letFn::letAsLastStatement():String[1]
                {
                    let last = 'last statement string'
                }

                function meta::pure::functions::lang::tests::letFn::letWithParam(val: String[1]):Any[*]
                {
                    let a = $val
                }

                function meta::pure::functions::lang::tests::letFn::letChainedWithAnotherFunction(elements: ModelElement[*]):ModelElement[*]
                {
                    let classes = $elements->removeDuplicates()
                }
            """;

    private static final Pattern INSTANCE_CLASS_PATTERN = Pattern.compile("\\^([\\w:]+)\\(");

    private final ModelRepository modelRepository;

    public ExecuteLegendLiteQuery(FunctionExecutionInterpreted functionExecution, ModelRepository modelRepository) {
        this.modelRepository = modelRepository;
    }

    // ===== execute =====

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

        String pureExpression = PrimitiveUtilities.getStringValue(
                Instance.getValueForMetaPropertyToOneResolved(params.get(0), M3Properties.values, processorSupport));
        pureExpression = reEscapeStringLiterals(pureExpression);

        System.out.println("[LegendLite PCT] Executing: " + pureExpression);

        try (Connection connection = DriverManager.getConnection("jdbc:duckdb:")) {
            try (var tzStmt = connection.createStatement()) {
                tzStmt.execute("SET TimeZone='UTC'");
            }

            // Inject class definitions from the interpreter's model
            Map<String, PureClass> extractedClasses = extractClassMetadata(pureExpression, processorSupport);
            String model = PURE_MODEL;
            if (!extractedClasses.isEmpty()) {
                StringBuilder classDefs = new StringBuilder();
                for (PureClass pc : extractedClasses.values()) {
                    classDefs.append(pc.toString()).append("\n");
                }
                System.out.println("[LegendLite PCT] Injected classes:\n" + classDefs);
                model = classDefs + PURE_MODEL;
            }

            ExecutionResult result = new QueryService().execute(model, pureExpression,
                    "test::TestRuntime", connection);

            return switch (result) {
                case ScalarResult s -> handleScalar(s, processorSupport);
                case CollectionResult c -> handleCollection(c, processorSupport);
                case TabularResult t -> handleTabular(t, processorSupport);
                case GraphResult g -> ValueSpecificationBootstrap.newStringLiteral(
                        modelRepository, g.json(), processorSupport);
            };
        } catch (Exception e) {
            throw new PureExecutionException(
                    functionExpressionCallStack.peek().getSourceInformation(),
                    remapErrorMessage(e.getMessage()), e);
        }
    }

    private CoreInstance handleScalar(ScalarResult result, ProcessorSupport ps) {
        Object value = result.value();
        if (value == null) {
            return ValueSpecificationBootstrap.wrapValueSpecification(
                    org.eclipse.collections.api.factory.Lists.immutable.empty(), true, ps);
        }
        CoreInstance ci = toCoreInstance(value, result.returnType(), ps);
        return ValueSpecificationBootstrap.wrapValueSpecification(ci, true, ps);
    }

    private CoreInstance handleCollection(CollectionResult result, ProcessorSupport ps) {
        Type elementType = result.returnType();
        var coreInstances = new ArrayList<CoreInstance>();
        for (Object value : result.values()) {
            if (value != null) {
                coreInstances.add(toCoreInstance(value, elementType, ps));
            }
        }
        return ValueSpecificationBootstrap.wrapValueSpecification(
                org.eclipse.collections.impl.factory.Lists.immutable.withAll(coreInstances), true, ps);
    }

    private CoreInstance handleTabular(TabularResult result, ProcessorSupport ps) {
        String tdsString = formatAsTds(result);
        System.out.println("[LegendLite PCT] TDS: " + tdsString.replace("\n", "\\n"));
        return createTDSResult(tdsString, ps);
    }

    // ===== toCoreInstance: single Java → CoreInstance conversion =====

    /**
     * Converts a Java value to a raw Pure CoreInstance.
     * Dispatches on Java type; uses Type for BigDecimal disambiguation
     * and class instance creation.
     */
    private CoreInstance toCoreInstance(Object value, Type type, ProcessorSupport ps) {
        if (value instanceof Boolean b) {
            return modelRepository.newBooleanCoreInstance(b);
        }
        if (value instanceof Integer i) {
            return modelRepository.newIntegerCoreInstance(i);
        }
        if (value instanceof Long l) {
            return modelRepository.newIntegerCoreInstance(l);
        }
        if (value instanceof BigInteger bi) {
            return modelRepository.newIntegerCoreInstance(bi.toString());
        }
        if (value instanceof BigDecimal bd) {
            if (type instanceof Type.Primitive p
                    && (p == Type.Primitive.DECIMAL || p == Type.Primitive.NUMBER)) {
                return modelRepository.newDecimalCoreInstance(bd);
            }
            if (type instanceof Type.PrecisionDecimal) {
                return modelRepository.newDecimalCoreInstance(bd.stripTrailingZeros());
            }
            return modelRepository.newFloatCoreInstance(bd);
        }
        if (value instanceof Double d) {
            if (type instanceof Type.Primitive p && p == Type.Primitive.DECIMAL) {
                return modelRepository.newDecimalCoreInstance(BigDecimal.valueOf(d));
            }
            if (type instanceof Type.PrecisionDecimal) {
                return modelRepository.newDecimalCoreInstance(BigDecimal.valueOf(d).stripTrailingZeros());
            }
            return modelRepository.newFloatCoreInstance(BigDecimal.valueOf(d));
        }
        if (value instanceof Float f) {
            return modelRepository.newFloatCoreInstance(BigDecimal.valueOf(f.doubleValue()));
        }
        if (value instanceof Number n) {
            return modelRepository.newFloatCoreInstance(BigDecimal.valueOf(n.doubleValue()));
        }
        // Dates
        if (value instanceof java.sql.Date sqlDate) {
            return toPureDateInstance(sqlDate.toLocalDate());
        }
        if (value instanceof LocalDate ld) {
            return toPureDateInstance(ld);
        }
        if (value instanceof java.sql.Timestamp ts) {
            // Type tells us if this was originally a StrictDate promoted to Timestamp
            if (type instanceof Type.Primitive p && p == Type.Primitive.STRICT_DATE) {
                return toPureDateInstance(ts.toLocalDateTime().toLocalDate());
            }
            return toPureDateTimeInstance(ts.toLocalDateTime());
        }
        if (value instanceof LocalDateTime ldt) {
            return toPureDateTimeInstance(ldt);
        }
        if (value instanceof OffsetDateTime odt) {
            return toPureDateTimeInstance(odt.withOffsetSameInstant(ZoneOffset.UTC).toLocalDateTime());
        }
        if (value instanceof LocalTime lt) {
            PureDate pd = DateFunctions.newPureDate(1, 1, 1, lt.getHour(), lt.getMinute(), lt.getSecond());
            return modelRepository.newCoreInstance(pd.toString(), modelRepository.getTopLevel("StrictTime"), null);
        }
        // Strings
        if (value instanceof String s) {
            return modelRepository.newStringCoreInstance(s);
        }
        // Struct → class instance
        if (value instanceof Map<?, ?> map) {
            @SuppressWarnings("unchecked")
            Map<String, Object> structMap = (Map<String, Object>) map;
            return createClassInstance(structMap, type, ps);
        }
        // List (struct arrays unwrapped by Row.java, e.g. zip → List<Pair>)
        if (value instanceof List<?> list) {
            Type elemType = type;
            var coreInstances = new ArrayList<CoreInstance>();
            for (Object elem : list) {
                if (elem != null) {
                    coreInstances.add(toCoreInstance(elem, elemType, ps));
                }
            }
            // Return as a single-element wrapping — the collection will be wrapped by caller
            if (coreInstances.size() == 1) return coreInstances.get(0);
            // For multi-element, this shouldn't happen in scalar context
            // but return first as fallback
            return coreInstances.isEmpty() ? modelRepository.newStringCoreInstance("[]")
                    : coreInstances.get(0);
        }
        // Fallback
        return modelRepository.newStringCoreInstance(value.toString());
    }

    private CoreInstance toPureDateInstance(LocalDate ld) {
        PureDate pd = DateFunctions.newPureDate(ld.getYear(), ld.getMonthValue(), ld.getDayOfMonth());
        return modelRepository.newCoreInstance(pd.toString(),
                modelRepository.getTopLevel("StrictDate"), null);
    }

    private CoreInstance toPureDateTimeInstance(LocalDateTime ldt) {
        PureDate pd;
        int nanos = ldt.getNano();
        if (nanos > 0) {
            String subsecond = stripTrailingZeros(String.format("%09d", nanos));
            pd = DateFunctions.newPureDate(ldt.getYear(), ldt.getMonthValue(), ldt.getDayOfMonth(),
                    ldt.getHour(), ldt.getMinute(), ldt.getSecond(), subsecond);
        } else {
            pd = DateFunctions.newPureDate(ldt.getYear(), ldt.getMonthValue(), ldt.getDayOfMonth(),
                    ldt.getHour(), ldt.getMinute(), ldt.getSecond());
        }
        return modelRepository.newCoreInstance(pd.toString(),
                modelRepository.getTopLevel("DateTime"), null);
    }

    // ===== TDS formatting =====

    /**
     * Formats a TabularResult as a TDS string for stringToTDS().
     * Column types come from the compiler schema (already Pure type names).
     */
    private String formatAsTds(ExecutionResult result) {
        StringBuilder sb = new StringBuilder();
        var columns = result.columns();
        for (int i = 0; i < columns.size(); i++) {
            if (i > 0) sb.append(",");
            Column col = columns.get(i);
            String colName = col.name();
            if (colName.contains("__|__")) {
                colName = "'" + colName + "'";
            }
            sb.append(colName).append(":").append(col.sqlType());
        }
        for (var row : result.rows()) {
            sb.append("\n");
            var values = row.values();
            for (int i = 0; i < values.size(); i++) {
                if (i > 0) sb.append(",");
                sb.append(formatValue(values.get(i)));
            }
        }
        return sb.toString();
    }

    private String formatValue(Object value) {
        if (value == null) return "null";
        String str = value.toString();
        if (str.contains(",") || str.contains("\"") || str.contains("\n")) {
            return "\"" + str.replace("\"", "\"\"") + "\"";
        }
        return str;
    }

    private CoreInstance createTDSResult(String tdsString, ProcessorSupport ps) {
        CoreInstance tdsResultClass = ps.package_getByUserPath("meta::legend::lite::pct::TDSResult");
        if (tdsResultClass == null) {
            throw new RuntimeException("TDSResult class not found in Pure model");
        }
        CoreInstance instance = modelRepository.newCoreInstance("TDSResult", tdsResultClass, null);
        Instance.addValueToProperty(instance, "tdsString",
                modelRepository.newStringCoreInstance(tdsString), ps);
        return ValueSpecificationBootstrap.wrapValueSpecification(instance, true, ps);
    }

    // ===== Class instance creation =====

    /**
     * Creates a Pure class instance from a DuckDB struct Map.
     * Uses Type for the class path and type arguments.
     */
    private CoreInstance createClassInstance(Map<String, Object> structMap, Type type,
                                            ProcessorSupport ps) {
        // Resolve class path from Type
        String qualifiedName = switch (type) {
            case Type.ClassType ct -> ct.qualifiedName();
            case Type.Parameterized p -> switch (p.rawType()) {
                case "Pair" -> "meta::pure::functions::collection::Pair";
                default -> p.rawType();
            };
            default -> "meta::pure::functions::collection::Pair"; // fallback for unknown struct types
        };

        CoreInstance classInstance = ps.package_getByUserPath(qualifiedName);
        if (classInstance == null) {
            throw new RuntimeException("Pure class not found: " + qualifiedName);
        }

        String simpleName = qualifiedName.substring(qualifiedName.lastIndexOf(':') + 1);
        CoreInstance instance = modelRepository.newCoreInstance(simpleName, classInstance, null);

        // Build classifierGenericType with type arguments from Type
        CoreInstance classifierGT = org.finos.legend.pure.m3.navigation.type.Type
                .wrapGenericType(classInstance, null, ps);

        if (type instanceof Type.Parameterized p) {
            // Set type arguments from the compiler-provided Type
            for (Type typeArg : p.typeArgs()) {
                String argTypeName = typeArg.typeName();
                CoreInstance argTypeClass = ps.package_getByUserPath(argTypeName);
                if (argTypeClass != null) {
                    CoreInstance argGT = org.finos.legend.pure.m3.navigation.type.Type
                            .wrapGenericType(argTypeClass, null, ps);
                    Instance.addValueToProperty(classifierGT, M3Properties.typeArguments, argGT, ps);
                }
            }
        }

        Instance.addValueToProperty(instance, M3Properties.classifierGenericType, classifierGT, ps);

        // Set properties from struct map
        for (Map.Entry<String, Object> entry : structMap.entrySet()) {
            String propName = entry.getKey();
            Object propValue = entry.getValue();
            if (propValue == null) continue;

            // Determine property Type from Parameterized typeArgs if available
            Type propType = Type.Primitive.ANY;
            if (type instanceof Type.Parameterized p && !p.typeArgs().isEmpty()) {
                // For Pair: first → typeArgs[0], second → typeArgs[1]
                int idx = indexOf(structMap, propName);
                if (idx >= 0 && idx < p.typeArgs().size()) {
                    propType = p.typeArgs().get(idx);
                }
            }

            CoreInstance valueInstance = toCoreInstance(propValue, propType, ps);
            Instance.addValueToProperty(instance, propName, valueInstance, ps);
        }

        return instance;
    }

    /** Returns the positional index of a key in an ordered map. */
    private static int indexOf(Map<String, ?> map, String key) {
        int i = 0;
        for (String k : map.keySet()) {
            if (k.equals(key)) return i;
            i++;
        }
        return -1;
    }

    // ===== Class metadata extraction =====

    /**
     * Extracts class definitions from the Pure interpreter for ^className() patterns
     * in the expression. Returns a map of qualified name → PureClass.
     */
    private Map<String, PureClass> extractClassMetadata(String pureExpression, ProcessorSupport ps) {
        try {
            Map<String, PureClass> classes = new HashMap<>();
            Set<String> visited = new HashSet<>();
            Matcher matcher = INSTANCE_CLASS_PATTERN.matcher(pureExpression);
            while (matcher.find()) {
                extractClassRecursive(matcher.group(1), classes, visited, ps);
            }
            return classes;
        } catch (Exception e) {
            System.out.println("[LegendLite PCT] Class metadata extraction failed: " + e.getMessage());
            return Map.of();
        }
    }

    private void extractClassRecursive(String className, Map<String, PureClass> classes,
                                       Set<String> visited, ProcessorSupport ps) {
        if (visited.contains(className)) return;
        visited.add(className);

        CoreInstance cls = ps.package_getByUserPath(className);
        if (cls == null) return;

        List<Property> properties = new ArrayList<>();
        for (CoreInstance prop : ps.class_getSimpleProperties(cls)) {
            CoreInstance nameInstance = prop.getValueForMetaPropertyToOne(M3Properties.name);
            if (nameInstance == null) continue;
            String propName = PrimitiveUtilities.getStringValue(nameInstance);
            if (propName == null) continue;

            CoreInstance mult = prop.getValueForMetaPropertyToOne(M3Properties.multiplicity);
            if (mult == null) continue;
            int upper = org.finos.legend.pure.m3.navigation.multiplicity.Multiplicity
                    .multiplicityUpperBoundToInt(mult);
            int lower = org.finos.legend.pure.m3.navigation.multiplicity.Multiplicity
                    .multiplicityLowerBoundToInt(mult);
            Multiplicity multiplicity = new Multiplicity.Bounded(lower, upper < 0 ? null : upper);

            CoreInstance genericType = prop.getValueForMetaPropertyToOne(M3Properties.genericType);
            CoreInstance rawType = (genericType != null)
                    ? genericType.getValueForMetaPropertyToOne(M3Properties.rawType) : null;
            if (rawType != null) {
                rawType = org.finos.legend.pure.m3.navigation.importstub.ImportStub
                        .withImportStubByPass(rawType, ps);
            }
            String typeName = "Any";
            if (rawType != null) {
                CoreInstance rawTypeName = rawType.getValueForMetaPropertyToOne(M3Properties.name);
                if (rawTypeName != null) {
                    typeName = PrimitiveUtilities.getStringValue(rawTypeName);
                }
            }

            // Resolve the property type: primitive by Pure-level name lookup, otherwise
            // recurse into the referenced class. Replaces a legacy try/catch-for-control-flow
            // pattern (Phase B 2.5b.2) with Optional-based dispatch.
            com.gs.legend.model.m3.Type propType;
            var primitive = com.gs.legend.model.m3.Type.Primitive.lookup(typeName);
            if (primitive.isPresent()) {
                propType = primitive.get();
            } else if (rawType != null) {
                String qualifiedTypeName = getQualifiedName(rawType);
                if (qualifiedTypeName == null
                        || qualifiedTypeName.startsWith("meta::pure::metamodel")) {
                    continue;
                }
                extractClassRecursive(qualifiedTypeName, classes, visited, ps);
                PureClass referenced = classes.get(qualifiedTypeName);
                if (referenced == null) continue;
                propType = new com.gs.legend.model.m3.Type.ClassType(referenced.qualifiedName());
            } else {
                continue;
            }
            properties.add(new Property(propName, propType, multiplicity));
        }

        String qualifiedName = getQualifiedName(cls);
        String packagePath = "";
        String simpleName = className;
        if (qualifiedName != null) {
            int lastSep = qualifiedName.lastIndexOf("::");
            if (lastSep >= 0) {
                packagePath = qualifiedName.substring(0, lastSep);
                simpleName = qualifiedName.substring(lastSep + 2);
            }
        }

        classes.put(qualifiedName != null ? qualifiedName : className,
                new PureClass(packagePath, simpleName, properties));
    }

    private String getQualifiedName(CoreInstance element) {
        try {
            return org.finos.legend.pure.m3.navigation.PackageableElement.PackageableElement
                    .getUserPathForPackageableElement(element);
        } catch (Exception e) {
            return null;
        }
    }

    // ===== Utilities =====

    private static String remapErrorMessage(String message) {
        if (message == null) return null;
        if ((message.contains("shift value") && message.contains("is out of range"))
                || message.contains("Overflow in left shift")
                || message.contains("Overflow in right shift")) {
            return "Unsupported number of bits to shift - max bits allowed is 62";
        }
        return message;
    }

    /**
     * Re-escapes literal special characters inside single-quoted strings.
     * The Pure interpreter resolves escape sequences before passing the expression,
     * but our parser expects them unresolved.
     */
    private static String reEscapeStringLiterals(String expr) {
        StringBuilder sb = new StringBuilder(expr.length());
        boolean inString = false;
        for (int i = 0; i < expr.length(); i++) {
            char c = expr.charAt(i);
            if (c == '\'' && !inString) {
                inString = true;
                sb.append(c);
            } else if (c == '\'' && inString) {
                inString = false;
                sb.append(c);
            } else if (inString) {
                switch (c) {
                    case '\n' -> sb.append("\\n");
                    case '\r' -> sb.append("\\r");
                    case '\t' -> sb.append("\\t");
                    case '\\' -> sb.append("\\\\");
                    default -> sb.append(c);
                }
            } else {
                sb.append(c);
            }
        }
        return sb.toString();
    }

    private String stripTrailingZeros(String subsecond) {
        int end = subsecond.length();
        while (end > 1 && subsecond.charAt(end - 1) == '0') {
            end--;
        }
        return subsecond.substring(0, end);
    }
}
