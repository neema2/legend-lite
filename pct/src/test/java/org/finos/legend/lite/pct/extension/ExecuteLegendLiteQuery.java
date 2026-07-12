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
import com.gs.legend.model.m3.Primitive;
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

                function meta::pure::functions::collection::tests::removeDuplicates::cmp(a:Any[1],b:Any[1]):Boolean[1]
                {
                    $a->toString() == $b->toString()
                }
            """;

    private static final Pattern INSTANCE_CLASS_PATTERN = Pattern.compile("\\^([\\w:]+)\\(");
    private static final Pattern TYPE_REF_PATTERN = Pattern.compile("@(\\w+(?:::\\w+)+)");
    private static final Pattern ENUM_REF_PATTERN = Pattern.compile("(\\w+(?:::\\w+)+)\\.\\w+");
    /** Parameter type annotations — match clauses ({@code a: My::Type[1]|...}), typed lambdas. */
    private static final Pattern PARAM_TYPE_PATTERN = Pattern.compile(":\\s*(\\w+(?:::\\w+)+)\\s*\\[");
    /** Bare element references as values ({@code STR_Person->toString()}). */
    private static final Pattern BARE_REF_PATTERN = Pattern.compile("(\\w+(?:::\\w+)+)\\s*->");

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
        pureExpression = inlineFunctionLiterals(pureExpression);

        System.out.println("[LegendLite PCT] Executing: " + pureExpression);

        try (Connection connection = DriverManager.getConnection("jdbc:duckdb:")) {
            try (var tzStmt = connection.createStatement()) {
                tzStmt.execute("SET TimeZone='UTC'");
            }

            // Inject class definitions from the interpreter's model
            java.util.Set<String> discoveredEnums = new java.util.LinkedHashSet<>();
            Map<String, PureClass> extractedClasses =
                    extractClassMetadata(pureExpression, discoveredEnums, processorSupport);
            java.util.List<String> enumDefs =
                    extractEnumDefinitions(pureExpression, discoveredEnums, processorSupport);
            String model = PURE_MODEL;
            if (!extractedClasses.isEmpty() || !enumDefs.isEmpty()) {
                StringBuilder classDefs = new StringBuilder();
                for (String ed : enumDefs) {
                    classDefs.append(ed).append("\n");
                }
                for (PureClass pc : extractedClasses.values()) {
                    classDefs.append(pc.toString()).append("\n");
                }
                System.out.println("[LegendLite PCT] Injected model:\n" + classDefs);
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
            // the error's SOURCE INFO must point at the TEST's own call site
            // (assertError checks line/column) — walk past adapter frames
            org.finos.legend.pure.m4.coreinstance.SourceInformation src =
                    functionExpressionCallStack.peek().getSourceInformation();
            for (var frame : functionExpressionCallStack) {
                var fs = frame.getSourceInformation();
                if (fs != null && fs.getSourceId() != null
                        && !fs.getSourceId().contains("core_legend_lite_pct")) {
                    src = fs;
                    break;
                }
            }
            throw new PureExecutionException(src, remapErrorMessage(e.getMessage()), e);
        }
    }

    private CoreInstance handleScalar(ScalarResult result, ProcessorSupport ps) {
        Object value = result.value();
        if (value == null) {
            return ValueSpecificationBootstrap.wrapValueSpecification(
                    org.eclipse.collections.api.factory.Lists.immutable.empty(), true, ps);
        }
        if (value instanceof java.util.Map<?, ?> map && isMapReturn(result.returnType())) {
            // Map<U,V> results flatten to [k1, v1, k2, v2, ...]; the pure
            // side rebuilds via pair()/newMap() (both native there).
            // (JDBC also hands STRUCT values as java.util.Map — the DECLARED
            // type gates, or ^Person(...) results would flatten here.)
            var flat = new ArrayList<CoreInstance>();
            for (var en : map.entrySet()) {
                flat.add(toCoreInstance(en.getKey(), result.returnType(), ps));
                flat.add(toCoreInstance(en.getValue(), result.returnType(), ps));
            }
            return ValueSpecificationBootstrap.wrapValueSpecification(
                    org.eclipse.collections.impl.factory.Lists.immutable.withAll(flat), true, ps);
        }
        if (value instanceof java.util.List<?> list) {
            // a List<T>-typed scalar (drop(1)->list()): elements convert
            // individually; the pure side wraps them back into ^List
            var elems = new ArrayList<CoreInstance>();
            for (Object v : list) {
                if (v != null) {
                    elems.add(toCoreInstance(v, result.returnType(), ps));
                }
            }
            return ValueSpecificationBootstrap.wrapValueSpecification(
                    org.eclipse.collections.impl.factory.Lists.immutable.withAll(elems), true, ps);
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
        // CLASS-typed elements (Pair<T,U>): the interpreted cast validates
        // the WRAPPER's genericType — hand it the declared one explicitly
        if (classFqnOf(elementType) != null && !isMapReturn(elementType)) {
            CoreInstance gt = genericTypeOf(elementType, ps);
            if (gt != null) {
                return ValueSpecificationBootstrap.wrapValueSpecification_ResultGenericTypeIsKnown(
                        org.eclipse.collections.impl.factory.Lists.immutable.withAll(coreInstances),
                        gt, true, ps);
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
    /**
     * A JDBC STRUCT (java.util.Map) whose DECLARED type is a CLASS builds a
     * REAL pure instance (the DynamicNew construction pattern) — Pair has
     * equality keys, so reconstructed pairs compare and print like natives.
     * Recursive: nested pair structs rebuild through the generic arguments.
     */
    private CoreInstance structToInstance(java.util.Map<?, ?> struct, Type declared,
                                          ProcessorSupport ps) {
        String fqn = classFqnOf(declared);
        CoreInstance classifier = fqn == null ? null : ps.package_getByUserPath(fqn);
        if (classifier == null) {
            throw new RuntimeException("cannot rebuild struct instance for type " + declared);
        }
        CoreInstance inst = modelRepository.newEphemeralAnonymousCoreInstance(null, classifier);
        // the INTERPRETED cast validates generics — stamp the classifier
        // generic type (the DynamicNew pattern) from the declared engine type
        CoreInstance cgt = genericTypeOf(declared, ps);
        if (cgt != null) {
            Instance.addValueToProperty(inst, M3Properties.classifierGenericType, cgt, ps);
        }
        for (var en : struct.entrySet()) {
            Object v = en.getValue();
            if (v == null) {
                continue;
            }
            String prop = String.valueOf(en.getKey());
            CoreInstance ci = toCoreInstance(v, propertyTypeOf(declared, prop), ps);
            Instance.setValuesForProperty(inst, prop,
                    org.eclipse.collections.impl.factory.Lists.immutable.with(ci), ps);
        }
        return inst;
    }

    /** A pure GenericType CoreInstance mirroring the declared engine type (recursive). */
    private CoreInstance genericTypeOf(Type t, ProcessorSupport ps) {
        String raw = rawPathOf(t);
        if (raw == null) {
            return null;
        }
        CoreInstance rawType = ps.package_getByUserPath(raw);
        if (rawType == null) {
            return null;
        }
        CoreInstance gtClass = ps.package_getByUserPath("meta::pure::metamodel::type::generics::GenericType");
        CoreInstance gt = modelRepository.newEphemeralAnonymousCoreInstance(null, gtClass);
        Instance.addValueToProperty(gt, "rawType", rawType, ps);
        int declared = 0;
        if (t instanceof Type.GenericType g) {
            for (Type arg : g.typeArgs()) {
                CoreInstance argGt = genericTypeOf(arg, ps);
                if (argGt == null) {
                    return null;
                }
                Instance.addValueToProperty(gt, "typeArguments", argGt, ps);
                declared++;
            }
        }
        // the ENGINE BRIDGE erases generic args (raw Pair) — pad missing
        // arguments with Any so the harness's cast is a legal downcast
        int params = rawType.getValueForMetaPropertyToMany("typeParameters").size();
        for (int i = declared; i < params; i++) {
            CoreInstance anyGt = modelRepository.newEphemeralAnonymousCoreInstance(null, gtClass);
            Instance.addValueToProperty(anyGt, "rawType",
                    ps.package_getByUserPath("meta::pure::metamodel::type::Any"), ps);
            Instance.addValueToProperty(gt, "typeArguments", anyGt, ps);
        }
        return gt;
    }

    /** The M3 path of a type's raw classifier (primitives at their simple names). */
    private static String rawPathOf(Type t) {
        return switch (t) {
            case Type.GenericType g -> rawPathOf(g.rawType());
            case Type.ClassType ct -> ct.qualifiedName();
            case Type.NameRef n -> n.qualifiedName();
            case Primitive p -> p.pureName();
            default -> null;
        };
    }

    private static String classFqnOf(Type t) {
        return switch (t) {
            case Type.GenericType g when g.rawType() instanceof Type.ClassType ct ->
                    ct.qualifiedName();
            case Type.ClassType ct -> ct.qualifiedName();
            case Type.NameRef n -> n.qualifiedName();
            default -> null;
        };
    }

    /** Pair's first/second resolve through the generic ARGUMENTS (nesting recurses). */
    private static Type propertyTypeOf(Type declared, String prop) {
        if (declared instanceof Type.GenericType g && g.typeArgs().size() == 2
                && "meta::pure::functions::collection::Pair".equals(classFqnOf(g))) {
            return "first".equals(prop) ? g.typeArgs().get(0)
                    : "second".equals(prop) ? g.typeArgs().get(1) : declared;
        }
        return declared;
    }

    /** The engine-typed return is the Map<U,V> carrier (never a class STRUCT). */
    private static boolean isMapReturn(Type t) {
        return t instanceof Type.GenericType g
                && g.rawType() instanceof Type.ClassType ct
                && ct.qualifiedName().equals("meta::pure::functions::collection::Map")
                || t instanceof Type.ClassType c
                        && c.qualifiedName().equals("meta::pure::functions::collection::Map")
                || t instanceof Type.NameRef n
                        && n.qualifiedName().equals("meta::pure::functions::collection::Map");
    }

    private CoreInstance toCoreInstance(Object value, Type type, ProcessorSupport ps) {
        if (value instanceof java.util.Map<?, ?> struct && classFqnOf(type) != null
                && !isMapReturn(type)) {
            return structToInstance(struct, type, ps);
        }

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
            if (type instanceof Primitive p
                    && (p == Primitive.DECIMAL || p == Primitive.NUMBER)) {
                return modelRepository.newDecimalCoreInstance(bd);
            }
            if (type instanceof Type.PrecisionDecimal) {
                // scale is part of the VALUE surface: abs(-3.0D) prints 3.0D
                return modelRepository.newDecimalCoreInstance(bd);
            }
            return modelRepository.newFloatCoreInstance(bd);
        }
        if (value instanceof Double d) {
            if (type instanceof Primitive p && p == Primitive.DECIMAL) {
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
            if (type instanceof Primitive p && p == Primitive.STRICT_DATE) {
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
            // type(x) crosses the wire as the type's NAME; resolve it to the
            // canonical Type instance — assertIs compares IDENTITY, and
            // package lookup returns the one true instance.
            if ("meta::pure::metamodel::type::Type".equals(classFqnOf(type))) {
                CoreInstance typeInstance = ps.package_getByUserPath(s);
                if (typeInstance != null) {
                    return typeInstance;
                }
            }
            // Enum values cross the wire as their NAME; the declared type
            // carries the enumeration — resolve to the CANONICAL enum-value
            // instance (equality on enums is identity in interpreted pure).
            if (type instanceof Type.EnumType et) {
                CoreInstance enumeration = ps.package_getByUserPath(et.qualifiedName());
                if (enumeration != null) {
                    for (CoreInstance v : Instance.getValueForMetaPropertyToManyResolved(
                            enumeration, M3Properties.values, ps)) {
                        if (s.equals(v.getName())) {
                            return v;
                        }
                    }
                }
            }
            // Precision-faithful date STRINGS (the wire's date convention:
            // partial dates, subsecond digit counts beyond the TIMESTAMP
            // carrier) — parse preserving every written digit.
            if (type instanceof Primitive p
                    && (p == Primitive.DATE || p == Primitive.DATE_TIME
                            || p == Primitive.STRICT_DATE)
                    && s.matches("-?\\d{4,}(-\\d{2})?(-\\d{2})?([T ].*)?")) {
                PureDate pd = DateFunctions.parsePureDate(s);
                String classifier = pd.hasHour() ? "DateTime"
                        : pd.hasDay() ? "StrictDate" : "Date";
                return modelRepository.newCoreInstance(pd.toString(),
                        modelRepository.getTopLevel(classifier), null);
            }
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
                // Pure pivot column IDENTITY includes the quotes
                // ('UK__|__LDN__|__sum'); TDSExtension strips ONE outer
                // layer, so the header carries an escaped inner pair.
                colName = "'\\'" + colName + "\\''";
            } else if (!colName.matches("[A-Za-z_][A-Za-z0-9_$]*")) {
                // Non-identifier names ('other kind') quote — the parser
                // strips the pair; the IDENTITY stays unquoted.
                colName = "'" + colName + "'";
            }
            // Pure 5.88's TDS parser resolves header types as PURE paths
            // (VARCHAR was "not found") — spell the Pure primitive, WITH a
            // data-driven multiplicity: the tests cast results to declared
            // Relation<(col:T[1])>/[0..1] shapes, and a header without the
            // annotation builds [0..1] columns that no longer cast to [1].
            boolean hasNull = false;
            for (var row : result.rows()) {
                if (row.values().get(i) == null) {
                    hasNull = true;
                    break;
                }
            }
            sb.append(colName).append(":").append(pureTypeName(col.sqlType()))
                    .append(hasNull ? "[0..1]" : "[1]");
        }
        for (var row : result.rows()) {
            sb.append("\n");
            var values = row.values();
            for (int i = 0; i < values.size(); i++) {
                if (i > 0) sb.append(",");
                // Pure prints VARIANT cells ALWAYS quoted ("[]", "null"),
                // comma or not.
                boolean variant = "JSON".equalsIgnoreCase(columns.get(i).sqlType());
                Object v = values.get(i);
                if (variant && v != null) {
                    sb.append("\"").append(v.toString().replace("\"", "\"\"")).append("\"");
                } else {
                    sb.append(formatValue(v));
                }
            }
        }
        return sb.toString();
    }

    private static String pureTypeName(String sqlType) {
        String t = sqlType.toUpperCase();
        int paren = t.indexOf('(');
        if (paren > 0) {
            t = t.substring(0, paren);
        }
        return switch (t) {
            case "VARCHAR", "CHAR", "TEXT" -> "String";
            case "INTEGER", "INT", "BIGINT", "HUGEINT", "SMALLINT", "TINYINT" -> "Integer";
            case "DOUBLE", "FLOAT", "REAL" -> "Float";
            case "DECIMAL", "NUMERIC" -> "Decimal";
            case "BOOLEAN" -> "Boolean";
            // The interpreted TestTDS cannot BUILD Date columns
            // (getDataAsType: "Not supported data type") and PCT compares
            // TDS results via toString() — dates travel as STRINGS spelled
            // in pure's print form (formatValue below).
            case "DATE", "TIMESTAMP", "TIMESTAMPTZ", "TIMESTAMP WITH TIME ZONE" -> "String";
            default -> "String";
        };
    }

    private String formatValue(Object value) {
        if (value == null) return "null";
        // Pure's date print forms: DateTime = ISO with millis + +0000
        // offset; StrictDate = plain date (the PCT expected strings).
        if (value instanceof java.sql.Timestamp ts) {
            var ldt = ts.toLocalDateTime();
            return ldt.format(java.time.format.DateTimeFormatter
                    .ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSS")) + "+0000";
        }
        if (value instanceof java.time.OffsetDateTime odt) {
            return odt.withOffsetSameInstant(java.time.ZoneOffset.UTC)
                    .format(java.time.format.DateTimeFormatter
                            .ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSS")) + "+0000";
        }
        if (value instanceof Double d && !d.isInfinite() && !d.isNaN()) {
            // pure float PRINT: plain decimal, never scientific; integral
            // floats keep a trailing .0 (2.25e19 -> '22500000000000000000.0')
            java.math.BigDecimal bd = java.math.BigDecimal.valueOf(d);
            String plain = bd.toPlainString();
            return bd.scale() <= 0 ? plain + ".0" : plain;
        }
        String str = value.toString();
        // NOTE: an empty string CANNOT survive the TDS wire — the parser's
        // null literals are ["", "null"], quoted or not (probe-verified);
        // the one affected test is the ledgered expected failure.
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
            case com.gs.legend.model.m3.LClass lc -> lc.qualifiedName();
            case Type.GenericType gt -> gt.rawType() instanceof com.gs.legend.model.m3.LClass lc
                    ? lc.qualifiedName()
                    : gt.rawType() instanceof Type.ClassType ct
                            ? ct.qualifiedName()
                            : "meta::pure::functions::collection::Pair"; // fallback
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

        if (type instanceof Type.GenericType p) {
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
            Type propType = Primitive.ANY;
            if (type instanceof Type.GenericType p && !p.typeArgs().isEmpty()) {
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
    /**
     * TEST-MODEL ENUM definitions referenced by the expression (My::Enum.VALUE
     * or @My::Enum) — platform enums are registered natively in core and
     * skipped; unknown FQNs resolve against the interpreter's graph.
     */
    private java.util.List<String> extractEnumDefinitions(String pureExpression,
            java.util.Set<String> discoveredEnums, ProcessorSupport ps) {
        java.util.List<String> defs = new java.util.ArrayList<>();
        try {
            java.util.Set<String> enumFqns = new java.util.LinkedHashSet<>(discoveredEnums);
            Matcher enumRef = ENUM_REF_PATTERN.matcher(pureExpression);
            while (enumRef.find()) {
                enumFqns.add(enumRef.group(1));
            }
            Matcher enumTypeRef = TYPE_REF_PATTERN.matcher(pureExpression);
            while (enumTypeRef.find()) {
                enumFqns.add(enumTypeRef.group(1));
            }
            for (String fqn : enumFqns) {
                if (fqn.startsWith("meta::pure::metamodel")) {
                    continue;
                }
                CoreInstance enumCls = ps.package_getByUserPath(fqn);
                if (enumCls == null
                        || !Instance.instanceOf(enumCls, "meta::pure::metamodel::type::Enumeration", ps)
                        || com.legend.builtin.Pure.findNativeEnum(fqn).isPresent()) {
                    continue;
                }
                StringBuilder def = new StringBuilder("Enum ").append(fqn).append(" { ");
                boolean first = true;
                for (CoreInstance v : Instance.getValueForMetaPropertyToManyResolved(
                        enumCls, M3Properties.values, ps)) {
                    if (!first) {
                        def.append(", ");
                    }
                    def.append(v.getName());
                    first = false;
                }
                def.append(" }");
                defs.add(def.toString());
            }
        } catch (Exception e) {
            System.out.println("[LegendLite PCT] Enum extraction failed: " + e.getMessage());
        }
        return defs;
    }

    private Map<String, PureClass> extractClassMetadata(String pureExpression,
            java.util.Set<String> discoveredEnums, ProcessorSupport ps) {
        try {
            Map<String, PureClass> classes = new HashMap<>();
            Set<String> visited = new HashSet<>();
            Matcher matcher = INSTANCE_CLASS_PATTERN.matcher(pureExpression);
            while (matcher.find()) {
                extractClassRecursive(matcher.group(1), classes, visited, discoveredEnums, ps);
            }
            // MODEL classes referenced as type arguments (to(@X), cast(@X))
            // or as parameter type annotations (match clauses a: X[1]|...):
            // multi-segment FQNs outside the metamodel/platform space whose
            // resolved element is a Class.
            java.util.Set<String> typeFqns = new java.util.LinkedHashSet<>();
            Matcher typeRef = TYPE_REF_PATTERN.matcher(pureExpression);
            while (typeRef.find()) {
                typeFqns.add(typeRef.group(1));
            }
            Matcher paramType = PARAM_TYPE_PATTERN.matcher(pureExpression);
            while (paramType.find()) {
                typeFqns.add(paramType.group(1));
            }
            Matcher bareRef = BARE_REF_PATTERN.matcher(pureExpression);
            while (bareRef.find()) {
                typeFqns.add(bareRef.group(1));
            }
            for (String fqn : typeFqns) {
                if (fqn.startsWith("meta::pure::metamodel")
                        || fqn.startsWith("meta::pure::precisePrimitives")) {
                    continue;
                }
                CoreInstance cls = ps.package_getByUserPath(fqn);
                // Never inject a class core knows NATIVELY (Pair, List, ...):
                // the extraction degrades type parameters to Any and a
                // redefinition could silently shift platform semantics.
                if (cls == null || com.legend.builtin.Pure.findNativeClass(fqn).isPresent()) {
                    continue;
                }
                if (Instance.instanceOf(cls, "meta::pure::metamodel::type::Enumeration", ps)) {
                    discoveredEnums.add(fqn);
                } else if (Instance.instanceOf(cls, "meta::pure::metamodel::type::Class", ps)) {
                    extractClassRecursive(fqn, classes, visited, discoveredEnums, ps);
                }
            }
            return classes;
        } catch (Exception e) {
            System.out.println("[LegendLite PCT] Class metadata extraction failed: " + e.getMessage());
            return Map.of();
        }
    }

    private void extractClassRecursive(String className, Map<String, PureClass> classes,
                                       Set<String> visited, java.util.Set<String> discoveredEnums,
                                       ProcessorSupport ps) {
        if (visited.contains(className)) return;
        visited.add(className);

        CoreInstance cls = ps.package_getByUserPath(className);
        if (cls == null) return;
        // An ENUMERATION is a class in M3 (it has a 'name' property) but must
        // inject as an Enum — injecting both a Class and an Enum under one
        // FQN split the type in two ("expected X, got X" with identical names).
        if (Instance.instanceOf(cls, "meta::pure::metamodel::type::Enumeration", ps)) {
            discoveredEnums.add(className);
            return;
        }

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
            // Resolve the property type via FQN — no simple-name resolver (see the
            // "single kind-aware resolver" policy on Type.java). For primitives,
            // Pure's FQN (e.g. {@code meta::pure::metamodel::type::Integer}) maps
            // directly to our {@link Primitive} singletons via {@link Primitive#findByFqn}.
            // For classes, we recurse. rawType==null means "no declared type" → Any.
            com.gs.legend.model.m3.Type propType;
            if (rawType == null) {
                propType = Primitive.ANY;
            } else {
                String qualifiedTypeName = getQualifiedName(rawType);
                var primitive = qualifiedTypeName != null
                        ? Primitive.findByFqn(qualifiedTypeName)
                        : java.util.Optional.<Primitive>empty();
                if (primitive.isPresent()) {
                    propType = primitive.get();
                } else if (qualifiedTypeName == null
                        || qualifiedTypeName.startsWith("meta::pure::metamodel")) {
                    continue;
                } else if (Instance.instanceOf(rawType,
                        "meta::pure::metamodel::type::Enumeration", ps)) {
                    // enum-typed property: reference by name; the DEFINITION
                    // is injected as an Enum, never as a shadow Class
                    discoveredEnums.add(qualifiedTypeName);
                    propType = new com.gs.legend.model.m3.Type.ClassType(qualifiedTypeName);
                } else {
                    extractClassRecursive(qualifiedTypeName, classes, visited, discoveredEnums, ps);
                    PureClass referenced = classes.get(qualifiedTypeName);
                    if (referenced == null) continue;
                    propType = new com.gs.legend.model.m3.Type.ClassType(referenced.qualifiedName());
                }
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
        // DuckDB wraps raised errors in a transport prefix ('Invalid Input
        // Error: <ours>') — strip the CLASS prefix so the message our SQL
        // guards raised (error('Cannot divide 5 by zero')) surfaces verbatim.
        java.util.regex.Matcher m = java.util.regex.Pattern
                .compile("^(?:Invalid Input Error|Out of Range Error|Conversion Error): (.*)$",
                        java.util.regex.Pattern.DOTALL)
                .matcher(message);
        if (m.matches()) {
            return m.group(1);
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
                    case '\\' -> {
                        // An ALREADY-escaped sequence (\', \n, \r, \t, \\)
                        // must pass through unchanged — the serializer emits
                        // control chars pre-escaped; doubling the backslash
                        // turned \n into a literal backslash-n (and \' into
                        // a string TERMINATOR, shredding pivot names).
                        char next = i + 1 < expr.length() ? expr.charAt(i + 1) : 0;
                        if (next == '\'' || next == 'n' || next == 'r'
                                || next == 't' || next == '\\') {
                            sb.append('\\').append(next);
                            i++;
                        } else {
                            sb.append("\\\\");
                        }
                    }
                    default -> sb.append(c);
                }
            } else {
                sb.append(c);
            }
        }
        return sb.toString();
    }

    /**
     * The harness serializes a CAPTURED concrete function by printing its
     * whole definition inline:
     * {@code fqn(a: T[1], b: T[1]): R[1] { body }} — the faithful lambda
     * equivalent is {@code {a: T[1], b: T[1] | body}} (a definition IS its
     * lambda; only the name is lost, and the name is not semantics).
     */
    private static final Pattern FN_LITERAL_PATTERN = Pattern.compile(
            "[\\w:]+\\(([^()]*)\\):\\s*[\\w:]+\\[[^\\]]*\\]\\s*\\{(.*?)\\}",
            java.util.regex.Pattern.DOTALL);

    private static String inlineFunctionLiterals(String expr) {
        Matcher m = FN_LITERAL_PATTERN.matcher(expr);
        StringBuilder out = new StringBuilder();
        while (m.find()) {
            m.appendReplacement(out, Matcher.quoteReplacement(
                    "{" + m.group(1).trim() + " | " + m.group(2).trim() + "}"));
        }
        m.appendTail(out);
        return out.toString();
    }

    private String stripTrailingZeros(String subsecond) {
        int end = subsecond.length();
        while (end > 1 && subsecond.charAt(end - 1) == '0') {
            end--;
        }
        return subsecond.substring(0, end);
    }
}
