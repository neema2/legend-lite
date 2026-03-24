package com.gs.legend.compiler;

import com.gs.legend.antlr.ValueSpecificationBuilder;
import com.gs.legend.ast.*;
import com.gs.legend.model.ModelContext;
import com.gs.legend.model.PureFunctionRegistry;

import com.gs.legend.model.mapping.ClassMapping;
import com.gs.legend.model.mapping.MappingRegistry;
import com.gs.legend.model.mapping.RelationalMapping;
import com.gs.legend.model.store.Table;
import com.gs.legend.parser.PureParser;
import com.gs.legend.plan.GenericType;


import java.util.*;

/**
 * Clean compiler for Pure expressions.
 *
 * <p>
 * Takes untyped {@link ValueSpecification} AST (from
 * {@link ValueSpecificationBuilder})
 * and produces typed {@link TypeInfo} with full type checking via
 * {@link RelationType}.
 *
 * <p>
 * Responsibilities:
 * <ul>
 * <li>Name resolution — resolve table/class names to tables via store
 * metadata</li>
 * <li>Property→column mapping — resolve $p.firstName → FIRST_NAME via
 * MappingRegistry</li>
 * <li>Type checking — validate column/property existence</li>
 * <li>Type inference — derive result types for projections and computed
 * columns</li>
 * <li>GenericType.Relation.Schema propagation — track columns through the pipeline</li>
 * </ul>
 */
public class TypeChecker implements TypeCheckEnv {

    /**
     * Built-in function registry — validates function existence, no more
     * passthrough.
     */
    private static final BuiltinFunctionRegistry builtinRegistry = BuiltinFunctionRegistry.instance();

    private final ModelContext modelContext;
    /** Per-node type info, consumed by PlanGenerator. */
    private final IdentityHashMap<ValueSpecification, TypeInfo> types = new IdentityHashMap<>();

    public TypeChecker(ModelContext modelContext) {
        this.modelContext = Objects.requireNonNull(modelContext, "ModelContext must not be null");
    }

    public ModelContext modelContext() {
        return modelContext;
    }

    @Override
    public java.util.Map<String, TypeInfo.AssociationTarget> resolveAssociations(
            List<ValueSpecification> body, ClassMapping mapping) {
        return resolveAssociationsInBody(body, mapping);
    }

    @Override
    public TypeInfo lookupCompiled(ValueSpecification vs) {
        return types.get(vs);
    }

    /**
     * Top-level compile: returns a {@link TypeCheckResult} bundling the typed
     * result and per-node side table.
     */
    public TypeCheckResult check(ValueSpecification vs) {
        TypeInfo rootInfo = compileExpr(vs, new CompilationContext());

        if (rootInfo == null) {
            throw new PureCompileException(
                    "TypeChecker: no TypeInfo stamped for root " + vs.getClass().getSimpleName());
        }
        if (rootInfo.expressionType() == null) {
            throw new PureCompileException(
                    "TypeChecker: expressionType not stamped for root " + vs.getClass().getSimpleName());
        }

        return new TypeCheckResult(vs, types);
    }

    /**
     * Internal: compiles a ValueSpecification to a typed result.
     * Called recursively for sub-expressions.
     */
    @Override
    public TypeInfo compileExpr(ValueSpecification vs, CompilationContext ctx) {
        return switch (vs) {
            case AppliedFunction af -> compileFunction(af, ctx);
            case ClassInstance ci -> compileClassInstance(ci, ctx);
            case LambdaFunction lf -> compileLambda(lf, ctx);
            case Variable v -> compileVariable(v, ctx);
            case AppliedProperty ap -> compileProperty(ap, ctx);
            case PackageableElementPtr pe -> scalarTyped(pe, GenericType.Primitive.STRING);
            case GenericTypeInstance gti -> scalarTyped(gti, GenericType.Primitive.STRING);
            case PureCollection coll -> compileCollection(coll, ctx);
            // Literals — scalar with known type
            case CInteger i -> scalarTyped(i, classifyInteger(i));
            case CFloat f -> scalarTyped(f, GenericType.Primitive.FLOAT);
            case CDecimal d -> scalarTyped(d, classifyDecimal(d));
            case CString s -> scalarTyped(s, GenericType.Primitive.STRING);
            case CBoolean b -> scalarTyped(b, GenericType.Primitive.BOOLEAN);
            case CDateTime dt -> scalarTyped(dt, GenericType.Primitive.DATE_TIME);
            case CStrictDate sd -> scalarTyped(sd, GenericType.Primitive.STRICT_DATE);
            case CStrictTime st -> scalarTyped(st, GenericType.Primitive.STRICT_TIME);
            case CLatestDate ld -> scalarTyped(ld, GenericType.Primitive.DATE_TIME);
            case CByteArray ba -> scalarTyped(ba, GenericType.Primitive.STRING);
            case EnumValue ev -> scalarTyped(ev, new GenericType.EnumType(ev.fullPath()));
            case UnitInstance ui -> scalarTyped(ui, GenericType.Primitive.FLOAT);
        };
    }

    // ========== Function Dispatch ==========

    private TypeInfo compileFunction(AppliedFunction af, CompilationContext ctx) {
        String funcName = simpleName(af.function());

        return switch (funcName) {
            // --- Relation Sources ---
            case "table", "class" -> compileTableAccess(af, ctx);
            case "getAll" -> compileGetAll(af, ctx);
            // --- Shape-preserving (work on both relations and lists) ---
            case "sort", "sortBy" -> compileSort(af, ctx);
            case "filter" -> compileFilter(af, ctx);
            case "limit", "take", "drop", "slice", "first", "last" -> compileSlicing(af, ctx);
            // --- Column operations ---
            case "rename" -> compileRename(af, ctx);
            case "select" -> compileSelect(af, ctx);
            case "distinct" -> compileDistinct(af, ctx);
            // --- Shape-changing ---
            case "concatenate" -> compileConcatenate(af, ctx);
            case "project" -> compileProject(af, ctx);
            case "groupBy" -> compileGroupBy(af, ctx);
            case "aggregate" -> compileAggregate(af, ctx);
            case "extend" -> compileExtend(af, ctx);
            case "join" -> compileJoin(af, ctx);
            case "asOfJoin" -> compileAsOfJoin(af, ctx);
            case "pivot" -> compilePivot(af, ctx);
            case "flatten" -> compileFlatten(af, ctx);
            case "from" -> compileFrom(af, ctx);
            // --- Scalar collection functions with lambdas ---
            case "fold" -> compileFold(af, ctx);
            case "zip" -> compileZip(af, ctx);
            case "letFunction" -> compileLet(af, ctx);
            // --- Type functions (cast, toMany, toOne, toVariant, to) ---
            case "cast", "toMany", "toOne", "toVariant", "to" -> compileTypeFunction(af, ctx);
            // --- Variant access (compiler resolves index vs field) ---
            case "get" -> compileGet(af, ctx);
            case "write" -> compileWrite(af, ctx);
            // --- GraphFetch / Serialize (M2M JSON output) ---
            case "graphFetch" -> compileGraphFetch(af, ctx);
            case "serialize" -> compileSerialize(af, ctx);
            case "eval" -> compileEval(af, ctx);
            case "match" -> compileMatch(af, ctx);
            // --- Conditional ---
            case "if" -> compileIf(af, ctx);
            // --- Registry-driven: all other functions resolved via NativeFunctionDef ---
            default -> compileRegistryOrUserFunction(af, funcName, ctx);
        };
    }

    // ========== Relation Sources ==========

    /**
     * Compiles table('store::db.TABLE') or class('model::MyClass').
     * Resolves to physical Table and builds RelationType.
     */
    private TypeInfo compileTableAccess(AppliedFunction af, CompilationContext ctx) {
        List<ValueSpecification> params = af.parameters();
        if (params.isEmpty()) {
            throw new PureCompileException("table/class() requires an argument");
        }

        String tableRef = extractStringValue(params.get(0));

        // For class('model::Person'), resolve via class name
        if ("class".equals(simpleName(af.function()))) {
            String className = tableRef.contains("::") ? tableRef.substring(tableRef.lastIndexOf("::") + 2) : tableRef;
            var mappingOpt = mappingRegistry().findByClassName(className);
            if (mappingOpt.isPresent()) {
                ClassMapping mapping = mappingOpt.get();
                GenericType.Relation.Schema relType = tableToRelationType(mapping.sourceTable());
                return typed(af, relType, mapping);
            }
        }

        Table table = resolveTable(tableRef);
        GenericType.Relation.Schema relType = tableToRelationType(table);
        return typed(af, relType, null);
    }

    /**
     * Compiles Person.all() → getAll(PackageableElementPtr("Person")).
     * Resolves class → mapping → table → RelationType.
     * Stores the mapping in context for property→column resolution in
     * project/filter.
     */
    private TypeInfo compileGetAll(AppliedFunction af, CompilationContext ctx) {
        List<ValueSpecification> params = af.parameters();
        if (params.isEmpty()) {
            throw new PureCompileException("getAll() requires a class argument");
        }

        String qualifiedName;
        String className;
        if (params.get(0) instanceof PackageableElementPtr(String fullPath)) {
            qualifiedName = fullPath;
            className = fullPath.contains("::") ? fullPath.substring(fullPath.lastIndexOf("::") + 2)
                    : fullPath;
        } else {
            throw new PureCompileException("Unresolved type for function: " + simpleName(af.function()));
        }

        // Try relational mapping first
        var mappingOpt = mappingRegistry().findByClassName(className);
        if (mappingOpt.isPresent()) {
            ClassMapping mapping = mappingOpt.get();
            // Return ClassType[*] — class instances, not Relation.
            // Schema comes from project(), not getAll().
            var info = TypeInfo.builder()
                    .mapping(mapping)
                    .expressionType(ExpressionType.many(new GenericType.ClassType(qualifiedName)))
                    .build();
            types.put(af, info);
            return info;
        }

        // Try PureClassMapping (M2M) — runtime-driven
        var pureMappingOpt = mappingRegistry().findPureClassMapping(className);
        if (pureMappingOpt.isPresent()) {
            return compileM2MGetAll(af, pureMappingOpt.get());
        }

        throw new PureCompileException("Unresolved type for function: " + simpleName(af.function()));
    }

    /**
     * Compiles getAll() for an M2M-mapped class.
     * Resolves source class → source RelationalMapping → source table.
     * Builds virtual GenericType.Relation.Schema from M2M property names.
     * Stores PureClassMapping in TypeInfo sidecar for PlanGenerator.
     */
    private TypeInfo compileM2MGetAll(AppliedFunction af,
            com.gs.legend.model.mapping.PureClassMapping pureMapping) {
        // Resolve source class → its mapping (may be relational or another M2M)
        String sourceClassName = pureMapping.sourceClassName();
        var sourceMapping = mappingRegistry().findAnyMapping(sourceClassName);

        if (sourceMapping.isEmpty()) {
            throw new PureCompileException(
                    "M2M source class '" + sourceClassName + "' has no mapping. "
                            + "The source class must be mapped to a database table or another M2M class.");
        }

        ClassMapping srcMapping = sourceMapping.get();

        // Recursive M2M chain resolution: if source is itself M2M, resolve its source
        // too.
        // Each intermediate PureClassMapping gets linked to its own source mapping.
        // The chain must ultimately terminate at a RelationalMapping.
        if (srcMapping instanceof com.gs.legend.model.mapping.PureClassMapping srcPcm) {
            // Recursively resolve the source M2M first (this compiles the intermediate
            // mapping)
            resolveM2MChain(srcPcm);
            // Re-fetch from registry to get the resolved version (records are immutable)
            srcMapping = mappingRegistry().findAnyMapping(sourceClassName).orElseThrow();
        }

        // Resolve the PureClassMapping: link it to its source mapping and target class
        com.gs.legend.model.m3.PureClass targetClass = null;
        if (modelContext != null) {
            targetClass = modelContext.findClass(pureMapping.targetClassName()).orElse(null);
        }
        var resolvedMapping = pureMapping.withResolved(targetClass, srcMapping);

        // Build virtual RelationType: M2M property names as columns.
        // Infer types from target class properties (via ModelContext).
        Map<String, GenericType> virtualColumns = new LinkedHashMap<>();
        for (String propName : pureMapping.propertyExpressions().keySet()) {
            // Look up property type from the target class
            GenericType propType = resolveM2MPropertyType(pureMapping.targetClassName(), propName);
            virtualColumns.put(propName, propType);
        }
        // Also add join reference properties (deep fetch)
        // Resolve @JoinName references → AssociationTarget for PlanGenerator
        var associations = new java.util.LinkedHashMap<String, TypeInfo.AssociationTarget>();
        for (var jrEntry : pureMapping.joinReferences().entrySet()) {
            String propName = jrEntry.getKey();
            String joinName = jrEntry.getValue();

            // Join properties are class-typed (nested objects) — mark as ANY for now
            virtualColumns.put(propName, GenericType.Primitive.ANY);

            // Resolve the join from the registry
            var joinOpt = mappingRegistry().findJoin(joinName);
            if (joinOpt.isEmpty())
                continue;
            com.gs.legend.model.store.Join join = joinOpt.get();

            // Determine target class from the property type on the target class
            boolean isToMany = false;
            ClassMapping targetMapping = null;
            if (targetClass != null) {
                var propOpt = targetClass.findProperty(propName);
                if (propOpt.isPresent()) {
                    var prop = propOpt.get();
                    isToMany = !prop.multiplicity().isSingular();
                    // Find the target class mapping by property type name
                    String targetClassName = prop.genericType().typeName();
                    if (targetClassName != null) {
                        targetMapping = mappingRegistry().findAnyMapping(targetClassName).orElse(null);
                    }
                }
            }
            if (targetMapping != null) {
                associations.put(propName, new TypeInfo.AssociationTarget(targetMapping, join, isToMany));
            }
        }

        GenericType.Relation.Schema virtualRelType = GenericType.Relation.Schema.withoutPivot(virtualColumns);

        // Type-check M2M property expressions: bind $src to source relationType+mapping
        // so that operands in expressions like `$src.firstName + ' ' + $src.lastName`
        // get tagged with their types (String, Integer, etc.) in the sidecar.
        CompilationContext srcCtx = buildSourceContext(srcMapping);
        if (srcCtx != null) {
            for (var entry : pureMapping.propertyExpressions().entrySet()) {
                typeCheckExpression(entry.getValue(), srcCtx);
            }
        }

        // Store resolved mapping in sidecar.
        // ClassMapping.sourceTable() chains through to the source RelationalMapping's
        // table.
        // ClassMapping.expressionForProperty() returns the M2M expression AST.
        var infoBuilder = TypeInfo.builder()
                .mapping(resolvedMapping)
                .expressionType(ExpressionType.many(new GenericType.Relation(virtualRelType)));
        if (!associations.isEmpty()) {
            infoBuilder.associations(Map.copyOf(associations));
        }
        var info = infoBuilder.build();
        types.put(af, info);
        return info;
    }

    /**
     * Recursively resolves an M2M chain, ensuring each intermediate
     * PureClassMapping
     * is linked to its source mapping. Terminates when the source is a
     * RelationalMapping.
     */
    private void resolveM2MChain(com.gs.legend.model.mapping.PureClassMapping pcm) {
        String srcClassName = pcm.sourceClassName();
        var srcMappingOpt = mappingRegistry().findAnyMapping(srcClassName);
        if (srcMappingOpt.isEmpty()) {
            throw new PureCompileException(
                    "M2M chain: source class '" + srcClassName + "' has no mapping.");
        }
        ClassMapping srcMapping = srcMappingOpt.get();

        // If source is also M2M, resolve recursively first
        if (srcMapping instanceof com.gs.legend.model.mapping.PureClassMapping innerPcm) {
            resolveM2MChain(innerPcm);
            // Re-fetch resolved version
            srcMapping = mappingRegistry().findAnyMapping(srcClassName).orElseThrow();
        }

        // Type-check this intermediate M2M's property expressions against its source
        CompilationContext srcCtx = buildSourceContext(srcMapping);
        if (srcCtx != null) {
            for (var entry : pcm.propertyExpressions().entrySet()) {
                typeCheckExpression(entry.getValue(), srcCtx);
            }
        }

        // Resolve this M2M mapping: link to its source
        com.gs.legend.model.m3.PureClass targetClass = null;
        if (modelContext != null) {
            targetClass = modelContext.findClass(pcm.targetClassName()).orElse(null);
        }
        var resolved = pcm.withResolved(targetClass, srcMapping);

        // Update the mapping registry with the resolved version
        mappingRegistry().updatePureClassMapping(pcm.targetClassName(), resolved);
    }

    /**
     * Builds a CompilationContext for type-checking M2M expressions against a
     * source mapping.
     * Handles both RelationalMapping sources (column types from schema) and
     * PureClassMapping sources (virtual column types from M2M property
     * expressions).
     */
    private CompilationContext buildSourceContext(ClassMapping srcMapping) {
        if (srcMapping instanceof RelationalMapping srcRm) {
            Map<String, GenericType> srcColumns = new LinkedHashMap<>();
            for (var pm : srcRm.propertyMappings()) {
                srcColumns.put(pm.propertyName(), srcRm.pureTypeForProperty(pm.propertyName()));
            }
            GenericType.Relation.Schema srcRelType = GenericType.Relation.Schema.withoutPivot(srcColumns);
            return new CompilationContext()
                    .withRelationType("src", srcRelType)
                    .withMapping("src", srcRm);
        }
        if (srcMapping instanceof com.gs.legend.model.mapping.PureClassMapping srcPcm) {
            // For M2M→M2M: bind $src to the intermediate M2M's virtual columns
            Map<String, GenericType> srcColumns = new LinkedHashMap<>();
            for (String propName : srcPcm.propertyExpressions().keySet()) {
                srcColumns.put(propName, resolveM2MPropertyType(srcPcm.targetClassName(), propName));
            }
            GenericType.Relation.Schema srcRelType = GenericType.Relation.Schema.withoutPivot(srcColumns);
            return new CompilationContext()
                    .withRelationType("src", srcRelType)
                    .withMapping("src", srcPcm);
        }
        return null;
    }

    /**
     * Resolves the type of an M2M target property from the model.
     * Falls back to String if class/property not found.
     */
    private GenericType resolveM2MPropertyType(String className, String propertyName) {
        if (modelContext != null) {
            var classOpt = modelContext.findClass(className);
            if (classOpt.isPresent()) {
                for (var prop : classOpt.get().properties()) {
                    if (prop.name().equals(propertyName)) {
                        return GenericType.Primitive.fromTypeName(prop.genericType().typeName());
                    }
                }
            }
        }
        // Default to String if we can't resolve
        return GenericType.Primitive.STRING;
    }

    // ========== GraphFetch / Serialize ==========

    /**
     * Compiles graphFetch(source, #{Tree}#).
     *
     * <p>
     * Type-checks:
     * <ol>
     * <li>Source must be class-based (has a ClassMapping)</li>
     * <li>Root class in tree must match source mapping's target class</li>
     * <li>All properties in tree must exist on the target class</li>
     * <li>Nested properties must be class-typed (not scalars)</li>
     * </ol>
     */
    private TypeInfo compileGraphFetch(AppliedFunction af, CompilationContext ctx) {
        // Compile source (e.g., Person.all())
        TypeInfo sourceInfo = compileExpr(af.parameters().get(0), ctx);

        // (1) Source must be class-based
        if (sourceInfo.mapping() == null) {
            throw new PureCompileException(
                    "graphFetch() requires a class-based source (e.g., Person.all()), "
                            + "but source has no ClassMapping");
        }

        // Extract GraphFetchTree from ClassInstance parameter
        com.gs.legend.ast.GraphFetchTree tree = null;
        if (af.parameters().size() > 1 && af.parameters().get(1) instanceof ClassInstance ci
                && ci.value() instanceof com.gs.legend.ast.GraphFetchTree gft) {
            tree = gft;
        }
        if (tree == null) {
            throw new PureCompileException("graphFetch() requires a graph fetch tree argument #{...}#");
        }

        // (2) Root class must match source mapping's target class
        var targetClass = sourceInfo.mapping().targetClass();
        if (!tree.rootClass().equals(targetClass.name())
                && !tree.rootClass().equals(targetClass.qualifiedName())) {
            throw new PureCompileException(
                    "graphFetch tree root class '" + tree.rootClass()
                            + "' does not match source class '" + targetClass.qualifiedName() + "'");
        }

        // (3+4) Validate all properties exist and nested types are correct
        var spec = toGraphFetchSpec(tree, targetClass);

        var info = TypeInfo.from(sourceInfo)
                .graphFetchSpec(spec)
                .build();
        types.put(af, info);
        return info;
    }

    /**
     * Compiles serialize(graphFetchSource, #{Tree}#).
     *
     * <p>
     * Type-checks:
     * <ol>
     * <li>Source must have a graphFetchSpec (must come from graphFetch())</li>
     * <li>Stamps returnType = String (JSON output)</li>
     * </ol>
     */
    private TypeInfo compileSerialize(AppliedFunction af, CompilationContext ctx) {
        // Compile source (must be a graphFetch result)
        TypeInfo sourceInfo = compileExpr(af.parameters().get(0), ctx);

        // (1) Source must have a graphFetchSpec
        com.gs.legend.plan.GraphFetchSpec spec = sourceInfo.graphFetchSpec();
        if (spec == null) {
            throw new PureCompileException(
                    "serialize() requires a graphFetch source — "
                            + "call ->graphFetch(#{...}#) before ->serialize()");
        }

        // Override with serialize tree if provided
        if (af.parameters().size() > 1 && af.parameters().get(1) instanceof ClassInstance ci
                && ci.value() instanceof com.gs.legend.ast.GraphFetchTree gft) {
            var targetClass = sourceInfo.mapping().targetClass();
            spec = toGraphFetchSpec(gft, targetClass);
        }

        // (2) Stamp expressionType = JSON (serialized graph output)
        var info = TypeInfo.from(sourceInfo)
                .graphFetchSpec(spec)
                .expressionType(ExpressionType.one(com.gs.legend.plan.GenericType.Primitive.JSON))
                .build();
        types.put(af, info);
        return info;
    }

    /**
     * Transforms a parser-level GraphFetchTree into a plan-level GraphFetchSpec.
     * Validates all properties against the target class:
     * - Each property must exist on the class (including inherited)
     * - Nested properties must be class-typed (not scalar/primitive)
     */
    private com.gs.legend.plan.GraphFetchSpec toGraphFetchSpec(
            com.gs.legend.ast.GraphFetchTree tree,
            com.gs.legend.model.m3.PureClass targetClass) {
        var properties = tree.properties().stream()
                .map(pf -> {
                    // Validate property exists on the class
                    var propOpt = targetClass.findProperty(pf.name());
                    if (propOpt.isEmpty()) {
                        throw new PureCompileException(
                                "Property '" + pf.name() + "' not found on class '"
                                        + targetClass.qualifiedName() + "'. Available: "
                                        + targetClass.allProperties().stream()
                                                .map(com.gs.legend.model.m3.Property::name)
                                                .toList());
                    }

                    if (pf.isNested()) {
                        // Validate nested property is class-typed
                        var prop = propOpt.get();
                        if (!(prop.genericType() instanceof com.gs.legend.model.m3.PureClass nestedClass)) {
                            throw new PureCompileException(
                                    "Property '" + pf.name() + "' on class '"
                                            + targetClass.qualifiedName()
                                            + "' is not class-typed — cannot nest in graphFetch tree. "
                                            + "Type: " + prop.genericType().typeName());
                        }
                        var nestedSpec = toGraphFetchSpec(pf.subTree(), nestedClass);
                        return com.gs.legend.plan.GraphFetchSpec.PropertySpec.nested(
                                pf.name(), nestedSpec);
                    }
                    return com.gs.legend.plan.GraphFetchSpec.PropertySpec.scalar(pf.name());
                })
                .toList();
        return new com.gs.legend.plan.GraphFetchSpec(tree.rootClass(), properties);
    }

    // ========== Shape-Preserving Operations ==========


    /** Compiles sort(source, sortSpecs). */
    /**
     * Compiles sort() / sortBy().
     * Delegates to SortChecker for signature-driven type validation.
     */
    private TypeInfo compileSort(AppliedFunction af, CompilationContext ctx) {
        List<ValueSpecification> params = af.parameters();
        TypeInfo source = compileExpr(params.get(0), ctx);
        var info = new com.gs.legend.compiler.checkers.SortChecker(this).check(af, source, ctx);
        types.put(af, info);
        return info;
    }



    // ========== filter ==========

    /**
     * Compiles filter(source, predicate).
     * Delegates to FilterChecker for signature-driven type validation.
     */
    private TypeInfo compileFilter(AppliedFunction af, CompilationContext ctx) {
        List<ValueSpecification> params = af.parameters();
        TypeInfo source = compileExpr(params.get(0), ctx);
        var info = new com.gs.legend.compiler.checkers.FilterChecker(this).check(af, source, ctx);
        types.put(af, info);
        return info;
    }

    // ========== limit / take / drop / slice / first / last ==========

    /**
     * Compiles slicing functions: limit, take, drop, slice, first, last.
     * All preserve the source type — they only change cardinality.
     * Works on both relations and lists.
     */
    private TypeInfo compileSlicing(AppliedFunction af, CompilationContext ctx) {
        List<ValueSpecification> params = af.parameters();
        TypeInfo source = compileExpr(params.get(0), ctx);
        var info = new com.gs.legend.compiler.checkers.SlicingChecker(this).check(af, source, ctx);
        types.put(af, info);
        return info;
    }

    /**
     * Resolves sort specifications from various AST patterns into normalized
     * SortSpecs.
     * Handles: asc(~col), desc(~col), sortInfo(~col), bare ~col, legacy CString.
     */


    // ========== Shape-Changing Operations ==========

    /** Compiles concatenate(left, right). */
    private TypeInfo compileConcatenate(AppliedFunction af, CompilationContext ctx) {
        List<ValueSpecification> params = af.parameters();
        TypeInfo source = compileExpr(params.get(0), ctx);
        var info = new com.gs.legend.compiler.checkers.ConcatenateChecker(this)
                .check(af, source, ctx);
        types.put(af, info);
        return info;
    }

    /**
     * Compiles project(source, projectionSpecs...).
     *
     * <p>
     * Handles two AST patterns:
     * <ol>
     * <li>project(source, [lambdas], ['aliases']) — Collection of lambdas +
     * aliases</li>
     * <li>project(source, lambda1, lambda2, ...) — bare lambdas as params</li>
     * </ol>
     *
     * Resolves property names to column names via mapping.
     */
    private TypeInfo compileProject(AppliedFunction af, CompilationContext ctx) {
        List<ValueSpecification> params = af.parameters();
        TypeInfo source = compileExpr(params.get(0), ctx);
        var info = new com.gs.legend.compiler.checkers.ProjectChecker(this).check(af, source, ctx);
        types.put(af, info);
        return info;
    }

    /**
     * Compiles select(source, ~col1, ~col2, ...).
     * Selects specific columns from a relation by name.
     */


    /**
     * Compiles flatten(source, ~col).
     * Output GenericType.Relation.Schema mirrors source but the flattened column changes type
     * from list/JSON to its element type. Column name stored in columnSpecs
     * for PlanGenerator to generate UNNEST.
     */
    private TypeInfo compileFlatten(AppliedFunction af, CompilationContext ctx) {
        List<ValueSpecification> params = af.parameters();
        TypeInfo source = compileExpr(params.get(0), ctx);
        var info = new com.gs.legend.compiler.checkers.FlattenChecker(this).check(af, source, ctx);
        types.put(af, info);
        return info;
    }

    // ========== rename ==========

    /**
     * Compiles rename(rel, ~oldCol, ~newCol).
     * Output schema: source schema with oldCol renamed to newCol.
     * Populates colSpecs with renamed(old, new) for PlanGenerator.
     */
    private TypeInfo compileRename(AppliedFunction af, CompilationContext ctx) {
        List<ValueSpecification> params = af.parameters();
        TypeInfo source = compileExpr(params.get(0), ctx);
        var info = new com.gs.legend.compiler.checkers.RenameChecker(this).check(af, source, ctx);
        types.put(af, info);
        return info;
    }

    // ========== select ==========

    /**
     * Compiles select(rel, ~[cols]).
     * Output schema: subset of source columns.
     * Populates colSpecs with column names for PlanGenerator.
     */
    private TypeInfo compileSelect(AppliedFunction af, CompilationContext ctx) {
        List<ValueSpecification> params = af.parameters();
        TypeInfo source = compileExpr(params.get(0), ctx);
        var info = new com.gs.legend.compiler.checkers.SelectChecker(this).check(af, source, ctx);
        types.put(af, info);
        return info;
    }

    // ========== distinct ==========

    /**
     * Compiles distinct(rel) or distinct(rel, ~[cols]).
     * Without columns: output schema = source schema (just adds DISTINCT).
     * With columns: output schema = subset of source columns (like select + DISTINCT).
     */
    private TypeInfo compileDistinct(AppliedFunction af, CompilationContext ctx) {
        List<ValueSpecification> params = af.parameters();
        TypeInfo source = compileExpr(params.get(0), ctx);
        var info = new com.gs.legend.compiler.checkers.DistinctChecker(this).check(af, source, ctx);
        types.put(af, info);
        return info;
    }

    /**
     * Compiles groupBy(source, groupCols, aggSpecs).
     * Output GenericType.Relation.Schema has group columns + aggregate columns.
     */
    private TypeInfo compileGroupBy(AppliedFunction af, CompilationContext ctx) {
        List<ValueSpecification> params = af.parameters();
        TypeInfo source = compileExpr(params.get(0), ctx);
        var info = new com.gs.legend.compiler.checkers.GroupByChecker(this)
                .check(af, source, ctx);
        types.put(af, info);
        return info;
    }

    /**
     * Compiles aggregate(source, aggSpecs).
     * Full-table aggregation (no group columns).
     * Delegates to {@link com.gs.legend.compiler.checkers.AggregateChecker}.
     */
    private TypeInfo compileAggregate(AppliedFunction af, CompilationContext ctx) {
        List<ValueSpecification> params = af.parameters();
        TypeInfo source = compileExpr(params.get(0), ctx);
        var info = new com.gs.legend.compiler.checkers.AggregateChecker(this)
                .check(af, source, ctx);
        types.put(af, info);
        return info;
    }

    /**
     * Compiles extend(source, ~newCol : lambda).
     * Delegates to {@link com.gs.legend.compiler.checkers.ExtendChecker}.
     */
    private TypeInfo compileExtend(AppliedFunction af, CompilationContext ctx) {
        List<ValueSpecification> params = af.parameters();
        TypeInfo source = compileExpr(params.get(0), ctx);
        var info = new com.gs.legend.compiler.checkers.ExtendChecker(this)
                .check(af, source, ctx);
        types.put(af, info);
        return info;
    }
    /**
     * Compiles join(left, right, joinType, condition).
     * Delegates to {@link com.gs.legend.compiler.checkers.JoinChecker}.
     */
    private TypeInfo compileJoin(AppliedFunction af, CompilationContext ctx) {
        List<ValueSpecification> params = af.parameters();
        TypeInfo left = compileExpr(params.get(0), ctx);
        var info = new com.gs.legend.compiler.checkers.JoinChecker(this)
                .check(af, left, ctx);
        types.put(af, info);
        return info;
    }

    /** Compiles asOfJoin — delegates to {@link com.gs.legend.compiler.checkers.AsOfJoinChecker}. */
    private TypeInfo compileAsOfJoin(AppliedFunction af, CompilationContext ctx) {
        List<ValueSpecification> params = af.parameters();
        TypeInfo left = compileExpr(params.get(0), ctx);
        var info = new com.gs.legend.compiler.checkers.AsOfJoinChecker(this)
                .check(af, left, ctx);
        types.put(af, info);
        return info;
    }

    /**
     * Compiles pivot — extracts pivot columns and aggregate specs into
     * TypeInfo.PivotSpec.
     * Pure: relation->pivot(~[pivotCols], ~[aggName : x | $x.col : y | $y->sum()])
     */
    private TypeInfo compilePivot(AppliedFunction af, CompilationContext ctx) {
        List<ValueSpecification> params = af.parameters();
        if (params.isEmpty()) {
            throw new PureCompileException("pivot() requires a source");
        }

        TypeInfo source = compileExpr(params.get(0), ctx);

        // Extract pivot columns from params[1]: ClassInstance(ColSpecArray)
        List<String> pivotColumns = new java.util.ArrayList<>();
        if (params.size() > 1 && params.get(1) instanceof ClassInstance ci) {
            if (ci.value() instanceof ColSpecArray(List<ColSpec> colSpecs)) {
                for (ColSpec cs : colSpecs) {
                    pivotColumns.add(cs.name());
                }
            } else if (ci.value() instanceof ColSpec cs) {
                pivotColumns.add(cs.name());
            }
        }

        // Extract aggregate specs from params[2]: ClassInstance(ColSpecArray)
        List<TypeInfo.PivotAggSpec> aggregates = new java.util.ArrayList<>();
        if (params.size() > 2 && params.get(2) instanceof ClassInstance ci) {
            List<ColSpec> aggSpecs;
            if (ci.value() instanceof ColSpecArray(List<ColSpec> colSpecs)) {
                aggSpecs = colSpecs;
            } else if (ci.value() instanceof ColSpec cs) {
                aggSpecs = List.of(cs);
            } else {
                throw new PureCompileException("pivot(): unsupported aggregate spec: " + ci.value());
            }

            for (ColSpec cs : aggSpecs) {
                String alias = cs.name();
                String aggFunction = "SUM";
                String valueColumn = null;
                ValueSpecification valueExpr = null;

                // function2 = aggregate function: y | $y->sum()
                if (cs.function2() != null && !cs.function2().body().isEmpty()) {
                    ValueSpecification aggBody = cs.function2().body().get(0);
                    if (aggBody instanceof AppliedFunction aggFn) {
                        aggFunction = switch (simpleName(aggFn.function())) {
                            case "plus", "sum" -> "SUM";
                            case "count" -> "COUNT";
                            case "average", "mean" -> "AVG";
                            case "min" -> "MIN";
                            case "max" -> "MAX";
                            default -> simpleName(aggFn.function()).toUpperCase();
                        };
                    }
                }

                // function1 = value extraction: x | $x.col or x | $x.a * $x.b
                String lambdaParam = null;
                if (cs.function1() != null && !cs.function1().body().isEmpty()) {
                    ValueSpecification body = cs.function1().body().get(0);
                    if (body instanceof AppliedProperty ap) {
                        valueColumn = ap.property();
                    } else {
                        // Complex expression — store AST for PlanGenerator to compile
                        valueExpr = body;
                        // Store lambda param name so PlanGenerator can identify row accesses
                        if (!cs.function1().parameters().isEmpty()) {
                            lambdaParam = cs.function1().parameters().get(0).name();
                        }
                    }
                }

                aggregates.add(new TypeInfo.PivotAggSpec(alias, aggFunction, valueColumn, valueExpr, lambdaParam));
            }
        }

        var pivotSpec = new TypeInfo.PivotSpec(pivotColumns, aggregates);

        // Compute group-by columns: source cols minus pivot cols minus aggregate value
        // cols.
        // These are the columns the compiler CAN know. Dynamic pivot columns are
        // data-dependent
        // and will be resolved from JDBC ResultSetMetaData at execution time.
        var groupByCols = new java.util.LinkedHashMap<String, GenericType>(source.schema().columns());
        pivotColumns.forEach(groupByCols::remove);
        for (var agg : aggregates) {
            if (agg.valueColumn() != null)
                groupByCols.remove(agg.valueColumn());
        }
        // Build dynamic pivot column specs from aggregates.
        // When aggReturnType returns the generic NUMBER, refine to the source column's
        // concrete type (e.g., SUM(Integer col) → Integer, not Number).
        var sourceColumns = source.schema().columns();
        var dynamicCols = aggregates.stream()
                .map(agg -> {
                    GenericType returnType = aggReturnType(agg.aggFunction());
                    if (returnType == GenericType.Primitive.NUMBER) {
                        if (agg.valueColumn() != null) {
                            // Refine from source column type (e.g., SUM(integerCol) → Integer)
                            GenericType colType = sourceColumns.get(agg.valueColumn());
                            if (colType != null)
                                returnType = colType;
                        } else if (agg.valueExpr() != null) {
                            // First try literal type (e.g., count pattern: SUM(1) → Integer)
                            returnType = inferLiteralType(agg.valueExpr(), returnType);
                            // If still NUMBER, try inferring from column references in the expression
                            // (e.g., SUM($x.treePlanted * $x.coefficient) → Integer from source cols)
                            if (returnType == GenericType.Primitive.NUMBER) {
                                returnType = inferExprType(agg.valueExpr(), sourceColumns, returnType);
                            }
                        }
                    }
                    return new com.gs.legend.plan.GenericType.Relation.Schema.DynamicPivotColumn(
                            agg.alias(), returnType);
                })
                .toList();
        var partialType = new GenericType.Relation.Schema(groupByCols, dynamicCols);

        var info = TypeInfo.builder().mapping(source.mapping()).pivotSpec(pivotSpec)
                .expressionType(ExpressionType.many(new GenericType.Relation(partialType))).build();
        types.put(af, info);
        return info;
    }

    /**
     * Compiles from(source, runtimeRef).
     * Passes through the source's type — from() is a runtime binding, not a type
     * change.
     */
    private TypeInfo compileFrom(AppliedFunction af, CompilationContext ctx) {
        List<ValueSpecification> params = af.parameters();
        if (params.isEmpty()) {
            throw new PureCompileException("from() requires a source");
        }

        TypeInfo source = compileExpr(params.get(0), ctx);
        return typed(af, source.schema(), source.mapping());
    }

    // ========== Window Functions (inside extend lambdas) ==========



    /**
     * Handles unknown/scalar functions — compiles the source (if it's a relation)
     * and propagates its type.
     *
     * Also checks the PureFunctionRegistry for user-defined functions.
     * If found, inlines the function body by substituting parameters with
     * argument source text, re-parsing, and compiling the result.
     */
    private static final PureFunctionRegistry functionRegistry = PureFunctionRegistry.withBuiltins();

    // ========== Scalar Collection Functions with Lambdas ==========

    /** Compiles fold(list, {x,y|body}, init). */
    private TypeInfo compileFold(AppliedFunction af, CompilationContext ctx) {
        List<ValueSpecification> params = af.parameters();
        if (params.size() < 3)
            throw new PureCompileException("fold requires 3 parameters: source, lambda, init");

        // 1. Compile source and init
        TypeInfo sourceInfo = compileExpr(params.get(0), ctx);
        compileExpr(params.get(2), ctx);

        // 2. Register fold lambda params: {elem, acc | body}
        if (params.get(1) instanceof LambdaFunction(List<Variable> parameters, List<ValueSpecification> body)) {
            // Detect fold+add pattern before full lambda compilation
            if (isFoldAddPattern((LambdaFunction) params.get(1))) {
                // Build synthetic: concatenate(init, source)
                var concat = new AppliedFunction("concatenate",
                        List.of(params.get(2), params.get(0)));
                compileExpr(concat, ctx);
                // Point fold node to the synthetic concatenate via inlinedBody.
                // Fold produces a single list value (ONE) — concatenate gives MANY, but fold
                // semantics
                // are ONE. compileLambda reads isMany() to decide whether to UNNEST.
                TypeInfo concatInfo = types.get(concat);
                ExpressionType foldType = concatInfo.expressionType() != null
                        ? ExpressionType.one(concatInfo.expressionType().type())
                        : concatInfo.expressionType();
                var info = TypeInfo.from(concatInfo)
                        .inlinedBody(concat)
                        .expressionType(foldType)
                        .build();
                types.put(af, info);
                return info;
            }

            // Determine element type from source
            GenericType elemType = GenericType.Primitive.ANY;
            if (sourceInfo != null && sourceInfo.isMany()
                    && sourceInfo.type() != null
                    && sourceInfo.type().elementType() != null) {
                elemType = sourceInfo.type().elementType();
            }

            // Determine init/accumulator type
            TypeInfo initInfo = types.get(params.get(2));
            GenericType accType = initInfo != null && initInfo.type() != null
                    ? initInfo.type()
                    : GenericType.Primitive.ANY;

            String elemParam = parameters.size() >= 1 ? parameters.get(0).name() : "x";
            String accParam = parameters.size() >= 2 ? parameters.get(1).name() : "y";

            // --- Cross-type scalar fold ---
            // DuckDB list_reduce requires init type = element type.
            // When they differ, decompose into: fold(map(source, elem→transform),
            // {__x,acc→acc+__x}, init)
            boolean isCrossType = isCrossTypeFold(elemType, accType, params.get(2));
            if (isCrossType && !body.isEmpty()) {
                // Compile body with params in scope to type-check it
                CompilationContext lambdaCtx = ctx
                        .withLambdaParam(elemParam, elemType)
                        .withLambdaParam(accParam, accType);
                compileExpr(body.get(0), lambdaCtx);

                // Extract element-only transform from Pure AST body
                ValueSpecification elemTransform = extractFoldElementTransform(
                        body.get(0), accParam);
                if (elemTransform != null) {
                    // Build: fold(map(source, {elem→transform}), {__x,acc→acc+__x}, init)
                    var mapLambda = new LambdaFunction(
                            List.of(new Variable(elemParam)), elemTransform);
                    var mapped = new AppliedFunction("map",
                            List.of(params.get(0), mapLambda), true);
                    String freshX = "__x";
                    var reduceBody = new AppliedFunction("plus",
                            List.of(new Variable(accParam), new Variable(freshX)));
                    var reduceLambda = new LambdaFunction(
                            List.of(new Variable(freshX), new Variable(accParam)),
                            reduceBody);
                    var newFold = new AppliedFunction("fold",
                            List.of(mapped, reduceLambda, params.get(2)), true);
                    compileExpr(newFold, ctx);
                    var info = TypeInfo.from(types.get(newFold)).inlinedBody(newFold).build();
                    types.put(af, info);
                    return info;
                }
            }

            // --- List-accumulator fold ---
            // Init is a list (e.g., [-1, 0]) but source elements are scalar.
            // Wrap each source element in a single-element list, then unwrap in the body.
            // We compile parts directly and set inlinedBody — we do NOT build a new fold
            // AST (that would re-enter compileFold and hit this branch again).
            if (params.get(2) instanceof PureCollection && !body.isEmpty()) {
                // Build: map(source, {__e → [__e]}) — wraps each element in a list
                String wrapParam = "__e";
                var wrapBody = new PureCollection(List.of(new Variable(wrapParam)));
                var wrapLambda = new LambdaFunction(
                        List.of(new Variable(wrapParam)), wrapBody);
                var wrappedSource = new AppliedFunction("map",
                        List.of(params.get(0), wrapLambda), true);
                // Compile the wrapped source so types propagate
                compileExpr(wrappedSource, ctx);

                // Rewrite body: replace all Variable(elemParam) with at(Variable(elemParam), 0)
                ValueSpecification rewrittenBody = substituteVariable(
                        body.get(0), elemParam,
                        new AppliedFunction("at",
                                List.of(new Variable(elemParam), new CInteger(0L)), true));

                // Compile the rewritten body with proper param types
                GenericType wrappedElemType = accType.isList() ? accType : GenericType.listOf(elemType);
                CompilationContext lambdaCtx = ctx
                        .withLambdaParam(elemParam, wrappedElemType)
                        .withLambdaParam(accParam, accType);
                compileExpr(rewrittenBody, lambdaCtx);

                // Build the synthetic fold AST for PlanGenerator to process
                var newLambda = new LambdaFunction(
                        List.of(new Variable(elemParam), new Variable(accParam)),
                        rewrittenBody);
                var newFold = new AppliedFunction("fold",
                        List.of(wrappedSource, newLambda, params.get(2)), true);
                // Set scalar TypeInfo directly — do NOT call compileExpr which would re-enter.
                // Fold produces a single list value — stamp ONE so ExecutionResult
                // unwraps the DuckDB array (vs MANY which treats rows as elements).
                var info = TypeInfo.builder()
                        .inlinedBody(newFold)
                        .expressionType(ExpressionType.one(accType))
                        .build();
                types.put(af, info);
                return info;
            }

            // --- Standard same-type fold ---
            CompilationContext lambdaCtx = ctx;
            if (parameters.size() >= 1) {
                lambdaCtx = lambdaCtx.withLambdaParam(elemParam, elemType);
            }
            if (parameters.size() >= 2) {
                lambdaCtx = lambdaCtx.withLambdaParam(accParam, accType);
            }

            // Compile body with params in scope
            if (!body.isEmpty()) {
                compileExpr(body.get(0), lambdaCtx);
            }
            // Propagate accumulator type — fold result has the same type as
            // init/accumulator.
            return scalarTyped(af, accType);
        }
        throw new PureCompileException("Unresolved type for function: " + simpleName(af.function()));
    }

    /** Checks if a fold lambda body is the add pattern: $acc->add($val) */
    private static boolean isFoldAddPattern(LambdaFunction lf) {
        if (lf.parameters().size() < 2 || lf.body().isEmpty())
            return false;
        String elemParam = lf.parameters().get(0).name();
        String accParam = lf.parameters().get(1).name();
        // Body must be: add(acc, val) — an AppliedFunction named "add"
        if (lf.body().get(0) instanceof AppliedFunction bodyAf
                && TypeInfo.simpleName(bodyAf.function()).equals("add")
                && bodyAf.parameters().size() == 2) {
            // First param of add is the accumulator, second is the element
            var addSource = bodyAf.parameters().get(0);
            var addElem = bodyAf.parameters().get(1);
            return addSource instanceof Variable accVar && accVar.name().equals(accParam)
                    && addElem instanceof Variable elemVar && elemVar.name().equals(elemParam);
        }
        return false;
    }

    /**
     * Returns true when the fold source element type differs from the init type,
     * indicating list_reduce would fail with a type mismatch in DuckDB.
     * Does NOT match list-accumulator case (init is a Collection) — that's handled
     * separately.
     */
    private static boolean isCrossTypeFold(GenericType elemType, GenericType accType,
            ValueSpecification initNode) {
        // List-accumulator is handled separately
        if (initNode instanceof PureCollection)
            return false;
        if (elemType == null || accType == null)
            return false;
        if (elemType == GenericType.Primitive.ANY || accType == GenericType.Primitive.ANY)
            return false;
        // Compare type names — different means cross-type
        String elemName = elemType.typeName();
        String accName = accType.isList() ? accType.elementType().typeName() : accType.typeName();
        if (elemName == null || accName == null)
            return false;
        return !elemName.equals(accName);
    }

    /**
     * Extracts the element-only transform from a fold body by stripping the
     * accumulator
     * from the left spine of a plus() chain.
     *
     * <p>
     * Example: body = plus(plus(acc, '; '), p.lastName) → returns plus('; ',
     * p.lastName)
     * <p>
     * Example: body = plus(acc, length(val)) → returns length(val)
     *
     * @return element-only subtree, or null if body can't be decomposed
     */
    private static ValueSpecification extractFoldElementTransform(
            ValueSpecification body, String accParam) {
        if (!(body instanceof AppliedFunction af))
            return null;
        String fname = TypeInfo.simpleName(af.function());
        if (!"plus".equals(fname) || af.parameters().size() != 2)
            return null;

        ValueSpecification left = af.parameters().get(0);
        ValueSpecification right = af.parameters().get(1);

        // Base case: left is the accumulator variable → return right
        if (left instanceof Variable v && v.name().equals(accParam)) {
            return right;
        }

        // Recursive case: left is another plus() chain containing the accumulator
        if (left instanceof AppliedFunction leftAf
                && "plus".equals(TypeInfo.simpleName(leftAf.function()))) {
            ValueSpecification stripped = extractFoldElementTransform(left, accParam);
            if (stripped != null) {
                return new AppliedFunction("plus", List.of(stripped, right));
            }
        }

        return null;
    }

    /**
     * Recursively replaces all Variable references matching the given name with a
     * replacement expression. Used for list-accumulator fold: replace $x with
     * at($x, 0).
     */
    private static ValueSpecification substituteVariable(
            ValueSpecification vs, String varName, ValueSpecification replacement) {
        if (vs instanceof Variable v && v.name().equals(varName)) {
            return replacement;
        }
        if (vs instanceof AppliedFunction af) {
            boolean changed = false;
            var newParams = new java.util.ArrayList<ValueSpecification>(af.parameters().size());
            for (var p : af.parameters()) {
                var sub = substituteVariable(p, varName, replacement);
                if (sub != p)
                    changed = true;
                newParams.add(sub);
            }
            return changed
                    ? new AppliedFunction(af.function(), newParams, af.hasReceiver())
                    : af;
        }
        if (vs instanceof LambdaFunction(List<Variable> parameters, List<ValueSpecification> body)) {
            // Don't substitute inside lambdas that shadow the variable name
            for (var param : parameters) {
                if (param.name().equals(varName))
                    return vs;
            }
            boolean changed = false;
            var newBody = new java.util.ArrayList<ValueSpecification>(body.size());
            for (var b : body) {
                var sub = substituteVariable(b, varName, replacement);
                if (sub != b)
                    changed = true;
                newBody.add(sub);
            }
            return changed ? new LambdaFunction(parameters, newBody) : vs;
        }
        if (vs instanceof PureCollection(List<ValueSpecification> values)) {
            boolean changed = false;
            var newVals = new java.util.ArrayList<ValueSpecification>(values.size());
            for (var v : values) {
                var sub = substituteVariable(v, varName, replacement);
                if (sub != v)
                    changed = true;
                newVals.add(sub);
            }
            return changed ? new PureCollection(newVals) : vs;
        }
        // Literals, ClassInstance, etc. — no variables to substitute
        return vs;
    }

    /** Compiles match(input, [branches], extraParams...) — static type dispatch. */
    private TypeInfo compileMatch(AppliedFunction af, CompilationContext ctx) {
        List<ValueSpecification> params = af.parameters();
        // Compile all params first
        for (var p : params) {
            try {
                compileExpr(p, ctx);
            } catch (PureCompileException ignored) {
            }
        }
        if (params.size() < 2)
            throw new PureCompileException("match requires at least 2 parameters: value, branches");

        // Determine input type name
        String inputTypeName = inferTypeName(params.get(0));
        if (inputTypeName == null)
            throw new PureCompileException("match: cannot infer input type");

        // Extract branches from Collection (params[1])
        List<LambdaFunction> branches;
        if (params.get(1) instanceof PureCollection(List<ValueSpecification> values)) {
            branches = values.stream()
                    .filter(v -> v instanceof LambdaFunction)
                    .map(v -> (LambdaFunction) v)
                    .toList();
        } else if (params.get(1) instanceof LambdaFunction lf) {
            branches = List.of(lf);
        } else {
            throw new PureCompileException("match: second parameter must be a lambda or collection of lambdas");
        }

        // Determine if input is a collection (affects multiplicity matching)
        boolean inputIsMany = (params.get(0) instanceof PureCollection(List<ValueSpecification> values)
                && values.size() != 1)
                || (types.get(params.get(0)) != null && types.get(params.get(0)).isMany());

        // Find matching branch by type + multiplicity
        for (var branch : branches) {
            if (branch.parameters().isEmpty())
                continue;
            Variable branchParam = branch.parameters().get(0);
            if (branchParam.typeName() == null)
                continue;
            String branchType = TypeInfo.simpleName(branchParam.typeName());
            if (branchType.equals(inputTypeName)
                    || branchType.equals("Any")
                    || inputTypeName.equals("Any")) {
                // Check multiplicity compatibility
                String mult = branchParam.multiplicity();
                boolean branchAcceptsMany = mult == null || mult.equals("*")
                        || mult.equals("0") || mult.contains("..");
                if (inputIsMany && !branchAcceptsMany)
                    continue;
                // Match found — compile the body with params bound as let bindings
                // so that variable references get substituted with actual values
                CompilationContext matchCtx = ctx;
                // Bind match param → input value as let binding
                matchCtx = matchCtx.withLetBinding(branchParam.name(), params.get(0));
                // If there's an extra param (params[2]), bind it similarly
                if (branch.parameters().size() > 1 && params.size() > 2) {
                    Variable extraParam = branch.parameters().get(1);
                    matchCtx = matchCtx.withLetBinding(extraParam.name(), params.get(2));
                }
                if (!branch.body().isEmpty()) {
                    ValueSpecification body = branch.body().get(0);
                    TypeInfo bodyInfo = compileExpr(body, matchCtx);
                    // Set inlinedBody so PlanGenerator follows the resolved expression
                    var info = TypeInfo.from(bodyInfo).inlinedBody(body).build();
                    types.put(af, info);
                    return info;
                }
            }
        }
        throw new PureCompileException("Unresolved type for function: " + simpleName(af.function()));
    }

    /** Infers the simple type name from an AST node. */
    private String inferTypeName(ValueSpecification vs) {
        if (vs instanceof CString)
            return "String";
        if (vs instanceof CInteger)
            return "Integer";
        if (vs instanceof CFloat)
            return "Float";
        if (vs instanceof CDecimal)
            return "Decimal";
        if (vs instanceof CBoolean)
            return "Boolean";
        if (vs instanceof CStrictDate)
            return "StrictDate";
        if (vs instanceof CDateTime)
            return "DateTime";
        if (vs instanceof PureCollection(List<ValueSpecification> values)) {
            // Infer from first element, or fall through to side table
            if (!values.isEmpty()) {
                return inferTypeName(values.get(0));
            }
        }
        // Check side table
        TypeInfo info = types.get(vs);
        if (info != null && info.type() != null) {
            GenericType st = info.type();
            // For list types (e.g. from cast), use the element type
            if (st.isList() && st.elementType() != null)
                st = st.elementType();
            if (st == GenericType.Primitive.STRING)
                return "String";
            if (st == GenericType.Primitive.INTEGER)
                return "Integer";
            if (st == GenericType.Primitive.FLOAT)
                return "Float";
            if (st == GenericType.Primitive.BOOLEAN)
                return "Boolean";
        }
        return null;
    }

    /** Compiles zip(list1, list2). */
    private TypeInfo compileZip(AppliedFunction af, CompilationContext ctx) {
        List<ValueSpecification> params = af.parameters();
        for (var p : params) {
            compileExpr(p, ctx);
        }
        // zip(list1, list2) → List<Pair<A,B>>
        GenericType elemA = GenericType.Primitive.ANY;
        GenericType elemB = GenericType.Primitive.ANY;
        if (params.size() >= 2) {
            TypeInfo aInfo = types.get(params.get(0));
            TypeInfo bInfo = types.get(params.get(1));
            if (aInfo != null && aInfo.isMany() && aInfo.type() != null) {
                elemA = aInfo.type().elementType();
            }
            if (bInfo != null && bInfo.isMany() && bInfo.type() != null) {
                elemB = bInfo.type().elementType();
            }
        }
        return scalarTypedMany(af, GenericType.listOf(GenericType.pairOf(elemA, elemB)));
    }

    /**
     * write() is a mutation: operates on relations (needs relational compilation
     * path)
     * but returns a scalar Integer count. relationType keeps PlanGenerator routing
     * through generateRelation → generateWrite; returnType tells execution layer
     * to extract a ScalarResult.
     */
    private TypeInfo compileWrite(AppliedFunction af, CompilationContext ctx) {
        List<ValueSpecification> params = af.parameters();
        for (var p : params) {
            try {
                compileExpr(p, ctx);
            } catch (PureCompileException ignored) {
            }
        }
        TypeInfo info = TypeInfo.builder()
                .expressionType(ExpressionType.one(GenericType.Primitive.INTEGER))
                .build();
        types.put(af, info);
        return info;
    }

    /** Compiles type conversion functions: cast, toMany, toOne, toVariant, to. */
    private TypeInfo compileTypeFunction(AppliedFunction af, CompilationContext ctx) {
        String func = simpleName(af.function());
        List<ValueSpecification> params = af.parameters();

        // Compile all params (source + @Type arg)
        for (var p : params) {
            compileExpr(p, ctx);
        }

        // Resolve target type from @Type argument (GenericTypeInstance)
        GenericType targetType = null;
        for (var p : params) {
            if (p instanceof GenericTypeInstance(String fullPath)) {
                targetType = GenericType.fromTypeName(simpleName(fullPath));
                break;
            }
        }

        return switch (func) {
            case "toMany" -> {
                // toMany always produces a list of the target type
                GenericType listType = targetType != null
                        ? GenericType.listOf(targetType)
                        : GenericType.listOf(GenericType.Primitive.ANY);
                TypeInfo info = TypeInfo.builder()
                        .expressionType(ExpressionType.one(listType)).build();
                types.put(af, info);
                yield info;
            }
            case "cast" -> {
                // cast preserves source shape: if source is relational, propagate relation type
                TypeInfo sourceInfo = !params.isEmpty() ? types.get(params.get(0)) : null;
                if (sourceInfo != null && sourceInfo.isRelational()) {
                    // Relational cast: propagate source relation type through
                    types.put(af, sourceInfo);
                    yield sourceInfo;
                }
                // Scalar cast: if source is list, result is list of target type
                if (sourceInfo != null && sourceInfo.type() != null && sourceInfo.type().isList() && targetType != null) {
                    TypeInfo info = TypeInfo.builder()
                            .expressionType(ExpressionType.one(GenericType.listOf(targetType))).build();
                    types.put(af, info);
                    yield info;
                }
                if (targetType != null) {
                    TypeInfo info = TypeInfo.builder()
                            .expressionType(ExpressionType.one(targetType)).build();
                    types.put(af, info);
                    yield info;
                }
                throw new PureCompileException("Unresolved type for function: " + simpleName(af.function()));
            }
            case "toOne" -> {
                // toOne extracts single value — if source has list type, return element type
                TypeInfo sourceInfo = !params.isEmpty() ? types.get(params.get(0)) : null;
                if (sourceInfo != null && sourceInfo.type() != null
                        && sourceInfo.type().isList()
                        && sourceInfo.type().elementType() != null) {
                    TypeInfo info = TypeInfo.builder()
                            .expressionType(ExpressionType.one(sourceInfo.type().elementType())).build();
                    types.put(af, info);
                    yield info;
                }
                // Non-list source: toOne is a no-op, propagate scalar type through
                if (sourceInfo != null && sourceInfo.type() != null) {
                    types.put(af, sourceInfo);
                    yield sourceInfo;
                }
                throw new PureCompileException("Unresolved type for function: " + simpleName(af.function()));
            }
            case "to" -> {
                // to(@Type) — variant conversion, produces target type
                if (targetType != null) {
                    TypeInfo info = TypeInfo.builder()
                            .expressionType(ExpressionType.one(targetType)).build();
                    types.put(af, info);
                    yield info;
                }
                throw new PureCompileException("Unresolved type for function: " + simpleName(af.function()));
            }
            case "toVariant" -> {
                // toVariant produces a variant (pass through source type for list detection)
                TypeInfo sourceInfo = !params.isEmpty() ? types.get(params.get(0)) : null;
                if (sourceInfo != null && sourceInfo.type() != null) {
                    types.put(af, sourceInfo);
                    yield sourceInfo;
                }
                throw new PureCompileException("Unresolved type for function: " + simpleName(af.function()));
            }
            default -> throw new PureCompileException("Unresolved type function: " + simpleName(af.function()));
        };
    }

    /**
     * Compiles variant get(source, key) — resolves access pattern (index vs field).
     */
    private TypeInfo compileGet(AppliedFunction af, CompilationContext ctx) {
        List<ValueSpecification> params = af.parameters();
        // Compile params that don't already have TypeInfo (avoid overwriting lambda
        // param info)
        for (var p : params) {
            if (types.get(p) == null) {
                try {
                    compileExpr(p, ctx);
                } catch (PureCompileException ignored) {
                }
            }
        }

        // Resolve access pattern from key argument
        TypeInfo.VariantAccess access = null;
        if (params.size() > 1) {
            ValueSpecification keyVs = params.get(1);
            if (keyVs instanceof CInteger(Number value)) {
                access = new TypeInfo.VariantAccess.IndexAccess(value.intValue());
            } else if (keyVs instanceof CString(String value)) {
                access = new TypeInfo.VariantAccess.FieldAccess(value);
            }
        }

        // Resolve target type from @Type annotation (3rd param)
        GenericType targetType = null;
        for (var pi : params) {
            if (pi instanceof GenericTypeInstance(String fullPath)) {
                targetType = GenericType.fromTypeName(simpleName(fullPath));
                break;
            }
        }

        var builder = TypeInfo.builder();
        // get() always returns a variant — default to JSON if no @Type annotation
        GenericType getType = targetType != null ? targetType : GenericType.Primitive.JSON;
        builder.expressionType(ExpressionType.one(getType));
        if (access != null)
            builder.variantAccess(access);
        TypeInfo info = builder.build();
        types.put(af, info);
        return info;
    }

    /**
     * Compiles functions driven by their registered signature.
     * Handles lambda parameters by binding their variables to the resolved type.
     *
     * Flow:
     * 1. Compile source (param 0) to resolve its type
     * 2. Resolve the right overload by arity + source type
     * 3. Bind type variables (T, V) from source
     * 4. For lambda params: bind lambda variable to resolved T, compile body
     * 5. Return based on signature's return type + multiplicity
     */
    // compileTypePropagating — replaced by ScalarChecker

    /**
     * Return type for aggregate functions, resolved from the registry.
     * Uses the scalar (arity-1) overload's parsed return type.
     * Throws if the aggregate is not registered — all aggregates MUST be in the
     * registry.
     */
    private GenericType aggReturnType(String aggFunc) {
        if (aggFunc == null) {
            // Inline lambda expression in groupBy — no function name to resolve
            return GenericType.Primitive.NUMBER;
        }
        var defs = builtinRegistry.resolve(aggFunc.toLowerCase());
        if (defs.isEmpty())
            defs = builtinRegistry.resolve(aggFunc);
        if (defs.isEmpty()) {
            throw new PureCompileException(
                    "Aggregate function '" + aggFunc + "' is not registered in BuiltinFunctionRegistry");
        }
        // Find the scalar aggregate overload (arity 1) — not the window overload (arity
        // 3)
        for (var d : defs) {
            if (d.arity() == 1 && d.returnType() instanceof PType.Concrete c) {
                GenericType gt = c.toGenericType();
                if (gt != null)
                    return gt;
            }
        }
        // Try any overload with a concrete return type
        for (var d : defs) {
            if (d.returnType() instanceof PType.Concrete c) {
                GenericType gt = c.toGenericType();
                if (gt != null)
                    return gt;
            }
        }
        // TypeVar return (e.g., minBy<T>→T, maxBy<T>→T): type depends on source column.
        // Return NUMBER so refinedAggReturnType() can refine to the actual source
        // column type.
        for (var d : defs) {
            if (d.returnType() instanceof PType.TypeVar) {
                return GenericType.Primitive.NUMBER;
            }
        }
        throw new PureCompileException(
                "Aggregate function '" + aggFunc + "' has no resolvable return type in its registered signatures");
    }



    /**
     * Infers a GenericType from a literal value expression (pivot count patterns).
     */
    private static GenericType inferLiteralType(ValueSpecification expr, GenericType fallback) {
        if (expr instanceof CInteger)
            return GenericType.Primitive.INTEGER;
        if (expr instanceof CFloat)
            return GenericType.Primitive.FLOAT;
        if (expr instanceof CDecimal)
            return GenericType.DEFAULT_DECIMAL;
        if (expr instanceof CString)
            return GenericType.Primitive.STRING;
        if (expr instanceof CBoolean)
            return GenericType.Primitive.BOOLEAN;
        return fallback;
    }

    /**
     * Infers a GenericType from an expression by walking the AST for column
     * references.
     * E.g., $x.treePlanted * $x.coefficient → finds treePlanted:Integer → Integer.
     * Returns the first resolved column type, or the fallback if none found.
     */
    private static GenericType inferExprType(ValueSpecification expr, Map<String, GenericType> sourceColumns,
            GenericType fallback) {
        if (expr instanceof AppliedProperty ap) {
            GenericType colType = sourceColumns.get(ap.property());
            if (colType != null)
                return colType;
        }
        if (expr instanceof AppliedFunction af) {
            for (var p : af.parameters()) {
                GenericType found = inferExprType(p, sourceColumns, null);
                if (found != null)
                    return found;
            }
        }
        return fallback;
    }



    /** Compiles if(condition, then-lambda, else-lambda) with type unification. */
    private TypeInfo compileIf(AppliedFunction af, CompilationContext ctx) {
        List<ValueSpecification> params = af.parameters();
        for (var p : params) {
            try {
                compileExpr(p, ctx);
            } catch (PureCompileException ignored) {
            }
        }
        // Unify then/else branch types
        GenericType resultType = null;
        if (params.size() >= 2) {
            TypeInfo thenInfo = types.get(params.get(1));
            if (thenInfo != null && thenInfo.type() != null) {
                resultType = thenInfo.type();
            }
        }
        if (params.size() >= 3) {
            TypeInfo elseInfo = types.get(params.get(2));
            if (elseInfo != null && elseInfo.type() != null) {
                GenericType elseType = elseInfo.type();
                if (resultType != null && !resultType.equals(elseType)) {
                    // Widen to common supertype: both numeric → NUMBER, otherwise ANY
                    if (resultType.isNumeric() && elseType.isNumeric()) {
                        resultType = GenericType.Primitive.NUMBER;
                    } else {
                        resultType = GenericType.Primitive.ANY;
                    }
                } else if (resultType == null) {
                    resultType = elseType;
                }
            }
        }
        if (resultType != null) {
            return scalarTyped(af, resultType);
        }
        throw new PureCompileException("Unresolved type for function: " + simpleName(af.function()));
    }

    /** Compiles block(stmt1, stmt2, ..., stmtN) — let binding scope. */


    /**
     * Compiles eval() using the inlinedBody sidecar pattern.
     * eval(functionRef, arg) → creates AppliedFunction(funcName, [arg]),
     * compiles it, and stores as inlinedBody so PlanGenerator processes the
     * resolved function instead of the original eval call.
     */
    private TypeInfo compileEval(AppliedFunction af, CompilationContext ctx) {
        List<ValueSpecification> params = af.parameters();
        // eval(functionRef, args...) — rewrite to normal function call
        if (params.size() >= 2 && params.get(0) instanceof PackageableElementPtr(String fullPath)) {
            String lastSegment = fullPath.contains("::")
                    ? fullPath.substring(fullPath.lastIndexOf("::") + 2)
                    : fullPath;
            String funcName = lastSegment.contains("_")
                    ? lastSegment.substring(0, lastSegment.indexOf('_'))
                    : lastSegment;
            // Create resolved function node and compile it
            List<ValueSpecification> evalArgs = params.subList(1, params.size());
            AppliedFunction resolved = new AppliedFunction(funcName, evalArgs);
            TypeInfo bodyResult = compileFunction(resolved, ctx);
            // Store as inlinedBody — PlanGenerator follows this pointer
            TypeInfo result = TypeInfo.from(bodyResult).inlinedBody(resolved).build();
            types.put(af, result);
            return result;
        }
        // eval(lambda, args) — compile lambda body and store as inlinedBody
        if (params.size() >= 2
                && params.get(0) instanceof LambdaFunction(List<Variable> parameters, List<ValueSpecification> body)) {
            if (!body.isEmpty()) {
                // Bind each lambda param to its corresponding arg value
                CompilationContext evalCtx = ctx;
                List<ValueSpecification> args = params.subList(1, params.size());
                for (int i = 0; i < parameters.size() && i < args.size(); i++) {
                    compileExpr(args.get(i), ctx);
                    evalCtx = evalCtx.withLetBinding(parameters.get(i).name(), args.get(i));
                }
                ValueSpecification evalBody = body.get(0);
                TypeInfo bodyResult = compileExpr(evalBody, evalCtx);
                TypeInfo result = TypeInfo.from(bodyResult).inlinedBody(evalBody).build();
                types.put(af, result);
                return result;
            }
        }
        // Fallback: route through ScalarChecker
        List<ValueSpecification> evalParams = af.parameters();
        TypeInfo evalSource = !evalParams.isEmpty() ? compileExpr(evalParams.get(0), ctx) : null;
        var evalInfo = new com.gs.legend.compiler.checkers.ScalarChecker(this).check(af, evalSource, ctx);
        types.put(af, evalInfo);
        return evalInfo;
    }

    /** Compiles letFunction('x', valueExpr) — standalone let. */
    private TypeInfo compileLet(AppliedFunction af, CompilationContext ctx) {
        List<ValueSpecification> params = af.parameters();
        if (params.size() >= 2) {
            return compileExpr(params.get(1), ctx);
        }
        if (!params.isEmpty()) {
            return compileExpr(params.get(0), ctx);
        }
        throw new PureCompileException("Unresolved type for function: " + simpleName(af.function()));
    }

    /**
     * Registry-gated dispatch: checks user-defined functions first, then the
     * builtin registry via ScalarChecker. If not found anywhere, throws a
     * compile error — NO passthrough.
     */
    private TypeInfo compileRegistryOrUserFunction(AppliedFunction af, String funcName, CompilationContext ctx) {
        String qualifiedName = af.function();

        // 1. User-defined function inlining (PureFunctionRegistry)
        var fn = functionRegistry.getFunction(qualifiedName)
                .or(() -> functionRegistry.getFunction(funcName));
        if (fn.isPresent()) {
            return inlineUserFunction(af, fn.get(), ctx);
        }

        // 2. Builtin function registry — strict type checking via ScalarChecker
        if (builtinRegistry.isRegistered(funcName)) {
            List<ValueSpecification> params = af.parameters();
            TypeInfo source = !params.isEmpty() ? compileExpr(params.get(0), ctx) : null;
            var info = new com.gs.legend.compiler.checkers.ScalarChecker(this).check(af, source, ctx);
            types.put(af, info);
            return info;
        }

        // 3. Unknown function → compile error. No passthrough!
        throw new PureCompileException(
                "Unknown function '" + funcName + "'. "
                        + "Function is not registered in the builtin registry and no user-defined function found. "
                        + "Available functions: " + builtinRegistry.functionCount() + " registered.");
    }

    /**
     * Inlines a user-defined Pure function by:
     * 1. Substituting $param with argument source text
     * 2. Re-parsing the expanded body into new AST
     * 3. Compiling the inlined AST recursively
     */
    private TypeInfo inlineUserFunction(AppliedFunction af,
            PureFunctionRegistry.FunctionEntry entry, CompilationContext ctx) {
        String body = entry.bodySource();
        List<String> paramNames = entry.paramNames();

        // Substitute parameters with argument source text
        if (af.hasReceiver() && af.sourceText() != null) {
            // Arrow call: $param0 = sourceText, $param1.. = argTexts
            if (!paramNames.isEmpty()) {
                body = body.replace("$" + paramNames.get(0), af.sourceText());
            }
            for (int i = 1; i < paramNames.size() && i <= af.argTexts().size(); i++) {
                String argText = af.argTexts().get(i - 1);
                if (argText != null && !argText.isEmpty()) {
                    body = body.replace("$" + paramNames.get(i), argText);
                }
            }
        } else {
            // Standalone call: all params from argTexts
            for (int i = 0; i < paramNames.size() && i < af.argTexts().size(); i++) {
                String argText = af.argTexts().get(i);
                if (argText != null && !argText.isEmpty()) {
                    body = body.replace("$" + paramNames.get(i), argText);
                }
            }
        }

        // Re-parse inlined body into new AST and compile
        ValueSpecification inlinedNode = PureParser.parseQuery(body);
        TypeInfo bodyResult = compileExpr(inlinedNode, ctx);
        // Store inlined body in TypeInfo — PlanGenerator processes it instead of the
        // original call
        TypeInfo result = TypeInfo.from(bodyResult).inlinedBody(inlinedNode).build();
        types.put(af, result);
        return result;
    }

    // ========== ClassInstance (DSL containers) ==========

    private TypeInfo compileClassInstance(ClassInstance ci, CompilationContext ctx) {
        return switch (ci.type()) {
            case "relation" -> compileRelationAccessor(ci, ctx);
            case "tdsLiteral" -> compileTdsLiteral(ci, ctx);
            case "instance" -> compileInstanceLiteral(ci, ctx);
            default -> throw new PureCompileException("Unknown ClassInstance type: " + ci.type());
        };
    }

    /**
     * Compiles a struct literal ^ClassName(prop=val, ...) with proper type info.
     * Builds a GenericType.Relation.Schema where each property becomes a typed column,
     * so PlanGenerator can distinguish arrays (need UNNEST) from scalars.
     */
    private TypeInfo compileInstanceLiteral(ClassInstance ci, CompilationContext ctx) {
        var data = (ValueSpecificationBuilder.InstanceData) ci.value();
        var columns = new LinkedHashMap<String, GenericType>();

        // Built-in Pure standard library types (no model context needed)
        String simpleName = data.className().contains("::")
                ? data.className().substring(data.className().lastIndexOf("::") + 2)
                : data.className();

        com.gs.legend.model.m3.PureClass pureClass = null;

        if ("Pair".equals(simpleName) && data.typeArguments().size() == 2) {
            // Pair<A, B> → built-in with properties first:A, second:B
            var firstType = GenericType.fromTypeName(data.typeArguments().get(0));
            var secondType = GenericType.fromTypeName(data.typeArguments().get(1));
            pureClass = new com.gs.legend.model.m3.PureClass(
                    data.className().contains("::")
                            ? data.className().substring(0, data.className().lastIndexOf("::"))
                            : "",
                    "Pair", java.util.List.of(
                            new com.gs.legend.model.m3.Property("first",
                                    com.gs.legend.model.m3.PrimitiveType.fromName(firstType.typeName()),
                                    new com.gs.legend.model.m3.Multiplicity(1, 1)),
                            new com.gs.legend.model.m3.Property("second",
                                    com.gs.legend.model.m3.PrimitiveType.fromName(secondType.typeName()),
                                    new com.gs.legend.model.m3.Multiplicity(1, 1))));
        }

        // Fall back to model context for user-defined classes
        if (pureClass == null && modelContext != null) {
            pureClass = modelContext.findClass(data.className()).orElse(null);
        }
        if (pureClass == null) {
            throw new PureCompileException(
                    "Struct literal: class '" + data.className() + "' not found in model context");
        }

        for (var entry : data.properties().entrySet()) {
            String propName = entry.getKey();
            var propOpt = pureClass.findProperty(propName);
            if (propOpt.isEmpty()) {
                throw new PureCompileException(
                        "Struct literal: property '" + propName + "' not found in class '"
                                + data.className() + "'");
            }
            var prop = propOpt.get();
            GenericType propType = GenericType.fromType(prop.genericType());
            // Wrap in List if multiplicity is [*]
            if (prop.isCollection()) {
                propType = GenericType.listOf(propType);
            }
            columns.put(propName, propType);
            // Compile the property value expression so Variables etc. are in the side table
            compileExpr(entry.getValue(), ctx);
            // If property is to-many [*] but value is a single element (not a Collection),
            // tag the value's TypeInfo with list scalarType so PlanGenerator wraps it in
            // [].
            if (prop.isCollection() && !(entry.getValue() instanceof PureCollection)) {
                var valInfo = types.get(entry.getValue());
                if (valInfo != null) {
                    types.put(entry.getValue(),
                            TypeInfo.from(valInfo).expressionType(ExpressionType.one(propType)).build());
                }
            }
        }

        var rt = GenericType.Relation.Schema.withoutPivot(columns);

        // Build identity mapping — scalar primitives get identity PropertyMappings
        var identityMapping = RelationalMapping.identity(pureClass);

        // Synthesize associations for to-many class-typed properties
        // These get AssociationTarget(null, null, true) — no Join signals UNNEST
        var associations = new java.util.LinkedHashMap<String, TypeInfo.AssociationTarget>();
        for (var prop : pureClass.allProperties()) {
            if (prop.isCollection() && prop.genericType() instanceof com.gs.legend.model.m3.PureClass elementClass) {
                // Resolve FULL class from modelContext — property genericType may be a
                // forward-reference stub with no properties
                var resolvedClass = modelContext != null
                        ? modelContext.findClass(elementClass.qualifiedName()).orElse(elementClass)
                        : elementClass;
                // Build identity mapping for element class so compileProject can resolve leaf
                // properties
                var targetMapping = RelationalMapping.identity(resolvedClass);
                associations.put(prop.name(), new TypeInfo.AssociationTarget(targetMapping, null, true));
            }
        }

        var info = TypeInfo.builder()
                .mapping(identityMapping)
                .associations(associations.isEmpty() ? java.util.Map.of() : java.util.Map.copyOf(associations))
                .expressionType(ExpressionType.many(new GenericType.Relation(rt)))
                .build();
        types.put(ci, info);
        return info;
    }

    private TypeInfo compileRelationAccessor(ClassInstance ci, CompilationContext ctx) {
        String tableRef = (String) ci.value();
        Table table = resolveTable(tableRef);
        return typed(ci, tableToRelationType(table), null);
    }

    private TypeInfo compileTdsLiteral(ClassInstance ci, CompilationContext ctx) {
        String raw = (String) ci.value();
        com.gs.legend.ast.TdsLiteral tds = com.gs.legend.ast.TdsLiteral.parse(raw);
        // Build GenericType.Relation.Schema from parsed column names and types
        Map<String, GenericType> columns = new LinkedHashMap<>();
        for (int i = 0; i < tds.columns().size(); i++) {
            var col = tds.columns().get(i);
            GenericType colType = mapTdsColumnType(col.type());
            // If no type annotation, infer from first non-null data value
            if (colType == null && !tds.rows().isEmpty()) {
                for (var row : tds.rows()) {
                    if (i < row.size() && row.get(i) != null) {
                        Object val = row.get(i);
                        if (val instanceof Long)
                            colType = GenericType.Primitive.INTEGER;
                        else if (val instanceof Double)
                            colType = GenericType.Primitive.FLOAT;
                        else if (val instanceof Boolean)
                            colType = GenericType.Primitive.BOOLEAN;
                        else
                            colType = GenericType.Primitive.STRING;
                        break;
                    }
                }
            }
            columns.put(col.name(), colType != null ? colType : GenericType.Primitive.STRING);
        }
        // Register type on the ORIGINAL ci so callers can look it up
        var info = typed(ci, GenericType.Relation.Schema.withoutPivot(columns), null);
        // Also register a parsed-TDS ClassInstance for PlanGenerator
        types.put(new ClassInstance("tdsLiteral", tds), info);
        return info;
    }

    /** Maps a TDS type annotation string to a GenericType. */
    private static GenericType mapTdsColumnType(String typeStr) {
        if (typeStr == null)
            return null;
        return switch (typeStr) {
            case "Integer" -> GenericType.Primitive.INTEGER;
            case "Float", "Number" -> GenericType.Primitive.FLOAT;
            case "Decimal" -> GenericType.DEFAULT_DECIMAL;
            case "Boolean" -> GenericType.Primitive.BOOLEAN;
            case "String" -> GenericType.Primitive.STRING;
            case "Date", "StrictDate" -> GenericType.Primitive.STRICT_DATE;
            case "DateTime" -> GenericType.Primitive.DATE_TIME;
            default -> GenericType.Primitive.STRING;
        };
    }

    /**
     * Type-checks a scalar expression, validating column references.
     * When a mapping is bound (class-based queries), resolves property
     * names through the mapping before checking column existence.
     */
    private void typeCheckExpression(ValueSpecification vs, CompilationContext ctx) {
        switch (vs) {
            case AppliedProperty ap -> {
                if (!ap.parameters().isEmpty() && ap.parameters().get(0) instanceof Variable v) {
                    GenericType.Relation.Schema relType = ctx.getRelationType(v.name());
                    if (relType != null && !relType.columns().isEmpty()) {
                        // Mapping exists — verify the property name is a valid column
                        // (projected columns use property names, not physical column names)
                        ClassMapping mapping = ctx.getMapping(v.name());
                        if (mapping != null) {
                            var columnOpt = (mapping instanceof RelationalMapping rm2)
                                    ? rm2.getColumnForProperty(ap.property())
                                    : java.util.Optional.<String>empty();
                            if (columnOpt.isPresent()) {
                                // Property is in the mapping — check the property name (alias)
                                // against projected columns, not the physical column name
                                if (relType.columns().containsKey(ap.property())) {
                                    // OK — property name matches a projected column
                                } else {
                                    // Try physical column name as fallback (direct table access)
                                    relType.requireColumn(columnOpt.get());
                                }
                            }
                            // If property not in mapping, allow it through
                            // (could be a computed field or direct column access)
                        } else {
                            // No mapping — check column name directly
                            relType.requireColumn(ap.property());
                        }
                        // Resolve column type and store in side table
                        GenericType colType = relType.columns().get(ap.property());
                        if (colType != null) {
                            types.put(ap, TypeInfo.builder()
                                    .expressionType(ExpressionType.one(colType)).build());
                        }
                    }
                }
            }
            case AppliedFunction af -> {
                // Check for user-defined function inlining
                String funcName = af.function();
                String simple = simpleName(funcName);
                var fn = functionRegistry.getFunction(funcName)
                        .or(() -> functionRegistry.getFunction(simple));
                if (fn.isPresent()) {
                    inlineUserFunction(af, fn.get(), ctx);
                }
                for (var param : af.parameters()) {
                    typeCheckExpression(param, ctx);
                }
                // Dispatch type functions properly (toMany, to, toVariant, cast)
                // so they populate correct TypeInfo with @Type arguments
                if ("toMany".equals(simple) || "to".equals(simple)
                        || "toVariant".equals(simple) || "cast".equals(simple)) {
                    compileTypeFunction(af, ctx);
                } else if ("get".equals(simple)) {
                    compileGet(af, ctx);
                } else if ("toOne".equals(simple) && !af.parameters().isEmpty()) {
                    TypeInfo innerType = types.get(af.parameters().get(0));
                    if (innerType != null && innerType.type() != null) {
                        types.put(af, TypeInfo.builder()
                                .expressionType(ExpressionType.one(innerType.type())).build());
                    }
                }
                // List-preserving functions: propagate source list type through
                // filter/sort/reverse produce same list type, map may transform element type
                // Only set if not already computed by dedicated compile methods (avoids
                // overwrite)
                if (types.get(af) == null
                        && ("filter".equals(simple) || "sort".equals(simple)
                                || "reverse".equals(simple) || "map".equals(simple))
                        && !af.parameters().isEmpty()) {
                    TypeInfo sourceType = types.get(af.parameters().get(0));
                    if (sourceType != null) {
                        types.put(af, sourceType);
                    }
                }
            }
            case LambdaFunction lf -> {
                for (var body : lf.body()) {
                    typeCheckExpression(body, ctx);
                }
            }
            default -> {
            }
        }
    }

    // ========== Table Resolution ==========

    /**
     * Resolves a table reference string to a physical Table.
     */
    private Table resolveTable(String tableRef) {
        int dotIdx = tableRef.lastIndexOf('.');
        String simpleDbName = tableRef;
        String tableName = tableRef;

        if (dotIdx > 0) {
            String dbRef = tableRef.substring(0, dotIdx);
            tableName = tableRef.substring(dotIdx + 1);
            simpleDbName = dbRef.contains("::")
                    ? dbRef.substring(dbRef.lastIndexOf("::") + 2)
                    : dbRef;
        }

        String tableKey = simpleDbName + "." + tableName;

        if (modelContext != null) {
            var tableOpt = modelContext.findTable(tableKey);
            if (tableOpt.isPresent())
                return tableOpt.get();
            tableOpt = modelContext.findTable(tableName);
            if (tableOpt.isPresent())
                return tableOpt.get();
        }

        var mappingOpt = mappingRegistry().findByTableName(tableKey);
        if (mappingOpt.isPresent())
            return mappingOpt.get().table();
        mappingOpt = mappingRegistry().findByTableName(tableName);
        if (mappingOpt.isPresent())
            return mappingOpt.get().table();

        throw new PureCompileException("Table not found: " + tableRef);
    }

    /** Converts a physical Table to a RelationType. */
    private GenericType.Relation.Schema tableToRelationType(Table table) {
        Map<String, GenericType> columns = new LinkedHashMap<>();
        for (var col : table.columns()) {
            columns.put(col.name(), col.dataType().toGenericType());
        }
        return GenericType.Relation.Schema.withoutPivot(columns);
    }

    // ========== Extraction Utilities ==========

    static String simpleName(String qualifiedName) {
        return TypeInfo.simpleName(qualifiedName);
    }



    private String extractStringValue(ValueSpecification vs) {
        if (vs instanceof CString(String value))
            return value;
        throw new PureCompileException("Expected string, got: " + vs.getClass().getSimpleName());
    }


    /**
     * Scans a filter lambda body for multi-hop property paths and resolves
     * associations. Called during compileFilter().
     *
     * @param body    The lambda body expressions
     * @param mapping The source mapping
     * @return Map of association property name → pre-resolved target
     */
    private java.util.Map<String, TypeInfo.AssociationTarget> resolveAssociationsInBody(
            List<ValueSpecification> body, ClassMapping mapping) {
        if (modelContext == null || mapping == null) {
            return java.util.Map.of();
        }
        var result = new java.util.LinkedHashMap<String, TypeInfo.AssociationTarget>();
        for (var expr : body) {
            scanForAssociationPaths(expr, mapping, result);
        }
        return result.isEmpty() ? java.util.Map.of() : java.util.Map.copyOf(result);
    }

    /**
     * Recursively scans an expression tree for multi-hop AppliedProperty chains.
     * When found, resolves the association and stores in the result map.
     */
    private void scanForAssociationPaths(ValueSpecification vs, ClassMapping mapping,
            java.util.Map<String, TypeInfo.AssociationTarget> result) {
        if (vs instanceof AppliedProperty ap) {
            // Check if this is a multi-hop chain like $p.items.name
            List<String> chain = extractPropertyChain(ap);
            if (chain.size() > 1) {
                String assocProp = chain.get(0);
                if (!result.containsKey(assocProp)) {
                    resolveAndStore(assocProp, mapping, result);
                }
            }
            // Also scan parameters recursively
            for (var param : ap.parameters()) {
                scanForAssociationPaths(param, mapping, result);
            }
        } else if (vs instanceof AppliedFunction af) {
            // Detect single-hop association properties used with exists/forAll/map:
            // $p.addresses->exists(a|$a.city == 'NY')
            // AST: AppliedFunction("exists", [AppliedProperty("addresses", [$p]), Lambda])
            if (("exists".equals(af.function()) || "forAll".equals(af.function()) || "map".equals(af.function()))
                    && !af.parameters().isEmpty()
                    && af.parameters().get(0) instanceof AppliedProperty assocProp) {
                String propName = assocProp.property();
                if (!result.containsKey(propName)) {
                    resolveAndStore(propName, mapping, result);
                }
            }
            for (var param : af.parameters()) {
                scanForAssociationPaths(param, mapping, result);
            }
        } else if (vs instanceof LambdaFunction lf) {
            for (var expr : lf.body()) {
                scanForAssociationPaths(expr, mapping, result);
            }
        }
    }

    /**
     * Resolves a single association property and stores in the result map.
     */
    private void resolveAndStore(String assocProp, ClassMapping mapping,
            java.util.Map<String, TypeInfo.AssociationTarget> result) {
        String sourceClassName = mapping.targetClass().name();
        var assocNav = modelContext.findAssociationByProperty(sourceClassName, assocProp);
        if (assocNav.isPresent()) {
            var nav = assocNav.get();
            String targetClassName = nav.targetClassName();
            var targetMapping = modelContext.findMapping(targetClassName);
            if (targetMapping.isPresent()) {
                result.put(assocProp, new TypeInfo.AssociationTarget(
                        targetMapping.get(), nav.join(), nav.isToMany()));
            }
        }
    }
    /**
     * Recursively extracts property chain from nested AppliedProperty.
     * e.g., $p.items.name → ["items", "name"]
     */
    private List<String> extractPropertyChain(AppliedProperty ap) {
        if (!ap.parameters().isEmpty() && ap.parameters().get(0) instanceof AppliedProperty parent) {
            var result = new java.util.ArrayList<>(extractPropertyChain(parent));
            result.add(ap.property());
            return result;
        }
        return new java.util.ArrayList<>(List.of(ap.property()));
    }


    private String extractPropertyName(ValueSpecification vs) {
        if (vs instanceof AppliedProperty ap) {
            return ap.property();
        }
        if (vs instanceof AppliedFunction af) {
            // For chained property access like $p.date->year(),
            // extract bottommost property
            if (!af.parameters().isEmpty()) {
                return extractPropertyName(af.parameters().get(0));
            }
            // Zero-arg function like now(), today() — use the function name
            return simpleName(af.function());
        }
        return null;
    }


    // ========== Other AST Nodes ==========

    private TypeInfo compileLambda(LambdaFunction lf, CompilationContext ctx) {
        // Scope lambda params with their declared types
        CompilationContext lambdaCtx = ctx;
        for (var p : lf.parameters()) {
            GenericType paramType = null;
            if (p.typeName() != null) {
                try {
                    paramType = GenericType.Primitive.fromTypeName(p.typeName());
                } catch (IllegalArgumentException e) {
                    // Non-primitive type (class, enum) — store as ClassType
                    paramType = new GenericType.ClassType(p.typeName());
                }
            }
            lambdaCtx = lambdaCtx.withLambdaParam(p.name(), paramType);
        }
        if (lf.body().isEmpty()) {
            throw new PureCompileException("Unresolved type for lambda");
        }

        // Multi-statement body: process let bindings, compile final expression
        // Matches legend-pure's M3 model: lambda body IS the statement list
        List<ValueSpecification> body = lf.body();
        for (int i = 0; i < body.size() - 1; i++) {
            var stmt = body.get(i);
            if (stmt instanceof AppliedFunction letAf
                    && simpleName(letAf.function()).equals("letFunction")
                    && letAf.parameters().size() >= 2
                    && letAf.parameters().get(0) instanceof CString(String value)) {
                // let x = valueExpr → compile value, store binding in context
                ValueSpecification valueExpr = letAf.parameters().get(1);
                compileExpr(valueExpr, lambdaCtx);
                lambdaCtx = lambdaCtx.withLetBinding(value, valueExpr);
            } else {
                compileExpr(stmt, lambdaCtx);
            }
        }

        // Compile the final (result) expression with enriched context
        TypeInfo bodyInfo = compileExpr(body.getLast(), lambdaCtx);
        if (bodyInfo.isScalar() && bodyInfo.type() != null) {
            // Propagate multiplicity from body — if body is MANY (list-producing),
            // the lambda must also be MANY so UNNEST is applied at the root.
            if (bodyInfo.isMany()) {
                return scalarTypedMany(lf, bodyInfo.type());
            }
            return scalarTyped(lf, bodyInfo.type());
        }
        // Mutations (e.g. write()) set relationType for PlanGenerator routing but
        // returnType as scalar (Integer). Propagate that returnType so the root
        // stamping logic doesn't overwrite it with Relation.
        if (bodyInfo.expressionType() != null && !bodyInfo.expressionType().isRelation()) {
            var info = TypeInfo.builder()
                    .mapping(bodyInfo.mapping())
                    .expressionType(bodyInfo.expressionType())
                    .build();
            types.put(lf, info);
            return info;
        }
        return typed(lf, bodyInfo.schema(), bodyInfo.mapping());
    }

    private TypeInfo compileVariable(Variable v, CompilationContext ctx) {
        GenericType.Relation.Schema varType = ctx.getRelationType(v.name());
        if (varType != null) {
            return typed(v, varType, null);
        }
        // Let binding → inline the bound expression via inlinedBody
        // PlanGenerator already handles inlinedBody at both relational and scalar
        // levels
        ValueSpecification letValue = ctx.getLetBinding(v.name());
        if (letValue != null) {
            TypeInfo letInfo = compileExpr(letValue, ctx);
            // Create a TypeInfo with the compiled type info AND inlinedBody pointing to the
            // bound value
            TypeInfo inlined = TypeInfo.from(letInfo).inlinedBody(letValue).build();
            types.put(v, inlined);
            return inlined;
        }
        // Lambda parameter — mark in side table with declared type
        GenericType lambdaType = ctx.getLambdaParamType(v.name());
        if (lambdaType != null) {
            var info = TypeInfo.builder()
                    .expressionType(ExpressionType.one(lambdaType)).lambdaParam(true).build();
            types.put(v, info);
            return info;
        }
        if (ctx.isLambdaParam(v.name())) {
            throw new PureCompileException(
                    "Lambda param '" + v.name() + "' has no resolved type — "
                            + "upstream must infer type from function signature or annotation");
        }
        throw new PureCompileException("Unresolved type for variable: " + v.name());
    }

    private TypeInfo compileProperty(AppliedProperty ap, CompilationContext ctx) {
        if (!ap.parameters().isEmpty() && ap.parameters().get(0) instanceof Variable v) {
            GenericType.Relation.Schema relType = ctx.getRelationType(v.name());
            if (relType != null) {
                relType.requireColumn(ap.property());
                // Resolve the column's type from the RelationType
                GenericType colType = relType.columns().get(ap.property());
                if (colType != null) {
                    return scalarTyped(ap, colType);
                }
            }
            // Lambda param with Tuple: resolve column type from Schema
            // Tuple = T in Relation<T> = row schema type
            GenericType paramType = ctx.getLambdaParamType(v.name());
            if (paramType instanceof GenericType.Tuple t) {
                t.schema().requireColumn(ap.property());
                GenericType colType = t.schema().columns().get(ap.property());
                if (colType != null) {
                    return scalarTyped(ap, colType);
                }
            }
            // Lambda param with ClassType: resolve field type from model context
            if (paramType instanceof GenericType.ClassType(String qualifiedName) && modelContext != null) {
                var classOpt = modelContext.findClass(qualifiedName);
                if (classOpt.isPresent()) {
                    var propOpt = classOpt.get().findProperty(ap.property());
                    if (propOpt.isPresent()) {
                        GenericType fieldType = GenericType.fromType(propOpt.get().genericType());
                        return scalarTyped(ap, fieldType);
                    }
                }
                // Association-injected properties (e.g., $p.addresses from Association Person_Address)
                // These are first-class properties in Pure, just stored on the Association rather than the Class.
                var assocNav = modelContext.findAssociationByProperty(qualifiedName, ap.property());
                if (assocNav.isPresent()) {
                    var nav = assocNav.get();
                    GenericType targetType = new GenericType.ClassType(nav.targetClassName());
                    if (nav.isToMany()) {
                        // To-many: return List<TargetClass> — caller handles exists/map desugaring
                        targetType = GenericType.listOf(targetType);
                    }
                    var info = TypeInfo.builder()
                            .expressionType(nav.isToMany()
                                    ? ExpressionType.many(targetType)
                                    : ExpressionType.one(targetType))
                            .build();
                    types.put(ap, info);
                    return info;
                }
            }
            // Let-bound variable → resolve inlinedBody to instance ClassInstance
            TypeInfo vInfo = types.get(v);
            if (vInfo == null) {
                vInfo = compileExpr(v, ctx);
            }
            if (vInfo != null && vInfo.inlinedBody() instanceof ClassInstance ci
                    && "instance".equals(ci.type())) {
                return inlineStructExtract(ap, ci, ctx);
            }
        }
        // .prop directly on instance literal: ^Person(firstName='John').firstName
        if (!ap.parameters().isEmpty()
                && ap.parameters().get(0) instanceof ClassInstance ci
                && "instance".equals(ci.type())) {
            compileExpr(ci, ctx);
            return inlineStructExtract(ap, ci, ctx);
        }
        // .prop on a list-producing function is sugar for ->map(_x | _x.prop)
        // Desugar by building a synthetic map node and pointing via inlinedBody
        if (!ap.parameters().isEmpty() && ap.parameters().get(0) instanceof AppliedFunction ownerFn) {
            TypeInfo ownerInfo = compileExpr(ownerFn, ctx);
            if (ownerInfo != null && ownerInfo.isMany()) {
                var propVar = new Variable("_prop_x");
                var propAccess = new AppliedProperty(ap.property(), List.of(propVar));
                var lambda = new LambdaFunction(List.of(propVar), propAccess);
                var mapNode = new AppliedFunction("map", List.of(ownerFn, lambda));
                TypeInfo mapInfo = compileExpr(mapNode, ctx);
                // Point original property node → synthetic map via inlinedBody
                var info = TypeInfo.from(mapInfo).inlinedBody(mapNode).build();
                types.put(ap, info);
                return info;
            }
            // .prop on a Tuple (row from offset functions like lead/lag/nth/first):
            // Resolve column type directly — no project() desugaring needed.
            if (ownerInfo != null && ownerInfo.type() instanceof GenericType.Tuple rt) {
                GenericType colType = rt.schema().getColumnType(ap.property());
                if (colType != null) {
                    return scalarTyped(ap, colType);
                }
            }
            // .prop on a relational result (e.g., filter(...).legalName)
            // → desugar to single-column project so PlanGenerator builds proper FROM clause
            // Pure return type is the column type as a collection (e.g., String[*])
            if (ownerInfo != null && ownerInfo.isRelational()) {
                var propVar = new Variable("_rel_x");
                var propAccess = new AppliedProperty(ap.property(), List.of(propVar));
                var colSpec = new ColSpec(ap.property(), new LambdaFunction(List.of(propVar), propAccess), null);
                var colSpecCI = new ClassInstance("colSpec", colSpec);
                var projectNode = new AppliedFunction("project", List.of(ownerFn, colSpecCI));
                TypeInfo projectInfo = compileExpr(projectNode, ctx);
                // Extract the column's GenericType for the return type
                GenericType colType = ownerInfo.schema().getColumnType(ap.property());
                ExpressionType propExprType = colType != null
                        ? ExpressionType.many(GenericType.listOf(colType)) // String[*], Integer[*], etc.
                        : null;
                var info = TypeInfo.from(projectInfo)
                        .inlinedBody(projectNode)
                        .expressionType(propExprType != null ? propExprType : projectInfo.expressionType())
                        .build();
                types.put(ap, info);
                return info;
            }
            // .prop on a ClassType result (e.g., at(1) returning a single struct)
            // → synthesize structExtract(ownerFn, 'prop')
            if (ownerInfo != null && ownerInfo.type() instanceof GenericType.ClassType) {
                var extractNode = new AppliedFunction("structExtract",
                        List.of(ownerFn, new CString(ap.property())));
                var info = TypeInfo.builder()
                        .inlinedBody(extractNode)
                        .expressionType(ExpressionType.one(GenericType.Primitive.ANY)).build();
                types.put(ap, info);
                return info;
            }
        }
        // .prop on an AppliedProperty that returns a ClassType (multi-hop association path)
        // e.g., $p.addresses.city → addresses returns ClassType("Address"), resolve city from Address
        if (!ap.parameters().isEmpty() && ap.parameters().get(0) instanceof AppliedProperty ownerAp) {
            TypeInfo ownerInfo = compileExpr(ownerAp, ctx);
            if (ownerInfo != null && modelContext != null) {
                // Extract the inner ClassType (unwrap List if to-many)
                GenericType ownerType = ownerInfo.type();
                String className = null;
                if (ownerType instanceof GenericType.ClassType(String qn)) {
                    className = qn;
                } else if (ownerType.isList() && ownerType.elementType() instanceof GenericType.ClassType(String qn)) {
                    className = qn;
                }
                if (className != null) {
                    var classOpt = modelContext.findClass(className);
                    if (classOpt.isPresent()) {
                        var propOpt = classOpt.get().findProperty(ap.property());
                        if (propOpt.isPresent()) {
                            GenericType fieldType = GenericType.fromType(propOpt.get().genericType());
                            return scalarTyped(ap, fieldType);
                        }
                    }
                }
            }
        }
        throw new PureCompileException("Unresolved type for property: " + ap.property());
    }

    /**
     * Synthesizes structExtract(instance, 'field') for .property on instance
     * literals.
     * PlanGenerator handles structExtract → STRUCT_EXTRACT(struct, 'field').
     */
    private TypeInfo inlineStructExtract(AppliedProperty ap, ClassInstance ci,
            CompilationContext ctx) {
        var extractNode = new AppliedFunction("structExtract",
                List.of(ci, new CString(ap.property())));
        var info = TypeInfo.builder()
                .inlinedBody(extractNode)
                .expressionType(ExpressionType.one(GenericType.Primitive.ANY)).build();
        types.put(ap, info);
        return info;
    }

    private TypeInfo compileCollection(PureCollection coll, CompilationContext ctx) {
        // Compile each element so they're in the side table
        for (var v : coll.values()) {
            compileExpr(v, ctx);
        }
        GenericType elementType = unifyElementType(coll.values());
        // For struct collections, carry both list type AND struct column info
        // so isList() works (for contains/head/find dispatch) AND project() can resolve columns
        if (!coll.values().isEmpty()) {
            TypeInfo firstElem = types.get(coll.values().get(0));
            if (firstElem != null && firstElem.mapping() != null && firstElem.schema() != null) {
                var info = TypeInfo.builder()
                        .mapping(firstElem.mapping())
                        .associations(firstElem.associations())
                        .expressionType(ExpressionType.many(GenericType.listOf(elementType)))
                        .build();
                types.put(coll, info);
                return info;
            }
        }
        return scalarTypedMany(coll, GenericType.listOf(elementType));
    }

    /**
     * Infers GenericType from a ValueSpecification AST node.
     * Used for type unification in collections.
     */
    private GenericType typeOf(ValueSpecification vs) {
        // Check side table first (for compiled sub-expressions)
        TypeInfo info = types.get(vs);
        if (info != null && info.type() != null)
            return info.type();
        // Fall back to pattern matching on literal types
        return switch (vs) {
            case CInteger i -> GenericType.Primitive.INTEGER;
            case CFloat f -> GenericType.Primitive.FLOAT;
            case CDecimal d -> GenericType.DEFAULT_DECIMAL;
            case CString s -> GenericType.Primitive.STRING;
            case CBoolean b -> GenericType.Primitive.BOOLEAN;
            case CDateTime dt -> GenericType.Primitive.DATE_TIME;
            case CStrictDate sd -> GenericType.Primitive.STRICT_DATE;
            case CStrictTime st -> GenericType.Primitive.STRICT_TIME;
            case CLatestDate ld -> GenericType.Primitive.DATE_TIME;
            case PureCollection c -> GenericType.listOf(unifyElementType(c.values()));
            case ClassInstance ci when "instance".equals(ci.type())
                    && ci.value() instanceof ValueSpecificationBuilder.InstanceData data ->
                new GenericType.ClassType(data.className());
            default -> GenericType.Primitive.ANY;
        };
    }

    /**
     * Finds the common supertype for a list of expressions.
     * All numeric → NUMBER, all temporal → DATE, all same ClassType → that
     * ClassType,
     * all ClassTypes with common supertype → LCA ClassType, mixed → ANY.
     */
    private GenericType unifyElementType(java.util.List<ValueSpecification> values) {
        if (values.isEmpty())
            return GenericType.Primitive.ANY;
        var elementTypes = values.stream().map(this::typeOf).distinct().toList();
        if (elementTypes.size() == 1)
            return elementTypes.getFirst();
        if (elementTypes.stream().allMatch(GenericType::isNumeric))
            return GenericType.Primitive.NUMBER;
        if (elementTypes.stream().allMatch(GenericType::isTemporal))
            return GenericType.Primitive.DATE;
        // All ClassTypes: try to find common supertype
        if (elementTypes.stream().allMatch(t -> t instanceof GenericType.ClassType) && modelContext != null) {
            var classTypes = elementTypes.stream()
                    .map(t -> ((GenericType.ClassType) t).qualifiedName()).toList();
            // Pairwise LCA reduction
            String current = classTypes.get(0);
            for (int i = 1; i < classTypes.size(); i++) {
                var lcaOpt = modelContext.findLowestCommonAncestor(current, classTypes.get(i));
                if (lcaOpt.isEmpty())
                    return GenericType.Primitive.ANY;
                current = lcaOpt.get().qualifiedName();
            }
            return new GenericType.ClassType(current);
        }
        return GenericType.Primitive.ANY;
    }

    // ========== Compilation Context ==========

    /**
     * Context for compilation — tracks variable → GenericType.Relation.Schema and variable →
     * Mapping bindings.
     */
    public record CompilationContext(
            Map<String, GenericType.Relation.Schema> relationTypes,
            Map<String, ClassMapping> mappings,
            Map<String, GenericType> lambdaParams,
            Map<String, ValueSpecification> letBindings) {

        public CompilationContext() {
            this(Map.of(), Map.of(), Map.of(), Map.of());
        }

        public CompilationContext withRelationType(String paramName, GenericType.Relation.Schema type) {
            var newTypes = new HashMap<>(relationTypes);
            newTypes.put(paramName, type);
            return new CompilationContext(Map.copyOf(newTypes), mappings, lambdaParams, letBindings);
        }

        public CompilationContext withMapping(String paramName, ClassMapping mapping) {
            var newMappings = new HashMap<>(mappings);
            newMappings.put(paramName, mapping);
            return new CompilationContext(relationTypes, Map.copyOf(newMappings), lambdaParams, letBindings);
        }

        public CompilationContext withLambdaParam(String name, GenericType type) {
            var m = new HashMap<>(lambdaParams);
            m.put(name, type); // type may be null for untyped params (e.g., forAll(e|...))
            return new CompilationContext(relationTypes, mappings, Collections.unmodifiableMap(m), letBindings);
        }

        public CompilationContext withLetBinding(String name, ValueSpecification value) {
            var m = new HashMap<>(letBindings);
            m.put(name, value);
            return new CompilationContext(relationTypes, mappings, lambdaParams, Map.copyOf(m));
        }

        public boolean isLambdaParam(String name) {
            return lambdaParams.containsKey(name);
        }

        public GenericType getLambdaParamType(String name) {
            return lambdaParams.get(name);
        }

        public ValueSpecification getLetBinding(String name) {
            return letBindings.get(name);
        }

        public GenericType.Relation.Schema getRelationType(String name) {
            return relationTypes.get(name);
        }

        public ClassMapping getMapping(String name) {
            return mappings.get(name);
        }
    }

    // ========== Literal Type Classification ==========

    /**
     * Classifies a CInteger to the appropriate precision type.
     * INTEGER for values in [-2^31, 2^31-1] (INT32 range).
     * INT64 for values that fit in 64-bit but exceed INT32 range.
     * INT128 for BigInteger values exceeding INT64 range.
     */
    private static GenericType classifyInteger(CInteger ci) {
        if (ci.value() instanceof java.math.BigInteger) {
            return GenericType.Primitive.INT128;
        }
        long v = ci.value().longValue();
        if (v > Integer.MAX_VALUE || v < Integer.MIN_VALUE) {
            return GenericType.Primitive.INT128;
        }
        return GenericType.Primitive.INTEGER;
    }

    /**
     * Classifies a CDecimal to the appropriate precision type.
     * PrecisionDecimal(18,0) for integer-valued decimals (no fractional part)
     * to preserve DECIMAL type in SQL instead of being coerced to INTEGER.
     * DECIMAL for decimals with fractional parts.
     */
    private static GenericType classifyDecimal(CDecimal d) {
        String plain = d.value().toPlainString();
        if (!plain.contains(".")) {
            return new GenericType.PrecisionDecimal(38, 0);
        }
        return GenericType.DEFAULT_DECIMAL;
    }

    /**
     * Convenience accessor — never null (ModelContext guarantees non-null empty
     * registry).
     */
    private MappingRegistry mappingRegistry() {
        return modelContext.getMappingRegistry();
    }

    // ========== Type Registration Utilities ==========

    /** Registers a scalar TypeInfo with a known GenericType (multiplicity ONE). */
    private TypeInfo scalarTyped(ValueSpecification ast, GenericType type) {
        var info = TypeInfo.builder().expressionType(ExpressionType.one(type)).build();
        types.put(ast, info);
        return info;
    }

    /**
     * Registers a scalar TypeInfo with Multiplicity.MANY — produces N independent
     * values.
     */
    private TypeInfo scalarTypedMany(ValueSpecification ast, GenericType type) {
        var info = TypeInfo.builder().expressionType(ExpressionType.many(type)).build();
        types.put(ast, info);
        return info;
    }

    /**
     * Registers a relational TypeInfo in the side table and returns it.
     * All relational type registration in TypeChecker goes through this method.
     */
    private TypeInfo typed(ValueSpecification ast, GenericType.Relation.Schema relationType,
            ClassMapping mapping) {
        return typed(ast, relationType, mapping, java.util.Map.of());
    }

    /**
     * Registers relational TypeInfo with pre-resolved associations in the side table.
     */
    private TypeInfo typed(ValueSpecification ast, GenericType.Relation.Schema relationType,
            ClassMapping mapping, java.util.Map<String, TypeInfo.AssociationTarget> associations) {
        var info = TypeInfo.builder()
                .mapping(mapping).associations(associations)
                .expressionType(ExpressionType.many(new GenericType.Relation(relationType))).build();
        types.put(ast, info);
        return info;
    }
}
