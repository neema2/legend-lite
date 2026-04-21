package com.gs.legend.model;
import com.gs.legend.model.m3.Type;

import com.gs.legend.compiler.BuiltinClassRegistry;
import com.gs.legend.compiler.BuiltinRegistry;
import com.gs.legend.model.def.*;
import com.gs.legend.model.m3.*;
import com.gs.legend.model.mapping.ClassMapping;
import com.gs.legend.model.mapping.MappingRegistry;
import com.gs.legend.model.mapping.RelationalMapping;
import com.gs.legend.model.store.*;
import com.gs.legend.parser.NameResolver;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * Builds runtime model objects from Pure definitions.
 * 
 * Converts parsed Pure syntax into the runtime metamodel:
 * - ClassDefinition -> PureClass
 * - AssociationDefinition -> Association
 * - DatabaseDefinition -> Table(s) and Join(s)
 * - MappingDefinition -> RelationalMapping
 * 
 * Implements {@link ModelContext} to provide model lookups during compilation.
 */
public final class PureModelBuilder implements ModelContext {

    // ==================== Instance fields ====================

    private final SymbolTable symbols = new SymbolTable();
    private final ArrayList<PureClass> classes = new ArrayList<>();
    private final ArrayList<ClassDefinition> pendingClassDefinitions = new ArrayList<>();
    private final ArrayList<Association> associations = new ArrayList<>();
    private final ArrayList<Table> tables = new ArrayList<>();
    private final ArrayList<Join> joins = new ArrayList<>();
    private final ArrayList<View> views = new ArrayList<>();
    private final ArrayList<Filter> filters = new ArrayList<>();
    private final ArrayList<DatabaseDefinition> databases = new ArrayList<>();
    private final ArrayList<ProfileDefinition> profiles = new ArrayList<>();
    private final ArrayList<List<FunctionDefinition>> functions = new ArrayList<>();
    // Phase 2: typed downstream form, built at assemble() time alongside resolvedBody.
    // Parallel to `functions`, same index (function qualifiedName -> id).
    private final ArrayList<List<com.gs.legend.model.m3.PureFunction>> pureFunctions = new ArrayList<>();
    private final ArrayList<ConnectionDefinition> connections = new ArrayList<>();
    private final ArrayList<RuntimeDefinition> runtimes = new ArrayList<>();
    private final ArrayList<ServiceDefinition> services = new ArrayList<>();
    private final ArrayList<EnumDefinition> enums = new ArrayList<>();
    private final ArrayList<MappingDefinition> mappingDefinitions = new ArrayList<>();
    private final Map<String, Map<String, Join>> explicitAssociationJoins = new HashMap<>();
    private final MappingRegistry mappingRegistry = new MappingRegistry(symbols);
    private final ImportScope imports = new ImportScope();
    private boolean strict = false;

        /**
     * Constructs a new {@code PureModelBuilder} pre-seeded with platform enum definitions and
     * built-in imports from the singleton {@link BuiltinRegistry} (loaded once at JVM init).
     *
     * <p>Every model starts with the same baseline: all 15 Pure primitives and 7 platform enums
     * (Month, DurationUnit, JoinKind, etc.) are in-scope by simple name, and {@link #findEnum}
     * returns the platform enum declarations required by built-in function signatures (e.g.,
     * {@code month()->Month[1]}).
     */
    public PureModelBuilder() {
        for (String fqn : BuiltinRegistry.BUILTIN_IMPORTS) {
            imports.addImport(fqn);
        }
        for (EnumDefinition platformEnum : BuiltinRegistry.PLATFORM_ENUMS) {
            addEnum(platformEnum);
        }
        for (ProfileDefinition platformProfile : BuiltinRegistry.PLATFORM_PROFILES) {
            addProfile(platformProfile);
        }
        // Phase 2.5e: seed platform classes (Pair, Relation stubs, etc.) from the pre-parsed
        // catalog. Two-pass registration mirrors addSource — stubs first so cross-references
        // in the catalog resolve when the full addClass pass walks properties.
        for (ClassDefinition cd : BuiltinClassRegistry.BUILTIN_CLASS_DEFINITIONS) {
            registerClassStub(cd);
        }
        for (ClassDefinition cd : BuiltinClassRegistry.BUILTIN_CLASS_DEFINITIONS) {
            addClass(cd);
        }
    }

    // Primitive-int-indexed helpers — zero boxing, O(1) access
    private static <T> T idGet(ArrayList<T> list, int id) {
        return id >= 0 && id < list.size() ? list.get(id) : null;
    }
    private static <T> void idPut(ArrayList<T> list, int id, T value) {
        int gap = id - list.size() + 1;
        if (gap > 0) list.addAll(java.util.Collections.nCopies(gap, null));
        list.set(id, value);
    }

    /**
     * Returns the shared SymbolTable for downstream consumers (MappingNormalizer, etc.).
     */
    public SymbolTable symbolTable() {
        return symbols;
    }
    

    /**
     * Returns the ImportScope for query resolution (same imports as the model source).
     */
    public ImportScope imports() {
        return imports;
    }

    /**
     * Returns the set of all known FQNs (for name resolution).
     */
    public Set<String> allFqns() {
        return symbols.allFqns();
    }

    /**
     * Parses a Pure query and resolves simple names to FQN using imports.
     * In strict mode, skips resolution — caller guarantees all names are FQN.
     */
    public com.gs.legend.ast.ValueSpecification resolveQuery(String query) {
        var raw = com.gs.legend.parser.PureParser.parseQuery(query);
        var resolved = strict ? raw
                : com.gs.legend.parser.NameResolver.resolveQuery(raw, imports, symbols.allFqns());
        // Strip the | lambda wrapper — standard Pure query syntax |expr produces a
        // zero-parameter LambdaFunction. Unwrap so the pipeline sees the same AST
        // for |Person.all()->graphFetch() and Person.all()->graphFetch().
        if (resolved instanceof com.gs.legend.ast.LambdaFunction lf
                && lf.parameters().isEmpty() && !lf.body().isEmpty()) {
            return lf.body().size() == 1 ? lf.body().getFirst() : resolved;
        }
        return resolved;
    }

    /**
     * Enables strict mode — skips name resolution.
     * Caller guarantees all names are already FQN.
     */
    public PureModelBuilder strict() {
        this.strict = true;
        return this;
    }

    /**
     * @return true if strict mode is enabled
     */
    public boolean isStrict() {
        return strict;
    }


    // Lazily-built indexes for O(1) association lookups (avoids O(N) scans)
    private Map<String, List<Association>> classToAssociations;  // className → associations referencing that class

    /**
     * Batch ingest — the primary entry point for multi-source models. Runs the model-build
     * pipeline once over the entire batch so that cross-source forward references resolve
     * at every phase:
     *
     * <ol>
     *   <li><b>Parse</b> every source up front (cached via {@link ParseCache}).</li>
     *   <li><b>Phase 0</b> — intern every element FQN across all sources.</li>
     *   <li><b>Phase 0.5</b> — merge every non-strict source's imports (explicit +
     *       auto-imports of own package) into the shared {@link ImportScope} so
     *       {@link NameResolver} sees cross-source wildcards.</li>
     *   <li><b>Phase 1</b> — per source, name-resolve its definitions against the unified
     *       imports + FQN set.</li>
     *   <li><b>Phase 2</b> — register enums across the batch.</li>
     *   <li><b>Phase 3a</b> — stub every class across the batch (names only) so class
     *       bodies at phase 3b can cross-reference.</li>
     *   <li><b>Phase 3b</b> — register class bodies.</li>
     *   <li><b>Phase 4</b> — resolve superclass references once.</li>
     *   <li><b>Phase 5a</b> — register associations + databases across the batch.</li>
     *   <li><b>Phase 5b</b> — resolve database includes once.</li>
     *   <li><b>Phase 5c</b> — register mappings, services, profiles, functions, connections,
     *       runtimes across the batch.</li>
     *   <li><b>Phase 5d</b> — resolve mapping includes once.</li>
     *   <li><b>Phase 6</b> — {@code buildPureFunctions} + call-graph cycle detection once.</li>
     * </ol>
     *
     * <p><strong>Default is interpreted++.</strong> Function <em>body</em> type-checking is
     * NOT performed here — that is the opt-in {@link #compile()} verb's job. Callers who
     * want full eager verification invoke it after {@code addSources}.
     *
     * @param pureSources Pure source strings, ingested as a batch
     * @return this builder for chaining
     */
    public PureModelBuilder addSources(String... pureSources) {
        // Invalidate lazy indexes — new definitions may add associations / joins.
        classToAssociations = null;

        // Parse every source once up front.
        List<SourceState> states = new ArrayList<>(pureSources.length);
        for (String src : pureSources) {
            com.gs.legend.parser.ParseResult parsed = ParseCache.global().getOrParse(src);
            boolean isStrict = strict || src.stripLeading().startsWith("\"use strict\"");
            states.add(new SourceState(parsed, isStrict));
        }

        // PHASE 0 (batch): intern every element FQN across the entire batch so later
        // phases see the full name universe.
        for (SourceState s : states) {
            for (PackageableElement def : s.rawDefinitions) {
                symbols.intern(def.qualifiedName());
            }
        }

        // PHASE 0.5 (batch): merge imports (explicit + auto-imports of own package) from
        // every non-strict source. Done before any NameResolver runs so wildcards from
        // later sources are in scope for earlier sources.
        for (SourceState s : states) {
            if (s.isStrict) continue;
            for (String pkg : s.parsed.imports().getWildcardImports()) {
                imports.addImport(pkg + "::*");
            }
            for (var entry : s.parsed.imports().getTypeImports().entrySet()) {
                imports.addImport(entry.getValue());
            }
            for (PackageableElement def : s.rawDefinitions) {
                String pkg = def.packagePath();
                if (!pkg.isEmpty()) imports.addImport(pkg + "::*");
            }
        }

        // PHASE 1 (per source): name resolution against the unified imports + FQN set.
        for (SourceState s : states) {
            if (s.isStrict) {
                s.definitions = s.rawDefinitions;
            } else {
                s.definitions = NameResolver.resolveDefinitions(
                        s.rawDefinitions, imports, symbols.allFqns());
            }
        }

        // PHASE 2 (batch): enums first — needed for type resolution in class bodies.
        for (SourceState s : states) {
            for (PackageableElement def : s.definitions) {
                if (def instanceof EnumDefinition enumDef) addEnum(enumDef);
            }
        }

        // PHASE 3a (batch): class stubs across the batch so phase 3b can cross-reference.
        for (SourceState s : states) {
            for (PackageableElement def : s.definitions) {
                if (def instanceof ClassDefinition classDef) registerClassStub(classDef);
            }
        }

        // PHASE 3b (batch): class bodies — now cross-source class references resolve
        // via the phase-3a stubs.
        for (SourceState s : states) {
            for (PackageableElement def : s.definitions) {
                if (def instanceof ClassDefinition classDef) addClass(classDef);
            }
        }

        // PHASE 4 (once): resolve superclass references across all registered classes.
        resolveSuperclasses();

        // PHASE 5a (batch): associations + databases.
        for (SourceState s : states) {
            for (PackageableElement def : s.definitions) {
                switch (def) {
                    case AssociationDefinition assocDef -> addAssociation(assocDef);
                    case DatabaseDefinition dbDef -> addDatabase(dbDef);
                    default -> { }
                }
            }
        }

        // PHASE 5b (once): database includes.
        resolveDatabaseIncludes();

        // PHASE 5c (batch): remaining element kinds.
        for (SourceState s : states) {
            for (PackageableElement def : s.definitions) {
                switch (def) {
                    case ClassDefinition ignored -> { }
                    case EnumDefinition ignored -> { }
                    case AssociationDefinition ignored -> { }
                    case DatabaseDefinition ignored -> { }
                    case MappingDefinition mappingDef -> addMapping(mappingDef);
                    case ServiceDefinition serviceDef -> addService(serviceDef);
                    case ProfileDefinition profileDef -> addProfile(profileDef);
                    case FunctionDefinition funcDef -> addFunction(funcDef);
                    case ConnectionDefinition connDef -> addConnection(connDef);
                    case RuntimeDefinition runtimeDef -> addRuntime(runtimeDef);
                }
            }
        }

        // PHASE 5d (once): mapping includes.
        resolveMappingIncludes();

        // PHASE 6 (once): pre-resolve function bodies + build typed PureFunctions + detect
        // call-graph cycles. Skipped only if the entire batch is strict (caller guarantees
        // pre-resolved bodies).
        boolean anyNonStrict = states.stream().anyMatch(s -> !s.isStrict);
        if (anyNonStrict) {
            buildPureFunctions();
        }

        return this;
    }

    /**
     * Per-source state threaded through the batch phases in {@link #addSources}. The
     * {@code definitions} field is set by phase 1 (name resolution) and then read by
     * later phases.
     */
    private static final class SourceState {
        final com.gs.legend.parser.ParseResult parsed;
        final List<PackageableElement> rawDefinitions;
        final boolean isStrict;
        List<PackageableElement> definitions;

        SourceState(com.gs.legend.parser.ParseResult parsed, boolean isStrict) {
            this.parsed = parsed;
            this.rawDefinitions = parsed.definitions();
            this.isStrict = isStrict;
            this.definitions = parsed.definitions();
        }
    }

    /**
     * Opt-in full body verification. Iterates every ingested
     * {@link com.gs.legend.model.m3.PureFunction} and invokes
     * {@link com.gs.legend.compiler.TypeChecker#check(com.gs.legend.model.m3.PureFunction)}
     * on it, which type-checks the body against the declared signature and memoizes the
     * compiled form.
     *
     * <p><strong>Default is interpreted++.</strong> {@code addSource} / {@code addSources}
     * already validates signatures, classifies known types, and rejects cyclic call graphs
     * at ingest. Function <em>body</em> type-checking stays lazy — a TypeChecker that
     * compiles a query will only pay body-compile cost for functions it actually walks.
     * {@code compile()} is the opt-in verb that flips that trade: pay O(N) now to surface
     * every body-level error (declared-vs-actual return type, unresolved reference inside
     * a body, overload mismatch at a nested call site, etc.) before any query runs.
     *
     * <p>Idempotent and re-runnable: {@code TypeChecker.check(PureFunction)} memoizes by
     * identity, so repeat calls are no-ops for functions already verified in this checker.
     *
     * <p>Uses a fresh {@link com.gs.legend.compiler.TypeChecker} rooted at {@code this}
     * builder. Callers who want to share a compiled side table with subsequent queries can
     * pass their own TypeChecker via {@link #compile(com.gs.legend.compiler.TypeChecker)}.
     *
     * @return this builder for chaining
     */
    public PureModelBuilder compile() {
        return compile(new com.gs.legend.compiler.TypeChecker(this));
    }

    /**
     * Variant of {@link #compile()} that lets the caller supply a specific
     * {@link com.gs.legend.compiler.TypeChecker}. Every ingested
     * {@link com.gs.legend.model.m3.PureFunction} is checked on that instance, so its
     * internal memoization tables are pre-warmed.
     */
    public PureModelBuilder compile(com.gs.legend.compiler.TypeChecker tc) {
        for (int id = 0; id < pureFunctions.size(); id++) {
            List<com.gs.legend.model.m3.PureFunction> list = pureFunctions.get(id);
            if (list == null) continue;
            for (var pf : list) {
                tc.check(pf);
            }
        }
        return this;
    }

    /**
     * Adds a single Pure source to the model. Thin wrapper around
     * {@link #addSources(String...)} so single-source and multi-source ingestion share
     * one code path — no drift, single implementation to maintain.
     *
     * <p>Default is interpreted++: no body verification. Call {@link #compile()} afterwards
     * for full eager verification.
     *
     * @param pureSource The Pure source code
     * @return this builder for chaining
     */
    public PureModelBuilder addSource(String pureSource) {
        return addSources(pureSource);
    }

    /**
     * Resolves database include directives. For each database that includes another,
     * registers the included database's tables, joins, views, and filters under the
     * including database's namespace. Handles transitive includes.
     */
    private void resolveDatabaseIncludes() {
        for (var dbDef : databases.stream().filter(java.util.Objects::nonNull).distinct().toList()) {
            for (String includedPath : dbDef.includes()) {
                DatabaseDefinition included = idGet(databases, symbols.resolveId(includedPath));
                if (included == null) continue;
                String dbFqn = dbDef.qualifiedName();

                // Merge tables — register under including db's FQN namespace
                for (var tableDef : included.tables()) {
                    Table table = convertTable(dbFqn, tableDef);
                    int qId = symbols.intern(table.qualifiedName());
                    if (idGet(tables, qId) != null) {
                        throw new IllegalStateException("Duplicate table '" + table.qualifiedName()
                                + "' found via database include of '" + included.qualifiedName() + "'");
                    }
                    idPut(tables, qId, table);
                }
                for (var schema : included.schemas()) {
                    for (var tableDef : schema.tables()) {
                        Table table = convertTable(dbFqn, schema.name(), tableDef);
                        int qId = symbols.intern(table.qualifiedName());
                        if (idGet(tables, qId) != null) {
                            throw new IllegalStateException("Duplicate table '" + table.qualifiedName()
                                    + "' found via database include of '" + included.qualifiedName() + "'");
                        }
                        idPut(tables, qId, table);
                    }
                }

                // Merge joins — register under including db's FQN namespace
                for (var joinDef : included.joins()) {
                    Join join = new Join(dbFqn, joinDef.name(), joinDef.operation());
                    int qId = symbols.intern(join.qualifiedName());
                    if (idGet(joins, qId) != null) {
                        throw new IllegalStateException("Duplicate join '" + join.qualifiedName()
                                + "' found via database include of '" + included.qualifiedName() + "'");
                    }
                    idPut(joins, qId, join);
                    mappingRegistry.registerJoin(join);
                }

                // Merge views
                for (var viewDef : included.views()) {
                    View view = convertView(dbFqn, viewDef);
                    int qId = symbols.intern(view.qualifiedName());
                    if (idGet(views, qId) != null) {
                        throw new IllegalStateException("Duplicate view '" + view.qualifiedName()
                                + "' found via database include of '" + included.qualifiedName() + "'");
                    }
                    idPut(views, qId, view);
                }

                // Merge filters
                for (var filterDef : included.filters()) {
                    Filter filter = new Filter(dbFqn, filterDef.name(), filterDef.condition());
                    int qId = symbols.intern(filter.qualifiedName());
                    if (idGet(filters, qId) != null) {
                        throw new IllegalStateException("Duplicate filter '" + filter.qualifiedName()
                                + "' found via database include of '" + included.qualifiedName() + "'");
                    }
                    idPut(filters, qId, filter);
                }
            }
        }
    }

    /**
     * Resolves mapping include directives. For each mapping that includes another,
     * copies the included mapping's class mappings into the including mapping's scope
     * in MappingRegistry.
     */
    private void resolveMappingIncludes() {
        for (var mappingDef : mappingDefinitions.stream().filter(java.util.Objects::nonNull).distinct().toList()) {
            for (var include : mappingDef.includes()) {
                String includedPath = include.includedMappingPath();
                // Look up included mapping's class mappings in the registry
                var includedMappings = mappingRegistry.getAllClassMappings(includedPath);
                if (includedMappings.isEmpty()) continue;

                // Register each included class mapping under the current mapping's scope
                for (var cm : includedMappings.values()) {
                    if (cm instanceof RelationalMapping rm) {
                        mappingRegistry.register(mappingDef.qualifiedName(), rm);
                    } else if (cm instanceof com.gs.legend.model.mapping.PureClassMapping pcm) {
                        mappingRegistry.registerPureClassMapping(mappingDef.qualifiedName(), pcm);
                    }
                }
            }
        }
    }

    /**
     * Pre-resolves function bodies and builds typed downstream {@link com.gs.legend.model.m3.PureFunction}s.
     *
     * <p>Walks every registered {@link FunctionDefinition}, parses its body text, name-resolves the
     * AST against the current import scope and known FQNs, and:
     * <ul>
     *   <li>dual-populates the parse-layer record with a {@code resolvedBody} (via
     *       {@link FunctionDefinition#withResolvedBody}) — transitional, removed in Phase 5;</li>
     *   <li>constructs a {@link com.gs.legend.model.m3.PureFunction} with typed {@link com.gs.legend.model.m3.Parameter}s,
     *       resolved return {@link com.gs.legend.model.m3.Type}, and the AST body, and stores it in
     *       {@link #pureFunctions} under the same id as the source {@code FunctionDefinition}.</li>
     * </ul>
     *
     * <p>Phase 3 migrates {@link #findFunction} to read {@code pureFunctions}; Phase 5 strips the
     * {@code resolvedBody} dual-carriage entirely.
     */
    private void buildPureFunctions() {
        for (int id = 0; id < functions.size(); id++) {
            List<FunctionDefinition> funcList = functions.get(id);
            if (funcList == null) continue;
            List<com.gs.legend.model.m3.PureFunction> pureList = new ArrayList<>(funcList.size());
            for (int i = 0; i < funcList.size(); i++) {
                FunctionDefinition fd = funcList.get(i);

                List<com.gs.legend.ast.ValueSpecification> resolved = fd.resolvedBody();
                if (resolved == null) {
                    List<com.gs.legend.ast.ValueSpecification> body =
                            com.gs.legend.parser.PureParser.parseCodeBlock(fd.body());
                    resolved = body.stream()
                            .map(stmt -> com.gs.legend.parser.NameResolver.resolveQuery(
                                    stmt, imports, symbols.allFqns()))
                            .toList();
                    fd = fd.withResolvedBody(resolved);
                    funcList.set(i, fd);
                }

                pureList.add(buildPureFunction(fd, resolved));
            }
            idPut(pureFunctions, id, pureList);
        }

        // Gate: reject cyclic user-function call graphs at ingest. Any downstream path
        // that walks user-function bodies (TypeChecker, PlanGenerator) can then assume
        // the graph is a DAG and no longer needs its own recursion guard.
        detectCallGraphCycles();

        // DIAGNOSTIC: -Dlegend.lite.forceCompile=true forces body verification on every
        // addSource so we can measure the blast radius of making compile() mandatory.
        // Not for production. Leaves interpreted++ as the default when the flag is off.
        if (Boolean.getBoolean("legend.lite.forceCompile")) {
            compile();
        }
    }

    /**
     * DFS-based call-graph cycle detector over the current set of user {@link com.gs.legend.model.m3.PureFunction}s.
     *
     * <p>Graph nodes are {@code (fqn, arity)} pairs, not bare FQNs. Overloads of the same
     * name with different arities — the dominant overload style in legend-lite — are
     * therefore distinct nodes, so a 2-arg {@code foo} calling the 1-arg {@code foo}
     * overload is NOT a cycle. Known limitation: same-arity overloads that differ only
     * by parameter type still share a node; a rare false positive that can be worked
     * around by distinct FQNs. Resolving the exact overload at every call site requires
     * full type info and is left to the TypeChecker.
     *
     * <p>Cycle edges are collected by AST-walking each body's {@link com.gs.legend.ast.AppliedFunction}
     * calls, keyed by target FQN and argument count. Error includes the cycle path
     * ({@code foo/1 -> bar/2 -> foo/1}) for debuggability. Thrown as
     * {@link com.gs.legend.compiler.PureCompileException} so callers of {@code addSource} /
     * {@code addSources} see a uniform compilation error.
     *
     * <p>Complexity: O(V + E) where V is the number of (fqn, arity) pairs and E the sum
     * of user-function calls across all bodies.
     */
    private void detectCallGraphCycles() {
        Set<OverloadKey> nodes = new java.util.HashSet<>();
        for (int id = 0; id < pureFunctions.size(); id++) {
            List<com.gs.legend.model.m3.PureFunction> list = pureFunctions.get(id);
            if (list == null) continue;
            for (var pf : list) {
                nodes.add(new OverloadKey(pf.qualifiedName(), pf.parameters().size()));
            }
        }
        if (nodes.isEmpty()) return;

        Map<OverloadKey, Set<OverloadKey>> callGraph = new HashMap<>();
        for (int id = 0; id < pureFunctions.size(); id++) {
            List<com.gs.legend.model.m3.PureFunction> list = pureFunctions.get(id);
            if (list == null) continue;
            for (var pf : list) {
                OverloadKey self = new OverloadKey(pf.qualifiedName(), pf.parameters().size());
                Set<OverloadKey> callees = callGraph.computeIfAbsent(
                        self, k -> new java.util.HashSet<>());
                for (var stmt : pf.body()) {
                    collectUserFunctionCalls(stmt, nodes, callees);
                }
            }
        }

        // 3-color DFS: WHITE not visited, GRAY on the current stack, BLACK fully explored.
        // Revisiting a GRAY node means a cycle; the path from that node back to itself is
        // extracted from the current traversal stack for a clear error message.
        Map<OverloadKey, Byte> color = new HashMap<>();
        for (var n : nodes) color.put(n, (byte) 0);
        java.util.Deque<OverloadKey> stack = new java.util.ArrayDeque<>();
        for (var start : nodes) {
            if (color.get(start) == (byte) 0) {
                List<OverloadKey> cycle = dfsForCycle(start, callGraph, color, stack);
                if (cycle != null) {
                    throw new com.gs.legend.compiler.PureCompileException(
                            "Cyclic user-function call graph detected: "
                                    + cycle.stream().map(OverloadKey::toDisplay)
                                            .collect(java.util.stream.Collectors.joining(" -> "))
                                    + ". Recursive user functions are not supported.");
                }
            }
        }
    }

    /** Node of the call graph: a function overload distinguished by FQN and arity. */
    private record OverloadKey(String fqn, int arity) {
        String toDisplay() {
            return fqn + "/" + arity;
        }
    }

    private static List<OverloadKey> dfsForCycle(
            OverloadKey node, Map<OverloadKey, Set<OverloadKey>> graph,
            Map<OverloadKey, Byte> color, java.util.Deque<OverloadKey> stack) {
        color.put(node, (byte) 1); // GRAY
        stack.push(node);
        for (OverloadKey next : graph.getOrDefault(node, Set.of())) {
            Byte c = color.get(next);
            if (c == null) continue; // defensive: callee not in user-func set
            if (c == (byte) 1) {
                // Cycle: collect from `next` down to `node` in stack order, then append `next`.
                List<OverloadKey> path = new ArrayList<>(stack); // iteration is head-first (LIFO)
                java.util.Collections.reverse(path); // now root-first
                List<OverloadKey> cycle = new ArrayList<>();
                boolean started = false;
                for (OverloadKey n : path) {
                    if (n.equals(next)) started = true;
                    if (started) cycle.add(n);
                }
                cycle.add(next);
                return cycle;
            }
            if (c == (byte) 0) {
                List<OverloadKey> found = dfsForCycle(next, graph, color, stack);
                if (found != null) return found;
            }
        }
        stack.pop();
        color.put(node, (byte) 2); // BLACK
        return null;
    }

    /**
     * Walks a {@link com.gs.legend.ast.ValueSpecification} subtree and records every
     * {@link com.gs.legend.ast.AppliedFunction} whose target (FQN, argCount) is a
     * registered user-function overload. Terminals (literals, Variables,
     * PackageableElementPtr) contribute no edges — only AppliedFunction,
     * AppliedProperty parameters, LambdaFunction bodies, and PureCollection members
     * carry AST children we need to descend into.
     */
    private static void collectUserFunctionCalls(
            com.gs.legend.ast.ValueSpecification vs,
            Set<OverloadKey> nodes,
            Set<OverloadKey> out) {
        switch (vs) {
            case com.gs.legend.ast.AppliedFunction af -> {
                OverloadKey callee = new OverloadKey(af.function(), af.parameters().size());
                if (nodes.contains(callee)) out.add(callee);
                for (var p : af.parameters()) collectUserFunctionCalls(p, nodes, out);
            }
            case com.gs.legend.ast.AppliedProperty ap -> {
                for (var p : ap.parameters()) collectUserFunctionCalls(p, nodes, out);
            }
            case com.gs.legend.ast.LambdaFunction lf -> {
                for (var b : lf.body()) collectUserFunctionCalls(b, nodes, out);
            }
            case com.gs.legend.ast.PureCollection c -> {
                for (var v : c.values()) collectUserFunctionCalls(v, nodes, out);
            }
            default -> { /* terminal: literal, Variable, PackageableElementPtr, etc. */ }
        }
    }

    /**
     * Converts a (resolved-body-carrying) {@link FunctionDefinition} into a
     * {@link com.gs.legend.model.m3.PureFunction}. Reads typed parameter / return types
     * from the parse-layer's {@code parsedType} / {@code parsedReturnType} fields. Phase 5
     * deletes those fields and shifts {@code Type} construction into this method (via
     * {@link com.gs.legend.model.m3.Type#resolve} over the String type names).
     */
    private com.gs.legend.model.m3.PureFunction buildPureFunction(
            FunctionDefinition fd, List<com.gs.legend.ast.ValueSpecification> resolvedBody) {
        List<com.gs.legend.model.m3.Parameter> typedParams = new ArrayList<>(fd.parameters().size());
        for (var p : fd.parameters()) {
            com.gs.legend.model.m3.Type paramType = p.parsedType();
            if (paramType == null) {
                throw new IllegalStateException(
                        "PureModelBuilder.buildPureFunction: function '" + fd.qualifiedName()
                                + "' parameter '" + p.name() + "' has no parsedType. "
                                + "Parse-layer producers must populate FunctionDefinition.ParameterDefinition.parsedType. "
                                + "(Phase 5 will move Type construction into PureModelBuilder; for now, producers are required to set it.)");
            }
            typedParams.add(new com.gs.legend.model.m3.Parameter(
                    p.name(), classifyFunctionSigType(paramType),
                    new com.gs.legend.model.m3.Multiplicity.Bounded(p.lowerBound(), p.upperBound())));
        }

        com.gs.legend.model.m3.Type returnType = fd.parsedReturnType();
        if (returnType == null) {
            throw new IllegalStateException(
                    "PureModelBuilder.buildPureFunction: function '" + fd.qualifiedName()
                            + "' has no parsedReturnType. "
                            + "Parse-layer producers must populate FunctionDefinition.parsedReturnType.");
        }

        return new com.gs.legend.model.m3.PureFunction(
                fd.qualifiedName(),
                /* typeParams */ List.of(),
                typedParams,
                classifyFunctionSigType(returnType),
                new com.gs.legend.model.m3.Multiplicity.Bounded(fd.returnLowerBound(), fd.returnUpperBound()),
                resolvedBody,
                fd.stereotypes(),
                fd.taggedValues());
    }

    /**
     * Classifies {@link Type.NameRef}s appearing in a function signature (param or return) to
     * their typed variants ({@link Type.ClassType} / {@link Type.EnumType} / {@link Primitive} /
     * {@link com.gs.legend.model.m3.LClass}) using the in-memory model. Structural forms
     * (GenericType, FunctionType, RelationTypeVar, etc.) are walked and their children
     * classified recursively.
     *
     * <p><strong>Lenient by design.</strong> Unknown names stay as {@link Type.NameRef}. This
     * preserves the cross-source forward-reference contract: a function in source A may
     * reference a class declared in source B not yet added. The TypeChecker still carries
     * the NameRef fallback path for that case — kept until a future batch-ingest
     * ({@code addSources}) lets us tighten to exhaustive classification.
     *
     * <p>Kind-map source: the builder's own {@code classes} / {@code enums} tables via
     * {@link ModelContext#findType} — pure in-memory lookup, does not force lazy loading
     * (AGENTS.md §5).
     */
    private Type classifyFunctionSigType(Type t) {
        return switch (t) {
            case Type.NameRef nr -> findType(nr.qualifiedName()).orElse(nr);
            case Type.GenericType gt -> {
                Type rawType = classifyFunctionSigType(gt.rawType());
                List<Type> args = new ArrayList<>(gt.typeArgs().size());
                boolean changed = rawType != gt.rawType();
                for (Type arg : gt.typeArgs()) {
                    Type c = classifyFunctionSigType(arg);
                    if (c != arg) changed = true;
                    args.add(c);
                }
                yield changed ? new Type.GenericType(rawType, args) : t;
            }
            case Type.FunctionType ft -> {
                boolean changed = false;
                List<Type.Parameter> newParams = new ArrayList<>(ft.params().size());
                for (Type.Parameter pp : ft.params()) {
                    Type c = classifyFunctionSigType(pp.type());
                    if (c != pp.type()) {
                        changed = true;
                        newParams.add(new Type.Parameter(pp.name(), c, pp.multiplicity()));
                    } else {
                        newParams.add(pp);
                    }
                }
                Type newReturn = classifyFunctionSigType(ft.returnType());
                if (newReturn != ft.returnType()) changed = true;
                yield changed ? new Type.FunctionType(newParams, newReturn, ft.returnMult()) : t;
            }
            case Type.RelationTypeVar rtv -> {
                boolean changed = false;
                List<Type.RelationTypeVar.Column> newCols = new ArrayList<>(rtv.columns().size());
                for (Type.RelationTypeVar.Column col : rtv.columns()) {
                    Type c = classifyFunctionSigType(col.type());
                    if (c != col.type()) {
                        changed = true;
                        newCols.add(new Type.RelationTypeVar.Column(col.name(), c, col.multiplicity()));
                    } else {
                        newCols.add(col);
                    }
                }
                yield changed ? new Type.RelationTypeVar(newCols) : t;
            }
            case Type.SchemaAlgebra sa -> {
                Type l = classifyFunctionSigType(sa.left());
                Type r = classifyFunctionSigType(sa.right());
                yield (l == sa.left() && r == sa.right()) ? t : new Type.SchemaAlgebra(l, sa.op(), r);
            }
            // Already-classified / structural leaves — no NameRefs to classify.
            case Primitive ignored -> t;
            case Type.ClassType ignored -> t;
            case Type.EnumType ignored -> t;
            case Type.TypeVar ignored -> t;
            case com.gs.legend.model.m3.LClass ignored -> t;
            case Type.PrecisionDecimal ignored -> t;
            case Type.Relation ignored -> t;
            case Type.Tuple ignored -> t;
            case Type.FunctionReference ignored -> t;
        };
    }

    /**
     * Pre-registers a class by name (stub) so forward references can resolve.
     * Called before addClass() to handle mutual/forward dependencies.
     */
    private void registerClassStub(ClassDefinition classDef) {
        PureClass stub = new PureClass(
                classDef.packagePath(),
                classDef.simpleName(),
                classDef.typeParams(),
                List.<String>of(), // superClassFqns — populated in addClass
                List.of(),         // no properties yet
                classDef.stereotypes(),
                classDef.taggedValues(),
                classDef.isNative());
        idPut(classes, symbols.intern(classDef.qualifiedName()), stub);
    }

    /**
     * Adds a Class definition.
     * Resolves properties (all class names already registered via registerClassStub).
     * Superclass references are resolved later in resolveSuperclasses().
     */
    public PureModelBuilder addClass(ClassDefinition classDef) {
        // Store the definition for later superclass validation
        int id = symbols.intern(classDef.qualifiedName());
        idPut(pendingClassDefinitions, id, classDef);

        java.util.Set<String> typeParamSet = java.util.Set.copyOf(classDef.typeParams());
        List<Property> properties = classDef.properties().stream()
                .map(p -> convertProperty(p, typeParamSet))
                .toList();

        // Populate superclass FQNs directly from the definition — NameResolver already
        // canonicalized them to qualified form. No resolved-PureClass lookup is needed
        // at this stage; superclass references are validated separately in resolveSuperclasses().
        PureClass pureClass = new PureClass(
                classDef.packagePath(),
                classDef.simpleName(),
                classDef.typeParams(),
                classDef.superClasses(),
                properties,
                classDef.stereotypes(),
                classDef.taggedValues(),
                classDef.isNative());

        idPut(classes, id, pureClass);

        return this;
    }

    /**
     * Validates superclass references for all registered classes. Each FQN in
     * {@code classDef.superClasses()} must resolve to a known class in the builder's
     * symbol table. No PureClass reconstruction is needed after flag day — the
     * {@code superClassFqns} field was populated directly in {@link #addClass}.
     */
    private void resolveSuperclasses() {
        for (int id = 0; id < pendingClassDefinitions.size(); id++) {
            ClassDefinition classDef = pendingClassDefinitions.get(id);
            if (classDef == null) continue;

            for (String superClassName : classDef.superClasses()) {
                PureClass superClass = idGet(classes, symbols.resolveId(superClassName));
                if (superClass == null) {
                    throw new IllegalStateException(
                            "Superclass not found: " + superClassName + " for class " + symbols.nameOf(id));
                }
            }
        }

        // Clear pending definitions
        pendingClassDefinitions.clear();
    }

    /**
     * Adds an Association definition.
     */
    public PureModelBuilder addAssociation(AssociationDefinition assocDef) {
        var end1 = assocDef.property1();
        var end2 = assocDef.property2();

        Association association = new Association(
                assocDef.packagePath(),
                assocDef.simpleName(),
                new Association.AssociationEnd(
                        end1.propertyName(),
                        end1.targetClass(),
                        new Multiplicity.Bounded(end1.lowerBound(), end1.upperBound())),
                new Association.AssociationEnd(
                        end2.propertyName(),
                        end2.targetClass(),
                        new Multiplicity.Bounded(end2.lowerBound(), end2.upperBound())));

        idPut(associations, symbols.intern(assocDef.qualifiedName()), association);

        return this;
    }

    /**
     * Adds a Database definition.
     */
    public PureModelBuilder addDatabase(DatabaseDefinition dbDef) {
        String dbFqn = dbDef.qualifiedName();
        idPut(databases, symbols.intern(dbFqn), dbDef);

        // Register tables
        for (var tableDef : dbDef.tables()) {
            Table table = convertTable(dbFqn, tableDef);
            idPut(tables, symbols.intern(table.qualifiedName()), table);
        }

        // Register schema tables
        for (var schema : dbDef.schemas()) {
            for (var tableDef : schema.tables()) {
                Table table = convertTable(dbFqn, schema.name(), tableDef);
                idPut(tables, symbols.intern(table.qualifiedName()), table);
            }
        }

        // Register joins
        for (var joinDef : dbDef.joins()) {
            Join join = new Join(dbFqn, joinDef.name(), joinDef.operation());
            idPut(joins, symbols.intern(join.qualifiedName()), join);
            mappingRegistry.registerJoin(join);
        }

        // Register views
        for (var viewDef : dbDef.views()) {
            View view = convertView(dbFqn, viewDef);
            idPut(views, symbols.intern(view.qualifiedName()), view);
        }

        // Register filters
        for (var filterDef : dbDef.filters()) {
            Filter filter = new Filter(dbFqn, filterDef.name(), filterDef.condition());
            idPut(filters, symbols.intern(filter.qualifiedName()), filter);
        }

        return this;
    }

    /**
     * Adds a Profile definition.
     */
    public PureModelBuilder addProfile(ProfileDefinition profileDef) {
        idPut(profiles, symbols.intern(profileDef.qualifiedName()), profileDef);
        return this;
    }

    /**
     * Adds a Function definition.
     */
    public PureModelBuilder addFunction(FunctionDefinition funcDef) {
        int funcId = symbols.intern(funcDef.qualifiedName());
        List<FunctionDefinition> funcList = idGet(functions, funcId);
        if (funcList == null) { funcList = new ArrayList<>(); idPut(functions, funcId, funcList); }
        funcList.add(funcDef);
        return this;
    }

    @Override
    public List<com.gs.legend.model.m3.PureFunction> findFunction(String name) {
        List<com.gs.legend.model.m3.PureFunction> result = idGet(pureFunctions, symbols.resolveId(name));
        return result != null ? result : List.of();
    }

    /**
     * Parse-layer lookup — used internally by {@link #buildPureFunctions} and tests that need to
     * assert on the original {@link FunctionDefinition} shape. Downstream consumers should use
     * {@link #findFunction} which returns the typed {@link com.gs.legend.model.m3.PureFunction}.
     */
    public List<FunctionDefinition> findFunctionDefinition(String name) {
        List<FunctionDefinition> result = idGet(functions, symbols.resolveId(name));
        return result != null ? result : List.of();
    }

    /**
     * Adds a Connection definition.
     */
    public PureModelBuilder addConnection(ConnectionDefinition connDef) {
        idPut(connections, symbols.intern(connDef.qualifiedName()), connDef);
        return this;
    }

    /**
     * Adds a Runtime definition.
     * If the runtime has JsonModelConnections, registers a variant identity
     * RelationalMapping for each one — so downstream sees a normal relational
     * mapping with expression-access properties on a SEMISTRUCTURED column.
     */
    public PureModelBuilder addRuntime(RuntimeDefinition runtimeDef) {
        idPut(runtimes, symbols.intern(runtimeDef.qualifiedName()), runtimeDef);

        // Register variant identity mappings for JSON-backed source classes
        if (runtimeDef.hasJsonConnections()) {
            for (var jmc : runtimeDef.jsonConnections()) {
                var pc = findClass(jmc.className()).orElse(null);
                if (pc == null) continue; // class not yet registered — skip silently
                var rm = RelationalMapping.variantIdentity(pc, jmc.url(), this);
                // Register under each mapping referenced by this runtime
                for (String mappingName : runtimeDef.mappings()) {
                    mappingRegistry.register(mappingName, rm);
                }
                // Register synthetic table so compiler's findTable() works during sourceSpec synthesis
                idPut(tables, symbols.intern(rm.table().qualifiedName()), rm.table());
            }
        }

        return this;
    }

    /**
     * Resolves a list of JoinChainElements to Join objects from the SymbolTable.
     * JoinChainElement.databaseName() is always non-null (enforced by its record constructor,
     * stamped by the parser, and resolved by NameResolver).
     */
    private List<Join> resolveJoins(List<com.gs.legend.model.def.JoinChainElement> chain) {
        return chain.stream().map(jce -> {
            String db = jce.databaseName();
            Join join = idGet(joins, symbols.resolveId(db + "." + jce.joinName()));
            if (join == null) {
                throw new IllegalStateException("Join not found: " + db + "." + jce.joinName());
            }
            return join;
        }).toList();
    }

    /**
     * Resolves a Join from a JoinChainElement using its databaseName + joinName as canonical key.
     */
    private Join resolveJoinByChainElement(com.gs.legend.model.def.JoinChainElement jce) {
        String db = jce.databaseName();
        if (db == null) {
            throw new IllegalStateException("No database context for join '" + jce.joinName() + "'");
        }
        Join join = idGet(joins, symbols.resolveId(db + "." + jce.joinName()));
        if (join == null) {
            throw new IllegalStateException("Join not found: " + db + "." + jce.joinName());
        }
        return join;
    }

    /**
     * Resolves a Join from a JoinReference, using its own databaseName or falling back to
     * the class mapping's database context.
     */
    private Join resolveJoinFromReference(MappingDefinition.JoinReference ref, String fallbackDb) {
        var firstHop = ref.joinChain().get(0);
        String db = firstHop.databaseName() != null ? firstHop.databaseName()
                : ref.databaseName() != null ? ref.databaseName() : fallbackDb;
        if (db == null) {
            throw new IllegalStateException("No database context for join '" + firstHop.joinName() + "'");
        }
        Join join = idGet(joins, symbols.resolveId(db + "." + firstHop.joinName()));
        if (join == null) {
            throw new IllegalStateException("Join not found: " + db + "." + firstHop.joinName());
        }
        return join;
    }

    /**
     * Adds a Mapping definition and registers it.
     * Handles both Relational and Pure (M2M) mappings:
     * - Relational: Creates PropertyMappings and registers with MappingRegistry
     * - Pure: Re-parses as M2M and registers M2M class mappings
     */
    public PureModelBuilder addMapping(MappingDefinition mappingDef) {
        // Store for include resolution
        idPut(mappingDefinitions, symbols.intern(mappingDef.qualifiedName()), mappingDef);

        // Process association mappings: register join associations
        for (var assocMapping : mappingDef.associationMappings()) {
            for (var prop : assocMapping.properties()) {
                if (!prop.joinChain().isEmpty()) {
                    var firstHop = prop.joinChain().get(0);
                    Join join = resolveJoinByChainElement(firstHop);
                    String resolvedAssocName = assocMapping.associationName();
                    explicitAssociationJoins.computeIfAbsent(resolvedAssocName, k -> new HashMap<>()).put(prop.propertyName(), join);
                }
            }
        }

        for (var classMapping : mappingDef.classMappings()) {
            if ("Pure".equals(classMapping.mappingType())) {
                // Pure (M2M) mapping - parse and register M2M class mapping
                registerM2MClassMapping(mappingDef.qualifiedName(), classMapping);
                continue;
            }

            // Find the PureClass
            PureClass pureClass = idGet(classes, symbols.resolveId(classMapping.className()));
            if (pureClass == null) {
                throw new IllegalStateException("Class not found: " + classMapping.className());
            }

            // Find the Table (or View → infer base table)
            Table table = null;
            View view = null;
            String dbFqn = classMapping.mainTable() != null ? classMapping.mainTable().databaseName() : null;
            if (classMapping.mainTable() != null) {
                String tableKey = dbFqn + "." + classMapping.mainTable().tableName();
                table = idGet(tables, symbols.resolveId(tableKey));
                if (table == null) {
                    // Views are usable as tables in mappings (same as legend-engine)
                    view = idGet(views, symbols.resolveId(tableKey));
                    if (view != null) {
                        String baseTableName = inferViewMainTable(view);
                        table = idGet(tables, symbols.resolveId(dbFqn + "." + baseTableName));
                        if (table == null) {
                            throw new IllegalStateException(
                                    "View '" + tableKey + "' references table '" + baseTableName + "' which was not found");
                        }
                    }
                }
                if (table == null) {
                    throw new IllegalStateException("Table or View not found: " + tableKey);
                }
            }

            // Build property mappings:
            // - Bare join references (no terminal column) are skipped — handled as associations
            // - Join references WITH terminal columns become join-chain PropertyMappings
            // - Embedded/inline PMDs are skipped — handled separately below
            List<PropertyMapping> propertyMappings = classMapping.propertyMappings().stream()
                    .filter(pm -> pm.structuredValue() == null)
                    .filter(pm -> !pm.isJoinReference()
                            || pm.joinReference().terminalColumn() != null)
                    .map(pm -> {
                        // Join chain property mapping: @J1 > @J2 | T.COL
                        if (pm.isJoinReference() && pm.joinReference().terminalColumn() != null) {
                            var joinRef = pm.joinReference();
                            var terminal = joinRef.terminalColumn();
                            String terminalCol;
                            if (terminal instanceof com.gs.legend.model.def.RelationalOperation.ColumnRef cr) {
                                terminalCol = cr.column();
                            } else {
                                terminalCol = pm.propertyName();
                            }
                            List<Join> resolvedJoins = resolveJoins(joinRef.joinChain());
                            return PropertyMapping.joinChain(pm.propertyName(), terminalCol, resolvedJoins);
                        }
                        if (pm.hasMappingExpression()) {
                            // DynaFunction expression: convert RelationalOperation → ValueSpecification
                            var joinNavs = findAllJoinNavigations(pm.mappingExpression());
                            if (joinNavs.size() >= 2 && classMapping.mainTable() != null) {
                                // Multi-join DynaFunction: build tableToParam for all referenced tables
                                String mainTableName = classMapping.mainTable().tableName();
                                var tableToParam = new java.util.HashMap<String, String>();
                                tableToParam.put(mainTableName, "src");
                                var allJoinChains = new java.util.ArrayList<java.util.List<Join>>();
                                for (int ji = 0; ji < joinNavs.size(); ji++) {
                                    var nav = joinNavs.get(ji);
                                    String paramName = "t" + (ji + 1);
                                    var termTables = RelationalMappingConverter.collectTableNames(nav.terminal());
                                    String termTable = termTables.stream()
                                            .filter(t -> !t.equals(mainTableName))
                                            .findFirst().orElse(termTables.iterator().next());
                                    tableToParam.put(termTable, paramName);
                                    allJoinChains.add(resolveJoins(nav.joinChain()));
                                }
                                var vsExpr = RelationalMappingConverter.convert(pm.mappingExpression(), tableToParam);
                                return PropertyMapping.dynaFunctionWithMultiJoin(pm.propertyName(), vsExpr, allJoinChains);
                            }
                            var joinNav = joinNavs.isEmpty() ? null : joinNavs.get(0);
                            if (joinNav != null && classMapping.mainTable() != null) {
                                // Combined join + DynaFunction: expression references columns across tables
                                String mainTableName = classMapping.mainTable().tableName();
                                // Terminal table from JoinNavigation's terminal ColumnRef
                                var terminalTables = RelationalMappingConverter.collectTableNames(joinNav.terminal());
                                String terminalTable = terminalTables.stream()
                                        .filter(t -> !t.equals(mainTableName))
                                        .findFirst().orElse(terminalTables.iterator().next());
                                var tableToParam = new java.util.HashMap<String, String>();
                                tableToParam.put(mainTableName, "src");
                                tableToParam.put(terminalTable, "tgt");
                                var vsExpr = RelationalMappingConverter.convert(pm.mappingExpression(), tableToParam);
                                List<Join> resolvedJoins = resolveJoins(joinNav.joinChain());
                                return PropertyMapping.dynaFunctionWithJoin(pm.propertyName(), vsExpr, resolvedJoins);
                            }
                            var vsExpr = RelationalMappingConverter.convert(pm.mappingExpression());
                            return PropertyMapping.dynaFunction(pm.propertyName(), vsExpr);
                        }
                        if (pm.isExpression()) {
                            // Expression-based mapping (e.g., PAYLOAD->get('price', @Integer))
                            String colName = extractColumnNameFromExpression(pm.expressionString());
                            String expr = pm.expressionString();

                            // Auto-infer cast type from class property when get() lacks @Type
                            var prop = pureClass.findProperty(pm.propertyName(), this);
                            if (prop.isPresent() && expr.matches(".*->get\\('[^']+?'\\)\\s*$")) {
                                String pureType = prop.get().typeFqn();
                                // Phase 2 C6 TODO: lift Property.typeFqn() to a typed Type so
                                // this becomes identity checks instead of raw string comparison.
                                if (!"Any".equals(pureType) && !"Variant".equals(pureType)) {
                                    expr = expr.replaceFirst(
                                            "->get\\('([^']+?)'\\)\\s*$",
                                            "->get('$1', @" + pureType + ")");
                                }
                            }
                            return PropertyMapping.expression(pm.propertyName(), colName, expr);
                        } else if (pm.hasEnumMapping()) {
                            // Enum column mapping with enumeration mapping
                            String columnName = pm.columnReference().columnName();
                            String enumMappingId = pm.enumMappingId();

                            // Find the property's enum type from the class definition.
                            // Both sides are now FQN-canonicalized: NameResolver rewrites
                            // EnumerationMappingDefinition.enumType to FQN during resolution
                            // (see NameResolver.resolveEnumerationMapping), and prop.typeFqn()
                            // is already FQN for class/enum-typed properties.
                            var prop = pureClass.properties().stream()
                                    .filter(p -> p.name().equals(pm.propertyName()))
                                    .findFirst()
                                    .orElse(null);
                            String enumType = prop != null ? prop.typeFqn() : null;

                            // Find the enumeration mapping
                            var enumMapping = mappingDef.findEnumerationMapping(enumType,
                                    enumMappingId.isEmpty() ? null : enumMappingId);

                            if (enumMapping.isPresent()) {
                                return PropertyMapping.enumColumn(pm.propertyName(), columnName,
                                        enumMapping.get().enumType(), enumMapping.get().valueMappings());
                            } else {
                                // Fallback to simple column if enum mapping not found
                                return PropertyMapping.column(pm.propertyName(), columnName);
                            }
                        } else {
                            // Simple column reference
                            return PropertyMapping.column(pm.propertyName(), pm.columnReference().columnName());
                        }
                    })
                    .toList();

            // Process bare join references (no terminal column) — these are association navigations.
            // Join refs WITH terminal columns (e.g., @EmpDept | T.NAME) are traverse property mappings,
            // already handled above as join-chain PropertyMappings.
            for (var pm : classMapping.propertyMappings()) {
                if (pm.isJoinReference() && pm.joinReference().terminalColumn() == null) {
                    Join join = resolveJoinFromReference(pm.joinReference(), dbFqn);
                    // Register under the association name (not class name) to match lookup
                    String assocFqn = findAssociationForProperty(classMapping.className(), pm.propertyName());
                    if (assocFqn == null) {
                        throw new IllegalStateException(
                                "Join reference for property '" + pm.propertyName()
                                        + "' on class '" + classMapping.className()
                                        + "' does not belong to any registered association");
                    }
                    explicitAssociationJoins.computeIfAbsent(assocFqn, k -> new HashMap<>()).put(pm.propertyName(), join);
                }
            }

            // Extract mapping-level directives
            String setId = classMapping.setId();
            boolean isRoot = classMapping.isRoot();
            boolean distinct = classMapping.distinct();
            String filterFqn = null;
            if (classMapping.filter() != null) {
                var f = classMapping.filter();
                filterFqn = f.databaseName() + "." + f.filterName();
            }

            // Build embedded property mappings from structured PMDs
            var embeddedMappings = new java.util.LinkedHashMap<String, List<PropertyMapping>>();
            for (var pm : classMapping.propertyMappings()) {
                if (pm.structuredValue() instanceof com.gs.legend.model.def.PropertyMappingValue.EmbeddedMapping emb) {
                    var subMappings = new java.util.ArrayList<PropertyMapping>();
                    for (var sub : emb.properties()) {
                        if (sub.columnReference() != null) {
                            subMappings.add(PropertyMapping.column(sub.propertyName(),
                                    sub.columnReference().columnName()));
                        }
                    }
                    embeddedMappings.put(pm.propertyName(), subMappings);
                } else if (pm.structuredValue() instanceof com.gs.legend.model.def.PropertyMappingValue.InlineMapping inl) {
                    // Inline: resolve target set ID → use its property mappings as embedded
                    var targetDef = mappingDef.findClassMappingBySetId(inl.targetSetId());
                    if (targetDef.isEmpty()) {
                        throw new IllegalStateException(
                                "Inline mapping target set ID '" + inl.targetSetId() + "' not found");
                    }
                    var subMappings = new java.util.ArrayList<PropertyMapping>();
                    for (var sub : targetDef.get().propertyMappings()) {
                        if (sub.columnReference() != null) {
                            subMappings.add(PropertyMapping.column(sub.propertyName(),
                                    sub.columnReference().columnName()));
                        }
                    }
                    embeddedMappings.put(pm.propertyName(), subMappings);
                } else if (pm.structuredValue() instanceof com.gs.legend.model.def.PropertyMappingValue.OtherwiseMapping ow) {
                    // Otherwise: embedded sub-mappings + fallback join
                    var subMappings = new java.util.ArrayList<PropertyMapping>();
                    for (var sub : ow.embedded().properties()) {
                        if (sub.columnReference() != null) {
                            subMappings.add(PropertyMapping.column(sub.propertyName(),
                                    sub.columnReference().columnName()));
                        }
                    }
                    embeddedMappings.put(pm.propertyName(), subMappings);
                    // Register fallback join as explicit association join
                    Join join = resolveJoinByChainElement(ow.fallbackJoin().joinChain().get(0));
                    explicitAssociationJoins.computeIfAbsent(classMapping.className(), k -> new HashMap<>()).put(pm.propertyName(), join);
                }
            }

            // Extract ~groupBy column names
            List<String> groupByColumns = classMapping.groupBy().stream()
                    .filter(op -> op instanceof com.gs.legend.model.def.RelationalOperation.ColumnRef)
                    .map(op -> ((com.gs.legend.model.def.RelationalOperation.ColumnRef) op).column())
                    .toList();

            // Create and register the mapping
            RelationalMapping mapping = new RelationalMapping(pureClass.qualifiedName(), table, propertyMappings,
                    false, setId, isRoot, distinct, filterFqn, embeddedMappings,
                    groupByColumns, view, null);
            mappingRegistry.register(mappingDef.qualifiedName(), mapping);
        }

        return this;
    }

    /**
     * Creates M2M class mappings from the parsed ClassMappingDefinition and
     * registers them.
     * This is called when addMapping encounters a "Pure" mapping type.
     * 
     * Registers BOTH old M2MClassMapping (for backward compat) and new
     * PureClassMapping (for clean pipeline). Phase 4 will remove old one.
     */
    private void registerM2MClassMapping(String mappingName, MappingDefinition.ClassMappingDefinition classMapping) {
        // === NEW: Parse M2M expressions via ValueSpecificationBuilder → PureClassMapping ===
        java.util.Map<String, com.gs.legend.ast.ValueSpecification> parsedExpressions = new java.util.LinkedHashMap<>();

        if (classMapping.m2mPropertyExpressions() != null) {
            for (var entry : classMapping.m2mPropertyExpressions().entrySet()) {
                String propertyName = entry.getKey();
                String expressionString = entry.getValue();
                parsedExpressions.put(propertyName,
                        com.gs.legend.parser.PureParser.parseQuery(expressionString));
            }
        }

        // Parse filter via ValueSpecificationBuilder
        com.gs.legend.ast.ValueSpecification parsedFilter = null;
        if (classMapping.filterExpression() != null && !classMapping.filterExpression().isEmpty()) {
            parsedFilter = com.gs.legend.parser.PureParser.parseQuery(classMapping.filterExpression());
        }

        // Register new PureClassMapping (clean pipeline). Target is tracked by FQN
        // (AGENTS.md §5 — no eagerly-resolved PureClass field); sourceMapping is filled
        // in later by MappingNormalizer.resolveM2MChain.
        com.gs.legend.model.mapping.PureClassMapping pureMapping =
                new com.gs.legend.model.mapping.PureClassMapping(
                        classMapping.className(),
                        classMapping.sourceClassName(),
                        parsedExpressions,
                        parsedFilter,
                        null); // sourceMapping — resolved later
        mappingRegistry.registerPureClassMapping(mappingName, pureMapping);
    }

    /**
     * Adds a Service definition.
     * Services are used by the hosted service runtime for HTTP endpoints.
     */
    public PureModelBuilder addService(ServiceDefinition serviceDef) {
        idPut(services, symbols.intern(serviceDef.qualifiedName()), serviceDef);
        return this;
    }

    /**
     * Adds an Enum definition.
     * Enums are type-safe value sets used for properties.
     */
    public PureModelBuilder addEnum(EnumDefinition enumDef) {
        idPut(enums, symbols.intern(enumDef.qualifiedName()), enumDef);
        return this;
    }

    /**
     * Looks up an enum definition by name (simple or qualified).
     * 
     * @param enumName The enum name to look up
     * @return The EnumDefinition, or null if not found
     */
    public EnumDefinition getEnum(String enumName) {
        return idGet(enums, symbols.resolveId(enumName));
    }

    /**
     * @return The built mapping registry
     */
    public MappingRegistry getMappingRegistry() {
        return mappingRegistry;
    }

    /**
     * @param className The class name (simple or qualified)
     * @return The PureClass
     */
    public PureClass getClass(String className) {
        return idGet(classes, symbols.resolveId(className));
    }

    /**
     * @param db The database FQN
     * @param name The table name
     * @return The Table
     */
    public Table getTable(String db, String name) {
        java.util.Objects.requireNonNull(db, "db");
        java.util.Objects.requireNonNull(name, "name");
        return idGet(tables, symbols.resolveId(db + "." + name));
    }

    /**
     * @param db The database FQN
     * @param name The join name
     * @return The Join, if found
     */
    public Optional<Join> getJoin(String db, String name) {
        java.util.Objects.requireNonNull(db, "db");
        java.util.Objects.requireNonNull(name, "name");
        return Optional.ofNullable(idGet(joins, symbols.resolveId(db + "." + name)));
    }

    /**
     * @param filterName The filter name (simple or qualified with db prefix)
     * @return The Filter, if found
     */
    public Optional<Filter> getFilter(String filterName) {
        return Optional.ofNullable(idGet(filters, symbols.resolveId(filterName)));
    }

    /**
     * Resolves all mapping names from a runtime definition.
     *
     * @param runtimeName The runtime name
     * @return The list of mapping names, or empty if no mappings defined
     */
    public java.util.List<String> resolveMappingNames(String runtimeName) {
        RuntimeDefinition runtime = idGet(runtimes, symbols.resolveId(runtimeName));
        if (runtime == null || runtime.mappings() == null || runtime.mappings().isEmpty()) {
            return java.util.List.of();
        }
        return runtime.mappings();
    }

    /**
     * @param associationName The association name
     * @return The Association, if found
     */
    public Optional<Association> getAssociation(String associationName) {
        return Optional.ofNullable(idGet(associations, symbols.resolveId(associationName)));
    }

    /**
     * @param connectionName The connection name (simple or qualified)
     * @return The ConnectionDefinition, or null if not found
     */
    public ConnectionDefinition getConnection(String connectionName) {
        return idGet(connections, symbols.resolveId(connectionName));
    }

    /**
     * @param runtimeName The runtime name (simple or qualified)
     * @return The RuntimeDefinition, or null if not found
     */
    public RuntimeDefinition getRuntime(String runtimeName) {
        return idGet(runtimes, symbols.resolveId(runtimeName));
    }

    /**
     * @param databaseName The database name (simple or qualified)
     * @return The raw DatabaseDefinition, or null if not found
     */
    public DatabaseDefinition getDatabaseDefinition(String databaseName) {
        return idGet(databases, symbols.resolveId(databaseName));
    }

    /**
     * @param mappingName The mapping name (simple or qualified)
     * @return The raw MappingDefinition, or null if not found
     */
    public MappingDefinition getMappingDefinition(String mappingName) {
        return idGet(mappingDefinitions, symbols.resolveId(mappingName));
    }

    /**
     * @param profileName The profile name (simple or qualified)
     * @return The ProfileDefinition, or null if not found
     */
    public ProfileDefinition getProfile(String profileName) {
        return idGet(profiles, symbols.resolveId(profileName));
    }

    /**
     * Resolves a live JDBC Connection from a Runtime name.
     * Looks up Runtime → Connection binding → ConnectionDefinition → JDBC Connection.
     *
     * @param runtimeName The runtime name (simple or qualified)
     * @return A live JDBC Connection
     * @throws java.sql.SQLException If connection cannot be established
     */
    public java.sql.Connection resolveConnection(String runtimeName) throws java.sql.SQLException {
        RuntimeDefinition runtime = idGet(runtimes, symbols.resolveId(runtimeName));
        if (runtime == null) {
            throw new IllegalArgumentException("Runtime not found: " + runtimeName);
        }
        String storeRef = runtime.connectionBindings().keySet().iterator().next();
        String connectionRef = runtime.connectionBindings().get(storeRef);
        ConnectionDefinition def = idGet(connections, symbols.resolveId(connectionRef));
        if (def == null) {
            throw new IllegalArgumentException("Connection not found: " + connectionRef);
        }
        return com.gs.legend.exec.ConnectionResolver.resolve(def);
    }

    /**
     * Resolves the SQL dialect from a Runtime name.
     * Looks up Runtime → Connection binding → ConnectionDefinition → dialect().
     * Does NOT require a live JDBC connection — works from model metadata alone.
     *
     * @param runtimeName The runtime name (simple or qualified)
     * @return The SQLDialect for this runtime's connection
     */
    public com.gs.legend.sqlgen.SQLDialect resolveDialect(String runtimeName) {
        RuntimeDefinition runtime = idGet(runtimes, symbols.resolveId(runtimeName));
        if (runtime == null) {
            throw new IllegalArgumentException("Runtime not found: " + runtimeName);
        }
        String storeRef = runtime.connectionBindings().keySet().iterator().next();
        String connectionRef = runtime.connectionBindings().get(storeRef);
        ConnectionDefinition def = idGet(connections, symbols.resolveId(connectionRef));
        if (def == null) {
            throw new IllegalArgumentException("Connection not found: " + connectionRef);
        }
        return com.gs.legend.exec.ConnectionResolver.dialectFor(def);
    }

    /**
     * @param serviceName The service name (simple or qualified)
     * @return The ServiceDefinition, or null if not found
     */
    public ServiceDefinition getService(String serviceName) {
        return idGet(services, symbols.resolveId(serviceName));
    }

    // ==================== Bulk Accessors (for NLQ indexing) ====================

    /**
     * Returns all registered classes (keyed by FQN).
     */
    public Map<String, PureClass> getAllClasses() {
        var result = new HashMap<String, PureClass>(classes.size());
        for (int i = 0; i < classes.size(); i++) {
            PureClass c = classes.get(i);
            if (c != null) result.put(symbols.nameOf(i), c);
        }
        return Map.copyOf(result);
    }

    /**
     * Returns all registered associations (keyed by FQN).
     */
    public Map<String, Association> getAllAssociations() {
        var result = new HashMap<String, Association>(associations.size());
        for (int i = 0; i < associations.size(); i++) {
            Association a = associations.get(i);
            if (a != null) result.put(symbols.nameOf(i), a);
        }
        return Map.copyOf(result);
    }

    /**
     * Returns all registered enums (keyed by FQN).
     */
    public Map<String, EnumDefinition> getAllEnums() {
        var result = new HashMap<String, EnumDefinition>(enums.size());
        for (int i = 0; i < enums.size(); i++) {
            EnumDefinition e = enums.get(i);
            if (e != null) result.put(symbols.nameOf(i), e);
        }
        return Map.copyOf(result);
    }

    /**
     * Returns all registered services (keyed by FQN).
     */
    public Map<String, ServiceDefinition> getAllServices() {
        var result = new HashMap<String, ServiceDefinition>(services.size());
        for (int i = 0; i < services.size(); i++) {
            ServiceDefinition s = services.get(i);
            if (s != null) result.put(symbols.nameOf(i), s);
        }
        return Map.copyOf(result);
    }

    // ==================== ModelContext Implementation ====================

    public Optional<ClassMapping> findMapping(String className) {
        // Try relational mapping first, then M2M
        var relOpt = mappingRegistry.findByClassName(className);
        if (relOpt.isPresent()) return Optional.of(relOpt.get());
        var pureOpt = mappingRegistry.findPureClassMapping(className);
        if (pureOpt.isPresent()) return Optional.of(pureOpt.get());
        return Optional.empty();
    }

    @Override
    public Optional<PureClass> findClass(String className) {
        return Optional.ofNullable(idGet(classes, symbols.resolveId(className)));
    }

    /**
     * Merges external class definitions into this model.
     * Used to add classes extracted from external sources (e.g., PCT interpreter metadata).
     * Does not overwrite existing classes from parsed Pure source.
     */
    public void addClasses(java.util.Map<String, PureClass> externalClasses) {
        if (externalClasses != null) {
            for (var entry : externalClasses.entrySet()) {
                int id = symbols.intern(entry.getKey());
                if (idGet(classes, id) == null) idPut(classes, id, entry.getValue());
            }
        }
    }

    /**
     * Returns all property→Join mappings for a class from explicit mapping references.
     * Includes both relational class mapping {@code @JoinName} and association mapping references.
     * Key: property name, Value: Join.
     *
     * <p>This is the mapping-level source of truth for which properties use which joins,
     * independent of whether the property comes from an Association or is declared directly on the class.
     */
    public java.util.Map<String, Join> findAllPropertyJoins(String className) {
        return explicitAssociationJoins.getOrDefault(className, Map.of());
    }

    /**
     * Full association navigation info for physical layers (MappingNormalizer).
     * NOT on ModelContext — only accessible via PureModelBuilder directly.
     */
    public record FullAssociationNavigation(
            String targetClassName,
            boolean isToMany,
            Join join) {}

    /**
     * Returns all association navigations for a given class, with full physical info.
     * Called by MappingNormalizer directly (not through ModelContext).
     */
    public java.util.Map<String, FullAssociationNavigation> findAllAssociationNavigationsFull_impl(String className) {
        var result = new java.util.LinkedHashMap<String, FullAssociationNavigation>();
        // Resolve to canonical FQN — target classes in associations are already FQN-normalized
        String fqn = className;
        for (Association assoc : getAssociationsForClass(fqn)) {
            var prop1 = assoc.property1();
            var prop2 = assoc.property2();

            if (prop2.targetClass().equals(fqn)) {
                boolean isToMany = prop1.multiplicity().isMany();
                Join join = findJoinForAssociationProperty(assoc.qualifiedName(), prop1.propertyName());
                result.putIfAbsent(prop1.propertyName(),
                        new FullAssociationNavigation(prop1.targetClass(), isToMany, join));
            }
            if (prop1.targetClass().equals(fqn)) {
                boolean isToMany = prop2.multiplicity().isMany();
                Join join = findJoinForAssociationProperty(assoc.qualifiedName(), prop2.propertyName());
                result.putIfAbsent(prop2.propertyName(),
                        new FullAssociationNavigation(prop2.targetClass(), isToMany, join));
            }
        }
        return result;
    }


    /**
     * Returns associations referencing the given class. Uses a lazily-built
     * index (className → List&lt;Association&gt;) for O(k) instead of O(N).
     */
    private List<Association> getAssociationsForClass(String className) {
        if (classToAssociations == null) {
            classToAssociations = buildClassToAssociationsIndex();
        }
        // className should already be FQN (resolved by caller)
        List<Association> result = classToAssociations.get(className);
        return result != null ? result : List.of();
    }

    private Map<String, List<Association>> buildClassToAssociationsIndex() {
        var index = new HashMap<String, List<Association>>();
        // Target classes are already FQN-normalized at registration time.
        // Each association is stored once under its FQN key — no dedup needed.
        for (Association assoc : associations) {
            if (assoc == null) continue;
            String target1 = assoc.property1().targetClass();
            String target2 = assoc.property2().targetClass();
            index.computeIfAbsent(target1, k -> new ArrayList<>()).add(assoc);
            if (!target1.equals(target2)) {
                index.computeIfAbsent(target2, k -> new ArrayList<>()).add(assoc);
            }
        }
        return index;
    }


    @Override
    public Map<String, AssociationNavigation> findAllAssociationNavigations(String className) {
        var result = new java.util.LinkedHashMap<String, AssociationNavigation>();
        for (var entry : findAllAssociationNavigationsFull(className).entrySet()) {
            var nav = entry.getValue();
            result.put(entry.getKey(), new AssociationNavigation(nav.targetClassName(), nav.isToMany()));
        }
        return result;
    }

    /**
     * Returns all association navigations with full physical info (including Join).
     * Called by MappingNormalizer directly (not through ModelContext).
     */
    public Map<String, FullAssociationNavigation> findAllAssociationNavigationsFull(String className) {
        return findAllAssociationNavigationsFull_impl(className);
    }

    @Override
    public Optional<AssociationNavigation> findAssociationByProperty(String fromClassName, String propertyName) {
        // Search associations for this class that have this property name.
        // Uses classToAssociations index for O(k) instead of O(N).
        String fqn = fromClassName;
        for (Association assoc : getAssociationsForClass(fqn)) {
            var prop1 = assoc.property1();
            var prop2 = assoc.property2();

            // Check if property1's name matches and property2's target is fromClassName
            if (prop1.propertyName().equals(propertyName) && prop2.targetClass().equals(fqn)) {
                boolean isToMany = prop1.multiplicity().isMany();
                return Optional.of(new AssociationNavigation(prop1.targetClass(), isToMany));
            }

            // Check if property2's name matches and property1's target is fromClassName
            if (prop2.propertyName().equals(propertyName) && prop1.targetClass().equals(fqn)) {
                boolean isToMany = prop2.multiplicity().isMany();
                return Optional.of(new AssociationNavigation(prop2.targetClass(), isToMany));
            }
        }
        return Optional.empty();
    }

    /**
     * Finds a join for an association property.
     * Checks explicit mappings first (from class mapping [DB]@JoinName syntax),
     * then falls back to convention-based matching.
     * 
     * @param associationName The association name
     * @param propertyName    The property name being navigated
     * @return The Join to use, or null if not found
     */
    /**
     * Finds the FQN of the association that owns the given property on the given class.
     * Returns null if no matching association is found.
     */
    private String findAssociationForProperty(String className, String propertyName) {
        for (Association assoc : getAssociationsForClass(className)) {
            var p1 = assoc.property1();
            var p2 = assoc.property2();
            if (p1.propertyName().equals(propertyName) && p2.targetClass().equals(className)) {
                return assoc.qualifiedName();
            }
            if (p2.propertyName().equals(propertyName) && p1.targetClass().equals(className)) {
                return assoc.qualifiedName();
            }
        }
        return null;
    }

    private Join findJoinForAssociationProperty(String associationName, String propertyName) {
        // Look up by association FQN → property name (scoped, no ambiguity)
        Map<String, Join> inner = explicitAssociationJoins.get(associationName);
        if (inner != null) {
            Join explicitJoin = inner.get(propertyName);
            if (explicitJoin != null) return explicitJoin;
        }
        return null;
    }


    public Optional<Join> findJoin(String db, String name) {
        java.util.Objects.requireNonNull(db, "db");
        java.util.Objects.requireNonNull(name, "name");
        return Optional.ofNullable(idGet(joins, symbols.resolveId(db + "." + name)));
    }

    @Override
    public Optional<Table> findTable(String db, String name) {
        java.util.Objects.requireNonNull(db, "db");
        java.util.Objects.requireNonNull(name, "name");
        return Optional.ofNullable(idGet(tables, symbols.resolveId(db + "." + name)));
    }

    @Override
    public Optional<EnumDefinition> findEnum(String enumName) {
        return Optional.ofNullable(idGet(enums, symbols.resolveId(enumName)));
    }

    @Override
    public boolean hasEnumValue(String enumName, String valueName) {
        EnumDefinition enumDef = idGet(enums, symbols.resolveId(enumName));
        return enumDef != null && enumDef.hasValue(valueName);
    }

    // ==================== Conversion Helpers ====================

    /**
     * Converts a property definition, resolving its type string structurally:
     *
     * <ol>
     *   <li>Parse the type string via {@link com.gs.legend.parser.PureModelParser#parsePureType}
     *       to produce a structured {@link Type} (possibly with unresolved {@link Type.NameRef}
     *       leaves and unresolved {@link Type.Parameterized} rawTypes).</li>
     *   <li>Walk the tree: any {@code NameRef(name)} where {@code name} is a declared
     *       type-parameter of the owning class is rewritten to {@link Type.TypeVar}. All other
     *       {@code NameRef}s must resolve to a primitive / class / enum via {@link #findType};
     *       failure to resolve throws, matching the no-fallback invariant.</li>
     * </ol>
     *
     * <p>Pre-phase-2.5e, this method called {@code Type.resolve(propDef.type(), this)} on the
     * whole string, which only worked for simple type names. Generic references like
     * {@code SortInfo<Any>} or bare type-param names like {@code T} failed resolution. The
     * structured path is required for the builtin class catalog (commit 2c).
     */
    private Property convertProperty(ClassDefinition.PropertyDefinition propDef,
                                     java.util.Set<String> typeParams) {
        Type parsed = com.gs.legend.parser.PureModelParser.parsePureType(propDef.type());
        if (parsed == null) {
            throw new IllegalStateException(
                    "Property '" + propDef.name() + "' has null or empty type string");
        }
        Type resolved = substituteTypeVarsAndClassify(parsed, typeParams, propDef.name());
        Multiplicity multiplicity = new Multiplicity.Bounded(propDef.lowerBound(), propDef.upperBound());
        return new Property(propDef.name(), resolved, multiplicity,
                propDef.taggedValues(), propDef.stereotypes());
    }

    /**
     * Walks a parsed {@link Type} tree from {@link com.gs.legend.parser.PureModelParser#parsePureType}
     * and produces a fully resolved tree:
     *
     * <ul>
     *   <li>{@link Type.NameRef} whose name is in {@code typeParams} → {@link Type.TypeVar}.</li>
     *   <li>Other {@link Type.NameRef} → classified primitive / class / enum via {@link #findType}
     *       (imports first, then kind lookup). Unresolvable names throw.</li>
     *   <li>Structural variants ({@link Type.GenericType}, {@link Type.FunctionType},
     *       {@link Type.RelationTypeVar}) recurse into their children. For
     *       {@link Type.GenericType}, a {@link Type.NameRef} rawType is resolved here
     *       (user class → {@link Type.ClassType}); an already-classified rawType
     *       (e.g., {@link com.gs.legend.model.m3.LClass} from {@code NameResolver}'s
     *       platform promotion) passes through unchanged.</li>
     *   <li>Already-classified leaves ({@link Primitive}, {@link Type.ClassType}, etc.) pass through.</li>
     * </ul>
     *
     * <p>The {@code context} argument is the property name, used only for error messages.
     */
    private Type substituteTypeVarsAndClassify(Type t, java.util.Set<String> typeParams, String context) {
        return switch (t) {
            case Type.NameRef nr -> {
                String name = nr.qualifiedName();
                if (typeParams.contains(name)) {
                    yield new Type.TypeVar(name);
                }
                String resolved = imports.resolve(name, symbols.allFqns());
                yield findType(resolved).orElseThrow(() -> new IllegalStateException(
                        "Property '" + context + "': unknown type '" + name + "'"
                                + (resolved.equals(name) ? "" : " (resolved to '" + resolved + "')")
                                + ". Not a primitive, class, enum, or declared type-parameter."));
            }
            case Type.GenericType gt -> {
                // Reuse the outer NameRef → classified-type logic for the rawType. Only a
                // concrete class / enum / primitive / already-classified LClass is meaningful
                // as a generic's rawType; a bare type-variable rawType (e.g. `T<X>`) has no
                // semantics in Pure and is rejected here rather than silently building a
                // GenericType(TypeVar, ...) that downstream dispatch can't interpret.
                Type resolvedRawType = substituteTypeVarsAndClassify(gt.rawType(), typeParams, context);
                if (resolvedRawType instanceof Type.TypeVar tv) {
                    throw new IllegalStateException(
                            "Property '" + context + "': generic rawType cannot be a type variable ("
                                    + tv.name() + "). Expected a class, enum, or platform class.");
                }
                List<Type> resolvedArgs = gt.typeArgs().stream()
                        .map(a -> substituteTypeVarsAndClassify(a, typeParams, context))
                        .toList();
                yield new Type.GenericType(resolvedRawType, resolvedArgs);
            }
            case Type.FunctionType ft -> {
                List<Type.Parameter> newParams = ft.params().stream()
                        .map(pp -> new Type.Parameter(pp.name(),
                                substituteTypeVarsAndClassify(pp.type(), typeParams, context),
                                pp.multiplicity()))
                        .toList();
                Type newReturn = substituteTypeVarsAndClassify(ft.returnType(), typeParams, context);
                yield new Type.FunctionType(newParams, newReturn, ft.returnMult());
            }
            case Type.RelationTypeVar rtv -> {
                List<Type.RelationTypeVar.Column> newCols = rtv.columns().stream()
                        .map(c -> new Type.RelationTypeVar.Column(c.name(),
                                substituteTypeVarsAndClassify(c.type(), typeParams, context),
                                c.multiplicity()))
                        .toList();
                yield new Type.RelationTypeVar(newCols);
            }
            // Already-resolved / structurally complete leaves — pass through unchanged.
            case Primitive prim -> prim;
            case Type.ClassType ct -> ct;
            case com.gs.legend.model.m3.LClass lc -> lc;
            case Type.EnumType et -> et;
            case Type.TypeVar tv -> tv;
            case Type.PrecisionDecimal pd -> pd;
            case Type.Relation r -> r;
            case Type.Tuple tu -> tu;
            case Type.SchemaAlgebra sa -> sa;
            case Type.FunctionReference fr -> fr;
        };
    }

    private Table convertTable(String dbFqn, DatabaseDefinition.TableDefinition tableDef) {
        return convertTable(dbFqn, "", tableDef);
    }

    private Table convertTable(String dbFqn, String schema, DatabaseDefinition.TableDefinition tableDef) {
        List<Column> columns = tableDef.columns().stream()
                .map(this::convertColumn)
                .toList();
        return new Table(dbFqn, schema, tableDef.name(), columns);
    }

    /**
     * Infers the view's main table by scanning ALL non-join column expressions for ColumnRefs.
     * Collects distinct table names and errors if 0 or >1 (matches legend-engine's identifyMainTable).
     */
    private String inferViewMainTable(View view) {
        var tableNames = new java.util.LinkedHashSet<String>();
        for (var col : view.columnMappings()) {
            var expr = col.expression();
            // Skip columns that reference joined tables (direct JoinNavigation or DynaFunc containing joins)
            if (expr instanceof com.gs.legend.model.def.RelationalOperation.JoinNavigation) {
                continue;
            }
            if (!findAllJoinNavigations(expr).isEmpty()) {
                continue;
            }
            tableNames.addAll(RelationalMappingConverter.collectTableNames(expr));
        }
        if (tableNames.isEmpty()) {
            throw new IllegalStateException(
                    "View '" + view.name() + "': cannot infer main table — no column references found");
        }
        if (tableNames.size() > 1) {
            throw new IllegalStateException(
                    "View '" + view.name() + "' references multiple tables " + tableNames
                    + " — there should be only one root table for views");
        }
        return tableNames.iterator().next();
    }

    private View convertView(String dbFqn, DatabaseDefinition.ViewDefinition viewDef) {
        List<View.ViewColumn> viewColumns = viewDef.columnMappings().stream()
                .map(vc -> new View.ViewColumn(vc.name(), vc.expression(), vc.primaryKey()))
                .toList();
        // Qualify ~filter: references with dbFqn at registration time
        RelationalOperation filterMapping = viewDef.filterMapping();
        if (filterMapping instanceof com.gs.legend.model.def.RelationalOperation.Literal lit
                && lit.value() instanceof String text && text.startsWith("~filter:")) {
            String bareFilterName = text.substring("~filter:".length());
            filterMapping = com.gs.legend.model.def.RelationalOperation.Literal.string(
                    "~filter:" + dbFqn + "." + bareFilterName);
        }
        return new View(dbFqn, viewDef.name(), filterMapping,
                viewDef.groupByColumns(), viewDef.distinct(), viewColumns);
    }

    private Column convertColumn(DatabaseDefinition.ColumnDefinition colDef) {
        SqlDataType dataType = resolveSqlType(colDef.dataType());
        return new Column(colDef.name(), dataType, !colDef.notNull());
    }

    private SqlDataType resolveSqlType(String typeName) {
        String upper = typeName.toUpperCase();
        if (upper.startsWith("VARCHAR"))
            return SqlDataType.VARCHAR;
        if (upper.equals("INTEGER") || upper.equals("INT"))
            return SqlDataType.INTEGER;
        if (upper.equals("BIGINT"))
            return SqlDataType.BIGINT;
        if (upper.equals("BOOLEAN") || upper.equals("BOOL"))
            return SqlDataType.BOOLEAN;
        if (upper.equals("DATE"))
            return SqlDataType.DATE;
        if (upper.equals("TIMESTAMP"))
            return SqlDataType.TIMESTAMP;
        if (upper.equals("DOUBLE") || upper.equals("FLOAT"))
            return SqlDataType.DOUBLE;
        if (upper.equals("DECIMAL") || upper.startsWith("DECIMAL"))
            return SqlDataType.DECIMAL;
        if (upper.equals("SEMISTRUCTURED") || upper.equals("JSON") || upper.equals("VARIANT"))
            return SqlDataType.SEMISTRUCTURED;
        throw new IllegalArgumentException("Unknown SQL data type: '" + typeName + "'. Add it to SqlDataType enum.");
    }

    /**
     * Finds ALL {@link com.gs.legend.model.def.RelationalOperation.JoinNavigation} nodes in a RelationalOperation tree.
     * Returns them in encounter order (left-to-right, depth-first).
     */
    public static java.util.List<com.gs.legend.model.def.RelationalOperation.JoinNavigation> findAllJoinNavigations(
            com.gs.legend.model.def.RelationalOperation op) {
        var result = new java.util.ArrayList<com.gs.legend.model.def.RelationalOperation.JoinNavigation>();
        collectJoinNavigations(op, result);
        return result;
    }

    private static void collectJoinNavigations(
            com.gs.legend.model.def.RelationalOperation op,
            java.util.List<com.gs.legend.model.def.RelationalOperation.JoinNavigation> out) {
        switch (op) {
            case com.gs.legend.model.def.RelationalOperation.JoinNavigation nav -> out.add(nav);
            case com.gs.legend.model.def.RelationalOperation.FunctionCall func ->
                    func.args().forEach(a -> collectJoinNavigations(a, out));
            case com.gs.legend.model.def.RelationalOperation.Comparison cmp -> {
                collectJoinNavigations(cmp.left(), out);
                collectJoinNavigations(cmp.right(), out);
            }
            case com.gs.legend.model.def.RelationalOperation.BooleanOp bool -> {
                collectJoinNavigations(bool.left(), out);
                collectJoinNavigations(bool.right(), out);
            }
            case com.gs.legend.model.def.RelationalOperation.Group g -> collectJoinNavigations(g.inner(), out);
            case com.gs.legend.model.def.RelationalOperation.IsNull n -> collectJoinNavigations(n.operand(), out);
            case com.gs.legend.model.def.RelationalOperation.IsNotNull n -> collectJoinNavigations(n.operand(), out);
            default -> { }
        }
    }

    /**
     * Extracts column name from an expression like "[DB] TABLE.COLUMN->cast(@Class)".
     */
    private String extractColumnNameFromExpression(String expression) {
        // Pattern: [DB] TABLE.COLUMN...
        java.util.regex.Pattern pattern = java.util.regex.Pattern.compile(
                "\\[\\w+\\]\\s+\\w+\\.(\\w+)");
        java.util.regex.Matcher matcher = pattern.matcher(expression);
        if (matcher.find()) {
            return matcher.group(1);
        }
        throw new IllegalArgumentException("Cannot extract column name from expression: " + expression);
    }
}
