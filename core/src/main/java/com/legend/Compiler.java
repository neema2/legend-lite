package com.legend;

import com.legend.compiler.KnowledgeLayer;
import com.legend.compiler.ModelBuilder;
import com.legend.compiler.NameResolver;
import com.legend.compiler.element.PureModelContext;
import com.legend.compiler.element.ModelContext;
import com.legend.compiler.spec.SpecCompiler;
import com.legend.compiler.spec.typed.TypedSpec;
import com.legend.normalizer.ModelNormalizer;
import com.legend.model.NormalizedModel;
import com.legend.parser.ElementParser;
import com.legend.model.ParsedModel;

import java.util.List;
import java.util.Objects;

/**
 * THE PLANNER (C2a, docs/PLAN_EXECUTION_SPLIT_AND_DATABASE_OWNER_2026_10_03.md): model text to a compiled model, a
 * query to its typed tree, its SQL tree and its SQL — the pipeline's steps A&rarr;J listed in
 * {@code package-info.java}, none of which needs a database. Running a planned query on a session (step K) is
 * {@link Execution}'s, which reaches the planner only through its public API ({@link #compileModel},
 * {@link #resolveQuery}, {@link #executesOn}, {@link #lower}). This class lives in {@code //core:planner}, whose
 * build has no execution library: a plan-only consumer (the WebAssembly planner) carries no database and no driver.
 */
public final class Compiler {

    private Compiler() {}

    /** Parse a model at the PRODUCT level (LEGEND_LITE) — the front door
     *  for product endpoints (the HTTP server), so they never touch the
     *  parser package directly. The Compiler is the product's provenance
     *  router: which level users get is decided HERE. */
    public static ParsedModel parseModel(String source) {
        return com.legend.parser.ElementParser.parse(source,
                com.legend.parser.Dialect.LEGEND_LITE);
    }

    /** Parse one query/expression at the PRODUCT level. */
    public static com.legend.protocol.spec.ValueSpecification
            parseQuery(String query) {
        return com.legend.parser.SpecParser.parse(query,
                com.legend.parser.Dialect.LEGEND_LITE);
    }


    /**
     * Frontend pipeline: Pure model source &rarr; typed {@link ModelContext}.
     *
     * <p>Drives steps 1&ndash;6 (the steps implemented in {@code core/} today;
     * the query/spec and backend steps land later):
     * <ol>
     *   <li><b>parse</b> &mdash; {@link ElementParser#parse} (lex + parse-element).</li>
     *   <li><b>resolve-names</b> &mdash; {@link NameResolver#resolve(ParsedModel)}
     *       rewrites simple names to FQNs against the user imports + platform
     *       prelude (the prelude is owned by the resolver, not this driver).</li>
     *   <li><b>normalize</b> (Phase E) &mdash; {@link ModelNormalizer#normalize}
     *       externalizes body sites into synthesized functions.</li>
     *   <li><b>element-compile</b> (Phase F) &mdash; {@link PureModelContext#from}
     *       builds the typed model; synth functions flatten into
     *       {@code findFunction} uniformly with user functions.</li>
     * </ol>
     *
     * <p>This is the single orchestration point: it owns step <em>ordering</em>.
     * Each step is the same method its own unit tests exercise &mdash; there is
     * no orchestrator-only code path.
     *
     * @param model Pure model source (classes, enums, associations, databases,
     *              mappings, services, runtimes, ...).
     * @return the populated, queryable {@link ModelContext}.
     */
    public static ModelContext compileModel(@com.legend.base.Nullable String model) {
        Objects.requireNonNull(model, "model");
        ParsedModel parsed = ElementParser.parse(model,
                com.legend.parser.Dialect.LEGEND_LITE);
        try {
            return buildModel(parsed);
        } catch (com.legend.error.ModelException e) {
            // Decorate with the offending ELEMENT's [line:col] — the offsets
            // live on the original parse (resolution rebuilds ParsedModel
            // without them), so the driver is where source meets failure.
            Integer off = e.element() == null ? null
                    : parsed.elementOffsets().get(e.element());
            if (off == null || parsed.source() == null) {
                throw e;
            }
            throw new com.legend.error.ModelException(e.phase(),
                    com.legend.error.LegendCompileException.position(parsed.source(), off)
                            + " " + e.getMessage(), e.element());
        }
    }

    /** One named source unit of a multi-file model (a MODULE member). */
    public record ModelSource(String name, String text) {
        public ModelSource {
            Objects.requireNonNull(name, "name");
            Objects.requireNonNull(text, "text");
        }
    }

    /**
     * A parsed multi-source MODULE: the merged {@link ParsedModel} plus the
     * duplicate elements that were dropped (first definition wins; each
     * loser is reported as {@code kind fqn (source, kept source)} so the
     * caller can wall it) and the per-unit texts for error decoration.
     */
    public record ParsedModule(ParsedModel model, List<String> duplicateElements,
                               java.util.Map<String, String> sourceTexts) {
        public ParsedModule {
            duplicateElements = List.copyOf(duplicateElements);
            sourceTexts = java.util.Collections.unmodifiableMap(new java.util.LinkedHashMap<>(sourceTexts));
        }
    }

    /**
     * Parse each source as its OWN unit — per-file import sections, per-file
     * positions — and merge into one model: the MODULE compile every real
     * legend project needs (the engine compiles a repository's files
     * together; cross-file references are normal). Imports never leak
     * across units: each element resolves against its own section's scope,
     * and the merged model's GLOBAL scope is empty (per-element scopes are
     * total, so the fallback never widens).
     */
    public static ParsedModule parseSources(List<ModelSource> sources) {
        return parseSources(sources, null);
    }

    /**
     * {@link #parseSources(List)} with an optional PER-FILE parse wall
     * sink (source name &rarr; first error line): an unparseable file is
     * reported and EXCLUDED instead of failing the whole batch — and the
     * parse result is REUSED for the merge, so callers never pre-parse
     * for validation and re-parse for assembly (the corpus runner's
     * throwaway-parse pattern). Null = strict (first parse error throws).
     */
    public static ParsedModule parseSources(List<ModelSource> sources,
            java.util.function.@com.legend.base.Nullable BiConsumer<String, String> parseWallSink) {
        return parseSources(sources, parseWallSink,
                com.legend.parser.Dialect.LEGEND_LITE);
    }

    /** As above, DIALECT-EXPLICIT: the caller declares its sources'
     *  provenance level (the corpus runner's m2 corpus is
     *  LEGEND_PLATFORM; user batches are LEGEND_LITE). */
    public static ParsedModule parseSources(List<ModelSource> sources,
            java.util.function.@com.legend.base.Nullable BiConsumer<String, String> parseWallSink,
            com.legend.parser.Dialect dialect) {
        Objects.requireNonNull(sources, "sources");
        List<com.legend.model.PackageableElement> elements = new java.util.ArrayList<>();
        java.util.Map<String, Integer> offsets = new java.util.HashMap<>();
        java.util.Map<String, com.legend.model.ImportScope> elementImports =
                new java.util.HashMap<>();
        java.util.Map<String, String> elementSources = new java.util.HashMap<>();
        java.util.Map<String, String> sourceTexts = new java.util.LinkedHashMap<>();
        java.util.Map<String, String> seen = new java.util.HashMap<>();   // key -> source
        List<String> duplicates = new java.util.ArrayList<>();
        for (ModelSource src : sources) {
            sourceTexts.put(src.name(), src.text());
            ParsedModel unit;
            try {
                unit = ElementParser.parse(src.text(), dialect);
            } catch (com.legend.error.LegendCompileException e) {
                if (parseWallSink == null) {
                    throw e;
                }
                parseWallSink.accept(src.name(),
                        String.valueOf(e.getMessage()).split("\n")[0]);
                continue;
            }
            for (com.legend.model.PackageableElement el : unit.elements()) {
                // FUNCTIONS overload: same FQN with different signatures is
                // NOT a duplicate — the dedup key carries the parameter
                // shape (dropping overloads silently lost the corpus's own
                // executeInDb wrappers)
                // (NATIVE overloads too: keying natives by name alone collapsed
                // legend-pure's six date(...) declarations to one — batch 5's
                // signature generator caught it)
                String key = el instanceof com.legend.model.Function fn
                        ? el.getClass().getSimpleName() + "::" + fn.qualifiedName() + "("
                                + fn.parameters().stream().map(pd -> String.valueOf(pd.type())
                                        + String.valueOf(pd.multiplicity()))
                                .reduce("", (x, y) -> x + "," + y) + ")"
                        : el.getClass().getSimpleName() + "::" + el.qualifiedName();
                String prior = seen.putIfAbsent(key, src.name());
                if (prior != null) {
                    // FIRST definition wins (the corpus carries alternative
                    // models in parent directories); the drop is REPORTED,
                    // never silent
                    duplicates.add(key + " (" + src.name()
                            + ", kept " + prior + ")");
                    continue;
                }
                elements.add(el);
                // per ELEMENT (ParsedModel.keyOf): an overload's section,
                // position and file are its own — keyed by name, the last
                // file read set every overload's import scope (Phase 3b, 5b)
                String elementKey = ParsedModel.keyOf(el);
                Integer off = unit.elementOffsets().get(elementKey);
                if (off != null) {
                    offsets.put(elementKey, off);
                }
                com.legend.model.ImportScope own = unit.elementImports().get(elementKey);
                if (own != null) {
                    elementImports.put(elementKey, own);
                }
                elementSources.put(elementKey, src.name());
            }
        }
        return new ParsedModule(
                new ParsedModel(elements, com.legend.model.ImportScope.empty(),
                        null, offsets, elementImports, elementSources),
                duplicates, sourceTexts);
    }

    /**
     * The back half of {@link #compileModel(String)} over an
     * already-parsed model: resolve names, normalize, build the context.
     * Multi-source callers decorate errors themselves (they hold the
     * per-unit texts).
     */
    public static ModelContext buildModel(ParsedModel parsed) {
        // the system metamodel store rides EVERY build (charter §4: one
        // owner, parsed elements, no parallel lane)
        Layer layer = normalizeWithSystem(NameResolver.resolveAlongside(parsed,
                bootFqns(), null), null);
        return PureModelContext.from(layer.model(), layer.index(), null, boot().checked(),
                com.legend.lowering.PlatformRegistrations.current());
    }

    /** A normalized layer with THE index its Phase E read (T4.1 step 2):
     * the gate adds the layer's products to that same index. */
    private record Layer(NormalizedModel model, ModelBuilder index) {}

    /**
     * name-resolved elements &rarr; F1 knowledge (association qualified
     * properties adopt into their owners) &rarr; THE ONE INDEX &rarr; Phase E
     * over it. The only {@code ModelBuilder.from} in the compile path: both
     * layers (boot, graph) enter here.
     */
    private static Layer normalizeLayer(ParsedModel resolved,
            java.util.@com.legend.base.Nullable Map<String, String> walls) {
        ParsedModel adopted = KnowledgeLayer.adoptAssociationQualifiedProperties(resolved, walls);
        ModelBuilder index = ModelBuilder.from(adopted);
        return new Layer(ModelNormalizer.normalize(adopted, index, walls), index);
    }

    /**
     * THE BOOT LAYER (user ruling 2026-09-02): the system metamodel's
     * elements are name-resolved and normalized ONCE per process,
     * content-addressed by the hash of their Pure source (Invariant 3 —
     * the artifact persists across compiles), and entered into every
     * graph's index exactly like the graph's own elements. Re-normalizing
     * them per graph compile was 5.7ms of an 8ms compile (docs/GATES.md,
     * 2026-09-02 budget entry); per graph what remains is indexing 78
     * prepared elements. One graph per process outside the test JVM.
     */
    private static final com.legend.cache.ContentStore BOOT =
            new com.legend.cache.ContentStore(4);

    /** The boot layer and its integrity: checked ON ITS OWN when it is
     * built, so a graph's check covers only the graph (and what spans the
     * two) instead of re-checking the whole platform per compile. */
    private record Boot(NormalizedModel model, PureModelContext.CheckedLayer checked) {
    }

    private static NormalizedModel bootLayer() {
        return boot().model();
    }

    private static Boot boot() {
        // the system metamodel AND the generated prelude module
        // (SYSTEM_PRELUDE_DESIGN §10): one boot source, its hash the cache
        // key; the prelude's elements keep their section imports (a
        // derived body resolves through them), the system metamodel's
        // resolve in the empty scope as before
        return BOOT.getOrCompute(BootKey.HASH, () -> {
            // the system metamodel's own row-reading twins of platform
            // functions (classMappingById, mainTable, …) are the platform's
            // implementations — they win over the library's copies exactly
            // as they win over a graph's (batch 169)
            ParsedModel pre = com.legend.builtin.SystemMetamodel.withoutSystemShadows(
                    com.legend.builtin.Prelude.parsedModel());
            List<com.legend.model.PackageableElement> elements = new java.util.ArrayList<>(
                    com.legend.builtin.SystemMetamodel.elements());
            elements.addAll(pre.elements());
            ParsedModel boot = new ParsedModel(elements, com.legend.model.ImportScope.empty(), null,
                    pre.elementOffsets(), pre.elementImports(), pre.elementSources());
            // the boot layer's own index is checked and then discarded: its
            // prepared elements enter every graph's index at that graph's gate
            Layer layer = normalizeLayer(NameResolver.resolve(boot), null);
            return new Boot(layer.model(),
                    PureModelContext.checkLayer(layer.model(), layer.index(),
                            com.legend.lowering.PlatformRegistrations.current()));
        });
    }

    /** The boot source's content address: both sources are constants of
     * the process, so their hash is too — computed once, not per compile
     * (it hashed ~330 KB on every call to find the one cached layer). */
    private static final class BootKey {
        static final com.legend.cache.Hash HASH = com.legend.cache.Hash.ofUtf8(
                com.legend.builtin.SystemMetamodel.source() + "\n"
                        + com.legend.builtin.Prelude.source());
    }

    /** The boot layer's FQNs — what a graph's own elements may name by import. */
    private static java.util.Set<String> bootFqns() {
        return BootFqns.ALL;
    }

    private static final class BootFqns {
        static final java.util.Set<String> ALL = union();

        private static java.util.Set<String> union() {
            java.util.Set<String> out = new java.util.HashSet<>(
                    com.legend.builtin.SystemMetamodel.elementFqns());
            out.addAll(com.legend.builtin.Prelude.elementFqns());
            return java.util.Collections.unmodifiableSet(new java.util.LinkedHashSet<>(out));
        }
    }

    /**
     * T4 (PRELUDE_MODULE_HOMEWORK §2): a graph class or enum redefining a
     * PRELUDE shape is a modeling error; transitionally the prelude wins
     * and the graph's copy is dropped — what the catalog-first lookup did
     * silently before §10 (the corpus tree's copies of platform classes,
     * the census's spec files). The receipt list is at the foot of
     * prelude.pure and burns to zero in phase 3.
     */
    private static ParsedModel withoutPreludeShadows(ParsedModel parsed) {
        List<com.legend.model.PackageableElement> kept = new java.util.ArrayList<>();
        for (com.legend.model.PackageableElement el : parsed.elements()) {
            boolean shadow = (el instanceof com.legend.model.ClassDefinition
                    && com.legend.builtin.Prelude.classFqns().contains(el.qualifiedName()))
                    || (el instanceof com.legend.model.EnumDefinition
                    && com.legend.builtin.Prelude.enumFqns().contains(el.qualifiedName()))
                    // the platform library's FUNCTIONS too (batch 169): a graph copy
                    // of a legend-pure function (the census's sources, a corpus
                    // tree's twin) yields to the module's — by function id (build
                    // rebuild Phase 3): another version under the same name is its
                    // own function and stays
                    || (el instanceof com.legend.model.FunctionDefinition fd
                    && com.legend.builtin.Prelude.functionIds().contains(com.legend.model.FunctionId.of(fd)));
            if (!shadow) {
                kept.add(el);
            }
        }
        return kept.size() == parsed.elements().size() ? parsed
                : new ParsedModel(kept, parsed.imports(), parsed.source(),
                        parsed.elementOffsets(), parsed.elementImports(),
                        parsed.elementSources(), parsed.unclaimedSections());
    }

    /**
     * Phase E over the graph's OWN elements (name-resolved first, so a
     * same-signature system function shadow is recognized by its resolved
     * parameter types — the corpus carries the engine's own
     * inferRelationalType), then the boot layer's prepared elements join
     * the normalized model; a graph element redefining a system element
     * is an error (SystemMetamodel.withoutSystemShadows).
     */
    private static Layer normalizeWithSystem(ParsedModel resolved,
            java.util.@com.legend.base.Nullable Map<String, String> walls) {
        Layer user = normalizeLayer(
                com.legend.builtin.SystemMetamodel.withoutSystemShadows(
                        withoutPreludeShadows(resolved)), walls);
        NormalizedModel sys = bootLayer();
        List<com.legend.model.PackageableElement> elements =
                new java.util.ArrayList<>(user.model().elements().size() + sys.elements().size());
        elements.addAll(user.model().elements());
        elements.addAll(sys.elements());
        // the compiled mappings carry their own facts (poisons, unions, the
        // nullable census) — the layer union has nothing else to merge
        return new Layer(new NormalizedModel(elements, user.model().imports(),
                union(user.model().legacySurfaces(), sys.legacySurfaces())), user.index());
    }

    private static <V> java.util.Map<String, V> union(
            java.util.Map<String, V> a, java.util.Map<String, V> b) {
        if (b.isEmpty()) {
            return a;
        }
        java.util.Map<String, V> out = new java.util.LinkedHashMap<>(a);
        out.putAll(b);
        return out;
    }

    /** A module built TOLERANTLY: the context over every element that
     * compiles, plus the walls (element FQN => first error line) for every
     * element that does not — the engine-parity behavior for compiling a
     * repository (compile what compiles, report the rest). */
    public record BuiltModule(ModelContext context,
                              java.util.Map<String, String> walls) {
        public BuiltModule {
            walls = java.util.Collections.unmodifiableMap(
                    new java.util.LinkedHashMap<>(walls));
        }
    }

    /**
     * Tolerant module build — POISON, DON'T DROP: every element stays in
     * the model; the walls map records each broken element's FIRST failure
     * reason (eager DIAGNOSIS over the whole module). A broken element
     * harms nothing that merely references it — the failure fires at USE
     * time (compiling the function on call, materializing the binding),
     * loudly, when something actually enters the quarantine. Dropping
     * instead cascaded: removing a walled helper failed every element
     * referencing it, and every test touching THOSE — 182 corpus tests
     * died in the blast radius of functions they never called.
     * One exception: a mapping that fails to NORMALIZE has no canonical
     * form to keep and is excluded (its absence is walled; the legacy
     * per-family harness behaved identically). Unattributed failures
     * still throw — a genuine bug must fail the build.
     */
    public static BuiltModule buildModule(ParsedModel parsed) {
        java.util.Map<String, String> walls = new java.util.LinkedHashMap<>();
        Layer layer = normalizeWithSystem(NameResolver.resolveAlongside(parsed,
                bootFqns(), walls), walls);
        PureModelContext ctx = PureModelContext.from(layer.model(), layer.index(), walls,
                boot().checked(), com.legend.lowering.PlatformRegistrations.current());
        return new BuiltModule(ctx, walls);
    }

    /**
     * Compile a multi-source MODULE. Errors carry the offending element's
     * SOURCE NAME and [line:col] within that source.
     */
    public static ModelContext compileModel(List<ModelSource> sources) {
        ParsedModule module = parseSources(sources);
        try {
            return buildModel(module.model());
        } catch (com.legend.error.ModelException e) {
            String fqn = e.element();
            String srcName = fqn == null ? null
                    : module.model().elementSources().get(fqn);
            Integer off = fqn == null ? null
                    : module.model().elementOffsets().get(fqn);
            if (srcName == null || off == null) {
                throw e;
            }
            throw new com.legend.error.ModelException(e.phase(),
                    srcName + " " + com.legend.error.LegendCompileException
                            .position(java.util.Objects.requireNonNull(
                                    module.sourceTexts().get(srcName)), off)
                            + " " + e.getMessage(), e.element());
        }
    }

    /** A query lowered for a runtime: its SQL tree, its typed (resolved) root, and the compiled model it was planned
     *  against — what execution renders with the session's dialect and runs ({@code Execution}). */
    public record LoweredQuery(com.legend.sql.SqlQuery plan, TypedSpec root, ModelContext ctx) {
    }

    /**
     * The execution target a query names, as the COMPILER reads it (U3, the plan's
     * connection): the runtime its {@code ->from(runtime)} binds, and the database its
     * {@code #>{db...}#} accessor reads -- null for a class query, whose store its mapping
     * chooses. Read off the typed tree by node type, never by a call's name.
     */
    public record Target(@com.legend.base.Nullable String runtime,
            @com.legend.base.Nullable String store) {
    }

    /**
     * THE QUERY STEP of the compile-once API (C2b, docs/PLAN_EXECUTION_SPLIT_AND_DATABASE_OWNER_2026_10_03.md):
     * {@code query}'s text against the compiled model {@code ctx} — parsed at the product level, its names resolved,
     * typed once. Its type, its named target, its plan and its lowering all come off the one {@link TypedQuery}; the
     * model is compiled once ({@link #compileModel}) and never again per question.
     */
    public static TypedQuery query(ModelContext ctx, String query) {
        return query(ctx, parseQuery(query));
    }

    /** {@link #query(ModelContext, String)} for a query that arrives ALREADY PARSED — an upstream {@code pure/v1}
     *  request's lambda (docs/UPSTREAM_ENDPOINTS_DESIGN_2026_09_27.md, U2): nothing is re-spelled as text. */
    public static TypedQuery query(ModelContext ctx, com.legend.protocol.spec.ValueSpecification query) {
        return new TypedQuery(ctx, NameResolver.resolveQuery(query));
    }

    /** {@link #query(ModelContext, ValueSpecification)} for a query that is ALREADY RESOLVED, the output of
     *  {@link #resolveQuery}: nothing is resolved twice. The path a measurement takes when it must count the
     *  resolution once (W1.0b's compile-only latency); the executor's own entry is {@link Execution}. */
    public static TypedQuery queryResolved(ModelContext ctx, com.legend.protocol.spec.ValueSpecification resolved) {
        return new TypedQuery(ctx, resolved);
    }

    /** THE dialect of a query planned without a session: the database its runtime executes on. */
    static com.legend.sql.dialect.SqlDialect dialectOf(ModelContext ctx,
            @com.legend.base.Nullable String runtimeFqn) {
        return com.legend.database.Databases.dialect(executesOn(ctx, runtimeFqn).type());
    }

    /** A query given no runtime: where it executes is undeclared. */
    public static final String NO_RUNTIME = "a query executes on its runtime's declared connection, and none was given:"
            + " add ->from(mapping, runtime) or supply a runtime";

    /**
     * THE database a query executes on -- declared, never inferred:
     * <ul>
     * <li>the database its runtime's connections declare (every relational connection the runtime
     * binds names the same {@code DatabaseType}); data the runtime binds from no database (a
     * {@code ModelStore}'s JSON or instances) travels inline in that database's SQL;</li>
     * <li>a runtime binding ONLY such data -- no database at all -- executes on the platform's
     * in-process DuckDB: legend-lite's counterpart of legend-engine's in-memory model-to-model
     * execution (docs/SEMANTICS_REGISTER.md, S27).</li>
     * </ul>
     * No runtime, an undefined runtime, a runtime binding no connection at all, and a runtime mixing
     * databases are refused, by name.
     */
    public static com.legend.database.Target executesOn(ModelContext ctx,
            @com.legend.base.Nullable String runtimeFqn) {
        if (runtimeFqn == null) {
            throw new com.legend.error.MappingResolutionException(NO_RUNTIME);
        }
        var rt = ctx.findRuntime(runtimeFqn).orElseThrow(() -> new com.legend.error.MappingResolutionException(
                "runtime '" + runtimeFqn + "' is not defined", runtimeFqn));
        // EVERY binding is inspected, in sorted (deterministic) order -- connection bindings are an
        // unordered map, and first-match-wins was nondeterministic (audit)
        var connections = new java.util.TreeMap<String, com.legend.model.ConnectionDefinition>();
        // a ModelStore's inline JsonModelConnections are model data too: the parser keeps them on the runtime
        // (jsonConnections), not among its connection bindings (C3b: a JSON-only runtime was refused as
        // "binds no connection")
        boolean modelData = !rt.jsonConnections().isEmpty();
        var bound = new java.util.TreeSet<String>();
        rt.connectionBindings().values().forEach(bound::addAll);
        for (String connFqn : bound) {
            var conn = ctx.findConnection(connFqn);
            if (conn.isPresent()) {
                connections.put(connFqn, conn.get());
            } else if (ctx.isModelConnection(connFqn)) {
                modelData = true;
            } else {
                throw new com.legend.error.MappingResolutionException(
                        "connection '" + connFqn + "' of runtime '" + runtimeFqn + "' is not defined", runtimeFqn);
            }
        }
        var types = new java.util.TreeMap<String, com.legend.model.ConnectionDefinition.DatabaseType>();
        connections.forEach((f, c) -> types.put(f, c.databaseType()));
        var distinct = new java.util.TreeSet<>(types.values());
        if (distinct.size() > 1) {
            throw new com.legend.error.NotImplementedException("runtime '" + runtimeFqn + "' mixes databases "
                    + types + " — one database per query is supported");
        }
        if (!distinct.isEmpty()) {
            return new com.legend.database.Target.Declared(distinct.first(),
                    java.util.List.copyOf(connections.values()));
        }
        if (modelData) {
            return new com.legend.database.Target.Platform();
        }
        throw new com.legend.error.NotImplementedException("runtime '" + runtimeFqn
                + "' binds no connection: a query executes on a runtime's declared connection");
    }

    /** THE query front door: raw-space desugars (the relational
     * {@code validate(...)} call → its synthesized execute; the engine's
     * own generateValidationQuery synthesis over the parsed AST, feature
     * #45), then name resolution. Every executor — the harness's flip
     * included — resolves through here, so a platform feature never
     * depends on a harness preamble to fire. */
    public static com.legend.protocol.spec.ValueSpecification resolveQuery(
            java.util.List<com.legend.protocol.spec.ValueSpecification> statements,
            com.legend.model.ImportScope imports, ModelContext ctx) {
        // ONE name resolution, first (the resolver's own scope rules: imports,
        // own package, prelude; candidates on a bare call) — every pass
        // below consumes the resolver's names (ResolvedNames) and constructs
        // only bare natives and lets, which need no further resolution
        statements = ((com.legend.protocol.spec.LambdaFunction)
                com.legend.compiler.NameResolver.resolveQueryIn(
                        new com.legend.protocol.spec.LambdaFunction(java.util.List.of(), statements),
                        imports, ctx.resolutionUniverse())).body();
        // a call the platform implements (validate: a Form row) is never inlined
        // as the user's body — StatementInline asks the implementation table, so
        // the two passes may run in either order
        statements = com.legend.compiler.StatementInline.rewrite(statements, imports, ctx);
        java.util.List<com.legend.protocol.spec.ValueSpecification> desugared =
                new java.util.ArrayList<>(statements.size());
        for (com.legend.protocol.spec.ValueSpecification st : statements) {
            desugared.add(com.legend.validation.ValidateDesugar
                    .rewrite(st, ctx, imports.wildcards()));
        }
        // a statement-root map over spelled bound names unrolls to its
        // element statements (batch 72a — the element asserts become
        // statement-root verdicts)
        desugared = com.legend.compiler.LiteralMapUnroll.rewrite(desugared);
        return new com.legend.protocol.spec.LambdaFunction(java.util.List.of(), desugared);
    }







    /**
     * COMPILED-STATE effect query over a resolved statement body: does
     * executing it WRITE (DDL/executeInDb, transitively through compiled
     * user-function bodies — owner: the compiler's
     * {@link com.legend.compiler.spec.StatementEffects} scan over the
     * native catalog)? TDG
     * generators count as effectful here: their carrier materializes
     * temp tables. The flip probe's re-run safety fact — derived from
     * the program, never from harness heuristics.
     */
    public static boolean hasStatementEffects(
            com.legend.protocol.spec.ValueSpecification resolved,
            ModelContext ctx) {
        return programFacts(resolved, ctx).effects();
    }

    /** The platform's facts about a resolved program ({@link ProgramFacts}),
     * in one typing pass. */
    public static ProgramFacts programFacts(
            com.legend.protocol.spec.ValueSpecification resolved,
            ModelContext ctx) {
        SpecCompiler specs = new SpecCompiler(ctx);
        java.util.List<TypedSpec> body = specs.typeQueryBody(resolved);
        java.util.Map<com.legend.model.FunctionId, Boolean> memo = new java.util.HashMap<>();
        java.util.Map<com.legend.model.FunctionId, Boolean> verdictMemo = new java.util.HashMap<>();
        java.util.Map<com.legend.model.FunctionId, java.util.Set<String>> storeMemo = new java.util.HashMap<>();
        boolean effects = false;
        boolean seeds = false;
        boolean verdicts = false;
        java.util.Set<String> stores = new java.util.LinkedHashSet<>();
        StringBuilder shape = new StringBuilder(body.size());
        for (int i = 0; i < body.size(); i++) {
            TypedSpec s = body.get(i);
            java.util.List<TypedSpec> preceding = body.subList(0, i);
            boolean effect = com.legend.compiler.spec.StatementEffects.containsEffect(s, specs, memo)
                    || com.legend.compiler.spec.StatementEffects.containsTdgGenerator(s);
            boolean verdict = com.legend.compiler.spec.StatementEffects.callsVerdict(s, specs, verdictMemo);
            shape.append(statementKind(s, effect, verdict));
            effects |= effect;
            stores.addAll(com.legend.compiler.spec.SeededStores.of(s, specs, storeMemo));
            // the ONE reader of runtime shapes: inline CSV test data anywhere
            // in the statement (a from(), an execute's runtime argument, a
            // let-bound connection copy) is a bound-context fact; values the
            // statement names chase its preceding lets
            seeds |= !com.legend.compiler.spec.typed.ExecutionContext.reader()
                    .bind(v -> com.legend.compiler.spec.typed.Lets.bound(v, preceding))
                    .read(java.util.Optional.empty(), s).csvSetups().isEmpty();
            verdicts |= verdict;
        }
        return new ProgramFacts(effects, seeds, verdicts, stores, shape.toString());
    }

    /** One statement's letter in {@link ProgramFacts#shape()}. */
    private static char statementKind(TypedSpec s, boolean effect, boolean verdict) {
        if (s instanceof com.legend.compiler.spec.typed.TypedNativeCall c
                && com.legend.builtin.NativeFn.ContextOwner.of(c.callee().id()).isPresent()) {
            return 'X';
        }
        if (s instanceof com.legend.compiler.spec.typed.TypedLet l) {
            TypedSpec v = l.value();
            while (v instanceof com.legend.compiler.spec.typed.TypedFrom f) {
                v = f.source();
            }
            if (v instanceof com.legend.compiler.spec.typed.TypedNativeCall ec
                    && com.legend.builtin.NativeFn.Handle.isExecute(ec.callee().id())) {
                return 'F';
            }
            return effect ? 'E' : 'L';
        }
        return verdict ? 'A' : effect ? 'E' : 'O';
    }




    /**
     * Phases G&frac12;&rarr;I for an already NAME-RESOLVED query AST — the
     * SQL PLAN without execution (the {@code toSQLString} surface: the
     * caller renders with a dialect of its choosing and compares text).
     */
    /**
     * {@code relationalRootForm}: a BARE class root renders as the engine's
     * flat relational SELECT — primary-key columns ({@code pk_0}..) plus
     * the property leaves — instead of the platform's JSON envelope. The
     * engine assembles objects HOST-side from that flat select, so its
     * {@code toSQLString} goldens pin this form; execution paths never use
     * it (Java orchestrates, the database executes — the envelope stays).
     */
    public static com.legend.sql.SqlQuery lowerResolved(
            com.legend.protocol.spec.ValueSpecification resolved, ModelContext ctx,
            String runtimeFqn, boolean relationalRootForm) {
        return lowerResolved(resolved, ctx, runtimeFqn, relationalRootForm,
                null);
    }

    /** {@code explicitMappingFqn}: class fetches resolve against THIS
     * mapping — the caller's API surface named one (scanColumns(tree, m):
     * lineage lowering must dispatch under the call's own mapping, never
     * the ambient runtime candidates). */
    public static com.legend.sql.SqlQuery lowerResolved(
            com.legend.protocol.spec.ValueSpecification resolved, ModelContext ctx,
            String runtimeFqn, boolean relationalRootForm,
            @com.legend.base.Nullable String explicitMappingFqn) {
        SpecCompiler specs = new SpecCompiler(ctx);
        java.util.List<TypedSpec> body = specs.typeQueryBody(resolved);
        body = new com.legend.compiler.spec.UserCallInliner(specs).inlineBody(body);
        boolean temporalRoot = com.legend.compiler.element.Temporal
                .anyTemporalGetAll(body, ctx);
        body = new com.legend.resolver.StoreResolver(ctx, specs)
                .resolve(body, runtimeFqn, explicitMappingFqn);
        CrossStoreGuard.check(body, ctx, runtimeFqn);
        if (relationalRootForm) {
            body = com.legend.resolver.RelationalRootForm.apply(body, ctx);
        }
        com.legend.lowering.Lowerer lw = new com.legend.lowering.Lowerer(
                t -> com.legend.compiler.element.ClassLayouts.layoutOf(ctx, t),
                f -> ctx.findClass(f).isPresent(), ctx.implementations());
        if (!temporalRoot) {
            lw = lw.withEngineExistsJoinForm();
        }
        return lw.lower(body);
    }



    /**
     * EAGER G — the compileAll mode: type-check every user function BODY
     * in the module UP FRONT (the default path compiles lazily at call
     * sites, so a function nobody calls never surfaces its type errors).
     * Failures come back as a wall map keyed by overload signature, never
     * thrown — corpus-wide diagnostics for the construct taxonomy. Bodiless
     * (native) overloads are skipped; an FQN whose whole overload set is
     * signature-broken walls under the plain FQN.
     */
    public static java.util.Map<String, String> compileAllBodies(ModelContext ctx) {
        SpecCompiler specs = new SpecCompiler(ctx);
        java.util.Map<String, String> walls = new java.util.LinkedHashMap<>();
        // the MODULE's own bodies: the boot layer's (the system metamodel's
        // functions, the prelude's lifted derived properties and constraints)
        // are compiled once per process and typed by the spec census
        // (SpecBodyCensusTest) — its failures are the census's rows, never a
        // user module's walls (PRELUDE_MODULE_HOMEWORK §9.4)
        java.util.Set<String> boot = new java.util.HashSet<>();
        for (com.legend.model.PackageableElement el : bootLayer().elements()) {
            if (el instanceof com.legend.model.FunctionDefinition) {
                boot.add(el.qualifiedName());
            }
        }
        for (String fqn : new java.util.TreeSet<>(ctx.functionFqns())) {
            if (boot.contains(fqn)) {
                continue;
            }
            java.util.List<com.legend.compiler.element.TypedFunction> overloads;
            try {
                overloads = ctx.findFunction(fqn);
            } catch (RuntimeException e) {
                walls.put(fqn, String.valueOf(e.getMessage()));
                continue;
            }
            for (com.legend.compiler.element.TypedFunction tf : overloads) {
                if (tf.body().isEmpty()) {
                    continue;
                }
                try {
                    specs.compile(tf);
                } catch (RuntimeException e) {
                    walls.put(tf.id().qualified(), String.valueOf(e.getMessage()));
                }
            }
        }
        return walls;
    }

}
