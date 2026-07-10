package com.legend.resolver;

import com.legend.compiler.element.ModelContext;
import com.legend.compiler.spec.SpecCompiler;
import com.legend.compiler.spec.typed.TypedDistinct;
import com.legend.compiler.spec.typed.TypedDrop;
import com.legend.compiler.spec.typed.TypedFilter;
import com.legend.compiler.spec.typed.TypedFrom;
import com.legend.compiler.spec.typed.TypedFuncCol;
import com.legend.compiler.spec.typed.TypedGetAll;
import com.legend.compiler.spec.typed.TypedLambda;
import com.legend.compiler.spec.typed.TypedLimit;
import com.legend.compiler.spec.typed.TypedProject;
import com.legend.compiler.spec.typed.TypedSlice;
import com.legend.compiler.spec.typed.TypedSort;
import com.legend.compiler.spec.typed.TypedSpec;
import com.legend.error.MappingResolutionException;
import com.legend.error.NotImplementedException;
import com.legend.parser.element.RuntimeDefinition;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/**
 * Phase H &mdash; the pure {@code TypedSpec -> TypedSpec} rewriter replacing
 * object-space class queries with relation pipelines resolved against the
 * active mapping (contract: {@link com.legend.resolver} package doc; design:
 * {@code docs/PHASE_H2_H3_RESOLVER_PLAN.md}).
 *
 * <p>H2 scope: {@code getAll -> [filter|limit|take|slice|drop]* -> project}
 * chains over a single class. The projection boundary exits object space
 * with its {@code info} UNCHANGED, so everything downstream (relation
 * space) passes through untouched. Unsupported object-space constructs are
 * loud, naming the construct and the owning phase.
 */
public final class StoreResolver {

    private final ClassSources sources;
    private int freshVarCounter;

    public StoreResolver(ModelContext ctx, SpecCompiler specs) {
        this.sources = new ClassSources(ctx, specs);
        this.ctx = Objects.requireNonNull(ctx, "ctx");
    }

    private final ModelContext ctx;

    /** Resolve every statement of a query body (lets + final expression). */
    public List<TypedSpec> resolve(List<TypedSpec> body) {
        return resolve(body, null);
    }

    /**
     * Resolve with a DRIVER-SUPPLIED execution context (the corpus shape:
     * queries carry no {@code ->from(...)}; the runtime arrives via the
     * service API). Precedence per the plan: an explicit {@code from()} in
     * the query always wins; the driver runtime is the outermost fallback.
     */
    public List<TypedSpec> resolve(List<TypedSpec> body, String driverRuntimeFqn) {
        // LAZY: the runtime is consulted only when a class fetch needs a
        // mapping — a pure relation query with an unusable runtime must not
        // fail (the corpus's date-literal regression).
        Context context = driverRuntimeFqn == null ? Context.NONE
                : Context.ofRuntime(driverRuntimeFqn);
        List<TypedSpec> out = new ArrayList<>(body.size());
        for (TypedSpec stmt : body) {
            out.add(resolveNode(stmt, context));
        }
        return out;
    }

    /**
     * The execution context: an explicit mapping, or a runtime whose
     * candidate mappings are dispatched PER FETCHED CLASS — the candidate
     * that binds the class wins; zero or several binders is loud (plan
     * audit catch 1's precedence rule).
     */
    private record Context(String explicitMapping, String runtimeFqn) {
        static final Context NONE = new Context(null, null);
        static Context ofMapping(String fqn) { return new Context(fqn, null); }
        static Context ofRuntime(String fqn) { return new Context(null, fqn); }
        boolean isNone() { return explicitMapping == null && runtimeFqn == null; }
    }

    // =====================================================================
    // The context walk
    // =====================================================================

    private TypedSpec resolveNode(TypedSpec n, Context context) {
        return switch (n) {
            case TypedFrom from -> {
                Context inner = from.mapping().map(m -> Context.ofMapping(m.fullPath()))
                        .orElseGet(() -> from.runtime()
                                .map(r -> Context.ofRuntime(r.fullPath()))
                                .orElse(context));
                yield new TypedFrom(resolveNode(from.source(), inner),
                        from.mapping(), from.runtime(), from.info());
            }
            case TypedGetAll g -> throw new NotImplementedException(
                    "bare class fetch '" + g.classFqn() + ".all()' without a"
                            + " relation-shaping operation is graph output (Phase H4)");
            case TypedFilter f when isObjectSpace(f.source()) ->
                    resolveChain(f, context);
            case TypedProject p when isObjectSpace(p.source()) ->
                    resolveChain(p, context);
            case TypedLimit l when isObjectSpace(l.source()) ->
                    resolveChain(l, context);
            case TypedDrop d when isObjectSpace(d.source()) ->
                    resolveChain(d, context);
            case TypedSlice s when isObjectSpace(s.source()) ->
                    resolveChain(s, context);
            // Relation-space wrappers over a chain that bottoms at a getAll:
            // rebuild with the resolved source. (Each wrapper keeps its own
            // info — relation-space types are stable across resolution.)
            case TypedFilter f when containsGetAll(f.source()) -> new TypedFilter(
                    resolveNode(f.source(), context), f.predicate(), f.info());
            case TypedProject p when containsGetAll(p.source()) -> new TypedProject(
                    resolveNode(p.source(), context), p.columns(), p.info());
            case TypedSort s when containsGetAll(s.source()) -> new TypedSort(
                    resolveNode(s.source(), context), s.keys(), s.info());
            case TypedLimit l when containsGetAll(l.source()) -> new TypedLimit(
                    resolveNode(l.source(), context), l.count(), l.info());
            case TypedDrop d when containsGetAll(d.source()) -> new TypedDrop(
                    resolveNode(d.source(), context), d.count(), d.info());
            case TypedSlice s when containsGetAll(s.source()) -> new TypedSlice(
                    resolveNode(s.source(), context), s.start(), s.stop(), s.info());
            case TypedDistinct d when containsGetAll(d.source()) -> new TypedDistinct(
                    resolveNode(d.source(), context), d.columns(), d.info());
            default -> {
                if (containsGetAll(n)) {
                    throw new NotImplementedException("class query under "
                            + n.getClass().getSimpleName()
                            + " is not resolvable yet (H2 vocabulary)");
                }
                yield n;   // no class fetch anywhere beneath: pure relation query
            }
        };
    }

    /** An op whose source chain is still in OBJECT space (class-typed). */
    private static boolean isObjectSpace(TypedSpec source) {
        return switch (source) {
            case TypedGetAll ignored -> true;
            case TypedFilter f -> isObjectSpace(f.source());
            case TypedLimit l -> isObjectSpace(l.source());
            case TypedDrop d -> isObjectSpace(d.source());
            case TypedSlice s -> isObjectSpace(s.source());
            default -> false;
        };
    }

    private static boolean containsGetAll(TypedSpec n) {
        if (n instanceof TypedGetAll) {
            return true;
        }
        for (TypedSpec c : n.children()) {
            if (containsGetAll(c)) {
                return true;
            }
        }
        return false;
    }

    // =====================================================================
    // Object-space chain resolution (the H2 heart)
    // =====================================================================

    /** The per-chain carrier — never escapes (no sidecar). */
    private record ObjectRelation(ClassSource source, String rowVar,
                                  TypedSpec pipeline, Set<String> strippedSlots) {}

    private TypedSpec resolveChain(TypedSpec top, Context context) {
        if (context.isNone()) {
            throw new MappingResolutionException(
                    "class query requires an execution context: add"
                            + " ->from(mapping, runtime) or supply a runtime");
        }
        return resolveObject(top, context);
    }

    private TypedSpec loudObjectRoot(TypedSpec top) {
        throw new NotImplementedException("class-shaped result at the query root"
                + " (" + top.getClass().getSimpleName() + ") is graph output (Phase H4)");
    }

    /**
     * Resolve one object-space op. Ops below the projection boundary fold
     * onto the pipeline; {@code project} exits object space and returns the
     * finished relation node.
     */
    private TypedSpec resolveObject(TypedSpec n, Context context) {
        return switch (n) {
            case TypedProject p -> {
                ObjectRelation rel = objectRelation(p.source(), context);
                List<TypedFuncCol> cols = new ArrayList<>(p.columns().size());
                for (TypedFuncCol col : p.columns()) {
                    cols.add(new TypedFuncCol(col.name(),
                            substitution(rel, col.fn()).rewriteLambda(col.fn())));
                }
                // The projection boundary: info UNCHANGED — downstream
                // relation ops need zero rewriting.
                yield new TypedProject(rel.pipeline(), cols, p.info());
            }
            default -> loudObjectRoot(n);
        };
    }

    /** Fold the below-boundary ops (filter/limit/take/slice/drop) onto the pipeline. */
    private ObjectRelation objectRelation(TypedSpec n, Context context) {
        return switch (n) {
            case TypedGetAll g -> instantiate(g, context);
            case TypedFilter f -> {
                ObjectRelation rel = objectRelation(f.source(), context);
                TypedLambda pred = substitution(rel, f.predicate())
                        .rewriteLambda(f.predicate());
                yield new ObjectRelation(rel.source(), rel.rowVar(),
                        new TypedFilter(rel.pipeline(), pred, rel.pipeline().info()),
                        rel.strippedSlots());
            }
            case TypedLimit l -> {
                ObjectRelation rel = objectRelation(l.source(), context);
                yield new ObjectRelation(rel.source(), rel.rowVar(),
                        new TypedLimit(rel.pipeline(), l.count(), rel.pipeline().info()),
                        rel.strippedSlots());
            }
            case TypedDrop d -> {
                ObjectRelation rel = objectRelation(d.source(), context);
                yield new ObjectRelation(rel.source(), rel.rowVar(),
                        new TypedDrop(rel.pipeline(), d.count(), rel.pipeline().info()),
                        rel.strippedSlots());
            }
            case TypedSlice s -> {
                ObjectRelation rel = objectRelation(s.source(), context);
                yield new ObjectRelation(rel.source(), rel.rowVar(),
                        new TypedSlice(rel.pipeline(), s.start(), s.stop(),
                                rel.pipeline().info()),
                        rel.strippedSlots());
            }
            default -> throw new NotImplementedException("object-space operation "
                    + n.getClass().getSimpleName() + " is not supported yet");
        };
    }

    private ObjectRelation instantiate(TypedGetAll g, Context context) {
        if (!g.milestoning().isEmpty()) {
            throw new MappingResolutionException("milestoned class fetch of '"
                    + g.classFqn() + "' is not supported yet (H-scope exclusion)",
                    g.classFqn());
        }
        ClassSource cs = sources.get(dispatch(context, g.classFqn()), g.classFqn());
        Pipelines.Stripped stripped = Pipelines.stripSlots(cs.pipeline(), cs.classFqn());
        String fresh = "_r" + freshVarCounter++;
        return new ObjectRelation(cs, fresh, stripped.pipeline(), stripped.slotAliases());
    }

    private Substitution substitution(ObjectRelation rel, TypedLambda userLambda) {
        return new Substitution(new Substitution.Target(
                userLambda.parameters().get(0), rel.rowVar(),
                rel.source().classFqn(), rel.source().mappingFqn(),
                rel.source().rowVar(), rel.source().bindings(),
                rowTypeOf(rel), rel.strippedSlots()));
    }

    private static com.legend.compiler.element.type.Type.RelationType rowTypeOf(ObjectRelation rel) {
        return (com.legend.compiler.element.type.Type.RelationType) rel.pipeline().info().type();
    }

    /** Per-class dispatch: the runtime candidate that BINDS the class wins. */
    private String dispatch(Context context, String classFqn) {
        if (context.explicitMapping() != null) {
            return context.explicitMapping();
        }
        RuntimeDefinition rt = ctx.findRuntime(context.runtimeFqn()).orElseThrow(() ->
                new MappingResolutionException("unknown runtime '"
                        + context.runtimeFqn() + "'", context.runtimeFqn()));
        List<String> binders = rt.mappings().stream()
                .filter(m -> sources.binds(m, classFqn))
                .toList();
        if (binders.size() != 1) {
            throw new MappingResolutionException("runtime '" + context.runtimeFqn()
                    + "' has " + binders.size() + " mappings binding class '"
                    + classFqn + "' (of " + rt.mappings().size()
                    + " candidates); class-query dispatch needs exactly one",
                    classFqn);
        }
        return binders.get(0);
    }
}
