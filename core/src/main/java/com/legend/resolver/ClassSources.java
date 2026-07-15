package com.legend.resolver;

import com.legend.compiler.element.ModelContext;
import com.legend.compiler.element.TypedFunction;
import com.legend.compiler.element.type.ExprType;
import com.legend.compiler.element.type.Multiplicity;
import com.legend.compiler.element.type.Type;
import com.legend.compiler.spec.CompiledFunction;
import com.legend.compiler.spec.SpecCompiler;
import com.legend.compiler.spec.typed.TypedCBoolean;
import com.legend.compiler.spec.typed.TypedCDate;
import com.legend.compiler.spec.typed.TypedCDecimal;
import com.legend.compiler.spec.typed.TypedCFloat;
import com.legend.compiler.spec.typed.TypedCInteger;
import com.legend.compiler.spec.typed.TypedCString;
import com.legend.compiler.spec.typed.TypedCollection;
import com.legend.compiler.spec.typed.TypedEnumValue;
import com.legend.compiler.spec.typed.TypedFilter;
import com.legend.compiler.spec.typed.TypedGetAll;
import com.legend.compiler.spec.typed.TypedIf;
import com.legend.compiler.spec.typed.TypedLambda;
import com.legend.compiler.spec.typed.TypedMap;
import com.legend.compiler.spec.typed.TypedNativeCall;
import com.legend.compiler.spec.typed.TypedNewInstance;
import com.legend.compiler.spec.typed.TypedNewInstanceCast;
import com.legend.compiler.spec.typed.TypedPropertyAccess;
import com.legend.compiler.spec.typed.TypedSpec;
import com.legend.compiler.spec.typed.TypedVariable;
import com.legend.error.MappingResolutionException;
import com.legend.error.NotImplementedException;
import com.legend.model.MappingDefinition;
import com.legend.model.MappingInclude;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.UnaryOperator;
/**
 * Loads and memoizes {@link ClassSource}s: (mapping, class) &rarr; compiled
 * mapping body &rarr; split at the {@code map(row|^Class(...))} terminal.
 *
 * <p>Dispatch is by class within the active mapping (its own bindings, then
 * included mappings transitively). Multi-set-ID classes are loud until H5.
 * The body compiles through the shared {@link SpecCompiler} (every
 * synthesized body type-checks &mdash; the H1 census invariant), so a
 * terminal that is not a single-statement {@code map} over a
 * {@code ^Class(...)} lambda is a resolver-vs-normalizer CONTRACT violation
 * ({@code IllegalStateException}), not a user error.
 *
 * <p>Cycle policy (H3, per the plan): an association target already on the
 * {@code resolving} stack gets a SHALLOW instantiation; any other re-entry
 * throws with the cycle path printed. Never null-and-skip &mdash; V1's
 * silent-leak family. (In H2 loading never recurses; the guard is
 * structural insurance.)
 */
public final class ClassSources {

    private final ModelContext ctx;
    private final SpecCompiler specs;
    private final Map<String, ClassSource> memo = new LinkedHashMap<>();
    private final LinkedHashSet<String> resolving = new LinkedHashSet<>();

    public ClassSources(ModelContext ctx, SpecCompiler specs) {
        this.ctx = Objects.requireNonNull(ctx, "ctx");
        this.specs = Objects.requireNonNull(specs, "specs");
    }

    /** The memoized extraction for {@code classFqn} under {@code mappingFqn}. */
    public ClassSource get(String mappingFqn, String classFqn) {
        return get(mappingFqn, classFqn, null, "");
    }

    /**
     * {@code upstreamMapping} dispatches a MODEL-TO-MODEL source class to
     * the mapping that binds it (the runtime's candidate set) when this
     * mapping doesn't — the corpus lists relational base mappings and M2M
     * mappings side by side in one runtime. {@code null} restricts
     * resolution to this mapping (+ includes).
     */
    public ClassSource get(String mappingFqn, String classFqn,
            UnaryOperator<String> upstreamMapping,
            String contextKey) {
        // The context key participates in memoization because an M2M
        // composition resolves its UPSTREAM through the runtime dispatch —
        // the same mapping::class composed under different runtimes reads
        // different stores (audit F1: memo poisoning was silent wrong data).
        String key = mappingFqn + '\u0000' + classFqn + '\u0000' + contextKey;
        ClassSource cached = memo.get(key);
        if (cached != null) {
            return cached;
        }
        if (!resolving.add(key)) {
            throw new IllegalStateException("resolver bug: class-source cycle "
                    + String.join(" -> ", resolving) + " -> " + key
                    + " (association targets mid-cycle must take the SHALLOW path)");
        }
        try {
            ClassSource built = build(mappingFqn, classFqn, upstreamMapping, contextKey);
            memo.put(key, built);
            return built;
        } finally {
            resolving.remove(key);
        }
    }

    private ClassSource build(String mappingFqn, String classFqn,
            UnaryOperator<String> upstreamMapping,
            String contextKey) {
        MappingDefinition mapping = ctx.findMapping(mappingFqn).orElseThrow(() ->
                new MappingResolutionException(
                        "unknown mapping '" + mappingFqn + "'", mappingFqn));
        MappingDefinition.ClassBinding binding = findBinding(mapping, classFqn,
                new LinkedHashSet<>());
        if (binding == null) {
            throw new MappingResolutionException("class '" + classFqn
                    + "' is not mapped in mapping '" + mappingFqn + "'"
                    + ctx.mappingPoison(mappingFqn, classFqn)
                            .map(r -> " (" + r + ")").orElse(""), classFqn);
        }

        List<TypedFunction> fns = ctx.findFunction(binding.functionFqn());
        if (fns.size() != 1) {
            throw new IllegalStateException("resolver bug: realizing function '"
                    + binding.functionFqn() + "' for class '" + classFqn
                    + "' has " + fns.size() + " overloads; synthesized FQNs are unique");
        }
        CompiledFunction cf = specs.compile(fns.get(0));

        // The terminal contract: a single-statement body whose last statement
        // is map(pipeline, row | ^Class(...)). Anything else is a normalizer
        // contract violation — the H1 census guarantees these bodies compile,
        // and the normalizer emits exactly this shape.
        TypedSpec last = cf.body().get(cf.body().size() - 1);
        if (!(last instanceof TypedMap map)) {
            throw new IllegalStateException("resolver bug: mapping body terminal for '"
                    + classFqn + "' in '" + mappingFqn + "' is "
                    + last.getClass().getSimpleName() + ", expected TypedMap"
                    + " (normalizer contract: pipeline -> map(row|^Class(...)))");
        }
        TypedLambda mapper = map.mapper();
        TypedSpec mapperBody = mapper.body().get(mapper.body().size() - 1);
        if (!(mapperBody instanceof TypedNewInstance ctor)
                || mapper.parameters().size() != 1) {
            throw new IllegalStateException("resolver bug: map terminal for '"
                    + classFqn + "' in '" + mappingFqn + "' is not a 1-param"
                    + " ^Class(...) constructor lambda: "
                    + mapperBody.getClass().getSimpleName());
        }

        TypedSpec pipeline = map.source();
        if (!(pipeline.info().type() instanceof Type.RelationType rowType)) {
            // A CLASS-typed pipeline is a MODEL-TO-MODEL mapping: the body is
            // getAll(Upstream)->map(src|^Target(...)). Composition is pure
            // β-transitivity (plan H5): resolve the UPSTREAM class through
            // this same mapping (memo + cycle guard ride along), then
            // substitute every $src.prop read with the upstream's binding —
            // the composed table sits over the upstream's own pipeline.
            if (pipeline.info().type() instanceof Type.ClassType src) {
                return composeModelToModel(mappingFqn, classFqn, binding,
                        pipeline, mapper, ctor, src, upstreamMapping, contextKey);
            }
            throw new IllegalStateException("resolver bug: mapping pipeline for '"
                    + classFqn + "' in '" + mappingFqn + "' types as "
                    + pipeline.info().type().typeName() + ", expected a relation row");
        }

        // Binding-table conformance: every ^Class key is a declared property.
        // (Full type/multiplicity conformance is G's guarantee — the body
        // compiled through NewChecker's strict subsumption. This assert
        // catches property-set drift loudly at the extraction seam.)
        Map<String, TypedSpec> bindings = new LinkedHashMap<>();
        for (Map.Entry<String, TypedSpec> e : ctor.properties().entrySet()) {
            // NewChecker is the construction gate: a ctor key that is not a
            // class property can ONLY be a validated mapping-LOCAL property
            // (+id: Integer[1]: COL — owned by the mapping); it binds like
            // any other (XStore predicates read locals through bindings)
            bindings.put(e.getKey(), e.getValue());
        }

        // A ~func Relation pipeline may ITSELF be a class query
        // (PersonWithFirmId.all()->filter->project — the relation-family
        // MixedMapping): resolve it recursively with a FRESH resolver
        // instance (own per-resolution state — never the caller's frame)
        // against this same mapping. Self-referential ~funcs would recurse
        // across instances — no corpus shape does; a cycle dies by stack,
        // loudly, not silently.
        if (containsGetAll(pipeline)) {
            var nested = new StoreResolver(ctx, specs)
                    .resolve(java.util.List.of(pipeline), null, mappingFqn);
            pipeline = nested.get(0);
        }

        return new ClassSource(mappingFqn, classFqn, binding.setId(),
                pipeline, mapper.parameters().get(0), bindings, rowType);
    }

    private static boolean containsGetAll(TypedSpec n) {
        if (n instanceof com.legend.compiler.spec.typed.TypedGetAll) {
            return true;
        }
        if (n instanceof com.legend.compiler.spec.typed.TypedNavigate nav) {
            // a navigate SLOT's target is getAll-shaped BY CONVENTION (the
            // legacyNavigate emission) — only pipeline-FLOW getAlls demand
            // the recursive resolution
            return containsGetAll(nav.source());
        }
        for (TypedSpec c : n.children()) {
            if (containsGetAll(c)) {
                return true;
            }
        }
        return false;
    }

    /**
     * MODEL-TO-MODEL composition (plan H5, scalar slice): the target's
     * binding table substitutes through the upstream class's — a binding
     * {@code fullName: $src.firstName + ' ' + $src.lastName} composes to
     * the upstream's own row expressions, so the result is an ordinary
     * relation-backed {@link ClassSource} and NOTHING downstream knows M2M
     * existed. Association navigation and whole-instance uses of the
     * source are the class-typed slice (H5b) — loud.
     */
    private ClassSource composeModelToModel(String mappingFqn, String classFqn,
            MappingDefinition.ClassBinding binding, TypedSpec pipeline,
            TypedLambda mapper, TypedNewInstance ctor, Type.ClassType srcType,
            UnaryOperator<String> upstreamMapping,
            String contextKey) {
        // Ops between the extent and the constructor: instance-space
        // FILTERS compose (their predicates substitute through the
        // upstream bindings like everything else); other ops are loud.
        List<TypedFilter> filters = new ArrayList<>();
        TypedSpec cur = pipeline;
        while (!(cur instanceof TypedGetAll)) {
            if (cur instanceof TypedFilter f) {
                filters.add(f);
                cur = f.source();
                continue;
            }
            throw new NotImplementedException("model-to-model pipeline of '"
                    + classFqn + "' in '" + mappingFqn + "' carries a "
                    + cur.getClass().getSimpleName() + " between the source"
                    + " extent and the constructor — not supported yet (H5c)");
        }
        // The upstream class resolves in THIS mapping when bound here (or
        // via includes); otherwise through the runtime dispatch — corpus
        // runtimes list the relational base and the M2M layers side by side.
        String srcMapping = binds(mappingFqn, srcType.fqn()) || upstreamMapping == null
                ? mappingFqn
                : upstreamMapping.apply(srcType.fqn());
        ClassSource inner = get(srcMapping, srcType.fqn(), upstreamMapping, contextKey);
        String srcVar = mapper.parameters().get(0);
        TypedSpec composedPipeline = inner.pipeline();
        for (int i = filters.size() - 1; i >= 0; i--) {
            TypedLambda lam = filters.get(i).predicate();
            String v = lam.parameters().get(0);
            List<TypedSpec> body = lam.body().stream().map(b ->
                    substituteSourceReads(b, v, inner, classFqn, mappingFqn, false)).toList();
            var fnType = new Type.FunctionType(
                    List.of(new Type.Param(inner.rowType(),
                            Multiplicity.Bounded.ONE)),
                    new Type.Param(Type.Primitive.BOOLEAN,
                            Multiplicity.Bounded.ONE));
            composedPipeline = new TypedFilter(
                    composedPipeline,
                    new TypedLambda(List.of(inner.rowVar()), body,
                            new ExprType(fnType,
                                    Multiplicity.Bounded.ONE)),
                    composedPipeline.info());
        }
        Map<String, TypedSpec> composed = new LinkedHashMap<>();
        for (Map.Entry<String, TypedSpec> e : ctor.properties().entrySet()) {
            if (ctx.findProperty(classFqn, e.getKey()).isEmpty()) {
                throw new IllegalStateException("resolver bug: mapping binding '"
                        + e.getKey() + "' is not a property of class '" + classFqn
                        + "' (G should have rejected the body)");
            }
            composed.put(e.getKey(), substituteSourceReads(e.getValue(), srcVar,
                    inner, classFqn, mappingFqn));
        }
        return new ClassSource(mappingFqn, classFqn, binding.setId(),
                composedPipeline, inner.rowVar(), composed, inner.rowType());
    }

    /**
     * β-substitute {@code $src.prop} reads with the upstream's bindings.
     * Closed vocabulary with a LOUD default — a node this rewriter does not
     * know is a normalizer contract change, never silent.
     */
    private TypedSpec substituteSourceReads(TypedSpec n, String srcVar,
            ClassSource inner, String classFqn, String mappingFqn) {
        return substituteSourceReads(n, srcVar, inner, classFqn, mappingFqn, true);
    }

    private TypedSpec substituteSourceReads(TypedSpec n, String srcVar,
            ClassSource inner, String classFqn, String mappingFqn,
            boolean bindingPosition) {
        if (n instanceof TypedPropertyAccess pa
                && pa.source() instanceof TypedVariable v
                && v.name().equals(srcVar)) {
            TypedSpec bound = inner.bindings().get(pa.property());
            if (bound == null) {
                // An ASSOCIATION property of the source class: in BINDING
                // position the read becomes a SOURCE-NAV MARKER — the access
                // re-pointed at the composed row var, source-class-typed —
                // consumed only by the GRAPH-CHILD path (address:
                // $src.rawAddresses fans out as a correlated child). A read
                // THROUGH the association ($src.boss.age) stays loud: it
                // would need a scalar join this composition cannot emit.
                if (bindingPosition
                        && ctx.findAssociationOf(inner.classFqn(), pa.property()).isPresent()) {
                    return new TypedPropertyAccess(
                            new TypedVariable(
                                    inner.rowVar(),
                                    ExprType.one(
                                            new Type
                                                    .ClassType(inner.classFqn()))),
                            pa.property(), pa.info());
                }
                throw new NotImplementedException("model-to-model binding of '"
                        + classFqn + "' in '" + mappingFqn + "' navigates '$"
                        + srcVar + "." + pa.property() + "' — an unmapped"
                        + " non-association property of source class '"
                        + inner.classFqn() + "' is not supported yet (H5b)");
            }
            return bound;
        }
        return switch (n) {
            case TypedVariable v
                    when v.name().equals(srcVar) ->
                    throw new NotImplementedException("model-to-model binding of '"
                            + classFqn + "' uses the whole source instance '$"
                            + srcVar + "' — not supported yet (H5b)");
            case TypedVariable v -> v;
            // ^Target($src.prop): the M2M CAST — substitute within its
            // source; the cast survives as a CLASS-TYPED binding (read
            // sites give it graph-child / H4 stories, never silent SQL).
            case TypedNewInstanceCast nic ->
                    new TypedNewInstanceCast(
                            nic.classFqn(),
                            substituteSourceReads(nic.source(), srcVar, inner,
                                    classFqn, mappingFqn, bindingPosition),
                            nic.info());
            case TypedPropertyAccess pa ->
                    new TypedPropertyAccess(
                            substituteSourceReads(pa.source(), srcVar, inner,
                                    classFqn, mappingFqn, false),
                            pa.property(), pa.info());
            case TypedNativeCall c ->
                    new TypedNativeCall(c.callee(),
                            c.args().stream().map(a -> substituteSourceReads(a,
                                    srcVar, inner, classFqn, mappingFqn, false)).toList(),
                            c.info());
            case TypedCollection c ->
                    new TypedCollection(
                            c.elements().stream().map(a -> substituteSourceReads(a,
                                    srcVar, inner, classFqn, mappingFqn, false)).toList(),
                            c.info());
            case TypedIf i ->
                    new TypedIf(
                            substituteSourceReads(i.condition(), srcVar, inner,
                                    classFqn, mappingFqn, false),
                            substituteSourceReads(i.thenBranch(), srcVar, inner,
                                    classFqn, mappingFqn, false),
                            i.elseBranch().map(e2 -> substituteSourceReads(e2,
                                    srcVar, inner, classFqn, mappingFqn, false)),
                            i.info());
            case TypedLambda l -> {
                if (l.parameters().contains(srcVar)) {
                    yield l;   // shadowing: substitution stops (capture rule)
                }
                // CAPTURE guard (audit F2): a lambda parameter named like
                // the UPSTREAM row var would capture the substituted
                // binding's row reads — loud, never silently mis-scoped.
                if (l.parameters().contains(inner.rowVar())
                        && readsVar(l, srcVar)) {
                    throw new NotImplementedException("model-to-model binding of '"
                            + classFqn + "' in '" + mappingFqn + "' has a lambda"
                            + " parameter named '" + inner.rowVar() + "' shadowing"
                            + " the upstream mapping's row variable — rename the"
                            + " parameter");
                }
                yield new TypedLambda(l.parameters(),
                        l.body().stream().map(b -> substituteSourceReads(b,
                                srcVar, inner, classFqn, mappingFqn, false)).toList(),
                        l.info());
            }
            case TypedCString ignored -> n;
            case TypedCInteger ignored -> n;
            case TypedCFloat ignored -> n;
            case TypedCDecimal ignored -> n;
            case TypedCBoolean ignored -> n;
            case TypedCDate ignored -> n;
            case TypedEnumValue ignored -> n;
            default -> throw new NotImplementedException(
                    "model-to-model binding node "
                            + n.getClass().getSimpleName()
                            + " is not substitutable yet (H5 vocabulary)");
        };
    }

    /** Whether any {@code $var} read occurs in {@code n}'s subtree. */
    private static boolean readsVar(TypedSpec n, String var) {
        if (n instanceof TypedVariable v
                && v.name().equals(var)) {
            return true;
        }
        for (TypedSpec c : n.children()) {
            if (readsVar(c, var)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Whether {@code mappingFqn} (or its includes) binds {@code classFqn} —
     * the runtime-dispatch probe: a multi-mapping runtime picks the ONE
     * candidate that binds the fetched class. Never throws (a multi-set-ID
     * binding still counts as "binds"; the loud path is {@link #get}).
     */
    public boolean binds(String mappingFqn, String classFqn) {
        return ctx.findMapping(mappingFqn)
                .map(m -> bindsIn(m, classFqn, new LinkedHashSet<>()))
                .orElse(false);
    }

    private boolean bindsIn(MappingDefinition mapping, String classFqn,
                            LinkedHashSet<String> visited) {
        if (!visited.add(mapping.qualifiedName())) {
            return false;
        }
        for (MappingDefinition.ClassBinding cb : mapping.classBindings()) {
            if (cb.classFqn().equals(classFqn)) {
                return true;
            }
        }
        for (MappingInclude inc : mapping.includes()) {
            MappingDefinition inner = ctx.findMapping(inc.mappingPath()).orElseThrow(() ->
                    new MappingResolutionException("mapping '" + mapping.qualifiedName()
                            + "' includes unknown mapping '" + inc.mappingPath()
                            + "' (a silently-unresolved include hid class bindings)"));
            if (bindsIn(inner, classFqn, visited)) {
                return true;
            }
        }
        return false;
    }

    /**
     * The class binding within {@code mapping} or its includes: hits are
     * collected across the WHOLE include closure — a class bound by two
     * included mappings is a loud ambiguity (real Legend errors on
     * duplicate class mappings), never a silent depth-first pick. A local
     * binding shadows included ones (checked first, include semantics).
     * Multi-set-ID within one mapping is a legal-but-unbuilt feature (H5).
     */
    private MappingDefinition.ClassBinding findBinding(MappingDefinition mapping,
                                                       String classFqn,
                                                       LinkedHashSet<String> visited) {
        List<MappingDefinition.ClassBinding> local = new ArrayList<>();
        for (MappingDefinition.ClassBinding cb : mapping.classBindings()) {
            if (cb.classFqn().equals(classFqn)) {
                local.add(cb);
            }
        }
        if (local.size() > 1) {
            throw new NotImplementedException("class '" + classFqn
                    + "' has " + local.size() + " set-id bindings in mapping '"
                    + mapping.qualifiedName()
                    + "'; multi-set-ID dispatch is not supported yet (H5)");
        }
        if (local.size() == 1) {
            return local.get(0);   // local shadows included
        }
        visited.add(mapping.qualifiedName());
        List<MappingDefinition.ClassBinding> included = new ArrayList<>();
        List<String> sources = new ArrayList<>();
        for (MappingInclude inc : mapping.includes()) {
            if (visited.contains(inc.mappingPath())) {
                continue;
            }
            MappingDefinition inner = ctx.findMapping(inc.mappingPath()).orElseThrow(() ->
                    new MappingResolutionException("mapping '" + mapping.qualifiedName()
                            + "' includes unknown mapping '" + inc.mappingPath() + "'"));
            MappingDefinition.ClassBinding found = findBinding(inner, classFqn, visited);
            if (found != null) {
                included.add(found);
                sources.add(inc.mappingPath());
            }
        }
        if (included.size() > 1) {
            throw new MappingResolutionException("class '" + classFqn
                    + "' is ambiguously mapped in '" + mapping.qualifiedName()
                    + "' via includes " + sources, classFqn);
        }
        return included.isEmpty() ? null : included.get(0);
    }
}