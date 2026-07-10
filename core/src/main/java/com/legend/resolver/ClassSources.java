package com.legend.resolver;

import com.legend.compiler.element.ModelContext;
import com.legend.compiler.element.TypedFunction;
import com.legend.compiler.element.type.Type;
import com.legend.compiler.spec.CompiledFunction;
import com.legend.compiler.spec.SpecCompiler;
import com.legend.compiler.spec.typed.TypedLambda;
import com.legend.compiler.spec.typed.TypedMap;
import com.legend.compiler.spec.typed.TypedNewInstance;
import com.legend.compiler.spec.typed.TypedSpec;
import com.legend.error.MappingResolutionException;
import com.legend.parser.element.MappingDefinition;
import com.legend.parser.element.MappingInclude;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;

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
        String key = mappingFqn + '\u0000' + classFqn;
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
            ClassSource built = build(mappingFqn, classFqn);
            memo.put(key, built);
            return built;
        } finally {
            resolving.remove(key);
        }
    }

    private ClassSource build(String mappingFqn, String classFqn) {
        MappingDefinition mapping = ctx.findMapping(mappingFqn).orElseThrow(() ->
                new MappingResolutionException(
                        "unknown mapping '" + mappingFqn + "'", mappingFqn));
        MappingDefinition.ClassBinding binding = findBinding(mapping, classFqn,
                new LinkedHashSet<>());
        if (binding == null) {
            throw new MappingResolutionException("class '" + classFqn
                    + "' is not mapped in mapping '" + mappingFqn + "'", classFqn);
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
            if (ctx.findProperty(classFqn, e.getKey()).isEmpty()) {
                throw new IllegalStateException("resolver bug: mapping binding '"
                        + e.getKey() + "' is not a property of class '" + classFqn
                        + "' (G should have rejected the body)");
            }
            bindings.put(e.getKey(), e.getValue());
        }

        return new ClassSource(mappingFqn, classFqn, binding.setId(),
                pipeline, mapper.parameters().get(0), bindings, rowType);
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
            if (ctx.findMapping(inc.mappingPath())
                    .map(m -> bindsIn(m, classFqn, visited)).orElse(false)) {
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
            throw new com.legend.error.NotImplementedException("class '" + classFqn
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