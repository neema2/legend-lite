package com.legend.compiler.spec;

import com.legend.compiler.element.type.PlatformTypes;
import com.legend.compiler.spec.typed.ExecutionContext;
import com.legend.compiler.spec.typed.TypedFrom;
import com.legend.compiler.spec.typed.TypedPackageableRef;
import com.legend.compiler.spec.typed.TypedSpec;
import com.legend.protocol.spec.AppliedFunction;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * {@code from} (engine {@code FromChecker}) &mdash; binds a value to an execution
 * context. Fully generic type-wise (a passthrough: {@code from<T>(Relation<T>[1]
 * [, runtime:Any[1]])} and the M2M {@code from<T>(T[*], mapping, runtime)});
 * every non-source argument must be a packageable-element reference (mapping /
 * runtime), slotted onto the node for the back-end:
 * {@code from(src)} &rarr; (&mdash;, &mdash;); {@code from(src, runtime)} &rarr;
 * (&mdash;, runtime); {@code from(src, mapping, runtime)} &rarr; (mapping, runtime).
 */
final class FromChecker {

    private FromChecker() {
    }

    static TypedSpec check(Typer t, AppliedFunction af, Env env) {
        // bind-once (family D): from()'s non-source arguments are METADATA
        // references consumed by NODE KIND, and a let-bound ref reaches
        // here as a variable whose node identity is lost. Resolve the
        // let-alias channel first so the reference node survives typing
        // (engine parallel: use-site inScopeVars resolution). ADOPT only
        // a reference — a let whose rhs is a helper CALL (instance-built
        // runtimes/mappings) keeps its variable so the walk's own splice
        // channels behave exactly as before.
        if (af.parameters().size() > 1) {
            List<com.legend.protocol.spec.ValueSpecification> ps =
                    new ArrayList<>(af.parameters());
            boolean changed = false;
            for (int i = 1; i < ps.size(); i++) {
                com.legend.protocol.spec.ValueSpecification r =
                        env.resolveAlias(ps.get(i));
                if (r != ps.get(i) && r instanceof com.legend.protocol.spec
                        .PackageableElementPtr) {
                    ps.set(i, r);
                    changed = true;
                }
            }
            if (changed) {
                af = af.withParameters(ps);
            }
        }
        Application a = t.checkGeneric(af, env);
        List<TypedPackageableRef> refs = new ArrayList<>(a.args().size() - 1);
        // the runtime VALUE argument (a constructed instance, a helper call,
        // a let-bound variable): the special form's rule reads the execution
        // context off it ONCE (ExecutionContext.Reader) — the instance's
        // connection content is otherwise harness-ambient (the module
        // runtime supplies the connections), so the runtime SLOT stays empty
        // and the mapping ref alone names the context
        ExecutionContext bound = ExecutionContext.NONE;
        for (int i = 1; i < a.args().size(); i++) {
            if (a.args().get(i) instanceof TypedPackageableRef ref) {
                refs.add(ref);
                continue;
            }
            if (a.args().get(i).info().type()
                    instanceof com.legend.compiler.element.type.Type
                            .ClassType ct
                    && (ct.fqn().equals(PlatformTypes.RUNTIME)
                            || t.model().isSubtype(ct.fqn(), PlatformTypes.RUNTIME))) {
                // a LET-BOUND runtime ($runtime = getModelChainRuntime($m) /
                // ^EngineRuntime(...)): the let's rhs TYPES here through
                // the alias channel and IS the value read
                TypedSpec value = a.args().get(i);
                if (value instanceof com.legend.compiler.spec.typed.TypedVariable
                        && env.resolveAlias(af.parameters().get(i))
                                instanceof com.legend.protocol.spec.ValueSpecification raw
                        && !(raw instanceof com.legend.protocol.spec.Variable)) {
                    value = t.synth(raw, env);
                }
                bound = ExecutionContext.reader()
                        .fnBody(fq -> t.model().findFunction(fq).stream()
                                .map(com.legend.compiler.element.TypedFunction::body)
                                .filter(java.util.Optional::isPresent)
                                .map(java.util.Optional::get)
                                .findFirst())
                        .canon(t::classFqnOf)
                        .dbOfCopy(cp -> {
                            var store = connectionStoreOf(t, env, cp.source());
                            return store != null && store.properties().get("element")
                                    instanceof TypedPackageableRef el ? el.fullPath() : null;
                        })
                        .read(Optional.empty(), value);
                continue;
            }
            throw new TypeInferenceException("from() argument " + i
                    + " must be a mapping or runtime reference, got "
                    + a.args().get(i).getClass().getSimpleName());
        }
        // slotting by KIND when instance-runtimes dropped a ref: a sole
        // surviving ref that is a MAPPING slots as the mapping (the
        // 3-arg from(src, mapping, <instance runtime>) shape)
        boolean soleIsMapping = refs.size() == 1
                && a.args().size() >= 3;
        Optional<TypedPackageableRef> mapping =
                refs.size() >= 2 || soleIsMapping
                        ? Optional.of(refs.get(0)) : Optional.empty();
        Optional<TypedPackageableRef> runtime = switch (refs.size()) {
            case 0 -> Optional.empty();
            case 1 -> soleIsMapping ? Optional.empty()
                    : Optional.of(refs.get(0));
            default -> Optional.of(refs.get(1));
        };
        // QUERY-SIDE chain channel (engine withChainedMappings_T_m__
        // Mapping_MANY__T_m_): source->withChainedMappings([...])->from(rt)
        // — identity on the stream; its mapping refs join chainMappings
        // and the node strips (same channel as ModelChainConnection)
        TypedSpec src = a.args().get(0);
        if (src instanceof com.legend.compiler.spec.typed.TypedNativeCall wc
                && com.legend.builtin.NativeFn.ResolverForm.of(wc.callee().id()).orElse(null) == com.legend.builtin.NativeFn.ResolverForm.WITH_CHAINED_MAPPINGS
                && wc.args().size() == 2) {
            List<String> queryChain = new ArrayList<>();
            collectMappingRefs(wc.args().get(1), queryChain);
            bound = bound.plusChain(queryChain);
            src = wc.args().get(0);
        }
        // withMapping (real mappingExtension.pure:386 — the from()
        // sibling routing marker): source->withMapping(M)[->cast(@..)]
        // ->from(runtime) IS from(source[->cast], M, runtime) — identity
        // on the stream, M slots as THE mapping, the marker strips (the
        // withChainedMappings idiom above; witnesses
        // testFromWithMapping{,AndIntermediateFuncCall}). An explicit
        // from-mapping wins over the marker (no silent override).
        TypedPackageableRef[] wmRef = new TypedPackageableRef[1];
        src = stripWithMapping(src, wmRef);
        if (wmRef[0] != null && mapping.isEmpty()) {
            mapping = Optional.of(wmRef[0]);
        }
        return new TypedFrom(src, bound.withMapping(mapping).withRuntime(runtime), a.out());
    }

    /** Strip a {@code withMapping(M)} marker off the from-source spine,
     * seeing through casts (the intermediate-call witness); the found
     * mapping ref lands in {@code found[0]}. Any other shape passes
     * through untouched — unrecognized spellings stay loud downstream. */
    private static TypedSpec stripWithMapping(TypedSpec n,
            TypedPackageableRef[] found) {
        if (n instanceof com.legend.compiler.spec.typed.TypedNativeCall wm
                && com.legend.builtin.NativeFn.ResolverForm.of(wm.callee().id()).orElse(null) == com.legend.builtin.NativeFn.ResolverForm.WITH_MAPPING
                && wm.args().size() == 2
                && wm.args().get(1) instanceof TypedPackageableRef mref) {
            found[0] = mref;
            return wm.args().get(0);
        }
        if (n instanceof com.legend.compiler.spec.typed.TypedCast c) {
            TypedSpec inner = stripWithMapping(c.source(), found);
            return inner == c.source() ? n
                    : c.withChildren(List.of(inner));
        }
        return n;
    }

    private static void collectMappingRefs(TypedSpec n,
            List<String> out) {
        if (n instanceof TypedPackageableRef r) {
            out.add(r.fullPath());
            return;
        }
        for (TypedSpec c : n.children()) {
            collectMappingRefs(c, out);
        }
    }

    /** The {@code ^ConnectionStore(element=…, connection=…)} instance
     * whose {@code .connection} the expression denotes — structural
     * navigation over constructed values: {@code $runtime.connectionStores
     * ->at(0).connection->cast(@…)} through lets and zero-arg helpers
     * ({@code testRuntime()}). Null when the expression is not that shape. */
    private static com.legend.compiler.spec.typed.@com.legend.base.Nullable TypedNewInstance
            connectionStoreOf(Typer t, Env env, TypedSpec e) {
        TypedSpec cur = e;
        while (cur instanceof com.legend.compiler.spec.typed.TypedCast c) {
            cur = c.source();
        }
        if (cur instanceof com.legend.compiler.spec.typed.TypedVariable v) {
            com.legend.protocol.spec.ValueSpecification raw =
                    env.resolveAlias(new com.legend.protocol.spec.Variable(v.name()));
            return raw instanceof com.legend.protocol.spec.Variable ? null
                    : connectionStoreOf(t, env, t.synth(raw, env));
        }
        if (cur instanceof com.legend.compiler.spec.typed.TypedPropertyAccess pa
                && pa.property().equals("connection")) {
            TypedSpec store = instanceOf(t, env, pa.source());
            return store instanceof com.legend.compiler.spec.typed.TypedNewInstance ni
                    ? ni : null;
        }
        return null;
    }

    /** The constructed instance an expression denotes: {@code ^X(...)}
     * itself, {@code coll->at(k)} of a literal collection, {@code inst.prop},
     * a let-bound variable, a zero-arg helper's value. */
    private static @com.legend.base.Nullable TypedSpec instanceOf(Typer t, Env env,
            TypedSpec e) {
        TypedSpec cur = e;
        while (cur instanceof com.legend.compiler.spec.typed.TypedCast c) {
            cur = c.source();
        }
        return switch (cur) {
            case com.legend.compiler.spec.typed.TypedNewInstance ni -> ni;
            case com.legend.compiler.spec.typed.TypedCollection tc
                    when tc.elements().size() == 1 -> instanceOf(t, env, tc.elements().get(0));
            case com.legend.compiler.spec.typed.TypedVariable v -> {
                com.legend.protocol.spec.ValueSpecification raw =
                        env.resolveAlias(new com.legend.protocol.spec.Variable(v.name()));
                yield raw instanceof com.legend.protocol.spec.Variable ? null
                        : instanceOf(t, env, t.synth(raw, env));
            }
            case com.legend.compiler.spec.typed.TypedPropertyAccess pa -> {
                TypedSpec src = instanceOf(t, env, pa.source());
                TypedSpec pv = src instanceof com.legend.compiler.spec.typed.TypedNewInstance ni
                        ? ni.properties().get(pa.property()) : null;
                yield pv == null ? null : instanceOf(t, env, pv);
            }
            case com.legend.compiler.spec.typed.TypedNativeCall nc
                    when !nc.args().isEmpty()
                    && (ResultEnvelopeSplice.AT_FQN.equals(nc.callee().qualifiedName())
                        || ResultEnvelopeSplice.TO_ONE_FQN.equals(nc.callee().qualifiedName())
                        || ResultEnvelopeSplice.FIRST_FQN.equals(nc.callee().qualifiedName())) -> {
                TypedSpec coll = instanceOf(t, env, nc.args().get(0));
                if (coll instanceof com.legend.compiler.spec.typed.TypedCollection tc2) {
                    int k = nc.args().size() == 2 && nc.args().get(1)
                            instanceof com.legend.compiler.spec.typed.TypedCInteger ci
                            ? ci.value().intValue() : 0;
                    yield k >= 0 && k < tc2.elements().size()
                            ? instanceOf(t, env, tc2.elements().get(k)) : null;
                }
                yield coll;
            }
            case com.legend.compiler.spec.typed.TypedUserCall uc
                    when uc.args().isEmpty() && uc.callee().body().isPresent()
                    && !uc.callee().body().get().isEmpty() -> {
                var body = uc.callee().body().get();
                yield instanceOf(t, env, t.synth(body.get(body.size() - 1), env));
            }
            case null, default -> null;
        };
    }
}
