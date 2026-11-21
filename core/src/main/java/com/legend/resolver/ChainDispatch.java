package com.legend.resolver;

import com.legend.compiler.element.ModelContext;
import com.legend.compiler.element.TypedFunction;
import com.legend.compiler.element.type.ExprType;
import com.legend.compiler.element.type.Multiplicity;
import com.legend.compiler.element.type.Type;
import com.legend.compiler.spec.typed.TypedCBoolean;
import com.legend.compiler.spec.typed.TypedCString;
import com.legend.compiler.spec.typed.TypedFilter;
import com.legend.compiler.spec.typed.TypedIf;
import com.legend.compiler.spec.typed.TypedLambda;
import com.legend.compiler.spec.typed.TypedMap;
import com.legend.compiler.spec.typed.TypedMatchRuntime;
import com.legend.compiler.spec.typed.TypedNativeCall;
import com.legend.compiler.spec.typed.TypedPropertyAccess;
import com.legend.compiler.spec.typed.TypedSpec;
import com.legend.compiler.spec.typed.TypedTypeRef;
import com.legend.compiler.spec.typed.TypedVariable;
import com.legend.error.NotImplementedException;

import java.util.List;
import java.util.Optional;
import java.util.function.IntSupplier;
import java.util.function.Supplier;

/**
 * Run-time type dispatch in CHAIN position (harness burn-down leg 1,
 * 2026-09-02). Batch 2 built the three forms on an instance VARIABLE
 * ({@code $p->cast(@Sub).prop}, {@code $p->match([...])},
 * {@code $p->instanceOf(Sub)}) on the union row's membership witness.
 * The chain forms reduce to them: {@code chain->cast(@Sub)} keeps the
 * union row GATED &mdash; a filter whose predicate RAISES on a non-
 * conforming row (pure: cast exception; never a silent filter) plus a
 * cast gate on the class source so reads of the target's own properties
 * are the witness-gated subtype reads; {@code chain->match([...])} IS
 * {@code chain->map(v|$v->match([...]))}.
 */
final class ChainDispatch {

    private final ModelContext ctx;
    private final Supplier<TypedFunction> failCallee;
    private final IntSupplier fresh;

    ChainDispatch(ModelContext ctx, Supplier<TypedFunction> failCallee,
            IntSupplier fresh) {
        this.ctx = ctx;
        this.failCallee = failCallee;
        this.fresh = fresh;
    }

    /** A chain-position {@code ->cast(@Sub)} met BELOW a flatten hop rides
     * the hop list as a pseudo-hop under this prefix (collectOpChain);
     * the hop loop re-roots it (CastReRoot). */
    static final String CAST_HOP = "cast@";

    /** The cast as a PSEUDO-HOP below a flatten hop: the gate (raise on
     * a non-member) runs in the segment below it, then the chain re-roots
     * at the subtype's own extent on the shared key (CastReRoot); reads
     * above it are the subtype's. */
    void pseudoHop(TypedSpec source, Type.ClassType from, Type.ClassType to,
            List<String> hops, List<Boolean> many, List<List<TypedSpec>> segs) {
        hops.add(CAST_HOP + to.fqn());
        many.add(false);
        segs.add(new java.util.ArrayList<>());
        gate(source, from, to, segs.get(segs.size() - 1), true);
    }

    /** The chain cast's gate: adds the raise filter to {@code ops} and
     * returns the gate class. One gate per chain, above every flatten
     * hop (the class source the gate rides is the chain's final one). */
    String gate(TypedSpec source, Type.ClassType from, Type.ClassType to,
            List<TypedSpec> ops, boolean allowed) {
        if (!allowed) {
            throw new NotImplementedException("->cast(@" + to.fqn()
                    + ") over a chain of " + from.fqn() + " (partial"
                    + " membership) below a flatten hop, or a second"
                    + " cast on one chain, is not supported yet");
        }
        ops.add(castGateFilter(source, from, to));
        return to.fqn();
    }

    /** Whether the match has ROW-returning arms (a class-typed body). */
    static boolean rowArms(TypedMatchRuntime mr) {
        return mr.arms().stream().anyMatch(a -> Type.asClassType(a.body().info().type())
                instanceof Type.ClassType);
    }

    /** {@code chain->match([a:A[1]|rowsA, b:B[1]|rowsB])} with ROW arms
     * is the UNION of one branch per arm: the chain filtered to the
     * arm's run-time type, cast to it (the gated chain cast), mapped
     * through the arm's body — exactly one branch accepts each row. */
    TypedSpec chainMatchAsUnion(TypedMatchRuntime mr, Type rowClass) {
        TypedFunction concatCallee = concatCallee();
        TypedFunction instanceOf = instanceOfCallee();
        TypedSpec out = null;
        for (TypedMatchRuntime.Arm arm : mr.arms()) {
            Type.ClassType armType = (Type.ClassType) ctx.findType(arm.typeFqn())
                    .orElseThrow(() -> new IllegalStateException(
                            "match arm names an unknown type '" + arm.typeFqn() + "'"));
            String v = "_ma" + fresh.getAsInt();
            TypedVariable var = new TypedVariable(v, ExprType.one(rowClass));
            ExprType boolOne = new ExprType(Type.Primitive.BOOLEAN, Multiplicity.Bounded.ONE);
            TypedSpec test = new TypedNativeCall(instanceOf, List.of(var,
                    new TypedTypeRef(armType, ExprType.one(armType))), boolOne);
            TypedLambda pred = new TypedLambda(List.of(v), List.of(test),
                    new ExprType(new Type.FunctionType(
                            List.of(new Type.Param(rowClass, Multiplicity.Bounded.ONE)),
                            new Type.Param(Type.Primitive.BOOLEAN, Multiplicity.Bounded.ONE)),
                            Multiplicity.Bounded.ONE));
            TypedSpec filtered = new TypedFilter(mr.input(), pred, mr.input().info(), TypedFilter.Stamp.NONE /* runtime-if branch */);
            TypedSpec cast = new com.legend.compiler.spec.typed.TypedCast(filtered, armType,
                    new ExprType(armType, mr.input().info().multiplicity()),
                    /*wire*/ false);
            TypedLambda body = new TypedLambda(List.of(arm.param()), List.of(arm.body()),
                    new ExprType(new Type.FunctionType(
                            List.of(new Type.Param(armType, Multiplicity.Bounded.ONE)),
                            new Type.Param(arm.body().info().type(),
                                    arm.body().info().multiplicity())),
                            Multiplicity.Bounded.ONE));
            TypedSpec branch = new TypedMap(cast, body, mr.info());
            out = out == null ? branch
                    : new TypedNativeCall(concatCallee, List.of(out, branch), mr.info());
        }
        return java.util.Objects.requireNonNull(out, "a match has at least one arm");
    }

    /** Post-order normalization of ROW-arm matches under {@code n}: a
     * match over an object-space chain becomes its union of branches; a
     * class-result {@code map(x|…)} whose spliced body is such a match
     * becomes the union too (the flatten IS the body with the source
     * spliced). The chain walk then sees a class-collection concatenate
     * at the root, which the distribute rules own. */
    TypedSpec normalizeRowMatches(TypedSpec n,
            java.util.function.Predicate<TypedSpec> objectSpace,
            java.util.function.Function<TypedSpec, Type> sourceClass,
            java.util.function.BiFunction<TypedLambda, TypedSpec, TypedSpec> splice) {
        TypedSpec r = n.mapChildren(c -> normalizeRowMatches(c, objectSpace,
                sourceClass, splice));
        if (r instanceof TypedMatchRuntime mr && rowArms(mr)
                && objectSpace.test(mr.input())) {
            return chainMatchAsUnion(mr, sourceClass.apply(mr.input()));
        }
        if (r instanceof TypedMap m && objectSpace.test(m.source())
                && Type.asClassType(m.mapper().functionType().result().type()) instanceof Type.ClassType
                && m.mapper().body().size() == 1
                && m.mapper().body().get(0) instanceof TypedMatchRuntime bm
                && rowArms(bm)) {
            TypedSpec spliced = splice.apply(m.mapper(), m.source());
            if (spliced instanceof TypedMatchRuntime sm && objectSpace.test(sm.input())) {
                return chainMatchAsUnion(sm, sourceClass.apply(sm.input()));
            }
        }
        return r;
    }

    /** {@code chain->match([...])} as {@code chain->map(v|$v->match([...]))}. */
    TypedMap chainMatchAsMap(TypedMatchRuntime mr, Type rowClass) {
        String v = "_mv" + fresh.getAsInt();
        TypedVariable var = new TypedVariable(v, ExprType.one(rowClass));
        ExprType one = new ExprType(mr.info().type(), Multiplicity.Bounded.ONE);
        TypedSpec inner = new TypedMatchRuntime(var, mr.arms(), mr.extraParam(),
                mr.extra(), one);
        TypedLambda mapper = new TypedLambda(List.of(v), List.of(inner),
                new ExprType(new Type.FunctionType(
                        List.of(new Type.Param(rowClass, Multiplicity.Bounded.ONE)),
                        new Type.Param(mr.info().type(), Multiplicity.Bounded.ONE)),
                        Multiplicity.Bounded.ONE));
        return new TypedMap(mr.input(), mapper, mr.info());
    }

    /** Whether {@code n} is a property-access chain rooted at {@code var}. */
    static boolean navRootedAt(TypedSpec n, String var) {
        TypedSpec cur = n;
        while (cur instanceof TypedPropertyAccess pa) {
            cur = pa.source();
        }
        return cur instanceof TypedVariable v && v.name().equals(var)
                && n instanceof TypedPropertyAccess;
    }

    /** Occurrences of the variable {@code var} beneath {@code n}. */
    static int countVarReads(TypedSpec n, String var) {
        int c = n instanceof TypedVariable v && v.name().equals(var) ? 1 : 0;
        for (TypedSpec k : n.children()) {
            c += countVarReads(k, var);
        }
        return c;
    }

    /** The chain-position cast's GATE: {@code filter(v | if($v->instanceOf(T),
     * |true, |fail('Cast exception')))} — a row-preserving filter whose
     * predicate RAISES on a non-conforming row (the database evaluates
     * the CASE per row; batch 2's fail lowering). */
    private TypedFilter castGateFilter(TypedSpec source, Type.ClassType from,
            Type.ClassType to) {
        String v = "_cg" + fresh.getAsInt();
        ExprType boolOne = new ExprType(Type.Primitive.BOOLEAN,
                Multiplicity.Bounded.ONE);
        TypedVariable var = new TypedVariable(v, ExprType.one(from));
        TypedSpec test = new TypedNativeCall(instanceOfCallee(), List.of(var,
                new TypedTypeRef(to, ExprType.one(to))),
                boolOne);
        // the product's raise of its kind (W5 of docs/records/d-plan-2026-11-16.md), the two classes' NAMES as
        // legend-pure's cast prints them (GenericType.print, fullPaths false; the BE audit 3's M3, 2026-11-21)
        TypedSpec raise = typeRaise(com.legend.sql.RaiseKind.INSTANCE_NOT_OF, nameOf(from.fqn()), nameOf(to.fqn()),
                boolOne);
        TypedSpec body = new TypedIf(test, new TypedCBoolean(true, boolOne),
                Optional.of(raise), boolOne);
        TypedLambda pred = new TypedLambda(List.of(v), List.of(body),
                new ExprType(new Type.FunctionType(
                        List.of(new Type.Param(from,
                                Multiplicity.Bounded.ONE)),
                        new Type.Param(Type.Primitive.BOOLEAN,
                                Multiplicity.Bounded.ONE)),
                        Multiplicity.Bounded.ONE));
        return new TypedFilter(source, pred, source.info(), TypedFilter.Stamp.NONE /* runtime-if branch */);
    }

    /** {@code typeRaise(kind, from, to)} ({@link com.legend.builtin.Pure.Lite#TYPE_RAISE}): the product's raise of a
     *  run-time type check that failed, typed {@code as}. */
    /** An element's name, its path's last part ({@code ux::A} is {@code A}). */
    static String nameOf(String fqn) {
        return fqn.substring(fqn.lastIndexOf(':') + 1);
    }

    static TypedSpec typeRaise(com.legend.sql.RaiseKind kind, String from, String to, ExprType as) {
        ExprType text = new ExprType(Type.Primitive.STRING, Multiplicity.Bounded.ONE);
        List<com.legend.compiler.element.TypedParameter> params = java.util.stream.Stream.of("kind", "from", "to")
                .map(n -> new com.legend.compiler.element.TypedParameter(n, Type.Primitive.STRING,
                        Multiplicity.Bounded.ONE)).toList();
        TypedFunction callee = ObjectReferenceArms.liteCallee(com.legend.builtin.Pure.Lite.TYPE_RAISE, params,
                Type.Primitive.BOOLEAN);
        return new TypedNativeCall(callee, List.of(new TypedCString(kind.name(), text), new TypedCString(from, text),
                new TypedCString(to, text)), as);
    }

    private TypedFunction instanceOfCallee() {
        return ctx.findFunction(Substitution.INSTANCE_OF_FQN)
                .stream().filter(f -> f.parameters().size() == 2)
                .findFirst().orElseThrow(() -> new IllegalStateException(
                        "resolver bug: no instanceOf(Any, Type) registration"));
    }

    /** The 1-arg fail overload — the RAISE arm of run-time branch choice
     * over a discriminated row (a match no arm accepts, a cast the row's
     * run-time type does not satisfy: pure raises, so does the SQL). */
    static boolean containsRowMatch(TypedSpec n) {
        if (n instanceof com.legend.compiler.spec.typed.TypedMatchRuntime mr
                && rowArms(mr)) {
            return true;
        }
        for (TypedSpec c : n.children()) {
            if (containsRowMatch(c)) {
                return true;
            }
        }
        return false;
    }

    /** A class-valued {@code if} with a RUN-TIME condition, outside any
     * lambda body, becomes the union of its guarded branches
     * ({@link #ifAsUnion}); a statically decided one keeps the resolver's
     * branch choice. Lambda bodies (a per-row if) are not rewritten here,
     * nor an object-space if (no branch reads the store: {@code ^C(...)}
     * instances evaluate on the constructed-instance route). */
    TypedSpec runtimeIfsAsUnions(TypedSpec n, java.util.function.Predicate<TypedSpec> readsStore) {
        if (n instanceof TypedLambda) {
            return n;
        }
        TypedSpec r = n.mapChildren(c -> runtimeIfsAsUnions(c, readsStore));
        if (r instanceof TypedIf i
                && Type.asClassType(i.info().type()) instanceof Type.ClassType
                && LiteralFolds.staticBool(i.condition()) == null
                && i.elseBranch().isPresent()
                && (readsStore.test(i.thenBranch()) || readsStore.test(i.elseBranch().get()))) {
            return ifAsUnion(i);
        }
        return r;
    }

    /** {@code if(c, |A, |B)} whose branches are CLASS values and whose
     * condition is decided at RUN TIME is the UNION of the two branches,
     * each guarded by the condition — {@code A->filter(_|c)} and
     * {@code B->filter(_|!c)}: exactly one guard holds, so the UNION ALL is
     * the branch pure evaluates (the ROW-arm match's union, with the
     * condition as the arm test). A branch's {@code ->toOne()} keeps its
     * guard INSIDE: a disabled branch is empty, never a failed toOne. */
    TypedSpec ifAsUnion(TypedIf i) {
        TypedSpec c = i.condition();
        ExprType boolOne = new ExprType(Type.Primitive.BOOLEAN, Multiplicity.Bounded.ONE);
        TypedSpec notC = new TypedNativeCall(notCallee(), List.of(c), boolOne);
        TypedSpec a = guarded(LiteralFolds.unthunk(i.thenBranch()), c);
        TypedSpec b = guarded(LiteralFolds.unthunk(i.elseBranch().orElseThrow(() ->
                new NotImplementedException("class query under if() without an else branch"))),
                notC);
        return new TypedNativeCall(concatCallee(), List.of(a, b), i.info());
    }

    private TypedSpec guarded(TypedSpec branch, TypedSpec cond) {
        if (branch instanceof TypedNativeCall one && one.args().size() == 1
                && TO_ONE_FQN.equals(one.callee().qualifiedName())) {
            return guarded(one.args().get(0), cond);
        }
        Type row = branch.info().type();
        String v = "_if" + fresh.getAsInt();
        TypedLambda pred = new TypedLambda(List.of(v), List.of(cond),
                new ExprType(new Type.FunctionType(
                        List.of(new Type.Param(row, Multiplicity.Bounded.ONE)),
                        new Type.Param(Type.Primitive.BOOLEAN, Multiplicity.Bounded.ONE)),
                        Multiplicity.Bounded.ONE));
        return new TypedFilter(branch, pred, new ExprType(row, Multiplicity.Bounded.ZERO_MANY), TypedFilter.Stamp.NONE /* runtime-if branch */);
    }

    private static final String TO_ONE_FQN = "meta::pure::functions::multiplicity::toOne";

    private TypedFunction notCallee() {
        return ctx.findFunction("meta::pure::functions::boolean::not").stream()
                .filter(f -> f.parameters().size() == 1).findFirst()
                .orElseThrow(() -> new IllegalStateException(
                        "resolver bug: no not(Boolean) registration"));
    }

    private TypedFunction concatCallee() {
        return ctx.findFunction(StoreResolver.CONCAT_FQN).stream()
                .filter(f -> f.parameters().size() == 2).findFirst()
                .orElseThrow(() -> new IllegalStateException(
                        "resolver bug: no concatenate(a, b) registration"));
    }

}
