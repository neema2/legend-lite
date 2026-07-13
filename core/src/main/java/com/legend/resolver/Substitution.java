package com.legend.resolver;

import com.legend.compiler.element.type.ExprType;
import com.legend.compiler.element.type.Multiplicity;
import com.legend.compiler.element.type.Type;
import com.legend.compiler.spec.typed.TypedCBoolean;
import com.legend.compiler.spec.typed.TypedCDate;
import com.legend.compiler.spec.typed.TypedCDecimal;
import com.legend.compiler.spec.typed.TypedCFloat;
import com.legend.compiler.spec.typed.TypedCInteger;
import com.legend.compiler.spec.typed.TypedCString;
import com.legend.compiler.spec.typed.TypedCollection;
import com.legend.compiler.spec.typed.TypedEnumValue;
import com.legend.compiler.spec.typed.TypedIf;
import com.legend.compiler.spec.typed.TypedLambda;
import com.legend.compiler.spec.typed.TypedNativeCall;
import com.legend.compiler.spec.typed.TypedPropertyAccess;
import com.legend.compiler.spec.typed.TypedSpec;
import com.legend.compiler.spec.typed.TypedVariable;
import com.legend.error.MappingResolutionException;
import com.legend.error.NotImplementedException;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * The &beta;-substitution engine: rewrites a user lambda written over CLASS
 * instances into the same expression over the mapping pipeline's ROW,
 * replacing each {@code $p.prop} with the binding table's typed expression
 * (its own row variable renamed to this instantiation's fresh row var).
 *
 * <p>THE single path-extraction funnel: H3's DemandScan reuses
 * {@link #propertyOnUserVar} so demand analysis and rewriting cannot drift.
 *
 * <p>Discipline (plan risk #1): a replacement always carries the SAME
 * {@link ExprType} as the node it replaces &mdash; binding conformance is
 * G's guarantee (the body compiled through NewChecker's strict subsumption)
 * &mdash; so every enclosing node's info stays valid and no restamping pass
 * exists. Nodes outside the H2 expression vocabulary fail LOUD naming
 * themselves (corpus-driven expansion, never silent).
 */
final class Substitution {

    /** The instantiation being substituted into: fresh row var + bindings. */
    record Target(String userVar, String freshRowVar, String classFqn,
                  String mappingFqn, String sourceRowVar,
                  Map<String, TypedSpec> bindings, Type.RelationType rowType,
                  java.util.Set<String> strippedSlots,
                  Map<String, String> slotPrefixes,
                  Map<String, AssocSub> assocs,
                  java.util.Set<String> assocEnds,
                  Map<String, ExistsSub> existsSubs,
                  Map<TypedSpec, String> aggReads,
                  com.legend.compiler.element.TypedFunction isNotEmptyCallee,
                  boolean filterPosition, boolean nested) {}

    /** A to-many association head consumable under exists/isEmpty/isNotEmpty. */
    record ExistsSub(TypedSpec targetPipeline, TypedLambda orientedCond,
                     String targetRowVar, Map<String, TypedSpec> targetBindings,
                     Type.RelationType targetRow, String targetClassFqn,
                     java.util.Set<String> targetSlotAliases, boolean toMany) {}

    /**
     * A demanded association head: how its leaf bindings substitute.
     * {@code readVar}/{@code readRowType} override where the rewritten
     * reads attach — null means the chain's fresh row var (joined form);
     * an EXISTS-inner registration points them at the subquery row.
     */
    record AssocSub(String prefix, String targetRowVar,
                    Map<String, TypedSpec> targetBindings, String targetClassFqn,
                    java.util.Set<String> targetSlotAliases,
                    Map<String, String> targetSlotPrefixes,
                    String readVar, Type.RelationType readRowType) {

        AssocSub(String prefix, String targetRowVar,
                 Map<String, TypedSpec> targetBindings, String targetClassFqn,
                 java.util.Set<String> targetSlotAliases) {
            this(prefix, targetRowVar, targetBindings, targetClassFqn,
                    targetSlotAliases, Map.of(), null, null);
        }

        AssocSub(String prefix, String targetRowVar,
                 Map<String, TypedSpec> targetBindings, String targetClassFqn,
                 java.util.Set<String> targetSlotAliases,
                 Map<String, String> targetSlotPrefixes) {
            this(prefix, targetRowVar, targetBindings, targetClassFqn,
                    targetSlotAliases, targetSlotPrefixes, null, null);
        }
    }

    private final Target target;

    Substitution(Target target) {
        this.target = Objects.requireNonNull(target, "target");
    }

    /**
     * Rewrite {@code lambda}'s body over the row: parameters
     * {@code [p]} become {@code [freshRowVar]} and the info is rebuilt as
     * {@code {row[1] -> <result>}}.
     */
    TypedLambda rewriteLambda(TypedLambda lambda) {
        if (lambda.parameters().size() != 1) {
            throw new NotImplementedException("object-space lambda with "
                    + lambda.parameters().size() + " parameters is not supported yet");
        }
        List<TypedSpec> body = new ArrayList<>(lambda.body().size());
        for (TypedSpec stmt : lambda.body()) {
            body.add(rewrite(stmt));
        }
        Type.FunctionType oldFn = (Type.FunctionType) lambda.info().type();
        Type.FunctionType newFn = new Type.FunctionType(
                List.of(new Type.Param(target.rowType(), Multiplicity.Bounded.ONE)),
                oldFn.result());
        return new TypedLambda(List.of(target.freshRowVar()), body,
                new ExprType(newFn, Multiplicity.Bounded.ONE));
    }

    /**
     * The identity over the row &mdash; a whole-instance aggregate map
     * ({@code x|$x}, COUNT(*)-style) becomes {@code _rN|$_rN}: the bare row
     * var, which the lowerer's bare-map arm turns into a no-value reducer.
     */
    TypedLambda identityLambda(TypedLambda lambda) {
        Type.FunctionType oldFn = (Type.FunctionType) lambda.info().type();
        Type.FunctionType newFn = new Type.FunctionType(
                List.of(new Type.Param(target.rowType(), Multiplicity.Bounded.ONE)),
                oldFn.result());
        return new TypedLambda(List.of(target.freshRowVar()),
                List.of(new TypedVariable(target.freshRowVar(),
                        new ExprType(target.rowType(), Multiplicity.Bounded.ONE))),
                new ExprType(newFn, Multiplicity.Bounded.ONE));
    }

    /**
     * THE path funnel: if {@code n} is a property access whose receiver is
     * the user's lambda variable, its property name; else {@code null}.
     * (H3 extends this to multi-hop paths; DemandScan shares it.)
     */
    static String propertyOnUserVar(TypedSpec n, String userVar) {
        java.util.List<String> p = pathOf(n, userVar);
        return p != null && p.size() == 1 ? p.get(0) : null;
    }

    /**
     * THE path funnel: the full property chain when {@code n}'s receiver
     * chain bottoms at the user's lambda variable ({@code $p.employer.legal}
     * &rArr; {@code [employer, legal]}); {@code null} otherwise. DemandScan
     * and the rewrite share this single extractor.
     */
    static java.util.List<String> pathOf(TypedSpec n, String userVar) {
        // toOne() look-through: $p.employer->toOne().legal is the idiomatic
        // spelling after an optional navigation — the coercion is
        // multiplicity-only and transparent to the path (audit R3).
        if (n instanceof TypedNativeCall c && c.args().size() == 1
                && c.callee().qualifiedName().equals("meta::pure::functions::multiplicity::toOne")) {
            return pathOf(c.args().get(0), userVar);
        }
        if (!(n instanceof TypedPropertyAccess pa)) {
            return null;
        }
        if (pa.source() instanceof TypedVariable v && v.name().equals(userVar)) {
            return java.util.List.of(pa.property());
        }
        java.util.List<String> inner = pathOf(pa.source(), userVar);
        if (inner == null) {
            return null;
        }
        java.util.List<String> out = new ArrayList<>(inner);
        out.add(pa.property());
        return out;
    }

    private TypedSpec rewrite(TypedSpec n) {
        // AGGREGATE over a to-many navigation (identity-registered by the
        // demand scan): the whole call reads its grouped-subselect column —
        // same ExprType as the node it replaces (discipline, plan risk #1).
        String aggCol = target.aggReads().get(n);
        if (aggCol != null) {
            return new TypedPropertyAccess(new TypedVariable(target.freshRowVar(),
                    new ExprType(target.rowType(), Multiplicity.Bounded.ONE)),
                    aggCol, n.info());
        }
        // TO-MANY navigation under an emptiness call: correlated EXISTS —
        // the target pipeline filtered by the association condition (parent
        // reads become the FREE outer row var, resolved through the
        // lowerer's enclosing-scope channel), the user predicate substituted
        // over the target's bindings. §133's single form.
        if (n instanceof TypedNativeCall call && !call.args().isEmpty()) {
            java.util.List<String> headPath = pathOf(call.args().get(0), target.userVar());
            if (headPath != null && headPath.size() == 1
                    && target.existsSubs().containsKey(headPath.get(0))
                    && isEmptinessFamily(call)) {
                return rewriteExists(call, target.existsSubs().get(headPath.get(0)));
            }
        }
        // Boolean LEAF (not and/or) crossing a filter-only to-many:
        // implicit EXISTS (plangen F1). and/or recurse so each leaf wraps
        // individually; a `not` wraps as a WHOLE leaf — ∃(¬X), never
        // ¬∃(X) (see isBooleanConnective; the `!=` corpus family).
        // Emptiness-family calls are NOT ∃-distributive (isEmpty over a
        // crossing is about ABSENCE — wrapping it in EXISTS silently
        // inverts rows; audit blocker): whether at the leaf top or inside
        // a `not`, they take the join/loud routes instead.
        if (n instanceof TypedNativeCall lc
                && lc.info().type() == Type.Primitive.BOOLEAN
                && !isBooleanConnective(lc)
                && !containsEmptinessFamily(lc)) {
            String head = toManyFilterHead(lc);
            if (head != null) {
                TypedSpec wrapped = rewriteImplicitExists(lc, head);
                // A NEGATED leaf admits parents with ZERO children (engine
                // parity: its LEFT-join null row passes the `or is null` it
                // adds under `not`) — if(any child, EXISTS(not X), true).
                if (com.legend.builtin.Pure.nativeNamed("not",
                        lc.callee().signatureKey())) {
                    TypedSpec anyChild = new TypedNativeCall(
                            target.isNotEmptyCallee(),
                            List.of(childrenRel(target.existsSubs().get(head))),
                            new ExprType(Type.Primitive.BOOLEAN,
                                    Multiplicity.Bounded.ONE));
                    return new TypedIf(anyChild, wrapped,
                            java.util.Optional.of(new TypedCBoolean(true,
                                    new ExprType(Type.Primitive.BOOLEAN,
                                            Multiplicity.Bounded.ONE))),
                            new ExprType(Type.Primitive.BOOLEAN,
                                    Multiplicity.Bounded.ONE));
                }
                return wrapped;
            }
        }
        java.util.List<String> path = pathOf(n, target.userVar());
        if (path != null && path.size() == 2) {
            return rewritePath(path.get(0), path.get(1), n);
        }
        if (path != null && path.size() > 2) {
            // MULTI-HOP association chain ($p.dept.org.name): the demand scan
            // registered one join per hop under its chain key — the leaf reads
            // the DEEPEST hop's target with the chained prefix (dept_org_).
            String chainKey = String.join(".", path.subList(0, path.size() - 1));
            if (target.assocs().containsKey(chainKey)) {
                return assocLeaf(chainKey, path.get(path.size() - 1));
            }
            // MULTI-HOP through NESTED EMBEDDED ctors ($p.firm.address.name
            // over a denormalized mapping): walk the ^Inner(...) chain to
            // the leaf expression — parent-alias reads all the way down
            TypedSpec cur = target.bindings().get(path.get(0));
            int hop = 1;
            while (cur != null && hop < path.size()) {
                TypedSpec inner = cur;
                if (inner instanceof TypedNativeCall c1 && c1.args().size() == 1
                        && c1.callee().qualifiedName().equals(
                                "meta::pure::functions::multiplicity::toOne")) {
                    inner = c1.args().get(0);
                }
                var ow = otherwiseOf(inner);
                if (ow != null) {
                    inner = ow.args().get(0);
                }
                if (inner instanceof com.legend.compiler.spec.typed.TypedNewInstance ni
                        && ni.properties().containsKey(path.get(hop))) {
                    cur = ni.properties().get(path.get(hop));
                    hop++;
                } else {
                    cur = null;
                }
            }
            if (cur != null) {
                return renameRowVar(cur);
            }
            throw new NotImplementedException("multi-hop navigation "
                    + String.join(".", path) + " through an embedded/slot head"
                    + " is not supported yet");
        }
        String prop = propertyOnUserVar(n, target.userVar());
        if (prop != null) {
            TypedSpec binding = target.bindings().get(prop);
            if (binding != null) {
                // A CLASS-typED binding used as a whole value ($p.firm bare,
                // $p.addr bare): graph output territory — the honest story,
                // not a "resolver bug" from the rewriter's vocabulary wall.
                TypedSpec inner = binding;
                if (inner instanceof TypedNativeCall c1 && c1.args().size() == 1
                        && c1.callee().qualifiedName().equals("meta::pure::functions::multiplicity::toOne")) {
                    inner = c1.args().get(0);
                }
                if (inner instanceof com.legend.compiler.spec.typed.TypedNewInstance
                        || inner.info().type()
                                instanceof com.legend.compiler.element.type.Type.ClassType) {
                    throw new NotImplementedException("class-typed property '$"
                            + target.userVar() + "." + prop + "' used as a whole"
                            + " value is graph output (Phase H4)");
                }
            }
            if (binding == null) {
                if (target.nested()) {
                    throw new NotImplementedException("nested navigation '$"
                            + target.userVar() + "." + prop + "' inside an"
                            + " exists/isEmpty predicate is not supported yet");
                }
                if (target.assocEnds().contains(prop)) {
                    throw new NotImplementedException("association property '$"
                            + target.userVar() + "." + prop + "' used other than"
                            + " as a navigation head (class-typed value /"
                            + " isEmpty / whole-instance) is not supported yet");
                }
                throw new MappingResolutionException("property '" + prop
                        + "' of class '" + target.classFqn()
                        + "' has no binding in mapping '" + target.mappingFqn()
                        + "' (unmapped, or routed to a non-root mapping set —"
                        + " multi-set union dispatch is a roadmap feature)",
                        target.classFqn());
            }
            return renameRowVar(binding);
        }
        return switch (n) {
            case TypedPropertyAccess pa when filteredNavLeafRead(pa) != null ->
                    filteredNavLeafRead(pa);
            case TypedVariable v when v.name().equals(target.userVar()) ->
                    throw new NotImplementedException(
                            "object-space use of the instance variable '$" + v.name()
                                    + "' other than property access is not supported yet");
            case TypedVariable v -> v;
            case TypedPropertyAccess pa -> new TypedPropertyAccess(
                    rewrite(pa.source()), pa.property(), pa.info());
            case TypedNativeCall c -> new TypedNativeCall(c.callee(),
                    rewriteAll(c.args()), c.info());
            case TypedCollection c -> new TypedCollection(rewriteAll(c.elements()), c.info());
            case TypedIf i -> new TypedIf(rewrite(i.condition()),
                    rewrite(i.thenBranch()), i.elseBranch().map(this::rewrite), i.info());
            case TypedLambda l -> {
                if (l.parameters().contains(target.freshRowVar())) {
                    throw new IllegalStateException("resolver bug: nested lambda"
                            + " binds the fresh row var '" + target.freshRowVar()
                            + "' — fresh-var selection must avoid user names");
                }
                yield l.parameters().contains(target.userVar())
                        ? l   // shadowing: substitution stops (standard capture rule)
                        : new TypedLambda(l.parameters(), rewriteAll(l.body()), l.info());
            }
            // Literals: nothing to substitute.
            case TypedCString ignored -> n;
            case TypedCInteger ignored -> n;
            case TypedCFloat ignored -> n;
            case TypedCDecimal ignored -> n;
            case TypedCBoolean ignored -> n;
            case TypedCDate ignored -> n;
            case TypedEnumValue ignored -> n;
            default -> throw new NotImplementedException(
                    "object-space expression node " + n.getClass().getSimpleName()
                            + " is not substitutable yet (H2 vocabulary)");
        };
    }

    /** {@code $p.head.leaf}: embedded ctor look-through, or association leaf. */
    private TypedSpec rewritePath(String head, String leaf, TypedSpec original) {
        TypedSpec headBinding = target.bindings().get(head);
        if (headBinding != null) {
            TypedSpec inner = headBinding;
            if (inner instanceof TypedNativeCall c && c.args().size() == 1
                    && c.callee().qualifiedName().equals("meta::pure::functions::multiplicity::toOne")) {
                inner = c.args().get(0);
            }
            // A class-typed navigate-slot read ($row.alias): the step was
            // demanded and registered under this HEAD — dispatch like an
            // association (target bindings, prefixed columns).
            if (target.assocs().containsKey(head)
                    && inner instanceof TypedPropertyAccess pa
                    && pa.source() instanceof TypedVariable) {
                return assocLeaf(head, leaf);
            }
            if (inner instanceof com.legend.compiler.spec.typed.TypedNewInstance ctor) {
                // EMBEDDED: the inner binding reads the PARENT row — a
                // parent-alias column, never a join (V1 §D.4 semantics).
                TypedSpec leafExpr = ctor.properties().get(leaf);
                if (leafExpr == null) {
                    throw new MappingResolutionException("property '" + leaf
                            + "' of embedded '" + head + "' on class '"
                            + target.classFqn() + "' is not mapped in mapping '"
                            + target.mappingFqn() + "'", target.classFqn());
                }
                return renameRowVar(leafExpr);
            }
            // OTHERWISE per-leaf dispatch (V1 §D.5): leaf in the embedded
            // partial => parent-alias read, no join; any other leaf =>
            // through the FALLBACK's demanded navigate slot. The same head
            // can go both ways in one query.
            TypedNativeCall ow = otherwiseOf(headBinding);
            if (ow != null) {
                TypedSpec leafExpr = ((com.legend.compiler.spec.typed.TypedNewInstance)
                        ow.args().get(0)).properties().get(leaf);
                if (leafExpr != null) {
                    return renameRowVar(leafExpr);
                }
                return assocLeaf(head, leaf);
            }
            if (inner instanceof com.legend.compiler.spec.typed.TypedNewInstanceCast) {
                throw new NotImplementedException("navigation '$" + target.userVar()
                        + "." + head + "." + leaf + "' crosses a MODEL-TO-MODEL"
                        + " cast binding — not supported yet (H5c)");
            }
            throw new NotImplementedException("navigation through class-typed"
                    + " slot property '" + head + "' is not supported yet");
        }
        return assocLeaf(head, leaf);
    }

    /** The leaf of a demanded association / navigate-slot head. */
    private TypedSpec assocLeaf(String head, String leaf) {
        AssocSub a = target.assocs().get(head);
        if (a == null) {
            if (target.nested()) {
                throw new NotImplementedException("nested navigation '" + head
                        + "." + leaf + "' inside an exists/isEmpty predicate is"
                        + " not supported yet");
            }
            if (target.existsSubs().containsKey(head)) {
                throw new NotImplementedException("to-many navigation '" + head
                        + "." + leaf + "' in this position (e.g. under isEmpty)"
                        + " is not supported yet");
            }
            throw new IllegalStateException("resolver bug: undemanded navigation"
                    + " '" + head + "." + leaf + "' — the demand scan and the"
                    + " rewrite disagreed");
        }
        TypedSpec leafBinding = a.targetBindings().get(leaf);
        if (leafBinding == null) {
            if (target.nested()) {
                throw new NotImplementedException("nested navigation '" + head
                        + "." + leaf + "' inside an exists/isEmpty predicate is"
                        + " not supported yet");
            }
            throw new MappingResolutionException("property '" + leaf
                    + "' of class '" + a.targetClassFqn()
                    + "' is not mapped in mapping '" + target.mappingFqn() + "'",
                    a.targetClassFqn());
        }
        TypedSpec leafInner = leafBinding;
        if (leafInner instanceof TypedNativeCall lc && lc.args().size() == 1
                && lc.callee().qualifiedName().equals("meta::pure::functions::multiplicity::toOne")) {
            leafInner = lc.args().get(0);
        }
        if (leafInner instanceof com.legend.compiler.spec.typed.TypedNewInstance) {
            throw new NotImplementedException("class-typed property '" + leaf
                    + "' of association target '" + a.targetClassFqn()
                    + "' (embedded) is not supported yet");
        }
        // A leaf reading the target's OWN join slots: DEMANDED slots were
        // materialized (their columns ride the target pipeline slot-prefixed)
        // and the read FLATTENS through the unified rewriter before the
        // chain prefix applies; an UNDEMANDED slot read is loud — the
        // demand scan and the rewrite disagreed, or the position (exists)
        // doesn't materialize target slots yet.
        if (Pipelines.referencesAliasOn(leafBinding, a.targetRowVar(),
                a.targetSlotAliases())) {
            java.util.Set<String> unconverted = new java.util.HashSet<>(a.targetSlotAliases());
            unconverted.removeAll(a.targetSlotPrefixes().keySet());
            if (Pipelines.referencesAliasOn(leafBinding, a.targetRowVar(), unconverted)) {
                throw new NotImplementedException("property '" + leaf + "' of class '"
                        + a.targetClassFqn() + "' is mapped through the target's own"
                        + " join slots; nested navigation joins are not supported"
                        + " in this position yet");
            }
            leafBinding = Pipelines.rewriteRowReads(leafBinding, a.targetRowVar(),
                    a.targetSlotPrefixes(), java.util.Set.of(),
                    v -> new TypedVariable(a.targetRowVar(), v.info()));
        }
        String readVar = a.readVar() != null ? a.readVar() : target.freshRowVar();
        Type.RelationType readRow = a.readRowType() != null ? a.readRowType()
                : target.rowType();
        return Pipelines.prefixColumns(leafBinding, a.targetRowVar(), a.prefix(),
                v -> new TypedVariable(readVar,
                        new ExprType(readRow, Multiplicity.Bounded.ONE)));
    }

    /** Filter-only to-many head inside this expression (implicit-EXISTS demand). */
    private String toManyFilterHead(TypedSpec n) {
        java.util.List<String> path = pathOf(n, target.userVar());
        // FILTER position + a TO-MANY head wraps in EXISTS even when a
        // projection elsewhere forced the JOIN: filtering the EXPLODED rows
        // would keep only matching children instead of selecting parents
        // (the reference semantics is EXISTS-then-explode).
        if (path != null && path.size() == 2
                && target.filterPosition()
                && target.existsSubs().containsKey(path.get(0))
                && target.existsSubs().get(path.get(0)).toMany()) {
            return path.get(0);
        }
        if (path != null && path.size() == 2
                && target.existsSubs().containsKey(path.get(0))
                && target.existsSubs().get(path.get(0)).toMany()
                && !target.assocs().containsKey(path.get(0))) {
            return path.get(0);
        }
        for (TypedSpec c : n.children()) {
            String h = toManyFilterHead(c);
            if (h != null) {
                return h;
            }
        }
        return null;
    }

    /**
     * Implicit EXISTS (plangen F1): a boolean LEAF crossing a to-many
     * association wraps — EXISTS(target WHERE assoc-corr AND leaf'), the
     * leaf's crossing reads rewritten onto the subquery row, everything
     * else staying correlated to the outer row.
     */
    /** The correlated child extent: target pipeline filtered by the
     * oriented association condition (parent reads on the outer row). */
    private TypedSpec childrenRel(ExistsSub ex) {
        TypedLambda cond = ex.orientedCond();
        String pVar = cond.parameters().get(0);
        String tVar = cond.parameters().get(1);
        List<TypedSpec> corrBody = cond.body().stream().map(b ->
                Pipelines.rewriteRowReads(b, pVar, Map.of(), java.util.Set.of(),
                        v -> new TypedVariable(target.freshRowVar(),
                                new ExprType(target.rowType(), Multiplicity.Bounded.ONE))))
                .toList();
        ExprType predType = new ExprType(new Type.FunctionType(
                List.of(new Type.Param(ex.targetRow(), Multiplicity.Bounded.ONE)),
                new Type.Param(Type.Primitive.BOOLEAN, Multiplicity.Bounded.ONE)),
                Multiplicity.Bounded.ONE);
        TypedLambda corr = new TypedLambda(List.of(tVar), corrBody, predType);
        return new com.legend.compiler.spec.typed.TypedFilter(
                ex.targetPipeline(), corr, ex.targetPipeline().info());
    }

    private TypedSpec rewriteImplicitExists(TypedNativeCall leaf, String head) {
        if (target.isNotEmptyCallee() == null) {
            throw new IllegalStateException(
                    "resolver bug: implicit EXISTS demanded without an isNotEmpty callee");
        }
        ExistsSub ex = target.existsSubs().get(head);
        TypedLambda cond = ex.orientedCond();
        String pVar = cond.parameters().get(0);
        String tVar = cond.parameters().get(1);
        List<TypedSpec> corrBody = cond.body().stream().map(b ->
                Pipelines.rewriteRowReads(b, pVar, Map.of(), java.util.Set.of(),
                        v -> new TypedVariable(target.freshRowVar(),
                                new ExprType(target.rowType(), Multiplicity.Bounded.ONE))))
                .toList();
        ExprType predType = new ExprType(new Type.FunctionType(
                List.of(new Type.Param(ex.targetRow(), Multiplicity.Bounded.ONE)),
                new Type.Param(Type.Primitive.BOOLEAN, Multiplicity.Bounded.ONE)),
                Multiplicity.Bounded.ONE);
        TypedLambda corr = new TypedLambda(List.of(tVar), corrBody, predType);

        // The leaf rewrites with the head registered as an UNPREFIXED
        // AssocSub reading the subquery row; everything else substitutes
        // as usual (outer reads stay correlated).
        Map<String, AssocSub> inner = new java.util.LinkedHashMap<>(target.assocs());
        inner.put(head, new AssocSub("", ex.targetRowVar(), ex.targetBindings(),
                ex.targetClassFqn(), ex.targetSlotAliases(), Map.of(), tVar, ex.targetRow()));
        Substitution innerSub = new Substitution(new Target(
                target.userVar(), target.freshRowVar(), target.classFqn(),
                target.mappingFqn(), target.sourceRowVar(), target.bindings(),
                target.rowType(), target.strippedSlots(), target.slotPrefixes(),
                inner, target.assocEnds(), Map.of(), Map.of(), null, true, true));
        TypedSpec leafInner = innerSub.rewrite(leaf);
        // ENGINE NEGATION ISOLATION: under a not-leaf the engine adds
        // `or <crossing read> is null` — a child whose crossed value is
        // NULL passes the negation (not(NULL) is NULL and would silently
        // drop the row otherwise). if(read present, ¬X, true).
        if (com.legend.builtin.Pure.nativeNamed("not", leaf.callee().signatureKey())) {
            TypedSpec read = crossingRead(leaf, head);
            if (read != null) {
                TypedSpec readInner = innerSub.rewrite(read);
                leafInner = new TypedIf(
                        new TypedNativeCall(target.isNotEmptyCallee(),
                                List.of(readInner),
                                new ExprType(Type.Primitive.BOOLEAN,
                                        Multiplicity.Bounded.ONE)),
                        leafInner,
                        java.util.Optional.of(new TypedCBoolean(true,
                                new ExprType(Type.Primitive.BOOLEAN,
                                        Multiplicity.Bounded.ONE))),
                        new ExprType(Type.Primitive.BOOLEAN,
                                Multiplicity.Bounded.ONE));
            }
        }
        TypedLambda pred = new TypedLambda(List.of(tVar), List.of(leafInner), predType);

        TypedSpec rel = new com.legend.compiler.spec.typed.TypedFilter(
                new com.legend.compiler.spec.typed.TypedFilter(
                        ex.targetPipeline(), corr, ex.targetPipeline().info()),
                pred, ex.targetPipeline().info());
        return new TypedNativeCall(target.isNotEmptyCallee(), List.of(rel),
                new ExprType(Type.Primitive.BOOLEAN, Multiplicity.Bounded.ONE));
    }

    /** The {@code $p.head.leaf} read inside a boolean leaf (the crossing
     * the implicit EXISTS wraps); null when none/ambiguous. */
    private TypedSpec crossingRead(TypedSpec n, String head) {
        java.util.List<String> path = pathOf(n, target.userVar());
        if (path != null && path.size() == 2 && path.get(0).equals(head)) {
            return n;
        }
        for (TypedSpec c : n.children()) {
            TypedSpec r = crossingRead(c, head);
            if (r != null) {
                return r;
            }
        }
        return null;
    }

    private static boolean isEmptinessFamily(TypedNativeCall c) {
        String key = c.callee().signatureKey();
        return com.legend.builtin.Pure.nativeNamed("isEmpty", key)
                || com.legend.builtin.Pure.nativeNamed("isNotEmpty", key)
                || com.legend.builtin.Pure.nativeNamed("exists", key);
    }

    /**
     * A FILTERED NAVIGATION consumed as a VALUE:
     * {@code $p.assoc->filter(pred)->toOne().leaf} (derived-property bodies
     * inline to exactly this shape). Rewrites to a CORRELATED single-column
     * relation — target pipeline filtered by the oriented association
     * condition AND the user predicate, projecting the leaf binding — which
     * the lowerer renders as a scalar subquery in scalar position (DuckDB
     * raises on more than one row: pure {@code toOne} semantics; empty is
     * NULL: the read is {@code [0..1]}). Returns null when the shape does
     * not match (the caller falls through to the ordinary walk).
     */
    private TypedSpec filteredNavLeafRead(TypedPropertyAccess pa) {
        TypedSpec src = pa.source();
        boolean firstRow = false;
        boolean unwrapped = true;
        if (src instanceof TypedNativeCall c && c.args().size() == 1) {
            String callee = c.callee().qualifiedName();
            if (callee.equals("meta::pure::functions::multiplicity::toOne")) {
                src = c.args().get(0);
                unwrapped = false;
            } else if (callee.equals("meta::pure::functions::collection::first")
                    || callee.equals("meta::pure::functions::collection::head")) {
                // first()/head() keep AT MOST one row — the scalar bridge's
                // toOne rendering raises on >1, so the subquery must LIMIT 1
                src = c.args().get(0);
                firstRow = true;
                unwrapped = false;
            }
        }
        if (unwrapped
                && !(pa.info().multiplicity() instanceof Multiplicity.Bounded ub
                        && Integer.valueOf(1).equals(ub.upper()))) {
            // a BARE [*] read ($p.xs->filter(..).name) is a collection, not
            // a scalar — imposing toOne semantics on it would raise (or
            // worse, be wrong under an aggregation); fall through loud
            return null;
        }
        if (!(src instanceof com.legend.compiler.spec.typed.TypedFilter f)
                || !(f.source() instanceof TypedPropertyAccess head)
                || !(head.source() instanceof TypedVariable hv)
                || !hv.name().equals(target.userVar())) {
            return null;
        }
        ExistsSub ex = target.existsSubs().get(head.property());
        if (ex == null) {
            return null;
        }
        // 1. correlated association condition over the target pipeline
        TypedLambda cond = ex.orientedCond();
        String pVar = cond.parameters().get(0);
        String tVar = cond.parameters().get(1);
        List<TypedSpec> corrBody = cond.body().stream().map(b ->
                Pipelines.rewriteRowReads(b, pVar, Map.of(), java.util.Set.of(),
                        v -> new TypedVariable(target.freshRowVar(),
                                new ExprType(target.rowType(), Multiplicity.Bounded.ONE))))
                .toList();
        TypedLambda corr = new TypedLambda(List.of(tVar), corrBody,
                new ExprType(new Type.FunctionType(
                        List.of(new Type.Param(ex.targetRow(), Multiplicity.Bounded.ONE)),
                        new Type.Param(Type.Primitive.BOOLEAN, Multiplicity.Bounded.ONE)),
                        Multiplicity.Bounded.ONE));
        TypedSpec rel = new com.legend.compiler.spec.typed.TypedFilter(
                ex.targetPipeline(), corr, ex.targetPipeline().info());
        // 2. the user predicate, substituted against the TARGET's bindings
        //    (outer reads correlate through a second pass — same as exists)
        TypedLambda predLam = f.predicate();
        Substitution predSub = new Substitution(new Target(
                predLam.parameters().get(0), tVar, ex.targetClassFqn(),
                target.mappingFqn(), ex.targetRowVar(), ex.targetBindings(),
                ex.targetRow(), ex.targetSlotAliases(), Map.of(), Map.of(),
                java.util.Set.of(), Map.of(), Map.of(), null, true, true));
        TypedLambda inner = predSub.rewriteLambda(predLam);
        TypedLambda innerOuter = new TypedLambda(inner.parameters(),
                inner.body().stream().map(this::rewrite).toList(), inner.info());
        rel = new com.legend.compiler.spec.typed.TypedFilter(rel, innerOuter, rel.info());
        // 3. project the leaf binding
        TypedSpec leafBinding = ex.targetBindings().get(pa.property());
        if (leafBinding == null) {
            throw new MappingResolutionException("property '" + pa.property()
                    + "' of class '" + ex.targetClassFqn()
                    + "' has no binding in mapping '" + target.mappingFqn()
                    + "' (filtered-navigation leaf)", ex.targetClassFqn());
        }
        if (Pipelines.referencesAliasOn(leafBinding, ex.targetRowVar(),
                ex.targetSlotAliases())) {
            throw new NotImplementedException("filtered-navigation leaf '"
                    + pa.property() + "' reads a join slot of '"
                    + ex.targetClassFqn() + "' — slot-demanding leaves under"
                    + " value-position filters are not supported yet");
        }
        Type leafType = pa.info().type();
        TypedLambda leafFn = new TypedLambda(List.of(ex.targetRowVar()),
                List.of(leafBinding),
                new ExprType(new Type.FunctionType(
                        List.of(new Type.Param(ex.targetRow(), Multiplicity.Bounded.ONE)),
                        new Type.Param(leafType, Multiplicity.Bounded.ONE)),
                        Multiplicity.Bounded.ONE));
        Type.RelationType outRow = new Type.RelationType(List.of(
                new Type.RelationType.Column(pa.property(), leafType,
                        pa.info().multiplicity())));
        TypedSpec projected = new com.legend.compiler.spec.typed.TypedProject(rel,
                List.of(new com.legend.compiler.spec.typed.TypedFuncCol(
                        pa.property(), leafFn)),
                new ExprType(outRow, Multiplicity.Bounded.ONE));
        if (firstRow) {
            projected = new com.legend.compiler.spec.typed.TypedLimit(projected,
                    new com.legend.compiler.spec.typed.TypedCInteger(1L,
                            ExprType.one(Type.Primitive.INTEGER)),
                    projected.info());
        }
        return projected;
    }

    private TypedSpec rewriteExists(TypedNativeCall call, ExistsSub ex) {
        TypedLambda cond = ex.orientedCond();   // params (parentRow, targetRow)
        String pVar = cond.parameters().get(0);
        String tVar = cond.parameters().get(1);
        List<TypedSpec> corrBody = cond.body().stream().map(b ->
                Pipelines.rewriteRowReads(b, pVar, Map.of(), java.util.Set.of(),
                        v -> new TypedVariable(target.freshRowVar(),
                                new ExprType(target.rowType(), Multiplicity.Bounded.ONE))))
                .toList();
        TypedLambda corr = new TypedLambda(List.of(tVar), corrBody,
                new ExprType(new Type.FunctionType(
                        List.of(new Type.Param(ex.targetRow(), Multiplicity.Bounded.ONE)),
                        new Type.Param(Type.Primitive.BOOLEAN, Multiplicity.Bounded.ONE)),
                        Multiplicity.Bounded.ONE));
        TypedSpec rel = new com.legend.compiler.spec.typed.TypedFilter(
                ex.targetPipeline(), corr, ex.targetPipeline().info());
        List<TypedSpec> newArgs = new ArrayList<>();
        newArgs.add(rel);
        if (call.args().size() == 2) {
            if (!(call.args().get(1) instanceof TypedLambda predLam)) {
                throw new NotImplementedException("non-lambda predicate in "
                        + call.callee().qualifiedName() + " over an association");
            }
            Substitution predSub = new Substitution(new Target(
                    predLam.parameters().get(0), tVar, ex.targetClassFqn(),
                    target.mappingFqn(), ex.targetRowVar(), ex.targetBindings(),
                    ex.targetRow(), ex.targetSlotAliases(), Map.of(), Map.of(),
                    java.util.Set.of(), Map.of(), Map.of(), null, true, true));
            TypedLambda inner = predSub.rewriteLambda(predLam);
            // OUTER reads inside the predicate ($s.name == $f.legal): a
            // second pass through THIS substitution turns them into
            // correlated free-var bindings (the pred param shadows nothing
            // of the outer var; already-rewritten inner reads don't match
            // the outer path funnel). Without it they escaped verbatim —
            // audit blocker (StackOverflow downstream).
            newArgs.add(new TypedLambda(inner.parameters(),
                    inner.body().stream().map(this::rewrite).toList(),
                    inner.info()));
        }
        return new TypedNativeCall(call.callee(), newArgs, call.info());
    }

    /**
     * {@code and}/{@code or} distribute over the per-leaf implicit EXISTS;
     * {@code not} does NOT (&not; is not &exist;-distributive): {@code !=}
     * is real pure's {@code not(equal(...))}, and hoisting the negation
     * outside would turn "has an address with a different city" into "has
     * no address with that city" — {@code not(X)} wraps as a WHOLE leaf,
     * &exist;(&not;X), the engine's ANY-semantics.
     */
    private static boolean isBooleanConnective(TypedNativeCall c) {
        String key = c.callee().signatureKey();
        return com.legend.builtin.Pure.nativeNamed("and", key)
                || com.legend.builtin.Pure.nativeNamed("or", key);
    }

    /**
     * THE otherwise recognizer (one, shared with the demand scan): the
     * binding's {@code otherwise(^Inner(...), $row.<slot>)} call, looking
     * through a {@code toOne} wrap; {@code null} when the binding is not an
     * otherwise composition. The normalizer emits exactly this shape —
     * partial FIRST, fallback slot read second (canonical by construction).
     */
    static TypedNativeCall otherwiseOf(TypedSpec binding) {
        TypedSpec inner = binding;
        if (inner instanceof TypedNativeCall c && c.args().size() == 1
                && c.callee().qualifiedName().equals("meta::pure::functions::multiplicity::toOne")) {
            inner = c.args().get(0);
        }
        if (inner instanceof TypedNativeCall oc && oc.args().size() == 2
                && com.legend.builtin.Pure.nativeNamed("otherwise",
                        oc.callee().signatureKey())
                && oc.args().get(0)
                        instanceof com.legend.compiler.spec.typed.TypedNewInstance) {
            return oc;
        }
        return null;
    }

    /** An emptiness-family call at this node or anywhere beneath it. */
    private static boolean containsEmptinessFamily(TypedSpec n) {
        if (n instanceof TypedNativeCall c && isEmptinessFamily(c)) {
            return true;
        }
        for (TypedSpec ch : n.children()) {
            if (containsEmptinessFamily(ch)) {
                return true;
            }
        }
        return false;
    }

    private List<TypedSpec> rewriteAll(List<TypedSpec> ns) {
        List<TypedSpec> out = new ArrayList<>(ns.size());
        for (TypedSpec n : ns) {
            out.add(rewrite(n));
        }
        return out;
    }

    /**
     * Freshen a binding expression through THE unified row-read rewriter
     * ({@link Pipelines#rewriteRowReads}) — slot-condition rewriting and
     * binding rewriting share one implementation with a loud default, so
     * the demand scan and the substitution cannot drift. The row variable
     * maps to this instantiation's fresh var (stamped with the
     * MATERIALIZED row type); converted-slot sub-row reads become their
     * prefixed flat columns; stripped-slot reads and out-of-vocabulary
     * nodes are loud resolver bugs.
     */
    private TypedSpec renameRowVar(TypedSpec n) {
        return Pipelines.rewriteRowReads(n, target.sourceRowVar(),
                target.slotPrefixes(), target.strippedSlots(),
                v -> new TypedVariable(target.freshRowVar(),
                        new ExprType(target.rowType(), Multiplicity.Bounded.ONE)));
    }}
