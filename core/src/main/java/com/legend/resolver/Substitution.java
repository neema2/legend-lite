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
                  Map<TypedSpec, AggRead> aggReads,
                  com.legend.compiler.element.TypedFunction isNotEmptyCallee,
                  com.legend.compiler.element.TypedFunction equalCallee,
                  java.util.List<TypedSpec> rootTemporalDates,
                  Map<String, java.util.List<TypedSpec>> headTemporalDates,
                  Map<String, String> milestoneColumns,
                  boolean filterPosition, boolean nested) {}

    /** An aggregated-navigation column read: {@code column} on the joined
     * row; {@code zeroWhenEmpty} wraps count-family reads (COUNT over no
     * children is pure 0, but the LEFT join delivers NULL). */
    record AggRead(String column, boolean zeroWhenEmpty) {}

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
                    String readVar, Type.RelationType readRowType,
                    Map<String, String> targetMilestoneColumns) {

        AssocSub(String prefix, String targetRowVar,
                 Map<String, TypedSpec> targetBindings, String targetClassFqn,
                 java.util.Set<String> targetSlotAliases) {
            this(prefix, targetRowVar, targetBindings, targetClassFqn,
                    targetSlotAliases, Map.of(), null, null, Map.of());
        }

        AssocSub(String prefix, String targetRowVar,
                 Map<String, TypedSpec> targetBindings, String targetClassFqn,
                 java.util.Set<String> targetSlotAliases,
                 Map<String, String> targetSlotPrefixes) {
            this(prefix, targetRowVar, targetBindings, targetClassFqn,
                    targetSlotAliases, targetSlotPrefixes, null, null, Map.of());
        }

        AssocSub(String prefix, String targetRowVar,
                 Map<String, TypedSpec> targetBindings, String targetClassFqn,
                 java.util.Set<String> targetSlotAliases,
                 Map<String, String> targetSlotPrefixes,
                 String readVar, Type.RelationType readRowType) {
            this(prefix, targetRowVar, targetBindings, targetClassFqn,
                    targetSlotAliases, targetSlotPrefixes, readVar, readRowType,
                    Map.of());
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
        // a MILESTONED property function ($o.product(%d)) is a property
        // step whose temporal arguments the demand scan collects separately
        if (n instanceof com.legend.compiler.spec.typed.TypedMilestonedAccess ma) {
            if (ma.source() instanceof TypedVariable v && v.name().equals(userVar)) {
                return java.util.List.of(ma.property());
            }
            java.util.List<String> inner = pathOf(ma.source(), userVar);
            if (inner == null) {
                return null;
            }
            java.util.List<String> out = new ArrayList<>(inner);
            out.add(ma.property());
            return out;
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
        AggRead aggRead = target.aggReads().get(n);
        if (aggRead != null) {
            TypedSpec read = new TypedPropertyAccess(
                    new TypedVariable(target.freshRowVar(),
                            new ExprType(target.rowType(), Multiplicity.Bounded.ONE)),
                    aggRead.column(), n.info());
            if (!aggRead.zeroWhenEmpty()) {
                return read;
            }
            return new TypedIf(
                    new TypedNativeCall(target.isNotEmptyCallee(), List.of(read),
                            new ExprType(Type.Primitive.BOOLEAN,
                                    Multiplicity.Bounded.ONE)),
                    read,
                    java.util.Optional.of(new com.legend.compiler.spec.typed
                            .TypedCInteger(0L, new ExprType(Type.Primitive.INTEGER,
                                    Multiplicity.Bounded.ONE))),
                    n.info());
        }
        // TO-MANY navigation under an emptiness call: correlated EXISTS —
        // the target pipeline filtered by the association condition (parent
        // reads become the FREE outer row var, resolved through the
        // lowerer's enclosing-scope channel), the user predicate substituted
        // over the target's bindings. §133's single form.
        if (n instanceof TypedNativeCall call && !call.args().isEmpty()) {
            java.util.List<String> headPath = pathOf(call.args().get(0), target.userVar());
            // exists over an EMBEDDED (same-row) head whose predicate reads
            // only embedded leaves: the predicate applies DIRECTLY over the
            // parent row's columns (engine: BOND_DETAILS like 'Bond%' — no
            // join, no EXISTS; SQL's NULL propagation supplies the absent
            // case). Wins over the otherwise-fallback ExistsSub: per-leaf
            // dispatch, the embedded partial owns its mapped leaves.
            if (headPath != null && headPath.size() == 1 && isEmptinessFamily(call)
                    && call.args().size() == 2
                    && com.legend.builtin.Pure.nativeNamed("exists",
                            call.callee().signatureKey())
                    && call.args().get(1) instanceof TypedLambda pl
                    && pl.parameters().size() == 1) {
                var partial = embeddedPartialOf(
                        target.bindings().get(headPath.get(0)));
                if (partial != null && predLeavesIn(pl, partial)) {
                    return rewriteEmbeddedExists(pl, partial);
                }
            }
            if (headPath != null && headPath.size() == 1
                    && target.existsSubs().containsKey(headPath.get(0))
                    && isEmptinessFamily(call)) {
                return rewriteExists(call, target.existsSubs().get(headPath.get(0)));
            }
        }
        // COLLECTION-position crossing under contains/in: set MEMBERSHIP —
        // the engine's golden is EXISTS with an equality on the member
        // (testContainsOnToManyProperty: `exists(select 1 … FIRSTNAME =
        // 'John')`). Only a BARE to-many read matches (a toOne-flattened
        // crossing is scalar and takes the join row semantics).
        if (n instanceof TypedNativeCall mc && mc.args().size() == 2
                && target.equalCallee() != null) {
            String mkey = mc.callee().signatureKey();
            boolean isContains = com.legend.builtin.Pure.nativeNamed("contains", mkey);
            boolean isIn = com.legend.builtin.Pure.nativeNamed("in", mkey);
            if (isContains || isIn) {
                TypedSpec coll = isContains ? mc.args().get(0) : mc.args().get(1);
                TypedSpec needle = isContains ? mc.args().get(1) : mc.args().get(0);
                java.util.List<String> cp = coll
                        instanceof TypedPropertyAccess ? pathOf(coll, target.userVar()) : null;
                if (cp != null && cp.size() == 2
                        && target.existsSubs().containsKey(cp.get(0))
                        && target.existsSubs().get(cp.get(0)).toMany()) {
                    return rewriteMembershipExists(
                            target.existsSubs().get(cp.get(0)), cp.get(1), needle);
                }
            }
        }
        // NEGATION ISOLATION over a to-many crossing (AUDIT 9 — the engine
        // testInNegated golden is `NOT X OR <read> IS NULL` over a bare
        // LEFT JOIN with per-row duplicate parents; the crossing itself
        // substitutes to the JOINED column through the ordinary path
        // funnel): a not-leaf whose crossing read is NULL — no child row,
        // or a null child value — passes. Explicit emptiness calls inside
        // a not are ¬∃ over the semi-join, handled by their own arm.
        if (n instanceof TypedNativeCall lc
                && lc.info().type() == Type.Primitive.BOOLEAN
                && com.legend.builtin.Pure.nativeNamed("not",
                        lc.callee().signatureKey())
                && !containsEmptinessFamily(lc)
                && target.filterPosition()) {
            TypedSpec read = toManyCrossingRead(lc);
            if (read != null) {
                TypedSpec readInner = rewrite(read);
                TypedSpec notInner = new TypedNativeCall(lc.callee(),
                        rewriteAll(lc.args()), lc.info());
                return new TypedIf(
                        new TypedNativeCall(target.isNotEmptyCallee(),
                                List.of(readInner),
                                new ExprType(Type.Primitive.BOOLEAN,
                                        Multiplicity.Bounded.ONE)),
                        notInner,
                        java.util.Optional.of(new TypedCBoolean(true,
                                new ExprType(Type.Primitive.BOOLEAN,
                                        Multiplicity.Bounded.ONE))),
                        new ExprType(Type.Primitive.BOOLEAN,
                                Multiplicity.Bounded.ONE));
            }
        }
        java.util.List<String> path = pathOf(n, target.userVar());
        if (path != null && path.size() == 2) {
            return rewritePath(path.get(0), path.get(1), n);
        }
        if (path != null && path.size() > 2) {
            // EMBEDDED milestone struct: the embedded instance SHARES the
            // owner's row — $p.embedded.milestoning.from reads the OWNER
            // table's milestone column
            if (path.size() == 3 && path.get(1).equals("milestoning")
                    && embeddedPartialOf(target.bindings().get(path.get(0))) != null
                    && target.milestoneColumns().containsKey(path.get(2))) {
                return milestoneColumnRead(
                        target.milestoneColumns().get(path.get(2)),
                        target.freshRowVar(), target.rowType(), "", n);
            }
            // TARGET milestone struct: $p.assoc.milestoning.from reads the
            // TARGET table's milestone column, prefixed on the joined row
            if (path.size() == 3 && path.get(1).equals("milestoning")
                    && target.assocs().containsKey(path.get(0))) {
                AssocSub a2 = target.assocs().get(path.get(0));
                String col2 = a2.targetMilestoneColumns().get(path.get(2));
                if (col2 != null) {
                    return milestoneColumnRead(col2,
                            a2.readVar() != null ? a2.readVar() : target.freshRowVar(),
                            a2.readRowType() != null ? a2.readRowType()
                                    : target.rowType(),
                            a2.readVar() != null ? "" : a2.prefix(), n);
                }
            }
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
                // GENERATED temporal-context property: $p.businessDate /
                // $p.processingDate reads back the fetch's context date
                if ((prop.equals("businessDate") || prop.equals("processingDate"))
                        && !target.rootTemporalDates().isEmpty()) {
                    return contextDate(target.rootTemporalDates(), prop);
                }
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
            case com.legend.compiler.spec.typed.TypedMilestonedAccess ma ->
                    new com.legend.compiler.spec.typed.TypedMilestonedAccess(
                            rewrite(ma.source()), ma.property(),
                            rewriteAll(ma.dates()), ma.sweep(), ma.info());
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
        // the generated milestone STRUCT: $p.milestoning.from/.thru reads
        // the MAIN table's milestone column
        if (head.equals("milestoning")
                && target.milestoneColumns().containsKey(leaf)) {
            return milestoneColumnRead(target.milestoneColumns().get(leaf),
                    target.freshRowVar(), target.rowType(), "", original);
        }
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
            // GENERATED temporal-context property on the TARGET instance:
            // the head's explicit property-function date wins, else the
            // propagated root context date
            if (leaf.equals("businessDate") || leaf.equals("processingDate")) {
                java.util.List<TypedSpec> dates = target.headTemporalDates()
                        .getOrDefault(head, target.rootTemporalDates());
                if (!dates.isEmpty()) {
                    return contextDate(dates, leaf);
                }
            }
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

    /**
     * Implicit EXISTS (plangen F1): a boolean LEAF crossing a to-many
     * association wraps — EXISTS(target WHERE assoc-corr AND leaf'), the
     * leaf's crossing reads rewritten onto the subquery row, everything
     * else staying correlated to the outer row.
     */

    /** A milestone-column read off the (possibly prefixed) row. */
    private static TypedSpec milestoneColumnRead(String column, String rowVar,
            Type.RelationType row, String prefix, TypedSpec original) {
        String name = prefix + column;
        for (Type.Column c : row.columns()) {
            if (c.name().equalsIgnoreCase(name)) {
                return new TypedPropertyAccess(
                        new TypedVariable(rowVar,
                                new ExprType(row, Multiplicity.Bounded.ONE)),
                        c.name(), new ExprType(c.type(), c.multiplicity()));
            }
        }
        // LOUD (audit 10): a read of a column absent from the row would
        // surface only as a SQL binder error
        throw new NotImplementedException("milestone column '" + name
                + "' is not on the substitution row");
    }

    /** A bi-temporal context carries (processingDate, businessDate) — the
     * generated property picks its own; a single date serves either. */
    private static TypedSpec contextDate(java.util.List<TypedSpec> dates, String prop) {
        return dates.size() == 2 && prop.equals("businessDate")
                ? dates.get(1) : dates.get(0);
    }

    /** {@code contains($p.head.leaf, v)} / {@code in(v, $p.head.leaf)}:
     * EXISTS(child WHERE assoc-corr AND leaf = v) — the correlated child
     * extent filtered by the equality; outer reads in the needle stay
     * correlated through this substitution. */
    private TypedSpec rewriteMembershipExists(ExistsSub ex, String leaf,
            TypedSpec needle) {
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
        TypedSpec leafBinding = ex.targetBindings().get(leaf);
        if (leafBinding == null) {
            throw new MappingResolutionException("property '" + leaf
                    + "' of class '" + ex.targetClassFqn()
                    + "' has no binding in mapping '" + target.mappingFqn()
                    + "' (membership crossing leaf)", ex.targetClassFqn());
        }
        TypedSpec eq = new TypedNativeCall(target.equalCallee(),
                List.of(leafBinding, rewrite(needle)),
                new ExprType(Type.Primitive.BOOLEAN, Multiplicity.Bounded.ONE));
        TypedLambda memberPred = new TypedLambda(List.of(ex.targetRowVar()),
                List.of(eq), predType);
        TypedSpec rel = new com.legend.compiler.spec.typed.TypedFilter(
                new com.legend.compiler.spec.typed.TypedFilter(
                        ex.targetPipeline(), corr, ex.targetPipeline().info()),
                memberPred, ex.targetPipeline().info());
        return new TypedNativeCall(target.isNotEmptyCallee(), List.of(rel),
                new ExprType(Type.Primitive.BOOLEAN, Multiplicity.Bounded.ONE));
    }

    /** The first {@code $p.head.leaf} read inside a boolean leaf whose
     * head is a TO-MANY association crossing; null when none. (A leaf
     * with several crossing reads null-guards only the first — engine
     * isolation guards the read it isolates.) */
    private TypedSpec toManyCrossingRead(TypedSpec n) {
        java.util.List<String> path = pathOf(n, target.userVar());
        if (path != null && path.size() == 2
                && target.existsSubs().containsKey(path.get(0))
                && target.existsSubs().get(path.get(0)).toMany()) {
            return n;
        }
        for (TypedSpec c : n.children()) {
            TypedSpec r = toManyCrossingRead(c);
            if (r != null) {
                return r;
            }
        }
        return null;
    }

    /** The embedded ctor of a binding: a bare {@code ^Inner(...)} (with
     * toOne look-through) or an otherwise composition's partial. */
    private static com.legend.compiler.spec.typed.TypedNewInstance embeddedPartialOf(
            TypedSpec binding) {
        if (binding == null) {
            return null;
        }
        TypedSpec inner = binding;
        if (inner instanceof TypedNativeCall c && c.args().size() == 1
                && c.callee().qualifiedName().equals(
                        "meta::pure::functions::multiplicity::toOne")) {
            inner = c.args().get(0);
        }
        var ow = otherwiseOf(inner);
        if (ow != null) {
            inner = ow.args().get(0);
        }
        return inner instanceof com.legend.compiler.spec.typed.TypedNewInstance ni
                ? ni : null;
    }

    /** Every property read on the predicate's param resolves in the partial. */
    private boolean predLeavesIn(TypedLambda pl,
            com.legend.compiler.spec.typed.TypedNewInstance partial) {
        java.util.Set<java.util.List<String>> paths = new java.util.LinkedHashSet<>();
        for (TypedSpec b : pl.body()) {
            collectParamPaths(b, pl.parameters().get(0), paths);
        }
        if (paths.isEmpty()) {
            return false;
        }
        for (java.util.List<String> path : paths) {
            if (path.size() != 1 || !partial.properties().containsKey(path.get(0))) {
                return false;
            }
        }
        return true;
    }

    private static void collectParamPaths(TypedSpec n, String var,
            java.util.Set<java.util.List<String>> out) {
        java.util.List<String> p = pathOf(n, var);
        if (p != null) {
            out.add(p);
        }
        if (n instanceof TypedLambda l && l.parameters().contains(var)) {
            return;
        }
        for (TypedSpec c : n.children()) {
            collectParamPaths(c, var, out);
        }
    }

    /** Substitute the predicate over the PARENT row: {@code $b.prop} becomes
     * the embedded partial's binding expression; everything else (outer
     * reads) runs through THIS substitution. */
    private TypedSpec rewriteEmbeddedExists(TypedLambda pl,
            com.legend.compiler.spec.typed.TypedNewInstance partial) {
        TypedSpec body = substEmbeddedReads(
                pl.body().get(pl.body().size() - 1), pl.parameters().get(0), partial);
        return rewrite(body);
    }

    private TypedSpec substEmbeddedReads(TypedSpec n, String var,
            com.legend.compiler.spec.typed.TypedNewInstance partial) {
        java.util.List<String> p = pathOf(n, var);
        if (p != null && p.size() == 1) {
            return renameRowVar(partial.properties().get(p.get(0)));
        }
        if (n instanceof TypedNativeCall c) {
            return new TypedNativeCall(c.callee(),
                    c.args().stream().map(a -> substEmbeddedReads(a, var, partial))
                            .toList(), c.info());
        }
        if (n instanceof TypedPropertyAccess pa) {
            return new TypedPropertyAccess(
                    substEmbeddedReads(pa.source(), var, partial),
                    pa.property(), pa.info());
        }
        if (n instanceof TypedIf i) {
            return new TypedIf(substEmbeddedReads(i.condition(), var, partial),
                    substEmbeddedReads(i.thenBranch(), var, partial),
                    i.elseBranch().map(b -> substEmbeddedReads(b, var, partial)),
                    i.info());
        }
        if (n instanceof TypedCollection c) {
            return new TypedCollection(
                    c.elements().stream().map(e -> substEmbeddedReads(e, var, partial))
                            .toList(), c.info());
        }
        return n;
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
                java.util.Set.of(), Map.of(), Map.of(), null, null,
                java.util.List.of(), Map.of(), Map.of(), true, true));
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
                    java.util.Set.of(), Map.of(), Map.of(), null, null,
                    java.util.List.of(), Map.of(), Map.of(), true, true));
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
