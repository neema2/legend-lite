package com.legend.resolver;

import com.legend.builtin.Pure;
import com.legend.compiler.element.TypedFunction;
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
import com.legend.compiler.spec.typed.TypedFilter;
import com.legend.compiler.spec.typed.TypedTableReference;
import com.legend.compiler.spec.typed.TypedFuncCol;
import com.legend.compiler.spec.typed.TypedIf;
import com.legend.compiler.spec.typed.TypedLambda;
import com.legend.compiler.spec.typed.TypedLimit;
import com.legend.compiler.spec.typed.TypedMap;
import com.legend.compiler.spec.typed.TypedMilestonedAccess;
import com.legend.compiler.spec.typed.TypedNativeCall;
import com.legend.compiler.spec.typed.TypedNewInstance;
import com.legend.compiler.spec.typed.TypedNewInstanceCast;
import com.legend.compiler.spec.typed.TypedProject;
import com.legend.compiler.spec.typed.TypedPropertyAccess;
import com.legend.compiler.spec.typed.TypedSpec;
import com.legend.compiler.spec.typed.TypedVariable;
import com.legend.error.MappingResolutionException;
import com.legend.error.NotImplementedException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
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

    /** Registry-key prefix for subType(@Sub) binding tables — never a
     * property name, so it cannot collide with navigation heads. */
    static final String SUBTYPE_KEY = "subType$";

    /** HOW READS LAND ON THE ROW: the user lambda's variable, the fresh
     * row var replacing it, the class/mapping identity, the binding table
     * written over {@code sourceRowVar}, the materialized row type, and
     * the slot conversion state (stripped = loud; prefixed = converted). */
    record RowScope(String userVar, String freshRowVar, String classFqn,
                    String mappingFqn, String sourceRowVar,
                    Map<String, TypedSpec> bindings, Type.RelationType rowType,
                    Set<String> strippedSlots,
                    Map<String, String> slotPrefixes,
                    Map<String, String> milestoneColumns) {}

    /** THE DEMAND-REGISTERED MATERIALS: association/navigate heads, the
     * honest-error end names, the exists materials, the aggregate reads
     * (identity-keyed), and the boolean-machinery callees. */
    record Registries(Map<String, AssocSub> assocs,
                      Set<String> assocEnds,
                      Map<String, ExistsSub> existsSubs,
                      Map<TypedSpec, AggRead> aggReads,
                      TypedFunction isNotEmptyCallee,
                      TypedFunction equalCallee) {

        /** Inner substitutions (exists/pred rewrites) carry NO registries:
         * nested navigation stays loud by construction. */
        static final Registries NONE = new Registries(Map.of(),
                Set.of(), Map.of(), Map.of(), null, null);
    }

    /** The temporal reads the substitution serves (generated-date
     * properties): the root context's point dates and the per-chain
     * property-function dates — legacy list shapes at this boundary. */
    record TemporalView(List<TypedSpec> rootTemporalDates,
                        Map<String, List<TypedSpec>> headTemporalDates) {

        static final TemporalView NONE =
                new TemporalView(List.of(), Map.of());
    }

    /** The instantiation being substituted into. Composed of the row
     * scope, the registries and the temporal view; the flat accessors
     * below keep the rewrite body reading naturally. */
    record Target(RowScope row, Registries regs, TemporalView temporal,
                  boolean filterPosition, boolean nested) {

        String userVar() {
            return row.userVar();
        }

        String freshRowVar() {
            return row.freshRowVar();
        }

        String classFqn() {
            return row.classFqn();
        }

        String mappingFqn() {
            return row.mappingFqn();
        }

        String sourceRowVar() {
            return row.sourceRowVar();
        }

        Map<String, TypedSpec> bindings() {
            return row.bindings();
        }

        Type.RelationType rowType() {
            return row.rowType();
        }

        Set<String> strippedSlots() {
            return row.strippedSlots();
        }

        Map<String, String> slotPrefixes() {
            return row.slotPrefixes();
        }

        Map<String, String> milestoneColumns() {
            return row.milestoneColumns();
        }

        Map<String, AssocSub> assocs() {
            return regs.assocs();
        }

        Set<String> assocEnds() {
            return regs.assocEnds();
        }

        Map<String, ExistsSub> existsSubs() {
            return regs.existsSubs();
        }

        Map<TypedSpec, AggRead> aggReads() {
            return regs.aggReads();
        }

        TypedFunction isNotEmptyCallee() {
            return regs.isNotEmptyCallee();
        }

        TypedFunction equalCallee() {
            return regs.equalCallee();
        }

        List<TypedSpec> rootTemporalDates() {
            return temporal.rootTemporalDates();
        }

        Map<String, List<TypedSpec>> headTemporalDates() {
            return temporal.headTemporalDates();
        }
    }

    /** An aggregated-navigation column read: {@code column} on the joined
     * row; {@code zeroWhenEmpty} wraps count-family reads (COUNT over no
     * children is pure 0, but the LEFT join delivers NULL). */
    record AggRead(String column, boolean zeroWhenEmpty) {}

    /** A to-many association head consumable under exists/isEmpty/isNotEmpty.
     * {@code targetSlotPrefixes}: the target's DEMANDED slots, materialized
     * into its pipeline (inner-predicate leaves through the target's own
     * joins — N1); un-materialized aliases stay stripped-loud. */
    record ExistsSub(TypedSpec targetPipeline, TypedLambda orientedCond,
                     String targetRowVar, Map<String, TypedSpec> targetBindings,
                     Type.RelationType targetRow, String targetClassFqn,
                     Set<String> targetSlotAliases,
                     Map<String, String> targetSlotPrefixes, boolean toMany,
                     TypedSpec scalarPipeline, Type.RelationType scalarRow,
                     Registries innerRegs) {

        ExistsSub(TypedSpec targetPipeline, TypedLambda orientedCond,
                  String targetRowVar, Map<String, TypedSpec> targetBindings,
                  Type.RelationType targetRow, String targetClassFqn,
                  Set<String> targetSlotAliases,
                  Map<String, String> targetSlotPrefixes, boolean toMany,
                  TypedSpec scalarPipeline, Type.RelationType scalarRow) {
            this(targetPipeline, orientedCond, targetRowVar, targetBindings,
                    targetRow, targetClassFqn, targetSlotAliases,
                    targetSlotPrefixes, toMany, scalarPipeline, scalarRow,
                    Registries.NONE);
        }

        /** R1 (recursive scope demand): nested predicate scopes carry
         * their OWN registered materials instead of staying loud. */
        ExistsSub withInnerRegs(Registries r) {
            return new ExistsSub(targetPipeline, orientedCond, targetRowVar,
                    targetBindings, targetRow, targetClassFqn,
                    targetSlotAliases, targetSlotPrefixes, toMany,
                    scalarPipeline, scalarRow, r);
        }

        ExistsSub(TypedSpec targetPipeline, TypedLambda orientedCond,
                  String targetRowVar, Map<String, TypedSpec> targetBindings,
                  Type.RelationType targetRow, String targetClassFqn,
                  Set<String> targetSlotAliases, boolean toMany) {
            this(targetPipeline, orientedCond, targetRowVar, targetBindings,
                    targetRow, targetClassFqn, targetSlotAliases, Map.of(), toMany,
                    targetPipeline, targetRow);
        }

        ExistsSub(TypedSpec targetPipeline, TypedLambda orientedCond,
                  String targetRowVar, Map<String, TypedSpec> targetBindings,
                  Type.RelationType targetRow, String targetClassFqn,
                  Set<String> targetSlotAliases,
                  Map<String, String> targetSlotPrefixes, boolean toMany) {
            this(targetPipeline, orientedCond, targetRowVar, targetBindings,
                    targetRow, targetClassFqn, targetSlotAliases,
                    targetSlotPrefixes, toMany, targetPipeline, targetRow);
        }
    }

    /**
     * A demanded association head: how its leaf bindings substitute.
     * {@code readVar}/{@code readRowType} override where the rewritten
     * reads attach — null means the chain's fresh row var (joined form);
     * an EXISTS-inner registration points them at the subquery row.
     */
    /** A materialized SUB-navigation of an association/navigate target:
     * the composed column prefix (relative to the OWNING target's row),
     * the sub-target's row var and BINDING table — multi-hop leaves
     * resolve through it (audit 12 F1: the property name is NOT a column
     * name). {@code children}: the NEXT level, self-similar — the walk is
     * hop-agnostic (the per-hop-count arms were the recurring bug seam). */
    record SubNav(String prefix, String rowVar, Map<String, TypedSpec> bindings,
                  Map<String, SubNav> children) {

        SubNav(String prefix, String rowVar, Map<String, TypedSpec> bindings) {
            this(prefix, rowVar, bindings, Map.of());
        }
    }

    record AssocSub(String prefix, String targetRowVar,
                    Map<String, TypedSpec> targetBindings, String targetClassFqn,
                    Set<String> targetSlotAliases,
                    Map<String, String> targetSlotPrefixes,
                    String readVar, Type.RelationType readRowType,
                    Map<String, String> targetMilestoneColumns,
                    Map<String, SubNav> subNavs) {

        AssocSub(String prefix, String targetRowVar,
                 Map<String, TypedSpec> targetBindings, String targetClassFqn,
                 Set<String> targetSlotAliases) {
            this(prefix, targetRowVar, targetBindings, targetClassFqn,
                    targetSlotAliases, Map.of(), null, null, Map.of(), Map.of());
        }

        AssocSub(String prefix, String targetRowVar,
                 Map<String, TypedSpec> targetBindings, String targetClassFqn,
                 Set<String> targetSlotAliases,
                 Map<String, String> targetSlotPrefixes) {
            this(prefix, targetRowVar, targetBindings, targetClassFqn,
                    targetSlotAliases, targetSlotPrefixes, null, null, Map.of(),
                    Map.of());
        }

        AssocSub(String prefix, String targetRowVar,
                 Map<String, TypedSpec> targetBindings, String targetClassFqn,
                 Set<String> targetSlotAliases,
                 Map<String, String> targetSlotPrefixes,
                 String readVar, Type.RelationType readRowType) {
            this(prefix, targetRowVar, targetBindings, targetClassFqn,
                    targetSlotAliases, targetSlotPrefixes, readVar, readRowType,
                    Map.of(), Map.of());
        }

        AssocSub(String prefix, String targetRowVar,
                 Map<String, TypedSpec> targetBindings, String targetClassFqn,
                 Set<String> targetSlotAliases,
                 Map<String, String> targetSlotPrefixes,
                 String readVar, Type.RelationType readRowType,
                 Map<String, String> targetMilestoneColumns) {
            this(prefix, targetRowVar, targetBindings, targetClassFqn,
                    targetSlotAliases, targetSlotPrefixes, readVar, readRowType,
                    targetMilestoneColumns, Map.of());
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
    /** Rewrite a lambda's BODY under this scope, KEEPING its parameters —
     * for constructed relation material flowing through an outer
     * correlation pass (the params bind the material's own rows). */
    TypedLambda rewriteLambdaBodyOnly(TypedLambda lambda) {
        List<TypedSpec> body = new ArrayList<>(lambda.body().size());
        for (TypedSpec stmt : lambda.body()) {
            body.add(rewrite(stmt));
        }
        return new TypedLambda(lambda.parameters(), body, lambda.info());
    }

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
        List<String> p = pathOf(n, userVar);
        return p != null && p.size() == 1 ? p.get(0) : null;
    }

    /**
     * THE path funnel: the full property chain when {@code n}'s receiver
     * chain bottoms at the user's lambda variable ({@code $p.employer.legal}
     * &rArr; {@code [employer, legal]}); {@code null} otherwise. DemandScan
     * and the rewrite share this single extractor.
     */
    static List<String> pathOf(TypedSpec n, String userVar) {
        // toOne() look-through: $p.employer->toOne().legal is the idiomatic
        // spelling after an optional navigation — the coercion is
        // multiplicity-only and transparent to the path (audit R3).
        if (n instanceof TypedNativeCall c && c.args().size() == 1
                && c.callee().qualifiedName().equals("meta::pure::functions::multiplicity::toOne")) {
            return pathOf(c.args().get(0), userVar);
        }
        // ->map(l|$l.prop...) is the auto-map spelling of the property
        // path — flatten for the demand scan exactly as the rewrite does
        // (ONE funnel: scan and substitution must not drift)
        if (n instanceof TypedMap m
                && m.mapper().parameters().size() == 1
                && m.mapper().body().size() == 1
                && pathOf(m.mapper().body().get(0),
                        m.mapper().parameters().get(0)) != null) {
            return pathOf(inlineParam(m.mapper().body().get(0),
                    m.mapper().parameters().get(0), m.source()), userVar);
        }
        // a MILESTONED property function ($o.product(%d)) is a property
        // step whose temporal arguments the demand scan collects separately
        if (n instanceof TypedMilestonedAccess ma) {
            if (ma.source() instanceof TypedVariable v && v.name().equals(userVar)) {
                return List.of(ma.property());
            }
            List<String> inner = pathOf(ma.source(), userVar);
            if (inner == null) {
                return null;
            }
            List<String> out = new ArrayList<>(inner);
            out.add(ma.property());
            return out;
        }
        // ->subType(@Sub).prop: canonicalize to the class-qualified
        // subtype-dispatch leaf (ClassMapping.subTypeColumn) — the union
        // synthesis projects it thread-local, so the read is an ordinary
        // (nav) leaf; a single-element result (cast on the head var) is
        // consumed by the SUBTYPE_KEY switch arm instead
        if (n instanceof TypedPropertyAccess pa0
                && pa0.source() instanceof TypedNativeCall sc
                && sc.callee().qualifiedName()
                        .equals("meta::pure::functions::lang::subType")
                && !sc.args().isEmpty()
                && sc.info().type() instanceof Type.ClassType sct) {
            if (sc.args().get(0) instanceof TypedVariable v0
                    && v0.name().equals(userVar)) {
                return List.of(com.legend.model.ClassMapping
                        .subTypeColumn(sct.fqn(), pa0.property()));
            }
            List<String> inner = pathOf(sc.args().get(0), userVar);
            if (inner == null) {
                return null;
            }
            List<String> out = new ArrayList<>(inner);
            out.add(com.legend.model.ClassMapping
                    .subTypeColumn(sct.fqn(), pa0.property()));
            return out;
        }
        if (!(n instanceof TypedPropertyAccess pa)) {
            return null;
        }
        if (pa.source() instanceof TypedVariable v && v.name().equals(userVar)) {
            return List.of(pa.property());
        }
        List<String> inner = pathOf(pa.source(), userVar);
        if (inner == null) {
            return null;
        }
        List<String> out = new ArrayList<>(inner);
        out.add(pa.property());
        return out;
    }

    /** The CALL-position arms (emptiness/exists family, membership
     * contains/in, negation isolation) — null when none matches and
     * the walk continues (their original fall-through). Order within
     * is load-bearing. */
    private TypedSpec rewriteCallArms(TypedSpec n) {
        if (n instanceof TypedNativeCall call && !call.args().isEmpty()) {
            List<String> headPath = pathOf(call.args().get(0), target.userVar());
            // exists over an EMBEDDED (same-row) head whose predicate reads
            // only embedded leaves: the predicate applies DIRECTLY over the
            // parent row's columns (engine: BOND_DETAILS like 'Bond%' — no
            // join, no EXISTS; SQL's NULL propagation supplies the absent
            // case). Wins over the otherwise-fallback ExistsSub: per-leaf
            // dispatch, the embedded partial owns its mapped leaves.
            if (headPath != null && headPath.size() == 1 && isEmptinessFamily(call)
                    && call.args().size() == 2
                    && Pure.nativeNamed("exists",
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
                return rewriteExists(call, target.existsSubs().get(headPath.get(0)),
                        List.of());
            }
            // CLASS-TYPED LEAF: isNotEmpty($p.a.b) where b is a navigation
            // step on the chain target — the DOTTED-path material fires a
            // correlated EXISTS on the exploded chain row (engine: semi-join
            // + key null check)
            if (headPath != null && headPath.size() >= 2 && isEmptinessFamily(call)
                    && target.existsSubs().containsKey(String.join(".", headPath))) {
                return rewriteExists(call,
                        target.existsSubs().get(String.join(".", headPath)),
                        List.of());
            }
            // FILTER-WRAPPED emptiness: isEmpty/exists($p.head->filter(f)
            // [->filter(g)...], pred?) — the filters merge into the
            // correlated set (engine: filter-in-chain parks on the
            // navigation target)
            if (isEmptinessFamily(call)) {
                TypedSpec exArg = call.args().get(0);
                List<TypedLambda> chainPreds = new ArrayList<>();
                while (exArg instanceof TypedFilter tf) {
                    chainPreds.add(tf.predicate());
                    exArg = tf.source();
                }
                if (!chainPreds.isEmpty()) {
                    List<String> fp = pathOf(exArg, target.userVar());
                    if (fp != null && fp.size() == 1
                            && target.existsSubs().containsKey(fp.get(0))) {
                        return rewriteExists(call,
                                target.existsSubs().get(fp.get(0)), chainPreds);
                    }
                }
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
            boolean isContains = Pure.nativeNamed("contains", mkey);
            boolean isIn = Pure.nativeNamed("in", mkey);
            if (isContains || isIn) {
                TypedSpec coll = isContains ? mc.args().get(0) : mc.args().get(1);
                TypedSpec needle = isContains ? mc.args().get(1) : mc.args().get(0);
                List<String> cp = coll
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
                && Pure.nativeNamed("not",
                        lc.callee().signatureKey())
                && !containsEmptinessFamily(lc)
                && target.filterPosition()) {
            TypedSpec read = toManyCrossingRead(lc);
            if (read != null) {
                TypedSpec readInner = rewrite(read);
                TypedSpec notInner = new TypedNativeCall(lc.callee(),
                        rewriteAll(lc.args()), lc.info());
                // a FILTER-LIFTED head ('#f' synthetic): the engine parks
                // the chain filter in the outer WHERE — a parent with NO
                // MATCHING child FAILS the enclosing filter (audit 14 pin:
                // testChainedFiltersQuery golden conjoins LASTNAME='Smith');
                // plain chains keep the engine's ANY-semantics pass-through
                // (testInNegated: NOT X OR read IS NULL)
                List<String> crossPath = pathOf(read, target.userVar());
                boolean filteredHead = crossPath != null
                        && SyntheticHeads.isFiltered(crossPath.get(0));
                return new TypedIf(
                        new TypedNativeCall(target.isNotEmptyCallee(),
                                List.of(readInner),
                                new ExprType(Type.Primitive.BOOLEAN,
                                        Multiplicity.Bounded.ONE)),
                        notInner,
                        Optional.of(new TypedCBoolean(!filteredHead,
                                new ExprType(Type.Primitive.BOOLEAN,
                                        Multiplicity.Bounded.ONE))),
                        new ExprType(Type.Primitive.BOOLEAN,
                                Multiplicity.Bounded.ONE));
            }
        }
        return null;
    }

    /** A MULTI-HOP path read (size > 2): milestone structs, the
     * hop-agnostic SubNav walk, chained association leaves, embedded
     * ctor drills — always resolves or throws loud. */
    private TypedSpec rewriteMultiHop(List<String> path, TypedSpec n) {
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
            // MULTI-HOP through a NAVIGATE-SLOT head ($a.b...z.pk where b
            // is a class-typed Join PM slot and each further hop a slot of
            // the previous target): the leaf resolves through the DEEPEST
            // SUB-TARGET'S BINDING (column renames honored — audit 12 F1:
            // the property name is not a physical column), read as the
            // COMPOSED prefixed column on the joined row (engine: per-hop
            // findPropertyMapping). The walk is hop-agnostic — the SubNav
            // tree carries prefixes composed per level.
            if (target.assocs().containsKey(path.get(0))) {
                AssocSub a3 = target.assocs().get(path.get(0));
                SubNav sub = a3.subNavs().get(path.get(1));
                int hop = 2;
                while (sub != null && hop + 1 < path.size()) {
                    sub = sub.children().get(path.get(hop));
                    hop++;
                }
                if (sub != null) {
                    String leaf = path.get(path.size() - 1);
                    String hops = String.join(".",
                            path.subList(0, path.size() - 1));
                    TypedSpec leafBinding = sub.bindings().get(leaf);
                    if (leafBinding == null) {
                        throw new MappingResolutionException("property '"
                                + leaf + "' of nested navigation '" + hops
                                + "' is not mapped in mapping '"
                                + target.mappingFqn() + "'", target.classFqn());
                    }
                    TypedSpec inner3 = leafBinding;
                    if (inner3 instanceof TypedNativeCall c3
                            && c3.args().size() == 1
                            && c3.callee().qualifiedName().equals(
                                    "meta::pure::functions::multiplicity::toOne")) {
                        inner3 = c3.args().get(0);
                    }
                    if (inner3 instanceof TypedPropertyAccess pa3
                            && pa3.source() instanceof TypedVariable v3
                            && v3.name().equals(sub.rowVar())) {
                        return milestoneColumnRead(
                                sub.prefix() + pa3.property(),
                                a3.readVar() != null ? a3.readVar()
                                        : target.freshRowVar(),
                                a3.readRowType() != null ? a3.readRowType()
                                        : target.rowType(),
                                a3.readVar() != null ? "" : a3.prefix(), n);
                    }
                    throw new NotImplementedException("nested navigation leaf '"
                            + leaf + "' of '" + hops + "' is mapped by a"
                            + " non-column expression — not supported yet");
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
                if (inner instanceof TypedNewInstance ni
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
            // HEAD-JOIN + EMBEDDED TAIL ($p.employees.address.name — the
            // assoc TARGET maps 'address' as an embedded ctor): walk the
            // target's binding chain to the leaf expression, then emit
            // exactly like a plain assoc leaf (slot check, chain-prefix
            // rename). A leaf missing from an otherwise-partial falls
            // through loud below.
            if (target.assocs().containsKey(path.get(0))) {
                AssocSub ha = target.assocs().get(path.get(0));
                TypedSpec curT = ha.targetBindings().get(path.get(1));
                int h = 2;
                while (curT != null && h < path.size()) {
                    TypedSpec inner4 = curT;
                    if (inner4 instanceof TypedNativeCall c4
                            && c4.args().size() == 1
                            && c4.callee().qualifiedName().equals(
                                    "meta::pure::functions::multiplicity::toOne")) {
                        inner4 = c4.args().get(0);
                    }
                    var ow4 = otherwiseOf(inner4);
                    if (ow4 != null) {
                        inner4 = ow4.args().get(0);
                    }
                    if (inner4 instanceof TypedNewInstance ni4
                            && ni4.properties().containsKey(path.get(h))) {
                        curT = ni4.properties().get(path.get(h));
                        h++;
                    } else {
                        curT = null;
                    }
                }
                if (curT != null && !(curT instanceof TypedNewInstance)) {
                    return assocBindingRead(ha, path.get(path.size() - 1), curT);
                }
            }
            throw new NotImplementedException("multi-hop navigation "
                    + String.join(".", path) + " through an embedded/slot head"
                    + " is not supported yet");
    }

    /** A 1-HOP head read: bindings, generated temporal dates, honest
     * bare-head errors — resolves, throws loud, or (no match) NULL to
     * continue the walk. */
    private TypedSpec rewriteHeadProp(String prop, TypedSpec n) {
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
                if (inner instanceof TypedNewInstance
                        || inner.info().type()
                                instanceof Type.ClassType) {
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
                // VERSION SWEEP (allVersions / allVersionsInRange — the
                // root context is EMPTY): each version row's OWN
                // validity-start column IS its generated date (engine maps
                // the property to BUS_FROM / PROCESSING_IN / snapshot)
                if (prop.equals("businessDate") || prop.equals("processingDate")) {
                    String col = target.milestoneColumns().get(
                            prop.equals("processingDate")
                                    ? "genProcessingDate" : "genBusinessDate");
                    if (col != null) {
                        return milestoneColumnRead(col, target.freshRowVar(),
                                target.rowType(), "", n);
                    }
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
        return null;
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
                    Optional.of(new com.legend.compiler.spec.typed
                            .TypedCInteger(0L, new ExprType(Type.Primitive.INTEGER,
                                    Multiplicity.Bounded.ONE))),
                    n.info());
        }
        // TO-MANY navigation under an emptiness call: correlated EXISTS —
        // the target pipeline filtered by the association condition (parent
        // reads become the FREE outer row var, resolved through the
        // lowerer's enclosing-scope channel), the user predicate substituted
        // over the target's bindings. §133's single form.
        TypedSpec callArm = rewriteCallArms(n);
        if (callArm != null) {
            return callArm;
        }
        List<String> path = pathOf(n, target.userVar());
        if (path != null && path.size() == 2) {
            return rewritePath(path.get(0), path.get(1), n);
        }
        if (path != null && path.size() > 2) {
            return rewriteMultiHop(path, n);
        }
        String prop = propertyOnUserVar(n, target.userVar());
        if (prop != null) {
            TypedSpec headArm = rewriteHeadProp(prop, n);
            if (headArm != null) {
                return headArm;
            }
        }
        return switch (n) {
            case TypedPropertyAccess pa when filteredNavLeafRead(pa) != null ->
                    filteredNavLeafRead(pa);
            case TypedPropertyAccess pa when subTypeLeafRead(pa) != null ->
                    subTypeLeafRead(pa);
            case TypedVariable v when v.name().equals(target.userVar()) ->
                    throw new NotImplementedException(
                            "object-space use of the instance variable '$" + v.name()
                                    + "' other than property access is not supported yet");
            case TypedVariable v -> v;
            case TypedPropertyAccess pa -> new TypedPropertyAccess(
                    rewrite(pa.source()), pa.property(), pa.info());
            case TypedMilestonedAccess ma ->
                    new TypedMilestonedAccess(
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
            // ->map(l|...) over a navigation IS the auto-map spelling
            // ($f.employees->map(l|$l.lastName) == $f.employees.lastName,
            // the engine desugar): inline the mapper param with the source
            // and substitute the flattened expression
            case TypedMap m
                    when m.mapper().parameters().size() == 1
                    && m.mapper().body().size() == 1
                    && pathOf(m.mapper().body().get(0),
                            m.mapper().parameters().get(0)) != null ->
                    // ONLY a property path over the param is the auto-map
                    // spelling (audit 12 F4: ->map(b|1)->sum() collapsed to
                    // the constant — non-path bodies stay at the loud wall)
                    rewrite(inlineParam(m.mapper().body().get(0),
                            m.mapper().parameters().get(0), m.source()));
            // Literals: nothing to substitute.
            case TypedCString ignored -> n;
            case TypedCInteger ignored -> n;
            case TypedCFloat ignored -> n;
            case TypedCDecimal ignored -> n;
            case TypedCBoolean ignored -> n;
            case TypedCDate ignored -> n;
            case TypedEnumValue ignored -> n;
            // CONSTRUCTED RELATION MATERIAL (R2): a nested scope's rewrite
            // (innerRegs — R1a) builds exists/leaf relations whose nodes
            // then flow through the OUTER correlation re-pass. They are
            // resolved pipelines, not object-space expressions: structure
            // passes through; predicate lambdas still rewrite (outer-var
            // reads left verbatim by the inner scope correlate here).
            // OBJECT-SPACE filters (class-typed sources) stay loud below.
            case TypedTableReference ignored -> n;
            case TypedFilter f when f.source().info().type()
                    instanceof Type.RelationType ->
                    // body-only rewrite: the lambda's OWN param binds its
                    // relation row and must survive (rewriteLambda would
                    // rebind it to THIS scope's row var, orphaning reads)
                    new TypedFilter(rewrite(f.source()),
                            rewriteLambdaBodyOnly(f.predicate()), f.info());
            default -> {
                String shape = String.valueOf(n);
                throw new NotImplementedException(
                        "object-space expression node " + n.getClass().getSimpleName()
                                + " is not substitutable yet (H2 vocabulary):"
                                + " " + (shape.length() > 220
                                        ? shape.substring(0, 220) + "…" : shape));
            }
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
            if (inner instanceof TypedNewInstance ctor) {
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
                TypedSpec leafExpr = ((TypedNewInstance)
                        ow.args().get(0)).properties().get(leaf);
                if (leafExpr != null) {
                    return renameRowVar(leafExpr);
                }
                return assocLeaf(head, leaf);
            }
            if (inner instanceof TypedNewInstanceCast) {
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
        // A subtype-dispatch leaf whose class carries a MEMBERSHIP WITNESS
        // (partial membership) needs row RESTRICTION at this to-many
        // position (engine routes the navigation to conforming member sets
        // only). A FILTERED synthetic head IS the restriction (the
        // canonicalizer parked the witness pred on it); a PLAIN head means
        // the restriction was not routed — loud, a plain join would emit
        // WRONG ROWS (NULL-celled non-members surviving the explosion)
        if (com.legend.model.ClassMapping.isSubTypeColumn(leaf)
                && head.equals(SyntheticHeads.realHead(head))) {
            for (String k : a.targetBindings().keySet()) {
                String wPfx = com.legend.model.ClassMapping.witnessPrefixOf(k);
                if (wPfx != null && leaf.startsWith(wPfx)) {
                    throw new NotImplementedException("subType(@...) over a"
                            + " navigation whose target has PARTIAL membership"
                            + " (row restriction) is not supported yet");
                }
            }
        }
        TypedSpec leafBinding = a.targetBindings().get(leaf);
        if (leafBinding == null) {
            // GENERATED temporal-context property on the TARGET instance:
            // the head's explicit property-function date wins, else the
            // propagated root context date
            if (leaf.equals("businessDate") || leaf.equals("processingDate")) {
                List<TypedSpec> dates = target.headTemporalDates()
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
        return assocBindingRead(a, leaf, leafBinding);
    }

    /** The EMISSION half of an association-target read: slot flatten
     * checks, then the chain-prefix rename onto the read row. The leaf
     * expression may be a direct binding or a ctor-walked EMBEDDED leaf
     * (rewriteMultiHop's head-join + embedded-tail arm). */
    private TypedSpec assocBindingRead(AssocSub a, String leaf,
            TypedSpec leafBinding) {
        TypedSpec leafInner = leafBinding;
        if (leafInner instanceof TypedNativeCall lc && lc.args().size() == 1
                && lc.callee().qualifiedName().equals("meta::pure::functions::multiplicity::toOne")) {
            leafInner = lc.args().get(0);
        }
        if (leafInner instanceof TypedNewInstance) {
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
            Set<String> unconverted = new HashSet<>(a.targetSlotAliases());
            unconverted.removeAll(a.targetSlotPrefixes().keySet());
            if (Pipelines.referencesAliasOn(leafBinding, a.targetRowVar(), unconverted)) {
                throw new NotImplementedException("property '" + leaf + "' of class '"
                        + a.targetClassFqn() + "' is mapped through the target's own"
                        + " join slots; nested navigation joins are not supported"
                        + " in this position yet");
            }
            leafBinding = Pipelines.rewriteRowReads(leafBinding, a.targetRowVar(),
                    a.targetSlotPrefixes(), Set.of(),
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

    /** Replace reads of {@code param} with {@code source} — the auto-map
     * inliner ({@code ->map(l|$l.prop)} flattens to the property path). */
    static TypedSpec inlineParam(TypedSpec n, String param, TypedSpec source) {
        if (n instanceof TypedVariable v && v.name().equals(param)) {
            return source;
        }
        return switch (n) {
            case TypedVariable v -> v;
            case TypedPropertyAccess pa -> new TypedPropertyAccess(
                    inlineParam(pa.source(), param, source), pa.property(), pa.info());
            case TypedMilestonedAccess ma ->
                    new TypedMilestonedAccess(
                            inlineParam(ma.source(), param, source), ma.property(),
                            ma.dates().stream().map(d ->
                                    inlineParam(d, param, source)).toList(),
                            ma.sweep(), ma.info());
            case TypedNativeCall c -> new TypedNativeCall(c.callee(),
                    c.args().stream().map(a ->
                            inlineParam(a, param, source)).toList(), c.info());
            case TypedIf i -> new TypedIf(inlineParam(i.condition(), param, source),
                    inlineParam(i.thenBranch(), param, source),
                    i.elseBranch().map(e -> inlineParam(e, param, source)), i.info());
            case TypedCollection c -> new TypedCollection(
                    c.elements().stream().map(e ->
                            inlineParam(e, param, source)).toList(), c.info());
            case TypedCString ignored -> n;
            case TypedCInteger ignored -> n;
            case TypedCFloat ignored -> n;
            case TypedCDecimal ignored -> n;
            case TypedCBoolean ignored -> n;
            case TypedCDate ignored -> n;
            case TypedEnumValue ignored -> n;
            case com.legend.compiler.spec.typed.TypedTypeRef ignored -> n;
            case TypedLambda l -> {
                if (l.parameters().contains(param)) {
                    yield l;   // shadowing: substitution stops
                }
                // CAPTURE guard (audit 12 F5): a nested lambda whose param
                // collides with a free variable of the inlined source would
                // silently capture it
                for (String lp : l.parameters()) {
                    if (readsVariable(source, lp)) {
                        throw new NotImplementedException("auto-map mapper"
                                + " nests a lambda whose parameter '" + lp
                                + "' collides with the mapped source —"
                                + " rename the parameter");
                    }
                }
                yield new TypedLambda(l.parameters(), l.body().stream().map(b ->
                        inlineParam(b, param, source)).toList(), l.info());
            }
            default -> throw new NotImplementedException(
                    "auto-map mapper body node " + n.getClass().getSimpleName()
                            + " is not inlinable yet");
        };
    }

    /** Whether any {@code $var} read occurs in the subtree. */
    private static boolean readsVariable(TypedSpec n, String var) {
        if (n instanceof TypedVariable v && v.name().equals(var)) {
            return true;
        }
        for (TypedSpec c : n.children()) {
            if (readsVariable(c, var)) {
                return true;
            }
        }
        return false;
    }

    /** A bi-temporal context carries (processingDate, businessDate) — the
     * generated property picks its own; a single date serves either. */
    private static TypedSpec contextDate(List<TypedSpec> dates, String prop) {
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
        // Same 't'-shadowing class as rewriteExists (audit 18): in a
        // nested scope target.freshRowVar() IS the enclosing exists'
        // renamed var — an unfreshened corr binder named 't' captures the
        // parent-correlation reads created below. Freshen collision-driven,
        // rename the TARGET-side reads FIRST.
        String freshT = tVar;
        while (freshT.equals(target.freshRowVar())
                || freshT.equals(target.userVar())) {
            freshT = freshT + "_n";
        }
        final String tRenamed = freshT;
        List<TypedSpec> corrBody = cond.body().stream()
                .map(b -> tRenamed.equals(tVar) ? b
                        : Pipelines.rewriteRowReads(b, tVar, Map.of(), Set.of(),
                                v -> new TypedVariable(tRenamed,
                                        new ExprType(ex.targetRow(),
                                                Multiplicity.Bounded.ONE))))
                .map(b -> Pipelines.rewriteRowReads(b, pVar, Map.of(), Set.of(),
                        v -> new TypedVariable(target.freshRowVar(),
                                new ExprType(target.rowType(), Multiplicity.Bounded.ONE))))
                .toList();
        ExprType predType = new ExprType(new Type.FunctionType(
                List.of(new Type.Param(ex.targetRow(), Multiplicity.Bounded.ONE)),
                new Type.Param(Type.Primitive.BOOLEAN, Multiplicity.Bounded.ONE)),
                Multiplicity.Bounded.ONE);
        TypedLambda corr = new TypedLambda(List.of(tRenamed), corrBody, predType);
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
        TypedSpec rel = new TypedFilter(
                new TypedFilter(
                        ex.targetPipeline(), corr, ex.targetPipeline().info()),
                memberPred, ex.targetPipeline().info());
        return new TypedNativeCall(target.isNotEmptyCallee(), List.of(rel),
                new ExprType(Type.Primitive.BOOLEAN, Multiplicity.Bounded.ONE));
    }

    /** The {@code $p.head.leaf} read inside a boolean leaf whose head is
     * a TO-MANY association crossing; null when none. A leaf crossing
     * TWO DISTINCT to-many heads is LOUD (audit 23 B1): the isolation
     * null-guard covers one read — guarding only the first silently
     * inverts booleans for parents empty on the other head. */
    private TypedSpec toManyCrossingRead(TypedSpec n) {
        List<TypedSpec> all = new ArrayList<>();
        collectToManyCrossings(n, all);
        if (all.isEmpty()) {
            return null;
        }
        java.util.Set<String> heads = new java.util.LinkedHashSet<>();
        for (TypedSpec r : all) {
            heads.add(pathOf(r, target.userVar()).get(0));
        }
        if (heads.size() > 1) {
            throw new NotImplementedException("boolean leaf crosses "
                    + heads.size() + " distinct to-many associations "
                    + heads + " — multi-crossing isolation is not"
                    + " supported yet");
        }
        return all.get(0);
    }

    private void collectToManyCrossings(TypedSpec n, List<TypedSpec> out) {
        List<String> path = pathOf(n, target.userVar());
        if (path != null && path.size() == 2
                && target.existsSubs().containsKey(path.get(0))
                && target.existsSubs().get(path.get(0)).toMany()) {
            out.add(n);
            return;
        }
        for (TypedSpec c : n.children()) {
            collectToManyCrossings(c, out);
        }
    }

    /** The embedded ctor of a binding: a bare {@code ^Inner(...)} (with
     * toOne look-through) or an otherwise composition's partial. */
    private static TypedNewInstance embeddedPartialOf(
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
        return inner instanceof TypedNewInstance ni
                ? ni : null;
    }

    /** Every property read on the predicate's param resolves in the partial. */
    private boolean predLeavesIn(TypedLambda pl,
            TypedNewInstance partial) {
        Set<List<String>> paths = new LinkedHashSet<>();
        for (TypedSpec b : pl.body()) {
            collectParamPaths(b, pl.parameters().get(0), paths);
        }
        if (paths.isEmpty()) {
            return false;
        }
        for (List<String> path : paths) {
            if (path.size() != 1 || !partial.properties().containsKey(path.get(0))) {
                return false;
            }
        }
        return true;
    }

    private static void collectParamPaths(TypedSpec n, String var,
            Set<List<String>> out) {
        List<String> p = pathOf(n, var);
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
            TypedNewInstance partial) {
        TypedSpec body = substEmbeddedReads(
                pl.body().get(pl.body().size() - 1), pl.parameters().get(0), partial);
        return rewrite(body);
    }

    private TypedSpec substEmbeddedReads(TypedSpec n, String var,
            TypedNewInstance partial) {
        List<String> p = pathOf(n, var);
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

    private static Set<String> unconvertedSlotsOf(ExistsSub ex) {
        Set<String> out =
                new LinkedHashSet<>(ex.targetSlotAliases());
        out.removeAll(ex.targetSlotPrefixes().keySet());
        return out;
    }

    private static boolean isEmptinessFamily(TypedNativeCall c) {
        String key = c.callee().signatureKey();
        return Pure.nativeNamed("isEmpty", key)
                || Pure.nativeNamed("isNotEmpty", key)
                || Pure.nativeNamed("exists", key);
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
        boolean dbg = System.getenv("LL_FNLR_DEBUG") != null;
        TypedSpec src = pa.source();
        boolean firstRow = false;
        boolean unwrapped = true;
        // multiplicity wrappers STACK (a qualifier body's own ->first()
        // under the call site's ->toOne(): toOne(first(filter(...)))) —
        // unwrap the whole chain; any first()/head() in it means the
        // subquery must LIMIT 1
        while (src instanceof TypedNativeCall c && c.args().size() == 1) {
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
            } else {
                break;
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
        if (!(src instanceof TypedFilter f)) {
            if (dbg && String.valueOf(src).contains("TypedFilter")) {
                System.err.println("[fnlr] not-direct-filter: "
                        + src.getClass().getSimpleName());
            }
            return null;
        }
        // the head may be a plain access OR a DATED property function
        // ($o.product(%d)->filter(..).name) — both key existsSubs by the
        // property; the dated head's temporal filter already rides the
        // registered exists pipeline (chain-keyed specs)
        String headProp;
        if (f.source() instanceof TypedPropertyAccess head
                && head.source() instanceof TypedVariable hv
                && hv.name().equals(target.userVar())) {
            headProp = head.property();
        } else if (f.source()
                instanceof TypedMilestonedAccess ma
                && ma.source() instanceof TypedVariable mv
                && mv.name().equals(target.userVar())) {
            headProp = ma.property();
        } else {
            if (dbg) {
                System.err.println("[fnlr] head not on userVar: "
                        + f.source().getClass().getSimpleName());
            }
            return null;
        }
        ExistsSub ex = target.existsSubs().get(headProp);
        if (ex == null) {
            if (dbg) {
                System.err.println("[fnlr] no ExistsSub for '" + headProp
                        + "' (keys=" + target.existsSubs().keySet() + ")");
            }
            return null;
        }
        // 1. correlated association condition over the target pipeline.
        // Freshen the corr binder against the enclosing scope (audit 18):
        // in a nested scope target.freshRowVar() IS the enclosing renamed
        // var — an unfreshened 't' binder here (and in the RowScope below)
        // would capture the parent-correlation reads. Rename TARGET-side
        // reads FIRST, then the parent rewrite (same order as rewriteExists).
        TypedLambda cond = ex.orientedCond();
        String pVar = cond.parameters().get(0);
        String tVar = cond.parameters().get(1);
        String freshT = tVar;
        while (freshT.equals(target.freshRowVar())
                || freshT.equals(target.userVar())) {
            freshT = freshT + "_n";
        }
        final String tRenamed = freshT;
        List<TypedSpec> corrBody = cond.body().stream()
                .map(b -> tRenamed.equals(tVar) ? b
                        : Pipelines.rewriteRowReads(b, tVar, Map.of(), Set.of(),
                                v -> new TypedVariable(tRenamed,
                                        new ExprType(ex.scalarRow(),
                                                Multiplicity.Bounded.ONE))))
                .map(b -> Pipelines.rewriteRowReads(b, pVar, Map.of(), Set.of(),
                        v -> new TypedVariable(target.freshRowVar(),
                                new ExprType(target.rowType(), Multiplicity.Bounded.ONE))))
                .toList();
        TypedLambda corr = new TypedLambda(List.of(tRenamed), corrBody,
                new ExprType(new Type.FunctionType(
                        List.of(new Type.Param(ex.scalarRow(), Multiplicity.Bounded.ONE)),
                        new Type.Param(Type.Primitive.BOOLEAN, Multiplicity.Bounded.ONE)),
                        Multiplicity.Bounded.ONE));
        // the SCALAR pipeline (slot-UNDEMANDED): a slot demanded by some
        // OTHER consumer (an exists site) must not fan this single-row
        // subquery out (audit 13 B3 — data-dependent "more than one row")
        TypedSpec rel = new TypedFilter(
                ex.scalarPipeline(), corr, ex.scalarPipeline().info());
        // 2. the user predicate, substituted against the TARGET's bindings
        //    (outer reads correlate through a second pass — same as exists)
        TypedLambda predLam = f.predicate();
        // the pred's target-side reads may hop the target's OWN
        // class-typed slots ($e.address.name — the qualifier-with-
        // operation family): the registration MATERIALIZED those slots
        // (innerLeaves demand) — dispatch through the recorded prefixes;
        // only the un-materialized aliases stay loud
        Set<String> unconvertedT = new java.util.LinkedHashSet<>(
                ex.targetSlotAliases());
        unconvertedT.removeAll(ex.targetSlotPrefixes().keySet());
        Substitution predSub = new Substitution(new Target(
                new RowScope(predLam.parameters().get(0), tRenamed,
                        ex.targetClassFqn(), target.mappingFqn(),
                        ex.targetRowVar(), ex.targetBindings(), ex.targetRow(),
                        unconvertedT, ex.targetSlotPrefixes(), Map.of()),
                ex.innerRegs(), TemporalView.NONE, true, true));
        TypedLambda inner = predSub.rewriteLambda(predLam);
        TypedLambda innerOuter = new TypedLambda(inner.parameters(),
                inner.body().stream().map(this::rewrite).toList(), inner.info());
        rel = new TypedFilter(rel, innerOuter, rel.info());
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
        TypedSpec projected = new TypedProject(rel,
                List.of(new TypedFuncCol(
                        pa.property(), leafFn)),
                new ExprType(outRow, Multiplicity.Bounded.ONE));
        if (firstRow) {
            projected = new TypedLimit(projected,
                    new TypedCInteger(1L,
                            ExprType.one(Type.Primitive.INTEGER)),
                    projected.info());
        }
        return projected;
    }

    /** $r->subType(@Sub).prop — the cast is a same-source dispatch: the
     * read resolves through the SUB class's registered binding table
     * renamed onto the row (non-member rows read the sub's columns as
     * NULL naturally). Null when the source is not a subType cast of
     * the instance variable; a cast whose subtype has no registration
     * (unmapped subtype, own-source subtype, or a nested position whose
     * registries never saw the scan) stays loud. */
    private TypedSpec subTypeLeafRead(TypedPropertyAccess pa) {
        if (!(pa.source() instanceof TypedNativeCall nc)
                || !nc.callee().qualifiedName()
                        .equals("meta::pure::functions::lang::subType")
                || nc.args().isEmpty()
                || !(nc.args().get(0) instanceof TypedVariable v)
                || !v.name().equals(target.userVar())
                || !(nc.info().type() instanceof Type.ClassType ct)) {
            return null;
        }
        if (!target.assocs().containsKey(SUBTYPE_KEY + ct.fqn())) {
            throw new NotImplementedException("subType(@" + ct.fqn()
                    + ") in this position is not supported yet");
        }
        return assocLeaf(SUBTYPE_KEY + ct.fqn(), pa.property());
    }

    private TypedSpec rewriteExists(TypedNativeCall call, ExistsSub ex,
            List<TypedLambda> chainPreds) {
        TypedLambda cond = ex.orientedCond();   // params (parentRow, targetRow)
        String pVar = cond.parameters().get(0);
        String tVar = cond.parameters().get(1);
        // NESTED levels reuse the join conditions' literal param names
        // (λ(s,t) everywhere) — an inner 't' would SHADOW the enclosing
        // scope's correlation var (R2: nested exists silently misbound).
        // Freshen collision-driven; target-side reads rename with it.
        String freshT = tVar;
        while (freshT.equals(target.freshRowVar())
                || freshT.equals(target.userVar())) {
            freshT = freshT + "_n";
        }
        final String tRenamed = freshT;
        // ORDER MATTERS: rename the TARGET-side reads FIRST — renaming
        // after the parent rewrite would capture the just-created parent
        // reads when the enclosing var is also named 't' (the R2 probe:
        // the inner exists silently correlated to the FIRM, not the person)
        List<TypedSpec> corrBody = cond.body().stream()
                .map(b -> tRenamed.equals(tVar) ? b
                        : Pipelines.rewriteRowReads(b, tVar, Map.of(), Set.of(),
                                v -> new TypedVariable(tRenamed,
                                        new ExprType(ex.targetRow(),
                                                Multiplicity.Bounded.ONE))))
                .map(b -> Pipelines.rewriteRowReads(b, pVar, Map.of(), Set.of(),
                        v -> new TypedVariable(target.freshRowVar(),
                                new ExprType(target.rowType(), Multiplicity.Bounded.ONE))))
                .toList();
        TypedLambda corr = new TypedLambda(List.of(tRenamed), corrBody,
                new ExprType(new Type.FunctionType(
                        List.of(new Type.Param(ex.targetRow(), Multiplicity.Bounded.ONE)),
                        new Type.Param(Type.Primitive.BOOLEAN, Multiplicity.Bounded.ONE)),
                        Multiplicity.Bounded.ONE));
        TypedSpec rel = new TypedFilter(
                ex.targetPipeline(), corr, ex.targetPipeline().info());
        // chain filters ($p.head->filter(f)->...) merge into the correlated
        // set: each substitutes over the target's bindings like the exists
        // predicate, then wraps rel (outer reads re-correlate via the
        // second pass below at the CALL level; chain preds get theirs here)
        for (TypedLambda cf : chainPreds) {
            Substitution cfSub = new Substitution(new Target(
                    new RowScope(cf.parameters().get(0), tRenamed,
                            ex.targetClassFqn(), target.mappingFqn(),
                            ex.targetRowVar(), ex.targetBindings(),
                            ex.targetRow(), unconvertedSlotsOf(ex),
                            ex.targetSlotPrefixes(), Map.of()),
                    ex.innerRegs(), TemporalView.NONE, true, true));
            TypedLambda cfInner = cfSub.rewriteLambda(cf);
            TypedLambda cfCorr = new TypedLambda(cfInner.parameters(),
                    cfInner.body().stream().map(this::rewrite).toList(),
                    new ExprType(new Type.FunctionType(
                            List.of(new Type.Param(ex.targetRow(),
                                    Multiplicity.Bounded.ONE)),
                            new Type.Param(Type.Primitive.BOOLEAN,
                                    Multiplicity.Bounded.ONE)),
                            Multiplicity.Bounded.ONE));
            rel = new TypedFilter(rel, cfCorr,
                    rel.info());
        }
        List<TypedSpec> newArgs = new ArrayList<>();
        newArgs.add(rel);
        if (call.args().size() == 2) {
            if (!(call.args().get(1) instanceof TypedLambda predLam)) {
                throw new NotImplementedException("non-lambda predicate in "
                        + call.callee().qualifiedName() + " over an association");
            }
            Set<String> unconvertedSlots =
                    new LinkedHashSet<>(ex.targetSlotAliases());
            unconvertedSlots.removeAll(ex.targetSlotPrefixes().keySet());
            Substitution predSub = new Substitution(new Target(
                    new RowScope(predLam.parameters().get(0), tRenamed,
                            ex.targetClassFqn(), target.mappingFqn(),
                            ex.targetRowVar(), ex.targetBindings(),
                            ex.targetRow(), unconvertedSlots,
                            ex.targetSlotPrefixes(), Map.of()),
                    ex.innerRegs(), TemporalView.NONE, true, true));
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
        return Pure.nativeNamed("and", key)
                || Pure.nativeNamed("or", key);
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
                && Pure.nativeNamed("otherwise",
                        oc.callee().signatureKey())
                && oc.args().get(0)
                        instanceof TypedNewInstance) {
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
