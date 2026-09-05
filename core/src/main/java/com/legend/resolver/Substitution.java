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
import com.legend.compiler.spec.typed.TypedCast;
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
                    Map<String, String> milestoneColumns,
                    @com.legend.Nullable String castGate) {
        /** Without a cast gate (ClassSource.castGate). */
        RowScope(String userVar, String freshRowVar, String classFqn,
                 String mappingFqn, String sourceRowVar,
                 Map<String, TypedSpec> bindings, Type.RelationType rowType,
                 Set<String> strippedSlots,
                 Map<String, String> slotPrefixes,
                 Map<String, String> milestoneColumns) {
            this(userVar, freshRowVar, classFqn, mappingFqn, sourceRowVar,
                    bindings, rowType, strippedSlots, slotPrefixes,
                    milestoneColumns, null);
        }
    }

    /** THE DEMAND-REGISTERED MATERIALS: association/navigate heads, the
     * honest-error end names, the exists materials, the aggregate reads
     * (identity-keyed), and the boolean-machinery callees. */
    record Registries(Map<String, AssocSub> assocs,
                      Set<String> assocEnds,
                      Map<String, ExistsSub> existsSubs,
                      Map<TypedSpec, AggRead> aggReads,
                      Map<TypedSpec, InQueryRead> inQueryReads,
                      @com.legend.Nullable TypedFunction isNotEmptyCallee,
                      @com.legend.Nullable TypedFunction equalCallee,
                      List<String> pkColumns,
                      @com.legend.Nullable TypedFunction inCallee,
                      @com.legend.Nullable TypedFunction andCallee,
                      @com.legend.Nullable TypedFunction orCallee,
                      @com.legend.Nullable TypedFunction failCallee) {

        Registries(Map<String, AssocSub> assocs, Set<String> assocEnds,
                   Map<String, ExistsSub> existsSubs,
                   Map<TypedSpec, AggRead> aggReads,
                   Map<TypedSpec, InQueryRead> inQueryReads,
                   @com.legend.Nullable TypedFunction isNotEmptyCallee,
                   @com.legend.Nullable TypedFunction equalCallee,
                   List<String> pkColumns,
                   @com.legend.Nullable TypedFunction inCallee,
                   @com.legend.Nullable TypedFunction andCallee,
                   @com.legend.Nullable TypedFunction orCallee) {
            this(assocs, assocEnds, existsSubs, aggReads, inQueryReads,
                    isNotEmptyCallee, equalCallee, pkColumns, inCallee,
                    andCallee, orCallee, null);
        }

        Registries(Map<String, AssocSub> assocs, Set<String> assocEnds,
                   Map<String, ExistsSub> existsSubs,
                   Map<TypedSpec, AggRead> aggReads,
                   Map<TypedSpec, InQueryRead> inQueryReads,
                   @com.legend.Nullable TypedFunction isNotEmptyCallee,
                   @com.legend.Nullable TypedFunction equalCallee) {
            this(assocs, assocEnds, existsSubs, aggReads, inQueryReads,
                    isNotEmptyCallee, equalCallee, List.of(), null, null,
                    null);
        }

        Registries(Map<String, AssocSub> assocs, Set<String> assocEnds,
                   Map<String, ExistsSub> existsSubs,
                   Map<TypedSpec, AggRead> aggReads,
                   Map<TypedSpec, InQueryRead> inQueryReads,
                   @com.legend.Nullable TypedFunction isNotEmptyCallee,
                   @com.legend.Nullable TypedFunction equalCallee,
                   List<String> pkColumns,
                   @com.legend.Nullable TypedFunction inCallee) {
            this(assocs, assocEnds, existsSubs, aggReads, inQueryReads,
                    isNotEmptyCallee, equalCallee, pkColumns, inCallee,
                    null, null);
        }

        Registries(Map<String, AssocSub> assocs, Set<String> assocEnds,
                   Map<String, ExistsSub> existsSubs,
                   Map<TypedSpec, AggRead> aggReads,
                   @com.legend.Nullable TypedFunction isNotEmptyCallee,
                   @com.legend.Nullable TypedFunction equalCallee) {
            this(assocs, assocEnds, existsSubs, aggReads, Map.of(),
                    isNotEmptyCallee, equalCallee);
        }

        /** Inner substitutions (exists/pred rewrites) carry NO registries:
         * nested navigation stays loud by construction. */
        static final Registries NONE = new Registries(Map.of(),
                Set.of(), Map.of(), Map.of(), null, null);
    }

    /** The temporal reads the substitution serves (generated-date
     * properties): the root context's point dates and the per-chain
     * property-function dates — legacy list shapes at this boundary. */
    record TemporalView(List<TypedSpec> rootTemporalDates,
                        Map<String, List<TypedSpec>> headTemporalDates,
                        @com.legend.Nullable TemporalContext rootCtx,
                        @com.legend.Nullable String forEachDateColumn) {

        TemporalView(List<TypedSpec> rootTemporalDates,
                Map<String, List<TypedSpec>> headTemporalDates) {
            this(rootTemporalDates, headTemporalDates, null, null);
        }

        static final TemporalView NONE =
                new TemporalView(List.of(), Map.of());

        /** DIMENSION-AWARE root context date (audit 23): businessDate
         * reads the business slot, processingDate the processing slot —
         * a cross-dimension ask under a dimensioned context is LOUD (the
         * legacy positional list silently served the wrong dimension).
         * Null when no root context (callers fall through). */
        @com.legend.Nullable TypedSpec rootContextDate(String prop) {
            if (rootCtx == null || rootCtx.isEmpty()) {
                return null;
            }
            TypedSpec d = prop.equals("businessDate")
                    ? rootCtx.business() : rootCtx.processing();
            if (d == null) {
                throw new NotImplementedException("generated '" + prop
                        + "' read under a context with no "
                        + (prop.equals("businessDate") ? "business"
                                : "processing") + "-dimension date");
            }
            return d;
        }
    }

    /** The instantiation being substituted into. Composed of the row
     * scope, the registries and the temporal view; the flat accessors
     * below keep the rewrite body reading naturally. */
    /** A membership collection that is ITSELF a resolved class query
     * (let validNames = Other.all().name->distinct(); ...->in($validNames)
     * — engine temp-table semantics ≡ IN-subquery ≡ EXISTS-equality):
     * the resolved single-column relation + its column, identity-keyed by
     * the in/contains call node (task #78 scalar-subquery IN). */
    record InQueryRead(TypedSpec relation, @com.legend.Nullable String column) {
    }

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

        Map<TypedSpec, InQueryRead> inQueryReads() {
            return regs.inQueryReads();
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

        @com.legend.Nullable TypedFunction isNotEmptyCallee() {
            return regs.isNotEmptyCallee();
        }

        @com.legend.Nullable TypedFunction equalCallee() {
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
                     Registries innerRegs, Map<String, SubNav> subNavs) {

        // §4AD census: every EXISTS-material construction is a firing
        // of the non-row-algebra navigation family (redesign work item)
        ExistsSub {
            com.legend.lowering.NavArmCensus.fire("exists-material");
        }

        ExistsSub(TypedSpec targetPipeline, TypedLambda orientedCond,
                  String targetRowVar, Map<String, TypedSpec> targetBindings,
                  Type.RelationType targetRow, String targetClassFqn,
                  Set<String> targetSlotAliases,
                  Map<String, String> targetSlotPrefixes, boolean toMany,
                  TypedSpec scalarPipeline, Type.RelationType scalarRow) {
            this(targetPipeline, orientedCond, targetRowVar, targetBindings,
                    targetRow, targetClassFqn, targetSlotAliases,
                    targetSlotPrefixes, toMany, scalarPipeline, scalarRow,
                    Registries.NONE, Map.of());
        }

        /** R1 (recursive scope demand): nested predicate scopes carry
         * their OWN registered materials instead of staying loud. */
        ExistsSub withInnerRegs(Registries r) {
            return new ExistsSub(targetPipeline, orientedCond, targetRowVar,
                    targetBindings, targetRow, targetClassFqn,
                    targetSlotAliases, targetSlotPrefixes, toMany,
                    scalarPipeline, scalarRow, r, subNavs);
        }

        /** Materialized target nav steps by PROPERTY name — the leaf-side
         * dispatch for continued chains past the filtered head
         * ({@code orgByName('X').parent.name}, #70). */
        ExistsSub withSubNavs(Map<String, SubNav> sn) {
            return new ExistsSub(targetPipeline, orientedCond, targetRowVar,
                    targetBindings, targetRow, targetClassFqn,
                    targetSlotAliases, targetSlotPrefixes, toMany,
                    scalarPipeline, scalarRow, innerRegs, sn);
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
                    @com.legend.Nullable String readVar,
                    Type.@com.legend.Nullable RelationType readRowType,
                    Map<String, String> targetMilestoneColumns,
                    Map<String, SubNav> subNavs,
                    boolean filteredTarget) {

        AssocSub(String prefix, String targetRowVar,
                 Map<String, TypedSpec> targetBindings, String targetClassFqn,
                 Set<String> targetSlotAliases,
                 Map<String, String> targetSlotPrefixes,
                 @com.legend.Nullable String readVar,
                 Type.@com.legend.Nullable RelationType readRowType,
                 Map<String, String> targetMilestoneColumns,
                 Map<String, SubNav> subNavs) {
            this(prefix, targetRowVar, targetBindings, targetClassFqn,
                    targetSlotAliases, targetSlotPrefixes, readVar,
                    readRowType, targetMilestoneColumns, subNavs, false);
        }

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
                 @com.legend.Nullable String readVar,
                 Type.@com.legend.Nullable RelationType readRowType,
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
        Type.FunctionType oldFn = lambda.functionType();
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
        Type.FunctionType oldFn = lambda.functionType();
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
    static @com.legend.Nullable String propertyOnUserVar(TypedSpec n, String userVar) {
        List<String> p = pathOf(n, userVar);
        return p != null && p.size() == 1 ? p.get(0) : null;
    }

    /**
     * THE path funnel: the full property chain when {@code n}'s receiver
     * chain bottoms at the user's lambda variable ({@code $p.employer.legal}
     * &rArr; {@code [employer, legal]}); {@code null} otherwise. DemandScan
     * and the rewrite share this single extractor.
     */

    /** tdsContains in predicate position (task #78): EXISTS over the
     * resolved projected relation — see the inline notes. */
    private TypedSpec rewriteTdsContains(TypedNativeCall call, TypedSpec n) {
        // tdsContains in predicate position: EXISTS over the resolved
        // projected relation, one equality per (function, column) pair
        // (engine pureToSQLQuery tdsContains processor; golden emits
        // EXISTS(SELECT 1 FROM (project…) t WHERE outer = t.col)).
        // NOTE the engine adds an or-both-null disjunct for TDS null
        // cells; pure eq-over-empty is FALSE and the corpus seeds are
        // non-null here, so plain equality is row-equal — revisit if
        // a null-celled TDS fixture appears.
            InQueryRead tq = target.inQueryReads().get(n);
            if (tq == null) {
                throw new NotImplementedException("tdsContains whose TDS"
                        + " argument is not a resolvable relation chain"
                        + " is not supported yet");
            }
            if (call.args().size() == 5) {
                return rewriteTdsContainsCross(call, n);
            }
            if (call.args().size() != 3) {
                throw new NotImplementedException("tdsContains with "
                        + call.args().size()
                        + " args is not supported yet");
            }
            TypedSpec fns = call.args().get(1);
            List<TypedSpec> fnList = fns instanceof TypedCollection tcol
                    ? tcol.elements() : List.of(fns);
            Type.RelationType tRow =
                    Type.requireRelationSchema(tq.relation().info().type());
            if (fnList.size() != 1 || tRow.columns().size() != 1) {
                throw new NotImplementedException("tdsContains with "
                        + fnList.size() + " function(s) over "
                        + tRow.columns().size() + " column(s) — only the"
                        + " single-function form is supported yet");
            }
            TypedSpec fn0 = fnList.get(0);
            if (fn0 instanceof TypedNativeCall c0 && c0.args().size() == 1
                    && com.legend.builtin.Pure.isToOneCall(c0.callee().qualifiedName())) {
                fn0 = c0.args().get(0);
            }
            if (!(fn0 instanceof TypedLambda fl)
                    || fl.parameters().size() != 1
                    || fl.body().isEmpty()) {
                throw new NotImplementedException("tdsContains function"
                        + " argument is not a plain lambda —"
                        + " not supported yet");
            }
            if (!fl.parameters().get(0).equals(target.userVar())
                    && !(call.args().get(0) instanceof TypedVariable ov
                            && fl.parameters().get(0).equals(ov.name()))) {
                throw new NotImplementedException("tdsContains function"
                        + " parameter '" + fl.parameters().get(0)
                        + "' does not bind the filter variable —"
                        + " renamed-parameter binding is not built yet");
            }
            TypedSpec outer = rewrite(
                    fl.body().get(fl.body().size() - 1));
            Type.Column tc0 = tRow.columns().get(0);
            String tv = "_tc";
            TypedSpec eqT = new TypedNativeCall(eqCallee(),
                    List.of(outer,
                            new TypedPropertyAccess(
                                    new TypedVariable(tv, new ExprType(
                                            tRow,
                                            Multiplicity.Bounded.ONE)),
                                    tc0.name(),
                                    new ExprType(tc0.type(),
                                            tc0.multiplicity()))),
                    new ExprType(Type.Primitive.BOOLEAN,
                            Multiplicity.Bounded.ONE));
            TypedLambda tPred = new TypedLambda(List.of(tv),
                    List.of(eqT),
                    new ExprType(new Type.FunctionType(
                            List.of(new Type.Param(tRow,
                                    Multiplicity.Bounded.ONE)),
                            new Type.Param(Type.Primitive.BOOLEAN,
                                    Multiplicity.Bounded.ONE)),
                            Multiplicity.Bounded.ONE));
            TypedSpec tFiltered = new TypedFilter(tq.relation(), tPred,
                    tq.relation().info());
            return new TypedNativeCall(neCallee(),
                    List.of(tFiltered), n.info());
    }

    /** tdsContains CROSS-OPERATION form (task #78): the cross lambda's
     * first param reads the PROJECTED outer row ($a.getString(id_i) =
     * the i-th function evaluated over the object), the second reads the
     * TDS relation row ($b.getString(col) = column read) — the body
     * rewrites into the EXISTS filter predicate over the tds relation. */
    private TypedSpec rewriteTdsContainsCross(TypedNativeCall call,
            TypedSpec n) {
        InQueryRead tq = target.inQueryReads().get(n);
        if (tq == null) {
            throw new NotImplementedException("tdsContains whose TDS"
                    + " argument is not a resolvable relation chain"
                    + " is not supported yet");
        }
        TypedSpec fns = call.args().get(1);
        List<TypedSpec> fnList = fns instanceof TypedCollection tcol
                ? tcol.elements() : List.of(fns);
        TypedSpec idsA = call.args().get(2);
        List<TypedSpec> idList = idsA instanceof TypedCollection icol
                ? icol.elements() : List.of(idsA);
        if (fnList.size() != idList.size()) {
            throw new NotImplementedException("tdsContains cross form:"
                    + " functions/ids arity mismatch");
        }
        Map<String, TypedSpec> outerById = new java.util.LinkedHashMap<>();
        for (int i = 0; i < fnList.size(); i++) {
            if (!(idList.get(i) instanceof
                    com.legend.compiler.spec.typed.TypedCString idc)) {
                throw new NotImplementedException("tdsContains cross form:"
                        + " non-literal id");
            }
            TypedSpec f = fnList.get(i);
            if (!(f instanceof TypedLambda fl)
                    || fl.parameters().size() != 1 || fl.body().isEmpty()
                    || (!fl.parameters().get(0).equals(target.userVar())
                            && !(call.args().get(0) instanceof TypedVariable ov
                                && fl.parameters().get(0).equals(ov.name())))) {
                throw new NotImplementedException("tdsContains cross form:"
                        + " function is not a plain lambda over the filter"
                        + " variable");
            }
            outerById.put(idc.value(),
                    rewrite(fl.body().get(fl.body().size() - 1)));
        }
        if (!(call.args().get(4) instanceof TypedLambda cross)
                || cross.parameters().size() != 2 || cross.body().isEmpty()) {
            throw new NotImplementedException("tdsContains cross form:"
                    + " crossOperation is not a 2-param lambda");
        }
        Type.RelationType tRow =
                Type.requireRelationSchema(tq.relation().info().type());
        String tv = "_tc";
        TypedSpec pred = crossCellSubst(
                cross.body().get(cross.body().size() - 1),
                cross.parameters().get(0), cross.parameters().get(1),
                outerById, tRow, tv);
        TypedLambda tPred = new TypedLambda(List.of(tv), List.of(pred),
                new ExprType(new Type.FunctionType(
                        List.of(new Type.Param(tRow,
                                Multiplicity.Bounded.ONE)),
                        new Type.Param(Type.Primitive.BOOLEAN,
                                Multiplicity.Bounded.ONE)),
                        Multiplicity.Bounded.ONE));
        TypedSpec tFiltered = new TypedFilter(tq.relation(), tPred,
                tq.relation().info());
        return new TypedNativeCall(neCallee(),
                List.of(tFiltered), n.info());
    }

    /** Substitute cross-lambda cell reads: getString($a, id) &rarr; the
     * outer expression; getString($b, col) &rarr; the relation column
     * read. Unhandled nodes still referencing either param throw loud. */
    private TypedSpec crossCellSubst(TypedSpec e, String aVar, String bVar,
            Map<String, TypedSpec> outerById, Type.RelationType tRow,
            String tv) {
        if (e instanceof TypedNativeCall g && g.args().size() == 2
                && g.callee().qualifiedName().equals(
                        "meta::pure::tds::getString")
                && g.args().get(0) instanceof TypedVariable rv
                && g.args().get(1) instanceof
                        com.legend.compiler.spec.typed.TypedCString col) {
            if (rv.name().equals(aVar)) {
                TypedSpec o = outerById.get(col.value());
                if (o == null) {
                    throw new NotImplementedException("tdsContains cross"
                            + " form: id '" + col.value() + "' is not in"
                            + " the ids list");
                }
                return o;
            }
            if (rv.name().equals(bVar)) {
                Type.Column c = tRow.columns().stream()
                        .filter(cc -> cc.name().equals(col.value()))
                        .findFirst().orElseThrow(() ->
                                new NotImplementedException("tdsContains"
                                        + " cross form: column '"
                                        + col.value() + "' is not on the"
                                        + " TDS relation"));
                return new TypedPropertyAccess(
                        new TypedVariable(tv, new ExprType(tRow,
                                Multiplicity.Bounded.ONE)),
                        c.name(), new ExprType(c.type(), c.multiplicity()));
            }
        }
        if (e instanceof TypedNativeCall c2) {
            List<TypedSpec> args = new java.util.ArrayList<>(
                    c2.args().size());
            for (TypedSpec a : c2.args()) {
                args.add(crossCellSubst(a, aVar, bVar, outerById, tRow, tv));
            }
            return c2.withChildren(args);
        }
        if (e instanceof TypedVariable v
                && (v.name().equals(aVar) || v.name().equals(bVar))) {
            throw new NotImplementedException("tdsContains cross form:"
                    + " row param '$" + v.name() + "' used outside a"
                    + " getString cell read — not supported yet");
        }
        return e;
    }

    /** TRUE when the chain from {@code n} down to its root pierces an
     * explicit (user-written) toOne() wrapper — synthesized bindings
     * never appear in the query lambda, so any toOne found here is the
     * user's strict-multiplicity assertion (task #72). */
    private static boolean piercesToOne(TypedSpec n) {
        while (true) {
            if (n instanceof TypedNativeCall c && c.args().size() == 1) {
                if (com.legend.builtin.Pure.isToOneCall(c.callee().qualifiedName())) {
                    return true;
                }
                n = c.args().get(0);
            } else if (n instanceof TypedPropertyAccess pa) {
                n = pa.source();
            } else if (n instanceof TypedMap m) {
                n = m.source();
            } else if (n instanceof TypedFilter f) {
                n = f.source();
            } else {
                return false;
            }
        }
    }

    /** §4AD task #72 — the strict-read hoist, WITNESSED flavor only:
     * {@code isEmpty(chain pierced by toOne through a ~filter-mapped
     * head)} rewrites to {@code isNotEmpty($v.head) && isEmpty(chain
     * sans toOne)}; both conjuncts take their existing routes (semi-
     * join presence over the ~filtered set; plain pierced leaf read —
     * per-row IS NULL). Null when the shape is not the witnessed one
     * (caller keeps the loud wall). */
    private @com.legend.Nullable TypedSpec strictReadHoist(
            TypedNativeCall call) {
        if (!com.legend.builtin.Pure.nativeNamed("isEmpty",
                        call.callee().signatureKey())
                || call.args().size() != 1
                || target.regs().isNotEmptyCallee() == null
                || target.regs().andCallee() == null) {
            return null;
        }
        TypedSpec stripped = stripToOnes(call.args().get(0));
        // the head read: the deepest access whose source is the user var
        TypedSpec headRead = stripped;
        while (headRead instanceof TypedPropertyAccess hp
                && !(hp.source() instanceof TypedVariable hv
                        && hv.name().equals(target.userVar()))) {
            headRead = hp.source();
        }
        if (!(headRead instanceof TypedPropertyAccess)) {
            return null;
        }
        var one = com.legend.compiler.element.type.Multiplicity.Bounded.ONE;
        ExprType boolOne =
                new ExprType(Type.Primitive.BOOLEAN, one);
        TypedNativeCall present = new TypedNativeCall(
                target.regs().isNotEmptyCallee(), List.of(headRead), boolOne);
        TypedNativeCall leafEmpty = new TypedNativeCall(
                call.callee(), List.of(stripped), call.info());
        return new TypedNativeCall(target.regs().andCallee(),
                List.of(present, leafEmpty), boolOne);
    }

    /** The chain with every explicit toOne()/first()/head-of-one wrapper
     * removed (SQL-erased, charter decision 1 — same policy as the
     * lift's filterBehindToOne). */
    private static TypedSpec stripToOnes(TypedSpec n) {
        if (n instanceof TypedNativeCall c && c.args().size() == 1
                && com.legend.builtin.Pure.isToOneCall(
                        c.callee().qualifiedName())) {
            return stripToOnes(c.args().get(0));
        }
        if (n instanceof TypedPropertyAccess pa) {
            TypedSpec src = stripToOnes(pa.source());
            return src == pa.source() ? pa
                    : new TypedPropertyAccess(src, pa.property(), pa.info());
        }
        if (n instanceof TypedMilestonedAccess ma) {
            TypedSpec src = stripToOnes(ma.source());
            return src == ma.source() ? ma
                    : new TypedMilestonedAccess(src, ma.property(),
                            ma.dates(), ma.sweep(), ma.info());
        }
        return n;
    }

    /** THE PATH VIEW — the one reader of navigation hop-sequences,
     * satisfied by BOTH spellings (path-view unification, closed by
     * measurement 2026-08-21): the sugar chain {@code $v.a.b}, the
     * explicit {@code ->map(l|$l.a.b)} (flattened through the lambda —
     * pure's own definition: the dot IS map sugar, map.pure
     * grammarDoc), toOne/trustOne coercions (transparent), and
     * milestoned property functions ({@code $o.product(%d)}).
     * 43 consumers across the resolver ask THIS reader; matchers never
     * pattern-match the two spellings separately. The complementary
     * canonical-form converter is {@link Pipelines#autoMapRead} — the
     * dot-desugaring pure itself defines, applied once at the
     * resolution boundary. Specialized walkers with DIFFERENT
     * contracts (root-only reads, unwrap-tracking peels) legitimately
     * stay bespoke — forcing them through this API would contort them
     * for purity without payoff (the D3-class ruling's lesson). */
    static @com.legend.Nullable List<String> pathOf(TypedSpec n, String userVar) {
        // toOne() look-through: $p.employer->toOne().legal is the idiomatic
        // spelling after an optional navigation — the coercion is
        // multiplicity-only and transparent to the path (audit R3).
        if (n instanceof TypedNativeCall c && c.args().size() == 1
                && com.legend.builtin.Pure.isToOneCall(c.callee().qualifiedName())) {
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
            // IDENTITY cast (subType(@Product) over a Product-typed nav —
            // the engine's context-propagation spelling): transparent,
            // the plain property path serves it (no stc dispatch)
            boolean identity = sc.args().get(0).info().type()
                    instanceof Type.ClassType argCt
                    && argCt.fqn().equals(sct.fqn());
            String comp = identity ? pa0.property()
                    : com.legend.model.ClassMapping
                            .subTypeColumn(sct.fqn(), pa0.property());
            if (sc.args().get(0) instanceof TypedVariable v0
                    && v0.name().equals(userVar)) {
                return List.of(comp);
            }
            List<String> inner = pathOf(sc.args().get(0), userVar);
            if (inner == null) {
                return null;
            }
            List<String> out = new ArrayList<>(inner);
            out.add(comp);
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
    private @com.legend.Nullable TypedSpec rewriteCallArms(TypedSpec n) {
        TypedSpec typeDispatch = typeDispatchArms(n);
        if (typeDispatch != null) {
            return typeDispatch;
        }
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
            // emptiness over an INLINED derived CONCATENATION of navs
            // (Person.addresses = $this.address->concatenate(
            // $this.firm.address); the Typer inlines parameterless
            // deriveds in query position): the engine emits ONE exists
            // over the UNION of member relations whose correlation keys
            // MERGE BY COLUMN NAME — an address row may satisfy the FIRM
            // key (golden testSimpleExists: 'Anthony Allen true').
            // Row-equal split: boolean fold of per-branch calls, each
            // correlated by the OR of ALL branches' key conditions that
            // bind on its row (isEmpty = AND of negated members).
            if (isEmptinessFamily(call)
                    && Pipelines.unwrapToOne(call.args().get(0))
                            instanceof TypedNativeCall cnc
                    && "meta::pure::functions::collection::concatenate"
                            .equals(cnc.callee().qualifiedName())
                    && cnc.args().size() == 2
                    && cnc.info().type() instanceof Type.ClassType
                    && target.regs().orCallee() != null
                    && target.regs().andCallee() != null) {
                List<ExistsSub> subs = new ArrayList<>();
                List<TypedLambda> rootConds = new ArrayList<>();
                for (TypedSpec arm : cnc.args()) {
                    List<String> ap = pathOf(Pipelines.unwrapToOne(arm),
                            target.userVar());
                    ExistsSub ax = ap == null ? null
                            : target.existsSubs().get(String.join(".", ap));
                    // the branch's MERGE KEY against the parent is its
                    // ROOT hop's condition (engine: the union member's ID
                    // column is the first hop's target key)
                    ExistsSub rx = ap == null ? null
                            : target.existsSubs().get(ap.get(0));
                    if (ax == null || rx == null) {
                        subs = null;
                        break;
                    }
                    subs.add(ax);
                    rootConds.add(rx.orientedCond());
                }
                if (subs != null) {
                    return mergedConcatExists(call, subs, rootConds);
                }
            }
            // CLASS-TYPED LEAF: isNotEmpty($p.a.b) where b is a navigation
            // step on the chain target — the DOTTED-path material fires a
            // correlated EXISTS on the exploded chain row (engine: semi-join
            // + key null check)
            if (headPath != null && headPath.size() >= 2 && isEmptinessFamily(call)
                    && target.existsSubs().containsKey(String.join(".", headPath))) {
                return rewriteExists(call,
                        java.util.Objects.requireNonNull(target.existsSubs()
                                .get(String.join(".", headPath))),
                        List.of());
            }
            // FILTER-WRAPPED emptiness: isEmpty/exists($p.head->filter(f)
            // [->filter(g)...], pred?) — the filters merge into the
            // correlated set (engine: filter-in-chain parks on the
            // navigation target)
            // STRICT-READ wall (task #72): an emptiness check whose input
            // pierces an EXPLICIT user toOne() through a ~filter-mapped
            // set is NOT isolation-eligible — the engine hoists the
            // mapping filter into the outer WHERE (golden testInputNot-
            // IsolatedWhenPropertyPathIsToOne expects 0 rows), so the
            // filter-in-ON emission would return wrong rows. Loud until
            // the strict-read hoist is built.
            if (isEmptinessFamily(call) && headPath != null
                    && !headPath.isEmpty()
                    && piercesToOne(call.args().get(0))) {
                AssocSub fh = target.assocs().get(headPath.get(0));
                if (fh != null && fh.filteredTarget()) {
                    // §4AD task #72 — the WITNESSED flavor (golden
                    // testInputNotIsolatedWhenPropertyPathIsToOne): the
                    // engine hoists the ~filter row-DROPPING and tests
                    // the pierced leaf per row. Since the join target is
                    // ALREADY ~filtered, re-evaluating the hoisted pred
                    // over the slot ≡ slot PRESENCE — spelled with the
                    // two EXISTING routes: isNotEmpty(head) rides the
                    // semi-join channel (whose set IS the ~filtered
                    // target), the leaf test rides the plain pierced
                    // read (toOne wrappers SQL-erased, charter dec. 1).
                    TypedSpec rewritten = strictReadHoist(call);
                    if (rewritten != null) {
                        return rewrite(rewritten);
                    }
                    throw new NotImplementedException("emptiness check over"
                            + " a toOne()-pierced navigation through the"
                            + " ~filter-mapped set of '" + headPath.get(0)
                            + "' needs the strict-read filter hoist —"
                            + " not supported yet (only the isEmpty"
                            + " flavor is golden-witnessed)");
                }
            }
            if (com.legend.builtin.Pure.nativeNamed("tdsContains",
                    call.callee().signatureKey())) {
                return rewriteTdsContains(call, n);
            }
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
                    // DOTTED chain spelling — isNotEmpty(filter(
                    // $this.firm.employees, pred)): the class-typed-leaf
                    // EXISTS material registered under the dotted path
                    // (registerDottedExistsSubs peels the same filters);
                    // the filter predicates merge into the correlated set
                    // exactly like the depth-1 arm.
                    ExistsSub dottedEx = fp == null || fp.size() < 2 ? null
                            : target.existsSubs().get(String.join(".", fp));
                    if (dottedEx != null) {
                        return rewriteExists(call, dottedEx, chainPreds);
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
                InQueryRead q = target.inQueryReads().get(n);
                if (q != null) {
                    // scalar-subquery membership: EXISTS(SELECT 1 FROM
                    // <resolved relation> WHERE col = needle) — the
                    // isNotEmpty-over-relation lowering (§133) emits the
                    // EXISTS; row equality ≡ the engine's temp-table IN.
                    Type.RelationType qRow =
                            Type.requireRelationSchema(q.relation().info().type());
                    String qv = "_iq";
                    Type.Column qc = qRow.columns().stream()
                            .filter(c -> c.name().equals(q.column()))
                            .findFirst().orElseThrow(() ->
                                    new IllegalStateException("resolver bug:"
                                            + " in-subquery column '"
                                            + q.column() + "' missing"));
                    TypedSpec eq = new TypedNativeCall(eqCallee(),
                            List.of(new TypedPropertyAccess(
                                            new TypedVariable(qv, new ExprType(
                                                    qRow, Multiplicity.Bounded.ONE)),
                                            java.util.Objects.requireNonNull(q.column()),
                                            new ExprType(qc.type(),
                                                    qc.multiplicity())),
                                    rewrite(singletonNeedle(needle))),
                            new ExprType(Type.Primitive.BOOLEAN,
                                    Multiplicity.Bounded.ONE));
                    TypedLambda qPred = new TypedLambda(List.of(qv),
                            List.of(eq),
                            new ExprType(new Type.FunctionType(
                                    List.of(new Type.Param(qRow,
                                            Multiplicity.Bounded.ONE)),
                                    new Type.Param(Type.Primitive.BOOLEAN,
                                            Multiplicity.Bounded.ONE)),
                                    Multiplicity.Bounded.ONE));
                    TypedSpec filtered = new TypedFilter(q.relation(), qPred,
                            q.relation().info());
                    return new TypedNativeCall(neCallee(),
                            List.of(filtered), n.info());
                }
                List<String> cp = coll
                        instanceof TypedPropertyAccess ? pathOf(coll, target.userVar()) : null;
                if (cp != null && cp.size() == 2
                        && target.existsSubs().containsKey(cp.get(0))
                        && target.existsSubs().get(cp.get(0)).toMany()) {
                    return rewriteMembershipExists(
                            target.existsSubs().get(cp.get(0)), cp.get(1), needle);
                }
                // audit 23: a DEEPER to-many membership path must not fall
                // to the generic walk (bare join = duplicated parents
                // under membership semantics) — loud until the chained
                // EXISTS route exists
                if (cp != null && cp.size() > 2
                        && target.existsSubs().containsKey(cp.get(0))
                        && target.existsSubs().get(cp.get(0)).toMany()) {
                    throw new NotImplementedException("set membership over"
                            + " the multi-hop to-many navigation "
                            + String.join(".", cp) + " is not supported yet");
                }
            }
        }
        // §4AD batch 7: the FILTER-POSITION pierced-toOne equality-fold
        // (EXISTS over the correlated leaf relation) is DELETED — the
        // lift claims filter position, the read compares over the fanned
        // joined row in WHERE (the engine's own shape; duplicates kept,
        // charter decision 2). Measured dead before deletion: the
        // fnlr-filter-equality-fold census arm fired ZERO corpus-wide
        // once the position gates dropped.
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
                // audit 23 B2 (engine-source re-grounding): the engine has
                // NO blanket negation rule — processNot emits a plain
                // not() and null-compensation is PER-OPERATOR
                // (dbExtension processNotEqual/processNotIn add OR IS
                // NULL; every other operator keeps three-valued semantics
                // where NULL rows DROP). This arm's pass-constant encodes
                // exactly that compensation, so it may serve ONLY the
                // compensated family; other operators are loud until
                // their engine emission is transcribed.
                if (!(lc.args().get(0) instanceof TypedNativeCall innerCmp
                        && (Pure.nativeNamed("equal",
                                        innerCmp.callee().signatureKey())
                                || Pure.nativeNamed("in",
                                        innerCmp.callee().signatureKey())
                                || Pure.nativeNamed("contains",
                                        innerCmp.callee().signatureKey())))) {
                    // ORDERING comparisons keep the engine's THREE-VALUED
                    // semantics: no processNotLessThan exists — the
                    // emission is a plain not() over the joined read and
                    // NULL rows DROP (no pass-constant compensation)
                    if (lc.args().get(0) instanceof TypedNativeCall oc2
                            && (Pure.nativeNamed("lessThan",
                                            oc2.callee().signatureKey())
                                    || Pure.nativeNamed("lessThanEqual",
                                            oc2.callee().signatureKey())
                                    || Pure.nativeNamed("greaterThan",
                                            oc2.callee().signatureKey())
                                    || Pure.nativeNamed("greaterThanEqual",
                                            oc2.callee().signatureKey()))) {
                        return lc.withChildren(rewriteAll(lc.args()));
                    }
                    throw new NotImplementedException("negated '"
                            + (lc.args().get(0) instanceof TypedNativeCall ic
                                    ? ic.callee().qualifiedName()
                                    : lc.args().get(0).getClass().getSimpleName())
                            + "' over a to-many crossing — the engine"
                            + " null-compensates only equal/in; this"
                            + " operator's emission is not transcribed yet");
                }
                TypedSpec readInner = rewrite(read);
                TypedSpec notInner = lc.withChildren(rewriteAll(lc.args()));
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
                        new TypedNativeCall(neCallee(),
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
                // OCCURRENCE-SPLIT: a projection-position read of a chain
                // that ALSO appears in a filter rides its own join copy
                // (engine per-call identity — the '#p' SubNav when minted)
                SubNav sub = target.filterPosition() ? null
                        : a3.subNavs().get(path.get(1) + "#p");
                if (sub == null) {
                    sub = a3.subNavs().get(path.get(1));
                }
                int hop = 2;
                while (sub != null && hop + 1 < path.size()
                        && sub.children().containsKey(path.get(hop))) {
                    sub = sub.children().get(path.get(hop));
                    hop++;
                }
                if (sub != null && hop + 1 < path.size()) {
                    // EMBEDDED CTOR TAIL under the sub-nav ($a.splits
                    // .incomeFunction.code — the sub-target maps the next
                    // hop as ^Inner(...)): the leaf is a same-row column
                    // of the sub-target, read through the composed prefix
                    TypedSpec ctorLeaf = ctorTailLeaf(sub, path, hop);
                    if (ctorLeaf instanceof TypedPropertyAccess paE
                            && paE.source() instanceof TypedVariable vE
                            && vE.name().equals(sub.rowVar())) {
                        return milestoneColumnRead(
                                sub.prefix() + paE.property(),
                                a3.readVar() != null ? a3.readVar()
                                        : target.freshRowVar(),
                                a3.readRowType() != null ? a3.readRowType()
                                        : target.rowType(),
                                a3.readVar() != null ? "" : a3.prefix(), n);
                    }
                }
                if (sub != null && hop + 1 == path.size()) {
                    String leaf = path.get(path.size() - 1);
                    String hops = String.join(".",
                            path.subList(0, path.size() - 1));
                    TypedSpec leafBinding = sub.bindings().get(leaf);
                    if (leafBinding == null) {
                        throw new MappingResolutionException("property '"
                                + SyntheticHeads.realHead(leaf)
                                + "' of nested navigation '" + hops
                                + "' is not mapped in mapping '"
                                + target.mappingFqn() + "'", target.classFqn());
                    }
                    TypedSpec inner3 = leafBinding;
                    if (inner3 instanceof TypedNativeCall c3
                            && c3.args().size() == 1
                            && com.legend.builtin.Pure.isToOneCall(c3.callee().qualifiedName())) {
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
                    // EXPRESSION-valued leaf (enum if-chain / computed
                    // binding): every sub-row column read re-points to the
                    // composed prefix on the outer read row — the same
                    // emission assocBindingRead gives assoc-target
                    // expression leaves.
                    String rv3 = a3.readVar() != null ? a3.readVar()
                            : target.freshRowVar();
                    Type.RelationType rr3 = a3.readRowType() != null
                            ? a3.readRowType() : target.rowType();
                    String fp3 = (a3.readVar() != null ? "" : a3.prefix())
                            + sub.prefix();
                    return Pipelines.prefixColumns(leafBinding, sub.rowVar(),
                            fp3, v -> new TypedVariable(rv3,
                                    new ExprType(rr3,
                                            Multiplicity.Bounded.ONE)));
                }
            }
            // MULTI-HOP association chain ($p.dept.org.name): the demand scan
            // registered one join per hop under its chain key — the leaf reads
            // the DEEPEST hop's target with the chained prefix (dept_org_).
            String chainKey = String.join(".", path.subList(0, path.size() - 1));
            if (target.assocs().containsKey(chainKey)) {
                return assocLeaf(chainKey, path.get(path.size() - 1));
            }
            TypedSpec subNavRead = chainKeySubNavRead(path, chainKey, n);
            if (subNavRead != null) {
                return subNavRead;
            }
            // MULTI-HOP through NESTED EMBEDDED ctors ($p.firm.address.name
            // over a denormalized mapping): walk the ^Inner(...) chain to
            // the leaf expression — parent-alias reads all the way down
            TypedSpec cur = target.bindings().get(path.get(0));
            int hop = 1;
            while (cur != null && hop < path.size()) {
                TypedSpec inner = cur;
                if (inner instanceof TypedNativeCall c1 && c1.args().size() == 1
                        && com.legend.builtin.Pure.isToOneCall(c1.callee().qualifiedName())) {
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
                            && com.legend.builtin.Pure.isToOneCall(c4.callee().qualifiedName())) {
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
            // SUBTYPE-EMBEDDED tail through an assoc head: the union
            // distributes stc_<Sub>___<prop>__<leaf> as a FLAT column —
            // read it like any other assoc leaf binding
            if (path.size() >= 3
                    && com.legend.model.ClassMapping.isSubTypeColumn(path.get(1))
                    && target.assocs().containsKey(path.get(0))) {
                AssocSub hf = target.assocs().get(path.get(0));
                String flat = String.join("__", path.subList(1, path.size()));
                TypedSpec fb = hf.targetBindings().get(flat);
                if (fb != null) {
                    return assocBindingRead(hf, flat, fb);
                }
            }
            AssocSub diag = target.assocs().get(path.get(0));
            if (System.getenv("LEGEND_LITE_STACKS") != null) {
                System.err.println("[multi-hop wall] path=" + path
                        + " targetBindingKeys=" + (diag == null ? "-"
                                : diag.targetBindings().keySet()));
                Thread.dumpStack();
            }
            throw new NotImplementedException("multi-hop navigation "
                    + String.join(".", path) + " through an embedded/slot head"
                    + " is not supported yet [assocs="
                    + target.assocs().keySet() + "; head subNavs="
                    + (diag == null ? "-" : diag.subNavs().keySet())
                    + "; head binding="
                    + (target.bindings().get(path.get(0)) == null ? "ABSENT"
                            : target.bindings().get(path.get(0))
                                    .getClass().getSimpleName()) + "]");
    }

    /** A LIST VALUE's map (a computed, many-valued String[*] — the
     * residual {@code if(..)->concatenate(..)} of toPostgresModel's
     * qualifiedName parts — never a row path, never a class collection):
     * a list operation, the mapper is data over its own element (the
     * lowering's list_transform). Pure's map over exactly one value is
     * application, which {@link #objectSpaceFanOut} keeps. */
    private boolean listValueMap(TypedMap m) {
        return m.mapper().parameters().size() == 1
                && !Type.isRelation(m.source().info().type())
                && !(m.source().info().type() instanceof Type.ClassType)
                && m.source().info().multiplicity() instanceof
                        com.legend.compiler.element.type.Multiplicity.Bounded mb
                && mb.isMany()
                && pathOf(m.source(), target.userVar()) == null;
    }

    /** ->map(l|...) over a navigation IS the auto-map spelling
     * ($f.employees->map(l|$l.lastName) == $f.employees.lastName, the
     * engine desugar): inline the mapper param with the source and
     * substitute the flattened expression. VALUE-POSITION fan-out (task
     * #78 step 2, engine golden testAdvancedDerivedPropertyThrough
     * Association: flat LEFT JOIN row explosion, mapper evaluated per
     * exploded row): ->map(e|body) over an object-space collection inlines
     * the param with the source — nav reads inside the body become path
     * reads served by the ONE flat join (dedup by head keeps multi-column
     * reads on the SAME exploded row). Property-path bodies are the
     * auto-map spelling of the same rule. Reducer-wrapped maps never reach
     * this arm (guarded at the reducer call above — audit 12 F4). */
    private static boolean objectSpaceFanOut(TypedMap m) {
        return m.mapper().parameters().size() == 1
                && m.mapper().body().size() == 1
                && !Type.isRelation(m.source().info().type());
    }

    /** The CHAIN KEY + SUB-NAV TAIL read of {@link #rewriteMultiHop}
     * ($a.links.rs.c.name): null when no registered chain prefix carries
     * the tail as a SubNav descent. */
    private @com.legend.Nullable TypedSpec chainKeySubNavRead(List<String> path,
            String chainKey, TypedSpec n) {
        // CHAIN KEY + SUB-NAV TAIL ($a.links.rs.c.name — the demand scan
        // registered the association hops up to 'links.rs' and the
        // target's own navigate slot 'c' rides that hop's SubNav): the
        // LONGEST registered chain prefix, then the sub-nav descent —
        // the same read the head+SubNav walk above emits (depth leg,
        // 2026-09-02)
        for (int len = path.size() - 2; len >= 2; len--) {
            String ck = String.join(".", path.subList(0, len));
            AssocSub ac = target.assocs().get(ck);
            if (ac == null) {
                continue;
            }
            SubNav sub = ac.subNavs().get(path.get(len));
            // a SubNav tree's prefixes are COMPOSED relative to the
            // AssocSub's target row at every depth (NavMaterializer
            // convention): the deepest prefix is the whole path
            int hop = len + 1;
            while (sub != null && hop + 1 < path.size()
                    && sub.children().containsKey(path.get(hop))) {
                sub = java.util.Objects.requireNonNull(
                        sub.children().get(path.get(hop)));
                hop++;
            }
            if (sub == null || hop + 1 != path.size()) {
                continue;
            }
            String acc = sub.prefix();
            String leaf = path.get(path.size() - 1);
            TypedSpec leafBinding = sub.bindings().get(leaf);
            if (leafBinding == null) {
                throw new MappingResolutionException("property '"
                        + SyntheticHeads.realHead(leaf)
                        + "' of nested navigation '" + chainKey
                        + "' is not mapped in mapping '"
                        + target.mappingFqn() + "'", target.classFqn());
            }
            TypedSpec innerC = leafBinding;
            if (innerC instanceof TypedNativeCall cc
                    && cc.args().size() == 1
                    && com.legend.builtin.Pure.isToOneCall(cc.callee().qualifiedName())) {
                innerC = cc.args().get(0);
            }
            String rvC = ac.readVar() != null ? ac.readVar() : target.freshRowVar();
            Type.RelationType rrC = ac.readRowType() != null
                    ? ac.readRowType() : target.rowType();
            if (innerC instanceof TypedPropertyAccess paC
                    && paC.source() instanceof TypedVariable vC
                    && vC.name().equals(sub.rowVar())) {
                return milestoneColumnRead(acc + paC.property(),
                        rvC, rrC, ac.readVar() != null ? "" : ac.prefix(), n);
            }
            String fpC = (ac.readVar() != null ? "" : ac.prefix()) + acc;
            return Pipelines.prefixColumns(leafBinding, sub.rowVar(), fpC,
                    v -> new TypedVariable(rvC, new ExprType(rrC,
                            Multiplicity.Bounded.ONE)));
        }
        return null;
    }

    /** A 1-HOP head read: bindings, generated temporal dates, honest
     * bare-head errors — resolves, throws loud, or (no match) NULL to
     * continue the walk. */
    private @com.legend.Nullable TypedSpec rewriteHeadProp(String prop, TypedSpec n) {
        if (prop != null) {
            TypedSpec binding = target.bindings().get(prop);
            if (binding != null) {
                // A CLASS-typED binding used as a whole value ($p.firm bare,
                // $p.addr bare): graph output territory — the honest story,
                // not a "resolver bug" from the rewriter's vocabulary wall.
                TypedSpec inner = binding;
                if (inner instanceof TypedNativeCall c1 && c1.args().size() == 1
                        && com.legend.builtin.Pure.isToOneCall(c1.callee().qualifiedName())) {
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
                    // audit 23: DIMENSION-AWARE when the context is
                    // dimensioned; the positional list stays as the
                    // fallback for legacy TemporalView constructions
                    TypedSpec hd = target.temporal().rootContextDate(prop);
                    return hd != null ? hd
                            : contextDate(target.rootTemporalDates(), prop);
                }
                if (prop.equals("businessDate") || prop.equals("processingDate")) {
                    // FOR-EACH-DATE: the generated date reads THE DATES
                    // COLUMN off the joined row (the engine projects the
                    // calendar date per row — temporalDateProjectionQuery)
                    String fed = target.temporal().forEachDateColumn();
                    if (fed != null) {
                        return milestoneColumnRead(fed, target.freshRowVar(),
                                target.rowType(), "", n);
                    }
                }
                // VERSION SWEEP (allVersions / allVersionsInRange — the
                // root context is EMPTY): each version row's OWN
                // validity-start column IS its generated date (engine maps
                // the property to BUS_FROM / PROCESSING_IN / snapshot)
                if (prop.equals("businessDate") || prop.equals("processingDate")) {
                    String col = target.milestoneColumns().get(
                            prop.equals("processingDate")
                                    ? TemporalFrame.GEN_PROCESSING_DATE
                                    : TemporalFrame.GEN_BUSINESS_DATE);
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
                // the chain was ->cast(@gate) in CHAIN position: a read of
                // the gate class's own property is the value-position
                // $p->cast(@gate).prop read (witness-gated subtype column)
                if (target.row().castGate() != null
                        && n instanceof TypedPropertyAccess gpa) {
                    return castLeafRead(target.row().castGate(), gpa.source(), gpa);
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


    /** {@link Registries} callee lookups are nullable (nested
     * predFilteredPipe registries carry none); the emitting arms
     * REQUIRE them — loud with the registry named. */
    private com.legend.compiler.element.TypedFunction neCallee() {
        return java.util.Objects.requireNonNull(target.isNotEmptyCallee(),
                "registries built without an isNotEmpty callee");
    }

    private boolean rootsAtUserVar(TypedSpec inst) {
        while (true) {
            if (inst instanceof TypedNativeCall w && w.args().size() == 1
                    && (com.legend.builtin.Pure.isToOneCall(
                            w.callee().qualifiedName())
                            || "meta::pure::functions::collection::first"
                                    .equals(w.callee().qualifiedName()))) {
                inst = w.args().get(0);
            } else if (inst instanceof TypedPropertyAccess pa) {
                inst = pa.source();
            } else {
                return inst instanceof TypedVariable v
                        && v.name().equals(target.userVar());
            }
        }
    }

    /** See the objectReferenceIn arm. Single-pk sets only (the corpus
     * shape); refs fold through take(coll, n) over the literal list. */
    private TypedSpec objectReferenceInRewrite(TypedNativeCall oc) {
        TypedSpec refsArg = oc.args().get(1);
        while (refsArg instanceof TypedNativeCall w && w.args().size() == 1
                && (com.legend.builtin.Pure.isToOneCall(w.callee().qualifiedName())
                        || w.callee().qualifiedName().endsWith("::first"))) {
            refsArg = w.args().get(0);
        }
        if (refsArg instanceof com.legend.compiler.spec.typed.TypedLimit tk
                && tk.source() instanceof
                        com.legend.compiler.spec.typed.TypedCollection tc0
                && tk.count() instanceof
                        com.legend.compiler.spec.typed.TypedCInteger tn) {
            refsArg = new com.legend.compiler.spec.typed.TypedCollection(
                    tc0.elements().subList(0, Math.min(
                            tn.value().intValue(), tc0.elements().size())),
                    tc0.info());
        }
        // the engine's generateObjectReferences[ForGivenSetId] call (or a
        // spelled collection of them): the pk maps are SPELLED on the
        // typed call — read them, never a reference string (batch 72b)
        List<Map<String, Object>> generated = ObjectReferenceArms.generatorPkMaps(refsArg);
        if (generated != null) {
            return pkMembership(oc, generated);
        }
        // a SINGLE spelled reference string
        if (refsArg instanceof com.legend.compiler.spec.typed
                .TypedCString one) {
            refsArg = new com.legend.compiler.spec.typed.TypedCollection(
                    List.of(one), new ExprType(Type.Primitive.STRING,
                            Multiplicity.Bounded.ZERO_MANY));
        }
        if (target.regs().inCallee() == null) {
            throw new NotImplementedException("objectReferenceIn needs an"
                    + " in callee (registries built without one)");
        }
        if (!(refsArg instanceof
                com.legend.compiler.spec.typed.TypedCollection refs)) {
            if (target.regs().pkColumns().size() != 1) {
                throw new NotImplementedException("objectReferenceIn over runtime"
                        + " references needs a single-pk set — pk columns: "
                        + target.regs().pkColumns());
            }
            return ObjectReferenceArms.runtimeRefMembership(oc, refsArg,
                    pkColRead("pk$_0"), target.freshRowVar() + "_ref",
                    java.util.Objects.requireNonNull(target.regs().inCallee()));
        }
        List<Map<String, Object>> pkMaps = new java.util.ArrayList<>();
        for (TypedSpec r : refs.elements()) {
            if (!(r instanceof com.legend.compiler.spec.typed
                    .TypedCString rs)) {
                throw new NotImplementedException(
                        "objectReferenceIn reference is not a literal");
            }
            // F3.4: the REAL segment walk (AsorRef, the one protocol
            // owner) replaces the lastIndexOf(":{") substring heuristic
            AsorRef.Ref ref = AsorRef.decode(rs.value());
            Object pkObj = ref == null ? null
                    : com.legend.sql.Json.parseOne(ref.pkJson());
            if (!(pkObj instanceof Map<?, ?> pkMap) || pkMap.isEmpty()) {
                throw new NotImplementedException("objectReferenceIn pk"
                        + " segment did not decode: " + rs.value());
            }
            Map<String, Object> m = new java.util.LinkedHashMap<>();
            pkMap.forEach((k, v) -> m.put(String.valueOf(k), v));
            pkMaps.add(m);
        }
        return pkMembership(oc, pkMaps);
    }

    /** The pk MEMBERSHIP predicate over decoded/spelled pk maps: one IN
     * for a single shared key, else an OR of per-map ANDs; the empty set
     * is FALSE (real pure). */
    private TypedSpec pkMembership(TypedNativeCall oc, List<Map<String, Object>> pkMaps) {
        if (pkMaps.isEmpty()) {
            // membership over the empty set is FALSE (real pure)
            return new com.legend.compiler.spec.typed.TypedCBoolean(false,
                    new ExprType(Type.Primitive.BOOLEAN,
                            Multiplicity.Bounded.ONE));
        }
        // SINGLE shared key across refs -> one IN; else OR of per-ref ANDs
        String soleKey = pkMaps.get(0).size() == 1
                ? pkMaps.get(0).keySet().iterator().next() : null;
        boolean uniform = soleKey != null && pkMaps.stream().allMatch(m ->
                m.size() == 1 && m.keySet().iterator().next()
                        .equalsIgnoreCase(soleKey));
        if (uniform && soleKey != null) {
            List<TypedSpec> pkVals = new java.util.ArrayList<>();
            for (Map<String, Object> m : pkMaps) {
                pkVals.add(pkLiteral(m.values().iterator().next()));
            }
            Type valT = pkVals.get(0).info().type();
            return new TypedNativeCall(java.util.Objects.requireNonNull(
                    target.regs().inCallee()),
                    List.of(pkColRead(soleKey), new com.legend.compiler.spec
                            .typed.TypedCollection(pkVals,
                                    new ExprType(valT,
                                            Multiplicity.Bounded.ZERO_MANY))),
                    oc.info());
        }
        TypedFunction and = target.regs().andCallee();
        TypedFunction or = target.regs().orCallee();
        if (and == null || or == null) {
            throw new NotImplementedException("objectReferenceIn multi-"
                    + "column references need and/or callees");
        }
        var bool1 = new ExprType(Type.Primitive.BOOLEAN,
                Multiplicity.Bounded.ONE);
        TypedSpec orAll = null;
        for (Map<String, Object> m : pkMaps) {
            TypedSpec conj = null;
            for (var e : m.entrySet()) {
                TypedSpec eq = new TypedNativeCall(eqCallee(),
                        List.of(pkColRead(e.getKey()),
                                pkLiteral(e.getValue())), bool1);
                conj = conj == null ? eq
                        : new TypedNativeCall(and, List.of(conj, eq), bool1);
            }
            orAll = orAll == null ? conj : new TypedNativeCall(or,
                    List.of(orAll, java.util.Objects.requireNonNull(conj)),
                    bool1);
        }
        return java.util.Objects.requireNonNull(orAll);
    }

    /** A decoded pk value as a typed literal (JSON kind decides). */
    private static TypedSpec pkLiteral(Object v) {
        if (v instanceof Long pl) {
            return new com.legend.compiler.spec.typed.TypedCInteger(pl,
                    new ExprType(Type.Primitive.INTEGER,
                            Multiplicity.Bounded.ONE));
        }
        if (v instanceof String psv) {
            return new com.legend.compiler.spec.typed.TypedCString(psv,
                    new ExprType(Type.Primitive.STRING,
                            Multiplicity.Bounded.ONE));
        }
        throw new NotImplementedException(
                "objectReferenceIn pk value kind: " + v);
    }

    /** The row read for a reference pk key: {@code pk\$_0} = positional
     * over the set's pk columns; anything else matches a ROW COLUMN by
     * name (case-insensitive, quotes stripped). */
    private TypedSpec pkColRead(String key) {
        String pkCol;
        if (key.startsWith("pk$_")) {
            if (target.regs().pkColumns().size() != 1) {
                throw new NotImplementedException("objectReferenceIn needs"
                        + " a single-pk set — pk columns: "
                        + target.regs().pkColumns());
            }
            pkCol = target.regs().pkColumns().get(0);
        } else {
            pkCol = key;
        }
        Type.RelationType row = target.rowType();
        Type.Column col = row.columns().stream()
                .filter(c -> c.name().equalsIgnoreCase(pkCol)
                        || RelationalRootForm.stripQ(c.name())
                                .equalsIgnoreCase(pkCol))
                .findFirst().orElseThrow(() -> new NotImplementedException(
                        "objectReferenceIn pk column '" + pkCol
                                + "' is not on the row"));
        return new TypedPropertyAccess(
                new TypedVariable(target.freshRowVar(),
                        new ExprType(row, Multiplicity.Bounded.ONE)),
                col.name(), new ExprType(col.type(), col.multiplicity()));
    }

    /** Inner scopes inherit the PARENT'S boolean-machinery callees when
     * their own registries lack them (Registries.NONE / material-only
     * builds) — pkColumns stay the inner scope's own. */
    private Registries withCallees(Registries r) {
        return r.inCallee() != null && r.equalCallee() != null
                && r.isNotEmptyCallee() != null && r.andCallee() != null ? r
                : new Registries(r.assocs(), r.assocEnds(), r.existsSubs(),
                        r.aggReads(), r.inQueryReads(),
                        r.isNotEmptyCallee() != null ? r.isNotEmptyCallee()
                                : target.regs().isNotEmptyCallee(),
                        r.equalCallee() != null ? r.equalCallee()
                                : target.regs().equalCallee(),
                        r.pkColumns(),
                        r.inCallee() != null ? r.inCallee()
                                : target.regs().inCallee(),
                        r.andCallee() != null ? r.andCallee()
                                : target.regs().andCallee(),
                        r.orCallee() != null ? r.orCallee()
                                : target.regs().orCallee(),
                        r.failCallee() != null ? r.failCallee()
                                : target.regs().failCallee());
    }

    private com.legend.compiler.element.TypedFunction eqCallee() {
        return java.util.Objects.requireNonNull(target.equalCallee(),
                "registries built without an equal callee");
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
                    new TypedNativeCall(neCallee(), List.of(read),
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
        // project OVER THE INSTANCE ($p.<head>->toOne()->project(cols)):
        // the mini-relation becomes the correlated target set with the
        // cols substituted over the target's bindings (engine: the
        // constraint's project processes fully inside the emptiness
        // EXISTS — the col-demanded joins ride in the FROM tree).
        if (n instanceof TypedProject tp) {
            List<String> pp = pathOf(
                    InnerDemand.instanceProjectSource(tp), target.userVar());
            if (pp != null && pp.size() == 1
                    && target.existsSubs().containsKey(pp.get(0))) {
                return rewriteInstanceProject(tp,
                        target.existsSubs().get(pp.get(0)));
            }
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
        // XStore property-space LOCAL read (route A):
        // legacyLocalProperty($row, 'p') is the emission's spelling for a
        // set-local (+p) property — not a class property, so it rides a
        // marker; same head dispatch as an ordinary property read.
        if (n instanceof TypedNativeCall lpc
                && lpc.callee().qualifiedName().equals(
                        com.legend.builtin.Pure.LEGACY_LOCAL_PROPERTY_FQN)
                && lpc.args().get(0) instanceof TypedVariable lpv
                && lpv.name().equals(target.userVar())
                && lpc.args().get(1) instanceof
                        com.legend.compiler.spec.typed.TypedCString lps) {
            TypedSpec localArm = rewriteHeadProp(lps.value(), n);
            if (localArm != null) {
                return localArm;
            }
        }
        TypedSpec hoisted = hoistedRewriteArms(n);
        if (hoisted != null) {
            return hoisted;
        }
        return switch (n) {
            // $p->filter(pred).leaf — the if-as-filter idiom over the
            // INSTANCE itself (engine golden testConcatenateWithFilter:
            // CASE WHEN pred THEN leaf ELSE NULL). The inner param inlines
            // with the instance var; both rewrite through the normal arms.
            case TypedPropertyAccess pa
                    when pa.source() instanceof TypedFilter f
                    && f.source() instanceof TypedVariable fv
                    && fv.name().equals(target.userVar())
                    && f.predicate().parameters().size() == 1
                    && f.predicate().body().size() == 1 ->
                    filteredInstanceRead(pa, f);
            case TypedPropertyAccess pa when unliftedFilteredRead(pa) ->
                    throw unliftedWall(pa);   // §4AD 5+7 route totality
            case TypedPropertyAccess pa when subTypeLeafRead(pa) != null ->
                    java.util.Objects.requireNonNull(subTypeLeafRead(pa), "subTypeLeafRead(pa)");
            case TypedVariable v when v.name().equals(target.userVar()) ->
                    throw new NotImplementedException(
                            "object-space use of the instance variable '$" + v.name()
                                    + "' other than property access is not supported yet");
            case TypedVariable v -> v;
            // HOST-side literal instance: a property read off ^X(k=v)
            // folds to the ctor value (engine constant-folds these before
            // routing; study #15)
            case TypedPropertyAccess pa
                    when pa.source() instanceof
                            com.legend.compiler.spec.typed.TypedNewInstance ni
                    && ni.properties().containsKey(pa.property()) ->
                    rewrite(ni.properties().get(pa.property()));
            // structural family: children rewrite, withChildren reassembles
            // — with the instance FOLD-THROUGH: a source that itself
            // folded to a ^X(k=v) literal feeds the outer read
            // ($host.coord.latitude chains fold hop by hop)
            case TypedPropertyAccess pa -> rebuildWithInstanceFold(pa);
            // a HOST literal instance passes through with its property
            // VALUES rewritten — downstream consumers (an inlined user
            // fn's property reads, the struct-value lowering) take it
            // from there
            case com.legend.compiler.spec.typed.TypedNewInstance ni ->
                    ni.mapChildren(this::rewrite);
            case TypedMilestonedAccess ma -> ma.mapChildren(this::rewrite);
            case TypedNativeCall c -> {
                // A REDUCER call whose collection arg is an object-space
                // ->map(computed) must NOT inline the mapper (audit 12 F4:
                // ->map(b|1)->sum() collapsed to the constant) — reducer
                // aggregation registers via the demand scan; reaching here
                // means the scan missed it, which stays a loud wall.
                if (CorrelatedSubselects.isAggregate(c)
                        && !c.args().isEmpty()
                        && c.args().get(0) instanceof TypedMap rm
                        && !Type.isRelation(rm.source().info().type())
                        && pathOf(rm.mapper().body().get(0),
                                rm.mapper().parameters().get(0)) == null) {
                    throw new NotImplementedException(
                            "reducer '" + c.callee().qualifiedName()
                            + "' over an unregistered computed ->map is not"
                            + " supported (the aggregate demand scan did not"
                            + " recognize this shape)");
                }
                yield c.mapChildren(this::rewrite);
            }
            case TypedCollection c -> c.mapChildren(this::rewrite);
            // ->cast(@T) over an object-space value: the cast rides, the
            // source substitutes (the in([...]) family spells casts over
            // property reads).
            case TypedCast tc -> tc.mapChildren(this::rewrite);
            case TypedIf i -> i.mapChildren(this::rewrite);
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
            case TypedMap m when listValueMap(m) ->
                    new TypedMap(rewrite(m.source()),
                            (TypedLambda) rewrite(m.mapper()), m.info());
            case TypedMap m when objectSpaceFanOut(m) ->
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
            case TypedFilter f when Type.isRelation(f.source().info().type()) ->
                    // body-only rewrite: the lambda's OWN param binds its
                    // relation row and must survive (rewriteLambda would
                    // rebind it to THIS scope's row var, orphaning reads)
                    new TypedFilter(rewrite(f.source()),
                            rewriteLambdaBodyOnly(f.predicate()), f.info());
            // RESOLVED RELATION MATERIAL in value position (the SubQueryLift
            // scalar subquery: an uncorrelated [0..1] single-column project
            // over resolved sources) — same pass-through family as the R2
            // constructed material above; the lift's uncorrelated guard
            // means nothing inside reads this scope's vars
            case TypedProject rp when Type.isRelation(rp.info().type()) -> n;
            // ...and the [0..1] LIMIT-1 tail of a correlated scalar
            // subquery (parentNavCondReads / navLeafSubquery emissions):
            // its correlation binds a FRESH row var, never this scope's
            case TypedLimit rl when Type.isRelation(rl.source().info().type()) -> n;
            // ...and its D6a successor (graph-leaf DISTINCT, not LIMIT 1)
            case com.legend.compiler.spec.typed.TypedDistinct rd
                    when Type.isRelation(rd.source().info().type()) -> n;
            case com.legend.compiler.spec.typed.TypedTds ignored -> n;
            // graphFetch in VALUE position is SOURCE-PRESERVING (engine
            // GraphFetchLowering = lower(source); the tree shapes only a
            // SERIALIZED result): the XStore result-sourcing idiom binds
            // 'let x = C.all()->toOne()->graphFetch(t)' and reads
            // $x->toOne().name — the value IS the instance set.
            case com.legend.compiler.spec.typed.TypedGraphFetch gf ->
                    rewrite(gf.source());
            // a STANDING user call (the inliner's recursion-defer
            // contract: host channels consume standing calls; the SQL
            // path walls here) — say WHY it stands, naming a detected
            // self-recursion cycle instead of dumping the node
            case com.legend.compiler.spec.typed.TypedUserCall uc -> {
                String shown = uc.callee().qualifiedName() + "/"
                        + uc.callee().parameters().size();
                throw new NotImplementedException(
                        com.legend.compiler.spec.UserCallInliner
                                .selfRecursive(uc.callee())
                        ? "TypedUserCall '" + shown + "' stands: recursion"
                                + " cycle involving " + shown + " (" + shown
                                + " -> " + shown + ") — recursive functions"
                                + " cannot lower to SQL"
                        : "object-space TypedUserCall '" + shown + "' did"
                                + " not β-reduce and cannot lower to"
                                + " SQL (H2 vocabulary)");
            }
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

    // ------------------------------------------------------------------
    // RUN-TIME BRANCH CHOICE ON THE ROW'S TYPE COLUMN (metamodel program
    // step 2, 2026-09-02). A union/inheritance row carries, per subtype
    // the mapping can name, thread-local columns (ClassMapping
    // .subTypeColumn) and — when membership is PARTIAL — a witness
    // column ($member: TRUE in conforming threads, NULL elsewhere). That
    // witness IS the row's type column for that subtype, so the three
    // pure type-dispatch forms over the instance variable lower to SQL
    // without a host call frame:
    //   $p->instanceOf(Sub)            → isNotEmpty(witness(Sub))
    //   $p->match([s:Sub[1]|v, …])     → if(witness(Sub1), v1, if(…, raise))
    //   $p->cast(@Sub).prop            → if(witness(Sub), stc read, raise)
    // TOTAL membership (no witness on the row) means every row conforms:
    // instanceOf is true, the arm is the catch-all, the cast is a plain
    // read. A subtype the row carries no columns for stays LOUD — never a
    // guessed boolean. Value arms only: an arm returning rows (a union of
    // per-kind extents) is a later step. Pure raises when no arm accepts
    // the value / the cast fails — so does the SQL (fail → ERROR).
    // ------------------------------------------------------------------

    static final String INSTANCE_OF_FQN = "meta::pure::functions::meta::instanceOf";
    static final String ELEMENT_TO_PATH_FQN = "meta::pure::functions::meta::elementToPath";

    /** The row's ONE primary-key pseudo-binding, rewritten into this scope. */
    private TypedSpec elementPath(TypedNativeCall c) {
        String key = null;
        int n = 0;
        for (String k : target.bindings().keySet()) {
            if (com.legend.model.ClassMapping.isPrimaryKeyBinding(k)) {
                key = k;
                n++;
            }
        }
        if (key == null || n != 1) {
            throw new NotImplementedException("elementToPath over a row of "
                    + target.classFqn() + ": the row keys on " + n
                    + " column(s) — one FQN key column is required");
        }
        return java.util.Objects.requireNonNull(rewriteHeadProp(key, c),
                "primary-key pseudo-binding read");
    }
    private static final String ANY_FQN = "meta::pure::metamodel::type::Any";

    /** The three dispatch forms over the instance variable; null when
     * {@code n} is none of them (the walk continues). */
    private @com.legend.Nullable TypedSpec typeDispatchArms(TypedSpec n) {
        return switch (n) {
            // $p->cast(@Sub).prop
            case TypedPropertyAccess pa
                    when pa.source() instanceof TypedCast hc
                    && hc.source() instanceof TypedVariable hv
                    && hv.name().equals(target.userVar())
                    && hc.target() instanceof Type.ClassType hct ->
                    castLeafRead(hct.fqn(), hc.source(), pa);
            // $p->match([s:Sub[1]|…, …]) — value arms
            case com.legend.compiler.spec.typed.TypedMatchRuntime mr
                    when mr.input() instanceof TypedVariable mv
                    && mv.name().equals(target.userVar()) ->
                    discriminatedMatch(mr);
            // $p->elementToPath(): an element's identity IS its row's key
            // (D2) — the primary-key pseudo-binding read; over a
            // REFERENCE it is the path literal
            case TypedNativeCall c
                    when c.callee().qualifiedName().equals(ELEMENT_TO_PATH_FQN)
                    && c.args().size() == 1
                    && c.args().get(0) instanceof
                            com.legend.compiler.spec.typed.TypedPackageableRef epr ->
                    new TypedCString(epr.fullPath(), c.info());
            case TypedNativeCall c
                    when c.callee().qualifiedName().equals(ELEMENT_TO_PATH_FQN)
                    && c.args().size() == 1
                    && c.args().get(0) instanceof TypedVariable ev
                    && ev.name().equals(target.userVar()) ->
                    elementPath(c);
            // $p->instanceOf(Sub)
            case TypedNativeCall c
                    when c.callee().qualifiedName().equals(INSTANCE_OF_FQN)
                    && c.args().size() == 2
                    && c.args().get(0) instanceof TypedVariable iv
                    && iv.name().equals(target.userVar()) ->
                    instanceOfHead(c);
            default -> null;
        };
    }

    /** The class a type-valued argument names: {@code @X} (TypedTypeRef)
     * or a bare class reference (TypedPackageableRef); null otherwise. */
    static @com.legend.Nullable String typeTargetFqn(TypedSpec typeArg) {
        return switch (typeArg) {
            case com.legend.compiler.spec.typed.TypedTypeRef tr ->
                    tr.target() instanceof Type.ClassType c ? c.fqn() : null;
            case com.legend.compiler.spec.typed.TypedPackageableRef pr ->
                    pr.fullPath();
            default -> null;
        };
    }

    /** The row's subtype binding table for {@code fqn}, or a loud wall. */
    private AssocSub subtypeTable(String fqn, String form) {
        AssocSub sub = target.assocs().get(SUBTYPE_KEY + fqn);
        if (sub == null) {
            throw new NotImplementedException(form + " over a row of "
                    + target.classFqn() + ": the row carries no columns of '"
                    + fqn + "' (not a mapped subtype on this row) — the"
                    + " run-time type is undecidable here [row columns "
                    + target.rowType().columns().stream().map(Type.Column::name).toList()
                    + "; subtype tables " + target.assocs().keySet().stream()
                            .filter(k -> k.startsWith(SUBTYPE_KEY)).toList() + "]");
        }
        return sub;
    }

    /** Whether every row conforms to {@code fqn} (no membership witness). */
    private static boolean totalMembership(AssocSub sub) {
        return !sub.targetBindings().containsKey(
                com.legend.model.ClassMapping.memberWitness());
    }

    /** {@code isNotEmpty(witness(fqn))} — the row's type test. */
    private TypedSpec witnessTest(String fqn) {
        return new TypedNativeCall(neCallee(),
                List.of(assocLeaf(SUBTYPE_KEY + fqn,
                        com.legend.model.ClassMapping.memberWitness())),
                new ExprType(Type.Primitive.BOOLEAN, Multiplicity.Bounded.ONE));
    }

    /** {@code fail(message)} standing in a VALUE position: pure raises
     * here and so does the SQL. The node carries the position's type
     * ({@code as}) — a raise has no value of its own, and the scalar
     * rule casts the ERROR call to that carrier so the CASE keeps its
     * type on every dialect. */
    private TypedSpec raise(String message, ExprType as) {
        TypedFunction fail = target.regs().failCallee();
        if (fail == null) {
            throw new NotImplementedException("run-time type dispatch in a"
                    + " scope without a fail callee (nested registries)");
        }
        return new TypedNativeCall(fail, List.of(new TypedCString(
                message, new ExprType(Type.Primitive.STRING,
                        Multiplicity.Bounded.ONE))), as);
    }

    private TypedSpec instanceOfHead(TypedNativeCall c) {
        String fqn = typeTargetFqn(c.args().get(1));
        if (fqn == null) {
            throw new NotImplementedException("instanceOf with a non-literal"
                    + " type argument over a mapped row");
        }
        if (fqn.equals(ANY_FQN) || fqn.equals(target.classFqn())) {
            return new TypedCBoolean(true, c.info());
        }
        AssocSub sub = subtypeTable(fqn, "instanceOf(" + fqn + ")");
        return totalMembership(sub)
                ? new TypedCBoolean(true, c.info())
                : witnessTest(fqn);
    }

    private TypedSpec castLeafRead(String fqn, TypedSpec head,
            TypedPropertyAccess pa) {
        if (fqn.equals(ANY_FQN) || fqn.equals(target.classFqn())) {
            // identity cast: the plain read
            return rewrite(new TypedPropertyAccess(head, pa.property(),
                    pa.info()));
        }
        AssocSub sub = subtypeTable(fqn, "cast(@" + fqn + ")");
        TypedSpec leaf = assocLeaf(SUBTYPE_KEY + fqn, pa.property());
        if (totalMembership(sub)) {
            return leaf;
        }
        // the raise stands PER JOINED ROW beside the leaf column (a to-many
        // leaf's list is the aggregation above this CASE): its stamp is
        // the scalar position, never the read's list multiplicity
        return new TypedIf(witnessTest(fqn), leaf,
                Optional.of(raise("Cast exception: " + target.classFqn()
                        + " cannot be cast to " + fqn,
                        new ExprType(pa.info().type(), Multiplicity.Bounded.ONE))),
                pa.info());
    }

    private TypedSpec discriminatedMatch(
            com.legend.compiler.spec.typed.TypedMatchRuntime mr) {
        if (mr.extra().isPresent()) {
            throw new NotImplementedException("match with an extra argument"
                    + " over a mapped row is not supported yet");
        }
        List<TypedSpec> conds = new ArrayList<>();
        List<TypedSpec> bodies = new ArrayList<>();
        TypedSpec elseArm = null;
        for (var arm : mr.arms()) {
            if (arm.body().info().type() instanceof Type.ClassType
                    || Type.isRelation(arm.body().info().type())) {
                throw new NotImplementedException("match arm '" + arm.typeFqn()
                        + "' over a mapped row returns rows — only VALUE arms"
                        + " lower today (a union of per-kind extents is a"
                        + " later step)");
            }
            if (arm.typeFqn().equals(ANY_FQN)
                    || arm.typeFqn().equals(target.classFqn())) {
                // catch-all: the parameter IS the instance
                elseArm = rewrite(inlineParam(arm.body(), arm.param(),
                        mr.input()));
                break;
            }
            AssocSub sub = subtypeTable(arm.typeFqn(),
                    "match arm '" + arm.typeFqn() + "'");
            TypedSpec body = rewrite(armParamReads(arm.body(), arm.param(),
                    arm.typeFqn()));
            if (totalMembership(sub)) {
                elseArm = body;   // every row conforms: unconditional arm
                break;
            }
            conds.add(witnessTest(arm.typeFqn()));
            bodies.add(body);
        }
        if (elseArm == null) {
            elseArm = raise("Match failure: no arm of ["
                    + mr.arms().stream().map(a -> a.typeFqn())
                            .collect(java.util.stream.Collectors.joining(", "))
                    + "] accepts the row's run-time type", mr.info());
        }
        TypedSpec out = elseArm;
        for (int i = conds.size() - 1; i >= 0; i--) {
            out = new TypedIf(conds.get(i), bodies.get(i), Optional.of(out),
                    mr.info());
        }
        return out;
    }

    /** An arm body with {@code $param.prop} reads served off the subtype's
     * binding table (the narrowed row); any other use of the parameter —
     * a navigation through it, the bare value — stays loud. */
    private TypedSpec armParamReads(TypedSpec n, String param, String fqn) {
        if (n instanceof TypedPropertyAccess pa
                && pa.source() instanceof TypedVariable v
                && v.name().equals(param)) {
            return assocLeaf(SUBTYPE_KEY + fqn, pa.property());
        }
        if (n instanceof TypedVariable v && v.name().equals(param)) {
            throw new NotImplementedException("match arm parameter '$" + param
                    + "' (" + fqn + ") used other than as a direct property"
                    + " read (navigation / whole value) is not supported yet");
        }
        if (n instanceof TypedLambda l && l.parameters().contains(param)) {
            return n;   // shadowed
        }
        return n.mapChildren(k -> armParamReads(k, param, fqn));
    }

    /** Leaf read through a filter on the INSTANCE itself (engine golden
     *  testConcatenateWithFilter: CASE WHEN pred THEN leaf ELSE NULL). */
    private TypedSpec filteredInstanceRead(TypedPropertyAccess pa, TypedFilter f) {
        TypedSpec pred = inlineParam(f.predicate().body().get(0),
                f.predicate().parameters().get(0), f.source());
        return new TypedIf(rewrite(pred),
                rewrite(new TypedPropertyAccess(f.source(),
                        pa.property(), pa.info())),
                java.util.Optional.empty(), pa.info());
    }

    /** Structural rebuild with the instance FOLD-THROUGH: a source that
     *  itself folded to a {@code ^X(k=v)} literal feeds the outer read
     *  ({@code $host.coord.latitude} chains fold hop by hop). */
    private TypedSpec rebuildWithInstanceFold(TypedPropertyAccess pa) {
        TypedSpec rebuilt = pa.mapChildren(this::rewrite);
        if (rebuilt instanceof TypedPropertyAccess rp
                && rp.source() instanceof
                        com.legend.compiler.spec.typed.TypedNewInstance ni
                && ni.properties().containsKey(rp.property())) {
            return rewrite(java.util.Objects.requireNonNull(
                    ni.properties().get(rp.property())));
        }
        return rebuilt;
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
        // a TEMPORAL class mapped WITHOUT milestone columns (engine
        // noMilestoningMap: allVersions over a plain table): the
        // generated struct's dates read NULL — `null as "from"` — never
        // the undemanded-navigation invariant error
        if (head.equals("milestoning")
                && target.milestoneColumns().isEmpty()
                && !target.bindings().containsKey("milestoning")) {
            return new com.legend.compiler.spec.typed.TypedCollection(
                    java.util.List.of(),
                    new com.legend.compiler.element.type.ExprType(
                            original.info().type(),
                            com.legend.compiler.element.type.Multiplicity
                                    .Bounded.ZERO_ONE));
        }
        TypedSpec headBinding = target.bindings().get(head);
        if (headBinding != null) {
            TypedSpec inner = headBinding;
            if (inner instanceof TypedNativeCall c && c.args().size() == 1
                    && com.legend.builtin.Pure.isToOneCall(c.callee().qualifiedName())) {
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
                if (leafExpr == null
                        && com.legend.model.ClassMapping.isSubTypeColumn(leaf)) {
                    // subtype-cast leaf over an embedded ctor whose class
                    // carries the property PLAIN (the reconciled base
                    // recompose): the cast is row-neutral — only the
                    // member thread that maps the field projects non-NULL
                    // (inline-embedded golden vehicleOwner->subType(@Person))
                    int cut = leaf.lastIndexOf("___");
                    leafExpr = cut < 0 ? null
                            : ctor.properties().get(leaf.substring(cut + 3));
                }
                if (leafExpr == null) {
                    // C0.5b: name the REAL property, never our synthetic
                    // #fN / dated identifiers (blaming users for internal
                    // names was the audit's misattribution finding)
                    throw new MappingResolutionException("property '"
                            + SyntheticHeads.realHead(leaf)
                            + "' of embedded '" + SyntheticHeads.realHead(head)
                            + "' on class '"
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
                // H5c cast-nav: the head registered as an UPSTREAM
                // association hop whose AssocSub carries the cast
                // target's COMPOSED bindings — dispatch like any assoc
                if (target.assocs().containsKey(head)) {
                    return assocLeaf(head, leaf);
                }
                throw new NotImplementedException("navigation '$" + target.userVar()
                        + "." + head + "." + leaf + "' crosses a MODEL-TO-MODEL"
                        + " cast binding — not supported yet (H5c)");
            }
            throw new NotImplementedException("navigation through class-typed"
                    + " slot property '" + head + "' is not supported yet"
                    + " [assocs=" + target.assocs().keySet() + "; inner="
                    + inner.getClass().getSimpleName() + "; leaf="
                    + leaf + "]");
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
                List<TypedSpec> own = target.headTemporalDates().get(head);
                if (own != null && !own.isEmpty()) {
                    return contextDate(own, leaf);
                }
                // ROOT propagation is DIMENSION-AWARE (audit 23): the
                // positional list silently served the root business date
                // to a processing-temporal target
                TypedSpec rd = target.temporal().rootContextDate(leaf);
                if (rd != null) {
                    return rd;
                }
                if (!target.rootTemporalDates().isEmpty()) {
                    return contextDate(target.rootTemporalDates(), leaf);
                }
            }
            if (target.nested()) {
                throw new NotImplementedException("nested navigation '" + head
                        + "." + leaf + "' inside an exists/isEmpty predicate is"
                        + " not supported yet");
            }
            throw new MappingResolutionException("property '"
                    + SyntheticHeads.realHead(leaf)
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
        TypedSpec leafInner = Pipelines.unwrapToOne(leafBinding);
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

    /** A target-side binder freshened against EVERY name in reach —
     * the two-name check captured a third in-scope name (triply-nested
     * exists, a user var literally named t_n; audit 23 #75). */
    private String freshTargetBinder(String tVar, TypedLambda cond,
            @com.legend.Nullable TypedSpec extra) {
        java.util.Set<String> taken = new java.util.LinkedHashSet<>();
        taken.add(target.freshRowVar());
        taken.add(target.userVar());
        for (TypedSpec b : cond.body()) {
            CorrelatedSubselects.collectVarNamesInto(b, taken);
        }
        if (extra != null) {
            CorrelatedSubselects.collectVarNamesInto(extra, taken);
        }
        // tVar is NOT exempted: when the binder name coincides with the
        // enclosing renamed var (the audit-18 't' shadowing class), the
        // rename MUST fire — an extra cosmetic rename is harmless, a
        // missed one self-correlates (the ResolveNestedNavTest pin)
        String fresh = tVar;
        while (taken.contains(fresh)) {
            fresh = fresh + "_n";
        }
        return fresh;
    }

    /** A milestone-column read off the (possibly prefixed) row. */
    private static TypedSpec milestoneColumnRead(String column, String rowVar,
            Type.RelationType row, String prefix, TypedSpec original) {
        String name = prefix + column;
        // EXACT match first; case-insensitive only when UNIQUE (audit 23
        // #75: two case-differing columns read whichever came first)
        Type.Column ci = null;
        boolean ciAmbiguous = false;
        for (Type.Column c : row.columns()) {
            if (c.name().equals(name)) {
                return new TypedPropertyAccess(
                        new TypedVariable(rowVar,
                                new ExprType(row, Multiplicity.Bounded.ONE)),
                        c.name(), new ExprType(c.type(), c.multiplicity()));
            }
            if (c.name().equalsIgnoreCase(name)) {
                ciAmbiguous = ci != null;
                ci = c;
            }
        }
        if (ci != null && !ciAmbiguous) {
            return new TypedPropertyAccess(
                    new TypedVariable(rowVar,
                            new ExprType(row, Multiplicity.Bounded.ONE)),
                    ci.name(), new ExprType(ci.type(), ci.multiplicity()));
        }
        if (ciAmbiguous) {
            throw new NotImplementedException("milestone column '" + name
                    + "' matches multiple case-differing columns on the"
                    + " substitution row");
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
            // structural family: children inline, withChildren reassembles
            case TypedPropertyAccess pa ->
                    pa.mapChildren(k -> inlineParam(k, param, source));
            case TypedMilestonedAccess ma ->
                    ma.mapChildren(k -> inlineParam(k, param, source));
            case TypedNativeCall c ->
                    c.mapChildren(k -> inlineParam(k, param, source));
            case TypedIf i ->
                    i.mapChildren(k -> inlineParam(k, param, source));
            case TypedCollection c ->
                    c.mapChildren(k -> inlineParam(k, param, source));
            // structural relation ops in the mapper body (a filtered/
            // mapped/sorted collection expression): children inline; the
            // TypedLambda arm's shadow + capture guards apply to their
            // inner lambdas exactly as anywhere else
            case TypedFilter f ->
                    f.mapChildren(k -> inlineParam(k, param, source));
            case TypedMap m ->
                    m.mapChildren(k -> inlineParam(k, param, source));
            case com.legend.compiler.spec.typed.TypedSortBy sb ->
                    sb.mapChildren(k -> inlineParam(k, param, source));
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
        return com.legend.compiler.spec.typed.VarUse.reads(n, var);
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
    /** The contains/in {@code val:[1]} slot: a SINGLETON literal IS the
     * to-one value (pure InstanceValue semantics — contains(['SRCE'])
     * compares the STRING; Scalars' list rule unwraps identically, this
     * is the membership-EXISTS route's copy of the same law). */
    private static TypedSpec singletonNeedle(TypedSpec v) {
        return v instanceof com.legend.compiler.spec.typed.TypedCollection tc
                && tc.elements().size() == 1 ? tc.elements().get(0) : v;
    }

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
        String freshT = freshTargetBinder(tVar, cond, null);
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
        TypedSpec eq = new TypedNativeCall(eqCallee(),
                List.of(leafBinding, rewrite(singletonNeedle(needle))),
                new ExprType(Type.Primitive.BOOLEAN, Multiplicity.Bounded.ONE));
        TypedLambda memberPred = new TypedLambda(List.of(ex.targetRowVar()),
                List.of(eq), predType);
        TypedSpec rel = new TypedFilter(
                new TypedFilter(
                        ex.targetPipeline(), corr, ex.targetPipeline().info(),
                        com.legend.compiler.spec.typed.TypedFilter
                                .Stamp.CORRELATION),
                memberPred, ex.targetPipeline().info());
        return new TypedNativeCall(neCallee(), List.of(rel),
                new ExprType(Type.Primitive.BOOLEAN, Multiplicity.Bounded.ONE));
    }

    /** The {@code $p.head.leaf} read inside a boolean leaf whose head is
     * a TO-MANY association crossing; null when none. A leaf crossing
     * TWO DISTINCT to-many heads is LOUD (audit 23 B1): the isolation
     * null-guard covers one read — guarding only the first silently
     * inverts booleans for parents empty on the other head. */
    private @com.legend.Nullable TypedSpec toManyCrossingRead(TypedSpec n) {
        List<TypedSpec> all = new ArrayList<>();
        collectToManyCrossings(n, all);
        if (all.isEmpty()) {
            return null;
        }
        java.util.Set<String> heads = new java.util.LinkedHashSet<>();
        for (TypedSpec r : all) {
            heads.add(java.util.Objects.requireNonNull(
                    pathOf(r, target.userVar())).get(0));
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
    static @com.legend.Nullable TypedNewInstance embeddedPartialOf(
            @com.legend.Nullable TypedSpec binding) {
        if (binding == null) {
            return null;
        }
        TypedSpec inner = Pipelines.unwrapToOne(binding);
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
            if (partialLeaf(partial, path) == null) {
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
            return;   // MAXIMAL path only — a prefix is not a leaf read
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
    private @com.legend.Nullable TypedSpec rewriteEmbeddedExists(TypedLambda pl,
            TypedNewInstance partial) {
        // audit 23: a multi-statement predicate body would silently DROP
        // its leading statements (a let's variable then leaks through the
        // bare-var sink as an unbound read) — loud until let-carrying
        // embedded predicates are threaded
        if (pl.body().size() != 1) {
            throw new NotImplementedException("embedded-exists predicate"
                    + " with " + pl.body().size() + " statements (let-"
                    + "carrying bodies) is not supported yet");
        }
        TypedSpec body = substEmbeddedReads(
                pl.body().get(0), pl.parameters().get(0), partial);
        return rewrite(body);
    }

    private TypedSpec substEmbeddedReads(TypedSpec n, String var,
            TypedNewInstance partial) {
        List<String> p = pathOf(n, var);
        if (p != null) {
            TypedSpec lf = partialLeaf(partial, p);
            if (lf != null) {
                return renameRowVar(lf);
            }
        }
        if (n instanceof TypedNativeCall c) {
            return c.withChildren(c.args().stream().map(a -> substEmbeddedReads(a, var, partial))
                            .toList());
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
        // audit 23: an unhandled node KIND that still reads the embedded
        // var would escape substitution — wrong correlation or a binder
        // leak. Nodes NOT reading the var pass through untouched.
        if (readsVariable(n, var)) {
            throw new NotImplementedException("embedded-exists predicate"
                    + " node " + n.getClass().getSimpleName() + " reading"
                    + " the embedded instance is not substitutable yet");
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
    /** The nested-scope relation arms (hoisted out of the rewrite
     * switch): null when neither applies, and the switch proceeds.
     * <ul>
     *   <li>FOREIGN-ROOTED object-space filter inside a NESTED pred
     *   scope (the nested-exists rung: {@code isNotEmpty(filter(
     *   $this.employees.addresses, b|..))} inside an outer exists
     *   predicate — {@code $this} belongs to the ENCLOSING scope): the
     *   source chain passes through verbatim for the outer correlation
     *   re-pass, which owns the root var and consumes the dotted EXISTS
     *   material; the predicate's OWN reads of THIS scope's var still
     *   rewrite (body-only — the lambda's binder survives for the outer
     *   pass's target rewrite).</li>
     *   <li>TDS pipeline OVER AN INSTANCE PROJECT (constraint 3 family:
     *   {@code $this.employees->project(..)->groupBy(..)->filter(..)
     *   ->tdsRows()->isEmpty()}): the groupBy is a structural relation
     *   op once its source takes the rewriteInstanceProject arm —
     *   children rewrite (agg/key lambdas bind THEIR OWN relation rows
     *   and pass the shadow rule). Any other object-space groupBy stays
     *   loud.</li>
     *   <li>{@code objectReferenceIn($p, refs)}: ASOR references DECODE
     *   at resolution (they are literals after harness extraction) into
     *   a primary-key membership predicate — the engine's store-object-
     *   reference round trip. The instance may be an EMBEDDED nav
     *   ({@code $p.firm->toOne()}) — its pk columns live on the SAME
     *   row, and the keyed decode matches row columns; a joined nav's
     *   missing columns stay loud at pkColRead.</li>
     * </ul> */
    private @com.legend.Nullable TypedSpec hoistedRewriteArms(TypedSpec n) {
        if (n instanceof TypedNativeCall oc && oc.args().size() == 2
                && "meta::pure::functions::collection::objectReferenceIn"
                        .equals(oc.callee().qualifiedName())
                && rootsAtUserVar(oc.args().get(0))) {
            return objectReferenceInRewrite(oc);
        }
        if (n instanceof TypedFilter f
                && !Type.isRelation(f.source().info().type())
                && target.nested() && foreignRootedNav(f.source())) {
            return new TypedFilter(f.source(),
                    rewriteLambdaBodyOnly(f.predicate()), f.info());
        }
        if (n instanceof com.legend.compiler.spec.typed.TypedGroupBy g
                && g.source() instanceof TypedProject gp
                && instanceProjectPath(gp) != null) {
            return g.mapChildren(this::rewrite);
        }
        return null;
    }

    /** The registered EXISTS head a project-over-instance source reads
     * ({@code $this.<head>->project(..)}), null when unregistered — ONE
     * recognizer for the direct arm and the groupBy-source gate. */
    private @com.legend.Nullable String instanceProjectPath(TypedProject tp) {
        List<String> pp = pathOf(InnerDemand.instanceProjectSource(tp),
                target.userVar());
        return pp != null && pp.size() == 1
                && target.existsSubs().containsKey(pp.get(0))
                ? pp.get(0) : null;
    }

    /** True when the expression is a property-path chain rooted at a
     * variable this scope does NOT own — the ENCLOSING scope's instance
     * var (the nested-exists rung's pass-through gate). */
    private boolean foreignRootedNav(TypedSpec n) {
        TypedSpec src = n;
        while (src instanceof TypedPropertyAccess pa) {
            src = pa.source();
        }
        return src instanceof TypedVariable v
                && !v.name().equals(target.userVar())
                && !v.name().equals(target.freshRowVar());
    }

    /** The route-totality wall (§4AD batches 5+7): every filtered-nav
     * read lifts; one still spelling raw at substitution is a LIFT GAP —
     * loud, never a silent correlated subquery. */
    private NotImplementedException unliftedWall(TypedPropertyAccess pa) {
        return new NotImplementedException(
                "filtered-navigation read '" + pa.property()
                + "' reached substitution unlifted — the router owns this"
                + " shape (batches 5+7); the lift pre-pass must rewrite it"
                + " [userVar=" + target.userVar() + "]");
    }

    /** §4AD batches 5+7 — the SHAPE TEST that guards the route-totality
     * WALL: a scalar read whose source unwraps (class hops, then
     * toOne()/first()/head() multiplicity wrappers) to a filtered
     * navigation. The correlated EMITTER this matcher used to feed
     * (filteredNavLeafRead — the banned correlated-scalar-subquery arm)
     * is DELETED: every position lifts to the #fN fan-out join, and the
     * census measured every surviving dispatch at ZERO before this
     * deletion. Bare un-wrapped [*] reads are NOT matched — the fan
     * channel owns them (audit 9).
     */
    private boolean unliftedFilteredRead(TypedPropertyAccess pa) {
        TypedSpec src = pa.source();
        while (src instanceof TypedPropertyAccess hp
                && hp.info().type() instanceof Type.ClassType) {
            src = hp.source();
        }
        boolean sawWrapper = false;
        while (src instanceof TypedNativeCall c && c.args().size() == 1
                && (com.legend.builtin.Pure.isToOneCall(
                        c.callee().qualifiedName())
                    || c.callee().qualifiedName().equals(
                        "meta::pure::functions::collection::first")
                    || c.callee().qualifiedName().equals(
                        "meta::pure::functions::collection::head"))) {
            src = c.args().get(0);
            sawWrapper = true;
        }
        if (!sawWrapper
                && !(pa.info().multiplicity() instanceof Multiplicity.Bounded b
                        && Integer.valueOf(1).equals(b.upper()))) {
            return false;
        }
        return src instanceof TypedFilter f
                && f.predicate().parameters().size() == 1;
    }

    /** $r->subType(@Sub).prop — the cast is a same-source dispatch: the
     * read resolves through the SUB class's registered binding table
     * renamed onto the row (non-member rows read the sub's columns as
     * NULL naturally). Null when the source is not a subType cast of
     * the instance variable; a cast whose subtype has no registration
     * (unmapped subtype, own-source subtype, or a nested position whose
     * registries never saw the scan) stays loud. */
    private @com.legend.Nullable TypedSpec subTypeLeafRead(TypedPropertyAccess pa) {
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

    /** The correlated target set for an {@link ExistsSub}: the target
     * pipeline filtered by the oriented condition, target-side reads
     * bound to a fresh binder, parent-side reads re-pointed at the
     * OUTER row var (the lowerer's enclosing-scope channel). */
    private record CorrTarget(TypedSpec rel, String binder) {}

    /** The concat-split emission: per-branch emptiness calls with
     * MERGED correlation keys (each branch ORs in every sibling's key
     * condition that binds on its row — the engine's union-with-merged-
     * columns semantics), folded with OR (exists/isNotEmpty) or AND
     * (isEmpty). */
    private TypedSpec mergedConcatExists(TypedNativeCall call,
            List<ExistsSub> subs, List<TypedLambda> rootConds) {
        List<TypedSpec> branches = new ArrayList<>(subs.size());
        for (int i = 0; i < subs.size(); i++) {
            List<TypedLambda> sibs = new ArrayList<>();
            for (int j = 0; j < subs.size(); j++) {
                if (j != i) {
                    sibs.add(rootConds.get(j));
                }
            }
            branches.add(rewriteExists(call, subs.get(i), List.of(), sibs));
        }
        TypedFunction fold = Pure.nativeNamed("isEmpty",
                call.callee().signatureKey())
                ? target.regs().andCallee() : target.regs().orCallee();
        TypedSpec out = branches.get(0);
        for (int i = 1; i < branches.size(); i++) {
            out = new TypedNativeCall(
                    java.util.Objects.requireNonNull(fold, "fold callee"),
                    List.of(out, branches.get(i)), call.info());
        }
        return out;
    }

    /** Whether every target-side read of {@code cond} (params
     * (parent, target)) names a column of {@code row} — the merge-by-
     * column-name admission test. */
    private static boolean condBindsOnRow(TypedLambda cond,
            Type.RelationType row) {
        Set<String> reads = new LinkedHashSet<>();
        for (TypedSpec b : cond.body()) {
            Pipelines.collectVarReads(b, cond.parameters().get(1), reads);
        }
        Set<String> cols = new LinkedHashSet<>();
        for (Type.Column c : row.columns()) {
            cols.add(c.name());
        }
        return !reads.isEmpty() && cols.containsAll(reads);
    }

    private TypedSpec boolFold(List<TypedSpec> conj, TypedFunction callee) {
        TypedSpec out = conj.get(0);
        for (int i = 1; i < conj.size(); i++) {
            out = new TypedNativeCall(callee, List.of(out, conj.get(i)),
                    new ExprType(Type.Primitive.BOOLEAN,
                            Multiplicity.Bounded.ONE));
        }
        return out;
    }

    private CorrTarget correlateTarget(ExistsSub ex) {
        return correlateTarget(ex, List.of());
    }

    private CorrTarget correlateTarget(ExistsSub ex,
            List<TypedLambda> siblingConds) {
        TypedLambda cond = ex.orientedCond();   // params (parentRow, targetRow)
        String pVar = cond.parameters().get(0);
        String tVar = cond.parameters().get(1);
        // NESTED levels reuse the join conditions' literal param names
        // (λ(s,t) everywhere) — an inner 't' would SHADOW the enclosing
        // scope's correlation var (R2: nested exists silently misbound).
        // Freshen collision-driven; target-side reads rename with it.
        String freshT = freshTargetBinder(tVar, cond, null);
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
        // MERGED-KEY siblings (concat-split): each sibling condition that
        // binds on THIS branch's row ORs in, rebound by column name —
        // conjunct lists AND-fold first (multi-statement lambda lesson)
        if (!siblingConds.isEmpty() && target.regs().orCallee() != null
                && target.regs().andCallee() != null) {
            List<TypedSpec> alts = new ArrayList<>();
            alts.add(boolFold(corrBody, target.regs().andCallee()));
            for (TypedLambda sc : siblingConds) {
                if (!condBindsOnRow(sc, ex.targetRow())) {
                    continue;
                }
                List<TypedSpec> sb = sc.body().stream()
                        .map(b -> Pipelines.rewriteRowReads(b,
                                sc.parameters().get(1), Map.of(), Set.of(),
                                v -> new TypedVariable(tRenamed,
                                        new ExprType(ex.targetRow(),
                                                Multiplicity.Bounded.ONE))))
                        .map(b -> Pipelines.rewriteRowReads(b,
                                sc.parameters().get(0), Map.of(), Set.of(),
                                v -> new TypedVariable(target.freshRowVar(),
                                        new ExprType(target.rowType(),
                                                Multiplicity.Bounded.ONE))))
                        .toList();
                alts.add(boolFold(sb, target.regs().andCallee()));
            }
            if (alts.size() > 1) {
                corrBody = List.of(boolFold(alts, target.regs().orCallee()));
            }
        }
        TypedLambda corr = new TypedLambda(List.of(tRenamed), corrBody,
                new ExprType(new Type.FunctionType(
                        List.of(new Type.Param(ex.targetRow(), Multiplicity.Bounded.ONE)),
                        new Type.Param(Type.Primitive.BOOLEAN, Multiplicity.Bounded.ONE)),
                        Multiplicity.Bounded.ONE));
        return new CorrTarget(new TypedFilter(
                ex.targetPipeline(), corr, ex.targetPipeline().info(),
                com.legend.compiler.spec.typed.TypedFilter.Stamp.CORRELATION),
                tRenamed);
    }

    private TypedSpec rewriteExists(TypedNativeCall call, ExistsSub ex,
            List<TypedLambda> chainPreds) {
        return rewriteExists(call, ex, chainPreds, List.of());
    }

    private TypedSpec rewriteExists(TypedNativeCall call, ExistsSub ex,
            List<TypedLambda> chainPreds, List<TypedLambda> siblingConds) {
        CorrTarget ct = correlateTarget(ex, siblingConds);
        final String tRenamed = ct.binder();
        TypedSpec rel = ct.rel();
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
                    withCallees(ex.innerRegs()), TemporalView.NONE, true, true));
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
                    withCallees(ex.innerRegs()), TemporalView.NONE, true, true));
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
        return call.withChildren(newArgs);
    }

    /** The project-over-instance rewrite (constraint 1c): each col fn
     * substitutes over the target's bindings like an exists predicate,
     * then a second pass through THIS substitution correlates its OUTER
     * reads ({@code $this.businessDate} date args). The project's own
     * relation type survives unchanged. */
    private TypedSpec rewriteInstanceProject(TypedProject tp, ExistsSub ex) {
        CorrTarget ct = correlateTarget(ex);
        Set<String> unconvertedSlots =
                new LinkedHashSet<>(ex.targetSlotAliases());
        unconvertedSlots.removeAll(ex.targetSlotPrefixes().keySet());
        List<TypedFuncCol> cols = new ArrayList<>();
        for (TypedFuncCol c : tp.columns()) {
            Substitution colSub = new Substitution(new Target(
                    new RowScope(c.fn().parameters().get(0), ct.binder(),
                            ex.targetClassFqn(), target.mappingFqn(),
                            ex.targetRowVar(), ex.targetBindings(),
                            ex.targetRow(), unconvertedSlots,
                            ex.targetSlotPrefixes(), Map.of()),
                    withCallees(ex.innerRegs()), TemporalView.NONE, true, true));
            TypedLambda inner = colSub.rewriteLambda(c.fn());
            cols.add(new TypedFuncCol(c.name(),
                    new TypedLambda(inner.parameters(),
                            inner.body().stream().map(this::rewrite).toList(),
                            inner.info()),
                    c.documentation()));
        }
        return new TypedProject(ct.rel(), cols, tp.info());
    }

    /**
     * THE otherwise recognizer (one, shared with the demand scan): the
     * binding's {@code otherwise(^Inner(...), $row.<slot>)} call, looking
     * through a {@code toOne} wrap; {@code null} when the binding is not an
     * otherwise composition. The normalizer emits exactly this shape —
     * partial FIRST, fallback slot read second (canonical by construction).
     */
    static @com.legend.Nullable TypedNativeCall otherwiseOf(TypedSpec binding) {
        TypedSpec inner = binding;
        if (inner instanceof TypedNativeCall c && c.args().size() == 1
                && com.legend.builtin.Pure.isToOneCall(c.callee().qualifiedName())) {
            inner = c.args().get(0);
        }
        if (inner instanceof TypedNativeCall oc && oc.args().size() == 2
                && Pure.nativeNamed(Pure.Lite.OTHERWISE,
                        oc.callee().signatureKey())
                && oc.args().get(0)
                        instanceof TypedNewInstance) {
            return oc;
        }
        return null;
    }

    /** The EMBEDDED-CTOR tail walk under a sub-nav: from {@code path[hop]}
     * (a {@code ^Inner(...)}-valued binding on the sub-target, toOne/
     * otherwise looked through) descend ctor properties to the leaf.
     * Returns the leaf's binding expression (over the sub-target's row
     * var), or {@code null} when any hop is not a ctor property — the
     * caller's loud wall stands. */
    private static @com.legend.Nullable TypedSpec ctorTailLeaf(SubNav sub, List<String> path,
            int hop) {
        return descendLeaf(sub.bindings().get(path.get(hop)), path, hop + 1);
    }

    /** The embedded-partial twin of {@link #ctorTailLeaf}: descend the
     * predicate path INSIDE the partial's ctor tree (ledger cluster 49 —
     * the gate and the substitution share this ONE resolver so they
     * cannot drift). A class-typed terminus is not a leaf: stays loud. */
    private static @com.legend.Nullable TypedSpec partialLeaf(
            TypedNewInstance partial, List<String> path) {
        TypedSpec r = descendLeaf(
                partial.properties().get(path.get(0)), path, 1);
        return r instanceof TypedNewInstance ? null : r;
    }

    private static @com.legend.Nullable TypedSpec descendLeaf(
            @com.legend.Nullable TypedSpec cur, List<String> path, int hop) {
        int h = hop;
        while (cur != null && h < path.size()) {
            TypedSpec inner = cur;
            if (inner instanceof TypedNativeCall c && c.args().size() == 1
                    && com.legend.builtin.Pure.isToOneCall(c.callee().qualifiedName())) {
                inner = c.args().get(0);
            }
            var ow = otherwiseOf(inner);
            if (ow != null) {
                inner = ow.args().get(0);
            }
            if (inner instanceof TypedNewInstance ni
                    && ni.properties().containsKey(path.get(h))) {
                cur = ni.properties().get(path.get(h));
                h++;
            } else {
                cur = null;
            }
        }
        if (cur instanceof TypedNativeCall c && c.args().size() == 1
                && com.legend.builtin.Pure.isToOneCall(c.callee().qualifiedName())) {
            cur = c.args().get(0);
        }
        return cur;
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
