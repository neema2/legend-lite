// SPDX-License-Identifier: Apache-2.0

package com.legend.normalizer;

import com.legend.builtin.Pure;
import com.legend.compiler.ModelBuilder;
import com.legend.compiler.SynthFqn;
import com.legend.error.LegendCompileException;
import com.legend.error.ModelException;
import com.legend.error.NotImplementedException;
import com.legend.protocol.TypeExpression;
import com.legend.model.AssociationMapping;
import com.legend.model.ClassDefinition;
import com.legend.model.ClassMapping;
import com.legend.model.DatabaseDefinition;
import com.legend.model.JoinChainElement;
import com.legend.model.LegacyMappingDefinition;
import com.legend.model.PropertyMapping;
import com.legend.model.RelationalOperation;
import com.legend.protocol.spec.AppliedFunction;
import com.legend.protocol.spec.AppliedProperty;
import com.legend.protocol.spec.ColSpec;
import com.legend.protocol.spec.LambdaFunction;
import com.legend.protocol.spec.ValueSpecification;
import com.legend.protocol.spec.Variable;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
/**
 * Union/inheritance operation-mapping synthesis: member-set concatenation, route classification, nav lifts and inbound route keys. Split from MappingNormalizer (the Doors split).
 */
final class UnionSynthesis {

    private UnionSynthesis() {}


    /** One routed navigation entry: the target's union-member ORDINAL
     * (declaration order = concatenate thread order = the engine's
     * {@code _N} suffix) and the entry's own join. Ordinal {@code -1}
     * marks a root/sole-set route (the un-routed navigation). */
    record UnionRoute(int targetOrdinal, PropertyMapping.Join join) {
    }

    /** The ordinal of a single set-pinned route to a NON-root set outside
     * any union: a route list of one; {@code -1} marks a root route (the
     * un-routed navigation). */
    static final int PINNED_SINGLE = -2;

    /** The function realizing {@code member}, named by the mapping that
     * DEFINES the set and by the driver's own rule: by class when the set
     * is its class's root or sole set in that mapping, by class and set id
     * otherwise (the driver binds every non-root set of a multi-set class
     * that way). A plain function reference: an include is a call. The
     * own record is searched first (a hoisted copy shadows the include's). */
    static String memberFunction(ResolvedMapping md, ClassMapping member) {
        String id = ResolvedMapping.idOf(member);
        for (LegacyMappingDefinition m : md.closure()) {
            for (ClassMapping cm : m.classMappings()) {
                if (cm.className().equals(member.className()) && ResolvedMapping.idOf(cm).equals(id)) {
                    return memberFunction(new PinTarget(m, member));
                }
            }
        }
        return memberFunction(new PinTarget(md.closure().get(0), member));
    }

    /** The set's own function under the mapping that DEFINES it. */
    static String memberFunction(PinTarget pt) {
        ClassMapping member = pt.set();
        long setsOfClass = pt.defining().classMappings().stream()
                .filter(cm -> cm.className().equals(member.className())).count();
        return member.root() || setsOfClass <= 1
                ? com.legend.compiler.SynthFqn.mappingClass(pt.defining().qualifiedName(), member.className())
                : com.legend.compiler.SynthFqn.mappingClassSet(pt.defining().qualifiedName(),
                        member.className(), ResolvedMapping.idOf(member));
    }

    /** A property pin ({@code prop[setId]}) resolved: the set and the
     * mapping that defines it. */
    record PinTarget(LegacyMappingDefinition defining, ClassMapping set) {
    }

    /** A pin is a NAME (engine: {@code targetSetImplementationId} is a
     * string the QUERIED mapping resolves at query time; the compiler
     * accepts a pin its own closure cannot see — the stress corpus'
     * {@code book[positions_Book]} in a project mapping that does not
     * include the positions mapping). The defining mapping's closure
     * resolves it first; otherwise the whole model does, by effective
     * set id — the same set any including mapping resolves, as long as
     * the id is unique model-wide. Null when no set carries the id;
     * loud when several do (the pin is ambiguous, not resolvable). The
     * route names the set's function under ITS defining mapping; the
     * queried mapping binds that function at query time like every
     * other route. Declared divergence: a query under a mapping that
     * cannot see the set still navigates here, where the engine fails. */
    static @com.legend.base.Nullable PinTarget resolvePin(ResolvedMapping md,
            @com.legend.base.Nullable String setId, ModelBuilder model) {
        if (setId == null) {
            return null;
        }
        ClassMapping own = md.set(setId);
        if (own != null) {
            // the defining mapping: matched by class + effective id (the
            // closure hands out SUBSTITUTED copies, never the same object)
            for (LegacyMappingDefinition m : md.closure()) {
                for (ClassMapping cm : m.classMappings()) {
                    if (cm.className().equals(own.className())
                            && setId.equals(ResolvedMapping.idOf(cm))) {
                        return new PinTarget(m, own);
                    }
                }
            }
            return new PinTarget(md.closure().get(0), own);
        }
        List<PinTarget> hits = pinOwners(setId, model);
        return hits.size() == 1 ? hits.get(0) : null;
    }

    /** Every mapping in the model defining a set with effective id
     * {@code setId} — one is a resolvable pin; several, an AMBIGUOUS one
     * (the poison reason names them; the property drops from this
     * synthesis exactly as an unknown pin does). */
    static List<PinTarget> pinOwners(String setId, ModelBuilder model) {
        List<PinTarget> hits = new ArrayList<>();
        model.legacyMappings().forEach(m -> {
            for (ClassMapping cm : m.classMappings()) {
                if (setId.equals(ResolvedMapping.idOf(cm))) {
                    hits.add(new PinTarget(m, cm));
                }
            }
        });
        return hits;
    }

    /** THE STACK (legacy routes as composition, design §9/§11): an
     * operation's function is its members' functions concatenated —
     * {@code m1() -> concatenate(m2()) -> …} in member order, nothing else.
     * The query side builds the union from the arms (ClassSources' stack
     * builder); the normalizer publishes nothing about them. */
    static ValueSpecification stackBody(ResolvedMapping md, ClassMapping operation,
            List<? extends ClassMapping> members, MappingLedger ledger) {
        ValueSpecification out = null;
        List<String> ids = new ArrayList<>(members.size());
        for (ClassMapping m : members) {
            ValueSpecification call = new AppliedFunction(memberFunction(md, m), List.of());
            out = out == null ? call : new AppliedFunction("concatenate", List.of(out, call));
            ids.add(ResolvedMapping.idOf(m));
        }
        // the arms as a FACT beside the body (the binding's memberSetIds)
        ledger.operationMembers.put(ResolvedMapping.idOf(operation), List.copyOf(ids));
        return java.util.Objects.requireNonNull(out, "an operation with no members");
    }

    /** Extends-merge identity: (property name, route) — per-set duplicates
     * of a routed property are distinct mappings. */
    static String pmIdentity(PropertyMapping pm) {
        return pm.propertyName() + ' '
                + (pm instanceof PropertyMapping.Join j
                        && j.targetSetId() != null ? j.targetSetId() : "");
    }


    /** Routed (set-pinned) Join PMs, DESCENDING into embedded bodies with
     * the owner class threaded (ledger cluster 66 — the flat scan left
     * unionRoutes blind to bridge(employees[set1], employees[set2])
     * declared inside an embedded block). ONE owner per property name so
     * the route's target class resolves against the EMBEDDED class: the
     * routes of a class mapping are keyed by property name, so the same
     * name routed under two owners has no place in this synthesis — it is
     * loud ({@link #recordOwner}), never first-owner-wins. */
    static void collectRoutedJoins(List<PropertyMapping> pms,
            String ownerCls, ResolvedMapping md, ModelBuilder model,
            Map<String, List<PropertyMapping.Join>> routedByProp,
            Map<String, String> ownerByProp) {
        collectRoutedJoins(pms, ownerCls, md, model, routedByProp, ownerByProp,
                new LinkedHashSet<>());
    }

    /** {@code splicing}: the Inline embedded set ids being descended right
     * now — the one way this walk can recurse (set a splices set b splices
     * set a); re-entry is a model error, never a StackOverflowError
     * (audit 2026-09-15 P0-3). */
    private static void collectRoutedJoins(List<PropertyMapping> pms,
            String ownerCls, ResolvedMapping md, ModelBuilder model,
            Map<String, List<PropertyMapping.Join>> routedByProp,
            Map<String, String> ownerByProp, Set<String> splicing) {
        for (PropertyMapping pm : pms) {
            switch (pm) {
                case PropertyMapping.Join j when j.targetSetId() != null -> {
                    routedByProp.computeIfAbsent(j.propertyName(),
                            k -> new ArrayList<>()).add(j);
                    recordOwner(ownerByProp, j.propertyName(), ownerCls, md);
                }
                case PropertyMapping.Embedded emb -> {
                    String inner = embeddedOwner(ownerCls,
                            emb.propertyName(), model);
                    if (inner != null) {
                        collectRoutedJoins(emb.propertyMappings(), inner,
                                md, model, routedByProp, ownerByProp, splicing);
                    }
                }
                case PropertyMapping.OtherwiseEmbedded oe -> {
                    // the fallback join is a pinned route of the property
                    if (oe.fallback() instanceof PropertyMapping.Join fj) {
                        String pin = fj.targetSetId() != null ? fj.targetSetId()
                                : oe.fallbackSetId();
                        routedByProp.computeIfAbsent(oe.propertyName(),
                                k -> new ArrayList<>()).add(new PropertyMapping.Join(
                                        oe.propertyName(), fj.database(), fj.joins(), pin));
                        recordOwner(ownerByProp, oe.propertyName(), ownerCls, md);
                    }
                    String inner = embeddedOwner(ownerCls,
                            oe.propertyName(), model);
                    if (inner != null) {
                        collectRoutedJoins(oe.embedded(), inner, md, model,
                                routedByProp, ownerByProp, splicing);
                    }
                }
                case PropertyMapping.InlineEmbedded ie -> {
                    if (!splicing.add(ie.setId())) {
                        throw new ModelException(LegendCompileException.Phase.NORMALIZE,
                                "Cycle materializing Inline embedded set '" + ie.setId()
                              + "' via '" + ie.propertyName() + "' on '" + ownerCls
                              + "': the splice chain " + splicing + " returns to it; mapping="
                              + md.qualifiedName());
                    }
                    try {
                        for (ClassMapping cm : md.classMappings()) {
                            if (cm instanceof ClassMapping.Relational r2
                                    && java.util.Objects.equals(
                                            ResolvedMapping.idOf(r2),
                                            ie.setId())) {
                                collectRoutedJoins(r2.propertyMappings(),
                                        r2.className(), md, model,
                                        routedByProp, ownerByProp, splicing);
                                break;
                            }
                        }
                    } finally {
                        splicing.remove(ie.setId());
                    }
                }
                case PropertyMapping.LocalProperty lp ->
                        collectRoutedJoins(List.of(lp.body()), ownerCls,
                                md, model, routedByProp, ownerByProp, splicing);
                default -> {
                }
            }
        }
    }

    /** The owner class a routed property name resolves against — a fact
     * with ONE value per name in a class mapping's collection. A second,
     * different owner (the same property name routed at the top and inside
     * an embedded block of another class) is a shape this synthesis does
     * not key: a model error, walled with its reason in a module build and
     * thrown in a strict one — never the first owner seen. */
    private static void recordOwner(Map<String, String> ownerByProp, String prop,
            String ownerCls, ResolvedMapping md) {
        String prior = ownerByProp.get(prop);
        if (prior == null) {
            ownerByProp.put(prop, ownerCls);
        } else if (!prior.equals(ownerCls)) {
            throw new ModelException(LegendCompileException.Phase.NORMALIZE,
                    "property '" + prop + "' is routed under two owners, '" + prior
                    + "' and '" + ownerCls + "', in one class mapping; routes are keyed"
                    + " by property name, so the mapping cannot be normalized; mapping="
                    + md.qualifiedName());
        }
    }

    private static @com.legend.base.Nullable String embeddedOwner(String ownerCls,
            String prop, ModelBuilder model) {
        ClassDefinition oc = MissProbe.knownMiss(model.knowledge().hierarchyClass(ownerCls));
        TypeExpression pt = oc == null ? null
                : model.knowledge().propertyType(oc, prop);
        return pt instanceof TypeExpression.NameRef nr
                && model.knowledge().hierarchyClass(nr.name()).isPresent() ? nr.name() : null;
    }

    /**
     * Classify every {@code prop[setId]}-routed class-typed Join PM of
     * {@code rcm} from its OWN {@code targetSetId} (per-PM fidelity —
     * audit 11: the name-keyed map's put() lost duplicates and made the
     * outcome depend on textual PM order). Outcomes per property:
     * <ul>
     *   <li>every route hits a member of the target class's union &rarr;
     *       {@code p.unionRoutes} (ONE navigate, OR over the entries,
     *       each member-suffixed — engine parity; coverage of ALL members
     *       is NOT assumed, un-routed members read NULL keys);</li>
     *   <li>every route hits the target's root/sole set &rarr; the
     *       un-routed navigation (duplicates dedup at emission);</li>
     *   <li>anything else (unknown set, non-root non-member set, chained
     *       join on a routed entry, mixed root+member) &rarr; the property
     *       DROPS from this synthesis with the reason on the poison
     *       ledger; demanding it fails loudly.</li>
     * </ul>
     */
    static void classifyUnionRoutes(ResolvedMapping md,
            ClassMapping.Relational rcm, ModelBuilder model, Pipeline p) {
        Map<String, List<PropertyMapping.Join>> routedByProp = new LinkedHashMap<>();
        Map<String, String> ownerByProp = new LinkedHashMap<>();
        collectRoutedJoins(rcm.propertyMappings(), rcm.className(), md,
                model, routedByProp, ownerByProp);
        for (var e : routedByProp.entrySet()) {
            String prop = e.getKey();
            ClassDefinition owner = model.knowledge().hierarchyClass(ownerByProp
                    .getOrDefault(prop, rcm.className())).orElseGet(MissProbe::miss);
            TypeExpression pt = owner == null ? null
                    : model.knowledge().propertyType(owner, prop);
            String targetClass = pt instanceof TypeExpression.NameRef nr
                    && model.knowledge().hierarchyClass(nr.name()).isPresent() ? nr.name() : null;
            ClassMapping.Union tu = targetClass == null ? null
                    : md.unionOf(targetClass);
            // FIX-A (audit-17 bucket analysis): an INHERITANCE op is a
            // union at the routing level — member set ids in the shared
            // enumeration's order
            List<String> memberIds = tu != null ? tu.memberSetIds() : null;
            if (memberIds == null && targetClass != null) {
                ClassMapping.Inheritance tih =
                        md.inheritanceOf(targetClass);
                if (tih != null) {
                    memberIds = inheritanceMembers(md, tih, model).stream()
                            .map(ResolvedMapping::idOf).toList();
                }
            }
            List<UnionRoute> routes = new ArrayList<>();
            String poison = null;
            // R-target (docs/LEG2_STACK_AUDIT_2026_09_14.md): several distinct
            // pins resolve to the target's root — a pin outside is dead; ONE
            // distinct pin is that set, member of the root or not
            long distinctPins = e.getValue().stream()
                    .map(PropertyMapping.Join::targetSetId).distinct().count();
            for (PropertyMapping.Join j : e.getValue()) {
                PinTarget pin = resolvePin(md, j.targetSetId(), model);
                if (pin == null) {
                    List<PinTarget> owners = j.targetSetId() == null ? List.of()
                            : pinOwners(j.targetSetId(), model);
                    poison = owners.size() > 1
                            ? "an AMBIGUOUS mapping set '" + j.targetSetId()
                                    + "' (not visible through the includes; defined by "
                                    + owners.stream().map(o -> o.defining().qualifiedName()).toList() + ")"
                            : "unknown mapping set '" + j.targetSetId() + "'";
                    break;
                }
                ClassMapping set = pin.set();
                int ord = memberIds == null ? -1
                        : md.memberOrdinal(memberIds, j.targetSetId());
                // engine rootClassMappingByClass — ONE owner, the resolved
                // mapping (audit 2026-09-15 P2-2: the inline count here
                // judged sole-ness over the QUERYING mapping's own sets
                // while the set itself was resolved through the closure).
                // A set OUTSIDE this closure is never this mapping's root:
                // its route names the set's function (pinned-single).
                boolean rootOrSole = set instanceof ClassMapping.Relational
                        && md.set(j.targetSetId()) != null && md.isRootOrSole(set);
                if (ord >= 0) {
                    routes.add(new UnionRoute(ord, j));
                } else if (memberIds != null && distinctPins > 1) {
                    // the TARGET class is union-mapped, this route's set is
                    // not among the members and the property pins several
                    // sets: the router resolves to the root, so the entry
                    // is DEAD — never a root route and never a poison
                    // (multipleChainedJoins V4: included y2/y3 sets beside a
                    // (y0, y1) union). A property's ONLY pin to such a set
                    // is that set (the pinned-single route below).
                    continue;
                } else if (rootOrSole) {
                    routes.add(new UnionRoute(-1, j));
                } else {
                    // a set-pinned route to a NON-root set: a route naming
                    // that set's function (legacy routes as composition) —
                    // one (employees2[p2] over multi-set Person) or several
                    // (the queried mapping resolves the pins: engine R-target)
                    routes.add(new UnionRoute(PINNED_SINGLE, j));
                }
            }
            // CHAINED member routes come in two engine shapes, both
            // accepted here: SHARED-PREFIX (unionOfViews golden — the
            // identical prefix hops emit ONCE as physical joins, the final
            // hop dispatches per member) and PER-ARM (V4 pair routes /
            // unionOfViews2 — routes diverge, each route's mid hops
            // materialize INSIDE the owning member's thread and the ONE
            // navigate reads each route's FIRST hop). The split is decided
            // by uniformChainedRoutes at the emitter AND the inbound key
            // collector — the same predicate, never allowed to drift.
            if (poison != null) {
                p.droppedRoutedProps.add(prop);
                p.ledger().poison(new com.legend.model.PoisonKey.ForClass(rcm.className()),
                        "property '" + prop + "' routes to " + poison
                                + "; the property is dropped from this synthesis");
                continue;
            }
            if (routes.stream().allMatch(r -> r.targetOrdinal() == -1)) {
                continue;   // root routes = the un-routed navigation
            }
            p.unionRoutes.put(prop, routes);
        }
    }

    /**
     * An Operation UNION class mapping: the extent is UNION ALL of the
     * member sets. Each member synthesizes its own pipeline+fields; the
     * SHARED SCALAR properties (declared type not a model class) project to
     * property-named columns, the projections concatenate, and one map
     * terminal reads the aligned row. Properties outside the shared set are
     * absent from the binding table — demanding one is loud downstream.
     */
    static ValueSpecification synthUnion(ResolvedMapping md,
                                                ClassMapping.Union u,
                                                ModelBuilder model,
                                                MappingLedger ledger) {
        if (u.memberSetIds().isEmpty()) {
            throw new NotImplementedException(
                    "Operation union with no member sets; class="
                  + u.className() + ", mapping=" + md.qualifiedName());
        }
        // member sets resolve by setId ACROSS INCLUDES; a member may map a
        // SUBCLASS of the operation class (special_union over an
        // inheritance hierarchy) — the shared-property projection over the
        // operation class is the semantics either way
        Map<String, ClassMapping> bySetId = new LinkedHashMap<>();
        bySetId.putAll(md.includedSets());
        for (ClassMapping cm : md.classMappings()) {
            bySetId.put(ResolvedMapping.idOf(cm), cm);
        }
        List<ClassMapping> memberSets = new ArrayList<>();
        for (String setId : u.memberSetIds()) {
            ClassMapping member = bySetId.get(setId);
            if (member instanceof ClassMapping.Pure) {
                // MIXED-KIND union (route b, docs/XSTORE_LEG.md): a Pure
                // member's relation exists only at resolve time — record
                // the member list for ClassSources' resolver-side arm
                // synthesis and withhold the eager class function (the
                // throw lands on the poison ledger; the resolver route
                // recognizes the registry before the ledger surfaces).
                ledger.mixedUnions.put(u.className(), List.copyOf(u.memberSetIds()));
                throw new NotImplementedException(
                        "Operation union member set '" + setId + "' of class '"
                      + u.className() + "' is a Pure (M2M) set — the mixed-kind"
                      + " union extent synthesizes at the resolver; mapping="
                      + md.qualifiedName());
            }
            if (!(member instanceof ClassMapping.Relational)
                    && !(member instanceof ClassMapping.RelationFunction)) {
                throw new NotImplementedException(
                        "Operation union member set '" + setId + "' of class '"
                      + u.className() + "' is " + (member == null ? "missing"
                              : "not a Relational or Relation(~func) set")
                      + "; mapping=" + md.qualifiedName());
            }
            // a member must map the operation class or a SUBCLASS — a
            // stray setId landing on an unrelated class with coincidental
            // property names would union unrelated rows (audit 8 S8)
            if (!model.knowledge().isSubtype(member.className(), u.className())) {
                throw new ModelException(
                        LegendCompileException.Phase.NORMALIZE,
                        "Operation union member set '" + setId + "' maps '"
                      + member.className() + "', which is not '" + u.className()
                      + "' or a subclass; mapping=" + md.qualifiedName());
            }
            memberSets.add(member);
        }
        // per-pair AssociationMapping entries ([sourceSet, targetSet]) land
        // on their owning MEMBER set as routed class-typed Join PMs — the
        // engine dispatches union navigation per member pair
        Map<String, List<PropertyMapping.Join>> pairEntries = new LinkedHashMap<>();
        md.pairEntries(u.className()).forEach((k, v) -> pairEntries.computeIfAbsent(k, x -> new ArrayList<>()).addAll(v));
        if (!pairEntries.isEmpty()) {
            for (int i = 0; i < memberSets.size(); i++) {
                if (!(memberSets.get(i) instanceof ClassMapping.Relational mr)) {
                    continue;
                }
                // OWN entries + EXTENDS-routed entries merged (e[aSet1,
                // eSet1] serves bSet1 extends aSet1 ALONGSIDE bSet1's own
                // h entry) — engine golden testExtendsForPropertyMapping
                // WithUnion result2 joins ONLY the routed thread (the
                // other thread carries a NULL key). Own entries first;
                // the pmIdentity dedup below keeps them authoritative.
                List<PropertyMapping.Join> add = new ArrayList<>();
                String cur = ResolvedMapping.idOf(mr);
                Set<String> seenSets = new HashSet<>();
                while (cur != null && seenSets.add(cur)) {
                    List<PropertyMapping.Join> lvl = pairEntries.get(cur);
                    if (lvl != null) {
                        add.addAll(lvl);
                    }
                    ClassMapping up =
                            md.set(cur);
                    cur = up instanceof ClassMapping.Relational ur
                            ? ur.extendsSetId() : null;
                }
                if (add.isEmpty()) {
                    continue;
                }
                List<PropertyMapping> pms = new ArrayList<>(mr.propertyMappings());
                for (PropertyMapping.Join j : add) {
                    if (pms.stream().noneMatch(p ->
                            pmIdentity(p).equals(pmIdentity(j)))) {
                        pms.add(j);
                    }
                }
                memberSets.set(i, new ClassMapping.Relational(mr.className(),
                        mr.setId(), mr.extendsSetId(), mr.root(), mr.mainTable(),
                        mr.filter(), mr.distinct(), mr.groupBy(), mr.primaryKey(),
                        pms, mr.sourceUrl(), mr.propertyTargetSets(),
                        mr.aggregation()));
            }
        }
        recordKeyThreadsOf(md, u.className(), memberSets, model, ledger);
        return stackBody(md, u, memberSets, ledger);
    }

    /**
     * Inheritance Operation: the extent is the UNION of every Relational
     * set mapped for the class's SUBCLASSES (transitively; nested
     * union/inheritance operations expand to their concrete members).
     * Queries on the base class can only touch base-class-typed properties,
     * so the shared-property projection over the base owner is exactly the
     * engine's router semantics.
     */
    static ValueSpecification synthInheritance(ResolvedMapping md,
            ClassMapping.Inheritance ih, ModelBuilder model, MappingLedger ledger) {
        // ENGINE ALGORITHM (router_operations.pure getMappedLeafTypes) —
        // the ordered member enumeration is SHARED with route
        // classification (inheritanceMembers): ordinal alignment by
        // construction.
        List<ClassMapping.Relational> members =
                inheritanceMembers(md, ih, model);
        if (members.isEmpty()) {
            throw new NotImplementedException(
                    "inheritance Operation for '" + ih.className()
                    + "' finds no mapped subclass sets; mapping="
                    + md.qualifiedName());
        }
        if (members.size() == 1) {
            // the sole member's own synthesis is the class's function (no
            // stack); the fact names the one arm
            ledger.operationMembers.put(ResolvedMapping.idOf(ih),
                    List.of(ResolvedMapping.idOf(members.get(0))));
            return MappingNormalizer.synthRelational(md, members.get(0), model, ledger);
        }
        // a SINGLE-TABLE hierarchy (every member over one bare table) is
        // the same stack: the builder reads the arms' shared table and
        // scans it once (StackBuilder.collapsedTable) — no policy here
        // per-pair AssociationMapping entries land on their owning member
        // exactly like the Union-op arm (person[map1,per1]: @PersonCar on
        // the Car member — engine dispatches inheritance navigation per
        // member pair; testGetAllFilterWithAssociation)
        recordKeyThreadsOf(md, ih.className(), members, model, ledger);
        return stackBody(md, ih, members, ledger);
    }

    /** The primary-key threads fact of an operation's row ({@code
     * <col>_<ordinal>} per member, a store fact for {@code importDataFlow}),
     * recorded from the members alone. */
    private static void recordKeyThreadsOf(ResolvedMapping md, String className,
            List<? extends ClassMapping> members, ModelBuilder model, MappingLedger ledger) {
        // a member without a declared ~mainTable keys on its INFERRED one
        List<ClassMapping> ms = new ArrayList<>(members.size());
        for (ClassMapping m : members) {
            if (m instanceof ClassMapping.Relational mr && mr.mainTable() == null
                    && MappingNormalizer.inferMainTableQuiet(mr) != null) {
                ms.add(new ClassMapping.Relational(mr.className(), mr.setId(),
                        mr.extendsSetId(), mr.root(), MappingNormalizer.inferMainTableQuiet(mr),
                        mr.filter(), mr.distinct(), mr.groupBy(), mr.primaryKey(),
                        mr.propertyMappings(), mr.sourceUrl(), mr.propertyTargetSets(),
                        mr.aggregation()));
            } else {
                ms.add(m);
            }
        }
        recordKeyThreads(md, className, ms, ownSharedKeys(ms, model), model, ledger);
    }

    /**
     * The ORDERED member Relational sets of an inheritance op — ONE
     * enumeration shared by {@link #synthInheritance} (concatenate thread
     * order) and route classification (ordinal computation), so the
     * ordinals align BY CONSTRUCTION (misalignment = silently wrong rows).
     */
    static List<ClassMapping.Relational> inheritanceMembers(
            ResolvedMapping md, ClassMapping.Inheritance ih,
            ModelBuilder model) {
        LinkedHashSet<ClassMapping> chosen = new LinkedHashSet<>();
        collectInheritanceMembers(md, ih.className(), model, chosen);
        List<ClassMapping.Relational> members = new ArrayList<>();
        for (ClassMapping cm : chosen) {
            switch (cm) {
                case ClassMapping.Relational mr -> members.add(mr);
                case ClassMapping.Union u2 -> {
                    Map<String, ClassMapping> bySetId = new LinkedHashMap<>();
                    bySetId.putAll(md.includedSets());
                    for (ClassMapping own : md.classMappings()) {
                        bySetId.put(ResolvedMapping.idOf(own), own);
                    }
                    for (String setId : u2.memberSetIds()) {
                        if (bySetId.get(setId) instanceof ClassMapping.Relational mr2) {
                            members.add(mr2);
                        } else {
                            throw new NotImplementedException(
                                    "inheritance member union set '" + setId
                                    + "' is not a Relational set; mapping="
                                    + md.qualifiedName());
                        }
                    }
                }
                default -> throw new NotImplementedException(
                        "inheritance Operation member for '" + cm.className()
                        + "' is a " + cm.getClass().getSimpleName()
                        + " mapping — not supported yet; mapping="
                        + md.qualifiedName());
            }
        }
        return members;
    }


    /** The engine's leaf-most-root member selection for an inheritance op. */
    static void collectInheritanceMembers(ResolvedMapping md,
            String base, ModelBuilder model, Set<ClassMapping> chosen) {
        // ROOT class mapping per class, includes first (own definitions win)
        Map<String, ClassMapping> rootByClass = new LinkedHashMap<>();
        rootByClass.putAll(md.roots());
        // strict specializations of base, and their leaves — over the
        // WHOLE class universe (user classes AND the native catalog: a
        // mapped metaclass's subclasses are natives; engine
        // getMappedLeafTypes walks every class)
        // the strict subtree of base off the two DIRECT subclass indexes
        // (model + native catalog, each built once); then the universe
        // order (model classes, then natives — the member order the
        // rosters pin) restricted to it. Was: isSubclassOf per universe
        // class per call — 45% of the metamodel's 22ms normalization.
        java.util.Set<String> subtree = model.knowledge().subtree(base);
        List<String> subs = new ArrayList<>();
        model.classes().forEach(cd -> {
            if (subtree.contains(cd.qualifiedName())) {
                subs.add(cd.qualifiedName());
            }
        });
        for (ClassDefinition nc : com.legend.builtin.Pure.allNativeClasses()) {
            if (subtree.contains(nc.qualifiedName()) && !subs.contains(nc.qualifiedName())) {
                subs.add(nc.qualifiedName());
            }
        }
        // a leaf has no subclass at all (every subclass of a member of the
        // subtree is itself in the subtree)
        List<String> leaves = subs.stream()
                .filter(c -> model.knowledge().directSubtypes(c).isEmpty())
                .toList();
        for (String leaf : leaves) {
            // nearest mapped ancestor at or above the leaf, STRICTLY below base
            ArrayDeque<String> level = new ArrayDeque<>();
            Set<String> seen = new HashSet<>();
            level.add(leaf);
            outer:
            while (!level.isEmpty()) {
                int n = level.size();
                for (int i = 0; i < n; i++) {
                    String c = level.poll();
                    if (!seen.add(c) || c.equals(base)) {
                        continue;
                    }
                    // STRICTLY BELOW base means inside base's subtree: a
                    // multiple-inheritance leaf (EmbeddedSetImplementation
                    // extends InstanceSetImplementation, PropertyMapping)
                    // also climbs its OTHER parent chain, which leaves the
                    // subtree and, meeting another inheritance-mapped
                    // class, recursed back here forever (the generated
                    // prelude's mapping shapes exposed it, 2026-09-04)
                    if (!subtree.contains(c)) {
                        continue;
                    }
                    ClassMapping cm = rootByClass.get(c);
                    if (cm != null) {
                        if (cm instanceof ClassMapping.Inheritance) {
                            collectInheritanceMembers(md, c, model, chosen);
                        } else {
                            chosen.add(cm);
                        }
                        break outer;
                    }
                    ClassDefinition cd = model.knowledge().hierarchyClass(c).orElseThrow(() -> new IllegalStateException("F7.8: class unresolved at UnionSynthesis#3 (this default NEVER fired on the corpus census; a miss here is a real model gap): " + c));
                    if (cd != null) {
                        for (TypeExpression sup : cd.superClasses()) {
                            if (sup instanceof TypeExpression.NameRef nr) {
                                level.add(nr.name());
                            }
                        }
                    }
                }
            }
        }
    }



    static final String MEMBER_WITNESS = ClassMapping.memberWitness();


    /** The normalizer's own filter form (the member pipeline a ~filter
     * emits) — matched by name in the parse-level spec, as the rest of
     * this synthesis does. */
    private static final String FILTER_FORM = "filter";

    record Thread(ValueSpecification pipe, List<ColSpec> cols) {
    }

    /** A member's scan source split into its innermost TABLE and the
     * navigation-slot wrappers ({@code legacyNavigate(source, slot, …)})
     * around it, innermost first. */
    record ScanSource(ValueSpecification table, List<AppliedFunction> wrappers) {
        static ScanSource of(ValueSpecification source) {
            ArrayDeque<AppliedFunction> ws = new ArrayDeque<>();
            ValueSpecification cur = source;
            while (cur instanceof AppliedFunction af
                    && af.function().equals(Pure.Lite.LEGACY_NAVIGATE)
                    && af.parameters().size() >= 2) {
                ws.push(af);
                cur = af.parameters().get(0);
            }
            return new ScanSource(cur, new ArrayList<>(ws));
        }

        static String slotName(AppliedFunction w) {
            return w.parameters().get(1) instanceof ColSpec cs ? cs.name()
                    : w.parameters().get(1).toString();
        }

        /** The table wrapped in the deduped union of {@code sources}'
         * slots, in first-seen order. */
        static ValueSpecification merged(List<ValueSpecification> sources) {
            ValueSpecification out = null;
            Set<String> seen = new LinkedHashSet<>();
            for (ValueSpecification src : sources) {
                ScanSource ss = of(src);
                if (out == null) {
                    out = ss.table();
                }
                for (AppliedFunction w : ss.wrappers()) {
                    if (seen.add(slotName(w))) {
                        List<ValueSpecification> ps = new ArrayList<>(w.parameters());
                        ps.set(0, out);
                        out = w.withParameters(ps);
                    }
                }
            }
            return java.util.Objects.requireNonNull(out);
        }
    }

    /**
     * THE TABLE KEY OF A SINGLE-TABLE HIERARCHY IS SHARED: a member's key
     * column that is its main table's sole PRIMARY KEY spells
     * {@code <col>__pk} for EVERY member (never member-suffixed) — the
     * merged scan projects it ONCE, ungated (a primary key names at most
     * its own row, whatever the row's kind), so a routed navigation's
     * per-member disjuncts collapse to one plain equality the database
     * can index (H2 rescanned a CASE-gated, OR-joined UNION extent per
     * outer row — ten typeInference tests at 9–18s, 2026-09-02).
     */
    static final String TABLE_KEY_SUFFIX = "__pk";

    /** The sole PRIMARY KEY column of a member set's main table, else null. */
    static @com.legend.base.Nullable String tableKey(@com.legend.base.Nullable ClassMapping cm,
            ModelBuilder model) {
        if (!(cm instanceof ClassMapping.Relational r) || r.mainTable() == null) {
            return null;
        }
        DatabaseDefinition.TableDefinition td = model.knowledge().table(r.mainTable().database(), r.mainTable().table()).orElseGet(MissProbe::miss);
        if (td == null) {
            return null;
        }
        String key = null;
        for (DatabaseDefinition.ColumnDefinition c : td.columns()) {
            if (c.primaryKey()) {
                if (key != null) {
                    return null;    // composite key: member-suffixed as before
                }
                key = c.name();
            }
        }
        return key;
    }

    /**
     * THE chained-route classification (shared by the emitter and the
     * inbound key collector): TRUE when every member route walks the
     * IDENTICAL prefix (all hops but the last) — the shared-prefix model
     * where the prefix emits once as physical joins. FALSE (per-arm /
     * push-into-arm) when member routes diverge in length or mid joins:
     * each route's mids then live INSIDE the owning member's thread and
     * the navigate reads each route's FIRST hop.
     */
    static boolean uniformChainedRoutes(List<PropertyMapping.Join> memberJs) {
        if (memberJs.isEmpty()
                || memberJs.stream().noneMatch(j -> j.joins().size() > 1)) {
            return true;
        }
        PropertyMapping.Join first = memberJs.get(0);
        for (PropertyMapping.Join j : memberJs) {
            if (j.joins().size() != first.joins().size()) {
                return false;
            }
            for (int h = 0; h + 1 < j.joins().size(); h++) {
                JoinChainElement a = first.joins().get(h);
                JoinChainElement b = j.joins().get(h);
                String dbA = a.databaseName() != null
                        ? a.databaseName() : first.database();
                String dbB = b.databaseName() != null
                        ? b.databaseName() : j.database();
                if (!a.joinName().equals(b.joinName())
                        || !java.util.Objects.equals(dbA, dbB)) {
                    return false;
                }
            }
        }
        return true;
    }

    /** The union-member routes of a routed property (root/sole routes carry ordinal -1). */
    static List<PropertyMapping.Join> memberJoins(List<UnionRoute> routes) {
        return routes.stream().filter(r -> r.targetOrdinal() >= 0)
                .map(UnionRoute::join).toList();
    }

    /**
     * PER-ARM inbound mid steps (push-into-arm, engine
     * unionOfViews2/JoinSequenceInProperty behavior): the route's chain
     * walked from the TARGET member's table BACK through the mids (all
     * hops but the FIRST, reversed), each joined inside the member
     * thread. The route key the navigate reads is the FIRST hop's
     * landing-table columns, exposed under the PROPERTY-SCOPED suffixed
     * name ({@code col__prop_ord} — deterministic on BOTH the emission
     * and registration sides, and collision-free across a member's
     * chains) off the last emitted mid alias — the exact mirror of
     * {@link #liftMidSteps} for outbound lifts.
     */
    static List<LiftMidStep> inboundArmSteps(PropertyMapping.Join j,
            String prop, String memberTable, ResolvedMapping md,
            ModelBuilder model) {
        List<LiftMidStep> steps = new ArrayList<>();
        String prevTable = memberTable;
        String prevAlias = null;
        for (int h = j.joins().size() - 1; h >= 1; h--) {
            JoinChainElement midHop = j.joins().get(h);
            String midDb = midHop.databaseName() != null
                    ? midHop.databaseName() : j.database();
            DatabaseDefinition.JoinDefinition mjd =
                    model.findJoin(midDb, midHop.joinName()).orElseThrow(() ->
                            new ModelException(
                                    LegendCompileException
                                            .Phase.NORMALIZE,
                                    "Join '" + midHop.joinName() + "' not"
                                    + " found in db '" + midDb + "'; PM='"
                                    + prop + "', mapping="
                                    + md.qualifiedName()));
            String midTgt = MappingNormalizer.determineTargetTable(mjd.operation(),
                    prevTable, midHop.joinName(), prop, h,
                    md.qualifiedName());
            Variable ms = new Variable("s");
            Variable mt = new Variable("t");
            Map<String, ValueSpecification> midScope = new LinkedHashMap<>();
            midScope.put(prevTable, prevAlias == null ? ms
                    : new AppliedProperty(ms, prevAlias));
            if (!midTgt.equals(prevTable)) {
                midScope.put(midTgt, mt);
            }
            ValueSpecification midCond = RelOpTranslator.translate(
                    mjd.operation(), midScope, mt, null,
                    RelOpTranslator.PipelineView.NONE);
            String midAlias = "nl__" + prop + "__inb__" + midHop.joinName();
            steps.add(new LiftMidStep(midAlias, midDb, midTgt,
                    new LambdaFunction(List.of(ms, mt),
                            List.of(midCond))));
            prevTable = midTgt;
            prevAlias = midAlias;
        }
        return steps;
    }

    /** One physical MID hop of a CHAINED lift entry, wrapped around the
     * owning member's thread pipeline ({@code join(pipe, ~alias:
     * tableReference, cond)} — engine: mid tables join INSIDE the member
     * thread, 3-sets golden). */
    record LiftMidStep(String alias, String db, String table,
            LambdaFunction cond) {
    }


    /**
     * The union's OWN shared table keys (B3.1: decided from the members
     * alone, no inbound scan): members over ONE main table whose sole
     * PRIMARY KEY the threads project once, ungated, as
     * {@code <col>__pk_<table>} (TABLE_KEY_SUFFIX) — the row identity a
     * cast re-root joins on (CastReRoot) and the plain indexable key of a
     * single-table hierarchy. (db, canonical table, key) &rarr; the first
     * member over it.
     */
    static Map<List<String>, Integer> ownSharedKeys(List<ClassMapping> members,
            ModelBuilder model) {
        Map<List<String>, List<Integer>> byTable = new LinkedHashMap<>();
        for (int o = 0; o < members.size(); o++) {
            if (members.get(o) instanceof ClassMapping.Relational mr
                    && mr.mainTable() != null) {
                String key = tableKey(mr, model);
                if (key != null) {
                    byTable.computeIfAbsent(List.of(mr.mainTable().database(),
                            MappingNormalizer.canonicalTable(mr.mainTable().table()), key),
                            k -> new ArrayList<>()).add(o);
                }
            }
        }
        Map<List<String>, Integer> out = new LinkedHashMap<>();
        for (var en : byTable.entrySet()) {
            if (en.getValue().size() > 1) {
                out.put(en.getKey(), en.getValue().get(0));
            }
        }
        return out;
    }

    /** The PRE-PASSED records of the closure: a set that extends another
     * navigates with the routes it INHERITED, and names its keys by its
     * own id (the navigator side reads the flattened record too). */
    static List<LegacyMappingDefinition> prePassedClosure(ResolvedMapping md,
            Map<String, ResolvedMapping> resolved) {
        List<LegacyMappingDefinition> records = new ArrayList<>();
        for (LegacyMappingDefinition m : md.closure()) {
            ResolvedMapping r = resolved.get(m.qualifiedName());
            records.add(r != null ? r.raw() : m);
        }
        return records;
    }

    /**
     * IMPORT DATA FLOW (engine {@code pureToSQLQuery_union.pure:140–150},
     * {@code resolvePrimaryKey} helperFunctions.pure:439–454): every member thread
     * projects its set's PRIMARY KEY — the declared {@code ~primaryKey}
     * columns of the main table, else the table's PRIMARY KEY — as
     * {@code <col>_<ordinal>} (NULL in the other members' threads), the
     * union's row identity across members (a SHARED table key is projected
     * once as {@code <col>__pk_<table>} and is not doubled here). The
     * {@code (name, kind)} facts are recorded on the ledger; the stack
     * builder projects the threads from them and the execute option
     * surfaces them as result columns ({@code ModelContext.unionKeyThreads}).
     */
    private static void recordKeyThreads(ResolvedMapping md, String className,
            List<ClassMapping> members, Map<List<String>, Integer> sharedKeys,
            ModelBuilder model, MappingLedger ledger) {
        List<com.legend.model.KeyThread> threads = new ArrayList<>();
        for (int o = 0; o < members.size(); o++) {
            if (!(members.get(o) instanceof ClassMapping.Relational mr)
                    || mr.mainTable() == null) {
                continue;   // a Relation(~func) member has no key table
            }
            String db = mr.mainTable().database();
            String table = mr.mainTable().table();
            for (String col : memberPrimaryKey(mr, model)) {
                if (sharedKeys.containsKey(List.of(db,
                        MappingNormalizer.canonicalTable(table), col))) {
                    continue;
                }
                threads.add(new com.legend.model.KeyThread(col + "_" + o,
                        model.knowledge().columnKind(db, table, col), col, o));
            }
        }
        // the SHARED table keys ride the same fact (ordinal -1): the stack
        // builder projects them once per member over the keyed table
        for (var sk : sharedKeys.entrySet()) {
            String db = sk.getKey().get(0);
            String table = sk.getKey().get(1);
            String col = sk.getKey().get(2);
            ClassMapping.Relational first = (ClassMapping.Relational) members.get(sk.getValue());
            String physicalTable = java.util.Objects.requireNonNull(first.mainTable()).table();
            threads.add(new com.legend.model.KeyThread(sharedKeyName(table, col),
                    model.knowledge().columnKind(db, physicalTable, col), col, -1, db, table));
        }
        ledger.unionKeyThreads.put(className, List.copyOf(threads));
    }

    /** A member set's primary key on its MAIN table: the declared
     * {@code ~primaryKey} plain column refs of that table, else the table's
     * PRIMARY KEY columns in declaration order (engine resolvePrimaryKey). */
    private static List<String> memberPrimaryKey(ClassMapping.Relational mr, ModelBuilder model) {
        var main = java.util.Objects.requireNonNull(mr.mainTable());
        List<String> declared = new ArrayList<>();
        for (RelationalOperation op : mr.primaryKey()) {
            if (op instanceof RelationalOperation.ColumnRef cr
                    && MappingNormalizer.canonicalTable(cr.table())
                            .equals(MappingNormalizer.canonicalTable(main.table()))) {
                declared.add(cr.column());
            }
        }
        if (!declared.isEmpty()) {
            return declared;
        }
        DatabaseDefinition.TableDefinition td = model.knowledge().table(main.database(), main.table()).orElseGet(MissProbe::miss);
        if (td == null) {
            return List.of();   // a view-backed member: no physical key
        }
        List<String> pk = new ArrayList<>();
        for (DatabaseDefinition.ColumnDefinition c : td.columns()) {
            if (c.primaryKey()) {
                pk.add(c.name());
            }
        }
        return pk;
    }

    /** The projected name of a shared table key: {@code <col>__pk_<table>}
     * (the table disambiguates two hierarchies whose keys share a name). */
    static String sharedKeyName(String table, String col) {
        return col + TABLE_KEY_SUFFIX + "_" + table.replaceAll("[^A-Za-z0-9_]", "_");
    }

}
