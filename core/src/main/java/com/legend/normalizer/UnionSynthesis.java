// SPDX-License-Identifier: Apache-2.0

package com.legend.normalizer;

import com.legend.builtin.Pure;
import com.legend.compiler.ModelBuilder;
import com.legend.compiler.SynthFqn;
import com.legend.error.LegendCompileException;
import com.legend.error.ModelException;
import com.legend.error.NotImplementedException;
import com.legend.protocol.Multiplicity;
import com.legend.model.NormalizedModel;
import com.legend.model.ParsedModel;
import com.legend.protocol.TypeExpression;
import com.legend.model.AssociationDefinition;
import com.legend.model.AssociationMapping;
import com.legend.model.AssociationPropertyMapping;
import com.legend.model.ClassDefinition;
import com.legend.model.ClassMapping;
import com.legend.model.ComparisonOp;
import com.legend.model.DatabaseDefinition;
import com.legend.model.EnumerationMapping;
import com.legend.model.FilterMapping;
import com.legend.model.FilterPointer;
import com.legend.model.FunctionDefinition;
import com.legend.model.JoinChainElement;
import com.legend.model.LegacyMappingDefinition;
import com.legend.model.LogicalOp;
import com.legend.model.MappingDefinition;
import com.legend.model.MappingInclude;
import com.legend.model.PackageableElement;
import com.legend.model.PropertyMapping;
import com.legend.protocol.Realization;
import com.legend.model.RelationalDataType;
import com.legend.model.RelationalOperation;
import com.legend.model.SynthHat;
import com.legend.protocol.spec.AppliedFunction;
import com.legend.protocol.spec.AppliedProperty;
import com.legend.protocol.spec.CBoolean;
import com.legend.protocol.spec.CDate;
import com.legend.protocol.spec.CDecimal;
import com.legend.protocol.spec.CFloat;
import com.legend.protocol.spec.CInteger;
import com.legend.protocol.spec.CString;
import com.legend.protocol.spec.ColSpec;
import com.legend.protocol.spec.ColSpecArray;
import com.legend.protocol.spec.EnumValue;
import com.legend.protocol.spec.KeyExpression;
import com.legend.protocol.spec.LambdaFunction;
import com.legend.protocol.spec.NewInstance;
import com.legend.protocol.spec.NewInstanceCast;
import com.legend.protocol.spec.PackageableElementPtr;
import com.legend.protocol.spec.PureCollection;
import com.legend.protocol.spec.TypeAnnotation;
import com.legend.protocol.spec.ValueSpecification;
import com.legend.protocol.spec.Variable;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;
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
     * that way). A plain function reference: an include is a call. */
    static String memberFunction(ResolvedMapping md, ClassMapping member) {
        String defining = md.qualifiedName();
        String id = ResolvedMapping.idOf(member);
        long setsOfClass = 0;
        for (LegacyMappingDefinition m : md.closure()) {
            boolean here = false;
            long n = 0;
            for (ClassMapping cm : m.classMappings()) {
                if (cm.className().equals(member.className())) {
                    n++;
                    if (ResolvedMapping.idOf(cm).equals(id)) {
                        here = true;
                    }
                }
            }
            if (here) {
                defining = m.qualifiedName();
                setsOfClass = n;
                break;
            }
        }
        return member.root() || setsOfClass <= 1
                ? com.legend.compiler.SynthFqn.mappingClass(defining, member.className())
                : com.legend.compiler.SynthFqn.mappingClassSet(defining, member.className(), id);
    }

    /** Extends-merge identity: (property name, route) — per-set duplicates
     * of a routed property are distinct mappings. */
    static String pmIdentity(PropertyMapping pm) {
        return pm.propertyName() + ' '
                + (pm instanceof PropertyMapping.Join j
                        && j.targetSetId() != null ? j.targetSetId() : "");
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
    /** Routed (set-pinned) Join PMs, DESCENDING into embedded bodies with
     * the owner class threaded (ledger cluster 66 — the flat scan left
     * unionRoutes blind to bridge(employees[set1], employees[set2])
     * declared inside an embedded block). Owner recorded per property so
     * the route's target class resolves against the EMBEDDED class. */
    static void collectRoutedJoins(List<PropertyMapping> pms,
            String ownerCls, ResolvedMapping md, ModelBuilder model,
            Map<String, List<PropertyMapping.Join>> routedByProp,
            Map<String, String> ownerByProp) {
        for (PropertyMapping pm : pms) {
            switch (pm) {
                case PropertyMapping.Join j when j.targetSetId() != null -> {
                    routedByProp.computeIfAbsent(j.propertyName(),
                            k -> new ArrayList<>()).add(j);
                    ownerByProp.putIfAbsent(j.propertyName(), ownerCls);
                }
                case PropertyMapping.Embedded emb -> {
                    String inner = embeddedOwner(ownerCls,
                            emb.propertyName(), model);
                    if (inner != null) {
                        collectRoutedJoins(emb.propertyMappings(), inner,
                                md, model, routedByProp, ownerByProp);
                    }
                }
                case PropertyMapping.OtherwiseEmbedded oe -> {
                    String inner = embeddedOwner(ownerCls,
                            oe.propertyName(), model);
                    if (inner != null) {
                        collectRoutedJoins(oe.embedded(), inner, md, model,
                                routedByProp, ownerByProp);
                    }
                }
                case PropertyMapping.InlineEmbedded ie -> {
                    for (ClassMapping cm : md.classMappings()) {
                        if (cm instanceof ClassMapping.Relational r2
                                && java.util.Objects.equals(
                                        ResolvedMapping.idOf(r2),
                                        ie.setId())) {
                            collectRoutedJoins(r2.propertyMappings(),
                                    r2.className(), md, model,
                                    routedByProp, ownerByProp);
                            break;
                        }
                    }
                }
                case PropertyMapping.LocalProperty lp ->
                        collectRoutedJoins(List.of(lp.body()), ownerCls,
                                md, model, routedByProp, ownerByProp);
                default -> {
                }
            }
        }
    }

    private static @com.legend.Nullable String embeddedOwner(String ownerCls,
            String prop, ModelBuilder model) {
        ClassDefinition oc = MissProbe.knownMiss(model.knowledge().hierarchyClass(ownerCls));
        TypeExpression pt = oc == null ? null
                : model.knowledge().propertyType(oc, prop);
        return pt instanceof TypeExpression.NameRef nr
                && model.knowledge().hierarchyClass(nr.name()).isPresent() ? nr.name() : null;
    }

    static void classifyUnionRoutes(ResolvedMapping md,
            ClassMapping.Relational rcm, ModelBuilder model, Pipeline p) {
        Map<String, List<PropertyMapping.Join>> routedByProp = new LinkedHashMap<>();
        Map<String, String> ownerByProp = new LinkedHashMap<>();
        collectRoutedJoins(rcm.propertyMappings(), rcm.className(), md,
                model, routedByProp, ownerByProp);
        for (var e : routedByProp.entrySet()) {
            String prop = e.getKey();
            ClassDefinition owner = model.knowledge().hierarchyClass(ownerByProp
                    .getOrDefault(prop, rcm.className())).orElse(null);
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
            for (PropertyMapping.Join j : e.getValue()) {
                ClassMapping set = md.set(j.targetSetId());
                if (set == null) {
                    poison = "unknown mapping set '" + j.targetSetId() + "'";
                    break;
                }
                int ord = memberIds == null ? -1
                        : md.memberOrdinal(memberIds, j.targetSetId());
                // engine rootClassMappingByClass: the * set, or the class's
                // SOLE set (sole-ness judged in the OWNING mapping's scope)
                boolean rootOrSole = set instanceof ClassMapping.Relational tr
                        && (tr.root() || md.classMappings().stream()
                                .filter(x -> x.className().equals(tr.className()))
                                .count() == 1);
                if (ord >= 0) {
                    routes.add(new UnionRoute(ord, j));
                } else if (memberIds != null) {
                    // the TARGET class is union-mapped and this route's set
                    // is not among the members: the set is unreachable from
                    // the union extent — engine consults only member
                    // routes, so the entry is DEAD, never a root route and
                    // never a poison (multipleChainedJoins V4: included
                    // y2/y3 sets beside a (y0, y1) union; root/sole-ness
                    // judged in the requesting mapping's scope would
                    // misread them as roots)
                    continue;
                } else if (rootOrSole) {
                    routes.add(new UnionRoute(-1, j));
                } else if (e.getValue().size() == 1) {
                    // a SINGLE set-pinned route to a NON-root set: a route
                    // list of ONE naming that set's function (legacy routes
                    // as composition, 2b) — employees2[p2] over multi-set
                    // Person; no stamped set-pin hint consulted
                    routes.add(new UnionRoute(PINNED_SINGLE, j));
                } else {
                    poison = "NON-root mapping set '" + j.targetSetId()
                            + "' — MULTI-route dispatch outside union members"
                            + " is a roadmap feature";
                    break;
                }
            }
            if (poison == null && routes.stream()
                    .anyMatch(r -> r.targetOrdinal() >= 0)
                    && routes.stream().anyMatch(r -> r.targetOrdinal() < 0)) {
                poison = "MIXED root-set and union-member routes";
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
                p.ledger().poisons.merge(rcm.className(),
                        "property '" + prop + "' routes to " + poison
                                + "; the property is dropped from this synthesis",
                        (a, b) -> a + "; " + b);
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
        return synthMemberUnion(md, u.className(), memberSets, model, ledger);
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
            return MappingNormalizer.synthRelational(md, members.get(0), model, ledger);
        }
        ValueSpecification sameTable =
                synthSameTableInheritance(md, ih, members, model, ledger);
        if (sameTable != null) {
            return sameTable;
        }
        // per-pair AssociationMapping entries land on their owning member
        // exactly like the Union-op arm (person[map1,per1]: @PersonCar on
        // the Car member — engine dispatches inheritance navigation per
        // member pair; testGetAllFilterWithAssociation)
        members = withPairEntries(md, ih.className(), members, model);
        return synthMemberUnion(md, ih.className(), members, model, ledger);
    }

    /** Inheritance members enriched with their per-pair AssociationMapping
     * entries (own set id + extends lineage), deduped by pmIdentity —
     * the same distribution the Union-op arm performs inline. */
    private static List<ClassMapping.Relational> withPairEntries(
            ResolvedMapping md, String className,
            List<ClassMapping.Relational> members, ModelBuilder model) {
        Map<String, List<PropertyMapping.Join>> pairEntries =
                new LinkedHashMap<>();
        md.pairEntries(className).forEach((k, v) -> pairEntries.computeIfAbsent(k, x -> new ArrayList<>()).addAll(v));
        if (pairEntries.isEmpty()) {
            return members;
        }
        List<ClassMapping.Relational> out = new ArrayList<>(members.size());
        for (ClassMapping.Relational mr : members) {
            List<PropertyMapping.Join> add = new ArrayList<>();
            String cur = ResolvedMapping.idOf(mr);
            Set<String> seenSets = new HashSet<>();
            while (cur != null && seenSets.add(cur)) {
                List<PropertyMapping.Join> lvl = pairEntries.get(cur);
                if (lvl != null) {
                    add.addAll(lvl);
                }
                ClassMapping up = md.set(cur);
                cur = up instanceof ClassMapping.Relational ur
                        ? ur.extendsSetId() : null;
            }
            if (add.isEmpty()) {
                out.add(mr);
                continue;
            }
            List<PropertyMapping> pms = new ArrayList<>(mr.propertyMappings());
            for (PropertyMapping.Join j : add) {
                if (pms.stream().noneMatch(p ->
                        pmIdentity(p).equals(pmIdentity(j)))) {
                    pms.add(j);
                }
            }
            out.add(new ClassMapping.Relational(mr.className(), mr.setId(),
                    mr.extendsSetId(), mr.root(), mr.mainTable(), mr.filter(),
                    mr.distinct(), mr.groupBy(), mr.primaryKey(), pms,
                    mr.sourceUrl(), mr.propertyTargetSets(),
                    mr.aggregation()));
        }
        return out;
    }

    /** SINGLE-TABLE hierarchy (engine cast semantics): members that ALL
     * share one root table and carry no ~filter read their casts
     * SAME-ROW off the shared table — the extent is the TABLE, one
     * source, no member union (a union would thread every physical row
     * once per member; the corpus inheritanceWithEmbedded goldens pin
     * per-row cast reads and ONE shared navigation join). Base-class
     * props mapped IDENTICALLY by every member that maps them hoist
     * into the parent source (engine merge-by-join-name folds equal
     * per-member emissions into one); a base prop mapped DIFFERENTLY
     * per member stays unmapped on the parent — bare reads go loud,
     * casts read it through the same-source stc transplants. Anything
     * outside this shape keeps the member union. */
    private static @com.legend.Nullable ValueSpecification synthSameTableInheritance(
            ResolvedMapping md, ClassMapping.Inheritance ih,
            List<ClassMapping.Relational> members, ModelBuilder model,
            MappingLedger ledger) {
        ClassDefinition base = model.knowledge().hierarchyClass(ih.className()).orElseThrow(() -> new IllegalStateException("F7.8: class unresolved at UnionSynthesis#2 (this default NEVER fired on the corpus census; a miss here is a real model gap): " + ih.className()));
        if (base == null) {
            return null;
        }
        LegacyMappingDefinition.TableReference shared =
                sharedInheritanceTable(members);
        if (shared == null) {
            return null;
        }
        // BASE-prop hoisting: a base-declared prop mapped by members is
        // hoisted onto the parent source iff every member that maps it
        // emits the IDENTICAL PropertyMapping (record equality); a
        // differing map stays off the parent (loud on bare reads).
        Map<String, LinkedHashSet<PropertyMapping>> baseProps =
                new LinkedHashMap<>();
        for (ClassMapping.Relational mr : members) {
            for (PropertyMapping pm : mr.propertyMappings()) {
                if (model.knowledge().propertyType(base, pm.propertyName()) != null) {
                    baseProps.computeIfAbsent(pm.propertyName(),
                            k -> new LinkedHashSet<>()).add(pm);
                }
            }
        }
        List<PropertyMapping> hoisted = new ArrayList<>();
        for (LinkedHashSet<PropertyMapping> variants : baseProps.values()) {
            if (variants.size() == 1) {
                hoisted.add(variants.iterator().next());
            }
        }
        return MappingNormalizer.synthRelational(md,
                new ClassMapping.Relational(ih.className(), ih.setId(),
                        null, ih.root(), shared, null, false,
                        List.of(), List.of(), hoisted, null,
                        java.util.Map.of(), null),
                model, ledger);
    }

    /** The single shared root table of an inheritance member list — the
     * SAME-TABLE gate: null when any member has its own table, a ~filter,
     * distinct, groupBy, or a sourceUrl (those shapes keep the union). */
    private static LegacyMappingDefinition.@com.legend.Nullable TableReference
            sharedInheritanceTable(List<ClassMapping.Relational> members) {
        LegacyMappingDefinition.TableReference shared = null;
        for (ClassMapping.Relational mr : members) {
            LegacyMappingDefinition.TableReference t = mr.mainTable() != null
                    ? mr.mainTable()
                    : MappingNormalizer.inferMainTableQuiet(mr);
            if (t == null || mr.filter() != null || mr.distinct()
                    || !mr.groupBy().isEmpty() || mr.sourceUrl() != null) {
                return null;
            }
            if (shared == null) {
                shared = t;
            } else if (!t.database().equals(shared.database())
                    || !t.table().equals(shared.table())) {
                return null;
            }
        }
        return shared;
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



    /**
     * SUBTYPE COLUMNS (engine router subType dispatch): each member whose
     * class is a proper SUBCLASS of the union root carries every scalar
     * property it maps under a class-qualified synthetic column
     * ({@link ClassMapping#subTypeColumn}) — its own thread reads the
     * mapped value, every other thread a typed NULL — so
     * {@code ->subType(@Sub).prop} reads NULL off non-member rows by
     * construction. Forced casts of SHARED properties included: the
     * subtype column is thread-local, never the aligned column.
     */
    private static Map<String, LinkedHashSet<String>> subTypeDispatchProps(
            String className, List<ClassMapping> members,
            List<MappingNormalizer.RelationalParts> parts, ModelBuilder model) {
        Map<String, LinkedHashSet<String>> subTypeProps = new LinkedHashMap<>();
        for (int j = 0; j < members.size(); j++) {
            String memberClass = members.get(j).className();
            if (memberClass.equals(className)) {
                continue;
            }
            ClassDefinition mcd = model.knowledge().hierarchyClass(memberClass).orElseThrow(() -> new IllegalStateException("F7.8: class unresolved at UnionSynthesis#5 (this default NEVER fired on the corpus census; a miss here is a real model gap): " + memberClass));
            // cast TARGETS: the member class and every ancestor strictly
            // below the union root — a cast to an INTERMEDIATE class
            // (subType(@RoadVehicle) over a Car|Bicycle union) is owned by
            // every conforming member thread
            for (String target : model.knowledge().ancestorsBelow(memberClass, className)) {
                ClassDefinition tcd = model.knowledge().hierarchyClass(target).orElseThrow(() -> new IllegalStateException("F7.8: class unresolved at UnionSynthesis#6 (this default NEVER fired on the corpus census; a miss here is a real model gap): " + target));
                // every cast target is a DISPATCH target even when the
                // member maps no scalar property of its own (a property-
                // less subtype — the datatype metamodel's Integer / Bit
                // rows, a marker subclass): its thread still needs the
                // membership witness below for instanceOf / match / cast
                subTypeProps.computeIfAbsent(target, k -> new LinkedHashSet<>());
                for (String prop : parts.get(j).fields().keySet()) {
                    TypeExpression t = mcd == null ? null
                            : model.knowledge().propertyType(mcd, prop);
                    boolean scalar = t instanceof TypeExpression.NameRef nr
                            && model.knowledge().hierarchyClass(nr.name()).isEmpty();
                    boolean visibleOnTarget = tcd != null
                            && model.knowledge().propertyType(tcd, prop) != null;
                    if (scalar && visibleOnTarget) {
                        subTypeProps.computeIfAbsent(target,
                                k -> new LinkedHashSet<>()).add(prop);
                    } else if (visibleOnTarget) {
                        // EMBEDDED subtype prop (Car maps mechanic(...)):
                        // distribute each thread-projectable ctor leaf as
                        // a FLAT stc column <prop>__<leaf> — the cast
                        // navigation reads it as an ordinary union column
                        KeyExpression fv = parts.get(j).fields().get(prop);
                        NewInstance ector = fv == null ? null
                                : ctorOf(fv.value());
                        if (ector != null) {
                            for (var pe : ector.properties()) {
                                if (ctorOf(pe.expression().value()) == null
                                        && isThreadProjectable(
                                                pe.expression().value(),
                                                parts.get(j).rowBind().name())) {
                                    subTypeProps.computeIfAbsent(target,
                                            k -> new LinkedHashSet<>())
                                            .add(prop + "__" + pe.key());
                                }
                            }
                        }
                    }
                }
            }
        }
        // MEMBERSHIP WITNESS: a cast target some member does NOT conform
        // to needs row RESTRICTION at to-many navigation positions — emit
        // a witness column (TRUE in conforming threads, NULL elsewhere).
        // Total-membership targets get NO witness: the cast is row-neutral.
        for (var en : subTypeProps.entrySet()) {
            for (ClassMapping m : members) {
                if (!m.className().equals(className)
                        && !model.knowledge().isSubtype(m.className(), en.getKey())) {
                    en.getValue().add(MEMBER_WITNESS);
                    break;
                }
            }
        }
        return subTypeProps;
    }

    static final String MEMBER_WITNESS = ClassMapping.memberWitness();


    /** One thread's subtype-dispatch columns — same order in every thread. */
    private static void addSubTypeDispatchCols(
            Map<String, LinkedHashSet<String>> subTypeProps,
            ClassMapping member, MappingNormalizer.RelationalParts pp,
            ModelBuilder model, List<ColSpec> cols) {
        for (var stEn : subTypeProps.entrySet()) {
            ClassDefinition subDef = model.knowledge().hierarchyClass(stEn.getKey()).orElseThrow(() -> new IllegalStateException("F7.8: class unresolved at UnionSynthesis#8 (this default NEVER fired on the corpus census; a miss here is a real model gap): " + stEn.getKey()));
            boolean own = model.knowledge().isSubtype(member.className(), stEn.getKey());
            for (String prop : stEn.getValue()) {
                if (prop.equals(MEMBER_WITNESS)) {
                    // toOne types both threads identically (literal vs NULL
                    // cast); lowering is erasure — the witness stays NULL
                    ValueSpecification w = own ? new CBoolean(true)
                            : new AppliedFunction(com.legend.builtin.Pure.Lite.TRUST_ONE, List.of(
                                    new AppliedFunction("cast", List.of(
                                            new PureCollection(List.of()),
                                            new TypeAnnotation.Named(
                                                    new TypeExpression.NameRef(
                                                            "Boolean"))))));
                    cols.add(new ColSpec(
                            ClassMapping.subTypeColumn(stEn.getKey(), prop),
                            new LambdaFunction(List.of(pp.rowBind()),
                                    List.of(w)), null));
                    continue;
                }
                int embCut = prop.indexOf("__");
                if (embCut > 0 && subDef != null
                        && model.knowledge().propertyType(subDef, prop) == null) {
                    addStcEmbeddedLeaf(stEn.getKey(), prop, embCut,
                            java.util.Objects.requireNonNull(subDef),
                            own, pp, model, cols);
                    continue;
                }
                KeyExpression mapped = own ? pp.fields().get(prop) : null;
                ValueSpecification value = mapped == null
                        ? MappingNormalizer.nullOfDeclaredType(subDef, prop, model)
                        : DeclaredCoercions.coerceToDeclaredNumeric(
                                mapped.value(), prop, stEn.getKey(), model);
                TypeExpression dt = subDef == null ? null
                        : model.knowledge().propertyType(subDef, prop);
                if (dt instanceof TypeExpression.NameRef dn
                        && "String".equals(MappingNormalizer.simpleTypeName(dn.name()))) {
                    value = new AppliedFunction("cast", List.of(value,
                            new TypeAnnotation.Named(
                                    new TypeExpression.NameRef("String"))));
                }
                value = new AppliedFunction(com.legend.builtin.Pure.Lite.TRUST_ONE, List.of(value));
                cols.add(new ColSpec(ClassMapping.subTypeColumn(stEn.getKey(), prop),
                        new LambdaFunction(List.of(pp.rowBind()),
                                List.of(value)), null));
            }
        }
    }

    /** One EMBEDDED subtype leaf as a flat stc thread column
     * ({@code stc_<Sub>___<prop>__<leaf>}): the owning member reads its
     * ctor leaf, other threads project the leaf's typed NULL. */
    private static void addStcEmbeddedLeaf(String target, String flatProp,
            int cut, ClassDefinition subDef, boolean own,
            MappingNormalizer.RelationalParts pp, ModelBuilder model,
            List<ColSpec> cols) {
        String top = flatProp.substring(0, cut);
        String sub = flatProp.substring(cut + 2);
        TypeExpression it = model.knowledge().propertyType(subDef, top);
        ClassDefinition inner = it instanceof TypeExpression.NameRef inr
                ? model.knowledge().hierarchyClass(inr.name()).orElseThrow(() -> new IllegalStateException("F7.8: class unresolved at UnionSynthesis#9 (this default NEVER fired on the corpus census; a miss here is a real model gap): " + inr.name())) : null;
        KeyExpression fv = own ? pp.fields().get(top) : null;
        NewInstance ector = fv == null ? null : ctorOf(fv.value());
        ValueSpecification value = ector != null
                && ector.first(sub) != null
                ? java.util.Objects.requireNonNull(ector.first(sub)).value()
                : MappingNormalizer.nullOfDeclaredType(inner, sub, model);
        value = DeclaredCoercions.coerceToDeclaredNumeric(value, sub,
                inner == null ? target : inner.qualifiedName(), model);
        TypeExpression sdt = inner == null ? null
                : model.knowledge().propertyType(inner, sub);
        if (sdt instanceof TypeExpression.NameRef sdn
                && "String".equals(MappingNormalizer.simpleTypeName(sdn.name()))) {
            value = new AppliedFunction("cast", List.of(value,
                    new TypeAnnotation.Named(
                            new TypeExpression.NameRef("String"))));
        }
        value = new AppliedFunction(com.legend.builtin.Pure.Lite.TRUST_ONE, List.of(value));
        cols.add(new ColSpec(ClassMapping.subTypeColumn(target, flatProp),
                new LambdaFunction(List.of(pp.rowBind()),
                        List.of(value)), null));
    }

    /** The shared-property UNION ALL over resolved member sets. */
    static ValueSpecification synthMemberUnion(ResolvedMapping md,
            String className, List<? extends ClassMapping> memberSets,
            ModelBuilder model, MappingLedger ledger) {
        List<MappingNormalizer.RelationalParts> parts = new ArrayList<>(memberSets.size());
        List<ClassMapping> members = new ArrayList<>(memberSets.size());
        for (ClassMapping cmIn : memberSets) {
            if (cmIn instanceof ClassMapping.RelationFunction rfm) {
                // Relation (~func) member: the parts are the inlined
                // relation body + its column reads — no main table, no
                // nav lifting (scalar columns only)
                Variable rfRow = new Variable("rf_row");
                Map<String, KeyExpression> rfFields = new LinkedHashMap<>();
                // the SAME extraction the single-set Relation path uses —
                // plain/enum/EMBEDDED/inline-embedded bindings all covered
                // (the embedded ^Inner values then distribute per sub-field
                // through the union's standard embedded machinery)
                MappingNormalizer.putRelationCols(rfFields, rfm.columns(),
                        rfRow, rfm.className(), md, model);
                members.add(rfm);
                parts.add(new MappingNormalizer.RelationalParts(
                        MappingNormalizer.relationFunctionPipeline(rfm, model), rfRow, rfFields));
                continue;
            }
            ClassMapping.Relational mr = (ClassMapping.Relational) cmIn;
            String setId = ResolvedMapping.idOf(mr);
            if (mr.sourceUrl() != null) {
                throw new NotImplementedException(
                        "Operation union over a JSON-source member set is not"
                      + " supported yet; mapping=" + md.qualifiedName());
            }
            if (mr.mainTable() == null) {
                LegacyMappingDefinition.TableReference inferred = MappingNormalizer.inferMainTable(mr);
                if (inferred == null) {
                    throw new NotImplementedException(
                            "union member set '" + setId + "' has no inferable"
                          + " main table; mapping=" + md.qualifiedName());
                }
                mr = new ClassMapping.Relational(mr.className(), mr.setId(),
                        mr.extendsSetId(), mr.root(), inferred, mr.filter(),
                        mr.distinct(), mr.groupBy(), mr.primaryKey(),
                        mr.propertyMappings(), mr.sourceUrl(),
                        mr.propertyTargetSets(), mr.aggregation());
            }
            var mrMain = java.util.Objects.requireNonNull(mr.mainTable(),
                    "union member set without ~mainTable");
            DatabaseDefinition.ViewDefinition memberView = model.findView(
                    mrMain.database(), mrMain.table()).orElse(null);
            if (memberView != null) {
                // VIEW-backed member set: the view expands as the member
                // thread's SOURCE SUBSELECT (engine unionOfViews golden —
                // each thread is `from (select ... from PersonExtensionT<i>)
                // as "root"`), the view name is the row scope, and PMs read
                // the view's DECLARED columns verbatim (the same subselect
                // treatment grouped view-backed class mappings get).
                ValueSpecification viewSource = ViewRelation.viewRelationExpr(
                        memberView, mr.mainTable().table(),
                        mr.mainTable().database(), model, md);
                members.add(mr);
                parts.add(MappingNormalizer.synthTableBackedParts(md, mr, model,
                        ledger, null, viewSource));
                continue;
            }
            members.add(mr);
            parts.add(MappingNormalizer.synthTableBackedParts(md, mr, model, ledger, null));
        }
        // the UNION of the members' scalar property sets, first-appearance
        // order — a member that does not map a property contributes a typed
        // NULL in its thread (engine: 'null as ...' / __SQLNULL__ columns;
        // partial-union reads come back TDSNull, testUnionPartial goldens)
        ClassDefinition owner = MissProbe.knownMiss(model.knowledge().hierarchyClass(className));
        List<String> common = new ArrayList<>();
        for (MappingNormalizer.RelationalParts pp : parts) {
            for (String prop : pp.fields().keySet()) {
                TypeExpression t = owner == null ? null
                        : model.knowledge().propertyType(owner, prop);
                boolean scalar = t instanceof TypeExpression.NameRef nr
                        && model.knowledge().hierarchyClass(nr.name()).isEmpty();
                if (scalar && !common.contains(prop)) {
                    common.add(prop);
                }
            }
        }
        // EMBEDDED PMs distribute per SUB-FIELD (engine union model): each
        // member thread projects its own embedded sub-columns under the
        // synthetic emb__<prop>__<sub> names (typed NULL in members that
        // don't map the sub — the partial-union mechanism); the union root
        // ctor recomposes ^Inner(...) over those columns, so the resolver's
        // existing EMBEDDED arm dispatches. Only THREAD-PROJECTABLE sub
        // values distribute (plain member-row reads / constants) — a sub
        // reading a hoisted join slot stays undistributed (loud downstream,
        // never a silently-wrong projection).
        EmbDist emb = collectEmbeddedDistribution(parts, owner, model);
        Map<String, LinkedHashSet<String>> embSubs = emb.subs();
        // embedded-only member sets carry the union through their
        // sub-fields (inheritanceWithEmbedded Car/Bicycle map only
        // mechanic(...)); only NEITHER is unbuildable
        // ...or a root with NO properties of its own whose members carry
        // subtype-dispatch columns (a metaclass hierarchy root such as
        // RelationalOperationElement over Table/TableAlias): the row is
        // the dispatch columns + witnesses, read through ->cast(@Member)
        if (common.isEmpty() && embSubs.isEmpty()
                && subTypeDispatchProps(className, members, parts, model).isEmpty()) {
            throw new NotImplementedException(
                    "Operation union members of '" + className
                  + "' map no scalar properties; mapping=" + md.qualifiedName());
        }
        Map<String, String> embInner = emb.inner();
        LinkedHashSet<String> embTops = emb.tops();
        // ==== NAV LIFT (engine union model): the members' class-typed
        // single-hop Join PMs lift to ONE legacyNavigate ON THE UNION —
        // member i's thread carries its join keys member-suffixed
        // (<col>_<i>, NULL in the other threads) and the navigate condition
        // ORs the per-entry conditions (target side suffixed too when the
        // entry routes to a union member of the TARGET class). Downstream,
        // the union class then looks like any nav-slot class.
        List<NavLift> lifts = collectNavLifts(md, className, members, model, ledger);
        // ordinal -> (projected name -> physical column): the key columns
        // each member thread projects (its own reads; typed NULL elsewhere)
        Map<Integer, Map<String, String>> srcKeysByOrdinal = new LinkedHashMap<>();
        Map<Integer, List<LiftChain>> chainsByOrdinal = new LinkedHashMap<>();
        for (NavLift lf : lifts) {
            for (var en : lf.srcKeysByOrdinal().entrySet()) {
                srcKeysByOrdinal.computeIfAbsent(en.getKey(),
                        k -> new LinkedHashMap<>()).putAll(en.getValue());
            }
            for (var en : lf.chainsByOrdinal().entrySet()) {
                chainsByOrdinal.computeIfAbsent(en.getKey(),
                        k -> new ArrayList<>()).addAll(en.getValue());
            }
        }
        // (db, table, key column) -> a member: the SHARED table keys the
        // threads project once (see TABLE_KEY_SUFFIX) — the union's OWN
        // decision from its members alone (B3.1)
        Map<List<String>, Integer> sharedKeys = ownSharedKeys(members, model);
        // THE MEMBERS' LINK KEYS (B3.1b): every route INTO a member of
        // this union names, through its Join, the column of the MEMBER's
        // table that links it — the member publishes that column under
        // the key's name (linkKeyName), NULL in the other members' threads;
        // the navigating class reads the name and nothing else. CHAINED
        // per-arm routes still push their mid hops into the owning
        // member's thread (B3.2 moves them to the navigating class).
        for (int o = 0; o < members.size(); o++) {
            Map<String, String> published = ledger.linkKeys.get(ResolvedMapping.idOf(members.get(o)));
            if (published != null) {
                srcKeysByOrdinal.computeIfAbsent(o, k -> new LinkedHashMap<>()).putAll(published);
            }
        }
        // the inbound CHAINS (per-arm chained routes into a member: its
        // thread carries the mids and publishes the first mid's column as
        // the route's link key) — over the pre-passed records, so a set
        // that extends another registers the routes it inherited
        collectInboundRouteKeys(md, prePassedClosure(md, ledger.resolved), model,
                members.stream().map(ResolvedMapping::idOf).toList(),
                members, new LinkedHashMap<>(), chainsByOrdinal, ledger);
        recordKeyThreads(md, className, members, srcKeysByOrdinal, sharedKeys, model, ledger);
        Map<String, LinkedHashSet<String>> subTypeProps =
                subTypeDispatchProps(className, members, parts, model);
        ValueSpecification union = null;
        // SINGLE-SCAN groups: FILTERED members over ONE physical table (the
        // engine's single-table-hierarchy idiom — one ~filter per subclass
        // set) synthesize as ONE thread over the unfiltered table, every
        // column gated by its member's filter predicate, the thread itself
        // restricted to rows some member claims. Row-identical to the
        // per-member threads (the filters partition the table), and a plain
        // indexed join for the database instead of a k-way union derived
        // table re-evaluated per outer row (H2's planner hung on the 21-kind
        // datatype hierarchy of the metamodel store — group F burn,
        // 2026-09-02).
        // a member's scan source is its TABLE wrapped in its own navigation
        // SLOTS (demand-driven: an unused slot lowers to nothing), so the
        // group key is the innermost table and the merged scan carries the
        // union of the members' slots — same-named slots must agree
        Map<String, List<Integer>> scanGroups = new LinkedHashMap<>();
        Map<String, Map<String, List<ValueSpecification>>> slotsByTable = new LinkedHashMap<>();
        for (int o = 0; o < parts.size(); o++) {
            FilteredScan fs = filteredScan(parts.get(o));
            boolean eligible = fs != null && !chainsByOrdinal.containsKey(o)
                    && members.get(o) instanceof ClassMapping.Relational mrx
                    && !mrx.distinct() && mrx.groupBy().isEmpty();
            String key = "#" + o;
            if (eligible && fs != null) {
                ScanSource ss = ScanSource.of(fs.source());
                String tableKey = ss.table().toString();
                Map<String, List<ValueSpecification>> seen =
                        slotsByTable.computeIfAbsent(tableKey, k -> new LinkedHashMap<>());
                boolean agrees = true;
                for (AppliedFunction w : ss.wrappers()) {
                    List<ValueSpecification> args = w.parameters().subList(1, w.parameters().size());
                    List<ValueSpecification> prev = seen.putIfAbsent(ScanSource.slotName(w), args);
                    if (prev != null && !prev.equals(args)) {
                        agrees = false;
                    }
                }
                if (agrees) {
                    key = tableKey;
                }
            }
            scanGroups.computeIfAbsent(key, k -> new ArrayList<>()).add(o);
        }
        for (List<Integer> group : scanGroups.values()) {
            ValueSpecification projected;
            if (group.size() == 1) {
                int ordinal = group.get(0);
                Thread t = threadOf(ordinal, parts.get(ordinal), members, common, owner,
                        className, embSubs, embInner, subTypeProps, srcKeysByOrdinal,
                        chainsByOrdinal, sharedKeys, md, model);
                projected = new AppliedFunction("project",
                        List.of(t.pipe(), new ColSpecArray(t.cols())));
            } else {
                // the marker: "this projection IS a union body" (the
                // resolver's union facts read it where a concatenate no
                // longer exists — Pure.Lite.UNION_SCAN, lowering identity)
                projected = new AppliedFunction(Pure.Lite.UNION_SCAN, List.of(
                        mergedScan(group, parts, members, common, owner, className,
                                embSubs, embInner, subTypeProps, srcKeysByOrdinal,
                                chainsByOrdinal, sharedKeys, md, model)));
            }
            union = union == null ? projected
                    : new AppliedFunction("concatenate", List.of(union, projected));
        }
        // the lifted navigations sit ABOVE the concatenate — one slot per
        // property, exactly the standard nav-slot pipeline shape
        for (NavLift lf : lifts) {
            ColSpec slot = new ColSpec(lf.property(), new LambdaFunction(List.of(),
                    List.of(new AppliedFunction("getAll", List.of(
                            new PackageableElementPtr(lf.targetClassFqn()))))),
                    null);
            union = new AppliedFunction(Pure.Lite.LEGACY_NAVIGATE,
                    lf.pairedCondition() == null
                            ? List.of(union, slot, lf.targetRows(), lf.condition())
                            : List.of(union, slot, lf.targetRows(), lf.condition(),
                                    lf.pairedCondition()));
        }
        return recomposeUnionRoot(className, union, common, lifts, embTops,
                emb, model);
    }

    /** The normalizer's own filter form (the member pipeline a ~filter
     * emits) — matched by name in the parse-level spec, as the rest of
     * this synthesis does. */
    private static final String FILTER_FORM = "filter";

    record Thread(ValueSpecification pipe, List<ColSpec> cols) {
    }

    /** A member whose pipeline is exactly {@code filter(<source>, row|pred)}. */
    record FilteredScan(ValueSpecification source, Variable row, ValueSpecification pred) {
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

    private static @com.legend.Nullable FilteredScan filteredScan(
            MappingNormalizer.RelationalParts pp) {
        if (pp.pipeline() instanceof AppliedFunction f && FILTER_FORM.equals(f.function())
                && f.parameters().size() == 2
                && f.parameters().get(1) instanceof LambdaFunction l
                && l.parameters().size() == 1 && l.body().size() == 1) {
            return new FilteredScan(f.parameters().get(0), l.parameters().get(0),
                    l.body().get(0));
        }
        return null;
    }

    /** One member thread's pipeline + column specs (the per-member emission
     * of synthMemberUnion, extracted so a single-scan group can merge
     * several members' columns over one source). */
    private static Thread threadOf(int ordinal, MappingNormalizer.RelationalParts pp,
            List<ClassMapping> members, List<String> common,
            @com.legend.Nullable ClassDefinition owner, String className,
            Map<String, LinkedHashSet<String>> embSubs, Map<String, String> embInner,
            Map<String, LinkedHashSet<String>> subTypeProps,
            Map<Integer, Map<String, String>> srcKeysByOrdinal,
            Map<Integer, List<LiftChain>> chainsByOrdinal,
            Map<List<String>, Integer> sharedKeys,
            ResolvedMapping md, ModelBuilder model) {
            List<ColSpec> cols = new ArrayList<>(common.size());
            for (String prop : common) {
                // member sets may disagree on the COLUMN kind (String col in
                // set1, Integer expression in set2) and MULTIPLICITY (a
                // join-terminal read is [0..1], a plain column [1]) — the
                // declared property is the union's schema contract: numeric/
                // date kinds coerce, and a declared-[1] property wraps in
                // toOne (typing [1] on both sides; lowering is erasure)
                KeyExpression mapped = pp.fields().get(prop);
                ValueSpecification value = mapped == null
                        ? MappingNormalizer.nullOfDeclaredType(owner, prop, model)
                        : DeclaredCoercions.coerceToDeclaredNumeric(
                                mapped.value(), prop, className, model);
                // String is safe INSIDE the union projection: the members
                // must agree on the declared kind, and the engine's union
                // coerces at the SQL boundary
                TypeExpression dt = owner == null ? null
                        : model.knowledge().propertyType(owner, prop);
                if (dt instanceof TypeExpression.NameRef dn
                        && ("String".equals(MappingNormalizer.simpleTypeName(dn.name())))) {
                    value = new AppliedFunction("cast", List.of(value,
                            new TypeAnnotation.Named(
                                    new TypeExpression.NameRef("String"))));
                }
                // every member column aligns to [1] (toOne types both sides
                // identically; lowering is erasure — the union's SQL columns
                // are nullable regardless, engine parity)
                value = new AppliedFunction(com.legend.builtin.Pure.Lite.TRUST_ONE, List.of(value));
                cols.add(new ColSpec(prop, new LambdaFunction(
                        List.of(pp.rowBind()), List.of(value)), null));
            }
            addEmbeddedThreadCols(embSubs, embInner, pp, model, cols);
            addSubTypeDispatchCols(subTypeProps, members.get(ordinal), pp,
                    model, cols);
            // lifted-navigation source keys: ONE column per key NAME across
            // the ordinals (a shared table key <col>__pk is projected once);
            // this thread reads its OWN key columns, other ordinals' keys
            // are typed NULL (nullable — no toOne wrap)
            Map<String, String> ownKeys = srcKeysByOrdinal.getOrDefault(ordinal, Map.of());
            Map<String, int[]> keyNames = new LinkedHashMap<>();   // name -> first ordinal
            Map<String, String> keyPhysical = new LinkedHashMap<>(); // name -> a physical column
            for (var en : srcKeysByOrdinal.entrySet()) {
                for (var key : en.getValue().entrySet()) {
                    keyNames.putIfAbsent(key.getKey(), new int[]{en.getKey()});
                    keyPhysical.putIfAbsent(key.getKey(), key.getValue());
                }
            }
            // a key this thread's OWN chain carries (B3.2: a per-arm chained
            // route's key is its first mid's column) is read off the mid
            // slot, whatever another member publishes under the name; the
            // column ORDER is the union-wide key order in every thread (the
            // concatenation aligns by position)
            Map<String, ValueSpecification> ownChainReads = new LinkedHashMap<>();
            Map<String, ValueSpecification> chainNulls = new LinkedHashMap<>();
            for (var en : chainsByOrdinal.entrySet()) {
                for (LiftChain ch : en.getValue()) {
                    for (var key : ch.keys().entrySet()) {
                        if (en.getKey() == ordinal) {
                            ownChainReads.putIfAbsent(key.getValue(), new AppliedProperty(
                                    new AppliedProperty(pp.rowBind(), java.util.Objects.requireNonNull(
                                            ch.keyAlias(), "lift chain without a key alias")),
                                    key.getKey()));
                        }
                        chainNulls.computeIfAbsent(key.getValue(),
                                k -> chainKeyNull(ch, key.getKey(), md, model));
                    }
                }
            }
            Set<String> projectedKeys = new LinkedHashSet<>();
            for (var kn : keyNames.entrySet()) {
                String name = kn.getKey();
                projectedKeys.add(name);
                String physical = java.util.Objects.requireNonNull(keyPhysical.get(name));
                // a link key names a DIFFERENT physical column per member
                // (FIRM_ID here, OWNER_ID there): this thread reads its own;
                // a chain key is a MID's column: the owning thread reads it
                // off its mid slot, the others type the NULL by the mid
                String own = ownKeys.get(name);
                ValueSpecification chainRead = ownChainReads.get(name);
                ValueSpecification chainNull = chainNulls.get(name);
                ValueSpecification read = chainRead != null ? chainRead
                        : own != null ? new AppliedProperty(pp.rowBind(), own)
                        : chainNull != null ? chainNull
                        : MappingNormalizer.nullOfPhysicalKind((ClassMapping.Relational)
                                members.get(kn.getValue()[0]), physical, md, model);
                // toOne types both threads identically (real read vs
                // NULL cast); lowering is erasure — the key stays NULL
                read = new AppliedFunction(com.legend.builtin.Pure.Lite.TRUST_ONE, List.of(read));
                cols.add(new ColSpec(name, new LambdaFunction(
                        List.of(pp.rowBind()), List.of(read)), null));
            }
            // THE SHARED TABLE KEY (TABLE_KEY_SUFFIX): every member over the
            // keyed table reads its own row's key UNGATED (a merged scan
            // collapses it to the plain column); other tables' threads NULL
            for (var sk : sharedKeys.entrySet()) {
                String db = sk.getKey().get(0);
                String table = sk.getKey().get(1);
                String col = sk.getKey().get(2);
                boolean mine = members.get(ordinal) instanceof ClassMapping.Relational mr
                        && mr.mainTable() != null
                        && mr.mainTable().database().equals(db)
                        && MappingNormalizer.canonicalTable(mr.mainTable().table()).equals(table)
                        && col.equals(tableKey(mr, model));
                ValueSpecification read = mine
                        ? new AppliedProperty(pp.rowBind(), col)
                        : MappingNormalizer.nullOfPhysicalKind((ClassMapping.Relational)
                                members.get(sk.getValue()), col, md, model);
                read = new AppliedFunction(com.legend.builtin.Pure.Lite.TRUST_ONE, List.of(read));
                cols.add(new ColSpec(sharedKeyName(table, col), new LambdaFunction(
                        List.of(pp.rowBind()), List.of(read)), null));
            }
            // CHAINED entries: the owning thread wraps its pipeline in the
            // MID-hop joins and reads the final hop's source keys via the
            // last mid slot; other threads project a typed NULL of the mid
            // table's column kind (engine 3-sets golden: fk1_1 from a_0)
            ValueSpecification threadPipe = pp.pipeline();
            // two chained entries may share a mid hop (two lifted props
            // navigating through the same mid join): ONE slot serves both
            Set<String> wrapped = new LinkedHashSet<>();
            for (LiftChain ch : chainsByOrdinal.getOrDefault(ordinal,
                    Collections.emptyList())) {
                for (LiftMidStep st : ch.steps()) {
                    if (!wrapped.add(st.alias())) {
                        continue;
                    }
                    threadPipe = new AppliedFunction(Pure.Lite.JOIN_SLOT, List.of(threadPipe,
                            new ColSpec(st.alias(), new LambdaFunction(List.of(),
                                    List.of(ViewRelation.relationExpr(
                                            st.db(), st.table(), model, md))),
                                    null),
                            st.cond()));
                }
            }
            addChainedLiftCols(chainsByOrdinal, ownChainReads, pp, md, model, cols, projectedKeys);
            return new Thread(threadPipe, cols);
    }

    /** The single-scan thread of a filtered same-table group: each column
     * is the member-gated if-chain over the members' own values, the source
     * is the shared unfiltered scan restricted to rows any member claims. */
    private static ValueSpecification mergedScan(List<Integer> group,
            List<MappingNormalizer.RelationalParts> parts, List<ClassMapping> members,
            List<String> common, @com.legend.Nullable ClassDefinition owner,
            String className, Map<String, LinkedHashSet<String>> embSubs,
            Map<String, String> embInner,
            Map<String, LinkedHashSet<String>> subTypeProps,
            Map<Integer, Map<String, String>> srcKeysByOrdinal,
            Map<Integer, List<LiftChain>> chainsByOrdinal,
            Map<List<String>, Integer> sharedKeys,
            ResolvedMapping md, ModelBuilder model) {
        List<FilteredScan> scans = new ArrayList<>();
        List<Thread> threads = new ArrayList<>();
        for (int o : group) {
            scans.add(java.util.Objects.requireNonNull(filteredScan(parts.get(o))));
            threads.add(threadOf(o, parts.get(o), members, common, owner, className,
                    embSubs, embInner, subTypeProps, srcKeysByOrdinal, chainsByOrdinal,
                    sharedKeys, md, model));
        }
        Variable row = scans.get(0).row();
        ValueSpecification any = null;
        for (FilteredScan fs : scans) {
            any = any == null ? fs.pred() : new AppliedFunction("or", List.of(any, fs.pred()));
        }
        ValueSpecification pipe = new AppliedFunction("filter", List.of(
                ScanSource.merged(scans.stream().map(FilteredScan::source).toList()),
                new LambdaFunction(List.of(row), List.of(java.util.Objects.requireNonNull(any)))));
        List<ColSpec> cols = new ArrayList<>();
        for (int c = 0; c < threads.get(0).cols().size(); c++) {
            String name = threads.get(0).cols().get(c).name();
            // the if-chain: member k's value where its filter holds; the last
            // member's value is the (unreachable — the thread is guarded)
            // terminal else
            ValueSpecification value = unwrapTrustOne(java.util.Objects.requireNonNull(
                    threads.get(threads.size() - 1).cols().get(c).function1()).body().get(0));
            for (int k = threads.size() - 2; k >= 0; k--) {
                ValueSpecification own = unwrapTrustOne(java.util.Objects.requireNonNull(
                        threads.get(k).cols().get(c).function1()).body().get(0));
                // structural (record) equality — printing both trees to
                // compare them rendered the growing if-chain per member per
                // column: 40% of the metamodel's 22ms normalization
                if (own.equals(value)) {
                    continue;   // the same value either way (a typed NULL no member owns)
                }
                value = new AppliedFunction("if", List.of(scans.get(k).pred(),
                        new LambdaFunction(List.of(), List.of(own)),
                        new LambdaFunction(List.of(), List.of(value))));
            }
            // the alignment wrap OUTSIDE the chain: every thread's column
            // types [1] (lowering is erasure — the CASE stays nullable)
            value = new AppliedFunction(Pure.Lite.TRUST_ONE, List.of(value));
            cols.add(new ColSpec(name, new LambdaFunction(List.of(row), List.of(value)), null));
        }
        return new AppliedFunction("project", List.of(pipe, new ColSpecArray(cols)));
    }

    /** A member column's value without its {@code trustOne} alignment wrap
     * — the merged if-chain is a nullable CASE (the union's columns are
     * nullable by contract; a [1] stamp over it trips the stamp invariant). */
    private static ValueSpecification unwrapTrustOne(ValueSpecification v) {
        return v instanceof AppliedFunction f && f.function().equals(Pure.Lite.TRUST_ONE)
                && f.parameters().size() == 1 ? f.parameters().get(0) : v;
    }

    /** The union root's recomposed ctor over the finished union pipeline:
     * plain reads for common scalar props and lifted navs, rebuilt
     * embedded ctors for the distributed/nav-served embedded tops. */
    private static ValueSpecification recomposeUnionRoot(String className,
            @com.legend.Nullable ValueSpecification union, List<String> common,
            List<NavLift> lifts, LinkedHashSet<String> embTops, EmbDist emb,
            ModelBuilder model) {
        Variable row = new Variable("u_row");
        Map<String, KeyExpression> ctor = new LinkedHashMap<>();
        for (String prop : common) {
            ctor.put(prop, new KeyExpression(
                    new AppliedProperty(row, prop), false, false));
        }
        for (NavLift lf : lifts) {
            if (ClassMapping.isSubTypeColumn(lf.property())) {
                continue;   // a subtype-only lift: not a property of the root
            }
            ctor.put(lf.property(), new KeyExpression(
                    new AppliedProperty(row, lf.property()), false, false));
        }
        for (String top : embTops) {
            ctor.put(top, new KeyExpression(
                    rebuildEmbCtor(top, emb.subs(), emb.inner(),
                            emb.navSubs(), row, model),
                    false, false));
        }
        return new AppliedFunction("map", List.of(union,
                new LambdaFunction(List.of(row),
                        List.of(MappingNormalizer.buildNewInstanceToOne(className, ctor, model)))));
    }

    /** The embedded ctor under a field value: unwrap {@code toOne(...)}
     * then the parser/normalizer {@code new(ptr, NewInstance)} wrapper
     * (MappingNormalizer.buildNewInstance emission). Null = not a ctor. */
    private static @com.legend.Nullable NewInstance ctorOf(ValueSpecification v) {
        if (v instanceof AppliedFunction f && com.legend.compiler.ResolvedNames.referents(f).stream().anyMatch(Pure::isToOneCall)
                && f.parameters().size() == 1) {
            v = f.parameters().get(0);
        }
        if (v instanceof AppliedFunction nf && AppliedFunction.isNew(nf)
                && nf.parameters().size() == 2) {
            v = nf.parameters().get(1);
        }
        return v instanceof NewInstance ni ? ni : null;
    }

    /** THREAD-PROJECTABLE: plain member-row reads (depth-1 property over
     * the row binder), literals, and functions thereof. A deeper property
     * chain (a hoisted join-slot sub-row read) is NOT — projecting it in
     * the thread would need the slot materialized inside the thread. */
    private static boolean isThreadProjectable(ValueSpecification v,
            String rowVar) {
        return switch (v) {
            // one-hop ($row.col) or two-hop ($row.slot.col): an embedded
            // sub bound THROUGH a join reads its emitted pipeline slot —
            // the owning member's thread carries that join (the same
            // two-hop-body shape chained-lift key columns project)
            case AppliedProperty ap -> ap.receiver()
                    instanceof com.legend.protocol.spec.Variable rv
                    ? rv.name().equals(rowVar)
                    : ap.receiver() instanceof AppliedProperty inner
                            && inner.receiver()
                                    instanceof com.legend.protocol.spec.Variable rv2
                            && rv2.name().equals(rowVar);
            case AppliedFunction f -> f.parameters().stream()
                    .allMatch(x -> isThreadProjectable(x, rowVar));
            case com.legend.protocol.spec.Variable var2 -> false;
            case NewInstance ni -> false;
            default -> true;   // literals / annotations
        };
    }

    /** CHAINED lift entries' key columns for one thread: the owning
     * ordinal reads its last-mid-slot keys; other ordinals project typed
     * NULLs of the mid table's column kind (engine 3-sets golden). */
    private static void addChainedLiftCols(
            Map<Integer, List<LiftChain>> chainsByOrdinal,
            Map<String, ValueSpecification> ownChainReads,
            MappingNormalizer.RelationalParts pp, ResolvedMapping md,
            ModelBuilder model, List<ColSpec> cols, Set<String> projectedKeys) {
        // ONE projection per chain key name, in the same (registration)
        // order for every thread, never a name the published keys already
        // projected: this thread's own read where its chain carries the
        // name, else a typed NULL of the mid column's kind
        Set<String> projected = new LinkedHashSet<>(projectedKeys);
        for (var en : chainsByOrdinal.entrySet()) {
            for (LiftChain ch : en.getValue()) {
                for (var key : ch.keys().entrySet()) {
                    if (!projected.add(key.getValue())) {
                        continue;
                    }
                    ValueSpecification read = ownChainReads.get(key.getValue());
                    if (read == null) {
                        read = chainKeyNull(ch, key.getKey(), md, model);
                    }
                    read = new AppliedFunction(com.legend.builtin.Pure.Lite.TRUST_ONE, List.of(read));
                    cols.add(new ColSpec(key.getValue(), new LambdaFunction(
                            List.of(pp.rowBind()), List.of(read)), null));
                }
            }
        }
    }

    /** A typed NULL of a chain key's mid column (view-aware: chained lifts
     * land on VIEW mid tables too — unionOfViewsToViewToUnion). */
    private static ValueSpecification chainKeyNull(LiftChain ch, String col,
            ResolvedMapping md, ModelBuilder model) {
        String kind = model.knowledge().columnKind(ch.keyDb(), ch.keyTable(), col);
        if (kind == null) {
            throw new NotImplementedException(
                    "chained union key column '" + col
                    + "' has no derivable pure kind on table '"
                    + ch.keyTable() + "'; mapping=" + md.qualifiedName());
        }
        return new AppliedFunction("cast", List.of(
                new PureCollection(List.of()),
                new TypeAnnotation.Named(new TypeExpression.NameRef(kind))));
    }

    /** The embedded distribution: dotted-path leaf sets ("firm" ->
     * {legalName}, "applicant.firm" -> {legalName}), per-path ctor classes
     * for the root recomposition, and top props in appearance order. A
     * top prop with ANY unprojectable leaf (join-slot sub-read) poisons
     * WHOLE — conservative, never a silently-wrong projection. */
    /** {@code navSubs}: class-typed SAME-NAME one-hop reads under each
     * ctor path — never member thread columns (a class collection has no
     * scalar projection); they recompose as plain union-level reads the
     * nav lift serves. Mirrors every prune {@code subs} takes. */
    private record EmbDist(Map<String, LinkedHashSet<String>> subs,
            Map<String, String> inner, LinkedHashSet<String> tops,
            Map<String, LinkedHashSet<String>> navSubs) {
    }

    private static EmbDist collectEmbeddedDistribution(
            List<MappingNormalizer.RelationalParts> parts,
            @com.legend.Nullable ClassDefinition unionClass, ModelBuilder model) {
        Map<String, LinkedHashSet<String>> embSubs = new LinkedHashMap<>();
        Map<String, String> embInner = new LinkedHashMap<>();
        Map<String, LinkedHashSet<String>> pathClasses = new LinkedHashMap<>();
        Map<String, LinkedHashSet<String>> navSubs = new LinkedHashMap<>();
        Set<String> poisoned = new LinkedHashSet<>();
        for (MappingNormalizer.RelationalParts pp : parts) {
            for (var fe : pp.fields().entrySet()) {
                // SUBTYPE-only embedded props (a member ctor field the
                // union class does not declare) belong to the stc subtype
                // dispatch, never the base recompose — distributing them
                // types ^Base(subProp=...) loudly (partial subtype family)
                if (unionClass == null || model.knowledge().propertyType(unionClass, fe.getKey()) == null) {
                    continue;
                }
                NewInstance ni = ctorOf(fe.getValue().value());
                if (ni != null) {
                    collectEmbLeaves(fe.getKey(), fe.getKey(), ni,
                            pp.rowBind().name(), embSubs, embInner, poisoned,
                            pathClasses, navSubs, model);
                }
            }
        }
        // MEMBERS DISAGREE on a path's ctor class (Inline[person] ^Person
        // vs Inline[airline] ^Airline under vehicleOwner): recompose as
        // the DECLARED property class and keep only ITS declared leaves —
        // subclass-only leaves stay off the base recompose (cast reads of
        // COMMON props survive; the merged-ctor form typed ^Person(planes)
        // loudly). Unresolvable declared chains poison the top (loud).
        for (var pce : pathClasses.entrySet()) {
            if (pce.getValue().size() < 2 || unionClass == null) {
                continue;
            }
            String path = pce.getKey();
            ClassDefinition decl = unionClass;
            for (String seg : path.split("\\.")) {
                TypeExpression t = decl == null ? null : model.knowledge().propertyType(decl, seg);
                decl = t instanceof TypeExpression.NameRef nr
                        ? model.knowledge().hierarchyClass(nr.name()).orElseThrow(() -> new IllegalStateException("F7.8: class unresolved at UnionSynthesis#11 (this default NEVER fired on the corpus census; a miss here is a real model gap): " + nr.name())) : null;
            }
            if (decl == null) {
                poisoned.add(path.contains(".")
                        ? path.substring(0, path.indexOf('.')) : path);
                continue;
            }
            embInner.put(path, decl.qualifiedName());
            ClassDefinition decl0 = decl;
            LinkedHashSet<String> lv = embSubs.get(path);
            if (lv != null) {
                lv.removeIf(leaf -> model.knowledge().propertyType(decl0, leaf) == null);
                if (lv.isEmpty()) {
                    embSubs.remove(path);
                }
            }
            LinkedHashSet<String> nv = navSubs.get(path);
            if (nv != null) {
                nv.removeIf(leaf -> model.knowledge().propertyType(decl0, leaf) == null);
                if (nv.isEmpty()) {
                    navSubs.remove(path);
                }
            }
            // NESTED ctor subtrees under a prop the declared class does
            // not carry (Airline's planes under VehicleOwner) prune too —
            // the recompose loop would re-enter them as ctor fields
            java.util.function.Predicate<String> off = k ->
                    k.startsWith(path + ".")
                    && model.knowledge().propertyType(decl0, k.substring(path.length() + 1).split("\\.")[0]) == null;
            embInner.keySet().removeIf(off);
            embSubs.keySet().removeIf(off);
            navSubs.keySet().removeIf(off);
        }
        for (String bad : poisoned) {
            embSubs.keySet().removeIf(k -> k.equals(bad)
                    || k.startsWith(bad + "."));
            embInner.keySet().removeIf(k -> k.equals(bad)
                    || k.startsWith(bad + "."));
            navSubs.keySet().removeIf(k -> k.equals(bad)
                    || k.startsWith(bad + "."));
        }
        LinkedHashSet<String> tops = new LinkedHashSet<>();
        for (String k : embSubs.keySet()) {
            tops.add(k.contains(".") ? k.substring(0, k.indexOf('.')) : k);
        }
        for (String k : navSubs.keySet()) {
            tops.add(k.contains(".") ? k.substring(0, k.indexOf('.')) : k);
        }
        return new EmbDist(embSubs, embInner, tops, navSubs);
    }

    /** One member thread's embedded sub-columns (schema-contract handling
     * mirrors the scalar threads: numeric coercion + String cast + toOne
     * alignment; absent members project typed NULLs). */
    private static void addEmbeddedThreadCols(
            Map<String, LinkedHashSet<String>> embSubs,
            Map<String, String> embInner,
            MappingNormalizer.RelationalParts pp, ModelBuilder model,
            List<ColSpec> cols) {
        for (var epe : embSubs.entrySet()) {
            String epath = epe.getKey();
            ClassDefinition inner = model.knowledge().hierarchyClass(embInner.get(epath))
                    .orElse(null);
            NewInstance ector = ctorAtPath(pp.fields(), epath);
            for (String sub : epe.getValue()) {
                ValueSpecification sv = ector != null
                        && ector.first(sub) != null
                        ? java.util.Objects.requireNonNull(
                                ector.first(sub)).value()
                        : MappingNormalizer.nullOfDeclaredType(
                                inner, sub, model);
                sv = DeclaredCoercions.coerceToDeclaredNumeric(
                        sv, sub, embInner.get(epath), model);
                TypeExpression sdt = inner == null ? null
                        : model.knowledge().propertyType(inner, sub);
                if (sdt instanceof TypeExpression.NameRef sdn
                        && "String".equals(MappingNormalizer
                                .simpleTypeName(sdn.name()))) {
                    sv = new AppliedFunction("cast", List.of(sv,
                            new TypeAnnotation.Named(
                                    new TypeExpression.NameRef("String"))));
                }
                sv = new AppliedFunction(com.legend.builtin.Pure.Lite.TRUST_ONE, List.of(sv));
                cols.add(new ColSpec(embCol(epath, sub),
                        new LambdaFunction(List.of(pp.rowBind()),
                                List.of(sv)), null));
            }
        }
    }

    /** Recursive leaf collection under dotted ctor paths; nested ctors
     * recurse, unprojectable leaves poison the whole TOP prop. */
    private static void collectEmbLeaves(String top, String pathKey,
            NewInstance ni, String rowVar,
            Map<String, LinkedHashSet<String>> embSubs,
            Map<String, String> embInner, Set<String> poisoned,
            Map<String, LinkedHashSet<String>> pathClasses,
            Map<String, LinkedHashSet<String>> navSubs, ModelBuilder model) {
        embInner.putIfAbsent(pathKey, ni.className());
        pathClasses.computeIfAbsent(pathKey, k -> new LinkedHashSet<>())
                .add(ni.className());
        for (var pe : ni.properties()) {
            NewInstance sub = ctorOf(pe.expression().value());
            if (sub != null) {
                collectEmbLeaves(top, pathKey + "." + pe.key(), sub,
                        rowVar, embSubs, embInner, poisoned, pathClasses,
                        navSubs, model);
                continue;
            }
            // CLASS-TYPED sub (an embedded ctor field holding a nav
            // read, $row.employees): NEVER a member thread column — a
            // class collection has no scalar projection. It recomposes
            // as a plain UNION-LEVEL read served by the nav lift
            // (scanJoinPms' embedded descent — one navigation
            // mechanism, no member-level twin).
            TypeExpression st = model.knowledge().propertyType(MissProbe.knownMiss(model.knowledge().hierarchyClass(ni.className())), pe.key());
            boolean sameNameRead = pe.expression().value()
                    instanceof AppliedProperty ap0
                    && ap0.receiver()
                            instanceof com.legend.protocol.spec.Variable rv0
                    && rv0.name().equals(rowVar)
                    && ap0.property().equals(pe.key());
            if (sameNameRead && st instanceof TypeExpression.NameRef snr
                    && model.knowledge().hierarchyClass(snr.name()).isPresent()) {
                navSubs.computeIfAbsent(pathKey,
                        k -> new LinkedHashSet<>()).add(pe.key());
                continue;
            }
            if (isThreadProjectable(pe.expression().value(), rowVar)) {
                embSubs.computeIfAbsent(pathKey, k -> new LinkedHashSet<>())
                        .add(pe.key());
            } else {
                poisoned.add(top);
            }
        }
    }

    /** The member's ctor at a dotted path, or null (member maps none). */
    private static @com.legend.Nullable NewInstance ctorAtPath(Map<String, KeyExpression> fields,
            String path) {
        String[] segs = path.split("\\.");
        KeyExpression ke = fields.get(segs[0]);
        NewInstance ni = ke == null ? null : ctorOf(ke.value());
        for (int i = 1; ni != null && i < segs.length; i++) {
            KeyExpression sub = ni.first(segs[i]);
            ni = sub == null ? null : ctorOf(sub.value());
        }
        return ni;
    }

    /** The synthetic union column for one embedded leaf. */
    private static String embCol(String path, String sub) {
        return "emb__" + path.replace(".", "__") + "__" + sub;
    }

    /** Recompose the union root's embedded ctor from projected columns. */
    private static ValueSpecification rebuildEmbCtor(String path,
            Map<String, LinkedHashSet<String>> embSubs,
            Map<String, String> embInner,
            Map<String, LinkedHashSet<String>> navSubs, Variable row,
            ModelBuilder model) {
        Map<String, KeyExpression> fields = new LinkedHashMap<>();
        for (String sub : embSubs.getOrDefault(path, new LinkedHashSet<>())) {
            fields.put(sub, new KeyExpression(
                    new AppliedProperty(row, embCol(path, sub)), false, false));
        }
        // class-typed subs recompose as PLAIN union-level reads — the
        // lifted navigation serves them (collectEmbLeaves' nav arm)
        for (String sub : navSubs.getOrDefault(path, new LinkedHashSet<>())) {
            fields.put(sub, new KeyExpression(
                    new AppliedProperty(row, sub), false, false));
        }
        for (String k : embInner.keySet()) {
            if (k.startsWith(path + ".") && k.indexOf('.', path.length() + 1) < 0) {
                fields.put(k.substring(path.length() + 1), new KeyExpression(
                        rebuildEmbCtor(k, embSubs, embInner, navSubs, row,
                                model),
                        false, false));
            }
        }
        return MappingNormalizer.buildNewInstanceToOne(
                embInner.get(path), fields, model);
    }

    /**
     * One lifted union navigation: the property, its target class extent,
     * the OR'd per-entry condition {@code {s,t|...}} (source reads
     * member-suffixed; target reads suffixed per routed target member),
     * the target-rows typing arg, and the per-ordinal source key columns
     * each member thread must carry.
     */
    record NavLift(String property, String targetClassFqn,
            ValueSpecification targetRows, LambdaFunction condition,
            @com.legend.Nullable LambdaFunction pairedCondition,
            Map<Integer, Map<String, String>> srcKeysByOrdinal,
            Map<Integer, List<LiftChain>> chainsByOrdinal) {
    }

    /** The MID hops of a chained lift entry as physical join steps,
     * prevAlias-scoped conditions composing hop to hop. */
    static ValueSpecification suffixTargetReads(ValueSpecification n,
            Variable t, int ord, Map<String, String> out) {
        return suffixTargetReads(n, t, "_" + ord, out);
    }

    /** Explicit-suffix variant: chained lifts scope their key names by
     * PROPERTY ({@code col__prop_ord}) so two chains of one member whose
     * mid tables share a column name never collide (V4: aT.fk1 vs
     * gT.fk1). All consumers read the names through {@code out} /
     * colspec-body provenance, never by pattern. */
    static ValueSpecification suffixTargetReads(ValueSpecification n,
            Variable t, String suffix, Map<String, String> out) {
        return suffixTargetReads(n, t, suffix, out, null);
    }

    /**
     * THE LINK KEY NAME (clean-sheet B3.1b): the column a navigation joins
     * on, as the member set publishes it — named by the NAVIGATING set and
     * the property ({@code ul_Firm_employees}; a union member navigating
     * carries its own set id, so {@code a1_b} and {@code a2_b} pair each
     * source member with its own target member), plus a position suffix
     * for a condition reading several target columns. Every routed member
     * projects its OWN physical column under the name, un-routed members a
     * typed NULL; the navigating class reads the name and nothing else.
     * A hand author names by meaning; the translator names by the route.
     */
    /**
     * THE NAVIGATING IDENTITY a link key is named by: the navigating set's
     * id — or its CLASS when the set is a member of a union whose members
     * all route {@code property} identically (same target sets through the
     * same joins). Identical routes pair every source member with every
     * routed target member, so one key serves them all and the members'
     * navigations stay textually equal (a single-table hierarchy whose
     * kinds navigate alike still merges into one scan); routes that differ
     * per source member keep per-set keys, so each pairs with its own
     * target member (the union-to-union trap row).
     */
    static String navigatingIdentity(ResolvedMapping md, ClassMapping navSet, String property,
            ModelBuilder model) {
        String own = ResolvedMapping.idOf(navSet);
        // the operation (union, or inheritance whose members are the
        // subclasses' sets) the set is a MEMBER of
        String opClass = null;
        List<String> memberIds = List.of();
        for (LegacyMappingDefinition m : md.closure()) {
            for (ClassMapping cm : m.classMappings()) {
                if (cm instanceof ClassMapping.Union cu && cu.memberSetIds().contains(own)) {
                    opClass = cu.className();
                    memberIds = cu.memberSetIds();
                } else if (cm instanceof ClassMapping.Inheritance ih) {
                    List<String> ids = inheritanceMembers(md, ih, model).stream()
                            .map(ResolvedMapping::idOf).toList();
                    if (ids.contains(own)) {
                        opClass = ih.className();
                        memberIds = ids;
                    }
                }
            }
        }
        if (opClass == null) {
            return own;
        }
        Set<String> mine = routeSignature(md, own, property);
        for (String id : memberIds) {
            if (!routeSignature(md, id, property).equals(mine)) {
                return own;
            }
        }
        return opClass.replace("::", "_");
    }

    /** Every routed Join PM of a set, through its embedded blocks (an
     * embedded property's sub-PMs navigate for the owning set — the
     * navigator emits them with the set's own identity). */
    static List<PropertyMapping.Join> routedJoins(ClassMapping cm) {
        List<PropertyMapping.Join> out = new ArrayList<>();
        if (cm instanceof ClassMapping.Relational r) {
            collectRoutedJoins(r.propertyMappings(), out);
        }
        return out;
    }

    private static void collectRoutedJoins(List<PropertyMapping> pms, List<PropertyMapping.Join> out) {
        for (PropertyMapping pm : pms) {
            switch (pm) {
                case PropertyMapping.Join j -> {
                    if (j.targetSetId() != null) {
                        out.add(j);
                    }
                }
                case PropertyMapping.Embedded e -> collectRoutedJoins(e.propertyMappings(), out);
                case PropertyMapping.OtherwiseEmbedded oe -> collectRoutedJoins(oe.embedded(), out);
                case PropertyMapping.LocalProperty lp -> collectRoutedJoins(List.of(lp.body()), out);
                default -> { }
            }
        }
    }

    /** A set's routes of {@code property}: its own class-PM routes (the
     * mapping's record, never an injected copy) plus the association pair
     * entries of the closure whose SOURCE set it is — as (target set,
     * join chain) — the one signature both the publisher and the
     * navigator compare. */
    private static Set<String> routeSignature(ResolvedMapping md, String setId, String property) {
        Set<String> out = new LinkedHashSet<>();
        ClassMapping cm = md.set(setId);
        if (cm != null) {
            for (PropertyMapping.Join j : routedJoins(cm)) {
                if (j.propertyName().equals(property)) {
                    out.add(joinSignature(j, java.util.Objects.requireNonNull(j.targetSetId())));
                }
            }
        }
        for (LegacyMappingDefinition m : md.closure()) {
            for (AssociationMapping am : m.associationMappings()) {
                if (!(am instanceof AssociationMapping.Relational rel)) {
                    continue;
                }
                for (AssociationPropertyMapping apm : rel.propertyMappings()) {
                    if (setId.equals(apm.sourceSetId()) && property.equals(apm.propertyName())
                            && apm.body() instanceof PropertyMapping.Join j) {
                        String tgt = j.targetSetId() != null ? j.targetSetId() : apm.targetSetId();
                        if (tgt != null) {
                            out.add(joinSignature(j, tgt));
                        }
                    }
                }
            }
        }
        return out;
    }

    private static String joinSignature(PropertyMapping.Join j, String targetSet) {
        StringBuilder b = new StringBuilder(targetSet).append('=');
        for (JoinChainElement h : j.joins()) {
            b.append(h.databaseName() != null ? h.databaseName() : j.database())
                    .append('@').append(h.joinName()).append('>');
        }
        return b.toString();
    }

    static String linkKeyName(String navigatingSetId, String property, int shape, int position) {
        return navigatingSetId + "_" + property + (shape < 0 ? "" : "_s" + shape)
                + (position == 0 ? "" : "_" + position);
    }

    /**
     * THE SHAPES of one property's routes: a route's condition with its
     * target reads erased and its source reads reduced to bare columns
     * (a shared-prefix chain reads {@code $s.alias.col}; the member side
     * translates {@code $s.col} — the same shape). Routes of ONE shape
     * share ONE link key (their source sides agree, so every routed
     * member's key may carry one name and the join is one equality);
     * routes of different shapes (audit 12 F3: FirmID into one member,
     * LegacyID into another) keep separate keys, each published only by
     * its own members, so a source column never matches the other
     * route's member. Both the navigator and the member side compute
     * this list from the same route conditions, in route order.
     */
    static List<ValueSpecification> routeShapes(List<ValueSpecification> rawConds,
            Variable s, Variable t) {
        List<ValueSpecification> shapes = new ArrayList<>();
        for (ValueSpecification raw : rawConds) {
            ValueSpecification shape = routeShape(raw, s, t);
            if (!shapes.contains(shape)) {
                shapes.add(shape);
            }
        }
        return shapes;
    }

    static ValueSpecification routeShape(ValueSpecification raw, Variable s, Variable t) {
        ValueSpecification erased = rewriteTargetReads(raw, t, col -> new AppliedProperty(t, "?"));
        return bareSourceReads(erased, s);
    }

    private static ValueSpecification bareSourceReads(ValueSpecification n, Variable s) {
        if (n instanceof AppliedProperty ap
                && ap.receiver() instanceof AppliedProperty inner
                && inner.receiver() instanceof Variable v
                && v.name().equals(s.name())) {
            return new AppliedProperty(v, ap.property());
        }
        return switch (n) {
            case AppliedFunction af -> af.withParameters(
                    af.parameters().stream().map(x -> bareSourceReads(x, s)).toList());
            case AppliedProperty ap -> new AppliedProperty(bareSourceReads(ap.receiver(), s),
                    ap.property());
            case PureCollection pc -> new PureCollection(pc.values().stream()
                    .map(x -> bareSourceReads(x, s)).toList());
            default -> n;
        };
    }

    /** The shapes of one set's routes of {@code prop} (its single-hop and
     * shared-prefix Join PMs): the member side of a lifted navigation's
     * key names, computed exactly as the target union's inbound scan
     * computes them. */
    static List<ValueSpecification> memberRouteShapes(ClassMapping member, String prop,
            String memberTable, ResolvedMapping md, ModelBuilder model,
            Variable s, Variable t) {
        List<ValueSpecification> conds = new ArrayList<>();
        for (PropertyMapping.Join j : routedJoins(member)) {
            if (j.propertyName().equals(prop)) {
                ValueSpecification c = lastHopCondition(j, memberTable, md, model, s, t);
                if (c != null) {
                    conds.add(c);
                }
            }
        }
        return routeShapes(conds, s, t);
    }

    /** A route's LAST hop translated over {@code s} (its previous table)
     * and {@code t} (the landing table) — the condition the navigator and
     * the member side both derive their key names from; null when the
     * Join is unknown (loud at the route's own emission). */
    static @com.legend.Nullable ValueSpecification lastHopCondition(PropertyMapping.Join j,
            @com.legend.Nullable String navigatingTable, ResolvedMapping md,
            ModelBuilder model, Variable s, Variable t) {
        return hopCondition(j, j.joins().size() - 1, navigatingTable, md, model, s, t);
    }

    /** The condition a route's link key is named and paired by: its LAST
     * hop for a single-hop route or a chain whose group shares a prefix
     * (the navigator lands there), its FIRST hop for a per-arm chain (the
     * mids ride the routed member's thread, which projects the first mid's
     * column as the key — B3.2). The navigator and the member side call
     * this with the same {@code uniform} verdict over the same group. */
    static @com.legend.Nullable ValueSpecification routeKeyCondition(PropertyMapping.Join j,
            boolean uniform, @com.legend.Nullable String navigatingTable, ResolvedMapping md,
            ModelBuilder model, Variable s, Variable t) {
        return uniform || j.joins().size() <= 1
                ? lastHopCondition(j, navigatingTable, md, model, s, t)
                : hopCondition(j, 0, navigatingTable, md, model, s, t);
    }

    /** Hop {@code idx} of a route translated over {@code s} (its previous
     * table: the navigating table for the first hop, else the table the
     * previous hop shares with it) and {@code t} (the landing table). */
    static @com.legend.Nullable ValueSpecification hopCondition(PropertyMapping.Join j, int idx,
            @com.legend.Nullable String navigatingTable, ResolvedMapping md,
            ModelBuilder model, Variable s, Variable t) {
        JoinChainElement hop = j.joins().get(idx);
        String db = hop.databaseName() != null ? hop.databaseName() : j.database();
        DatabaseDefinition.JoinDefinition jd = model.findJoin(db, hop.joinName()).orElse(null);
        if (jd == null) {
            return null;
        }
        String prevTable = navigatingTable;
        if (idx > 0) {
            JoinChainElement prevHop = j.joins().get(idx - 1);
            String pdb = prevHop.databaseName() != null ? prevHop.databaseName() : j.database();
            DatabaseDefinition.JoinDefinition pjd = model.findJoin(pdb, prevHop.joinName()).orElse(null);
            if (pjd == null) {
                return null;
            }
            Set<String> pt = new LinkedHashSet<>();
            RelOpTranslator.collectTablesIn(pjd.operation(), pt);
            Set<String> ht = new LinkedHashSet<>();
            RelOpTranslator.collectTablesIn(jd.operation(), ht);
            pt.retainAll(ht);
            prevTable = pt.isEmpty() ? null : pt.iterator().next();
        }
        Set<String> tables = new LinkedHashSet<>();
        RelOpTranslator.collectTablesIn(jd.operation(), tables);
        if (prevTable != null) {
            tables.remove(MappingNormalizer.canonicalTable(prevTable));
            tables.remove(prevTable);
        }
        Map<String, ValueSpecification> scope = new LinkedHashMap<>();
        if (prevTable != null) {
            scope.put(MappingNormalizer.canonicalTable(prevTable), s);
        }
        for (String tb : tables) {
            scope.put(tb, t);
        }
        try {
            return RelOpTranslator.translate(jd.operation(), scope, t, null,
                    RelOpTranslator.PipelineView.NONE);
        } catch (NotImplementedException | ModelException e) {
            return null;    // loud at the route's own emission
        }
    }

    /** A SAME-TABLE inheritance target reached through ONE join: its body
     * is the shared table itself (no member threads to publish a key), and
     * every member is that table's rows, so the navigation reads the
     * physical column plainly (the pre-B3 rule, restored). */
    static boolean sameTableInheritanceMerge(ResolvedMapping md,
            ModelBuilder model, @com.legend.Nullable String targetClassFqn,
            List<UnionRoute> routes) {
        if (targetClassFqn == null) {
            return false;
        }
        ClassMapping.Inheritance ih = md.inheritanceOf(targetClassFqn);
        if (ih == null) {
            return false;
        }
        Set<String> joins = new HashSet<>();
        for (UnionRoute r : routes) {
            if (r.join().joins().size() != 1) {
                return false;
            }
            JoinChainElement hop = r.join().joins().get(0);
            joins.add((hop.databaseName() != null ? hop.databaseName()
                    : r.join().database()) + "@" + hop.joinName());
        }
        return joins.size() == 1
                && sharedInheritanceTable(inheritanceMembers(md, ih, model)) != null;
    }

    /** The shape index a route's key names carry: -1 when the property's
     * routes all share one shape. */
    static int shapeIndex(List<ValueSpecification> shapes, ValueSpecification raw,
            Variable s, Variable t) {
        if (shapes.size() <= 1) {
            return -1;
        }
        int i = shapes.indexOf(routeShape(raw, s, t));
        if (i < 0) {
            throw new IllegalStateException("normalizer bug: a route's condition is not"
                    + " among its property's shapes — the navigator and the member side"
                    + " translated the Join differently");
        }
        return i;
    }

    /** The target-side reads ({@code $t.col}) of a translated join
     * condition, in traversal order. */
    static void collectTargetReads(ValueSpecification n, Variable t, List<String> out) {
        rewriteTargetReads(n, t, col -> {
            out.add(col);
            return new AppliedProperty(t, col);
        });
    }

    /** Every target-side read {@code $t.col} of {@code n} replaced by
     * {@code read.apply(col)} — the one traversal the member-column
     * emission and the ordinal suffixing share. */
    static ValueSpecification rewriteTargetReads(ValueSpecification n, Variable t,
            java.util.function.Function<String, ValueSpecification> read) {
        if (n instanceof AppliedProperty ap
                && ap.receiver() instanceof Variable v
                && v.name().equals(t.name())) {
            return read.apply(ap.property());
        }
        return switch (n) {
            case AppliedFunction af -> af.withParameters(
                    af.parameters().stream().map(x ->
                            rewriteTargetReads(x, t, read)).toList());
            case AppliedProperty ap -> new AppliedProperty(
                    rewriteTargetReads(ap.receiver(), t, read), ap.property());
            case Variable v -> v;
            case CString ignored -> n;
            case CInteger ignored -> n;
            case CFloat ignored -> n;
            case CDecimal ignored -> n;
            case CBoolean ignored -> n;
            case CDate ignored -> n;
            case PureCollection pc -> new PureCollection(pc.values().stream()
                    .map(x -> rewriteTargetReads(x, t, read)).toList());
            default -> throw new NotImplementedException(
                    "routed join condition carries a "
                    + n.getClass().getSimpleName()
                    + " — its target reads cannot be rewritten yet");
        };
    }

    /** The pure kind of a route's target column, from the store (loud when
     * the table declares no derivable kind). */
    static String targetColumnKind(String db, String table, String col,
            ModelBuilder model, String mappingName) {
        String kind = model.knowledge().columnKind(db, table, col);
        if (kind == null) {
            throw new NotImplementedException("routed navigation key column '"
                    + col + "' has no derivable pure kind on table '" + table
                    + "'; mapping=" + mappingName);
        }
        return kind;
    }

    /** Typing colspecs for link keys read off a navigate's target rows:
     * each key types as its own landing table's column kind (a NULL cast
     * — the typing shim of the legacy bridge, no semantics). */
    static List<ColSpec> linkKeySpecs(Map<String, String[]> keyCols,
            ModelBuilder model, String mappingName) {
        List<ColSpec> specs = new ArrayList<>();
        for (var en : keyCols.entrySet()) {
            Variable kr = new Variable("kr");
            String[] v = en.getValue();
            String kind = targetColumnKind(v[1], v[2], v[0], model, mappingName);
            specs.add(new ColSpec(en.getKey(), new LambdaFunction(List.of(kr),
                    List.of(new AppliedFunction("cast", List.of(
                            new PureCollection(List.of()),
                            new TypeAnnotation.Named(new TypeExpression.NameRef(kind)))))),
                    null));
        }
        return specs;
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
    static @com.legend.Nullable String tableKey(@com.legend.Nullable ClassMapping cm,
            ModelBuilder model) {
        if (!(cm instanceof ClassMapping.Relational r) || r.mainTable() == null) {
            return null;
        }
        DatabaseDefinition.TableDefinition td = model.knowledge().table(r.mainTable().database(), r.mainTable().table()).orElse(null);
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

    /** {@link #suffixTargetReads(ValueSpecification, Variable, String, Map)}
     * with the member's table key ({@code tableKeyCol}, nullable) spelled
     * {@code <col>__pk} instead of member-suffixed. */
    static ValueSpecification suffixTargetReads(ValueSpecification n,
            Variable t, String suffix, Map<String, String> out,
            @com.legend.Nullable String tableKeyCol) {
        if (n instanceof AppliedProperty ap
                && ap.receiver() instanceof Variable v
                && v.name().equals(t.name())) {
            String suffixed = ap.property().equals(tableKeyCol)
                    ? ap.property() + TABLE_KEY_SUFFIX : ap.property() + suffix;
            out.put(ap.property(), suffixed);
            return new AppliedProperty(v, suffixed);
        }
        return switch (n) {
            case AppliedFunction af -> af.withParameters(
                    af.parameters().stream().map(x ->
                            suffixTargetReads(x, t, suffix, out, tableKeyCol)).toList());
            case AppliedProperty ap -> new AppliedProperty(
                    suffixTargetReads(ap.receiver(), t, suffix, out, tableKeyCol),
                    ap.property());
            case Variable v -> v;
            case CString ignored -> n;
            case CInteger ignored -> n;
            case CFloat ignored -> n;
            case CDecimal ignored -> n;
            case CBoolean ignored -> n;
            case CDate ignored -> n;
            case PureCollection pc -> new PureCollection(pc.values().stream()
                    .map(x -> suffixTargetReads(x, t, suffix, out, tableKeyCol)).toList());
            default -> throw new NotImplementedException(
                    "partial-union route join condition carries a "
                    + n.getClass().getSimpleName()
                    + " — not suffixable yet");
        };
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

    /** The MERGED (un-suffixed target) lift eligibility — see the caller's
     * partiallyMilestoning-golden comment. SAME-JOIN across every route is
     * REQUIRED: merged and routed emissions only coincide when the join is
     * literally shared; diagonal routes (different joins per member —
     * graph rootLevel SameStore golden) demand strict member pairing. */
    private static boolean liftTargetMerged(List<int[]> ordsPre,
            List<PropertyMapping.Join> jsPre, String prop,
            ClassMapping.@com.legend.Nullable Union targetUnion, String targetClassFqn,
            List<ClassMapping> members, ResolvedMapping md,
            ModelBuilder model) {
        if (targetUnion == null) {
            return false;
        }
        Set<Integer> tgtOrds = new HashSet<>();
        Set<Integer> srcMembers = new HashSet<>();
        Set<String> tgtColSets = new HashSet<>();
        boolean mergeable = true;
        for (int k2 = 0; mergeable && k2 < jsPre.size(); k2++) {
            PropertyMapping.Join j0 = jsPre.get(k2);
            if (j0.targetSetId() == null || j0.joins().size() != 1) {
                mergeable = false;
                break;
            }
            int o = md.memberOrdinal(targetUnion.memberSetIds(), j0.targetSetId());
            if (o < 0 || !srcMembers.add(ordsPre.get(k2)[0])) {
                mergeable = false;   // 2 routes on one source member
                break;
            }
            tgtOrds.add(o);
            JoinChainElement hop0 = j0.joins().get(0);
            String db0 = hop0.databaseName() != null
                    ? hop0.databaseName() : j0.database();
            DatabaseDefinition.JoinDefinition jd0 =
                    model.findJoin(db0, hop0.joinName()).orElse(null);
            if (jd0 == null) {
                mergeable = false;
                break;
            }
            String srcT = java.util.Objects.requireNonNull(((ClassMapping.Relational)
                    members.get(ordsPre.get(k2)[0])).mainTable(),
                    "union member set without ~mainTable").table();
            String tgtT = MappingNormalizer.determineTargetTable(jd0.operation(), srcT,
                    hop0.joinName(), prop, 1, md.qualifiedName());
            Set<String> cols0 = new TreeSet<>();
            MappingNormalizer.collectColumnsOfTable(jd0.operation(), tgtT, cols0);
            tgtColSets.add(String.join(",", cols0));
        }
        // NOTE (graph rootLevel SameStore vs partiallyMilestoning): the
        // ENGINE's two subsystems disagree on this exact shape — its
        // RELATIONAL path (pureToSQLQuery golden, rows [2,2]) merges and
        // cross-matches; its GRAPH executor pairs strictly per member
        // (product=null). The merged form is therefore CORRECT here (this
        // lift feeds the relational navigate); the graph-side strict
        // pairing needs a SECOND (paired) condition carried alongside —
        // the dual-condition design banked in task #84. A same-target-
        // table narrowing was tried and reverted: it broke the
        // partiallyMilestoning trio whose golden demands the cross-match.
        return mergeable
                && tgtOrds.size() == targetUnion.memberSetIds().size()
                && colsProjectedByTarget(tgtColSets, targetClassFqn, model);
    }

    /** ENGINE SINGLE-SET TARGET ROUTING (memory inclusive-union-dupes-
     * analysis): when the TARGET class is NOT union-mapped, every member's
     * SINGLE-hop PM cites the SAME (db, join), and each member
     * set-qualifies its OWN private target set, the engine routes the
     * navigation to ONE implementation — the LAST member's binding;
     * earlier members contribute NULL through the crossing (inclusive
     * golden: 'null as prodFk_1' in member 0, rows [2] not [2,2]). A
     * UNION-mapped target OR a shared/unqualified target set keeps the
     * per-member OR dispatch (snapshot golden: the engine's own OR form;
     * VarReferenceWithUnion golden: both members' rows live). */
    private static boolean singleSetTargetCollapse(
            ClassMapping.@com.legend.Nullable Union targetUnion, List<PropertyMapping.Join> js) {
        if (targetUnion != null || js.size() < 2) {
            return false;
        }
        boolean distinctTargetSets = js.stream()
                .map(PropertyMapping.Join::targetSetId)
                .filter(java.util.Objects::nonNull)
                .distinct().count() == js.size();
        if (!distinctTargetSets) {
            return false;
        }
        return js.stream().allMatch(x -> {
            if (x.joins().size() != 1 || js.get(0).joins().size() != 1) {
                return false;
            }
            JoinChainElement hx = x.joins().get(0);
            JoinChainElement h0 = js.get(0).joins().get(0);
            String dbx = hx.databaseName() != null ? hx.databaseName()
                    : x.database();
            String db0 = h0.databaseName() != null ? h0.databaseName()
                    : js.get(0).database();
            return hx.joinName().equals(h0.joinName()) && dbx.equals(db0);
        });
    }

    static List<LiftMidStep> liftMidSteps(PropertyMapping.Join j,
            String prop, String srcTable, ResolvedMapping md,
            ModelBuilder model) {
        List<LiftMidStep> midSteps = new ArrayList<>();
        String prevTable = srcTable;
        String prevAlias = null;
        for (int h = 0; h + 1 < j.joins().size(); h++) {
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
                    prevTable, midHop.joinName(), prop, h + 1,
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
            String midAlias = "nl__" + prop + "__" + midHop.joinName();
            midSteps.add(new LiftMidStep(midAlias, midDb, midTgt,
                    new LambdaFunction(List.of(ms, mt),
                            List.of(midCond))));
            prevTable = midTgt;
            prevAlias = midAlias;
        }
        return midSteps;
    }

    /** One physical MID hop of a CHAINED lift entry, wrapped around the
     * owning member's thread pipeline ({@code join(pipe, ~alias:
     * tableReference, cond)} — engine: mid tables join INSIDE the member
     * thread, 3-sets golden). */
    record LiftMidStep(String alias, String db, String table,
            LambdaFunction cond) {
    }

    /** A chained entry's per-member material: the mid steps plus the FINAL
     * hop's source-key columns (on the LAST mid table, read via its slot
     * and projected member-suffixed — engine {@code fk1_1}). */
    record LiftChain(List<LiftMidStep> steps,
            @com.legend.Nullable String keyAlias,
            String keyDb, String keyTable, Map<String, String> keys) {
    }


    /** MERGED target reads resolve against the union's PROJECTED row —
     * valid only when every read column IS a projected name (a mapped
     * value column like the partiallyMilestoning golden's {@code id}). A
     * RAW key ({@code fk}) takes the SUFFIXED NULL-crossed form, where
     * member pairing comes free: off-member suffixes read NULL, so only
     * same-member pairs match (engine sqlQueryMerging golden
     * {@code fk_0=fk_0 OR fk_1=fk_1}). */
    private static boolean colsProjectedByTarget(Set<String> tgtColSets,
            String targetClassFqn, ModelBuilder model) {
        if (tgtColSets.size() != 1) {
            return false;
        }
        ClassDefinition tgtOwner = model.knowledge().hierarchyClass(targetClassFqn).orElseThrow(() -> new IllegalStateException("F7.8: class unresolved at UnionSynthesis#12 (this default NEVER fired on the corpus census; a miss here is a real model gap): " + targetClassFqn));
        for (String c : tgtColSets.iterator().next().split(",")) {
            if (c.isEmpty() || tgtOwner == null
                    || model.knowledge().propertyType(tgtOwner, c) == null) {
                return false;
            }
        }
        return true;
    }

    /** One member's Join-PM scan: each class-typed Join records
     * (ordinal, join) under its property name. {@code declaredOwner}
     * walks the UNION-DECLARED chain: a property the declared chain
     * does not carry (subtype-only PMs — Street.coordinate under an
     * Address union) is NOT lifted; it stays member-local for the stc
     * subtype dispatch. The lift TARGET derives from the DECLARED
     * owner's property type — the same fact the lift loop used to
     * re-derive from the union class — recorded once at scan. */
    private static void scanJoinPms(List<PropertyMapping> pms,
            @com.legend.Nullable ClassDefinition owner,
            @com.legend.Nullable ClassDefinition declaredOwner, int ordinal,
            Map<String, List<int[]>> found,
            Map<String, List<PropertyMapping.Join>> joins,
            Map<String, String> targetByProp, ModelBuilder model) {
        scanJoinPms(pms, owner, declaredOwner, ordinal, found, joins,
                targetByProp, model, null);
    }

    /** {@code unionRoot}: the union class at the TOP level of the scan
     * (null inside an embedded descent) — a subtype-only class-typed Join
     * PM lifts under its stc key there (batch 108). */
    private static void scanJoinPms(List<PropertyMapping> pms,
            @com.legend.Nullable ClassDefinition owner,
            @com.legend.Nullable ClassDefinition declaredOwner, int ordinal,
            Map<String, List<int[]>> found,
            Map<String, List<PropertyMapping.Join>> joins,
            Map<String, String> targetByProp, ModelBuilder model,
            @com.legend.Nullable String unionRoot) {
        for (PropertyMapping pm : pms) {
            if (pm instanceof PropertyMapping.Embedded e) {
                // EMBEDDED descent: a class-typed Join inside an embedded
                // ctor is a union-level navigation like any other (the
                // recomposed ctor keeps the $row.<sub> read this lift
                // serves — the embedded-union witnesses' formerly-
                // unservable read). Both owner chains step through the
                // embedded property's type.
                TypeExpression et = owner == null ? null
                        : model.knowledge().propertyType(owner, e.propertyName());
                ClassDefinition subOwner = et instanceof TypeExpression.NameRef nr
                        ? model.knowledge().hierarchyClass(nr.name()).orElse(null) : null;
                TypeExpression edt = declaredOwner == null ? null
                        : model.knowledge().propertyType(declaredOwner, e.propertyName());
                ClassDefinition subDeclared = edt instanceof TypeExpression.NameRef enr
                        ? model.knowledge().hierarchyClass(enr.name()).orElse(null) : null;
                scanJoinPms(e.propertyMappings(), subOwner, subDeclared,
                        ordinal, found, joins, targetByProp, model);
                continue;
            }
            if (!(pm instanceof PropertyMapping.Join j)) {
                continue;
            }
            TypeExpression pt = owner == null ? null
                    : model.knowledge().propertyType(owner, pm.propertyName());
            if (!(pt instanceof TypeExpression.NameRef pnr)
                    || model.knowledge().hierarchyClass(pnr.name()).isEmpty()) {
                continue;   // scalar join-terminal shapes stay member-local
            }
            TypeExpression dt = declaredOwner == null ? null
                    : model.knowledge().propertyType(declaredOwner, pm.propertyName());
            if (!(dt instanceof TypeExpression.NameRef dnr)) {
                // SUBTYPE-ONLY class-typed Join PM (Bicycle[map2].person
                // under a Vehicle union — `person` is declared on
                // RoadVehicle, not on the union class): the scalar stc
                // dispatch has no column form for a navigation, so the
                // join LIFTS like any other — under the stc key of every
                // cast target that declares it (member class and its
                // ancestors below the root: subType(@Bicycle).person and
                // subType(@RoadVehicle).person both read it), the routes
                // being the conforming members' own; a non-member row
                // carries NULL keys and joins nothing (batch 108,
                // testInheritanceMultipleLevel). The recomposed ctor skips
                // stc keys — the resolver's row pseudo-bindings serve them.
                if (unionRoot != null && owner != null) {
                    for (String target : model.knowledge().ancestorsBelow(
                            owner.qualifiedName(), unionRoot)) {
                        ClassDefinition tcd = model.knowledge().hierarchyClass(target)
                                .orElse(null);
                        if (tcd == null || model.knowledge().propertyType(tcd, pm.propertyName()) == null) {
                            continue;
                        }
                        String key = ClassMapping.subTypeColumn(target,
                                pm.propertyName());
                        String prior0 = targetByProp.putIfAbsent(key, pnr.name());
                        if (prior0 != null && !prior0.equals(pnr.name())) {
                            throw new IllegalStateException(
                                    "union nav lift name collision: subtype"
                                    + " navigation '" + key + "' navigates to '"
                                    + prior0 + "' and '" + pnr.name()
                                    + "' across members");
                        }
                        found.computeIfAbsent(key, k -> new ArrayList<>())
                                .add(new int[]{ordinal});
                        joins.computeIfAbsent(key, k -> new ArrayList<>()).add(j);
                    }
                }
                continue;
            }
            // one alias, one target: a name colliding across DIFFERENT
            // declared scopes (top-level vs embedded) would OR unrelated
            // navigations under one lift — loud
            String prior = targetByProp.putIfAbsent(pm.propertyName(),
                    dnr.name());
            if (prior != null && !prior.equals(dnr.name())) {
                throw new IllegalStateException(
                        "union nav lift name collision: property '"
                        + pm.propertyName() + "' navigates to '" + prior
                        + "' and '" + dnr.name() + "' across members");
            }
            found.computeIfAbsent(pm.propertyName(), k -> new ArrayList<>())
                    .add(new int[]{ordinal});
            joins.computeIfAbsent(pm.propertyName(), k -> new ArrayList<>())
                    .add(j);
        }
    }

    static List<NavLift> collectNavLifts(ResolvedMapping md,
            String className, List<ClassMapping> members,
            ModelBuilder model, MappingLedger ledger) {
        // property -> per-member entries, member order
        Map<String, List<int[]>> found = new LinkedHashMap<>();
        Map<String, List<PropertyMapping.Join>> joins = new LinkedHashMap<>();
        Map<String, String> targetByProp = new LinkedHashMap<>();
        ClassDefinition declared = model.knowledge().hierarchyClass(className).orElseThrow(() -> new IllegalStateException("F7.8: class unresolved at UnionSynthesis#14 (this default NEVER fired on the corpus census; a miss here is a real model gap): " + className));
        for (int i = 0; i < members.size(); i++) {
            if (!(members.get(i) instanceof ClassMapping.Relational mr)) {
                continue;   // Relation(~func) members carry no Join PMs
            }
            ClassDefinition memberOwner = model.knowledge().hierarchyClass(mr.className()).orElseThrow(() -> new IllegalStateException("F7.8: class unresolved at UnionSynthesis#13 (this default NEVER fired on the corpus census; a miss here is a real model gap): " + mr.className()));
            scanJoinPms(mr.propertyMappings(), memberOwner, declared, i,
                    found, joins, targetByProp, model, className);
        }
        List<NavLift> lifts = new ArrayList<>();
        for (String prop : found.keySet()) {
            String targetClassFqn = java.util.Objects.requireNonNull(
                    targetByProp.get(prop),
                    "scan recorded a Join PM without its target class");
            if (!ledger.isMapped(targetClassFqn)) {
                continue;
            }
            // BITEMPORAL UNGATE (Leg 2): the per-dimension stampers
            // (milestonedPipeByStrategy walks only tables CARRYING each
            // dimension) are capability-aware by construction after the
            // temporal-frame arc — the audit-11 gate that protected the
            // hybrid over-match (12 vs 18) is retired; the hybrid family
            // gates the rows.

            ClassMapping.Union targetUnion = md.unionOf(targetClassFqn);
            // Pre-validate the property's entries: any unsupported or
            // unresolvable entry SKIPS the whole property's lift (poison
            // reason recorded; demanding the property fails loudly) —
            // audit 11: a partial lift matched the wrong members, a throw
            // here poisoned scalar-only union queries.
            String skipReason = null;
            for (PropertyMapping.Join j0 : java.util.Objects.requireNonNull(joins.get(prop))) {
                if (j0.targetSetId() != null && (targetUnion == null
                        || md.memberOrdinal(targetUnion.memberSetIds(), j0.targetSetId()) < 0)) {
                    // a route naming the target's ROOT/SOLE set is the
                    // UN-routed navigation (engine rootClassMappingByClass;
                    // multipleChainedJoins V2: z[y1, z0] into single-set Z)
                    ClassMapping set = md.set(j0.targetSetId());
                    // <= 1 RETAINED (audit 23 probed-and-reverted): the
                    // V5 chained-union family routes into a class whose
                    // sets live in an INCLUDE (zero own-mapping sets) and
                    // pins the root-navigation degradation as row-correct.
                    boolean rootOrSole = set instanceof ClassMapping.Relational tr
                            && (tr.root() || md.classMappings().stream()
                                    .filter(x -> x.className().equals(tr.className()))
                                    .count() <= 1);
                    if (rootOrSole) {
                        continue;
                    }
                    // H5 SET-ID DISPATCH: a route naming a NON-root set of
                    // a multi-set non-union target is a single-target
                    // navigation to THAT set — the landing table below is
                    // already the set's own table, and the resolver's
                    // routedTargetSetOf hint materializes the set's
                    // binding (engine inclusive-milestoning union goldens
                    // join the named set's table directly).
                    if (set instanceof ClassMapping.Relational) {
                        continue;
                    }
                    skipReason = "route '[" + j0.targetSetId() + "]' that is"
                            + " not a member of the target class's union";
                    break;
                }
            }
            if (skipReason != null) {
                ledger.poisons.merge(className,
                        "union navigation '" + prop + "' uses " + skipReason
                                + "; the property is not lifted",
                        (a, b) -> a + "; " + b);
                continue;
            }
            // MERGED (un-suffixed target) lift — the engine's cross-match
            // form, pinned by the partiallyMilestoning golden (source
            // members o1->p1, o2->p2; both joins read target column `id`;
            // ON prodFk_0 = id OR prodFk_1 = id; 2x2 rows asserted): fires
            // iff routes cover EVERY target member with exactly ONE route
            // PER SOURCE MEMBER and all entries read the SAME target
            // columns. A source member carrying routes to MULTIPLE target
            // members (unionToUnion: firm[f1] AND firm[f2] on each Person
            // set) keeps the per-pair suffixed form (testUnion golden
            // FirmID_0 = ID_0 OR FirmID_1 = ID_1 — audit 12: the merged
            // form cross-matched colliding keys, [0..1] fan-out).
            boolean liftTargetMerged = liftTargetMerged(found.get(prop),
                    joins.get(prop), prop, targetUnion, targetClassFqn,
                    members, md, model);
            Variable s = new Variable("s");
            Variable t = new Variable("t");
            ValueSpecification orCond = null;
            ValueSpecification orPaired = null;
            // raw (unsuffixed) entry condition -> the source members
            // reading it: same-source members merge into ONE disjunct
            Map<ValueSpecification, LinkedHashSet<Integer>> sameSource = new LinkedHashMap<>();
            boolean allSingleHop = true;
            String landingDb = null;
            String landingTable = null;
            // link key name -> [physical column, db, landing table] (typing)
            Map<String, String[]> tgtKeyCols = new LinkedHashMap<>();
            Map<Integer, Map<String, String>> srcKeys = new LinkedHashMap<>();
            Map<Integer, List<LiftChain>> chains = new LinkedHashMap<>();
            List<int[]> ords = found.get(prop);
            List<PropertyMapping.Join> js = joins.get(prop);
            boolean sameJoin = singleSetTargetCollapse(targetUnion, js);
            for (int k = 0; k < js.size(); k++) {
                if (sameJoin && k < js.size() - 1) {
                    continue;
                }
                int memberOrd = ords.get(k)[0];
                PropertyMapping.Join j = js.get(k);
                String srcTable = java.util.Objects.requireNonNull(((ClassMapping.Relational)
                        members.get(memberOrd)).mainTable(),
                        "union member set without ~mainTable").table();
                // MID hops (all but the last): physical join steps around
                // the owning member's thread (engine: mids join INSIDE the
                // thread; the final hop is the union-level navigation)
                List<LiftMidStep> midSteps = liftMidSteps(j, prop, srcTable,
                        md, model);
                String prevTable = midSteps.isEmpty() ? srcTable
                        : midSteps.get(midSteps.size() - 1).table();
                String prevAlias = midSteps.isEmpty() ? null
                        : midSteps.get(midSteps.size() - 1).alias();
                JoinChainElement hop = j.joins().get(j.joins().size() - 1);
                String hopDb = hop.databaseName() != null ? hop.databaseName()
                        : j.database();
                DatabaseDefinition.JoinDefinition jd =
                        model.findJoin(hopDb, hop.joinName()).orElseThrow(() ->
                                new ModelException(
                                        LegendCompileException
                                                .Phase.NORMALIZE,
                                        "Join '" + hop.joinName() + "' not found"
                                        + " in db '" + hopDb + "'; PM='" + prop
                                        + "', mapping=" + md.qualifiedName()));
                String tgtTable = MappingNormalizer.determineTargetTable(jd.operation(), prevTable,
                        hop.joinName(), prop, j.joins().size(),
                        md.qualifiedName());
                if (landingTable == null) {
                    landingDb = hopDb;
                    landingTable = tgtTable;
                }
                Map<String, ValueSpecification> scope = new LinkedHashMap<>();
                scope.put(prevTable, s);
                if (!tgtTable.equals(prevTable)) {
                    scope.put(tgtTable, t);
                }
                ValueSpecification cond = RelOpTranslator.translate(
                        jd.operation(), scope, t, null,
                        RelOpTranslator.PipelineView.NONE);
                Map<String, String> srcOut = new LinkedHashMap<>();
                // chained lifts: property-scoped key names (col__prop_ord)
                // — two chains of ONE member may land on mid tables sharing
                // a column name (V4: aT.fk1 vs gT.fk1)
                ValueSpecification rawCond = cond;
                cond = midSteps.isEmpty()
                        ? suffixTargetReads(cond, s, memberOrd, srcOut)
                        : suffixTargetReads(cond, s,
                                "__" + prop + "_" + memberOrd, srcOut);
                if (midSteps.isEmpty()) {
                    sameSource.computeIfAbsent(rawCond, rc -> new LinkedHashSet<>())
                            .add(memberOrd);
                } else {
                    allSingleHop = false;
                }
                if (midSteps.isEmpty()) {
                    Map<String, String> byName = srcKeys.computeIfAbsent(memberOrd,
                            x -> new LinkedHashMap<>());
                    for (var so : srcOut.entrySet()) {
                        byName.put(so.getValue(), so.getKey());   // name -> physical
                    }
                } else {
                    chains.computeIfAbsent(memberOrd, x -> new ArrayList<>())
                            .add(new LiftChain(midSteps, prevAlias,
                                    midSteps.get(midSteps.size() - 1).db(),
                                    prevTable, srcOut));
                }
                // the PAIRED variant builds ALWAYS; the emitted predicate
                // is the MERGED (raw-target) form only when
                // liftTargetMerged — the paired variant then rides
                // alongside for GRAPH children, whose engine subsystem
                // pairs strictly (TypedNavigate.pairedPredicate). The
                // target side reads the LINK KEY this member's route
                // published on the target union's member (B3.1b: named by
                // this member's set and the property, so each source
                // member pairs with its own target member)
                ValueSpecification pairedEntry = cond;
                if (j.targetSetId() != null && targetUnion != null
                        && targetUnion.memberSetIds().contains(j.targetSetId())) {
                    // a member's own routes of this property: their shapes
                    // (one member set navigates with its own set id, so a
                    // member's routes rarely differ in shape)
                    String navSet = navigatingIdentity(md, members.get(memberOrd), prop, model);
                    ValueSpecification shapeCond = lastHopCondition(j, srcTable, md, model, s, t);
                    int shape = shapeCond == null ? -1
                            : shapeIndex(memberRouteShapes(members.get(memberOrd), prop,
                                    srcTable, md, model, s, t), shapeCond, s, t);
                    int[] pos = {0};
                    pairedEntry = rewriteTargetReads(cond, t, col -> {
                        String name = linkKeyName(navSet, prop, shape, pos[0]++);
                        tgtKeyCols.put(name, new String[]{col, hopDb, tgtTable});
                        if (liftTargetMerged) {
                            // the MERGED predicate keeps the RAW read (a
                            // projected value column of the target): it
                            // types through the shim beside the key
                            tgtKeyCols.putIfAbsent(col, new String[]{col, hopDb, tgtTable});
                        }
                        return new AppliedProperty(t, name);
                    });
                }
                if (!liftTargetMerged) {
                    cond = pairedEntry;
                }
                orCond = orDistinct(orCond, cond);
                orPaired = orDistinct(orPaired, pairedEntry);
            }
            // members reading the SAME source column(s) against one target
            // expression contribute ONE disjunct: coalesce over their
            // member-suffixed reads (at most one is non-null per row, equal
            // when several are) — an indexable probe, not a k-way OR
            if (allSingleHop && liftTargetMerged
                    && sameSource.values().stream().anyMatch(o -> o.size() > 1)) {
                orCond = mergeSameSource(sameSource, s);
            }
            LambdaFunction pairedLam = liftTargetMerged && orPaired != null
                    && orPaired != orCond
                    ? new LambdaFunction(List.of(s, t), List.of(orPaired))
                    : null;
            // the typing arg: the landing table's own row, plus the link
            // keys the paired predicate reads (a typing shim: each key's
            // kind from its own landing table; no semantics)
            ValueSpecification targetRows = ViewRelation.relationExpr(
                    java.util.Objects.requireNonNull(landingDb), java.util.Objects.requireNonNull(landingTable), model, md);
            if (!tgtKeyCols.isEmpty()) {
                targetRows = new AppliedFunction("project", List.of(targetRows,
                        new ColSpecArray(linkKeySpecs(tgtKeyCols, model, md.qualifiedName()))));
            }
            lifts.add(new NavLift(prop, targetClassFqn, targetRows,
                    new LambdaFunction(List.of(s, t), List.of(orCond)),
                    pairedLam, srcKeys, chains));
        }
        return lifts;
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

    /** The LINK KEYS of every relational set in the mapping's closure
     * (B3.1b): routed into by class PMs or association pair entries,
     * each set publishes the columns those routes read under the key
     * names the navigators spell — stamped on the ledger as a fact. */
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

    static void publishLinkKeys(ResolvedMapping md,
            Map<String, ResolvedMapping> resolved, ModelBuilder model, MappingLedger ledger) {
        List<LegacyMappingDefinition> records = prePassedClosure(md, resolved);
        List<ClassMapping> sets = new ArrayList<>();
        for (LegacyMappingDefinition m : records) {
            for (ClassMapping cm : m.classMappings()) {
                if (cm instanceof ClassMapping.Relational) {
                    sets.add(cm);
                }
            }
        }
        collectInboundRouteKeys(md, records, model, sets.stream().map(ResolvedMapping::idOf).toList(),
                sets, new LinkedHashMap<>(), null, ledger);
        // a set that EXTENDS another is that set's rows too: a route into
        // the parent reaches the child (the engine resolves routes through
        // the extends chain) — the child publishes the parent's keys, its
        // own entries winning
        for (ClassMapping cm : sets) {
                if (!(cm instanceof ClassMapping.Relational r) || r.extendsSetId() == null) {
                continue;
            }
            Map<String, String> mine = ledger.linkKeys.computeIfAbsent(
                    ResolvedMapping.idOf(cm), k -> new LinkedHashMap<>());
            Set<String> seen = new HashSet<>();
            ClassMapping parent = md.set(r.extendsSetId());
            while (parent instanceof ClassMapping.Relational pr && seen.add(ResolvedMapping.idOf(pr))) {
                Map<String, String> theirs = ledger.linkKeys.get(ResolvedMapping.idOf(pr));
                if (theirs != null) {
                    theirs.forEach(mine::putIfAbsent);
                }
                parent = pr.extendsSetId() == null ? null : md.set(pr.extendsSetId());
            }
        }
    }

    /**
     * Scan the mapping closure (own + includes) for routed Join PMs (class
     * PMs and per-pair association entries) whose target set is one of
     * this union's members. A SINGLE-HOP route registers the member's LINK
     * KEY: the columns of the member's table its Join reads, published
     * under {@link #linkKeyName} (ordinal &rarr; physical column &rarr;
     * name, into {@code sink}; the (set, name, column) fact onto the
     * ledger for the resolver's mixed-union arms). A CHAINED per-arm route
     * pushes its mid hops into the owning member's thread and projects
     * its property-scoped chain keys there (B3.2 moves these).
     */
    static void collectInboundRouteKeys(ResolvedMapping md,
            List<LegacyMappingDefinition> records,
            ModelBuilder model, List<String> memberIds,
            List<ClassMapping> members,
            Map<Integer, Map<String, String>> sink,
            @com.legend.Nullable Map<Integer, List<LiftChain>> chainsSink,
            MappingLedger ledger) {
        for (LegacyMappingDefinition m : records) {
            for (ClassMapping cm : m.classMappings()) {
                if (!(cm instanceof ClassMapping.Relational rcm)) {
                    continue;
                }
                Map<String, List<PropertyMapping.Join>> byProp =
                        new LinkedHashMap<>();
                for (PropertyMapping.Join j : routedJoins(rcm)) {
                    byProp.computeIfAbsent(j.propertyName(), k -> new ArrayList<>()).add(j);
                }
                LegacyMappingDefinition.TableReference navMain = rcm.mainTable() != null
                        ? rcm.mainTable() : MappingNormalizer.inferMainTableQuiet(rcm);
                for (var bpe : byProp.entrySet()) {
                    List<PropertyMapping.Join> group = bpe.getValue();
                    String navSet = navigatingIdentity(md, rcm, bpe.getKey(), model);
                    List<Map.Entry<PropertyMapping.Join, Integer>> ords = new ArrayList<>();
                    for (PropertyMapping.Join j : group) {
                        int ord = md.memberOrdinal(memberIds, j.targetSetId());
                        if (ord >= 0) {
                            ords.add(Map.entry(j, ord));
                        }
                    }
                    boolean uniform = uniformChainedRoutes(
                            ords.stream().map(Map.Entry::getKey).toList());
                    registerInboundGroup(ords, navSet, navMain == null ? null : navMain.table(),
                            members, uniform, md, model, sink, chainsSink, ledger);
                }
            }
            // per-pair ASSOCIATION entries route INTO this union too: the
            // navigating set is the pair's SOURCE set
            for (AssociationMapping am : m.associationMappings()) {
                if (!(am instanceof AssociationMapping.Relational rel)) {
                    continue;
                }
                // one entry per PAIR (several pairs of one source set share
                // one Join body: the target set is the pair's, not the body's)
                Map<String, List<Map.Entry<PropertyMapping.Join, Integer>>> byProp =
                        new LinkedHashMap<>();
                Map<String, String> sourceSetByKey = new LinkedHashMap<>();
                for (AssociationPropertyMapping apm : rel.propertyMappings()) {
                    if (!(apm.body() instanceof PropertyMapping.Join j)) {
                        continue;
                    }
                    String tgtSet = j.targetSetId() != null
                            ? j.targetSetId() : apm.targetSetId();
                    if (tgtSet == null) {
                        continue;
                    }
                    int ord = md.memberOrdinal(memberIds, tgtSet);
                    if (ord < 0) {
                        continue;
                    }
                    String key = apm.sourceSetId() + "\u0000" + apm.propertyName();
                    byProp.computeIfAbsent(key, k -> new ArrayList<>()).add(Map.entry(j, ord));
                    sourceSetByKey.put(key, apm.sourceSetId());
                }
                for (var bp : byProp.entrySet()) {
                    List<Map.Entry<PropertyMapping.Join, Integer>> ords = bp.getValue();
                    boolean uniform = uniformChainedRoutes(
                            ords.stream().map(Map.Entry::getKey).toList());
                    String srcSetId = java.util.Objects.requireNonNull(sourceSetByKey.get(bp.getKey()));
                    ClassMapping srcCm = md.set(srcSetId);
                    String prop = bp.getKey().substring(bp.getKey().indexOf('\u0000') + 1);
                    String srcSet = srcCm == null ? srcSetId : navigatingIdentity(md, srcCm, prop, model);
                    String srcTable = srcCm instanceof ClassMapping.Relational sr
                            ? (sr.mainTable() != null ? sr.mainTable().table()
                                    : java.util.Optional.ofNullable(MappingNormalizer.inferMainTableQuiet(sr))
                                            .map(LegacyMappingDefinition.TableReference::table).orElse(null))
                            : null;
                    registerInboundGroup(ords, srcSet, srcTable,
                            members, uniform, md, model, sink, chainsSink, ledger);
                }
            }
        }
    }

    /** One navigating set's routes of one property into this union: the
     * shapes over the whole group first (the key names carry the shape
     * index exactly as the navigator spells them), then each entry. */
    private static void registerInboundGroup(List<Map.Entry<PropertyMapping.Join, Integer>> ords,
            String navigatingSet, @com.legend.Nullable String navigatingTable,
            List<ClassMapping> members, boolean uniform,
            ResolvedMapping md, ModelBuilder model,
            Map<Integer, Map<String, String>> sink,
            @com.legend.Nullable Map<Integer, List<LiftChain>> chainsSink, MappingLedger ledger) {
        Variable s = new Variable("s");
        Variable t = new Variable("t");
        List<ValueSpecification> conds = new ArrayList<>();
        for (var en : ords) {
            ValueSpecification c = routeKeyCondition(en.getKey(), uniform, navigatingTable,
                    md, model, s, t);
            if (c != null) {
                conds.add(c);
            }
        }
        List<ValueSpecification> shapes = routeShapes(conds, s, t);
        for (var en : ords) {
            registerInboundEntry(en.getKey(), en.getValue(), navigatingSet, navigatingTable,
                    shapes, members, uniform, md, model, sink, chainsSink, ledger);
        }
    }

    /**
     * One inbound routed entry. SINGLE-HOP (and shared-prefix chained,
     * whose prefix the navigator emits as physical joins): the member's
     * link key columns are the target reads of the route's LAST hop
     * translated exactly as the navigator translates it, in the same
     * order, named by position. PER-ARM chained: the mid hops push into
     * the member's thread with property-scoped chain keys.
     */
    private static void registerInboundEntry(PropertyMapping.Join j, int ord,
            String navigatingSet, @com.legend.Nullable String navigatingTable,
            List<ValueSpecification> shapes,
            List<ClassMapping> members, boolean uniform,
            ResolvedMapping md, ModelBuilder model,
            Map<Integer, Map<String, String>> sink,
            @com.legend.Nullable Map<Integer, List<LiftChain>> chainsSink, MappingLedger ledger) {
        if (!(members.get(ord) instanceof ClassMapping.Relational routedMember)) {
            return;     // routes into Relation(~func) members have no
                        // physical key table (loud at navigation if demanded)
        }
        LegacyMappingDefinition.TableReference memberMain = routedMember.mainTable() != null
                ? routedMember.mainTable() : MappingNormalizer.inferMainTableQuiet(routedMember);
        if (memberMain == null) {
            return;     // a set with no table of its own: loud at navigation
        }
        String memberTable = memberMain.table();
        if (!uniform && j.joins().size() > 1) {
            Variable s = new Variable("s");
            Variable t = new Variable("t");
            ValueSpecification first = hopCondition(j, 0, navigatingTable, md, model, s, t);
            if (first == null) {
                return;     // loud at the route's own emission
            }
            // the arm's key: the first mid's columns the navigator's first
            // hop reads, published under the link key's name (same shape
            // list, same positions as the navigator spells them). The fact
            // names the MID's column: the member's thread reads it off the
            // mid slot it carries (an includer whose closure adds such a
            // route re-binds the union by this fact, like any key)
            int shape = shapeIndex(shapes, first, s, t);
            List<String> reads = new ArrayList<>();
            collectTargetReads(first, t, reads);
            Map<String, String> keys = new LinkedHashMap<>();
            Map<String, String> mine = sink.computeIfAbsent(ord, k -> new LinkedHashMap<>());
            Map<String, String> facts = ledger.linkKeys.computeIfAbsent(
                    ResolvedMapping.idOf(routedMember), k -> new LinkedHashMap<>());
            for (int k = 0; k < reads.size(); k++) {
                String name = linkKeyName(navigatingSet, j.propertyName(), shape, k);
                keys.put(reads.get(k), name);
                facts.putIfAbsent(name, reads.get(k));
                mine.putIfAbsent(name, reads.get(k));
            }
            if (chainsSink == null) {
                return;     // the key publication: the mids ride the union's own scan
            }
            List<LiftMidStep> steps = inboundArmSteps(j, j.propertyName(),
                    memberTable, md, model);
            LiftMidStep landing = steps.get(steps.size() - 1);
            List<LiftChain> have = chainsSink.computeIfAbsent(ord,
                    k -> new ArrayList<>());
            boolean dup = have.stream().anyMatch(ch -> ch.keys().values()
                    .stream().anyMatch(keys.values()::contains));
            if (!dup && !keys.isEmpty()) {
                have.add(new LiftChain(steps, landing.alias(), landing.db(),
                        landing.table(), keys));
            }
            return;
        }
        if (chainsSink != null) {
            return;     // the union's own scan registers chains only; keys are published
        }
        Variable s = new Variable("s");
        Variable t = new Variable("t");
        ValueSpecification cond = lastHopCondition(j, navigatingTable, md, model, s, t);
        if (cond == null) {
            return;     // loud at the route's own emission
        }
        int shape = shapeIndex(shapes, cond, s, t);
        List<String> reads = new ArrayList<>();
        collectTargetReads(cond, t, reads);
        Map<String, String> mine = sink.computeIfAbsent(ord, k -> new LinkedHashMap<>());
        Map<String, String> facts = ledger.linkKeys.computeIfAbsent(
                ResolvedMapping.idOf(routedMember), k -> new LinkedHashMap<>());
        for (int k = 0; k < reads.size(); k++) {
            String name = linkKeyName(navigatingSet, j.propertyName(), shape, k);
            String prev = facts.put(name, reads.get(k));
            if (prev != null && !prev.equals(reads.get(k))) {
                throw new NotImplementedException("member set '" + ResolvedMapping.idOf(routedMember)
                        + "' is routed to by '" + navigatingSet + "." + j.propertyName()
                        + "' through two joins reading different columns (" + prev + ", "
                        + reads.get(k) + "); mapping=" + md.qualifiedName());
            }
            mine.put(name, reads.get(k));
        }
    }

    /**
     * IMPORT DATA FLOW (engine {@code pureToSQLQuery_union.pure:140–150},
     * {@code resolvePrimaryKey} functions.pure:190): every member thread
     * projects its set's PRIMARY KEY — the declared {@code ~primaryKey}
     * columns of the main table, else the table's PRIMARY KEY — as
     * {@code <col>_<ordinal>} (NULL in the other members' threads), the
     * union's row identity across members. The projection rides the same
     * per-ordinal key map the routed navigations use (a key a route already
     * demanded is one column, not two; a SHARED table key is projected once
     * as {@code <col>__pk_<table>} and is not doubled here). The
     * {@code (name, kind)} facts are recorded on the model for the execute
     * option that surfaces the threads as result columns
     * ({@code ModelContext.unionKeyThreads}).
     */
    private static void recordKeyThreads(ResolvedMapping md, String className,
            List<ClassMapping> members, Map<Integer, Map<String, String>> srcKeysByOrdinal,
            Map<List<String>, Integer> sharedKeys, ModelBuilder model,
            MappingLedger ledger) {
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
                String name = col + "_" + o;
                srcKeysByOrdinal.computeIfAbsent(o, k -> new LinkedHashMap<>())
                        .putIfAbsent(name, col);
                threads.add(new com.legend.model.KeyThread(name,
                        model.knowledge().columnKind(db, table, col)));
            }
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
        DatabaseDefinition.TableDefinition td = model.knowledge().table(main.database(), main.table()).orElse(null);
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

    /** The lift predicate rebuilt from its raw-condition groups: one
     * disjunct per group, same-source members coalesced. */
    private static ValueSpecification mergeSameSource(
            Map<ValueSpecification, LinkedHashSet<Integer>> sameSource, Variable s) {
        ValueSpecification rebuilt = null;
        for (var g : sameSource.entrySet()) {
            List<Integer> os = new ArrayList<>(g.getValue());
            Map<String, String> ignore = new LinkedHashMap<>();
            rebuilt = orDistinct(rebuilt, os.size() == 1
                    ? suffixTargetReads(g.getKey(), s, os.get(0), ignore)
                    : coalesceReads(g.getKey(), s, os, ignore));
        }
        return java.util.Objects.requireNonNull(rebuilt);
    }

    /** Nested {@code coalesce} over a member's suffixed reads of one
     * source column — the ONE key expression of members sharing a physical
     * column (at most one is non-null per row; equal when several are). */
    static ValueSpecification coalesceReads(ValueSpecification n, Variable s,
            List<Integer> ordinals, Map<String, String> out) {
        if (n instanceof AppliedProperty ap && ap.receiver() instanceof Variable v
                && v.name().equals(s.name())) {
            ValueSpecification acc = null;
            for (int i = ordinals.size() - 1; i >= 0; i--) {
                String suffixed = ap.property() + "_" + ordinals.get(i);
                out.put(ap.property(), suffixed);
                ValueSpecification read = new AppliedProperty(v, suffixed);
                acc = acc == null ? read
                        : new AppliedFunction("coalesce", List.of(read, acc));
            }
            return java.util.Objects.requireNonNull(acc);
        }
        return switch (n) {
            case AppliedFunction af -> af.withParameters(af.parameters().stream()
                    .map(x -> coalesceReads(x, s, ordinals, out)).toList());
            case AppliedProperty ap -> new AppliedProperty(
                    coalesceReads(ap.receiver(), s, ordinals, out), ap.property());
            case PureCollection pc -> new PureCollection(pc.values().stream()
                    .map(x -> coalesceReads(x, s, ordinals, out)).toList());
            default -> n;
        };
    }

    /** {@code acc or cond}, skipping a disjunct structurally equal to one
     * already present (members sharing a table key contribute IDENTICAL
     * conditions — one equality, indexable). */
    static ValueSpecification orDistinct(@com.legend.Nullable ValueSpecification acc,
            ValueSpecification cond) {
        if (acc == null) {
            return cond;
        }
        ArrayDeque<ValueSpecification> stack = new ArrayDeque<>();
        stack.push(acc);
        while (!stack.isEmpty()) {
            ValueSpecification d = stack.pop();
            if (d instanceof AppliedFunction f && "or".equals(f.function())
                    && f.parameters().size() == 2) {
                stack.push(f.parameters().get(0));
                stack.push(f.parameters().get(1));
            } else if (d.equals(cond)) {
                return acc;
            }
        }
        return new AppliedFunction("or", List.of(acc, cond));
    }
}
