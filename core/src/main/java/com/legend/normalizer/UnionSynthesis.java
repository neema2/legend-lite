// SPDX-License-Identifier: Apache-2.0

package com.legend.normalizer;

import com.legend.compiler.ModelBuilder;
import com.legend.compiler.SynthFqn;
import com.legend.error.LegendCompileException;
import com.legend.error.ModelException;
import com.legend.error.NotImplementedException;
import com.legend.model.Multiplicity;
import com.legend.model.NormalizedModel;
import com.legend.model.ParsedModel;
import com.legend.model.TypeExpression;
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
import com.legend.model.Realization;
import com.legend.model.RelationalDataType;
import com.legend.model.RelationalOperation;
import com.legend.model.SynthHat;
import com.legend.model.spec.AppliedFunction;
import com.legend.model.spec.AppliedProperty;
import com.legend.model.spec.CBoolean;
import com.legend.model.spec.CFloat;
import com.legend.model.spec.CInteger;
import com.legend.model.spec.CString;
import com.legend.model.spec.ColSpec;
import com.legend.model.spec.ColSpecArray;
import com.legend.model.spec.EnumValue;
import com.legend.model.spec.KeyExpression;
import com.legend.model.spec.LambdaFunction;
import com.legend.model.spec.NewInstance;
import com.legend.model.spec.NewInstanceCast;
import com.legend.model.spec.PackageableElementPtr;
import com.legend.model.spec.PureCollection;
import com.legend.model.spec.TypeAnnotation;
import com.legend.model.spec.ValueSpecification;
import com.legend.model.spec.Variable;
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

    /**
     * The Union operation mapping for {@code classFqn} in {@code md} or its
     * includes (own wins), or {@code null}. Member ORDINAL order = the
     * union's declaration order = the synthesized concatenate's thread
     * order = the engine's {@code _N} key suffix.
     */
    static ClassMapping.Union unionForClass(LegacyMappingDefinition md,
            ModelBuilder model, String classFqn) {
        for (ClassMapping cm : md.classMappings()) {
            if (cm instanceof ClassMapping.Union u
                    && u.className().equals(classFqn)) {
                return u;
            }
        }
        for (MappingInclude inc : md.includes()) {
            LegacyMappingDefinition inner =
                    model.findLegacyMapping(inc.mappingPath()).orElse(null);
            if (inner != null) {
                ClassMapping.Union u = unionForClass(inner, model, classFqn);
                if (u != null) {
                    return u;
                }
            }
        }
        return null;
    }

    /** One routed navigation entry: the target's union-member ORDINAL
     * (declaration order = concatenate thread order = the engine's
     * {@code _N} suffix) and the entry's own join. Ordinal {@code -1}
     * marks a root/sole-set route (the un-routed navigation). */
    record UnionRoute(int targetOrdinal, PropertyMapping.Join join) {
    }

    /** Extends-merge identity: (property name, route) — per-set duplicates
     * of a routed property are distinct mappings. */
    static String pmIdentity(PropertyMapping pm) {
        return pm.propertyName() + ' '
                + (pm instanceof PropertyMapping.Join j
                        && j.targetSetId() != null ? j.targetSetId() : "");
    }

    /**
     * The ordinal of the member whose set id OR extends-LINEAGE matches
     * {@code setId}: a re-rooted union ({@code Person[mySet1] extends
     * [set1]}) is navigated by routes naming the ANCESTOR sets — the
     * corpus extends-of-union shape. {@code -1} when no member matches.
     */
    /**
     * Whether the property's routes need NO target-side discrimination:
     * they cover EVERY member of the target union. The engine then joins
     * on the MERGED (unsuffixed) target key — both threads project it, so
     * every member matches (snapshot-union AND partially-milestoning
     * goldens: prodFk_0 = id OR prodFk_1 = id, target side shared even
     * with different joins per member). Partial coverage keeps the
     * member-suffixed NULL-crossed form (un-routed members must not
     * match — the TDSNull golden).
     */
    static boolean mergedTargetRoutes(List<UnionRoute> routes,
            ClassMapping.Union targetUnion) {
        if (targetUnion == null) {
            return false;
        }
        // Merge (single un-suffixed condition) ONLY when every route names
        // the SAME join — then the one condition IS every entry's condition
        // (audit 12: coverage alone dropped the second route's join
        // entirely; J1(FK1=ID)+J2(FK2=ID) matched member 2 by the wrong
        // key). Distinct joins keep the suffixed OR.
        Set<Integer> ords = new HashSet<>();
        Set<String> joins = new HashSet<>();
        for (UnionRoute r : routes) {
            if (r.targetOrdinal() < 0 || r.join().joins().size() != 1) {
                return false;
            }
            ords.add(r.targetOrdinal());
            JoinChainElement hop = r.join().joins().get(0);
            joins.add((hop.databaseName() != null ? hop.databaseName()
                    : r.join().database()) + "@" + hop.joinName());
        }
        return ords.size() == targetUnion.memberSetIds().size()
                && joins.size() == 1;
    }

    static int memberOrdinalOf(List<String> memberIds,
            LegacyMappingDefinition md, ModelBuilder model, String setId) {
        int direct = memberIds.indexOf(setId);
        if (direct >= 0) {
            return direct;
        }
        for (int i = 0; i < memberIds.size(); i++) {
            ClassMapping m = MappingNormalizer.findSetById(md, model, memberIds.get(i));
            Set<String> seen = new HashSet<>();
            while (m instanceof ClassMapping.Relational r
                    && r.extendsSetId() != null && seen.add(r.extendsSetId())) {
                if (r.extendsSetId().equals(setId)) {
                    return i;
                }
                m = MappingNormalizer.findSetById(md, model, r.extendsSetId());
            }
        }
        return -1;
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
    static void classifyUnionRoutes(LegacyMappingDefinition md,
            ClassMapping.Relational rcm, ModelBuilder model, Pipeline p) {
        Map<String, List<PropertyMapping.Join>> routedByProp = new LinkedHashMap<>();
        for (PropertyMapping pm : rcm.propertyMappings()) {
            if (pm instanceof PropertyMapping.Join j && j.targetSetId() != null) {
                routedByProp.computeIfAbsent(j.propertyName(),
                        k -> new ArrayList<>()).add(j);
            }
        }
        for (var e : routedByProp.entrySet()) {
            String prop = e.getKey();
            ClassDefinition owner = model.findClass(rcm.className()).orElse(null);
            TypeExpression pt = owner == null ? null
                    : MappingNormalizer.findPropertyTypeDeep(owner, prop, model);
            String targetClass = pt instanceof TypeExpression.NameRef nr
                    && model.findClass(nr.name()).isPresent() ? nr.name() : null;
            ClassMapping.Union tu = targetClass == null ? null
                    : unionForClass(md, model, targetClass);
            // FIX-A (audit-17 bucket analysis): an INHERITANCE op is a
            // union at the routing level — member set ids in the shared
            // enumeration's order
            List<String> memberIds = tu != null ? tu.memberSetIds() : null;
            if (memberIds == null && targetClass != null) {
                ClassMapping.Inheritance tih =
                        inheritanceForClass(md, model, targetClass);
                if (tih != null) {
                    memberIds = inheritanceMembers(md, tih, model).stream()
                            .map(MappingNormalizer::setIdOf).toList();
                }
            }
            List<UnionRoute> routes = new ArrayList<>();
            String poison = null;
            for (PropertyMapping.Join j : e.getValue()) {
                ClassMapping set = MappingNormalizer.findSetById(md, model, j.targetSetId());
                if (set == null) {
                    poison = "unknown mapping set '" + j.targetSetId() + "'";
                    break;
                }
                int ord = memberIds == null ? -1
                        : memberOrdinalOf(memberIds, md, model,
                                j.targetSetId());
                // engine rootClassMappingByClass: the * set, or the class's
                // SOLE set (sole-ness judged in the OWNING mapping's scope)
                boolean rootOrSole = set instanceof ClassMapping.Relational tr
                        && (tr.root() || md.classMappings().stream()
                                .filter(x -> x.className().equals(tr.className()))
                                .count() == 1);
                if (ord >= 0) {
                    if (j.joins().size() != 1) {
                        poison = "union member set '" + j.targetSetId()
                                + "' via a CHAINED join — per-member chained"
                                + " joins are not supported yet";
                        break;
                    }
                    routes.add(new UnionRoute(ord, j));
                } else if (rootOrSole) {
                    routes.add(new UnionRoute(-1, j));
                } else {
                    poison = "NON-root mapping set '" + j.targetSetId()
                            + "' — multi-set dispatch outside union members"
                            + " is a roadmap feature";
                    break;
                }
            }
            if (poison == null && routes.stream()
                    .anyMatch(r -> r.targetOrdinal() >= 0)
                    && routes.stream().anyMatch(r -> r.targetOrdinal() < 0)) {
                poison = "MIXED root-set and union-member routes";
            }
            if (poison != null) {
                p.droppedRoutedProps.add(prop);
                model.mappingPoisons.merge(
                        md.qualifiedName() + "::" + rcm.className(),
                        "property '" + prop + "' routes to " + poison
                                + "; the property is dropped from this synthesis",
                        (a, b) -> a + "; " + b);
                continue;
            }
            if (routes.stream().allMatch(r -> r.targetOrdinal() < 0)) {
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
    static ValueSpecification synthUnion(LegacyMappingDefinition md,
                                                ClassMapping.Union u,
                                                ModelBuilder model) {
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
        MappingNormalizer.collectIncludedSetIds(md, model, bySetId, new HashSet<>());
        for (ClassMapping cm : md.classMappings()) {
            bySetId.put(MappingNormalizer.setIdOf(cm), cm);
        }
        List<ClassMapping> memberSets = new ArrayList<>();
        for (String setId : u.memberSetIds()) {
            ClassMapping member = bySetId.get(setId);
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
            if (!isSubclassOf(member.className(), u.className(), model)) {
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
        AssociationSynthesis.collectPairAssociationEntries(md, model, u.className(), pairEntries,
                new HashSet<>());
        if (!pairEntries.isEmpty()) {
            for (int i = 0; i < memberSets.size(); i++) {
                if (!(memberSets.get(i) instanceof ClassMapping.Relational mr)) {
                    continue;
                }
                List<PropertyMapping.Join> add = pairEntries.get(MappingNormalizer.setIdOf(mr));
                if (add == null) {
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
                        pms, mr.sourceUrl(), mr.propertyTargetSets()));
            }
        }
        return synthMemberUnion(md, u.className(), memberSets, model);
    }

    /**
     * Inheritance Operation: the extent is the UNION of every Relational
     * set mapped for the class's SUBCLASSES (transitively; nested
     * union/inheritance operations expand to their concrete members).
     * Queries on the base class can only touch base-class-typed properties,
     * so the shared-property projection over the base owner is exactly the
     * engine's router semantics.
     */
    static ValueSpecification synthInheritance(LegacyMappingDefinition md,
            ClassMapping.Inheritance ih, ModelBuilder model) {
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
            return MappingNormalizer.synthRelational(md, members.get(0), model);
        }
        return synthMemberUnion(md, ih.className(), members, model);
    }

    /**
     * The ORDERED member Relational sets of an inheritance op — ONE
     * enumeration shared by {@link #synthInheritance} (concatenate thread
     * order) and route classification (ordinal computation), so the
     * ordinals align BY CONSTRUCTION (misalignment = silently wrong rows).
     */
    static List<ClassMapping.Relational> inheritanceMembers(
            LegacyMappingDefinition md, ClassMapping.Inheritance ih,
            ModelBuilder model) {
        LinkedHashSet<ClassMapping> chosen = new LinkedHashSet<>();
        collectInheritanceMembers(md, ih.className(), model, chosen);
        List<ClassMapping.Relational> members = new ArrayList<>();
        for (ClassMapping cm : chosen) {
            switch (cm) {
                case ClassMapping.Relational mr -> members.add(mr);
                case ClassMapping.Union u2 -> {
                    Map<String, ClassMapping> bySetId = new LinkedHashMap<>();
                    MappingNormalizer.collectIncludedSetIds(md, model, bySetId, new HashSet<>());
                    for (ClassMapping own : md.classMappings()) {
                        bySetId.put(MappingNormalizer.setIdOf(own), own);
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

    /** The inheritance op mapping a class, own then includes — the
     * Inheritance sibling of {@link #unionForClass}. */
    static ClassMapping.Inheritance inheritanceForClass(LegacyMappingDefinition md,
            ModelBuilder model, String classFqn) {
        for (ClassMapping cm : md.classMappings()) {
            if (cm instanceof ClassMapping.Inheritance ih
                    && ih.className().equals(classFqn)) {
                return ih;
            }
        }
        for (MappingInclude inc : md.includes()) {
            LegacyMappingDefinition inner =
                    model.findLegacyMapping(inc.mappingPath()).orElse(null);
            if (inner != null) {
                ClassMapping.Inheritance ih = inheritanceForClass(inner, model, classFqn);
                if (ih != null) {
                    return ih;
                }
            }
        }
        return null;
    }

    /** The engine's leaf-most-root member selection for an inheritance op. */
    static void collectInheritanceMembers(LegacyMappingDefinition md,
            String base, ModelBuilder model, Set<ClassMapping> chosen) {
        // ROOT class mapping per class, includes first (own definitions win)
        Map<String, ClassMapping> rootByClass = new LinkedHashMap<>();
        collectRootClassMappings(md, model, rootByClass, new HashSet<>());
        // strict specializations of base, and their leaves
        List<String> subs = new ArrayList<>();
        model.classes().forEach(cd -> {
            String fqn = cd.qualifiedName();
            if (!fqn.equals(base) && isSubclassOf(fqn, base, model)) {
                subs.add(fqn);
            }
        });
        List<String> leaves = subs.stream()
                .filter(c -> subs.stream().noneMatch(o -> !o.equals(c)
                        && isSubclassOf(o, c, model)))
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
                    ClassMapping cm = rootByClass.get(c);
                    if (cm != null) {
                        if (cm instanceof ClassMapping.Inheritance) {
                            collectInheritanceMembers(md, c, model, chosen);
                        } else {
                            chosen.add(cm);
                        }
                        break outer;
                    }
                    ClassDefinition cd = model.findClass(c).orElse(null);
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

    /** ROOT set per class across this mapping + its includes (own wins). */
    static void collectRootClassMappings(LegacyMappingDefinition md,
            ModelBuilder model, Map<String, ClassMapping> out, Set<String> seen) {
        for (MappingInclude inc : md.includes()) {
            if (seen.add(inc.mappingPath())) {
                LegacyMappingDefinition included =
                        model.findLegacyMapping(inc.mappingPath()).orElse(null);
                if (included != null) {
                    collectRootClassMappings(included, model, out, seen);
                }
            }
        }
        Map<String, Integer> setsPerClass = new LinkedHashMap<>();
        for (ClassMapping cm : md.classMappings()) {
            setsPerClass.merge(cm.className(), 1, Integer::sum);
        }
        for (ClassMapping cm : md.classMappings()) {
            // engine rootClassMappingByClass: the * set, or the class's
            // SOLE set (corpus mappings often omit * on singletons)
            if (cm.root() || setsPerClass.get(cm.className()) == 1) {
                out.put(cm.className(), cm);
            }
        }
    }

    /** {@code candidate} equals {@code base} or transitively extends it. */
    static boolean isSubclassOf(String candidate, String base, ModelBuilder model) {
        if (candidate.equals(base)) {
            return true;
        }
        ClassDefinition cd = model.findClass(candidate).orElse(null);
        if (cd == null) {
            return false;
        }
        for (TypeExpression sup : cd.superClasses()) {
            if (sup instanceof TypeExpression.NameRef nr
                    && isSubclassOf(nr.name(), base, model)) {
                return true;
            }
        }
        return false;
    }

    /** The shared-property UNION ALL over resolved member sets. */
    static ValueSpecification synthMemberUnion(LegacyMappingDefinition md,
            String className, List<? extends ClassMapping> memberSets,
            ModelBuilder model) {
        List<MappingNormalizer.RelationalParts> parts = new ArrayList<>(memberSets.size());
        List<ClassMapping> members = new ArrayList<>(memberSets.size());
        for (ClassMapping cmIn : memberSets) {
            if (cmIn instanceof ClassMapping.RelationFunction rfm) {
                // Relation (~func) member: the parts are the inlined
                // relation body + its column reads — no main table, no
                // nav lifting (scalar columns only)
                Variable rfRow = new Variable("rf_row");
                Map<String, KeyExpression> rfFields = new LinkedHashMap<>();
                for (ClassMapping.RelationFunction.Col c : rfm.columns()) {
                    if (c.local()) {
                        continue;
                    }
                    if (c.column() == null) {
                        throw new com.legend.error.NotImplementedException(
                                "union member '" + rfm.className() + "': relation"
                                        + " column mapping for property '"
                                        + c.property() + "' has no source column"
                                        + " (unrecognized binding shape)");
                    }
                    ValueSpecification read = new AppliedProperty(rfRow, c.column());
                    if (c.enumMappingId() != null) {
                        read = MappingNormalizer.translateEnumeratedSource(c.property(),
                                c.enumMappingId(), read, md, rfm.className(), model);
                    }
                    rfFields.put(c.property(), new KeyExpression(read, false, false));
                }
                members.add(rfm);
                parts.add(new MappingNormalizer.RelationalParts(
                        MappingNormalizer.relationFunctionPipeline(rfm, model), rfRow, rfFields));
                continue;
            }
            ClassMapping.Relational mr = (ClassMapping.Relational) cmIn;
            String setId = MappingNormalizer.setIdOf(mr);
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
                        mr.propertyTargetSets());
            }
            if (model.findView(mr.mainTable().database(),
                    mr.mainTable().table()).isPresent()) {
                throw new NotImplementedException(
                        "Operation union over a VIEW-backed member set is not"
                      + " supported yet; mapping=" + md.qualifiedName());
            }
            members.add(mr);
            parts.add(MappingNormalizer.synthTableBackedParts(md, mr, model, null));
        }
        // the UNION of the members' scalar property sets, first-appearance
        // order — a member that does not map a property contributes a typed
        // NULL in its thread (engine: 'null as ...' / __SQLNULL__ columns;
        // partial-union reads come back TDSNull, testUnionPartial goldens)
        ClassDefinition owner = model.findClass(className).orElse(null);
        List<String> common = new ArrayList<>();
        for (MappingNormalizer.RelationalParts pp : parts) {
            for (String prop : pp.fields().keySet()) {
                TypeExpression t = owner == null ? null
                        : MappingNormalizer.findPropertyTypeDeep(owner, prop, model);
                boolean scalar = t instanceof TypeExpression.NameRef nr
                        && model.findClass(nr.name()).isEmpty();
                if (scalar && !common.contains(prop)) {
                    common.add(prop);
                }
            }
        }
        if (common.isEmpty()) {
            throw new NotImplementedException(
                    "Operation union members of '" + className
                  + "' map no scalar properties; mapping=" + md.qualifiedName());
        }
        // ==== NAV LIFT (engine union model): the members' class-typed
        // single-hop Join PMs lift to ONE legacyNavigate ON THE UNION —
        // member i's thread carries its join keys member-suffixed
        // (<col>_<i>, NULL in the other threads) and the navigate condition
        // ORs the per-entry conditions (target side suffixed too when the
        // entry routes to a union member of the TARGET class). Downstream,
        // the union class then looks like any nav-slot class.
        List<NavLift> lifts = collectNavLifts(md, className, members, model);
        // ordinal -> (base column -> suffixed name): the source keys each
        // member thread projects (its own reads; typed NULL elsewhere)
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
        // keys demanded by EXTERNAL routed navigations INTO this union
        // (audit 11: the union body carries every routed key with full
        // PROVENANCE — the resolver must never re-derive key meaning from
        // column-name patterns, a real column spelled like a suffix hijacked
        // the NULL thread)
        collectInboundRouteKeys(md, model,
                members.stream().map(MappingNormalizer::setIdOf).toList(),
                members, srcKeysByOrdinal);
        ValueSpecification union = null;
        int ordinal = -1;
        for (MappingNormalizer.RelationalParts pp : parts) {
            ordinal++;
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
                        : MappingNormalizer.coerceToDeclaredNumeric(
                                mapped.value(), prop, className, model);
                // String is safe INSIDE the union projection: the members
                // must agree on the declared kind, and the engine's union
                // coerces at the SQL boundary
                TypeExpression dt = owner == null ? null
                        : MappingNormalizer.findPropertyTypeDeep(owner, prop, model);
                if (dt instanceof TypeExpression.NameRef dn
                        && ("String".equals(MappingNormalizer.simpleTypeName(dn.name())))) {
                    value = new AppliedFunction("cast", List.of(value,
                            new TypeAnnotation.Named(
                                    new TypeExpression.NameRef("String"))));
                }
                // every member column aligns to [1] (toOne types both sides
                // identically; lowering is erasure — the union's SQL columns
                // are nullable regardless, engine parity)
                value = new AppliedFunction("toOne", List.of(value));
                cols.add(new ColSpec(prop, new LambdaFunction(
                        List.of(pp.rowBind()), List.of(value)), null));
            }
            // lifted-navigation source keys: this thread reads its OWN key
            // columns under their member-suffixed names; other ordinals'
            // keys are typed NULL (nullable — no toOne wrap)
            for (var en : srcKeysByOrdinal.entrySet()) {
                for (var key : en.getValue().entrySet()) {
                    ValueSpecification read = en.getKey() == ordinal
                            ? new AppliedProperty(pp.rowBind(), key.getKey())
                            : MappingNormalizer.nullOfPhysicalKind((ClassMapping.Relational)
                                    members.get(en.getKey()),
                                    key.getKey(), md, model);
                    // toOne types both threads identically (real read vs
                    // NULL cast); lowering is erasure — the key stays NULL
                    read = new AppliedFunction("toOne", List.of(read));
                    cols.add(new ColSpec(key.getValue(), new LambdaFunction(
                            List.of(pp.rowBind()), List.of(read)), null));
                }
            }
            // CHAINED entries: the owning thread wraps its pipeline in the
            // MID-hop joins and reads the final hop's source keys via the
            // last mid slot; other threads project a typed NULL of the mid
            // table's column kind (engine 3-sets golden: fk1_1 from a_0)
            ValueSpecification threadPipe = pp.pipeline();
            for (LiftChain ch : chainsByOrdinal.getOrDefault(ordinal,
                    Collections.emptyList())) {
                for (LiftMidStep st : ch.steps()) {
                    threadPipe = new AppliedFunction("join", List.of(threadPipe,
                            new ColSpec(st.alias(), new LambdaFunction(List.of(),
                                    List.of(new AppliedFunction("tableReference",
                                            List.of(new PackageableElementPtr(st.db()),
                                                    new CString(st.table()))))),
                                    null),
                            st.cond()));
                }
            }
            for (var en : chainsByOrdinal.entrySet()) {
                for (LiftChain ch : en.getValue()) {
                    for (var key : ch.keys().entrySet()) {
                        ValueSpecification read;
                        if (en.getKey() == ordinal) {
                            read = new AppliedProperty(new AppliedProperty(
                                    pp.rowBind(), ch.keyAlias()), key.getKey());
                        } else {
                            DatabaseDefinition.ColumnDefinition cd =
                                    MappingNormalizer.findPhysicalColumn(ch.keyDb(), ch.keyTable(),
                                            key.getKey(), model);
                            String kind = cd == null ? null
                                    : MappingNormalizer.pureKindOf(cd.dataType());
                            if (kind == null) {
                                throw new NotImplementedException(
                                        "chained union key column '" + key.getKey()
                                        + "' has no derivable pure kind on table '"
                                        + ch.keyTable() + "'; mapping="
                                        + md.qualifiedName());
                            }
                            read = new AppliedFunction("cast", List.of(
                                    new PureCollection(List.of()),
                                    new TypeAnnotation.Named(
                                            new TypeExpression.NameRef(kind))));
                        }
                        read = new AppliedFunction("toOne", List.of(read));
                        cols.add(new ColSpec(key.getValue(), new LambdaFunction(
                                List.of(pp.rowBind()), List.of(read)), null));
                    }
                }
            }
            ValueSpecification projected = new AppliedFunction("project",
                    List.of(threadPipe, new ColSpecArray(cols)));
            union = union == null ? projected
                    : new AppliedFunction("concatenate", List.of(union, projected));
        }
        // the lifted navigations sit ABOVE the concatenate — one slot per
        // property, exactly the standard nav-slot pipeline shape
        for (NavLift lf : lifts) {
            union = new AppliedFunction("legacyNavigate", List.of(union,
                    new ColSpec(lf.property(), new LambdaFunction(List.of(),
                            List.of(new AppliedFunction("getAll", List.of(
                                    new PackageableElementPtr(lf.targetClassFqn()))))),
                            null),
                    lf.targetRows(), lf.condition()));
        }
        Variable row = new Variable("u_row");
        Map<String, KeyExpression> ctor = new LinkedHashMap<>();
        for (String prop : common) {
            ctor.put(prop, new KeyExpression(
                    new AppliedProperty(row, prop), false, false));
        }
        for (NavLift lf : lifts) {
            ctor.put(lf.property(), new KeyExpression(
                    new AppliedProperty(row, lf.property()), false, false));
        }
        return new AppliedFunction("map", List.of(union,
                new LambdaFunction(List.of(row),
                        List.of(MappingNormalizer.buildNewInstanceToOne(className, ctor, model)))));
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
            Map<Integer, Map<String, String>> srcKeysByOrdinal,
            Map<Integer, List<LiftChain>> chainsByOrdinal) {
    }

    /** The MID hops of a chained lift entry as physical join steps,
     * prevAlias-scoped conditions composing hop to hop. */
    static List<LiftMidStep> liftMidSteps(PropertyMapping.Join j,
            String prop, String srcTable, LegacyMappingDefinition md,
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
    record LiftChain(List<LiftMidStep> steps, String keyAlias,
            String keyDb, String keyTable, Map<String, String> keys) {
    }

    /** Collect the member class-typed Join PMs liftable onto the union. */
    static List<NavLift> collectNavLifts(LegacyMappingDefinition md,
            String className, List<ClassMapping> members,
            ModelBuilder model) {
        // property -> per-member entries, member order
        Map<String, List<int[]>> found = new LinkedHashMap<>();
        Map<String, List<PropertyMapping.Join>> joins = new LinkedHashMap<>();
        for (int i = 0; i < members.size(); i++) {
            if (!(members.get(i) instanceof ClassMapping.Relational mr)) {
                continue;   // Relation(~func) members carry no Join PMs
            }
            ClassDefinition memberOwner = model.findClass(mr.className()).orElse(null);
            for (PropertyMapping pm : mr.propertyMappings()) {
                if (!(pm instanceof PropertyMapping.Join j)) {
                    continue;
                }
                TypeExpression pt = memberOwner == null ? null
                        : MappingNormalizer.findPropertyTypeDeep(memberOwner, pm.propertyName(), model);
                if (!(pt instanceof TypeExpression.NameRef pnr)
                        || model.findClass(pnr.name()).isEmpty()) {
                    continue;   // scalar join-terminal shapes stay member-local
                }
                found.computeIfAbsent(pm.propertyName(), k -> new ArrayList<>())
                        .add(new int[]{i});
                joins.computeIfAbsent(pm.propertyName(), k -> new ArrayList<>())
                        .add(j);
            }
        }
        List<NavLift> lifts = new ArrayList<>();
        for (String prop : found.keySet()) {
            ClassDefinition owner = model.findClass(className).orElse(null);
            TypeExpression pt = owner == null ? null
                    : MappingNormalizer.findPropertyTypeDeep(owner, prop, model);
            if (!(pt instanceof TypeExpression.NameRef pnr)
                    || !model.isMappedClass(pnr.name())) {
                continue;
            }
            String targetClassFqn = pnr.name();
            // BITEMPORAL GATE (narrowed from the audit-11 full temporal
            // gate): single-dimension temporal unions lift correctly
            // (member threads filter per capability; 5 corpus families
            // pass). BITEMPORAL unions with per-member capability mixes
            // still over-match (hybridMilestoningUnionMap expects 12,
            // ungated lift returns 18 — a member capability subset goes
            // unfiltered under the two-date context) — LOUD until the
            // per-member capability x two-date filtering lands.
            if (MappingNormalizer.isBitemporalClass(className, model)
                    || MappingNormalizer.isBitemporalClass(targetClassFqn, model)) {
                continue;
            }
            ClassMapping.Union targetUnion = unionForClass(md, model, targetClassFqn);
            // Pre-validate the property's entries: any unsupported or
            // unresolvable entry SKIPS the whole property's lift (poison
            // reason recorded; demanding the property fails loudly) —
            // audit 11: a partial lift matched the wrong members, a throw
            // here poisoned scalar-only union queries.
            String skipReason = null;
            for (PropertyMapping.Join j0 : joins.get(prop)) {
                if (j0.targetSetId() != null && (targetUnion == null
                        || memberOrdinalOf(targetUnion.memberSetIds(), md,
                                model, j0.targetSetId()) < 0)) {
                    // a route naming the target's ROOT/SOLE set is the
                    // UN-routed navigation (engine rootClassMappingByClass;
                    // multipleChainedJoins V2: z[y1, z0] into single-set Z)
                    ClassMapping set = MappingNormalizer.findSetById(md, model, j0.targetSetId());
                    boolean rootOrSole = set instanceof ClassMapping.Relational tr
                            && (tr.root() || md.classMappings().stream()
                                    .filter(x -> x.className().equals(tr.className()))
                                    .count() <= 1);
                    if (rootOrSole) {
                        continue;
                    }
                    skipReason = "route '[" + j0.targetSetId() + "]' that is"
                            + " not a member of the target class's union";
                    break;
                }
            }
            if (skipReason != null) {
                model.mappingPoisons.merge(
                        md.qualifiedName() + "::" + className,
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
            boolean liftTargetMerged;
            {
                Set<Integer> tgtOrds = new HashSet<>();
                Set<Integer> srcMembers = new HashSet<>();
                Set<String> tgtColSets = new HashSet<>();
                boolean mergeable = targetUnion != null;
                List<int[]> ordsPre = found.get(prop);
                List<PropertyMapping.Join> jsPre = joins.get(prop);
                for (int k2 = 0; mergeable && k2 < jsPre.size(); k2++) {
                    PropertyMapping.Join j0 = jsPre.get(k2);
                    if (j0.targetSetId() == null || j0.joins().size() != 1) {
                        mergeable = false;
                        break;
                    }
                    int o = memberOrdinalOf(targetUnion.memberSetIds(), md,
                            model, j0.targetSetId());
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
                    String srcT = ((ClassMapping.Relational)
                            members.get(ordsPre.get(k2)[0])).mainTable().table();
                    String tgtT = MappingNormalizer.determineTargetTable(jd0.operation(), srcT,
                            hop0.joinName(), prop, 1, md.qualifiedName());
                    Set<String> cols0 = new TreeSet<>();
                    MappingNormalizer.collectColumnsOfTable(jd0.operation(), tgtT, cols0);
                    tgtColSets.add(String.join(",", cols0));
                }
                liftTargetMerged = mergeable
                        && tgtOrds.size() == targetUnion.memberSetIds().size()
                        && tgtColSets.size() == 1;
            }
            Variable s = new Variable("s");
            Variable t = new Variable("t");
            ValueSpecification orCond = null;
            Map<String, String> tgtKeyCols = new LinkedHashMap<>(); // suffixed -> base
            String landingDb = null;
            String landingTable = null;
            Map<Integer, Map<String, String>> srcKeys = new LinkedHashMap<>();
            Map<Integer, List<LiftChain>> chains = new LinkedHashMap<>();
            List<int[]> ords = found.get(prop);
            List<PropertyMapping.Join> js = joins.get(prop);
            for (int k = 0; k < js.size(); k++) {
                int memberOrd = ords.get(k)[0];
                PropertyMapping.Join j = js.get(k);
                String srcTable = ((ClassMapping.Relational)
                        members.get(memberOrd)).mainTable().table();
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
                cond = MappingNormalizer.suffixTargetReads(cond, s, memberOrd, srcOut);
                if (midSteps.isEmpty()) {
                    srcKeys.computeIfAbsent(memberOrd, x -> new LinkedHashMap<>())
                            .putAll(srcOut);
                } else {
                    chains.computeIfAbsent(memberOrd, x -> new ArrayList<>())
                            .add(new LiftChain(midSteps, prevAlias,
                                    midSteps.get(midSteps.size() - 1).db(),
                                    prevTable, srcOut));
                }
                Integer tgtOrd = j.targetSetId() != null && targetUnion != null
                        && !liftTargetMerged
                        ? memberOrdinalOf(targetUnion.memberSetIds(), md,
                                model, j.targetSetId())
                        : null;
                if (tgtOrd != null && tgtOrd >= 0) {
                    Map<String, String> tgtOut = new LinkedHashMap<>();
                    cond = MappingNormalizer.suffixTargetReads(cond, t, tgtOrd, tgtOut);
                    for (var en2 : tgtOut.entrySet()) {
                        tgtKeyCols.put(en2.getValue(), en2.getKey());
                    }
                }
                orCond = orCond == null ? cond
                        : new AppliedFunction("or", List.of(orCond, cond));
            }
            ValueSpecification targetRows = new AppliedFunction("tableReference",
                    List.of(new PackageableElementPtr(landingDb),
                            new CString(landingTable)));
            if (!tgtKeyCols.isEmpty()) {
                List<ColSpec> keySpecs = new ArrayList<>();
                for (var en2 : tgtKeyCols.entrySet()) {
                    Variable kr = new Variable("kr");
                    keySpecs.add(new ColSpec(en2.getKey(), new LambdaFunction(
                            List.of(kr), List.of(new AppliedProperty(kr,
                                    en2.getValue()))), null));
                }
                targetRows = new AppliedFunction("project",
                        List.of(targetRows, new ColSpecArray(keySpecs)));
            }
            lifts.add(new NavLift(prop, targetClassFqn, targetRows,
                    new LambdaFunction(List.of(s, t), List.of(orCond)), srcKeys,
                    chains));
        }
        return lifts;
    }

    /**
     * Scan the mapping closure (own + includes) for routed Join PMs whose
     * target set is one of this union's members; add each route's
     * MEMBER-side key columns to {@code sink} as
     * ordinal &rarr; (base &rarr; {@code base_ordinal}) so the union body
     * projects them with full provenance.
     */
    static void collectInboundRouteKeys(LegacyMappingDefinition md,
            ModelBuilder model, List<String> memberIds,
            List<ClassMapping> members,
            Map<Integer, Map<String, String>> sink) {
        List<LegacyMappingDefinition> closure = new ArrayList<>();
        MappingNormalizer.collectMappingClosure(md, model, closure, new HashSet<>());
        for (LegacyMappingDefinition m : closure) {
            for (ClassMapping cm : m.classMappings()) {
                if (!(cm instanceof ClassMapping.Relational rcm)) {
                    continue;
                }
                for (PropertyMapping pm : rcm.propertyMappings()) {
                    if (!(pm instanceof PropertyMapping.Join j)
                            || j.targetSetId() == null
                            || j.joins().size() != 1) {
                        continue;
                    }
                    int ord = memberOrdinalOf(memberIds, md, model,
                            j.targetSetId());
                    if (ord < 0) {
                        continue;
                    }
                    JoinChainElement hop = j.joins().get(0);
                    String db = hop.databaseName() != null
                            ? hop.databaseName() : j.database();
                    DatabaseDefinition.JoinDefinition jd =
                            model.findJoin(db, hop.joinName()).orElse(null);
                    if (jd == null) {
                        continue;   // loud at the route's own emission
                    }
                    if (!(members.get(ord)
                            instanceof ClassMapping.Relational routedMember)) {
                        continue;   // routes into Relation(~func) members
                                    // have no physical key table (loud at
                                    // navigation if demanded)
                    }
                    String memberTable = routedMember.mainTable().table();
                    Set<String> cols = new LinkedHashSet<>();
                    MappingNormalizer.collectColumnsOfTable(jd.operation(), memberTable, cols);
                    // self-join hops spell the member side {target}.col
                    MappingNormalizer.collectTargetColumns(jd.operation(), cols);
                    for (String c : cols) {
                        sink.computeIfAbsent(ord, k -> new LinkedHashMap<>())
                                .put(c, c + "_" + ord);
                    }
                }
            }
            // per-pair ASSOCIATION entries route INTO this union too: the
            // FINAL hop's target-side columns (on the routed member's main
            // table) ride suffixed — the lifted navigation's condition
            // reads them (multipleChainedJoins: t.fk_1)
            for (AssociationMapping am : m.associationMappings()) {
                if (!(am instanceof AssociationMapping.Relational rel)) {
                    continue;
                }
                for (AssociationPropertyMapping apm : rel.propertyMappings()) {
                    if (!(apm.body() instanceof PropertyMapping.Join j)) {
                        continue;
                    }
                    String tgtSet = j.targetSetId() != null
                            ? j.targetSetId() : apm.targetSetId();
                    if (tgtSet == null) {
                        continue;
                    }
                    int ord = memberOrdinalOf(memberIds, md, model, tgtSet);
                    if (ord < 0 || !(members.get(ord)
                            instanceof ClassMapping.Relational routedMember)) {
                        continue;
                    }
                    JoinChainElement hop = j.joins().get(j.joins().size() - 1);
                    String db = hop.databaseName() != null
                            ? hop.databaseName() : j.database();
                    DatabaseDefinition.JoinDefinition jd =
                            model.findJoin(db, hop.joinName()).orElse(null);
                    if (jd == null) {
                        continue;   // loud at the route's own emission
                    }
                    String memberTable = routedMember.mainTable().table();
                    Set<String> cols = new LinkedHashSet<>();
                    MappingNormalizer.collectColumnsOfTable(jd.operation(), memberTable, cols);
                    // self-join hops spell the member side {target}.col
                    MappingNormalizer.collectTargetColumns(jd.operation(), cols);
                    for (String c : cols) {
                        sink.computeIfAbsent(ord, k -> new LinkedHashMap<>())
                                .put(c, c + "_" + ord);
                    }
                }
            }
        }
    }
}
