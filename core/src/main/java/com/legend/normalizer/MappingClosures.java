// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.normalizer;

import com.legend.compiler.ModelBuilder;
import com.legend.model.AssociationDefinition;
import com.legend.model.AssociationMapping;
import com.legend.model.AssociationPropertyMapping;
import com.legend.model.ClassMapping;
import com.legend.model.EnumerationMapping;
import com.legend.model.JsonModelConnection;
import com.legend.model.LegacyMappingDefinition;
import com.legend.model.MappingInclude;
import com.legend.model.PropertyMapping;
import com.legend.model.RuntimeDefinition;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * THE INCLUDE-ORDER FACTS of every mapping of a graph (T4.1 step 4),
 * computed ONCE per mapping over the mappings' SURFACES and memoized on
 * the knowledge kernel ({@code model.knowledge().derived}): a pure
 * function of the index, asked from anywhere Phase E holds the index.
 *
 * <p>A mapping's SURFACE is the authored mapping plus the identity sets
 * every runtime's {@link JsonModelConnection} bindings synthesize into it
 * (our cross-bake of the runtime's bindings into the mapping surface — the
 * engine keeps JsonModelConnection data in memory and never rewrites the
 * mapping; formerly the index's own rewrite) — so an INCLUDER sees an
 * included mapping's identity sets, as it did before step 2 moved the
 * cross-bake into Phase E.
 *
 * <p>Each accessor keeps the rule of the include walker it replaced,
 * recursion shape included — the walkers differed from one another
 * (visible sets: a LATER include overrides an earlier one; operation sets
 * per class: the FIRST include found wins; roots: includes first, then
 * the mapping's own), and those differences are the current semantics.
 * The mapping's OWN part is supplied by the caller (the pre-passed
 * mapping synthesis consumes), the included part comes from here.
 */
final class MappingClosures {

    private final ModelBuilder model;
    private final Map<String, LegacyMappingDefinition> surfaces = new HashMap<>();
    private final Map<String, Closure> closures = new HashMap<>();

    private MappingClosures(ModelBuilder model) {
        this.model = model;
    }

    static MappingClosures of(ModelBuilder model) {
        return model.knowledge().derived(MappingClosures.class, MappingClosures::new);
    }

    /** The included part of {@code mappingFqn}'s closure (its own part is
     * the caller's mapping). */
    Closure closure(String mappingFqn) {
        Closure c = closures.get(mappingFqn);
        if (c == null) {
            c = new Closure(mappingFqn);
            closures.put(mappingFqn, c);
        }
        return c;
    }

    /** The SURFACE of the authored mapping: itself plus the identity
     * {@link ClassMapping.Relational} ({@code sourceUrl} set) for every
     * class a runtime's JSON connection binds through it, unless the
     * mapping already maps that class (user-authored wins). */
    LegacyMappingDefinition surface(LegacyMappingDefinition authored) {
        LegacyMappingDefinition s = surfaces.get(authored.qualifiedName());
        if (s == null) {
            s = withJsonIdentitySets(authored);
            surfaces.put(authored.qualifiedName(), s);
        }
        return s;
    }

    private Optional<LegacyMappingDefinition> surfaceOf(String path) {
        return model.findLegacyMapping(path).map(this::surface);
    }

    private LegacyMappingDefinition withJsonIdentitySets(LegacyMappingDefinition md) {
        List<ClassMapping> updated = null;
        for (RuntimeDefinition rd : model.runtimes().toList()) {
            if (rd.jsonConnections().isEmpty() || !rd.mappings().contains(md.qualifiedName())) {
                continue;
            }
            for (JsonModelConnection jmc : rd.jsonConnections()) {
                String classFqn = jmc.className();
                List<ClassMapping> current = updated != null ? updated : md.classMappings();
                if (current.stream().anyMatch(cm -> classFqn.equals(cm.className()))) {
                    continue;   // user-authored class mapping wins
                }
                if (updated == null) {
                    updated = new ArrayList<>(md.classMappings());
                }
                updated.add(new ClassMapping.Relational(
                        classFqn,
                        /* setId */ null,
                        /* extendsSetId */ null,
                        /* root */ true,
                        /* mainTable */ null,
                        /* filter */ null,
                        /* distinct */ false,
                        /* groupBy */ List.of(),
                        /* primaryKey */ List.of(),
                        /* propertyMappings */ List.of(),
                        /* sourceUrl */ jmc.url(), java.util.Map.of(), null));
            }
        }
        return updated == null ? md : md.withClassMappings(updated);
    }

    /** The included part of one mapping's closure, each fact computed on
     * first ask by the walk it replaced, over surfaces. */
    final class Closure {
        private final String fqn;
        private @com.legend.base.Nullable List<LegacyMappingDefinition> mappings;
        private @com.legend.base.Nullable LinkedHashMap<String, ClassMapping> sets;
        private @com.legend.base.Nullable Map<String, ClassMapping.Union> unions;
        private @com.legend.base.Nullable Map<String, ClassMapping.Inheritance> inheritances;
        private @com.legend.base.Nullable LinkedHashMap<String, ClassMapping> roots;
        private @com.legend.base.Nullable List<EnumerationMapping> enums;
        private final Map<String, Map<String, List<PropertyMapping.Join>>> pairEntries = new HashMap<>();

        private Closure(String fqn) {
            this.fqn = fqn;
        }

        private List<MappingInclude> includes() {
            return surfaceOf(fqn).map(LegacyMappingDefinition::includes).orElse(List.of());
        }

        /** The included mappings, depth-first in include order, each once
         * (the mapping itself excluded). */
        List<LegacyMappingDefinition> mappings() {
            List<LegacyMappingDefinition> known = mappings;
            if (known == null) {
                List<LegacyMappingDefinition> out = new ArrayList<>();
                Set<String> seen = new HashSet<>();
                seen.add(fqn);
                for (MappingInclude inc : includes()) {
                    surfaceOf(inc.mappingPath()).ifPresent(m -> walkMappings(m, out, seen));
                }
                known = List.copyOf(out);
                mappings = known;
            }
            return known;
        }

        private void walkMappings(LegacyMappingDefinition md,
                List<LegacyMappingDefinition> out, Set<String> seen) {
            if (!seen.add(md.qualifiedName())) {
                return;
            }
            out.add(md);
            for (MappingInclude inc : md.includes()) {
                surfaceOf(inc.mappingPath()).ifPresent(m -> walkMappings(m, out, seen));
            }
        }

        /** Set ids visible through the includes, transitively: a nearer
         * mapping's set overrides a deeper one, a LATER include overrides
         * an earlier one; an include's store substitutions apply to
         * everything pulled through it (grandparents included). */
        Map<String, ClassMapping> sets() {
            LinkedHashMap<String, ClassMapping> out = sets;
            if (out == null) {
                out = new LinkedHashMap<>();
                walkSets(includes(), out, new HashSet<>());
                sets = out;
            }
            return out;
        }

        private void walkSets(List<MappingInclude> incs, Map<String, ClassMapping> bySetId,
                Set<String> seen) {
            for (MappingInclude inc : incs) {
                if (!seen.add(inc.mappingPath())) {
                    continue;
                }
                LegacyMappingDefinition included = surfaceOf(inc.mappingPath()).orElseThrow(
                        () -> MissProbe.neverFired("MappingClosures#1 (include of '"
                                + inc.mappingPath() + "' has no mapping surface)"));
                Map<String, ClassMapping> local = new LinkedHashMap<>();
                walkSets(included.includes(), local, seen);
                for (ClassMapping cm : included.classMappings()) {
                    local.put(ResolvedMapping.idOf(cm), cm);
                }
                if (!inc.substitutions().isEmpty()) {
                    local.replaceAll((k, v) -> StoreSubstitutionRewrite.apply(v, inc.substitutions()));
                }
                bySetId.putAll(local);
            }
        }

        /** The Union operation set for {@code classFqn} through the
         * includes in the engine's order (an include's includes first, then
         * its own sets; the LATER include after the earlier), the LAST found
         * — {@code rootClassMappingByClass}'s {@code last()} (R1). */
        ClassMapping.@com.legend.base.Nullable Union union(@com.legend.base.Nullable String classFqn) {
            Map<String, ClassMapping.Union> out = unions;
            if (out == null) {
                out = new LinkedHashMap<>();
                Map<String, ClassMapping.Inheritance> outI = new LinkedHashMap<>();
                for (MappingInclude inc : includes()) {
                    Map<String, ClassMapping.Union> o = out;
                    surfaceOf(inc.mappingPath()).ifPresent(m -> walkOps(m, o, outI));
                }
                unions = out;
                inheritances = outI;
            }
            return classFqn == null ? null : out.get(classFqn);
        }

        /** The Inheritance operation set for {@code classFqn}, the same
         * last-wins rule. */
        ClassMapping.@com.legend.base.Nullable Inheritance inheritance(String classFqn) {
            union("");   // builds both
            return java.util.Objects.requireNonNull(inheritances).get(classFqn);
        }

        private void walkOps(LegacyMappingDefinition md, Map<String, ClassMapping.Union> out,
                Map<String, ClassMapping.Inheritance> outI) {
            for (MappingInclude inc : md.includes()) {
                surfaceOf(inc.mappingPath()).ifPresent(m -> walkOps(m, out, outI));
            }
            for (ClassMapping cm : md.classMappings()) {
                if (cm instanceof ClassMapping.Union u) {
                    out.put(u.className(), u);
                } else if (cm instanceof ClassMapping.Inheritance ih) {
                    outI.put(ih.className(), ih);
                }
            }
        }

        /** Set ids taken by more than one DISTINCT set across this mapping's
         * closure (an include's includes first, then its own; then this
         * mapping's own), each with the two mappings — the engine's
         * {@code collectAndValidateClassMappingIds} (R5). */
        List<String> duplicateIds() {
            Map<String, String> owner = new LinkedHashMap<>();
            Map<String, ClassMapping> first = new LinkedHashMap<>();
            List<String> dups = new ArrayList<>();
            Set<String> seen = new HashSet<>();
            walkIds(fqn, owner, first, dups, seen);
            return dups;
        }

        private void walkIds(String mappingFqn, Map<String, String> owner,
                Map<String, ClassMapping> first, List<String> dups, Set<String> seen) {
            if (!seen.add(mappingFqn)) {
                return;
            }
            LegacyMappingDefinition md = surfaceOf(mappingFqn).orElseThrow(() -> MissProbe.neverFired("MappingClosures#2"));
            for (MappingInclude inc : md.includes()) {
                walkIds(inc.mappingPath(), owner, first, dups, seen);
            }
            Set<String> own = new HashSet<>();
            for (ClassMapping cm : md.classMappings()) {
                String id = ResolvedMapping.idOf(cm);
                String prev = owner.get(id);
                if (prev != null && !prev.equals(mappingFqn) && first.get(id) != cm) {
                    dups.add(id + " (" + prev + ", " + mappingFqn + ")");
                } else if (prev == null) {
                    owner.put(id, mappingFqn);
                    first.put(id, cm);
                }
                if (!own.add(id)) {
                    dups.add(id + " (twice in " + mappingFqn + ")");
                }
            }
        }

        /** ROOT class mapping per class through the includes: deeper
         * includes first, a nearer mapping overrides; the root is the
         * {@code *} set or the class's sole set in that mapping. */
        Map<String, ClassMapping> roots() {
            LinkedHashMap<String, ClassMapping> out = roots;
            if (out == null) {
                out = new LinkedHashMap<>();
                Set<String> seen = new HashSet<>();
                for (MappingInclude inc : includes()) {
                    if (seen.add(inc.mappingPath())) {
                        LinkedHashMap<String, ClassMapping> o = out;
                        surfaceOf(inc.mappingPath()).ifPresent(m -> walkRoots(m, o, seen));
                    }
                }
                roots = out;
            }
            return out;
        }

        private void walkRoots(LegacyMappingDefinition md, Map<String, ClassMapping> out,
                Set<String> seen) {
            for (MappingInclude inc : md.includes()) {
                if (seen.add(inc.mappingPath())) {
                    surfaceOf(inc.mappingPath()).ifPresent(m -> walkRoots(m, out, seen));
                }
            }
            ownRoots(md, out);
        }

        /** The mapping's own root sets: the {@code *} set, or the class's
         * SOLE set (engine rootClassMappingByClass; corpus mappings often
         * omit {@code *} on singletons). */
        static void ownRoots(LegacyMappingDefinition md, Map<String, ClassMapping> out) {
            Map<String, Integer> setsPerClass = new LinkedHashMap<>();
            for (ClassMapping cm : md.classMappings()) {
                setsPerClass.merge(cm.className(), 1, Integer::sum);
            }
            for (ClassMapping cm : md.classMappings()) {
                if (cm.root() || java.util.Objects.requireNonNull(setsPerClass.get(cm.className())) == 1) {
                    out.put(cm.className(), cm);
                }
            }
        }

        /** Enumeration mappings through the includes, transitively; a bare
         * include path resolves in the mapping's own package first. */
        List<EnumerationMapping> enumerationMappings() {
            List<EnumerationMapping> e = enums;
            if (e == null) {
                List<EnumerationMapping> out = new ArrayList<>();
                Set<String> seen = new HashSet<>();
                seen.add(fqn);
                surfaceOf(fqn).ifPresent(m -> walkEnums(m, out, seen));
                e = List.copyOf(out);
                enums = e;
            }
            return e;
        }

        private void walkEnums(LegacyMappingDefinition md, List<EnumerationMapping> out,
                Set<String> seen) {
            for (MappingInclude inc : md.includes()) {
                String path = inc.mappingPath();
                if (!path.contains("::") && md.qualifiedName().contains("::")) {
                    String inPkg = md.qualifiedName().substring(0,
                            md.qualifiedName().lastIndexOf("::")) + "::" + path;
                    if (model.findLegacyMapping(inPkg).isPresent()) {
                        path = inPkg;
                    }
                }
                if (!seen.add(path)) {
                    continue;
                }
                LegacyMappingDefinition included = surfaceOf(path).orElseThrow(() -> MissProbe.neverFired("MappingClosures#3"));
                out.addAll(included.enumerationMappings());
                walkEnums(included, out, seen);
            }
        }

        /** Per-pair association entries for {@code classFqn} (or a
         * subclass of the end's owner) through the includes: source set
         * id &rarr; the routed Join PMs, the mapping's own entries first
         * (supplied by the caller), then each include's, depth-first. */
        Map<String, List<PropertyMapping.Join>> pairEntries(String classFqn) {
            Map<String, List<PropertyMapping.Join>> hit = pairEntries.get(classFqn);
            if (hit == null) {
                Map<String, List<PropertyMapping.Join>> out = new LinkedHashMap<>();
                Set<String> seen = new HashSet<>();
                seen.add(fqn);
                for (MappingInclude inc : includes()) {
                    surfaceOf(inc.mappingPath()).ifPresent(m -> walkPairs(m, classFqn, out, seen));
                }
                out.replaceAll((k, v) -> List.copyOf(v));
                hit = java.util.Collections.unmodifiableMap(out);
                pairEntries.put(classFqn, hit);
            }
            return hit;
        }

        private void walkPairs(LegacyMappingDefinition md, String classFqn,
                Map<String, List<PropertyMapping.Join>> out, Set<String> seen) {
            if (!seen.add(md.qualifiedName())) {
                return;
            }
            ownPairs(md, classFqn, out);
            for (MappingInclude inc : md.includes()) {
                surfaceOf(inc.mappingPath()).ifPresent(m -> walkPairs(m, classFqn, out, seen));
            }
        }

        /** The mapping's own pair entries for {@code classFqn}. */
        void ownPairs(LegacyMappingDefinition md, String classFqn,
                Map<String, List<PropertyMapping.Join>> out) {
            for (AssociationMapping am : md.associationMappings()) {
                if (!(am instanceof AssociationMapping.Relational rel)) continue;
                AssociationDefinition ad =
                        model.findAssociation(am.associationName()).orElseThrow(() -> MissProbe.neverFired("MappingClosures#4"));
                for (AssociationPropertyMapping apm : rel.propertyMappings()) {
                    if (!(apm.body() instanceof PropertyMapping.Join join)
                            || apm.sourceSetId() == null) {
                        continue;
                    }
                    String owner = AssociationSynthesis.associationOwnerClass(ad, apm.propertyName());
                    // the union class may INHERIT the end (B extends A picking
                    // up AE's 'e' — extends/union family): owner-or-superclass
                    if (owner == null || !(owner.equals(classFqn)
                            || model.knowledge().isSubtype(classFqn, owner))) {
                        continue;
                    }
                    PropertyMapping.Join stamped = join.targetSetId() == null
                            ? new PropertyMapping.Join(join.propertyName(),
                                    join.database(), join.joins(), apm.targetSetId())
                            : join;
                    out.computeIfAbsent(apm.sourceSetId(), k -> new ArrayList<>())
                            .add(stamped);
                }
            }
        }
    }
}
