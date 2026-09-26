// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0
package com.legend.normalizer;

import com.legend.model.AssociationMapping;
import com.legend.model.ClassMapping;
import com.legend.model.EnumerationMapping;
import com.legend.model.LegacyMappingDefinition;
import com.legend.model.MappingDefinition;
import com.legend.model.MappingInclude;
import com.legend.model.PropertyMapping;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * A mapping RESOLVED for synthesis (B1, B3.3): its pre-passed record (extends
 * flattened, same-extent inheritance applied, store refs qualified, implicit
 * operation sets appended — every step a rewrite of THIS record under
 * construction, {@link MappingPrePass}), its authored surface, the sets' own
 * declared keys, the sets the validation walled, and its include closure.
 * Every closure question synthesis asks — a set by id, the operation set of
 * a class, roots, pair entries, a union member's ordinal — is asked of this
 * record, in one construction from the mapping's text and its closure.
 * Whether a class is MAPPED for this mapping is the ledger's answer over the
 * pre-passed closure ({@link MappingLedger#isMapped}), never a graph-wide fact.
 */
final class ResolvedMapping {
    final LegacyMappingDefinition md;
    final MappingClosures.Closure closure;
    private final LegacyMappingDefinition surface;
    private final Map<String, MappingDefinition.ClassBinding.DeclaredKeys> declaredKeys;
    /** The sets the validation walled, BY SET ID (never by object: the
     * construction steps rebuild the records) with the recorded reason. */
    private final Map<String, String> invalid;
    /** The lifted view bodies (E.5, LiftedViews) every view-expansion site of
     * this mapping reads — an input of the phase, never recomputed here. */
    private final LiftedViews views;
    /** This record's OWN sets keyed the two ways synthesis asks of them —
     * by class (declaration order) and by effective id (the first set
     * declaring an id) — and the ROOT set per class over the whole closure.
     * Built with the record, so every class or id question is a lookup,
     * never a walk of the sets. */
    private final Map<String, List<ClassMapping>> ownByClass;
    private final Map<String, ClassMapping> ownById;
    private final Map<String, ClassMapping> roots;

    ResolvedMapping(LegacyMappingDefinition md, LegacyMappingDefinition surface,
            Map<String, MappingDefinition.ClassBinding.DeclaredKeys> declaredKeys,
            Map<String, String> invalid, MappingClosures.Closure closure, LiftedViews views) {
        this.md = md;
        this.closure = closure;
        this.surface = surface;
        this.declaredKeys = declaredKeys;
        this.invalid = invalid;
        this.views = views;
        Map<String, List<ClassMapping>> byClass = new HashMap<>();
        Map<String, ClassMapping> byId = new HashMap<>();
        for (ClassMapping cm : md.classMappings()) {
            byClass.computeIfAbsent(cm.className(), k -> new ArrayList<>()).add(cm);
            byId.putIfAbsent(idOf(cm), cm);
        }
        this.ownByClass = byClass;
        this.ownById = byId;
        Map<String, ClassMapping> rootsByClass = new LinkedHashMap<>(closure.roots());
        MappingClosures.Closure.ownRoots(md, rootsByClass);
        this.roots = java.util.Collections.unmodifiableMap(rootsByClass);
    }

    LiftedViews views() { return views; }

    /** The same record over a rewritten mapping (a construction step, the
     * multi-hop injection). */
    ResolvedMapping withMapping(LegacyMappingDefinition rewritten) {
        return new ResolvedMapping(rewritten, surface, declaredKeys, invalid, closure, views);
    }

    /** The same record with the sets the validation walled (by set id). */
    ResolvedMapping withInvalid(Map<String, String> walled) {
        return new ResolvedMapping(md, surface, declaredKeys, walled, closure, views);
    }

    LegacyMappingDefinition surface() { return surface; }
    Map<String, MappingDefinition.ClassBinding.DeclaredKeys> declaredKeys() { return declaredKeys; }

    /** The validation's recorded reason for {@code cm}'s set, else null. */
    @com.legend.base.Nullable String invalidReason(ClassMapping cm) { return invalid.get(idOf(cm)); }




    // ---- the raw record's face ------------------------------------------

    LegacyMappingDefinition raw() { return md; }
    String qualifiedName() { return md.qualifiedName(); }
    List<ClassMapping> classMappings() { return md.classMappings(); }
    List<MappingInclude> includes() { return md.includes(); }
    List<AssociationMapping> associationMappings() { return md.associationMappings(); }
    List<EnumerationMapping> enumerationMappings() { return md.enumerationMappings(); }
    @com.legend.base.Nullable String testSuitesSource() { return md.testSuitesSource(); }

    // ---- identities -----------------------------------------------------

    /** A set's EFFECTIVE id — {@link com.legend.model.SetId}, the one rule. */
    static String idOf(ClassMapping cm) {
        return com.legend.model.SetId.of(cm);
    }

    // ---- resolutions (today's rules; B2 adopts the engine's) -------------
    /** The set with id {@code setId}: this mapping's own first, else one
     * visible through the includes. Null for a null id or no such set. */
    @com.legend.base.Nullable ClassMapping set(@com.legend.base.Nullable String setId) {
        if (setId == null) {
            return null;
        }
        ClassMapping own = ownById.get(setId);
        return own != null ? own : closure.sets().get(setId);
    }

    /** This mapping then its includes, depth-first in include order, each once. */
    List<LegacyMappingDefinition> closure() {
        List<LegacyMappingDefinition> out = new ArrayList<>(closure.mappings().size() + 1);
        out.add(md);
        Set<String> seen = new HashSet<>();
        seen.add(md.qualifiedName());
        for (LegacyMappingDefinition m : closure.mappings()) {
            if (seen.add(m.qualifiedName())) {
                out.add(m);
            }
        }
        return out;
    }

    /** {@code classFqn}'s Relational sets across {@link #closure()}: this
     * mapping's own in declaration order, then each include's (union V3:
     * association mappings routinely live in a mapping that only INCLUDES
     * the class-mapping definitions). */
    List<ClassMapping.Relational> relationalSets(@com.legend.base.Nullable String classFqn) {
        List<ClassMapping.Relational> out = new ArrayList<>();
        for (ClassMapping cm : ownByClass.getOrDefault(classFqn, List.of())) {
            if (cm instanceof ClassMapping.Relational rcm) {
                out.add(rcm);
            }
        }
        out.addAll(closure.relationalSets(classFqn));
        return out;
    }

    /** Set ids visible through the includes (a later include overrides an
     * earlier one; substitutions applied); own sets excluded. */
    Map<String, ClassMapping> includedSets() {
        return closure.sets();
    }

    /** Every visible set by id: the includes', then this mapping's own on top. */
    Map<String, ClassMapping> visibleSets() {
        Map<String, ClassMapping> out = new LinkedHashMap<>(closure.sets());
        for (ClassMapping cm : md.classMappings()) {
            out.put(idOf(cm), cm);
        }
        return out;
    }

    /** The Union operation set for {@code classFqn}: own first, else the
     * first found through the includes. */
    ClassMapping.@com.legend.base.Nullable Union unionOf(@com.legend.base.Nullable String classFqn) {
        for (ClassMapping cm : ownByClass.getOrDefault(classFqn, List.of())) {
            if (cm instanceof ClassMapping.Union u) {
                return u;
            }
        }
        return closure.union(classFqn);
    }

    /** The Inheritance operation set for {@code classFqn}, the same rule. */
    ClassMapping.@com.legend.base.Nullable Inheritance inheritanceOf(String classFqn) {
        for (ClassMapping cm : ownByClass.getOrDefault(classFqn, List.of())) {
            if (cm instanceof ClassMapping.Inheritance ih) {
                return ih;
            }
        }
        return closure.inheritance(classFqn);
    }

    /** ROOT set per class: the includes' (deeper first), this mapping's own
     * overriding; the {@code *} set or the class's sole set. */
    Map<String, ClassMapping> roots() {
        return roots;
    }

    /** Is {@code set} the ROOT (or sole) set of its class — the engine's
     * {@code rootClassMappingByClass} answer? ONE owner (audit 2026-09-15
     * P2-2: six sites counted sole-ness over different scopes; the sharpest
     * resolved a route's set THROUGH the include closure but counted over
     * the QUERYING mapping's own sets, so an included class with one
     * unmarked set counted zero). Judged in the OWNING scope: the closure's
     * roots with this mapping's own overriding. */
    boolean isRootOrSole(ClassMapping set) {
        ClassMapping root = roots.get(set.className());
        return root != null && idOf(root).equals(idOf(set));
    }

    /** Own enumeration mappings plus the includes', transitively. */
    List<EnumerationMapping> enumerationMappingsWithIncludes() {
        List<EnumerationMapping> out = new ArrayList<>(md.enumerationMappings());
        out.addAll(closure.enumerationMappings());
        return out;
    }

    /** Per-pair association entries for {@code classFqn}: own first, then
     * each include's, depth-first. */
    Map<String, List<PropertyMapping.Join>> pairEntries(String classFqn) {
        Map<String, List<PropertyMapping.Join>> out = new LinkedHashMap<>();
        closure.ownPairs(md, classFqn, out);
        closure.pairEntries(classFqn).forEach((setId, joins) ->
                out.computeIfAbsent(setId, k -> new ArrayList<>()).addAll(joins));
        return out;
    }

    /** A union member's ordinal for {@code setId}: the member itself, or
     * the member whose {@code extends} chain reaches it; -1 otherwise. */
    int memberOrdinal(List<String> memberIds, @com.legend.base.Nullable String setId) {
        int direct = memberIds.indexOf(setId);
        if (direct >= 0) {
            return direct;
        }
        for (int i = 0; i < memberIds.size(); i++) {
            ClassMapping m = set(memberIds.get(i));
            Set<String> seen = new HashSet<>();
            while (m instanceof ClassMapping.Relational r
                    && r.extendsSetId() != null && seen.add(r.extendsSetId())) {
                if (r.extendsSetId().equals(setId)) {
                    return i;
                }
                m = set(r.extendsSetId());
            }
        }
        return -1;
    }
}
