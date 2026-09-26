// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.normalizer;

import com.legend.model.ClassMapping;
import com.legend.model.LegacyMappingDefinition;
import com.legend.model.PropertyMapping;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * The SURFACE facts Phase F used to re-read off the authored mapping
 * (T4.1 step 4b), computed once at Phase E and stamped on the compiled
 * mapping as {@link com.legend.model.MappingDefinition.NormalizationFacts}.
 * Each rule is the F-side reader's, verbatim, over the mapping's surface
 * (the authored mapping plus its identity sets — the object those readers
 * read).
 */
final class MappingFacts {

    private MappingFacts() {}

    /** class &rarr; an Operation union's member CLASSES in member order;
     * absent when a member set does not resolve within the mapping. */
    static Map<String, List<String>> unionMembers(LegacyMappingDefinition surface) {
        Map<String, List<String>> out = new LinkedHashMap<>();
        for (ClassMapping cm : surface.classMappings()) {
            if (!(cm instanceof ClassMapping.Union u) || out.containsKey(u.className())) {
                continue;   // the reader took the FIRST union of the class
            }
            List<String> members = new ArrayList<>();
            boolean complete = true;
            for (String sid : u.memberSetIds()) {
                String memberClass = null;
                for (ClassMapping m2 : surface.classMappings()) {
                    if (sid.equals(m2.setId())) {
                        memberClass = m2.className();
                    }
                }
                if (memberClass == null) {
                    complete = false;
                    break;
                }
                members.add(memberClass);
            }
            if (complete) {
                out.put(u.className(), List.copyOf(members));
            }
        }
        return out;
    }

    /** owner class &rarr; property &rarr; the ONE class every route of the
     * property lands on (the first Relational set of the owner with a
     * routed answer; a set whose routes disagree yields no answer for the
     * owner at all — the reader's early return). */
    static Map<String, Map<String, String>> routedTargetClasses(LegacyMappingDefinition surface) {
        Map<String, Map<String, String>> out = new LinkedHashMap<>();
        Map<String, Set<String>> propsByOwner = new LinkedHashMap<>();
        for (ClassMapping cm : surface.classMappings()) {
            if (cm instanceof ClassMapping.Relational rcm) {
                for (PropertyMapping pm : rcm.propertyMappings()) {
                    if (pm instanceof PropertyMapping.Join j && j.targetSetId() != null) {
                        propsByOwner.computeIfAbsent(rcm.className(), k -> new LinkedHashSet<>())
                                .add(j.propertyName());
                    }
                }
            }
        }
        propsByOwner.forEach((owner, props) -> {
            for (String prop : props) {
                String answer = routedTargetClass(surface, owner, prop);
                if (answer != null) {
                    out.computeIfAbsent(owner, k -> new LinkedHashMap<>()).put(prop, answer);
                }
            }
        });
        return out;
    }

    private static @com.legend.base.Nullable String routedTargetClass(LegacyMappingDefinition lm,
            String ownerClass, String prop) {
        for (ClassMapping cm : lm.classMappings()) {
            if (!(cm instanceof ClassMapping.Relational rcm) || !rcm.className().equals(ownerClass)) {
                continue;
            }
            String routed = null;
            for (PropertyMapping pm : rcm.propertyMappings()) {
                if (pm instanceof PropertyMapping.Join j && j.propertyName().equals(prop)
                        && j.targetSetId() != null) {
                    for (ClassMapping m2 : lm.classMappings()) {
                        if (j.targetSetId().equals(m2.setId())) {
                            if (routed != null && !routed.equals(m2.className())) {
                                return null;
                            }
                            routed = m2.className();
                        }
                    }
                }
            }
            if (routed != null) {
                return routed;
            }
        }
        return null;
    }

}
