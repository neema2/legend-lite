// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.edge;

import com.legend.compiler.element.ModelContext;
import com.legend.error.NotImplementedException;
import com.legend.lineage.ScanRelations;
import com.legend.model.ClassMapping;
import com.legend.model.LegacyMappingDefinition;

/**
 * legend-engine's SET-IMPLEMENTATION IDENTITY for the plan printer (J40, 2026-11-18, the orchestrator's assignment:
 * moved from the product's lineage.ScanRelations, read by the edge's printers alone): a class under a mapping as
 * {@code [definingMappingName, setId, dbFqn, mainTable]}, over the product's mapping reads (ScanRelations' mapping,
 * withIncludes, allClassMappings, typeMatches, mainDbOf, mainTableOf).
 */
final class SetImplementations {

    private SetImplementations() {
    }

    /** The engine's set-implementation identity for a class under a
     * mapping (#47 plan printer): {@code [definingMappingName, setId,
     * dbFqn, mainTable]} — the DEFINING mapping is the include-child
     * declaring the set; the default setId is the class FQN with
     * underscores. Loud when the class maps ambiguously. */
    static String[] rootImpl(ModelContext ctx, String mappingFqn,
            String classFqn) {
        return rootImpl(ctx, mappingFqn, classFqn, java.util.List.of());
    }

    /** {@code chainMappings}: ModelChainConnection mappings — the M2M2R
     * surface; a ~src class unmapped in {@code mappingFqn} chases its
     * relational set through them (engine chained-mapping resolution). */
    static String[] rootImpl(ModelContext ctx, String mappingFqn,
            String classFqn, java.util.List<String> chainMappings) {
        String[] r = rootImplOrNull(ctx, mappingFqn, classFqn, 0,
                chainMappings);
        if (r == null) {
            throw new NotImplementedException("plan: no class mapping for '"
                    + classFqn + "' under '" + mappingFqn + "'"
                    + (chainMappings.isEmpty() ? ""
                            : " or chain " + chainMappings));
        }
        return r;
    }

    private static String @com.legend.base.Nullable [] rootImplOrNull(
            ModelContext ctx, String mappingFqn,
            String classFqn, int depth,
            java.util.List<String> chainMappings) {
        LegacyMappingDefinition md = ScanRelations.mapping(ctx, mappingFqn);
        for (LegacyMappingDefinition m : ScanRelations.withIncludes(ctx, md)) {
            for (ClassMapping.Relational r : ScanRelations.allClassMappings(m)) {
                if (ScanRelations.typeMatches(r.className(), classFqn)) {
                    String name = m.qualifiedName().substring(
                            m.qualifiedName().lastIndexOf(':') + 1);
                    String setId = com.legend.model.SetId.of(r);
                    return new String[]{name, setId, ScanRelations.mainDbOf(r),
                            ScanRelations.mainTableOf(r)};
                }
            }
        }
        // an M2M (Pure) set: the physical identity follows ~src to the
        // UPSTREAM class's relational set (the H5 collapse — the plan's
        // db/table identity is the composed source's; max 4 hops, the
        // corpus chains twice); the upstream class may live in THIS
        // mapping or a CHAIN mapping (ModelChainConnection)
        if (depth < 4) {
            for (LegacyMappingDefinition m : ScanRelations.withIncludes(ctx, md)) {
                for (com.legend.model.ClassMapping cm : m.classMappings()) {
                    if (cm instanceof ClassMapping.Pure pm
                            && ScanRelations.typeMatches(pm.className(), classFqn)
                            // no ~src = no upstream to chase; the chain
                            // simply ends here rather than being unknown
                            && pm.sourceClass() != null) {
                        String[] up = rootImplOrNull(ctx, mappingFqn,
                                pm.sourceClass(), depth + 1, chainMappings);
                        for (int i = 0; up == null
                                && i < chainMappings.size(); i++) {
                            up = rootImplOrNull(ctx, chainMappings.get(i),
                                    pm.sourceClass(), depth + 1,
                                    java.util.List.of());
                        }
                        if (up == null) {
                            return null;
                        }
                        // 5th element marks the M2M chase: the plan's TDS
                        // tuple types spell PURE defaults, not the
                        // physical columns (the M2M layer erases them —
                        // m2m2rShowcase golden name VARCHAR(8192))
                        return new String[]{up[0], up[1], up[2], up[3],
                                "m2m"};
                    }
                }
            }
        }
        return null;
    }
}
