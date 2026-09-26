// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.resolver;

import com.legend.compiler.element.ModelContext;
import com.legend.model.ClassMapping;
import com.legend.model.DatabaseDefinition;

/**
 * Leg 4 — views as identity-carrying frames: a class whose ~mainTable is
 * a VIEW joins as a derived table NAMED BY THE VIEW (the engine's
 * relational model makes a view a {@code Table}; its alias groups by the
 * view's own name — {@code orderpnlview_0}, never the underlying
 * physical table's).
 */
public final class ViewFrames {

    private ViewFrames() {
    }

    /** The VIEW name behind {@code cs}'s main source, or null when the
     * class maps a physical table (or the lookup cannot resolve). Walks
     * the mapping INCLUDE CLOSURE — the class set may live in an
     * included mapping (modelJoins' LegalEntityMapping). Reads the
     * {@code RelationalSource} STAMP (the Phase-E resolved main source,
     * scope-inference included); the view check below is a DATABASE
     * registry lookup, not mapping semantics. */
    public static @com.legend.base.Nullable String frameNameOf(ModelContext ctx, ClassSource cs) {
        if (ctx == null || cs == null) {
            return null;
        }
        var md = ctx.findMapping(cs.mappingFqn()).orElse(null);
        if (md == null) {
            return null;
        }
        for (var cb : md.classBindingsWithIncludes(cs.classFqn(), ctx::findMapping)) {
            if (!(cb instanceof com.legend.model.MappingDefinition
                            .ClassBinding.Relational rb)
                    || !(rb.source() instanceof com.legend.model
                            .MappingDefinition.RelationalSource.Table src)) {
                continue;
            }
            String table = src.table();
            String database = src.database();
            if (table.startsWith("default.")) {
                table = table.substring("default.".length());
            }
            DatabaseDefinition db =
                    ctx.findDatabase(database).orElse(null);
            if (db == null) {
                return null;
            }
            // SCHEMA-QUALIFIED view refs (Entity.LegalEntity_View): the
            // view registry keys by BARE name within its schema
            String t = table.contains(".")
                    ? table.substring(table.lastIndexOf('.') + 1)
                    : table;
            if (db.views().stream().anyMatch(v -> v.name().equals(t))) {
                return t;
            }
            for (var sch : db.schemas()) {
                if (sch.views().stream().anyMatch(v -> v.name().equals(t))) {
                    return t;
                }
            }
            return null;
        }
        return null;
    }
}
