// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0
package com.legend.compiler;

import com.legend.model.DatabaseDefinition;
import com.legend.model.DatabaseDefinition.TableDefinition;

import java.util.HashMap;
import java.util.Map;

/**
 * One database's tables by the spellings a mapping or a join may use —
 * built once, when the database is ingested, so resolving a table
 * reference is a lookup, never a walk of the database's tables.
 *
 * <ul>
 *   <li>A BARE name is the first table so named in the flat list (the
 *       default schema's first, {@code FromProtocol}), then in each named
 *       schema in order.</li>
 *   <li>{@code SCHEMA.T} is schema {@code SCHEMA}'s table {@code T} only,
 *       the first such schema declaring it.</li>
 *   <li>{@code default.T} is a TOP-LEVEL table {@code T} only (ENGINE
 *       PARITY, RelationalParseTreeWalker:149: a database's top-level
 *       tables ARE schema 'default' — a named schema's table is not), then
 *       an explicit {@code Schema default(...)} block's.</li>
 * </ul>
 * Names compare exactly.
 */
final class TableIndex {

    private final Map<String, TableDefinition> top = new HashMap<>();
    private final Map<String, TableDefinition> bare = new HashMap<>();
    private final Map<String, Map<String, TableDefinition>> bySchema = new HashMap<>();

    private TableIndex() {
    }

    static TableIndex of(DatabaseDefinition db) {
        TableIndex ix = new TableIndex();
        for (TableDefinition t : db.tables()) {
            ix.bare.putIfAbsent(t.name(), t);
        }
        for (TableDefinition t : db.defaultSchemaTables()) {
            ix.top.putIfAbsent(t.name(), t);
        }
        for (DatabaseDefinition.SchemaDefinition s : db.schemas()) {
            Map<String, TableDefinition> own =
                    ix.bySchema.computeIfAbsent(s.name(), k -> new HashMap<>());
            for (TableDefinition t : s.tables()) {
                own.putIfAbsent(t.name(), t);
                ix.bare.putIfAbsent(t.name(), t);
            }
        }
        // TABULAR FUNCTIONS are named relations too (upstream: NamedRelation
        // beside Table), reached by the same spellings; a table keeps a name
        // both declare. The top level's are the flat ones no schema owns.
        java.util.Set<TableDefinition> inSchemas =
                java.util.Collections.newSetFromMap(new java.util.IdentityHashMap<>());
        for (DatabaseDefinition.SchemaDefinition s : db.schemas()) {
            Map<String, TableDefinition> own =
                    ix.bySchema.computeIfAbsent(s.name(), k -> new HashMap<>());
            for (TableDefinition f : s.tabularFunctions()) {
                own.putIfAbsent(f.name(), f);
                inSchemas.add(f);
            }
        }
        for (TableDefinition f : db.tabularFunctions()) {
            ix.bare.putIfAbsent(f.name(), f);
            if (!inSchemas.contains(f)) {
                ix.top.putIfAbsent(f.name(), f);
            }
        }
        return ix;
    }

    /** The table {@code name} spells in this database, or null. */
    @com.legend.base.Nullable TableDefinition find(String name) {
        int dot = name.indexOf('.');
        if (dot <= 0) {
            return bare.get(name);
        }
        String schema = name.substring(0, dot);
        String table = name.substring(dot + 1);
        if (schema.equals("default")) {
            TableDefinition hit = top.get(table);
            if (hit != null) {
                return hit;
            }
        }
        Map<String, TableDefinition> own = bySchema.get(schema);
        return own == null ? null : own.get(table);
    }
}
