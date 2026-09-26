// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.lineage;

import com.legend.compiler.element.ModelContext;
import com.legend.error.NotImplementedException;
import com.legend.model.DatabaseDefinition;
import com.legend.model.MappingDefinition;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * A scanColumns handle's ColumnWithContext AS ROWS (harness burn-down
 * group I, 2026-09-03): {@code column_contexts(scan_id, ordinal, db_fqn,
 * schema_name, table_name, column_name, context)} — the columns the
 * LOWERED plan reads ({@link ScanColumns#entries}), each resolved to the
 * store's own Column row by the mapping's databases (the plan spells
 * tables by name; the store keys columns by database, schema and table).
 * A table no database of the mapping defines, or two define, is loud.
 */
public final class ColumnLineageRows {

    private ColumnLineageRows() {
    }

    public static Map<String, List<List<String>>> rows(String scope, ModelContext ctx,
            String mappingFqn, List<ScanColumns.Entry> entries) {
        List<DatabaseDefinition> dbs = mappingDatabases(ctx, mappingFqn);
        List<DatabaseDefinition> all = new ArrayList<>();
        for (String f : java.util.Objects.requireNonNullElse(
                ctx.classifierInstances(com.legend.compiler.element.type.PlatformTypes.DATABASE), List.<String>of())) {
            ctx.findDatabase(f).ifPresent(all::add);
        }
        List<List<String>> rows = new ArrayList<>();
        for (int i = 0; i < entries.size(); i++) {
            ScanColumns.Entry e = entries.get(i);
            // the mapping's own databases first; a table they do not define
            // (a mapping whose sets bind a store declared elsewhere) resolves
            // over every database the model declares — one hit, or loud
            String[] owner = ownerOf(dbs, e.table());
            String[] o = owner != null ? owner : ownerOf(all, e.table());
            if (o == null) {
                throw new NotImplementedException("scanColumns: table '" + e.table()
                        + "' is defined by no database of the model");
            }
            rows.add(List.of(scope, Integer.toString(i), o[0], o[1], o[2],
                    e.column(), e.context()));
        }
        Map<String, List<List<String>>> out = new LinkedHashMap<>();
        out.put("column_contexts", rows);
        return out;
    }

    /** The mapping's relational sets' databases, includes closed. */
    private static List<DatabaseDefinition> mappingDatabases(ModelContext ctx,
            String mappingFqn) {
        Set<String> fqns = new LinkedHashSet<>();
        MappingDefinition md = ctx.findMapping(mappingFqn).orElseThrow(
                () -> new NotImplementedException("scanColumns: unknown mapping "
                        + mappingFqn));
        for (var cb : md.classBindingsWithIncludes(ctx::findMapping)) {
            if (cb instanceof MappingDefinition.ClassBinding.Relational r
                    && r.source() instanceof MappingDefinition.RelationalSource.Table t) {
                fqns.add(t.database());
            }
        }
        List<DatabaseDefinition> out = new ArrayList<>();
        java.util.ArrayDeque<String> work = new java.util.ArrayDeque<>(fqns);
        Set<String> seen = new LinkedHashSet<>();
        while (!work.isEmpty()) {
            String f = work.poll();
            if (!seen.add(f)) {
                continue;
            }
            ctx.findDatabase(f).ifPresent(db -> {
                out.add(db);
                work.addAll(db.includes());
            });
        }
        return out;
    }

    /** {db fqn, schema, table} of the ONE table the plan named; null when
     * none of {@code dbs} defines it. */
    private static String @com.legend.base.Nullable [] ownerOf(List<DatabaseDefinition> dbs,
            String spelled) {
        String schemaHint = spelled.contains(".")
                ? spelled.substring(0, spelled.lastIndexOf('.')) : null;
        String table = spelled.contains(".")
                ? spelled.substring(spelled.lastIndexOf('.') + 1) : spelled;
        List<String[]> hits = new ArrayList<>();
        for (DatabaseDefinition db : dbs) {
            // the DEFAULT schema's tables ride the database directly
            if (schemaHint == null || schemaHint.equals("default")) {
                for (var t : db.tables()) {
                    if (t.name().equals(table)) {
                        hits.add(new String[]{db.qualifiedName(), "default", t.name()});
                    }
                }
            }
            for (var sc : db.schemas()) {
                if (schemaHint != null && !sc.name().equals(schemaHint)) {
                    continue;
                }
                for (var t : sc.tables()) {
                    if (t.name().equals(table)) {
                        hits.add(new String[]{db.qualifiedName(), sc.name(), t.name()});
                    }
                }
            }
        }
        if (hits.isEmpty()) {
            return null;
        }
        if (hits.size() != 1) {
            throw new NotImplementedException("scanColumns: table '" + spelled
                    + "' resolves to " + hits.size() + " store tables");
        }
        return hits.get(0);
    }
}
