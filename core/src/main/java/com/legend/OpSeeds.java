// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend;

import com.legend.builtin.Pure;
import com.legend.compiler.element.ModelContext;
import com.legend.model.DatabaseDefinition;
import com.legend.model.PropertyMapping;
import com.legend.model.RelationalOperation;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

/**
 * The metamodel store's RELATIONAL-OPERATION seeds (group F burn,
 * 2026-09-02): every mapping expression and view column expression of the
 * active context as a TREE of {@code relational_elements} rows (one row per
 * node, parent + ordinal, the engine's node kinds: DynaFunction / Literal
 * / LiteralList / TableAliasColumn / RelationalOperationElementWithJoin),
 * each node stamped with the compiler's inferred SQL type as a
 * {@code data_types} row ({@link RelationalTypeInference} — the engine's
 * {@code inferRelationalType} recurses per query; ours reads the fact as
 * a row, the include-closure precedent), plus the rows that OWN the trees:
 * {@code property_mappings} (one per effective relational property mapping
 * of every relational set — own first, then the extends chain's, an
 * inherited name overridden by a nearer one; {@code declared_depth} says
 * which) and {@code view_column_mappings}. Column declared types are
 * data-type rows too ({@link #columnTypeId}).
 *
 * <p>Named gaps (rows NOT produced): embedded / inline-embedded /
 * otherwise-embedded property mappings (m3 EmbeddedSetImplementations,
 * not RelationalPropertyMappings), local mapping properties ({@code +p}),
 * a join-slot mapping's JoinTreeNode (only its terminal column is a
 * child), a mixed constructed tree's row-valued arguments.
 */
final class OpSeeds {

    private final com.legend.compiler.element.RelationalOpRows rows;
    final List<List<String>> ops;
    final List<List<String>> dataTypes;
    final List<List<String>> viewColumnMappings = new ArrayList<>();
    final List<List<String>> propertyMappings = new ArrayList<>();
    /** (association fqn, end name) for every association end a property
     * mapping binds — the Property rows a class does not declare. */
    final Set<List<String>> associationProperties = new LinkedHashSet<>();

    private final ModelContext ctx;

    private OpSeeds(ModelContext ctx) {
        this.ctx = ctx;
        this.rows = new com.legend.compiler.element.RelationalOpRows(ctx);
        this.ops = rows.ops;
        this.dataTypes = rows.dataTypes;
    }

    static OpSeeds of(ModelContext ctx) {
        OpSeeds s = new OpSeeds(ctx);
        s.columnTypes();
        s.viewColumns();
        s.propertyMappingRows();
        s.primaryKeyNodes();
        return s;
    }

    static String columnTypeId(String dbFqn, String schema, String table, String column) {
        return "col:" + dbFqn + "|" + schema + "|" + table + "|" + column;
    }

    // ------------------------------------------------------------------
    // data types

    private void columnTypes() {
        for (String dbFqn : dbs()) {
            DatabaseDefinition db = ctx.findDatabase(dbFqn).orElse(null);
            if (db == null) {
                continue;
            }
            for (DatabaseDefinition.TableDefinition t : db.defaultSchemaTables()) {
                for (DatabaseDefinition.ColumnDefinition c : t.columns()) {
                    rows.dataType(columnTypeId(dbFqn, "default", t.name(), c.name()), c.dataType());
                }
            }
            for (DatabaseDefinition.SchemaDefinition s : db.schemas()) {
                for (DatabaseDefinition.TableDefinition t : s.tables()) {
                    for (DatabaseDefinition.ColumnDefinition c : t.columns()) {
                        rows.dataType(columnTypeId(dbFqn, s.name(), t.name(), c.name()),
                                c.dataType());
                    }
                }
            }
        }
    }

    // ------------------------------------------------------------------
    // view column mappings

    private void viewColumns() {
        for (String dbFqn : dbs()) {
            DatabaseDefinition db = ctx.findDatabase(dbFqn).orElse(null);
            if (db == null) {
                continue;
            }
            for (DatabaseDefinition.ViewDefinition v : db.defaultSchemaViews()) {
                viewColumns(dbFqn, db, "default", v);
            }
            for (DatabaseDefinition.SchemaDefinition s : db.schemas()) {
                for (DatabaseDefinition.ViewDefinition v : s.views()) {
                    viewColumns(dbFqn, db, s.name(), v);
                }
            }
        }
    }

    private void viewColumns(String dbFqn, DatabaseDefinition db, String schema,
            DatabaseDefinition.ViewDefinition v) {
        for (DatabaseDefinition.ViewDefinition.ViewColumnMapping cm : v.columnMappings()) {
            String id = "vc:" + dbFqn + "|" + schema + "|" + v.name() + "|" + cm.name();
            viewColumnMappings.add(List.of(dbFqn, schema, v.name(), cm.name(), id));
            rows.node(cm.expression(), id, null, null, dbFqn, db, null);
        }
    }

    // ------------------------------------------------------------------
    // property mappings

    private void propertyMappingRows() {
        MetamodelSeeds.SetGraph graph = MetamodelSeeds.setGraph(ctx);
        java.util.Map<String, java.util.Map<String, com.legend.model.ClassMapping.Relational>>
                legacyByMapping = new java.util.HashMap<>();
        for (MetamodelSeeds.SetRow set : graph.sets()) {
            List<MetamodelSeeds.SetRow> chain = graph.ancestry(set);
            Set<String> seen = new LinkedHashSet<>();
            int ordinal = 0;
            for (int depth = 0; depth < chain.size(); depth++) {
                MetamodelSeeds.SetRow owner = chain.get(depth);
                var legacy = legacyByMapping.computeIfAbsent(owner.mappingFqn(),
                        m -> MetamodelSeeds.legacySets(ctx, m)).get(owner.id());
                if (legacy == null) {
                    continue;
                }
                for (PropertyMapping pm : legacy.propertyMappings()) {
                    RelationalOperation op = opOf(pm);
                    if (op == null || !seen.add(pm.propertyName())) {
                        continue;
                    }
                    String id = "pm:" + set.mappingFqn() + "|" + set.id() + "|" + ordinal;
                    String ownerFqn = propertyOwner(set.binding().classFqn(), pm.propertyName());
                    propertyMappings.add(java.util.Arrays.asList(set.mappingFqn(), set.id(),
                            Integer.toString(ordinal), ownerFqn, pm.propertyName(), id,
                            Integer.toString(depth)));
                    DatabaseDefinition db = ctx.findDatabase(owner.table().database()).orElse(null);
                    rows.node(op, id, null, null, owner.table().database(), db,
                            owner.table().table());
                    ordinal++;
                }
            }
        }
    }

    /** One TableAliasColumn node per compiled primary-key column of every
     * relational set, owned by the set ({@code pk_mapping_fqn},
     * {@code pk_set_id}; ordinal = key position) — the rows
     * {@code RootRelationalInstanceSetImplementation.primaryKey} reads. */
    private void primaryKeyNodes() {
        for (MetamodelSeeds.SetRow set : MetamodelSeeds.relationalSetsOf(ctx)) {
            List<String> cols = MetamodelSeeds.primaryKeyColumns(ctx, set);
            DatabaseDefinition db = ctx.findDatabase(set.table().database()).orElse(null);
            for (int i = 0; i < cols.size(); i++) {
                String id = "pk:" + set.mappingFqn() + "|" + set.id() + "|" + i;
                rows.node(new RelationalOperation.ColumnRef(set.table().database(),
                        set.table().table(), cols.get(i)), id, null, i,
                        set.table().database(), db, set.table().table(),
                        set.mappingFqn(), set.id());
            }
        }
    }

    /** The relational operation element a property mapping carries;
     * null for the m3 shapes that are not RelationalPropertyMappings
     * (embedded family, local properties — named gaps). */
    private static @com.legend.base.Nullable RelationalOperation opOf(PropertyMapping pm) {
        return switch (pm) {
            case PropertyMapping.Column c ->
                    new RelationalOperation.ColumnRef(c.database(), c.table(), c.column());
            case PropertyMapping.EnumeratedColumn c ->
                    new RelationalOperation.ColumnRef(c.database(), c.table(), c.column());
            case PropertyMapping.Expression e -> e.expression();
            case PropertyMapping.EnumeratedExpression e -> e.expression();
            case PropertyMapping.JoinTerminalColumn j -> new RelationalOperation.JoinNavigation(
                    j.database(), j.joins(), j.terminalColumn());
            case PropertyMapping.Join j -> new RelationalOperation.JoinNavigation(
                    j.database(), j.joins(), null);
            default -> null;
        };
    }

    /** The element DECLARING a mapped property: the nearest class in the
     * set's class hierarchy that declares it, else the association whose
     * end it is (recorded as a Property row), else null (no Property row
     * — the mapping is invisible to a by-name lookup, as in the engine
     * where a nameless property never matches). */
    private @com.legend.base.Nullable String propertyOwner(String classFqn, String prop) {
        java.util.ArrayDeque<String> work = new java.util.ArrayDeque<>();
        Set<String> seen = new LinkedHashSet<>();
        work.add(classFqn);
        while (!work.isEmpty()) {
            String cur = work.poll();
            if (!seen.add(cur)) {
                continue;
            }
            var tc = classOrNull(ctx, cur);
            if (tc == null) {
                continue;
            }
            for (var p : tc.properties()) {
                if (p instanceof com.legend.compiler.element.Property.Stored
                        && p.name().equals(prop)) {
                    return cur;
                }
            }
            work.addAll(tc.superClassFqns());
        }
        var assoc = ctx.findAssociationOf(classFqn, prop).orElse(null);
        if (assoc != null) {
            associationProperties.add(List.of(assoc.qualifiedName(), prop));
            return assoc.qualifiedName();
        }
        return null;
    }

    // ------------------------------------------------------------------
    // the tree

    /** A class's typed declaration, or null when it does not compile — a
     * corpus class whose property types the model does not carry (engine
     * source referencing protocol classes) has no rows; its own reads stay
     * loud where they happen, the seed never fails on its account. */
    static com.legend.compiler.element.@com.legend.base.Nullable TypedClass classOrNull(
            ModelContext ctx, String fqn) {
        try {
            return ctx.findClass(fqn).orElse(null);
        } catch (com.legend.error.LegendCompileException e) {
            return null;
        }
    }

    private List<String> dbs() {
        List<String> fqns = ctx.classifierInstances(com.legend.compiler.element.type.PlatformTypes.DATABASE);
        return fqns == null ? List.of() : fqns;
    }
}
