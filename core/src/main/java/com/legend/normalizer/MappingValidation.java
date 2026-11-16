// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.normalizer;

import com.legend.compiler.ModelBuilder;
import com.legend.error.LegendCompileException;
import com.legend.error.ModelException;
import com.legend.model.ClassDefinition;
import com.legend.model.ClassMapping;
import com.legend.model.LegacyMappingDefinition;
import com.legend.model.PropertyMapping;
import com.legend.model.RelationalOperation;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * VALIDATION BEFORE SYNTHESIS (T4.1 step 5): the user-model checks Phase
 * E used to raise from inside a class's synthesis — a property mapping
 * naming a property its class does not declare, an M2M binding whose
 * {@code [source, target]} route is not benign — run over every set of
 * the pre-passed mapping before any synthesis, with §6's line held:
 * a STRICT build rejects them (the engine rejects them at compile time),
 * a MODULE build records them as per-set poisons and synthesis skips
 * the set (the binding is withheld; the reason raises at use). The
 * roadmap gaps ({@code NotImplementedException}) are not validation:
 * they stay per-set poisons in both builds.
 */
final class MappingValidation {

    private MappingValidation() {}

    /** The invalid sets of {@code md} BY SET ID, in declaration order, with
     * the engine's compile-time rejection of each — recorded, never thrown
     * here: the driver applies strict/module (B4). The key is the set's id,
     * never the object: later construction steps (the multi-hop injection)
     * rebuild the {@code ClassMapping} records, and an identity key lost the
     * verdict there (audit 2026-09-15 P0-1, proven by probe); declaration
     * order makes "a strict build rejects the first" a stable statement
     * (P0-2). Mapping-level errors (duplicate ids) still throw: they are the
     * mapping's, not a set's. */
    static Map<String, ModelException> run(ResolvedMapping r, ModelBuilder model) {
        LegacyMappingDefinition md = r.raw();
        // MAPPING-level errors the engine's compiler raises (R5): a class
        // mapping id taken by two distinct sets across the include closure,
        // (an include listed twice is rejected where the include graph is
        // first walked, StoreSubstitutionRewrite.resolveAllStores). Thrown in BOTH builds — a module build
        // walls the whole mapping through the pre-pass's own catch, exactly
        // as the engine rejects the mapping.
        MappingClosures.Closure closure = MappingClosures.of(model).closure(md.qualifiedName());
        List<String> dupIds = closure.duplicateIds();
        if (!dupIds.isEmpty()) {
            throw new ModelException(LegendCompileException.Phase.MODEL,
                    "Duplicated class mappings found with ID " + dupIds
                    + " in mapping '" + md.qualifiedName() + "'", md.qualifiedName());
        }
        Map<String, ModelException> invalid = new LinkedHashMap<>();
        for (ClassMapping cm : md.classMappings()) {
            try {
                switch (cm) {
                    case ClassMapping.Relational rcm -> {
                        validatePmNames(rcm, model, md);
                        validateColumns(rcm, model);
                    }
                    case ClassMapping.Pure pcm -> validateM2mRoutes(pcm, model, r);
                    case ClassMapping.Union u -> validateUnionMembers(u, r);
                    default -> { }
                }
            } catch (ModelException e) {
                invalid.put(ResolvedMapping.idOf(cm), e);
            }
        }
        return invalid;
    }

    /** An Operation union's every member set is a class mapping of the mapping or its includes, as legend-engine's
     * compiler requires (ClassMappingSecondPassBuilder: "Can't find class mapping"). */
    private static void validateUnionMembers(ClassMapping.Union u, ResolvedMapping r) {
        java.util.Set<String> ids = new java.util.HashSet<>(r.includedSets().keySet());
        for (ClassMapping cm : r.raw().classMappings()) {
            ids.add(ResolvedMapping.idOf(cm));
        }
        for (String member : u.memberSetIds()) {
            if (!ids.contains(member)) {
                throw new ModelException(LegendCompileException.Phase.MAPPING, "Can't find class mapping '" + member
                        + "' in mapping '" + r.raw().qualifiedName() + "'", r.raw().qualifiedName());
            }
        }
    }

    /** Every property mapping names a property its class declares or
     * inherits (engine parity: property lookup walks generalizations). */
    private static void validatePmNames(ClassMapping.Relational rcm,
            ModelBuilder model, LegacyMappingDefinition md) {
        ClassDefinition cd = MissProbe.knownMiss(model.knowledge().hierarchyClass(rcm.className()));
        if (cd == null) return;
        for (PropertyMapping pm : rcm.propertyMappings()) {
            if (pm instanceof PropertyMapping.LocalProperty) continue;
            if (model.knowledge().propertyType(cd, pm.propertyName()) == null) {
                throw new ModelException(LegendCompileException.Phase.MODEL,
                        "PropertyMapping '" + pm.propertyName() + "' references property "
                      + "not declared on class '" + rcm.className() + "'; mapping="
                      + md.qualifiedName());
            }
        }
    }

    /** Every column a relational set names is one its table or view declares (F-U1, 2026-11-14), as legend-engine's
     * compiler requires: {@code HelperRelationalBuilder.getColumn} (line 163, "Can't find column 'C'") is reached for
     * every column of a property mapping (1424), an embedded one (2160), {@code ~primaryKey} (1514) and
     * {@code ~groupBy} (RelationalCompilerExtension.java:314), and a table that is neither a table nor a view of the
     * database is refused first ({@code getRelation}, 276: "Can't find table 'T' in schema 'S' and database 'D'"). A
     * join's own columns are the database's, checked with it ({@code ModelIntegrity.checkDatabase}). */
    private static void validateColumns(ClassMapping.Relational rcm, ModelBuilder model) {
        LegacyMappingDefinition.TableReference main = rcm.mainTable() != null ? rcm.mainTable()
                : MappingNormalizer.inferMainTableQuiet(rcm);
        String scope = main == null ? null : main.database();
        for (PropertyMapping pm : rcm.propertyMappings()) {
            validateColumns(pm, scope, model);
        }
        for (RelationalOperation op : rcm.primaryKey()) {
            validateColumns(op, scope, model);
        }
        for (RelationalOperation op : rcm.groupBy()) {
            validateColumns(op, scope, model);
        }
    }

    /** {@code scope}: the database a column reference with none of its own is read in (the set's main table's). */
    private static void validateColumns(PropertyMapping pm, @com.legend.base.Nullable String scope,
            ModelBuilder model) {
        switch (pm) {
            case PropertyMapping.Column c -> validateColumn(c.database(), c.table(), c.column(), model);
            case PropertyMapping.EnumeratedColumn c -> validateColumn(c.database(), c.table(), c.column(), model);
            case PropertyMapping.Expression e -> validateColumns(e.expression(), scope, model);
            case PropertyMapping.EnumeratedExpression e -> validateColumns(e.expression(), scope, model);
            case PropertyMapping.JoinTerminalColumn j -> validateColumns(j.terminalColumn(), j.database(), model);
            case PropertyMapping.Embedded e -> {
                for (PropertyMapping sub : e.propertyMappings()) {
                    validateColumns(sub, scope, model);
                }
                for (RelationalOperation op : e.primaryKey()) {
                    validateColumns(op, scope, model);
                }
            }
            case PropertyMapping.OtherwiseEmbedded o -> {
                for (PropertyMapping sub : o.embedded()) {
                    validateColumns(sub, scope, model);
                }
                validateColumns(o.fallback(), scope, model);
            }
            case PropertyMapping.LocalProperty l -> validateColumns(l.body(), scope, model);
            // a join's columns are its database's (above); an inline set's are that set's, validated as it
            case PropertyMapping.Join ignored -> { }
            case PropertyMapping.InlineEmbedded ignored -> { }
        }
    }

    private static void validateColumns(RelationalOperation op, @com.legend.base.Nullable String scope,
            ModelBuilder model) {
        if (op instanceof RelationalOperation.ColumnRef cr) {
            validateColumn(cr.databaseName() != null ? cr.databaseName() : scope, cr.table(), cr.column(), model);
            return;
        }
        // a terminal after a join is read in the join's database; every other node's columns are its children's
        // (RelationalOperation.children, exhaustive over the kinds)
        String within = op instanceof RelationalOperation.JoinNavigation j && j.databaseName() != null
                ? j.databaseName() : scope;
        for (RelationalOperation child : op.children()) {
            validateColumns(child, within, model);
        }
    }

    private static void validateColumn(@com.legend.base.Nullable String database, String table, String column,
            ModelBuilder model) {
        if (database == null) {
            throw new IllegalStateException("F-U1: the column " + table + "." + column + " is read in no database:"
                    + " its set has no main table to read it in (never met on the corpus, 2026-11-14)");
        }
        // a database the model does not hold is a missing reference, refused as one where references are checked
        // ("Can't find database 'D'", ReferenceIntegrityTest.aMissingDatabaseIsSaidToBeMissing): no table of it is read
        if (model.findDatabase(database).isEmpty()) {
            return;
        }
        var td = model.findTableDefinition(database, table);
        if (td.isPresent()) {
            if (!declares(td.get(), column)) {
                throw new ModelException(LegendCompileException.Phase.MODEL, "Can't find column '" + column + "'");
            }
            return;
        }
        var view = model.findView(database, table);
        if (view.isPresent()) {
            if (view.get().columnMappings().stream().noneMatch(c -> c.name().equals(column))) {
                throw new ModelException(LegendCompileException.Phase.MODEL, "Can't find column '" + column + "'");
            }
            return;
        }
        int dot = table.indexOf('.');
        throw new ModelException(LegendCompileException.Phase.MODEL, "Can't find table '"
                + (dot < 0 ? table : table.substring(dot + 1)) + "' in schema '"
                + (dot < 0 ? "default" : table.substring(0, dot)) + "' and database '"
                + MappingNormalizer.simpleTypeName(database) + "'");
    }

    /** Its declared columns, and the milestoning columns its milestoning declares (as
     * {@code ModelIntegrity.hasColumn} reads them). */
    private static boolean declares(com.legend.model.DatabaseDefinition.TableDefinition td, String name) {
        if (td.columns().stream().anyMatch(c -> c.name().equals(name))) {
            return true;
        }
        var m = td.milestoning();
        if (m == null) {
            return false;
        }
        var b = m.business();
        var p = m.processing();
        return java.util.Arrays.asList(b == null ? null : b.from(), b == null ? null : b.thru(),
                b == null ? null : b.snapshotDate(), p == null ? null : p.in(), p == null ? null : p.out(),
                p == null ? null : p.snapshotDate()).contains(name);
    }

    /** Every routed M2M binding names a benign route. */
    private static void validateM2mRoutes(ClassMapping.Pure pcm,
            ModelBuilder model, ResolvedMapping md) {
        ClassDefinition tgt = MissProbe.knownMiss(model.knowledge().hierarchyClass(pcm.className()));
        for (ClassMapping.Pure.PropertyBinding pb : pcm.propertyBindings()) {
            M2mRouteGuards.requireBenignRoute(pb, pcm, tgt, md, model);
        }
    }
}
