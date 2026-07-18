// SPDX-License-Identifier: Apache-2.0

package com.legend.normalizer;

import com.legend.error.NotImplementedException;
import com.legend.model.ClassMapping;
import com.legend.model.LegacyMappingDefinition;
import com.legend.model.PropertyMapping;
import com.legend.model.RelationalOperation;
import com.legend.model.spec.AppliedFunction;
import com.legend.model.spec.AppliedProperty;
import com.legend.model.spec.ColSpec;
import com.legend.model.spec.ColSpecArray;
import com.legend.model.spec.LambdaFunction;
import com.legend.model.spec.ValueSpecification;
import com.legend.model.spec.Variable;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * ~groupBy synthesis (doc §5.3.5) + stage 2: class-typed Join PMs on a
 * grouped set navigate the GROUPED relation on its key columns. Split
 * from MappingNormalizer (file-size seam, audit-19 window).
 */
final class GroupBySynthesis {

    private GroupBySynthesis() {
    }

    /**
     * The grouped relation's OUTPUT name for each ColumnRef ~groupBy key
     * of the main table — the SAME naming applyGroupBy projects (a key a
     * non-aggregate PM claims takes the PM's name; unclaimed keys are
     * {@code k<i>__<base>}). Only plain column keys are navigable.
     */
    static Map<String, String> groupedKeyColumnNames(
            ClassMapping.Relational rcm, String mainTable) {
        List<RelationalOperation> keyOps = rcm.groupBy();
        String[] keyNames = new String[keyOps.size()];
        for (int i = 0; i < keyOps.size(); i++) {
            String base = keyBaseName(keyOps.get(i));
            keyNames[i] = base != null ? "k" + i + "__" + base : "k" + i;
        }
        Set<Integer> claimed = new HashSet<>();
        for (PropertyMapping pm : rcm.propertyMappings()) {
            if (pm instanceof PropertyMapping.Join || isAggregatePm(pm)) {
                continue;
            }
            RelationalOperation pmOp = pmAsRelationalOp(pm);
            if (pmOp == null) {
                continue;
            }
            for (int i = 0; i < keyOps.size(); i++) {
                if (!claimed.contains(i) && groupByOpsMatch(pmOp, keyOps.get(i))) {
                    claimed.add(i);
                    keyNames[i] = pm.propertyName();
                    break;
                }
            }
        }
        Map<String, String> out = new LinkedHashMap<>();
        for (int i = 0; i < keyOps.size(); i++) {
            if (keyOps.get(i) instanceof RelationalOperation.ColumnRef cr
                    && (cr.table() == null || cr.table().equals(mainTable))) {
                out.put(cr.column(), keyNames[i]);
            }
        }
        return out;
    }

    /**
     * Rewrite the just-appended navigate's condition: source-side reads
     * ({@code $s.<physicalCol>}) become the grouped output columns. A
     * source read outside the key map is LOUD — never a silent miss.
     */
    static ValueSpecification renameGroupedNavCond(
            ValueSpecification expr, Map<String, String> groupedNames,
            String propName, LegacyMappingDefinition md) {
        if (!(expr instanceof AppliedFunction nav
                && nav.function().equals("legacyNavigate")
                && nav.parameters().size() == 4
                && nav.parameters().get(3) instanceof LambdaFunction cond
                && cond.parameters().size() == 2)) {
            throw new NotImplementedException("~groupBy Join PM '" + propName
                    + "' did not emit a single navigate step; mapping="
                    + md.qualifiedName());
        }
        String srcVar = cond.parameters().get(0).name();
        List<ValueSpecification> newBody = new ArrayList<>();
        for (ValueSpecification b : cond.body()) {
            newBody.add(renameSourceReads(b, srcVar, groupedNames, propName, md));
        }
        List<ValueSpecification> ps = new ArrayList<>(nav.parameters());
        ps.set(3, new LambdaFunction(cond.parameters(), newBody));
        return new AppliedFunction(nav.function(), ps);
    }

    private static ValueSpecification renameSourceReads(ValueSpecification v,
            String srcVar, Map<String, String> groupedNames, String propName,
            LegacyMappingDefinition md) {
        if (v instanceof AppliedProperty ap
                && ap.receiver() instanceof Variable rv
                && rv.name().equals(srcVar)) {
            String renamed = groupedNames.get(ap.property());
            if (renamed == null) {
                throw new NotImplementedException("~groupBy Join PM '"
                        + propName + "' joins on column '" + ap.property()
                        + "', which is not a ~groupBy key — a grouped set"
                        + " cannot be navigated on a non-key; mapping="
                        + md.qualifiedName());
            }
            return new AppliedProperty(rv, renamed);
        }
        return switch (v) {
            case AppliedFunction af -> new AppliedFunction(af.function(),
                    af.parameters().stream().map(x -> renameSourceReads(
                            x, srcVar, groupedNames, propName, md)).toList());
            case AppliedProperty ap2 -> new AppliedProperty(
                    renameSourceReads(ap2.receiver(), srcVar, groupedNames,
                            propName, md), ap2.property());
            default -> v;
        };
    }

    static ValueSpecification applyGroupBy(ValueSpecification source,
                                                  ClassMapping.Relational rcm,
                                                  Variable rowBind, String mainTable,
                                                  Pipeline p, LegacyMappingDefinition md) {
        Map<String, ValueSpecification> scope = new LinkedHashMap<>();
        scope.put(mainTable, rowBind);
        MappingNormalizer.seedAliasScope(scope, p, rowBind, mainTable);

        List<RelationalOperation> keyOps = rcm.groupBy();
        String[] keyNames = new String[keyOps.size()];
        for (int i = 0; i < keyOps.size(); i++) {
            String base = keyBaseName(keyOps.get(i));
            keyNames[i] = base != null ? "k" + i + "__" + base : "k" + i;
        }
        // Walk PMs once: align non-agg PMs to keys by structural
        // equality, collect aggregate PMs, reject orphan formulas.
        Set<Integer> claimedKeys = new HashSet<>();
        List<PropertyMapping> aggPms = new ArrayList<>();
        for (PropertyMapping pm : rcm.propertyMappings()) {
            if (isAggregatePm(pm)) { aggPms.add(pm); continue; }
            // A JOIN PM (class-typed navigation) is not part of the grouped
            // ROW — the engine navigates grouped sets through the join
            // machinery, never the grouped projection. Stage 1 (audit-17
            // bucket analysis): WITHHOLD the property instead of sinking
            // the whole class mapping — queries that never touch it run;
            // touching it raises the ordinary not-mapped error, loud.
            if (pm instanceof PropertyMapping.Join) {
                continue;
            }
            RelationalOperation pmOp = pmAsRelationalOp(pm);
            if (pmOp == null) {
                throw new NotImplementedException(
                        "PropertyMapping '" + pm.getClass().getSimpleName()
                      + "' for property '" + pm.propertyName()
                      + "' is not supported under ~groupBy (only Column, Expression, "
                      + "JoinTerminalColumn, and aggregate Expression PMs are allowed). "
                      + "Mapping=" + md.qualifiedName());
            }
            int matchIdx = -1;
            for (int i = 0; i < keyOps.size(); i++) {
                if (!claimedKeys.contains(i) && groupByOpsMatch(pmOp, keyOps.get(i))) {
                    matchIdx = i; break;
                }
            }
            if (matchIdx < 0) {
                throw new NotImplementedException(
                        "PM '" + pm.propertyName() + "' is a per-row expression that is "
                      + "neither an aggregate nor a declared ~groupBy key; ~groupBy "
                      + "mappings forbid per-row formulas outside the key list. Mapping="
                      + md.qualifiedName());
            }
            claimedKeys.add(matchIdx);
            keyNames[matchIdx] = pm.propertyName();
        }
        // Build key ColSpecs.
        List<ColSpec> keyCols = new ArrayList<>(keyOps.size());
        for (int i = 0; i < keyOps.size(); i++) {
            ValueSpecification keyValue = RelOpTranslator.translate(keyOps.get(i), scope, null,
                    rowBind, p.view());
            keyCols.add(new ColSpec(keyNames[i],
                    new LambdaFunction(List.of(rowBind), List.of(keyValue)), null));
        }
        // Build aggregate ColSpecs with fn1 (selector) + fn2 (aggregate).
        List<ColSpec> aggCols = new ArrayList<>(aggPms.size());
        for (PropertyMapping pm : aggPms) {
            RelationalOperation.FunctionCall fc = (RelationalOperation.FunctionCall)
                    ((PropertyMapping.Expression) pm).expression();
            if (fc.args().size() != 1) {
                throw new NotImplementedException(
                        "Aggregate PM '" + pm.propertyName() + "' uses '" + fc.name()
                      + "' with " + fc.args().size() + " args; only single-argument "
                      + "aggregates lift to the two-stage AggColSpec form. Mapping="
                      + md.qualifiedName());
            }
            ValueSpecification selector = RelOpTranslator.translate(fc.args().get(0), scope, null,
                    rowBind, p.view());
            Variable vals = new Variable("vals");
            ValueSpecification aggBody = new AppliedFunction(
                    fc.name(), List.of(vals));
            aggCols.add(new ColSpec(pm.propertyName(),
                    new LambdaFunction(List.of(rowBind), List.of(selector)),
                    new LambdaFunction(List.of(vals), List.of(aggBody))));
        }
        return new AppliedFunction("groupBy", List.of(source,
                new ColSpecArray(keyCols), new ColSpecArray(aggCols)));
    }

    static boolean isAggregatePm(PropertyMapping pm) {
        if (pm instanceof PropertyMapping.Expression expr
                && expr.expression() instanceof RelationalOperation.FunctionCall fc) {
            return MappingNormalizer.AGGREGATE_FNS.contains(fc.name());
        }
        return false;
    }

    static RelationalOperation pmAsRelationalOp(PropertyMapping pm) {
        if (pm instanceof PropertyMapping.Column col) {
            return new RelationalOperation.ColumnRef(col.database(), col.table(), col.column());
        }
        if (pm instanceof PropertyMapping.Expression expr) {
            return expr.expression();
        }
        if (pm instanceof PropertyMapping.JoinTerminalColumn jtc) {
            return new RelationalOperation.JoinNavigation(jtc.database(),
                    jtc.joins(), jtc.terminalColumn());
        }
        return null;
    }

    /**
     * Structural equality MODULO the database qualifier: a PM rewritten
     * through a view carries the mapping's db FQN while the view's own
     * ~groupBy keys parse unqualified — same table+column IS the same key.
     */
    static boolean groupByOpsMatch(RelationalOperation a, RelationalOperation b) {
        if (a instanceof RelationalOperation.ColumnRef ca
                && b instanceof RelationalOperation.ColumnRef cb) {
            // The qualifier is ignored only when ONE side lacks it (the
            // view-rewrite stamps the mapping's db onto PMs while view keys
            // parse unqualified); two EXPLICIT different dbs never match
            // (same-named tables across included dbs; audit).
            boolean dbOk = ca.databaseName() == null || cb.databaseName() == null
                    || ca.databaseName().equals(cb.databaseName());
            return dbOk && Objects.equals(ca.table(), cb.table())
                    && Objects.equals(ca.column(), cb.column());
        }
        return Objects.equals(a, b);
    }

    static String keyBaseName(RelationalOperation op) {
        if (op instanceof RelationalOperation.ColumnRef cr) return cr.column();
        if (op instanceof RelationalOperation.TargetColumnRef tr) return tr.column();
        return null;
    }
}
