package com.legend.normalizer;

import com.legend.compiler.ModelBuilder;
import com.legend.error.LegendCompileException;
import com.legend.error.ModelException;
import com.legend.error.NotImplementedException;
import com.legend.model.DatabaseDefinition;
import com.legend.model.FilterMapping;
import com.legend.model.FilterPointer;
import com.legend.model.LegacyMappingDefinition;
import com.legend.model.RelationalOperation;
import com.legend.model.spec.AppliedFunction;
import com.legend.model.spec.AppliedProperty;
import com.legend.model.spec.ColSpec;
import com.legend.model.spec.ColSpecArray;
import com.legend.model.spec.CString;
import com.legend.model.spec.LambdaFunction;
import com.legend.model.spec.PackageableElementPtr;
import com.legend.model.spec.ValueSpecification;
import com.legend.model.spec.Variable;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Views as standalone RELATION expressions (V1b/V1c): a grouped or
 * filtered view expands to its own {@code tableReference -> [~filter]
 * -> (groupBy | project) -> [distinct]} pipeline, consumed either as a
 * class-mapping source override or as a join-hop target.
 */
final class ViewRelation {

    private ViewRelation() {
    }

    /**
     * A VIEW as a standalone RELATION expression — the join-hop target:
     * {@code tableReference(physRoot) -> [~filter] -> (groupBy | project)
     * -> [distinct]}. Output column names are the view's declared column
     * names, so join conditions and terminal reads spelling
     * {@code <view>.<col>} resolve against this row.
     */
    static ValueSpecification viewRelationExpr(
            DatabaseDefinition.ViewDefinition view, String viewName, String db,
            ModelBuilder model, LegacyMappingDefinition md) {
        return viewRelationExpr(view, viewName, db, model, md,
                new java.util.HashSet<>());
    }

    private static ValueSpecification viewRelationExpr(
            DatabaseDefinition.ViewDefinition view, String viewName, String db,
            ModelBuilder model, LegacyMappingDefinition md,
            java.util.Set<String> expanding) {
        if (!expanding.add(viewName)) {
            throw new ModelException(LegendCompileException.Phase.NORMALIZE,
                    "view '" + viewName + "' expands through itself (cyclic"
                  + " view-on-view chain); mapping=" + md.qualifiedName());
        }
        String phys = MappingNormalizer.inferViewMainTable(view, viewName, md);
        Variable r = new Variable("vr");
        // VIEW-ON-VIEW: the inferred root is itself a view — expand it
        // recursively as the SOURCE relation (engine lineage model:
        // orderPnlViewOnView → root → (v) orderPnlView → (t) orderPnlTable;
        // scanRelationsTests.pure golden). The outer view's ColumnRefs
        // spell <innerView>.<declaredCol>, which is exactly the inner
        // relation's output row, so the scope key stays `phys`.
        DatabaseDefinition.ViewDefinition innerView =
                model.findView(db, phys).orElse(null);
        ValueSpecification source = innerView != null
                ? viewRelationExpr(innerView, phys, db, model, md, expanding)
                : new AppliedFunction("tableReference",
                        List.of(new PackageableElementPtr(db), new CString(phys)));
        // JOIN-NAVIGATING view columns (orderPnl: @Join | T.COL) hoist as
        // slots on a real Pipeline — the same pass-2 machinery PM bodies
        // use; the JoinNavigation arm of RelOpTranslator then resolves
        // them through the pipeline view (V1c).
        Pipeline vp = new Pipeline(source, null);
        for (DatabaseDefinition.ViewDefinition.ViewColumnMapping vc0
                : view.columnMappings()) {
            List<JoinChainEmission.JoinNavSpec> navs0 = new ArrayList<>();
            JoinChainEmission.collectJoinNavigations(vc0.expression(), navs0);
            for (JoinChainEmission.JoinNavSpec nav : navs0) {
                JoinChainEmission.emitJoinChain(vp, nav.chain(), nav.chainDb(),
                        vc0.name(), null, db, phys, r, model, md,
                        /*classTypedTerminus*/ false);
            }
        }
        // The ~filter condition and ~groupBy key ops can navigate joins
        // too — hoist theirs the SAME way, BEFORE the pipeline expr is
        // taken: an un-hoisted chain used to fall back to a flattened
        // alias and the terminal read silently bound through another
        // chain's slot (audit 18 finding 2; slotFor is loud now).
        DatabaseDefinition.FilterDefinition viewFilterDef = null;
        if (view.filter() != null) {
            if (!(view.filter() instanceof FilterMapping.Direct direct)) {
                throw new NotImplementedException(
                        "view '" + viewName + "' used as a join target has a"
                      + " join-mediated ~filter; only direct view filters"
                      + " expand as relations. mapping=" + md.qualifiedName());
            }
            String dbFqn = switch (direct.filter()) {
                case FilterPointer.Cross c -> c.db();
                case FilterPointer.Local l -> db;
            };
            viewFilterDef = model.findFilter(
                    dbFqn, direct.filter().name()).orElseThrow(() ->
                    new ModelException(
                            LegendCompileException.Phase.NORMALIZE,
                            "~filter '" + direct.filter().name() + "' of view '"
                          + viewName + "' not found in db '" + dbFqn
                          + "'; mapping=" + md.qualifiedName()));
            List<JoinChainEmission.JoinNavSpec> navs = new ArrayList<>();
            JoinChainEmission.collectJoinNavigations(
                    viewFilterDef.condition(), navs);
            for (JoinChainEmission.JoinNavSpec nav : navs) {
                JoinChainEmission.emitJoinChain(vp, nav.chain(), nav.chainDb(),
                        viewName + "_filter", null, db, phys, r, model, md,
                        /*classTypedTerminus*/ false);
            }
        }
        for (RelationalOperation keyOp : view.groupByColumns()) {
            List<JoinChainEmission.JoinNavSpec> navs = new ArrayList<>();
            JoinChainEmission.collectJoinNavigations(keyOp, navs);
            for (JoinChainEmission.JoinNavSpec nav : navs) {
                JoinChainEmission.emitJoinChain(vp, nav.chain(), nav.chainDb(),
                        viewName + "_groupBy", null, db, phys, r, model, md,
                        /*classTypedTerminus*/ false);
            }
        }
        ValueSpecification src = vp.expr;
        Map<String, ValueSpecification> scope = new LinkedHashMap<>();
        scope.put(phys, r);
        MappingNormalizer.seedAliasScope(scope, vp, r, phys);
        if (viewFilterDef != null) {
            ValueSpecification cond = RelOpTranslator.translate(
                    viewFilterDef.condition(), scope, null, r, vp.view());
            src = new AppliedFunction("filter", List.of(src,
                    new LambdaFunction(List.of(r), List.of(cond))));
        }
        boolean syntheticKeyCols = false;
        if (!view.groupByColumns().isEmpty()) {
            List<RelationalOperation> keyOps = view.groupByColumns();
            boolean[] claimed = new boolean[keyOps.size()];
            List<ColSpec> keyCols = new ArrayList<>();
            List<ColSpec> aggCols = new ArrayList<>();
            for (DatabaseDefinition.ViewDefinition.ViewColumnMapping vc : view.columnMappings()) {
                RelationalOperation expr = vc.expression();
                if (expr instanceof RelationalOperation.FunctionCall fc
                        && MappingNormalizer.AGGREGATE_FNS.contains(fc.name()) && fc.args().size() == 1) {
                    ValueSpecification selector = RelOpTranslator.translate(
                            fc.args().get(0), scope, null, r, vp.view());
                    Variable vals = new Variable("vals");
                    aggCols.add(new ColSpec(vc.name(),
                            new LambdaFunction(List.of(r), List.of(selector)),
                            new LambdaFunction(List.of(vals),
                                    List.of(new AppliedFunction(fc.name(), List.of(vals))))));
                    continue;
                }
                int match = -1;
                for (int i = 0; i < keyOps.size(); i++) {
                    if (!claimed[i] && GroupBySynthesis.groupByOpsMatch(expr, keyOps.get(i))) {
                        match = i;
                        break;
                    }
                }
                if (match < 0) {
                    throw new NotImplementedException(
                            "view '" + viewName + "' column '" + vc.name()
                          + "' is a per-row expression that is neither an"
                          + " aggregate nor a declared ~groupBy key."
                          + " mapping=" + md.qualifiedName());
                }
                claimed[match] = true;
                ValueSpecification keyValue = RelOpTranslator.translate(expr, scope,
                        null, r, vp.view());
                keyCols.add(new ColSpec(vc.name(),
                        new LambdaFunction(List.of(r), List.of(keyValue)), null));
            }
            for (int i = 0; i < keyOps.size(); i++) {
                if (!claimed[i]) {
                    // grouped-but-unprojected key: keep the grouping exact
                    ValueSpecification keyValue = RelOpTranslator.translate(keyOps.get(i),
                            scope, null, r, vp.view());
                    keyCols.add(new ColSpec("k" + i,
                            new LambdaFunction(List.of(r), List.of(keyValue)), null));
                    syntheticKeyCols = true;
                }
            }
            src = new AppliedFunction("groupBy", List.of(src,
                    new ColSpecArray(keyCols), new ColSpecArray(aggCols)));
        } else {
            List<ColSpec> cols = new ArrayList<>(view.columnMappings().size());
            for (DatabaseDefinition.ViewDefinition.ViewColumnMapping vc : view.columnMappings()) {
                ValueSpecification val = RelOpTranslator.translate(vc.expression(), scope,
                        null, r, vp.view());
                cols.add(new ColSpec(vc.name(),
                        new LambdaFunction(List.of(r), List.of(val)), null));
            }
            src = new AppliedFunction("project", List.of(src, new ColSpecArray(cols)));
        }
        if (view.distinct()) {
            // ~distinct is over the view's DECLARED columns (the engine
            // projects only those, pureToSQLQuery.pure:5147-5157). The
            // grouped path may carry synthetic k<i> columns keeping the
            // grouping exact — distinct over rows already unique per
            // keys+aggs removes NOTHING; drop the synthetic columns first
            // so groups agreeing on declared columns collapse (audit 18
            // finding 4).
            if (syntheticKeyCols) {
                Variable vd = new Variable("vd");
                List<ColSpec> declared = new ArrayList<>(view.columnMappings().size());
                for (DatabaseDefinition.ViewDefinition.ViewColumnMapping vc
                        : view.columnMappings()) {
                    declared.add(new ColSpec(vc.name(),
                            new LambdaFunction(List.of(vd),
                                    List.of(new AppliedProperty(vd, vc.name()))), null));
                }
                src = new AppliedFunction("project",
                        List.of(src, new ColSpecArray(declared)));
            }
            src = new AppliedFunction("distinct", List.of(src));
        }
        return src;
    }

    /** The pure kind of {@code col} on {@code table}: a physical column's
     * kind directly, or a VIEW's declared column resolved through its
     * plain ColumnRef expression — recursively, since the referenced
     * relation may itself be a view (view-on-view:
     * orderNegativePnlViewOnView.ORDER_ID → orderPnlView.ORDER_ID →
     * orderTable.ID). Computed view columns yield null — the caller's
     * loud wall names them. */
    static String columnPureKind(String db, String table, String col,
            ModelBuilder model) {
        return columnPureKind(db, table, col, model,
                new java.util.HashSet<>());
    }

    private static String columnPureKind(String db, String table, String col,
            ModelBuilder model, java.util.Set<String> seen) {
        if (!seen.add(db + "@" + table + "." + col)) {
            return null;
        }
        DatabaseDefinition.ColumnDefinition cd =
                MappingNormalizer.findPhysicalColumn(db, table, col, model);
        if (cd != null) {
            return MappingNormalizer.pureKindOf(cd.dataType());
        }
        DatabaseDefinition.ViewDefinition view =
                model.findView(db, table).orElse(null);
        if (view == null) {
            return null;
        }
        for (DatabaseDefinition.ViewDefinition.ViewColumnMapping vc
                : view.columnMappings()) {
            if (vc.name().equals(col) && vc.expression()
                    instanceof RelationalOperation.ColumnRef cr) {
                String cdb = cr.databaseName() != null ? cr.databaseName() : db;
                return columnPureKind(cdb, cr.table(), cr.column(), model, seen);
            }
        }
        return null;
    }
}
