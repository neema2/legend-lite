package com.legend.normalizer;


import com.legend.builtin.Pure;
import com.legend.compiler.ModelBuilder;
import com.legend.error.LegendCompileException;
import com.legend.error.ModelException;
import com.legend.error.NotImplementedException;
import com.legend.model.ClassMapping;
import com.legend.model.DatabaseDefinition;
import com.legend.model.FilterMapping;
import com.legend.model.FilterPointer;
import com.legend.model.LegacyMappingDefinition;
import com.legend.model.PropertyMapping;
import com.legend.model.RelationalOperation;
import com.legend.protocol.spec.AppliedFunction;
import com.legend.protocol.spec.AppliedProperty;
import com.legend.protocol.spec.ColSpec;
import com.legend.protocol.spec.ColSpecArray;
import com.legend.protocol.spec.CString;
import com.legend.protocol.spec.LambdaFunction;
import com.legend.protocol.spec.PackageableElementPtr;
import com.legend.protocol.spec.ValueSpecification;
import com.legend.protocol.spec.Variable;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import com.legend.model.JoinChainElement;
import java.util.LinkedHashSet;

/**
 * Views as standalone RELATION expressions (V1b/V1c): a grouped or
 * filtered view expands to its own {@code tableReference -> [~filter]
 * -> (groupBy | project) -> [distinct]} pipeline, consumed either as a
 * class-mapping source override or as a join-hop target.
 */
final class ViewRelation {

    private ViewRelation() {
    }

    /** A class's MAIN SOURCE as a relation expression: the physical
     * tableReference, or — when the ~mainTable is a VIEW — the view's
     * RELATION expression (the identity-carrying frame; a bare
     * tableReference would name an unknown physical table). */
    static ValueSpecification mainSourceRef(ResolvedMapping md,
            String classFqn, ModelBuilder model) {
        return sourceRefFor(MappingNormalizer.mainTableDefOf(md, classFqn, model),
                model, md);
    }

    /** The relation expression for an already-resolved table reference —
     * the tail of {@link #mainSourceRef} for callers that anchor a class
     * on a table other than its own ~mainTable (embedded-owner anchors). */
    static ValueSpecification sourceRefFor(
            LegacyMappingDefinition.TableReference ref, ModelBuilder model,
            ResolvedMapping md) {
        String table = MappingNormalizer.canonicalTable(ref.table());
        DatabaseDefinition.ViewDefinition view =
                model.findView(ref.database(), table).orElseGet(MissProbe::miss);
        return view != null
                ? viewRelationExpr(view, table, ref.database(), model, md)
                : new AppliedFunction("tableReference", List.of(
                        new PackageableElementPtr(ref.database()),
                        new CString(table)));
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
            ModelBuilder model, ResolvedMapping md) {
        return viewRelationExpr(view, viewName, db, model, md,
                new java.util.HashSet<>());
    }

    private static ValueSpecification viewRelationExpr(
            DatabaseDefinition.ViewDefinition view, String viewName, String db,
            ModelBuilder model, ResolvedMapping md,
            java.util.Set<String> expanding) {
        if (!expanding.add(viewName)) {
            throw new ModelException(LegendCompileException.Phase.NORMALIZE,
                    "view '" + viewName + "' expands through itself (cyclic"
                  + " view-on-view chain); mapping=" + md.qualifiedName());
        }
        String phys = inferViewMainTable(view, viewName, md, model, db);
        Variable r = new Variable("vr");
        // VIEW-ON-VIEW: the inferred root is itself a view — expand it
        // recursively as the SOURCE relation (engine lineage model:
        // orderPnlViewOnView → root → (v) orderPnlView → (t) orderPnlTable;
        // scanRelationsTests.pure golden). The outer view's ColumnRefs
        // spell <innerView>.<declaredCol>, which is exactly the inner
        // relation's output row, so the scope key stays `phys`.
        DatabaseDefinition.ViewDefinition innerView =
                model.findView(db, phys).orElseGet(MissProbe::miss);
        ValueSpecification source = innerView != null
                ? viewRelationExpr(innerView, phys, db, model, md, expanding)
                : new AppliedFunction("tableReference",
                        List.of(new PackageableElementPtr(db), new CString(phys)));
        // JOIN-NAVIGATING view columns (orderPnl: @Join | T.COL) hoist as
        // slots on a real Pipeline — the same pass-2 machinery PM bodies
        // use; the JoinNavigation arm of RelOpTranslator then resolves
        // them through the pipeline view (V1c).
        Pipeline vp = Pipeline.forView(source);
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
                        && GroupBySynthesis.isGroupReducer(fc)) {
                    ValueSpecification selector = RelOpTranslator.translate(
                            fc.args().get(0), scope, null, r, vp.view());
                    Variable vals = new Variable("vals");
                    aggCols.add(new ColSpec(vc.name(),
                            new LambdaFunction(List.of(r), List.of(selector)),
                            new LambdaFunction(List.of(vals),
                                    List.of(new AppliedFunction(
                                            RelOpTranslator.dynaFnName(fc),
                                            List.of(vals))))));
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
                    // H2-LENIENT per-row column under view ~groupBy
                    // (modelJoins LegalEntity_View: name/value bare over
                    // group by ENTITY_ID — H2 1.x returns a per-group
                    // witness): an implicit first()-reduced aggregate —
                    // DB-side (ANY_VALUE), the engine-style text spells
                    // the bare column exactly like the golden
                    ValueSpecification wSel = RelOpTranslator.translate(
                            expr, scope, null, r, vp.view());
                    Variable wVals = new Variable("vals");
                    aggCols.add(new ColSpec(vc.name(),
                            new LambdaFunction(List.of(r), List.of(wSel)),
                            new LambdaFunction(List.of(wVals),
                                    List.of(new AppliedFunction("first",
                                            List.of(wVals))))));
                    continue;
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
            // the view's rows grouped by key EXPRESSIONS: the internal-desugar
            // identity (Pure.Lite.GROUP_BY_COMPUTED_KEYS, batch 5 leg 5d)
            src = new AppliedFunction(com.legend.builtin.Pure.Lite.GROUP_BY_COMPUTED_KEYS, List.of(src,
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

    /** Every {@code <view>.<col>} reference in {@code op} (a filter or
     * join condition written against a VIEW of {@code db}) substituted by
     * the view column's underlying expression, recursively (a view on a
     * view): the flattened set reads the base tables, so its filter must
     * too. Non-view references pass through. */
    static RelationalOperation inlineViewRefs(RelationalOperation op, String db,
            ModelBuilder model) {
        if (op instanceof RelationalOperation.ColumnRef cr) {
            DatabaseDefinition.ViewDefinition view = model.findView(db, cr.table()).orElseGet(MissProbe::miss);
            if (view != null) {
                for (DatabaseDefinition.ViewDefinition.ViewColumnMapping vc : view.columnMappings()) {
                    if (vc.name().equals(cr.column())) {
                        return inlineViewRefs(vc.expression(), db, model);
                    }
                }
            }
            return op;
        }
        return op.mapChildren(x -> inlineViewRefs(x, db, model));
    }

    /**
     * THE FRAME RULE (engine {@code findTableForColumnInAlias}, the view's
     * alias): under a view-backed set the row is the view's declared
     * columns, and a reference to a column of the view's ROOT table
     * resolves to the declared column that carries exactly that column —
     * loud when none does (the engine's "column not in the view"). Every
     * relational operation the set evaluates (property expressions, the
     * ~filter conditions, ~groupBy keys, ~primaryKey) passes through here;
     * a join condition departs from the view by name and needs nothing.
     */
    static RelationalOperation frameRewrite(RelationalOperation op,
            DatabaseDefinition.ViewDefinition view, String viewName, ResolvedMapping md) {
        if (!(op instanceof RelationalOperation.ColumnRef cr)) {
            return op.mapChildren(x -> frameRewrite(x, view, viewName, md));
        }
        String table = MappingNormalizer.canonicalTable(cr.table());
        // a reference to the VIEW itself spells the column as the view
        // declares it (unquoted identifiers are case-insensitive; the frame
        // row carries the declared spelling)
        if (table.equalsIgnoreCase(viewName)) {
            for (DatabaseDefinition.ViewDefinition.ViewColumnMapping vc : view.columnMappings()) {
                if (vc.name().equalsIgnoreCase(cr.column())) {
                    return vc.name().equals(cr.column()) ? cr
                            : new RelationalOperation.ColumnRef(cr.databaseName(), cr.table(), vc.name());
                }
            }
            return cr;
        }
        // a reference to a column the view CARRIES (a declared column whose
        // expression is exactly that table's column) resolves to the declared
        // column — the view's own column mappings are the fact, no root
        // inference; a reference to a table the view reads but a column it
        // does not carry is loud
        boolean readsTable = false;
        for (DatabaseDefinition.ViewDefinition.ViewColumnMapping vc : view.columnMappings()) {
            if (vc.expression() instanceof RelationalOperation.ColumnRef vcr
                    && MappingNormalizer.canonicalTable(vcr.table()).equalsIgnoreCase(table)) {
                readsTable = true;
                if (vcr.column().equalsIgnoreCase(cr.column())) {
                    return new RelationalOperation.ColumnRef(cr.databaseName(), viewName, vc.name());
                }
            }
        }
        if (readsTable) {
            throw new NotImplementedException("column '" + cr.table() + "." + cr.column()
                    + "' is read under view '" + viewName + "', which declares no column"
                    + " carrying it; mapping=" + md.qualifiedName());
        }
        return cr;
    }

    /** {@link #frameRewrite} when {@code (db, table)} names a view — the
     * set's main relation is its frame; the operation unchanged otherwise. */
    static RelationalOperation frameRewriteIfView(RelationalOperation op, String db,
            String table, ResolvedMapping md, ModelBuilder model) {
        DatabaseDefinition.ViewDefinition view = model.findView(db, table).orElseGet(MissProbe::miss);
        RelationalOperation out = view == null ? op : frameRewrite(op, view, table, md);
        return declaredSpelling(out, db, model);
    }

    /** Every reference to a VIEW's column — the departing frame's or a
     * target view's — spelled as the view declares it (unquoted identifiers
     * are case-insensitive; a view's row carries the declared spelling). */
    static RelationalOperation declaredSpelling(RelationalOperation op, String db,
            ModelBuilder model) {
        if (op instanceof RelationalOperation.ColumnRef cr) {
            String vdb = cr.databaseName() != null && !cr.databaseName().isEmpty()
                    ? cr.databaseName() : db;
            DatabaseDefinition.ViewDefinition v = model.findView(vdb,
                    MappingNormalizer.canonicalTable(cr.table())).orElseGet(MissProbe::miss);
            if (v == null) {
                return cr;
            }
            for (DatabaseDefinition.ViewDefinition.ViewColumnMapping vc : v.columnMappings()) {
                if (vc.name().equalsIgnoreCase(cr.column())) {
                    return vc.name().equals(cr.column()) ? cr
                            : new RelationalOperation.ColumnRef(cr.databaseName(), cr.table(), vc.name());
                }
            }
            return cr;
        }
        return op.mapChildren(x -> declaredSpelling(x, db, model));
    }

    /** The set rewritten through its view's frame: property mappings,
     * ~groupBy keys and ~primaryKey name the view's declared columns. */
    static ClassMapping.Relational throughFrame(ClassMapping.Relational rcm,
            DatabaseDefinition.ViewDefinition view, String viewName, ResolvedMapping md) {
        List<PropertyMapping> pms = new ArrayList<>(rcm.propertyMappings().size());
        for (PropertyMapping pm : rcm.propertyMappings()) {
            pms.add(pmThroughFrame(pm, view, viewName, md));
        }
        List<RelationalOperation> groupBy = new ArrayList<>(rcm.groupBy().size());
        for (RelationalOperation k : rcm.groupBy()) {
            groupBy.add(frameRewrite(k, view, viewName, md));
        }
        List<RelationalOperation> pk = new ArrayList<>(rcm.primaryKey().size());
        for (RelationalOperation k : rcm.primaryKey()) {
            pk.add(frameRewrite(k, view, viewName, md));
        }
        return new ClassMapping.Relational(rcm.className(), rcm.setId(), rcm.extendsSetId(),
                rcm.root(), rcm.mainTable(), rcm.filter(), rcm.distinct(), groupBy, pk, pms,
                null, rcm.propertyTargetSets(), rcm.aggregation());
    }

    private static PropertyMapping pmThroughFrame(PropertyMapping pm,
            DatabaseDefinition.ViewDefinition view, String viewName, ResolvedMapping md) {
        return switch (pm) {
            case PropertyMapping.Column col -> {
                RelationalOperation.ColumnRef r = (RelationalOperation.ColumnRef) frameRewrite(
                        new RelationalOperation.ColumnRef(col.database(), col.table(), col.column()),
                        view, viewName, md);
                yield new PropertyMapping.Column(col.propertyName(), col.database(), r.table(), r.column());
            }
            case PropertyMapping.EnumeratedColumn ec -> {
                RelationalOperation.ColumnRef r = (RelationalOperation.ColumnRef) frameRewrite(
                        new RelationalOperation.ColumnRef(ec.database(), ec.table(), ec.column()),
                        view, viewName, md);
                yield new PropertyMapping.EnumeratedColumn(ec.propertyName(), ec.enumMappingId(),
                        ec.database(), r.table(), r.column());
            }
            case PropertyMapping.Expression ex -> new PropertyMapping.Expression(ex.propertyName(),
                    frameRewrite(ex.expression(), view, viewName, md));
            case PropertyMapping.LocalProperty lp -> new PropertyMapping.LocalProperty(
                    lp.propertyName(), lp.type(), lp.multiplicity(),
                    pmThroughFrame(lp.body(), view, viewName, md));
            case PropertyMapping.Embedded em -> {
                List<PropertyMapping> subs = new ArrayList<>(em.propertyMappings().size());
                for (PropertyMapping s : em.propertyMappings()) {
                    subs.add(pmThroughFrame(s, view, viewName, md));
                }
                List<RelationalOperation> epk = new ArrayList<>(em.primaryKey().size());
                for (RelationalOperation k : em.primaryKey()) {
                    epk.add(frameRewrite(k, view, viewName, md));
                }
                yield new PropertyMapping.Embedded(em.propertyName(), subs, epk);
            }
            // joins depart from the view by name (their conditions are
            // translated against the frame row at emission); the other
            // kinds carry no column of the root
            default -> pm;
        };
    }

    static String inferViewMainTable(DatabaseDefinition.ViewDefinition view,
                                            String viewName, ResolvedMapping md) {
        return inferViewMainTable(view, viewName, md, null, null);
    }

    static String inferViewMainTable(DatabaseDefinition.ViewDefinition view,
                                            String viewName, ResolvedMapping md,
                                            @com.legend.base.Nullable ModelBuilder model, @com.legend.base.Nullable String dbFqn) {
        Set<String> tables = new LinkedHashSet<>();
        for (DatabaseDefinition.ViewDefinition.ViewColumnMapping vc : view.columnMappings()) {
            RelationalOperation expr = vc.expression();
            List<JoinChainEmission.JoinNavSpec> navs = new ArrayList<>();
            JoinChainEmission.collectJoinNavigations(expr, navs);
            if (!navs.isEmpty()) continue;   // joined column — not the view's root table
            RelOpTranslator.collectTablesIn(expr, tables);
        }
        if (tables.isEmpty() && model != null && dbFqn != null) {
            // JOIN-ONLY view (every column @J|table.COL): the root is the
            // first chain's first JOIN's OTHER side — the join connects
            // the view's root to the terminal table the columns read
            // (PersonViewWithDistinct: @PersonWithPersonView joins
            // personTable to personViewWithGroupBy; columns read
            // personTable, so the root is personViewWithGroupBy)
            String inferred = joinOnlyViewRoot(view, model, dbFqn);
            if (inferred != null) {
                return inferred;
            }
        }
        if (tables.isEmpty()) {
            throw new ModelException(LegendCompileException.Phase.NORMALIZE,
                    "View '" + viewName + "': cannot infer underlying main table — no "
                  + "non-join column references found; mapping=" + md.qualifiedName());
        }
        if (tables.size() > 1) {
            throw new ModelException(LegendCompileException.Phase.NORMALIZE,
                    "View '" + viewName + "' references multiple root tables " + tables
                  + "; a view must resolve to a single root table. Mapping="
                  + md.qualifiedName());
        }
        return tables.iterator().next();
    }

    /** The root of a JOIN-ONLY view: the first chain's first join's
     * condition tables MINUS the terminal tables the columns read — a
     * single remainder is the root (table or view); null keeps the
     * caller's loud wall. */
    private static @com.legend.base.Nullable String joinOnlyViewRoot(DatabaseDefinition.ViewDefinition view,
            ModelBuilder model, String dbFqn) {
        Set<String> terminals = new LinkedHashSet<>();
        JoinChainElement first = null;
        for (DatabaseDefinition.ViewDefinition.ViewColumnMapping vc : view.columnMappings()) {
            if (!(vc.expression() instanceof RelationalOperation.JoinNavigation jn)
                    || jn.chain().isEmpty()) {
                continue;
            }
            if (first == null) {
                first = jn.chain().get(0);
            }
            if (jn.terminal() != null) {
                RelOpTranslator.collectTablesIn(jn.terminal(), terminals);
            }
        }
        if (first == null) {
            return null;
        }
        final JoinChainElement head = first;
        String db = head.databaseName() != null ? head.databaseName() : dbFqn;
        var found = model.findDatabase(db).orElseThrow(() -> MissProbe.neverFired("ViewRelation#6"));
        if (found == null) {
            return null;
        }
        var jd = found.joins().stream()
                .filter(j -> j.name().equals(head.joinName())).findFirst()
                .orElseThrow(() -> MissProbe.neverFired("ViewRelation#7"));
        if (jd == null) {
            return null;
        }
        Set<String> condTables = new LinkedHashSet<>();
        RelOpTranslator.collectTablesIn(jd.operation(), condTables);
        condTables.removeAll(terminals);
        return condTables.size() == 1 ? condTables.iterator().next() : null;
    }

}
