package com.gs.legend.plan.lowering.relation;

import com.gs.legend.compiler.StoreResolution;
import com.gs.legend.compiler.typed.TypedCollection;
import com.gs.legend.compiler.typed.TypedNewInstance;
import com.gs.legend.compiler.typed.TypedSpec;
import com.gs.legend.model.m3.Type;
import com.gs.legend.plan.lowering.LoweringContext;
import com.gs.legend.plan.lowering.Lowerer;
import com.gs.legend.plan.lowering.scalar.StructLowering;
import com.gs.legend.plan.sql.SqlRelation;
import com.gs.legend.sqlgen.SqlExpr;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Single relational lowering rule for {@link TypedCollection} and
 * {@link TypedNewInstance} literals. Internal type-dispatch decides between
 * a multi-row class-collection {@link SqlRelation.Values} and a primitive /
 * struct-literal scalar wrap — callers see one arm per typed kind, this rule
 * decides on shape from the type-checker's stamped element type
 * (architecture doc §6.2 — dispatch in the rule, not the case statement).
 *
 * <ul>
 *   <li><b>Class collection</b> — {@code coll.info().type() instanceof Type.ClassType}:
 *       lowered to a {@link SqlRelation.Values} table with one row per
 *       instance and one column per top-level property. Property values
 *       lower as scalars (nested classes via {@link SqlExpr.StructLiteral},
 *       collections via {@link SqlExpr.ArrayLiteral}).</li>
 *   <li><b>Scalar collection</b> — anything else (primitives, enums,
 *       heterogeneous): lowered as an {@link SqlExpr.ArrayLiteral} via
 *       {@link StructLowering} and wrapped via
 *       {@link LoweringContext#wrapScalar}, which adds {@code UNNEST(...)}
 *       so the array unfolds into N rows.</li>
 *   <li><b>{@link TypedNewInstance} at relation root</b> — lowered as a
 *       1-row VALUES (symmetric with the singleton-collection case).</li>
 * </ul>
 *
 * <p>Property navigation off the VALUES alias uses the identity
 * {@link com.gs.legend.compiler.StoreResolution} stamped by
 * {@link com.gs.legend.compiler.MappingResolver} — column names are property
 * names. Inline struct-array properties surface as
 * {@link com.gs.legend.compiler.StoreResolution.JoinResolution.StructArrayUnnest}
 * and lower to lateral UNNEST joins via
 * {@link com.gs.legend.plan.lowering.Relations#install}.
 */
public final class CollectionLowering {
    private CollectionLowering() {}

    /**
     * Lower a {@link TypedCollection}. Internal dispatch on element type
     * decides between class-collection (→ VALUES) and scalar-collection
     * (→ scalar wrap). Empty class-collections take the scalar path
     * (no rows, no schema to derive — the wrap fallback yields a sensible
     * one-row null result).
     */
    public static SqlRelation lower(TypedCollection coll, LoweringContext ctx) {
        if (coll.info().type() instanceof Type.ClassType) {
            return buildClassValues(coll, ctx);
        }
        return ctx.toRelation(coll);
    }

    /** Lower a {@link TypedNewInstance} at relation root as a 1-row VALUES. */
    public static SqlRelation lower(TypedNewInstance n, LoweringContext ctx) {
        StoreResolution store = ctx.storeFor(n);
        if (store == null) {
            // Caller stamped no store — fall back to scalar struct wrap.
            // Unusual for a relation-position New; keeps us crash-free for
            // malformed inputs (matches the type-checker's promise that
            // every class-typed node has a store).
            return ctx.toRelation(n);
        }
        return buildValues(List.of(n), store, ctx);
    }

    /**
     * Build VALUES for a class-typed collection. Column schema comes from
     * the {@link StoreResolution} stamped by {@link com.gs.legend.compiler.MappingResolver}
     * (column = property name, in declaration order) — never from instance
     * iteration. An empty class-collection emits VALUES with a single
     * synthetic row of NULLs filtered to zero rows; the schema is intact
     * regardless of value count (architecture: schema follows type, not
     * data).
     */
    private static SqlRelation buildClassValues(TypedCollection coll, LoweringContext ctx) {
        StoreResolution store = ctx.storeFor(coll);
        if (store == null) {
            throw new IllegalStateException(
                    "CollectionLowering: class-typed collection has no StoreResolution stamped; "
                            + "MappingResolver.collectionInstanceStore must run before lowering. "
                            + "type=" + coll.info().type());
        }
        List<TypedNewInstance> instances = new ArrayList<>(coll.values().size());
        for (TypedSpec v : coll.values()) {
            if (!(v instanceof TypedNewInstance ni)) {
                throw new IllegalStateException(
                        "CollectionLowering: class-typed collection contains non-TypedNewInstance "
                                + "value: " + v.getClass().getSimpleName());
            }
            instances.add(ni);
        }
        return buildValues(instances, store, ctx);
    }

    /**
     * Materialise rows: column order from {@code store.propertyToColumn()}
     * (the source-of-truth schema), one row per instance. Missing
     * properties on an instance lower to NULL. Property values lower
     * scalar via {@link Lowerer#lowerScalar}.
     */
    private static SqlRelation buildValues(List<TypedNewInstance> instances,
                                           StoreResolution store,
                                           LoweringContext ctx) {
        List<String> columnNames = new ArrayList<>(store.propertyToColumn().keySet());

        List<List<SqlExpr>> rows = new ArrayList<>(instances.size());
        for (TypedNewInstance ni : instances) {
            Map<String, TypedSpec> propValues = ni.values();
            List<SqlExpr> row = new ArrayList<>(columnNames.size());
            for (String col : columnNames) {
                TypedSpec v = propValues.get(col);
                row.add(v == null ? new SqlExpr.NullLiteral() : Lowerer.lowerScalar(v, ctx));
            }
            rows.add(List.copyOf(row));
        }

        List<SqlRelation.OutputCol> outputs = new ArrayList<>(columnNames.size());
        for (String c : columnNames) outputs.add(new SqlRelation.OutputCol(c, null));

        String alias = ctx.nextAlias();
        return new SqlRelation.Values(rows, columnNames, alias, outputs);
    }
}
