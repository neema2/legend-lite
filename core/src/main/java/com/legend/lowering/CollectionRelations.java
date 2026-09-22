// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0
package com.legend.lowering;

import com.legend.compiler.element.type.Type;
import com.legend.compiler.spec.typed.TypedCollectionRelation;
import com.legend.compiler.spec.typed.TypedMap;
import com.legend.compiler.spec.typed.TypedNativeCall;
import com.legend.compiler.spec.typed.TypedSpec;
import com.legend.lowering.Resolvers.ColumnResolver;
import com.legend.sql.OutputCol;
import com.legend.sql.SqlExpr;
import com.legend.sql.SqlFn;
import com.legend.sql.SqlSelect;
import com.legend.sql.SqlSource;

import java.util.ArrayList;
import java.util.List;

/**
 * Collection VALUES in RELATION position (extracted from {@link Lowerer}
 * at the file-size guardrail): a collection that travels as a list value
 * becomes rows through {@code UNNEST}, in list order.
 *
 * <ul>
 *   <li>{@link #flatten}: {@code relation::variant::flatten(collection,
 *       ~col)} — the single named column (real flatten.pure semantics).</li>
 *   <li>{@link #explode}: a CLASS-typed collection value — {@code range(n)
 *       ->map(i|…)->zip($scores)} (a {@code Pair[*]} list computed in the
 *       database: list_zip over list_transform over range) feeding
 *       {@code ->project([col(p|$p.first,'name'), …])} — is the relation
 *       of its elements' LAYOUT fields (ClassLayouts: the model's declared
 *       stored properties, type arguments substituted), one row per
 *       element, so the project's column lambdas read {@code $p.first} as
 *       the column {@code first} exactly as over a store row. The engine
 *       builds this TDS in memory (tds.pure project over instances); here
 *       the database computes both the list and its rows (batch 76).</li>
 * </ul>
 */
final class CollectionRelations {
    private CollectionRelations() {
    }

    /** {@code relation::variant::flatten(collection, ~col)}: the collection
     * UNNESTs as the single column. Inside lateral(...) the collection may
     * read the OUTER row (the enclosing-resolver channel); otherwise it must
     * be self-contained. */
    static SqlSelect flatten(Lowerer lo, TypedCollectionRelation cr,
            List<ColumnResolver> outerScopes) {
        SqlExpr value = lo.scalar(cr.value(), (v, name) -> {
            for (var outer : outerScopes) {
                if (Resolvers.Resolution.attempt(() -> outer.resolve(v, name))
                        instanceof Resolvers.Resolution.Resolved o) {
                    return o.expr();
                }
            }
            throw new IllegalStateException("collection-relation value must"
                    + " be self-contained, referenced column: " + name);
        });
        Type elem = (Type.requireRelationSchema(cr.info().type()))
                .columns().get(0).type();
        SqlExpr list = elem instanceof Type.ClassType
                ? SqlExpr.Call.of(SqlFn.VARIANT_ELEMENTS, value)
                : value;
        // A VARIANT ELEMENT column keeps JSON elements; the list may
        // itself be a variant (fromJson(...)->toMany(@Variant)).
        if (cr.value().info().type() instanceof Type.ClassType vc
                && com.legend.compiler.element.type.PlatformTypes.isVariant(vc)
                && !(elem instanceof Type.ClassType)) {
            list = SqlExpr.Call.of(SqlFn.VARIANT_ELEMENTS, value);
        }
        return SqlSelect.starOf(new SqlSource.Subselect(
                rows(list, cr.column(),
                        Fold.named(lo.outputsOf(cr.info()), cr.column()), List.of()),
                lo.nextAlias(), null));
    }

    /** THE one explode site: a list value as rows of one column — UNNEST
     * in the select list (placement is dialect assembly).
     * Both consumers above and below go through it. */
    static SqlSelect rows(SqlExpr list, String column,
            @com.legend.base.Nullable OutputCol out, List<OutputCol> outputs) {
        return rows(list, column, out, outputs, new SqlSource.Dual());
    }

    /** The explode over a named source (a verdict's document rows). */
    static SqlSelect rows(SqlExpr list, String column,
            @com.legend.base.Nullable OutputCol out, List<OutputCol> outputs, SqlSource from) {
        return new SqlSelect(List.of(new SqlSelect.Projection(
                        SqlExpr.Call.of(SqlFn.UNNEST, list), column, out)),
                false, from, null, List.of(), null, null,
                List.of(), null, null, outputs);
    }

    /** A class-typed collection VALUE (a computed list — never a relation,
     * never an instance literal, never a row map over a relation) whose
     * elements have a model layout. */
    static boolean classValued(Lowerer lo, TypedSpec v) {
        if (!(v instanceof TypedNativeCall || v instanceof TypedMap)
                || Type.relationValued(v.info())
                || (v instanceof TypedMap m && Type.relationValued(m.source().info()))
                || VariantShapes.isInstanceLiteral(v)) {
            return false;
        }
        // a plain class or an instantiated generic (Pair<String, Integer>)
        Type t = v.info().type();
        boolean classLike = t instanceof Type.GenericType
                || (t instanceof Type.ClassType ct
                        && !com.legend.compiler.element.type.PlatformTypes.isVariant(ct));
        return classLike && lo.classLayout(t).isPresent();
    }

    /** The relation of a class-typed collection value's elements: one row
     * per element (list order), one column per layout field. */
    static SqlSelect explode(Lowerer lo, TypedSpec v) {
        Type elem = v.info().type();
        List<Type.Column> layout = lo.classLayout(elem).orElseThrow();
        SqlExpr list = lo.scalar(v, lo.noScope());
        String inner = lo.nextAlias();
        OutputCol elemOut = new OutputCol("elem", lo.sqlTypeOf(elem), false);
        SqlSource src = new SqlSource.Subselect(
                rows(list, "elem", elemOut, List.of(elemOut)), inner, null);
        List<SqlSelect.Projection> ps = new ArrayList<>(layout.size());
        List<OutputCol> outs = new ArrayList<>(layout.size());
        for (Type.Column c : layout) {
            OutputCol out = new OutputCol(c.name(), lo.sqlTypeOf(c.type()),
                    !(c.multiplicity() instanceof
                            com.legend.compiler.element.type.Multiplicity.Bounded b
                            && b.lower() >= 1));
            outs.add(out);
            ps.add(new SqlSelect.Projection(
                    SqlExpr.StructGet.of(SqlExpr.Column.of(inner, elemOut), c.name()),
                    c.name(), out));
        }
        return new SqlSelect(ps, false, src, null, List.of(), null, null,
                List.of(), null, null, outs);
    }
}
