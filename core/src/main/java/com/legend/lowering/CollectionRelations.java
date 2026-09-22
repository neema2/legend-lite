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
import com.legend.sql.SqlAgg;
import com.legend.sql.SqlSource;
import com.legend.sql.SqlType;

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

    // ------------------------------------------------------------------
    // zip AT ROW POSITION (2026-09-22): a collection stays RELATIONAL as long as it
    // is at row position. zip(a, b) over two row sets is a JOIN ON THE ROW NUMBER —
    // the inner join stops at the shorter side, which is zip's own truncation
    // (zip.pure) — producing the Pair layout every Pair read expects (first, second),
    // exactly the columns explode() would spell. Plain standard SQL, every target.
    // The scalar rule (ListEncodings.zip: DuckDB's list_zip) stays for a zip that
    // must be ONE VALUE inside a row — the list vocabulary, rung 2c's problem.
    // ------------------------------------------------------------------

    /** {@code zip(a, b)}. */
    static boolean zipCall(TypedSpec v) {
        return v instanceof TypedNativeCall n
                && com.legend.compiler.element.type.PlatformTypes.COLLECTION_ZIP.equals(n.callee().qualifiedName())
                && n.args().size() == 2;
    }

    /** A source the relation lane plans as rows: a relation, or a zip whose arms are rows. */
    static boolean rowSource(TypedSpec v) {
        return Type.relationValued(v.info()) || (zipCall(v) && rowArms((TypedNativeCall) v));
    }

    /** Both zip arms are numberable rows: a single-column relation, a literal collection,
     * or such a zip. A list-valued arm (a computed list) keeps the LIST form (explode). */
    static boolean rowArms(TypedNativeCall z) {
        return rowArm(z.args().get(0)) && rowArm(z.args().get(1));
    }

    private static boolean rowArm(TypedSpec arm) {
        if (arm instanceof com.legend.compiler.spec.typed.TypedCollection) {
            return true;
        }
        if (zipCall(arm)) {
            return rowArms((TypedNativeCall) arm);
        }
        return Type.relationValued(arm.info())
                && Type.schemaView(arm.info().type()) instanceof Type.RelationType rt
                && rt.columns().size() == 1;
    }

    /** {@code zip(a, b)} at relation position: the ROW form when both arms lower to one
     * column each; otherwise the list form as before (a platform-synthesized zip whose arm
     * carries a sort key beside its value is typed one column but lowers to two). */
    static SqlSelect zipRelation(Lowerer lo, TypedNativeCall z) {
        SqlSelect rows = zipRows(lo, z);
        return rows != null ? rows : explode(lo, z);
    }

    /** The relation of {@code zip(a, b)}: rows (first, second) joined on the row number;
     * null when an arm does not lower to exactly one column. */
    static @com.legend.base.Nullable SqlSelect zipRows(Lowerer lo, TypedNativeCall z) {
        List<Type.Column> layout = lo.classLayout(z.info().type()).orElseThrow(() ->
                new com.legend.error.NotImplementedException("zip at row position: no Pair layout for "
                        + z.info().type()));
        String l = lo.nextAlias();
        String r = lo.nextAlias();
        SqlSelect left = armRows(lo, z.args().get(0), layout.get(0), l);
        SqlSelect right = armRows(lo, z.args().get(1), layout.get(1), r);
        if (left == null || right == null) {
            return null;
        }
        SqlSource join = new SqlSource.Join(
                new SqlSource.Subselect(left, l, null), new SqlSource.Subselect(right, r, null),
                SqlSource.Join.Kind.INNER,
                SqlExpr.Call.of(SqlFn.EQUAL, SqlExpr.Column.of(l, left.outputs(), RN),
                        SqlExpr.Column.of(r, right.outputs(), RN)));
        List<SqlSelect.Projection> ps = new ArrayList<>(2);
        List<OutputCol> outs = new ArrayList<>(2);
        OutputCol fo = left.outputs().get(0);
        OutputCol so = right.outputs().get(0);
        outs.add(fo);
        outs.add(so);
        ps.add(new SqlSelect.Projection(SqlExpr.Column.of(l, left.outputs(), fo.name()), fo.name(), fo));
        ps.add(new SqlSelect.Projection(SqlExpr.Column.of(r, right.outputs(), so.name()), so.name(), so));
        return new SqlSelect(ps, false, join, null, List.of(), null, null, List.of(), null, null, outs);
    }

    private static final String RN = "__rn";

    /** One zip arm as numbered rows {@code (<field>, __rn)}: a relation's first
     * column in its order, or a literal collection as VALUES in list order. */
    private static @com.legend.base.Nullable SqlSelect armRows(Lowerer lo, TypedSpec arm, Type.Column field, String alias) {
        String inner = lo.nextAlias();
        SqlSource src;
        SqlExpr value;
        SqlType type;
        if (arm instanceof com.legend.compiler.spec.typed.TypedCollection c) {
            List<List<SqlExpr>> rows = new ArrayList<>(c.elements().size());
            for (TypedSpec e : c.elements()) {
                rows.add(List.of(lo.scalar(e, lo.noScope())));
            }
            type = lo.sqlTypeOf(field.type());
            OutputCol vo = new OutputCol("value", type, rows.isEmpty());
            src = new SqlSource.Values(rows, List.of("value"), inner, List.of(vo));
            value = SqlExpr.Column.of(inner, List.of(vo), "value");
        } else if (Type.relationValued(arm.info()) || classValued(lo, arm) || zipCall(arm)) {
            SqlSelect rel = lo.relation(arm);
            if (rel.outputs().size() != 1) {
                return null;   // not one value per row: the list form stays the road
            }
            OutputCol vo = rel.outputs().get(0);
            type = vo.type();
            src = new SqlSource.Subselect(rel, inner, null);
            value = SqlExpr.Column.of(inner, rel.outputs(), vo.name());
        } else {
            throw new com.legend.error.NotImplementedException("zip at row position: arm "
                    + arm.getClass().getSimpleName() + " is neither rows nor a literal collection");
        }
        OutputCol fieldOut = new OutputCol(field.name(), type, true);
        OutputCol rnOut = new OutputCol(RN, SqlType.Scalar.BIGINT, false);
        SqlExpr rn = new SqlExpr.WindowCall(new SqlAgg.RankingFn(SqlAgg.Fn.ROW_NUMBER, List.of()),
                List.of(), List.of(), null);
        return new SqlSelect(List.of(
                new SqlSelect.Projection(value, field.name(), fieldOut),
                new SqlSelect.Projection(rn, RN, rnOut)),
                false, src, null, List.of(), null, null, List.of(), null, null, List.of(fieldOut, rnOut));
    }
}
