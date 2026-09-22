// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.lowering;

import com.legend.compiler.element.ClassLayouts;
import com.legend.compiler.element.EqualityKeys;
import com.legend.compiler.element.type.Type;
import com.legend.sql.OutputCol;
import com.legend.sql.SqlExpr;
import com.legend.sql.SqlFn;
import com.legend.sql.SqlType;
import com.legend.sql.TypeFact;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;
import java.util.function.IntSupplier;

/**
 * F10 proper — the identity layout's {@code __canon} field, filled at the
 * CONSTRUCTION site (ClassLayouts.SYNTHETIC_CANON): the struct WITHOUT its
 * canon is bound ONCE (a one-element list transform is SQL's let) and the
 * canon reads the bound value's fields by name, so every child expression
 * is spelled exactly once — a recursive polymorphic shape's text stays
 * linear in its size. A layout without the slot (a plain lane) is the
 * plain struct literal.
 */
final class ConstructionCanon {

    private ConstructionCanon() {
    }

    static SqlExpr bind(@com.legend.base.Nullable Function<Type, @com.legend.base.Nullable EqualityKeys> keysOf,
            IntSupplier fresh, Type t, String classFqn,
            List<SqlExpr.StructLit.Field> fields) {
        String canonName = ClassLayouts.SYNTHETIC_CANON;
        if (fields.stream().noneMatch(f -> canonName.equals(f.name()))) {
            return new SqlExpr.StructLit(fields);
        }
        List<SqlExpr.StructLit.Field> base = fields.stream()
                .filter(f -> !canonName.equals(f.name())).toList();
        SqlExpr.StructLit struct = new SqlExpr.StructLit(base);
        if (!(struct.type() instanceof TypeFact.Typed tt)
                || !(tt.type() instanceof SqlType.Struct st)) {
            // an untypeable layout keeps its NULL canon slot (the verdict
            // then declines the side — never a fabricated equality)
            List<SqlExpr.StructLit.Field> keep = new ArrayList<>(base);
            keep.add(new SqlExpr.StructLit.Field(canonName, new SqlExpr.NullLit(), SqlType.Scalar.JSON));
            return new SqlExpr.StructLit(keep);
        }
        String s = "__s" + fresh.getAsInt();
        SqlExpr bound = SqlExpr.Column.of(null, s, st, false, OutputCol.Origin.DERIVED);
        List<SqlExpr.StructLit.Field> refs = new ArrayList<>(base.size());
        for (SqlExpr.StructLit.Field f : base) {
            refs.add(new SqlExpr.StructLit.Field(f.name(),
                    new SqlExpr.StructGet(bound, f.name()), f.declared()));
        }
        EqualityKeys keys = keysOf == null ? null : keysOf.apply(t);
        SqlExpr canon = CanonicalRenderSql.constructionCanon(refs, keys, classFqn);
        SqlExpr inserted = SqlExpr.Call.of(SqlFn.STRUCT_INSERT, bound,
                new SqlExpr.StringLit(canonName), canon);
        return SqlExpr.Call.of(SqlFn.LIST_GET,
                SqlExpr.Call.of(SqlFn.LIST_TRANSFORM,
                        PureSql.asList(struct, false),
                        new SqlExpr.Lambda(List.of(s), inserted)),
                new SqlExpr.IntLit(1));
    }
}
