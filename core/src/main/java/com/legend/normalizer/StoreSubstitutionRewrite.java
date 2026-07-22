// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.normalizer;

import com.legend.model.ClassMapping;
import com.legend.model.FilterMapping;
import com.legend.model.FilterPointer;
import com.legend.model.JoinChainElement;
import com.legend.model.LegacyMappingDefinition;
import com.legend.model.MappingInclude;
import com.legend.model.PropertyMapping;
import com.legend.model.RelationalOperation;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * STORE SUBSTITUTION over included class mappings (real Legend:
 * {@code include AMapping[testDatabase->testDatabase2]} — every store
 * reference pulled through the include re-points to the replacement;
 * chained includes compose, corpus testExtendsWithStoreSubstitution.pure).
 * Exhaustive over every database-carrying node — a new PM /
 * RelationalOperation variant cannot be added without a rule here (the
 * windowize stance).
 */
final class StoreSubstitutionRewrite {

    private StoreSubstitutionRewrite() {
    }

    static ClassMapping apply(ClassMapping cm,
            List<MappingInclude.StoreSubstitution> subs) {
        if (subs.isEmpty() || !(cm instanceof ClassMapping.Relational r)) {
            return cm;
        }
        Map<String, String> m = subs.stream().collect(Collectors.toMap(
                MappingInclude.StoreSubstitution::originalStore,
                MappingInclude.StoreSubstitution::replacementStore,
                (a, b) -> b));
        return new ClassMapping.Relational(r.className(), r.setId(),
                r.extendsSetId(), r.root(),
                r.mainTable() == null ? null
                        : new LegacyMappingDefinition.TableReference(
                                db(r.mainTable().database(), m),
                                r.mainTable().table()),
                r.filter() == null ? null : filter(r.filter(), m),
                r.distinct(),
                r.groupBy().stream().map(g -> op(g, m)).toList(),
                r.primaryKey().stream().map(g -> op(g, m)).toList(),
                r.propertyMappings().stream().map(p -> pm(p, m)).toList(),
                r.sourceUrl(), r.propertyTargetSets());
    }

    private static String db(String database, Map<String, String> m) {
        return database == null ? null : m.getOrDefault(database, database);
    }

    private static FilterMapping filter(FilterMapping f, Map<String, String> m) {
        return switch (f) {
            case FilterMapping.Direct d ->
                    new FilterMapping.Direct(pointer(d.filter(), m));
            case FilterMapping.JoinMediated j -> new FilterMapping.JoinMediated(
                    db(j.sourceDb(), m),
                    j.joins().stream().map(c -> chain(c, m)).toList(),
                    pointer(j.filter(), m), j.joinType());
        };
    }

    private static FilterPointer pointer(FilterPointer p, Map<String, String> m) {
        return switch (p) {
            case FilterPointer.Local l -> l;
            case FilterPointer.Cross c ->
                    new FilterPointer.Cross(db(c.db(), m), c.name());
        };
    }

    private static JoinChainElement chain(JoinChainElement c,
            Map<String, String> m) {
        return c.databaseName() == null ? c
                : new JoinChainElement(c.joinName(), c.joinType(),
                        db(c.databaseName(), m), c.includeSelf());
    }

    private static PropertyMapping pm(PropertyMapping p, Map<String, String> m) {
        return switch (p) {
            case PropertyMapping.Column c -> new PropertyMapping.Column(
                    c.propertyName(), db(c.database(), m), c.table(), c.column());
            case PropertyMapping.EnumeratedExpression e ->
                    new PropertyMapping.EnumeratedExpression(e.propertyName(),
                            e.enumMappingId(), op(e.expression(), m));
            case PropertyMapping.EnumeratedColumn e ->
                    new PropertyMapping.EnumeratedColumn(e.propertyName(),
                            e.enumMappingId(), db(e.database(), m), e.table(),
                            e.column());
            case PropertyMapping.Join j -> new PropertyMapping.Join(
                    j.propertyName(), db(j.database(), m),
                    j.joins().stream().map(c -> chain(c, m)).toList(),
                    j.targetSetId());
            case PropertyMapping.JoinTerminalColumn j ->
                    new PropertyMapping.JoinTerminalColumn(j.propertyName(),
                            db(j.database(), m),
                            j.joins().stream().map(c -> chain(c, m)).toList(),
                            op(j.terminalColumn(), m), j.enumMappingId(),
                            j.enumMapped());
            case PropertyMapping.Expression e -> new PropertyMapping.Expression(
                    e.propertyName(), op(e.expression(), m));
            case PropertyMapping.Embedded e -> new PropertyMapping.Embedded(
                    e.propertyName(),
                    e.propertyMappings().stream().map(x -> pm(x, m)).toList());
            case PropertyMapping.InlineEmbedded ie -> ie;
            case PropertyMapping.OtherwiseEmbedded oe ->
                    new PropertyMapping.OtherwiseEmbedded(oe.propertyName(),
                            oe.embedded().stream().map(x -> pm(x, m)).toList(),
                            oe.fallbackSetId(), pm(oe.fallback(), m));
            case PropertyMapping.LocalProperty lp ->
                    new PropertyMapping.LocalProperty(lp.propertyName(),
                            lp.type(), lp.multiplicity(), pm(lp.body(), m));
        };
    }

    private static RelationalOperation op(RelationalOperation o,
            Map<String, String> m) {
        return switch (o) {
            case RelationalOperation.ColumnRef c ->
                    new RelationalOperation.ColumnRef(db(c.databaseName(), m),
                            c.table(), c.column());
            case RelationalOperation.TargetColumnRef t -> t;
            case RelationalOperation.Literal l -> l;
            case RelationalOperation.FunctionCall f ->
                    new RelationalOperation.FunctionCall(f.name(),
                            f.args().stream().map(a -> op(a, m)).toList());
            case RelationalOperation.Comparison c ->
                    new RelationalOperation.Comparison(op(c.left(), m), c.op(),
                            op(c.right(), m));
            case RelationalOperation.BooleanOp b ->
                    new RelationalOperation.BooleanOp(op(b.left(), m), b.op(),
                            op(b.right(), m));
            case RelationalOperation.IsNull n ->
                    new RelationalOperation.IsNull(op(n.operand(), m));
            case RelationalOperation.IsNotNull n ->
                    new RelationalOperation.IsNotNull(op(n.operand(), m));
            case RelationalOperation.Group g ->
                    new RelationalOperation.Group(op(g.inner(), m));
            case RelationalOperation.TypeRef t -> t;
            case RelationalOperation.ArrayLiteral a ->
                    new RelationalOperation.ArrayLiteral(
                            a.elements().stream().map(e -> op(e, m)).toList());
            case RelationalOperation.JoinNavigation j ->
                    new RelationalOperation.JoinNavigation(
                            db(j.databaseName(), m),
                            j.chain().stream().map(c -> chain(c, m)).toList(),
                            j.terminal() == null ? null : op(j.terminal(), m));
        };
    }
}
