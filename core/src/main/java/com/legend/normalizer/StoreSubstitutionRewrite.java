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
        if (subs.isEmpty()) {
            return cm;
        }
        return applyWith(cm, subs.stream().collect(Collectors.toMap(
                MappingInclude.StoreSubstitution::originalStore,
                MappingInclude.StoreSubstitution::replacementStore,
                (a, b) -> b)));
    }

    static ClassMapping applyWith(ClassMapping cm, Map<String, String> m) {
        if (!(cm instanceof ClassMapping.Relational r)) {
            return cm;
        }
        return new ClassMapping.Relational(r.className(), r.setId(),
                r.extendsSetId(), r.root(),
                r.mainTable() == null ? null
                        : new LegacyMappingDefinition.TableReference(
                                java.util.Objects.requireNonNull(
                                        db(r.mainTable().database(), m)),
                                r.mainTable().table()),
                r.filter() == null ? null : filter(r.filter(), m),
                r.distinct(),
                r.groupBy().stream().map(g -> op(g, m)).toList(),
                r.primaryKey().stream().map(g -> op(g, m)).toList(),
                r.propertyMappings().stream().map(p -> pm(p, m)).toList(),
                r.sourceUrl(), r.propertyTargetSets(),
                views(r.aggregation(), m));
    }

    /** The AggregationAware node's views rewrite with the node (each view
     * is a Relational set: same substitution). */
    private static ClassMapping.@com.legend.Nullable AggregationAware views(
            ClassMapping.@com.legend.Nullable AggregationAware agg, Map<String, String> m) {
        if (agg == null) {
            return null;
        }
        return new ClassMapping.AggregationAware(agg.views().stream()
                .map(v -> new ClassMapping.AggregateView(v.index(), v.canAggregate(),
                        v.groupByFunctions(), v.aggregateValues(),
                        (ClassMapping.Relational) applyWith(v.set(), m)))
                .toList());
    }

    private static @com.legend.Nullable String db(
            @com.legend.Nullable String database, Map<String, String> m) {
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
                    new FilterPointer.Cross(java.util.Objects.requireNonNull(db(c.db(), m)), c.name());
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
                    c.propertyName(), java.util.Objects.requireNonNull(db(c.database(), m)),
                    c.table(), c.column());
            case PropertyMapping.EnumeratedExpression e ->
                    new PropertyMapping.EnumeratedExpression(e.propertyName(),
                            e.enumMappingId(), op(e.expression(), m));
            case PropertyMapping.EnumeratedColumn e ->
                    new PropertyMapping.EnumeratedColumn(e.propertyName(),
                            e.enumMappingId(), java.util.Objects.requireNonNull(db(e.database(), m)),
                            e.table(),
                            e.column());
            case PropertyMapping.Join j -> new PropertyMapping.Join(
                    j.propertyName(), java.util.Objects.requireNonNull(db(j.database(), m)),
                    j.joins().stream().map(c -> chain(c, m)).toList(),
                    j.targetSetId());
            case PropertyMapping.JoinTerminalColumn j ->
                    new PropertyMapping.JoinTerminalColumn(j.propertyName(),
                            
                            java.util.Objects.requireNonNull(db(j.database(), m)),
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
            case RelationalOperation.ArrayLiteral a ->
                    new RelationalOperation.ArrayLiteral(
                            a.elements().stream().map(e -> op(e, m)).toList());
            case RelationalOperation.Lambda lam ->
                    new RelationalOperation.Lambda(lam.parameters(), op(lam.body(), m));
            case RelationalOperation.LambdaParam p -> p;
            case RelationalOperation.JoinNavigation j ->
                    new RelationalOperation.JoinNavigation(
                            db(j.databaseName(), m),
                            j.chain().stream().map(c -> chain(c, m)).toList(),
                            j.terminal() == null ? null : op(j.terminal(), m));
        };
    }

    /** The same exhaustive walk over an ASSOCIATION mapping's PM bodies. */
    static com.legend.model.AssociationMapping applyAssoc(
            com.legend.model.AssociationMapping am, Map<String, String> m) {
        if (!(am instanceof com.legend.model.AssociationMapping.Relational rel)) {
            return am;
        }
        return new com.legend.model.AssociationMapping.Relational(
                rel.associationName(),
                rel.propertyMappings().stream()
                        .map(apm -> new com.legend.model
                                .AssociationPropertyMapping(apm.sourceSetId(),
                                        apm.targetSetId(), pm(apm.body(), m)))
                        .toList());
    }

    /**
     * Every database name the mapping's stores reference — collected by
     * running the SAME exhaustive rewrite with a RECORDING map (zero
     * drift: a new db-carrying node cannot be added without the rewrite
     * rule, and the recorder rides the rule).
     */
    static void collectDatabases(LegacyMappingDefinition md,
            java.util.Set<String> out) {
        Map<String, String> recorder = new java.util.AbstractMap<>() {
            @Override
            public java.util.Set<Entry<String, String>> entrySet() {
                return java.util.Set.of();
            }

            @Override
            public String getOrDefault(Object k, String d) {
                if (k != null) {
                    out.add((String) k);
                }
                return d;
            }
        };
        for (ClassMapping cm : md.classMappings()) {
            applyWith(cm, recorder);
        }
        for (com.legend.model.AssociationMapping am : md.associationMappings()) {
            applyAssoc(am, recorder);
        }
    }

    /**
     * IMPORT-SCOPE store-ref qualification (the graph milestoning-union
     * trio): a mapping spelling {@code [db]} under
     * {@code import a::b::*} means {@code a::b::db} — global
     * unique-simple-name resolution silently picks a SAME-NAMED shared
     * store instead (the corpus's {@code tests::db}). Unqualified refs
     * whose scope-qualified candidate is a REGISTERED database rewrite to
     * the FQN; everything else is untouched (the lenient simple-name
     * fallback still serves scope-less models).
     */
    static LegacyMappingDefinition qualifyStoreRefs(LegacyMappingDefinition md,
            com.legend.compiler.ModelBuilder model) {
        com.legend.model.ImportScope scope = model.importsOf(md.qualifiedName());
        if (scope == null || scope.wildcards().isEmpty()) {
            return md;
        }
        java.util.Set<String> raws = new java.util.LinkedHashSet<>();
        collectDatabases(md, raws);
        Map<String, String> q = new java.util.LinkedHashMap<>();
        for (String r : raws) {
            if (r == null || r.contains("::") || model.hasDatabaseExact(r)) {
                continue;
            }
            // rewrite ONLY the SHADOWED cases: the raw spelling's lenient
            // simple-name resolution lands somewhere OTHER than the
            // scope-qualified candidate (or nowhere). When they agree the
            // raw spelling stays — downstream machinery keyed on it is
            // untouched (the propertyLevel family regressed wholesale
            // under unconditional qualification).
            String current = model.findDatabase(r)
                    .map(com.legend.model.DatabaseDefinition::qualifiedName)
                    .orElse(null);
            for (String w : scope.wildcards()) {
                String cand = w + "::" + r;
                if (model.hasDatabaseExact(cand)) {
                    if (!cand.equals(current)) {
                        q.put(r, cand);
                    }
                    break;
                }
            }
        }
        if (q.isEmpty()) {
            return md;
        }
        LegacyMappingDefinition out = md.withClassMappings(
                md.classMappings().stream().map(cm -> applyWith(cm, q)).toList());
        return new LegacyMappingDefinition(out.qualifiedName(), out.includes(),
                out.classMappings(),
                out.associationMappings().stream()
                        .map(am -> applyAssoc(am, q)).toList(),
                out.enumerationMappings(), out.testSuitesSource());
    }
}
