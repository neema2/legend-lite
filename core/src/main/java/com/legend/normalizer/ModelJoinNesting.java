// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.normalizer;

import com.legend.builtin.Pure;
import com.legend.compiler.ModelBuilder;
import com.legend.error.NotImplementedException;
import com.legend.model.AssociationDefinition;
import com.legend.model.AssociationMapping;
import com.legend.model.ClassMapping;
import com.legend.protocol.TypeExpression;
import com.legend.protocol.spec.AppliedFunction;
import com.legend.protocol.spec.AppliedProperty;
import com.legend.protocol.spec.LambdaFunction;
import com.legend.protocol.spec.PureCollection;
import com.legend.protocol.spec.ValueSpecification;
import com.legend.protocol.spec.Variable;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * ModelJoin NESTED-HOP composition ({@code $employees.address.city}):
 * the mid property is another ModelJoin association — the end's
 * pipeline composes with the nested target joined in (prefixed), and
 * the read rewrites to the nested target's prefixed column. Split
 * from {@link MappingNormalizer} (guardrail seam).
 */
final class ModelJoinNesting {

    private ModelJoinNesting() {
    }

    record Composed(@com.legend.base.Nullable ValueSpecification pipeA,
            @com.legend.base.Nullable ValueSpecification pipeB,
            Map<String, Map<String, Map<String, String>>> nestedCols) {}

    static Composed compose(ResolvedMapping md, ModelBuilder model,
            AssociationMapping.ModelJoin mj, AssociationDefinition ad2,
            String classA, String classB, String aVar, String bVar,
            Map<String, ClassMapping.RelationFunction> rfByVar,
            @com.legend.base.Nullable ValueSpecification pipeA0, @com.legend.base.Nullable ValueSpecification pipeB0) {
        // NESTED HOPS ($employees.address.city): the mid property is
        // ANOTHER ModelJoin association — the END's pipeline composes
        // with the nested target joined in (the engine golden's side
        // subselect: person ⟕ address on Person_Address's condition),
        // and the read rewrites to the nested target's column on the
        // composite row (task #78).
        ValueSpecification pipeA = pipeA0;
        ValueSpecification pipeB = pipeB0;
        Map<String, Map<String, Map<String, String>>> nestedCols =
                new LinkedHashMap<>();
        Set<String[]> hops = new LinkedHashSet<>();
        collectNestedHops(mj.lambda().body().get(mj.lambda().body().size() - 1),
                Set.of(aVar, bVar), hops);
        for (String[] hop : hops) {
            String var = hop[0];
            String prop = hop[1];
            ClassMapping.RelationFunction rf0 = java.util.Objects.requireNonNull(rfByVar.get(var));
            boolean plainCol = rf0.columns().stream().anyMatch(
                    c -> c.property().equals(prop) && c.column() != null);
            if (plainCol || nestedCols.getOrDefault(var, Map.of())
                    .containsKey(prop)) {
                continue;
            }
            String endCls = var.equals(aVar) ? classA : classB;
            AssociationDefinition nad = model.findAssociationOf(endCls, prop)
                    .orElseThrow(() -> MissProbe.neverFired("ModelJoinNesting#1"));
            String nestedCls = nad.property1().propertyName().equals(prop)
                    ? nad.property1().targetClassFqn()
                    : nad.property2().targetClassFqn();
            AssociationMapping.ModelJoin nmj = null;
            for (AssociationMapping am2 : md.associationMappings()) {
                if (am2 instanceof AssociationMapping.ModelJoin cand
                        && model.findAssociation(cand.associationName())
                                .map(x -> x.qualifiedName()
                                        .equals(nad.qualifiedName()))
                                .orElse(false)) {
                    nmj = cand;
                }
            }
            if (nmj == null) {
                throw new NotImplementedException("ModelJoin for '"
                        + mj.associationName() + "': nested hop '$" + var
                        + "." + prop + "' navigates association '"
                        + nad.qualifiedName() + "' which has no ModelJoin"
                        + " in this mapping — only ModelJoin-backed nested"
                        + " hops compose yet");
            }
            XStorePureEnds.XEnd nEnd = XStorePureEnds.xstoreEndOf(md, nestedCls, null, model);
            ClassMapping.RelationFunction nRf = nEnd.colsView();
            // the nested end is the stock NAVIGATE step (user call:
            // "both XStore and ModelJoin are just navigate()") — the
            // ColSpec-join spelling every @Join property emission uses,
            // so demand/materialization/prefixing are the ONE slot
            // machinery, no bespoke composition
            if (nmj.lambda().parameters().size() != 2) {
                throw new NotImplementedException("nested ModelJoin '"
                        + nmj.associationName() + "' needs a 2-param lambda");
            }
            Variable np0 = nmj.lambda().parameters().get(0);
            Variable np1 = nmj.lambda().parameters().get(1);
            String nt0 = np0.type() instanceof TypeExpression.NameRef nn0
                    ? nn0.name() : null;
            String nt1 = np1.type() instanceof TypeExpression.NameRef nn1
                    ? nn1.name() : null;
            String[] nPair = pairEndVars(nmj.associationName(), nad,
                    endCls, nestedCls, np0, np1, nt0, nt1);
            Variable j0 = new Variable("_mj0");
            Variable j1 = new Variable("_mj1");
            ValueSpecification nCond = RelationReads.rewrite(
                    nmj.lambda().body().get(nmj.lambda().body().size() - 1),
                    Map.of(nPair[0], j0, nPair[1], j1),
                    Map.of(nPair[0], rf0, nPair[1], nRf),
                    nmj.associationName(), md, Map.of(), model);
            ValueSpecification composite = new AppliedFunction(Pure.Lite.JOIN_SLOT, List.of(
                    var.equals(aVar) ? pipeA : pipeB,
                    new com.legend.protocol.spec.ColSpec(prop,
                            new LambdaFunction(List.of(),
                                    List.of(nEnd.pipeline())),
                            null),
                    new LambdaFunction(List.of(j0, j1), List.of(nCond))));
            if (var.equals(aVar)) {
                pipeA = composite;
            } else {
                pipeB = composite;
            }
            Map<String, String> leafCols = new LinkedHashMap<>();
            for (ClassMapping.RelationFunction.Col nc : java.util.Objects.requireNonNull(nRf).columns()) {
                if (nc.column() != null) {
                    leafCols.put(nc.property(), nc.column());
                }
            }
            nestedCols.computeIfAbsent(var, k -> new LinkedHashMap<>())
                    .put(prop, leafCols);
        }
        return new Composed(pipeA, pipeB, nestedCols);
    }

    /** The end-var pairing shared by top-level and NESTED ModelJoins:
     * exact type FQN when the ends differ, else the engine's
     * property-name rule — never suffix matching. */
    static String[] pairEndVars(String assocName,
            AssociationDefinition ad2, String classA, String classB,
            Variable p0, Variable p1, @com.legend.base.Nullable String t0, @com.legend.base.Nullable String t1) {
        if (!classA.equals(classB) && classA.equals(t0) && classB.equals(t1)) {
            return new String[] {p0.name(), p1.name()};
        }
        if (!classA.equals(classB) && classA.equals(t1) && classB.equals(t0)) {
            return new String[] {p1.name(), p0.name()};
        }
        if (p0.name().equals(ad2.property1().propertyName())
                && p1.name().equals(ad2.property2().propertyName())) {
            return new String[] {p0.name(), p1.name()};
        }
        if (p1.name().equals(ad2.property1().propertyName())
                && p0.name().equals(ad2.property2().propertyName())) {
            return new String[] {p1.name(), p0.name()};
        }
        throw new NotImplementedException(
                "ModelJoin for '" + assocName + "': lambda params ["
                + p0.name() + ": " + t0 + ", " + p1.name() + ": " + t1
                + "] pair with the association ends neither by exact type"
                + " nor by the engine's property-name rule");
    }


    /** Two-hop reads {@code $end.assocProp.leaf} in a ModelJoin body —
     * the composite-join pre-pass's demand. */
    private static void collectNestedHops(ValueSpecification v,
            Set<String> endVars, Set<String[]> out) {
        if (v instanceof AppliedProperty ap
                && ap.receiver() instanceof AppliedProperty mid
                && mid.receiver() instanceof Variable var
                && endVars.contains(var.name())) {
            out.add(new String[] {var.name(), mid.property()});
            return;
        }
        switch (v) {
            case AppliedFunction af -> af.parameters()
                    .forEach(x -> collectNestedHops(x, endVars, out));
            case AppliedProperty ap2 ->
                    collectNestedHops(ap2.receiver(), endVars, out);
            case PureCollection pc -> pc.values()
                    .forEach(x -> collectNestedHops(x, endVars, out));
            case LambdaFunction lf -> lf.body()
                    .forEach(x -> collectNestedHops(x, endVars, out));
            default -> v.children()
                    .forEach(x -> collectNestedHops(x, endVars, out));
        }
    }

}
