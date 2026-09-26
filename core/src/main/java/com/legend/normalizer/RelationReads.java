// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.normalizer;

import com.legend.compiler.ModelBuilder;
import com.legend.error.NotImplementedException;
import com.legend.model.ClassDefinition;
import com.legend.model.ClassMapping;
import com.legend.protocol.Multiplicity;
import com.legend.protocol.spec.AppliedFunction;
import com.legend.protocol.spec.AppliedProperty;
import com.legend.protocol.spec.LambdaFunction;
import com.legend.protocol.spec.PureCollection;
import com.legend.protocol.spec.ValueSpecification;
import com.legend.protocol.spec.Variable;

import java.util.List;
import java.util.Map;

/**
 * XStore/ModelJoin condition rewriting: {@code $this.p}/{@code $that.p}
 * property reads become COLUMN reads on the two relation rows (relocated
 * from {@link MappingNormalizer} at the file guardrail). A [1]-DECLARED
 * property reads through {@code toOne} — the column view types [0..1]
 * (physical nullability) and would summon comparison guards the engine
 * never spells (the multiplicity-masking trap, 4th sighting).
 */
final class RelationReads {

    private RelationReads() {
    }

    /** {@code $this.p}/{@code $that.p} → column reads on the two relation rows. */
    static ValueSpecification xstore(ValueSpecification v,
            Variable thisRow, ClassMapping.@com.legend.base.Nullable RelationFunction thisRf,
            Variable thatRow, ClassMapping.@com.legend.base.Nullable RelationFunction thatRf,
            String assocName, ResolvedMapping md, ModelBuilder model) {
        return rewrite(v,
                Map.of("this", thisRow, "that", thatRow),
                Map.of("this", thisRf, "that", thatRf), assocName, md,
                Map.of(), model);
    }

    /** {@code $var.prop} → the var's Relation mapping's COLUMN read. */


    static ValueSpecification rewrite(ValueSpecification v,
            Map<String, Variable> rowByVar,
            Map<String, ClassMapping.RelationFunction> rfByVar,
            String assocName, ResolvedMapping md,
            @com.legend.base.Nullable Map<String, Map<String, Map<String, String>>> nestedCols) {
        return rewrite(v, rowByVar, rfByVar, assocName, md,
                nestedCols, null);
    }

    static ValueSpecification rewrite(ValueSpecification v,
            Map<String, Variable> rowByVar,
            Map<String, ClassMapping.RelationFunction> rfByVar,
            String assocName, ResolvedMapping md,
            @com.legend.base.Nullable Map<String, Map<String, Map<String, String>>> nestedCols,
            @com.legend.base.Nullable ModelBuilder model) {
        return rewrite(v, rowByVar, rfByVar, assocName, md, nestedCols, model, 0);
    }

    /** {@code derivedDepth}: how many derived-property inlines enclose this
     * rewrite — a parameter of the recursion (Phase 2d, batch 138), a
     * thread-local counter before; a self-referential derived property
     * runs out of depth and falls through to the loud wall. */
    private static ValueSpecification rewrite(ValueSpecification v,
            Map<String, Variable> rowByVar,
            Map<String, ClassMapping.RelationFunction> rfByVar,
            String assocName, ResolvedMapping md,
            @com.legend.base.Nullable Map<String, Map<String, Map<String, String>>> nestedCols,
            @com.legend.base.Nullable ModelBuilder model, int derivedDepth) {
        // NESTED hop read: $end.assocProp.leaf resolves to the nested
        // target's column on the END's composite row
        if (v instanceof AppliedProperty ap0
                && ap0.receiver() instanceof AppliedProperty mid0
                && mid0.receiver() instanceof Variable var0
                && nestedCols != null
                && nestedCols.getOrDefault(var0.name(), Map.of())
                        .containsKey(mid0.property())) {
            Map<String, String> leafCols = java.util.Objects.requireNonNull(
                    java.util.Objects.requireNonNull(nestedCols.get(var0.name()))
                            .get(mid0.property()));
            String col = leafCols.get(ap0.property());
            if (col == null) {
                throw new NotImplementedException(
                        "association '" + assocName + "': $" + var0.name()
                        + "." + mid0.property() + "." + ap0.property()
                        + " has no column binding on the nested Relation"
                        + " mapping (mapping=" + md.qualifiedName() + ")");
            }
            // the SLOT-READ spelling ($row.<navSlot>.<COL>) — typed and
            // demanded by the stock navigate-step machinery
            return new AppliedProperty(new AppliedProperty(
                    java.util.Objects.requireNonNull(rowByVar.get(var0.name())),
                    mid0.property()), col);
        }
        if (v instanceof AppliedProperty ap
                && ap.receiver() instanceof Variable var
                && rowByVar.containsKey(var.name())) {
            ClassMapping.RelationFunction rf = java.util.Objects.requireNonNull(rfByVar.get(var.name()));
            for (ClassMapping.RelationFunction.Col c : rf.columns()) {
                if (c.property().equals(ap.property()) && c.column() != null) {
                    ValueSpecification read = new AppliedProperty(
                            rowByVar.get(var.name()), c.column());
                    // conform-by-emission: a [1]-DECLARED property reads
                    // through toOne — the column view types [0..1]
                    // (physical nullability) and would summon comparison
                    // guards the engine never spells (masking trap #4)
                    ClassDefinition rcd = model == null ? null
                            : model.knowledge().hierarchyClass(rf.className()).orElseThrow(() -> new IllegalStateException("F7.8: class unresolved at RelationReads#1 (this default NEVER fired on the corpus census; a miss here is a real model gap): " + rf.className()));
                    if (model != null && rcd != null
                            && Multiplicity.Concrete.PURE_ONE.equals(
                            model.knowledge().propertyMultiplicity(rcd, ap.property()))) {
                        read = new AppliedFunction(com.legend.builtin.Pure.Lite.TRUST_ONE, List.of(read));
                    }
                    return read;
                }
            }
            // an EXPRESSION-bodied binding (prop: $src.COL + 1 / 'lit')
            // inlines with $src bound to the row — the same Col.bindSrc
            // step the normalizer's projection path runs (study #17); the
            // plain-column branch above stays first (a $src.COL spelling
            // carries BOTH fields and the column read is the proven path)
            for (ClassMapping.RelationFunction.Col c : rf.columns()) {
                if (c.property().equals(ap.property()) && c.expr() != null) {
                    return ClassMapping.RelationFunction.Col.bindSrc(
                            c.expr(), rowByVar.get(var.name()));
                }
            }
            // DERIVED (qualified) property read in a join condition
            // (ledger cluster 51): the engine inlines the qualifier body
            // into the condition — beta-inline the zero-arg Inline body
            // with $this bound to the receiver, then rewrite the result
            // so its leaf reads take the plain-column arm above.
            // Depth-guarded: a self-referential derived property falls
            // through to the loud wall below.
            if (model != null && derivedDepth < 16) {
                ClassDefinition dcd =
                        model.knowledge().hierarchyClass(rf.className()).orElseThrow(() -> new IllegalStateException("F7.8: class unresolved at RelationReads#2 (this default NEVER fired on the corpus census; a miss here is a real model gap): " + rf.className()));
                com.legend.protocol.DerivedPropertyDefinition dp =
                        model.knowledge().derivedInline(dcd, ap.property());
                if (dp != null) {
                    return rewrite(
                            substVars(dp.expression().get(0),
                                    Map.of("this", var)),
                            rowByVar, rfByVar, assocName, md,
                            nestedCols, model, derivedDepth + 1);
                }
            }
            throw new NotImplementedException(
                    "association '" + assocName + "': $" + var.name() + "."
                    + ap.property() + " has no column binding on the Relation"
                    + " mapping of '" + rf.className() + "' (mapping="
                    + md.qualifiedName() + ")");
        }
        return switch (v) {
            case AppliedFunction af -> af.withParameters(
                    af.parameters().stream().map(x -> rewrite(x,
                            rowByVar, rfByVar, assocName, md, nestedCols,
                            model, derivedDepth)).toList());
            case AppliedProperty ap2 -> new AppliedProperty(
                    rewrite(ap2.receiver(), rowByVar, rfByVar,
                            assocName, md, nestedCols, model, derivedDepth), ap2.property());
            case PureCollection pc -> new PureCollection(
                    pc.values().stream().map(x -> rewrite(x,
                            rowByVar, rfByVar, assocName, md, nestedCols,
                            model, derivedDepth)).toList());
            case LambdaFunction lf2 -> new LambdaFunction(lf2.parameters(),
                    lf2.body().stream().map(x -> rewrite(x,
                            rowByVar, rfByVar, assocName, md, nestedCols,
                            model, derivedDepth)).toList());
            default -> v.mapChildren(x -> rewrite(x, rowByVar, rfByVar,
                    assocName, md, nestedCols, model, derivedDepth));
        };
    }

    /** Name-keyed binder substitution — the same map-keyed idiom the
     * xstore/rewrite entry points use for {@code this}/{@code that}. */
    private static ValueSpecification substVars(ValueSpecification v,
            Map<String, ? extends ValueSpecification> binds) {
        if (v instanceof Variable vv) {
            ValueSpecification r = binds.get(vv.name());
            return r != null ? r : vv;
        }
        return v.mapChildren(x -> substVars(x, binds));
    }
}
