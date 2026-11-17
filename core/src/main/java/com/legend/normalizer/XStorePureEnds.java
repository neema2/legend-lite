// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.normalizer;

import com.legend.compiler.spec.Calls;
import com.legend.builtin.Pure;
import com.legend.compiler.ModelBuilder;
import com.legend.compiler.SynthFqn;
import com.legend.error.LegendCompileException;
import com.legend.error.ModelException;
import com.legend.error.NotImplementedException;
import com.legend.model.AssociationDefinition;
import com.legend.model.AssociationMapping;
import com.legend.model.ClassMapping;
import com.legend.model.FunctionDefinition;
import com.legend.model.LegacyMappingDefinition;
import com.legend.protocol.Multiplicity;
import com.legend.model.PropertyMapping;
import com.legend.model.SynthHat;
import com.legend.protocol.TypeExpression;
import com.legend.protocol.spec.AppliedFunction;
import com.legend.protocol.spec.AppliedProperty;
import com.legend.protocol.spec.CString;
import com.legend.protocol.spec.LambdaFunction;
import com.legend.protocol.spec.PureCollection;
import com.legend.protocol.spec.ValueSpecification;
import com.legend.protocol.spec.Variable;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

/**
 * XStore association ENDS and the PROPERTY-SPACE emission (route A,
 * docs/XSTORE_LEG.md): a {@code ClassMapping.Pure} end has no relation at
 * normalize time — its rows exist only once the resolver composes the M2M
 * set over its (possibly JSON-framed) source. The synthesized predicate
 * for such an association therefore pins the two SETS by id and keeps the
 * condition in PROPERTY space over the end classes:
 * {@code legacyAssocPredicate(a, b, 'setA', 'setB', {srcRow,tgtRow|cond})}
 * (the String-args overload). Set-LOCAL ({@code +prop}) reads cannot type
 * as class properties, so they emit as
 * {@code legacyLocalProperty($row, 'prop')} — conform-by-emission; the
 * resolver's association route substitutes both sides through the sets'
 * composed bindings. Relational/Relation ends keep the column-space
 * emission ({@link MappingNormalizer#synthesizeXStoreMapping}) verbatim.
 */
final class XStorePureEnds {

    private XStorePureEnds() {
    }

    /** An XStore END resolved against the mapping: a Relation(~func) set
     * (pipeline + column view), a RELATIONAL set converted to its column
     * view, or a PURE set (property space — no pipeline, no column view;
     * {@code localProps} names its {@code +prop} lines). */
    record XEnd(@com.legend.base.Nullable ValueSpecification pipeline, 
            ClassMapping.@com.legend.base.Nullable RelationFunction colsView, 
            String setId, boolean pure, Set<String> localProps,
            boolean lossyView, Map<String, TypeExpression> localTypes) {
        XEnd(@com.legend.base.Nullable ValueSpecification pipeline,
                ClassMapping.@com.legend.base.Nullable RelationFunction colsView,
                String setId, boolean pure, Set<String> localProps) {
            this(pipeline, colsView, setId, pure, localProps, false, Map.of());
        }
    }

    /** The set ids an XStore association's ends name, {@code a} property1's
     *  target class's, {@code b} property2's; null where the lines name none. */
    record EndSets(@com.legend.base.Nullable String a, @com.legend.base.Nullable String b) {
    }

    /** The line's {@code [srcSet, tgtSet]} ids select the sets: srcSet = the
     *  OWNING end's, tgtSet = the line's target end's (engine
     *  {@code PropertyMappingBuilder} uses them the same way). The first line
     *  speaks for the association. */
    static EndSets endSets(AssociationMapping.Cross xs, AssociationDefinition ad) {
        if (xs.propertyMappings2().isEmpty()) {
            return new EndSets(null, null);
        }
        var l0 = xs.propertyMappings2().get(0);
        boolean p1 = l0.propertyName().equals(ad.property1().propertyName());
        return new EndSets(p1 ? l0.targetSetId() : l0.sourceSetId(),
                p1 ? l0.sourceSetId() : l0.targetSetId());
    }

    /**
     * Why an XStore association is withheld, when an end is a set in a store
     * lite does not execute (a ServiceStore class mapping: no set lite builds
     * matches the end, a foreign one does); null otherwise. legend-engine
     * 4.145.0 compiles such a line ({@code PropertyMappingBuilder.visit(
     * XStorePropertyMapping)} finds the set by id; {@code $that} types as the
     * ServiceStore set's mapping class, {@code +props} included) and runs it,
     * calling the services; lite compiles the model, emits no predicate, and a
     * query navigating the association is refused with this reason (2026-11-16,
     * the Studio roundtrip's STO_ServiceStore-path-offset; register row S413,
     * pending). A set id the mapping does not have at all is not this: it stays
     * {@link #xstoreEndOf}'s compile error, as the engine's is.
     */
    static @com.legend.base.Nullable String foreignEndReason(ResolvedMapping md,
            AssociationMapping.Cross xs, AssociationDefinition ad) {
        EndSets sets = endSets(xs, ad);
        String[][] ends = {
            {ad.property1().targetClassFqn(), sets.a()},
            {ad.property2().targetClassFqn(), sets.b()},
        };
        LegacyMappingDefinition.ForeignSet foreign = null;
        for (String[] end : ends) {
            if (hasSet(md, end[0], end[1])) {
                continue;
            }
            LegacyMappingDefinition.ForeignSet f = foreignSetOf(md, end[0], end[1]);
            if (f == null) {
                // an end naming no set at all: synthesis raises the compile
                // error, as before
                return null;
            }
            if (foreign == null) {
                foreign = f;
            }
        }
        return foreign == null ? null
                : "XStore association '" + ad.qualifiedName() + "' joins set '" + foreign.effectiveId()
                        + "' of '" + foreign.className() + "', a " + foreign.store() + " class mapping: lite"
                        + " does not execute " + foreign.store() + " (register row S413)";
    }

    /** Whether a class mapping of the closure, of any kind, is the end. */
    private static boolean hasSet(ResolvedMapping md, String classFqn, @com.legend.base.Nullable String setId) {
        for (LegacyMappingDefinition m : md.closure()) {
            for (ClassMapping cm : m.classMappings()) {
                if (cm.className().equals(classFqn)
                        && (setId == null || setId.equals(ResolvedMapping.idOf(cm)))) {
                    return true;
                }
            }
        }
        return false;
    }

    /** The foreign set of the closure that is the end. */
    private static LegacyMappingDefinition.@com.legend.base.Nullable ForeignSet foreignSetOf(
            ResolvedMapping md, String classFqn, @com.legend.base.Nullable String setId) {
        for (LegacyMappingDefinition m : md.closure()) {
            for (LegacyMappingDefinition.ForeignSet f : m.foreignSets()) {
                if (f.className().equals(classFqn)
                        && (setId == null || setId.equals(f.effectiveId()))) {
                    return f;
                }
            }
        }
        return null;
    }

    static XEnd xstoreEndOf(ResolvedMapping md,
            String classFqn, @com.legend.base.Nullable String setId, ModelBuilder model) {
        // the end sets may live in INCLUDED mappings (modelJoins:
        // XStore lines over include LegalEntityMapping/TradesMapping) —
        // the engine compiles the include closure as one mapping
        List<LegacyMappingDefinition> closure = new ArrayList<>();
        closure.addAll(md.closure());
        List<ClassMapping> cms = new ArrayList<>();
        for (LegacyMappingDefinition m : closure) {
            cms.addAll(m.classMappings());
        }
        for (ClassMapping cm : cms) {
            if (cm instanceof ClassMapping.RelationFunction rf
                    && rf.className().equals(classFqn)
                    && (setId == null
                            || setId.equals(ResolvedMapping.idOf(rf)))) {
                Set<String> locals = new LinkedHashSet<>();
                for (ClassMapping.RelationFunction.Col c : rf.columns()) {
                    if (c.local()) {
                        locals.add(c.property());
                    }
                }
                return new XEnd(
                        MappingNormalizer.relationFunctionPipeline(rf, model),
                        rf, ResolvedMapping.idOf(rf), false, locals);
            }
        }
        for (ClassMapping cm : cms) {
            if (cm instanceof ClassMapping.Relational rcm
                    && rcm.className().equals(classFqn)
                    && (setId == null
                            || setId.equals(ResolvedMapping.idOf(rcm)))) {
                List<ClassMapping.RelationFunction.Col> cols = new ArrayList<>();
                Set<String> locals = new LinkedHashSet<>();
                Map<String, TypeExpression> localTypes = new LinkedHashMap<>();
                // LOSSY: a property the column view cannot carry — an
                // expression-bound or join-chain +prop, a join-mapped
                // scalar, an embedded ctor. Such an end takes the
                // property-space route, where the resolver substitutes the
                // set's REAL bindings (the engine compiles each end's
                // property mapping into the condition —
                // relationalModelJoins.pure; batch 110). An EXACT view
                // keeps the column-space emission verbatim.
                boolean lossy = false;
                for (PropertyMapping pm : rcm.propertyMappings()) {
                    if (pm instanceof PropertyMapping.Column c && c.binding() != null) {
                        // a Binding-decoded column is an object, not the
                        // column: the view cannot carry it (2026-11-14)
                        lossy = true;
                    } else if (pm instanceof PropertyMapping.Column c) {
                        cols.add(new ClassMapping.RelationFunction.Col(
                                c.propertyName(), c.column(), false));
                    } else if (pm instanceof PropertyMapping.LocalProperty lp) {
                        if (lp.body() instanceof PropertyMapping.Column lc) {
                            cols.add(new ClassMapping.RelationFunction.Col(
                                    lp.propertyName(), lc.column(), true));
                        } else {
                            lossy = true;
                        }
                        locals.add(lp.propertyName());
                        localTypes.put(lp.propertyName(), lp.type());
                    } else {
                        lossy = true;
                    }
                }
                // TABLE-BACKED: the column view is LOSSY (a +prop bound to
                // an expression or a join chain has no column here) — an
                // XStore over such an end takes the property-space route,
                // where the resolver substitutes the set's REAL bindings
                // (the engine compiles each end's property mapping into the
                // condition — relationalModelJoins.pure; batch 110)
                return new XEnd(
                        ViewRelation.mainSourceRef(md, classFqn, model),
                        new ClassMapping.RelationFunction(classFqn,
                                ResolvedMapping.idOf(rcm), null, rcm.root(),
                                "<relational>", cols),
                        ResolvedMapping.idOf(rcm), false, locals, lossy,
                        localTypes);
            }
        }
        for (ClassMapping cm : cms) {
            if (cm instanceof ClassMapping.Pure pcm
                    && pcm.className().equals(classFqn)
                    && (setId == null
                            || setId.equals(ResolvedMapping.idOf(pcm)))) {
                Set<String> locals = new LinkedHashSet<>();
                for (ClassMapping.Pure.PropertyBinding pb
                        : pcm.propertyBindings()) {
                    if (pb.local()) {
                        locals.add(pb.propertyName());
                    }
                }
                return new XEnd(null, null,
                        ResolvedMapping.idOf(pcm), true, locals);
            }
        }
        throw new NotImplementedException(
                "XStore/ModelJoin association end class '" + classFqn
                + "' resolves to no Relation or Relational set"
                + (setId != null ? " for set id '" + setId + "'" : "")
                + " in '" + md.qualifiedName() + "'");
    }

    /**
     * The property-space synthesis — same orientation and
     * direction-agreement rules as the column-space emission, condition
     * kept over the end classes with set-local reads marked.
     */
    static FunctionDefinition synthesize(ResolvedMapping md,
            AssociationMapping.Cross xs, AssociationDefinition ad,
            String classA, String classB, XEnd endA, XEnd endB) {
        Variable srcRow = new Variable("srcRow");
        Variable tgtRow = new Variable("tgtRow");
        boolean selfAssoc = classA.equals(classB);
        List<ValueSpecification> conds = new ArrayList<>();
        for (AssociationMapping.Cross.XStoreProperty cand
                : xs.propertyMappings2()) {
            boolean isProp1 = cand.propertyName()
                    .equals(ad.property1().propertyName());
            if (!isProp1 && !cand.propertyName()
                    .equals(ad.property2().propertyName())) {
                throw new ModelException(
                        LegendCompileException.Phase.NORMALIZE,
                        "XStore line '" + cand.propertyName() + "' matches"
                        + " neither end of association '" + xs.associationName()
                        + "'; mapping=" + md.qualifiedName());
            }
            // orientation mirrors synthesizeXStoreMapping (audit 8 S1)
            Variable thatRow;
            if (selfAssoc) {
                thatRow = isProp1 ? tgtRow : srcRow;
            } else {
                thatRow = isProp1 ? srcRow : tgtRow;
            }
            Variable thisRow = thatRow == srcRow ? tgtRow : srcRow;
            XEnd thatEnd = isProp1 ? endA : endB;
            XEnd thisEnd = isProp1 ? endB : endA;
            // AUTHORED operand order is emitted (the engine spells
            // `$this.entityIdFk == $that.entityId` verbatim — the modelJoins
            // goldens, and testPersonToFirmUsingFromProject's XStore plan
            // equals its single-store plan); canonicalization serves ONLY
            // the direction-agreement comparison below (batch 110 — the
            // same rule synthesizeXStoreMapping applies)
            conds.add(renameReads(cand.expression(),
                    Map.of("this", thisRow, "that", thatRow),
                    Map.of("this", thisEnd, "that", thatEnd)));
        }
        if (conds.isEmpty()) {
            throw new ModelException(
                    LegendCompileException.Phase.NORMALIZE,
                    "XStore mapping for '" + xs.associationName()
                    + "' has no property lines; mapping=" + md.qualifiedName());
        }
        ValueSpecification cond = conds.get(0);
        ValueSpecification canon0 = MappingNormalizer.canonicalizeEqualOperands(
                cond, srcRow.name());
        for (ValueSpecification c : conds) {
            if (!MappingNormalizer.canonicalizeEqualOperands(c, srcRow.name())
                    .equals(canon0)) {
                throw new NotImplementedException(
                        "XStore association '" + xs.associationName()
                        + "' has direction-specific conditions; a single"
                        + " shared predicate is required for now (mapping="
                        + md.qualifiedName() + ")");
            }
        }
        return predicate(md, xs.associationName(), classA, classB, endA, endB,
                srcRow, tgtRow, cond);
    }

    /**
     * The property-space synthesis of a ModelJoin with a Pure-set end
     * ({@code {firm: Firm[1], employee: Person[1] | $firm.legalName ==
     * $employee.firstName}}): the lambda's two parameters, paired with the
     * association's ends ({@link ModelJoinNesting#pairEndVars}: {@code aVar}
     * the first end's), rename onto the predicate's rows exactly as an
     * XStore line's {@code $this}/{@code $that} do, and the same predicate is
     * emitted -- the sets pinned by id, the condition over the end classes,
     * the resolver substituting each side through its set's composed
     * bindings (2026-11-14, the Studio roundtrip's
     * DSL_Mapping-model-join-mapping; until then refused, "the property-space
     * route covers XStore only so far").
     */
    static FunctionDefinition synthesizeModelJoin(ResolvedMapping md,
            AssociationMapping.ModelJoin mj, String classA, String classB,
            XEnd endA, XEnd endB, String aVar, String bVar) {
        Variable srcRow = new Variable("srcRow");
        Variable tgtRow = new Variable("tgtRow");
        ValueSpecification cond = renameReads(
                mj.lambda().body().get(mj.lambda().body().size() - 1),
                Map.of(aVar, srcRow, bVar, tgtRow),
                Map.of(aVar, endA, bVar, endB));
        return predicate(md, mj.associationName(), classA, classB, endA, endB,
                srcRow, tgtRow, cond);
    }

    /** {@code legacyAssocPredicate(a, b, 'setA', 'setB', {srcRow, tgtRow | cond})}
     *  as the association's synthesized function. */
    private static FunctionDefinition predicate(ResolvedMapping md,
            String associationName, String classA, String classB,
            XEnd endA, XEnd endB, Variable srcRow, Variable tgtRow,
            ValueSpecification cond) {
        Variable a = new Variable("a");
        Variable b = new Variable("b");
        ValueSpecification body = Calls.exact(Pure.Lite.LEGACY_ASSOC_PREDICATE,
                List.of(a, b,
                        new CString(endA.setId()),
                        new CString(endB.setId()),
                        new LambdaFunction(List.of(srcRow, tgtRow),
                                List.of(cond))));
        FunctionDefinition.ParameterDefinition pA =
                new FunctionDefinition.ParameterDefinition("a",
                        new TypeExpression.NameRef(classA),
                        Multiplicity.Concrete.PURE_ONE);
        FunctionDefinition.ParameterDefinition pB =
                new FunctionDefinition.ParameterDefinition("b",
                        new TypeExpression.NameRef(classB),
                        Multiplicity.Concrete.PURE_ONE);
        return new FunctionDefinition(
                SynthFqn.mappingAssoc(md.qualifiedName(), associationName),
                List.of(), List.of(), List.of(pA, pB),
                new TypeExpression.NameRef(
                        "meta::pure::metamodel::type::Boolean"),
                Multiplicity.Concrete.PURE_ONE,
                List.of(body),
                List.of(), List.of())
                .withSynthesizedFrom(new FunctionDefinition.Synthesized(
                        SynthHat.ASSOC, md.qualifiedName(),
                        associationName));
    }

    /** {@code $this.p}/{@code $that.p} → the oriented row var's read —
     * a plain property read, or the {@code legacyLocalProperty} marker
     * when the end SET declares {@code p} as a local ({@code +p}). Deep
     * reads recurse through the receiver (the head var renames; the tail
     * types — or walls — at compile). */
    private static ValueSpecification renameReads(ValueSpecification v,
            Map<String, Variable> rowByVar, Map<String, XEnd> endByVar) {
        if (v instanceof AppliedProperty ap
                && ap.receiver() instanceof Variable var
                && rowByVar.containsKey(var.name())) {
            Variable row = rowByVar.get(var.name());
            XEnd end = java.util.Objects.requireNonNull(endByVar.get(var.name()));
            if (end.localProps().contains(ap.property())) {
                // NOTE (batch 110 probe): the marker is Any-typed, so an
                // ORDERING comparison over a +prop on this route does not
                // type (lessThan(Any, Integer)); a cast to the declared
                // local type was tried and regressed six XStore Pure-end
                // tests — the typed-local read is an open leg of route A
                return Calls.exact(Pure.Lite.LEGACY_LOCAL_PROPERTY,
                        List.of(row, new CString(ap.property())));
            }
            return new AppliedProperty(row, ap.property());
        }
        return switch (v) {
            case Variable var when rowByVar.containsKey(var.name()) ->
                    rowByVar.get(var.name());
            case AppliedFunction af -> af.withParameters(
                    af.parameters().stream().map(x ->
                            renameReads(x, rowByVar, endByVar)).toList());
            case AppliedProperty ap2 -> new AppliedProperty(
                    renameReads(ap2.receiver(), rowByVar, endByVar),
                    ap2.property());
            case PureCollection pc -> new PureCollection(
                    pc.values().stream().map(x ->
                            renameReads(x, rowByVar, endByVar)).toList());
            case LambdaFunction lf -> {
                // a lambda's own parameter SHADOWS an end's variable inside it: a ModelJoin names its ends as
                // the user wrote them ({firm, employee | ... ->exists(employee | $employee.id == 1)}); an
                // XStore's this/that cannot be shadowed
                Map<String, Variable> rows = new java.util.HashMap<>(rowByVar);
                lf.parameters().forEach(p -> rows.remove(p.name()));
                yield new LambdaFunction(lf.parameters(),
                        lf.body().stream().map(x ->
                                renameReads(x, rows, endByVar)).toList());
            }
            default -> v.mapChildren(x -> renameReads(x, rowByVar, endByVar));
        };
    }
}
