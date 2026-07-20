// SPDX-License-Identifier: Apache-2.0

package com.legend.normalizer;

import com.legend.compiler.ModelBuilder;
import com.legend.compiler.SynthFqn;
import com.legend.error.LegendCompileException;
import com.legend.error.ModelException;
import com.legend.error.NotImplementedException;
import com.legend.model.Multiplicity;
import com.legend.model.NormalizedModel;
import com.legend.model.ParsedModel;
import com.legend.model.TypeExpression;
import com.legend.model.AssociationDefinition;
import com.legend.model.AssociationMapping;
import com.legend.model.AssociationPropertyMapping;
import com.legend.model.ClassDefinition;
import com.legend.model.ClassMapping;
import com.legend.model.ComparisonOp;
import com.legend.model.DatabaseDefinition;
import com.legend.model.EnumerationMapping;
import com.legend.model.FilterMapping;
import com.legend.model.FilterPointer;
import com.legend.model.FunctionDefinition;
import com.legend.model.JoinChainElement;
import com.legend.model.LegacyMappingDefinition;
import com.legend.model.LogicalOp;
import com.legend.model.MappingDefinition;
import com.legend.model.MappingInclude;
import com.legend.model.PackageableElement;
import com.legend.model.PropertyMapping;
import com.legend.model.Realization;
import com.legend.model.RelationalDataType;
import com.legend.model.RelationalOperation;
import com.legend.model.SynthHat;
import com.legend.model.spec.AppliedFunction;
import com.legend.model.spec.AppliedProperty;
import com.legend.model.spec.CBoolean;
import com.legend.model.spec.CFloat;
import com.legend.model.spec.CInteger;
import com.legend.model.spec.CString;
import com.legend.model.spec.ColSpec;
import com.legend.model.spec.ColSpecArray;
import com.legend.model.spec.EnumValue;
import com.legend.model.spec.KeyExpression;
import com.legend.model.spec.LambdaFunction;
import com.legend.model.spec.NewInstance;
import com.legend.model.spec.NewInstanceCast;
import com.legend.model.spec.PackageableElementPtr;
import com.legend.model.spec.PureCollection;
import com.legend.model.spec.TypeAnnotation;
import com.legend.model.spec.ValueSpecification;
import com.legend.model.spec.Variable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
/**
 * Association-mapping realization: multi-hop end injection (Option A), per-pair entry gathering, predicate-function synthesis (doc 5.6.1). Split from MappingNormalizer (the Doors split).
 */
final class AssociationSynthesis {

    private AssociationSynthesis() {}

    /**
     * Option A ({@code docs/MAPPING_LEGACY_TO_FUNCTION.md} §5.6.1b): a multi-hop
     * association end is realized as a class-typed Join PM on the end's
     * owning class, flowing through the same join-chain machinery as a
     * class-typed property mapping ({@link #emitJoinChain}). This pre-pass
     * appends those Join PMs to the relevant {@link ClassMapping.Relational}
     * before class-mapping synthesis. Single-hop ends are left to the
     * standalone {@code legacyAssocPredicate} path (§5.6.1).
     */
    static LegacyMappingDefinition injectMultiHopAssociationPMs(LegacyMappingDefinition md,
                                                                 ModelBuilder model) {
        if (md.associationMappings().isEmpty()) return md;
        Map<String, List<PropertyMapping>> injected = new LinkedHashMap<>();
        for (AssociationMapping am : md.associationMappings()) {
            if (!(am instanceof AssociationMapping.Relational rel)) continue;
            AssociationDefinition ad = model.findAssociation(am.associationName()).orElse(null);
            if (ad == null) continue;
            for (AssociationPropertyMapping apm : rel.propertyMappings()) {
                if (!(apm.body() instanceof PropertyMapping.Join join)) continue;
                if (join.joins().size() < 2) {
                    continue;   // single-hop -> predicate path
                }
                String owner = associationOwnerClass(ad, apm.propertyName());
                if (owner == null) continue;
                if (apm.sourceSetId() != null
                        && UnionSynthesis.unionForClass(md, model, owner) != null) {
                    // per-pair entries on a UNION-mapped owner land on their
                    // member set at union synthesis instead
                    // (collectPairAssociationEntries, include-closure aware)
                    continue;
                }
                injected.computeIfAbsent(owner, k -> new ArrayList<>()).add(join);
            }
        }
        if (injected.isEmpty()) return md;
        List<ClassMapping> rewritten = new ArrayList<>(md.classMappings().size());
        for (ClassMapping cm : md.classMappings()) {
            List<PropertyMapping> add = injected.get(cm.className());
            if (add != null && cm instanceof ClassMapping.Relational rcm) {
                List<PropertyMapping> pms = new ArrayList<>(rcm.propertyMappings());
                pms.addAll(add);
                rewritten.add(new ClassMapping.Relational(
                        rcm.className(), rcm.setId(), rcm.extendsSetId(), rcm.root(),
                        rcm.mainTable(), rcm.filter(), rcm.distinct(), rcm.groupBy(),
                        rcm.primaryKey(), pms, rcm.sourceUrl(),
                        rcm.propertyTargetSets()));
            } else {
                rewritten.add(cm);
            }
        }
        return md.withClassMappings(rewritten);
    }

    /**
     * SET-QUALIFIED per-pair AssociationMapping entries
     * ({@code y[x1, y1]: [db]@X1_A > @A_Y1} — multipleChainedJoins) whose
     * OWNING end is union class {@code classFqn}, gathered across the
     * INCLUDE CLOSURE (the entries, the members and the union operation
     * may live in different mapping definitions), keyed by the owning
     * member's set id with the target route stamped on the Join.
     */
    static void collectPairAssociationEntries(LegacyMappingDefinition md,
            ModelBuilder model, String classFqn,
            Map<String, List<PropertyMapping.Join>> out, Set<String> seen) {
        if (!seen.add(md.qualifiedName())) {
            return;
        }
        for (AssociationMapping am : md.associationMappings()) {
            if (!(am instanceof AssociationMapping.Relational rel)) continue;
            AssociationDefinition ad =
                    model.findAssociation(am.associationName()).orElse(null);
            if (ad == null) continue;
            for (AssociationPropertyMapping apm : rel.propertyMappings()) {
                if (!(apm.body() instanceof PropertyMapping.Join join)
                        || apm.sourceSetId() == null) {
                    continue;
                }
                String owner = associationOwnerClass(ad, apm.propertyName());
                if (owner == null || !owner.equals(classFqn)) {
                    continue;
                }
                PropertyMapping.Join stamped = join.targetSetId() == null
                        ? new PropertyMapping.Join(join.propertyName(),
                                join.database(), join.joins(), apm.targetSetId())
                        : join;
                out.computeIfAbsent(apm.sourceSetId(), k -> new ArrayList<>())
                        .add(stamped);
            }
        }
        for (MappingInclude inc : md.includes()) {
            LegacyMappingDefinition inner =
                    model.findLegacyMapping(inc.mappingPath()).orElse(null);
            if (inner != null) {
                collectPairAssociationEntries(inner, model, classFqn, out, seen);
            }
        }
    }

    /**
     * The class that <em>owns</em> association property {@code propName}: in
     * {@code Association(p1: B, p2: A)}, property {@code p1} is declared on the
     * class {@code p2} points at (and vice versa). Returns {@code null} if
     * {@code propName} is neither end, or the opposite end is non-NameRef.
     */
    static String associationOwnerClass(AssociationDefinition ad, String propName) {
        if (ad.property1().propertyName().equals(propName)) {
            return MappingNormalizer.nameRefOrNull(ad.property2().targetClass());
        }
        if (ad.property2().propertyName().equals(propName)) {
            return MappingNormalizer.nameRefOrNull(ad.property1().targetClass());
        }
        return null;
    }

    static FunctionDefinition synthesizeAssociationMapping(LegacyMappingDefinition md,
                                                                  AssociationMapping am,
                                                                  ModelBuilder model) {
        AssociationDefinition ad0 = model.findAssociation(am.associationName())
                .orElse(null);
        if (am instanceof AssociationMapping.ModelJoin mj && ad0 != null) {
            return MappingNormalizer.synthesizeModelJoinMapping(md, mj, model,
                    ad0.property1().targetClassFqn(),
                    ad0.property2().targetClassFqn());
        }
        if (am instanceof AssociationMapping.Cross xs && ad0 != null) {
            return MappingNormalizer.synthesizeXStoreMapping(md, xs, model,
                    ad0.property1().targetClassFqn(),
                    ad0.property2().targetClassFqn());
        }
        if (!(am instanceof AssociationMapping.Relational rel)) {
            throw new NotImplementedException(
                    "Association mapping kind " + am.getClass().getSimpleName()
                  + " not supported; mapping=" + md.qualifiedName());
        }
        AssociationDefinition ad = model.findAssociation(am.associationName())
                .orElseThrow(() -> new ModelException(LegendCompileException.Phase.NORMALIZE, 
                        "AssociationMapping references unknown association '"
                      + am.associationName() + "'; mapping=" + md.qualifiedName()));
        String classA = ad.property1().targetClassFqn();
        String classB = ad.property2().targetClassFqn();

        if (rel.propertyMappings().isEmpty()) {
            throw new ModelException(LegendCompileException.Phase.NORMALIZE, 
                    "AssociationMapping for '" + am.associationName()
                  + "' has no property mappings; mapping=" + md.qualifiedName());
        }
        // Pick the FIRST property mapping as the primary; multi-PM
        // disambiguation by [srcSetId, tgtSetId] could differentiate
        // direction-specific joins but the predicate condition is the
        // same regardless (it describes the association in one place).
        AssociationPropertyMapping firstAm = rel.propertyMappings().get(0);
        if (!(firstAm.body() instanceof PropertyMapping.Join firstJoin)) {
            throw new NotImplementedException(
                    "AssociationMapping property body kind "
                  + firstAm.body().getClass().getSimpleName()
                  + " not supported (only Join bodies are bridged); mapping="
                  + md.qualifiedName()
                  + ". See docs/MAPPING_LEGACY_TO_FUNCTION.md §5.6.");
        }
        // Multi-hop association: realized as per-end navigation injected into
        // the class realizing functions (Option A; see
        // docs/MAPPING_LEGACY_TO_FUNCTION.md §5.6.1b). A (A,B)->Boolean predicate
        // cannot bind the intermediate row, so no standalone predicate is
        // emitted — return null and let injectMultiHopAssociationPMs handle it.
        if (firstJoin.joins().size() >= 2) {
            return null;
        }
        // First-PM-wins is RETAINED (audit 23 probed-and-reverted): a
        // direction-agreement wall broke testSimpleQueryToAssociationMapping
        // + testProjectThroughAssoWithAssociationMapping — classic
        // association mappings legitimately spell the two directions with
        // DIFFERENT (inverse-equivalent) joins, and the first join's
        // predicate is row-correct for both (engine joins are
        // direction-neutral). Residual: two directions with genuinely
        // NON-equivalent joins would still take the first silently.
        // An end class with NO ~mainTable mapping (its properties live only
        // as Join PMs on the other end) cannot anchor a standalone
        // (A,B)->Boolean predicate — no binding is emitted, and NAVIGATING
        // the association stays loud at resolve time ("association not
        // mapped in mapping"). Declaring it is not an error.
        if (!MappingNormalizer.hasMainTable(md, classA, model) || !MappingNormalizer.hasMainTable(md, classB, model)) {
            return null;
        }

        Variable srcRow = new Variable("srcRow");
        Variable tgtRow = new Variable("tgtRow");
        ValueSpecification predicateBody = buildAssocPredicateBody(firstJoin, classA,
                classB, srcRow, tgtRow, am.associationName(), md, model);
        // predicateBody's tgtRow reads the JOIN's landing table; the call
        // declares tgtRow's row type as classB's ~mainTable. Those must be
        // the SAME table or the lambda would silently mistype (checked
        // inside buildAssocPredicateBody, which knows the landing table).

        Variable a = new Variable("a");
        Variable b = new Variable("b");
        // The adapter lambda speaks ROW scope; its row types are knowable
        // right here (the two ends' ~mainTable), so say them: the src/tgt
        // Relation args bind the signature's S,T and the lambda's columns
        // type through the ordinary kernel — no Any punt, no bespoke
        // checker. The resolver reads the tables from the CALL instead of
        // re-deriving them from the classes' mappings.
        ValueSpecification body = new AppliedFunction("legacyAssocPredicate", List.of(
                a, b,
                MappingNormalizer.mainTableRefOf(md, classA, model),
                MappingNormalizer.mainTableRefOf(md, classB, model),
                new LambdaFunction(List.of(srcRow, tgtRow),
                                         List.of(predicateBody))));

        FunctionDefinition.ParameterDefinition pA = new FunctionDefinition.ParameterDefinition(
                "a", new TypeExpression.NameRef(classA), Multiplicity.Concrete.PURE_ONE);
        FunctionDefinition.ParameterDefinition pB = new FunctionDefinition.ParameterDefinition(
                "b", new TypeExpression.NameRef(classB), Multiplicity.Concrete.PURE_ONE);
        return new FunctionDefinition(
                SynthFqn.mappingAssoc(md.qualifiedName(), am.associationName()),
                List.of(), List.of(), List.of(pA, pB),
                new TypeExpression.NameRef("meta::pure::metamodel::type::Boolean"),
                Multiplicity.Concrete.PURE_ONE,
                List.of(body),
                List.of(), List.of())
                .withSynthesizedFrom(new FunctionDefinition.Synthesized(
                        SynthHat.ASSOC, md.qualifiedName(), am.associationName()));
    }

    /**
     * Build the predicate body for an AssociationMapping. Single-hop
     * joins translate to the join condition directly over (srcRow,
     * tgtRow). Multi-hop joins chain conditions through intermediate
     * row bindings: each hop's predicate is conjoined with the next
     * via {@code and(...)}, with intermediate rows resolved by named
     * binding through the chain alias scope.
     */
    static ValueSpecification buildAssocPredicateBody(PropertyMapping.Join join,
                                                             String classA, String classB,
                                                             Variable srcRow, Variable tgtRow,
                                                             String associationName,
                                                             LegacyMappingDefinition md,
                                                             ModelBuilder model) {
        if (join.joins().isEmpty()) {
            throw new ModelException(LegendCompileException.Phase.NORMALIZE, 
                    "AssociationMapping for '" + associationName
                  + "' has empty join chain; mapping=" + md.qualifiedName());
        }
        String sourceTable = MappingNormalizer.mainTableOf(md, classA, model);
        if (join.joins().size() == 1) {
            JoinChainElement hop = join.joins().get(0);
            String hopDb = hop.databaseName() != null ? hop.databaseName() : join.database();
            DatabaseDefinition.JoinDefinition jd = model.findJoin(hopDb, hop.joinName())
                    .orElseThrow(() -> new ModelException(LegendCompileException.Phase.NORMALIZE, 
                            "AssociationMapping join '" + hop.joinName()
                          + "' not found in db '" + hopDb + "'; association='"
                          + associationName + "', mapping=" + md.qualifiedName()));
            RelationalOperation cond2 = MappingNormalizer.resolveViewRefsInJoin(
                    jd.operation(), hopDb, sourceTable, model, md,
                    model.findView(hopDb, sourceTable).isPresent() ? sourceTable : null,
                    null);
            String targetTable = MappingNormalizer.determineTargetTable(cond2, sourceTable,
                    hop.joinName(), associationName, 1, md.qualifiedName());
            MappingNormalizer.requireNonViewTarget(targetTable, hopDb, hop.joinName(), model, md);
            // The synthesized legacyAssocPredicate call declares tgtRow's row
            // type as classB's ~mainTable; the join must actually land there,
            // or the lambda's column reads would silently mistype.
            String classBTable = MappingNormalizer.mainTableOf(md, classB, model);
            if (!targetTable.equals(classBTable)) {
                throw new NotImplementedException(
                        "AssociationMapping join '" + hop.joinName() + "' lands on table '"
                      + targetTable + "' but the target end class '" + classB
                      + "' is mapped to ~mainTable '" + classBTable + "'; an "
                      + "association joining through a non-mainTable row is not "
                      + "supported. Association='" + associationName + "', mapping="
                      + md.qualifiedName());
            }
            Map<String, ValueSpecification> condScope = new LinkedHashMap<>();
            condScope.put(sourceTable, srcRow);
            if (!targetTable.equals(sourceTable)) condScope.put(targetTable, tgtRow);
            return RelOpTranslator.translate(jd.operation(), condScope, tgtRow, /*rowBind*/ null, RelOpTranslator.PipelineView.NONE);
        }
        // Unreachable: multi-hop associations are intercepted in
        // synthesizeAssociationMapping (returns null) and realized as per-end
        // navigation injected into the class realizing functions (Option A;
        // see docs/MAPPING_LEGACY_TO_FUNCTION.md §5.6.1b). A (A,B)->Boolean
        // predicate cannot bind the intermediate row(s). This guard fires only
        // if that interception is bypassed — a compiler invariant violation.
        throw new ModelException(LegendCompileException.Phase.NORMALIZE, 
                "Multi-hop AssociationMapping for '" + associationName + "' ("
              + join.joins().size() + " join hops) reached the predicate "
              + "builder; it should have been handled by per-end injection. "
              + "Mapping=" + md.qualifiedName());
    }

}
