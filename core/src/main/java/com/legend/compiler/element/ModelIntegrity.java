package com.legend.compiler.element;

import com.legend.compiler.ModelBuilder;
import com.legend.compiler.SynthFqn;
import com.legend.parser.element.ClassDefinition;
import com.legend.parser.element.Function;
import com.legend.parser.element.MappingDefinition;
import com.legend.parser.element.Realization;

import java.util.List;

/**
 * THE eager reference-safety pass (F.a + F.b unified): every reference a model
 * element makes — type names, realizing functions, mapping bindings,
 * association ends — must exist (and, for behavior hats, have the right
 * structural shape), checked ONCE, whole-model, at context construction. An
 * invalid model never becomes a queryable context, even if nothing ever
 * demands the bad element. Knowledge-level only: classification and symbol
 * lookups, no {@code Typed*} materialization; bodies are Phase G.
 *
 * <p>ONE pass because these checks share their nature (dangling references),
 * their time (construction), and their input (the whole model) — previously
 * scattered across a lazy check inside the class compiler (which missed
 * undemanded elements entirely — found by the pipeline stage-failure suite)
 * and a separate mapping-binding walk.
 */
final class ModelIntegrity {

    private ModelIntegrity() {
    }

    static void check(ModelBuilder model, TypeClassifier classifier, FunctionCompiler functions) {
        model.classes().forEach(cd -> checkClass(cd, classifier, functions));
        model.functions().forEach(f -> checkFunction(f, classifier));
        model.associations().forEach(a -> {
            classifier.classify(a.property1().targetClass(), List.of());
            classifier.classify(a.property2().targetClass(), List.of());
        });
        model.mappings().forEach(md -> checkMapping(md, model, classifier, functions));
    }

    /** Class references: property/derived types + realizer functions + constraint shapes. */
    private static void checkClass(ClassDefinition cd, TypeClassifier classifier,
                                   FunctionCompiler functions) {
        List<String> typeParams = cd.typeParams();
        for (ClassDefinition.PropertyDefinition pd : cd.properties()) {
            classifier.classify(pd.type(), typeParams);
        }
        for (ClassDefinition.DerivedPropertyDefinition dp : cd.derivedProperties()) {
            classifier.classify(dp.type(), typeParams);
            for (ClassDefinition.ParameterDefinition p : dp.parameters()) {
                classifier.classify(p.type(), typeParams);
            }
            functions.requireFunction(
                    realizedFqn(dp.realization(), SynthFqn.prop(cd.qualifiedName(), dp.name())),
                    "derived property '" + dp.name() + "' of " + cd.qualifiedName());
        }
        for (ClassDefinition.ConstraintDefinition con : cd.constraints()) {
            String fqn = realizedFqn(con.realization(),
                    SynthFqn.constraint(cd.qualifiedName(), con.name()));
            String site = "constraint '" + con.name() + "' of " + cd.qualifiedName();
            functions.requireFunction(fqn, site);
            functions.requireShape(fqn, FunctionCompiler::returnsBooleanOne,
                    site, "returning Boolean[1]");
        }
    }

    /** Function signature references (params + return). */
    private static void checkFunction(Function f, TypeClassifier classifier) {
        for (var p : f.parameters()) {
            classifier.classify(p.type(), f.typeParameters());
        }
        classifier.classify(f.returnType(), f.typeParameters());
    }

    /**
     * F.b mapping-binding integrity: every class binding names a real class
     * realized by a real {@code (): Class[*]} function; every association
     * binding names a real association realized by a real
     * {@code (Source, Target): Boolean[1]} predicate.
     */
    private static void checkMapping(MappingDefinition md, ModelBuilder model,
                                     TypeClassifier classifier, FunctionCompiler functions) {
        for (MappingDefinition.ClassBinding cb : md.classBindings()) {
            if (!classifier.isClassFqn(cb.classFqn())) {
                throw new com.legend.error.ModelException(com.legend.error.LegendCompileException.Phase.MODEL, "mapping '" + md.qualifiedName()
                      + "' binds unknown class '" + cb.classFqn() + "'");
            }
            String site = "mapping '" + md.qualifiedName()
                    + "' class binding for '" + cb.classFqn() + "'";
            functions.requireFunction(cb.functionFqn(), site);
            functions.requireShape(cb.functionFqn(),
                    f -> f.parameters().isEmpty() && functions.returnsClassMany(f),
                    site, "of the form (): Class[*]");
        }
        for (MappingDefinition.AssociationBinding ab : md.associationBindings()) {
            if (model.findAssociation(ab.associationFqn()).isEmpty()) {
                throw new com.legend.error.ModelException(com.legend.error.LegendCompileException.Phase.MODEL, "mapping '" + md.qualifiedName()
                      + "' binds unknown association '" + ab.associationFqn() + "'");
            }
            String site = "mapping '" + md.qualifiedName()
                    + "' association binding for '" + ab.associationFqn() + "'";
            functions.requireFunction(ab.predicateFunctionFqn(), site);
            functions.requireShape(ab.predicateFunctionFqn(),
                    f -> f.parameters().size() == 2 && FunctionCompiler.returnsBooleanOne(f),
                    site, "of the form (source, target): Boolean[1]");
        }
    }

    private static String realizedFqn(Realization r, String liftedFqn) {
        return r instanceof Realization.Ref ref ? ref.functionFqn() : liftedFqn;
    }
}
