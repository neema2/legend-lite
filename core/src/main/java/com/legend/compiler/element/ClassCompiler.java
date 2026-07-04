package com.legend.compiler.element;

import com.legend.compiler.SynthFqn;
import com.legend.parser.TypeExpression;
import com.legend.parser.element.ClassDefinition;
import com.legend.parser.element.Realization;

import java.util.ArrayList;
import java.util.List;

/**
 * Phase F's <strong>class-element</strong> compiler: a parsed
 * {@link ClassDefinition} &rarr; its {@link TypedClass} &mdash; stored and
 * derived properties, constraints, superclass FQNs. Types classify through the
 * {@link TypeClassifier} kernel; every behavior hat (derived property,
 * constraint) gets F.a/F.c reference safety through the
 * {@link FunctionCompiler}. Bodies are Phase G.
 */
final class ClassCompiler {

    private final TypeClassifier classifier;
    private final FunctionCompiler functions;

    ClassCompiler(TypeClassifier classifier, FunctionCompiler functions) {
        this.classifier = classifier;
        this.functions = functions;
    }

    TypedClass compile(ClassDefinition cd) {
        List<String> typeParams = cd.typeParams();

        List<String> superFqns = new ArrayList<>(cd.superClasses().size());
        for (TypeExpression sup : cd.superClasses()) {
            superFqns.add(TypeClassifier.headFqn(sup));
        }

        List<Property> properties = new ArrayList<>();
        for (ClassDefinition.PropertyDefinition pd : cd.properties()) {
            properties.add(new Property.Stored(
                    pd.name(),
                    classifier.classify(pd.type(), typeParams),
                    TypeClassifier.multiplicity(pd.multiplicity())));
        }
        for (ClassDefinition.DerivedPropertyDefinition dp : cd.derivedProperties()) {
            List<TypedParameter> params = new ArrayList<>(dp.parameters().size());
            for (ClassDefinition.ParameterDefinition p : dp.parameters()) {
                params.add(new TypedParameter(
                        p.name(),
                        classifier.classify(p.type(), typeParams),
                        TypeClassifier.multiplicity(p.multiplicity())));
            }
            // The realizing function FQN: for the sugar (Inline) form it is the
            // lifted <owner>$prop$<name>; for a Door-4 ref binding it is the
            // user-bound function FQN. Either way the typed property carries only
            // a signature + that FQN reference.
            // Reference safety already ran (ModelIntegrity, eager+total).
            String fqn = realizedFqn(dp.realization(), SynthFqn.prop(cd.qualifiedName(), dp.name()));
            properties.add(new Property.Derived(
                    dp.name(),
                    classifier.classify(dp.type(), typeParams),
                    TypeClassifier.multiplicity(dp.multiplicity()),
                    params,
                    fqn));
        }

        List<TypedConstraint> constraints = new ArrayList<>(cd.constraints().size());
        for (ClassDefinition.ConstraintDefinition con : cd.constraints()) {
            // Lifted <owner>$constraint$<name> for sugar; the bound FQN for Door 4.
            String fqn = realizedFqn(con.realization(), SynthFqn.constraint(cd.qualifiedName(), con.name()));
            constraints.add(TypedConstraint.of(con.name(), fqn));
        }

        return new TypedClass(
                cd.qualifiedName(), typeParams, superFqns, properties, constraints, cd.isNative());
    }

    /**
     * The realizing-function FQN for a behavior hat: the user-bound FQN for a
     * Door-4 {@link Realization.Ref}, otherwise the lifted
     * {@code <owner>$...$<name>} FQN that Phase E synthesized for the sugar form.
     */
    private static String realizedFqn(Realization r, String liftedFqn) {
        return r instanceof Realization.Ref ref ? ref.functionFqn() : liftedFqn;
    }
}
