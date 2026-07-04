package com.legend.compiler.element;

import com.legend.compiler.ModelBuilder;
import com.legend.parser.element.MappingDefinition;

/**
 * Phase F's <strong>mapping-element</strong> compiler. Today a mapping's
 * compiled form IS its canonical binding table (there is no lazy
 * {@code TypedMapping} to materialize &mdash; see the F-5 note), so this
 * compiler's whole job is <strong>F.b binding integrity</strong>, checked
 * eagerly at context construction: every class binding names a real class
 * realized by a real function of shape {@code (): Class[*]}; every association
 * binding names a real association realized by a real predicate of shape
 * {@code (Source, Target): Boolean[1]}. Reference safety and structural shape
 * only &mdash; bodies are Phase G; when Phase H needs a {@code TypedMapping},
 * this is its home.
 */
final class MappingCompiler {

    private MappingCompiler() {
    }

    static void check(ModelBuilder model, TypeClassifier classifier, FunctionCompiler functions) {
        model.mappings().forEach(md -> {
            for (MappingDefinition.ClassBinding cb : md.classBindings()) {
                if (!classifier.isClassFqn(cb.classFqn())) {
                    throw new IllegalStateException("mapping '" + md.qualifiedName()
                          + "' binds unknown class '" + cb.classFqn() + "'");
                }
                String site = "mapping '" + md.qualifiedName()
                        + "' class binding for '" + cb.classFqn() + "'";
                functions.requireFunction(cb.functionFqn(), site);            // F.a/F.b existence
                functions.requireShape(cb.functionFqn(),                      // F.c shape: () : Class[*]
                        f -> f.parameters().isEmpty() && functions.returnsClassMany(f),
                        site, "of the form (): Class[*]");
            }
            for (MappingDefinition.AssociationBinding ab : md.associationBindings()) {
                if (model.findAssociation(ab.associationFqn()).isEmpty()) {
                    throw new IllegalStateException("mapping '" + md.qualifiedName()
                          + "' binds unknown association '" + ab.associationFqn() + "'");
                }
                String site = "mapping '" + md.qualifiedName()
                        + "' association binding for '" + ab.associationFqn() + "'";
                functions.requireFunction(ab.predicateFunctionFqn(), site);   // existence
                functions.requireShape(ab.predicateFunctionFqn(),             // F.c: (Src,Tgt) : Boolean[1]
                        f -> f.parameters().size() == 2 && FunctionCompiler.returnsBooleanOne(f),
                        site, "of the form (source, target): Boolean[1]");
            }
        });
    }
}
