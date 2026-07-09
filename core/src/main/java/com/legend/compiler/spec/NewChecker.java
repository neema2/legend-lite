package com.legend.compiler.spec;

import com.legend.compiler.element.type.ExprType;
import com.legend.compiler.element.Property;
import com.legend.compiler.element.type.Multiplicity;
import com.legend.compiler.element.type.Type;
import com.legend.compiler.spec.typed.TypedNewInstance;
import com.legend.compiler.spec.typed.TypedSpec;
import com.legend.parser.spec.NewInstance;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Instance construction {@code ^Class(prop=value, …)} (engine {@code NewChecker}).
 * Each named property must exist on the class; its value is type-checked and
 * validated against the property's declared type. The result is {@code Class[1]}.
 */
final class NewChecker {

    private NewChecker() {
    }

    static TypedSpec check(Typer t, NewInstance ni, Env env) {
        if (t.model().findClass(ni.className()).isEmpty()) {
            throw new TypeInferenceException("unknown class '" + ni.className() + "' in ^" + ni.className() + "(…)");
        }
        Map<String, TypedSpec> properties = new LinkedHashMap<>();
        ni.properties().forEach((name, key) -> {
            Property prop = t.model().findProperty(ni.className(), name).orElseThrow(() ->
                    new TypeInferenceException("class '" + ni.className() + "' has no property '" + name + "'"));
            TypedSpec value = t.synth(key.value(), env);
            t.kernel().unify(prop.type(), value.info().type(), new Bindings());   // value must conform
            // Multiplicity conformance: FULL subsumption, exactly real
            // pure's NewValidator (Multiplicity.subsumes) — the declared
            // range must contain the value's range, so even [0..1] into a
            // [1] property is a static error and the writer must spell
            // ->toOne(). Synthesized mapping bodies conform by EMISSION:
            // MappingNormalizer wraps store reads bound to [1] properties
            // in toOne(...) rather than this checker weakening — the
            // hand-written surface stays pure-compatible. EXCEPTION:
            // navigate() values — statically T[*] by design; conformance
            // is the mapping resolver's question.
            if (!(value instanceof com.legend.compiler.spec.typed.TypedNavigate)
                    && prop.multiplicity() instanceof Multiplicity.Bounded declared
                    && value.info().multiplicity() instanceof Multiplicity.Bounded actual) {
                boolean lowOk = actual.lower() >= declared.lower();
                boolean highOk = declared.upper() == null
                        || (actual.upper() != null && actual.upper() <= declared.upper());
                if (!lowOk || !highOk) {
                    throw new TypeInferenceException("property '" + name + "' of '"
                            + ni.className() + "' declares multiplicity " + declared
                            + " but the value has " + actual);
                }
            }
            properties.put(name, value);
        });
        return new TypedNewInstance(ni.className(), properties,
                new ExprType(new Type.ClassType(ni.className()), Multiplicity.Bounded.ONE));
    }
}
