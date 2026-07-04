package com.legend.compiler.spec;

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
            properties.put(name, value);
        });
        return new TypedNewInstance(ni.className(), properties,
                new ExprType(new Type.ClassType(ni.className()), Multiplicity.Bounded.ONE));
    }
}
