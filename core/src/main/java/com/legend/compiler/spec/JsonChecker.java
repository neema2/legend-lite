// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.compiler.spec;

import com.legend.compiler.element.type.ExprType;
import com.legend.compiler.element.type.Multiplicity;
import com.legend.compiler.element.type.PlatformTypes;
import com.legend.compiler.element.type.Type;
import com.legend.compiler.spec.typed.TypedJsonAccess;
import com.legend.compiler.spec.typed.TypedSpec;
import com.legend.protocol.spec.AppliedFunction;
import com.legend.protocol.spec.AppliedProperty;
import com.legend.protocol.spec.LambdaFunction;
import com.legend.protocol.spec.ValueSpecification;
import com.legend.protocol.spec.Variable;

import java.util.List;

/**
 * The {@code meta::json} TREE READS (real json.pure:32-70 classes) typed
 * by EMISSION onto the variant lane: a JSON element value IS the
 * database's JSON value, so {@code JSONObject.keyValuePairs->filter(kv|
 * $kv.key.value == key).value} is the member access, {@code JSONArray
 * .values} the element list, {@code JSONString.value} the text — each a
 * {@link TypedJsonAccess} the lowerer switches on. A to-many receiver
 * auto-maps (real pure's map-on-dot rule), so every emission is
 * per-element. Reads the model cannot represent on the carrier (a bare
 * {@code .key}, {@code .value} on an unrefined element) stay loud.
 */
final class JsonChecker {

    private JsonChecker() {
    }

    /** Property read over a JSON-family receiver ({@code source} already
     * typed by the caller). */
    static TypedSpec access(Typer t, AppliedProperty ap, TypedSpec source,
            Env env) {
        Type st = source.info().type();
        String fqn = ((Type.ClassType) st).fqn();
        Multiplicity m = source.info().multiplicity();
        if (m.isMany()) {
            // real pure: the . operator over a many receiver IS a map
            return t.synth(new AppliedFunction("map", List.of(ap.receiver(),
                    new LambdaFunction(List.of(new Variable("_jm")),
                            List.of(new AppliedProperty(new Variable("_jm"),
                                    ap.property()))))), env);
        }
        String p = ap.property();
        Type element = new Type.ClassType(PlatformTypes.JSON_ELEMENT);
        if (PlatformTypes.JSON_OBJECT.equals(fqn) && p.equals("keyValuePairs")) {
            return new TypedJsonAccess(source, TypedJsonAccess.Op.MEMBERS, null,
                    new ExprType(new Type.ClassType(PlatformTypes.JSON_KEY_VALUE),
                            Multiplicity.Bounded.ZERO_MANY));
        }
        if (PlatformTypes.JSON_ARRAY.equals(fqn) && p.equals("values")) {
            return new TypedJsonAccess(source, TypedJsonAccess.Op.ELEMENTS, null,
                    new ExprType(element, Multiplicity.Bounded.ZERO_MANY));
        }
        if (PlatformTypes.JSON_STRING.equals(fqn) && p.equals("value")) {
            return new TypedJsonAccess(source, TypedJsonAccess.Op.TEXT, null,
                    new ExprType(Type.Primitive.STRING, m));
        }
        if (PlatformTypes.JSON_NUMBER.equals(fqn) && p.equals("value")) {
            return new TypedJsonAccess(source, TypedJsonAccess.Op.NUMBER, null,
                    new ExprType(Type.Primitive.NUMBER, m));
        }
        if (PlatformTypes.JSON_BOOLEAN.equals(fqn) && p.equals("value")) {
            return new TypedJsonAccess(source, TypedJsonAccess.Op.BOOLEAN, null,
                    new ExprType(Type.Primitive.BOOLEAN, m));
        }
        if (PlatformTypes.JSON_KEY_VALUE.equals(fqn) && p.equals("value")) {
            return new TypedJsonAccess(source, TypedJsonAccess.Op.IDENTITY, null,
                    new ExprType(element, m));
        }
        if (PlatformTypes.JSON_KEY_VALUE.equals(fqn) && p.equals("key")) {
            throw new TypeInferenceException("JSONKeyValue.key reads only inside"
                    + " keyValuePairs->filter(kv|$kv.key.value == <key>) — the"
                    + " member access (a member is represented by its value)");
        }
        if (PlatformTypes.JSON_ELEMENT.equals(fqn) || PlatformTypes.JSON_NULL.equals(fqn)) {
            throw new TypeInferenceException("class '" + fqn + "' has no property '"
                    + p + "' — cast the element to its kind (@JSONObject, @JSONArray,"
                    + " @JSONString, …) first");
        }
        throw new TypeInferenceException("class '" + fqn + "' has no property '" + p + "'");
    }

    /** {@code X.keyValuePairs->filter(kv|$kv.key.value == key)} — the
     * member access idiom (real jsonExtension.pure:37 getValue's body),
     * typed as the member. Null when the call is not this shape. */
    static @com.legend.base.Nullable TypedSpec filter(Typer t, AppliedFunction af,
            Env env) {
        if (af.parameters().size() != 2
                || !(af.parameters().get(0) instanceof AppliedProperty kvs)
                || !kvs.property().equals("keyValuePairs")
                || !(af.parameters().get(1) instanceof LambdaFunction lf)
                || lf.parameters().size() != 1 || lf.body().size() != 1
                || !(lf.body().get(0) instanceof AppliedFunction eq)
                || eq.parameters().size() != 2
                || !isEqual(t, eq)) {
            return null;
        }
        String kv = lf.parameters().get(0).name();
        ValueSpecification keyExpr = null;
        for (int i = 0; i < 2; i++) {
            if (af_isKeyValueRead(eq.parameters().get(i), kv)) {
                keyExpr = eq.parameters().get(1 - i);
            }
        }
        if (keyExpr == null) {
            return null;
        }
        TypedSpec recv = t.synth(env.resolveAlias(kvs.receiver()), env);
        if (!PlatformTypes.isJsonElement(recv.info().type())) {
            return null;
        }
        if (recv.info().multiplicity().isMany()) {
            // objects[*].keyValuePairs->filter(key): the member of EACH
            // object (real pure's map-on-dot over the many receiver)
            return t.synth(new AppliedFunction("map", List.of(kvs.receiver(),
                    new LambdaFunction(List.of(new Variable("_jo")),
                            List.of(new AppliedFunction(af.function(), List.of(
                                    new AppliedProperty(new Variable("_jo"),
                                            "keyValuePairs"),
                                    lf)))))), env);
        }
        TypedSpec key = t.synth(keyExpr, env);
        if (key.info().type() != Type.Primitive.STRING) {
            throw new TypeInferenceException("JSON member key must be a String, got "
                    + key.info().type().typeName());
        }
        return new TypedJsonAccess(recv, TypedJsonAccess.Op.MEMBER, key,
                new ExprType(new Type.ClassType(PlatformTypes.JSON_KEY_VALUE),
                        Multiplicity.Bounded.ZERO_ONE));
    }

    /** {@code ^JSONArray(values = <elements>)}: the JSON array of its
     * elements BY EMISSION — {@code toVariant(elements)} cast to the
     * kind (real json.pure:56; the variant lane's array value). Null
     * for any other construction. */
    static @com.legend.base.Nullable TypedSpec newInstance(Typer t,
            com.legend.protocol.spec.NewInstance ni, Env env) {
        String fqn = t.model().findClass(ni.className())
                .map(c -> c.qualifiedName()).orElse(ni.className());
        if (!PlatformTypes.JSON_ARRAY.equals(fqn)) {
            return null;
        }
        var values = ni.first("values");
        ValueSpecification elements = values == null
                ? new com.legend.protocol.spec.PureCollection(List.of())
                : values.value();
        return t.synth(new AppliedFunction("cast", List.of(
                new AppliedFunction("meta::pure::functions::variant::convert::toVariant",
                        List.of(elements)),
                new com.legend.protocol.spec.TypeAnnotation.Named(
                        new com.legend.protocol.TypeExpression.NameRef(
                                PlatformTypes.JSON_ARRAY, null), null))), env);
    }

    /** {@code $kv.key.value}. */
    private static boolean af_isKeyValueRead(ValueSpecification v, String kv) {
        return v instanceof AppliedProperty val && val.property().equals("value")
                && val.receiver() instanceof AppliedProperty key
                && key.property().equals("key")
                && key.receiver() instanceof Variable var && var.name().equals(kv);
    }

    /** The call resolves to the registered {@code equal} native (the
     * parser's {@code ==} desugar) — catalog identity, not a spelling. */
    private static boolean isEqual(Typer t, AppliedFunction eq) {
        String equalFqn = com.legend.builtin.Pure.EQUAL__ANY_MANY__ANY_MANY.qualifiedName();
        return t.model().findFunction(eq.function()).stream()
                .anyMatch(f -> f.qualifiedName().equals(equalFqn));
    }
}
