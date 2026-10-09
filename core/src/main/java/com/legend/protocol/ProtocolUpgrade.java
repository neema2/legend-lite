// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.protocol;

import com.legend.json.Json;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Older protocol JSON brought to the current shape, as upstream brings it on every read -- its
 * {@code CorePureProtocolExtension.getProtocolConverters}, ported (the reference checkout as spec).
 * Applied bottom-up, as Jackson applies a converter to each object after its children:
 *
 * <ul>
 *   <li>a variable whose type is named in {@code class} (the older protocol, read by upstream's variable reader
 *       itself, before any converter) has that type;</li>
 *   <li>a variable of type {@code Result} with no type argument is {@code Result<Any|1..*>};</li>
 *   <li>{@code ^BasicColumnSpecification(...)} is {@code col(...)}, {@code ^TdsOlapRank(...)} is
 *       {@code func(...)}, {@code ^AggregateValue(...)} is {@code agg(...)} and {@code ^Pair(...)}
 *       is {@code pair(...)}.</li>
 * </ul>
 *
 * Backwards compatibility only (the user, 2026-09-27: old TDS is accepted; lite's own clients
 * build with the Relation API). Everything else passes through unchanged.
 */
public final class ProtocolUpgrade {

    private ProtocolUpgrade() {
    }

    /** {@code node} with every converter applied; a new tree, {@code node} untouched. */
    public static Json.Node upgrade(Json.Node node) {
        if (node instanceof Json.Arr a) {
            List<Json.Node> items = new ArrayList<>(a.items().size());
            for (Json.Node i : a.items()) {
                items.add(upgrade(i));
            }
            return new Json.Arr(items);
        }
        if (!(node instanceof Json.Obj o)) {
            return node;
        }
        LinkedHashMap<String, Json.Node> fields = new LinkedHashMap<>();
        for (Map.Entry<String, Json.Node> e : o.fields().entrySet()) {
            fields.put(e.getKey(), upgrade(e.getValue()));
        }
        Json.Obj up = new Json.Obj(fields);
        String type = up.getStringOr("_type", "");
        if ("var".equals(type)) {
            return resultVariable(classType(up));
        }
        if ("func".equals(type) && "new".equals(up.getStringOr("function", ""))) {
            return newToFunction(up);
        }
        return up;
    }

    /** An object: {@link #upgrade} of an object is an object. */
    public static Json.Obj upgrade(Json.Obj node) {
        return (Json.Obj) upgrade((Json.Node) node);
    }

    // ---------------------------------------------------------------------

    /**
     * An older variable names its type in {@code class} ({@code Variable.VariableDeserializer}, "backward
     * compatibility - old protocol"): the generic type of that name, as the engine reads it, and before the
     * {@code Result} converter, as the engine's reader runs before its converters. A {@code class} beside a
     * {@code genericType}, or one that is not a name, is left as written, for the reader to refuse.
     */
    private static Json.Obj classType(Json.Obj v) {
        if (!(v.fields().get("class") instanceof Json.Str name) || v.has("genericType")) {
            return v;
        }
        LinkedHashMap<String, Json.Node> f = new LinkedHashMap<>(v.fields());
        f.remove("class");
        f.put("genericType", namedType(name.value()));
        return new Json.Obj(f);
    }

    /** The generic type of a name, with no arguments. */
    private static Json.Obj namedType(String path) {
        LinkedHashMap<String, Json.Node> raw = new LinkedHashMap<>();
        raw.put("_type", Json.str("packageableType"));
        raw.put("fullPath", Json.str(path));
        LinkedHashMap<String, Json.Node> g = new LinkedHashMap<>();
        g.put("rawType", new Json.Obj(raw));
        g.put("typeArguments", new Json.Arr(List.of()));
        g.put("multiplicityArguments", new Json.Arr(List.of()));
        g.put("typeVariableValues", new Json.Arr(List.of()));
        return new Json.Obj(g);
    }

    private static Json.Obj resultVariable(Json.Obj v) {
        Json.Obj gt = v.getObjOr("genericType", null);
        if (gt == null) {
            return v;
        }
        Json.Obj raw = gt.getObjOr("rawType", null);
        if (raw == null || !"packageableType".equals(raw.getStringOr("_type", ""))) {
            return v;
        }
        String path = raw.getStringOr("fullPath", "");
        Json.Arr args = gt.getArrOr("typeArguments", null);
        if (!("meta::pure::mapping::Result".equals(path) || "Result".equals(path))
                || (args != null && !args.items().isEmpty())) {
            return v;
        }
        LinkedHashMap<String, Json.Node> pureMany = new LinkedHashMap<>();
        pureMany.put("lowerBound", Json.num(1));
        LinkedHashMap<String, Json.Node> g = new LinkedHashMap<>(gt.fields());
        g.put("typeArguments", new Json.Arr(List.of(namedType("meta::pure::metamodel::type::Any"))));
        g.put("multiplicityArguments", new Json.Arr(List.of(new Json.Obj(pureMany))));
        LinkedHashMap<String, Json.Node> f = new LinkedHashMap<>(v.fields());
        f.put("genericType", new Json.Obj(g));
        return new Json.Obj(f);
    }

    private static Json.Obj newToFunction(Json.Obj af) {
        List<Json.Node> params = items(af, "parameters");
        if (params.isEmpty()) {
            return af;
        }
        String type = newType(params.get(0));
        if (type == null) {
            return af;
        }
        if (SPECIAL.contains(type) && (params.size() < 3 || !(params.get(2) instanceof Json.Obj keys)
                || !"collection".equals(keys.getStringOr("_type", "")))) {
            // the engine's converter casts the third argument to a collection (CorePureProtocolExtension) and fails
            throw Wire.refuse("a new of " + type + " whose keys are not a collection: the engine cannot read it");
        }
        return switch (type) {
            case "meta::pure::tds::BasicColumnSpecification", "BasicColumnSpecification" -> {
                List<Json.Node> ps = present(keyed(params, "func", "lambda"), keyed(params, "name", "string"),
                        keyed(params, "documentation", "string"));
                yield call(af, "meta::pure::tds::col", ps.size() == 3
                        ? "col_Function_1__String_1__String_1__BasicColumnSpecification_1_"
                        : "col_Function_1__String_1__BasicColumnSpecification_1_", ps);
            }
            case "meta::pure::tds::TdsOlapRank", "TdsOlapRank" -> call(af, "meta::pure::tds::func",
                    "func_FunctionDefinition_1__TdsOlapRank_1_", present(keyed(params, "func", "lambda")));
            case "meta::pure::functions::collection::AggregateValue", "AggregateValue" -> {
                List<Json.Node> ps = present(keyed(params, "name", "string"), keyed(params, "mapFn", "lambda"),
                        keyed(params, "aggregateFn", "lambda"));
                if (ps.size() == 3) {
                    yield call(af, "meta::pure::tds::agg",
                            "agg_String_1__FunctionDefinition_1__FunctionDefinition_1__AggregateValue_1_", ps);
                } else if (ps.size() == 2) {
                    yield call(af, "meta::pure::functions::collection::agg",
                            "agg_FunctionDefinition_1__FunctionDefinition_1__AggregateValue_1_", ps);
                }
                throw new IllegalArgumentException(
                        "Unexpected number of parameters values for AggregateValue, got " + ps.size());
            }
            case "meta::pure::functions::collection::Pair", "Pair" -> {
                Json.Node first = keyedAny(params, "first");
                Json.Node second = keyedAny(params, "second");
                yield call(af, "meta::pure::functions::collection::pair", "pair_U_1__V_1__Pair_1_",
                        List.of(first != null ? first : emptyCollection(), second != null ? second : emptyCollection()));
            }
            default -> af;
        };
    }

    /** The classes the engine's converter turns a {@code new} of into a call. */
    private static final java.util.Set<String> SPECIAL = java.util.Set.of("meta::pure::tds::BasicColumnSpecification",
            "BasicColumnSpecification", "meta::pure::tds::TdsOlapRank", "TdsOlapRank",
            "meta::pure::functions::collection::AggregateValue", "AggregateValue",
            "meta::pure::functions::collection::Pair", "Pair");

    /** The older pointers the engine reads as a {@code PackageableElementPtr} before its converter runs. */
    private static final java.util.Set<String> OLDER_POINTERS = java.util.Set.of("class", "enum", "mappingInstance",
            "databaseInstance");

    /**
     * {@code new}'s type: a generic type's first argument, or a packageable element -- today's pointer or an older one
     * ({@code class}, {@code enum}, ...; {@code primitiveType}'s {@code name} first), as the engine's readers make each
     * a pointer before the converter sees it.
     */
    private static @com.legend.base.Nullable String newType(Json.Node p) {
        if (!(p instanceof Json.Obj o)) {
            return null;
        }
        String t = o.getStringOr("_type", "");
        if (OLDER_POINTERS.contains(t)) {
            return o.getStringOr("fullPath", null);
        }
        if ("primitiveType".equals(t)) {
            return o.getStringOr("name", o.getStringOr("fullPath", null));
        }
        if ("genericTypeInstance".equals(t)) {
            Json.Obj gt = o.getObjOr("genericType", null);
            List<Json.Node> args = gt == null ? List.of() : items(gt, "typeArguments");
            if (!args.isEmpty() && args.get(0) instanceof Json.Obj a) {
                Json.Obj raw = a.getObjOr("rawType", null);
                if (raw != null && "packageableType".equals(raw.getStringOr("_type", ""))) {
                    return raw.getStringOr("fullPath", null);
                }
            }
            return null;
        }
        return "packageableElementPtr".equals(t) ? o.getStringOr("fullPath", null) : null;
    }

    /** The expression under key {@code key} of {@code new}'s third (collection) argument, when of {@code type}. */
    private static @com.legend.base.Nullable Json.Node keyed(List<Json.Node> params, String key, String type) {
        Json.Node e = keyedAny(params, key);
        return e instanceof Json.Obj o && type.equals(o.getStringOr("_type", "")) ? e : null;
    }

    private static @com.legend.base.Nullable Json.Node keyedAny(List<Json.Node> params, String key) {
        if (params.size() < 3 || !(params.get(2) instanceof Json.Obj c) || !"collection".equals(c.getStringOr("_type", ""))) {
            return null;
        }
        for (Json.Node v : items(c, "values")) {
            if (v instanceof Json.Obj ke && ke.getOr("key", null) instanceof Json.Obj k && key.equals(keyName(k))) {
                return ke.getOr("expression", null);
            }
        }
        return null;
    }

    /** A key's name: a string literal's value, or the older literal's one-item {@code values}. */
    private static @com.legend.base.Nullable String keyName(Json.Obj k) {
        String value = k.getStringOr("value", null);
        if (value != null) {
            return value;
        }
        List<Json.Node> values = items(k, "values");
        return values.size() == 1 && values.get(0) instanceof Json.Str s ? s.value() : null;
    }

    @SafeVarargs
    private static List<Json.Node> present(@com.legend.base.Nullable Json.Node... nodes) {
        List<Json.Node> out = new ArrayList<>();
        for (Json.Node n : nodes) {
            if (n != null) {
                out.add(n);
            }
        }
        return out;
    }

    private static Json.Obj call(Json.Obj af, String function, String fControl, List<Json.Node> params) {
        LinkedHashMap<String, Json.Node> f = new LinkedHashMap<>(af.fields());
        f.put("function", Json.str(function));
        f.put("fControl", Json.str(fControl));
        f.put("parameters", new Json.Arr(params));
        return new Json.Obj(f);
    }

    private static Json.Obj emptyCollection() {
        LinkedHashMap<String, Json.Node> f = new LinkedHashMap<>();
        f.put("_type", Json.str("collection"));
        f.put("values", new Json.Arr(List.of()));
        return new Json.Obj(f);
    }

    private static List<Json.Node> items(Json.Obj o, String key) {
        Json.Arr a = o.getArrOr(key, null);
        return a == null ? List.of() : a.items();
    }
}
